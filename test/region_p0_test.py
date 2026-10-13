#!/usr/bin/env python3
"""Region correctness (dev/REGION-JOINS-PLAN.md P0): inline anchor clauses rcontains() and
contains(), innermost binding, zero-width tcnt, token-group matches, and loud errors where
anchors used to be dropped silently.

  test/region_p0_test.py --pando-index build/pando-index --server build/pando-server --pando build/pando
"""
import argparse, json, os, socket, subprocess, sys, tempfile, time, urllib.request

# "the old man sleeps .": S(NP(the old man) VP(sleeps)); a pause between "man" and "sleeps"
# (zero-width: start_pos > end_pos); a discontinuous error over "the" and "man".
EVENTS = [
    {"type": "header", "version": 2, "positional": ["form", "id", "err_gid", "err_code"],
     "multivalue": ["err_gid", "err_code"], "structural": ["s", "node", "pause"], "nested": ["node"],
     "zerowidth": ["pause"], "token_groups": ["err"], "token_group_membership": ["err_gid"],
     "token_group_prop_attrs": {"err": ["code"]}},
    {"type": "token", "form": "the", "id": "w-1"}, {"type": "token", "form": "old", "id": "w-2"},
    {"type": "token", "form": "man", "id": "w-3"}, {"type": "token", "form": "sleeps", "id": "w-4"},
    {"type": "token", "form": ".", "id": "w-5"},
    {"type": "region", "struct": "s", "start_pos": 0, "end_pos": 4, "attrs": {"id": "s-1"}},
    {"type": "region", "struct": "node", "start_pos": 0, "end_pos": 3, "attrs": {"cat": "S", "id": "n1"}},
    {"type": "region", "struct": "node", "start_pos": 0, "end_pos": 2, "attrs": {"cat": "NP", "id": "n2"}},
    {"type": "region", "struct": "node", "start_pos": 3, "end_pos": 3, "attrs": {"cat": "VP", "id": "n3"}},
    {"type": "region", "struct": "pause", "start_pos": 3, "end_pos": 2, "attrs": {"dur": "0.8"}},
    {"type": "region", "struct": "err", "spans": [[0, 0], [2, 2]], "attrs": {"id": "e1", "code": "AGR"}},
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando", required=True)
    o = ap.parse_args()
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        src, idx = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
        with open(src, "w") as fh:
            for e in EVENTS:
                fh.write(json.dumps(e) + "\n")
        subprocess.run([o.pando_index, "--format", "jsonl", src, idx], check=True, capture_output=True)
        port = free_port()
        srv = subprocess.Popen([o.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def post(body):
            for _ in range(50):
                try:
                    req = urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps(body).encode())
                    try:
                        return json.loads(urllib.request.urlopen(req, timeout=30).read())
                    except urllib.error.HTTPError as e:
                        return json.loads(e.read() or b"{}")
                except (ConnectionError, urllib.error.URLError):
                    time.sleep(0.1)
            raise RuntimeError("server did not start")

        def total(q):
            r = post({"query": q, "limit": 50})
            if "result" not in r:
                return f"error: {r.get('error')}"
            return len(r["result"]["hits"])

        def error_of(q):
            r = post({"query": q, "limit": 5})
            return "" if "result" in r else str(r.get("error") or r)

        def check(name, got, want):
            if got != want:
                fails.append(f"{name}: got {got!r}, want {want!r}")

        try:
            # inline rcontains(peer): this row tree-dominates the peer (reflexive)
            check("rcontains: S and NP both dominate NP", total('n:<node cat="NP"> m:<node rcontains(n)> [form="the"]'), 2)
            check("rcontains: NP does not dominate S", total('s:<node cat="S"> n:<node cat="NP" rcontains(s)> [form="the"]'), 0)
            # inline contains(peer): span containment
            check("contains: S contains NP", total('n:<node cat="NP"> s:<node cat="S" contains(n)> [form="the"]'), 1)
            check("contains: NP does not contain S", total('s:<node cat="S"> n:<node cat="NP" contains(s)> [form="the"]'), 0)
            # a filtered query whose page is not full reports an exact total
            r = post({"query": 's:<node cat="S"> n:<node cat="NP"> [form="the"] :: rchild(s, n) = 1', "limit": 50})
            check("exact total on a partial page", r.get("result", {}).get("page", {}).get("total_exact"), True)
            # fanout: both nodes starting at "the" bind
            check("fanout", total('n:<node> [form="the"]'), 2)
            # zero-width: tcnt 0, enumerated as a region
            check("zero-width tcnt", total('p:<pause> :: tcnt(p) = 0'), 1)
            # token group: one match with both parts
            r = post({"query": '<err code="AGR">', "limit": 5})
            hits = r.get("result", {}).get("hits", [])
            check("token group hits", len(hits), 1)
            if hits:
                check("token group tokens", [t.get("form") for t in hits[0].get("tokens", [])], ["the", "man"])
            # loud errors instead of silently different queries
            for q, frag in [
                ('[form="the"] <s>', "no token after it"),
                ('<s>', "needs a label"),
                ('a:<node cat="S"> b:<node cat="NP"> :: rchild(a, b) = 1', "Only one region without a token"),
                ('[form="the"] containing nosuch', "Unknown structure 'nosuch'"),
                ('<nosuch>', "Unknown region anchor"),
            ]:
                e = error_of(q)
                if frag not in e:
                    fails.append(f"error for {q!r}: {e!r} (want …{frag}…)")
            # `<` followed by a letter is only a region anchor when its body is one
            check("`<b` as comparison in ::", total('a:[form="the"] [] b:[form="man"] :: a <b & distance(a, b) > 1'), 1)
            # queries that must keep working
            check("token-anchored pair", total('s:<node cat="S"> n:<node cat="NP"> [form="the"] :: rchild(s, n) = 1'), 1)
            check("region-only", total('n:<node cat="NP">'), 1)
            check("end anchor", total('[form="man"] </node>'), 1)
        finally:
            srv.terminate()
            srv.wait()

        # innermost binding (CLI only): one binding per anchor, the tightest node
        p = subprocess.run([o.pando, idx, '--anchor-binding', 'innermost', 'n:<node> [form="the"]', '--count-only'],
                           capture_output=True, text=True)
        check("innermost count", p.stdout.strip().split()[-1] if p.stdout.strip() else p.stderr.strip(), "1")
        # TIGER macros use the corpus' category attribute (`cat` here)
        p = subprocess.run([o.pando, idx, '--cql', 'tiger', 'idom S NP', '--count-only'], capture_output=True, text=True)
        check("tiger idom with cat", p.stdout.strip().split()[-1] if p.stdout.strip() else p.stderr.strip(), "1")

    if fails:
        print("\n".join("FAIL " + x for x in fails))
        sys.exit(1)
    print("PASS region_p0")


if __name__ == "__main__":
    main()
