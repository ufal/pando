#!/usr/bin/env python3
"""Dependency subtrees as bindings (dev/REGION-JOINS-PLAN.md P0b): `np:dep_subtree(n)` after `::`,
the subtree in /query hits (spans, tok_ids, head), gaps(), and errors for forms that are not
supported.

  test/subtree_test.py --pando-index build/pando-index --server build/pando-server --pando build/pando
"""
import argparse, json, os, socket, subprocess, sys, tempfile, time, urllib.request


def tok(tid, form, upos, deprel, head):
    return {"type": "token", "id": tid, "form": form, "upos": upos, "deprel": deprel, "head": head}


# 1: "a man came who smiled" — the relative clause is extraposed: subtree of "man" = a man … who smiled
# 2: "the dog barked" — contiguous subtree
S1 = [tok("w-1", "a", "DET", "det", 2), tok("w-2", "man", "NOUN", "nsubj", 3), tok("w-3", "came", "VERB", "root", 0),
      tok("w-4", "who", "PRON", "nsubj", 5), tok("w-5", "smiled", "VERB", "acl:relcl", 2)]
S2 = [tok("w-6", "the", "DET", "det", 2), tok("w-7", "dog", "NOUN", "nsubj", 3), tok("w-8", "barked", "VERB", "root", 0)]


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

    def check(name, got, want):
        if got != want:
            fails.append(f"{name}: got {got!r}, want {want!r}")

    with tempfile.TemporaryDirectory() as tmp:
        src, idx = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
        with open(src, "w") as fh:
            fh.write(json.dumps({"type": "header", "version": 2, "positional": ["form", "upos", "deprel", "id"],
                                 "structural": ["s"]}) + "\n")
            pos = 0
            for sent in (S1, S2):
                for t in sent:
                    fh.write(json.dumps(t) + "\n")
                fh.write(json.dumps({"type": "region", "struct": "s", "start_pos": pos, "end_pos": pos + len(sent) - 1, "attrs": {}}) + "\n")
                pos += len(sent)
        b = subprocess.run([o.pando_index, "--format", "jsonl", src, idx], capture_output=True, text=True)
        if b.returncode != 0:
            print("index build failed:", b.stderr)
            sys.exit(1)

        def cli(q, *extra):
            p = subprocess.run([o.pando, idx, q, *extra], capture_output=True, text=True)
            return (p.stdout + p.stderr).strip()

        # `::` form and the (undocumented) inline form find the same
        check("count :: form", cli('n:[deprel="nsubj" & upos="NOUN"] :: np:dep_subtree(n) & gaps(np) > 0', "--count-only").split()[-1], "1")
        check("count inline form", cli('n:[deprel="nsubj" & upos="NOUN"] np:dep_subtree(n) :: gaps(np) > 0', "--count-only").split()[-1], "1")
        out = cli('n:[deprel="nsubj" & upos="NOUN"] :: np:dep_subtree(n); tabulate forms(np), gaps(np), tcnt(np)')
        if "a man ... who smiled\t1\t4" not in out or "the dog\t0\t2" not in out:
            fails.append(f"tabulate forms/gaps/tcnt: {out!r}")
        out = cli('n:[deprel="nsubj" & upos="NOUN"] :: np:dep_subtree(n); count by gaps(np)')
        if not any(l.split("\t")[:2] == ["1", "1"] for l in out.splitlines()) or not any(l.split("\t")[:2] == ["0", "1"] for l in out.splitlines()):
            fails.append(f"count by gaps: {out!r}")
        # not supported: a subtree as a query of its own, `=` instead of `name:`
        for q, frag in [('n = [deprel="nsubj"]; np = dep_subtree(n) :: tcnt(np) > 1', "not supported"),
                        ('n:[deprel="nsubj"] :: np = dep_subtree(n)', "name:` binds")]:
            e = cli(q, "--count-only")
            if frag not in e:
                fails.append(f"error for {q!r}: {e!r} (want …{frag}…)")

        port = free_port()
        srv = subprocess.Popen([o.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            r = None
            for _ in range(50):
                try:
                    body = {"query": 'n:[deprel="nsubj" & upos="NOUN"] :: np:dep_subtree(n) & gaps(np) > 0', "limit": 5, "fragment": True}
                    req = urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps(body).encode())
                    r = json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]
                    break
                except (ConnectionError, urllib.error.URLError):
                    time.sleep(0.1)
            if r is None:
                raise RuntimeError("server did not start")
            check("result groups", [(g["name"], g.get("region")) for g in r["groups"]], [("n", None), ("np", "dep_subtree")])
            h = r["hits"][0]
            check("hit tokens (query tokens only)", [t.get("form") for t in h["tokens"]], ["man"])
            g = {x["name"]: x for x in h["groups"]}.get("np", {})
            check("subtree group region", g.get("region"), "dep_subtree")
            check("subtree tok_ids", g.get("tok_ids"), ["w-1", "w-2", "w-4", "w-5"])
            check("subtree spans", g.get("spans"), [[0, 1], [3, 4]])
            check("subtree head", g.get("region_id"), "w-2")
            # tabulate rows carry their sentence: tokens and the hit's own tokens (gap unmarked)
            req = urllib.request.Request(f"http://127.0.0.1:{port}/run", data=json.dumps(
                {"cql": 'n:[deprel="nsubj" & upos="NOUN"] :: np:dep_subtree(n) & gaps(np) > 0; tabulate forms(np)'}).encode())
            tr = json.loads(urllib.request.urlopen(req, timeout=30).read())
            tr = tr.get("result", tr)
            rc = (tr.get("row_contexts") or [None])[0] or {}
            marked = [t for i, t in enumerate(rc.get("tokens", [])) if i in rc.get("hit", [])]
            check("tabulate row context tokens", rc.get("tokens"), ["a", "man", "came", "who", "smiled"])
            check("tabulate row context hit (gap unmarked)", marked, ["a", "man", "who", "smiled"])
            check("tabulate row context token id", rc.get("tok_id"), "w-1")
            hg = {x["id"]: x["tok_ids"] for x in h.get("highlight_map", {}).get("groups", [])}
            check("fragment highlight np", hg.get("np"), ["w-1", "w-2", "w-4", "w-5"])
            check("fragment highlight n", hg.get("n"), ["w-2"])
        finally:
            srv.terminate()
            srv.wait()

    if fails:
        print("\n".join("FAIL " + x for x in fails))
        sys.exit(1)
    print("PASS subtree")


if __name__ == "__main__":
    main()
