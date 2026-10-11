#!/usr/bin/env python3
"""Named region anchors in /query hits: `named_regions` and the hit's `groups` carry the
region's span, id, attributes and token ids (a UI highlights the whole region); token groups are
numbered without the anchors; a region-only query with a condition is filtered before
the page is cut.

  test/region_groups_test.py --pando-index build/pando-index --server build/pando-server
"""
import argparse, json, os, socket, subprocess, sys, tempfile, time, urllib.request

# "the old man sleeps": S(NP(the old man) VP(sleeps)), ids w-1 … w-4
EVENTS = [
    {"type": "header", "version": 2, "positional": ["form", "id"], "structural": ["s", "node"], "nested": ["node"]},
    {"type": "token", "form": "the", "id": "w-1"}, {"type": "token", "form": "old", "id": "w-2"},
    {"type": "token", "form": "man", "id": "w-3"}, {"type": "token", "form": "sleeps", "id": "w-4"},
    {"type": "region", "struct": "s", "start_pos": 0, "end_pos": 3, "attrs": {"id": "s-1"}},
    {"type": "region", "struct": "node", "start_pos": 0, "end_pos": 3, "attrs": {"cat": "S", "id": "n1"}},
    {"type": "region", "struct": "node", "start_pos": 0, "end_pos": 2, "attrs": {"cat": "NP", "id": "n2"}},
    {"type": "region", "struct": "node", "start_pos": 3, "end_pos": 3, "attrs": {"cat": "VP", "id": "n3"}},
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
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

        def query(body):
            for _ in range(50):
                try:
                    req = urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps(body).encode())
                    return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]
                except (ConnectionError, urllib.error.URLError):
                    time.sleep(0.1)
            raise RuntimeError("server did not start")
        try:
            r = query({"query": 's:<node cat="S"> np:<node cat="NP"> w:[form="the"] :: rchild(s, np) = 1', "limit": 5})
            h = r["hits"][0]
            nr = h.get("named_regions", {})
            if nr.get("np", {}).get("start") != 0 or nr.get("np", {}).get("end") != 2 or nr.get("np", {}).get("id") != "n2":
                fails.append(f"named_regions np: {nr.get('np')}")
            if nr.get("s", {}).get("end") != 3:
                fails.append(f"named_regions s: {nr.get('s')}")
            groups = {g["name"]: g for g in h.get("groups", [])}
            if groups.get("np", {}).get("tok_ids") != ["w-1", "w-2", "w-3"] or groups.get("np", {}).get("region") != "node":
                fails.append(f"hit group np: {groups.get('np')}")
            # the region's own attributes, all of them (the UI picks what to show)
            if groups.get("np", {}).get("attrs") != {"cat": "NP", "id": "n2"} or nr.get("np", {}).get("attrs") != {"cat": "NP", "id": "n2"}:
                fails.append(f"region attrs np: {groups.get('np', {}).get('attrs')} / {nr.get('np', {}).get('attrs')}")
            if groups.get("w", {}).get("index") != 0 or groups.get("w", {}).get("start") != 0:
                fails.append(f"token group w (numbered without the anchors): {groups.get('w')}")
            names = [g["name"] for g in r.get("groups", [])]
            if names != ["w", "s", "np"]:
                fails.append(f"result groups: {names}")
            f = query({"query": 'np:<node cat="NP"> [form="the"]', "limit": 1, "fragment": True})["hits"][0]
            hg = {g["id"]: g["tok_ids"] for g in f.get("highlight_map", {}).get("groups", [])}
            if hg.get("np") != ["w-1", "w-2", "w-3"]:
                fails.append(f"fragment highlight np: {hg.get('np')}")
            # a region-only query with a condition: filtered before the page is cut (the
            # first NP-sized node is not long enough; a page of 1 used to come back empty)
            e = query({"query": 'n:<node> :: tcnt(n) < 2', "limit": 1})
            if len(e["hits"]) != 1 or e["hits"][0]["match_start"] != 3:
                fails.append(f"region-only page of 1: {[(h['match_start'], h['match_end']) for h in e['hits']]}")
            e = query({"query": 'n:<node> :: tcnt(n) < 2', "limit": 1, "total": True})
            if e["page"]["total"] != 1 or e["hits"][0]["match_start"] != 3:
                fails.append(f"region-only total: {e['page']} {[(h['match_start'], h['match_end']) for h in e['hits']]}")
            # a region-only query has no query token: no token group, tokens without one
            ro = query({"query": 'n:<node cat="NP">', "limit": 1, "fragment": True})["hits"][0]
            if [g["name"] for g in ro.get("groups", [])] != ["n"] or any("group" in t for t in ro.get("tokens", [])):
                fails.append(f"region-only groups: {ro.get('groups')}")
            if [g["id"] for g in ro.get("highlight_map", {}).get("groups", [])] != ["n"]:
                fails.append(f"region-only highlight groups: {ro.get('highlight_map', {}).get('groups')}")
            p = query({"query": '[form="man"]', "limit": 1})
            if [g["name"] for g in p["groups"]] != ["t1"] or "named_regions" in p["hits"][0]:
                fails.append(f"plain query changed: {p['groups']}")
        finally:
            srv.terminate()
            srv.wait()
    if fails:
        print("\n".join("FAIL " + x for x in fails))
        sys.exit(1)
    print("PASS region_groups")


if __name__ == "__main__":
    main()
