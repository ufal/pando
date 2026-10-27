#!/usr/bin/env python3
"""User overlays kept with an index (`<index>/overlays/<layer>/`) are merged when pando-server
opens the corpus: /info lists the layer and its `overlay-<layer>-…` attributes, queries and
`count by` see the labels, and an overlay older than the main index (the corpus was reindexed,
so its positions no longer apply) is left out and reported instead of failing the open.

  test/overlay_server_test.py --pando-index build/pando-index --server build/pando-server
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)
    return cond


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def get(base, path):
    with urllib.request.urlopen(base + path, timeout=30) as r:
        return json.loads(r.read())


def post(base, path, body):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def serve(server, index):
    port = free_port()
    p = subprocess.Popen([server, index, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            get(base, "/health")
            return p, base
        except Exception:
            time.sleep(0.05)
    p.kill()
    raise SystemExit("pando-server did not start")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    a = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="pando-overlay-")
    vrt = os.path.join(tmp, "c.vrt")
    index = os.path.join(tmp, "idx")
    n = 0
    with open(vrt, "w") as out:
        out.write("<!-- positional-attributes: word lemma id -->\n")
        for d in range(3):
            out.write(f'<text id="xmlfiles/d{d}.xml">\n<s>\n')
            for w in range(20):
                word = "feel" if w % 5 == 0 else "x"
                out.write(f"{word}\t{word}\tw-{w + 1}\n")
                n += 1
            out.write("</s>\n</text>\n")
    subprocess.run([a.pando_index, "--format", "vertical", vrt, index],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # ann labels three "feel" tokens (positions 0, 5, 25)
    labels = [(0, "Touch", "TRANS"), (5, "Experience", "INTRANS"), (25, None, "INTRANS")]
    jl = os.path.join(tmp, "ann.jsonl")
    with open(jl, "w") as out:
        out.write(json.dumps({"type": "header", "version": 2,
                              "positional": ["cl_gid", "cl_sense", "cl_transitivity"],
                              "token_groups": ["cl"], "token_group_membership": ["cl_gid"],
                              "token_group_prop_attrs": {"cl": ["sense", "transitivity"]},
                              "multivalue": ["cl_gid"], "token_count": n}) + "\n")
        for i, (pos, sense, tr) in enumerate(labels):
            attrs = {"id": f"g{i}", "transitivity": tr}
            if sense:
                attrs["sense"] = sense
            out.write(json.dumps({"type": "region", "struct": "cl", "start_pos": pos, "end_pos": pos,
                                  "attrs": attrs, "region_id": f"g{i}"}) + "\n")
    time.sleep(1.1)   # the overlay must be newer than the main corpus.info
    subprocess.run([a.pando_index, "--format", "jsonl", "--overlay-index", "--index-dir", index, jl,
                    os.path.join(index, "overlays", "ann")],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    p, base = serve(a.server, index)
    try:
        info = get(base, "/info")["result"]
        check([o["layer"] for o in info["server"].get("overlays", [])] == ["ann"], f"overlays in /info: {info['server'].get('overlays')}")
        check("overlay-ann-cl-transitivity" in json.dumps(info["attributes"]), "overlay attribute listed in /info")
        r = post(base, "/query", {"query": '[overlay-ann-cl-transitivity="INTRANS"]', "limit": 10})["result"]
        check(sorted(h["match_start"] for h in r["hits"]) == [5, 25], f"INTRANS hits: {r['hits']}")
        r = post(base, "/query", {"query": '[lemma="feel" & overlay-ann-cl-sense="Touch"]', "limit": 10})["result"]
        check([h["match_start"] for h in r["hits"]] == [0], "label combined with a token attribute")
        try:
            r = post(base, "/query", {"query": '[overlay-bob-cl-transitivity="TRANS"]', "limit": 10})
            bad = json.dumps(r)
        except urllib.error.HTTPError as e:
            bad = e.read().decode()
        check("Unknown attribute 'overlay-bob-cl-transitivity'" in bad, f"an overlay that is not loaded is an error: {bad[:200]}")
        r = post(base, "/run", {"query": '[overlay-ann-cl-transitivity!="_"]; count by match.overlay-ann-cl-transitivity'})
        txt = json.dumps(r)
        check('"TRANS"' in txt and '"INTRANS"' in txt, f"count by the label: {txt[:300]}")
    finally:
        p.kill()
        p.wait()

    # reindexing the main corpus makes the overlay stale: left out, reported
    time.sleep(1.1)
    os.utime(os.path.join(index, "corpus.info"))
    p, base = serve(a.server, index)
    try:
        info = get(base, "/info")["result"]
        check(not info["server"].get("overlays"), "stale overlay left out")
        check(any("older than the main index" in x for x in info["server"].get("overlays_skipped", [])),
              f"stale overlay reported: {info['server'].get('overlays_skipped')}")
    finally:
        p.kill()
        p.wait()

    print("overlay_server:", "FAIL" if FAILS else "ok", flush=True)
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
