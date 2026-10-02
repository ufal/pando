#!/usr/bin/env python3
"""Versioned index directories (index_publish.h): pando-index --publish ROOT,
--upgrade ROOT --publish, index_id, in-place refusal, pruning, the publish lock,
and what a running pando-server reports when a newer version appears.

  test/publish_test.py --pando build/pando --pando-index build/pando-index \\
      --server build/pando-server --conllu test/data/sample.conllu
"""

import argparse
import fcntl
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import Server, check, FAILS  # noqa: E402


def run(cmd, ok=True):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if ok and p.returncode != 0:
        raise RuntimeError(f"{cmd}: exit {p.returncode}: {p.stderr[-600:]}")
    return p


def index_id(d):
    with open(os.path.join(d, "corpus.info")) as f:
        ids = [l.strip()[9:] for l in f if l.startswith("index_id=")]
    return ids[-1] if ids else ""


def versions(root):
    return sorted(v for v in os.listdir(os.path.join(root, "versions")) if not v.startswith("."))


def total(pando, corpus, q):
    p = run([pando, corpus, q, "--json", "--total", "--limit", "1"])
    return json.loads(p.stdout)["result"]["page"]["total"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    ap.add_argument("--conllu", required=True)
    o = ap.parse_args()
    idx, src = o.pando_index, o.conllu
    q = '[upos="NOUN"]'

    with tempfile.TemporaryDirectory() as tmp:
        root = os.path.join(tmp, "mycorpus")
        cur = os.path.join(root, "current")

        # 1. a build published: versions/<id>, current -> it, index_id = <id>
        run([idx, src, "--publish", root])
        v = versions(root)
        check(len(v) == 1 and os.path.islink(cur) and os.readlink(cur) == os.path.join("versions", v[0]),
              f"first publish: {v} {os.readlink(cur) if os.path.islink(cur) else None}")
        check(index_id(cur) == v[0], f"index_id {index_id(cur)} = version {v[0]}")
        check(not [x for x in os.listdir(os.path.join(root, "versions")) if x.startswith(".building")],
              "no building directory left")
        n1 = total(o.pando, cur, q)
        p = run([o.pando, cur, "show info", "--json"], ok=False)
        if p.returncode == 0:
            name = json.loads(p.stdout).get("result", {}).get("name")
            check(name in (None, "mycorpus"), f"show info name is the root's: {name}")

        # 2. a server on ROOT/current keeps its version and reports a newer one
        srv = Server(o.server, cur)
        try:
            st, h = srv.get("/health")
            ix = h.get("index", {})
            check(st == 200 and ix.get("index_id") == v[0] and ix.get("newer_on_disk") is False
                  and ix.get("published_root", "").endswith("mycorpus"), f"/health index: {ix}")
            run([idx, src, "--publish", root])
            v2 = versions(root)
            check(len(v2) == 2 and index_id(cur) == v2[-1], f"second publish: {v2}")
            st, h = srv.get("/health")
            ix = h.get("index", {})
            check(ix.get("index_id") == v[0] and ix.get("newer_on_disk") is True
                  and ix.get("current_version", "").endswith(v2[-1]), f"/health after publish: {ix}")
            st, r = srv.post("/query", {"query": q, "total": True, "limit": 1})
            check(st == 200 and r["result"]["page"]["total"] == n1, f"old version still answers: {st}")
            st, r = srv.get("/info")
            info_ix = r.get("result", {}).get("index", {})
            check(info_ix.get("index_id") == v[0] and info_ix.get("disk_bytes", 0) > 0
                  and any(a.get("attr") == "form" and a.get("rev") == "plain" for a in info_ix.get("attrs", [])),
                  f"/info index profile: { {k: info_ix.get(k) for k in ('index_id', 'disk_bytes', 'attrs')} }")
        finally:
            srv.close()

        # 3. upgrade a copy: the new version has packed postings, the old one is untouched
        before = versions(root)[-1]
        run([idx, "--upgrade", root, "--publish", "--packed-rev"])
        v3 = versions(root)
        new = v3[-1]
        check(new != before and index_id(cur) == new, f"upgrade --publish: {v3}")
        newdir, olddir = os.path.join(root, "versions", new), os.path.join(root, "versions", before)
        check(os.path.exists(os.path.join(newdir, "form.rev.pfb"))
              and not os.path.exists(os.path.join(olddir, "form.rev.pfb")),
              "packed postings only in the upgraded copy")
        check(total(o.pando, cur, q) == n1, "same total after the upgrade")

        # 4. no in-place changes to a published root or version
        for cmd, what in (([idx, "--upgrade", cur, "--packed-rev"], "--upgrade ROOT/current"),
                          ([idx, "--upgrade", root], "--upgrade ROOT"),
                          ([idx, src, cur], "build into ROOT/current"),
                          ([idx, src, root], "build into ROOT")):
            p = run(cmd, ok=False)
            check(p.returncode != 0 and "--publish" in p.stderr, f"{what} refused: {p.returncode} {p.stderr[-200:]}")
        check(versions(root) == v3, "refusals changed nothing")

        # 5. pruning: --keep 2 keeps the new and the previous version
        for _ in range(2):
            run([idx, src, "--publish", root, "--keep", "2"])
        v5 = versions(root)
        check(len(v5) == 2 and index_id(cur) == v5[-1], f"--keep 2: {v5}")

        # 6. one publish per root at a time
        with open(os.path.join(root, ".publish.lock"), "w") as lk:
            fcntl.flock(lk, fcntl.LOCK_EX)
            p = run([idx, src, "--publish", root], ok=False)
            check(p.returncode != 0 and "another publish" in p.stderr, f"locked: {p.returncode} {p.stderr[-200:]}")
        check(versions(root) == v5, "a refused publish leaves nothing behind")

        # 7. --publish on an index directory itself is refused
        plain = os.path.join(tmp, "plain")
        run([idx, src, plain])
        p = run([idx, src, "--publish", plain], ok=False)
        check(p.returncode != 0 and "index directory itself" in p.stderr, f"--publish on an index dir: {p.stderr[-200:]}")

        # 8. a plain directory: every --upgrade gives a new index_id, a server sees it
        id0 = index_id(plain)
        check(bool(id0), "plain build has an index_id")
        srv = Server(o.server, plain)
        try:
            st, h = srv.get("/health")
            check(h.get("index", {}).get("newer_on_disk") is False and h["index"].get("published_root") is None,
                  f"plain /health: {h.get('index')}")
            run([idx, "--upgrade", plain])
            check(index_id(plain) != id0, "--upgrade changes index_id")
            st, h = srv.get("/health")
            check(h.get("index", {}).get("newer_on_disk") is True, f"plain dir upgraded under a server: {h.get('index')}")
        finally:
            srv.close()

    print(f"{'OK' if not FAILS else 'FAILED'}: {len(FAILS)} failures")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
