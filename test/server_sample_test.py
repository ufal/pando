#!/usr/bin/env python3
"""pando-server /query "sample" / "shuffle" / "seed" (KonText random sample and shuffle).

On an index built from a CoNLL-U file checks, for a few queries:

  * sample N: N hits (all when fewer), in corpus order, all of them hits of the
    query; page.total = the sample's size, result.sample.population = all hits;
  * the same seed gives the same sample on every page (offset) and in every
    request; another seed gives another sample;
  * shuffle: every hit once over the pages, in a random order that is the same
    for the same seed (page 2 continues page 1); total = all hits;
  * sample + shuffle: the sample's hits in a random order;
  * with a post-filter (`within s having`) the sample is drawn from the hits that pass;
  * sample / shuffle with a session → 404 / 400 (not stored in sessions).

  test/server_sample_test.py --server build/pando-server --pando-index build/pando-index \\
      --conllu test/data/sample.conllu
"""

import argparse
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import Server, check, FAILS  # noqa: E402


def starts(r):
    return [(h["match_start"], h["match_end"]) for h in r.get("result", {}).get("hits", [])]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index")
    ap.add_argument("--conllu")
    ap.add_argument("--corpus")
    opts = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        corpus = opts.corpus
        if not corpus:
            corpus = os.path.join(tmp, "idx")
            subprocess.run([opts.pando_index, opts.conllu, corpus], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        srv = Server(opts.server, corpus)
        try:
            for q in ('[upos="NOUN"]', '[upos="ADJ"] [upos="NOUN"]', '[upos="DET"] []{0,2} [upos="NOUN"]',
                      '[upos="NOUN"] within s having [upos="ADJ"]'):
                st, full = srv.post("/query", {"query": q, "limit": 100000, "total": True})
                check(st == 200, f"{q}: full page {st}")
                all_hits = starts(full)
                n = full["result"]["page"]["total"]
                check(len(all_hits) == n, f"{q}: full page holds every hit ({len(all_hits)} of {n})")
                hitset = set(all_hits)
                k = max(1, n // 3)

                # sample: size, order, membership, page consistency
                st, a = srv.post("/query", {"query": q, "limit": k, "sample": k, "seed": 7})
                check(st == 200, f"{q}: sample {st}")
                sa = starts(a)
                smp = a["result"].get("sample", {})
                check(len(sa) == k and a["result"]["page"]["total"] == k, f"{q}: sample of {k} ({len(sa)})")
                check(smp.get("population") == n and smp.get("size") == k, f"{q}: sample fields {smp}")
                check(sa == sorted(sa), f"{q}: sample in corpus order")
                check(set(sa) <= hitset, f"{q}: sample hits are hits of the query")
                half = k // 2
                st, p1 = srv.post("/query", {"query": q, "limit": half, "sample": k, "seed": 7})
                st, p2 = srv.post("/query", {"query": q, "limit": k - half, "offset": half, "sample": k, "seed": 7})
                check(starts(p1) + starts(p2) == sa, f"{q}: sample pages concatenate")
                st, b = srv.post("/query", {"query": q, "limit": k, "sample": k, "seed": 7})
                check(starts(b) == sa, f"{q}: same seed, same sample")
                if n > 5:
                    st, c = srv.post("/query", {"query": q, "limit": k, "sample": k, "seed": 8})
                    check(starts(c) != sa, f"{q}: another seed, another sample")
                st, big = srv.post("/query", {"query": q, "limit": n + 10, "sample": n + 10, "seed": 7})
                check(sorted(starts(big)) == sorted(all_hits) and big["result"]["page"]["total"] == n,
                      f"{q}: a sample larger than the hits is all of them")

                # shuffle: a permutation, stable over pages
                st, s1 = srv.post("/query", {"query": q, "limit": half, "shuffle": True, "seed": 5})
                st, s2 = srv.post("/query", {"query": q, "limit": n - half, "offset": half, "shuffle": True, "seed": 5})
                perm = starts(s1) + starts(s2)
                check(sorted(perm) == sorted(all_hits), f"{q}: shuffle pages hold every hit once")
                check(s1["result"]["page"]["total"] == n, f"{q}: shuffle total")
                if n > 5:
                    check(perm != sorted(perm), f"{q}: shuffled order is not corpus order")
                st, s3 = srv.post("/query", {"query": q, "limit": n, "shuffle": True, "seed": 5})
                check(starts(s3) == perm, f"{q}: shuffle order is the same for the same seed")

                # sample + shuffle: the sample's hits in a random order
                st, ss = srv.post("/query", {"query": q, "limit": k, "sample": k, "shuffle": True, "seed": 7})
                check(sorted(starts(ss)) == sa and ss["result"]["page"]["total"] == k,
                      f"{q}: shuffled sample = the sample")

            st, r = srv.post("/session", {})
            sid = r.get("session_id") or r.get("id") or r.get("result", {}).get("session_id")
            st, r = srv.post("/query", {"query": '[upos="NOUN"]', "sample": 3, "session_id": sid})
            check(st == 400, f"sample with a session → 400 ({st})")
        finally:
            srv.close()
    if FAILS:
        print(f"{len(FAILS)} failure(s)")
        return 1
    print("server sample / shuffle: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
