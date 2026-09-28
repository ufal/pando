#!/usr/bin/env python3
"""pando-server limits by tier (limits.h): "tier" per request, --limits FILE, --trust-tier.

Starts pando-server on an index built from a CoNLL-U file with three tiers —
visitor (default: deny transitive + regex without a literal start, few hits,
short background counts), user (more hits, a 0.8 s cap), admin (no limits) — and checks:

  * /health reports the tiers;
  * denied features answer 403 {"denied": …, "tier": …} for /query and /run, per tier;
    a regex with a literal start is allowed;
  * max_count_hits: `count by` / `sort` / `coll` over more hits answer 413
    {"limit": "max_count_hits"} (inline and on a session set); under the limit
    the rows equal the admin's; `size` and pages are never limited;
  * max_hits: a session set that would materialise more hits → 413 "max_hits";
  * total_timeout_ms: a background count stops at the tier's limit (timed_out),
    a request of the same tier does not restart it, an admin request does and it finishes;
  * without --trust-tier the request's "tier" is ignored (default tier);
  * with --big (a UD-sized --corpus): a regex scan over the word lexicon stops at
    the tier's timeout_ms, which a request's own "timeout_ms" cannot raise.

  test/server_limits_test.py --server build/pando-server --pando-index build/pando-index \\
      --conllu test/data/sample.conllu
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import Server, check, FAILS, rows_key  # noqa: E402

TIERS = {
    "tiers": {
        "visitor": {"timeout_ms": 1500, "total_timeout_ms": 1000, "max_count_hits": 40, "max_hits": 30,
                    "deny": ["transitive", "regex_no_prefix"]},
        "user": {"timeout_ms": 800, "max_count_hits": 100000, "max_hits": 100000, "deny": ["parallel"]},
        "admin": {},
    },
    "default_tier": "visitor",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index")
    ap.add_argument("--conllu")
    ap.add_argument("--corpus")
    ap.add_argument("--big", action="store_true")
    opts = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        corpus = opts.corpus
        if not corpus:
            corpus = os.path.join(tmp, "idx")
            subprocess.run([opts.pando_index, opts.conllu, corpus], check=True, capture_output=True)
        lim = os.path.join(tmp, "limits.json")
        with open(lim, "w") as f:
            json.dump(TIERS, f)

        srv = Server(opts.server, corpus, "--limits", lim, "--trust-tier", "--debug-total-delay", "3000")
        try:
            st, h = srv.get("/health")
            check(st == 200 and set(h.get("tiers", {})) == {"visitor", "user", "admin"}
                  and h.get("trust_tier") is True and "tiers" in h.get("features", []), f"/health tiers: {h.get('tiers')}")

            def q(tier, query, **kw):
                body = {"query": query, "limit": 3}
                if tier:
                    body["tier"] = tier
                body.update(kw)
                return srv.post("/query", body)

            def run(tier, cql, **kw):
                body = {"cql": cql, "group_limit": 1000}
                if tier:
                    body["tier"] = tier
                body.update(kw)
                return srv.post("/run", body)

            trans = '[upos="VERB"] >> [upos="NOUN"]'
            for tier, want in ((None, 403), ("visitor", 403), ("user", 200), ("admin", 200), ("nosuch", 403)):
                st, r = q(tier, trans)
                check(st == want and (st != 403 or (r.get("denied") == "transitive" and r.get("tier") == "visitor")),
                      f"transitive as {tier}: {st} {r.get('denied')} {r.get('tier')}")
            st, r = run("visitor", f"{trans}; count by lemma")
            check(st == 403 and r.get("denied") == "transitive", f"/run transitive as visitor: {st}")
            for tier, want in (("visitor", 403), ("user", 200), ("admin", 200)):
                st, r = q(tier, '[lemma=".*s"]')
                check(st == want, f"regex without prefix as {tier}: {st}")
            st, r = q("visitor", '[lemma="th.*"]')
            check(st == 200, f"regex with a literal start as visitor: {st}")

            if not opts.big:   # hit counts sized for the sample corpus
                # max_count_hits (visitor 40)
                big, small = '[upos="NOUN"]', '[upos="ADJ"] [upos="NOUN"]'
                _, rb = q("admin", big, total=True)
                _, rs = q("admin", small, total=True)
                nb, ns = rb["result"]["page"]["total"], rs["result"]["page"]["total"]
                check(nb > 40 and ns <= 40, f"test corpus: {nb} / {ns} hits (need > 40 / <= 40)")
                st, r = run("visitor", f"{big}; count by lemma")
                check(st == 413 and r.get("limit") == "max_count_hits" and r.get("tier") == "visitor"
                      and r.get("max_count_hits") == 40, f"visitor count over the limit: {st} {r}")
                _, want = run("admin", f"{small}; count by lemma")
                st, got = run("visitor", f"{small}; count by lemma")
                check(st == 200 and rows_key(got) == rows_key(want), f"visitor count under the limit: {st}")
                _, want = run("admin", f"{big}; count by lemma")
                st, got = run("user", f"{big}; count by lemma")
                check(st == 200 and rows_key(got) == rows_key(want), f"user count: {st}")
                for cql in (f"{big}; sort by lemma", f"{big}; coll", f"Q = {big}; count Q by upos"):
                    st, r = run("visitor", cql)
                    check(st == 413 and r.get("limit") == "max_count_hits", f"visitor {cql}: {st}")
                st, r = run("visitor", f"Q = {big}; size Q")
                check(st == 200 and r.get("result") == nb, f"size is not limited: {st} {r.get('result')}")
                st, r = q("visitor", big, total=True, offset=45)
                check(st == 200 and r["result"]["page"]["total"] == nb, f"pages and totals are not limited: {st}")

                # sessions: max_count_hits for commands, max_hits for materialising
                sid = srv.session()
                srv.post("/query", {"session_id": sid, "name": "B", "query": big, "limit": 1, "total": True, "tier": "admin"})
                st, r = run("visitor", "sort B by lemma", session_id=sid)
                check(st == 413 and r.get("limit") == "max_count_hits", f"visitor sort of a session set: {st}")
                st, r = run("admin", "sort B by lemma", session_id=sid, limit=2)
                check(st == 200, f"admin sort of a session set: {st}")
                st, r = srv.post("/query", {"session_id": sid, "from": "B", "offset": 20, "limit": 2, "tier": "visitor"})
                check(st == 200, f"visitor page of a sorted (kept) set: {st}")
                srv.post("/session/close", {"session_id": sid})
                sid = srv.session()
                srv.post("/query", {"session_id": sid, "name": "B", "query": big, "limit": 1, "total": True})
                st, r = srv.post("/query", {"session_id": sid, "from": "B", "offset": 12000, "limit": 2, "tier": "visitor"})
                check(st == 200, f"visitor deep page of a lazy set pages by query: {st}")
                st, r = run("user", "sort B by lemma", session_id=sid)
                check(st == 200, f"user sort under max_hits: {st}")

                # total_timeout_ms (visitor 1000 ms; every count takes 3 s here)
                tq = '[upos="ADJ"]'
                st, r = q("visitor", tq, total="async")
                job = r["result"]["job_id"]
                t_end = time.monotonic() + 10
                state = None
                while time.monotonic() < t_end:
                    _, s = srv.get(f"/status?job={job}")
                    state = s["job"]
                    if state["state"] not in ("queued", "running"):
                        break
                    time.sleep(0.2)
                check(state and state["state"] == "cancelled" and state.get("timed_out") is True,
                      f"visitor background count stops at its limit: {state}")
                st, r = q("visitor", tq, total="async")
                check(r["result"]["job"]["state"] == "cancelled", f"same tier does not restart it: {r['result']['job']}")
                st, r = q("admin", tq, total="async")
                check(r["result"]["job"]["state"] in ("queued", "running"), f"admin restarts it: {r['result']['job']}")
                t_end = time.monotonic() + 15
                while time.monotonic() < t_end:
                    _, s = srv.get(f"/status?job={job}")
                    if s["job"]["state"] == "finished":
                        break
                    time.sleep(0.2)
                check(s["job"]["state"] == "finished", f"admin count finishes: {s['job']}")

            if opts.big:
                slow = '[word=".*a.*"] [word=".*e.*"] [word=".*i.*"] [word=".*o.*"]'
                t0 = time.monotonic()
                st, r = q("admin", slow, timeout_ms=300)
                dt = time.monotonic() - t0
                check(st == 408 and dt < 2.0, f"regex scan stops at the timeout: {st} after {dt:.2f} s")
                t0 = time.monotonic()
                st, r = q("user", slow, timeout_ms=999999)
                dt = time.monotonic() - t0
                check(st == 408 and 0.7 < dt < 2.5, f"user timeout (0.8 s) cannot be raised: {st} after {dt:.2f} s")
        finally:
            srv.close()

        # without --trust-tier the body's tier is ignored
        srv = Server(opts.server, corpus, "--limits", lim)
        try:
            st, r = srv.post("/query", {"query": '[upos="VERB"] >> [upos="NOUN"]', "tier": "admin"})
            check(st == 403 and r.get("tier") == "visitor", f"untrusted tier ignored: {st} {r.get('tier')}")
        finally:
            srv.close()

    print(f"{'OK' if not FAILS else 'FAILED'}: {len(FAILS)} failures")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
