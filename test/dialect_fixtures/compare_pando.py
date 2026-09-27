#!/usr/bin/env python3
"""Run the dialect fixtures on pando and compare with CQP / Manatee references.

  test/dialect_fixtures/compare_pando.py --pando build/pando --corpus DIR \\
      --expected test/dialect_fixtures/expected/cqp-ud_demo.jsonl

The reference's engine picks the pando front-end (cqp → --cql cwb,
manatee → --cql manatee); --dialect overrides it (e.g. native, to see how far
native pando is from the reference). Without --expected, --engine selects the
query column and the pando results are only written (--out).

Per query: total, distinct (match, matchend) pairs, md5 of the pair set; on a
mismatch, examples of extra / missing pairs from the stored window.
Exit status 0 = everything matches, 1 = a mismatch or error.
"""

import argparse
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fixtures  # noqa: E402


def run_pando(opts, dialect, q, args):
    cmd = [opts.pando, opts.corpus, q, "--cql", dialect, "--timing", *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=opts.timeout)


def pando_record(opts, dialect, qid, q, engine):
    t0 = time.monotonic()
    p = run_pando(opts, dialect, q, ["--count-only"])
    if p.returncode != 0:
        return fixtures.error_record(qid, q, "pando", (p.stderr or p.stdout), time.monotonic() - t0)
    total = int(p.stdout.strip().splitlines()[-1])
    path = (re.search(r"path=(\S+)", p.stderr) or [None, "?"])[1]
    if total > opts.max_dump:
        rec = {"id": qid, "query": q, "engine": "pando", "total": total, "unique": None,
               "md5": None, "window": None, "error": None, "path": path}
        rec["seconds"] = round(time.monotonic() - t0, 3)
        return rec
    p = run_pando(opts, dialect, q, ["--dump-matches"])
    if p.returncode != 0:
        return fixtures.error_record(qid, q, "pando", (p.stderr or p.stdout), time.monotonic() - t0)
    starts, ends = [], []
    for line in p.stdout.splitlines()[1:]:
        s, _, e = line.partition(";")
        ss = [int(x) for x in s.split() if int(x) >= 0]
        ee = [int(x) for x in e.split() if int(x) >= 0]
        starts.append(min(ss))
        ends.append(max(ee))
    return fixtures.record(qid, q, "pando", starts, ends, total=total,
                           seconds=time.monotonic() - t0, extra={"path": path})


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--corpus", required=True, help="pando index of the same corpus")
    ap.add_argument("--expected", help="reference .jsonl from run_cqp.py / run_manatee.py")
    ap.add_argument("--engine", choices=("cqp", "manatee"), help="query column when there is no --expected")
    ap.add_argument("--dialect", help="pando --cql front-end (default: cwb for cqp, manatee for manatee)")
    ap.add_argument("--queries", default=os.path.join(here, "queries.tsv"))
    ap.add_argument("--out", help="also write pando's records here")
    ap.add_argument("--max-dump", type=int, default=5_000_000,
                    help="compare only totals above this many hits (default 5M)")
    ap.add_argument("--filter", help="only query ids starting with this")
    ap.add_argument("--timeout", type=float, default=900)
    opts = ap.parse_args()

    ref_meta, ref = ({}, {})
    if opts.expected:
        ref_meta, ref = fixtures.read_jsonl(opts.expected)
    engine = ref_meta.get("engine") or opts.engine
    if engine not in ("cqp", "manatee"):
        ap.error("need --expected or --engine")
    dialect = opts.dialect or ("cwb" if engine == "cqp" else "manatee")

    recs, bad = [], 0
    print(f"# pando --cql {dialect} vs {engine}" + (f" ({opts.expected})" if opts.expected else ""))
    print(f"{'id':5} {'ref total':>10} {'pando':>10} {'pairs':6} {'path':14} query")
    for qid, q, _note in fixtures.load_queries(opts.queries, engine):
        if opts.filter and not qid.startswith(opts.filter):
            continue
        try:
            r = pando_record(opts, dialect, qid, q, engine)
        except subprocess.TimeoutExpired:
            r = fixtures.error_record(qid, q, "pando", f"timeout after {opts.timeout}s")
        recs.append(r)
        e = ref.get(qid)
        status, detail = "", []
        if r["error"]:
            status = "ERROR"
            detail.append("pando: " + r["error"].splitlines()[0])
        if e is not None:
            if e.get("error"):
                status = status or "REF-ERR"
                detail.append(f"{engine}: " + e["error"].splitlines()[0])
            elif not r["error"]:
                same_total = e["total"] == r["total"]
                same_set = None
                if e.get("md5") and r.get("md5"):
                    same_set = e["md5"] == r["md5"]
                pairs = "same" if same_set else ("DIFF" if same_set is False else "n/a")
                if not same_total or same_set is False:
                    status = "MISMATCH"
                    if e.get("window") is not None and r.get("window") is not None:
                        a = {tuple(x) for x in e["window"]}
                        b = {tuple(x) for x in r["window"]}
                        extra, missing = sorted(b - a)[:5], sorted(a - b)[:5]
                        detail.append(f"window: pando extra {len(b - a)} {extra}; missing {len(a - b)} {missing}")
                r["_pairs"] = pairs
        ref_total = "" if e is None else (e["total"] if not e.get("error") else "ERROR")
        print(f"{qid:5} {str(ref_total):>10} {str(r['total'] if not r['error'] else 'ERROR'):>10} "
              f"{r.pop('_pairs', ''):6} {str(r.get('path', '')):14} {q}"
              + (f"  <-- {status}" if status else ""))
        for d in detail:
            print("      " + d)
        if status in ("ERROR", "MISMATCH"):
            bad += 1
    if opts.out:
        fixtures.write_jsonl(opts.out, {"engine": "pando", "dialect": dialect, "reference_engine": engine,
                                        "window": fixtures.WINDOW}, recs)
    if opts.expected:
        print(f"\n{len(recs) - bad}/{len(recs)} queries match {engine}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
