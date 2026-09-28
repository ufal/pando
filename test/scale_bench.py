#!/usr/bin/env python3
"""P4.1 scaling benchmark: the same queries with --threads 1, 2, 4, … on one or
more corpora, checked for identical results.

For every corpus × query × thread count: one warm-up run, then --reps runs;
reports the median `query_sec` (from --timing: execution only, no process
start, corpus open or output) and the speedup over --threads 1. Checks that
the total (and, for `… ; count by …` queries, the whole printed output) is the
same at every thread count — any difference is a FAIL (exit status 1).

  test/scale_bench.py --pando build/pando --corpus /data/ud_demo \\
      --corpus /data/ud_demo_x16 --queries test/perf_queries.tsv \\
      --agg-queries test/perf_agg_queries.tsv --threads 1,2,4,8 --out p41.json

Query files: `id<TAB>query` per line ('#' comments, blank lines ignored).
Plain queries run as `--limit 20 --total` (a KonText first page + exact total);
aggregation queries without a page (`count by` rows are the output).
"""

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time


def read_queries(path, flt):
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            qid, _, q = line.partition("\t")
            if flt and flt not in qid and flt not in q:
                continue
            out.append((qid.strip(), q.strip()))
    return out


def run(pando, corpus, query, threads, agg, timeout, env):
    args = [pando, corpus, query, "--timing", "--threads", str(threads)]
    if not agg:
        args += ["--limit", "20", "--total"]
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env=env)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip()[:300])
    g = lambda pat: re.search(pat, p.stderr)
    sec = float(g(r"query_sec=([\d.]+)").group(1))
    total = int(g(r"\btotal=(\d+)").group(1)) if g(r"\btotal=(\d+)") else None
    path = g(r"path=(\S+)").group(1) if g(r"path=(\S+)") else "-"
    parts = int(g(r"\bparts=(\d+)").group(1)) if g(r"\bparts=(\d+)") else 1
    return sec, total, path, parts, p.stdout


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", default="build/pando")
    ap.add_argument("--corpus", action="append", required=True)
    ap.add_argument("--queries", default="test/perf_queries.tsv")
    ap.add_argument("--agg-queries", default="test/perf_agg_queries.tsv")
    ap.add_argument("--threads", default="1,2,4,8")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=1800)
    ap.add_argument("--filter", help="only query ids / queries containing this substring")
    ap.add_argument("--min-ms", type=float, default=20.0,
                    help="queries faster than this with 1 thread are left out of the mean speedup")
    ap.add_argument("--out", help="write all results as JSON")
    opts = ap.parse_args()

    threads = [int(t) for t in opts.threads.split(",")]
    if threads[0] != 1:
        threads = [1] + threads
    jobs = [(qid, q, False) for qid, q in read_queries(opts.queries, opts.filter)]
    if opts.agg_queries and os.path.exists(opts.agg_queries):
        jobs += [(qid, q, True) for qid, q in read_queries(opts.agg_queries, opts.filter)]
    env = dict(os.environ)
    for k in ("PANDO_FASTPATH", "PANDO_BITMAPS", "PANDO_MASK_BITS", "PANDO_PARTITION_MIN"):
        env.pop(k, None)

    results, fails = [], 0
    for corpus in opts.corpus:
        print(f"\n== {corpus}")
        print(f"{'id':6} {'path':15} {'parts':>5} {'total':>11}  "
              + "  ".join(f"{'t' + str(t):>9}" for t in threads) + "   speedup  query", flush=True)
        speedups = {t: [] for t in threads}
        for qid, q, agg in jobs:
            row = {"corpus": corpus, "id": qid, "query": q, "agg": agg, "ms": {}}
            outs, totals, cells, parts, path = {}, {}, [], 1, "-"
            try:
                for t in threads:
                    run(opts.pando, corpus, q, t, agg, opts.timeout, env)   # warm-up
                    samples = []
                    for _ in range(opts.reps):
                        sec, total, path_t, parts_t, out = run(opts.pando, corpus, q, t, agg, opts.timeout, env)
                        samples.append(sec)
                    med = statistics.median(samples) * 1000
                    row["ms"][t] = med
                    totals[t], outs[t] = total, out
                    if t == 1:
                        path = path_t
                    parts = max(parts, parts_t)
                    cells.append(f"{med:8.1f}ms")
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                print(f"{qid:6} ERROR {e}")
                fails += 1
                continue
            note = ""
            if len(set(totals.values())) > 1:
                note = "  FAIL(totals differ: " + ", ".join(f"t{t}={v}" for t, v in totals.items()) + ")"
                fails += 1
            elif agg and len(set(outs.values())) > 1:
                note = "  FAIL(count by output differs)"
                fails += 1
            base = row["ms"][1]
            best_t = min(row["ms"], key=row["ms"].get)
            sp = base / row["ms"][best_t] if row["ms"][best_t] > 0 else float("nan")
            if base >= opts.min_ms:
                for t in threads:
                    if row["ms"][t] > 0:
                        speedups[t].append(base / row["ms"][t])
            row.update(path=path, parts=parts, total=totals.get(1), best_threads=best_t, speedup=sp)
            results.append(row)
            print(f"{qid:6} {path[:15]:15} {parts:>5} {str(totals.get(1)):>11}  " + "  ".join(cells)
                  + f"   {sp:5.2f}x@{best_t:<3} {q}{note}", flush=True)
        line = "   ".join(
            f"t{t}: {math.exp(statistics.mean(math.log(x) for x in v)):.2f}x ({len(v)} q)"
            for t, v in speedups.items() if v and t != 1)
        print(f"geometric-mean speedup over queries >= {opts.min_ms:g} ms at t1: {line or '-'}")

    if opts.out:
        meta = {"pando": opts.pando, "threads": threads, "reps": opts.reps,
                "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "host": os.uname().nodename,
                "cpus": os.cpu_count()}
        with open(opts.out, "w", encoding="utf-8") as f:
            json.dump({"meta": meta, "results": results}, f, indent=1)
    print(f"\n{len(results)} rows, {fails} failures")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
