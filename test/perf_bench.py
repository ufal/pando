#!/usr/bin/env python3
"""Performance benchmark / regression check for pando query paths.

Runs each query of a TSV file against one or more pando binaries on the same
corpus and reports the median `query_sec` (from --timing, so process start and
corpus open are excluded), the exact total, and the execution path
(`path=` from --timing, e.g. seq_merge2, dep_bitset, generic).

  * Totals must agree between binaries (and with --baseline, if given);
    any disagreement is reported as FAIL and makes the exit status 1.
  * With --baseline, a query is flagged SLOWER when its median is more than
    --tol (fraction) and more than --min-ms slower than the baseline.
  * --out writes all results as JSON (use it later as --baseline).
    Timings only compare on the same machine in the same state; across
    machines (or after a VM restart) prefer two binaries side by side.

Examples:
  test/perf_bench.py --corpus /data/ud_demo --queries test/perf_queries.tsv \\
      --pando before=/tmp/pando_old --pando after=build/pando
  test/perf_bench.py --corpus /data/ud_demo --queries test/perf_queries.tsv \\
      --pando build/pando --out perf-$(git rev-parse --short HEAD).json \\
      --baseline perf-previous.json

Query file: `id<TAB>query` per line; '#' comments and blank lines ignored.
Modes (--modes, comma-separated): total = `--limit 20 --total` (KonText-style
first page + exact total), count = `--count-only`, page = `--limit 20`.
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

MODE_ARGS = {
    "total": ["--limit", "20", "--total"],
    "count": ["--count-only"],
    "page": ["--limit", "20"],
}


def run_once(binary, corpus, query, mode, timeout, env):
    p = subprocess.run([binary, corpus, query, "--timing", *MODE_ARGS[mode]],
                       capture_output=True, text=True, timeout=timeout, env=env)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip()[:300])
    q = re.search(r"query_sec=([\d.]+)", p.stderr)
    t = re.search(r"\btotal=(\d+)", p.stderr)
    path = re.search(r"path=(\S+)", p.stderr)
    return (float(q.group(1)) if q else float("nan"),
            int(t.group(1)) if t else None,
            path.group(1) if path else "-")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", action="append", required=True,
                    help="binary, or label=binary; repeat to compare binaries")
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--modes", default="total")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--filter", help="only query ids / queries containing this substring")
    ap.add_argument("--out", help="write results JSON here")
    ap.add_argument("--baseline", help="results JSON from an earlier run to compare against")
    ap.add_argument("--tol", type=float, default=0.20, help="relative slowdown tolerance (default 0.20)")
    ap.add_argument("--min-ms", type=float, default=5.0, help="ignore slowdowns below this many ms")
    ap.add_argument("--threads", type=int, default=0, help="pass --threads N (0 = default)")
    opts = ap.parse_args()

    bins = []
    for spec in opts.pando:
        label, _, path = spec.partition("=") if "=" in spec else (os.path.basename(spec), "", spec)
        bins.append((label, path))
    modes = opts.modes.split(",")
    for m in modes:
        if m not in MODE_ARGS:
            ap.error(f"unknown mode {m}")
    if opts.threads:
        for m in MODE_ARGS:
            MODE_ARGS[m] = MODE_ARGS[m] + ["--threads", str(opts.threads)]

    queries = []
    with open(opts.queries, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            qid, _, q = line.partition("\t")
            if opts.filter and opts.filter not in qid and opts.filter not in q:
                continue
            queries.append((qid.strip(), q.strip()))

    baseline = {}
    if opts.baseline:
        with open(opts.baseline, encoding="utf-8") as f:
            for r in json.load(f)["results"]:
                baseline[(r["id"], r["mode"])] = r

    env = dict(os.environ)
    env.pop("PANDO_FASTPATH", None)
    results, fails, slower = [], 0, 0
    hdr = f"{'id':8} {'mode':5} {'total':>10}  " + "  ".join(f"{l[:14]:>14} {'path':14}" for l, _ in bins)
    if len(bins) > 1:
        hdr += f"  {'ratio':>6}"
    if baseline:
        hdr += f"  {'baseline':>9}"
    print(hdr + "  query")
    for qid, q in queries:
        for mode in modes:
            row = {"id": qid, "query": q, "mode": mode, "bins": {}}
            totals = set()
            cells = []
            for label, path in bins:
                try:
                    run_once(path, opts.corpus, q, mode, opts.timeout, env)  # warm-up
                    samples = []
                    for _ in range(opts.reps):
                        sec, total, ppath = run_once(path, opts.corpus, q, mode, opts.timeout, env)
                        samples.append(sec)
                    med = statistics.median(samples)
                    row["bins"][label] = {"median_ms": med * 1000, "min_ms": min(samples) * 1000,
                                          "total": total, "path": ppath}
                    if mode != "page":
                        totals.add(total)
                    cells.append(f"{med * 1000:12.1f}ms {ppath[:14]:14}")
                except (RuntimeError, subprocess.TimeoutExpired) as e:
                    row["bins"][label] = {"error": str(e)}
                    cells.append(f"{'ERROR':>14} {'':14}")
                    fails += 1
            note = ""
            if len(totals) > 1:
                note += "  FAIL(totals differ)"
                fails += 1
            line = f"{qid:8} {mode:5} {next(iter(totals)) if totals else '-':>10}  " + "  ".join(cells)
            meds = [b.get("median_ms") for b in row["bins"].values()]
            if len(bins) > 1 and all(m is not None for m in meds) and meds[-1] > 0:
                line += f"  {meds[0] / meds[-1]:5.2f}x"
            b = baseline.get((qid, mode))
            if b:
                bb = next(iter(b["bins"].values()))
                cur = row["bins"][bins[-1][0]]
                if "median_ms" in bb and "median_ms" in cur:
                    line += f"  {bb['median_ms']:7.1f}ms"
                    if mode != "page" and bb.get("total") != cur.get("total"):
                        note += "  FAIL(total != baseline)"
                        fails += 1
                    if (cur["median_ms"] > bb["median_ms"] * (1 + opts.tol)
                            and cur["median_ms"] - bb["median_ms"] > opts.min_ms):
                        note += "  SLOWER"
                        slower += 1
            print(line + "  " + q + note, flush=True)
            results.append(row)

    if opts.out:
        meta = {"corpus": opts.corpus, "reps": opts.reps, "bins": dict(bins),
                "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "host": os.uname().nodename}
        with open(opts.out, "w", encoding="utf-8") as f:
            json.dump({"meta": meta, "results": results}, f, indent=1)
    print(f"\n{len(results)} measurements, {fails} failures, {slower} slower than baseline")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
