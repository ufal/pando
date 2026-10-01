#!/usr/bin/env python3
"""Differential test: fast paths vs generic executor.

Runs every query under PANDO_FASTPATH=on / nomerge / off (and "onbits": on with
region filters forced to bitsets, PANDO_MASK_BITS=1; "nobm" / "bmforce": on with
the bitmap kernels off / forced, PANDO_BITMAPS) and requires

  * identical exact totals (``--count-only``) in every mode,
  * identical totals from the concordance path (``--limit K --total``),
  * the same capped total with ``--max-total`` (half the exact total),
  * identical full match sets (``--dump-matches``, compared as sorted sets)
    when the total is at most ``--max-full``,
  * the first page (``--dump-page --limit K``, the normal concordance path
    without total) to be K distinct members of the full set.

A query with an aggregation (``…; count by …`` / ``group by``) is instead run
once per mode and the printed buckets and totals must be identical.

Exit status 0 = all equal, 1 = a mismatch, 2 = usage / run error.

Usage:
  test/fastpath_diff.py --pando build/pando --corpus DIR --queries FILE
  test/fastpath_diff.py --pando build/pando --conllu test/data/sample.conllu \
      --pando-index build/pando-index --queries test/fastpath_queries.txt

Query file: one query per line; blank lines and lines starting with '#' are
ignored. A line may start with ``fullmax=N<TAB>`` to override --max-full.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

ALL_MODES = ("on", "onbits", "nobm", "bmforce", "nomerge", "off", "mt", "mtbits", "mtoff", "prog", "packed")
MODES = ALL_MODES
REF = "off"
# onbits  = fast paths with region filters forced to the bitset representation
#           (PANDO_MASK_BITS=1) instead of position intervals;
# nobm    = fast paths without the bitmap kernels (PANDO_BITMAPS=off): the merge paths;
# bmforce = bitmap kernels wherever the query compiles to them (PANDO_BITMAPS=force),
#           also for rare operands the planner would give to a merge.
# The bitmap modes only differ from "on" on an index with <attr>.bm files.
# mt / mtbits / mtoff = on / onbits / off with P4.1 range partitioning
#           (--threads MT_THREADS, ranges down to PANDO_PARTITION_MIN tokens so a
#           small corpus is split too); the concordance page with a total must be
#           identical to the single-threaded one, in order.
MT_THREADS = 4
MT_MODES = ("mt", "mtbits", "mtoff")
PARTITION_MIN = "50"
# packed  = on with the postings decoded from <attr>.rev.pfb (P4.2, PANDO_REV=packed;
#           the sample index gets them with `pando-index --upgrade --packed-rev`).
# prog    = on with P6.5e progressive pages for every complex operand
#           (PANDO_PROGRESSIVE_MIN=1) in ranges from PROGRESSIVE_WINDOW tokens: a
#           page without a total must be the "on" page, in order.
PROGRESSIVE_WINDOW = "64"


def run(pando, corpus, query, args, mode, timeout):
    env = dict(os.environ, PANDO_FASTPATH="off" if mode in ("off", "mtoff") else
               "nomerge" if mode == "nomerge" else "on")
    for k in ("PANDO_MASK_BITS", "PANDO_BITMAPS", "PANDO_PARTITION_MIN",
              "PANDO_PROGRESSIVE_MIN", "PANDO_PROGRESSIVE_WINDOW", "PANDO_REV"):
        env.pop(k, None)
    if mode == "packed":
        env["PANDO_REV"] = "packed"
    if mode == "prog":
        env["PANDO_PROGRESSIVE_MIN"] = "1"
        env["PANDO_PROGRESSIVE_WINDOW"] = PROGRESSIVE_WINDOW
    if mode in MT_MODES:
        env["PANDO_PARTITION_MIN"] = PARTITION_MIN
        args = [*args, "--threads", str(MT_THREADS)]
    if mode in ("onbits", "mtbits"):
        env["PANDO_MASK_BITS"] = "1"
    elif mode == "nobm":
        env["PANDO_BITMAPS"] = "off"
    elif mode == "bmforce":
        env["PANDO_BITMAPS"] = "force"
    t0 = time.monotonic()
    p = subprocess.run([pando, corpus, query, "--timing", *args],
                       capture_output=True, text=True, env=env, timeout=timeout)
    dt = time.monotonic() - t0
    if p.returncode != 0:
        raise RuntimeError(f"[{mode}] exit {p.returncode}: {p.stderr.strip()[:300]}")
    m = re.search(r"path=(\S+)", p.stderr)
    return p.stdout, (m.group(1) if m else "?"), dt, p.stderr


def total_from_timing(stderr):
    m = re.search(r"\btotal=(\d+)", stderr)
    return int(m.group(1)) if m else None


def parse_dump(stdout):
    lines = stdout.splitlines()
    if not lines or not lines[0].startswith("total="):
        raise RuntimeError("unexpected --dump-matches output: " + (lines[0] if lines else "<empty>"))
    head = dict(kv.split("=") for kv in lines[0].split())
    return int(head["total"]), head.get("exact") == "1", lines[1:]


AGG_RE = re.compile(r";\s*(count|group)\s+by\b", re.I)


def check_agg_query(opts, query):
    """Aggregation: the whole output (buckets + total) must not depend on the path."""
    problems, info, outs = [], {}, {}
    for mode in MODES:
        out, path, dt, err = run(opts.pando, opts.corpus, query, [], mode, opts.timeout)
        outs[mode] = out
        info[mode] = (path, dt)
        if mode == "mt":
            m = re.search(r"\bparts=(\d+)", err)
            info["parts"] = int(m.group(1)) if m else 1
    m = re.search(r"Total:\s*(\d+)", outs[REF])
    if not m:
        problems.append(f"no 'Total:' line in the reference output: {outs[REF].strip()[-200:]!r}")
    ref = int(m.group(1)) if m else -1
    for mode in MODES:
        if outs[mode] != outs[REF]:
            a, b = outs[mode].splitlines(), outs[REF].splitlines()
            diff = next((i for i in range(max(len(a), len(b)))
                         if i >= len(a) or i >= len(b) or a[i] != b[i]), None)
            problems.append(f"[{mode}] aggregation output differs from generic at line {diff}: "
                            f"{a[diff] if diff is not None and diff < len(a) else '<eof>'!r} vs "
                            f"{b[diff] if diff is not None and diff < len(b) else '<eof>'!r}")
    info["full"] = "agg"
    return {mode: ref for mode in MODES}, info, problems


def check_query(opts, query, max_full):
    if AGG_RE.search(query):
        return check_agg_query(opts, query)
    problems = []
    info = {}
    totals = {}
    for mode in MODES:
        out, path, dt, err = run(opts.pando, opts.corpus, query, ["--count-only"], mode, opts.timeout)
        totals[mode] = int(out.strip().splitlines()[-1])
        info[mode] = (path, dt)
        if mode == "mt":
            m = re.search(r"\bparts=(\d+)", err)
            info["parts"] = int(m.group(1)) if m else 1
    if len(set(totals.values())) != 1:
        problems.append(f"count-only totals differ: {totals}")
    ref_total = totals[REF]

    k = opts.page
    pages = {}
    for mode in MODES:
        out, _, _, err = run(opts.pando, opts.corpus, query, ["--limit", str(k), "--total"], mode, opts.timeout)
        pages[mode] = out
        t = total_from_timing(err)
        if t != ref_total:
            problems.append(f"[{mode}] --limit {k} --total gives {t}, expected {ref_total}")
    # P4.1: the partitioned page is the single-threaded page, in the same order
    for mode, ref in (("mt", "on"), ("mtbits", "onbits"), ("mtoff", "off")):
        if mode in pages and ref in pages and pages[mode] != pages[ref]:
            problems.append(f"[{mode}] --limit {k} --total page differs from [{ref}]")

    # capped total (--max-total): every mode must stop at the same cap
    if ref_total >= 2:
        cap = ref_total // 2
        for mode in MODES:
            _, _, _, err = run(opts.pando, opts.corpus, query,
                               ["--limit", str(k), "--total", "--max-total", str(cap)], mode, opts.timeout)
            t = total_from_timing(err)
            if t != cap:
                problems.append(f"[{mode}] --max-total {cap} gives total {t}, expected {cap}")

    # P6.5e: the progressive page is the plain page, in the same order
    if "prog" in MODES and "on" in MODES:
        for lim in (k, 3 * k):
            po, _, _, _ = run(opts.pando, opts.corpus, query, ["--dump-page", "--limit", str(lim)], "on", opts.timeout)
            pp, _, _, _ = run(opts.pando, opts.corpus, query, ["--dump-page", "--limit", str(lim)], "prog", opts.timeout)
            if parse_dump(po)[2] != parse_dump(pp)[2]:
                problems.append(f"[prog] --dump-page --limit {lim} differs from [on]")

    if ref_total <= max_full:
        sets = {}
        for mode in MODES:
            out, _, _, _ = run(opts.pando, opts.corpus, query, ["--dump-matches"], mode, opts.timeout)
            total, exact, rows = parse_dump(out)
            if total != len(rows):
                problems.append(f"[{mode}] dump total={total} but {len(rows)} rows")
            if len(rows) != ref_total:
                # --dump-matches materialises every hit; --count-only / --total count
                # past the page — both must agree (anchors, post-filters, …)
                problems.append(f"[{mode}] dump has {len(rows)} hits but count-only total is {ref_total}")
            if not exact:
                problems.append(f"[{mode}] dump total not exact")
            if len(rows) != len(set(rows)):
                problems.append(f"[{mode}] duplicate matches in dump ({len(rows) - len(set(rows))})")
            sets[mode] = set(rows)
        for mode in MODES:
            if sets[mode] != sets[REF]:
                a = sorted(sets[mode] - sets[REF])[:3]
                b = sorted(sets[REF] - sets[mode])[:3]
                problems.append(f"[{mode}] match set differs from generic: extra={a} missing={b}")
        # first page (normal concordance path, no total): K distinct members of the full set
        for mode in MODES:
            out, _, _, _ = run(opts.pando, opts.corpus, query,
                               ["--dump-page", "--limit", str(k)], mode, opts.timeout)
            _, _, rows = parse_dump(out)
            want = min(k, ref_total)
            if len(rows) != want or len(set(rows)) != len(rows):
                problems.append(f"[{mode}] first page has {len(rows)} rows ({len(set(rows))} distinct), expected {want}")
            bad = [r for r in rows if r not in sets[REF]]
            if bad:
                problems.append(f"[{mode}] first page contains non-matches: {bad[:3]}")
        info["full"] = True
    else:
        info["full"] = False
    return totals, info, problems


def build_sample_index(pando_index, conllu):
    d = tempfile.mkdtemp(prefix="pando_fpdiff_")
    p = subprocess.run([pando_index, conllu, d], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout, p.stderr, file=sys.stderr)
        raise SystemExit(2)
    check_head_rel_upgrade(pando_index, d)
    # P4.2: packed postings next to the plain ones (mode "packed")
    p = subprocess.run([pando_index, "--upgrade", d, "--packed-rev"], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout, p.stderr, file=sys.stderr)
        raise SystemExit(2)
    return d


def check_head_rel_upgrade(pando_index, d):
    """dep.head_rel written at index time must equal the one `--upgrade` derives."""
    built = os.path.join(d, "dep.head_rel")
    if not os.path.exists(os.path.join(d, "dep.head")):
        return
    if not os.path.exists(built):
        print("FAIL: index has dep.head but no dep.head_rel", file=sys.stderr)
        raise SystemExit(1)
    with open(built, "rb") as f:
        at_index_time = f.read()
    os.rename(built, built + ".orig")
    p = subprocess.run([pando_index, "--upgrade", d], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout, p.stderr, file=sys.stderr)
        raise SystemExit(2)
    with open(built, "rb") as f:
        upgraded = f.read()
    os.remove(built + ".orig")
    if upgraded != at_index_time:
        print("FAIL: dep.head_rel from pando-index differs from --upgrade", file=sys.stderr)
        raise SystemExit(1)
    print("dep.head_rel: index-time file == --upgrade result")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--corpus", help="indexed corpus directory")
    ap.add_argument("--conllu", help="build a temporary index from this CoNLL-U file instead")
    ap.add_argument("--pando-index", help="pando-index binary (with --conllu)")
    ap.add_argument("--queries", required=True)
    ap.add_argument("--max-full", type=int, default=1_500_000,
                    help="compare full match sets when total <= this (default 1.5M)")
    ap.add_argument("--page", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=600)
    ap.add_argument("--filter", help="only queries containing this substring")
    ap.add_argument("--modes", help="comma-separated subset of the modes (default: all); "
                    "the reference is 'off' if listed, else the first")
    opts = ap.parse_args()
    global MODES, REF
    if opts.modes:
        MODES = tuple(m for m in opts.modes.split(","))
        bad = [m for m in MODES if m not in ALL_MODES]
        if bad:
            ap.error(f"unknown modes {bad}")
    REF = "off" if "off" in MODES else MODES[0]

    tmp = None
    if opts.conllu:
        if not opts.pando_index:
            ap.error("--conllu needs --pando-index")
        tmp = opts.corpus = build_sample_index(opts.pando_index, opts.conllu)
    if not opts.corpus:
        ap.error("need --corpus or --conllu")

    queries = []
    with open(opts.queries, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            mf = opts.max_full
            if line.startswith("fullmax="):
                pre, line = line.split("\t", 1)
                mf = int(pre.split("=", 1)[1])
            if opts.filter and opts.filter not in line:
                continue
            queries.append((line, mf))

    failures = 0
    split = 0
    print(f"{'total':>10}  {'full':4}  {'on-path':15} {'parts':>5} {'on s':>7} {'nobm s':>9} {'off s':>7}  query")
    try:
        for q, mf in queries:
            try:
                totals, info, problems = check_query(opts, q, mf)
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                failures += 1
                print(f"{'ERROR':>10}  {'':4}  {'':15} {'':>7} {'':>9} {'':>7}  {q}\n    {e}")
                continue
            status = "" if not problems else "  <-- MISMATCH"
            parts = info.get("parts", 1)
            split += parts > 1
            t = lambda m: info[m][1] if m in info else float("nan")
            first = info.get("on", info[MODES[0]])
            print(f"{totals[REF]:>10}  {info['full'] if isinstance(info['full'], str) else ('yes' if info['full'] else 'no'):4}  {first[0]:15} "
                  f"{parts:>5} {t('on'):7.3f} {t('nobm'):9.3f} {t('off'):7.3f}  {q}{status}")
            for p in problems:
                print("    " + p)
            if problems:
                failures += 1
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n{len(queries) - failures}/{len(queries)} queries consistent across modes"
          f" ({split} split into position ranges in mode mt)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
