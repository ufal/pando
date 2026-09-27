#!/usr/bin/env python3
"""Run the dialect fixtures on CQP (CWB) and record the reference results.

  test/dialect_fixtures/run_cqp.py --registry ~/cwb/registry --corpus UD_DEMO \\
      --out test/dialect_fixtures/expected/cqp-ud_demo.jsonl

Each query runs in its own `cqp -c` process:

  [set MatchingStrategy …; set HardBoundary …;]
  A = <query>;
  size A;
  tabulate A match, matchend > "<tmp>";

CQP's own defaults are used unless --strategy / --hard-boundary are given; the
effective values (`set MatchingStrategy; set HardBoundary;`) and `cqp -v` are
stored in the file's meta line, so the reference says which semantics it is.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fixtures  # noqa: E402


def cqp_run(opts, script, timeout):
    cmd = [opts.cqp, "-c", "-r", os.path.expanduser(opts.registry), "-D", opts.corpus]
    return subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=timeout)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cqp", default="cqp")
    ap.add_argument("--registry", required=True, help="CWB registry directory")
    ap.add_argument("--corpus", required=True, help="CWB corpus id, e.g. UD_DEMO")
    ap.add_argument("--queries", default=os.path.join(here, "queries.tsv"))
    ap.add_argument("--out", required=True)
    ap.add_argument("--strategy", help="MatchingStrategy (standard|shortest|longest|traditional); default: CQP's")
    ap.add_argument("--hard-boundary", type=int, help="HardBoundary; default: CQP's")
    ap.add_argument("--filter", help="only query ids starting with this")
    ap.add_argument("--timeout", type=float, default=900)
    opts = ap.parse_args()

    prefix = ""
    if opts.strategy:
        prefix += f"set MatchingStrategy {opts.strategy};\n"
    if opts.hard_boundary is not None:
        prefix += f"set HardBoundary {opts.hard_boundary};\n"

    ver = subprocess.run([opts.cqp, "-v"], capture_output=True, text=True)
    settings = cqp_run(opts, prefix + "set MatchingStrategy;\nset HardBoundary;\n", 60)
    meta = {"engine": "cqp", "corpus": opts.corpus,
            "cqp_version": (ver.stdout + ver.stderr).strip()[:500],
            "settings": (settings.stdout + settings.stderr).strip()[:1000],
            "strategy_arg": opts.strategy, "hard_boundary_arg": opts.hard_boundary,
            "window": fixtures.WINDOW, "created": time.strftime("%Y-%m-%d %H:%M:%S")}
    print(f"# {meta['cqp_version'].splitlines()[0] if meta['cqp_version'] else 'cqp'}")
    print(f"# settings: {meta['settings']!r}")

    records = []
    for qid, q, _note in fixtures.load_queries(opts.queries, "cqp"):
        if opts.filter and not qid.startswith(opts.filter):
            continue
        with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as tf:
            tmp = tf.name
        script = f'{prefix}A = {q};\nsize A;\ntabulate A match, matchend > "{tmp}";\n'
        t0 = time.monotonic()
        try:
            p = cqp_run(opts, script, opts.timeout)
            dt = time.monotonic() - t0
            nums = [ln.strip() for ln in p.stdout.splitlines() if re.fullmatch(r"\s*\d+\s*", ln)]
            err = "\n".join(ln for ln in (p.stdout + p.stderr).splitlines()
                            if "error" in ln.lower() or "syntax" in ln.lower())
            if not nums or not os.path.exists(tmp):
                records.append(fixtures.error_record(qid, q, "cqp", err or p.stderr or p.stdout, dt))
            else:
                total = int(nums[-1])
                arr = np.fromfile(tmp, dtype=np.int64, sep=" ").reshape(-1, 2)
                records.append(fixtures.record(qid, q, "cqp", arr[:, 0], arr[:, 1], total=total,
                                               seconds=dt, extra={"warnings": err or None}))
        except subprocess.TimeoutExpired:
            records.append(fixtures.error_record(qid, q, "cqp", f"timeout after {opts.timeout}s",
                                                 time.monotonic() - t0))
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)
        r = records[-1]
        print(f"{qid:5} {str(r['total']) if r['error'] is None else 'ERROR':>10} "
              f"{r.get('seconds', 0):7.2f}s  {q}" + (f"\n      {r['error']}" if r["error"] else ""))
    fixtures.write_jsonl(opts.out, meta, records)
    print(f"wrote {opts.out}")


if __name__ == "__main__":
    main()
