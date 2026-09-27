#!/usr/bin/env python3
"""Run the dialect fixtures on Manatee and record the reference results.

  MANATEE_REGISTRY=/path/to/registry \\
  test/dialect_fixtures/run_manatee.py --corpus ud_demo \\
      --out test/dialect_fixtures/expected/manatee-ud_demo.jsonl

Uses the `manatee` Python module (as KonText does):
  conc = manatee.Concordance(corpus, query, 0, -1); conc.sync()
  total = conc.size(); hits via conc.beg_at(i) / conc.end_at(i)

The total is what KonText shows. Manatee's end positions are exclusive; the
script checks this on a one-token probe query and stores inclusive matchends,
like the CQP runner. If the Concordance API differs in your Manatee version,
--api rs uses corpus.eval_query() (a RangeStream) instead.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fixtures  # noqa: E402


def hits_conc(manatee, corp, q):
    conc = manatee.Concordance(corp, q, 0, -1)
    conc.sync()
    n = conc.size()
    return n, [conc.beg_at(i) for i in range(n)], [conc.end_at(i) for i in range(n)]


def hits_rs(corp, q):
    rs = corp.eval_query(q)
    b, e = [], []
    while not rs.end():
        b.append(rs.peek_beg())
        e.append(rs.peek_end())
        rs.next()
    return len(b), b, e


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", required=True, help="Manatee corpus name or registry file path")
    ap.add_argument("--queries", default=os.path.join(here, "queries.tsv"))
    ap.add_argument("--out", required=True)
    ap.add_argument("--api", choices=("conc", "rs"), default="conc")
    ap.add_argument("--filter", help="only query ids starting with this")
    opts = ap.parse_args()

    import manatee  # noqa: E402  (only needed on the Manatee machine)

    corp = manatee.Corpus(opts.corpus)
    run = (lambda q: hits_conc(manatee, corp, q)) if opts.api == "conc" else (lambda q: hits_rs(corp, q))

    # end exclusive? probe with a one-token query
    n, b, e = run('[upos="NOUN"]')
    width = (e[0] - b[0]) if n else 1
    end_adjust = -1 if width == 1 else 0
    meta = {"engine": "manatee", "corpus": opts.corpus, "api": opts.api,
            "manatee_version": getattr(manatee, "version", lambda: "?")(),
            "corpus_size": corp.size(), "end_exclusive": width == 1,
            "window": fixtures.WINDOW, "created": time.strftime("%Y-%m-%d %H:%M:%S")}
    print(f"# manatee {meta['manatee_version']}, corpus size {meta['corpus_size']}, "
          f"end exclusive: {meta['end_exclusive']}")

    records = []
    for qid, q, _note in fixtures.load_queries(opts.queries, "manatee"):
        if opts.filter and not qid.startswith(opts.filter):
            continue
        t0 = time.monotonic()
        try:
            n, b, e = run(q)
            e = [x + end_adjust for x in e]
            records.append(fixtures.record(qid, q, "manatee", b, e, total=n, seconds=time.monotonic() - t0))
        except Exception as ex:  # manatee raises plain exceptions for syntax errors
            records.append(fixtures.error_record(qid, q, "manatee", f"{type(ex).__name__}: {ex}",
                                                 time.monotonic() - t0))
        r = records[-1]
        print(f"{qid:5} {str(r['total']) if r['error'] is None else 'ERROR':>10} "
              f"{r.get('seconds', 0):7.2f}s  {q}" + (f"\n      {r['error']}" if r["error"] else ""))
    fixtures.write_jsonl(opts.out, meta, records)
    print(f"wrote {opts.out}")


if __name__ == "__main__":
    main()
