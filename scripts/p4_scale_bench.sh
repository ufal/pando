#!/usr/bin/env bash
# P4.1 scaling benchmark on this machine: --threads 1, 2, 4, … on the demo
# corpus and on an N-times replicated copy of it, results checked for equality.
#
# Run from the pando repo root:
#   scripts/p4_scale_bench.sh
#   scripts/p4_scale_bench.sh --copies 60          # ≈2.3B tokens: 64-bit postings (≈85 GB disk)
#   scripts/p4_scale_bench.sh --threads 1,2,4,8,12 --reps 5
#
# Options (defaults are the kontext-pando layout on the Mac Studio):
#   --src DIR      CoNLL-U sources of the demo corpus  (~/programming/kontext-pando/data/raw/ud_demo)
#   --base DIR     the demo corpus index, read only    (~/programming/kontext-pando/data/pando/ud_demo)
#   --work DIR     where the replicated index goes     (/Volumes/Data2/Corpora/kontext-pando/pando)
#   --copies N     replication factor (16 ≈ 600M tokens, the ParlaMint band)
#   --threads L    thread counts (default 1,2,4,8 and the performance-core count)
#   --reps N       timed runs per point (median; default 3)
#   --skip-build   use the existing build-p41/
#
# Writes <work>/p41-<host>-<date>.{log,json}. Nothing is pushed or committed.
set -euo pipefail

SRC="$HOME/programming/kontext-pando/data/raw/ud_demo"
BASE="$HOME/programming/kontext-pando/data/pando/ud_demo"
WORK="/Volumes/Data2/Corpora/kontext-pando/pando"
COPIES=16
THREADS=""
REPS=3
SKIP_BUILD=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) SRC="$2"; shift 2 ;;
    --base) BASE="$2"; shift 2 ;;
    --work) WORK="$2"; shift 2 ;;
    --copies) COPIES="$2"; shift 2 ;;
    --threads) THREADS="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

ncpu() {
  sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || nproc
}
if [[ -z "$THREADS" ]]; then
  P=$(ncpu)
  THREADS="1,2,4,8"
  [[ "$P" -gt 8 ]] && THREADS="$THREADS,$P"
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  cmake -S . -B build-p41 -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build build-p41 -j "$(ncpu)" --target pando pando-index
fi
PANDO="$PWD/build-p41/pando"
PANDO_INDEX="$PWD/build-p41/pando-index"

X="$WORK/ud_demo_x$COPIES"
if [[ ! -f "$X/corpus.info" ]]; then
  echo "==> building $X ($COPIES copies of $SRC)"
  python3 scripts/replicate_corpus.py --src "$SRC" --copies "$COPIES" --out "$X" --pando-index "$PANDO_INDEX"
fi
[[ -f "$BASE/upos.bm" ]] || echo "note: $BASE has no bitmaps (pando-index --upgrade not run); its paths may differ from the replica's"

STAMP="$(hostname -s)-$(date +%Y%m%d-%H%M)"
OUT="$WORK/p41-$STAMP"
echo "==> threads $THREADS, reps $REPS → $OUT.{log,json}"
python3 test/scale_bench.py --pando "$PANDO" --corpus "$BASE" --corpus "$X" \
    --queries test/perf_queries.tsv --agg-queries test/perf_agg_queries.tsv \
    --threads "$THREADS" --reps "$REPS" --out "$OUT.json" 2>&1 | tee "$OUT.log"
