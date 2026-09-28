#!/usr/bin/env python3
"""Build an N-times larger corpus from the same CoNLL-U sources (scale tests, P4).

Makes a tree of symlinks with N copies of every .conllu file under SRC
(copy k under <out>.src/rKKK/<relative path>), indexes it with pando-index (which
reads the files in sorted order, so each copy is one contiguous stretch of the
new corpus) and runs `pando-index --upgrade` for the derived files (bitmaps,
dep.head_rel, dep pairs, fold indexes).

The copies are exact repeats: same lexicon, same per-copy densities, N times
the postings. Good for how the executor scales with corpus size (partitioned
counts, 32- → 64-bit postings past 2^31 tokens, page cache); not for lexicon
growth, which a real corpus of that size would show.

  scripts/replicate_corpus.py --src data/raw/ud_demo --copies 16 \\
      --out /Volumes/Data2/Corpora/kontext-pando/pando/ud_demo_x16 \\
      --pando-index build/pando-index

Disk: roughly N × the source index size (ud_demo: ~1.4 GB → ×16 ≈ 22 GB,
×60 ≈ 85 GB). The 2^31-token boundary (8-byte postings) is crossed at ×57
for the 38M-token ud_demo.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, help="directory (recursive) or file of .conllu sources")
    ap.add_argument("--copies", type=int, required=True)
    ap.add_argument("--out", required=True, help="output index directory (must not exist)")
    ap.add_argument("--pando-index", default="build/pando-index")
    ap.add_argument("--keep-links", action="store_true", help="keep the <out>.src symlink tree")
    ap.add_argument("--no-upgrade", action="store_true", help="skip pando-index --upgrade")
    opts = ap.parse_args()

    src = os.path.abspath(opts.src)
    out = os.path.abspath(opts.out)
    if opts.copies < 1:
        ap.error("--copies must be >= 1")
    if os.path.exists(out):
        ap.error(f"{out} exists; remove it first")
    if os.path.isfile(src):
        files = [src]
        base = os.path.dirname(src)
    else:
        base = src
        files = []
        for root, _dirs, names in os.walk(src, followlinks=True):
            files += [os.path.join(root, n) for n in names if n.endswith(".conllu")]
        files.sort()
    if not files:
        sys.exit(f"no .conllu files under {src}")

    links = out + ".src"
    if os.path.exists(links):
        shutil.rmtree(links)
    width = max(3, len(str(opts.copies - 1)))
    for k in range(opts.copies):
        for f in files:
            dst = os.path.join(links, f"r{k:0{width}d}", os.path.relpath(f, base))
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            os.symlink(f, dst)
    print(f"{len(files)} files x {opts.copies} copies -> {links}", flush=True)

    t0 = time.monotonic()
    subprocess.run([opts.pando_index, links, out], check=True)
    t1 = time.monotonic()
    print(f"pando-index: {t1 - t0:.0f} s", flush=True)
    if not opts.no_upgrade:
        subprocess.run([opts.pando_index, "--upgrade", out], check=True)
        print(f"pando-index --upgrade: {time.monotonic() - t1:.0f} s", flush=True)
    if not opts.keep_links:
        shutil.rmtree(links)

    size = None
    with open(os.path.join(out, "corpus.info"), encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("size="):
                size = int(line.split("=", 1)[1])
    total_bytes = sum(os.path.getsize(os.path.join(out, n)) for n in os.listdir(out)
                      if os.path.isfile(os.path.join(out, n)))
    print(f"{out}: {size} tokens, {total_bytes / 1e9:.1f} GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
