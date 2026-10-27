#!/usr/bin/env python3
"""A rebuild into the same folder must not keep derived dependency files of the earlier build.

    index_rebuild_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/sample.conllu

Builds the sample, then a variant with the same tokens and different heads (every word attached to its
sentence's root) into the SAME folder, and requires:

  * dep.head_rel8 (which readers prefer) to give the head of dep.head for every token, also after the
    folder's file times are made equal (a copy that does not keep them);
  * dependency queries (dep fast paths, head attributes, the generic path) to count as on a fresh build
    of the variant in an empty folder.

Before 2026-10-26 the upgrade rewrote dep.head_rel8 from the stale dep.head_rel8 it had opened (iswoc:
3% of the heads from an earlier tree) and kept the head attributes and edge postings derived from it.
Exit status 0 = all equal, 1 = a difference, 2 = a run error.
"""

import argparse
import bisect
import os
import struct
import subprocess
import sys
import tempfile

QUERIES = [
    '[upos="VERB"] > [deprel="nsubj"]',
    '[upos="NOUN"] > [upos="ADJ"]',
    '[deprel="nsubj" & parent [upos="VERB"]]',
    '[upos="VERB" & child [deprel="nsubj"]]',
    '[upos="VERB" & not child [deprel="nsubj"]]',
    '[upos="NOUN" & count(child [upos="ADJ"]) >= 1]',
    '[upos="VERB"] >> [upos="NOUN"]',
]


def run(cmd, env=None):
    p = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if p.returncode != 0:
        print(" ".join(cmd), p.stdout, p.stderr, sep="\n", file=sys.stderr)
        raise SystemExit(2)
    return p.stdout


def flat_variant(src, dst):
    """The same tokens; every non-root word attached to the sentence's root."""
    with open(src, encoding="utf-8") as f, open(dst, "w", encoding="utf-8") as g:
        sent = []

        def flush():
            root = next((r[0] for r in sent if not isinstance(r, str) and r[6] == "0"), None)
            for r in sent:
                if isinstance(r, str):
                    g.write(r)
                    continue
                if root and r[6] not in ("0", "_") and "-" not in r[0] and "." not in r[0]:
                    r[6] = root
                g.write("\t".join(r) + "\n")
            sent.clear()

        for line in f:
            if line.strip() and not line.startswith("#"):
                sent.append(line.rstrip("\n").split("\t"))
            else:
                if not line.strip():
                    flush()
                sent.append(line)
        flush()


def heads_differ(d):
    """Tokens whose dep.head_rel8 head differs from dep.head's."""
    with open(os.path.join(d, "dep.head"), "rb") as f:
        loc = f.read()
    with open(os.path.join(d, "dep.head_rel8"), "rb") as f:
        r8 = f.read()
    n = len(r8)
    loc = struct.unpack(f"<{n}h", loc)
    r8 = struct.unpack(f"<{n}b", r8)
    with open(os.path.join(d, "s.rgn"), "rb") as f:
        raw = f.read()
    sents = [struct.unpack_from("<qq", raw, i * 16) for i in range(len(raw) // 16)]
    starts = [s for s, _ in sents]
    bad = 0
    for p in range(n):
        if r8[p] == -128:   # escape: the head is in dep.head_rel8.exc
            continue
        i = bisect.bisect_right(starts, p) - 1
        hl = None if loc[p] < 0 or i < 0 else sents[i][0] + loc[p]
        bad += hl != (None if r8[p] == 0 else p + r8[p])
    return bad


def counts(pando, d, mode):
    env = dict(os.environ, PANDO_FASTPATH=mode)
    return [int(run([pando, "--count-only", d, q], env).split()[-1]) for q in QUERIES]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_rebuild_")
    variant = os.path.join(tmp, "flat.conllu")
    flat_variant(a.conllu, variant)
    same, fresh = os.path.join(tmp, "same"), os.path.join(tmp, "fresh")
    run([a.pando_index, a.conllu, same])
    run([a.pando_index, variant, same])          # into the same folder
    run([a.pando_index, variant, fresh])
    failures = 0
    bad = heads_differ(same)
    print(f"rebuilt folder: {bad} tokens with a dep.head_rel8 head other than dep.head's")
    failures += bad > 0
    # a copy that does not keep file times: everything equally old, then upgrade again
    for name in os.listdir(same):
        os.utime(os.path.join(same, name), (1_000_000_000, 1_000_000_000))
    run([a.pando_index, "--upgrade", same])
    bad = heads_differ(same)
    print(f"after equal file times + --upgrade: {bad} tokens differ")
    failures += bad > 0
    want = counts(a.pando, fresh, "off")
    for d, label in ((same, "rebuilt"), (fresh, "fresh")):
        for mode in ("on", "off"):
            got = counts(a.pando, d, mode)
            for q, w, g in zip(QUERIES, want, got):
                if w != g:
                    print(f"MISMATCH {label} {mode}: {q}: {g} (fresh, generic: {w})")
                    failures += 1
    print(f"{len(QUERIES)} queries x 2 folders x 2 modes: {'all equal' if not failures else f'{failures} problem(s)'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
