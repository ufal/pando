#!/usr/bin/env python3
"""PML-TQ `a-root` with counted nodes (conditions on the sentence's tree) against a brute-force count.

    pmltq_aroot_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/sample.conllu

`a-root [ N-x descendant a-node [F], descendant a-node [X] ]` is one result per word X in a sentence
with at most N words of the tree matching F (here every word has a head or is a root word); `child
a-node […]` under a-root counts root words. Also `order-follows $x` (word order of two named nodes).
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile


def sentences(path):
    cur = []
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if not line:
            if cur:
                yield cur
            cur = []
            continue
        if line.startswith("#"):
            continue
        f = line.split("\t")
        if not f[0].isdigit():
            continue
        cur.append({"id": int(f[0]), "upos": f[3], "head": int(f[6]) if f[6].isdigit() else -1, "deprel": f[7]})
    if cur:
        yield cur


def words(s, upos=None):
    return [w for w in s if upos is None or w["upos"] == upos]


def roots(s):
    return [w for w in s if w["head"] == 0]


def ordered_objects(s):
    """verb with two NOUN/PRON children, the second after the first (order-follows)"""
    n = 0
    for v in words(s, "VERB"):
        kids = [w for w in s if w["head"] == v["id"] and w["upos"] in ("NOUN", "PROPN", "PRON")]
        n += sum(1 for a in kids for b in kids if b["id"] > a["id"])
    return n


# (PML-TQ query, brute-force count per sentence)
CASES = [
    ('a-root [ 10-x descendant a-node [], descendant a-node [ upos = "NOUN" ] ]',
     lambda s: len(words(s, "NOUN")) if len(s) <= 10 else 0),
    ('a-root [ 5..12x descendant a-node [], descendant a-node [] ]',
     lambda s: len(s) if 5 <= len(s) <= 12 else 0),
    ('a-root [ 15+x descendant a-node [], descendant a-node [ upos = "VERB" ] ]',
     lambda s: len(words(s, "VERB")) if len(s) >= 15 else 0),
    ('a-root [ 1+x descendant a-node [ upos = "ADJ" ], descendant a-node [ upos = "VERB" ] ]',
     lambda s: len(words(s, "VERB")) if words(s, "ADJ") else 0),
    ('a-root [ 0x descendant a-node [ upos = "PUNCT" ], descendant a-node [ upos = "NOUN" ] ]',
     lambda s: len(words(s, "NOUN")) if not words(s, "PUNCT") else 0),
    ('a-root [ 2x descendant a-node [ upos = "DET" ], child a-node [ upos = "VERB" ] ]',
     lambda s: sum(1 for w in roots(s) if w["upos"] == "VERB") if len(words(s, "DET")) == 2 else 0),
    ('a-root [ 1x child a-node [], descendant a-node [ upos = "ADP" ] ]',
     lambda s: len(words(s, "ADP")) if len(roots(s)) == 1 else 0),
    ('a-node $v := [ upos = "VERB", child a-node $a := [ upos ~ "NOUN|PROPN|PRON" ], '
     'child a-node $b := [ upos ~ "NOUN|PROPN|PRON", order-follows $a ] ]',
     ordered_objects),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_pmltq_aroot_")
    idx = os.path.join(tmp, "idx")
    p = subprocess.run([a.pando_index, a.conllu, idx], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stderr)
        return 2
    sents = list(sentences(a.conllu))
    failures = nonzero = 0
    for q, per_sentence in CASES:
        want = sum(per_sentence(s) for s in sents)
        r = subprocess.run([a.pando, "--count-only", "--cql", "pmltq", idx, q], capture_output=True, text=True)
        out = r.stdout.split()
        got = int(out[-1]) if out and out[-1].isdigit() else (r.stdout + r.stderr).strip()[:200]
        ok = got == want
        failures += not ok
        nonzero += isinstance(got, int) and got > 0
        print(("ok   " if ok else "FAIL ") + f"{want:6} {got!s:>6}  {q}")
    if nonzero < 5:
        print(f"FAIL only {nonzero} cases match anything: the test says little on this corpus")
        failures += 1
    print("PML-TQ a-root counts: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
