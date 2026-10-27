#!/usr/bin/env python3
"""Equivalence layers (equivalence_layer.h): classes of tokens named by an attribute's values, `[A] =:X [B]`.

    equivalence_layer_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/coref_sample.conllu

Indexes a self-authored CoNLL-U with coreference ids in MISC (`coref=e1`, `--misc-keys coref`; two texts, one class of one token),
declares `pando-index --relation coref=equivalence,attr=coref` and requires:
  * the declaration measured: scope text (every class inside one text), the class counts;
  * `[A] =:coref [B]`, `!=:coref`, `=:coref` in a chain with a sequence, `:: a =:coref b` / `:: a !=:coref b` and
    `count by` over it to count as a brute-force evaluation over the CoNLL-U (B never A itself);
  * errors that say what works: `=` without a layer between tokens, `=:head`, `>:coref`, a label on `=:coref`.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile


def tokens(path):
    """All tokens in corpus order: (form, lemma, upos, class id or None)."""
    out = []
    for line in open(path, encoding="utf-8"):
        c = line.rstrip("\n").split("\t")
        if len(c) != 10 or "-" in c[0] or "." in c[0]:
            continue
        cid = next((f[6:] for f in c[9].split("|") if f.startswith("coref=")), None)
        out.append((c[1], c[2], c[3], cid))
    return out


def mates(toks, i, j):
    return i != j and toks[i][3] is not None and toks[i][3] == toks[j][3]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    d = os.path.join(tempfile.mkdtemp(prefix="pando_equiv_"), "idx")
    for cmd in ([a.pando_index, "--misc-keys", "coref", a.conllu, d], [a.pando_index, "--relation", "coref=equivalence,attr=coref", d]):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stdout, p.stderr, file=sys.stderr)
            return 2
    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    line = next((l for l in info.splitlines() if l.startswith("relation.coref=")), "")
    check("equivalence" in line and "attr=coref" in line and "scope=text" in line, f"declared: {line}")
    check("1 of one token" in p.stderr and "5 classes" in p.stderr, f"measured: {p.stderr.strip().splitlines()[-1]}")

    def count(q):
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        return r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else "ERROR " + r.stderr.strip()[:200]

    t = tokens(a.conllu)
    n = len(t)
    ANY = lambda x: True
    UP = lambda u: (lambda x: x[2] == u)
    pairs = lambda c1, c2: sum(1 for i in range(n) for j in range(n) if c1(t[i]) and c2(t[j]) and mates(t, i, j))
    cases = [
        ('[] =:coref []', pairs(ANY, ANY)),
        ('[upos="PRON"] =:coref [upos="PROPN"]', pairs(UP("PRON"), UP("PROPN"))),
        ('[lemma="Anna"] =:coref []', pairs(lambda x: x[1] == "Anna", ANY)),
        ('[] !=:coref []', sum(1 for i in range(n) if not any(mates(t, i, j) for j in range(n)))),
        ('[upos="PROPN"] !=:coref [upos="PRON"]', sum(1 for i in range(n) if t[i][2] == "PROPN"
                                                     and not any(t[j][2] == "PRON" and mates(t, i, j)
                                                                 for j in range(n)))),
        ('[upos="DET"] [upos="NOUN"] =:coref [upos="PRON"]',
         sum(1 for i in range(n - 1) for j in range(n) if t[i][2] == "DET" and t[i + 1][2] == "NOUN"
             and t[j][2] == "PRON" and mates(t, i + 1, j))),
        # `::` filters between named tokens k apart
        ('a:[] [] [] b:[] :: a =:coref b', sum(1 for i in range(n - 3) if mates(t, i, i + 3))),
        ('a:[upos="PROPN"] [] [] [] [] [] [] b:[] :: a =:coref b',
         sum(1 for i in range(n - 7) if t[i][2] == "PROPN" and mates(t, i, i + 7))),
        ('a:[upos="PROPN"] [] b:[] :: a !=:coref b', sum(1 for i in range(n - 2) if t[i][2] == "PROPN"
                                                         and not mates(t, i, i + 2))),
    ]
    for q, want in cases:
        got = count(q)
        check(got == str(want), f"{q}: {got} (reference {want})")

    r = subprocess.run([a.pando, d, 'a:[upos="PRON"] =:coref b:[]; count by a.lemma, b.lemma'], capture_output=True,
                       text=True)
    got = {}
    for l in r.stdout.splitlines():
        f = l.split("\t")
        if len(f) >= 3 and f[2].isdigit():
            got[(f[0], f[1])] = int(f[2])
    want = {}
    for i in range(n):
        for j in range(n):
            if t[i][2] == "PRON" and mates(t, i, j):
                want[(t[i][1], t[j][1])] = want.get((t[i][1], t[j][1]), 0) + 1
    check(got == want, f"count by a.lemma, b.lemma over =:coref: {got}")

    for q, needle in [('[] = []', "equivalence layer"), ('[] =:head []', "pointer layer"),
                      ('[] >:coref []', "equivalence layer"), ('[] =:coref:x []', "no labels")]:
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        msg = (r.stdout + r.stderr).strip()
        check(r.returncode != 0 or needle in msg, f"{q} refused: {msg[:120]}")
        if needle not in msg:
            check(False, f"{q}: message lacks '{needle}'")
    print("equivalence layers: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
