#!/usr/bin/env python3
"""Equivalence layers (equivalence_layer.h): classes of tokens named by an attribute's values, `[A] =:X [B]`.

    equivalence_layer_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/coref_sample.conllu

Indexes a self-authored CoNLL-U with coreference ids in MISC (`coref=e1`, `--misc-keys coref`; two texts, one class of one token),
declares `pando-index --relation coref=equivalence,attr=coref` and requires:
  * the declaration measured: scope text (every class inside one text), the class counts;
  * `[A] =:coref [B]`, `!=:coref`, `=:coref` in a chain with a sequence, `:: a =:coref b` / `:: a !=:coref b` and
    `count by` over it to count as a brute-force evaluation over the CoNLL-U (B never A itself);
  * errors that say what works: `=` without a layer between tokens, `=:head`, `>:coref`, a label on `=:coref`;
  * a corpus with `default_within=text`: a layer whose classes cross texts (scope corpus, as `tuid` across
    languages) is not cut by the default; one inside a text keeps it;
  * coreference as a converter writes it (JSONL: a multivalue `coref` attribute on mention heads, the layer declared
    in the header): measured (scope doc), an id given only alone found, `=:coref` across a two-id value.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import json
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
    # default_within=text (TEITOK corpora): a class layer across texts must not be cut by it
    with open(os.path.join(d, "corpus.info"), "a", encoding="utf-8") as f:
        f.write("default_within=text\n")
    p = subprocess.run([a.pando_index, "--relation", "uposclass=equivalence,attr=upos", d], capture_output=True, text=True)
    check("scope=corpus" in p.stderr, f"a class layer across texts: {p.stderr.strip().splitlines()[0] if p.stderr else p}")
    pron = [i for i in range(n) if t[i][2] == "PRON"]
    check(count('[upos="PRON"] =:uposclass []') == str(len(pron) * (len(pron) - 1)),
          f"default_within=text leaves =:uposclass across texts: {count('[upos=\"PRON\"] =:uposclass []')} "
          f"(reference {len(pron) * (len(pron) - 1)})")
    check(count('[] =:coref []') == str(pairs(ANY, ANY)), "default_within=text with =:coref (scope text): unchanged")

    # coreference as a converter writes it (pando knows no CorefUD convention): the mention heads' entity ids in a
    # multivalue attribute, declared an equivalence layer in the JSONL header; documents as regions
    ev = [{"type": "header", "version": 2, "positional": ["form", "lemma", "coref"], "structural": ["s", "doc"],
           "multivalue": ["coref"], "relations": {"coref": "equivalence,attr=coref"}}]
    sents = [("d1", [("The", "the", "_"), ("man", "man", "e1"), ("saw", "see", "_"), ("his", "he", "e1|e5"),
                     ("dog", "dog", "e2"), (".", ".", "_")]),
             ("d1", [("It", "it", "e2"), ("liked", "like", "_"), ("him", "he", "e1"), (".", ".", "_")]),
             ("d2", [("Mary", "Mary", "e3"), ("met", "meet", "_"), ("her", "she", "e3"), ("mother", "mother", "e4"),
                     (".", ".", "_")])]
    pos, docs = 0, {}
    for k, (doc, toks) in enumerate(sents):
        start = pos
        for f, l, c in toks:
            ev.append({"type": "token", "form": f, "lemma": l, "coref": c})
            pos += 1
        ev.append({"type": "region", "struct": "s", "start_pos": start, "end_pos": pos - 1, "attrs": {"id": f"s{k}"}})
        docs.setdefault(doc, [start, pos - 1])[1] = pos - 1
    for doc, (a0, b0) in docs.items():
        ev.append({"type": "region", "struct": "doc", "start_pos": a0, "end_pos": b0, "attrs": {"id": doc}})
    tmp2 = tempfile.mkdtemp(prefix="pando_coref_")
    src2, d2 = os.path.join(tmp2, "c.jsonl"), os.path.join(tmp2, "idx")
    with open(src2, "w", encoding="utf-8") as f:
        for e in ev:
            f.write(json.dumps(e) + "\n")
    p = subprocess.run([a.pando_index, "--format", "jsonl", src2, d2], capture_output=True, text=True)
    info = open(os.path.join(d2, "corpus.info"), encoding="utf-8").read() if p.returncode == 0 else ""
    line = next((l for l in info.splitlines() if l.startswith("relation.coref=")), "")
    check("attr=coref" in line and "scope=doc" in line, f"coref declared in the header and measured: {line or p.stderr[-200:]}")
    check("multivalue=coref" in info, "coref multivalue")
    # e1: man, his, him; e2: dog, It; e3: Mary, her; e4: mother alone; e5: his alone
    for q, want in [('[coref="e1"]', 3), ('[coref="e2"]', 2), ('[coref="e4"]', 1), ('[coref="e5"]', 1),
                    ('[lemma="man"] =:coref [lemma="he"]', 2), ('[lemma="dog"] =:coref [lemma="it"]', 1),
                    ('[lemma="Mary"] =:coref []', 1), ('[lemma="mother"] =:coref []', 0), ('[] =:coref []', 10)]:
        r = subprocess.run([a.pando, "--count-only", d2, q], capture_output=True, text=True)
        got = r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else r.stderr.strip()[:200]
        check(got == str(want), f"coref {q}: {got} (expected {want})")

    print("equivalence layers: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
