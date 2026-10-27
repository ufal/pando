#!/usr/bin/env python3
"""Relation layers in queries: `deps` from CoNLL-U DEPS as deviations from the basic tree (relation_layer.*).

    relation_layer_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/deps_sample.conllu

Indexes a CoNLL-U with enhanced dependencies (inherited edges, a refined label, conjunct propagation, `ref`, edges to
an empty node, a token with DEPS `_`) and requires:
  * `relation.deps=…,inherit=head` in corpus.info, only the deviating tokens stored;
  * `[A] >:deps [B]`, `[A] <:deps [B]`, labels (`>:deps:nsubj`, quoted `>:deps:"conj:and"`) to count as a direct
    count in the CoNLL-U (edges to empty nodes left out, DEPS `_` = the basic edge);
  * `>:head:label` to equal `>` with a deprel condition.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile


def sentences(path):
    s = []
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if not line:
            if s:
                yield s
            s = []
            continue
        if line.startswith("#"):
            continue
        c = line.split("\t")
        if "-" in c[0] or "." in c[0]:
            continue
        s.append(c)
    if s:
        yield s


def deps_edges(tok):
    if tok[8] in ("_", ""):
        return {(int(tok[6]), tok[7])}
    out = set()
    for e in tok[8].split("|"):
        h, _, lab = e.partition(":")
        if "." not in h:
            out.add((int(h), lab))
    return out


def reference(path, head_ok, dep_ok, label=None, basic=False):
    n = 0
    for s in sentences(path):
        for d in s:
            heads = {h for h, lab in ({(int(d[6]), d[7])} if basic else deps_edges(d))
                     if 0 < h <= len(s) and (label is None or lab == label)}
            n += sum(1 for h in heads if head_ok(s[h - 1]) and dep_ok(d))
    return n


ANY = lambda t: True
CASES = [   # query, reference(head_ok, dep_ok, label, basic)
    ('[] >:deps []', (ANY, ANY, None, False)),
    ('[upos="VERB"] >:deps:nsubj []', (lambda t: t[3] == "VERB", ANY, "nsubj", False)),
    ('[upos="VERB"] >:head:nsubj []', (lambda t: t[3] == "VERB", ANY, "nsubj", True)),
    ('[lemma="walk"] >:deps [upos="PROPN"]', (lambda t: t[2] == "walk", lambda t: t[3] == "PROPN", None, False)),
    ('[upos="NOUN"] <:deps [upos="VERB"]', (lambda t: t[3] == "VERB", lambda t: t[3] == "NOUN", None, False)),
    ('[] >:deps:"conj:and" []', (ANY, ANY, "conj:and", False)),
    ('[] >:deps:ref []', (ANY, ANY, "ref", False)),
    ('[] >:deps:"obl:in" []', (ANY, ANY, "obl:in", False)),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    d = os.path.join(tempfile.mkdtemp(prefix="pando_rellayer_"), "idx")
    p = subprocess.run([a.pando_index, a.conllu, d], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout, p.stderr, file=sys.stderr)
        return 2
    failures = 0
    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    line = next((l for l in info.splitlines() if l.startswith("relation.deps=")), "")
    if "inherit=head" not in line:
        print(f"FAIL: relation.deps line: {line!r}")
        failures += 1
    else:
        print(line)
    for q, (head_ok, dep_ok, label, basic) in CASES:
        want = reference(a.conllu, head_ok, dep_ok, label, basic)
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        got = r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else "ERROR " + r.stderr.strip()[:200]
        ok = got == str(want)
        failures += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {q}: {got} (reference {want})")
    b = subprocess.run([a.pando, "--count-only", d, '[upos="VERB"] > [deprel="nsubj"]'], capture_output=True, text=True)
    h = subprocess.run([a.pando, "--count-only", d, '[upos="VERB"] >:head:nsubj []'], capture_output=True, text=True)
    if b.stdout.split()[-1:] != h.stdout.split()[-1:]:
        print(f"FAIL: >:head:nsubj {h.stdout.strip()} vs > [deprel=nsubj] {b.stdout.strip()}")
        failures += 1
    print("relation layers: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
