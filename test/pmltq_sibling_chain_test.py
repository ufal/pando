#!/usr/bin/env python3
"""PML-TQ queries whose nodes share a head (`~` in pando's chain): counts against a brute-force matcher.

    pmltq_sibling_chain_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/sample.conllu

Each pattern is a small tree (conditions, `child` / `parent` / `sibling` nodes). It is written out as a PML-TQ
query for pando and matched by brute force over the CoNLL-U: every assignment of distinct words to the
pattern's nodes that satisfies it is one result (PML-TQ's counting). Covers several dependents of one node,
a run ending in a dependent with its own dependent, a parent above the run, siblings, a root's siblings
(none), two nodes with the same conditions (never one word twice), and the same tree written from another
node (the same count).
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile

# a pattern node: (conditions {attr: value}, [(relation, node), ...]); relation: child, parent, sibling
V = lambda **c: c


def pmltq(node):
    conds, rel = node
    parts = [f'{k} = "{v}"' for k, v in conds.items()]
    parts += [f"{r} {pmltq(n)}" for r, n in rel]
    return "a-node [ " + ", ".join(parts) + " ]" if parts else "a-node [ ]"


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
        cur.append({"id": int(f[0]), "form": f[1], "lemma": f[2], "upos": f[3],
                    "head": int(f[6]) if f[6].isdigit() else -1, "deprel": f[7]})
    if cur:
        yield cur


def count(sent, pattern):
    """Assignments of distinct words of `sent` to the pattern's nodes."""
    by_id = {w["id"]: w for w in sent}
    flat = []  # (conds, index of the related node, relation to it)

    def walk(node, parent_idx, rel):
        idx = len(flat)
        flat.append((node[0], parent_idx, rel))
        for r, n in node[1]:
            walk(n, idx, r)

    walk(pattern, None, None)

    def ok(w, conds):
        return all(w.get(k) == v for k, v in conds.items())

    def related(a, b, rel):  # a = the earlier node's word, b = this node's word
        if rel == "child":
            return b["head"] == a["id"]
        if rel == "parent":
            return a["head"] == b["id"]
        if rel == "sibling":
            return a["head"] == b["head"] and a["head"] > 0
        raise ValueError(rel)

    total = 0

    def assign(i, chosen):
        nonlocal total
        if i == len(flat):
            total += 1
            return
        conds, pi, rel = flat[i]
        for w in sent:
            if w["id"] in chosen or not ok(w, conds):
                continue
            if pi is not None and not related(by_id[chosen[pi]], w, rel):
                continue
            chosen.append(w["id"])
            assign(i + 1, chosen)
            chosen.pop()

    assign(0, [])
    return total


PATTERNS = [
    # three dependents of one verb
    (V(upos="VERB"), [("child", (V(deprel="nsubj"), [])), ("child", (V(deprel="obj"), [])),
                      ("child", (V(deprel="obl"), []))]),
    # two dependents with the same conditions: never one word twice
    (V(upos="VERB"), [("child", (V(upos="NOUN"), [])), ("child", (V(upos="NOUN"), [])),
                      ("child", (V(upos="PUNCT"), []))]),
    (V(upos="NOUN"), [("child", (V(), [])), ("child", (V(), [])), ("child", (V(), []))]),
    # the run ends in a dependent with its own dependent
    (V(upos="VERB"), [("child", (V(deprel="nsubj"), [])),
                      ("child", (V(deprel="obl"), [("child", (V(deprel="case"), []))])),
                      ("child", (V(deprel="punct"), []))]),
    # a parent above the run
    (V(upos="NOUN"), [("parent", (V(upos="VERB"), [])), ("child", (V(deprel="det"), [])),
                      ("child", (V(deprel="amod"), []))]),
    # siblings
    (V(deprel="nsubj"), [("sibling", (V(deprel="obj"), []))]),
    (V(deprel="det"), [("sibling", (V(deprel="amod"), [])), ("sibling", (V(deprel="case"), []))]),
    (V(deprel="nsubj"), [("sibling", (V(deprel="obl"), [("child", (V(deprel="case"), []))]))]),
    # a root has no siblings
    (V(deprel="root"), [("sibling", (V(), []))]),
    # the first pattern from another node
    (V(deprel="obl"), [("parent", (V(upos="VERB"), [("child", (V(deprel="nsubj"), [])),
                                                    ("child", (V(deprel="obj"), []))]))]),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_pmltq_sib_")
    idx = os.path.join(tmp, "idx")
    p = subprocess.run([a.pando_index, a.conllu, idx], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stderr)
        return 2
    sents = list(sentences(a.conllu))
    failures = 0
    nonzero = 0
    for pat in PATTERNS:
        q = "PMLTQ " + pmltq(pat)
        want = sum(count(s, pat) for s in sents)
        r = subprocess.run([a.pando, "--count-only", idx, q], capture_output=True, text=True)
        out = r.stdout.split()
        got = int(out[-1]) if out and out[-1].isdigit() else r.stderr.strip()[:200]
        ok = got == want
        failures += not ok
        nonzero += isinstance(got, int) and got > 0
        print(("ok   " if ok else "FAIL ") + f"{want:6} {got!s:>6}  {q}")
    if nonzero < 5:
        print(f"FAIL only {nonzero} patterns match anything: the test says little on this corpus")
        failures += 1
    print("PML-TQ same-head chains: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
