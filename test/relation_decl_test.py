#!/usr/bin/env python3
"""Relation declarations (src/index/relation_decl.*): `relation.head=` in corpus.info, measured by pando-index.

    relation_decl_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/sample.conllu

Requires:
  * the sample's basic tree declared `pointer,functional,tree,acyclic,label=deprel,scope=s` with `span` equal to the
    largest |head - id| in the CoNLL-U;
  * a corpus that is not a tree (a sentence with two roots; a sentence with a head cycle) declared without `tree`
    (two roots) and without `acyclic` (the cycle), still indexed and queryable (the basic tree is described, never
    refused);
  * corpus.info lines that parse back: queries run without a relation warning.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile

NOT_A_TREE = """# sent_id = two-roots
1\tJohn\tJohn\tPROPN\t_\t_\t0\troot\t_\t_
2\twalks\twalk\tVERB\t_\t_\t0\troot\t_\t_
3\t.\t.\tPUNCT\t_\t_\t2\tpunct\t_\t_

# sent_id = cycle
1\tMary\tMary\tPROPN\t_\t_\t2\tnsubj\t_\t_
2\tsees\tsee\tVERB\t_\t_\t3\tccomp\t_\t_
3\tit\tit\tPRON\t_\t_\t2\tobj\t_\t_
4\t.\t.\tPUNCT\t_\t_\t0\troot\t_\t_

"""


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        print(" ".join(cmd), p.stdout, p.stderr, sep="\n", file=sys.stderr)
        raise SystemExit(2)
    return p


def relation_line(d, name="head"):
    with open(os.path.join(d, "corpus.info"), encoding="utf-8") as f:
        for line in f:
            if line.startswith(f"relation.{name}="):
                return line.strip().split("=", 1)[1].split(",")
    return None


def max_span(conllu):
    span = 0
    with open(conllu, encoding="utf-8") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            c = line.split("\t")
            if "-" in c[0] or "." in c[0] or c[6] in ("_", "0"):
                continue
            span = max(span, abs(int(c[6]) - int(c[0])))
    return span


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_reldecl_")
    failures = 0

    sample = os.path.join(tmp, "sample")
    run([a.pando_index, a.conllu, sample])
    props = relation_line(sample)
    want = {"pointer", "functional", "tree", "acyclic", "label=deprel", "scope=s", f"span={max_span(a.conllu)}"}
    if props is None or not want <= set(props):
        print(f"FAIL sample: relation.head={props}, expected at least {sorted(want)}")
        failures += 1
    else:
        print("sample: relation.head=" + ",".join(props))
    q = run([a.pando, "--count-only", sample, '[upos="VERB"] > [deprel="nsubj"]'])
    if "relation" in q.stderr.lower():
        print("FAIL sample: relation warning when loading: " + q.stderr.strip())
        failures += 1

    bad_conllu = os.path.join(tmp, "not_a_tree.conllu")
    with open(bad_conllu, "w", encoding="utf-8") as f:
        f.write(NOT_A_TREE)
    bad = os.path.join(tmp, "bad")
    run([a.pando_index, bad_conllu, bad])
    props = relation_line(bad) or []
    if "tree" in props or "acyclic" in props or "functional" not in props:
        print(f"FAIL not a tree: relation.head={props} (expected functional, without tree and acyclic)")
        failures += 1
    else:
        print("not a tree: relation.head=" + ",".join(props))
    total = run([a.pando, "--count-only", bad, '[upos="PUNCT"]']).stdout.split()[-1]
    if total != "2":
        print(f"FAIL not a tree: [upos=\"PUNCT\"] counts {total}, expected 2 (the index must stay usable)")
        failures += 1

    print("relation declarations: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
