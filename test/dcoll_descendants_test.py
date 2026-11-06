#!/usr/bin/env python3
"""`dcoll descendants`: each descendant counted once per node, a node of several hits as often as it occurs,
nodes nested in each other's subtrees each adding their own (DependencyIndex::for_each_descendant_count, one
pass per sentence). Observed counts against references straight from the CoNLL-U.

A damaged index (the sentence regions one token off from dep.head, as an old UD 2.17 build had) turns the trees
into cycles of heads: dcoll descendants and dep_subtree() must still end, a node counting what a walk from it
reaches (each token once, the node itself not).

  test/dcoll_descendants_test.py --pando build/pando --pando-index build/pando-index \\
      --conllu test/data/sample.conllu
"""
import argparse, array, json, os, shutil, subprocess, sys, tempfile
from collections import Counter


def read_conllu(path):
    """the tokens in corpus order as (lemma, upos, head position or None), and the sentences as (start, end)"""
    toks, sents = [], []
    for block in open(path, encoding="utf-8").read().split("\n\n"):
        start = len(toks)
        for line in block.splitlines():
            if not line or line.startswith("#"):
                continue
            f = line.split("\t")
            if "-" in f[0] or "." in f[0]:
                continue
            toks.append((f[2], f[3], None if f[6] in ("0", "_") else start + int(f[6]) - 1))
        if len(toks) > start:
            sents.append((start, len(toks) - 1))
    return toks, sents


def tree_reference(toks, nodes):
    """every token counts the node occurrences among its ancestors"""
    times = Counter(nodes)
    ref = Counter()
    for t, (lemma, _, h) in enumerate(toks):
        seen = set()
        while h is not None and h != t and h not in seen:
            seen.add(h)
            ref[lemma] += times[h]
            h = toks[h][2]
    return +ref


def walk_reference(toks, regions, local_heads, nodes):
    """per node occurrence, the tokens a walk down the sentence's children reaches (each once, not the node)"""
    n = len(toks)
    ref = Counter()
    for s, e in regions:
        e = min(e, n - 1)
        if e < s:
            continue
        kids = [[] for _ in range(e - s + 1)]
        for i in range(e - s + 1):
            h = local_heads[s + i]
            if 0 <= h <= e - s:
                kids[h].append(i)
        for p in nodes:
            if not s <= p <= e:
                continue
            seen, todo = {p - s}, [p - s]
            while todo:
                for k in kids[todo.pop()]:
                    if k not in seen:
                        seen.add(k)
                        todo.append(k)
                        ref[toks[s + k][0]] += 1
    return +ref


def main():
    ap = argparse.ArgumentParser()
    for a in ("--pando", "--pando-index", "--conllu"):
        ap.add_argument(a, required=True)
    o = ap.parse_args()
    toks, sents = read_conllu(o.conllu)
    fails = []

    nouns = [p for p, t in enumerate(toks) if t[1] == "NOUN"]
    verbs_per_child = [h for (_, _, h) in toks if h is not None and toks[h][1] == "VERB"]
    cases = [
        ('[upos="NOUN"]; dcoll descendants by lemma', nouns),
        ('[]; dcoll descendants by lemma', list(range(len(toks)))),
        ('a:[upos="VERB"] > b:[]; dcoll a.descendants by lemma', verbs_per_child),
    ]

    def obs(idx, query, label):
        try:
            out = subprocess.run([o.pando, "--json", "--max-items", "1000000", idx, "set min-freq 1; " + query],
                                 capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            fails.append(f"{label}: no answer within 60 s")
            return None
        if out.returncode != 0:
            fails.append(f"{label}: exit {out.returncode} {out.stderr[:200]}")
            return None
        return Counter({c["word"]: c["obs"] for c in json.loads(out.stdout)["result"]["collocates"]})

    def compare(label, got, want):
        if got is None:
            return
        if got != want:
            diff = sorted(w for w in set(got) | set(want) if got[w] != want[w])
            fails.append(f"{label}: {len(diff)} lemmas differ, e.g. "
                         + ", ".join(f"{w} {got[w]}≠{want[w]}" for w in diff[:4]))

    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([o.pando_index, o.conllu, idx], check=True, capture_output=True)
        for query, nodes in cases:
            compare(query, obs(idx, query, query), tree_reference(toks, nodes))

        # damaged: every sentence region one token later (the last one past the end of the corpus)
        bad = os.path.join(tmp, "bad")
        shutil.copytree(idx, bad)
        rgn = array.array("q")
        with open(os.path.join(bad, "s.rgn"), "rb") as f:
            rgn.frombytes(f.read())
        regions = [(rgn[i] + 1, rgn[i + 1] + 1) for i in range(0, len(rgn), 2)]
        if regions != [(s + 1, e + 1) for s, e in sents]:
            fails.append("s.rgn does not hold the CoNLL-U sentences")
        with open(os.path.join(bad, "s.rgn"), "wb") as f:
            array.array("q", [x for r in regions for x in r]).tofile(f)
        heads = array.array("h")
        with open(os.path.join(bad, "dep.head"), "rb") as f:
            heads.frombytes(f.read())
        cycles = 0
        for s, e in regions:
            e = min(e, len(toks) - 1)
            for i in range(e - s + 1):
                k, steps = i, 0
                while 0 <= k <= e - s and steps <= e - s:
                    k, steps = heads[s + k], steps + 1
                cycles += steps > e - s
        if not cycles:
            fails.append("the damaged copy has no cycle of heads (the test tests nothing)")
        # nodes repeated without the tree (`>` follows the heads, which the shifted sentences change): each verb
        # three times, the node positions from tabulate
        rep = 'a:[upos="VERB"] []{0,2}'
        tab = subprocess.run([o.pando, bad, rep + "; tabulate 0 1000000 a.cpos"], capture_output=True, text=True,
                             timeout=60)
        verbs3 = [int(x) for x in tab.stdout.split()[1:]]
        if len(verbs3) <= len(set(verbs3)):
            fails.append(f"damaged: {rep} repeats no node ({len(verbs3)} hits; {tab.stderr[:200]})")
        cases = cases[:2] + [(rep + "; dcoll a.descendants by lemma", verbs3)]
        for query, nodes in cases:
            compare("damaged " + query, obs(bad, query, "damaged " + query),
                    walk_reference(toks, regions, heads, nodes))
        try:
            out = subprocess.run([o.pando, "--json", "--total", bad,
                                  'n:[upos="NOUN"] :: np:dep_subtree(n) & tcnt(np) > 2'],
                                 capture_output=True, text=True, timeout=60)
            if out.returncode != 0:
                fails.append(f"damaged dep_subtree: exit {out.returncode} {out.stderr[:200]}")
        except subprocess.TimeoutExpired:
            fails.append("damaged dep_subtree: no answer within 60 s")

    for f in fails:
        print("FAIL:", f)
    print("dcoll_descendants:", "FAIL" if fails else "ok")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
