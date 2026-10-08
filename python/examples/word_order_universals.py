#!/usr/bin/env python3
"""Greenberg's word-order correlations, measured on a multilingual UD corpus.

Greenberg (1963) and Dryer (1992): languages where the object precedes the verb (OV)
tend to have postpositions, VO languages prepositions, while the order of adjective
and noun is not one of the pairs that correlate with OV/VO (Dryer). This script
measures all three orders per language from the dependency trees and tests the
correlations across the languages, with bootstrap confidence intervals and the
exceptions. (On a UD collection the languages are far from a balanced sample:
mostly Indo-European, several historical stages, so read the numbers as a demo.)

Division of labour:
  * pando finds the dependency pairs (verb → object, noun → adposition, noun →
    adjective) in the whole corpus in about a second each, as random samples with
    corpus positions: no CoNLL-U files to parse, no trees to build in Python;
  * Python does what no query language is made for: proportions per language,
    rank correlations across languages, resampling, a 2×2 table, the exceptions.

  PYTHONPATH=python/src python python/examples/word_order_universals.py \\
      /path/to/ud_demo --pando-bin build/pando

Standard library only (pandas would shorten the table code, but isn't needed).
Language = the UD file-name prefix of each hit's document (`eo_cairo-ud-test.conllu`
→ `eo`), so treebanks of one language are counted together.
"""

import argparse
import random
import statistics
from collections import defaultdict

from pando_cql import Corpus

# feature → (query, does the dependent come first?): head is the first query token
FEATURES = {
    "OV":    ('v:[upos="VERB"] > o:[deprel="obj"]',
              "object before its verb"),
    "Postp": ('n:[upos="NOUN" | upos="PROPN" | upos="PRON"] > a:[deprel="case" & upos="ADP"]',
              "adposition after its noun (a postposition)"),
    "AdjN":  ('n:[upos="NOUN"] > a:[deprel="amod" & upos="ADJ"]',
              "adjective before its noun"),
}


def language(doc_id):
    return doc_id.split("_", 1)[0]


def collect(corpus_dir, pando_bin, sample, seed):
    """{feature: {language: [n_pairs, n_with_dependent_first]}} from one sampled query each."""
    args = ["--sample", str(sample), "--seed", str(seed), "--limit", str(sample),
            "--context", "0", "--attrs", "upos"]          # positions are all we need
    counts = {}
    with Corpus.open(corpus_dir, pando_bin=pando_bin, session=False, extra_args=args) as corp:
        for feat, (query, _) in FEATURES.items():
            res = corp.run(query)
            per_lang = defaultdict(lambda: [0, 0])
            for hit in res["hits"]:
                head, dep = hit["tokens"][0], hit["tokens"][1]
                c = per_lang[language(hit["doc_id"])]
                c[0] += 1
                c[1] += dep["pos"] < head["pos"]
            # "Postp" counts the adposition *after* its noun
            if feat == "Postp":
                for c in per_lang.values():
                    c[1] = c[0] - c[1]
            counts[feat] = per_lang
            print(f"  {feat:5s} {len(res['hits']):7d} pairs sampled of {res['page']['total']:,}")
    return counts


def ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):                 # ties get their average rank
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            r[order[k]] = (i + j) / 2
        i = j + 1
    return r


def spearman(xs, ys):
    return statistics.correlation(ranks(xs), ranks(ys))


def bootstrap_ci(xs, ys, n=2000, seed=1):
    rnd = random.Random(seed)
    idx = range(len(xs))
    vals = []
    for _ in range(n):
        pick = [rnd.choice(idx) for _ in idx]
        a, b = [xs[i] for i in pick], [ys[i] for i in pick]
        if len(set(a)) > 1 and len(set(b)) > 1:
            vals.append(spearman(a, b))
    vals.sort()
    return vals[int(0.025 * len(vals))], vals[int(0.975 * len(vals)) - 1]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("corpus", help="pando index of a multilingual UD corpus")
    ap.add_argument("--pando-bin", default=None)
    ap.add_argument("--sample", type=int, default=300000, help="pairs sampled per feature")
    ap.add_argument("--min", type=int, default=40, help="pairs a language needs for each feature")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    print("Sampling dependency pairs with pando:")
    counts = collect(args.corpus, args.pando_bin, args.sample, args.seed)

    langs = sorted(l for l in counts["OV"]
                   if all(counts[f].get(l, [0])[0] >= args.min for f in FEATURES))
    share = {f: {l: counts[f][l][1] / counts[f][l][0] for l in langs} for f in FEATURES}
    print(f"\n{len(langs)} languages with at least {args.min} pairs for every feature\n")

    print("Correlations across languages (Spearman, 95% bootstrap interval over languages):")
    for a, b in [("OV", "Postp"), ("OV", "AdjN"), ("Postp", "AdjN")]:
        xs, ys = [share[a][l] for l in langs], [share[b][l] for l in langs]
        lo, hi = bootstrap_ci(xs, ys, seed=args.seed)
        print(f"  {a:5s} ~ {b:5s}  rho = {spearman(xs, ys):+.2f}   [{lo:+.2f}, {hi:+.2f}]")

    # dominant orders: a 2×2 table and the languages that go against the trend
    table = defaultdict(list)
    for l in langs:
        table[(share["OV"][l] > 0.5, share["Postp"][l] > 0.5)].append(l)
    print("\nDominant order (more than half of the pairs):")
    print(f"  {'':12s}{'prepositions':>14s}{'postpositions':>15s}")
    for ov, name in [(False, "VO"), (True, "OV")]:
        print(f"  {name:12s}{len(table[(ov, False)]):14d}{len(table[(ov, True)]):15d}")
    for key, label in [((True, False), "OV with prepositions"), ((False, True), "VO with postpositions")]:
        ex = sorted(table[key], key=lambda l: -counts["OV"][l][0])
        print(f"  {label}: {', '.join(ex) if ex else '-'}")

    print("\nPer language (share of pairs with the order; n = pairs sampled):")
    print(f"  {'lang':6s}{'OV':>7s}{'Postp':>7s}{'AdjN':>7s}{'n(OV)':>8s}")
    for l in sorted(langs, key=lambda l: share["OV"][l]):
        print(f"  {l:6s}{share['OV'][l]:7.2f}{share['Postp'][l]:7.2f}{share['AdjN'][l]:7.2f}"
              f"{counts['OV'][l][0]:8d}")


if __name__ == "__main__":
    main()
