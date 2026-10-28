#!/usr/bin/env python3
"""The Manatee CQL dialect against a corpus: Manatee's answers, from Manatee's semantics written out here.

    manatee_dialect_corpus_test.py --pando build/pando --pando-index build/pando-index

Generates a small corpus (self-authored, seeded): documents with a `genre`, sentences of 3-12 tokens, `upos` and
`lemma` from small sets. For each query, given with `MANATEE ` in front, it requires pando's total and hits to be
Manatee's, computed here:
  * a quoted value is a regex on the whole value, `==` literal, `!` its complement;
  * a sequence's hits are every (start, end) pair; `A | B` and repeated groups `(A B)+` their union, each pair once;
  * `within <s/>` is Manatee's merge (query/frstream.cc, RQinNode): hits by start, then end; once a hit runs past
    its region, the merge moves to the region holding its end and skips every hit starting before it; with
    `<doc genre="a"/>` the regions are those documents;
  * `<s>` / `</s>`: a sentence's first / last token; labels and `& 1.lemma = 2.lemma`.
The S4 shape (`[DET] []{0,2} [NOUN] within <s/>`) takes the tail-only path, the doc-attribute and union ones the
general path. Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import bisect
import json
import os
import random
import re
import subprocess
import sys
import tempfile

UPOS = ["DET", "NOUN", "ADJ", "VERB", "PRON", "AUX", "CCONJ", "PUNCT"]
LEMMAS = {"DET": ["the", "a"], "NOUN": ["house", "dog", "bed"], "ADJ": ["big", "old"], "VERB": ["see", "be", "bark"],
          "PRON": ["he", "it"], "AUX": ["be", "have"], "CCONJ": ["and"], "PUNCT": ["."]}


def generate(rnd):
    toks, sents, docs = [], [], []
    for d in range(8):
        dstart = len(toks)
        for _ in range(rnd.randint(4, 9)):
            s = len(toks)
            for _ in range(rnd.randint(3, 12)):
                u = rnd.choice(UPOS)
                toks.append((u, rnd.choice(LEMMAS[u])))
            sents.append((s, len(toks) - 1))
        docs.append((dstart, len(toks) - 1, "a" if d % 3 == 0 else "b"))
    ev = [{"type": "header", "version": 2, "positional": ["form", "lemma", "upos"], "structural": ["s", "doc"]}]
    for k, (a, b) in enumerate(sents):
        ev += [{"type": "token", "form": l, "lemma": l, "upos": u} for u, l in toks[a:b + 1]]
        ev.append({"type": "region", "struct": "s", "start_pos": a, "end_pos": b, "attrs": {"id": f"s{k}"}})
    for k, (a, b, g) in enumerate(docs):
        ev.append({"type": "region", "struct": "doc", "start_pos": a, "end_pos": b, "attrs": {"id": f"d{k}", "genre": g}})
    return ev, toks, sents, docs


def seq_hits(toks, pattern):
    """(start, end) pairs of a pattern: a list of (predicate or None for [], min, max); max 100 for unbounded."""
    n = len(toks)
    out = set()

    def rec(i, k, start):
        if k == len(pattern):
            if i > start:
                out.add((start, i - 1))
            return
        pred, lo, hi = pattern[k]
        j = i
        for reps in range(0, hi + 1):
            if reps >= lo:
                rec(j, k + 1, start)
            if j >= n or (pred is not None and not pred(toks[j])):
                break
            j += 1

    for s in range(n):
        rec(s, 0, s)
    return out


def manatee_within(hits, regions):
    hits = sorted(hits)
    ends = [e for _, e in regions]
    out, i, o = [], 0, 0
    while i < len(hits) and o < len(regions):
        b, e = hits[i]
        ob, oe = regions[o]
        if b < ob:
            i = bisect.bisect_left(hits, (ob, -1), i)
            continue
        if e > oe:
            o = bisect.bisect_left(ends, e, o)
            continue
        out.append(hits[i])
        i += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    ev, toks, sents, docs = generate(random.Random(5))
    tmp = tempfile.mkdtemp(prefix="pando_manatee_")
    src, d = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
    with open(src, "w", encoding="utf-8") as f:
        for e in ev:
            f.write(json.dumps(e) + "\n")
    p = subprocess.run([a.pando_index, "--format", "jsonl", src, d], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stderr)
        return 2

    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    def run(q, *extra):
        r = subprocess.run([a.pando, *extra, d, "MANATEE " + q], capture_output=True, text=True)
        return r.stdout if r.returncode == 0 else "ERROR " + r.stderr.strip()[:300]

    def pando_hits(q):
        out = run(q, "--json", "--limit", "100000")
        try:
            return sorted((h["match_start"], h["match_end"]) for h in json.loads(out)["result"]["hits"])
        except (ValueError, KeyError):
            return out[:300]

    U = lambda x: (lambda t: t[0] == x)
    L = lambda x: (lambda t: t[1] == x)
    allp = lambda *pats: set().union(*(seq_hits(toks, p) for p in pats))
    genre_a = [(s, e) for s, e, g in docs if g == "a"]
    first_tok = {s for s, _ in sents}
    last_tok = {e for _, e in sents}
    cases = [
        ('[upos="DET"] []{0,2} [upos="NOUN"]', allp([(U("DET"), 1, 1), (None, 0, 2), (U("NOUN"), 1, 1)])),
        ('[upos="DET"] []{0,2} [upos="NOUN"] within <s/>',
         manatee_within(allp([(U("DET"), 1, 1), (None, 0, 2), (U("NOUN"), 1, 1)]), sents)),
        ('[lemma="be"] []* [upos="NOUN"] within <s/>',
         manatee_within(allp([(L("be"), 1, 1), (None, 0, 100), (U("NOUN"), 1, 1)]), sents)),
        ('[upos="ADJ"]+ [upos="NOUN"] within <s/>',
         manatee_within(allp([(U("ADJ"), 1, 100), (U("NOUN"), 1, 1)]), sents)),
        ('[upos="DET"] []{0,3} [upos="NOUN"] within <doc genre="a"/>',
         manatee_within(allp([(U("DET"), 1, 1), (None, 0, 3), (U("NOUN"), 1, 1)]), genre_a)),
        ('[upos="DET"] [upos="NOUN"] | [upos="PRON"]',
         allp([(U("DET"), 1, 1), (U("NOUN"), 1, 1)], [(U("PRON"), 1, 1)])),
        ('[upos="DET"] [upos="NOUN"] | [upos="PRON"] within <s/>',
         manatee_within(allp([(U("DET"), 1, 1), (U("NOUN"), 1, 1)], [(U("PRON"), 1, 1)]), sents)),
        ('([upos="ADJ"] [upos="CCONJ"])+ [upos="NOUN"]',
         allp(*[[(U("ADJ"), 1, 1), (U("CCONJ"), 1, 1)] * k + [(U("NOUN"), 1, 1)] for k in range(1, 6)])),
        ('([upos="ADJ"] [upos="ADJ"]?)? [upos="NOUN"]',
         allp([(U("NOUN"), 1, 1)], [(U("ADJ"), 1, 1), (U("ADJ"), 0, 1), (U("NOUN"), 1, 1)])),
        ('<s> [upos="DET"]', {(i, i) for i in first_tok if toks[i][0] == "DET"}),
        ('[upos="NOUN"] </s>', {(i, i) for i in last_tok if toks[i][0] == "NOUN"}),
        ('[lemma="b.*"]', {(i, i) for i in range(len(toks)) if re.fullmatch("b.*", toks[i][1])}),
        ('[!lemma="b.*" & upos="NOUN"]', {(i, i) for i in range(len(toks)) if toks[i][0] == "NOUN"
                                           and not re.fullmatch("b.*", toks[i][1])}),
        ('[lemma=="b.*"]', set()),
        ('"the"', set()),   # the default attribute is `word`: this corpus has `form`, aliased (the lemma `the`)
        ('1:[upos="ADJ"] 2:[] & 1.lemma = 2.lemma', {(i, i + 1) for i in range(len(toks) - 1)
                                                     if toks[i][0] == "ADJ" and toks[i][1] == toks[i + 1][1]}),
    ]
    the = {(i, i) for i in range(len(toks)) if toks[i][1] == "the"}
    cases[14] = ('"the"', the)
    for q, want in cases:
        want = sorted(want)
        got_total = run(q, "--count-only").split()
        got = pando_hits(q)
        check(got == want and got_total[-1:] == [str(len(want))],
              f"{q}: {got_total[-1] if got_total else '?'} hits (Manatee {len(want)})"
              + ("" if got == want else f"; got {str(got)[:200]}"))
    r = subprocess.run([a.pando, "--timing", "--count-only", d, 'MANATEE [upos="DET"] []{0,2} [upos="NOUN"] within <s/>'],
                       capture_output=True, text=True)
    check("path=manatee_within" in r.stdout + r.stderr, "within <s/> with a gap: Manatee's merge")
    print("manatee dialect: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
