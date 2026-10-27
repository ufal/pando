#!/usr/bin/env python3
"""The class join for equivalence layers (executor_classes.cpp): `a:[A] =:X b:[B]` walks each class once.

    class_join_test.py --pando build/pando --pando-index build/pando-index

Generates a small parallel corpus (self-authored, seeded): texts in three languages (`text_lang`), each token with a
`tuid` (multivalue: some tokens align to two units, some pairs share two units, some tokens are unaligned `_`),
indexes it as JSONL and declares `relation.tuid=equivalence,attr=tuid`. For each query it requires:
  * the class join to be taken (plan path class_join) where the shape allows it;
  * the same total and the same hits as the generic expansion (PANDO_FASTPATH=off), and every hit and pages at an
    offset in hit order: by the first token, then the second;
  * the total to equal a brute-force count over the generated tokens (pairs of distinct tokens sharing a unit,
    each pair once however many units they share).
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import json
import os
import random
import subprocess
import sys
import tempfile

LANGS = ["eng", "ces", "nld"]
WORDS = {"eng": ["the", "house", "is", "old", "a", "dog"], "ces": ["dům", "je", "starý", "pes"],
         "nld": ["het", "huis", "is", "oud", "een", "hond"]}


def generate(rnd):
    """(events, tokens): six texts of 15-30 tokens, languages in turn; tokens as (form, lang, set of units)."""
    toks, texts = [], []
    for t in range(6):
        lang = LANGS[t % 3]
        start = len(toks)
        for _ in range(rnd.randint(15, 30)):
            r = rnd.random()
            units = set() if r < 0.15 else {f"u{rnd.randint(1, 12)}"}
            if r > 0.8:
                units.add(f"u{rnd.randint(1, 12)}")
            toks.append((rnd.choice(WORDS[lang]), lang, units))
        texts.append((start, len(toks) - 1, lang))
    events = [{"type": "header", "version": 2, "positional": ["form", "tuid"], "multivalue": ["tuid"],
               "structural": ["s", "text"]}]
    events += [{"type": "token", "form": f, "tuid": "|".join(sorted(u)) or "_"} for f, _, u in toks]
    for k, (a, b, lang) in enumerate(texts):
        events.append({"type": "region", "struct": "text", "start_pos": a, "end_pos": b,
                       "attrs": {"lang": lang, "id": f"t{k}"}})
        events.append({"type": "region", "struct": "s", "start_pos": a, "end_pos": b, "attrs": {"id": f"s{k}"}})
    return events, toks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    rnd = random.Random(7)
    events, toks = generate(rnd)
    tmp = tempfile.mkdtemp(prefix="pando_classjoin_")
    src, d = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
    with open(src, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    for cmd in ([a.pando_index, "--format", "jsonl", src, d],
                [a.pando_index, "--relation", "tuid=equivalence,attr=tuid", d]):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            print(f"run error: {' '.join(cmd)}\n{p.stderr}")
            return 2

    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    def run(q, fast, *extra):
        env = dict(os.environ, PANDO_FASTPATH="on" if fast else "off")
        r = subprocess.run([a.pando, *extra, d, q], capture_output=True, text=True, env=env)
        return r.stdout if r.returncode == 0 else "ERROR " + r.stderr.strip()[:200]

    def hits(q, fast, offset=0, limit=100000):
        out = run(f"{q}; tabulate {offset} {limit} a.cpos, b.cpos", fast, "--limit", str(offset + limit))
        rows = [l for l in out.splitlines() if l and not l.startswith("#") and l[0].isdigit()]
        return [tuple(int(x) for x in r.split("\t")) for r in rows]

    n = len(toks)
    ANY = lambda i: True
    FORM = lambda w: (lambda i: toks[i][0] == w)
    LANG = lambda l: (lambda i: toks[i][1] == l)
    AND = lambda f, g: (lambda i: f(i) and g(i))
    brute = lambda fa, fb: sum(1 for i in range(n) for j in range(n)
                               if i != j and fa(i) and fb(j) and toks[i][2] & toks[j][2])
    cases = [
        ('a:[] =:tuid b:[]', ANY, ANY),
        ('a:[form="house"] =:tuid b:[]', FORM("house"), ANY),
        ('a:[] =:tuid b:[form="huis"]', ANY, FORM("huis")),
        ('a:[form="the"] =:tuid b:[form="het"]', FORM("the"), FORM("het")),
        ('a:[] =:tuid b:[] :: a.text_lang="eng" & b.text_lang="ces"', LANG("eng"), LANG("ces")),
        ('a:[form="is"] =:tuid b:[] :: b.text_lang!="nld"', FORM("is"), lambda i: toks[i][1] != "nld"),
        ('a:[form="is"] =:tuid b:[form="is"]', FORM("is"), FORM("is")),
        ('a:[form="dog"] =:tuid b:[] :: a.text_lang="eng" & b.text_lang!="eng"',
         AND(FORM("dog"), LANG("eng")), lambda i: toks[i][1] != "eng"),
    ]
    for q, fa, fb in cases:
        want = brute(fa, fb)
        fast = run(q, True, "--count-only").split()
        slow = run(q, False, "--count-only").split()
        got_f = fast[-1] if fast else "?"
        got_s = slow[-1] if slow else "?"
        check(got_f == str(want) and got_s == str(want), f"{q}: {got_f} (generic {got_s}, reference {want})")
        plan = run(q, True, "--json", "--debug", "--limit", "1")
        check("class_join" in plan, f"{q}: plan class_join")
        # hit order: by the first token, then the second (the generic expansion lists by its seed token)
        hf, hs = hits(q, True), hits(q, False)
        ref = sorted((i, j) for i in range(n) for j in range(n)
                     if i != j and fa(i) and fb(j) and toks[i][2] & toks[j][2])
        check(hf == ref and sorted(hs) == ref, f"{q}: all {len(hf)} hits in order (the generic path's as a set)")
        for off in (3, 17):
            check(hits(q, True, off, 5) == ref[off:off + 5], f"{q}: page at offset {off}")

    print("class join: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
