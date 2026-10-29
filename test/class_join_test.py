#!/usr/bin/env python3
"""The class join for equivalence layers (executor_classes.cpp): `a:[A] =:X b:[B]` walks each class once.

    class_join_test.py --pando build/pando --pando-index build/pando-index [--server build/pando-server]

Generates a small parallel corpus (self-authored, seeded): texts in three languages (`text_lang`), each token with a
`tuid` (multivalue: some tokens align to two units, some pairs share two units, some tokens are unaligned `_`),
indexes it as JSONL (the build declares and measures `relation.tuid` and `relation.s_tuid` itself; an index
without the declarations has them as well). For each query it requires:
  * the class join to be taken (plan path class_join) where the shape allows it;
  * the same total and the same hits as the generic expansion (PANDO_FASTPATH=off), and every hit and pages at an
    offset in hit order: by the first token, then the second;
  * the total to equal a brute-force count over the generated tokens (pairs of distinct tokens sharing a unit,
    each pair once however many units they share);
  * a pair whose tokens lie far apart (across an unaligned 120-token text) to be shown as runs with an elision,
    in the KWIC line, the JSON context and (with --server) the XML fragment, never with what lies between;
  * `a:[A] with b:[B] :: a.tuid = b.tuid` to take the class join as well, with the pairs of a brute-force reference
    (a token never with itself, as `with` has it since 2026-10-28) and of the general joins;
  * `=:s_tuid` over a region attribute (sentences aligned by `s_tuid`; one left unaligned) and `with … a.s_tuid =
    b.s_tuid`: the class join with the pairs of tokens in aligned sentences, as the reference and the general paths;
  * `[A] within L [having [C]]` and `not within` with L = s_tuid / tuid: the hits in a class (with a C token), or
    not, as the reference counts them;
  * `--aligned s_tuid` / `--aligned tuid` (with `--aligned-where`): each hit with the sentences aligned with it,
    the aligned tokens marked, as the reference gives them;
  * Manatee's aligned corpora (`MANATEE A within nld: B`, `!within`): A outside nld wholly inside a sentence whose
    s_tuid class has a B hit in nld (Manatee's within merge for a varying-length A), as the reference gives them;
  * attributes declared multivalue (`w1|w2` on a token, `s1|s2` on a segment) and nested segments: every path
    (class join, generic, `with`, the cross-statement form) as the reference;
  * `count by` over fields of either token to give the buckets of a brute-force tally, fast and generic.
The class join reads class members over 4 threads here (PANDO_CLASS_JOIN_THREADS / PANDO_CLASS_JOIN_MIN, unless set).
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import json
import os
import random
import subprocess
import sys
import socket
import tempfile
import time
import urllib.error
import urllib.request

LANGS = ["eng", "ces", "nld"]
WORDS = {"eng": ["the", "house", "is", "old", "a", "dog"], "ces": ["dům", "je", "starý", "pes"],
         "nld": ["het", "huis", "is", "oud", "een", "hond"]}


def generate(rnd):
    """(events, tokens, sentence tuids): six texts of 15-30 tokens, languages in turn, and an unaligned filler text;
    tokens as (form, lang, set of units). Each text is cut into sentences of 4-8 tokens; sentence k of the texts of
    one round of languages shares the sentence tuid `p<round>.<k>` (`s_tuid`), one sentence has none (`_`)."""
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
        texts.append((start, len(toks) - 1, lang, t))
        if t == 2:   # an unaligned text in between, longer than the display gap (match_display.h)
            start = len(toks)
            toks += [("filler", "xxx", set())] * 120
            texts.append((start, len(toks) - 1, "xxx", None))
    cut = random.Random(11)
    sents = []   # (first, last, sentence tuid)
    for a, b, lang, t in texts:
        k, p = 0, a
        while p <= b:
            q = b if t is None else min(b, p + cut.randint(3, 7))
            if b - q < 3:
                q = b
            stuid = "_" if t is None or (t == 4 and k == 0) else f"p{t // 3}.{k}"
            sents.append((p, q, stuid))
            k, p = k + 1, q + 1
    stuids = [None] * len(toks)
    for a, b, st in sents:
        for p in range(a, b + 1):
            stuids[p] = None if st == "_" else st
    events = [{"type": "header", "version": 2, "positional": ["form", "tuid"], "multivalue": ["tuid"],
               "structural": ["s", "text"]}]
    # (JSONL v2: an `s` region follows its own tokens)
    for k, (a, b, st) in enumerate(sents):
        events += [{"type": "token", "form": f, "tuid": "|".join(sorted(u)) or "_"} for f, _, u in toks[a:b + 1]]
        events.append({"type": "region", "struct": "s", "start_pos": a, "end_pos": b,
                       "attrs": {"id": f"s{k}", "tuid": st}})
    for k, (a, b, lang, _) in enumerate(texts):
        events.append({"type": "region", "struct": "text", "start_pos": a, "end_pos": b,
                       "attrs": {"lang": lang, "id": f"t{k}"}})
    return events, toks, stuids


def main():
    # the class join's member read over threads even on this small corpus (the generic reference runs without)
    os.environ.setdefault("PANDO_CLASS_JOIN_THREADS", "4")
    os.environ.setdefault("PANDO_CLASS_JOIN_MIN", "1")
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", help="pando-server: also check the XML fragment of a far-apart pair")
    a = ap.parse_args()
    rnd = random.Random(7)
    events, toks, stuids = generate(rnd)
    tmp = tempfile.mkdtemp(prefix="pando_classjoin_")
    src, d = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
    with open(src, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    # no --relation: `tuid` and `s_tuid` are declared and measured by the build itself
    for cmd in ([a.pando_index, "--format", "jsonl", src, d],):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            print(f"run error: {' '.join(cmd)}\n{p.stderr}")
            return 2

    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    for name in ("tuid", "s_tuid"):
        line = next((l for l in info.splitlines() if l.startswith(f"relation.{name}=")), "")
        check(f"equivalence,attr={name}" in line and "span=" in line and "span=-1" not in line,
              f"{name} declared and measured by the build: {line}")
    # an index without the declarations (built before them): the layers are there all the same
    import shutil
    old_idx = os.path.join(tmp, "old")
    shutil.copytree(d, old_idx)
    with open(os.path.join(old_idx, "corpus.info"), "w", encoding="utf-8") as f:
        f.write("".join(l + "\n" for l in info.splitlines() if not l.startswith("relation.")))
    for q in ('a:[form="house"] =:tuid b:[]', 'a:[] =:s_tuid b:[]', '[form="house"] within s_tuid having [form="huis"]'):
        r1 = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        r2 = subprocess.run([a.pando, "--count-only", old_idx, q], capture_output=True, text=True)
        check(r1.stdout == r2.stdout and r1.returncode == 0 and r2.returncode == 0,
              f"{q} on an index without declarations: {r2.stdout.strip() or r2.stderr.strip()[:100]}")

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
        # a language in the token (sliced search, 2026-10-29): the languages lie in turns, so each side's slice is
        # several stretches; the narrow side either first or second
        ('a:[text_lang="eng"] =:tuid b:[form="huis" & text_lang="nld"]', LANG("eng"), AND(FORM("huis"), LANG("nld"))),
        ('a:[form="the" & text_lang="eng"] =:tuid b:[text_lang="nld"]', AND(FORM("the"), LANG("eng")), LANG("nld")),
    ]
    for q, fa, fb in cases:
        want = brute(fa, fb)
        fast = run(q, True, "--count-only").split()
        slow = run(q, False, "--count-only").split()
        got_f = fast[-1] if fast else "?"
        got_s = slow[-1] if slow else "?"
        check(got_f == str(want) and got_s == str(want), f"{q}: {got_f} (generic {got_s}, reference {want})")
        r = subprocess.run([a.pando, "--timing", "--count-only", d, q], capture_output=True, text=True)
        check("path=class_join" in r.stdout + r.stderr, f"{q}: plan class_join")   # (pairs carry no debug block)
        # hit order: by the first token, then the second (the generic expansion lists by its seed token)
        hf, hs = hits(q, True), hits(q, False)
        ref = sorted((i, j) for i in range(n) for j in range(n)
                     if i != j and fa(i) and fb(j) and toks[i][2] & toks[j][2])
        check(hf == ref and sorted(hs) == ref, f"{q}: all {len(hf)} hits in order (the generic path's as a set)")
        for off in (3, 17):
            check(hits(q, True, off, 5) == ref[off:off + 5], f"{q}: page at offset {off}")

    # count by: the buckets as a brute-force tally over the pairs (fields on either token, positional and region)
    field = {"form": lambda i: toks[i][0], "text_lang": lambda i: toks[i][1]}
    for q, fa, fb, fields in [
        ('a:[] =:tuid b:[]', ANY, ANY, ["a.text_lang", "b.text_lang"]),
        ('a:[form="house"] =:tuid b:[]', FORM("house"), ANY, ["b.form"]),
        ('a:[] =:tuid b:[] :: a.text_lang="eng"', LANG("eng"), ANY, ["a.form", "b.form", "b.text_lang"]),
        ('a:[form="is"] =:tuid b:[form="is"]', FORM("is"), FORM("is"), ["a.text_lang", "b.form"]),
    ]:
        want = {}
        for i in range(n):
            for j in range(n):
                if i != j and fa(i) and fb(j) and toks[i][2] & toks[j][2]:
                    k = tuple(field[f[2:]](i if f[0] == "a" else j) for f in fields)
                    want[k] = want.get(k, 0) + 1
        cq = f"{q}; count by {', '.join(fields)}"
        got = {}
        for fast in (True, False):
            out = run(cq, fast)
            g = {}
            for l in out.splitlines():
                c = l.split("\t")
                if len(c) > len(fields) and c[len(fields)].isdigit():
                    g[tuple(c[:len(fields)])] = int(c[len(fields)])
            got[fast] = g
        check(got[True] == want and got[False] == want,
              f"{cq}: {len(got[True])} buckets (generic {len(got[False])}, reference {len(want)})")
        r = subprocess.run([a.pando, "--timing", d, cq], capture_output=True, text=True)
        check("path=class_join" in r.stdout + r.stderr, f"{cq}: plan class_join")

    # `a:[A] with b:[B] :: a.tuid = b.tuid` goes through the class join too: the pairs (a token never with itself,
    # as `with` has it) as the general joins give them, a `::` filter on the source after `with` included
    def with_pairs(q, fast):
        out = run(q, fast, "--json", "--limit", "100000")
        try:
            r = json.loads(out)["result"]
        except ValueError:
            return out[:200]
        return [(p["source"]["tokens"][0]["pos"], p["target"]["tokens"][0]["pos"]) for p in r.get("pairs", [])]
    for q, fa, fb in [
        ('a:[form="house"] with b:[] :: a.tuid = b.tuid', FORM("house"), ANY),
        ('a:[] with b:[form="huis"] :: a.tuid = b.tuid', ANY, FORM("huis")),
        ('a:[form="is"] with b:[form="is"] :: a.tuid = b.tuid', FORM("is"), FORM("is")),
        ('a:[form="the"] with b:[] :: a.text_lang="eng" & b.text_lang!="eng" & a.tuid = b.tuid',
         AND(FORM("the"), LANG("eng")), lambda i: toks[i][1] != "eng"),
        ('a:[form="dog"] :: a.text_lang="eng" with b:[] :: b.text_lang="nld" & a.tuid = b.tuid',
         AND(FORM("dog"), LANG("eng")), LANG("nld")),
    ]:
        ref = sorted((i, j) for i in range(n) for j in range(n)
                     if i != j and fa(i) and fb(j) and toks[i][2] & toks[j][2])
        fast, slow = with_pairs(q, True), with_pairs(q, False)
        r = subprocess.run([a.pando, "--timing", "--count-only", d, q], capture_output=True, text=True)
        check(fast == ref, f"{q}: {len(fast) if isinstance(fast, list) else fast} pairs (reference {len(ref)})")
        check("path=class_join" in r.stdout + r.stderr, f"{q}: plan class_join")
        # the general joins (a source filter after `with` used to give 0 pairs)
        check(slow == ref, f"{q}: the general joins give {len(slow) if isinstance(slow, list) else slow}")
    # not of the class join's shape (two source tokens): the general joins, a source filter after `with` read
    q = 'a:[form="the"] [] with b:[] :: a.text_lang="eng" & b.text_lang="nld" & a.tuid = b.tuid'
    ref = sorted((i, j) for i in range(n - 1) for j in range(n) if toks[i][0] == "the" and toks[i][1] == "eng"
                 and toks[j][1] == "nld" and toks[i][2] & toks[j][2])
    got = with_pairs(q, True)
    check(got == ref, f"{q}: {len(got) if isinstance(got, list) else got} pairs (reference {len(ref)})")
    # `a:[A] =:X b:[B]` over an alignment layer (scope corpus) answers as `with … :: a.X = b.X`: the same pairs, a
    # the source (2026-10-28); a following command (`; tabulate`, `count by`) still reads the two-token hits
    for q, w in [('a:[form="house"] =:tuid b:[]', 'a:[form="house"] with b:[] :: a.tuid = b.tuid'),
                 ('a:[form="is"] =:tuid b:[form="is"] :: a.text_lang="eng"',
                  'a:[form="is"] with b:[form="is"] :: a.text_lang="eng" & a.tuid = b.tuid')]:
        got, want = with_pairs(q, True), with_pairs(w, True)
        check(isinstance(got, list) and got == want and len(got) > 0,
              f"{q}: {len(got) if isinstance(got, list) else got} pairs, as {w}")
        api = run(q, True, "--api", "--limit", "3")
        check('"pairs"' in api and '"parallel": true' in api, f"{q}: --api gives pairs")

    # classes of regions: `s_tuid` (sentences and their translations); a token's class is its sentence's tuid
    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    check("relation.s_tuid=equivalence,attr=s_tuid,scope=corpus" in info, "s_tuid declared and measured (scope corpus)")
    same_s = lambda i, j: stuids[i] is not None and stuids[i] == stuids[j]
    for q, fa, fb in [
        ('a:[] =:s_tuid b:[]', ANY, ANY),
        ('a:[form="house"] =:s_tuid b:[form="huis"]', FORM("house"), FORM("huis")),
        ('a:[] =:s_tuid b:[] :: a.text_lang="eng" & b.text_lang="ces"', LANG("eng"), LANG("ces")),
        ('a:[form="is"] =:s_tuid b:[form="is"]', FORM("is"), FORM("is")),
        ('a:[form="is" & text_lang="eng"] =:s_tuid b:[text_lang="nld"]', AND(FORM("is"), LANG("eng")), LANG("nld")),
        ('a:[text_lang="ces"] =:s_tuid b:[] :: b.text_lang="eng"', LANG("ces"), LANG("eng")),
    ]:
        ref = sorted((i, j) for i in range(n) for j in range(n) if i != j and fa(i) and fb(j) and same_s(i, j))
        fast, slow = hits(q, True), hits(q, False)
        r = subprocess.run([a.pando, "--timing", "--count-only", d, q], capture_output=True, text=True)
        check(fast == ref and sorted(slow) == ref and "path=class_join" in r.stdout + r.stderr
              and (r.stdout.split() or ["?"])[0] == str(len(ref)),
              f"{q}: {len(fast)} hits (generic {len(slow)}, reference {len(ref)}), total and plan")
    q = 'a:[form="house"] :: a.text_lang="eng" with b:[] :: b.text_lang="nld" & a.s_tuid = b.s_tuid'
    ref = sorted((i, j) for i in range(n) for j in range(n) if toks[i][0] == "house" and toks[i][1] == "eng"
                 and toks[j][1] == "nld" and same_s(i, j))
    fast, slow = with_pairs(q, True), with_pairs(q, False)
    check(fast == ref and slow == ref, f"{q}: {len(fast)} pairs (general joins {len(slow)}, reference {len(ref)})")

    # `within L [having [C]]` / `not within …` with L an equivalence layer: the hit's class (one with a C token)
    def classes_of(i, layer):
        return toks[i][2] if layer == "tuid" else ({stuids[i]} if stuids[i] else set())
    for layer in ("s_tuid", "tuid"):
        for having, hc in [(None, None), ('[form="huis"]', FORM("huis")), ('[text_lang="ces"]', LANG("ces"))]:
            with_c = set()
            if hc:
                for j in range(n):
                    if hc(j):
                        with_c |= classes_of(j, layer)
            for neg in (False, True):
                q = f'[form="house"] {"not " if neg else ""}within {layer}' + (f" having {having}" if having else "")
                inside = [i for i in range(n) if toks[i][0] == "house"
                          and (classes_of(i, layer) & with_c if hc else classes_of(i, layer))]
                want = len([i for i in range(n) if toks[i][0] == "house"]) - len(inside) if neg else len(inside)
                got = run(q, True, "--count-only").split()
                check(got[-1:] == [str(want)], f"{q}: {got[-1] if got else '?'} (reference {want})")
    r = subprocess.run([a.pando, "--count-only", d, '[form="the"] [] within s_tuid'], capture_output=True, text=True)
    check("one-token query" in r.stdout + r.stderr and "=:s_tuid" in r.stdout + r.stderr,
          "two tokens within s_tuid: refused, pointing to =:s_tuid")

    # --aligned: each hit with the sentences aligned with it (aligned_pieces.h): over s_tuid the other sentences of
    # its sentence's class, over tuid the sentences of its tokens' class mates with those marked; --aligned-where
    sent_of = {}
    k = 0
    for i in range(n):
        if i == 0 or (stuids[i] != stuids[i - 1]) or toks[i][1] != toks[i - 1][1]:
            k += 1
        sent_of[i] = k
    def aligned(q, layer, where=""):
        extra = ["--aligned", layer] + (["--aligned-where", where] if where else [])
        out = run(q, True, "--json", "--limit", "1000", *extra)
        try:
            return [(h["match_start"], [(x["start"], x["end"], x["marked"]) for x in h["aligned"]])
                    for h in json.loads(out)["result"]["hits"]]
        except (ValueError, KeyError):
            return out[:200]
    def sentence_span(i):
        a = i
        while a > 0 and sent_of[a - 1] == sent_of[i]:
            a -= 1
        b = i
        while b + 1 < n and sent_of[b + 1] == sent_of[i]:
            b += 1
        return a, b
    for layer, where in [("s_tuid", ""), ("tuid", ""), ("s_tuid", "text_lang=nld"), ("tuid", "text_lang=ces|nld")]:
        langs = where.split("=")[1].split("|") if where else None
        want = []
        for i in range(n):
            if toks[i][0] != "house":
                continue
            pieces = {}
            for j in range(n):
                if (sent_of[j] == sent_of[i]) or (langs and toks[j][1] not in langs):
                    continue
                if layer == "s_tuid" and same_s(i, j):
                    pieces.setdefault(sentence_span(j), [])
                if layer == "tuid" and toks[i][2] & toks[j][2]:
                    pieces.setdefault(sentence_span(j), []).append(j)
            want.append((i, [(a_, b_, sorted(m)) for (a_, b_), m in sorted(pieces.items())]))
        got = aligned('[form="house"]', layer, where)
        check(got == want, f"--aligned {layer} {where}: {sum(len(p) for _, p in want)} aligned sentences for "
                           f"{len(want)} hits" + ("" if got == want else f" (got {str(got)[:300]})"))
    # Manatee's aligned corpora: `A within nld: B` — A (outside nld) wholly inside a sentence whose s_tuid class has
    # a B hit in nld (Manatee's within merge for a varying-length A), `!within`: not inside one
    import bisect
    def merge_in(hits, ranges):
        hits, ends, out, i, o = sorted(hits), [e for _, e in ranges], [], 0, 0
        while i < len(hits) and o < len(ranges):
            b, e = hits[i]
            if b < ranges[o][0]:
                i = bisect.bisect_left(hits, (ranges[o][0], -1), i); continue
            if e > ranges[o][1]:
                o = bisect.bisect_left(ends, e, o); continue
            out.append(hits[i]); i += 1
        return out
    def merge_not_in(hits, ranges):
        out, o = [], 0
        for b, e in sorted(hits):
            while o < len(ranges) and ranges[o][1] < e:
                o += 1
            if not (o < len(ranges) and ranges[o][0] <= b and e <= ranges[o][1]):
                out.append((b, e))
        return out
    def segments(lang, bpred):
        cls = {stuids[j] for j in range(n) if toks[j][1] == lang and bpred(j) and stuids[j]}
        return sorted({sentence_span(i) for i in range(n) if stuids[i] in cls and toks[i][1] != lang})
    def seq(pats, lang):   # (start, end) of a pattern outside `lang`
        out = set()
        for s0 in range(n):
            def rec(i, k):
                if k == len(pats):
                    if i > s0:
                        out.add((s0, i - 1))
                    return
                pred, lo, hi = pats[k]
                j = i
                for reps in range(hi + 1):
                    if reps >= lo:
                        rec(j, k + 1)
                    if j >= n or (pred and not pred(j)):
                        break
                    j += 1
            rec(s0, 0)
        return {h for h in out if toks[h[0]][1] != lang}
    F = lambda w: (lambda j: toks[j][0] == w)
    for q, pats, lang, bpred, neg in [
        ('[form="house"] within nld: [form="huis"]', [(F("house"), 1, 1)], "nld", F("huis"), False),
        ('[form="house"] !within nld: [form="huis"]', [(F("house"), 1, 1)], "nld", F("huis"), True),
        ('[form="the"] [] within ces: [form="dům"]', [(F("the"), 1, 1), (None, 1, 1)], "ces", F("dům"), False),
        ('[form="the"] []{0,3} [form="house"] within nld: [form="huis"]',
         [(F("the"), 1, 1), (None, 0, 3), (F("house"), 1, 1)], "nld", F("huis"), False),
        ('[form="is"] []* [form="old"] !within ces: [form="je"]',
         [(F("is"), 1, 1), (None, 0, 100), (F("old"), 1, 1)], "ces", F("je"), True),
    ]:
        hits = seq(pats, lang)
        want = sorted((merge_not_in if neg else merge_in)(hits, segments(lang, bpred)))
        out = run("MANATEE " + q, True, "--json", "--limit", "100000")
        try:
            got = sorted((h["match_start"], h["match_end"]) for h in json.loads(out)["result"]["hits"])
        except (ValueError, KeyError):
            got = out[:200]
        total = run("MANATEE " + q, True, "--count-only").split()
        check(got == want and total[-1:] == [str(len(want))],
              f"MANATEE {q}: {total[-1] if total else '?'} hits (Manatee {len(want)})" + ("" if got == want else f" got {got}"))
    r = subprocess.run([a.pando, "--count-only", d, 'MANATEE [form="house"] within xyz: [form="huis"]'],
                       capture_output=True, text=True)
    check("no aligned corpus 'xyz'" in r.stdout + r.stderr, "an unknown aligned corpus: refused with a message")

    r = subprocess.run([a.pando, d, '[form="house"]', "--aligned", "form"], capture_output=True, text=True)
    check(r.returncode == 0, "--aligned over any token attribute (form) runs")
    r = subprocess.run([a.pando, d, '[form="house"]', "--aligned", "nosuch"], capture_output=True, text=True)
    check("not an equivalence layer" in r.stdout + r.stderr, "--aligned nosuch: refused with a message")

    # display: a pair across the filler text is a pair (source | target, each with its own context, since
    # 2026-10-28), never one hit stretching over the filler
    q = 'a:[form="the"] =:tuid b:[form="het"]'
    pairs = [(i, j) for i in range(n) for j in range(n) if i != j and toks[i][0] == "the" and toks[j][0] == "het"
             and toks[i][2] & toks[j][2]]
    far = [ij for ij in pairs if any(toks[k][1] == "xxx" for k in range(min(ij), max(ij)))]
    out = run(q, True, "--limit", "1000")
    lines = [l for l in out.splitlines() if "<!" in l]
    check(len(far) > 0 and out.count("[pair ") == len(pairs) and len(lines) == 2 * len(pairs)
          and not any("filler" in l[l.index("<!"):l.rindex("!>")] for l in lines),
          f"{q}: {out.count('[pair ')} pairs shown (reference {len(pairs)}, {len(far)} across the filler)")
    js = json.loads(run(q, True, "--json", "--limit", "1000"))
    sides = [p[k].get("context", {}) for p in js["result"].get("pairs", []) for k in ("source", "target")]
    check(len(sides) == 2 * len(pairs) and not any("filler" in c.get("match", "") for c in sides),
          f"{q}: JSON pairs, each side's match without the filler")

    if a.server:   # the XML fragment flexicorp shows: the runs, a <gap/> between, not the filler
        with socket.socket() as sk:
            sk.bind(("127.0.0.1", 0))
            port = sk.getsockname()[1]
        srv = subprocess.Popen([a.server, d, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            r = None
            for _ in range(100):
                try:
                    body = {"query": q, "limit": 1000, "fragment": True}
                    req = urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps(body).encode())
                    r = json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]
                    break
                except (ConnectionError, urllib.error.URLError):
                    time.sleep(0.1)
            body = {"query": '[form="house"]', "limit": 1000, "aligned": "s_tuid", "aligned_where": "text_lang=nld"}
            req = urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps(body).encode())
            ra = json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]
            cli = json.loads(run('[form="house"]', True, "--json", "--limit", "1000", "--aligned", "s_tuid",
                                 "--aligned-where", "text_lang=nld"))["result"]["hits"]
            check([h.get("aligned") for h in ra["hits"]] == [h.get("aligned") for h in cli] and len(cli) > 0,
                  "server /query \"aligned\": the same aligned sentences as the CLI")
            frags = [p[k].get("fragment", "") for p in (r or {}).get("pairs", []) for k in ("source", "target")]
            # (each side's fragment is its own context: filler next to a token may be in it, nothing lies between)
            check(len(frags) == 2 * len(pairs) and all(frags),
                  f"{q}: server pairs, a fragment per side ({len(frags)} fragments)")

            # pages of pairs (2026-10-29): the class join gives the pairs up to the page's end, the total counted when
            # asked (and the distinct sources with it); without a total the page alone, the total inexact while more
            # follow. Every page as the CLI's full list, for `=:` and `with`, a narrow side either first or second.
            def post(body):
                req = urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps(body).encode())
                return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]
            for pq in ['a:[form="house"] =:tuid b:[]', 'a:[] =:tuid b:[form="huis"]', 'a:[] =:s_tuid b:[]',
                       'a:[form="is"] with b:[] :: a.tuid = b.tuid', 'a:[] with b:[form="huis"] :: a.tuid = b.tuid']:
                full = with_pairs(pq, True)
                for total in (True, False):
                    pages, off, page = [], 0, None
                    while off < len(full) + 4:
                        page = post({"query": pq, "offset": off, "limit": 4, "total": total})
                        pages += [(p["source"]["tokens"][0]["pos"], p["target"]["tokens"][0]["pos"])
                                  for p in page["pairs"]]
                        if total:
                            check(page["page"]["total"] == len(full) and page["page"]["total_exact"],
                                  f"{pq}: server page at {off}, total {page['page']['total']} (CLI {len(full)})")
                            check(page["page"]["total_sources"] == len({s for s, _ in full}),
                                  f"{pq}: server page at {off}, {page['page']['total_sources']} sources")
                        elif off + 4 < len(full):   # inexact, or counted all the same (the narrow side second)
                            check(not page["page"]["total_exact"] or page["page"]["total"] == len(full),
                                  f"{pq}: no total asked, page at {off}: inexact or the total")
                        off += 4
                    check(pages == full and len(full) > 0,
                          f"{pq}: server pages{' with total' if total else ''} as the CLI's {len(full)} pairs")
        finally:
            srv.terminate()
            srv.wait()

    # attributes declared multivalue (as TEITOK settings declare a `tuid` that can hold several ids): a token aligned
    # with two words (`w1|w2`), a segment merging two (`s1|s2`), a segment inside a segment; every path the same
    # (sentence-like regions as `seg` / `seg_tuid`, as tico19 has them: JSONL `s` regions follow their own tokens)
    small = [{"type": "header", "version": 2, "positional": ["form", "tuid"], "structural": ["seg", "text"],
              "nested": ["seg"], "multivalue": ["tuid", "seg_tuid"]}]
    words = [("big", "w1|w2"), ("house", "w3"), ("groot", "w1"), ("huis", "w3|w4"), ("x", "w2"), ("y", "w4"),
             ("z", "_")]
    small += [{"type": "token", "form": f, "tuid": t} for f, t in words]
    regions = [("seg", 0, 1, {"tuid": "s1|s2"}), ("seg", 2, 3, {"tuid": "s1"}), ("seg", 4, 6, {"tuid": "s2"}),
               ("seg", 5, 6, {"tuid": "s3"}), ("text", 0, 1, {"lang": "eng"}), ("text", 2, 6, {"lang": "nld"})]
    small += [{"type": "region", "struct": st, "start_pos": a0, "end_pos": b0, "attrs": at}
              for st, a0, b0, at in regions]
    src2, d2 = os.path.join(tmp, "small.jsonl"), os.path.join(tmp, "small")
    with open(src2, "w", encoding="utf-8") as f:
        for e in small:
            f.write(json.dumps(e) + "\n")
    p = subprocess.run([a.pando_index, "--format", "jsonl", src2, d2], capture_output=True, text=True)
    def cnt(q, fast=True):
        env = dict(os.environ, PANDO_FASTPATH="on" if fast else "off")
        r = subprocess.run([a.pando, "--count-only", d2, q], capture_output=True, text=True, env=env)
        return r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else "ERR " + r.stderr[-100:]
    tids = [set(t.split("|")) - {"_"} for _, t in words]
    tok_pairs = lambda fa, fb: sum(1 for i in range(len(words)) for j in range(len(words))
                                   if i != j and fa(i) and fb(j) and tids[i] & tids[j])
    # sentence classes per token: every sentence holding it (nested), each id of a value
    sids = [set() for _ in words]
    for st, a0, b0, at in regions:
        if st == "seg":
            for i in range(a0, b0 + 1):
                sids[i] |= set(at["tuid"].split("|"))
    s_pairs = lambda fa, fb: sum(1 for i in range(len(words)) for j in range(len(words))
                                 if i != j and fa(i) and fb(j) and sids[i] & sids[j])
    ENG = lambda i: i < 2
    NLD = lambda i: i >= 2
    for q, want in [('a:[] =:tuid b:[]', tok_pairs(lambda i: True, lambda i: True)),
                    ('a:[form="big"] =:tuid b:[]', tok_pairs(lambda i: i == 0, lambda i: True)),
                    ('a:[text_lang="eng"] with b:[text_lang="nld"] :: a.tuid = b.tuid', tok_pairs(ENG, NLD)),
                    ('e:[text_lang="eng"]; x:[text_lang="nld"] :: e.tuid = x.tuid',
                     sum(1 for j in range(len(words)) if NLD(j) and any(tids[i] & tids[j] for i in range(2)))),
                    ('a:[] =:seg_tuid b:[]', s_pairs(lambda i: True, lambda i: True)),
                    ('a:[text_lang="eng"] =:seg_tuid b:[text_lang="nld"]', s_pairs(ENG, NLD))]:
        fast, slow = cnt(q), cnt(q, False)
        check(fast == slow == str(want), f"multivalue ids: {q}: {fast} (generic {slow}, reference {want})")

    print("class join: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
