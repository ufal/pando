#!/usr/bin/env python3
"""The class join for equivalence layers (executor_classes.cpp): `a:[A] =:X b:[B]` walks each class once.

    class_join_test.py --pando build/pando --pando-index build/pando-index [--server build/pando-server]

Generates a small parallel corpus (self-authored, seeded): texts in three languages (`text_lang`), each token with a
`tuid` (multivalue: some tokens align to two units, some pairs share two units, some tokens are unaligned `_`),
indexes it as JSONL and declares `relation.tuid=equivalence,attr=tuid`. For each query it requires:
  * the class join to be taken (plan path class_join) where the shape allows it;
  * the same total and the same hits as the generic expansion (PANDO_FASTPATH=off), and every hit and pages at an
    offset in hit order: by the first token, then the second;
  * the total to equal a brute-force count over the generated tokens (pairs of distinct tokens sharing a unit,
    each pair once however many units they share);
  * a pair whose tokens lie far apart (across an unaligned 120-token text) to be shown as runs with an elision,
    in the KWIC line, the JSON context and (with --server) the XML fragment, never with what lies between;
  * `a:[A] with b:[B] :: a.tuid = b.tuid` to take the class join as well, with the pairs of a brute-force reference
    (a token with itself included) and of the general joins;
  * `count by` over fields of either token to give the buckets of a brute-force tally, fast and generic.
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
        if t == 2:   # an unaligned text in between, longer than the display gap (match_display.h)
            start = len(toks)
            toks += [("filler", "xxx", set())] * 120
            texts.append((start, len(toks) - 1, "xxx"))
    events = [{"type": "header", "version": 2, "positional": ["form", "tuid"], "multivalue": ["tuid"],
               "structural": ["s", "text"]}]
    # (JSONL v2: an `s` region follows its own tokens)
    for k, (a, b, lang) in enumerate(texts):
        events += [{"type": "token", "form": f, "tuid": "|".join(sorted(u)) or "_"} for f, _, u in toks[a:b + 1]]
        events.append({"type": "region", "struct": "s", "start_pos": a, "end_pos": b, "attrs": {"id": f"s{k}"}})
    for k, (a, b, lang) in enumerate(texts):
        events.append({"type": "region", "struct": "text", "start_pos": a, "end_pos": b,
                       "attrs": {"lang": lang, "id": f"t{k}"}})
    return events, toks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", help="pando-server: also check the XML fragment of a far-apart pair")
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

    # `a:[A] with b:[B] :: a.tuid = b.tuid` goes through the class join too: the pairs (a token with itself
    # included, as `with` has it) as the general joins give them, a `::` filter on the source after `with` included
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
        ref = sorted((i, j) for i in range(n) for j in range(n) if fa(i) and fb(j) and toks[i][2] & toks[j][2])
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

    # display: a pair across the filler text shows the runs around its tokens with an elision, not the filler
    q = 'a:[form="the"] =:tuid b:[form="het"]'
    pairs = [(i, j) for i in range(n) for j in range(n) if i != j and toks[i][0] == "the" and toks[j][0] == "het"
             and toks[i][2] & toks[j][2]]
    far = [ij for ij in pairs if any(toks[k][1] == "xxx" for k in range(min(ij), max(ij)))]
    out = run(q, True, "--limit", "1000")
    lines = [l for l in out.splitlines() if "<!" in l]
    # the match part: from the first highlighted token to the last
    middles = [l[l.index("<!"):l.rindex("!>")] for l in lines]
    elided = sum("\u2026" in m for m in middles)
    check(len(far) > 0 and len(lines) == len(pairs) and not any("filler" in m for m in middles)
          and elided >= len(far),
          f"{q}: KWIC lines elide what lies between ({elided} elided, {len(far)} of {len(lines)} across the filler)")
    js = json.loads(run(q, True, "--json", "--limit", "1000"))
    ctxs = [h.get("context", {}) for h in js["result"]["hits"]]
    check(len(ctxs) == len(pairs) and not any("filler" in c.get("match", "") for c in ctxs),
          f"{q}: JSON context match without the filler")

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
            frags = [h.get("fragment", "") for h in (r or {}).get("hits", [])]
            check(len(frags) == len(pairs) and not any('form="filler"' in f or ">filler<" in f for f in frags)
                  and sum("<gap/>" in f for f in frags) >= len(far),
                  f"{q}: server fragments without the filler ({sum('<gap/>' in f for f in frags)} with <gap/>)")
        finally:
            srv.terminate()
            srv.wait()

    print("class join: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
