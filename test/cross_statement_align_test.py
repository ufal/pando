#!/usr/bin/env python3
"""Alignment filters on an earlier statement's label (`eo:[…]; ang:[…] :: eo.s_tuid =
ang.s_tuid & eo.upos = ang.upos`): the hits agree with a brute-force reference.

Builds a small vertical corpus (four languages; sentence-level `tuid` on the `s`
region, some sentences unaligned (`_`), some covering two source sentences (`t3|t4`),
and the same id copied onto every token) and compares, per query, the matched tokens
of the second statement with the ones computed here from the vertical file:

  * a region attribute (`s_tuid`) compares components (`t3|t4` aligns with `t3`),
    missing values (`_`) never align; with few source values (looked up one by one)
    and with many (one pass over the distinct values);
  * a token attribute (`upos`, `lemma`, the token-level `tuid`) compares whole values
    (it is not declared multivalue: `t3|t4` does not align with `t3`), missing values
    never align;
  * several filters at once, a target without other conditions, a source with no hits.

  test/cross_statement_align_test.py --pando build/pando --pando-index build/pando-index
"""

import argparse
import os
import random
import re
import subprocess
import sys
import tempfile

FAILS = []

LANGS = ["eo", "ang", "nl", "cs"]
UPOS = ["NOUN", "VERB", "ADJ", "DET", "ADP", "ADV", "PRON", "PUNCT"]


def build_corpus(path, nsent):
    """Write the vertical file; return the tokens as dicts (in corpus order)."""
    rnd = random.Random(11)
    lemmas = {u: [f"{u.lower()}{i}" for i in range(12)] for u in UPOS}
    lemmas["VERB"][0] = "skribi"
    toks = []
    with open(path, "w", encoding="utf-8") as f:
        f.write("<!-- positional-attributes: form lemma upos tuid -->\n")
        for lang in LANGS:
            f.write(f'<text langcode="{lang}" id="{lang}">\n')
            i = 0
            while i < nsent:
                tu = f"t{i}"
                if i % 13 == 5:
                    tu = "_"
                elif lang == "nl" and i % 17 == 3 and i + 1 < nsent:
                    tu = f"t{i}|t{i + 1}"
                    i += 1
                f.write(f'<s tuid="{tu}">\n')
                for _ in range(rnd.randint(3, 9)):
                    u = rnd.choice(UPOS)
                    lemma = rnd.choice(lemmas[u])
                    form = f"w{len(toks)}"
                    f.write(f"{form}\t{lemma}\t{u}\t{tu}\n")
                    toks.append(dict(form=form, lemma=lemma, upos=u, tuid=tu, s_tuid=tu, lang=lang))
                f.write("</s>\n")
                i += 1
            f.write("</text>\n")
    return toks


def components(v):
    return set() if v in ("", "_") else set(v.split("|"))


def expected(toks, src, tgt, filters):
    """Forms of the target tokens that pass every (attr, kind) filter against the source hits."""
    sel = [t for t in toks if src(t)]
    out = []
    for t in toks:
        if not tgt(t):
            continue
        ok = True
        for attr, kind in filters:
            if kind == "region":   # components; missing values never align
                vals = set().union(*(components(s[attr]) for s in sel)) if sel else set()
                ok = ok and bool(components(t[attr]) & vals)
            else:                  # a token attribute: whole values; missing never align
                ok = ok and t[attr] not in ("", "_") and t[attr] in {s[attr] for s in sel}
        if ok:
            out.append(t["form"])
    return out


def run(pando, corpus, query):
    p = subprocess.run([pando, "--limit", "1000000", corpus, query], capture_output=True, text=True)
    if "error" in (p.stdout + p.stderr).lower():
        return None, (p.stdout + p.stderr).strip()[:300]
    return re.findall(r"<! (w\d+)", p.stdout), None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        vrt = os.path.join(tmp, "aligned.vrt")
        corpus = os.path.join(tmp, "idx")
        toks = build_corpus(vrt, 300)
        subprocess.run([args.pando_index, "--format", "vertical", vrt, corpus],
                       check=True, capture_output=True)

        lang = lambda code: (lambda t: t["lang"] == code)
        eo_sk = lambda t: t["lang"] == "eo" and t["lemma"] == "skribi"
        eo_adj = lambda t: t["lang"] == "eo" and t["upos"] == "ADJ"   # most sentence ids
        nl_sk = lambda t: t["lang"] == "nl" and t["lemma"] == "skribi"
        cases = [
            ('eo:[text_langcode="eo" & lemma="skribi"]; ang:[text_langcode="ang"] :: eo.s_tuid = ang.s_tuid',
             eo_sk, lang("ang"), [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & lemma="skribi"]; ang:[text_langcode="ang"] :: ang.s_tuid = eo.s_tuid',
             eo_sk, lang("ang"), [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & lemma="skribi"]; nl:[text_langcode="nl"] :: eo.s_tuid = nl.s_tuid',
             eo_sk, lang("nl"), [("s_tuid", "region")]),
            ('nl:[text_langcode="nl" & lemma="skribi"]; eo:[text_langcode="eo"] :: nl.s_tuid = eo.s_tuid',
             nl_sk, lang("eo"), [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & lemma="skribi"]; x:[] :: eo.s_tuid = x.s_tuid',
             eo_sk, lambda t: True, [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & upos="ADJ"]; ang:[text_langcode="ang" & lemma="noun3"] :: eo.s_tuid = ang.s_tuid',
             eo_adj, lambda t: t["lang"] == "ang" and t["lemma"] == "noun3", [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & upos="ADJ"]; nl:[text_langcode="nl" & upos="NOUN"] :: eo.s_tuid = nl.s_tuid',
             eo_adj, lambda t: t["lang"] == "nl" and t["upos"] == "NOUN", [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & lemma="skribi"]; ang:[text_langcode="ang"] :: eo.upos = ang.upos',
             eo_sk, lang("ang"), [("upos", "token")]),
            ('eo:[text_langcode="eo" & upos="ADJ"]; ang:[text_langcode="ang"] :: eo.lemma = ang.lemma',
             eo_adj, lang("ang"), [("lemma", "token")]),
            ('eo:[text_langcode="eo" & lemma="skribi"]; nl:[text_langcode="nl"] :: eo.tuid = nl.tuid',
             eo_sk, lang("nl"), [("tuid", "token")]),
            ('eo:[text_langcode="eo" & lemma="skribi" & s_tuid != "_"]; ang:[text_langcode="ang"] '
             ':: eo.s_tuid = ang.s_tuid & eo.upos = ang.upos',
             lambda t: eo_sk(t) and t["s_tuid"] != "_", lang("ang"), [("s_tuid", "region"), ("upos", "token")]),
            ('eo:[text_langcode="eo" & lemma="nosuchlemma"]; ang:[text_langcode="ang"] :: eo.s_tuid = ang.s_tuid',
             lambda t: False, lang("ang"), [("s_tuid", "region")]),
            ('eo:[text_langcode="eo" & lemma="nosuchlemma"]; ang:[text_langcode="ang"] :: eo.upos = ang.upos',
             lambda t: False, lang("ang"), [("upos", "token")]),
        ]
        for query, src, tgt, filters in cases:
            exp = expected(toks, src, tgt, filters)
            got, err = run(args.pando, corpus, query)
            if err:
                FAILS.append(f"{query}: {err}")
                print("FAIL:", query, err)
            elif got != exp:
                FAILS.append(query)
                print(f"FAIL: {query}\n   expected {len(exp)}, got {len(got)}; "
                      f"missing {sorted(set(exp) - set(got))[:5]}, extra {sorted(set(got) - set(exp))[:5]}")
            else:
                print(f"ok   {len(exp):5d}  {query[:110]}")

    if FAILS:
        print(f"{len(FAILS)} failure(s)")
        sys.exit(1)
    print("all cross-statement alignment queries agree with the reference")


if __name__ == "__main__":
    main()
