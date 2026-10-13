#!/usr/bin/env python3
"""Restricted `count by` (QUERY-PERF-TODO P4.1b, P4.13 part A, P4.14, P4.15).

A synthetic corpus with known metadata (texts: country, year; speeches: gender, party; sentences:
sentiment) is indexed, and every `count by` query below is checked three ways:
  1. against counts computed here in Python from the generated data (ground truth);
  2. against the per-hit routes (PANDO_AGG_MASK=off, PANDO_HAVING_MASK=off, PANDO_FASTPATH=off);
  3. split over threads (--threads 4 with PANDO_PARTITION_MIN forcing tiny partitions).

  test/restricted_count_test.py --pando-index build/pando-index --pando build/pando
"""
import argparse
import json
import os
import random
import subprocess
import sys
import tempfile

UPOS = ["NOUN", "VERB", "ADJ", "DET", "PRON", "PUNCT"]
LEMMAS = {"NOUN": ["crisis", "government", "law", "woman"], "VERB": ["be", "say"], "ADJ": ["new", "big"],
          "DET": ["the", "a"], "PRON": ["we", "they"], "PUNCT": ["."]}
COUNTRIES = ["NL", "GB", "LV"]
GENDERS = ["F", "M"]
PARTIES = ["Coalition", "Opposition"]
SENTI = ["Positive", "Negative", "Neutral"]


def generate(rng):
    """Texts > speeches > sentences > tokens, with metadata; returns (jsonl lines, token records).
    Region events are written when the region closes (after its tokens), as pando's JSONL reader expects."""
    lines = [json.dumps({"type": "header", "version": 2, "positional": ["form", "lemma", "upos"],
                         "structural": ["text", "u", "s"]})]
    toks, pos, nsent = [], 0, 0

    def region(struct, a, b, attrs):
        lines.append(json.dumps({"type": "region", "struct": struct, "start_pos": a, "end_pos": b, "attrs": attrs}))

    for t in range(12):
        country, year = COUNTRIES[t % 3], str(2014 + t % 6)
        t_start = pos
        for _ in range(rng.randint(2, 4)):
            gender, party = rng.choice(GENDERS), rng.choice(PARTIES)
            u_start = pos
            for _ in range(rng.randint(2, 5)):
                senti = rng.choice(SENTI)
                s_start = pos
                for _ in range(rng.randint(3, 9)):
                    upos = rng.choice(UPOS)
                    lemma = rng.choice(LEMMAS[upos])
                    lines.append(json.dumps({"type": "token", "form": lemma, "lemma": lemma, "upos": upos}))
                    toks.append(dict(pos=pos, upos=upos, lemma=lemma, country=country, year=year,
                                     gender=gender, party=party, senti=senti, s=nsent))
                    pos += 1
                region("s", s_start, pos - 1, {"senti": senti})
                nsent += 1
            region("u", u_start, pos - 1, {"gender": gender, "party": party})
        region("text", t_start, pos - 1, {"country": country, "year": year})
    return lines, toks


def counts(rows, key):
    out = {}
    for r in rows:
        k = key(r)
        out[k] = out.get(k, 0) + 1
    return out


def parse_count(text):
    """`count by` output → {value or tuple of values: count}; the header line names the columns."""
    out, ncols = {}, None
    for line in text.splitlines():
        parts = line.split("\t")
        if "count" in parts and ncols is None:
            ncols = parts.index("count")
            continue
        if ncols is None or line.startswith("Total:") or len(parts) <= ncols:
            continue
        key = tuple(parts[:ncols]) if ncols > 1 else parts[0]
        out[key] = int(parts[ncols])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--pando", required=True)
    o = ap.parse_args()
    rng = random.Random(7)
    lines, toks = generate(rng)
    sents_with = lambda lem: {t["s"] for t in toks if t["lemma"] == lem}   # noqa: E731

    # (query, ground truth from the generated data)
    cases = [
        ('[upos="NOUN"] within text_country="NL"; count by match.u_gender',
         counts([t for t in toks if t["upos"] == "NOUN" and t["country"] == "NL"], lambda t: t["gender"])),
        ('[upos="NOUN"] :: match.text_country="NL"; count by match.u_gender',
         counts([t for t in toks if t["upos"] == "NOUN" and t["country"] == "NL"], lambda t: t["gender"])),
        ('[lemma="crisis"] within text_country="GB"; count by match.s_senti',
         counts([t for t in toks if t["lemma"] == "crisis" and t["country"] == "GB"], lambda t: t["senti"])),
        ('[upos="ADJ"] within text_country="NL"; count by lemma',
         counts([t for t in toks if t["upos"] == "ADJ" and t["country"] == "NL"], lambda t: t["lemma"])),
        ('[upos="ADJ"] within text_country="NL"; count by upos',
         counts([t for t in toks if t["upos"] == "ADJ" and t["country"] == "NL"], lambda t: t["upos"])),
        ('[upos="NOUN"] :: match.u_gender="F"; count by match.u_party',
         counts([t for t in toks if t["upos"] == "NOUN" and t["gender"] == "F"], lambda t: t["party"])),
        ('[upos="NOUN"] :: match.text_year>="2017"; count by match.text_country',
         counts([t for t in toks if t["upos"] == "NOUN" and t["year"] >= "2017"], lambda t: t["country"])),
        ('[upos="NOUN"] :: match.text_country!="GB"; count by match.text_year',
         counts([t for t in toks if t["upos"] == "NOUN" and t["country"] != "GB"], lambda t: t["year"])),
        ('[upos="NOUN"] :: match.text_country!="GB" & match.u_gender="F"; count by match.text_year',
         counts([t for t in toks if t["upos"] == "NOUN" and t["country"] != "GB" and t["gender"] == "F"],
                lambda t: t["year"])),
        ('[upos="NOUN"] within s having [lemma="crisis"]; count by match.text_country',
         counts([t for t in toks if t["upos"] == "NOUN" and t["s"] in sents_with("crisis")], lambda t: t["country"])),
        ('[upos="NOUN" | upos="ADJ"] within text_country="LV"; count by match.u_gender',
         counts([t for t in toks if t["upos"] in ("NOUN", "ADJ") and t["country"] == "LV"], lambda t: t["gender"])),
        ('[upos="NOUN"] within text_country="XX"; count by match.u_gender', {}),
        ('[lemma="woman"] within text_country="NL"; count by match.u_gender, match.text_year',
         counts([t for t in toks if t["lemma"] == "woman" and t["country"] == "NL"],
                lambda t: (t["gender"], t["year"]))),
    ]

    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        src, idx = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
        with open(src, "w") as fh:
            fh.write("\n".join(lines) + "\n")
        subprocess.run([o.pando_index, "--format", "jsonl", src, idx], check=True, capture_output=True)

        def run(q, env_extra, threads):
            env = dict(os.environ, **env_extra)
            r = subprocess.run([o.pando, "--threads", str(threads), idx, q], capture_output=True, text=True, env=env)
            if r.returncode != 0:
                fails.append(f"{q} [{env_extra} t{threads}]: exit {r.returncode}: {r.stderr.strip()[:200]}")
            return r.stdout

        variants = [("default", {}, 1),
                    ("old routes", {"PANDO_AGG_MASK": "off", "PANDO_HAVING_MASK": "off"}, 1),
                    ("fast paths off", {"PANDO_FASTPATH": "off"}, 1),
                    ("partitioned", {"PANDO_PARTITION_MIN": "16"}, 4)]
        for q, truth in cases:
            outs = {name: run(q, env, t) for name, env, t in variants}
            for name, out in outs.items():
                got = parse_count(out)
                if got != truth:
                    fails.append(f"{q} [{name}]: got {got}, want {truth}")
            if len({outs[n] for n in outs}) != 1:
                fails.append(f"{q}: outputs differ between variants")

    for f in fails:
        print("FAIL", f)
    print(f"{len(cases)} queries x {4} variants, {len(fails)} failures")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
