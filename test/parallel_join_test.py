#!/usr/bin/env python3
"""Aligned `with` queries: the joins agree with a brute-force reference, and
pando-server /query returns the pairs (paged, from one cached result).

Builds a small vertical corpus (several languages, sentence-level `tuid` on the
`s` region and copied onto every token, as TEITOK parallel corpora have it),
then for each query compares the set of (source, target) match starts

  * from `pando --api` and from pando-server /query (all pages concatenated)
  * with the pairs computed here from the vertical file.

The corpus is big enough that the target side of a selective query is counted
and hash-joined instead of pulled by alignment value, so both routes of
execute_parallel are exercised; a `with b:<s …>` query takes the region join.

  test/parallel_join_test.py --pando build/pando --pando-index build/pando-index \\
      --server build/pando-server
"""

import argparse
import json
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)
    return cond


LANGS = ["English", "Dutch", "Czech", "German"]
UPOS = ["NOUN"] * 6 + ["VERB"] * 4 + ["ADV"] * 2 + ["DET"] * 3 + ["ADP"] * 3 + ["PUNCT"] * 3


def build_vertical(path, docs=6, sents=40, words=12, seed=7):
    """Tokens as dicts; the same tuid for the same sentence in every language."""
    rnd = random.Random(seed)
    toks = []
    sentences = []   # (start, end, lang, tuid)
    with open(path, "w") as out:
        out.write("<!-- positional-attributes: word lemma upos tuid -->\n")
        for lang in LANGS:
            code = lang[:3].lower()
            for d in range(docs):
                out.write(f'<text id="xmlfiles/{code}/d{d}.xml" lang="{lang}">\n')
                for s in range(sents):
                    tu = f"d{d}s{s}"
                    out.write(f'<s tuid="{tu}">\n')
                    start = len(toks)
                    for _ in range(words):
                        if lang == "English" and rnd.random() < 0.15:
                            lemma = rnd.choice(["always", "never", "often"])
                        else:
                            lemma = f"{code}{rnd.randrange(400)}"
                        upos = rnd.choice(UPOS)
                        toks.append({"lemma": lemma, "upos": upos, "lang": lang, "tuid": tu})
                        out.write(f"{lemma}\t{lemma}\t{upos}\t{tu}\n")
                    sentences.append((start, len(toks) - 1, lang, tu))
                    out.write("</s>\n")
                out.write("</text>\n")
    return toks, sentences


def ref_tokens(toks, pred):
    return [i for i, t in enumerate(toks) if pred(t)]


def ref_pairs_tok(toks, src_pred, tgt_pred):
    by_tu = {}
    for j in ref_tokens(toks, tgt_pred):
        by_tu.setdefault(toks[j]["tuid"], []).append(j)
    return {(i, j) for i in ref_tokens(toks, src_pred) for j in by_tu.get(toks[i]["tuid"], [])}


def ref_pairs_sent(toks, sentences, src_pred, lang):
    by_tu = {}
    for (start, end, lg, tu) in sentences:
        if lg == lang:
            by_tu.setdefault(tu, []).append(start)
    return {(i, s) for i in ref_tokens(toks, src_pred) for s in by_tu.get(toks[i]["tuid"], [])}


def cli_pairs(pando, index, q):
    out = subprocess.run([pando, "--api", "--limit", "100000", index, q],
                         capture_output=True, text=True, check=True).stdout
    res = json.loads(out)["result"]
    return [(p["source"]["match_start"], p["target"]["match_start"]) for p in res.get("pairs", [])]


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def post(base, path, body):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    a = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="pando-parallel-")
    vrt = os.path.join(tmp, "c.vrt")
    index = os.path.join(tmp, "idx")
    toks, sentences = build_vertical(vrt)
    subprocess.run([a.pando_index, "--format", "vertical", vrt, index],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    eng = lambda t: t["lang"] == "English"
    cases = [
        # selective target: counted, then hash-joined
        ('a:[lemma="always" & text_lang="English"] with b:[upos="ADV" & text_lang="Dutch"] :: a.tuid = b.tuid',
         ref_pairs_tok(toks, lambda t: t["lemma"] == "always" and eng(t),
                       lambda t: t["upos"] == "ADV" and t["lang"] == "Dutch")),
        # a whole language on the target side
        ('a:[lemma="never"] with b:[text_lang="Czech"] :: a.tuid = b.tuid',
         ref_pairs_tok(toks, lambda t: t["lemma"] == "never",
                       lambda t: t["lang"] == "Czech")),
        # rare source, unrestricted target: pulled by tuid
        ('a:[lemma="eng7"] with b:[] :: a.tuid = b.tuid',
         ref_pairs_tok(toks, lambda t: t["lemma"] == "eng7", lambda t: True)),
        # region target and region attribute: the region hash join
        ('a:[lemma="often"] with b:<s text_lang="German"> :: a.s_tuid = b.s_tuid',
         ref_pairs_sent(toks, sentences, lambda t: t["lemma"] == "often", "German")),
    ]

    port = free_port()
    base = f"http://127.0.0.1:{port}"
    srv = subprocess.Popen([a.server, index, str(port), "4"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(200):
            try:
                urllib.request.urlopen(base + "/health", timeout=1)
                break
            except OSError:
                time.sleep(0.05)
        for q, want in cases:
            check(len(want) > 0, f"reference has pairs: {q}")
            got = cli_pairs(a.pando, index, q)
            check(set(got) == want, f"CLI pairs == reference ({len(got)} vs {len(want)}): {q}")
            check(got == sorted(got), f"CLI pairs in corpus order of source, then target: {q}")
            # server: pages of 7 until done
            pages, off, total = [], 0, None
            for _ in range(10000):
                r = post(base, "/query", {"query": q, "offset": off, "limit": 7, "total": True})["result"]
                check(r.get("parallel") is True, f"/query marks the result parallel: {q}")
                total = r["page"]["total"]
                got_page = [(p["source"]["match_start"], p["target"]["match_start"]) for p in r["pairs"]]
                pages += got_page
                off += 7
                if not got_page or off >= total:
                    break
            check(total == len(want), f"/query page.total == pairs ({total} vs {len(want)}): {q}")
            check(pages == got, f"/query pages == CLI pairs, in order: {q}")
            check(not r.get("hits"), f"/query parallel: hits stay empty: {q}")
        # the second page of a parallel query comes from the cached pair set
        q = cases[0][0]
        st0 = json.loads(urllib.request.urlopen(base + "/health", timeout=5).read())
        post(base, "/query", {"query": q, "offset": 14, "limit": 7, "total": True})
        st1 = json.loads(urllib.request.urlopen(base + "/health", timeout=5).read())
        c0 = (st0.get("cache") or {}).get("hits")
        c1 = (st1.get("cache") or {}).get("hits")
        if check(c0 is not None and c1 is not None, "/health reports the result cache"):
            check(c1 > c0, f"a later page is a cache hit ({c0} -> {c1})")
    finally:
        srv.terminate()
        srv.wait()

    if FAILS:
        print(f"{len(FAILS)} failure(s)")
        sys.exit(1)
    print(f"parallel joins: {len(cases)} queries OK")


if __name__ == "__main__":
    main()
