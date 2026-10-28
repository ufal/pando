#!/usr/bin/env python3
"""CoNLL-U read by pando-index, and the same data as JSONL: the same structures, attributes and relation layers.

    conllu_jsonl_parity_test.py --pando build/pando --pando-index build/pando-index

A self-authored CoNLL-U file with a bare `# newpar` and a bare `# newdoc` (both valid CoNLL-U), `# meta::` document
metadata, enhanced dependencies (DEPS) and a quote token; the JSONL is what a converter (flexencoder --conllu) writes
for it: tokens with `head` and `deps` (CoNLL-U DEPS, numbered within the sentence), `s` regions after their tokens,
`doc` / `par` regions.

Requires:
  * CoNLL-U: a `par` region for each `# newpar` (id `_` when it has none), a `doc` for a bare `# newdoc`,
    `# meta::genre` as doc_genre, a translation `# text_en = …` as s_text_en;
  * JSONL: `deps` on token events gives the relation layer `deps` as DEPS in CoNLL-U does (declaration and edges),
    a quote token read back as `"`;
  * both: the same counts.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

# (id, form, lemma, upos, head, deprel, deps)
SENTS = [
    [(1, "Anna", "Anna", "PROPN", 2, "nsubj", "2:nsubj|4:nsubj"), (2, "came", "come", "VERB", 0, "root", "0:root"),
     (3, "and", "and", "CCONJ", 4, "cc", "4:cc"), (4, "sang", "sing", "VERB", 2, "conj", "2:conj:and"),
     (5, '"', '"', "PUNCT", 2, "punct", "2:punct")],
    [(1, "Bob", "Bob", "PROPN", 2, "nsubj", "2:nsubj"), (2, "left", "leave", "VERB", 0, "root", "0:root")],
    [(1, "It", "it", "PRON", 2, "nsubj", "2:nsubj"), (2, "rained", "rain", "VERB", 0, "root", "0:root")],
]
# (sentence index, comment lines before it)
COMMENTS = {0: ["# newdoc id = d1", "# meta::genre = news", "# newpar"], 1: ["# newpar id = p2", "# text_en = Bob left."],
            2: ["# newdoc", "# newpar"]}


def write_conllu(path):
    with open(path, "w", encoding="utf-8") as f:
        for i, s in enumerate(SENTS):
            for c in COMMENTS.get(i, []):
                f.write(c + "\n")
            f.write(f"# sent_id = s{i + 1}\n")
            for tid, form, lemma, upos, head, rel, deps in s:
                f.write("\t".join([str(tid), form, lemma, upos, "_", "_", str(head), rel, deps, "_"]) + "\n")
            f.write("\n")


def write_jsonl(path):
    ev, pos = [], 0
    docs, pars, sents = [], [], []
    doc_start, par_start = None, None
    for i, s in enumerate(SENTS):
        for c in COMMENTS.get(i, []):
            if c.startswith("# newdoc"):
                if doc_start is not None:
                    docs[-1][1] = pos - 1
                docs.append([pos, None, {"id": c.split("=")[1].strip() if "=" in c else "_", "lang": "_",
                                         "langcode": "_"}])
                doc_start = pos
            elif c.startswith("# newpar"):
                if par_start is not None:
                    pars[-1][1] = pos - 1
                pars.append([pos, None, {"id": c.split("=")[1].strip() if "=" in c else "_"}])
                par_start = pos
            elif c.startswith("# meta::"):
                k, v = c[8:].split("=")
                docs[-1][2][k.strip()] = v.strip()
        start = pos
        for tid, form, lemma, upos, head, rel, deps in s:
            ev.append({"type": "token", "tok_id": str(tid), "head": head, "deps": deps, "form": form, "lemma": lemma,
                       "upos": upos, "xpos": "_", "feats": "_", "deprel": rel})
            pos += 1
        ev.append({"type": "region", "struct": "s", "start_pos": start, "end_pos": pos - 1,
                   "attrs": {"sent_id": f"s{i + 1}"}})
    docs[-1][1] = pars[-1][1] = pos - 1
    for a, b, attrs in docs:
        ev.append({"type": "region", "struct": "doc", "start_pos": a, "end_pos": b, "attrs": attrs})
    for a, b, attrs in pars:
        ev.append({"type": "region", "struct": "par", "start_pos": a, "end_pos": b, "attrs": attrs})
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"type": "header", "version": 2,
                            "positional": ["form", "lemma", "upos", "xpos", "feats", "deprel"]}) + "\n")
        for e in ev:
            f.write(json.dumps(e) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_conllu_jsonl_")
    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    src_c, src_j = os.path.join(tmp, "in.conllu"), os.path.join(tmp, "in.jsonl")
    write_conllu(src_c)
    write_jsonl(src_j)
    idx_c, idx_j = os.path.join(tmp, "c"), os.path.join(tmp, "j")
    for cmd in ([a.pando_index, src_c, idx_c], [a.pando_index, "--format", "jsonl", src_j, idx_j]):
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stderr)
            return 2

    def info(d):
        return {l.split("=", 1)[0]: l.strip().split("=", 1)[1] for l in open(os.path.join(d, "corpus.info"),
                                                                             encoding="utf-8") if "=" in l}

    def count(d, q):
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        out = r.stdout.split()
        return int(out[-1]) if out and out[-1].isdigit() else r.stderr.strip()[:200]

    ic, ij = info(idx_c), info(idx_j)
    check("par" in ic.get("structural", "").split(","), "CoNLL-U: bare # newpar gives par regions")
    check(count(idx_c, "<par>[]") == 3 and count(idx_c, '[par_id="_"]') == 7,
          f"CoNLL-U: 3 paragraphs, 7 tokens in those without an id ({count(idx_c, '<par>[]')}, "
          f"{count(idx_c, '[par_id=\"_\"]')})")
    check(count(idx_c, "<doc>[]") == 2, f"CoNLL-U: a bare # newdoc opens a document ({count(idx_c, '<doc>[]')})")
    check(count(idx_c, '[s_text_en="Bob left."]') == 2,
          f"CoNLL-U: # text_en as s_text_en ({count(idx_c, '[s_text_en=\"Bob left.\"]')})")
    check(count(idx_c, '[doc_genre="news"]') == 7, f"CoNLL-U: # meta::genre as doc_genre ({count(idx_c, '[doc_genre=\"news\"]')})")
    for key in ("relation.deps", "relation.head"):
        check(ic.get(key) == ij.get(key) and ic.get(key), f"{key}: {ic.get(key)} / {ij.get(key)}")
    for q in ("[]", "<s>[]", "<doc>[]", "<par>[]", "a:[] > b:[]", "a:[] >:deps b:[]", 'a:[] >:deps:"nsubj" b:[]',
              'a:[] >:deps:"conj:and" b:[]', '[form="\\""]', '[doc_genre="news"]', '[par_id="p2"]'):
        nc, nj = count(idx_c, q), count(idx_j, q)
        check(nc == nj and isinstance(nc, int), f"{q}: CoNLL-U {nc}, JSONL {nj}")
    print("CoNLL-U / JSONL parity: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
