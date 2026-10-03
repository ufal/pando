#!/usr/bin/env python3
"""pando-index on dependency input it cannot store must build a usable index, not crash.

  - a numeric (CoNLL-U style) head beyond the end of its sentence: stored as a root
    (it used to index past the sentence's child lists: SIGSEGV in end_sentence);
  - a "sentence" longer than the int16 tree offsets allow (no sentence regions, so a
    whole document is one sentence): stored without dependencies (it used to write
    wrapped offsets, or crash);
  - both reported as warnings; well-formed sentences around them keep their trees.

  test/jsonl_bad_deps_test.py --pando build/pando --pando-index build/pando-index
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

FAILS = []


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def tok(tid, upos, deprel, head=None, head_tok_id=None):
    t = {"type": "token", "tok_id": tid, "form": "x", "upos": upos, "deprel": deprel}
    if head is not None:
        t["head"] = head            # numeric: 1-based within the sentence
    if head_tok_id is not None:
        t["head_tok_id"] = head_tok_id
    return t


def write_jsonl(path, events):
    with open(path, "w") as f:
        f.write(json.dumps({"type": "header", "version": 2, "positional": ["form", "upos", "deprel"]}) + "\n")
        for e in events:
            f.write(json.dumps(e) + "\n")


def total(pando, idx, q):
    p = subprocess.run([pando, idx, q, "--json", "--total", "--limit", "1"], capture_output=True, text=True)
    if p.returncode != 0:
        return None
    return json.loads(p.stdout)["result"]["page"]["total"]


def build(pando_index, jsonl, idx):
    return subprocess.run([pando_index, "--format", "jsonl", jsonl, idx], capture_output=True, text=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    o = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        # 1. good sentence, sentence with heads beyond its end, good sentence
        ev = []
        pos = 0

        def sentence(toks):
            nonlocal pos
            ev.extend(toks)
            ev.append({"type": "region", "struct": "s", "start_pos": pos, "end_pos": pos + len(toks) - 1, "attrs": {}})
            pos += len(toks)

        ev.append({"type": "region_start", "struct": "text", "attrs": {"id": "d1"}})
        sentence([tok("w-1", "VERB", "root", head=0), tok("w-2", "NOUN", "nsubj", head=1)])
        sentence([tok("w-3", "VERB", "root", head=0), tok("w-4", "NOUN", "nsubj", head=40),
                  tok("w-5", "NOUN", "obj", head=7)])
        sentence([tok("w-6", "VERB", "root", head=0), tok("w-7", "NOUN", "nsubj", head=1)])
        ev.append({"type": "region_end", "struct": "text"})
        j1, i1 = os.path.join(tmp, "bad_heads.jsonl"), os.path.join(tmp, "bad_heads")
        write_jsonl(j1, ev)
        p = build(o.pando_index, j1, i1)
        check(p.returncode == 0, f"heads beyond the sentence: build exits 0 (got {p.returncode}: {p.stderr[-300:]})")
        check(os.path.exists(os.path.join(i1, "corpus.info")), "heads beyond the sentence: corpus.info written")
        check("head outside their sentence" in p.stderr and " 2 token(s)" in p.stderr,
              f"heads beyond the sentence: warning with count ({p.stderr.strip().splitlines()[-1:] if p.stderr else ''})")
        check(total(o.pando, i1, '[upos="VERB"] > [deprel="nsubj"]') == 2,
              "well-formed sentences keep their trees (2 VERB > nsubj)")
        check(total(o.pando, i1, '[]') == 7, "all 7 tokens indexed")

        # 2. one 'sentence' of 20000 tokens with heads (no s regions) + a normal sentence
        n = 20000
        ev = [tok(f"w-{i}", "VERB" if i % 2 else "NOUN", "nsubj", head_tok_id=f"w-{i + 1 if i < n else 1}")
              for i in range(1, n + 1)]
        j2, i2 = os.path.join(tmp, "long.jsonl"), os.path.join(tmp, "long")
        write_jsonl(j2, ev)
        p = build(o.pando_index, j2, i2)
        check(p.returncode == 0, f"over-long sentence: build exits 0 (got {p.returncode}: {p.stderr[-300:]})")
        check(os.path.exists(os.path.join(i2, "corpus.info")), "over-long sentence: corpus.info written")
        check("longer than 16383 tokens" in p.stderr, "over-long sentence: warning")
        dh = os.path.join(i2, "dep.head")
        check(not os.path.exists(dh) or os.path.getsize(dh) == 2 * n,
              "over-long sentence: dep.* stay aligned with the tokens")
        check(total(o.pando, i2, '[] > []') in (0, None), "over-long sentence: no (wrapped) dependency hits")

    print(f"{'OK' if not FAILS else 'FAILED'}: {len(FAILS)} failures")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
