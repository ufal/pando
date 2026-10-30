#!/usr/bin/env python3
"""`within S` for a single-token query when S is flat, nested= or overlapping=: the token (or a
repetition's run) must lie inside a region of S. Single-token queries used to ignore an
explicit `within` (every token matched); multi-token queries are checked alongside. Every
count must agree with fast paths and bitmaps on and off.

  test/within_multi_test.py --pando build/pando --pando-index build/pando-index
"""
import argparse, itertools, json, os, subprocess, sys, tempfile

TOKENS = [("the", "DET"), ("old", "ADJ"), ("man", "NOUN"), ("sleeps", "VERB"), ("and", "CCONJ"),
          ("he", "PRON"), ("snores", "VERB"), ("a", "DET"), ("dog", "NOUN"), ("barks", "VERB"),
          ("at", "ADP"), ("him", "PRON")]
# mention rows: [0,2] holds [2,2], [7,8] holds [8,8]; tokens in some mention: 0 1 2 5 7 8 11
MENTIONS = [(0, 2), (2, 2), (5, 5), (7, 8), (8, 8), (11, 11)]
# flat m: tokens 1 2 only
FLAT_M = [(1, 2)]

WANT = {
    '[] within mention': 7,
    '[upos="VERB"] within mention': 0,
    '[upos="NOUN"] within mention': 2,
    '[upos="DET"|upos="ADJ"] within mention': 3,
    '[upos!="VERB"] within mention': 7,
    '[upos="DET"|upos="ADJ"|upos="NOUN"]+ within mention': 2,
    '[] [] within mention': 3,
    '[upos="DET"] [] within mention': 2,
    '[] not within mention': 5,
    '<mention> [upos="VERB"]': 0,
    '[] within m': 2,
    '[form="the"] within m': 0,
    '[form="the"|form="old"] within m': 1,
    '[]+ within m': 1,
    '[] within s': 12,
}
COUNT_BY = '[] within mention; count by upos'   # 7 hits over DET NOUN PRON ADJ


def write_corpus(path, kind):
    ev = [{"type": "header", "version": 2, "positional": ["form", "upos"],
           "structural": ["s", "mention", "m"], kind: ["mention"]}]
    ev += [{"type": "token", "form": f, "upos": u} for f, u in TOKENS]
    ev += [{"type": "region", "struct": "s", "start_pos": 0, "end_pos": 6, "attrs": {"id": "s1"}},
           {"type": "region", "struct": "s", "start_pos": 7, "end_pos": 11, "attrs": {"id": "s2"}}]
    ev += [{"type": "region", "struct": "mention", "start_pos": a, "end_pos": b, "attrs": {"eid": f"e{i}"}}
           for i, (a, b) in enumerate(MENTIONS)]
    ev += [{"type": "region", "struct": "m", "start_pos": a, "end_pos": b, "attrs": {}} for a, b in FLAT_M]
    with open(path, "w") as fh:
        for e in ev:
            fh.write(json.dumps(e) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    o = ap.parse_args()
    fails = 0
    with tempfile.TemporaryDirectory() as tmp:
        for kind in ("overlapping", "nested"):
            src, idx = os.path.join(tmp, kind + ".jsonl"), os.path.join(tmp, kind)
            write_corpus(src, kind)
            subprocess.run([o.pando_index, "--format", "jsonl", src, idx], check=True, capture_output=True)
            for fp, bm in itertools.product(("on", "off"), ("on", "off")):
                env = dict(os.environ, PANDO_FASTPATH=fp, PANDO_BITMAPS=bm)
                tag = f"{kind} fastpath={fp} bitmaps={bm}"
                for q, n in WANT.items():
                    p = subprocess.run([o.pando, idx, q, "--count-only"], capture_output=True, text=True, env=env)
                    if p.stdout.strip() != str(n):
                        print(f"FAIL [{tag}] {q}: {p.stdout.strip()!r} != {n} {p.stderr.strip()}")
                        fails += 1
                p = subprocess.run([o.pando, idx, COUNT_BY], capture_output=True, text=True, env=env)
                if "Total: 7 matches, 4 groups" not in p.stdout:
                    print(f"FAIL [{tag}] {COUNT_BY}: {p.stdout.strip().splitlines()[-1:]} {p.stderr.strip()}")
                    fails += 1
    if fails:
        sys.exit(1)
    print("PASS within_multi")


if __name__ == "__main__":
    main()
