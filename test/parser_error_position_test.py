#!/usr/bin/env python3
"""A query syntax error says where it is: "... at position N", N the byte offset in the query of the
token the parser objects to (flexicorp marks it in the query box). Errors that name no token get the
position the parser stopped at.

  test/parser_error_position_test.py --pando build/pando --pando-index build/pando-index
"""
import argparse, os, re, subprocess, sys, tempfile

CONLLU = """# sent_id = 1
# text = the cat saw the dog
1\tthe\tthe\tDET\t_\t_\t2\tdet\t_\t_
2\tcat\tcat\tNOUN\t_\t_\t3\tnsubj\t_\t_
3\tsaw\tsee\tVERB\t_\t_\t0\troot\t_\t_
4\tthe\tthe\tDET\t_\t_\t5\tdet\t_\t_
5\tdog\tdog\tNOUN\t_\t_\t3\tobj\t_\t_

"""
# query -> (the token the error is at, as the text found at that byte offset)
CASES = {
    'v:[upos="VERB"] > [deprel="nsubj"] or [deprel="obj"]': "or",          # a word between tokens
    '[upos="NOUN"]]': "]",                                                  # a stray bracket
    'v:[upos="VERB"] not [upos="NOUN"]': "not",                             # `not` without within/containing
    '[form=]': "]",                                                         # a missing value
    '[form="無非"] x': "x",                                                  # bytes, not characters
    'a:[upos="NOUN"] :: a.b': None,                                         # stopped at (no named token)
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    o = ap.parse_args()
    fails = 0
    with tempfile.TemporaryDirectory() as tmp:
        src, idx = os.path.join(tmp, "t.conllu"), os.path.join(tmp, "idx")
        open(src, "w", encoding="utf-8").write(CONLLU)
        subprocess.run([o.pando_index, src, idx], check=True, capture_output=True)
        for q, at in CASES.items():
            p = subprocess.run([o.pando, idx, q, "--count-only"], capture_output=True, text=True)
            err = (p.stderr + p.stdout).strip()
            m = re.search(r" at position (\d+)", err)
            if not m:
                print(f"FAIL no position: {q!r} -> {err!r}")
                fails += 1
                continue
            pos = int(m.group(1))
            if at is not None and q.encode("utf-8")[pos:pos + len(at.encode("utf-8"))].decode("utf-8", "replace") != at:
                print(f"FAIL wrong place: {q!r} -> position {pos}, found {q.encode('utf-8')[pos:pos + 4]!r}, wanted {at!r}")
                fails += 1
    if fails:
        sys.exit(1)
    print("PASS parser_error_position")


if __name__ == "__main__":
    main()
