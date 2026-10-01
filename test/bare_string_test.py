#!/usr/bin/env python3
"""A bare "string" / /regex/ token on a corpus without contr_form (no contractions)
is the one-token [form=…], as in CQP: "the the" is two hits, not one run.

  test/bare_string_test.py --pando build/pando --pando-index build/pando-index
"""
import argparse, os, subprocess, sys, tempfile

CONLLU = """# sent_id = 1
# text = the the cat saw the dog
1\tthe\tthe\tDET\t_\t_\t3\tdet\t_\t_
2\tthe\tthe\tDET\t_\t_\t3\tdet\t_\t_
3\tcat\tcat\tNOUN\t_\t_\t4\tnsubj\t_\t_
4\tsaw\tsee\tVERB\t_\t_\t0\troot\t_\t_
5\tthe\tthe\tDET\t_\t_\t6\tdet\t_\t_
6\tdog\tdog\tNOUN\t_\t_\t4\tobj\t_\t_

"""
WANT = {'"the"': 3, '/th.*/': 3, '"the" "the"': 1, '"the" [upos="NOUN"]': 2, '"the"+': 2, '"the"{2}': 1,
        '"dog"': 1}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    o = ap.parse_args()
    fails = 0
    with tempfile.TemporaryDirectory() as tmp:
        src, idx = os.path.join(tmp, "t.conllu"), os.path.join(tmp, "idx")
        open(src, "w").write(CONLLU)
        subprocess.run([o.pando_index, src, idx], check=True, capture_output=True)
        for q, n in WANT.items():
            for mode in ("on", "off"):
                p = subprocess.run([o.pando, idx, q, "--count-only"], capture_output=True, text=True,
                                   env=dict(os.environ, PANDO_FASTPATH=mode))
                got = p.stdout.strip()
                if got != str(n):
                    print(f"FAIL ({mode}) {q}: {got!r} != {n}")
                    fails += 1
    if fails:
        sys.exit(1)
    print("PASS bare_string")


if __name__ == "__main__":
    main()
