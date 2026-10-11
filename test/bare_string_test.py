#!/usr/bin/env python3
"""A bare "string" / /regex/ token on a corpus without contr_form (no contractions)
is the one-token [form=…], as in CQP: "the the" is two hits, not one run. On a corpus with
multi-word tokens (a `contr` region with `form`), "au" finds the contraction over its parts.

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

CONLLU_MWT = """# sent_id = 1
# text = Il va au marché
1\tIl\til\tPRON\t_\t_\t2\tnsubj\t_\t_
2\tva\taller\tVERB\t_\t_\t0\troot\t_\t_
3-4\tau\t_\t_\t_\t_\t_\t_\t_\t_
3\tà\tà\tADP\t_\t_\t5\tcase\t_\t_
4\tle\tle\tDET\t_\t_\t5\tdet\t_\t_
5\tmarché\tmarché\tNOUN\t_\t_\t2\tobl\t_\t_

"""
# one hit over both parts of the contraction; the parts themselves still match as forms
WANT_MWT = {'"au"': 1, '"à"': 1, '"marché"': 1}
# punctuation: `"\."` / `"\?"` are the characters (escapes reach the pattern), a lone `"?"` is a
# question mark, and `"."` keeps its CWB meaning (any one-character token)
WANT_PUNCT = {r'[form="\."]': 1, r'[form="\?"]': 1, '[form="?"]': 1, '[form="."]': 2, r'[form!="\."]': 4,
              r'"\?"': 1}


CONLLU_PUNCT = """# sent_id = 1
# text = Is it ? Yes .
1\tIs\tbe\tAUX\t_\t_\t2\tcop\t_\t_
2\tit\tit\tPRON\t_\t_\t0\troot\t_\t_
3\t?\t?\tPUNCT\t_\t_\t2\tpunct\t_\t_
4\tYes\tyes\tINTJ\t_\t_\t2\tdiscourse\t_\t_
5\t.\t.\tPUNCT\t_\t_\t2\tpunct\t_\t_

"""

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
        # the CWB dialect: a bare "string" [flags] is [word="string" flags], one token per hit as in CQP
        for q, want in {'"the"': 3, '"the" "the"': 1, '"THE" %c': 3, '"th.*"': 3}.items():
            p = subprocess.run([o.pando, idx, "--cql", "cwb", q, "--count-only"], capture_output=True, text=True)
            got = p.stdout.strip() or p.stderr.strip()
            if got != str(want):
                print(f"FAIL (cwb) {q}: {got!r} != {want}")
                fails += 1
        # multi-word token: `contr` region with form="au" over à + le
        src2, idx2 = os.path.join(tmp, "m.conllu"), os.path.join(tmp, "idx2")
        open(src2, "w").write(CONLLU_MWT)
        subprocess.run([o.pando_index, src2, idx2], check=True, capture_output=True)
        for q, n in WANT_MWT.items():
            p = subprocess.run([o.pando, idx2, q, "--count-only"], capture_output=True, text=True)
            got = p.stdout.strip()
            if got != str(n):
                print(f"FAIL (mwt) {q}: {got!r} != {n}")
                fails += 1
        # punctuation, with escapes
        src3, idx3 = os.path.join(tmp, "p.conllu"), os.path.join(tmp, "idx3")
        open(src3, "w").write(CONLLU_PUNCT)
        subprocess.run([o.pando_index, src3, idx3], check=True, capture_output=True)
        for q, n in WANT_PUNCT.items():
            p = subprocess.run([o.pando, idx3, q, "--count-only"], capture_output=True, text=True)
            got = (p.stdout.strip() or p.stderr.strip())
            if got != str(n):
                print(f"FAIL (punct) {q}: {got!r} != {n}")
                fails += 1
        # the CWB dialect passes the backslash on as CQP does
        p = subprocess.run([o.pando, idx3, "--cql", "cwb", r'[word="\."]', "--count-only"], capture_output=True, text=True)
        if p.stdout.strip() != "1":
            print(f"FAIL (cwb) [word=\"\\.\"]: {p.stdout.strip() or p.stderr.strip()!r} != 1")
            fails += 1
    if fails:
        sys.exit(1)
    print("PASS bare_string")


if __name__ == "__main__":
    main()
