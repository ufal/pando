#!/usr/bin/env python3
"""Date functions in global filters (year / decade / century) read a region attribute
the same way through a named token (a.text_year) as without one (text_year).

  test/date_func_test.py --pando build/pando --pando-index build/pando-index
"""
import argparse, os, subprocess, sys, tempfile

CONLLU = """# newregion text
# text_year = 1765
# newregion s
# sent_id = 1
1\tthe\tthe\tDET\t_\t_\t2\tdet\t_\t_
2\tcat\tcat\tNOUN\t_\t_\t3\tnsubj\t_\t_
3\tslept\tsleep\tVERB\t_\t_\t0\troot\t_\t_

# newregion text
# text_year = 1801
# newregion s
# sent_id = 2
1\ta\ta\tDET\t_\t_\t2\tdet\t_\t_
2\tdog\tdog\tNOUN\t_\t_\t3\tnsubj\t_\t_
3\tbarked\tbark\tVERB\t_\t_\t0\troot\t_\t_

"""
WANT = {
    '[upos="NOUN"] :: decade(text_year) = 1760': 1,
    'a:[upos="NOUN"] :: decade(a.text_year) = 1760': 1,
    'a:[upos="NOUN"] :: century(a.text_year) = 19': 1,
    'a:[upos="NOUN"] :: year(a.text_year) >= 1700': 2,
    'a:[upos="DET"] b:[upos="NOUN"] :: year(b.text_year) > 1800': 1,
}


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
            p = subprocess.run([o.pando, idx, q, "--count-only"], capture_output=True, text=True)
            got = p.stdout.strip()
            if got != str(n):
                print(f"FAIL {q}: {got!r} != {n} {p.stderr.strip()}")
                fails += 1
    if fails:
        sys.exit(1)
    print("PASS date_func")


if __name__ == "__main__":
    main()
