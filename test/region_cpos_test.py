#!/usr/bin/env python3
"""`t.cpos` for a region binding is where the region starts (as `a.cpos` for a token), in tabulate
and in count/freq fields; with `tcnt(t)` the region's extent — what flexicorp's dispersion view uses.

  test/region_cpos_test.py --pando build/pando --pando-index build/pando-index
"""
import argparse, os, subprocess, sys, tempfile

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando-rcpos-")
    vrt, idx = os.path.join(tmp, "c.vrt"), os.path.join(tmp, "idx")
    with open(vrt, "w") as out:
        out.write("<!-- positional-attributes: word -->\n")
        for d, n in (("a", 5), ("b", 7)):
            out.write(f'<text id="{d}">\n<s>\n' + "".join("x\n" if i != 2 else "y\n" for i in range(n)) + "</s>\n</text>\n")
    subprocess.run([a.pando_index, "--format", "vertical", vrt, idx], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run = lambda q: subprocess.run([a.pando, idx, q], capture_output=True, text=True, check=True).stdout.strip().splitlines()
    fails = []
    got = run("t:<text>; tabulate t.id, t.cpos, tcnt(t)")
    if got[1:] != ["a\t0\t5", "b\t5\t7"]:
        fails.append(f"tabulate: {got}")
    got = [l.split("\t")[:3] for l in run('t:<text> :: t.id="b"; freq by t.id, t.cpos, tcnt(t);')[1:2]]
    if got != [["b", "5", "7"]]:
        fails.append(f"freq fields: {got}")
    got = sorted(l.split("\t")[0] for l in run('[word="y"]; freq by cpos;')[1:] if l[:1].isdigit())
    if got != ["2", "7"]:
        fails.append(f"token cpos: {got}")
    for f in fails:
        print("FAIL:", f)
    print("region_cpos:", "FAIL" if fails else "ok")
    sys.exit(1 if fails else 0)

if __name__ == "__main__":
    main()
