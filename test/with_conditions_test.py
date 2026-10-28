#!/usr/bin/env python3
"""`with` and what links its two sides.

    with_conditions_test.py --pando build/pando --pando-index build/pando-index

A self-authored corpus aligned by sentence only (s_tuid on <s>, no token tuid), English and Dutch. Requires:
  * `:: a.tuid = b.tuid` (an attribute the corpus does not have): an error naming what it is aligned by, not 0 hits;
  * `with` with nothing linking its sides: the cross product, with a warning (stderr; "warnings" in the API JSON);
  * with a condition (`a.s_tuid = b.s_tuid`, `distance(a,b)`), or as `=:s_tuid`: no warning.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

SENTS = [("en", "s1", "u1", ["The", "cat", "sleeps"]), ("en", "s2", "u2", ["A", "dog", "barks"]),
         ("nl", "s3", "u1", ["De", "kat", "slaapt"]), ("nl", "s4", "u2", ["Een", "hond", "blaft"])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_with_")
    src, idx = os.path.join(tmp, "c.jsonl"), os.path.join(tmp, "idx")
    pos, texts = 0, {}
    with open(src, "w", encoding="utf-8") as f:
        f.write(json.dumps({"type": "header", "version": 2, "positional": ["form"]}) + "\n")
        for lang, sid, tuid, words in SENTS:
            start = pos
            for w in words:
                f.write(json.dumps({"type": "token", "form": w}) + "\n")
                pos += 1
            f.write(json.dumps({"type": "region", "struct": "s", "start_pos": start, "end_pos": pos - 1,
                                "attrs": {"id": sid, "tuid": tuid}}) + "\n")
            texts.setdefault(lang, [start, pos - 1])[1] = pos - 1
        for lang, (s, e) in texts.items():
            f.write(json.dumps({"type": "region", "struct": "text", "start_pos": s, "end_pos": e,
                                "attrs": {"id": lang, "lang": lang}}) + "\n")
    p = subprocess.run([a.pando_index, "--format", "jsonl", src, idx], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stderr)
        return 2
    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    def run(q, *extra):
        return subprocess.run([a.pando, *extra, idx, q], capture_output=True, text=True)

    r = run('a:[form="cat"] with b:[] :: a.tuid = b.tuid', "--count-only")
    msg = (r.stdout + r.stderr).strip()
    check("no attribute 'tuid'" in msg and "aligned by s_tuid" in msg, f"unknown attribute: an error with a hint ({msg[-160:]})")

    r = run('a:[form="cat"] with b:[]', "--count-only")
    check(r.stdout.strip().splitlines()[-1] == "12" and "Warning:" in r.stderr and "linking" in r.stderr,
          f"nothing linking the sides: the cross product (12), with a warning ({r.stdout.strip()}; {r.stderr.strip()[:60]})")

    r = run('a:[form="cat"] with b:[]', "--api")
    try:
        warnings = json.loads(r.stdout).get("result", {}).get("warnings")
    except ValueError:
        warnings = None
    check(warnings is None or bool(warnings), f"API JSON: no 'warnings' field, or one with the warning ({warnings})")

    for q in ('a:[form="cat"] with b:[] :: a.s_tuid = b.s_tuid', 'a:[form="cat"] with b:[] :: distance(a,b) < 2',
              'a:[form="cat"] =:s_tuid b:[]'):
        r = run(q, "--count-only")
        check(r.returncode == 0 and "Warning" not in r.stderr, f"{q}: {r.stdout.strip()}, no warning")
    r = run('a:[form="cat"] with b:[] :: a.s_tuid = b.s_tuid', "--count-only")
    check(r.stdout.strip() == "6", f"with :: a.s_tuid = b.s_tuid: 6 (the 6 tokens of the two u1 sentences; {r.stdout.strip()})")
    print("with conditions: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
