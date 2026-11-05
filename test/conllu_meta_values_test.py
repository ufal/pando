#!/usr/bin/env python3
"""CoNLL-U metadata comments whose values contain spaces are kept (EF-4).

    conllu_meta_values_test.py --pando build/pando --pando-index build/pando-index

A self-authored CoNLL-U file with a `text` region (`# newregion text`), a document (`# newdoc id`) and
metadata with and without spaces:

  * `# text_author = Jan Novák`, `# text_title = A Short Walk` → text_author, text_title (were dropped);
  * `# text_genre = news` → text_genre (as before);
  * `# meta::source = Daily Paper` → doc_source;
  * `# text_en = Bob left.` in a sentence's comments → the sentence translation s_text_en, not a text
    attribute (as before);
  * `# text_orig = …` after the first sentence's `# sent_id` (in the block that opened the text) and
    `# text_ortho = …` in a later sentence (UD) → s_text_orig, s_text_ortho, not text attributes;
  * `# sent_id` keeps its one-word ids.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile

CONLLU = """# newregion text
# text_author = Jan Novák
# text_title = A Short Walk
# text_genre = news
# newdoc id = d1
# meta::source = Daily Paper
# sent_id = s1
# text = Anna came.
# text_orig = Anna kam .
1\tAnna\tAnna\tPROPN\t_\t_\t2\tnsubj\t_\t_
2\tcame\tcome\tVERB\t_\t_\t0\troot\t_\t_

# sent_id = s2
# text = Bob left.
# text_en = Bob left.
# text_ortho = Bob lefft .
1\tBob\tBob\tPROPN\t_\t_\t2\tnsubj\t_\t_
2\tleft\tleave\tVERB\t_\t_\t0\troot\t_\t_

"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_conllu_meta_")
    src, idx = os.path.join(tmp, "in.conllu"), os.path.join(tmp, "idx")
    with open(src, "w", encoding="utf-8") as f:
        f.write(CONLLU)
    p = subprocess.run([a.pando_index, src, idx], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stderr)
        return 2
    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    def count(q):
        r = subprocess.run([a.pando, "--count-only", idx, q], capture_output=True, text=True)
        out = r.stdout.split()
        return int(out[-1]) if out and out[-1].isdigit() else r.stderr.strip()[:200]

    for q, want in (('[text_author="Jan Novák"]', 4), ('[text_title="A Short Walk"]', 4),
                    ('[text_genre="news"]', 4), ('[doc_source="Daily Paper"]', 4),
                    ('[s_text_en="Bob left."]', 2), ('[s_sent_id="s2"]', 2),
                    ('[s_text_orig="Anna kam ."]', 2), ('[s_text_ortho="Bob lefft ."]', 2)):
        n = count(q)
        check(n == want, f"{q}: {n} (want {want})")
    # the translation is not a text attribute
    r = subprocess.run([a.pando, idx, "show info"], capture_output=True, text=True)
    text_line = next((l for l in r.stdout.splitlines() if l.strip().startswith("text ")), "")
    text_attrs = text_line.split("attrs:")[-1].split()
    for attr in ("en", "orig", "ortho"):
        check(attr not in text_attrs, f"text has no attribute {attr} ({text_line.strip()})")
    print("CoNLL-U metadata values: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
