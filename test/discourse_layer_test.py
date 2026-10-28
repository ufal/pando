#!/usr/bin/env python3
"""Relation layers given edge by edge (JSONL `relation` events), as a converter writes discourse relations.

    discourse_layer_test.py --pando build/pando --pando-index build/pando-index

pando knows no annotation convention (GUM's `Discourse=`, TEITOK's standoff links): a converter (flexencoder,
flexiconv) writes the units as regions and the relations as events of a declared layer. A self-authored corpus,
two documents with discourse units (`edu` regions) and relations between their first tokens:

    header:  "relations": {"rst": "pointer,label=rel,scope=doc"}
    event:   {"type": "relation", "layer": "rst", "source": 15, "target": 8, "label": "causal-cause"}

Requires:
  * the layer declared in corpus.info (a layer of its own: no inherit) with its measured span;
  * `a:[] >:rst:REL b:[]` (b attached to a), `>>:rst`, a token without edges having none (no inherited tree),
    two relations between the same pair, an edge given twice kept once;
  * `count by` a region attribute, `dcoll (via rst)`;
  * an edge of an undeclared layer, an inherit= declaration and an event without a target: refused.
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

# (form, lemma), two documents: d1 = tokens 0-9, d2 = tokens 10-15
TOKENS = [("It", "it"), ("rained", "rain"), ("so", "so"), ("we", "we"), ("stayed", "stay"),
          ("because", "because"), ("books", "book"), ("were", "be"), ("new", "new"), (".", "."),
          ("Anna", "Anna"), ("smiled", "smile"), (".", "."), ("She", "she"), ("won", "win"), (".", ".")]
SENTS = [(0, 4), (5, 9), (10, 12), (13, 15)]
DOCS = [(0, 9, "d1"), (10, 15, "d2")]
EDUS = [(0, 1, "ROOT"), (2, 4, "causal-result"), (5, 9, "causal-cause"), (10, 12, "ROOT"), (13, 15, "explanation-evidence")]
# (source = the attaching unit's first token, target = the unit it attaches to, label)
EDGES = [(2, 0, "causal-result"), (5, 2, "causal-cause"), (5, 2, "explanation-justify"), (5, 0, "elaboration"),
         (13, 10, "explanation-evidence"), (13, 10, "explanation-evidence")]   # the last one twice: kept once


def write(path, header_rel, events):
    with open(path, "w", encoding="utf-8") as f:
        head = {"type": "header", "version": 2, "positional": ["form", "lemma"], "structural": ["s", "doc", "edu"]}
        if header_rel is not None:
            head["relations"] = header_rel
        f.write(json.dumps(head) + "\n")
        k = 0
        for a, b in SENTS:
            for i in range(a, b + 1):
                f.write(json.dumps({"type": "token", "form": TOKENS[i][0], "lemma": TOKENS[i][1]}) + "\n")
            f.write(json.dumps({"type": "region", "struct": "s", "start_pos": a, "end_pos": b,
                                "attrs": {"id": f"s{k}"}}) + "\n")
            k += 1
        for a, b, d in DOCS:
            f.write(json.dumps({"type": "region", "struct": "doc", "start_pos": a, "end_pos": b, "attrs": {"id": d}}) + "\n")
        for a, b, rel in EDUS:
            f.write(json.dumps({"type": "region", "struct": "edu", "start_pos": a, "end_pos": b, "attrs": {"rel": rel}}) + "\n")
        for e in events:
            f.write(json.dumps(e) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_rel_events_")
    failures = 0

    def check(ok, msg):
        nonlocal failures
        failures += not ok
        print(("ok   " if ok else "FAIL ") + msg)

    def build(name, header_rel, events):
        src, d = os.path.join(tmp, name + ".jsonl"), os.path.join(tmp, name)
        write(src, header_rel, events)
        p = subprocess.run([a.pando_index, "--format", "jsonl", src, d], capture_output=True, text=True)
        return d, p

    events = [{"type": "relation", "layer": "rst", "source": s, "target": t, "label": l} for s, t, l in EDGES]
    d, p = build("ok", {"rst": "pointer,label=rel,scope=doc"}, events)
    if p.returncode != 0:
        print(p.stderr)
        return 2

    def run(q, *extra):
        r = subprocess.run([a.pando, *extra, d, q], capture_output=True, text=True)
        return r.stdout if r.returncode == 0 else "ERROR " + r.stderr.strip()[:300]

    def count(q):
        out = run(q, "--count-only").split()
        return int(out[-1]) if out and out[-1].isdigit() else out

    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    line = next((l for l in info.splitlines() if l.startswith("relation.rst=")), "")
    check("pointer" in line and "inherit" not in line and "scope=doc" in line and "span=5" in line,
          f"declared and measured: {line}")
    edges = set(EDGES)
    pairs = {(t, s) for s, t, _ in edges}
    check(count("a:[] >:rst b:[]") == len(pairs), f"a:[] >:rst b:[]: {count('a:[] >:rst b:[]')} (reference {len(pairs)})")
    for label in sorted({l for _, _, l in edges}):
        want = sorted((t, s) for s, t, l in edges if l == label)
        out = run(f'a:[] >:rst:"{label}" b:[]; tabulate 0 100 a.cpos, b.cpos')
        got = sorted(tuple(map(int, l.split("\t"))) for l in out.splitlines() if l and l[0].isdigit())
        check(got == want, f">:rst:{label}: {got} (reference {want})")
    check(count('a:[lemma="it"] >>:rst b:[]') == 2, f'a:[lemma="it"] >>:rst b:[]: {count("a:[lemma=\"it\"] >>:rst b:[]")} (reference 2: so, because)')
    check(count('a:[lemma="rain"] >:rst b:[]') == 0 and count('a:[] <:rst b:[lemma="rain"]') == 0,
          "a token without edges has none (no inherited tree)")
    out = run('a:[] >:rst b:[]; count by b.edu_rel')
    got = {l.split("\t")[0]: int(l.split("\t")[1]) for l in out.splitlines() if "\t" in l and l.split("\t")[1].isdigit()}
    check(got == {"causal-result": 1, "causal-cause": 2, "explanation-evidence": 1}, f"count by b.edu_rel: {got}")
    out = run('[lemma="it" | lemma="so"]; dcoll (via rst) children by lemma', "--min-freq", "1")
    rows = {l.split("\t")[0]: l.split("\t")[1] for l in out.splitlines() if "\t" in l and l.split("\t")[1].isdigit()}
    check(rows == {"so": "1", "because": "2"}, f"dcoll (via rst) children: {rows}")

    # refused: an undeclared layer, an inherit= declaration, an event without a target
    _, p = build("undeclared", {}, events)
    check(p.returncode != 0 and "not declared" in p.stderr, f"edges of an undeclared layer refused: {p.stderr.strip()[-120:]}")
    _, p = build("inherit", {"rst": "pointer,label=rel,inherit=head"}, events)
    check(p.returncode != 0 and "inherit" in p.stderr, f"inherit= refused: {p.stderr.strip()[-120:]}")
    _, p = build("notarget", {"rst": "pointer,label=rel"}, [{"type": "relation", "layer": "rst", "source": 2}])
    check(p.returncode != 0 and "target" in p.stderr, f"an event without a target refused: {p.stderr.strip()[-120:]}")
    print("relation events: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
