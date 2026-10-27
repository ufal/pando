#!/usr/bin/env python3
"""Relation layers in queries: `deps` from CoNLL-U DEPS as deviations from the basic tree (relation_layer.*).

    relation_layer_test.py --pando build/pando --pando-index build/pando-index --conllu test/data/deps_sample.conllu

Indexes a CoNLL-U with enhanced dependencies (inherited edges, a refined label, conjunct propagation, `ref`, edges to
an empty node, a token with DEPS `_`) and requires:
  * `relation.deps=…,inherit=head` in corpus.info, only the deviating tokens stored;
  * `[A] >:deps [B]`, `[A] <:deps [B]`, labels (`>:deps:nsubj`, quoted `>:deps:"conj:and"`) to count as a direct
    count in the CoNLL-U (edges to empty nodes left out, DEPS `_` = the basic edge);
  * `>:head:label` to equal `>` with a deprel condition;
  * layers in longer queries (chains, mixed with sequence, `!>:deps` / `!<:deps`, `>>:deps`, `count by`), checked
    against a brute-force evaluation per sentence;
  * relation filters between named tokens (`:: a !>:head b`, `:: b >>:deps a`) next to the position order
    (`:: a < b`);
  * `pando-index --derive` (derive_deps.h): `eff` (effective heads, replacing `conj`) and conj-propagation (added
    edges, as enhanced UD), against the same rules written out here (edge counts per label) and edges by hand;
  * `--format conllu` to give back each token's DEPS (sorted; `_` as the basic edge; tokens with an edge to an empty
    node left out of the comparison).
Exit status 0 = all as required, 1 = a difference, 2 = a run error.
"""

import argparse
import os
import subprocess
import sys
import tempfile


def sentences(path):
    s = []
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if not line:
            if s:
                yield s
            s = []
            continue
        if line.startswith("#"):
            continue
        c = line.split("\t")
        if "-" in c[0] or "." in c[0]:
            continue
        s.append(c)
    if s:
        yield s


def deps_edges(tok):
    if tok[8] in ("_", ""):
        return {(int(tok[6]), tok[7])}
    out = set()
    for e in tok[8].split("|"):
        h, _, lab = e.partition(":")
        if "." not in h:
            out.add((int(h), lab))
    return out


def reference(path, head_ok, dep_ok, label=None, basic=False):
    n = 0
    for s in sentences(path):
        for d in s:
            heads = {h for h, lab in ({(int(d[6]), d[7])} if basic else deps_edges(d))
                     if 0 < h <= len(s) and (label is None or lab == label)}
            n += sum(1 for h in heads if head_ok(s[h - 1]) and dep_ok(d))
    return n


ANY = lambda t: True
CASES = [   # query, reference(head_ok, dep_ok, label, basic)
    ('[] >:deps []', (ANY, ANY, None, False)),
    ('[upos="VERB"] >:deps:nsubj []', (lambda t: t[3] == "VERB", ANY, "nsubj", False)),
    ('[upos="VERB"] >:head:nsubj []', (lambda t: t[3] == "VERB", ANY, "nsubj", True)),
    ('[lemma="walk"] >:deps [upos="PROPN"]', (lambda t: t[2] == "walk", lambda t: t[3] == "PROPN", None, False)),
    ('[upos="NOUN"] <:deps [upos="VERB"]', (lambda t: t[3] == "VERB", lambda t: t[3] == "NOUN", None, False)),
    ('[] >:deps:"conj:and" []', (ANY, ANY, "conj:and", False)),
    ('[] >:deps:ref []', (ANY, ANY, "ref", False)),
    ('[] >:deps:"obl:in" []', (ANY, ANY, "obl:in", False)),
]


def layer_heads(tok, layer, label=None):
    """1-based heads of a token in a layer ('deps' or 'head'), optionally with one label."""
    edges = deps_edges(tok) if layer == "deps" else {(int(tok[6]), tok[7])}
    return {h for h, lab in edges if h > 0 and (label is None or lab == label)}


def related(s, i, j, rel):
    """rel holds between 0-based tokens i and j of sentence s (i written first in the query)."""
    op, layer, label = rel
    if op == "seq":
        return j == i + 1
    if op in (">", "<"):
        head, dep = (i, j) if op == ">" else (j, i)
        return head + 1 in layer_heads(s[dep], layer, label)
    # >> / <<: reachable over one or more edges
    anc, desc = (i, j) if op == ">>" else (j, i)
    seen, todo = set(), [desc]
    while todo:
        for h in layer_heads(s[todo.pop()], layer):
            if h - 1 not in seen:
                seen.add(h - 1)
                todo.append(h - 1)
    return anc in seen and anc != desc


def brute_count(path, conds, rels, extra=None):
    """Hits of a chain query: every assignment of sentence tokens to the query tokens (and `extra(s, tokens)`, a
    `::` filter, holds)."""
    n = 0
    for s in sentences(path):
        def extend(prefix):
            nonlocal n
            k = len(prefix)
            if k == len(conds):
                n += extra is None or extra(s, prefix)
                return
            for j, t in enumerate(s):
                if conds[k](t) and (k == 0 or related(s, prefix[-1], j, rels[k - 1])):
                    extend(prefix + [j])
        extend([])
    return n


def brute_negated(path, kept_cond, other_cond, layer, label):
    """`[A] !> [B]` keeps A, `[A] !< [B]` keeps B: the head side, kept when no dependent has the other condition."""
    n = 0
    for s in sentences(path):
        for k, t in enumerate(s):
            if kept_cond(t):
                n += not any(other_cond(d) and k + 1 in layer_heads(d, layer, label) for d in s)
    return n


def derived_edges(s, eff):
    """derive_deps.cpp's rules on one sentence (eff: effective-head, else conj-propagation): per token the set of
    (head, label), the basic edge included unless replaced."""
    base = lambda r: r.split(":")[0]
    no_prop = {"parataxis", "list", "aux", "cop", "punct", "reparandum", "orphan", "discourse"}
    head = [int(t[6]) for t in s]
    rel = [t[7] for t in s]
    edges = [{(head[i], rel[i])} for i in range(len(s))]
    children = {}
    for i, h in enumerate(head):
        children.setdefault(h, []).append(i + 1)
    subj = lambda c: any(base(rel[k - 1]) in ("nsubj", "csubj") for k in children.get(c, []))
    conjuncts = [i for i in range(len(s)) if base(rel[i]) == "conj" and head[i] > 0]
    for i in conjuncts:
        f = head[i]
        if eff:
            while base(rel[f - 1]) == "conj" and head[f - 1] > 0:
                f = head[f - 1]
            edges[i] = {(head[f - 1], rel[f - 1])}
        elif base(rel[f - 1]) != "conj" and head[f - 1] > 0 and base(rel[f - 1]) not in no_prop:
            edges[i].add((head[f - 1], rel[f - 1]))
    for i in conjuncts:
        if s[i][3] not in ("VERB", "ADJ", "AUX") or subj(i + 1):
            continue
        f = head[i]
        while eff and not subj(f) and base(rel[f - 1]) == "conj" and head[f - 1] > 0:
            f = head[f - 1]
        for sb in children.get(f, []):
            if base(rel[sb - 1]) not in ("nsubj", "csubj"):
                continue
            for sj in [sb] + [c for c in children.get(sb, []) if base(rel[c - 1]) == "conj"]:
                if sj != i + 1:
                    edges[sj - 1].add((i + 1, rel[sb - 1]))
    return edges


UP = lambda u: (lambda t: t[3] == u)
LEM = lambda l: (lambda t: t[2] == l)
CHAINS = [   # query, token conditions, relations (op, layer, label)
    ('[upos="PROPN"] <:deps:nsubj [upos="VERB"] >:deps:obj []', [UP("PROPN"), UP("VERB"), ANY],
     [("<", "deps", "nsubj"), (">", "deps", "obj")]),
    ('[upos="DET"] [upos="NOUN"] <:deps [upos="VERB"]', [UP("DET"), UP("NOUN"), UP("VERB")],
     [("seq", None, None), ("<", "deps", None)]),
    ('[lemma="sleep"] >>:deps []', [LEM("sleep"), ANY], [(">>", "deps", None)]),
    ('[] <<:deps [lemma="sleep"]', [ANY, LEM("sleep")], [("<<", "deps", None)]),
    ('[upos="NOUN"] <:deps [] <:deps []', [UP("NOUN"), ANY, ANY], [("<", "deps", None), ("<", "deps", None)]),
    ('[upos="NOUN"] <:deps [upos="VERB"] <:head:conj []', [UP("NOUN"), UP("VERB"), ANY],
     [("<", "deps", None), ("<", "head", "conj")]),
    # a layer on one edge must not carry over to the next plain one
    ('[upos="NOUN"] <:deps:nsubj [upos="VERB"] < []', [UP("NOUN"), UP("VERB"), ANY],
     [("<", "deps", "nsubj"), ("<", "head", None)]),
    ('[upos="VERB"] >:deps:nsubj [] [upos="VERB"]', [UP("VERB"), ANY, UP("VERB")],
     [(">", "deps", "nsubj"), ("seq", None, None)]),
]
FILTERED = [   # `:: a REL b` between the two named tokens: query, conditions, chain relation, filter
    ('a:[] >:deps b:[] :: a !>:head b', [ANY, ANY], [(">", "deps", None)],
     lambda s, t: not related(s, t[0], t[1], (">", "head", None))),
    ('a:[] >:deps b:[] :: a >:head b', [ANY, ANY], [(">", "deps", None)],
     lambda s, t: related(s, t[0], t[1], (">", "head", None))),
    ('a:[upos="VERB"] >:deps:nsubj b:[] :: a !>:head:nsubj b', [UP("VERB"), ANY], [(">", "deps", "nsubj")],
     lambda s, t: not related(s, t[0], t[1], (">", "head", "nsubj"))),
    ('a:[] > b:[] :: a !>:deps b', [ANY, ANY], [(">", "head", None)],
     lambda s, t: not related(s, t[0], t[1], (">", "deps", None))),
    ('a:[upos="NOUN"] [] b:[] :: b >>:deps a', [UP("NOUN"), ANY, ANY], [("seq", None, None), ("seq", None, None)],
     lambda s, t: related(s, t[2], t[0], (">>", "deps", None))),
    ('a:[] >:deps b:[] :: a < b', [ANY, ANY], [(">", "deps", None)], lambda s, t: t[0] < t[1]),
]
NEGATED = [  # query, kept (head side), other (dependent side), layer, label
    ('[upos="VERB"] !>:deps:nsubj []', UP("VERB"), ANY, "deps", "nsubj"),
    ('[upos="NOUN"] !<:deps [upos="VERB"]', UP("VERB"), UP("NOUN"), "deps", None),
    ('[upos="VERB"] !>:head:nsubj []', UP("VERB"), ANY, "head", "nsubj"),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    a = ap.parse_args()
    d = os.path.join(tempfile.mkdtemp(prefix="pando_rellayer_"), "idx")
    p = subprocess.run([a.pando_index, a.conllu, d], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout, p.stderr, file=sys.stderr)
        return 2
    failures = 0
    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    line = next((l for l in info.splitlines() if l.startswith("relation.deps=")), "")
    if "inherit=head" not in line:
        print(f"FAIL: relation.deps line: {line!r}")
        failures += 1
    else:
        print(line)
    for q, (head_ok, dep_ok, label, basic) in CASES:
        want = reference(a.conllu, head_ok, dep_ok, label, basic)
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        got = r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else "ERROR " + r.stderr.strip()[:200]
        ok = got == str(want)
        failures += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {q}: {got} (reference {want})")
    def count(q):
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True)
        return r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else "ERROR " + r.stderr.strip()[:200]

    checks = [(q, brute_count(a.conllu, c, r)) for q, c, r in CHAINS]
    checks += [(q, brute_count(a.conllu, c, r, f)) for q, c, r, f in FILTERED]
    checks += [(q, brute_negated(a.conllu, k, o, l, lab)) for q, k, o, l, lab in NEGATED]
    for q, want in checks:
        got = count(q)
        ok = got == str(want)
        failures += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {q}: {got} (reference {want})")

    # count by over a layer: the groups add up as the reference says
    want = {}
    for s in sentences(a.conllu):
        for t in s:
            for h in layer_heads(t, "deps", "nsubj"):
                if s[h - 1][3] == "VERB":
                    want[t[2]] = want.get(t[2], 0) + 1
    r = subprocess.run([a.pando, d, 'a:[upos="VERB"] >:deps:nsubj b:[]; count by b.lemma'], capture_output=True, text=True)
    got = {}
    for line in r.stdout.splitlines():
        f = line.split("\t")
        if len(f) >= 2 and f[1].isdigit():
            got[f[0]] = int(f[1])
    ok = got == want
    failures += not ok
    print(f"{'ok  ' if ok else 'FAIL'} count by b.lemma over >:deps:nsubj: {got} (reference {want})")

    # derived layers: `eff` (effective-head, the default) and `cprop` (--rule conj-propagation), their edges per
    # label as the rules give them, and a few edges by hand
    expected = {
        "eff": [('[lemma="walk"] >:eff:nsubj [lemma="Mary"]', 1), ('[lemma="John"] >:eff [lemma="Mary"]', 0),
                ('[lemma="snore"] >:eff:nsubj [lemma="dog"]', 1), ('[lemma="say"] >:eff:parataxis [lemma="see"]', 1),
                ('[lemma="think"] >:eff:ccomp [lemma="jump"]', 1), ('[lemma="jump"] >:eff:nsubj [lemma="Ann"]', 1),
                ('[lemma="love"] >:eff:xcomp [lemma="talk"]', 1), ('[lemma="walk"] >:eff [lemma="talk"]', 0)],
        "cprop": [('[lemma="walk"] >:cprop:nsubj [lemma="Mary"]', 1), ('[lemma="John"] >:cprop [lemma="Mary"]', 2),
                  ('[lemma="snore"] >:cprop:nsubj [lemma="dog"]', 1), ('[lemma="say"] >:cprop [lemma="see"]', 0),
                  ('[lemma="think"] >:cprop [lemma="jump"]', 0), ('[lemma="love"] >:cprop:xcomp [lemma="talk"]', 1)],
    }
    for name, extra, eff in [("eff", [], True), ("cprop", ["--rule", "conj-propagation"], False)]:
        r = subprocess.run([a.pando_index, "--derive", name, d] + extra, capture_output=True, text=True)
        if r.returncode != 0:
            print(f"FAIL: --derive {name}:", r.stderr.strip()[:300])
            failures += 1
            continue
        want = {}
        for s in sentences(a.conllu):
            for es in derived_edges(s, eff):
                for h, lab in es:
                    if h > 0:
                        want[lab] = want.get(lab, 0) + 1
        bad = 0
        for lab in sorted(want):
            got = count(f'[] >:{name}:"{lab}" []')
            if got != str(want[lab]):
                bad += 1
                print(f"FAIL [] >:{name}:{lab} []: {got} (reference {want[lab]})")
        failures += bad
        print(f"{'ok  ' if not bad else 'FAIL'} --derive {name}: {len(want)} labels against the rules")
        for q, want_n in expected[name]:
            got = count(q)
            ok = got == str(want_n)
            failures += not ok
            print(f"{'ok  ' if ok else 'FAIL'} {q}: {got} (expected {want_n})")
    info = open(os.path.join(d, "corpus.info"), encoding="utf-8").read()
    for line in ("source=derived:effective-head", "source=derived:conj-propagation"):
        if line not in info:
            print(f"FAIL: no relation declared {line}")
            failures += 1
    r = subprocess.run([a.pando_index, "--derive", "deps", d], capture_output=True, text=True)
    if r.returncode == 0:
        print("FAIL: --derive deps overwrote the annotated deps layer")
        failures += 1

    # totals over thin layers: the tree's total corrected at the deviating tokens (execute_layer_decomposed) must
    # equal the generic expansion's (PANDO_FASTPATH=off)
    def count_env(q, env):
        r = subprocess.run([a.pando, "--count-only", d, q], capture_output=True, text=True,
                           env=dict(os.environ, **env))
        return r.stdout.split()[-1] if r.returncode == 0 and r.stdout.split() else "ERROR " + r.stderr.strip()[:200]
    bad = 0
    decomposed = 0
    for layer in ("deps", "eff", "cprop"):
        for q in (f'[upos="VERB"] >:{layer}:nsubj []', f'[] >:{layer} []', f'[upos="NOUN"] <:{layer} [upos="VERB"]',
                  f'[upos="PROPN"] <:{layer}:nsubj []', f'[upos="VERB"] !>:{layer}:nsubj []',
                  f'[upos="NOUN"] !<:{layer} [upos="VERB"]', f'[] !>:{layer}:"conj:and" []', f'[lemma="love"] >:{layer} []'):
            on, off = count_env(q, {}), count_env(q, {"PANDO_FASTPATH": "off"})
            if on != off:
                bad += 1
                print(f"FAIL {q}: {on} with the fast paths, {off} without")
            r = subprocess.run([a.pando, "--timing", "--count-only", d, q], capture_output=True, text=True)
            decomposed += "layer_inherit" in r.stderr
    failures += bad
    print(f"{'ok  ' if not bad and decomposed == 24 else 'FAIL'} totals over thin layers with and without the "
          f"fast paths: {24 - bad} of 24 equal, {decomposed} through layer_inherit")
    failures += decomposed != 24

    # edges of a thin layer that the tree lacks, read off the stored deviations (execute_layer_deviations): same
    # hits and totals as the generic expansion with its per-hit filter
    def hits_env(q, env):
        r = subprocess.run([a.pando, "--limit", "100000", d, q], capture_output=True, text=True,
                           env=dict(os.environ, **env))
        return sorted(r.stdout.splitlines()[:-1]) if r.returncode == 0 else ["ERROR " + r.stderr.strip()[:200]]
    bad = 0
    enumerated = 0
    dev_queries = [f'a:[] >:{L} b:[] :: a !>:head b' for L in ("deps", "eff", "cprop")] + [
        'a:[upos="VERB"] >:deps:nsubj b:[] :: a !>:head:nsubj b', 'a:[] <:eff b:[upos="VERB"] :: b !>:head a',
        'a:[upos="NOUN"] <:deps b:[] :: a !<:head b', 'a:[] >:eff:nsubj b:[] :: a !>:head b']
    for q in dev_queries:
        on, off = count_env(q, {}), count_env(q, {"PANDO_FASTPATH": "off"})
        if on != off or hits_env(q, {}) != hits_env(q, {"PANDO_FASTPATH": "off"}) or on in ("0", "ERROR"):
            bad += 1
            print(f"FAIL {q}: {on} with the fast paths, {off} without (or the hits differ)")
        r = subprocess.run([a.pando, "--timing", "--count-only", d, q], capture_output=True, text=True)
        enumerated += "layer_deviations" in r.stderr
    failures += bad + (enumerated != len(dev_queries))
    print(f"{'ok  ' if not bad and enumerated == len(dev_queries) else 'FAIL'} deviations of thin layers: "
          f"{len(dev_queries) - bad} of {len(dev_queries)} equal to the generic path, {enumerated} enumerated")

    # DEPS round trip through the CoNLL-U output
    r = subprocess.run([a.pando, "--limit", "100000", "--format", "conllu", d, "[]"], capture_output=True, text=True)
    out = [l.split("\t") for l in r.stdout.splitlines() if l and not l.startswith("#")]
    src = [t for s in sentences(a.conllu) for t in s]
    bad = 0
    for t, o in zip(src, out):
        if any("." in e.split(":")[0] for e in t[8].split("|")):
            continue
        want = t[8] if t[8] != "_" else f"{t[6]}:{t[7]}"
        want = "|".join(sorted(want.split("|"), key=lambda e: (int(e.split(":")[0]), e.split(":", 1)[1])))
        bad += want != o[8]
    ok = len(out) == len(src) and bad == 0
    failures += not ok
    print(f"{'ok  ' if ok else 'FAIL'} DEPS in --format conllu: {len(out)} tokens, {bad} different")

    b = subprocess.run([a.pando, "--count-only", d, '[upos="VERB"] > [deprel="nsubj"]'], capture_output=True, text=True)
    h = subprocess.run([a.pando, "--count-only", d, '[upos="VERB"] >:head:nsubj []'], capture_output=True, text=True)
    if b.stdout.split()[-1:] != h.stdout.split()[-1:]:
        print(f"FAIL: >:head:nsubj {h.stdout.strip()} vs > [deprel=nsubj] {b.stdout.strip()}")
        failures += 1
    print("relation layers: " + ("as required" if not failures else f"{failures} problem(s)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
