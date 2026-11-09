#!/usr/bin/env python3
"""Input readers on the corner cases met converting Manatee corpora (2026-11-09):

  vertical (test/data/vertical/*.vert, self-authored)
    sentences.vert   <s id=… lang=…> keeps its attributes (s_id, s_lang), still the sentences
    empty_tags.vert  <x/> and <x a="v" /> are zero-width regions at the next token; nothing left open
    escapes.vert     attribute values with \\" \\\\ in double, ' in single quotes, XML entities, &#N;
    header.vert      <!-- positional-attributes: … --> gives those columns and no others (no upos)
    unclosed.vert    a <para> never closed ends before the next <para>, or at the end of the corpus;
                     a stray </sp> closes nothing; both warned about
    nested.vert      <x><x></x></x> stays nested
  JSONL v2           without any head: no relation.head / head_attrs; with heads: as before

Expected ranges are those of Manatee's encodevert (open 2.225.8: dumpstructrng, end exclusive there).
With --manatee-bin DIR (a built Manatee src/ folder with encodevert and dumpstructrng) they are checked
against encodevert itself too.

  test/vertical_reader_test.py --pando build/pando --pando-index build/pando-index [--manatee-bin DIR]
"""
import argparse, json, os, re, struct, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
VERT = os.path.join(HERE, "data", "vertical")

# per file: regions (inclusive ends; end = start - 1 is zero-width), region attribute values,
# corpus.info lines, stderr fragments; "manatee": compare the ranges with encodevert
CASES = {
    "sentences": {
        "rgn": {"s": [(0, 1), (2, 2)], "doc": [(0, 2)]},
        "val": {"s_id": ["s1", "s2"], "s_lang": ["en", "cs"]},
        "count": {'[] within <s lang="en"/>': 2, '<s id="s2"/>': 1},
        "manatee": True,
    },
    "empty_tags": {
        "rgn": {"pb": [(0, -1), (5, 4)], "gap": [(2, 1)], "p": [(3, 3)], "lb": [(4, 3)], "doc": [(0, 4)]},
        "val": {"pb_n": ["1", "2"], "gap_reason": ["noise"]},
        "info": {"structural": "doc,gap,lb,p,pb,s", "zerowidth": "gap,lb,pb"},
        "not_in_stderr": ["never closed", "zero-width regions"],
        "manatee": True,
    },
    "escapes": {
        "rgn": {"p": [(0, 0)], "doc": [(0, 0)]},
        "val": {"doc_title": ['say "hi"'], "p_id": ["a&b"], "p_note": ["it's"], "p_q": ['<x> "y"'],
                "p_bs": ["c\\d"], "p_sq": ["don't"], "p_num": ["čč"]},
        "manatee": True,
    },
    "header": {
        "info": {"positional": "form,lemma,phon,tag"},
        "count": {'[tag="NNMP1"]': 1, '[phon="liďi"]': 1},
    },
    "unclosed": {
        "rgn": {"doc": [(0, 6)], "sp": [(0, 1), (2, 2), (3, 4), (5, 5)], "para": [(0, 2), (3, 3), (5, 6)]},
        "val": {"para_type": ["a", "b", "c"]},
        "count": {"[] within <para/>": 6, '[] within <para type="a"/>': 3, "<para/>": 3},
        "stderr": ["unclosed.vert:3: <para> is never closed (its enclosing <sp> ends at line 6); "
                   "ended before the next <para> (line 11)",
                   "unclosed.vert:17: <para> is never closed (its enclosing <sp> ends at line 19); "
                   "ended at the end of the corpus",
                   "unclosed.vert:22: </sp> closes nothing"],
        "manatee": True,
    },
    "nested": {
        "rgn": {"x": [(0, 2), (1, 1)]},
        "val": {"x_n": ["1", "2"]},
        "info": {"nested": "x"},
        "not_in_stderr": ["never closed", "closes nothing"],
    },
}


def read_rgn(path):
    if not os.path.exists(path):
        return "missing"
    with open(path, "rb") as fh:
        data = fh.read()
    return [struct.unpack_from("<qq", data, i) for i in range(0, len(data), 16)]


def read_val(path):
    if not os.path.exists(path):
        return "missing"
    with open(path, "rb") as fh:
        return [v.decode("utf-8") for v in fh.read().split(b"\0")[:-1]]


def corpus_info(idx):
    with open(os.path.join(idx, "corpus.info"), encoding="utf-8") as fh:
        return dict(l.rstrip("\n").split("=", 1) for l in fh if "=" in l)


def manatee_ranges(bindir, vert, tmp, structures):
    """encodevert's ranges for `structures` (a registry made from the file's tags), ends inclusive."""
    attrs = {}
    for l in open(vert, encoding="utf-8"):
        m = re.match(r"<([A-Za-z_]\w*)(.*?)/?>\s*$", l)
        if m:
            attrs.setdefault(m.group(1), set()).update(re.findall(r"(\w+)=", m.group(2)))
    name = os.path.splitext(os.path.basename(vert))[0]
    reg, data = os.path.join(tmp, "reg_" + name), os.path.join(tmp, "manatee_" + name)
    with open(reg, "w") as fh:
        fh.write(f'PATH "{data}/"\nENCODING "UTF-8"\nATTRIBUTE word\nATTRIBUTE lemma\n')
        for s, a in attrs.items():
            fh.write(f"STRUCTURE {s} {{\n" + "".join(f"\tATTRIBUTE {x}\n" for x in sorted(a)) + "}\n")
    env = dict(os.environ)
    libs = [os.path.join(bindir, d) for d in (".libs", "../finlib/.libs", "../hat/.libs", "../lib/.libs")]
    for var in ("DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        env[var] = os.pathsep.join(libs + [env.get(var, "")])
    subprocess.run([os.path.join(bindir, "encodevert"), "-c", reg, vert], check=True, capture_output=True, env=env)
    out = {}
    for s in structures:
        raw = subprocess.run([os.path.join(bindir, "dumpstructrng"), reg, s, "-"], check=True,
                             capture_output=True, env=env).stdout
        nums = struct.unpack(f"<{len(raw) // 4}i", raw)
        out[s] = [(nums[i], nums[i + 1] - 1) for i in range(0, len(nums), 2)]
    return out


def count(pando, idx, q):
    p = subprocess.run([pando, "--cql", "manatee", idx, q, "--count-only"], capture_output=True, text=True)
    return p.stdout.strip() or p.stderr.strip()


def check_vertical(o, tmp, fails):
    for name, want in CASES.items():
        vert, idx = os.path.join(VERT, name + ".vert"), os.path.join(tmp, name)
        p = subprocess.run([o.pando_index, "--format", "vertical", vert, idx], capture_output=True, text=True)
        if p.returncode != 0:
            fails.append(f"{name}: pando-index failed: {p.stderr[-400:]}")
            continue
        for s, rows in want.get("rgn", {}).items():
            got = read_rgn(os.path.join(idx, s + ".rgn"))
            if got != rows:
                fails.append(f"{name}: {s}.rgn {got} != {rows}")
        for a, vals in want.get("val", {}).items():
            got = read_val(os.path.join(idx, a + ".val"))
            if got != vals:
                fails.append(f"{name}: {a}.val {got} != {vals}")
        info = corpus_info(idx)
        for k, v in want.get("info", {}).items():
            if info.get(k) != v:
                fails.append(f"{name}: corpus.info {k}={info.get(k)!r}, want {v!r}")
        for q, n in want.get("count", {}).items():
            got = count(o.pando, idx, q)
            if got != str(n):
                fails.append(f"{name}: {q} -> {got!r}, want {n}")
        for frag in want.get("stderr", []):
            if frag not in p.stderr:
                fails.append(f"{name}: no warning {frag!r} in:\n{p.stderr}")
        for frag in want.get("not_in_stderr", []):
            if frag in p.stderr:
                fails.append(f"{name}: unexpected {frag!r} in:\n{p.stderr}")
        if o.manatee_bin and want.get("manatee"):
            got = manatee_ranges(o.manatee_bin, vert, tmp, list(want["rgn"]))
            for s, rows in want["rgn"].items():
                if s != "s" and got[s] != rows:   # pando's s = its sentences
                    fails.append(f"{name}: encodevert {s} {got[s]} != {rows}")


def check_jsonl_heads(o, tmp, fails):
    header = {"type": "header", "version": 2, "positional": ["form", "lemma"], "structural": ["s"]}
    plain = [header, {"type": "token", "form": "a", "lemma": "a"}, {"type": "token", "form": "b", "lemma": "b"},
             {"type": "region", "struct": "s", "start_pos": 0, "end_pos": 1, "attrs": {"id": "s1"}}]
    headed = [dict(header, positional=["form", "lemma", "deprel"]),
              {"type": "token", "tok_id": "1", "head": 0, "form": "a", "lemma": "a", "deprel": "root"},
              {"type": "token", "tok_id": "2", "head_tok_id": "1", "form": "b", "lemma": "b", "deprel": "dep"},
              {"type": "region", "struct": "s", "start_pos": 0, "end_pos": 1, "attrs": {"id": "s1"}}]
    for name, events, want_heads in (("jsonl_plain", plain, False), ("jsonl_headed", headed, True)):
        src, idx = os.path.join(tmp, name + ".jsonl"), os.path.join(tmp, name)
        with open(src, "w") as fh:
            fh.writelines(json.dumps(e) + "\n" for e in events)
        subprocess.run([o.pando_index, "--format", "jsonl", src, idx], check=True, capture_output=True)
        info = corpus_info(idx)
        has = "relation.head" in info
        if has != want_heads:
            fails.append(f"{name}: relation.head {'present' if has else 'missing'}: {info}")
        if not want_heads and info.get("head_attrs"):
            fails.append(f"{name}: head_attrs={info['head_attrs']} without heads")
        if not want_heads and os.path.exists(os.path.join(idx, "dep.head")):
            fails.append(f"{name}: dep.head written without heads")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--manatee-bin", help="Manatee src/ folder: check the expected ranges with encodevert too")
    o = ap.parse_args()
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        check_vertical(o, tmp, fails)
        check_jsonl_heads(o, tmp, fails)
    for f in fails:
        print("FAIL", f)
    if fails:
        sys.exit(1)
    print("PASS vertical_reader" + (" (ranges checked with encodevert)" if o.manatee_bin else ""))


if __name__ == "__main__":
    main()
