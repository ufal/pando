#!/usr/bin/env python3
"""Check a VRT made by pando_export_vrt.py against the pando index it came from:
token count, every value of the given columns, and each structure's region
boundaries (positions of <s> ... </s> etc.).

  scripts/check_vrt_export.py CORPUS_DIR ud_demo.vrt.gz --attrs form,deprel,lemma,upos --structs text,doc,s
"""
import argparse, gzip, html, os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pando_export_vrt import read_info, read_strings, load_dat  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("corpus"); ap.add_argument("vrt")
ap.add_argument("--attrs", required=True); ap.add_argument("--structs", required=True)
o = ap.parse_args()
n = int(read_info(o.corpus)["size"])
attrs = o.attrs.split(","); structs = o.structs.split(",")
cols = [(read_strings(f"{o.corpus}/{a}.lex", f"{o.corpus}/{a}.lex.idx"), load_dat(f"{o.corpus}/{a}.dat", n)) for a in attrs]
starts = {s: [] for s in structs}; ends = {s: [] for s in structs}
f = gzip.open(o.vrt, "rt", encoding="utf-8") if o.vrt.endswith(".gz") else open(o.vrt, encoding="utf-8")
pos = 0; bad = 0; chunk = []
def flush():
    global bad
    if not chunk: return
    p0 = pos - len(chunk)
    for k, (lex, dat) in enumerate(cols):
        want = lex[np.asarray(dat[p0:pos], dtype=np.int64)]
        got = [html.unescape(r[k]) if k < len(r) else None for r in chunk]
        for i, (g, w) in enumerate(zip(got, want)):
            if g != w.replace("\t", " ").replace("\n", " ").replace("\r", " "):
                bad += 1
                if bad <= 5: print(f"pos {p0+i} {attrs[k]}: vrt={g!r} index={w!r}")
    chunk.clear()
for line in f:
    line = line.rstrip("\n")
    if line.startswith("</"):
        ends[line[2:-1]].append(pos - 1); continue
    if line.startswith("<"):
        name = line[1:].split(" ", 1)[0].rstrip(">"); starts[name].append(pos); continue
    chunk.append(line.split("\t")); pos += 1
    if len(chunk) >= 1 << 20: flush()
flush()
ok = pos == n and bad == 0
print(f"tokens: vrt={pos} index={n}; value mismatches: {bad}")
for s in structs:
    rg = np.fromfile(f"{o.corpus}/{s}.rgn", dtype=np.int64).reshape(-1, 2)
    rg = rg[rg[:, 1] >= rg[:, 0]]
    same = len(starts[s]) == len(rg) and np.array_equal(np.array(starts[s]), rg[:, 0]) and np.array_equal(np.array(ends[s]), rg[:, 1])
    print(f"{s}: {len(starts[s])} regions, boundaries {'OK' if same else 'DIFFER'}")
    ok = ok and same
sys.exit(0 if ok else 1)
