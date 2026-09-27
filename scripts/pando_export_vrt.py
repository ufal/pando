#!/usr/bin/env python3
"""Export a pando index as a vertical (VRT) file for CWB/CQP or Manatee.

Used to build reference corpora for the dialect fixtures (test/dialect_fixtures):
the exported corpus has exactly the same tokens (one line per corpus position,
so CQP / Manatee corpus positions equal pando positions) and the same
structures as the pando index.

  scripts/pando_export_vrt.py CORPUS_DIR ud_demo.vrt.gz
  scripts/pando_export_vrt.py CORPUS_DIR ud_demo.vrt.gz --attrs form,lemma,upos,deprel \\
      --structs "text:id,langcode;s:id"

It also prints (and with --write-configs writes next to the output) the
`cwb-encode` command line and a Manatee registry file for the same columns.

Token fields are XML-escaped (& < > as &amp; &lt; &gt;), so tokens such as
`</>` or `<3` cannot be mistaken for tags; encode with `cwb-encode -x`, which
decodes the entities again. Tabs / newlines inside values become spaces.
Needs numpy.
"""

import argparse
import gzip
import os
import re
import sys

import numpy as np

DEFAULT_STRUCT_ATTRS = ("id", "langcode", "lang", "treebank", "genre")


def read_info(corpus):
    info = {}
    with open(os.path.join(corpus, "corpus.info"), encoding="utf-8") as f:
        for line in f:
            if "=" in line:
                k, v = line.rstrip("\n").split("=", 1)
                info[k.strip()] = v.strip()
    return info


def read_strings(data_path, idx_path):
    """Strings stored as NUL-terminated entries + int64 offsets (n + 1)."""
    idx = np.fromfile(idx_path, dtype=np.int64)
    raw = open(data_path, "rb").read()
    out = np.empty(len(idx) - 1, dtype=object)
    for i in range(len(idx) - 1):
        out[i] = raw[idx[i]:idx[i + 1] - 1].decode("utf-8", "replace")
    return out


def esc(s):
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return s.replace("\t", " ").replace("\n", " ").replace("\r", " ")


def esc_attr(s):
    return esc(s).replace('"', "&quot;")


def load_dat(path, n):
    size = os.path.getsize(path)
    width = size // n
    dtype = {1: np.uint8, 2: np.uint16, 4: np.int32}.get(width)
    if dtype is None or width * n != size:
        raise SystemExit(f"{path}: unexpected .dat size {size} for {n} tokens")
    return np.memmap(path, dtype=dtype, mode="r")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("corpus", help="pando index directory")
    ap.add_argument("output", help="output .vrt, .vrt.gz, or - for stdout")
    ap.add_argument("--attrs", help="positional attrs, comma-separated; the first becomes CQP's "
                    "`word` (default: form first, then all other positional attrs)")
    ap.add_argument("--structs", help='structures and their attrs, e.g. "text:id,langcode;doc:id;s:id" '
                    "(default: text, doc, par, s with id/langcode/lang/treebank/genre where present)")
    ap.add_argument("--name", default="ud_demo", help="corpus name for the configs (default ud_demo)")
    ap.add_argument("--write-configs", action="store_true",
                    help="write <output>.cwb-encode.sh and <output>.manatee next to the output")
    ap.add_argument("--chunk", type=int, default=1 << 20)
    opts = ap.parse_args()

    corpus = opts.corpus
    info = read_info(corpus)
    n = int(info["size"])
    positional = [a for a in info.get("positional", "").split(",") if a]
    structural = [s for s in info.get("structural", "").split(",") if s]
    region_attrs = [r for r in info.get("region_attrs", "").split(",") if r]

    if opts.attrs:
        attrs = [a.strip() for a in opts.attrs.split(",") if a.strip()]
    else:
        attrs = (["form"] if "form" in positional else []) + [a for a in positional if a != "form"]
    attrs = [a for a in attrs if os.path.exists(os.path.join(corpus, a + ".dat"))]
    if not attrs:
        raise SystemExit("no positional attributes found")

    if opts.structs:
        structs = []
        for part in opts.structs.split(";"):
            part = part.strip()
            if not part:
                continue
            name, _, al = part.partition(":")
            structs.append((name.strip(), [a.strip() for a in al.split(",") if a.strip()]))
    else:
        structs = []
        for s in ("text", "doc", "par", "s"):
            if s in structural:
                structs.append((s, [a for a in DEFAULT_STRUCT_ATTRS if f"{s}_{a}" in region_attrs]))

    # load token columns
    cols = []
    for a in attrs:
        lex = read_strings(os.path.join(corpus, a + ".lex"), os.path.join(corpus, a + ".lex.idx"))
        lex = np.array([esc(s) for s in lex], dtype=object)
        cols.append((lex, load_dat(os.path.join(corpus, a + ".dat"), n)))

    # structure events: opens before a token, closes after a token
    opens, closes = [], []   # (pos, order, text)
    for order, (sname, sattrs) in enumerate(structs):
        rg = np.fromfile(os.path.join(corpus, sname + ".rgn"), dtype=np.int64).reshape(-1, 2)
        vals = []
        for a in sattrs:
            base = os.path.join(corpus, f"{sname}_{a}")
            if not os.path.exists(base + ".val"):
                raise SystemExit(f"missing {base}.val")
            v = read_strings(base + ".val", base + ".val.idx")
            if len(v) != len(rg):
                raise SystemExit(f"{base}.val: {len(v)} values for {len(rg)} regions")
            vals.append((a, v))
        for i in range(len(rg)):
            s, e = int(rg[i, 0]), int(rg[i, 1])
            if e < s:
                continue
            at = "".join(f' {a}="{esc_attr(v[i])}"' for a, v in vals)
            opens.append((s, order, f"<{sname}{at}>"))
            closes.append((e, -order, f"</{sname}>"))
    opens.sort()
    closes.sort()

    out = sys.stdout if opts.output == "-" else (
        gzip.open(opts.output, "wt", encoding="utf-8", compresslevel=3) if opts.output.endswith(".gz")
        else open(opts.output, "w", encoding="utf-8"))
    oi = ci = 0
    for c0 in range(0, n, opts.chunk):
        c1 = min(n, c0 + opts.chunk)
        fields = [lex[np.asarray(dat[c0:c1], dtype=np.int64)] for lex, dat in cols]
        lines = ["\t".join(t) for t in zip(*fields)] if len(fields) > 1 else list(fields[0])
        buf = []
        p = c0
        while p < c1:
            # next position with an event in this chunk
            nxt = c1
            if oi < len(opens) and opens[oi][0] < nxt:
                nxt = max(opens[oi][0], p)
            if ci < len(closes) and closes[ci][0] + 1 < nxt:
                nxt = max(closes[ci][0] + 1, p)
            if nxt > p:
                buf.append("\n".join(lines[p - c0:nxt - c0]))
                buf.append("\n")
                p = nxt
                continue
            # at p: closes of regions ending at p-1, then opens at p, then the token
            while ci < len(closes) and closes[ci][0] + 1 == p:
                buf.append(closes[ci][2] + "\n")
                ci += 1
            while oi < len(opens) and opens[oi][0] == p:
                buf.append(opens[oi][2] + "\n")
                oi += 1
            buf.append(lines[p - c0] + "\n")
            p += 1
        out.write("".join(buf))
    while ci < len(closes):
        out.write(closes[ci][2] + "\n")
        ci += 1
    if out is not sys.stdout:
        out.close()

    # configs
    word, rest = attrs[0], attrs[1:]
    s_opts = " ".join("-S " + sname + ":0" + "".join("+" + a for a in sa) for sname, sa in structs)
    cwb = (f"# column 1 ({word}) is CQP's `word`\n"
           f"mkdir -p DATA_DIR/{opts.name}\n"
           f"cwb-encode -x -s -B -c utf8 -d DATA_DIR/{opts.name} -f VRT_FILE "
           f"-R REGISTRY_DIR/{opts.name.lower()} "
           + " ".join(f"-P {a}" for a in rest) + " " + s_opts + "\n"
           f"cwb-makeall -r REGISTRY_DIR -V {opts.name.upper()}\n")
    man = [f'NAME "{opts.name}"', f'PATH "DATA_DIR/manatee/{opts.name}/"',
           'VERTICAL "VRT_FILE"', 'ENCODING "utf-8"', 'LANGUAGE "multilingual"',
           "DEFAULTATTR word", "", "ATTRIBUTE word", ""]
    for a in rest:
        man += [f"ATTRIBUTE {a}", ""]
    for sname, sa in structs:
        if not sa:
            man += [f"STRUCTURE {sname}", ""]
            continue
        man.append(f"STRUCTURE {sname} {{")
        for a in sa:
            man.append(f"    ATTRIBUTE {a}")
        man += ["}", ""]
    man_txt = "\n".join(man) + "\n# build: encodevert -c THIS_FILE   (VRT_FILE uncompressed, or a pipe)\n"
    sys.stderr.write(f"exported {n} tokens, columns: {', '.join(attrs)}; "
                     f"structures: {', '.join(s for s, _ in structs)}\n\n{cwb}\n")
    if opts.write_configs and opts.output != "-":
        with open(opts.output + ".cwb-encode.sh", "w") as f:
            f.write(cwb)
        with open(opts.output + ".manatee", "w") as f:
            f.write(man_txt)


if __name__ == "__main__":
    main()
