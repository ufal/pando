"""Shared helpers for the dialect fixtures (queries.tsv, result records).

A result record (one JSON object per line in a results .jsonl file):

  {"id": "R01", "query": "...", "engine": "cqp", "total": 2498881,
   "unique": 2498881, "sha256": "…", "head": [[m, e], ...], "window": [[m, e], ...], "error": null,
   "seconds": 1.2}

  total   number of hits the engine reports (CQP `size`, Manatee conc.size(),
          pando total)
  unique  distinct (match, matchend) pairs; both positions inclusive corpus
          positions of the first and the last matched token
  sha256  sha256 of the sorted distinct pairs, one "match matchend\\n" per pair
          (null when the hit list was too large to collect)
  head    the first HEAD (50) sorted distinct pairs
  window  all distinct pairs with match < WINDOW (default 100000), sorted —
          enough to see *how* two engines differ when the hashes disagree
"""

import hashlib
import json
import os

import numpy as np

WINDOW = 100_000
HEAD = 50
MAX_PAIRS = 30_000_000


def load_queries(path, engine):
    """[(id, query, note)] for engine in {"cqp", "manatee"}; skips "-" entries."""
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("\t")
            while len(parts) < 4:
                parts.append("")
            qid, cqp, man, note = parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()
            q = cqp if engine == "cqp" else (cqp if man == "=" else man)
            if not q or q == "-":
                continue
            out.append((qid, q, note))
    return out


def record(qid, query, engine, starts, ends, total=None, window=WINDOW, seconds=None, extra=None):
    """Build a result record from match / matchend arrays (inclusive)."""
    m = np.asarray(starts, dtype=np.int64)
    e = np.asarray(ends, dtype=np.int64)
    rec = {"id": qid, "query": query, "engine": engine,
           "total": int(total if total is not None else len(m)), "error": None}
    if len(m) > MAX_PAIRS:
        rec.update(unique=None, sha256=None, head=None, window=None)
    else:
        pairs = np.unique(np.stack([m, e], axis=1), axis=0) if len(m) else np.zeros((0, 2), np.int64)
        h = hashlib.sha256()
        for c in range(0, len(pairs), 1 << 20):
            blk = pairs[c:c + (1 << 20)]
            h.update("".join(f"{a} {b}\n" for a, b in blk.tolist()).encode())
        rec["unique"] = int(len(pairs))
        rec["sha256"] = h.hexdigest()
        rec["head"] = pairs[:HEAD].tolist()
        rec["window"] = pairs[pairs[:, 0] < window].tolist()
    if seconds is not None:
        rec["seconds"] = round(seconds, 3)
    if extra:
        rec.update(extra)
    return rec


def error_record(qid, query, engine, msg, seconds=None):
    rec = {"id": qid, "query": query, "engine": engine, "total": None, "unique": None,
           "sha256": None, "head": None, "window": None, "error": msg.strip()[:2000]}
    if seconds is not None:
        rec["seconds"] = round(seconds, 3)
    return rec


def write_jsonl(path, meta, records):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"meta": meta}, ensure_ascii=False) + "\n")
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")


def read_jsonl(path):
    meta, recs = {}, {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            if "meta" in obj:
                meta = obj["meta"]
            else:
                recs[obj["id"]] = obj
    return meta, recs
