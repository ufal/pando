# pando-cql

Python client for [pando](https://github.com/teitok/pando): run CQL via the `pando` CLI, get **hit tables** (one row per hit, any token or region attribute, corpus positions) and **count/group** results as rows for **pandas**.

- **No C++ changes** required — uses existing `pando --api --json`.
- Multi-field count **hierarchy stays in the CLI**; this package flattens leaf rows in Python only.
- CQL `as` aliases are **not** required (use `DataFrame.rename`).

## Install

From the pando repo:

```bash
# Prefer a venv; editable install needs a writable site-packages
python3 -m venv .venv && source .venv/bin/activate
pip install -e './python[pandas,dev]'

# Fallback without install:
export PYTHONPATH=/path/to/pando/python/src
pip install pandas pytest   # as needed
```

Requires a built `pando` binary on `PATH`, or set `PANDO_BIN` (read automatically by `Corpus.open`).
`Corpus.open` fails early with a clear error if the binary is missing/unusable or if the path is not a pando index (no `corpus.info`).

## Quick example

```python
from pando_cql import Corpus
import pandas as pd

corp = Corpus.open("/path/to/corpus/index")
corp.run('Matches = [lemma="book"]')
df = pd.DataFrame(corp.run("count Matches by form, upos"))
df = (
    df.rename(columns={"form": "word", "upos": "pos"})
      .query("freq >= 2")
      .sort_values("freq", ascending=False)
)
print(df[["word", "pos", "freq"]])
corp.close()
```

Or:

```bash
export PANDO_BIN=/path/to/build/pando   # only if pando is not on PATH
PYTHONPATH=python/src python python/examples/pandas_count_filter.py /path/to/index
```

## Hit tables

`Corpus.table(query, fields)` runs a query and tabulates its hits: one row per hit, one
column per field (CQL `tabulate` fields: `h.lemma`, region attributes such as
`h.text_langcode`, and `h.cpos`, the token's corpus position, as an int).
`sample=` / `seed=` take a reproducible random sample, `limit=` caps the rows;
`.frame()` gives a pandas DataFrame, `.total` the number of hits of the query.

```python
from pando_cql import Corpus

corpus = Corpus("/path/to/ud_corpus")
t = corpus.table('h:[upos="VERB"] > d:[deprel="obj"]',
                 "h.text_langcode, h.cpos, d.cpos", sample=300000, seed=1).frame()
ov = (t["d.cpos"] < t["h.cpos"]).groupby(t["h.text_langcode"]).mean()   # share of OV per language
```

A complete example (Greenberg's word-order correlations across the languages of a UD
collection, in a dozen lines): `examples/word_order_slide.py`; the long version with
bootstrap intervals and without pandas: `examples/word_order_universals.py`.

## Notes

- Prefer `count … by form, upos` (attribute names). Token-qualified `a.form` can yield empty groups with some named-query patterns in current pando — use bare attrs or `Last` as in the CQL docs.
- `Corpus` keeps a **persistent** CLI session so named queries survive across `run()` and `table()` calls (`table()` queries can use labels of earlier statements). `session=False` runs every call in its own process.
- `run("set …")` answers with `{"ok": true, "operation": "set", …}` (pando since October 2026; older pando printed nothing in JSON mode, which made a session wait forever).
- Metric column is always `freq` in Python (JSON still uses `count`).

## Design tracker

See [`../dev/pando_cql_python.md`](../dev/pando_cql_python.md).
