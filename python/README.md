# pando-cql

Python client for [pando](https://github.com/teitok/pando): run CQL via the `pando` CLI, get **hit tables** (one row per hit, any token or region attribute, corpus positions) and **count/group** results as rows for **pandas**.

- **No C++ changes** required — uses existing `pando --api --json`.
- Multi-field count **hierarchy stays in the CLI**; this package flattens leaf rows in Python only.
- **Counts are complete**: every combination of values is a row. pando's own answers stop at 20
  values under each group (a display limit); the package switches that off (`all_groups=False`
  keeps it). Before 0.2.1 multi-field counts were silently cut at those limits.
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

## Map layers for dialect data (`pando_geo`)

`pando_geo` turns variation in a geotagged corpus into GIS layers. An atlas file (TOML) names the
corpus, the attribute with each text's place and its coordinates (`text_place`, `text_geo`: "lat lon",
"lat,lon", a `geo:` URI or WKT `POINT(lon lat)`, as pando's `geodist()` reads them), and the
variables: a query whose hits vary, the field that holds the variant, and patterns that group its
values into variants. Per variable it writes, in EPSG:4326:

- `NAME_points.geojson`: one point per place, with `n`, `count_*` and `share_*` per variant and the
  `dominant` variant (places below `min_n` hits are left out);
- `NAME_voronoi.geojson`: the Voronoi (Thiessen) cell of each place, with the same properties: areas
  from where the data is, not from administrative borders (clipped to a box around the places);
- `NAME.svg`: a preview (cells coloured by the dominant variant, paler where it is less dominant);
- `NAME.html`: the same as a web map on OpenStreetMap (Leaflet; click a place for its counts). It loads
  Leaflet and the map tiles from the internet.

```bash
pip install -e './python[geo]'
pando-geo examples/archimob_atlas.toml --out maps        # or: python -m pando_geo …
```

Open the GeoJSON in QGIS (or any GIS); the example maps Swiss German *nicht* (nöd / nüd / nid / ned /
nit) and the diphthong of *gesagt* in the ArchiMob interviews.
