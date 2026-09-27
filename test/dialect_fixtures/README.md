# Dialect fixtures (P0.4)

Reference results from real CQP (CWB) and Manatee for `pando --cql cwb` and
`pando --cql manatee`: the dialect front-ends should accept the same queries
and give the same answers (totals *and* the same `match … matchend` hits) as
the engine they emulate. This matters most for repetition and optional tokens
(`[A] []{0,3} [B]`, `[A] [B]? [C]`, `[A] []+ [C]`), where the engines have
their own matching strategies and repetition limits.

| File | What |
|------|------|
| `queries.tsv` | the fixture queries, with a CQP and a Manatee column |
| `run_cqp.py` | runs them on CQP, writes `expected/cqp-<corpus>.jsonl` |
| `run_manatee.py` | runs them on Manatee (python `manatee` module), writes `expected/manatee-<corpus>.jsonl` |
| `kontext_runner.js` | runs them on Manatee through a KonText site's `/view?…&format=json` (browser console) |
| `compare_pando.py` | runs them on pando and compares with a reference file |
| `fixtures.py` | shared helpers; describes the record format |
| `expected/` | committed reference results |

## 1. Same corpus in all engines

Export the pando index to a vertical file, so CQP / Manatee corpus positions
are identical to pando's (one line per token, same structures):

```sh
scripts/pando_export_vrt.py path/to/ud_demo ud_demo.vrt.gz \
    --attrs form,lemma,upos,deprel --structs "text:langcode,treebank;doc;s" --write-configs
scripts/check_vrt_export.py path/to/ud_demo ud_demo.vrt.gz \
    --attrs form,lemma,upos,deprel --structs text,doc,s          # optional round-trip check
```

`--write-configs` writes `ud_demo.vrt.gz.cwb-encode.sh` and
`ud_demo.vrt.gz.manatee`; replace `DATA_DIR`, `REGISTRY_DIR` and `VRT_FILE`.
The first column (`form`) is CQP's / Manatee's `word`.

CWB:

```sh
mkdir -p ~/cwb/data/ud_demo ~/cwb/registry
gunzip -c ud_demo.vrt.gz | cwb-encode -x -s -B -c utf8 -d ~/cwb/data/ud_demo \
    -R ~/cwb/registry/ud_demo -P lemma -P upos -P deprel \
    -S text:0+langcode+treebank -S doc:0 -S s:0
cwb-makeall -r ~/cwb/registry -V UD_DEMO
```

Manatee: edit the registry file (PATH, VERTICAL = the *uncompressed* .vrt, or
a `| gunzip -c …` pipe if your encodevert accepts one), then `encodevert -c
<registry file>`.

## 2. Record the references

```sh
test/dialect_fixtures/run_cqp.py --registry ~/cwb/registry --corpus UD_DEMO \
    --out test/dialect_fixtures/expected/cqp-ud_demo.jsonl
MANATEE_REGISTRY=/path/to/registry test/dialect_fixtures/run_manatee.py \
    --corpus ud_demo --out test/dialect_fixtures/expected/manatee-ud_demo.jsonl
```

Both use the engine's default settings (CQP: `--strategy`, `--hard-boundary`
to override); the settings in effect are stored in the file's first line.
A query the engine rejects is recorded as an error — also a useful fixture
(the pando dialect should reject it as well, or knowingly extend it).

## 3. Compare pando

```sh
test/dialect_fixtures/compare_pando.py --pando build/pando --corpus path/to/ud_demo \
    --expected test/dialect_fixtures/expected/cqp-ud_demo.jsonl
test/dialect_fixtures/compare_pando.py --pando build/pando --corpus path/to/ud_demo \
    --expected test/dialect_fixtures/expected/cqp-ud_demo.jsonl --dialect native   # how far is native?
```

For each query: reference total, pando total, whether the (match, matchend)
sets are identical (sha256), and on a mismatch examples of extra / missing hits
among the first 100000 corpus positions. Above `--max-dump` hits (default 5M)
only totals are compared.

## Manatee through KonText (what `expected/manatee-ud_ewt.jsonl` is)

The local kontext-pando KonText serves `ud_ewt_manatee` (UD English EWT,
254 820 tokens) from Manatee. Its positions are ud_demo positions
9659667 … 9914486 (texts en_ewt dev, test, train), so the matching pando index
is an export of that range:

```sh
scripts/pando_export_vrt.py path/to/ud_demo ewt.vrt --range 9659667:9914487 \
    --attrs form,lemma,upos,deprel --structs "doc;s" --escape minimal --header
pando-index --format vertical ewt.vrt path/to/ud_ewt
test/dialect_fixtures/compare_pando.py --pando build/pando --corpus path/to/ud_ewt \
    --expected test/dialect_fixtures/expected/manatee-ud_ewt.jsonl --dialect native
```

The reference was recorded with `kontext_runner.js` in a browser tab on the
KonText site (`runFixtures("ud_ewt_manatee", QUERIES)`). KonText cuts the KWIC
of hits longer than 50 tokens (`kwic_cap` in the file's meta line), so the
comparison caps pando's match ends the same way. `ud_ewt_manatee` has no `s`
or `text` structures: those fixtures are recorded as Manatee errors.

## Manatee on the full corpus (`expected/manatee-ud.jsonl`)

KonText's `ud_manatee` is ud_demo compiled in Manatee (same 38 181 322
positions as the pando index behind `ud_pando`). Hit sets are recorded for
queries with at most 500 000 hits, totals for the rest; `first_ms` is the
KonText request time. Compare with pando's native syntax for the structure
queries (the CQP column spells `within s`, `:: match.text_langcode=…`):

```sh
test/dialect_fixtures/compare_pando.py --pando build/pando --corpus path/to/ud_demo \
    --expected test/dialect_fixtures/expected/manatee-ud.jsonl --dialect native --pando-column cqp
```

## CQP on the full corpus (`expected/cqp-ud.jsonl`)

`ud_cwb` is the same corpus encoded for CWB (word lemma upos xpos deprel feats,
`s`, `text` {id, langcode, treebank}). On a machine with `cqp`:

```sh
test/dialect_fixtures/run_cqp.py --registry /Volumes/Data2/Corpora/kontext-pando/cwb/registry \
    --corpus UD_CWB --out test/dialect_fixtures/expected/cqp-ud.jsonl
test/dialect_fixtures/compare_pando.py --pando build/pando --corpus path/to/ud_demo \
    --expected test/dialect_fixtures/expected/cqp-ud.jsonl --dialect native
```

Hit lists are tabulated for totals up to `--max-hits` (default 5M); `size_seconds`
is CQP's time for the query and its total.
