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
sets are identical (md5), and on a mismatch examples of extra / missing hits
among the first 100000 corpus positions. Above `--max-dump` hits (default 5M)
only totals are compared.
