# Quick start

## 1. Build

See [Installation](Installation.md).

## 2. Prepare a corpus

Pando reads a **directory** of mmap files plus `corpus.info`, not raw CoNLL-U directly. Typical flow:

1. Convert or emit **JSONL** in the event format expected by `pando-index` (see [Index and corpus layout](Index-and-Corpus-Layout.md)).
2. Run `pando-index` to produce a corpus directory.

The repository includes small samples; see [Sample corpora](Sample-Corpora.md) for all three: bundled CoNLL-U, JSONL fixture, and the full-UD download script.

- `test/data/` — example inputs for tests and experiments (including `sample.conllu`).
- Rich JSONL fixture: run **`python scripts/gen_sample_rich_jsonl.py`**, then index the file it writes (see [Sample corpora](Sample-Corpora.md)).

## 3. Run a query

```bash
./build/pando /path/to/corpus '[lemma="book"]' --limit 5
```

If the first argument is not a corpus path, the CLI resolves the corpus from `./pando` or `.` when `corpus.info` exists (TEITOK-style layout); see `pando --help` for the resolution order.

Useful flags:

- `--json` — structured hits
- `--total` — exact total count even when limiting displayed hits
- `--no-mv-explode` — for `count` / `freq`, keep pipe-joined multivalue keys (see [Multivalue attributes](Multivalue-Attributes.md))

## 4. Read the query language

The full tutorial is [PANDO-CQL.md](PANDO-CQL.md) (in this folder). Topic wikis:

- [Dependency queries](Dependency-Queries.md)
- [Multivalue attributes](Multivalue-Attributes.md)
- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md)
- [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md)
