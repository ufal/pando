# Quick start

## 1. Build

See [Installation](Installation.md).

## 2. Prepare a corpus

Pando reads a **directory** of mmap files plus `corpus.info`, not raw CoNLL-U directly. Typical flow:

1. Convert or emit **JSONL** in the event format expected by `pando-index` (see [Index and corpus layout](Index-and-Corpus-Layout.md)).
2. Run `pando-index` to produce a corpus directory.

The repository includes small samples and docs:

- [../docs/SAMPLE-CORPUS.md](../docs/SAMPLE-CORPUS.md) — UD sample and indexing steps.
- `test/data/` — example inputs for tests and experiments.
- `dev/sample-rich-events.jsonl` — richer structural / multivalue fixture (see `dev/` notes).

## 3. Run a query

```bash
./build/pando /path/to/corpus '[lemma="book"]' --limit 5
```

If the first argument is not a corpus path, the CLI may fall back to `./pando` or `.` when `corpus.info` exists (TEITOK-style layout); see `pando --help` for the exact rules.

Useful flags:

- `--json` — structured hits
- `--total` — exact total count even when limiting displayed hits
- `--no-mv-explode` — for `count` / `freq`, keep pipe-joined multivalue keys (see [Multivalue attributes](Multivalue-Attributes.md))

## 4. Read the query language

The full tutorial is [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md). Topic wikis:

- [Dependency queries](Dependency-Queries.md)
- [Multivalue attributes](Multivalue-Attributes.md)
- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md)
- [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md)

## 5. Preview this wiki locally

```bash
python scripts/serve-wiki-preview.py
```

Open http://localhost:8765/
