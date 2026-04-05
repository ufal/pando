# CLI reference (short)

Exact options change over time; always run **`pando --help`**, **`pando-index --help`**, **`pando-check --help`**, **`pando-server --help`**.

## `pando`

| Flag | Role |
| --- | --- |
| `--json` / `--api` | Structured JSON output |
| `--total` | Compute exact total matches with `--limit` |
| `--limit`, `--offset` | Pagination |
| `--context` | KWIC width |
| `--no-mv-explode` | `count` / `freq`: keep composite multivalue keys (see wiki) |
| `--debug` | Query plan / timing info |
| `--threads` | Parallelism for multi-token queries |
| `--cql` | Dialect front-end (native / optional modules) |
| Collocation / `dcoll` | `--window`, `--left`, `--right`, `--min-freq`, `--measures`, `--max-items` (see [Collocations and keyness](Collocations-and-Keyness.md)) |

Interactive REPL supports `set`, `show settings`, etc.

## `pando-index`

Builds or updates a corpus directory from input (JSONL). See `dev/` and `docs/SAMPLE-CORPUS.md`.

## `pando-check`

Validates layout and internal consistency.

## `pando-server`

HTTP JSON API over the same engine (when enabled in build).

## Wiki preview

```bash
python scripts/serve-wiki-preview.py
```

Open http://localhost:8765/
