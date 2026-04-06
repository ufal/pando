# CLI reference

Exact options change over time; always run **`pando --help`**, **`pando-index --help`**, **`pando-check --help`**, **`pando-server --help`** for the installed build.

## `pando`

### Output and format

| Flag | Role |
| --- | --- |
| `--version`, `-V` | Print version and exit |
| `--json` | Structured JSON output |
| `--conllu` | Text hits: full sentence per match as CoNLL-U (needs sentence structure `s`) |
| `--format json\|conllu` | Same as `--json` / `--conllu` |
| `--api` | API-style JSON: single-object responses (implies JSON) |
| `--debug[=N]` | Debug info (plan, timing, cardinalities); optional level `N` |

### Query language front-end

| Flag | Role |
| --- | --- |
| `--cql native\|pmltq\|tiger` | Dialect (default: `native`). Optional: `cwb` when built with CWB dialect. |
| `--pmltq-export-sql` | With `--cql pmltq`: emit ClickPMLTQ SQL only (skips corpus load; see `--help` for env) |

### Hits, totals, and performance

| Flag | Role |
| --- | --- |
| `--total` | Exact total match count even when `--limit` truncates displayed hits |
| `--max-total N` | Cap total count when using `--total` |
| `--limit N` | Max hits to return (default: 20) |
| `--offset N` | Skip first N hits |
| `--context N` | Context width in tokens for JSON (default: 5) |
| `--attrs A,B,...` | Token attributes in output (text: `/attr`; JSON: fields; defaults differ by mode) |
| `--count-only` | Print only the match count |
| `--timing` | Print timing on stderr (`open_sec`, `query_sec`, …) |
| `--sample N` | Random sample of N matches (reservoir sampling) |
| `--seed N` | RNG seed for `--sample` (reproducible runs) |
| `--threads N` | Parallel seed expansion for multi-token queries (default: 1) |
| `--preload` | Load mmap pages eagerly at corpus open (slower open, can speed first queries) |

### Quoting and string semantics

| Flag | Role |
| --- | --- |
| `--strict-quoted-strings` | In native CQL, only `/pattern/` uses regex; **`"..."` string literals are always literal** (no CWB-style “regex-looking” heuristic inside quotes). See [PANDO-CQL.md](PANDO-CQL.md) and `quoted_string_pattern` in sources. |
| `--max-gap N` | Cap for `+` and `*` token repetition (default: large internal cap; see `--help`) |

### Multivalue and aggregates

| Flag | Role |
| --- | --- |
| `--no-mv-explode` | For `count` / `freq`: keep pipe-joined multivalue keys (no per-component buckets). See [Multivalue attributes](Multivalue-Attributes.md). |

### Collocation / `coll` / `dcoll`

| Flag | Role |
| --- | --- |
| `--window N` | Symmetric window (default: 5) |
| `--left N`, `--right N` | Asymmetric window (override `--window`) |
| `--min-freq N` | Minimum co-occurrence frequency (default: 5) |
| `--max-items N` | Max collocates to list (default: 50) |
| `--measures M,...` | e.g. `logdice`, `mi`, `mi3`, `tscore`, `ll`, `dice` (default: `logdice`) |

More detail: [Collocations and keyness](Collocations-and-Keyness.md).

### Interactive REPL

When stdin is a TTY and no query is given, `pando` runs an interactive loop (`set`, `show settings`, etc.). Settings there may mirror some of the flags above (e.g. limits for grouped output).

## `pando-index`

Builds or updates a corpus directory from JSONL (or streaming integration). See [Sample corpora](Sample-Corpora.md) and [Index and corpus layout](Index-and-Corpus-Layout.md).

## `pando-check`

Validates layout and internal consistency.

## `pando-server`

HTTP JSON API over the same engine (when enabled in the build).
