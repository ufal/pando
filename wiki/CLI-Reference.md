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

```
pando-server <corpus_dir> [port] [threads] [--preload] [options]
```

| Option | Role |
| --- | --- |
| `port`, `threads` | Listen port (default 8765) and request threads |
| `--preload` | Read all index pages at startup (default: lazy mmap) |
| `--total-workers N` | Concurrent background counts (default 2) |
| `--result-cache N` | Cached query results / totals (default 512; unused finished ones evicted first) |
| `--result-ttl SEC` | Drop a finished result nobody asked about for SEC (default 3600) |
| `--abandon-after SEC` | Cancel a background count nobody polled for SEC (default 120; 0 = never) |
| `--debug-total-delay MS` | Testing: reveal every total gradually over MS, so a client can be tested against a "slow" count on a small corpus |

### Endpoints

| Endpoint | Role |
| --- | --- |
| `POST /query` | One query: `query`, `limit`, `offset`, `total`, `max_total`, `context`, `sentence`, `attrs`, `debug`, `strict_quoted_strings` |
| `GET /status?job=ID` | State of a background total (404 once it has expired: re-send the `/query`) |
| `POST /cancel?job=ID` | Stop a queued / running count (send a body, even `{}`, or `Content-Length: 0`) |
| `GET /jobs` | All cached results and running counts |
| `POST /run` | A full CQL program (named queries, `count`, `coll`, …) |
| `GET /info`, `/values/ATTR`, `/regions/TYPE`, `/context?pos=`, `/health` | Corpus description, values, regions, KWIC context |

### Totals: `"total": false | true | "async"`

* `false` — the page only; `page.total` is the number of hits found so far.
* `true` — the page and the exact total. A total computed before for the same
  query (same text, `max_total`, `strict_quoted_strings`) is reused: only the page
  is computed.
* `"async"` — the page at once; the exact total is counted in the background.
  The result has `"job": {...}` and `page.total` is the count so far
  (`total_exact: false`) until the job has finished. Poll `GET /status?job=ID`:

```json
{"ok": true, "job": {"id": "dd3abc781972108e", "state": "running", "finished": false,
 "total": 534076, "total_exact": false, "counted": 534076, "progress": 0.2891,
 "estimate": 1847062, "elapsed_ms": 238.0}}
```

`state` is `queued`, `running`, `finished`, `cancelled` or `failed` (`error`).
`counted` grows while running; `progress` is the share of the corpus scanned
and `estimate` the total extrapolated from it (both `null` when the query's
execution path does not report progress). The job id is derived from the query,
so every request for the same concordance finds the same job; when the page
already holds every hit the job is finished at once. Polling `/status` keeps a
job alive; one nobody asks about is cancelled after `--abandon-after`.

For KonText (the way Manatee concordances work there): `query_submit` sends
`"total": "async"` with `limit: 1` and reports `finished` = job finished; `view`
sends the page request (same query → same job, `concsize` = `page.total`);
`get_conc_cache_status` reads `/status` (`finished`, `concsize` = `total`) instead
of re-running the query.
