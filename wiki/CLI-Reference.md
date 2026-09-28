# CLI reference

Exact options change over time; always run **`pando --help`**, **`pando-index --help`**, **`pando-check --help`**, **`pando-server --help`** for the installed build (and `--version` to see which build that is).

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
| `--threads N` | Count a total (`--total`, `--count-only`) or a `count by` over N position ranges in parallel; same result as one thread, first page in the same order (default: 1). See [Parallel counting](#parallel-counting-threads) |
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
| `--query-threads N` | Split every counting query — a `/query` total, a background (`"async"`) total, a `/run` `count by` — over N position ranges counted in parallel (default 1: one thread per query). `/health` reports it as `query_threads` |
| `--query-timeout MS` | Default time limit of a `/query` or `/run` (0 = none; per request `"timeout_ms"`): 408 `timed_out` |
| `--session-ttl SEC` | Close a client session nobody used for SEC (default 1800) |
| `--max-sessions N` | Open client sessions (default 256; a new one closes the least recently used idle one) |
| `--session-memory MB` | Materialised hits over all sessions (default 2048; 0 = no limit). Above it, the hits of the least recently used sessions are dropped — their sets stay and are rebuilt on demand |
| `--limits FILE` | Server options as JSON, above all limits by tier (see "Limits by tier" below): `{"tiers": {…}, "default_tier": "visitor", "trust_tier": true}`. Also takes the other options of the C ABI (`query_timeout_ms`, `session_max_hits`, …); flags given before it are the defaults |
| `--trust-tier` | Honour the request's `"tier"`. Only for a server that clients cannot reach directly (behind KonText / FQS, which set the tier) |
| `--session-max-hits N` | Hits one stored set may materialise (default 5000000; 0 = no limit): above it `sort` / `coll` / … answer 413 `too_large` |

### Parallel counting (`--threads`)

`pando --threads N` and `pando-server --query-threads N` (P4.1) split a query
that has to see every hit — an exact total, a background total, a
`… ; count by …` — into up to N ranges of corpus positions and count them at
the same time. The corpus stays one corpus with one position space; the ranges
exist only for the duration of the query:

- ranges are cut at sentence starts (`s`), so a dependency tree is never split;
  a sequence belongs to the range that holds its first token, so a match that
  runs over a cut is counted once;
- the result is the one-thread result: the same total, the same `count by`
  buckets, and the first page in the same order (the ranges' pages are joined in
  position order);
- a short first range runs alone first. Paths that count in O(#values) anyway
  (a single EQ token, a regex / `%c` id set, `[H] > [C]` edge postings) are not
  split, nor are queries whose path cannot be restricted to a range (anchors,
  some region filters): they run as before, with no extra work;
- a page without a total (`--limit 20` alone) is never split: it stops at its
  first hits.

Worth it for long counts on large corpora; on a server that already runs many
queries at once, more request threads may be the better use of the cores
(`--query-threads` defaults to 1). Ranges smaller than 2^20 tokens are not
made (`PANDO_PARTITION_MIN` overrides, for tests on small corpora).

**Progressive pages.** A page without a total of a token sequence with a huge
complex operand and no rare token (`[word=".*a.*"] [word=".*e.*"]`: two regexes
matching millions of tokens each) is found range by range, in sentence-aligned
ranges of 64K, 256K, 1M, … tokens, each building only its part of the operands,
until the page is full: 20 hits in ~20 ms instead of ~0.5 s once the regexes'
lexicon scans are cached (the first time, the scan itself still costs 0.1–1 s).
The page is the plain page, in the same order. Not for dependency relations,
an optional first token, unbounded repeats or post-filters (`containing`,
`not within`, `:: a < b`, …): those run as before. `PANDO_PROGRESSIVE_WINDOW`
(first range) and `PANDO_PROGRESSIVE_MIN` (the operand size that triggers it,
default 2^20 tokens) override, for tests. `--timing` reports the ranges as
`parts=`.

### Endpoints

| Endpoint | Role |
| --- | --- |
| `POST /query` | One query: `query`, `limit`, `offset`, `total`, `max_total`, `context`, `sentence`, `attrs`, `debug`, `strict_quoted_strings` |
| `GET /status?job=ID` | State of a background total (404 once it has expired: re-send the `/query`) |
| `POST /cancel?job=ID` | Stop a queued / running count (send a body, even `{}`, or `Content-Length: 0`) |
| `GET /jobs` | All cached results and running counts |
| `GET /version` | The pando build answering, its `features` and the served corpus (also in `/health`) |
| `POST /run` | A full CQL program (named queries, `count`, `coll`, …); `session_id`, `timeout_ms` |
| `POST /session` | Create a client session (`{"session_id": optional, "ttl_s": optional}`; an existing id is reused: `created: false`) |
| `GET /session?session_id=` | The session's hit sets (query, materialised, hits, total, bytes, sort steps, aliases) |
| `POST /session/close` | Close a session (`session_id` in the body or the query string) |
| `GET /sessions` | Open sessions, their memory, the budget |
| `GET /info`, `/values/ATTR`, `/regions/TYPE`, `/context?pos=`, `/health` | Corpus description, values, regions, KWIC context |

### Sessions: stored hit sets (P6.1)

Without a session every request runs its query again. A client session keeps
results — *hit sets* — so that sorting, counting and paging work on the stored
set instead:

```text
POST /session                                   → {"session_id": "s1f…", "created": true, "ttl_s": 1800}
POST /query {"session_id": "s1f…", "name": "Q1", "query": "[upos=\"ADJ\"] [upos=\"NOUN\"]",
             "limit": 20, "total": "async"}      → page 1 (+ job); stored as Q1 (and Last)
POST /run   {"session_id": "s1f…", "cql": "sort Q1 by lemma", "limit": 20}   → page 1, sorted
POST /query {"session_id": "s1f…", "from": "Q1", "offset": 5000, "limit": 20} → a page of the sorted set
POST /run   {"session_id": "s1f…", "cql": "count Q1 by lemma"}              → counts (no re-run of the page)
```

* **A hit set is a recipe plus a cache.** The recipe is the parsed query and
  the `sort … by` steps applied to it; the cache is the materialised hits,
  built the first time a command needs every hit (`sort`, `coll`, `tabulate`,
  a page of a sorted set, a page beyond offset 10000) and kept. A set whose
  cache was dropped (memory budget) is rebuilt from the recipe, sorted as
  before, so answers never depend on what was cached.
* **Memory**: plain hits are kept as token positions only (≈ 8 bytes per
  token per hit: 1.3M two-token hits ≈ 41 MB); hits with named regions,
  subtrees or token groups, and parallel sets, as full match objects. While a
  set is being built its hits exist as match objects (≈ 350 bytes each); these
  builds share `--session-memory` and wait for each other (one always runs), so
  concurrent sorts cannot exhaust the machine. The total is counted before a
  build, so `--session-max-hits` refuses a set before anything is built.
* **`count / group / freq … by`** on a set use the aggregation sink (the query
  again, partitioned with `--query-threads`, no hits stored) — faster than
  grouping stored hits, and the order does not matter.
* **Pages**: `"from": "<set>"` pages a stored set — sorted sets in their sorted
  order, unsorted ones by running the query to the page (or slicing the cache).
  `"total"` works as on a query; a set stored with `"total": "async"` takes its
  total from that background count (`job` in the result) once it is done.
* **Names**: `/query` `"name"` (letters, digits, `_`) stores the set under that
  name *and* as `Last`; without a name only as `Last`. In `/run`, `Q = …;`,
  `Last`, `sort Q by …`, `drop Q`, `show named` are the CQL session commands
  (see PANDO-CQL); `Last` and the name of the same query are one set (sorting
  one sorts the other).
* **Concurrency**: requests on one session run one at a time; different
  sessions run in parallel. `/run` without a `session_id` uses the one shared
  session as before.
* **Soft state**: a session expires after `--session-ttl` without a request;
  `--max-sessions` closes the least recently used idle one; unknown or expired
  sessions answer **404** with `"unknown_session": true` (an unknown set:
  `"unknown_hitset": true`). The client then creates a session and sends its
  query again. A set that would materialise more than `--session-max-hits`
  hits answers **413** `"too_large"` for `sort` / `coll` / …; counts and pages
  still work.
* A client may choose its id (`POST /session {"session_id": "kontext-u42"}`,
  1–128 of `A–Z a–z 0–9 _ - . :`), so several front-end workers can share a
  session without passing ids around.

Responses on a session carry `"session_id"` (and `"hitset"` on `/query`) as
top-level members.

### Limits by tier

A front-end that knows who is asking (FQS; KonText or TEITOK in front of
pando-server) sends `"tier": "<name>"` with `/query` and `/run` — typically
`visitor` (not logged in), `user`, `admin` (a corpus administrator). The
server takes the tier from the request only with `--trust-tier` (C ABI
`trust_tier`); otherwise, and for a request without a known tier,
`default_tier` applies. Every limit is optional (0 / missing = no limit from
the tier):

```json
{"tiers": {
   "visitor": {"timeout_ms": 20000, "total_timeout_ms": 60000, "max_count_hits": 2000000,
               "max_hits": 500000, "threads": 1, "deny": ["transitive", "regex_no_prefix"]},
   "user":    {"timeout_ms": 60000, "total_timeout_ms": 300000, "max_count_hits": 20000000,
               "max_hits": 5000000, "threads": 4},
   "admin":   {"threads": 8}},
 "default_tier": "visitor"}
```

| Limit | Effect |
| --- | --- |
| `timeout_ms` | A `/query` or `/run` stops with 408 after this long. A request's own `"timeout_ms"` can lower it, never raise it (without a tier cap: `--query-timeout`) |
| `total_timeout_ms` | A background (`"async"`) count stops after this long: the job is `cancelled` with `"timed_out": true`, `total` = the count so far. The same tier does not restart it; a tier with a longer (or no) limit does |
| `max_count_hits` | `count / group / freq / stats / coll / dcoll / keyness / tabulate / describe / sort` over a query or stored set with more hits answer 413 `{"too_large": true, "limit": "max_count_hits", "hits": …, "hits_at_least": …}`. The hits are counted first, and only up to the limit, so refusing is cheap. Totals, `size` and pages are not limited |
| `max_hits` | Hits one stored set may materialise (sorting, sorted / deep pages in a session): 413 `"limit": "max_hits"` (default: `--session-max-hits`) |
| `threads` | Position ranges a counting query is split over (default `--query-threads`) |
| `deny` | Features refused with 403 `{"denied": "<feature>", "tier": …}`: `transitive` (`>>`, `<<`, descendant / ancestor conditions), `unbounded_repeat` (`+`, `*`, `{n,}`), `regex_no_prefix` (a regex without a literal start, which scans the whole lexicon), `regex` (any regex), `parallel` (aligned queries), `negated_relation` (`!>`, `!<`) |

Queues, per-user limits and a global CPU budget belong to the host that sees
every corpus (FQS); pando enforces what a request may do once it runs.
`/health` reports the tiers. A regex scan over the lexicon honours the
timeout too (before, `[word=".*a.*"] [word=".*e.*"] [word=".*i.*"]` ran its 11 s
whatever the limit).

### Versions

Every binary reports the same build identity: `pando --version`,
`pando-index --version`, `pando-server --version` print e.g.
`0.1.20 (v0.1.20-23-g2a000ce, perf/phase1)` — the CMake project version plus
`git describe --tags --always --dirty` and the branch, taken at build time (just
`0.1.20` for a build of the tagged commit, or outside a git checkout).

`pando-server` returns it in `GET /version`, `GET /health` and `/info`
(`result.server`):

```json
{"ok": true, "version": "0.1.20", "build": "v0.1.20-23-g2a000ce", "commit": "2a000ce…",
 "branch": "perf/phase1", "build_string": "0.1.20 (v0.1.20-23-g2a000ce, perf/phase1)",
 "features": ["query", "run", "context", "values", "regions", "async_total", "result_cache",
              "limit0_total", "bitmaps", "dep_pairs", "fold_index", "sentence_context", "version"],
 "started": "2026-09-27T17:45:52Z", "uptime_s": 3600, "corpus": "/data/pando/ud_demo",
 "threads": 8, "total_workers": 2}
```

Clients should test `features` rather than probe for errors. `/info` (and
`pando --json` `show info`) also has `result.pando` (the build answering) and
`result.index`: `indexed_with` / `upgraded_with` (the pando-index build that
built the index / last ran `--upgrade`, from `corpus.info`; `null` for older
indexes) and the status of the derived files — `bitmaps`, `structure_bitmaps`,
`dep_pairs` (`ok`, `stale` = older than its source and ignored, `missing`),
`dep_head_rel`, `fold_indexes`. A `stale` or `missing` entry means: run
`pando-index --upgrade <corpus_dir>` with the current build.

### Totals: `"total": false | true | "async"`

* `false` — the page only; `page.total` is the number of hits found so far.
* `true` — the page and the exact total. A total computed before for the same
  query (same text, `max_total`, `strict_quoted_strings`) is reused: only the page
  is computed.
* `"async"` — the page at once; the exact total is counted in the background.
  The result has `"job_id": "ID"` and `"job": {...}`, and `page.total` is the
  count so far (`total_exact: false`) until the job has finished. Poll
  `GET /status?job=ID` (`result` and `job` hold the same object):

```json
{"ok": true, "result": {"id": "dd3abc781972108e", "state": "running", "finished": false,
 "total": 534076, "total_exact": false, "counted": 534076, "progress": 0.2891,
 "estimate": 1847062, "elapsed_ms": 238.0}, "job": {...}}
```

With `"total": true` or `"async"`, `"limit": 0` returns no hits, only the total
/ the job (without a total, `limit: 0` still means all hits).

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
