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
| `--sample N` | Random sample of N matches, shown in corpus order; the same `--seed` gives the same sample on every path and thread count |
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

`pando-index --upgrade <corpus_dir>` adds the sidecar indexes the current build
knows (fold, bitmap, dependency edge postings, …) to an existing corpus. Options
for `--upgrade`:

- `--packed-rev [auto|none|A[,B...]]` — also write block-compressed postings
  (`<attr>.rev.pfb`) for every single-valued positional attribute (`auto`, the
  value when the option is given without one) or the ones listed. Default `none`.
- `--head-attrs auto|none|A[,B...]` — store attribute A of each token's
  dependency head (`head#A.*`, listed as `head_attrs=` in `corpus.info`; storage
  only), which makes `[X] > [Y]`, `[Y] < [X]` and `parent [X]` much faster; see
  [Dependency queries](Dependency-Queries.md#head-attributes). Default `auto`:
  `upos`, `deprel` and `lemma` (those the corpus has) when it has a dependency
  index — also when an index is built (`pando-index --head-attrs none <input>
  <dir>` to build without). `none` is remembered in `corpus.info`, so later
  upgrades leave such a corpus without them until a list is given. Built before
  folds, bitmaps and packed postings so these cover them too.
- `--packed-dat [auto|none|A[,B...]]` — compact token ids (`<attr>.dat.pk`),
  about 60% of `.dat`, at a cost when many ids are read; see
  [Packed ids](Index-and-Corpus-Layout.md#packed-ids-datpk). Default none.
- `--drop-dat` — remove the plain `<attr>.dat` once its packed ids are verified
  (implies `--packed-dat auto`).
- `--compact-deps` — remove `dep.head` and `dep.head_rel` once `dep.head_rel8`
  (one byte per token, always written) is verified to give the same heads; see
  [Dependency queries](Dependency-Queries.md#index).
- `--drop-rev` — after verifying that the packed postings decode to the same
  positions, remove the plain `<attr>.rev` files (implies `--packed-rev auto`).
  Saves disk space; queries then decode postings (see
  [Packed postings](Index-and-Corpus-Layout.md#packed-postings-revpfb)).

**Versioned publishing (hot swap).** A corpus that a server keeps open is not
rebuilt or upgraded in place: build next to it and switch — see
[Index and corpus layout](Index-and-Corpus-Layout.md#versioned-index-directories-hot-swap).

- `pando-index [options] <input> --publish ROOT` — instead of `<output_dir>`:
  builds into `ROOT/versions/.building-<id>`, renames it to `ROOT/versions/<id>`
  when it is complete, and points the symlink `ROOT/current` at it (atomically).
  `--keep N` versions are kept (default 3; never the new or the previous one).
- `pando-index --upgrade ROOT --publish [upgrade options]` — copies the current
  version (keeping modification times), upgrades the copy, then switches.
- `pando-index --upgrade ROOT/current …`, or a build into `ROOT/current` or
  `ROOT`, is refused with the `--publish` command to use instead.
- One publish per `ROOT` at a time (`ROOT/.publish.lock`); a failed or
  interrupted publish leaves only a `.building-*` directory, removed by the next.

Every build and every `--upgrade` writes a new `index_id=` to `corpus.info`.

Environment for `pando`, `pando-server` and embedders: `PANDO_REV=auto|raw|packed`
(`auto`: `.rev` when present, otherwise `.rev.pfb`; `packed`: `.rev.pfb` whenever
it is present and up to date) and `PANDO_REV_CACHE_MB` (cache of decoded long
lists, default 256). `PANDO_HEADATTR=off`: dependency queries do not use head
attributes. `PANDO_DAT=packed`: token ids from `.dat.pk` when it is
there. `PANDO_LEXICON_THREADS=N`: the most threads for the lexicon scan of a
regex (default: the usable CPUs, at most 8; scans of 256K entries or more are
split; a request's tier `threads` lowers it). `PANDO_POOL_THREADS=N`: size of the
process worker pool (default: the usable CPUs, see "Parallel counting").
`PANDO_RANGES_PER_THREAD=N` / `PANDO_RANGE_TARGET=N`: at most N position ranges
per thread of a parallel count (default 4) / no range below N tokens while
there are at least as many ranges as threads (default 8388608).

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
| `--warm hot\|all\|none` | Read the index files most queries touch (`hot`) or all of them into the page cache in the background after startup (default `none`; `POST /warm` later; see [Embedding the server](Embedding-the-Server.md)) |
| `--total-workers N` | Concurrent background counts (default 2) |
| `--result-cache N` | Cached query results / totals (default 512; unused finished ones evicted first) |
| `--cache-mb MB` | Memory for recent results reused by repeated requests (default 128 per corpus; 0 = off): pages of `/query` and of stored sets, `count / group / freq / coll / dcoll / tabulate / size` results, sort indexes and sorted pages (blocks of 1024 sorted hits). Shared by all requests and sessions, keyed on the query text, the parser options and the options a result depends on; queries that use the session (labels of earlier statements, `where` sets, `dep_subtree` sources) are not cached. `/health` reports `cache` (entries, bytes, hits, misses) |
| `--result-ttl SEC` | Drop a finished result nobody asked about for SEC (default 3600) |
| `--abandon-after SEC` | Cancel a background count nobody polled for SEC (default 120; 0 = never) |
| `--debug-total-delay MS` | Testing: reveal every total gradually over MS, so a client can be tested against a "slow" count on a small corpus |
| `--query-threads N\|auto` | Split every counting query — a `/query` total, a background (`"async"`) total, a `/run` `count by` — over position ranges counted by up to N threads (default 1: one thread per query; `auto` = min(pool threads, 8)). The extra threads are borrowed from the process pool when idle (see "Parallel counting"). A tier's `threads` replaces it. `/health` reports it as `query_threads` |
| `--pool-threads N\|auto` | Size of the worker pool all queries of the process borrow from (default `auto`: the CPUs the process may use — the affinity mask and the cgroup CPU quota, not the host's cores). `/health` reports `pool` |
| `--warm-streams N` | Parallel read streams of the warm-up (default 4; network storage reads several times faster with several, a local disk needs 1–2) |
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

**The worker pool.** The extra threads come from one pool per process, shared
by every corpus it has open (an embedding host such as FQS keeps many open).
A query with N threads runs ranges on its own (request) thread and borrows up to
N − 1 *idle* pool workers; a worker that frees up while ranges remain joins then.
It never waits for one: on a busy server a query just runs on its request thread
alone, with the same result. The CPU a process uses is so at most its request
threads plus the pool (`--pool-threads`, default: the CPUs the process may use —
affinity mask and cgroup quota, so a container with a 4-CPU limit on a 64-core
host gets 4). The corpus is cut into up to 4 ranges per thread
(`PANDO_RANGES_PER_THREAD`), none smaller than 8M tokens (`PANDO_RANGE_TARGET`)
unless that leaves fewer ranges than threads, taken in turn, so one dense range
does not hold up the others. Regex lexicon scans use the same pool. `/health` `pool`: `threads`,
`started`, `busy`, `steps` (parallel steps run), `helpers_wanted` /
`helpers_granted` (granted well below wanted: the pool is the bottleneck),
`usable_cpus`.

Worth it for long counts on large corpora. One query stops gaining well before
32–64 threads (memory bandwidth; ranges are at least 2^20 tokens), so on a large
machine keep the per-query width (`--query-threads`, a tier's `threads`) at
8–16 and let the pool serve more queries at once. Ranges smaller than 2^20
tokens are not made (`PANDO_PARTITION_MIN` overrides, for tests on small
corpora).

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
| `POST /query` | One query: `query`, `limit`, `offset`, `total`, `max_total`, `context`, `sentence`, `attrs`, `fragment`, `debug`, `strict_quoted_strings` (see "Token fragments" below) |
| `GET /status?job=ID` | State of a background total (404 once it has expired: re-send the `/query`) |
| `POST /cancel?job=ID` | Stop a queued / running count (send a body, even `{}`, or `Content-Length: 0`) |
| `GET /jobs` | All cached results and running counts |
| `GET /version` | The pando build answering, its `features` and the served corpus (also in `/health`) |
| `POST /run` | A full CQL program (named queries, `count`, `coll`, …); `session_id`, `timeout_ms` |
| `POST /session` | Create a client session (`{"session_id": optional, "ttl_s": optional}`; an existing id is reused: `created: false`) |
| `GET /session?session_id=` | The session's hit sets (query, materialised, hits, total, bytes, sort steps, aliases) |
| `POST /session/close` | Close a session (`session_id` in the body or the query string) |
| `GET /sessions` | Open sessions, their memory, the budget |
| `POST /warm`, `GET /warm` | Start reading the hot (`{"level": "hot"}`, default) or all (`"all"`) index files into the page cache in the background, e.g. when a front-end selects the corpus; the warm-up status. `GET /warm?residency=hot\|all` adds `"residency"`: `bytes`, `resident_bytes`, `resident` (share) of those files in the page cache now (mincore; on demand only) |
| `GET /info`, `/values/ATTR`, `/regions/TYPE`, `/context?pos=`, `/health` | Corpus description, values, regions, KWIC context |

### Token fragments (`"fragment": true`)

For front-ends that render tokens (TEITOK's flexicorp interface) on corpora
without XML files of their own — an index built straight from CoNLL-U — each
hit of a `/query` with `"fragment": true` also carries:

- `fragment`: its context (the `context` window, or the sentence with
  `"sentence": true`) as TEITOK-style XML,
  `<s id="s-12"><tok id="w-301" lemma="…" upos="…" deprel="nsubj" head="w-303">form</tok> …</s>`
  — every positional attribute in `attrs` (default all) as an attribute,
  `head` as the head's token id, sentences as `<s>`;
- token ids on `tokens[]` (`"id": "w-<position + 1>"`, or the corpus' own `id`
  attribute when it has one) and `"group"`: the query token;
- `highlight_map` (flexicorp's highlight contract): `default.tok_ids` / `match`
  with the matched ids, and `groups` per query token (its label, e.g. `v:`, else
  `t1`, `t2`, …) when the query has more than one;
- and the result a `legend` of those groups, so a UI colours each query token.

FQS passes `"fragment": true` through (and TEITOK sends it for projects
without `xmlfiles/`, where it also shows sentences by default).

### Counts by several fields (`count by A, B`)

With one field `count by` returns `rows` (the top `group_limit` values, default
1000). With several fields the JSON is a tree, `hierarchy`: the top `group_limit`
values of the first field by count, under each of them the top `child_limit`
values of the second field within it (default 20), and so on; equal counts are
ordered by value. Every non-leaf node has `groups`, the number of distinct values
under it (also those not returned), and the result has `groups` (distinct
combinations), `top_groups` (distinct values of the first field),
`groups_returned` and `child_limit`. Set them per request (`/run`
`"group_limit"`, `"child_limit"`; 0 = all) or in a program (`set group_limit 50;
set child_limit 10`). Only the returned values are decoded to strings: on the 38M
demo `a:[upos="VERB"] > b:[upos="NOUN"]; count by a.lemma, b.lemma` (2.6M
combinations) answers in 2.1 s with 2.3 MB of JSON, where it took 13.7 s and 70 MB
with every child returned. The text output of `pando` lists the top
`group_limit` combinations as rows.

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
* **`sort`**: the first sort of a set that is not kept counts the hits per sort
  key (no hits stored) and pages re-run the query for the hits on the page;
  with `--cache-mb` the key counts and blocks of sorted hits are reused by later
  pages, other sessions and repeated requests.
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
  hits answers **413** `"too_large"` for a command that keeps every hit (`raw`,
  `stats`, a second `sort`, …); counts, `coll`, a first `sort` and pages still
  work.
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
| `threads` | The most threads a request of this tier may use: the threads its counting query is split over (default `--query-threads`), its regex lexicon scans and the background total it starts. All borrowed from the process pool, so under load a request may get fewer (never more). Without a tier `threads`, scans use up to min(usable CPUs, 8) |
| `deny` | Features refused with 403 `{"denied": "<feature>", "tier": …}`: `transitive` (`>>`, `<<`, descendant / ancestor conditions), `unbounded_repeat` (`+`, `*`, `{n,}`), `regex_no_prefix` (a regex without a literal start, which scans the whole lexicon), `regex` (any regex), `parallel` (aligned queries), `negated_relation` (`!>`, `!<`) |

Queues, per-user limits and the number of requests running at once belong to
the host that sees every corpus (FQS); pando enforces what a request may do once
it runs, and bounds the extra threads of all requests together by its pool.
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
 "threads": 8, "query_threads": 1, "pool": {"threads": 16, "started": 7, "busy": 0, "steps": 412,
 "helpers_wanted": 1210, "helpers_granted": 1187, "usable_cpus": 16}, "total_workers": 2,
 "index": {"index_id": "20261002T143421535Z-7e1d864b", "index_identity": "20261002T143421535Z-7e1d864b",
           "index_dir": "/data/pando/ud_demo/versions/20261002T143421535Z-7e1d864b",
           "published_root": "/data/pando/ud_demo", "current_version": "/data/pando/ud_demo/versions/…",
           "newer_on_disk": false}}
```

Clients should test `features` rather than probe for errors. `/info` (and
`pando --json` `show info`) also has `result.pando` (the build answering) and
`result.index`: `indexed_with` / `upgraded_with` (the pando-index build that
built the index / last ran `--upgrade`, from `corpus.info`; `null` for older
indexes), the identity fields of `/health` `index` (`index_id`; `index_identity`
= `index_id`, or `info:<mtime>:<size>` of `corpus.info` for older indexes;
`index_dir`, symlinks resolved; `published_root`, `current_version`;
`newer_on_disk`: a newer version was published, or the directory rebuilt, since
this one was opened), what the directory holds (`disk_bytes`,
`bytes_per_token`, `attrs`: `dat` / `rev` as `plain`, `packed`, `both` or
`none`; `head_attrs`; `dep_files`) and the status of the derived files — `bitmaps`, `structure_bitmaps`,
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

### Random samples and shuffled concordances: `"sample"`, `"shuffle"`, `"seed"`

* `"sample": N` — a random N of the hits (KonText's *Random sample*), shown in
  corpus order and paged with `offset` / `limit`; `page.total` is the sample's
  size. The result has `"sample": {"size", "population", "requested",
  "shuffled", "seed"}` (`population` = all hits).
* `"shuffle": true` — every hit in a random order (KonText's *Shuffle*);
  `page.total` = all hits. With `"sample"`: the sample in a random order.
* `"seed"` — the same seed gives the same sample and the same order on every
  page, path and thread count (the hits with the smallest hash of seed and
  position); 0 or absent picks a new random one, so a client that pages should
  send one (kontext-pando derives it from the concordance's operations).

Both go through every hit once per request (about the time of a total; memory
for offset + limit, or N, hits), synchronously: `"total"` is ignored, and they
are not stored in sessions (400 with `"session_id"`).
