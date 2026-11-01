# Result format (the JSON contract)

Every answer of `pando-server`, and of everything that runs the same code (`pando::ServerApi`: the C
ABI in [Embedding the server](Embedding-the-Server.md), `libflexicorp_pando`'s request route, FQS
with the warm library), is a JSON object with a **format version**:

```json
{"ok": true, "operation": "query", "result": {...}, "format_version": 1}
```

This page describes format version **1**. Programs that read pando's answers (FQS, flexicorp,
KonText, your own) can rely on it.

## The rules

- **Within a format version, changes are additive only.** A new key, a new result kind or a new
  route can appear. No key disappears, changes its type or changes its meaning. Anything else
  raises `format_version`.
- **Read defensively.** Ignore keys you do not know. When an answer has no body you recognise,
  report it as an error that names its keys, never as an empty result.
- **The contract is tested.** [`contract/`](../contract/) holds a small self-authored corpus,
  one request per result kind (`requests.json`), and pando's complete answers (`answers/*.json`).
  `ctest -R contract` replays the requests and fails when an answer's shape (keys and value types)
  differs from the stored one. A new key fails too, so it enters the contract on purpose:
  `contract/contract_test.py --write` stores the answers anew, and the diff is reviewed.
- **Consumers test against the same answers.** Feed `contract/answers/*.json` to your reader:
  every kind must come out right and non-empty, and an unknown shape must be reported.

Two other emitters exist and are **not yet** under this contract. They are being brought onto it:
- **the `pando` command line** (`--json`, `--api`): the Python client and scripts use it;
- **`flexicorp-pando` for plain queries** (`flexicorp_json.h`): TEITOK's route without FQS.

## The answer

| key | |
|---|---|
| `ok` | `true`, or `false` with `error` (a message) |
| `operation` | the result kind (below) |
| `last_command` | for programs: the command whose output this is |
| `result` | the body: an object, or for some kinds a list or a number |
| `reused` | for programs: named queries that were reused instead of run again |
| `session_id`, `hitset` | with a client session |
| `format_version` | this page's version |

A program (`/run`) answers with the output of its **last** output-producing statement.

## Result kinds

### Hits (`/query`, or a query as a program's last statement): `operation: "query"`

`result`:
- `query: {language, text}`
- `page: {start, size, returned, total, total_exact}`
- `hits[]`
- `groups[]`: the query's tokens and region anchors (`{index, id, name}`; regions `{id, name, region}`)
- `legend[]`: with `fragment` and more than one group

Optional: `job_id` and `job` (an asynchronous total), `sample` (with `sample` / `shuffle`), and
`warnings[]` (only when there are any).

A hit:
- `doc_id`, `match_start`, `match_end`, `context: {left, match, right}`;
- `tokens[]`: `{pos, <attribute>: value, …}`, with `id` and `group` where they apply;
- `groups[]`: per query token `{index, id, name, start, end}`; per region anchor also `region`,
  `region_id`, `attrs`, `tok_ids[]`;
- optional: `named_regions`, `aligned[]` (option `aligned`), `fragment` and `highlight_map`
  (option `fragment`).

With `total: "async"`, `page.total` is the count so far, `page.total_exact` is `false`, and
`job.*` says how far the count is. `GET /status?job=` answers `{result: <job>, job: <job>}`. With
`limit: 0` there are no hits, only the total.

### Aligned pairs (`with`, `=:X`): `operation: "query"`, `result.parallel: true`

`result`:
- `page`, with `returned_pairs`, `total_pairs`, `total_sources`;
- `hits: []` (always empty);
- `pairs[]`: `{aligned: true, alignment: {kind, pair_index}, source: <hit>, target: <hit>, source_text_id, target_text_id}`;
- `legend[]`, with `side`.

Pages are cut by pair.

### Programs (`/run`)

| `operation` | `result` | items |
|---|---|---|
| `count` (one field) | `total_matches, groups, groups_returned, fields[], rows[]` | `{key, count, pct}` |
| `count` (several fields) | `total_matches, groups, top_groups, groups_returned, child_limit, fields[], hierarchy[]` | `{field, value, count, pct, pct_of_parent?, groups?, children[]?}` |
| `freq` | `corpus_size, total_matches, freq_mode: "single", source_query, per_subcorpus_ipm, fields[], rows[]` | `{key, count, pct, ipm, rf_pct?, subcorpus_size?}` |
| `freq` (`freq A, B by …`) | `compare_queries[], corpus_size, freq_mode: "compare", per_subcorpus_ipm, fields[], totals_per_query{}, rows[]` | `{key, subcorpus_size?, queries: {Q: {count, pct, ipm, q_ipm, rf_pct?}}}` |
| `stats` | `by[], metrics[], rows[]` | `{key, n_total, values[{n_valid, value}]}` |
| `tabulate` | `fields[], total_matches, offset, limit, rows_returned, rows[][], row_contexts[]` | a row is a **list** of cells |
| `describe` | `total_matches, offset, limit, rows_returned, rows[]` | `{match_index, match_start, match_end, kind, tokens[] \| regions[]}` |
| `raw` | a **list** | `{positions[], tokens[]}` |
| `coll` | `attribute, window, matches, stoplist, measures[], collocates[]` | `{word, obs, freq, <measure>}` |
| `dcoll` | `attribute, relations[], matches, stoplist, measures[], collocates[]` | as `coll`, with `breakdown{}` |
| `keyness` | `attribute, focus_size, ref_size, corpus_size, stoplist, rows[]` | `{word, focus_freq, ref_freq, keyness, effect}` |
| `size` | a **number** | |
| `query` (`sort`, an assignment) | as hits | |
| `info` (`show info`) | as `GET /info` | |
| `show_attrs` | a list | `{name, vocab}` |
| `show_regions` | `structures[], region_attrs[]` | |
| `regions` (`show regions T`) | `type, total, returned, regions[]` | |
| `values` (`show values A`) | `attr, type, unique, returned, values[]` | `{value, count}` |
| `show_named` | a list | `{name, matches, materialised, total_known}` |
| `show_settings` | the settings | |
| `set` | no `result`; `setting, value` | |
| `drop` | no `result` | |

### Other routes

| route | answer |
|---|---|
| `GET /info` | `{operation: "info", result: {name, size, tokens, has_deps, attributes[], structures[], region_attrs[], multivalue[], pando{}, index{}, …}}` |
| `GET /values/A`, `GET /regions/T` | as `show values`, `show regions T` |
| `GET /context?pos=` | `{pos, left, match, right, doc_id, id, corpus_size}` (no `result`) |
| `GET /health`, `/version` | the build, features, index identity, sessions, warm-up, cache, tiers |
| `GET /status?job=`, `POST /cancel`, `GET /jobs` | the job, `{job, cancelled}`, `{jobs[]}` |
| `POST /session`, `GET /session`, `POST /session/close`, `GET /sessions` | session id, its stored sets, closing, all sessions |
| `GET /warm` | `{warm{}, residency?}` |

`contract/answers/` has a complete example of each.

## Errors

`{"ok": false, "error": "<message>", "format_version": 1}`, plus extra keys for some errors:

| HTTP | when | extra keys |
|---|---|---|
| 400 | a query that does not parse, bad input | |
| 403 | the query uses something its tier denies | `denied, tier` |
| 404 | unknown route, attribute, region type, job, session or hit set | `unknown_session`, `unknown_hitset` |
| 405 | a known route with the wrong method | |
| 408 | the time limit passed | `timed_out: true` |
| 413 | more hits than the tier allows | `too_large: true, limit, hits, …` |
| 500 | an internal error | |

**An error inside a program (`/run`) is HTTP 200 with `ok: false`**, e.g. `freq` without
`by`. Always check `ok`, not only the status.

## Known problems (format version 1)

These are recorded so that readers are not surprised. Fixing them is either additive, or comes
with a new version.

- **Numbers:** the same field can print as `25` in one row and `25.0` in the next. Read numbers as
  numbers, not by their textual form.
- **Error types:** there is no `error_kind`. A syntax error's position is only in the message text.
- **Group names in programs:** a page from a program can name its groups after the program's
  first statement, not the one shown. A `sort`ed page has no group names.
- **Query language in programs:** the `query.language` of a program's page is always
  `pando-cql`, also for a dialect.
- **Unknown names and commands:** an undefined name (`count Z by lemma`) is not an error, and an
  unsupported command (`cat`) answers `operation: "unknown"`.
