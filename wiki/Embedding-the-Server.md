# Embedding the server (C ABI)

`pando-server` answers HTTP requests for one corpus. The same API is available
in-process: open a corpus once, send requests as `(method, path, query string,
body)`, and get back the JSON body that `pando-server` would send. Both use the
same code (`src/api/server_api.{h,cpp}`), so responses are the same whichever
transport you use. This is the entry point for multi-corpus hosts such as FQS,
which keep corpora open, route requests to them and add their own protocols on top.

| Layer | File | Use |
|---|---|---|
| C++ | `src/api/server_api.h` — `pando::ServerApi` | C++ embedders |
| C ABI | `src/api/server_capi.h` — `pando_server_*` | Rust (`libloading`), PHP FFI, Python `ctypes`, … |
| HTTP | `src/cli/server_main.cpp` — `pando-server` | a thin httplib shell over `ServerApi` |

## Routes

These are the same as for `pando-server`: `GET /health`, `/version`, `/info`, `/values/<attr>`,
`/regions/<struct>`, `/context`, `/status`, `/jobs`, `/session`, `/sessions`, and `POST /query`,
`/run`, `/cancel`, `/session`, `/session/close` (client sessions: see "Sessions" in the CLI
reference). An unknown path answers 404 and a known path with the wrong method answers 405. Both are
JSON `{"ok": false, "error": …}`. `/query` and `/run` accept `"timeout_ms"`. When it expires the
answer is 408 `{"ok": false, "timed_out": true}`. The time limit covers the page and a
synchronous count (for `/run`, the whole program, including the wait for its session), not
background totals.

## C ABI

```c
#include "api/server_capi.h"

if (pando_server_abi_version() != PANDO_SERVER_ABI_VERSION) { /* refuse to use it */ }
puts(pando_server_build_string());          // "0.1.20 (v0.1.20-27-g…, perf/phase1)"

char* err = NULL;
pando_server_t* s = pando_server_open("/data/ud_demo",
    "{\"total_workers\": 2, \"query_timeout_ms\": 30000, \"embedded_in\": \"fqs 0.4\"}", &err);
if (!s) { fprintf(stderr, "%s\n", err); pando_server_free(err); }

int status;
char* js = pando_server_request(s, "POST", "/query", NULL,
    "{\"query\": \"[upos=\\\"NOUN\\\"]\", \"limit\": 20, \"total\": \"async\"}", 0, &status);
/* … result.job_id → poll: */
char* st = pando_server_request(s, "GET", "/status", "job=1a2b3c4d5e6f7a8b", NULL, 0, &status);
pando_server_free(js); pando_server_free(st);

pando_server_close(s);
```

Open options (JSON, all optional): `preload`, `warm` (`"hot"` / `"all"`: read the index
files most queries touch, or all of them, into the page cache in a background
thread when the handle opens; see below), `total_workers`, `result_cache`,
`result_ttl`, `abandon_after`, `cache_mb` (recent pages / command results / sort
indexes reused across requests, default 128 MB per open corpus, 0 = off), `query_timeout_ms`, `query_threads` (threads a
counting query is split over, default 1, `"auto"` = min(pool threads, 8) — see the CLI
reference), `pool_threads` (the process-wide worker pool those threads are borrowed
from, shared by every handle in the process; default `"auto"`: the CPUs the process may
use, from the affinity mask and the cgroup quota; the first handle that sets it
decides, a later different value is ignored with a warning), `warm_streams`
(parallel read streams of the warm-up, default 4),
`threads` (reported only), `session_ttl`, `max_sessions`, `session_memory_mb`,
`session_max_hits` (client sessions, defaults 1800 s, 256, 2048 MB, 5000000),
`tiers`, `default_tier`, `trust_tier` (limits by tier: see the CLI reference; an
embedding host that builds the request bodies itself — FQS — sets `trust_tier`
and puts its own `"tier"` in every body, dropping any a client sent),
`embedded_in` (reported in `/health`, `/version` and `/info` `server`), and
`debug_total_delay_ms` (for testing).

`pando_server_build_json()` returns the version, build, commit, branch, ABI version and
features. A host can report it for each engine without opening a corpus.

## Contract for hosts

**Threads.** A handle can serve any number of threads at once. Queries run
concurrently on the shared corpus. `/run` without a `session_id` is serialised inside the handle,
because that named-query session is shared state; requests on one client session are serialised
per session, different sessions run concurrently. With `query_threads` (or a tier's `threads`) > 1 a
counting request also borrows up to that many − 1 workers of the process pool while it lasts — only idle
ones, it never waits for one, so under load it runs on the calling thread alone. The process then uses at
most (calling threads) + `pool_threads` CPUs; bounding the first term (how many requests run at once) is
the host's admission. `pando_server_request` blocks, so call it from a blocking pool
(in tokio, use `spawn_blocking`).

**Errors.** No entry point throws or aborts on bad input. Errors come back as JSON
with an HTTP-style status. A crash in the engine still takes down the host
process. That is the cost of running in-process.

**Eviction.** `pando_server_busy(s)` counts the requests in flight plus the background
counts that are queued or running. `pando_server_idle_seconds(s)` is the time since the last
request. Keep a reference count per handle, and close a handle only when no request holds it
and `busy() == 0`. `close` cancels running counts and joins their workers. Job ids
belong to a handle, so a host that serves several corpora must include the corpus in
the job id or in the `/status` route. The same holds for client sessions: they live in
the handle (one corpus), and closing an idle handle closes its sessions — clients then
get 404 `unknown_session` and start again, which the session contract allows. A host
that evicts idle corpora may want to keep a handle whose `GET /sessions` still lists
recently used sessions.

**Swapping a corpus for a new version (hot swap).** Publish new versions with
`pando-index … --publish ROOT` (or `--upgrade ROOT --publish`) and open
`ROOT/current`. A handle resolves the symlink when it opens and reads only
that version from then on, also for files it opens later; `ROOT/current` moving
on does not affect it. To swap, make before break:

1. notice the new version: `/health` `index.newer_on_disk` turns `true` (or
   compare `index.index_id` with `corpus.info` `index_id` of `ROOT/current`, or
   act on the end of your own reindex job);
2. open a second handle on `ROOT/current`, with `"warm": "hot"` (and wait for
   `GET /warm` `state: done` if the first queries should not read the disk);
3. send new requests to the new handle;
4. close the old one as for eviction: no request holds it and `busy() == 0`.

Sessions, job ids and cached results belong to a handle, so they end with the
old one (clients get 404 `unknown_session` / `unknown job` and re-run, which then
sees the new data). Close old handles within `--keep` publishes: the files of a
pruned version stay readable while mapped, but one the handle has not opened yet
would be missing. Never rebuild or `--upgrade` a directory a handle has open:
files are memory-mapped and some opened on first use, so an in-place change
mixes versions or, for a truncated file, crashes the process (SIGBUS) —
`pando-index` refuses that for published roots.

**What "warm" means for pando.** Opening is cheap: the index is `mmap`ed lazily, and
on ud_demo (38 M tokens) a whole CLI run with a warm page cache takes about 4 ms. The
speed of a warm corpus is in the OS page cache, which a handle neither holds nor frees.
Its RSS is mostly reclaimable file pages, so an RSS budget measures the wrong thing. What a
long-lived handle keeps is state: cached totals and running jobs, so `/query` and
`/status` share one count, and the `/run` session. Use `"warm": "hot"` (or
`"preload": true`, which reads everything before the open returns) when "warm" should
mean "in memory". Keep that separate from keeping a handle open.

**Warming up.** `"warm": "hot"` reads, in a background thread, the files that most
queries touch: lexicons and their indexes, `.rev.idx`, bitmaps (`.bm`, `.bnd.bm`),
fold permutations, region files and their value postings, `dep.head_rel` and the
indexes of packed and edge postings — on ud_demo (38 M tokens) about 560 MB, about
15 bytes per token. The per-position `.dat` files and the large posting lists are left
to the queries that need them. `"all"` reads every file. A front-end can do what
KonText does — load a corpus when the user selects it, before the first search — with
`POST /warm` (`{"level": "hot"}`, the default, or `"all"`), which answers at once;
`GET /warm` and `/health` (`"warm"`) report `state` (`idle` / `running` / `done`),
`level`, `files` / `files_done`, `bytes` / `bytes_done` and `seconds` (since the first
start). Queries are answered while it runs (they then read what they need themselves).
Files are read in 64 MB segments by `warm_streams` threads (default 4): on network
storage (Ceph, NFS) several streams read several times faster than one.
`GET /warm?residency=hot` (or `all`) adds `"residency"`: how many of those bytes are in
the page cache now (`resident_bytes`, `resident` as a share) — whether the machine has
the RAM to keep the corpus warm.
In fqs, `"pando": {"warm": "hot"}` in the limits file (or a corpus' `settings.limits`)
warms every corpus as it is opened.

**Memory per request.** Most queries stream. Commands that materialise every hit do not.
`sort by` over a large result is the main one: `[upos="VERB"]; sort by lemma`
on ud_demo peaks at about 3.3 GB. Limit concurrent heavy requests per corpus.

## Building

With `-DPANDO_BUILD_SHARED=ON`, `libpando.so` / `libpando.dylib` exports
`pando_server_*` alongside the older `pando_*` functions. It also installs
`server_capi.h`. An adapter that adds this tree with `add_subdirectory` and links
`pando_api` into its own shared library needs `CMAKE_POSITION_INDEPENDENT_CODE ON`, as
`flexicorp_pando` already sets. Such an adapter can then re-export the functions, or wrap them:

```c
// flexicorp_pando: forward to the embedded server
char* flexicorp_pando_request(flexicorp_pando_ctx_t* ctx, const char* method, const char* path,
                              const char* query, const char* body, size_t len, int* status) {
    return pando_server_request(ctx->server, method, path, query, body, len, status);
}
```

Tests:

- `server_api_test`, in ctest, covers the routes, errors, lifetime, and 8 threads sending mixed requests.
  It is also clean under ThreadSanitizer.
- `test/server_capi_ctypes.py` loads the shared library the way a host would.
- `test/server_jobs_test.py` covers the HTTP contract.
