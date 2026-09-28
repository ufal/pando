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
`/regions/<struct>`, `/context`, `/status`, `/jobs`, and `POST /query`, `/run`,
`/cancel`. An unknown path answers 404 and a known path with the wrong method answers 405. Both are
JSON `{"ok": false, "error": …}`. `/query` accepts `"timeout_ms"`. When it expires the
answer is 408 `{"ok": false, "timed_out": true}`. The time limit covers the page and a
synchronous count, not background totals.

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

Open options (JSON, all optional): `preload`, `total_workers`, `result_cache`,
`result_ttl`, `abandon_after`, `query_timeout_ms`, `query_threads` (position ranges
counted in parallel per counting query, default 1 — see the CLI reference),
`threads` (reported only),
`embedded_in` (reported in `/health`, `/version` and `/info` `server`), and
`debug_total_delay_ms` (for testing).

`pando_server_build_json()` returns the version, build, commit, branch, ABI version and
features. A host can report it for each engine without opening a corpus.

## Contract for hosts

**Threads.** A handle can serve any number of threads at once. Queries run
concurrently on the shared corpus. `/run` is serialised inside the handle, because the named-query
session is shared state. With `query_threads` > 1 a counting request also runs
that many worker threads of its own while it lasts. `pando_server_request` blocks, so call it from a blocking pool
(in tokio, use `spawn_blocking`).

**Errors.** No entry point throws or aborts on bad input. Errors come back as JSON
with an HTTP-style status. A crash in the engine still takes down the host
process. That is the cost of running in-process.

**Eviction.** `pando_server_busy(s)` counts the requests in flight plus the background
counts that are queued or running. `pando_server_idle_seconds(s)` is the time since the last
request. Keep a reference count per handle, and close a handle only when no request holds it
and `busy() == 0`. `close` cancels running counts and joins their workers. Job ids
belong to a handle, so a host that serves several corpora must include the corpus in
the job id or in the `/status` route.

**What "warm" means for pando.** Opening is cheap: the index is `mmap`ed lazily, and
on ud_demo (38 M tokens) a whole CLI run with a warm page cache takes about 4 ms. The
speed of a warm corpus is in the OS page cache, which a handle neither holds nor frees.
Its RSS is mostly reclaimable file pages, so an RSS budget measures the wrong thing. What a
long-lived handle keeps is state: cached totals and running jobs, so `/query` and
`/status` share one count, and the `/run` session. Use `"preload": true`, or a read of
the hot files, when "warm" should mean "in memory". Keep that separate from keeping a handle open.

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
