/*
 * server_capi.h — C ABI for the embedded pando server (ServerApi).
 *
 * The pando-server HTTP API (POST /query, GET /status, POST /run, GET /info, …)
 * without HTTP: an embedder (the flexicorp adapter under FQS, PHP FFI, Python
 * ctypes, …) opens a corpus once and sends requests; the answers are exactly
 * the JSON bodies pando-server sends, from the same code.
 *
 *   pando_server_t* s = pando_server_open("/data/ud_demo", "{\"total_workers\": 2}", &err);
 *   int status;
 *   char* js = pando_server_request(s, "POST", "/query", NULL,
 *                                   "{\"query\": \"[upos=\\\"NOUN\\\"]\", \"total\": \"async\"}", 0, &status);
 *   ... pando_server_free(js);
 *   js = pando_server_request(s, "GET", "/status", "job=1a2b3c", NULL, 0, &status);
 *   ...
 *   pando_server_close(s);
 *
 * Thread safety: one handle may be used from any number of threads at once
 * (queries run concurrently; /run is serialised inside). Only open and close
 * must not race with other calls on the same handle.
 *
 * Eviction: pando_server_busy() is the number of requests in flight plus the
 * background counts queued or running. Closing a handle cancels its counts and
 * waits for their workers; an embedder that evicts idle corpora should close a
 * handle only when busy() == 0 and no request is in flight (keep a refcount).
 *
 * Errors: no function throws or aborts on bad input; request errors are JSON
 * bodies ({"ok": false, "error": …}) with an HTTP-style status.
 *
 * Memory: strings returned by pando_server_request must be freed with
 * pando_server_free(); the build strings are static.
 */

#ifndef PANDO_SERVER_CAPI_H
#define PANDO_SERVER_CAPI_H

#include <stddef.h>

#ifndef PANDO_API
#  if defined(PANDO_BUILDING_SHARED)
#    if defined(_WIN32)
#      define PANDO_API __declspec(dllexport)
#    else
#      define PANDO_API __attribute__((visibility("default")))
#    endif
#  else
#    define PANDO_API
#  endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Bumped on any incompatible change of these functions (not of the JSON). */
#define PANDO_SERVER_ABI_VERSION 1

typedef struct pando_server pando_server_t;

/* PANDO_SERVER_ABI_VERSION of the library actually loaded (check after dlopen). */
PANDO_API int pando_server_abi_version(void);

/* The pando build, e.g. "0.1.20 (v0.1.20-27-gfc84a20, perf/phase1)". Static. */
PANDO_API const char* pando_server_build_string(void);

/* {"version": …, "build": …, "commit": …, "branch": …, "features": [...]}. Static. */
PANDO_API const char* pando_server_build_json(void);

/*
 * Open a corpus (index directory) and start its background-total workers.
 *   options_json: NULL or a JSON object, all keys optional:
 *     "preload": bool           read all index pages at open (default false: lazy mmap)
 *     "total_workers": N        concurrent background counts (default 2)
 *     "result_cache": N         cached query results (default 512)
 *     "result_ttl": SEC         drop an unused finished result after SEC (default 3600)
 *     "abandon_after": SEC      cancel a count nobody polled for SEC (default 120; 0 = never)
 *     "query_timeout_ms": MS    default /query time limit (0 = none; per request "timeout_ms")
 *     "threads": N              reported in /health ("threads")
 *     "query_threads": N|"auto" count a total / count by over N position ranges in parallel (default 1;
 *                               "auto" = min(pool threads, 8)); ranges beyond the first run on idle
 *                               workers of the process pool only
 *     "pool_threads": N|"auto"  the process-wide worker pool shared by every corpus this process has
 *                               open (default auto: the CPUs the process may use: affinity mask, cgroup
 *                               quota); the first explicit value wins, later ones are ignored
 *     "warm_streams": N         parallel read streams of the background warm-up (default 4)
 *     "session_ttl": SEC        close a client session unused for SEC (default 1800)
 *     "max_sessions": N         open client sessions (default 256)
 *     "session_memory_mb": MB   materialised hits over all sessions (default 2048; 0 = no limit)
 *     "session_max_hits": N     hits one stored set may materialise (default 5000000; 0 = no limit)
 *     "tiers": {"<name>": {"timeout_ms", "total_timeout_ms", "max_count_hits", "max_hits",
 *               "threads", "deny": [...]}, ...}, "default_tier": "name", "trust_tier": bool
 *                               limits by tier (limits.h); a request's "tier" only with trust_tier
 *     "embedded_in": "name"     reported in /health, /version, /info server ("embedded_in")
 *     "debug_total_delay_ms": MS  testing: reveal every background total gradually over MS
 *   error_out: NULL or where to store a malloc'd error message on failure
 *              (free with pando_server_free).
 * Returns NULL on failure.
 */
PANDO_API pando_server_t* pando_server_open(const char* corpus_dir, const char* options_json,
                                            char** error_out);

/*
 * One request, as pando-server would answer it.
 *   method:       "GET" or "POST"
 *   path:         "/query", "/status", "/values/lemma", … — may carry "?a=1&b=2"
 *                 when query_string is NULL
 *   query_string: NULL or "job=abc&limit=10" (percent-encoded as in a URL)
 *   body:         NULL or the request body (JSON for POST routes)
 *   body_len:     length of body; 0 = strlen(body)
 *   status_out:   NULL or where to store the HTTP-style status (200, 400, 404, 408, …)
 * Returns a malloc'd, NUL-terminated JSON string (never NULL unless out of memory).
 */
PANDO_API char* pando_server_request(pando_server_t* s, const char* method, const char* path,
                                     const char* query_string, const char* body, size_t body_len,
                                     int* status_out);

/* Requests in flight + background counts queued or running. */
PANDO_API size_t pando_server_busy(pando_server_t* s);

/* Seconds since the last request started (idle eviction). */
PANDO_API double pando_server_idle_seconds(pando_server_t* s);

/* The corpus directory the handle serves. Owned by the handle. */
PANDO_API const char* pando_server_corpus_dir(pando_server_t* s);

/* Cancel background counts, join workers, unmap the corpus. NULL is fine. */
PANDO_API void pando_server_close(pando_server_t* s);

/* Free a string returned by this API. NULL is fine. */
PANDO_API void pando_server_free(char* p);

#ifdef __cplusplus
}
#endif

#endif /* PANDO_SERVER_CAPI_H */
