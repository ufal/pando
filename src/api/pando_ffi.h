/*
 * pando_ffi.h — C API for libpando shared library
 *
 * Provides a stable C interface for consumers that cannot link C++ directly
 * (e.g. PHP FFI, Python ctypes, other language bindings).
 *
 * Thread safety: each pando_handle_t is independent. Do not share a handle
 * across threads without external synchronization. Multiple handles to the
 * same corpus directory are fine (they share nothing).
 *
 * Memory: strings returned by pando_query / pando_info / pando_run must be
 * freed with pando_free_string(). Handles must be closed with pando_close().
 */

#ifndef PANDO_FFI_H
#define PANDO_FFI_H

/* Symbol visibility: exported from libpando.so even with -fvisibility=hidden. */
#if defined(PANDO_BUILDING_SHARED)
#   if defined(_WIN32)
#       define PANDO_API __declspec(dllexport)
#   else
#       define PANDO_API __attribute__((visibility("default")))
#   endif
#else
#   define PANDO_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle to a corpus + session. */
typedef void* pando_handle_t;

/*
 * Open a corpus directory.
 *   corpus_dir: path to the indexed corpus (directory containing .lex, .rev, etc.)
 *   preload:    if non-zero, preload all mmap'd files into memory
 * Returns NULL on failure (e.g. directory not found, missing files); then
 * pando_last_error() says why. An open handle holds no file descriptors (the
 * index files are memory-mapped and closed), so many handles can be open at once;
 * use one handle per thread.
 */
PANDO_API pando_handle_t pando_open(const char* corpus_dir, int preload);

/*
 * Why the last pando_open on this thread failed (e.g. "Cannot open …/form.lex:
 * No such file or directory"); "" after a successful open. Owned by pando:
 * valid until the next pando_open on this thread; do not free.
 */
PANDO_API const char* pando_last_error(void);

/*
 * Ask the pando_query / pando_run call running on handle h (in another thread)
 * to stop; it then returns {"ok":false,"error":"query cancelled","cancelled":true}.
 * Safe to call when nothing runs (the next call on h resets the request).
 */
PANDO_API void pando_cancel(pando_handle_t h);

/*
 * Run a single CQL query and return the JSON result.
 *   h:         handle from pando_open
 *   cql:       CQL query string (single statement, no trailing command)
 *   opts_json: JSON object with optional keys: limit, offset, max_total, context,
 *              total, debug, strict_quoted_strings, attrs (array of names).
 *              Pass NULL or "{}" for defaults.
 * Returns a malloc'd JSON string (caller must free with pando_free_string),
 * or NULL on error.
 */
PANDO_API char* pando_query(pando_handle_t h, const char* cql, const char* opts_json);

/*
 * Run a full CQL program (multiple statements + commands like count, coll, etc.)
 * and return the JSON output.
 *   h:         handle from pando_open
 *   cql:       CQL program string
 *   opts_json: JSON object with optional keys: limit, offset, max_total, context,
 *              total, strict_quoted_strings, group_limit, attrs (array), and for
 *              coll / dcoll: coll_left, coll_right (window), coll_min_freq,
 *              coll_max_items, measures (array, e.g. ["logdice","mi"]).
 *              Pass NULL or "{}" for defaults.
 * Errors come back as {"ok":false,"error":"…"} (a syntax error says where).
 * Session state (named queries) persists across calls on the same handle.
 * Returns a malloc'd JSON string (caller must free with pando_free_string),
 * or NULL on error.
 */
PANDO_API char* pando_run(pando_handle_t h, const char* cql, const char* opts_json);

/*
 * Return corpus info as JSON (size, attributes, structures, etc.).
 *   h: handle from pando_open
 * Returns a malloc'd JSON string (caller must free with pando_free_string),
 * or NULL on error.
 */
PANDO_API char* pando_info(pando_handle_t h);

/*
 * Free a string returned by pando_query / pando_run / pando_info.
 * Safe to call with NULL.
 */
PANDO_API void pando_free_string(char* s);

/*
 * Close a corpus handle and free all resources.
 * Safe to call with NULL.
 */
PANDO_API void pando_close(pando_handle_t h);

#ifdef __cplusplus
}
#endif

#endif /* PANDO_FFI_H */
