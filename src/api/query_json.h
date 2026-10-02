#pragma once

#include "corpus/corpus.h"
#include "query/executor.h"
#include <string>
#include <string_view>
#include <vector>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>

namespace pando {

struct QueryOptions {
    size_t limit   = 20;
    size_t offset  = 0;
    size_t max_total = 0;
    int context    = 5;
    bool total     = false;
    bool debug     = false;
    /// When true, left/right context expands to enclosing sentence structure ``s``
    /// (falls back to ``context`` token window if no ``s`` region covers the match).
    bool sentence  = false;
    std::vector<std::string> attrs;  // empty = all token attributes in JSON; else only these
    /// When true: only `/pattern/` is regex; quoted strings are literal (matches `--strict-quoted-strings` on CLI).
    bool strict_quoted_strings = false;
    /// Alignment filters (`:: a.attr = b.attr`): include empty/"_" values (legacy behavior).
    bool allow_empty_alignment = false;
    /// P4.1: count the total over this many position ranges in parallel (1 = one thread).
    unsigned threads = 1;
    /// P4.1d: the request's thread cap (a tier's `threads`; 0 = none): bounds the
    /// lexicon scans of a background total started for this query too.
    unsigned thread_cap = 0;
    /// KonText "random sample" / "shuffle": `sample` N = a random N of the hits, shown
    /// in corpus order; `shuffle` = the hits in a random order (with `sample`: the
    /// sample in a random order). The same `seed` gives the same sample / order on
    /// every page (0 = a new random one). Every hit is enumerated; the page holds
    /// only offset + limit (shuffle) or N (sample) of them.
    size_t sample = 0;
    bool shuffle = false;
    uint32_t seed = 0;
};

// Run a single query (one statement, no trailing command). Returns (MatchSet, elapsed_ms).
std::pair<MatchSet, double> run_single_query(const Corpus& corpus,
                                            const std::string& query_text,
                                            const QueryOptions& opts);
/// Same, with a progress / cancel block: the executor publishes its progress there
/// and stops with QueryCancelled once `progress->cancel` is set (nullptr = none).
std::pair<MatchSet, double> run_single_query(const Corpus& corpus,
                                            const std::string& query_text,
                                            const QueryOptions& opts,
                                            ExecProgress* progress);

// Build JSON string for query result (same format as pando --json).
// `extra_result_fields`: raw JSON members appended inside "result" (e.g. `"job": {...}`).
// `matches_offset`: ms.matches[0] is hit number matches_offset (a page slice of a
// larger set); the page is [opts.offset, +opts.limit) of the whole set.
std::string to_query_result_json(const Corpus& corpus,
                                 const std::string& query_text,
                                 const MatchSet& ms,
                                 const QueryOptions& opts,
                                 double elapsed_ms,
                                 std::string_view extra_result_fields = {},
                                 size_t matches_offset = 0);

// Build JSON string for corpus info (CLI `show info`, /info, FFI). `operation` is the JSON
// "operation" field ("info" vs "show_info" for CLI).
// `extra_result_fields`: raw JSON members appended inside "result" (e.g. `"server": {...}`).
// The result always has `"pando": {version, build, commit, branch}` (the binary answering)
// and `"index": {...}` (index_status_json_fields).
std::string to_info_json(const Corpus& corpus, std::string_view operation = "info",
                         std::string_view extra_result_fields = {});

// P3 / versioning: which derived index files the corpus has and whether they are
// current — JSON members `"indexed_with": …, "upgraded_with": …, "bitmaps": […],
// "structure_bitmaps": […], "dep_pairs": […], "dep_head_rel": …, "fold_indexes": {…}`
// (no braces). Status per file: "ok", "stale" (present but older than its source or
// inconsistent → ignored; run `pando-index --upgrade`) or "missing".
std::string index_status_json_fields(const Corpus& corpus);

// Build JSON string listing unique values + counts for a positional or region attribute.
// Returns empty string if attribute not found.
// Positional and region strings are split on '|' (RG-5f): each component receives the
// parent row's count (same convention as multivalue_eq / membership queries).
std::string to_values_json(const Corpus& corpus, const std::string& attr_name, size_t limit = 0);

// Sorted by count descending; for CLI / reuse alongside to_values_json.
// When `split_mv` is true, pipe-separated components are counted individually (RG-5f).
std::vector<std::pair<std::string, size_t>> positional_attr_show_values_mv(const PositionalAttr& pa,
                                                                           bool split_mv = false);
std::vector<std::pair<std::string, size_t>> region_attr_show_values_mv(const StructuralAttr& sa,
                                                                       const std::string& region_attr,
                                                                       bool split_mv = false);

// Build JSON string listing all regions of a given type with their attributes.
// Returns empty string if structure type not found.
std::string to_regions_json(const Corpus& corpus, const std::string& type_name, size_t limit = 0);

// ── Full-program session API ──────────────────────────────────────────────
// Run a complete CQL program (multiple statements + commands like count, coll, etc.)
// and return the JSON output of the final command/query as a string.
// Maintains session state (named queries, named tokens) across calls on the same
// ProgramSession object, enabling cross-query workflows.

//
// P6.1 hit sets: every stored result (`Name = …;`, `Last`, a /query deposited in
// a server session) is a *recipe* — the parsed query plus the `sort` steps applied
// to it — and a *cache*: the materialised hits, built the first time a command
// needs every hit (sort, coll, tabulate, a page of a sorted set, …) and kept until
// dropped (drop_caches, memory pressure). Plain hits (no named regions / subtrees
// / token groups) are kept compact: the token positions only, 8-16 bytes per token
// instead of ~350 bytes per Match. `count / group / freq … by` on a set
// that is not materialised runs the aggregation sink instead (no hits stored), and
// a known total is kept apart from the hits, so `size` does not re-count.
// `Last` and the name of the same query share one hit set.
//
// Not thread-safe: one call at a time per session (the server locks per session).

/// A command needed the hits of a set larger than a limit: the session's max_hits
/// (materialising) or ProgramOptions::max_count_hits (anything over all hits).
struct HitSetTooLarge : std::runtime_error {
    size_t hits = 0;              // the set's hits (a lower bound when at_least)
    size_t limit = 0;
    std::string limit_name;       // "max_hits" / "max_count_hits"
    bool at_least = false;        // counting stopped at the limit
    HitSetTooLarge(const std::string& name, size_t h, size_t lim, std::string which = "max_hits",
                   bool lower_bound = false);
};

/// A command or page named a hit set the session does not have.
struct UnknownHitSet : std::runtime_error {
    explicit UnknownHitSet(const std::string& name) : std::runtime_error("unknown hit set: " + name) {}
};

struct HitSetInfo {
    std::string name;
    std::string query;            // the query text (a /run program shows its statement's program)
    bool parallel = false;
    bool materialised = false;    // the hits are in memory
    size_t hits = 0;              // materialised hits (0 when not materialised)
    bool total_known = false;
    size_t total = 0;
    bool total_exact = false;
    size_t bytes = 0;             // estimated memory of the materialised hits
    std::vector<std::vector<std::string>> sorts;   // `sort … by` steps, in order
    std::vector<std::string> aliases;              // other names of the same set ("Last")
    double idle_s = 0;            // seconds since last use
};

struct ProgramSession {
    struct Impl;
    std::unique_ptr<Impl> impl_;
    ProgramSession();
    ~ProgramSession();
    ProgramSession(ProgramSession&&) noexcept;
    ProgramSession& operator=(ProgramSession&&) noexcept;

    /// Store a /query result as the hit set `name` (empty = only `Last`) and as
    /// `Last`. `query_text` is parsed again (ParserOptions from `opts`) and kept for
    /// re-execution; `result`'s hits are kept only when they are all of them, its
    /// total when it was counted exactly. Throws on a parse error.
    void store_query(const Corpus& corpus, const std::string& name, const std::string& query_text,
                     const QueryOptions& opts, const MatchSet& result);
    bool has(const std::string& name) const;
    std::optional<HitSetInfo> info(const std::string& name) const;
    /// Every name, sorted (a set with two names is listed under each).
    std::vector<HitSetInfo> list() const;
    /// The /query text a stored set was deposited with (empty for /run programs:
    /// no single query text to hand to a background count).
    std::string query_text(const std::string& name) const;
    /// Record a total counted elsewhere (a background job over the same query).
    void set_total(const std::string& name, size_t total, bool exact);
    /// /query JSON for the page [opts.offset, +opts.limit) of stored set `name`:
    /// from the materialised hits when there are any or the set is sorted
    /// (materialising it), otherwise the query runs again for just that page.
    /// With opts.total and no known total, the total is counted (and kept).
    /// `shown_total`: when the set's total is not known, show this (count so far,
    /// exact?) instead — a background count still running; nothing is counted.
    /// Throws UnknownHitSet, HitSetTooLarge, QueryCancelled.
    std::string page_json(const Corpus& corpus, const std::string& name, const QueryOptions& opts,
                          ExecProgress* progress = nullptr, std::string_view extra_result_fields = {},
                          std::optional<std::pair<size_t, bool>> shown_total = std::nullopt);
    /// Materialised bytes over all sets (estimate).
    size_t cache_bytes() const;
    /// Drop the materialised hits of every set (they are rebuilt on demand);
    /// returns the bytes freed. Totals and sort steps are kept.
    size_t drop_caches();
    /// Refuse to materialise more than this many hits for one set (0 = no limit).
    void set_max_hits(size_t n);
    /// Admission for a materialisation: called with the estimated bytes the hits
    /// take while they are built (as Match objects, before they are compacted);
    /// the returned token is held until then. May block; may throw (QueryCancelled)
    /// to refuse. Unset = no admission control.
    using AdmitFn = std::function<std::shared_ptr<void>(size_t bytes, ExecProgress* progress)>;
    void set_admission(AdmitFn admit);
    size_t size() const;          // number of names
    void clear();
    /// P6.4: results of commands, pages and sort indexes of sets whose query is
    /// self-contained are kept in / taken from this cache (nullptr = none).
    void set_result_cache(class ResultCache* cache);
};

struct ProgramOptions {
    size_t limit   = 20;
    size_t offset  = 0;
    size_t max_total = 0;
    int context    = 5;
    bool total     = false;
    bool strict_quoted_strings = false;
    bool allow_empty_alignment = false;
    size_t group_limit = 1000;
    /// P6.6: `count by A, B`: values of B (and further fields) under each A (0 = all).
    size_t child_limit = 20;
    std::vector<std::string> attrs;
    // Collocation settings
    int coll_left = 5;
    int coll_right = 5;
    size_t coll_min_freq = 5;
    size_t coll_max_items = 50;
    size_t coll_stoplist = 0; // 0 = disabled; otherwise top-N most frequent words are excluded
    std::vector<std::string> coll_measures;
    /// P4.1: totals and `count by` over this many position ranges in parallel.
    unsigned threads = 1;
    /// Progress / cancel block for every query the program runs (nullptr = none);
    /// a set `cancel` stops the program with QueryCancelled.
    ExecProgress* progress = nullptr;
    /// Commands over all hits (count / group / freq / stats / tabulate / describe /
    /// raw / coll / dcoll / keyness / sort) refuse a set with more hits: the hits are
    /// counted up to this bound first; HitSetTooLarge (0 = no limit).
    size_t max_count_hits = 0;
};

// Run a full CQL program and return the JSON output.
// The session persists named queries across calls.
std::string run_program_json(Corpus& corpus, ProgramSession& session,
                             const std::string& cql, ProgramOptions opts = {});

} // namespace pando
