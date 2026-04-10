#pragma once

#include "corpus/corpus.h"
#include "query/executor.h"
#include <string>
#include <string_view>
#include <vector>
#include <memory>

namespace pando {

struct QueryOptions {
    size_t limit   = 20;
    size_t offset  = 0;
    size_t max_total = 0;
    int context    = 5;
    bool total     = false;
    bool debug     = false;
    std::vector<std::string> attrs;  // empty = all token attributes in JSON; else only these
    /// When true: only `/pattern/` is regex; quoted strings are literal (matches `--strict-quoted-strings` on CLI).
    bool strict_quoted_strings = false;
};

// Run a single query (one statement, no trailing command). Returns (MatchSet, elapsed_ms).
std::pair<MatchSet, double> run_single_query(const Corpus& corpus,
                                            const std::string& query_text,
                                            const QueryOptions& opts);

// Build JSON string for query result (same format as pando --json).
std::string to_query_result_json(const Corpus& corpus,
                                 const std::string& query_text,
                                 const MatchSet& ms,
                                 const QueryOptions& opts,
                                 double elapsed_ms);

// Build JSON string for corpus info (CLI `show info`, /info, FFI). `operation` is the JSON
// "operation" field ("info" vs "show_info" for CLI).
std::string to_info_json(const Corpus& corpus, std::string_view operation = "info");

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

struct ProgramSession {
    struct Impl;
    std::unique_ptr<Impl> impl_;
    ProgramSession();
    ~ProgramSession();
    ProgramSession(ProgramSession&&) noexcept;
    ProgramSession& operator=(ProgramSession&&) noexcept;
};

struct ProgramOptions {
    size_t limit   = 20;
    size_t offset  = 0;
    size_t max_total = 0;
    int context    = 5;
    bool total     = false;
    bool strict_quoted_strings = false;
    size_t group_limit = 1000;
    std::vector<std::string> attrs;
    // Collocation settings
    int coll_left = 5;
    int coll_right = 5;
    size_t coll_min_freq = 5;
    size_t coll_max_items = 50;
    std::vector<std::string> coll_measures;
};

// Run a full CQL program and return the JSON output.
// The session persists named queries across calls.
std::string run_program_json(Corpus& corpus, ProgramSession& session,
                             const std::string& cql, ProgramOptions opts = {});

} // namespace pando
