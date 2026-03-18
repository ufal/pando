#pragma once

#include "core/types.h"
#include "query/ast.h"
#include "corpus/corpus.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <memory>

#ifdef PANDO_USE_RE2
#include <re2/re2.h>
#else
#include <regex>
#include <mutex>
#endif

namespace manatree {

struct Match {
    std::vector<CorpusPos> positions;    // start position of each query token's span
    std::vector<CorpusPos> span_ends;    // end position (inclusive) of each token's span
    // #14: name → position for named query tokens (e.g. a:[], b:[] → a→pos0, b→pos1)
    std::unordered_map<std::string, CorpusPos> name_to_position;

    // Overall match extent (handles both single-position and span tokens)
    CorpusPos first_pos() const { return positions.empty() ? 0 : positions.front(); }
    CorpusPos last_pos() const { return span_ends.empty() ? (positions.empty() ? 0 : positions.back()) : span_ends.back(); }
};

struct MatchSet {
    std::vector<Match> matches;
    size_t num_tokens = 0;

    size_t total_count = 0;
    bool   total_exact = true;

    // #16: Source | Target: pairs (source_match, target_match) when parallel query
    std::vector<std::pair<Match, Match>> parallel_matches;

    // Debug info (always populated)
    size_t seed_token = 0;
    std::vector<size_t> cardinalities;
};

// Query plan: start from the most selective token, expand outward.
struct QueryPlan {
    size_t seed = 0;
    struct Step {
        size_t from;
        size_t to;
        size_t edge_idx;
        bool   reversed;
    };
    std::vector<Step> steps;
    std::vector<size_t> cardinalities;
};

class QueryExecutor {
public:
    explicit QueryExecutor(const Corpus& corpus);

    // max_total_cap: when count_total and cap > 0, stop counting at cap and set total_exact=false
    // sample_size > 0: reservoir sample this many random matches (ignores max_matches for output).
    // random_seed: for reproducible --sample (default 0 uses time-based seed when sampling).
    // num_threads > 1: process seed positions in parallel (multi-token queries only; materializes seeds).
    MatchSet execute(const TokenQuery& query,
                     size_t max_matches = 0,
                     bool count_total = false,
                     size_t max_total_cap = 0,
                     size_t sample_size = 0,
                     uint32_t random_seed = 0,
                     unsigned num_threads = 1);

    // #16: Source | Target: run source and target queries, join on source_query.global_alignment_filters.
    // Returns MatchSet with parallel_matches filled; matches is empty.
    MatchSet execute_parallel(const TokenQuery& source_query,
                              const TokenQuery& target_query,
                              size_t max_matches = 0,
                              bool count_total = false);

private:
    // ── Cardinality estimation (O(1) per EQ condition via rev.idx) ──────

    size_t estimate_cardinality(const ConditionPtr& cond) const;
    size_t estimate_leaf(const AttrCondition& ac) const;

    // ── Query planning ──────────────────────────────────────────────────

    QueryPlan plan_query(const TokenQuery& query) const;

    // ── Per-position condition checking (avoids materializing sets) ─────

    bool check_conditions(CorpusPos pos, const ConditionPtr& cond) const;
    bool check_leaf(CorpusPos pos, const AttrCondition& ac) const;

    // ── Relation traversal from a single position ───────────────────────

    // Given a position on one side of a relation, return positions on the
    // other side.  `reversed` means we're traversing the edge backward
    // (from the right token to the left token in the original query).
    std::vector<CorpusPos> find_related(CorpusPos pos,
                                        RelationType rel,
                                        bool reversed) const;

    // Callback-based variant: avoids heap allocation for SEQUENCE (the common case).
    // Calls f(related_pos) for each related position; f returns false to stop early.
    template<typename F>
    void for_each_related(CorpusPos pos, RelationType rel, bool reversed, F&& f) const {
        if (rel == RelationType::SEQUENCE) {
            if (reversed) {
                if (pos > 0) f(pos - 1);
            } else {
                if (pos + 1 < corpus_.size()) f(pos + 1);
            }
        } else {
            auto v = find_related(pos, rel, reversed);
            for (CorpusPos r : v)
                if (!f(r)) break;
        }
    }

    // ── Seed resolution (uses inverted index for the starting token) ────

    std::vector<CorpusPos> resolve_conditions(const ConditionPtr& cond) const;
    std::vector<CorpusPos> resolve_leaf(const AttrCondition& ac) const;

    // Lazy iteration: call f(pos) for each matching position; return false from f to stop.
    void for_each_seed_position(const ConditionPtr& cond,
                                std::function<bool(CorpusPos)> f) const;

    // Expand one seed position through all plan steps and within-clause filter.
    // Calls emit(pm) for each complete partial match vector (size 2*n).
    // emit returns true to continue expanding, false to stop early.
    void expand_seed(const TokenQuery& query, const QueryPlan& plan,
                     const StructuralAttr* within_sa, CorpusPos seed_p,
                     std::function<bool(std::vector<CorpusPos>&&)> emit) const;

    // Expand one seed position to all full matches (for parallel execution).
    // Convenience wrapper around expand_seed that builds Match objects.
    std::vector<Match> expand_one_seed(const TokenQuery& query,
                                       const QueryPlan& plan,
                                       const StructuralAttr* within_sa,
                                       CorpusPos seed_p) const;

    std::string normalize_attr(const std::string& attr) const;

    static std::vector<CorpusPos> intersect(const std::vector<CorpusPos>& a,
                                            const std::vector<CorpusPos>& b);
    static std::vector<CorpusPos> unite(const std::vector<CorpusPos>& a,
                                        const std::vector<CorpusPos>& b);

    // #12: Apply global filters
    void apply_region_filters(const TokenQuery& query, MatchSet& result) const;
    void apply_global_filters(const TokenQuery& query, MatchSet& result) const;

    const Corpus& corpus_;

#ifdef PANDO_USE_RE2
    // RE2 objects are thread-safe for matching once constructed.
    // Only the cache insertion needs synchronization (handled by mutable + unique_ptr).
    mutable std::unordered_map<std::string, std::unique_ptr<re2::RE2>> regex_cache_;
    mutable std::mutex regex_cache_mutex_;  // protects cache insertion only
#else
    mutable std::unordered_map<std::string, std::regex> regex_cache_;
    mutable std::mutex regex_cache_mutex_;
#endif
};

} // namespace manatree
