#pragma once

#include "core/types.h"
#include "query/ast.h"
#include "corpus/corpus.h"
#include <algorithm>
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <memory>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "index/fold_map.h"

#ifdef PANDO_USE_RE2
#include <re2/re2.h>
#else
#include <regex>
#include <mutex>
#endif

namespace pando {

/// Resolved structural region instance (RG-REG-2): type name + row index in that .rgn table.
struct RegionRef {
    std::string struct_name;
    size_t region_idx = 0;
};

struct Match {
    std::vector<CorpusPos> positions;    // start position of each query token's span
    std::vector<CorpusPos> span_ends;    // end position (inclusive) of each token's span
    /// Names from `np:<node …>`-style anchors → region row (filled when anchor constraints run).
    std::unordered_map<std::string, RegionRef> named_regions;
    /// `label:dep_subtree(src)` — sorted unique token positions in the subtree (head + descendants).
    std::unordered_map<std::string, std::vector<CorpusPos>> named_dep_subtrees;
    /// Token-group sidecar props (`groups/<struct>.jsonl`), set for `<err>` / overlay group matches.
    std::vector<std::pair<std::string, std::string>> token_group_props;
    /// True when this match comes from token-group expansion (`<err>` etc.), not token chain.
    bool token_group_match = false;

    // Overall match extent: min/max across ALL positions and span_ends.
    // Safe for dependency queries where positions may not be in corpus order.
    CorpusPos first_pos() const {
        if (positions.empty()) return 0;
        CorpusPos mn = positions[0];
        for (CorpusPos p : positions)
            if (p != NO_HEAD && p < mn) mn = p;
        return mn;
    }
    CorpusPos last_pos() const {
        if (positions.empty()) return 0;
        // Use span_ends when present, otherwise positions
        CorpusPos mx = 0;
        for (size_t i = 0; i < positions.size(); ++i) {
            CorpusPos end = (!span_ends.empty()) ? span_ends[i] : positions[i];
            if (end != NO_HEAD && end > mx) mx = end;
        }
        return mx;
    }

    // Collect all matched corpus positions into a sorted vector.
    // Used by KWIC display to know which positions are "highlighted".
    std::vector<CorpusPos> matched_positions() const {
        std::vector<CorpusPos> out;
        for (size_t i = 0; i < positions.size(); ++i) {
            if (positions[i] == NO_HEAD) continue;
            CorpusPos end = (!span_ends.empty()) ? span_ends[i] : positions[i];
            for (CorpusPos p = positions[i]; p <= end; ++p)
                out.push_back(p);
        }
        for (const auto& kv : named_dep_subtrees) {
            for (CorpusPos p : kv.second)
                out.push_back(p);
        }
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        return out;
    }
};

// O1: Lightweight name→position resolution. Build once from query, resolve at read time.
using NameIndexMap = std::unordered_map<std::string, size_t>;

inline NameIndexMap build_name_map(const TokenQuery& query) {
    NameIndexMap map;
    for (size_t i = 0; i < query.tokens.size(); ++i)
        if (!query.tokens[i].name.empty())
            map[query.tokens[i].name] = i;
    return map;
}

// Resolve a named token to its position in a match, using the name map.
inline CorpusPos resolve_name(const Match& m, const NameIndexMap& names,
                              const std::string& name) {
    auto it = names.find(name);
    if (it == names.end() || it->second >= m.positions.size()) return NO_HEAD;
    return m.positions[it->second];
}

/// Normalize attribute names from queries: `namespace/key` → `namespace.key` (except
/// `feats/Feature`, which stays slash-separated or maps to `feats#Feature` / `feats_Feature`
/// when a split column exists — same rules as QueryExecutor::normalize_attr).
std::string normalize_query_attr_name(const Corpus& corpus, const std::string& attr);

/// True if `name` is `feats/Feature` (UD sub-key on combined `feats` column, query form).
bool feats_is_subkey(const std::string& name, std::string& feat_name_out);

/// True if corpus has a materialized UD split column for `feat_name` (`feats#Name` or legacy `feats_Name`).
bool corpus_has_ud_split_feats_column(const Corpus& corpus, const std::string& feat_name);
/// Value for one UD feature from a combined `feats` blob (`Key=Val|…`), or `"_"` if absent.
std::string feats_extract_value(std::string_view feats_blob, const std::string& feat_name);

/// If `field` is `tcnt(region_label)`, returns the number of token positions in that named
/// region anchor, or in a `dep_subtree` binding. Returns `std::nullopt` if `field` is not `tcnt(...)`.
inline std::optional<std::string> evaluate_tcnt_tabulate_field(
    const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
    const std::string& field) {
    if (field.size() < 7 || field.compare(0, 5, "tcnt(") != 0 || field.back() != ')')
        return std::nullopt;
    const std::string inner = field.substr(5, field.size() - 6);
    if (inner.empty() || inner.find('.') != std::string::npos)
        throw std::runtime_error("Invalid tcnt(...) field: expected tcnt(region_name)");

    auto ds = m.named_dep_subtrees.find(inner);
    if (ds != m.named_dep_subtrees.end())
        return std::to_string(static_cast<long long>(ds->second.size()));

    auto nr = m.named_regions.find(inner);
    if (nr != m.named_regions.end()) {
        const RegionRef& rr = nr->second;
        if (!corpus.has_structure(rr.struct_name))
            return std::string("0");
        const auto& sa = corpus.structure(rr.struct_name);
        Region rg = sa.get(rr.region_idx);
        if (rg.start > rg.end)
            return std::string("0");
        return std::to_string(static_cast<long long>(rg.end - rg.start + 1));
    }
    if (resolve_name(m, name_map, inner) != NO_HEAD) {
        // For token-group anchor matches (`n:<err>; ...`), the label is represented in the
        // stripped name map as slot 0 so group-aware commands can project `n.*`. Here that
        // should count group member tokens, not be rejected as a query-token label.
        if (m.token_group_match)
            return std::to_string(static_cast<long long>(m.matched_positions().size()));
        throw std::runtime_error(
            "tcnt(...) counts tokens inside a named region anchor; '" + inner +
            "' refers to a query token, not a region binding");
    }
    throw std::runtime_error(
        "tcnt(...) requires a named region binding; '" + inner +
        "' does not refer to a region anchor from the query");
}

/// If `field` is `forms(region_label)`, returns token forms for that label.
/// For token-group matches, disjoint spans are joined as " ... " (e.g. "booked ... over").
inline std::optional<std::string> evaluate_forms_tabulate_field(
    const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
    const std::string& field) {
    if (field.size() < 8 || field.compare(0, 6, "forms(") != 0 || field.back() != ')')
        return std::nullopt;
    const std::string inner = field.substr(6, field.size() - 7);
    if (inner.empty() || inner.find('.') != std::string::npos)
        throw std::runtime_error("Invalid forms(...) field: expected forms(region_name)");
    if (!corpus.has_attr("form"))
        throw std::runtime_error("forms(...) requires positional attribute 'form'");
    const auto& form = corpus.attr("form");

    auto emit_span = [&](CorpusPos start, CorpusPos end, std::string& out) {
        bool first = out.empty();
        for (CorpusPos p = start; p <= end; ++p) {
            if (!first) out += " ";
            out += std::string(form.value_at(p));
            first = false;
        }
    };

    auto nr = m.named_regions.find(inner);
    if (nr != m.named_regions.end()) {
        const RegionRef& rr = nr->second;
        if (!corpus.has_structure(rr.struct_name)) return std::string{};
        const auto& sa = corpus.structure(rr.struct_name);
        Region rg = sa.get(rr.region_idx);
        if (rg.start > rg.end) return std::string{};
        std::string out;
        emit_span(rg.start, rg.end, out);
        return out;
    }

    if (resolve_name(m, name_map, inner) != NO_HEAD && m.token_group_match) {
        std::vector<std::pair<CorpusPos, CorpusPos>> spans;
        spans.reserve(m.positions.size());
        for (size_t i = 0; i < m.positions.size(); ++i) {
            if (m.positions[i] == NO_HEAD) continue;
            CorpusPos e = (!m.span_ends.empty()) ? m.span_ends[i] : m.positions[i];
            spans.emplace_back(m.positions[i], e);
        }
        if (spans.empty()) return std::string{};
        std::sort(spans.begin(), spans.end());
        std::string out;
        for (size_t i = 0; i < spans.size(); ++i) {
            if (i) out += " ... ";
            std::string part;
            emit_span(spans[i].first, spans[i].second, part);
            out += part;
        }
        return out;
    }

    if (resolve_name(m, name_map, inner) != NO_HEAD) {
        throw std::runtime_error(
            "forms(...) requires a named region anchor; '" + inner +
            "' refers to a query token, not a region binding");
    }
    throw std::runtime_error(
        "forms(...) requires a named region binding; '" + inner +
        "' does not refer to a region anchor from the query");
}

/// If `field` is `spellout(region_label,attr)`, returns token spellout text from
/// positional attr `attr` for that region/group, joining disjoint spans with " ... ".
inline std::optional<std::string> evaluate_spellout_tabulate_field(
    const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
    const std::string& field) {
    if (field.size() < 13 || field.compare(0, 9, "spellout(") != 0 || field.back() != ')')
        return std::nullopt;
    const std::string inner = field.substr(9, field.size() - 10);
    size_t comma = inner.find(',');
    if (comma == std::string::npos || comma == 0 || comma + 1 >= inner.size())
        throw std::runtime_error("Invalid spellout(...) field: expected spellout(region_name,attr)");
    auto trim_spaces = [](std::string s) {
        while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
        size_t i = 0;
        while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
        if (i) s.erase(0, i);
        return s;
    };
    std::string label = trim_spaces(inner.substr(0, comma));
    std::string attr = trim_spaces(inner.substr(comma + 1));
    if (label.find('.') != std::string::npos || attr.find('.') != std::string::npos)
        throw std::runtime_error("Invalid spellout(...) field: expected spellout(region_name,attr)");
    if (!corpus.has_attr(attr))
        throw std::runtime_error("spellout(...) requires a positional attribute '" + attr + "'");
    const auto& pa = corpus.attr(attr);

    auto emit_span = [&](CorpusPos start, CorpusPos end, std::string& out) {
        bool first = out.empty();
        for (CorpusPos p = start; p <= end; ++p) {
            if (!first) out += " ";
            out += std::string(pa.value_at(p));
            first = false;
        }
    };

    auto nr = m.named_regions.find(label);
    if (nr != m.named_regions.end()) {
        const RegionRef& rr = nr->second;
        if (!corpus.has_structure(rr.struct_name)) return std::string{};
        const auto& sa = corpus.structure(rr.struct_name);
        Region rg = sa.get(rr.region_idx);
        if (rg.start > rg.end) return std::string{};
        std::string out;
        emit_span(rg.start, rg.end, out);
        return out;
    }

    if (resolve_name(m, name_map, label) != NO_HEAD && m.token_group_match) {
        std::vector<std::pair<CorpusPos, CorpusPos>> spans;
        spans.reserve(m.positions.size());
        for (size_t i = 0; i < m.positions.size(); ++i) {
            if (m.positions[i] == NO_HEAD) continue;
            CorpusPos e = (!m.span_ends.empty()) ? m.span_ends[i] : m.positions[i];
            spans.emplace_back(m.positions[i], e);
        }
        if (spans.empty()) return std::string{};
        std::sort(spans.begin(), spans.end());
        std::string out;
        for (size_t i = 0; i < spans.size(); ++i) {
            if (i) out += " ... ";
            std::string part;
            emit_span(spans[i].first, spans[i].second, part);
            out += part;
        }
        return out;
    }

    if (resolve_name(m, name_map, label) != NO_HEAD) {
        throw std::runtime_error(
            "spellout(...) requires a named region anchor; '" + label +
            "' refers to a query token, not a region binding");
    }
    throw std::runtime_error(
        "spellout(...) requires a named region binding; '" + label +
        "' does not refer to a region anchor from the query");
}

/// Project one tabulate / group-by field. Throws `std::runtime_error` for unknown attribute
/// names (token, region, or named-region binding); empty string is only returned when the
/// attribute exists but has no value at this match (e.g. position outside a region).
std::string read_tabulate_field(const Corpus& corpus, const Match& m,
                                const NameIndexMap& name_map,
                                const std::string& field);

class PositionalAttr;
class StructuralAttr;

// Streaming aggregation (group/count/freq): integer-keyed buckets, decode for display.
struct AggregateBucketData {
    struct VecHash {
        size_t operator()(const std::vector<int64_t>& v) const noexcept;
    };
    struct VecEq {
        bool operator()(const std::vector<int64_t>& a, const std::vector<int64_t>& b) const noexcept {
            return a == b;
        }
    };

    struct Column {
        /// RegionFromBinding: `n.form` where `n` labels `<contr>` — same disambiguation as
        /// read_tabulate_field (named region before positional when both exist).
        enum class Kind { Positional, Region, RegionFromBinding, FeatsComposite } kind =
                Kind::Positional;
        const PositionalAttr* pa = nullptr;
        const StructuralAttr* sa = nullptr;
        std::string region_attr_name;
        /// When kind == FeatsComposite: UD feature name (e.g. `Number`); `pa` is combined `feats`.
        std::string feats_sub_key;
        /// Named token for field anchor; empty = match start (first_pos).
        std::string named_anchor;
    };

    size_t total_hits = 0;
    bool total_exact = true;
    std::vector<Column> columns;
    std::unordered_map<std::vector<int64_t>, size_t, VecHash, VecEq> counts;

    struct RegionInternCol {
        std::unordered_map<std::string, int64_t> str_to_id;
        std::vector<std::string> id_to_str;
    };
    /// Parallel to columns; only used when column.kind == Region.
    std::vector<RegionInternCol> region_intern;
};

/// Decode one bucket key to the same tab-separated string as make_key/read_field.
std::string decode_aggregate_bucket_key(const AggregateBucketData& data,
                                          const std::vector<int64_t>& key);

// ── Region cursor for amortized O(1) region lookup ──────────────────────
//
// When iterating sorted corpus positions (e.g. from an inverted index posting
// list), this cursor advances linearly through the sorted region array,
// achieving amortized O(1) per lookup instead of O(log N) binary search.

struct RegionCursor {
    const Region* regions = nullptr;
    size_t n_regions = 0;
    size_t cur = 0;

    RegionCursor() = default;

    explicit RegionCursor(const StructuralAttr& sa)
        : regions(sa.region_data()), n_regions(sa.region_count()), cur(0) {}

    // Find the region containing pos. Assumes positions arrive in
    // non-decreasing order. Returns -1 if pos falls in a gap.
    int64_t find(CorpusPos pos) {
        while (cur < n_regions && regions[cur].end < pos)
            ++cur;
        if (cur < n_regions && regions[cur].start <= pos)
            return static_cast<int64_t>(cur);
        return -1;
    }

};

// ── Pre-resolved region filter (avoid re-parsing per match) ─────────────

struct ResolvedRegionFilter {
    const StructuralAttr* sa = nullptr;
    std::string attr_name;
    CompOp op = CompOp::EQ;
    std::string value;
    std::string anchor_name;
    bool has_reverse = false;
};

struct MatchSet {
    std::vector<Match> matches;
    size_t num_tokens = 0;

    size_t total_count = 0;
    bool   total_exact = true;

    // #16: Source | Target: pairs (source_match, target_match) when parallel query
    std::vector<std::pair<Match, Match>> parallel_matches;

    /// When set, group/count/freq used streaming buckets (matches may be empty).
    std::shared_ptr<AggregateBucketData> aggregate_buckets;

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
                     unsigned num_threads = 1,
                     const std::vector<std::string>* aggregate_by_fields = nullptr,
                     bool skip_name_validation = false);

    // #16: Source | Target: run source and target queries, join on source_query.global_alignment_filters.
    // Returns MatchSet with parallel_matches filled; matches is empty.
    MatchSet execute_parallel(const TokenQuery& source_query,
                              const TokenQuery& target_query,
                              size_t max_matches = 0,
                              bool count_total = false);

    /// Single-token `label:dep_subtree(src)` executed as its own statement: `src` resolves to
    /// token positions via `source_name_map` / `MatchSet` (session named query or prior `Last`).
    MatchSet execute_dep_subtree_from_named(const TokenQuery& query,
                                            const MatchSet& source_matches,
                                            const NameIndexMap& source_name_map,
                                            size_t max_matches = 0,
                                            bool count_total = false,
                                            size_t max_total_cap = 0) const;

    /// Same token-name indices as execute() / Match.positions (strips region anchors first).
    static NameIndexMap build_name_map_for_stripped_query(const TokenQuery& query);

    /// RG-REG-5: how to bind named regions when a single anchor position is contained
    /// in multiple candidate rows (nested / overlapping / zero-width structural types).
    ///
    /// * Fanout (default): yield one Match per row that matches attrs/peer clauses.
    ///   For multiple anchor constraints, the Cartesian product is produced and each
    ///   fanned-out Match counts toward limits/caps as an independent match.
    /// * Innermost: pick the single tightest-enclosing row (smallest span containing
    ///   the anchor position); deterministic tiebreak on region_idx. Flat types behave
    ///   identically under both modes.
    enum class AnchorBindingMode { Fanout, Innermost };
    void set_anchor_binding_mode(AnchorBindingMode m) { anchor_binding_mode_ = m; }
    AnchorBindingMode anchor_binding_mode() const { return anchor_binding_mode_; }

private:
    AnchorBindingMode anchor_binding_mode_ = AnchorBindingMode::Fanout;
    // ── Cardinality estimation (O(1) per EQ condition via rev.idx) ──────

    size_t estimate_cardinality(const ConditionPtr& cond) const;
    size_t estimate_leaf(const AttrCondition& ac) const;

    // ── Query planning ──────────────────────────────────────────────────

    QueryPlan plan_query(const TokenQuery& query) const;

    /// Fills `m.named_dep_subtrees` for each `dep_subtree` token that has a label.
    void materialize_dep_subtree_bindings(const TokenQuery& query, Match& m) const;

    // ── Per-position condition checking (avoids materializing sets) ─────

    bool check_conditions(CorpusPos pos, const ConditionPtr& cond) const;
    bool check_leaf(CorpusPos pos, const AttrCondition& ac) const;

    // Multivalue cardinality at a position (same rules as [nvals(attr) op N]).
    std::optional<int64_t> nvals_cardinality_at(CorpusPos pos, const std::string& attr_spec) const;

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
    // Template version: avoids std::function overhead for the EQ leaf hot path.
    template<typename F>
    void for_each_seed_position(const ConditionPtr& cond, F&& f) const {
        // Fast path: simple EQ on a positional attribute — avoids std::function entirely.
        // Skip for multivalue attrs: component values aren't in the main lexicon,
        // so we must delegate to the impl which uses the .mv.rev index.
        if (cond && cond->is_leaf) {
            const AttrCondition& ac = cond->leaf;
            if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive) {
                std::string name = normalize_attr(ac.attr);
                if (corpus_.has_attr(name) && !corpus_.is_multivalue(name)) {
                    const auto& pa = corpus_.attr(name);
                    LexiconId id = (ac.resolved_id >= 0)
                        ? static_cast<LexiconId>(ac.resolved_id)
                        : pa.lexicon().lookup(ac.value);
                    if (id == UNKNOWN_LEX) return;
                    pa.for_each_position_id(id, std::forward<F>(f));
                    return;
                }
            }
        }
        // Complex cases: delegate to non-template impl (AND/OR recursion, NEQ, regex, etc.)
        for_each_seed_position_impl(cond, std::function<bool(CorpusPos)>(std::forward<F>(f)));
    }

    // Non-template implementation for complex condition trees.
    void for_each_seed_position_impl(const ConditionPtr& cond,
                                     std::function<bool(CorpusPos)> f) const;

    // Expand one seed position through all plan steps and within-clause filter.
    // Calls emit(pm) for each complete partial match vector (size 2*n).
    // emit returns true to continue expanding, false to stop early.
    // within_span_semantics: true when corpus declares `nested=` or `overlapping=`
    // for this structure — use span-in-some-region check; false = fast path (one
    // `find_region` identity per match, correct for typical flat regions).
    void expand_seed(const TokenQuery& query, const QueryPlan& plan,
                     const StructuralAttr* within_sa, CorpusPos seed_p,
                     std::function<bool(std::vector<CorpusPos>&&)> emit,
                     bool within_span_semantics) const;

    // Expand one seed position to all full matches (for parallel execution).
    // Convenience wrapper around expand_seed that builds Match objects.
    std::vector<Match> expand_one_seed(const TokenQuery& query,
                                       const QueryPlan& plan,
                                       const StructuralAttr* within_sa,
                                       CorpusPos seed_p,
                                       bool within_span_semantics) const;

    // #25: Pre-resolve EQ string values to LexiconIds for integer comparison in check_leaf.
    void compile_conditions(const ConditionPtr& cond) const;
    void compile_query(const TokenQuery& query) const;

    /// Fail fast when a **token or region label** inside the query is used in the wrong role
    /// (see dev/VARIABLE-BINDINGS-CHECKLIST.md). Not related to session-level **named queries**
    /// (`GroupCommand.query_name`). Call on the pre–anchor-strip query.
    /// When `merge_labels_from` is set (parallel `source with target`), include that query's
    /// labels so `:: a.attr = b.attr` can name tokens from either side.
    void validate_query_name_bindings(const TokenQuery& query,
                                    const TokenQuery* merge_labels_from = nullptr) const;

    std::string normalize_attr(const std::string& attr) const;

    static std::vector<CorpusPos> intersect(const std::vector<CorpusPos>& a,
                                            const std::vector<CorpusPos>& b);
    static std::vector<CorpusPos> unite(const std::vector<CorpusPos>& a,
                                        const std::vector<CorpusPos>& b);

    // #12: Apply global filters (name_map built from query via build_name_map)
    void apply_region_filters(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const;
    void apply_global_filters(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const;

    // Region anchor constraint (from <s>, </s> in query)
    struct AnchorConstraint {
        size_t token_idx;          // index of the real token this anchor binds to
        std::string region;        // region name (e.g. "s")
        bool is_start;             // true = token must be at region start, false = at region end
        std::vector<std::pair<std::string, std::string>> attrs;  // optional attr constraints
        /// Non-empty → bind this region row to `named_regions[name]` (e.g. `np:<node …>`).
        std::string binding_name;
        /// Peer clauses from anchor text: `rchild` / `contains` (see `AnchorRegionClause`).
        std::vector<AnchorRegionClause> anchor_region_clauses;
        /// True: region-start anchor with no following token — enumerate all rows of `region`.
        bool region_enumeration = false;
    };

    /// Verify anchor constraints on m, fill m.named_regions for bindings; false = reject match.
    /// Single-binding convenience: in Fanout mode picks the first resolution, in
    /// Innermost mode picks the innermost row. Prefer `expand_anchor_constraints`
    /// on hot paths where fan-out should produce multiple output matches.
    bool resolve_anchor_constraints(Match& m,
                                    const std::vector<AnchorConstraint>& constraints) const;

    /// RG-REG-5 fan-out: enumerate every valid anchor-binding assignment for `base`
    /// (Cartesian product across constraints). `emit(m)` is called once per resolved
    /// Match; return false from emit to stop early. In Innermost mode at most one
    /// Match is emitted per input. Returns the number of emissions.
    size_t expand_anchor_constraints(
        const Match& base,
        const std::vector<AnchorConstraint>& constraints,
        const std::function<bool(Match&)>& emit) const;

    /// True if all `q.global_region_filters` hold (inline path; same as add_match filter).
    bool match_passes_inline_region_filters(const Match& m, const TokenQuery& q,
                                            const NameIndexMap& name_map) const;

    /// `np:<s> :: np.s_* = …` with no query tokens: scan region rows, bind name, apply :: filters.
    MatchSet execute_region_enumeration(const std::vector<AnchorConstraint>& anchor_constraints,
                                        const TokenQuery& q,
                                        size_t max_matches,
                                        bool count_total,
                                        size_t max_total_cap,
                                        size_t sample_size,
                                        uint32_t random_seed) const;

    // Strip anchor tokens from a query, returning the cleaned query and constraints
    static TokenQuery strip_anchors(const TokenQuery& query,
                                    std::vector<AnchorConstraint>& constraints);

    // Apply anchor constraints as post-filter
    void apply_anchor_filters(const std::vector<AnchorConstraint>& constraints,
                              MatchSet& result) const;

    // Apply within-having as post-filter
    void apply_within_having(const TokenQuery& query, MatchSet& result) const;

    // Apply containing/not-containing clauses as post-filter
    void apply_containing(const TokenQuery& query, MatchSet& result) const;

    // Apply not-within as post-filter (inverts within)
    void apply_not_within(const TokenQuery& query, MatchSet& result) const;

    // Apply positional ordering constraints (:: a < b)
    void apply_position_orders(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const;

    bool match_survives_post_filters_for_aggregate(const TokenQuery& query,
                                                   const NameIndexMap& name_map,
                                                   const std::vector<AnchorConstraint>& anchor_constraints,
                                                   MatchSet& scratch,
                                                   const Match& m) const;

    // ── Fast aggregation path ────────────────────────────────────────────
    //
    // For single-token queries with aggregation and no complex post-filters,
    // bypass Match construction entirely. Iterates seed positions and
    // computes bucket keys using integer array lookups only.
    // Named anchors in aggregate fields must resolve to token index 0 (same as fill_aggregate_key).
    // RG-REG-7: `RegionFromBinding` columns are supported when every anchor constraint is
    // "simple" (token_idx=0, non-multi structural type, no peer clauses), in which case the
    // fast path resolves each anchor inline per seed position and reads the bound region row.
    bool try_fast_aggregate(const TokenQuery& q,
                            AggregateBucketData& agg,
                            const std::vector<ResolvedRegionFilter>& resolved_filters,
                            const std::vector<AnchorConstraint>& token_anchor_constraints,
                            size_t max_total_cap,
                            MatchSet& result,
                            const NameIndexMap& name_map) const;

    // Pre-resolve :: region filters to avoid per-match string parsing.
    std::vector<ResolvedRegionFilter> resolve_region_filters(
            const TokenQuery& q) const;

    const Corpus& corpus_;

    // Fold map cache: keyed by "attr:mode" where mode is "lc", "noacc", "lcnoacc"
    const FoldMap& get_fold_map(const std::string& attr, bool case_fold, bool accent_fold) const;
    mutable std::unordered_map<std::string, FoldMap> fold_map_cache_;
    mutable std::mutex fold_map_mutex_;

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

} // namespace pando
