#include "query/executor.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <queue>
#include <random>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <functional>

#ifndef PANDO_USE_RE2
#include <mutex>
#endif

namespace manatree {

// RG-5f: Multivalue membership — check if any pipe-separated component of `val`
// equals `target`.  Falls back to direct comparison when no pipe is present.
static bool multivalue_eq(std::string_view val, const std::string& target) {
    if (val == target) return true;
    // Fast path: no pipe → single value, already compared above.
    auto pipe = val.find('|');
    if (pipe == std::string_view::npos) return false;
    // Split on '|' and check each component.
    size_t start = 0;
    while (start < val.size()) {
        size_t p = val.find('|', start);
        if (p == std::string_view::npos) p = val.size();
        if (val.substr(start, p - start) == target) return true;
        start = p + 1;
    }
    return false;
}

// Non-empty pipe-separated components (aligned with multivalue_eq / show values explode).
static size_t count_mv_components(std::string_view val) {
    size_t count = 0;
    size_t start = 0;
    while (start <= val.size()) {
        size_t p = val.find('|', start);
        size_t end = (p == std::string_view::npos) ? val.size() : p;
        if (end > start) ++count;
        if (p == std::string_view::npos) break;
        start = p + 1;
    }
    return count;
}

static void insert_mv_components(std::unordered_set<std::string>& out, std::string_view val) {
    size_t start = 0;
    while (start <= val.size()) {
        size_t p = val.find('|', start);
        size_t end = (p == std::string_view::npos) ? val.size() : p;
        if (end > start) out.insert(std::string(val.substr(start, end - start)));
        if (p == std::string_view::npos) break;
        start = p + 1;
    }
}

static int64_t nvals_from_string(std::string_view val, bool is_mv_declared) {
    if (is_mv_declared || val.find('|') != std::string_view::npos)
        return static_cast<int64_t>(count_mv_components(val));
    if (val.empty() || val == "_")
        return 0;
    return 1;
}

static bool compare_nvals_count(int64_t n, CompOp op, int64_t rhs) {
    switch (op) {
        case CompOp::EQ:  return n == rhs;
        case CompOp::NEQ: return n != rhs;
        case CompOp::LT:  return n < rhs;
        case CompOp::GT:  return n > rhs;
        case CompOp::LTE: return n <= rhs;
        case CompOp::GTE: return n >= rhs;
        case CompOp::REGEX: return false;
    }
    return false;
}

QueryExecutor::QueryExecutor(const Corpus& corpus) : corpus_(corpus) {}

const FoldMap& QueryExecutor::get_fold_map(const std::string& attr, bool case_fold, bool accent_fold) const {
    std::string key = attr + ":" + (case_fold ? "1" : "0") + (accent_fold ? "1" : "0");
    std::lock_guard<std::mutex> lock(fold_map_mutex_);
    auto it = fold_map_cache_.find(key);
    if (it != fold_map_cache_.end()) return it->second;
    const auto& lex = corpus_.attr(attr).lexicon();
    FoldMap fm;
    if (case_fold && accent_fold) fm = FoldMap::build_lc_no_accents(lex);
    else if (case_fold) fm = FoldMap::build_lowercase(lex);
    else fm = FoldMap::build_no_accents(lex);
    auto [ins, _] = fold_map_cache_.emplace(key, std::move(fm));
    return ins->second;
}

// ── Attribute name normalization ────────────────────────────────────────

std::string QueryExecutor::normalize_attr(const std::string& attr) const {
    if (attr.size() > 6 && attr.compare(0, 6, "feats.") == 0) {
        std::string split_name = "feats_" + attr.substr(6);
        if (corpus_.has_attr(split_name)) return split_name;
        return attr;
    }
    if (attr.size() > 6 && attr.compare(0, 6, "feats_") == 0) {
        if (corpus_.has_attr(attr)) return attr;
        return "feats." + attr.substr(6);
    }
    return attr;
}

// ── Combined feats helpers ──────────────────────────────────────────────
//
// When feats is stored as a single string like "Case=Nom|Number=Sing",
// extract the value for a specific feature name.

static bool is_feats_sub(const std::string& name, std::string& feat_name) {
    if (name.size() > 6 && name.compare(0, 6, "feats.") == 0) {
        feat_name = name.substr(6);
        return true;
    }
    return false;
}

static std::string extract_feat(std::string_view feats,
                                const std::string& feat_name) {
    if (feats == "_" || feats.empty()) return "_";

    std::string prefix = feat_name + "=";
    size_t search = 0;
    while (search < feats.size()) {
        size_t pos = feats.find(prefix, search);
        if (pos == std::string::npos) return "_";
        if (pos == 0 || feats[pos - 1] == '|') {
            size_t val_start = pos + prefix.size();
            size_t val_end = feats.find('|', val_start);
            if (val_end == std::string::npos) val_end = feats.size();
            return std::string(feats.substr(val_start, val_end - val_start));
        }
        search = pos + 1;
    }
    return "_";
}

static bool feats_entry_matches(std::string_view feats,
                                const std::string& feat_name,
                                const std::string& value) {
    return extract_feat(feats, feat_name) == value;
}

// ── Condition compilation (#25): pre-resolve EQ values to LexiconIds ────
//
// Walk the condition tree and resolve string EQ values to integer IDs.
// check_leaf can then compare id_at(pos) == resolved_id instead of
// value_at(pos) == string, avoiding a lexicon lookup per position.

void QueryExecutor::compile_conditions(const ConditionPtr& cond) const {
    if (!cond) return;
    if (cond->is_leaf) {
        AttrCondition& ac = const_cast<AttrCondition&>(cond->leaf);
        if (ac.is_nvals) return;
        if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive) {
            std::string name = normalize_attr(ac.attr);
            // Only resolve for positional attrs (not feats sub, not region attrs)
            std::string feat_name;
            if (!is_feats_sub(name, feat_name) && corpus_.has_attr(name)
                && !corpus_.is_multivalue(name)) {
                ac.resolved_id = corpus_.attr(name).lexicon().lookup(ac.value);
            }
        }
        return;
    }
    if (cond->is_structural) {
        compile_conditions(cond->nested_conditions);
        return;
    }
    if (cond->is_count) {
        compile_conditions(cond->count_filter);
        return;
    }
    compile_conditions(cond->left);
    compile_conditions(cond->right);
}

void QueryExecutor::compile_query(const TokenQuery& query) const {
    for (const auto& tok : query.tokens)
        compile_conditions(tok.conditions);
    compile_conditions(query.within_having);
    for (const auto& cc : query.containing_clauses)
        compile_conditions(cc.subtree_cond);
}

// ── Cardinality estimation ──────────────────────────────────────────────
//
// Uses the rev.idx to get exact counts in O(1) per EQ condition.
// For AND, takes min; for OR, takes sum.  This is a conservative upper
// bound that's free to compute.

size_t QueryExecutor::estimate_leaf(const AttrCondition& ac) const {
    std::string name = normalize_attr(ac.attr);
    if (ac.is_nvals)
        return static_cast<size_t>(corpus_.size());

    // Combined feats mode: scan the feats lexicon for matching entries
    std::string feat_name;
    if (is_feats_sub(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        size_t total = 0;
        LexiconId n = pa.lexicon().size();
        for (LexiconId id = 0; id < n; ++id) {
            if (feats_entry_matches(pa.lexicon().get(id), feat_name, ac.value))
                total += pa.count_of_id(id);
        }
        if (ac.op == CompOp::NEQ)
            return static_cast<size_t>(corpus_.size()) - total;
        return total;
    }

    if (!corpus_.has_attr(name)) {
        // Try region attribute fallback: name = "struct_region"
        auto us = name.find('_');
        if (us != std::string::npos && us + 1 < name.size()) {
            std::string struct_name = name.substr(0, us);
            std::string region_attr = name.substr(us + 1);
            if (corpus_.has_structure(struct_name)) {
                const auto& sa = corpus_.structure(struct_name);
                if (sa.has_region_attr(region_attr)) {
                    if (ac.op == CompOp::EQ && sa.has_region_value_reverse(region_attr)) {
                        size_t spans = sa.token_span_sum_for_attr_eq(region_attr, ac.value);
                        if (spans == SIZE_MAX)
                            return static_cast<size_t>(corpus_.size());
                        return std::min(spans, static_cast<size_t>(corpus_.size()));
                    }
                    return static_cast<size_t>(corpus_.size());
                }
            }
        }
        return 0;
    }
    const auto& pa = corpus_.attr(name);

    // Fold-aware cardinality estimation
    if ((ac.case_insensitive || ac.diacritics_insensitive) && ac.op == CompOp::EQ) {
        const auto& fm = get_fold_map(name, ac.case_insensitive, ac.diacritics_insensitive);
        std::string folded_query;
        if (ac.case_insensitive && ac.diacritics_insensitive)
            folded_query = FoldMap::to_lower_no_accents(ac.value);
        else if (ac.case_insensitive)
            folded_query = FoldMap::to_lower(ac.value);
        else
            folded_query = FoldMap::strip_accents(ac.value);
        const auto& ids = fm.lookup(folded_query);
        size_t total = 0;
        for (LexiconId id : ids)
            total += pa.count_of_id(id);
        return total;
    }

    // RG-5f: MV-aware cardinality via component reverse index
    if (corpus_.is_multivalue(name) && pa.has_mv() && ac.op == CompOp::EQ) {
        return pa.mv_count_of(ac.value);
    }
    if (corpus_.is_multivalue(name) && pa.has_mv() && ac.op == CompOp::NEQ) {
        size_t eq = pa.mv_count_of(ac.value);
        return static_cast<size_t>(corpus_.size()) - eq;
    }

    switch (ac.op) {
        case CompOp::EQ:
            return pa.count_of(ac.value);
        case CompOp::NEQ: {
            size_t eq = pa.count_of(ac.value);
            return static_cast<size_t>(corpus_.size()) - eq;
        }
        default:
            return static_cast<size_t>(corpus_.size());
    }
}

size_t QueryExecutor::estimate_cardinality(const ConditionPtr& cond) const {
    if (!cond) return static_cast<size_t>(corpus_.size());
    if (cond->is_leaf) return estimate_leaf(cond->leaf);
    if (cond->is_structural) return static_cast<size_t>(corpus_.size());  // conservative estimate
    if (cond->is_count) return static_cast<size_t>(corpus_.size());       // conservative estimate

    size_t l = estimate_cardinality(cond->left);
    size_t r = estimate_cardinality(cond->right);
    if (cond->bool_op == BoolOp::AND)
        return std::min(l, r);
    return std::min(l + r, static_cast<size_t>(corpus_.size()));
}

// ── Query planning ──────────────────────────────────────────────────────
//
// Picks the lowest-cardinality token as seed, then BFS outward along the
// query chain to determine step order.  This ensures we start from the
// most selective restriction and never materialize the large side.

QueryPlan QueryExecutor::plan_query(const TokenQuery& query) const {
    QueryPlan plan;
    size_t n = query.tokens.size();
    if (n == 0) return plan;

    std::vector<size_t> card(n);
    for (size_t i = 0; i < n; ++i)
        card[i] = estimate_cardinality(query.tokens[i].conditions);

    // Pick the lowest-cardinality *non-optional* token as seed.
    // Optional tokens (min_repeat == 0) can match nothing, so they make
    // terrible anchors — seed_len=0 would create a corrupt span (end < start).
    // If every token is optional, fall back to the lowest-cardinality one;
    // the seed_len guard below still protects us.
    plan.seed = 0;
    for (size_t i = 1; i < n; ++i) {
        bool cur_optional  = (query.tokens[plan.seed].min_repeat == 0);
        bool cand_optional = (query.tokens[i].min_repeat == 0);
        // Prefer non-optional over optional; among same optionality, prefer lower cardinality
        if ((!cand_optional && cur_optional) ||
            (cand_optional == cur_optional && card[i] < card[plan.seed]))
            plan.seed = i;
    }

    // BFS from seed along the linear chain
    std::vector<bool> visited(n, false);
    visited[plan.seed] = true;
    std::queue<size_t> q;
    q.push(plan.seed);

    while (!q.empty()) {
        size_t cur = q.front();
        q.pop();

        // Left neighbor: tokens[cur-1] connected by relations[cur-1]
        if (cur > 0 && !visited[cur - 1]) {
            visited[cur - 1] = true;
            plan.steps.push_back({cur, cur - 1, cur - 1, /*reversed=*/true});
            q.push(cur - 1);
        }
        // Right neighbor: tokens[cur+1] connected by relations[cur]
        if (cur + 1 < n && !visited[cur + 1]) {
            visited[cur + 1] = true;
            plan.steps.push_back({cur, cur + 1, cur, /*reversed=*/false});
            q.push(cur + 1);
        }
    }

    plan.cardinalities = std::move(card);
    return plan;
}

// ── Per-position condition checking ─────────────────────────────────────
//
// Evaluates a condition tree against a single corpus position.
// Avoids materializing candidate sets for non-seed tokens — just reads
// the per-position attribute value from the .dat file (O(1) array lookup).

std::optional<int64_t> QueryExecutor::nvals_cardinality_at(
        CorpusPos pos, const std::string& name_in) const {
    std::string name = normalize_attr(name_in);

    std::string feat_name;
    if (is_feats_sub(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        std::string_view feats_str = pa.value_at(pos);
        std::string feat_val = extract_feat(feats_str, feat_name);
        int64_t n = (feat_val.empty() || feat_val == "_") ? 0 : 1;
        return n;
    }

    RegionAttrParts rap;
    if (!corpus_.has_attr(name) && split_region_attr_name(name, rap)
        && corpus_.has_structure(rap.struct_name)) {
        const auto& sa = corpus_.structure(rap.struct_name);
        const std::string& region_attr = rap.attr_name;
        if (sa.has_region_attr(region_attr)) {
            bool is_multi_region = corpus_.is_overlapping(rap.struct_name)
                                 || corpus_.is_nested(rap.struct_name);
            if (is_multi_region) {
                std::unordered_set<std::string> uniq;
                sa.for_each_region_at(pos, [&](size_t rgn_idx) -> bool {
                    insert_mv_components(uniq, sa.region_value(region_attr, rgn_idx));
                    return true;
                });
                return static_cast<int64_t>(uniq.size());
            }
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0)
                return int64_t{0};
            std::string_view v = sa.region_value(region_attr, static_cast<size_t>(rgn));
            return nvals_from_string(v, corpus_.is_multivalue(name));
        }
    }
    if (!corpus_.has_attr(name))
        return std::nullopt;
    const auto& pa = corpus_.attr(name);
    std::string_view val = pa.value_at(pos);
    return nvals_from_string(val, corpus_.is_multivalue(name));
}

bool QueryExecutor::check_leaf(CorpusPos pos, const AttrCondition& ac) const {
    if (ac.is_nvals) {
        auto n = nvals_cardinality_at(pos, ac.attr);
        if (!n) return false;
        return compare_nvals_count(*n, ac.op, ac.nvals_compare);
    }

    std::string name = normalize_attr(ac.attr);

    // Combined feats mode: feats.Number="Sing" → check within combined feats string
    std::string feat_name;
    if (is_feats_sub(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        std::string_view feats_str = pa.value_at(pos);
        std::string feat_val = extract_feat(feats_str, feat_name);
        switch (ac.op) {
            case CompOp::EQ:  return feat_val == ac.value;
            case CompOp::NEQ: return feat_val != ac.value;
            default:          return false;
        }
    }

    if (!corpus_.has_attr(name)) {
        // Try region attribute fallback: name = "struct_region"
        auto us = name.find('_');
        if (us != std::string::npos && us + 1 < name.size()) {
            std::string struct_name = name.substr(0, us);
            std::string region_attr = name.substr(us + 1);
            if (corpus_.has_structure(struct_name)) {
                const auto& sa = corpus_.structure(struct_name);
                if (sa.has_region_attr(region_attr)) {
                    bool is_multi_region = corpus_.is_overlapping(struct_name)
                                         || corpus_.is_nested(struct_name);

                    // For overlapping/nested structures, check ALL regions
                    // containing pos (existential semantics for EQ/REGEX).
                    if (is_multi_region) {
                        bool any_match = false;
                        sa.for_each_region_at(pos, [&](size_t rgn_idx) -> bool {
                            std::string_view val = sa.region_value(region_attr, rgn_idx);
                            switch (ac.op) {
                                case CompOp::EQ:
                                    if (multivalue_eq(val, ac.value)) { any_match = true; return false; }
                                    break;
                                case CompOp::NEQ:
                                    // Existential: true if ANY region has val != target
                                    if (!multivalue_eq(val, ac.value)) { any_match = true; return false; }
                                    break;
                                case CompOp::REGEX: {
#ifdef PANDO_USE_RE2
                                    const re2::RE2* compiled;
                                    {
                                        std::lock_guard<std::mutex> lock(regex_cache_mutex_);
                                        auto it = regex_cache_.find(ac.value);
                                        if (it == regex_cache_.end())
                                            it = regex_cache_.emplace(ac.value, std::make_unique<re2::RE2>(ac.value)).first;
                                        compiled = it->second.get();
                                    }
                                    if (re2::RE2::PartialMatch(val, *compiled)) { any_match = true; return false; }
#else
                                    std::lock_guard<std::mutex> lock(regex_cache_mutex_);
                                    auto it = regex_cache_.find(ac.value);
                                    if (it == regex_cache_.end())
                                        it = regex_cache_.emplace(ac.value, std::regex(ac.value)).first;
                                    std::string s(val);
                                    if (std::regex_search(s, it->second)) { any_match = true; return false; }
#endif
                                    break;
                                }
                                case CompOp::LT:
                                    if (val < ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::GT:
                                    if (val > ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::LTE:
                                    if (val <= ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::GTE:
                                    if (val >= ac.value) { any_match = true; return false; }
                                    break;
                            }
                            return true;  // continue scanning
                        });
                        return any_match;
                    }

                    // Flat structure: single region lookup (original path).
                    int64_t rgn = sa.find_region(pos);
                    if (rgn >= 0) {
                        std::string_view val = sa.region_value(region_attr, static_cast<size_t>(rgn));
                        switch (ac.op) {
                            case CompOp::EQ:    return multivalue_eq(val, ac.value);
                            case CompOp::NEQ:   return !multivalue_eq(val, ac.value);
                            case CompOp::REGEX: {
#ifdef PANDO_USE_RE2
                                const re2::RE2* compiled;
                                {
                                    std::lock_guard<std::mutex> lock(regex_cache_mutex_);
                                    auto it = regex_cache_.find(ac.value);
                                    if (it == regex_cache_.end())
                                        it = regex_cache_.emplace(ac.value, std::make_unique<re2::RE2>(ac.value)).first;
                                    compiled = it->second.get();
                                }
                                return re2::RE2::PartialMatch(val, *compiled);
#else
                                std::lock_guard<std::mutex> lock(regex_cache_mutex_);
                                auto it = regex_cache_.find(ac.value);
                                if (it == regex_cache_.end())
                                    it = regex_cache_.emplace(ac.value, std::regex(ac.value)).first;
                                std::string s(val);
                                return std::regex_search(s, it->second);
#endif
                            }
                            case CompOp::LT:    return val < ac.value;
                            case CompOp::GT:    return val > ac.value;
                            case CompOp::LTE:   return val <= ac.value;
                            case CompOp::GTE:   return val >= ac.value;
                        }
                    }
                }
            }
        }
        return false;
    }
    const auto& pa = corpus_.attr(name);

    // #25: Fast path — use pre-resolved LexiconId for integer comparison
    if (ac.resolved_id >= 0) {
        // resolved_id is only set for EQ without fold flags
        return pa.id_at(pos) == static_cast<LexiconId>(ac.resolved_id);
    }

    std::string_view val = pa.value_at(pos);

    // Fold-aware comparison for %c / %d flags
    if ((ac.case_insensitive || ac.diacritics_insensitive) &&
        (ac.op == CompOp::EQ || ac.op == CompOp::NEQ)) {
        std::string folded;
        if (ac.case_insensitive && ac.diacritics_insensitive)
            folded = FoldMap::to_lower_no_accents(val);
        else if (ac.case_insensitive)
            folded = FoldMap::to_lower(val);
        else
            folded = FoldMap::strip_accents(val);
        // The query value should already be folded by the caller (or we fold it here)
        std::string folded_query;
        if (ac.case_insensitive && ac.diacritics_insensitive)
            folded_query = FoldMap::to_lower_no_accents(ac.value);
        else if (ac.case_insensitive)
            folded_query = FoldMap::to_lower(ac.value);
        else
            folded_query = FoldMap::strip_accents(ac.value);
        if (ac.op == CompOp::EQ) return folded == folded_query;
        return folded != folded_query;
    }

    switch (ac.op) {
        case CompOp::EQ:    return multivalue_eq(val, ac.value);
        case CompOp::NEQ:   return !multivalue_eq(val, ac.value);
        case CompOp::REGEX: {
#ifdef PANDO_USE_RE2
            // Look up or insert compiled RE2 under lock, then match outside lock.
            // RE2 objects are thread-safe for matching once constructed.
            const re2::RE2* compiled;
            {
                std::lock_guard<std::mutex> lock(regex_cache_mutex_);
                auto it = regex_cache_.find(ac.value);
                if (it == regex_cache_.end())
                    it = regex_cache_.emplace(ac.value, std::make_unique<re2::RE2>(ac.value)).first;
                compiled = it->second.get();
            }
            return re2::RE2::PartialMatch(val, *compiled);
#else
            std::lock_guard<std::mutex> lock(regex_cache_mutex_);
            auto it = regex_cache_.find(ac.value);
            if (it == regex_cache_.end())
                it = regex_cache_.emplace(ac.value, std::regex(ac.value)).first;
            std::string s(val);
            return std::regex_search(s, it->second);
#endif
        }
        case CompOp::LT:    return val < ac.value;
        case CompOp::GT:    return val > ac.value;
        case CompOp::LTE:   return val <= ac.value;
        case CompOp::GTE:   return val >= ac.value;
    }
    return false;
}

bool QueryExecutor::check_conditions(CorpusPos pos,
                                     const ConditionPtr& cond) const {
    if (!cond) return true;
    if (cond->is_leaf) return check_leaf(pos, cond->leaf);

    if (cond->is_count) {
        if (!corpus_.has_deps())
            throw std::runtime_error("Count conditions require dependency index");
        const auto& deps = corpus_.deps();
        std::vector<CorpusPos> related;
        switch (cond->count_rel) {
            case StructRelType::CHILD:      related = deps.children(pos); break;
            case StructRelType::PARENT:     { auto h = deps.head(pos); if (h != NO_HEAD) related.push_back(h); break; }
            case StructRelType::SIBLING:    { auto h = deps.head(pos); if (h != NO_HEAD) { related = deps.children(h); related.erase(std::remove(related.begin(), related.end(), pos), related.end()); } break; }
            case StructRelType::DESCENDANT: related = deps.subtree(pos); break;
            case StructRelType::ANCESTOR:   related = deps.ancestors(pos); break;
        }
        int64_t cnt = 0;
        for (CorpusPos rp : related) {
            if (!cond->count_filter || check_conditions(rp, cond->count_filter))
                ++cnt;
        }
        switch (cond->count_op) {
            case CompOp::EQ:  return cnt == cond->count_value;
            case CompOp::NEQ: return cnt != cond->count_value;
            case CompOp::LT:  return cnt <  cond->count_value;
            case CompOp::GT:  return cnt >  cond->count_value;
            case CompOp::LTE: return cnt <= cond->count_value;
            case CompOp::GTE: return cnt >= cond->count_value;
            case CompOp::REGEX: return false;
        }
        return false;
    }

    if (cond->is_structural) {
        if (!corpus_.has_deps())
            throw std::runtime_error("Structural conditions require dependency index");
        const auto& deps = corpus_.deps();
        std::vector<CorpusPos> related;
        switch (cond->struct_rel) {
            case StructRelType::CHILD:      related = deps.children(pos); break;
            case StructRelType::PARENT:     { auto h = deps.head(pos); if (h != NO_HEAD) related.push_back(h); break; }
            case StructRelType::SIBLING:    { auto h = deps.head(pos); if (h != NO_HEAD) { related = deps.children(h); related.erase(std::remove(related.begin(), related.end(), pos), related.end()); } break; }
            case StructRelType::DESCENDANT: related = deps.subtree(pos); break;
            case StructRelType::ANCESTOR:   related = deps.ancestors(pos); break;
        }
        bool any_match = false;
        for (CorpusPos rp : related) {
            if (check_conditions(rp, cond->nested_conditions)) {
                any_match = true;
                break;
            }
        }
        return cond->struct_negated ? !any_match : any_match;
    }

    if (cond->bool_op == BoolOp::AND)
        return check_conditions(pos, cond->left) &&
               check_conditions(pos, cond->right);
    else
        return check_conditions(pos, cond->left) ||
               check_conditions(pos, cond->right);
}

// ── Relation traversal ──────────────────────────────────────────────────
//
// Given a position on one side of a relation edge, returns the positions
// on the other side.  For SEQUENCE this is 1 position; for GOVERNS it's
// the children list or the single head; for transitive relations it walks
// the tree (walk-up is O(depth), walk-down is DFS).

static bool is_dep_relation(RelationType rel) {
    return rel != RelationType::SEQUENCE;
}

std::vector<CorpusPos> QueryExecutor::find_related(
        CorpusPos pos, RelationType rel, bool reversed) const {

    std::vector<CorpusPos> out;

    // Defensive: ignore invalid positions to avoid out-of-bounds in deps/attrs
    if (pos < 0 || pos >= corpus_.size())
        return out;

    if (is_dep_relation(rel) && !corpus_.has_deps())
        throw std::runtime_error(
            "Query uses dependency relations but corpus has no dependency index");

    const auto& deps = corpus_.deps();

    // Invert the relation when traversing backward through the edge
    RelationType eff = rel;
    if (reversed) {
        switch (rel) {
            case RelationType::SEQUENCE:       eff = RelationType::SEQUENCE; break;
            case RelationType::GOVERNS:        eff = RelationType::GOVERNED_BY; break;
            case RelationType::GOVERNED_BY:    eff = RelationType::GOVERNS; break;
            case RelationType::TRANS_GOVERNS:  eff = RelationType::TRANS_GOV_BY; break;
            case RelationType::TRANS_GOV_BY:   eff = RelationType::TRANS_GOVERNS; break;
            case RelationType::NOT_GOVERNS:    eff = RelationType::NOT_GOV_BY; break;
            case RelationType::NOT_GOV_BY:     eff = RelationType::NOT_GOVERNS; break;
        }
    }

    switch (eff) {
        case RelationType::SEQUENCE:
            if (reversed) {
                if (pos > 0) out.push_back(pos - 1);
            } else {
                if (pos + 1 < corpus_.size()) out.push_back(pos + 1);
            }
            break;

        case RelationType::GOVERNS:
            out = deps.children(pos);
            break;

        case RelationType::GOVERNED_BY: {
            CorpusPos h = deps.head(pos);
            if (h != NO_HEAD) out.push_back(h);
            break;
        }

        case RelationType::TRANS_GOVERNS:
            out = deps.subtree(pos);
            break;

        case RelationType::TRANS_GOV_BY:
            out = deps.ancestors(pos);
            break;

        case RelationType::NOT_GOVERNS:
        case RelationType::NOT_GOV_BY:
            // Negative relations handled as post-filters in execute()
            break;
    }
    return out;
}

// ── Lazy seed iteration (avoids materializing full position vector for EQ) ─

void QueryExecutor::for_each_seed_position_impl(const ConditionPtr& cond,
                                                std::function<bool(CorpusPos)> f) const {
    if (!cond) {
        for (CorpusPos p = 0; p < corpus_.size(); ++p)
            if (!f(p)) return;
        return;
    }
    if (cond->is_leaf) {
        const AttrCondition& ac = cond->leaf;
        if (ac.is_nvals) {
            for (CorpusPos p = 0; p < corpus_.size(); ++p)
                if (check_leaf(p, ac) && !f(p)) return;
            return;
        }
        std::string name = normalize_attr(ac.attr);
        std::string feat_name;
        if (is_feats_sub(name, feat_name) && !corpus_.has_attr(name)
            && corpus_.has_attr("feats")) {
            auto vec = resolve_leaf(ac);
            for (CorpusPos p : vec)
                if (!f(p)) return;
            return;
        }
        if (!corpus_.has_attr(name)) {
            RegionAttrParts parts;
            if (split_region_attr_name(name, parts) &&
                corpus_.has_structure(parts.struct_name)) {
                const auto& sa = corpus_.structure(parts.struct_name);
                if (sa.has_region_attr(parts.attr_name) &&
                    ac.op == CompOp::EQ) {
                    // Fast path: iterate only positions inside matching regions
                    const int64_t* rgn_ids = nullptr;
                    size_t rgn_count = 0;
                    if (sa.regions_for_value(parts.attr_name, ac.value,
                                             rgn_ids, rgn_count)) {
                        for (size_t k = 0; k < rgn_count; ++k) {
                            Region r = sa.get(static_cast<size_t>(rgn_ids[k]));
                            for (CorpusPos pos = r.start; pos <= r.end; ++pos)
                                if (!f(pos)) return;
                        }
                        return;
                    }
                    // Fallback: no .rev, materialize
                    auto vec = resolve_leaf(ac);
                    for (CorpusPos p : vec)
                        if (!f(p)) return;
                    return;
                }
                // Non-EQ region attr or no region attr: materialize
                if (sa.has_region_attr(parts.attr_name)) {
                    auto vec = resolve_leaf(ac);
                    for (CorpusPos p : vec)
                        if (!f(p)) return;
                    return;
                }
            }
            return;
        }
        const auto& pa = corpus_.attr(name);
        if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive) {
            // RG-5f: For multivalue attributes, use the .mv.rev component
            // reverse index for O(log V) seed resolution.
            if (corpus_.is_multivalue(name) && pa.has_mv()) {
                LexiconId mv_id = pa.mv_lookup(ac.value);
                if (mv_id != UNKNOWN_LEX)
                    pa.for_each_position_mv(mv_id, f);
                return;
            }
            // #25: Use pre-resolved ID if available, otherwise lookup
            LexiconId id = (ac.resolved_id >= 0)
                ? static_cast<LexiconId>(ac.resolved_id)
                : pa.lexicon().lookup(ac.value);
            if (id == UNKNOWN_LEX) return;
            pa.for_each_position_id(id, f);
            return;
        }
        // NEQ, REGEX, or fold-aware EQ: materialize
        auto vec = resolve_leaf(ac);
        for (CorpusPos p : vec)
            if (!f(p)) return;
        return;
    }
    if (cond->bool_op == BoolOp::AND) {
        size_t left_est  = estimate_cardinality(cond->left);
        size_t right_est = estimate_cardinality(cond->right);
        const ConditionPtr& cheap     = (left_est <= right_est) ? cond->left : cond->right;
        const ConditionPtr& expensive = (left_est <= right_est) ? cond->right : cond->left;
        for_each_seed_position(cheap, [&](CorpusPos p) {
            return check_conditions(p, expensive) ? f(p) : true;
        });
        return;
    }
    // OR: need merged list
    auto left  = resolve_conditions(cond->left);
    auto right = resolve_conditions(cond->right);
    auto merged = unite(left, right);
    for (CorpusPos p : merged)
        if (!f(p)) return;
}

// ── Seed resolution (inverted index lookup) ─────────────────────────────

std::vector<CorpusPos> QueryExecutor::resolve_leaf(
        const AttrCondition& ac) const {
    std::string name = normalize_attr(ac.attr);

    if (ac.is_nvals) {
        std::vector<CorpusPos> result;
        for (CorpusPos p = 0; p < corpus_.size(); ++p)
            if (check_leaf(p, ac)) result.push_back(p);
        return result;
    }

    // Combined feats mode: scan feats lexicon, union matching position lists
    std::string feat_name;
    if (is_feats_sub(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        LexiconId n = pa.lexicon().size();

        std::vector<CorpusPos> result;
        for (LexiconId id = 0; id < n; ++id) {
            bool match = feats_entry_matches(pa.lexicon().get(id),
                                             feat_name, ac.value);
            if ((ac.op == CompOp::EQ && match) ||
                (ac.op == CompOp::NEQ && !match)) {
                auto positions = pa.positions_of_id(id);
                result.insert(result.end(), positions.begin(), positions.end());
            }
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    if (!corpus_.has_attr(name)) {
        // Try region attribute fallback: name = "struct_region"
        auto us = name.find('_');
        if (us != std::string::npos && us + 1 < name.size()) {
            std::string struct_name = name.substr(0, us);
            std::string region_attr = name.substr(us + 1);
            if (corpus_.has_structure(struct_name)) {
                const auto& sa = corpus_.structure(struct_name);
                if (sa.has_region_attr(region_attr)) {
                    if (ac.op == CompOp::EQ) {
                        std::vector<CorpusPos> result;
                        // Fast path: use .rev index to get matching region indices,
                        // then emit only positions inside those regions.
                        const int64_t* rgn_ids = nullptr;
                        size_t rgn_count = 0;
                        if (sa.regions_for_value(region_attr, ac.value,
                                                 rgn_ids, rgn_count)) {
                            result.reserve(rgn_count * 64);  // rough estimate
                            for (size_t k = 0; k < rgn_count; ++k) {
                                Region r = sa.get(static_cast<size_t>(rgn_ids[k]));
                                for (CorpusPos pos = r.start; pos <= r.end; ++pos)
                                    result.push_back(pos);
                            }
                            // Region indices from .rev are sorted, so positions
                            // are already in order.
                            return result;
                        }
                        // Fallback: no reverse index, linear scan
                        for (CorpusPos pos = 0; pos < corpus_.size(); ++pos) {
                            int64_t rgn = sa.find_region(pos);
                            if (rgn >= 0) {
                                std::string_view val = sa.region_value(
                                    region_attr, static_cast<size_t>(rgn));
                                if (val == ac.value)
                                    result.push_back(pos);
                            }
                        }
                        return result;
                    }
                    // For other operations on region attrs, return empty (not supported)
                    return {};
                }
            }
        }
        return {};
    }
    const auto& pa = corpus_.attr(name);

    // Fold-aware EQ resolution via fold map
    if ((ac.case_insensitive || ac.diacritics_insensitive) && ac.op == CompOp::EQ) {
        const auto& fm = get_fold_map(name, ac.case_insensitive, ac.diacritics_insensitive);
        std::string folded_query;
        if (ac.case_insensitive && ac.diacritics_insensitive)
            folded_query = FoldMap::to_lower_no_accents(ac.value);
        else if (ac.case_insensitive)
            folded_query = FoldMap::to_lower(ac.value);
        else
            folded_query = FoldMap::strip_accents(ac.value);
        const auto& ids = fm.lookup(folded_query);
        if (ids.empty()) return {};
        if (ids.size() == 1) return pa.positions_of_id(ids[0]);
        // Union posting lists of all matching lex IDs
        std::vector<CorpusPos> result;
        for (LexiconId id : ids) {
            auto pos = pa.positions_of_id(id);
            result.insert(result.end(), pos.begin(), pos.end());
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    switch (ac.op) {
        case CompOp::EQ:
            return pa.positions_of(ac.value);
        case CompOp::NEQ:
            return pa.positions_not(ac.value, corpus_.size());
        case CompOp::REGEX: {
#ifdef PANDO_USE_RE2
            // Compile RE2 under lock, then match outside lock (thread-safe).
            const re2::RE2* compiled;
            {
                std::lock_guard<std::mutex> lock(regex_cache_mutex_);
                auto it = regex_cache_.find(ac.value);
                if (it == regex_cache_.end())
                    it = regex_cache_.emplace(ac.value, std::make_unique<re2::RE2>(ac.value)).first;
                compiled = it->second.get();
            }
            return pa.positions_matching(*compiled);
#else
            std::lock_guard<std::mutex> lock(regex_cache_mutex_);
            auto it = regex_cache_.find(ac.value);
            if (it == regex_cache_.end())
                it = regex_cache_.emplace(ac.value, std::regex(ac.value)).first;
            return pa.positions_matching(it->second);
#endif
        }
        default:
            throw std::runtime_error("Unsupported comparison on positional attr");
    }
}

std::vector<CorpusPos> QueryExecutor::resolve_conditions(
        const ConditionPtr& cond) const {
    if (!cond) {
        std::vector<CorpusPos> all(static_cast<size_t>(corpus_.size()));
        for (CorpusPos i = 0; i < corpus_.size(); ++i)
            all[static_cast<size_t>(i)] = i;
        return all;
    }
    if (cond->is_leaf) return resolve_leaf(cond->leaf);

    if (cond->bool_op == BoolOp::AND) {
        // Resolve the cheaper side, filter the results by the expensive side.
        // This avoids materializing huge complement sets for NEQ conditions
        // (e.g., [upos!="PUNCT" & lemma="cat"] only materializes "cat" positions).
        size_t left_est  = estimate_cardinality(cond->left);
        size_t right_est = estimate_cardinality(cond->right);

        const ConditionPtr& cheap     = (left_est <= right_est) ? cond->left : cond->right;
        const ConditionPtr& expensive = (left_est <= right_est) ? cond->right : cond->left;

        auto positions = resolve_conditions(cheap);
        positions.erase(
            std::remove_if(positions.begin(), positions.end(),
                [&](CorpusPos p) { return !check_conditions(p, expensive); }),
            positions.end());
        return positions;
    } else {
        auto left  = resolve_conditions(cond->left);
        auto right = resolve_conditions(cond->right);
        return unite(left, right);
    }
}

// ── Value comparison helper (used by both inline and post-hoc filters) ──

static bool compare_value(CompOp op, const std::string& a, const std::string& b) {
    switch (op) {
        case CompOp::EQ:  return a == b;
        case CompOp::NEQ: return a != b;
        case CompOp::LT:  return a < b;
        case CompOp::GT:  return a > b;
        case CompOp::LTE: return a <= b;
        case CompOp::GTE: return a >= b;
        default: return a == b;
    }
}

// ── Pre-resolved region filters ──────────────────────────────────────────

std::vector<ResolvedRegionFilter> QueryExecutor::resolve_region_filters(
        const TokenQuery& q) const {
    std::vector<ResolvedRegionFilter> out;
    out.reserve(q.global_region_filters.size());
    for (const auto& gf : q.global_region_filters) {
        ResolvedRegionFilter rf;
        rf.op = gf.op;
        rf.value = gf.value;
        rf.anchor_name = gf.anchor_name;

        RegionAttrParts parts;
        if (!split_region_attr_name(gf.region_attr, parts)) {
            out.push_back(std::move(rf));
            continue;
        }
        if (corpus_.has_structure(parts.struct_name)) {
            const auto& sa = corpus_.structure(parts.struct_name);
            if (sa.has_region_attr(parts.attr_name)) {
                rf.sa = &sa;
                rf.attr_name = parts.attr_name;
                rf.has_reverse = sa.has_region_value_reverse(parts.attr_name);
            }
        }
        out.push_back(std::move(rf));
    }
    return out;
}

// ── Fast aggregation path ────────────────────────────────────────────────
//
// For single-token queries with aggregation and no complex post-filters,
// bypass Match construction entirely. The hot loop is:
//   seed pos → RegionCursor.find(O(1) amortized) → array index → counter++
//
// Returns true if the fast path was taken; false = fall back to standard path.

bool QueryExecutor::try_fast_aggregate(
        const TokenQuery& q,
        AggregateBucketData& agg,
        const std::vector<ResolvedRegionFilter>& resolved_filters,
        size_t max_total_cap,
        MatchSet& result) const {

    // Guard: single-token, non-repeating, no named-anchor columns or filters
    if (q.tokens.size() != 1) return false;
    if (q.tokens[0].min_repeat != 1 || q.tokens[0].max_repeat != 1) return false;
    for (const auto& col : agg.columns) {
        if (!col.named_anchor.empty()) return false;
    }
    for (const auto& rf : resolved_filters) {
        if (!rf.anchor_name.empty()) return false;
    }

    // ── Precompute per-column fast-lookup structures ─────────────────────

    const size_t ncols = agg.columns.size();

    struct ColFastInfo {
        bool is_positional = true;
        const PositionalAttr* pa = nullptr;
        const StructuralAttr* sa = nullptr;
        std::vector<LexiconId> region_to_lex;
        LexiconId lex_size = 0;
        std::string attr_name;
    };

    std::vector<ColFastInfo> col_info(ncols);
    bool sole_positional_col = (ncols == 1);

    for (size_t i = 0; i < ncols; ++i) {
        const auto& col = agg.columns[i];
        auto& ci = col_info[i];
        if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            ci.is_positional = true;
            ci.pa = col.pa;
        } else {
            ci.is_positional = false;
            ci.sa = col.sa;
            ci.attr_name = col.region_attr_name;
            ci.region_to_lex = col.sa->precompute_region_to_lex(col.region_attr_name);
            ci.lex_size = col.sa->region_attr_lex_size(col.region_attr_name);
            sole_positional_col = false;
            if (ci.region_to_lex.empty()) return false;  // no reverse index
        }
    }

    // ── Precompute filter cursors ────────────────────────────────────────

    struct FilterCursor {
        const ResolvedRegionFilter* rf = nullptr;
        RegionCursor cursor;
        bool valid = false;
    };

    std::vector<FilterCursor> filter_cursors;
    filter_cursors.reserve(resolved_filters.size());
    for (const auto& rf : resolved_filters) {
        FilterCursor fc;
        fc.rf = &rf;
        if (rf.sa) {
            fc.cursor = RegionCursor(*rf.sa);
            fc.valid = true;
        }
        filter_cursors.push_back(std::move(fc));
    }

    // Region cursors for aggregate columns
    std::vector<RegionCursor> agg_cursors(ncols);
    for (size_t i = 0; i < ncols; ++i) {
        if (!col_info[i].is_positional)
            agg_cursors[i] = RegionCursor(*col_info[i].sa);
    }

    // ── Special case: single positional column → flat array ─────────────

    if (sole_positional_col && filter_cursors.empty()) {
        const auto& pa = *col_info[0].pa;
        LexiconId lex_sz = pa.lexicon().size();
        std::vector<size_t> flat(static_cast<size_t>(lex_sz), 0);
        size_t total = 0;

        for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
            if (max_total_cap > 0 && total >= max_total_cap) return false;
            ++flat[static_cast<size_t>(pa.id_at(pos))];
            ++total;
            return true;
        });

        agg.total_hits = total;
        for (LexiconId id = 0; id < lex_sz; ++id) {
            if (flat[static_cast<size_t>(id)] > 0) {
                std::vector<int64_t> key = {static_cast<int64_t>(id)};
                agg.counts[std::move(key)] = flat[static_cast<size_t>(id)];
            }
        }
        result.total_count = total;
        result.total_exact = !(max_total_cap > 0 && total >= max_total_cap);
        return true;
    }

    // ── Special case: single region column → flat array via region_to_lex ─

    if (ncols == 1 && !col_info[0].is_positional && filter_cursors.empty()) {
        const auto& ci = col_info[0];
        LexiconId lex_sz = ci.lex_size;
        if (lex_sz <= 0) return false;
        std::vector<size_t> flat(static_cast<size_t>(lex_sz), 0);
        size_t total = 0;
        RegionCursor cursor(*ci.sa);

        for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
            if (max_total_cap > 0 && total >= max_total_cap) return false;
            int64_t rgn = cursor.find(pos);
            if (rgn < 0) return true;
            LexiconId lid = ci.region_to_lex[static_cast<size_t>(rgn)];
            if (lid == UNKNOWN_LEX) return true;
            ++flat[static_cast<size_t>(lid)];
            ++total;
            return true;
        });

        // Build results using 1-based intern IDs for decode_aggregate_bucket_key
        agg.total_hits = total;
        agg.region_intern.resize(1);
        auto& ri = agg.region_intern[0];
        for (LexiconId id = 0; id < lex_sz; ++id) {
            if (flat[static_cast<size_t>(id)] > 0) {
                int64_t intern_id = static_cast<int64_t>(ri.id_to_str.size() + 1);
                std::string val(ci.sa->region_attr_lex_get(ci.attr_name, id));
                ri.str_to_id[val] = intern_id;
                ri.id_to_str.push_back(val);
                std::vector<int64_t> key = {intern_id};
                agg.counts[std::move(key)] = flat[static_cast<size_t>(id)];
            }
        }
        result.total_count = total;
        result.total_exact = !(max_total_cap > 0 && total >= max_total_cap);
        return true;
    }

    // ── General case: multi-column or with :: filters ───────────────────

    std::vector<int64_t> key_buf(ncols);
    size_t total = 0;
    bool capped = false;

    auto pass_filters = [&](CorpusPos pos) -> bool {
        for (auto& fc : filter_cursors) {
            if (!fc.valid) return false;
            int64_t rgn = fc.cursor.find(pos);
            if (rgn < 0) return false;
            const auto& rf = *fc.rf;
            if (rf.op == CompOp::EQ && rf.has_reverse) {
                if (!rf.sa->region_matches_attr_eq_rev(rf.attr_name,
                        static_cast<size_t>(rgn), rf.value))
                    return false;
                continue;
            }
            std::string_view rval = rf.sa->region_value(rf.attr_name,
                                                         static_cast<size_t>(rgn));
            std::string rval_s(rval);
            switch (rf.op) {
                case CompOp::EQ:  if (rval_s != rf.value) return false; break;
                case CompOp::NEQ: if (rval_s == rf.value) return false; break;
                case CompOp::LT:  if (!(rval_s < rf.value)) return false; break;
                case CompOp::GT:  if (!(rval_s > rf.value)) return false; break;
                case CompOp::LTE: if (!(rval_s <= rf.value)) return false; break;
                case CompOp::GTE: if (!(rval_s >= rf.value)) return false; break;
                default: return false;
            }
        }
        return true;
    };

    for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
        if (max_total_cap > 0 && total >= max_total_cap) {
            capped = true;
            return false;
        }
        if (!filter_cursors.empty() && !pass_filters(pos))
            return true;

        for (size_t i = 0; i < ncols; ++i) {
            const auto& ci = col_info[i];
            if (ci.is_positional) {
                key_buf[i] = static_cast<int64_t>(ci.pa->id_at(pos));
            } else {
                int64_t rgn = agg_cursors[i].find(pos);
                if (rgn < 0) return true;
                LexiconId lid = ci.region_to_lex[static_cast<size_t>(rgn)];
                if (lid == UNKNOWN_LEX) return true;
                // 0-based lex id; remapped to 1-based intern ID below
                key_buf[i] = static_cast<int64_t>(lid);
            }
        }
        ++total;
        ++agg.counts[key_buf];
        return true;
    });

    agg.total_hits = total;

    // Populate region_intern and remap keys for decode_aggregate_bucket_key.
    // Build per-column lex_id → 1-based intern_id mappings first, then remap
    // all keys in a single pass over the counts map.
    agg.region_intern.resize(ncols);
    std::vector<std::unordered_map<int64_t, int64_t>> lex_to_intern_maps(ncols);
    bool need_remap = false;

    for (size_t i = 0; i < ncols; ++i) {
        if (col_info[i].is_positional) continue;
        need_remap = true;
        const auto& ci = col_info[i];
        auto& ri = agg.region_intern[i];
        auto& lex_to_intern = lex_to_intern_maps[i];

        for (const auto& [key, cnt] : agg.counts) {
            int64_t lid = key[i];
            if (lex_to_intern.count(lid)) continue;
            int64_t intern_id = static_cast<int64_t>(ri.id_to_str.size() + 1);
            std::string val(ci.sa->region_attr_lex_get(ci.attr_name,
                            static_cast<LexiconId>(lid)));
            ri.str_to_id[val] = intern_id;
            ri.id_to_str.push_back(val);
            lex_to_intern[lid] = intern_id;
        }
    }

    if (need_remap) {
        std::unordered_map<std::vector<int64_t>, size_t,
                           AggregateBucketData::VecHash,
                           AggregateBucketData::VecEq> new_counts;
        for (auto& [key, cnt] : agg.counts) {
            std::vector<int64_t> new_key = key;
            for (size_t i = 0; i < ncols; ++i) {
                if (col_info[i].is_positional) continue;
                new_key[i] = lex_to_intern_maps[i][key[i]];
            }
            new_counts[std::move(new_key)] += cnt;
        }
        agg.counts = std::move(new_counts);
    }

    result.total_count = total;
    result.total_exact = !capped;
    return true;
}

// ── Main execution ──────────────────────────────────────────────────────

namespace {

bool build_aggregate_plan(const Corpus& corpus, const std::vector<std::string>& fields,
                          AggregateBucketData& out) {
    out.columns.clear();
    out.region_intern.clear();
    out.counts.clear();
    out.total_hits = 0;
    out.columns.reserve(fields.size());
    out.region_intern.resize(fields.size());
    for (const std::string& field : fields) {
        AggregateBucketData::Column col;
        std::string attr_spec = field;
        if (field.rfind("match.", 0) == 0 && field.size() > 6) {
            attr_spec = field.substr(6);
        } else {
            auto dot = field.find('.');
            if (dot != std::string::npos && dot > 0) {
                col.named_anchor = field.substr(0, dot);
                attr_spec = field.substr(dot + 1);
            }
        }
        std::string attr = attr_spec;
        if (attr.size() > 5 && attr.substr(0, 5) == "feats" && attr.find('.') != std::string::npos)
            attr[attr.find('.')] = '_';
        if (corpus.has_attr(attr)) {
            col.kind = AggregateBucketData::Column::Kind::Positional;
            col.pa = &corpus.attr(attr);
            out.columns.push_back(std::move(col));
            continue;
        }
        bool found_reg = false;
        for (const auto& ra_name : corpus.region_attr_names()) {
            if (ra_name != attr_spec) continue;
            auto us = ra_name.find('_');
            if (us == std::string::npos || us + 1 >= ra_name.size()) return false;
            std::string sn = ra_name.substr(0, us);
            std::string ran = ra_name.substr(us + 1);
            if (!corpus.has_structure(sn)) return false;
            const auto& sa = corpus.structure(sn);
            if (!sa.has_region_attr(ran)) return false;
            col.kind = AggregateBucketData::Column::Kind::Region;
            col.sa = &sa;
            col.region_attr_name = std::move(ran);
            out.columns.push_back(std::move(col));
            found_reg = true;
            break;
        }
        if (!found_reg) return false;
    }
    return true;
}

bool fill_aggregate_key(AggregateBucketData& data, const Match& m, const NameIndexMap& nm,
                        std::vector<int64_t>& key_out) {
    key_out.resize(data.columns.size());
    for (size_t i = 0; i < data.columns.size(); ++i) {
        const auto& col = data.columns[i];
        CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                 : resolve_name(m, nm, col.named_anchor);
        if (pos == NO_HEAD) return false;
        if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            key_out[i] = static_cast<int64_t>(col.pa->id_at(pos));
        } else {
            int64_t rgn = col.sa->find_region(pos);
            if (rgn < 0) return false;
            std::string val(col.sa->region_value(col.region_attr_name, static_cast<size_t>(rgn)));
            auto& st = data.region_intern[i];
            auto it = st.str_to_id.find(val);
            if (it != st.str_to_id.end()) {
                key_out[i] = it->second;
            } else {
                int64_t id = static_cast<int64_t>(st.id_to_str.size() + 1);
                st.str_to_id.emplace(val, id);
                st.id_to_str.push_back(std::move(val));
                key_out[i] = id;
            }
        }
    }
    return true;
}

} // namespace

size_t AggregateBucketData::VecHash::operator()(const std::vector<int64_t>& v) const noexcept {
    size_t h = v.size();
    for (int64_t x : v) {
        uint64_t ux = static_cast<uint64_t>(x);
        h ^= std::hash<uint64_t>{}(ux + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
    }
    return h;
}

std::string decode_aggregate_bucket_key(const AggregateBucketData& data,
                                          const std::vector<int64_t>& key) {
    std::string out;
    for (size_t i = 0; i < key.size() && i < data.columns.size(); ++i) {
        if (i > 0) out += '\t';
        const auto& col = data.columns[i];
        if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            LexiconId lid = static_cast<LexiconId>(key[i]);
            out += col.pa->lexicon().get(lid);
        } else {
            int64_t id = key[i];
            const auto& st = data.region_intern[i];
            if (id >= 1 && static_cast<size_t>(id) <= st.id_to_str.size())
                out += st.id_to_str[static_cast<size_t>(id - 1)];
        }
    }
    return out;
}

bool QueryExecutor::match_survives_post_filters_for_aggregate(
        const TokenQuery& query,
        const NameIndexMap& name_map,
        const std::vector<AnchorConstraint>& anchor_constraints,
        MatchSet& scratch,
        const Match& m) const {
    scratch.matches.clear();
    scratch.matches.push_back(m);
    scratch.total_count = 1;
    apply_anchor_filters(anchor_constraints, scratch);
    if (scratch.matches.empty()) return false;
    apply_within_having(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_not_within(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_containing(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_position_orders(query, name_map, scratch);
    if (scratch.matches.empty()) return false;
    apply_global_filters(query, name_map, scratch);
    return !scratch.matches.empty();
}

// `:: text_langcode="nld"` is not part of token cardinality — without this check we would
// still enumerate every token matching [] and reject each in pass_region_filters.
// For EQ, if no region of that type ever carries the value, the result is necessarily empty.
static bool global_region_eq_unsatisfiable(const Corpus& corpus,
                                          const std::vector<GlobalRegionFilter>& filters) {
    for (const auto& gf : filters) {
        if (gf.op != CompOp::EQ) continue;
        size_t us = gf.region_attr.find('_');
        if (us == std::string::npos || us + 1 >= gf.region_attr.size()) continue;
        std::string struct_name = gf.region_attr.substr(0, us);
        std::string attr_name = gf.region_attr.substr(us + 1);
        if (!corpus.has_structure(struct_name)) return true;
        const auto& sa = corpus.structure(struct_name);
        if (!sa.has_region_attr(attr_name)) return true;
        if (sa.has_region_value_reverse(attr_name)) {
            if (sa.count_regions_with_attr_eq(attr_name, gf.value) == 0)
                return true;
            continue;
        }
        bool any = false;
        for (size_t ri = 0; ri < sa.region_count(); ++ri) {
            std::string rval(sa.region_value(attr_name, ri));
            if (rval == gf.value) {
                any = true;
                break;
            }
        }
        if (!any) return true;
    }
    return false;
}

MatchSet QueryExecutor::execute(const TokenQuery& query,
                                size_t max_matches,
                                bool count_total,
                                size_t max_total_cap,
                                size_t sample_size,
                                uint32_t random_seed,
                                unsigned num_threads,
                                const std::vector<std::string>* aggregate_by_fields) {
    // Strip region anchors (<s>, </s>) from query, recording constraints
    std::vector<AnchorConstraint> anchor_constraints;
    bool has_anchors = false;
    for (const auto& tok : query.tokens) {
        if (tok.is_anchor()) { has_anchors = true; break; }
    }
    TokenQuery stripped_query;
    if (has_anchors) stripped_query = strip_anchors(query, anchor_constraints);
    const TokenQuery& q = has_anchors ? stripped_query : query;

    MatchSet result;
    if (q.tokens.empty()) return result;
    size_t n = q.tokens.size();
    result.num_tokens = n;
    NameIndexMap name_map = build_name_map(q);

    // #25: Pre-resolve EQ values to LexiconIds for fast integer comparison
    compile_query(q);

    if (global_region_eq_unsatisfiable(corpus_, q.global_region_filters)) {
        result.total_exact = true;
        return result;
    }

    std::shared_ptr<AggregateBucketData> agg_storage;
    AggregateBucketData* agg_ptr = nullptr;
    MatchSet post_scratch;
    bool agg_capped = false;
    if (aggregate_by_fields && !aggregate_by_fields->empty() && sample_size == 0) {
        agg_storage = std::make_shared<AggregateBucketData>();
        if (build_aggregate_plan(corpus_, *aggregate_by_fields, *agg_storage))
            agg_ptr = agg_storage.get();
        else
            agg_storage.reset();
    }
    if (agg_ptr)
        post_scratch.matches.reserve(1);

    // Per-match post-filter pass is expensive at millions of hits; skip when only
    // inline :: region filters apply (handled above) and no other post-filters exist.
    const bool agg_per_match_post =
        agg_ptr
        && (!anchor_constraints.empty()
            || (q.within_having && !q.within.empty() && corpus_.has_structure(q.within))
            || (q.not_within && !q.within.empty() && corpus_.has_structure(q.within))
            || !q.containing_clauses.empty() || !q.position_orders.empty()
            || !q.global_alignment_filters.empty());

    unsigned eff_threads = (agg_ptr && num_threads > 1) ? 1u : num_threads;

    std::vector<Match> reservoir;
    if (sample_size > 0) reservoir.reserve(sample_size);
    std::mt19937 rng(random_seed != 0 ? random_seed : static_cast<uint32_t>(std::random_device{}()));

    // Partial match vectors are of size 2*n: [0..n-1]=starts, [n..2n-1]=ends
    // For non-repeating tokens: pm[i] == pm[n+i]
    auto build_match = [n](std::vector<CorpusPos>&& pm) -> Match {
        Match m;
        m.positions.assign(pm.begin(), pm.begin() + n);
        m.span_ends.assign(pm.begin() + n, pm.begin() + 2 * n);
        return m;
    };

    // Inline region-filter check so matches that fail :: filters are never
    // counted against the limit.  Runs in O(filters) per candidate — typically 1.
    auto pass_region_filters = [&](const Match& m) -> bool {
        for (const auto& gf : q.global_region_filters) {
            size_t us = gf.region_attr.find('_');
            if (us == std::string::npos || us + 1 >= gf.region_attr.size()) return false;
            std::string struct_name = gf.region_attr.substr(0, us);
            std::string attr_name = gf.region_attr.substr(us + 1);
            if (!corpus_.has_structure(struct_name)) return false;
            const auto& sa = corpus_.structure(struct_name);
            if (!sa.has_region_attr(attr_name)) return false;
            CorpusPos pos = m.first_pos();
            if (!gf.anchor_name.empty()) {
                CorpusPos ap = resolve_name(m, name_map, gf.anchor_name);
                if (ap != NO_HEAD) pos = ap;
            }
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0) return false;
            if (gf.op == CompOp::EQ && sa.has_region_value_reverse(attr_name)) {
                if (!sa.region_matches_attr_eq_rev(attr_name, static_cast<size_t>(rgn), gf.value))
                    return false;
                continue;
            }
            std::string rval(sa.region_value(attr_name, static_cast<size_t>(rgn)));
            if (!compare_value(gf.op, rval, gf.value)) return false;
        }
        return true;
    };

    auto add_match = [&](std::vector<CorpusPos>&& positions) {
        Match m = build_match(std::move(positions));
        // Apply :: region filters inline so rejected matches don't consume the limit
        if (!q.global_region_filters.empty() && !pass_region_filters(m))
            return;
        if (agg_ptr) {
            if (agg_per_match_post &&
                !match_survives_post_filters_for_aggregate(q, name_map, anchor_constraints,
                                                          post_scratch, m))
                return;
            std::vector<int64_t> akey;
            if (!fill_aggregate_key(*agg_ptr, m, name_map, akey))
                return;
            if (max_total_cap > 0 && agg_ptr->total_hits >= max_total_cap) {
                agg_capped = true;
                return;
            }
            ++agg_ptr->total_hits;
            ++agg_ptr->counts[std::move(akey)];
            return;
        }
        ++result.total_count;
        if (sample_size > 0) {
            if (reservoir.size() < sample_size) {
                reservoir.push_back(std::move(m));
            } else {
                std::uniform_real_distribution<double> u(0, 1);
                if (u(rng) < static_cast<double>(sample_size) / static_cast<double>(result.total_count)) {
                    std::uniform_int_distribution<size_t> idx(0, sample_size - 1);
                    reservoir[idx(rng)] = std::move(m);
                }
            }
        } else if (max_matches == 0 || result.matches.size() < max_matches) {
            result.matches.push_back(std::move(m));
        }
    };

    auto reached_limit = [&]() {
        if (sample_size > 0) return false;  // sampling needs full enumeration
        if (agg_ptr) return false;
        return max_matches > 0 && result.total_count >= max_matches
               && !count_total;
    };
    auto reached_total_cap = [&]() {
        if (agg_ptr) return agg_capped;
        return count_total && max_total_cap > 0 && result.total_count >= max_total_cap;
    };

    // ── Single-token fast path ──────────────────────────────────────────

    if (n == 1) {
        size_t est = estimate_cardinality(q.tokens[0].conditions);
        result.cardinalities = {est};
        result.seed_token = 0;

        // ── Try fast aggregation path (no Match construction) ────────────
        if (agg_ptr && !agg_per_match_post) {
            auto resolved_filters = resolve_region_filters(q);
            if (try_fast_aggregate(q, *agg_ptr, resolved_filters,
                                   max_total_cap, result)) {
                result.aggregate_buckets = std::move(agg_storage);
                return result;
            }
            // Fast path declined — fall through to standard path
        }

        int min_rep = q.tokens[0].min_repeat;
        int max_rep = q.tokens[0].max_repeat;

        auto try_spans_from = [&](CorpusPos p) {
            // For single-token query with repetition: try spans starting at p
            for (int len = min_rep; len <= max_rep; ++len) {
                CorpusPos end = p + len - 1;
                if (end >= corpus_.size()) break;
                if (len > 1 && !check_conditions(end, q.tokens[0].conditions)) break;
                // All positions p..end must match (position p already verified by caller)
                // Positions p+1..end-1 checked incrementally below
                bool valid = true;
                if (len == 1) {
                    // already checked by caller
                } else {
                    // We verified p (caller) and end (above); check intermediate
                    for (CorpusPos cp = p + 1; cp < end; ++cp) {
                        if (!check_conditions(cp, q.tokens[0].conditions)) {
                            valid = false;
                            break;
                        }
                    }
                }
                if (!valid) break;
                std::vector<CorpusPos> pm = {p, end};  // 2*1: start, end
                add_match(std::move(pm));
                if (reached_limit() || reached_total_cap()) return false;
            }
            return !reached_limit() && !reached_total_cap();
        };

        bool use_scan = !q.tokens[0].conditions
                        || est > static_cast<size_t>(corpus_.size()) / 2;

        if (use_scan) {
            for (CorpusPos p = 0; p < corpus_.size(); ++p) {
                if (check_conditions(p, q.tokens[0].conditions)) {
                    if (!try_spans_from(p)) break;
                }
            }
        } else {
            for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos p) {
                return try_spans_from(p);
            });
        }
        if (agg_ptr) {
            result.matches.clear();
            result.total_count = agg_ptr->total_hits;
            result.total_exact = !agg_capped;
            result.aggregate_buckets = std::move(agg_storage);
            return result;
        }
        apply_anchor_filters(anchor_constraints, result);
        apply_within_having(q, result);
        apply_not_within(q, result);
        apply_containing(q, result);
        apply_position_orders(q, name_map, result);
        apply_global_filters(q, name_map, result);
        result.total_exact = !reached_limit() && !reached_total_cap();
        return result;
    }

    // ── Sequence fast path: bypass plan/step/expand for pure linear sequences ──
    //
    // For queries like [upos="ADJ"] [upos="NOUN"] where all relations are
    // SEQUENCE, no repetitions, no anchors: iterate the cheapest token and
    // check all others at fixed offsets.  No Match vector bookkeeping,
    // no find_related, no partial-match machinery.
    {
        bool all_seq = true;
        for (const auto& rel : q.relations)
            if (rel.type != RelationType::SEQUENCE) { all_seq = false; break; }
        bool no_rep = true;
        bool no_anchor = true;
        for (const auto& tok : q.tokens) {
            if (tok.has_repetition()) no_rep = false;
            if (tok.is_anchor()) no_anchor = false;
        }

        if (all_seq && no_rep && no_anchor && n >= 2) {
            // Pick cheapest token as seed
            size_t seed = 0;
            size_t best_est = SIZE_MAX;
            std::vector<size_t> card(n);
            for (size_t i = 0; i < n; ++i) {
                card[i] = estimate_cardinality(q.tokens[i].conditions);
                if (card[i] < best_est) { best_est = card[i]; seed = i; }
            }
            result.seed_token = seed;
            result.cardinalities = card;

            // Within-region support
            std::string effective_within = q.within.empty()
                ? corpus_.default_within() : q.within;
            bool has_within = !effective_within.empty() &&
                              corpus_.has_structure(effective_within);
            const StructuralAttr* within_sa = has_within
                ? &corpus_.structure(effective_within) : nullptr;
            int64_t within_hint = -1;

            int64_t seed_offset = static_cast<int64_t>(seed);
            CorpusPos corpus_end = corpus_.size();

            for_each_seed_position(q.tokens[seed].conditions, [&](CorpusPos seed_p) {
                // Compute position of first token from this seed
                CorpusPos p0 = seed_p - seed_offset;
                CorpusPos pN = p0 + static_cast<int64_t>(n) - 1;
                if (p0 < 0 || pN >= corpus_end) return true;  // out of bounds, skip

                // Within-region check on the full span
                if (within_sa) {
                    int64_t rgn = (within_hint >= 0)
                        ? within_sa->find_region_from(p0, within_hint)
                        : within_sa->find_region(p0);
                    if (rgn < 0) return true;
                    within_hint = rgn;
                    Region r = within_sa->get(static_cast<size_t>(rgn));
                    if (pN > r.end) return true;  // span crosses region boundary
                }

                // Check all non-seed tokens at their fixed offsets
                bool all_match = true;
                for (size_t i = 0; i < n; ++i) {
                    if (i == seed) continue;
                    if (!check_conditions(p0 + static_cast<int64_t>(i),
                                          q.tokens[i].conditions)) {
                        all_match = false;
                        break;
                    }
                }
                if (!all_match) return true;

                // Build match: 2*n entries (start, end for each token; all single-position)
                std::vector<CorpusPos> pm(2 * n);
                for (size_t i = 0; i < n; ++i) {
                    pm[2 * i]     = p0 + static_cast<int64_t>(i);
                    pm[2 * i + 1] = p0 + static_cast<int64_t>(i);
                }
                add_match(std::move(pm));
                return !reached_limit() && !reached_total_cap();
            });

            apply_anchor_filters(anchor_constraints, result);
            apply_within_having(q, result);
            apply_not_within(q, result);
            apply_containing(q, result);
            apply_position_orders(q, name_map, result);
            apply_global_filters(q, name_map, result);
            result.total_exact = !reached_limit() && !reached_total_cap();
            return result;
        }
    }

    // ── Plan: pick seed by cardinality, expand outward ──────────────────

    QueryPlan plan = plan_query(q);
    result.seed_token = plan.seed;
    result.cardinalities = plan.cardinalities;

    // #9: Use default_within from corpus when query does not specify within
    std::string effective_within = q.within.empty()
        ? corpus_.default_within() : q.within;
    bool has_within = !effective_within.empty() &&
                      corpus_.has_structure(effective_within);
    const StructuralAttr* within_sa = has_within
        ? &corpus_.structure(effective_within) : nullptr;

    // Parallel path: materialize seeds and process chunks in parallel (multi-token only)
    if (eff_threads > 1 && n > 1) {
        std::vector<CorpusPos> seeds = resolve_conditions(q.tokens[plan.seed].conditions);
        // #24: Pre-filter seeds that fall outside any within-region (parallel path).
        // Seeds are sorted, so cursor-based scan is efficient.
        if (within_sa && !seeds.empty()) {
            int64_t hint = -1;
            seeds.erase(std::remove_if(seeds.begin(), seeds.end(), [&](CorpusPos p) {
                int64_t rgn = (hint >= 0)
                    ? within_sa->find_region_from(p, hint)
                    : within_sa->find_region(p);
                if (rgn < 0) return true;  // remove: not in any region
                hint = rgn;
                return false;
            }), seeds.end());
        }
        if (!seeds.empty()) {
            size_t nw = std::min(static_cast<size_t>(eff_threads), seeds.size());
            size_t chunk_sz = (seeds.size() + nw - 1) / nw;
            std::vector<std::vector<Match>> thread_matches(nw);
            std::vector<std::thread> workers;
            for (size_t w = 0; w < nw; ++w) {
                size_t start = w * chunk_sz;
                size_t end = std::min(start + chunk_sz, seeds.size());
                workers.emplace_back([this, &q, &plan, within_sa, &seeds,
                                      &thread_matches, start, end, w]() {
                    for (size_t i = start; i < end; ++i) {
                        auto matches = expand_one_seed(q, plan, within_sa, seeds[i]);
                        for (auto& m : matches)
                            thread_matches[w].push_back(std::move(m));
                    }
                });
            }
            for (auto& t : workers) t.join();
            result.total_count = 0;
            for (const auto& vec : thread_matches) result.total_count += vec.size();
            for (auto& vec : thread_matches)
                for (auto& m : vec)
                    result.matches.push_back(std::move(m));
            if (sample_size > 0 && result.matches.size() > sample_size) {
                std::mt19937 rng(random_seed != 0 ? random_seed : static_cast<uint32_t>(std::random_device{}()));
                std::shuffle(result.matches.begin(), result.matches.end(), rng);
                result.matches.resize(sample_size);
            } else if (max_matches > 0 && !count_total && result.matches.size() > max_matches) {
                result.matches.resize(max_matches);
            }
            apply_anchor_filters(anchor_constraints, result);
            apply_within_having(q, result);
            apply_not_within(q, result);
            apply_containing(q, result);
            apply_position_orders(q, name_map, result);
            apply_global_filters(q, name_map, result);
            result.total_exact = true;
            return result;
        }
    }

    // Sequential path: process one seed at a time (lazy when possible)
    // #28: Maintain within-region cursor across seeds for O(1) amortized lookup
    // #24: Pre-check within-region for seed position before expand_seed.
    //      Seeds that fall outside any within-region are skipped entirely,
    //      avoiding the full expand_seed call.
    int64_t within_hint = -1;
    for_each_seed_position(q.tokens[plan.seed].conditions, [&](CorpusPos seed_p) {
        // #24: Early within-region rejection at seed level
        if (within_sa) {
            int64_t rgn = (within_hint >= 0)
                ? within_sa->find_region_from(seed_p, within_hint)
                : within_sa->find_region(seed_p);
            if (rgn < 0) return true;  // seed not in any region — skip, continue
            within_hint = rgn;         // advance cursor for next seed
        }
        expand_seed(q, plan, within_sa, seed_p, [&](std::vector<CorpusPos>&& pm) -> bool {
            add_match(std::move(pm));
            return !reached_limit() && !reached_total_cap();
        }, within_sa ? &within_hint : nullptr);
        return !reached_limit() && !reached_total_cap();
    });

    if (sample_size > 0 && !reservoir.empty())
        result.matches = std::move(reservoir);
    if (agg_ptr) {
        result.matches.clear();
        result.total_count = agg_ptr->total_hits;
        result.total_exact = !agg_capped;
        result.aggregate_buckets = std::move(agg_storage);
        return result;
    }
    apply_anchor_filters(anchor_constraints, result);
    apply_within_having(q, result);
    apply_not_within(q, result);
    apply_containing(q, result);
    apply_position_orders(q, name_map, result);
    apply_global_filters(q, name_map, result);
    result.total_exact = !reached_limit() && !reached_total_cap();
    return result;
}

// ── Shared seed expansion (single source of truth for match logic) ───────

void QueryExecutor::expand_seed(const TokenQuery& query,
                                const QueryPlan& plan,
                                const StructuralAttr* within_sa,
                                CorpusPos seed_p,
                                std::function<bool(std::vector<CorpusPos>&&)> emit,
                                int64_t* within_hint) const {
    size_t n = query.tokens.size();
    int seed_min_rep = std::max(query.tokens[plan.seed].min_repeat, 1); // seed_len=0 is nonsensical
    int seed_max_rep = query.tokens[plan.seed].max_repeat;

    for (int seed_len = seed_min_rep; seed_len <= seed_max_rep; ++seed_len) {
        // Validate the seed span: positions seed_p..seed_p+seed_len-1 must all match
        if (seed_len > 1) {
            CorpusPos end_p = seed_p + seed_len - 1;
            if (end_p >= corpus_.size()) break;
            bool valid = true;
            for (CorpusPos q = seed_p + 1; q <= end_p; ++q) {
                if (!check_conditions(q, query.tokens[plan.seed].conditions)) {
                    valid = false;
                    break;
                }
            }
            if (!valid) break;
        }

        // Partial match vectors are of size 2*n: [0..n-1]=starts, [n..2n-1]=ends
        std::vector<std::vector<CorpusPos>> partial;
        {
            std::vector<CorpusPos> pm(2 * n, NO_HEAD);
            pm[plan.seed] = seed_p;
            pm[n + plan.seed] = seed_p + seed_len - 1;
            partial.push_back(std::move(pm));
        }

        // Expand outward through plan steps
        for (const auto& step : plan.steps) {
            if (step.edge_idx >= query.relations.size())
                throw std::runtime_error("Internal error: query plan edge index out of range");
            RelationType rel = query.relations[step.edge_idx].type;
            const auto& target_cond = query.tokens[step.to].conditions;
            int to_min = query.tokens[step.to].min_repeat;
            int to_max = query.tokens[step.to].max_repeat;
            bool is_negative = (rel == RelationType::NOT_GOVERNS ||
                                rel == RelationType::NOT_GOV_BY);

            std::vector<std::vector<CorpusPos>> new_partial;

            if (is_negative) {
                // Negative relation: keep partial only if NO target matches
                RelationType pos_rel = (rel == RelationType::NOT_GOVERNS)
                                       ? RelationType::GOVERNS
                                       : RelationType::GOVERNED_BY;
                for (const auto& pm : partial) {
                    CorpusPos from_pos = pm[step.from];
                    if (from_pos == NO_HEAD) continue;
                    bool any_match = false;
                    for_each_related(from_pos, pos_rel, step.reversed, [&](CorpusPos r) -> bool {
                        if (r >= 0 && r < corpus_.size() &&
                            check_conditions(r, target_cond)) {
                            any_match = true;
                            return false;  // stop early
                        }
                        return true;
                    });
                    if (!any_match)
                        new_partial.push_back(pm);
                }
            } else if (rel == RelationType::SEQUENCE && (to_min != 1 || to_max != 1)) {
                // SEQUENCE with repetition/optional on the target token
                for (const auto& pm : partial) {
                    CorpusPos from_end = pm[n + step.from];
                    CorpusPos from_start = pm[step.from];
                    if (from_end == NO_HEAD) {
                        // From-token was skipped (optional) — walk to nearest placed token
                        CorpusPos fallback = NO_HEAD;
                        if (!step.reversed) {
                            for (int k = static_cast<int>(step.from) - 1; k >= 0; --k) {
                                if (pm[n + k] != NO_HEAD) { fallback = pm[n + k]; break; }
                            }
                        } else {
                            for (size_t k = step.from + 1; k < n; ++k) {
                                if (pm[k] != NO_HEAD) { fallback = pm[k]; break; }
                            }
                        }
                        if (fallback == NO_HEAD) continue;
                        from_end = fallback;
                        from_start = fallback;
                    }

                    CorpusPos base;
                    if (!step.reversed) {
                        base = from_end + 1;    // span starts right after from's end
                    } else {
                        base = from_start - 1;  // span ends right before from's start
                    }

                    // Optional token: min=0 path (skip this token entirely)
                    if (to_min == 0) {
                        auto skipped = pm;
                        new_partial.push_back(std::move(skipped));
                    }

                    // Try spans of length 1..to_max
                    for (int len = 1; len <= to_max; ++len) {
                        CorpusPos p = step.reversed ? (base - len + 1) : (base + len - 1);
                        if (p < 0 || p >= corpus_.size()) break;
                        if (!check_conditions(p, target_cond)) break;

                        if (len >= std::max(to_min, 1)) {
                            auto extended = pm;
                            if (!step.reversed) {
                                extended[step.to] = base;
                                extended[n + step.to] = base + len - 1;
                            } else {
                                extended[step.to] = base - len + 1;
                                extended[n + step.to] = base;
                            }
                            new_partial.push_back(std::move(extended));
                        }
                    }
                }
            } else {
                // Standard non-repeating path (or non-SEQUENCE relation)
                for (const auto& pm : partial) {
                    CorpusPos from_pos;
                    if (rel == RelationType::SEQUENCE) {
                        from_pos = step.reversed ? pm[step.from] : pm[n + step.from];
                    } else {
                        from_pos = pm[step.from];
                    }
                    if (from_pos == NO_HEAD) {
                        // From-token was skipped (optional with min=0).
                        // Walk to nearest placed token for SEQUENCE fallback.
                        if (rel == RelationType::SEQUENCE) {
                            CorpusPos fallback = NO_HEAD;
                            if (!step.reversed) {
                                for (int k = static_cast<int>(step.from) - 1; k >= 0; --k) {
                                    if (pm[n + k] != NO_HEAD) { fallback = pm[n + k]; break; }
                                }
                            } else {
                                for (size_t k = step.from + 1; k < n; ++k) {
                                    if (pm[k] != NO_HEAD) { fallback = pm[k]; break; }
                                }
                            }
                            if (fallback == NO_HEAD) continue;
                            from_pos = fallback;
                        } else {
                            continue;
                        }
                    }

                    // Optional token: min=0 path (skip)
                    if (to_min == 0 && rel == RelationType::SEQUENCE) {
                        auto skipped = pm;
                        new_partial.push_back(std::move(skipped));
                    }

                    for_each_related(from_pos, rel, step.reversed, [&](CorpusPos r) -> bool {
                        if (r >= 0 && r < corpus_.size() &&
                            check_conditions(r, target_cond)) {
                            auto extended = pm;
                            extended[step.to] = r;
                            extended[n + step.to] = r;
                            new_partial.push_back(std::move(extended));
                        }
                        return true;
                    });
                }
            }

            partial = std::move(new_partial);
            if (partial.empty()) break;
        }

        // Within-clause filter + emit
        bool stop = false;
        for (auto& pm : partial) {
            if (within_sa) {
                CorpusPos anchor = NO_HEAD;
                for (size_t i = 0; i < n; ++i) {
                    if (pm[i] != NO_HEAD) { anchor = pm[i]; break; }
                }
                if (anchor == NO_HEAD) continue;
                // #28: Use cursor-based find_region when hint is available
                int64_t rgn = within_hint
                    ? within_sa->find_region_from(anchor, *within_hint)
                    : within_sa->find_region(anchor);
                if (rgn < 0) continue;
                if (within_hint) *within_hint = rgn;  // advance cursor
                bool ok = true;
                for (size_t i = 0; i < n; ++i) {
                    if (pm[i] == NO_HEAD) continue;
                    if (within_sa->find_region(pm[i]) != rgn) { ok = false; break; }
                    if (pm[n + i] != pm[i] && within_sa->find_region(pm[n + i]) != rgn) { ok = false; break; }
                }
                if (!ok) continue;
            }
            if (!emit(std::move(pm))) { stop = true; break; }
        }
        if (stop) break;
    }
}

// ── expand_one_seed (convenience wrapper for parallel execution) ──────────

std::vector<Match> QueryExecutor::expand_one_seed(const TokenQuery& query,
                                                  const QueryPlan& plan,
                                                  const StructuralAttr* within_sa,
                                                  CorpusPos seed_p) const {
    std::vector<Match> out;
    size_t n = query.tokens.size();

    expand_seed(query, plan, within_sa, seed_p, [&](std::vector<CorpusPos>&& pm) -> bool {
        Match m;
        m.positions.assign(pm.begin(), pm.begin() + n);
        m.span_ends.assign(pm.begin() + n, pm.begin() + 2 * n);
        out.push_back(std::move(m));
        return true;  // always continue (parallel path collects all)
    });

    return out;
}

// ── #16 Source | Target parallel execution ──────────────────────────────

MatchSet QueryExecutor::execute_parallel(const TokenQuery& source_query,
                                         const TokenQuery& target_query,
                                         size_t max_matches,
                                         bool count_total) {
    MatchSet result;
    result.num_tokens = source_query.tokens.size() + target_query.tokens.size();

    MatchSet source_set = execute(source_query, 0, true, 0, 0, 0, 1);
    MatchSet target_set = execute(target_query, 0, true, 0, 0, 0, 1);

    NameIndexMap src_names = build_name_map(source_query);
    NameIndexMap tgt_names = build_name_map(target_query);

    // Apply region filters (e.g. :: match.text_lang="en") to source/target before joining.
    apply_region_filters(source_query, src_names, source_set);
    apply_region_filters(target_query, tgt_names, target_set);

    const auto& filters = source_query.global_alignment_filters;
    for (const auto& s : source_set.matches) {
        for (const auto& t : target_set.matches) {
            bool aligned = true;
            for (const auto& af : filters) {
                CorpusPos p1 = resolve_name(s, src_names, af.name1);
                CorpusPos p2 = resolve_name(t, tgt_names, af.name2);
                if (p1 == NO_HEAD || p2 == NO_HEAD) {
                    aligned = false;
                    break;
                }
                std::string an1 = normalize_attr(af.attr1);
                std::string an2 = normalize_attr(af.attr2);
                if (!corpus_.has_attr(an1) || !corpus_.has_attr(an2)) {
                    aligned = false;
                    break;
                }
                std::string v1(corpus_.attr(an1).value_at(p1));
                std::string v2(corpus_.attr(an2).value_at(p2));
                if (v1 != v2) {
                    aligned = false;
                    break;
                }
            }
            if (aligned) {
                result.parallel_matches.emplace_back(s, t);
                result.total_count++;
                if (max_matches > 0 && result.total_count >= max_matches && !count_total)
                    goto done;
            }
        }
    }
done:
    result.total_exact = true;
    return result;
}

// ── #12 Global filters ───────────────────────────────────────────────────

void QueryExecutor::apply_region_filters(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const {
    if (query.global_region_filters.empty()) return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (const auto& m : result.matches) {
        bool pass = true;

        for (const auto& gf : query.global_region_filters) {
            size_t us = gf.region_attr.find('_');
            if (us == std::string::npos || us + 1 >= gf.region_attr.size()) continue;
            std::string struct_name = gf.region_attr.substr(0, us);
            std::string attr_name = gf.region_attr.substr(us + 1);
            if (!corpus_.has_structure(struct_name)) { pass = false; break; }
            const auto& sa = corpus_.structure(struct_name);
            if (!sa.has_region_attr(attr_name)) { pass = false; break; }
            // Resolve position: use anchor token name if set, otherwise match start
            CorpusPos pos = m.first_pos();
            if (!gf.anchor_name.empty()) {
                CorpusPos ap = resolve_name(m, name_map, gf.anchor_name);
                if (ap != NO_HEAD) pos = ap;
            }
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0) { pass = false; break; }
            if (gf.op == CompOp::EQ && sa.has_region_value_reverse(attr_name)) {
                if (!sa.region_matches_attr_eq_rev(attr_name, static_cast<size_t>(rgn), gf.value)) {
                    pass = false;
                    break;
                }
                continue;
            }
            std::string rval(sa.region_value(attr_name, static_cast<size_t>(rgn)));
            if (!compare_value(gf.op, rval, gf.value)) { pass = false; break; }
        }

        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

void QueryExecutor::apply_global_filters(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const {
    if (query.global_region_filters.empty() && query.global_alignment_filters.empty()
        && query.global_function_filters.empty())
        return;

    // First apply region filters
    apply_region_filters(query, name_map, result);
    if (query.global_alignment_filters.empty() && query.global_function_filters.empty())
        return;
    if (result.matches.empty())
        return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (const auto& m : result.matches) {
        bool pass = true;

        // Alignment filters: :: a.attr = b.attr
        for (const auto& af : query.global_alignment_filters) {
            CorpusPos p1 = resolve_name(m, name_map, af.name1);
            CorpusPos p2 = resolve_name(m, name_map, af.name2);
            if (p1 == NO_HEAD || p2 == NO_HEAD) { pass = false; break; }
            std::string an1 = normalize_attr(af.attr1);
            std::string an2 = normalize_attr(af.attr2);
            if (!corpus_.has_attr(an1) || !corpus_.has_attr(an2))
                { pass = false; break; }
            std::string v1(corpus_.attr(an1).value_at(p1));
            std::string v2(corpus_.attr(an2).value_at(p2));
            if (v1 != v2) { pass = false; break; }
        }

        // Function filters: :: distance(a,b) < 5, :: depth(a) > depth(b), etc.
        if (pass) {
            // Evaluate a single GlobalFuncCall against a match.
            // Returns {true, value} on success, {false, 0} on failure (match rejected).
            auto eval_func = [&](const GlobalFuncCall& fc) -> std::pair<bool, int64_t> {
                switch (fc.func) {
                case GlobalFunctionType::DISTANCE:
                case GlobalFunctionType::DISTABS: {
                    if (fc.args.size() < 2) return {false, 0};
                    CorpusPos p1 = resolve_name(m, name_map, fc.args[0]);
                    CorpusPos p2 = resolve_name(m, name_map, fc.args[1]);
                    if (p1 == NO_HEAD || p2 == NO_HEAD) return {false, 0};
                    return {true, std::abs(p1 - p2)};
                }
                case GlobalFunctionType::STRLEN: {
                    if (fc.args.empty()) return {false, 0};
                    const std::string& spec = fc.args[0];
                    auto dot = spec.find('.');
                    if (dot == std::string::npos) return {false, 0};
                    std::string tok_name = spec.substr(0, dot);
                    std::string attr_name = normalize_attr(spec.substr(dot + 1));
                    CorpusPos p = resolve_name(m, name_map, tok_name);
                    if (p == NO_HEAD || !corpus_.has_attr(attr_name)) return {false, 0};
                    std::string_view val = corpus_.attr(attr_name).value_at(p);
                    int64_t cp_count = 0;
                    for (unsigned char c : val)
                        if ((c & 0xC0) != 0x80) ++cp_count;
                    return {true, cp_count};
                }
                case GlobalFunctionType::FREQ: {
                    if (fc.args.empty()) return {false, 0};
                    const std::string& spec = fc.args[0];
                    auto dot = spec.find('.');
                    if (dot == std::string::npos) return {false, 0};
                    std::string tok_name = spec.substr(0, dot);
                    std::string attr_name = normalize_attr(spec.substr(dot + 1));
                    CorpusPos p = resolve_name(m, name_map, tok_name);
                    if (p == NO_HEAD || !corpus_.has_attr(attr_name)) return {false, 0};
                    const auto& pa = corpus_.attr(attr_name);
                    LexiconId lid = pa.id_at(p);
                    if (lid == UNKNOWN_LEX) return {false, 0};
                    return {true, static_cast<int64_t>(pa.count_of_id(lid))};
                }
                case GlobalFunctionType::NCHILDREN: {
                    if (fc.args.empty() || !corpus_.has_deps()) return {false, 0};
                    CorpusPos p = resolve_name(m, name_map, fc.args[0]);
                    if (p == NO_HEAD) return {false, 0};
                    return {true, static_cast<int64_t>(corpus_.deps().children_count(p))};
                }
                case GlobalFunctionType::DEPTH: {
                    if (fc.args.empty() || !corpus_.has_deps()) return {false, 0};
                    CorpusPos p = resolve_name(m, name_map, fc.args[0]);
                    if (p == NO_HEAD) return {false, 0};
                    return {true, static_cast<int64_t>(corpus_.deps().depth(p))};
                }
                case GlobalFunctionType::NDESCENDANTS: {
                    if (fc.args.empty() || !corpus_.has_deps()) return {false, 0};
                    CorpusPos p = resolve_name(m, name_map, fc.args[0]);
                    if (p == NO_HEAD) return {false, 0};
                    int16_t ein = corpus_.deps().euler_in(p);
                    int16_t eout = corpus_.deps().euler_out(p);
                    return {true, static_cast<int64_t>((eout - ein) / 2)};
                }
                case GlobalFunctionType::NVALS: {
                    if (fc.args.size() != 1) return {false, 0};
                    const std::string& spec = fc.args[0];
                    auto dot = spec.find('.');
                    if (dot == std::string::npos) return {false, 0};
                    std::string tok_name = spec.substr(0, dot);
                    std::string attr_raw = spec.substr(dot + 1);
                    CorpusPos p = resolve_name(m, name_map, tok_name);
                    if (p == NO_HEAD) return {false, 0};
                    auto n = nvals_cardinality_at(p, attr_raw);
                    if (!n) return {false, 0};
                    return {true, *n};
                }
                }
                return {false, 0};
            };

            for (const auto& ff : query.global_function_filters) {
                auto [lhs_ok, lhs_val] = eval_func(ff.lhs);
                if (!lhs_ok) { pass = false; break; }

                int64_t rhs_val = ff.int_value;
                if (ff.has_rhs_func) {
                    auto [rhs_ok, rv] = eval_func(ff.rhs);
                    if (!rhs_ok) { pass = false; break; }
                    rhs_val = rv;
                }

                bool ok = false;
                switch (ff.op) {
                    case CompOp::EQ:  ok = (lhs_val == rhs_val); break;
                    case CompOp::NEQ: ok = (lhs_val != rhs_val); break;
                    case CompOp::LT:  ok = (lhs_val <  rhs_val); break;
                    case CompOp::GT:  ok = (lhs_val >  rhs_val); break;
                    case CompOp::LTE: ok = (lhs_val <= rhs_val); break;
                    case CompOp::GTE: ok = (lhs_val >= rhs_val); break;
                    case CompOp::REGEX: ok = false; break;
                }
                if (!ok) { pass = false; break; }
            }
        }

        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

// ── Anchor preprocessing and filtering ───────────────────────────────────

TokenQuery QueryExecutor::strip_anchors(const TokenQuery& query,
                                        std::vector<AnchorConstraint>& constraints) {
    TokenQuery cleaned;
    cleaned.within = query.within;
    cleaned.not_within = query.not_within;
    cleaned.within_having = query.within_having;
    cleaned.containing_clauses = query.containing_clauses;
    cleaned.global_region_filters = query.global_region_filters;
    cleaned.global_alignment_filters = query.global_alignment_filters;
    cleaned.global_function_filters = query.global_function_filters;
    cleaned.position_orders = query.position_orders;

    // Map from old token indices to new (after stripping anchors)
    std::vector<int> old_to_new(query.tokens.size(), -1);

    for (size_t i = 0; i < query.tokens.size(); ++i) {
        if (query.tokens[i].is_anchor()) {
            // Record the constraint: bind to the nearest real token
            AnchorConstraint ac;
            ac.region = query.tokens[i].anchor_region;
            ac.is_start = (query.tokens[i].anchor == RegionAnchorType::REGION_START);
            ac.attrs = query.tokens[i].anchor_attrs;

            if (ac.is_start) {
                // <s> binds to the next real token
                for (size_t j = i + 1; j < query.tokens.size(); ++j) {
                    if (!query.tokens[j].is_anchor()) {
                        // We'll set token_idx after we know the new index
                        ac.token_idx = j;  // temporarily store old index
                        break;
                    }
                }
            } else {
                // </s> binds to the previous real token
                for (int j = static_cast<int>(i) - 1; j >= 0; --j) {
                    if (!query.tokens[static_cast<size_t>(j)].is_anchor()) {
                        ac.token_idx = static_cast<size_t>(j);
                        break;
                    }
                }
            }
            constraints.push_back(ac);
        } else {
            old_to_new[i] = static_cast<int>(cleaned.tokens.size());
            cleaned.tokens.push_back(query.tokens[i]);
        }
    }

    // Remap constraint token indices from old to new
    for (auto& ac : constraints) {
        int new_idx = old_to_new[ac.token_idx];
        if (new_idx < 0) {
            // Anchor binding to another anchor — shouldn't happen, ignore
            ac.token_idx = 0;
        } else {
            ac.token_idx = static_cast<size_t>(new_idx);
        }
    }

    // Rebuild relations: only keep relations between consecutive real tokens
    // The relation between old tokens[i] and tokens[i+1] maps to the relation
    // between the real tokens they connect.
    // For a chain like <s> [] [] </s>, the relations are:
    //   [<s>→[]] [[]→[]] [[]]→</s>]
    // After stripping, we just need the relation between [] and []
    // Use SEQUENCE for all surviving consecutive pairs (original relations carry over)
    for (size_t i = 0; i + 1 < cleaned.tokens.size(); ++i) {
        // Find the original relation between these tokens
        // Default to SEQUENCE if we can't determine from originals
        cleaned.relations.push_back({RelationType::SEQUENCE});
    }

    // Better: try to find original relations between the real tokens
    // Overwrite with actual relation types where possible
    cleaned.relations.clear();
    std::vector<size_t> real_indices;
    for (size_t i = 0; i < query.tokens.size(); ++i) {
        if (!query.tokens[i].is_anchor())
            real_indices.push_back(i);
    }
    for (size_t k = 0; k + 1 < real_indices.size(); ++k) {
        size_t from = real_indices[k];
        size_t to = real_indices[k + 1];
        // Find the relation on the edge closest to 'from' going toward 'to'
        // The original relations[i] connects tokens[i] and tokens[i+1]
        // Between from and to, pick the first non-anchor relation
        RelationType rel = RelationType::SEQUENCE;  // default
        for (size_t e = from; e < to && e < query.relations.size(); ++e) {
            if (!query.tokens[e].is_anchor() || !query.tokens[e + 1].is_anchor()) {
                rel = query.relations[e].type;
                break;
            }
        }
        cleaned.relations.push_back({rel});
    }

    return cleaned;
}

void QueryExecutor::apply_anchor_filters(const std::vector<AnchorConstraint>& constraints,
                                         MatchSet& result) const {
    if (constraints.empty()) return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());

    for (const auto& m : result.matches) {
        bool pass = true;
        for (const auto& ac : constraints) {
            if (ac.token_idx >= m.positions.size()) { pass = false; break; }
            CorpusPos pos = ac.is_start ? m.positions[ac.token_idx]
                                        : (ac.token_idx < m.span_ends.size()
                                           ? m.span_ends[ac.token_idx]
                                           : m.positions[ac.token_idx]);
            if (!corpus_.has_structure(ac.region)) { pass = false; break; }
            const auto& sa = corpus_.structure(ac.region);
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0) { pass = false; break; }

            Region reg = sa.get(static_cast<size_t>(rgn));
            if (ac.is_start) {
                // Token must be at region start
                if (pos != reg.start) { pass = false; break; }
            } else {
                // Token must be at region end
                if (pos != reg.end) { pass = false; break; }
            }

            // Check optional region attributes: <text genre="book">
            for (const auto& [key, val] : ac.attrs) {
                if (!sa.has_region_attr(key)) { pass = false; break; }
                std::string_view rv = sa.region_value(key, static_cast<size_t>(rgn));
                if (rv != val) { pass = false; break; }
            }
            if (!pass) break;
        }
        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

void QueryExecutor::apply_within_having(const TokenQuery& query, MatchSet& result) const {
    if (!query.within_having || query.within.empty()) return;
    if (!corpus_.has_structure(query.within)) return;

    const auto& sa = corpus_.structure(query.within);

    std::vector<Match> kept;
    kept.reserve(result.matches.size());

    for (const auto& m : result.matches) {
        CorpusPos pos = m.first_pos();
        int64_t rgn = sa.find_region(pos);
        if (rgn < 0) continue;

        Region reg = sa.get(static_cast<size_t>(rgn));
        CorpusPos rgn_start = reg.start;
        CorpusPos rgn_end   = reg.end;

        // Check if any position in the region satisfies the having condition
        bool found = false;
        for (CorpusPos p = rgn_start; p <= rgn_end; ++p) {
            if (check_conditions(p, query.within_having)) {
                found = true;
                break;
            }
        }
        if (found) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

// ── Containing / not-within / position-order filters ─────────────────

void QueryExecutor::apply_containing(const TokenQuery& query, MatchSet& result) const {
    if (query.containing_clauses.empty()) return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());

    for (const auto& m : result.matches) {
        CorpusPos ms = m.first_pos();
        CorpusPos me = m.last_pos();
        bool pass = true;

        for (const auto& cc : query.containing_clauses) {
            bool found = false;

            if (cc.is_subtree) {
                // Dependency subtree containment: find a token in [ms, me] matching
                // cc.subtree_cond whose full subtree is also within [ms, me].
                if (!corpus_.has_deps()) { found = false; }
                else {
                    const auto& deps = corpus_.deps();
                    for (CorpusPos p = ms; p <= me; ++p) {
                        if (!check_conditions(p, cc.subtree_cond)) continue;
                        auto sub = deps.subtree(p);
                        bool all_inside = true;
                        for (CorpusPos sp : sub) {
                            if (sp < ms || sp > me) { all_inside = false; break; }
                        }
                        if (all_inside) { found = true; break; }
                    }
                }
            } else {
                // Structural region containment: check if any region of the
                // specified type has both start and end within [ms, me].
                if (!corpus_.has_structure(cc.region)) { found = false; }
                else {
                    const auto& sa = corpus_.structure(cc.region);
                    // Binary search: find first region whose start >= ms
                    size_t count = sa.region_count();
                    // Linear scan from the region containing ms
                    int64_t rgn = sa.find_region(ms);
                    if (rgn < 0) rgn = 0;
                    for (size_t r = static_cast<size_t>(rgn); r < count; ++r) {
                        Region reg = sa.get(r);
                        if (reg.start > me) break;  // past match end
                        if (reg.start >= ms && reg.end <= me) {
                            found = true;
                            break;
                        }
                    }
                }
            }

            if (cc.negated) found = !found;
            if (!found) { pass = false; break; }
        }

        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

void QueryExecutor::apply_not_within(const TokenQuery& query, MatchSet& result) const {
    if (!query.not_within || query.within.empty()) return;
    if (!corpus_.has_structure(query.within)) return;

    const auto& sa = corpus_.structure(query.within);

    std::vector<Match> kept;
    kept.reserve(result.matches.size());

    for (const auto& m : result.matches) {
        // "not within s": match must NOT be entirely inside any region of type s
        bool inside = false;
        for (size_t i = 0; i < m.positions.size(); ++i) {
            if (m.positions[i] == NO_HEAD) continue;
            int64_t rgn = sa.find_region(m.positions[i]);
            if (rgn >= 0) { inside = true; break; }
        }
        if (!inside) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

void QueryExecutor::apply_position_orders(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const {
    if (query.position_orders.empty()) return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());

    for (const auto& m : result.matches) {
        bool pass = true;
        for (const auto& po : query.position_orders) {
            CorpusPos p1 = resolve_name(m, name_map, po.name1);
            CorpusPos p2 = resolve_name(m, name_map, po.name2);
            if (p1 == NO_HEAD || p2 == NO_HEAD) { pass = false; break; }
            bool ok = false;
            switch (po.op) {
                case CompOp::LT:  ok = (p1 < p2); break;
                case CompOp::GT:  ok = (p1 > p2); break;
                default: ok = (p1 < p2); break;
            }
            if (!ok) { pass = false; break; }
        }
        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

// ── Set operations ──────────────────────────────────────────────────────

// Galloping (exponential) search: find first element >= target in [lo, hi).
// Returns iterator to the first element >= target, or hi if none.
static auto gallop(std::vector<CorpusPos>::const_iterator lo,
                   std::vector<CorpusPos>::const_iterator hi,
                   CorpusPos target) {
    // Exponential jump to find bracket
    size_t step = 1;
    auto it = lo;
    while (it < hi && *it < target) {
        lo = it;
        it += static_cast<ptrdiff_t>(step);
        step <<= 1;
    }
    if (it > hi) it = hi;
    // Binary search within bracket [lo, it)
    return std::lower_bound(lo, it, target);
}

std::vector<CorpusPos> QueryExecutor::intersect(
        const std::vector<CorpusPos>& a,
        const std::vector<CorpusPos>& b) {
    std::vector<CorpusPos> out;
    if (a.empty() || b.empty()) return out;

    // When one list is >8× larger, galloping is faster than linear merge.
    // O(|small| × log(|large| / |small|)) vs O(|small| + |large|).
    const auto& small = (a.size() <= b.size()) ? a : b;
    const auto& large = (a.size() <= b.size()) ? b : a;

    if (large.size() > 8 * small.size()) {
        out.reserve(small.size());
        auto lo = large.begin();
        for (CorpusPos val : small) {
            lo = gallop(lo, large.end(), val);
            if (lo == large.end()) break;
            if (*lo == val) {
                out.push_back(val);
                ++lo;
            }
        }
    } else {
        // Balanced sizes: standard two-pointer merge
        auto ia = a.begin(), ib = b.begin();
        while (ia != a.end() && ib != b.end()) {
            if (*ia == *ib) { out.push_back(*ia); ++ia; ++ib; }
            else if (*ia < *ib) ++ia;
            else ++ib;
        }
    }
    return out;
}

std::vector<CorpusPos> QueryExecutor::unite(
        const std::vector<CorpusPos>& a,
        const std::vector<CorpusPos>& b) {
    std::vector<CorpusPos> out;
    std::merge(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(out));
    auto it = std::unique(out.begin(), out.end());
    out.erase(it, out.end());
    return out;
}

} // namespace manatree
