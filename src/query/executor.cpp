#include "query/executor.h"
#include <algorithm>
#include <iostream>
#include <queue>
#include <random>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#ifndef PANDO_USE_RE2
#include <mutex>
#endif

namespace manatree {

QueryExecutor::QueryExecutor(const Corpus& corpus) : corpus_(corpus) {}

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

// ── Cardinality estimation ──────────────────────────────────────────────
//
// Uses the rev.idx to get exact counts in O(1) per EQ condition.
// For AND, takes min; for OR, takes sum.  This is a conservative upper
// bound that's free to compute.

size_t QueryExecutor::estimate_leaf(const AttrCondition& ac) const {
    std::string name = normalize_attr(ac.attr);

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

    if (!corpus_.has_attr(name)) return 0;
    const auto& pa = corpus_.attr(name);

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

bool QueryExecutor::check_leaf(CorpusPos pos, const AttrCondition& ac) const {
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

    if (!corpus_.has_attr(name)) return false;
    const auto& pa = corpus_.attr(name);
    std::string_view val = pa.value_at(pos);

    switch (ac.op) {
        case CompOp::EQ:    return val == ac.value;
        case CompOp::NEQ:   return val != ac.value;
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
            return re2::RE2::FullMatch(val, *compiled);
#else
            std::lock_guard<std::mutex> lock(regex_cache_mutex_);
            auto it = regex_cache_.find(ac.value);
            if (it == regex_cache_.end())
                it = regex_cache_.emplace(ac.value, std::regex(ac.value)).first;
            std::string s(val);
            return std::regex_match(s, it->second);
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

void QueryExecutor::for_each_seed_position(const ConditionPtr& cond,
                                           std::function<bool(CorpusPos)> f) const {
    if (!cond) {
        for (CorpusPos p = 0; p < corpus_.size(); ++p)
            if (!f(p)) return;
        return;
    }
    if (cond->is_leaf) {
        const AttrCondition& ac = cond->leaf;
        std::string name = normalize_attr(ac.attr);
        std::string feat_name;
        if (is_feats_sub(name, feat_name) && !corpus_.has_attr(name)
            && corpus_.has_attr("feats")) {
            // Feats: still use resolve_leaf (multiple lex ids)
            auto vec = resolve_leaf(ac);
            for (CorpusPos p : vec)
                if (!f(p)) return;
            return;
        }
        if (!corpus_.has_attr(name)) return;
        const auto& pa = corpus_.attr(name);
        if (ac.op == CompOp::EQ) {
            LexiconId id = pa.lexicon().lookup(ac.value);
            if (id == UNKNOWN_LEX) return;
            pa.for_each_position_id(id, f);
            return;
        }
        // NEQ, REGEX: materialize
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

    if (!corpus_.has_attr(name)) return {};
    const auto& pa = corpus_.attr(name);

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

// ── Main execution ──────────────────────────────────────────────────────

MatchSet QueryExecutor::execute(const TokenQuery& query,
                                size_t max_matches,
                                bool count_total,
                                size_t max_total_cap,
                                size_t sample_size,
                                uint32_t random_seed,
                                unsigned num_threads) {
    MatchSet result;
    if (query.tokens.empty()) return result;
    size_t n = query.tokens.size();
    result.num_tokens = n;

    std::vector<Match> reservoir;
    if (sample_size > 0) reservoir.reserve(sample_size);
    std::mt19937 rng(random_seed != 0 ? random_seed : static_cast<uint32_t>(std::random_device{}()));

    // Partial match vectors are of size 2*n: [0..n-1]=starts, [n..2n-1]=ends
    // For non-repeating tokens: pm[i] == pm[n+i]
    auto build_match = [&query, n](std::vector<CorpusPos>&& pm) -> Match {
        Match m;
        m.positions.assign(pm.begin(), pm.begin() + n);
        m.span_ends.assign(pm.begin() + n, pm.begin() + 2 * n);
        for (size_t i = 0; i < n; ++i)
            if (!query.tokens[i].name.empty() && m.positions[i] != static_cast<CorpusPos>(-1))
                m.name_to_position[query.tokens[i].name] = m.positions[i];
        return m;
    };

    auto add_match = [&](std::vector<CorpusPos>&& positions) {
        ++result.total_count;
        Match m = build_match(std::move(positions));
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
        return max_matches > 0 && result.total_count >= max_matches
               && !count_total;
    };
    auto reached_total_cap = [&]() {
        return count_total && max_total_cap > 0 && result.total_count >= max_total_cap;
    };

    // ── Single-token fast path ──────────────────────────────────────────

    if (n == 1) {
        size_t est = estimate_cardinality(query.tokens[0].conditions);
        result.cardinalities = {est};
        result.seed_token = 0;

        int min_rep = query.tokens[0].min_repeat;
        int max_rep = query.tokens[0].max_repeat;

        auto try_spans_from = [&](CorpusPos p) {
            // For single-token query with repetition: try spans starting at p
            for (int len = min_rep; len <= max_rep; ++len) {
                CorpusPos end = p + len - 1;
                if (end >= corpus_.size()) break;
                if (len > 1 && !check_conditions(end, query.tokens[0].conditions)) break;
                // All positions p..end must match (position p already verified by caller)
                // Positions p+1..end-1 checked incrementally below
                bool valid = true;
                if (len == 1) {
                    // already checked by caller
                } else {
                    // We verified p (caller) and end (above); check intermediate
                    for (CorpusPos q = p + 1; q < end; ++q) {
                        if (!check_conditions(q, query.tokens[0].conditions)) {
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

        bool use_scan = !query.tokens[0].conditions
                        || est > static_cast<size_t>(corpus_.size()) / 2;

        if (use_scan) {
            for (CorpusPos p = 0; p < corpus_.size(); ++p) {
                if (check_conditions(p, query.tokens[0].conditions)) {
                    if (!try_spans_from(p)) break;
                }
            }
        } else {
            for_each_seed_position(query.tokens[0].conditions, [&](CorpusPos p) {
                return try_spans_from(p);
            });
        }
        apply_global_filters(query, result);
        result.total_exact = !reached_limit() && !reached_total_cap();
        return result;
    }

    // ── Plan: pick seed by cardinality, expand outward ──────────────────

    QueryPlan plan = plan_query(query);
    result.seed_token = plan.seed;
    result.cardinalities = plan.cardinalities;

    // #9: Use default_within from corpus when query does not specify within
    std::string effective_within = query.within.empty()
        ? corpus_.default_within() : query.within;
    bool has_within = !effective_within.empty() &&
                      corpus_.has_structure(effective_within);
    const StructuralAttr* within_sa = has_within
        ? &corpus_.structure(effective_within) : nullptr;

    // Parallel path: materialize seeds and process chunks in parallel (multi-token only)
    if (num_threads > 1 && n > 1) {
        std::vector<CorpusPos> seeds = resolve_conditions(query.tokens[plan.seed].conditions);
        if (!seeds.empty()) {
            size_t nw = std::min(static_cast<size_t>(num_threads), seeds.size());
            size_t chunk_sz = (seeds.size() + nw - 1) / nw;
            std::vector<std::vector<Match>> thread_matches(nw);
            std::vector<std::thread> workers;
            for (size_t w = 0; w < nw; ++w) {
                size_t start = w * chunk_sz;
                size_t end = std::min(start + chunk_sz, seeds.size());
                workers.emplace_back([this, &query, &plan, within_sa, &seeds,
                                      &thread_matches, start, end, w]() {
                    for (size_t i = start; i < end; ++i) {
                        auto matches = expand_one_seed(query, plan, within_sa, seeds[i]);
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
            apply_global_filters(query, result);
            result.total_exact = true;
            return result;
        }
    }

    // Sequential path: process one seed at a time (lazy when possible)
    for_each_seed_position(query.tokens[plan.seed].conditions, [&](CorpusPos seed_p) {
        expand_seed(query, plan, within_sa, seed_p, [&](std::vector<CorpusPos>&& pm) -> bool {
            add_match(std::move(pm));
            return !reached_limit() && !reached_total_cap();
        });
        return !reached_limit() && !reached_total_cap();
    });

    if (sample_size > 0 && !reservoir.empty())
        result.matches = std::move(reservoir);
    apply_global_filters(query, result);
    result.total_exact = !reached_limit() && !reached_total_cap();
    return result;
}

// ── Shared seed expansion (single source of truth for match logic) ───────

void QueryExecutor::expand_seed(const TokenQuery& query,
                                const QueryPlan& plan,
                                const StructuralAttr* within_sa,
                                CorpusPos seed_p,
                                std::function<bool(std::vector<CorpusPos>&&)> emit) const {
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
                int64_t rgn = within_sa->find_region(anchor);
                if (rgn < 0) continue;
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
        for (size_t i = 0; i < n; ++i)
            if (!query.tokens[i].name.empty() && m.positions[i] != NO_HEAD)
                m.name_to_position[query.tokens[i].name] = m.positions[i];
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

    // Apply region filters (e.g. :: match.text_lang="en") to source/target before joining.
    apply_region_filters(source_query, source_set);
    apply_region_filters(target_query, target_set);

    const auto& filters = source_query.global_alignment_filters;
    for (const auto& s : source_set.matches) {
        for (const auto& t : target_set.matches) {
            bool aligned = true;
            for (const auto& af : filters) {
                auto it1 = s.name_to_position.find(af.name1);
                auto it2 = t.name_to_position.find(af.name2);
                if (it1 == s.name_to_position.end() || it2 == t.name_to_position.end()) {
                    aligned = false;
                    break;
                }
                std::string an1 = normalize_attr(af.attr1);
                std::string an2 = normalize_attr(af.attr2);
                if (!corpus_.has_attr(an1) || !corpus_.has_attr(an2)) {
                    aligned = false;
                    break;
                }
                std::string v1(corpus_.attr(an1).value_at(it1->second));
                std::string v2(corpus_.attr(an2).value_at(it2->second));
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

void QueryExecutor::apply_region_filters(const TokenQuery& query, MatchSet& result) const {
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
            CorpusPos pos = m.first_pos();
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0) { pass = false; break; }
            std::string rval(sa.region_value(attr_name, static_cast<size_t>(rgn)));
            if (!compare_value(gf.op, rval, gf.value)) { pass = false; break; }
        }

        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

void QueryExecutor::apply_global_filters(const TokenQuery& query, MatchSet& result) const {
    if (query.global_region_filters.empty() && query.global_alignment_filters.empty())
        return;

    // First apply region filters
    apply_region_filters(query, result);
    if (query.global_alignment_filters.empty() || result.matches.empty())
        return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (const auto& m : result.matches) {
        bool pass = true;

        for (const auto& af : query.global_alignment_filters) {
            auto it1 = m.name_to_position.find(af.name1);
            auto it2 = m.name_to_position.find(af.name2);
            if (it1 == m.name_to_position.end() || it2 == m.name_to_position.end())
                { pass = false; break; }
            std::string an1 = normalize_attr(af.attr1);
            std::string an2 = normalize_attr(af.attr2);
            if (!corpus_.has_attr(an1) || !corpus_.has_attr(an2))
                { pass = false; break; }
            std::string v1(corpus_.attr(an1).value_at(it1->second));
            std::string v2(corpus_.attr(an2).value_at(it2->second));
            if (v1 != v2) { pass = false; break; }
        }

        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

// ── Set operations ──────────────────────────────────────────────────────

std::vector<CorpusPos> QueryExecutor::intersect(
        const std::vector<CorpusPos>& a,
        const std::vector<CorpusPos>& b) {
    std::vector<CorpusPos> out;
    auto ia = a.begin(), ib = b.begin();
    while (ia != a.end() && ib != b.end()) {
        if (*ia == *ib) { out.push_back(*ia); ++ia; ++ib; }
        else if (*ia < *ib) ++ia;
        else ++ib;
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
