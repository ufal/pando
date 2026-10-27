// Relation layers in queries (dev/PROPOSAL-RELATION-LAYERS.md): `[A] >:X [B]` / `[A] <:X [B]`, optionally with a label
// (`>:X:nsubj`). `>:head` is the basic tree: rewritten into `>` / `<` with a deprel condition, so the tree's fast paths
// apply. Other layers (relation_layer.h, e.g. enhanced `deps`) are joined here: the dependent side's candidates, each
// one's heads in the layer (its own edges, or the inherited basic edge), the head side checked at each head; one hit
// per (head, dependent) pair.

#include "query/executor.h"

#include <algorithm>
#include <stdexcept>

namespace pando {

bool QueryExecutor::rewrite_head_layer(const TokenQuery& q, TokenQuery& out) {
    bool any = false;
    for (const auto& r : q.relations)
        if (r.layer == "head") any = true;
    if (!any) return false;
    out = q;
    for (size_t i = 0; i < out.relations.size(); ++i) {
        QueryRelation& r = out.relations[i];
        if (r.layer != "head") continue;
        if (!r.label.empty()) {
            // the label is the dependent's deprel: B for `[A] > [B]`, A for `[A] < [B]`
            QueryToken& dep = out.tokens[r.type == RelationType::GOVERNS ? i + 1 : i];
            AttrCondition ac;
            ac.attr = "deprel";
            ac.op = CompOp::EQ;
            ac.value = r.label;
            auto leaf = ConditionNode::make_leaf(std::move(ac));
            dep.conditions = dep.conditions ? ConditionNode::make_branch(BoolOp::AND, dep.conditions, leaf) : leaf;
        }
        r.layer.clear();
        r.label.clear();
    }
    return true;
}

MatchSet QueryExecutor::execute_layer_relation(const TokenQuery& q, size_t max_matches, bool count_total,
                                               size_t max_total_cap) const {
    const QueryRelation& rel = q.relations.at(0);
    const RelationLayer* layer = corpus_.relation_layer(rel.layer);
    if (!layer)
        throw std::runtime_error("This corpus has no relation layer '" + rel.layer + "'" +
                                 (corpus_.relations().empty() ? std::string() : " (it has: " + [&] {
                                     std::string names;
                                     for (const auto& r : corpus_.relations()) names += (names.empty() ? "" : ", ") + r.name;
                                     return names;
                                 }() + ")"));
    if (q.tokens.size() != 2 || q.relations.size() != 1)
        throw std::runtime_error(":" + rel.layer + " works between two tokens for now: [A] >:" + rel.layer + " [B]");
    for (const auto& t : q.tokens)
        if (t.has_repetition() || t.is_anchor() || t.is_dep_subtree)
            throw std::runtime_error(":" + rel.layer + " takes plain tokens on both sides for now");
    if (!q.global_region_filters.empty() || !q.global_alignment_filters.empty() || !q.global_function_filters.empty()
        || !q.position_orders.empty() || q.within_having || q.not_within || !q.containing_clauses.empty())
        throw std::runtime_error(":" + rel.layer + " takes no :: filters, within, containing yet; plain tokens work");

    // [A] >:X [B]: B's X heads include A (A governs); [A] <:X [B]: A's X heads include B
    const bool head_first = rel.type == RelationType::GOVERNS;
    const size_t dep_i = head_first ? 1 : 0, head_i = 1 - dep_i;
    const ConditionPtr& dep_cond = q.tokens[dep_i].conditions;
    const ConditionPtr& head_cond = q.tokens[head_i].conditions;

    std::vector<CorpusPos> deps = dep_cond ? resolve_conditions(dep_cond) : std::vector<CorpusPos>{};
    if (!dep_cond) {
        deps.resize(static_cast<size_t>(corpus_.size()));
        for (size_t p = 0; p < deps.size(); ++p) deps[p] = static_cast<CorpusPos>(p);
    }

    MatchSet result;
    result.num_tokens = 2;
    result.plan_path = "layer_join";
    std::vector<std::pair<CorpusPos, CorpusPos>> pairs;   // (first token, second token)
    size_t total = 0;
    bool capped = false;
    for (CorpusPos d : deps) {
        layer->for_each_head(d, [&](CorpusPos h, std::string_view label) {
            if (h == NO_HEAD) return true;
            if (!rel.label.empty() && label != rel.label) return true;
            if (head_cond && !check_conditions(h, head_cond)) return true;
            ++total;
            if (max_matches == 0 || pairs.size() < max_matches || count_total)
                pairs.emplace_back(head_first ? h : d, head_first ? d : h);
            if (max_total_cap > 0 && total >= max_total_cap) { capped = true; return false; }
            return true;
        });
        if (capped) break;
        if (!count_total && max_matches > 0 && pairs.size() >= max_matches) break;
    }
    // one hit per pair (a layer may repeat an edge with another label), in corpus order of the first token
    std::sort(pairs.begin(), pairs.end());
    const size_t before = pairs.size();
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
    total -= before - pairs.size();
    const size_t keep = max_matches == 0 ? pairs.size() : std::min(max_matches, pairs.size());
    result.matches.reserve(keep);
    for (size_t i = 0; i < keep; ++i) {
        Match m;
        m.positions.push_back(pairs[i].first);
        m.positions.push_back(pairs[i].second);
        m.span_ends.push_back(pairs[i].first);
        m.span_ends.push_back(pairs[i].second);
        result.matches.push_back(std::move(m));
    }
    result.total_count = count_total ? total : result.matches.size();
    result.total_exact = !capped && (count_total || max_matches == 0 || pairs.size() < max_matches);
    return result;
}

}  // namespace pando
