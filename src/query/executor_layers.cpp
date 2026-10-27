// Relation layers in queries (dev/PROPOSAL-RELATION-LAYERS.md): `[A] >:X [B]`, `<:X`, `!>:X`, `!<:X`, `>>:X`, `<<:X`,
// optionally with a label on a single edge (`>:X:nsubj`). `>:head` is the basic tree: rewritten into the plain operators
// with a deprel condition, so the tree's fast paths apply. Other layers (relation_layer.h, e.g. enhanced `deps`) are
// traversed edge by edge in the generic expansion (expand_seed → for_each_related), so they combine with everything
// it supports: more tokens, names, filters, within, count by. One hit per (head, dependent) pair: an edge repeated
// with another label is one relation between the two tokens.

#include "query/executor.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

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
            // the label is the dependent's deprel: B for `[A] > [B]` / `!>`, A for `[A] < [B]` / `!<`
            const bool dep_second = r.type == RelationType::GOVERNS || r.type == RelationType::NOT_GOVERNS;
            QueryToken& dep = out.tokens[dep_second ? i + 1 : i];
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

std::vector<CorpusPos> QueryExecutor::find_layer_related(CorpusPos pos, const QueryRelation& qr, RelationType rel,
                                                         bool reversed) const {
    const RelationLayer* layer = corpus_.relation_layer(qr.layer);
    if (!layer) {
        std::string names;
        for (const auto& r : corpus_.relations()) names += (names.empty() ? "" : ", ") + r.name;
        throw std::runtime_error("This corpus has no relation layer '" + qr.layer + "'" +
                                 (names.empty() ? std::string() : " (it has: " + names + ")"));
    }
    // the direction actually walked: towards the dependents (down) or the heads (up)
    bool down = rel == RelationType::GOVERNS || rel == RelationType::TRANS_GOVERNS || rel == RelationType::NOT_GOVERNS;
    if (reversed) down = !down;
    const bool transitive = rel == RelationType::TRANS_GOVERNS || rel == RelationType::TRANS_GOV_BY;

    std::vector<CorpusPos> out;
    auto step = [&](CorpusPos from, auto&& f) {
        auto take = [&](CorpusPos p, std::string_view label) {
            if (p != NO_HEAD && (qr.label.empty() || label == qr.label)) f(p);
            return true;
        };
        if (down) layer->for_each_child(from, take);
        else layer->for_each_head(from, take);
    };
    if (!transitive) {
        step(pos, [&](CorpusPos p) { out.push_back(p); });
    } else {
        // a layer need not be a tree: every token reachable over one or more edges, each once
        std::unordered_set<CorpusPos> seen{pos};
        std::vector<CorpusPos> todo{pos};
        while (!todo.empty()) {
            const CorpusPos from = todo.back();
            todo.pop_back();
            step(from, [&](CorpusPos p) {
                if (seen.insert(p).second) {
                    out.push_back(p);
                    todo.push_back(p);
                }
            });
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

}  // namespace pando
