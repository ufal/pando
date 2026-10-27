// Relation layers in queries (dev/PROPOSAL-RELATION-LAYERS.md): `[A] >:X [B]`, `<:X`, `!>:X`, `!<:X`, `>>:X`, `<<:X`,
// optionally with a label on a single edge (`>:X:nsubj`), and `[A] =:X [B]` / `!=:X` for an equivalence layer. `>:head` is the basic tree: rewritten into the plain operators
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
        if (r.type == RelationType::EQUIV || r.type == RelationType::NOT_EQUIV)
            throw std::runtime_error("'head' is the basic tree, a pointer layer: [A] >:head [B], [A] <:head [B]");
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

// `[A] =:X [B]`: the other members of A's classes in the equivalence layer X, whose classes are the values of an
// attribute (relation_decl.h `attr`; a multivalue attribute puts a token in several classes)
std::vector<CorpusPos> QueryExecutor::find_class_mates(CorpusPos pos, const RelationDecl& decl) const {
    std::vector<CorpusPos> out;
    if (!corpus_.has_attr(decl.attr))
        throw std::runtime_error("The equivalence layer '" + decl.name + "' names the attribute '" + decl.attr +
                                 "', which this corpus does not have");
    const std::string_view v = corpus_.attr(decl.attr).value_at(pos);
    const bool mv = corpus_.is_multivalue(decl.attr);
    size_t a = 0;
    while (a <= v.size()) {
        size_t b = mv ? v.find('|', a) : std::string_view::npos;
        if (b == std::string_view::npos) b = v.size();
        const std::string_view id = v.substr(a, b - a);
        if (!id.empty() && id != "_") {
            AttrCondition ac;
            ac.attr = decl.attr;
            ac.op = CompOp::EQ;
            ac.value = std::string(id);
            for (CorpusPos p : resolve_leaf(ac))
                if (p != pos) out.push_back(p);
        }
        a = b + 1;
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::vector<CorpusPos> QueryExecutor::find_layer_related(CorpusPos pos, const QueryRelation& qr, RelationType rel,
                                                         bool reversed) const {
    const RelationDecl* decl = corpus_.relation(qr.layer);
    const bool equiv_op = rel == RelationType::EQUIV || rel == RelationType::NOT_EQUIV;
    if (decl && decl->kind == RelationDecl::Kind::Equivalence) {
        if (!equiv_op)
            throw std::runtime_error("'" + qr.layer + "' is an equivalence layer: [A] =:" + qr.layer + " [B] (same class), "
                                     "[A] !=:" + qr.layer + " [B]");
        if (!qr.label.empty()) throw std::runtime_error("an equivalence layer has no labels: =:" + qr.layer);
        return find_class_mates(pos, *decl);
    }
    if (equiv_op)
        throw std::runtime_error("=:" + qr.layer + " needs an equivalence layer" +
                                 (decl ? "; '" + qr.layer + "' is a pointer layer: >:" + qr.layer + ", <:" + qr.layer
                                       : std::string()));
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

bool QueryExecutor::relation_holds(CorpusPos a, CorpusPos b, const QueryRelation& qr) const {
    RelationType rel = qr.type;
    const bool negated = rel == RelationType::NOT_GOVERNS || rel == RelationType::NOT_GOV_BY
                         || rel == RelationType::NOT_EQUIV;
    if (rel == RelationType::NOT_GOVERNS) rel = RelationType::GOVERNS;
    if (rel == RelationType::NOT_GOV_BY) rel = RelationType::GOVERNED_BY;
    if (rel == RelationType::NOT_EQUIV) rel = RelationType::EQUIV;
    if (qr.layer == "head" && rel == RelationType::EQUIV)
        throw std::runtime_error("'head' is the basic tree, a pointer layer: :: a >:head b");
    bool found = false;
    if (qr.layer == "head") {
        // the basic tree; a label is the dependent's deprel (b for `>`, a for `<`)
        for (CorpusPos r : find_related(a, rel, false))
            if (r == b) { found = true; break; }
        if (found && !qr.label.empty()) {
            const CorpusPos dep = rel == RelationType::GOVERNS ? b : a;
            found = corpus_.has_attr("deprel") && corpus_.attr("deprel").value_at(dep) == qr.label;
        }
    } else {
        const auto related = find_layer_related(a, qr, rel, false);
        found = std::binary_search(related.begin(), related.end(), b);
    }
    return found != negated;
}

}  // namespace pando
