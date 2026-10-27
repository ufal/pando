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

bool QueryExecutor::layer_crosses_default_within(const TokenQuery& q) const {
    const std::string& dw = corpus_.default_within();
    if (dw.empty()) return false;
    auto wider = [&](const std::string& name) {
        if (name.empty() || name == "head") return false;
        const RelationDecl* d = corpus_.relation(name);
        return d && d->scope != "s" && d->scope != dw;
    };
    for (const auto& r : q.relations)
        if (wider(r.layer)) return true;
    for (const auto& po : q.position_orders)
        if (po.is_relation && wider(po.relation.layer)) return true;
    return false;
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

std::optional<MatchSet> QueryExecutor::execute_layer_decomposed(const TokenQuery& q, size_t max_matches,
                                                                bool count_total, size_t max_total_cap,
                                                                size_t sample_size, unsigned num_threads,
                                                                const std::vector<std::string>* aggregate_by_fields) {
    // the shape: [A] OP:L[:lab] [B], two plain tokens, OP one of > < !> !<, L a thin pointer layer over the tree
    if (!count_total || sample_size > 0 || (aggregate_by_fields && !aggregate_by_fields->empty())) return std::nullopt;
    if (!fastpaths_enabled() || !corpus_.has_deps() || q.tokens.size() != 2 || q.relations.size() != 1)
        return std::nullopt;
    const QueryRelation& rel = q.relations[0];
    const RelationType t = rel.type;
    const bool negated = t == RelationType::NOT_GOVERNS || t == RelationType::NOT_GOV_BY;
    if (rel.layer.empty() || rel.layer == "head"
        || (t != RelationType::GOVERNS && t != RelationType::GOVERNED_BY && !negated))
        return std::nullopt;
    const RelationDecl* decl = corpus_.relation(rel.layer);
    const RelationLayer* layer = corpus_.relation_layer(rel.layer);
    if (!decl || !layer || decl->inherit != "head" || decl->materialized) return std::nullopt;
    for (const auto& tok : q.tokens)
        if (tok.has_repetition() || tok.is_anchor() || tok.is_dep_subtree || !tok.name.empty()) return std::nullopt;
    if (!q.within.empty() || q.not_within || q.within_having || !q.containing_clauses.empty()
        || !q.global_region_filters.empty() || !q.global_alignment_filters.empty()
        || !q.global_function_filters.empty() || !q.position_orders.empty())
        return std::nullopt;
    if (!corpus_.has_attr("deprel")) return std::nullopt;

    // which token is the head side and which the dependent: `[A] > [B]` / `[A] !> [B]` A heads B;
    // `[A] < [B]` / `[A] !< [B]` B heads A (for the negations the head side is the token kept)
    const bool head_first = t == RelationType::GOVERNS || t == RelationType::NOT_GOVERNS;
    const ConditionPtr& hc = q.tokens[head_first ? 0 : 1].conditions;
    const ConditionPtr& dc = q.tokens[head_first ? 1 : 0].conditions;
    const auto& deprel = corpus_.attr("deprel");
    const auto& deps = corpus_.deps();
    auto basic_edge_ok = [&](CorpusPos h, CorpusPos d) {
        return (rel.label.empty() || deprel.value_at(d) == rel.label) && check_conditions(d, dc)
               && check_conditions(h, hc);
    };

    // the basic tree's total, through its fast paths
    TokenQuery basic = q;
    basic.relations[0].layer = "head";
    TokenQuery bq;
    rewrite_head_layer(basic, bq);
    const MatchSet b = execute(bq, 1, true, 0, 0, 0, num_threads);
    if (!b.total_exact) return std::nullopt;
    int64_t total = static_cast<int64_t>(b.total_count);

    // corrections at the deviating tokens D only
    const size_t nd = layer->deviating();
    if (!negated) {
        // pairs (head, d), d ∈ D: the tree's edge counted there is replaced by d's own edges
        for (size_t i = 0; i < nd; ++i) {
            const CorpusPos d = layer->deviating_pos(i);
            const CorpusPos bh = deps.head(d);
            if (bh != NO_HEAD && basic_edge_ok(bh, d)) --total;
            if (!check_conditions(d, dc)) continue;
            CorpusPos last = NO_HEAD;
            layer->for_each_head(d, [&](CorpusPos h, std::string_view label) {
                if (h != NO_HEAD && h != last && (rel.label.empty() || label == rel.label) && check_conditions(h, hc)) {
                    ++total;
                    last = h;   // edges come sorted by head: one hit per (head, d) pair
                }
                return true;
            });
        }
    } else {
        // head-side tokens whose dependents change: the tree heads and the layer heads of D
        std::vector<CorpusPos> touched;
        for (size_t i = 0; i < nd; ++i) {
            const CorpusPos d = layer->deviating_pos(i);
            if (const CorpusPos bh = deps.head(d); bh != NO_HEAD) touched.push_back(bh);
            layer->for_each_head(d, [&](CorpusPos h, std::string_view) {
                if (h != NO_HEAD) touched.push_back(h);
                return true;
            });
        }
        std::sort(touched.begin(), touched.end());
        touched.erase(std::unique(touched.begin(), touched.end()), touched.end());
        for (CorpusPos h : touched) {
            if (!check_conditions(h, hc)) continue;
            bool basic_has = false, layer_has = false;
            for (CorpusPos c : deps.children(h))
                if (basic_edge_ok(h, c)) { basic_has = true; break; }
            layer->for_each_child(h, [&](CorpusPos c, std::string_view label) {
                if ((rel.label.empty() || label == rel.label) && check_conditions(c, dc)) {
                    layer_has = true;
                    return false;
                }
                return true;
            });
            total += static_cast<int64_t>(!layer_has) - static_cast<int64_t>(!basic_has);
        }
    }

    // the page: the generic expansion stops at it
    MatchSet result = max_matches == 0 ? MatchSet{} : execute(q, max_matches, false, 0, 0, 0, 1);
    if (max_matches == 0) result.num_tokens = 2;
    result.total_count = static_cast<size_t>(std::max<int64_t>(total, 0));
    result.total_exact = true;
    if (max_total_cap > 0 && result.total_count > max_total_cap) {
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    result.plan_path = "layer_inherit:" + b.plan_path;
    return result;
}

std::optional<MatchSet> QueryExecutor::execute_layer_deviations(const TokenQuery& q, size_t max_matches,
                                                                bool count_total, size_t max_total_cap,
                                                                size_t sample_size,
                                                                const std::vector<std::string>* aggregate_by_fields) const {
    if (sample_size > 0 || (aggregate_by_fields && !aggregate_by_fields->empty()) || !fastpaths_enabled()) return std::nullopt;
    if (!corpus_.has_deps() || q.tokens.size() != 2 || q.relations.size() != 1 || q.position_orders.size() != 1)
        return std::nullopt;
    const QueryRelation& rel = q.relations[0];
    if (rel.layer.empty() || rel.layer == "head"
        || (rel.type != RelationType::GOVERNS && rel.type != RelationType::GOVERNED_BY))
        return std::nullopt;
    const RelationDecl* decl = corpus_.relation(rel.layer);
    const RelationLayer* layer = corpus_.relation_layer(rel.layer);
    if (!decl || !layer || decl->inherit != "head" || decl->materialized) return std::nullopt;
    for (const auto& tok : q.tokens)
        if (tok.has_repetition() || tok.is_anchor() || tok.is_dep_subtree || tok.name.empty()) return std::nullopt;
    if (!q.within.empty() || q.not_within || q.within_having || !q.containing_clauses.empty()
        || !q.global_region_filters.empty() || !q.global_alignment_filters.empty() || !q.global_function_filters.empty())
        return std::nullopt;
    // the filter: no basic-tree edge between the same head and dependent (`:: h !>:head d` or `:: d !<:head h`)
    const auto& po = q.position_orders[0];
    const QueryRelation& f = po.relation;
    if (!po.is_relation || f.layer != "head"
        || (f.type != RelationType::NOT_GOVERNS && f.type != RelationType::NOT_GOV_BY))
        return std::nullopt;
    const size_t head_i = rel.type == RelationType::GOVERNS ? 0 : 1, dep_i = 1 - head_i;
    const std::string& f_head = f.type == RelationType::NOT_GOVERNS ? po.name1 : po.name2;
    const std::string& f_dep = f.type == RelationType::NOT_GOVERNS ? po.name2 : po.name1;
    if (f_head != q.tokens[head_i].name || f_dep != q.tokens[dep_i].name) return std::nullopt;
    if (!f.label.empty() && !corpus_.has_attr("deprel")) return std::nullopt;

    const ConditionPtr& hc = q.tokens[head_i].conditions;
    const ConditionPtr& dc = q.tokens[dep_i].conditions;
    const auto& deps = corpus_.deps();
    std::vector<std::pair<CorpusPos, CorpusPos>> pairs;   // (first token, second token)
    for (size_t i = 0; i < layer->deviating(); ++i) {
        const CorpusPos d = layer->deviating_pos(i);
        if (!check_conditions(d, dc)) continue;
        const CorpusPos bh = deps.head(d);
        // the tree's edge into d, as far as the filter sees it (with its label, if it names one)
        const bool tree_edge_labelled = f.label.empty() || corpus_.attr("deprel").value_at(d) == f.label;
        CorpusPos last = NO_HEAD;
        layer->for_each_head(d, [&](CorpusPos h, std::string_view label) {
            if (h == NO_HEAD || h == last || (!rel.label.empty() && label != rel.label)) return true;
            if (h == bh && tree_edge_labelled) return true;   // the tree has this edge
            if (!check_conditions(h, hc)) return true;
            last = h;
            pairs.emplace_back(head_i == 0 ? h : d, head_i == 0 ? d : h);
            return true;
        });
    }
    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
    MatchSet result;
    result.num_tokens = 2;
    result.plan_path = "layer_deviations";
    const size_t keep = max_matches == 0 ? pairs.size() : std::min(max_matches, pairs.size());
    result.matches.reserve(keep);
    for (size_t i = 0; i < keep; ++i) {
        Match m;
        m.positions = {pairs[i].first, pairs[i].second};
        m.span_ends = {pairs[i].first, pairs[i].second};
        result.matches.push_back(std::move(m));
    }
    result.total_count = pairs.size();   // every pair is known: the total comes free
    result.total_exact = true;
    if (max_total_cap > 0 && result.total_count > max_total_cap) {
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    return result;
}

}  // namespace pando
