#include "subcorpus/restrict.h"

#include "corpus/corpus.h"

#include <algorithm>

namespace pando {

namespace {

/// One hit = one position: a single plain token (no repetition, no contraction run, no subtree).
bool one_position_per_hit(const TokenQuery& q) {
    size_t real = 0;
    for (const auto& t : q.tokens) {
        if (t.is_anchor()) continue;
        ++real;
        if (t.has_repetition() || t.bare_string || t.is_dep_subtree) return false;
    }
    return real == 1 && q.union_alternatives.empty();
}

ConditionPtr in_set_leaf(const Corpus& corpus, const std::shared_ptr<const PositionSet>& set) {
    AttrCondition ac;
    // a positional attribute (the paths that look at `attr` before `op` see an existing one)
    ac.attr = corpus.has_attr("form") ? "form" : (corpus.attr_names().empty() ? "word" : corpus.attr_names()[0]);
    ac.op = CompOp::IN;
    ac.value = "subcorpus";
    ac.in_set = set;
    return ConditionNode::make_leaf(std::move(ac));
}

void add_in_set(std::vector<QueryToken>& tokens, const ConditionPtr& leaf) {
    for (auto& t : tokens) {
        if (t.is_anchor() || t.is_dep_subtree) continue;
        const ConditionPtr& c = t.conditions;
        if (t.bare_string && c && !c->is_leaf && !c->is_structural && !c->is_count && c->bool_op == BoolOp::OR
            && c->left && c->right) {
            // [form="s" | contr_form="s"]: inside each side (the executor keeps the form side alone
            // on a corpus without contractions, simplify_bare_strings)
            t.conditions = ConditionNode::make_branch(BoolOp::OR,
                ConditionNode::make_branch(BoolOp::AND, c->left, leaf),
                ConditionNode::make_branch(BoolOp::AND, c->right, leaf));
            continue;
        }
        t.conditions = c ? ConditionNode::make_branch(BoolOp::AND, c, leaf) : leaf;
    }
}

/// Regions as hits: the starts of the regions of `structure` that overlap the set.
std::shared_ptr<const PositionSet::Runs> overlapping_region_starts(const Corpus& corpus, const std::string& structure,
                                                                  const PositionSet& set) {
    auto out = std::make_shared<PositionSet::Runs>();
    if (!corpus.has_structure(structure)) return out;
    const auto& sa = corpus.structure(structure);
    for (size_t i = 0; i < sa.region_count(); ++i) {
        const Region r = sa.get(i);
        if (r.start <= r.end && set.overlaps(r.start, r.end)) out->emplace_back(r.start, r.start);
    }
    std::sort(out->begin(), out->end());
    out->erase(std::unique(out->begin(), out->end()), out->end());
    return out;
}

}  // namespace

void restrict_query_to_subcorpus(const Corpus& corpus, TokenQuery& q, const std::shared_ptr<const PositionSet>& set) {
    if (!set) return;
    const bool tokens = std::any_of(q.tokens.begin(), q.tokens.end(), [](const QueryToken& t) { return !t.is_anchor(); });
    if (!tokens) {
        // region anchors only: a region counts when part of it is inside
        for (const auto& t : q.tokens) {
            if (t.anchor != RegionAnchorType::REGION_START) continue;
            GlobalRegionFilter gf;
            gf.positions = overlapping_region_starts(corpus, t.anchor_region, *set);
            q.global_region_filters.push_back(std::move(gf));
            break;
        }
        return;
    }
    if (auto runs = set->ranges()) {
        GlobalRegionFilter gf;   // the first position: the mask paths slice to the ranges
        gf.positions = runs;
        q.global_region_filters.push_back(std::move(gf));
        if (one_position_per_hit(q)) return;
    }
    const ConditionPtr leaf = in_set_leaf(corpus, set);
    add_in_set(q.tokens, leaf);
    for (auto& alt : q.union_alternatives) add_in_set(alt, leaf);
}

void restrict_statement_to_subcorpus(const Corpus& corpus, Statement& st, const std::shared_ptr<const PositionSet>& set) {
    if (!st.has_query) return;
    restrict_query_to_subcorpus(corpus, st.query, set);
    if (st.is_parallel) restrict_query_to_subcorpus(corpus, st.target_query, set);
}

} // namespace pando
