// The class join for an equivalence layer (dev/PROPOSAL-RELATION-LAYERS.md, dev/LINK-LAYERS.md fast path 4):
// `a:[A] =:X b:[B]`, the pairs of distinct tokens that share a class of X, e.g. a word and its translations over
// `tuid`. The generic expansion finds the class mates of each A token one by one; here the classes are walked once
// each. Every class touched by the more selective side is read once (its members, each marked for A and B), so a
// count is Σ_c |A∩c|·|B∩c| − |A∩B∩c| without listing pairs, and a page is the pairs in hit order (by the first
// token, then the second), read off the members of the touched classes.

#include "query/executor.h"
#include "corpus/equivalence_classes.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace pando {

namespace {

struct Member {
    CorpusPos pos;
    LexiconId cls;
    bool in_a, in_b;
};

}  // namespace

std::optional<MatchSet> QueryExecutor::execute_class_join(const TokenQuery& q, size_t max_matches,
                                                          size_t max_total_cap, size_t sample_size,
                                                          const std::vector<std::string>* aggregate_by_fields) const {
    if (sample_size > 0 || (aggregate_by_fields && !aggregate_by_fields->empty()) || !fastpaths_enabled()) return std::nullopt;
    if (range_ || windowed() || q.tokens.size() != 2 || q.relations.size() != 1) return std::nullopt;
    const QueryRelation& rel = q.relations[0];
    if (rel.type != RelationType::EQUIV || rel.layer.empty()) return std::nullopt;
    const RelationDecl* decl = corpus_.relation(rel.layer);
    if (!decl || decl->kind != RelationDecl::Kind::Equivalence || !rel.label.empty() || !corpus_.has_attr(decl->attr))
        return std::nullopt;
    for (const auto& tok : q.tokens)
        if (tok.has_repetition() || tok.is_anchor() || tok.is_dep_subtree) return std::nullopt;
    if (!q.within.empty() || q.not_within || q.within_having || !q.containing_clauses.empty()
        || !q.global_alignment_filters.empty() || !q.global_function_filters.empty() || !q.position_orders.empty())
        return std::nullopt;
    // a default within narrower than the layer's scope would cut the classes: the generic path drops it for such
    // layers (layer_crosses_default_within); one as wide or wider holds every class, so the classes stand as they are
    EquivalenceClasses classes;
    if (!classes.open(corpus_.attr(decl->attr), corpus_.is_multivalue(decl->attr))) return std::nullopt;

    // each side: its token conditions and the `::` region filters on its name (a filter naming both, or none, is
    // not of this shape)
    // a `::` filter on a region attribute (`a.text_iso="eng"`) holds for every token of a region or for none: it is
    // evaluated once per region (not for a nested or overlapping structure, where a token is in several)
    struct Filter {
        TokenQuery one;
        const StructuralAttr* regions = nullptr;
        std::vector<int8_t> memo;   // per region: -1 not yet evaluated, else the outcome
    };
    struct Side {
        const ConditionPtr* cond = nullptr;
        std::vector<Filter> filters;
        NameIndexMap names;
        std::vector<CorpusPos> listed;   // its candidates, when its conditions narrow it down
        bool has_list = false;
    };
    Side side[2];
    for (int i = 0; i < 2; ++i) {
        side[i].cond = &q.tokens[static_cast<size_t>(i)].conditions;
        if (!q.tokens[static_cast<size_t>(i)].name.empty()) side[i].names[q.tokens[static_cast<size_t>(i)].name] = 0;
    }
    for (const auto& f : q.global_region_filters) {
        if (f.positions || f.anchor_name.empty()) return std::nullopt;
        int i = f.anchor_name == q.tokens[0].name ? 0 : f.anchor_name == q.tokens[1].name ? 1 : -1;
        if (i < 0) return std::nullopt;
        Filter flt;
        flt.one.global_region_filters.push_back(f);
        const size_t us = f.region_attr.find('_');
        const std::string sn = us == std::string::npos ? std::string() : f.region_attr.substr(0, us);
        if (!sn.empty() && corpus_.has_structure(sn) && !corpus_.is_nested(sn) && !corpus_.is_overlapping(sn)) {
            flt.regions = &corpus_.structure(sn);
            flt.memo.assign(flt.regions->region_count(), -1);
        }
        side[i].filters.push_back(std::move(flt));
    }
    auto passes = [&](Side& s, CorpusPos p) {
        if (!check_conditions(p, *s.cond)) return false;
        for (Filter& f : s.filters) {
            const int64_t r = f.regions ? f.regions->find_region(p) : -1;
            if (r >= 0 && f.memo[static_cast<size_t>(r)] >= 0) {
                if (!f.memo[static_cast<size_t>(r)]) return false;
                continue;
            }
            Match m;
            m.positions = {p};
            m.span_ends = {p};
            const bool ok = match_passes_inline_region_filters(m, f.one, s.names);
            if (r >= 0) f.memo[static_cast<size_t>(r)] = ok;
            if (!ok) return false;
        }
        return true;
    };
    for (auto& s : side) {
        if (!*s.cond) continue;
        s.listed = resolve_conditions(*s.cond);
        s.has_list = true;
    }

    // the classes touched: those of the smaller listed side (its candidates that pass its filters), else all
    std::vector<LexiconId> touched;
    const int driver = side[0].has_list && (!side[1].has_list || side[0].listed.size() <= side[1].listed.size()) ? 0
                       : side[1].has_list ? 1 : -1;
    if (driver >= 0) {
        for (CorpusPos p : side[driver].listed)
            if (side[driver].filters.empty() || passes(side[driver], p))
                classes.for_each_class(p, [&](LexiconId c) { touched.push_back(c); });
        std::sort(touched.begin(), touched.end());
        touched.erase(std::unique(touched.begin(), touched.end()), touched.end());
    } else {
        const PositionalAttr& attr = corpus_.attr(decl->attr);
        const LexiconId n = corpus_.is_multivalue(decl->attr) ? attr.mv_lexicon().size() : attr.lexicon().size();
        for (LexiconId c = 0; c < n; ++c)
            if (classes.is_class(c)) touched.push_back(c);
    }

    // every member of the touched classes, marked; the count per class
    std::vector<Member> members;
    size_t total = 0;
    for (LexiconId c : touched) {
        const size_t first = members.size();
        size_t na = 0, nb = 0, nab = 0;
        classes.for_each_member(c, [&](CorpusPos p) {
            const bool a = passes(side[0], p), b = passes(side[1], p);
            if (!a && !b) return;
            na += a;
            nb += b;
            nab += a && b;
            members.push_back({p, c, a, b});
        });
        if (na == 0 || nb == 0) members.resize(first);   // no pair in this class
        else total += na * nb - nab;
    }
    // a multivalue attribute: a pair sharing k classes was counted k times
    std::unordered_map<CorpusPos, std::vector<size_t>> multi;   // position → its member rows, when in several classes
    {
        std::unordered_map<CorpusPos, size_t> seen;
        for (size_t i = 0; i < members.size(); ++i) {
            auto [it, fresh] = seen.try_emplace(members[i].pos, i);
            if (!fresh) {
                auto& rows = multi[members[i].pos];
                if (rows.empty()) rows.push_back(it->second);
                rows.push_back(i);
            }
        }
    }
    // the class of each member row: its range in `members` (rows are grouped by class, in corpus order)
    std::unordered_map<LexiconId, std::pair<size_t, size_t>> range_of;
    for (size_t i = 0; i < members.size();) {
        size_t j = i;
        while (j < members.size() && members[j].cls == members[i].cls) ++j;
        range_of[members[i].cls] = {i, j};
        i = j;
    }
    // partners of the member row r with a larger position, as (first token, second token) pairs
    auto pairs_from = [&](size_t r, std::vector<std::pair<CorpusPos, CorpusPos>>& out) {
        const Member& m = members[r];
        const auto [lo, hi] = range_of[m.cls];
        for (size_t k = lo; k < hi; ++k) {
            const Member& o = members[k];
            if (o.pos <= m.pos) continue;
            if (m.in_a && o.in_b) out.emplace_back(m.pos, o.pos);
            if (m.in_b && o.in_a) out.emplace_back(o.pos, m.pos);
        }
    };
    if (!multi.empty()) {
        size_t extra = 0;
        std::vector<std::pair<CorpusPos, CorpusPos>> ps;
        for (const auto& [pos, rows] : multi) {
            ps.clear();
            for (size_t r : rows) pairs_from(r, ps);
            std::sort(ps.begin(), ps.end());
            for (size_t k = 1; k < ps.size(); ++k) extra += ps[k] == ps[k - 1];
        }
        total -= extra;
    }

    // the page, in hit order as the generic expansion gives it: by the first token, then the second; the A members
    // in corpus order, each with the B members of its classes
    MatchSet result;
    result.num_tokens = 2;
    result.plan_path = "class_join";
    if (max_matches > 0) {
        std::vector<size_t> order;
        for (size_t i = 0; i < members.size(); ++i)
            if (members[i].in_a) order.push_back(i);
        std::sort(order.begin(), order.end(), [&](size_t x, size_t y) { return members[x].pos < members[y].pos; });
        std::vector<CorpusPos> bs;
        for (size_t i = 0; i < order.size() && result.matches.size() < max_matches;) {
            const CorpusPos a = members[order[i]].pos;
            bs.clear();
            for (; i < order.size() && members[order[i]].pos == a; ++i) {
                const auto [lo, hi] = range_of[members[order[i]].cls];
                for (size_t k = lo; k < hi; ++k)
                    if (members[k].in_b && members[k].pos != a) bs.push_back(members[k].pos);
            }
            std::sort(bs.begin(), bs.end());
            bs.erase(std::unique(bs.begin(), bs.end()), bs.end());
            for (CorpusPos b : bs) {
                if (result.matches.size() >= max_matches) break;
                Match m;
                m.positions = {a, b};
                m.span_ends = {a, b};
                result.matches.push_back(std::move(m));
            }
        }
    }
    result.total_count = total;
    result.total_exact = true;
    if (max_total_cap > 0 && result.total_count > max_total_cap) {
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    return result;
}

}  // namespace pando
