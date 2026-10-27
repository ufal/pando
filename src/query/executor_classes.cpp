// The class join for an equivalence layer (dev/PROPOSAL-RELATION-LAYERS.md, dev/LINK-LAYERS.md fast path 4):
// `a:[A] =:X b:[B]`, the pairs of distinct tokens that share a class of X, e.g. a word and its translations over
// `tuid`. The generic expansion finds the class mates of each A token one by one; here the classes are walked once
// each. Every class touched by the more selective side is read once (its members, each marked for A and B), so a
// count is Σ_c |A∩c|·|B∩c| − |A∩B∩c| without listing pairs, and a page is the pairs in hit order (by the first
// token, then the second), read off the members of the touched classes.

#include "query/executor.h"
#include "corpus/equivalence_classes.h"
#include "query/executor_aggregate_internal.h"

#include <algorithm>
#include <iterator>
#include <optional>
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
    if (sample_size > 0 || !fastpaths_enabled()) return std::nullopt;
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
    // a side's token conditions: membership in its resolved candidates when it has them (a region attribute as a
    // token condition, `[text_iso="eng"]`, is costly to test token by token), else tested at the token
    auto passes = [&](Side& s, CorpusPos p) {
        if (s.has_list ? !std::binary_search(s.listed.begin(), s.listed.end(), p) : !check_conditions(p, *s.cond))
            return false;
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
    // a side's candidates: from its token conditions, else from a region filter (the tokens of the regions it
    // holds for, `b.text_iso="ces"`); a side with neither has no list
    for (auto& s : side) {
        if (*s.cond) {
            s.listed = resolve_conditions(*s.cond);
            s.has_list = true;
            continue;
        }
        for (Filter& f : s.filters) {
            if (!f.regions) continue;
            for (size_t r = 0; r < f.regions->region_count(); ++r) {
                const Region rg = f.regions->get(r);
                if (rg.start > rg.end) continue;
                Match m;
                m.positions = {rg.start};
                m.span_ends = {rg.start};
                f.memo[r] = match_passes_inline_region_filters(m, f.one, s.names);
                if (f.memo[r])
                    for (CorpusPos p = rg.start; p <= rg.end; ++p) s.listed.push_back(p);
            }
            s.has_list = true;
            break;
        }
    }

    // `count by`: each field reads one of the two tokens (`a.lemma`, `b.text_iso`); a field on neither (the match
    // start) is not of this shape
    const bool aggregating = aggregate_by_fields && !aggregate_by_fields->empty();
    std::shared_ptr<AggregateBucketData> agg;
    std::vector<int> col_side;
    if (aggregating) {
        agg = std::make_shared<AggregateBucketData>();
        if (!build_aggregate_plan(corpus_, *aggregate_by_fields, *agg)) return std::nullopt;
        for (const auto& col : agg->columns) {
            if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding || col.named_anchor.empty())
                return std::nullopt;
            const int i = col.named_anchor == q.tokens[0].name ? 0 : col.named_anchor == q.tokens[1].name ? 1 : -1;
            if (i < 0) return std::nullopt;
            col_side.push_back(i);
        }
    }

    // the members, grouped by class and in corpus order within a class, each marked A / B, read whichever way
    // touches fewer tokens: every member of the classes touched by the smaller listed side (all classes when neither
    // is listed), or, both sides listed, the two lists themselves (a class may hold many more tokens, other
    // languages, than either side wants)
    std::vector<Member> members;
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
    size_t scan_cost = 0;
    for (LexiconId c : touched) scan_cost += classes.size(c);
    if (side[0].has_list && side[1].has_list && side[0].listed.size() + side[1].listed.size() < scan_cost) {
        std::vector<CorpusPos> both;
        both.reserve(side[0].listed.size() + side[1].listed.size());
        std::set_union(side[0].listed.begin(), side[0].listed.end(), side[1].listed.begin(), side[1].listed.end(),
                       std::back_inserter(both));
        for (CorpusPos p : both) {
            const bool a = passes(side[0], p), b = passes(side[1], p);
            if (a || b) classes.for_each_class(p, [&](LexiconId c) { members.push_back({p, c, a, b}); });
        }
        std::stable_sort(members.begin(), members.end(),
                         [](const Member& x, const Member& y) { return x.cls < y.cls; });
    } else {
        for (LexiconId c : touched)
            classes.for_each_member(c, [&](CorpusPos p) {
                const bool a = passes(side[0], p), b = passes(side[1], p);
                if (a || b) members.push_back({p, c, a, b});
            });
    }
    // the count per class; a class without both an A and a B member holds no pair and is dropped
    size_t total = 0;
    {
        size_t out = 0;
        for (size_t i = 0; i < members.size();) {
            size_t j = i, na = 0, nb = 0, nab = 0;
            for (; j < members.size() && members[j].cls == members[i].cls; ++j) {
                na += members[j].in_a;
                nb += members[j].in_b;
                nab += members[j].in_a && members[j].in_b;
            }
            if (na > 0 && nb > 0) {
                total += na * nb - nab;
                for (size_t k = i; k < j; ++k) members[out++] = members[k];
            }
            i = j;
        }
        members.resize(out);
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

    // `count by`: per class, the A members counted by their A fields and the B members by their B fields; the
    // buckets get the cross products (no pair listed), less a token paired with itself and the repeats of a pair
    // sharing several classes
    if (aggregating) {
        NameIndexMap nm;
        nm[q.tokens[0].name] = 0;
        nm[q.tokens[1].name] = 1;
        const size_t ncol = col_side.size();
        // the key of a token on every field (both sides read at it); false when a field has no value there. A plain
        // positional field is its lexicon id; a region field holds for a whole region, so it is read once per region
        Match probe;
        probe.positions = {0, 0};
        probe.span_ends = {0, 0};
        std::vector<int64_t> full;
        auto key_full = [&](CorpusPos p, std::vector<int64_t>& k) {
            probe.positions[0] = probe.positions[1] = probe.span_ends[0] = probe.span_ends[1] = p;
            return fill_aggregate_key(*agg, corpus_, probe, nm, k);
        };
        using Col = AggregateBucketData::Column;
        bool simple = true;
        std::vector<std::vector<int64_t>> region_memo(ncol);   // per region: kUnread, kNoValue, or the key
        constexpr int64_t kUnread = INT64_MIN, kNoValue = INT64_MIN + 1;
        for (size_t i = 0; i < ncol; ++i) {
            const Col& c = agg->columns[i];
            if (c.kind == Col::Kind::Positional && c.date_transform == Col::DateTransform::None) continue;
            if (c.kind == Col::Kind::Region && c.sa && !c.sa->has_parent_region_id()) {
                region_memo[i].assign(c.sa->region_count(), kUnread);
                continue;
            }
            simple = false;
        }
        auto key_at = [&](CorpusPos p, std::vector<int64_t>& k) {
            if (!simple) return key_full(p, k);
            k.resize(ncol);
            for (size_t i = 0; i < ncol; ++i) {
                const Col& c = agg->columns[i];
                if (region_memo[i].empty()) {
                    k[i] = static_cast<int64_t>(c.pa->id_at(p));
                    continue;
                }
                const int64_t r = c.sa->find_region(p);
                if (r < 0) {
                    if (!key_full(p, full)) return false;
                    k[i] = full[i];
                    continue;
                }
                int64_t& v = region_memo[i][static_cast<size_t>(r)];
                if (v == kUnread) v = key_full(p, full) ? full[i] : kNoValue;
                if (v == kNoValue) return false;
                k[i] = v;
            }
            return true;
        };
        std::vector<int64_t> ka, kb;
        auto pair_key = [&](CorpusPos a, CorpusPos b, std::vector<int64_t>& k) {
            if (!key_at(a, ka) || !key_at(b, kb)) return false;
            k.resize(ncol);
            for (size_t i = 0; i < ncol; ++i) k[i] = col_side[i] == 0 ? ka[i] : kb[i];
            return true;
        };
        auto add = [&](const std::vector<int64_t>& k, int64_t n) {
            if (n == 0) return;
            auto& c = agg->counts[k];
            c = static_cast<size_t>(static_cast<int64_t>(c) + n);
            agg->total_hits = static_cast<size_t>(static_cast<int64_t>(agg->total_hits) + n);
        };
        using Hist = std::unordered_map<std::vector<int64_t>, int64_t, AggregateBucketData::VecHash,
                                        AggregateBucketData::VecEq>;
        Hist ha, hb;
        std::vector<int64_t> key, half(ncol), pk(ncol);
        for (const auto& [cls, rng] : range_of) {
            ha.clear();
            hb.clear();
            for (size_t k = rng.first; k < rng.second; ++k) {
                const Member& m = members[k];
                if (!key_at(m.pos, key)) continue;
                for (int s = 0; s < 2; ++s) {
                    if (!(s == 0 ? m.in_a : m.in_b)) continue;
                    for (size_t i = 0; i < ncol; ++i) half[i] = col_side[i] == s ? key[i] : 0;
                    ++(s == 0 ? ha : hb)[half];
                }
                if (m.in_a && m.in_b) add(key, -1);   // a token with itself: both sides read at it
            }
            for (const auto& [ha_k, na] : ha)
                for (const auto& [hb_k, nb] : hb) {
                    for (size_t i = 0; i < ncol; ++i) pk[i] = col_side[i] == 0 ? ha_k[i] : hb_k[i];
                    add(pk, na * nb);
                }
        }
        std::vector<std::pair<CorpusPos, CorpusPos>> ps;
        for (const auto& [pos, rows] : multi) {
            ps.clear();
            for (size_t r : rows) pairs_from(r, ps);
            std::sort(ps.begin(), ps.end());
            for (size_t k = 1; k < ps.size(); ++k)
                if (ps[k] == ps[k - 1] && pair_key(ps[k].first, ps[k].second, pk)) add(pk, -1);
        }
        for (auto it = agg->counts.begin(); it != agg->counts.end();)
            it = it->second == 0 ? agg->counts.erase(it) : std::next(it);
        MatchSet result;
        result.num_tokens = 2;
        result.plan_path = "class_join";
        result.total_count = agg->total_hits;
        result.total_exact = true;
        result.aggregate_buckets = std::move(agg);
        return result;
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
