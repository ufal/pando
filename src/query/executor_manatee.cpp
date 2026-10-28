// What the Manatee CQL dialect asks of the executor beyond pando's own queries, with Manatee's answers:
//
// * Unions (TokenQuery::union_alternatives: `A | B` between sequences, repeated groups `(A B)+` expanded): a hit is
//   a (start, end) pair, counted once whichever alternative finds it, as Manatee's union does (query/frstream.cc,
//   RQUnionNode, read as a specification). Alternatives whose lengths cannot coincide cannot find the same pair:
//   their totals add up (each through pando's fast paths) and the page is a merge; otherwise every hit is listed.
//
// * Manatee's `within` (TokenQuery::manatee_within): Manatee joins the hits with the regions in one forward pass
//   (RQinNode::locate): with the current hit and the current region,
//     - a hit starting before the region: skip to the first hit starting at the region or later;
//     - a hit ending after the region: move to the first region ending at the hit's end or later;
//     - else the hit is in the region: report it, take the next hit.
//   The hits come by start, then end. Once a hit runs past its region, the regions move on and every later-starting
//   hit of that region is skipped, also one wholly inside it; pando's `within` keeps those. Only patterns of varying
//   length show the difference (dev/CQL-MANATEE-DIALECT.md: [lemma="be"] []* [upos="VERB"] within <s/>, 38 576 hits
//   in Manatee, 48 345 in pando's own `within`; this pass gives 38 576).

#include "query/executor.h"
#include "query/executor_aggregate_internal.h"

#include <algorithm>
#include <cstdint>
#include <set>
#include <utility>

namespace pando {

namespace {

// the number of tokens a sequence covers: [shortest, longest]
std::pair<long long, long long> length_range(const std::vector<QueryToken>& tokens) {
    long long lo = 0, hi = 0;
    for (const auto& t : tokens) {
        if (t.is_anchor()) continue;
        lo += t.min_repeat;
        hi += t.max_repeat;
    }
    return {lo, hi};
}

}  // namespace

std::optional<MatchSet> QueryExecutor::execute_manatee(const TokenQuery& q, size_t max_matches, bool count_total,
                                                       size_t max_total_cap, size_t sample_size,
                                                       const std::vector<std::string>* aggregate_by_fields) {
    const bool merge_within = q.manatee_within && !q.within.empty() && !q.not_within && corpus_.has_structure(q.within)
                              && !corpus_.is_nested(q.within) && !corpus_.is_overlapping(q.within);
    if (!merge_within && q.union_alternatives.empty()) return std::nullopt;
    if (sample_size > 0) return std::nullopt;

    std::vector<const std::vector<QueryToken>*> alts{&q.tokens};
    for (const auto& a : q.union_alternatives)
        if (!a.empty()) alts.push_back(&a);
    TokenQuery base = q;
    base.union_alternatives.clear();
    base.manatee_within = false;
    // Manatee's within: the hits are taken anywhere, the regions those its `<s attr="v"/>` conditions hold for (the
    // region filters without a token name come from them)
    TokenQuery region_q;
    if (merge_within) {
        base.within.clear();
        base.global_region_filters.clear();
        for (const auto& f : q.global_region_filters)
            (f.anchor_name.empty() ? region_q : base).global_region_filters.push_back(f);
    }
    auto alternative = [&](size_t i) {
        TokenQuery a = base;
        a.tokens = *alts[i];
        return a;
    };

    MatchSet result;
    result.num_tokens = q.tokens.size();
    result.plan_path = merge_within ? "manatee_within" : "manatee_union";
    result.total_exact = true;
    const bool aggregating = aggregate_by_fields && !aggregate_by_fields->empty();

    // alternatives that cannot find the same (start, end): their totals add up, the page is a merge
    bool disjoint = !merge_within && !aggregating;
    std::vector<std::pair<long long, long long>> ranges;
    for (const auto* a : alts) ranges.push_back(length_range(*a));
    for (size_t i = 0; disjoint && i < ranges.size(); ++i)
        for (size_t j = i + 1; j < ranges.size(); ++j)
            if (ranges[i].first <= ranges[j].second && ranges[j].first <= ranges[i].second) disjoint = false;
    if (disjoint) {
        std::vector<Match> page;
        size_t total = 0;
        bool exact = true;
        for (size_t i = 0; i < alts.size(); ++i) {
            MatchSet ms = execute(alternative(i), max_matches, count_total, 0, 0, 0, 1, nullptr, true);
            total += count_total ? ms.total_count : ms.matches.size();
            exact = exact && (count_total ? ms.total_exact : true);
            for (auto& m : ms.matches) page.push_back(std::move(m));
        }
        sort_matches_by_position(page);
        if (max_matches > 0 && page.size() > max_matches) page.resize(max_matches);
        result.matches = std::move(page);
        result.total_count = count_total ? total : result.matches.size();
        result.total_exact = count_total ? exact : false;
        if (max_total_cap > 0 && result.total_count > max_total_cap) {
            result.total_count = max_total_cap;
            result.total_exact = false;
        }
        return result;
    }

    // Manatee's within for one sequence over plain regions (`within <s/>`), without listing every hit: a region keeps
    // its hits up to (by start, then end) its first hit running past its end; the regions that hit reaches into
    // are skipped. Such a hit starts within the pattern's longest length of the region's end, and so do the hits it
    // costs (later in the region, or in the short regions skipped), so only the hits starting in each region's tail
    // are listed: pando's own `within` total (its fast paths) less the hits the merge drops.
    if (merge_within && alts.size() == 1 && region_q.global_region_filters.empty() && !aggregating) {
        long long longest = 0;
        for (const auto& t : q.tokens)
            if (!t.is_anchor()) longest += t.max_repeat;
        const StructuralAttr& sa = corpus_.structure(q.within);
        std::vector<std::pair<CorpusPos, CorpusPos>> regions;
        auto tails = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
        for (size_t r = 0; r < sa.region_count(); ++r) {
            const Region rg = sa.get(r);
            if (rg.start > rg.end) continue;
            regions.emplace_back(rg.start, rg.end);
            tails->push_back({std::max<int64_t>(rg.start, rg.end - std::max<long long>(longest, 1) + 1), rg.end});
        }
        TokenQuery tail_q = base;
        GlobalRegionFilter starts;
        starts.positions = tails;
        tail_q.global_region_filters.push_back(std::move(starts));
        MatchSet tail = execute(tail_q, 0, false, 0, 0, 0, 1, nullptr, true);
        std::vector<std::pair<CorpusPos, CorpusPos>> th;   // (start, end), by start, then end
        th.reserve(tail.matches.size());
        for (const auto& m : tail.matches) th.emplace_back(m.first_pos(), m.last_pos());
        std::sort(th.begin(), th.end());
        th.erase(std::unique(th.begin(), th.end()), th.end());
        // the merge over the tail hits: a hit of a region it lies in is dropped when it comes after the region's
        // first crossing hit, or lies in a region skipped
        std::set<std::pair<CorpusPos, CorpusPos>> dropped;
        auto region_of = [&](CorpusPos p) {   // the region holding p, or -1
            auto it = std::upper_bound(regions.begin(), regions.end(), std::make_pair(p, CorpusPos(INT64_MAX)));
            if (it == regions.begin()) return int64_t{-1};
            --it;
            return p <= it->second ? static_cast<int64_t>(it - regions.begin()) : int64_t{-1};
        };
        size_t k = 0;
        while (k < th.size()) {
            const int64_t r = region_of(th[k].first);
            if (r < 0) { ++k; continue; }
            const CorpusPos rend = regions[static_cast<size_t>(r)].second;
            // this region's tail hits, up to its first crossing hit
            size_t j = k;
            while (j < th.size() && th[j].first <= rend && th[j].second <= rend) ++j;
            if (j >= th.size() || th[j].first > rend) { k = j; continue; }   // no crossing hit: all kept
            const CorpusPos cross_end = th[j].second;
            // the merge resumes at the first region ending at cross_end or later; every hit starting before it is
            // skipped: those lying in a region are dropped
            auto next_region = std::partition_point(regions.begin() + r, regions.end(),
                                                    [&](const auto& rg) { return rg.second < cross_end; });
            const CorpusPos resume = next_region == regions.end() ? INT64_MAX : next_region->first;
            for (k = j + 1; k < th.size() && th[k].first < resume; ++k) {
                const int64_t rk = region_of(th[k].first);
                if (rk >= 0 && th[k].second <= regions[static_cast<size_t>(rk)].second) dropped.insert(th[k]);
            }
        }
        // pando's own within: its total, and the page with the dropped hits taken out
        TokenQuery own = q;
        own.manatee_within = false;
        const size_t want = max_matches == 0 ? 0 : max_matches + dropped.size();
        MatchSet ms = execute(own, want, count_total, 0, 0, 0, 1, nullptr, true);
        std::vector<Match> page;
        for (auto& m : ms.matches)
            if (!dropped.count({m.first_pos(), m.last_pos()})) page.push_back(std::move(m));
        if (max_matches > 0 && page.size() > max_matches) page.resize(max_matches);
        result.matches = std::move(page);
        result.total_count = count_total ? ms.total_count - dropped.size() : result.matches.size();
        result.total_exact = count_total ? ms.total_exact : false;
        if (max_total_cap > 0 && result.total_count > max_total_cap) {
            result.total_count = max_total_cap;
            result.total_exact = false;
        }
        return result;
    }

    // every hit of every alternative, each (start, end) once, by start, then end
    std::vector<Match> hits;
    for (size_t i = 0; i < alts.size(); ++i) {
        MatchSet ms = execute(alternative(i), 0, false, 0, 0, 0, 1, nullptr, true);
        for (auto& m : ms.matches) hits.push_back(std::move(m));
    }
    std::vector<size_t> order(hits.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    auto beg = [&](size_t i) { return hits[i].first_pos(); };
    auto end = [&](size_t i) { return hits[i].last_pos(); };
    std::stable_sort(order.begin(), order.end(), [&](size_t x, size_t y) {
        return beg(x) != beg(y) ? beg(x) < beg(y) : end(x) < end(y);
    });
    order.erase(std::unique(order.begin(), order.end(),
                            [&](size_t x, size_t y) { return beg(x) == beg(y) && end(x) == end(y); }),
                order.end());

    std::vector<size_t> kept;
    if (!merge_within) {
        kept = std::move(order);
    } else {
        const StructuralAttr& sa = corpus_.structure(q.within);
        std::vector<std::pair<CorpusPos, CorpusPos>> regions;
        const NameIndexMap no_names;
        for (size_t r = 0; r < sa.region_count(); ++r) {
            const Region rg = sa.get(r);
            if (rg.start > rg.end) continue;
            if (!region_q.global_region_filters.empty()) {
                Match m;
                m.positions = {rg.start};
                m.span_ends = {rg.start};
                if (!match_passes_inline_region_filters(m, region_q, no_names)) continue;
            }
            regions.emplace_back(rg.start, rg.end);
        }
        size_t i = 0, o = 0;
        while (i < order.size() && o < regions.size()) {
            const CorpusPos b = beg(order[i]), e = end(order[i]);
            const auto [ob, oe] = regions[o];
            if (b < ob) {           // the first hit starting at the region or later
                i = static_cast<size_t>(std::partition_point(order.begin() + static_cast<long>(i), order.end(),
                                                             [&](size_t x) { return beg(x) < ob; }) - order.begin());
                continue;
            }
            if (e > oe) {           // the first region ending at the hit's end or later
                o = static_cast<size_t>(std::partition_point(regions.begin() + static_cast<long>(o), regions.end(),
                                                             [&](const auto& rg) { return rg.second < e; }) -
                                        regions.begin());
                continue;
            }
            kept.push_back(order[i]);
            ++i;
        }
    }

    result.total_count = kept.size();
    if (aggregating) {
        auto agg = std::make_shared<AggregateBucketData>();
        if (!build_aggregate_plan(corpus_, *aggregate_by_fields, *agg)) return std::nullopt;
        const NameIndexMap names = build_name_map(q);
        std::vector<int64_t> key;
        for (size_t k : kept) {
            if (!fill_aggregate_key(*agg, corpus_, hits[k], names, key)) continue;
            ++agg->total_hits;
            ++agg->counts[key];
        }
        result.total_count = agg->total_hits;
        result.aggregate_buckets = std::move(agg);
        return result;
    }
    // the page in pando's hit order
    std::vector<Match> page;
    page.reserve(kept.size());
    for (size_t k : kept) page.push_back(std::move(hits[k]));
    sort_matches_by_position(page);
    if (max_matches > 0 && page.size() > max_matches) page.resize(max_matches);
    result.matches = std::move(page);
    if (max_total_cap > 0 && result.total_count > max_total_cap) {
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    return result;
}

}  // namespace pando
