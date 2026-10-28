// Manatee's aligned corpora (TokenQuery::aligned_parts, from the Manatee CQL dialect): `A within ces: B`.
//
// In Manatee (corp/calign.cc map_aligned, query/frstream.cc RQinNode / RQnotInNode, read as a specification) B is
// run on the aligned corpus `ces`; the numbers of its alignment segments (ALIGNSTRUCT, mostly `s`) holding a hit of
// B give the same-numbered segments of the current corpus, and A is joined with those as with any `within` (its
// merge, see executor_manatee.cpp) or `!within` (a hit is dropped when it lies inside the first segment reaching
// its end). Clauses apply left to right, each to what the one before kept.
//
// In pando's single index the languages are parts of one corpus: `ces` is a value of a document attribute
// (`text_iso="ces"`, the first `text` / `doc` attribute that has it), the aligned segments are a class of the
// sentence alignment `s_tuid` (equivalence_classes.h), and A is the corpus outside `ces` (in Manatee it lives in
// the current corpus). A pattern of fixed length is in a segment exactly when it lies wholly inside one (the merge
// drops nothing else), so only the hits starting in those segments are listed; otherwise every hit of A is.

#include "query/executor.h"
#include "corpus/equivalence_classes.h"

#include <algorithm>
#include <stdexcept>

namespace pando {

namespace {

using Ranges = std::vector<std::pair<CorpusPos, CorpusPos>>;

// Manatee's within: the hits (by start, then end) kept by the merge with the ranges (sorted, disjoint)
std::vector<size_t> merge_in(const Ranges& hits, const std::vector<size_t>& order, const Ranges& ranges) {
    std::vector<size_t> kept;
    size_t i = 0, o = 0;
    while (i < order.size() && o < ranges.size()) {
        const auto [b, e] = hits[order[i]];
        const auto [rb, re] = ranges[o];
        if (b < rb) {
            i = static_cast<size_t>(std::partition_point(order.begin() + static_cast<long>(i), order.end(),
                                                         [&](size_t x) { return hits[x].first < rb; }) - order.begin());
            continue;
        }
        if (e > re) {
            o = static_cast<size_t>(std::partition_point(ranges.begin() + static_cast<long>(o), ranges.end(),
                                                         [&](const auto& r) { return r.second < e; }) - ranges.begin());
            continue;
        }
        kept.push_back(order[i++]);
    }
    return kept;
}

// Manatee's !within: a hit is dropped when it lies inside the first range reaching its end (ranges move forward)
std::vector<size_t> merge_not_in(const Ranges& hits, const std::vector<size_t>& order, const Ranges& ranges) {
    std::vector<size_t> kept;
    size_t o = 0;
    for (size_t x : order) {
        const auto [b, e] = hits[x];
        while (o < ranges.size() && ranges[o].second < e) ++o;
        if (o < ranges.size() && b >= ranges[o].first && e <= ranges[o].second) continue;
        kept.push_back(x);
    }
    return kept;
}

bool fixed_length(const TokenQuery& q) {
    if (!q.union_alternatives.empty()) return false;
    for (const auto& t : q.tokens)
        if (!t.is_anchor() && t.min_repeat != t.max_repeat) return false;
    return true;
}

}  // namespace

// `ces`: the document attribute that has this value (`text_iso`, `text_lang`, …), as a region filter on a hit
GlobalRegionFilter QueryExecutor::aligned_corpus_filter(const std::string& name, CompOp op) const {
    for (const char* doc : {"text", "doc"}) {
        if (!corpus_.has_structure(doc)) continue;
        const StructuralAttr& sa = corpus_.structure(doc);
        for (const auto& key : sa.region_attr_names()) {
            if (!sa.has_region_value_reverse(key) || sa.region_attr_lex_lookup(key, name) == UNKNOWN_LEX) continue;
            GlobalRegionFilter f;
            f.region_attr = key.rfind(std::string(doc) + "_", 0) == 0 ? key : std::string(doc) + "_" + key;
            f.op = op;
            f.value = name;
            return f;
        }
    }
    throw std::runtime_error("within " + name + ": …: this corpus has no aligned corpus '" + name +
                             "' (no text or doc attribute has the value '" + name + "')");
}

std::optional<MatchSet> QueryExecutor::execute_aligned(const TokenQuery& q, size_t max_matches, bool count_total,
                                                       size_t max_total_cap, size_t sample_size,
                                                       const std::vector<std::string>* aggregate_by_fields) {
    if (q.aligned_parts.empty()) return std::nullopt;
    if (sample_size > 0 || (aggregate_by_fields && !aggregate_by_fields->empty()))
        throw std::runtime_error("within an aligned corpus (…: …) gives hits; sampling or counting them by a field "
                                 "is done on the hits");
    // the alignment segments: the sentence alignment's classes
    EquivalenceClasses classes;
    if (!classes.open(corpus_, "s_tuid") || !classes.regions())
        throw std::runtime_error("within an aligned corpus (…: …) needs the sentence alignment s_tuid");

    // A: outside each aligned corpus named
    TokenQuery a = q;
    a.aligned_parts.clear();
    for (const auto& part : q.aligned_parts)
        a.global_region_filters.push_back(aligned_corpus_filter(part.corpus, CompOp::NEQ));

    // per clause, its segments: those aligned with a segment of the corpus holding a hit of B
    std::vector<Ranges> segments;
    for (const auto& part : q.aligned_parts) {
        TokenQuery b = *part.query;
        b.global_region_filters.push_back(aligned_corpus_filter(part.corpus, CompOp::EQ));
        MatchSet bh = execute(b, 0, false, 0, 0, 0, 1, nullptr, true);
        std::vector<LexiconId> cls;
        for (const auto& m : bh.matches) classes.for_each_class(m.first_pos(), [&](LexiconId c) { cls.push_back(c); });
        std::sort(cls.begin(), cls.end());
        cls.erase(std::unique(cls.begin(), cls.end()), cls.end());
        // the aligned corpus's own segments are not the current corpus's
        TokenQuery own_lang;
        own_lang.global_region_filters.push_back(aligned_corpus_filter(part.corpus, CompOp::EQ));
        const NameIndexMap no_names;
        Ranges r;
        for (LexiconId c : cls)
            classes.for_each_region(c, [&](const Region& rg) {
                Match m;
                m.positions = {rg.start};
                m.span_ends = {rg.start};
                if (!match_passes_inline_region_filters(m, own_lang, no_names)) r.emplace_back(rg.start, rg.end);
            });
        std::sort(r.begin(), r.end());
        r.erase(std::unique(r.begin(), r.end()), r.end());
        segments.push_back(std::move(r));
    }

    MatchSet result;
    result.num_tokens = q.tokens.size();
    result.plan_path = "manatee_aligned";
    result.total_exact = true;

    // A's hits: for a fixed-length A with only `within` clauses, those starting in the first clause's segments
    const bool restrict = fixed_length(a) && !q.aligned_parts[0].negated;
    TokenQuery listed = a;
    if (restrict) {
        auto iv = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
        for (const auto& [s, e] : segments[0]) {
            if (!iv->empty() && s <= iv->back().second + 1) iv->back().second = std::max<int64_t>(iv->back().second, e);
            else iv->push_back({s, e});
        }
        GlobalRegionFilter starts;
        starts.positions = std::move(iv);
        listed.global_region_filters.push_back(std::move(starts));
    }
    MatchSet ah = execute(listed, 0, false, 0, 0, 0, 1, nullptr, true);
    Ranges hits;
    hits.reserve(ah.matches.size());
    for (const auto& m : ah.matches) hits.emplace_back(m.first_pos(), m.last_pos());
    std::vector<size_t> order(hits.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::stable_sort(order.begin(), order.end(), [&](size_t x, size_t y) { return hits[x] < hits[y]; });
    order.erase(std::unique(order.begin(), order.end(), [&](size_t x, size_t y) { return hits[x] == hits[y]; }),
                order.end());
    for (size_t k = 0; k < q.aligned_parts.size(); ++k) {
        order = q.aligned_parts[k].negated ? merge_not_in(hits, order, segments[k]) : merge_in(hits, order, segments[k]);
        std::sort(order.begin(), order.end(), [&](size_t x, size_t y) { return hits[x] < hits[y]; });
    }

    result.total_count = order.size();
    std::vector<Match> page;
    page.reserve(order.size());
    for (size_t k : order) page.push_back(std::move(ah.matches[k]));
    sort_matches_by_position(page);
    if (max_matches > 0 && page.size() > max_matches) page.resize(max_matches);
    result.matches = std::move(page);
    if (!count_total && max_matches > 0) result.total_exact = result.total_count <= max_matches;
    if (max_total_cap > 0 && result.total_count > max_total_cap) {
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    return result;
}

}  // namespace pando
