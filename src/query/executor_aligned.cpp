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

// one token and its conditions, nothing else (a hit of it can be tested at a position)
bool plain_token(const TokenQuery& q) {
    return q.tokens.size() == 1 && !q.tokens[0].has_repetition() && !q.tokens[0].is_anchor() && q.relations.empty() &&
           q.within.empty() && !q.within_having && q.more_withins.empty() && q.union_alternatives.empty() &&
           q.containing_clauses.empty() && q.global_region_filters.empty() && q.global_alignment_filters.empty() &&
           q.global_function_filters.empty() && q.position_orders.empty();
}

bool fixed_length(const TokenQuery& q) {
    if (!q.union_alternatives.empty()) return false;
    for (const auto& t : q.tokens)
        if (!t.is_anchor() && t.min_repeat != t.max_repeat) return false;
    return true;
}

// the alignment segments (Manatee's ALIGNSTRUCT): the classes of the sentence alignment, `s_tuid`, else
// `seg_tuid`, else another region's `*_tuid` below the document
bool open_alignment(const Corpus& corpus, EquivalenceClasses& classes) {
    std::vector<std::string> names{"s_tuid", "seg_tuid"};
    for (const auto& r : corpus.region_attr_names())
        if (is_alignment_id_attr(r) && r.rfind("text_", 0) != 0 && r.rfind("doc_", 0) != 0) names.push_back(r);
    for (const auto& n : names)
        if (classes.open(corpus, n) && classes.regions()) return true;
    return false;
}

// A of `A within c: B …`: outside each aligned corpus named, one token
bool one_token(const TokenQuery& a) {
    return a.tokens.size() == 1 && !a.tokens[0].has_repetition() && !a.tokens[0].is_anchor() &&
           a.union_alternatives.empty();
}

// Keeps a query's progress (and its cancel) out of the inner queries it runs: they would report their own
// counts and positions as the query's
struct InnerProgress {
    ExecProgress inner;
    ExecProgress*& slot;
    ExecProgress* outer;
    explicit InnerProgress(ExecProgress*& p) : slot(p), outer(p) {
        inner.parent = outer;
        if (outer) slot = &inner;
    }
    ~InnerProgress() { slot = outer; }
};

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
    EquivalenceClasses classes;
    if (!open_alignment(corpus_, classes))
        throw std::runtime_error("within an aligned corpus (…: …) needs a sentence alignment (s_tuid, seg_tuid, …)");
    // a background count: an estimate first (sampled_total); the inner queries below do not report as this one
    if (count_total && progress_ && progress_->estimate.load() == 0)
        if (TokenQuery lq; lazy_aligned_query(q, lq)) progress_->estimate.store(sampled_total(lq));
    InnerProgress quiet(progress_);

    // A: outside each aligned corpus named
    TokenQuery a = q;
    a.aligned_parts.clear();
    for (const auto& part : q.aligned_parts)
        a.global_region_filters.push_back(aligned_corpus_filter(part.corpus, CompOp::NEQ));

    // a page without a total, A and every B one token: each A hit is tested against its own segments
    // (lazy_aligned_query), so the page does not wait for every hit of B and A
    // (only when a B is frequent: for rare ones their segments are quickly computed, below)
    const bool frequent_b = std::any_of(q.aligned_parts.begin(), q.aligned_parts.end(), [&](const AlignedPart& part) {
        if (!part.query || !plain_token(*part.query)) return false;
        const ConditionPtr& c = part.query->tokens[0].conditions;
        if (c) compile_conditions(c);
        return estimate_cardinality(c) >= lazy_class_min();
    });
    if (TokenQuery lazy; !count_total && max_matches > 0 && frequent_b && lazy_aligned_query(q, lazy)) {
        MatchSet ms = execute(lazy, max_matches, false, 0, 0, 0, 1, nullptr, true);
        ms.plan_path = "manatee_aligned";
        ms.total_exact = false;
        return ms;
    }

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
        // as disjoint ranges (nested sentences merged into the outer one)
        std::sort(r.begin(), r.end());
        Ranges merged;
        for (const auto& [rs, re] : r) {
            if (!merged.empty() && rs <= merged.back().second) merged.back().second = std::max(merged.back().second, re);
            else merged.emplace_back(rs, re);
        }
        segments.push_back(std::move(merged));
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

namespace pando {

// The per-hit form of an aligned query, as a page reads it: `within s_tuid having …` (and further within clauses)
// through the lazy rewrites; Manatee's `A within c: B …` with A and every B one token as A outside the aligned
// corpora, with a test per clause: a B token in one of the hit's segments in corpus c (none for `!within`).
bool QueryExecutor::lazy_aligned_query(const TokenQuery& q, TokenQuery& out) {
    if (q.aligned_parts.empty()) {
        bool any = false;
        TokenQuery cur = q;
        for (TokenQuery next;; cur = std::move(next)) {
            if (rewrite_more_withins(cur, next, true, true) || rewrite_within_class(cur, next, true, true)) {
                any = true;
                continue;
            }
            break;
        }
        if (any) out = std::move(cur);
        return any;
    }
    TokenQuery a = q;
    a.aligned_parts.clear();
    for (const auto& part : q.aligned_parts)
        a.global_region_filters.push_back(aligned_corpus_filter(part.corpus, CompOp::NEQ));
    if (!one_token(a) || !std::all_of(q.aligned_parts.begin(), q.aligned_parts.end(), [](const AlignedPart& part) {
            return part.query && plain_token(*part.query);
        }))
        return false;
    EquivalenceClasses classes;
    if (!open_alignment(corpus_, classes)) return false;
    for (const auto& part : q.aligned_parts) {
        const ConditionPtr cond = part.query->tokens[0].conditions;
        if (cond) compile_conditions(cond);
        TokenQuery lang;
        lang.global_region_filters.push_back(aligned_corpus_filter(part.corpus, CompOp::EQ));
        const bool negated = part.negated;
        auto test = [this, classes, cond, lang, negated](int64_t p) {
            const NameIndexMap no_names;
            Match m;
            m.positions = {0};
            m.span_ends = {0};
            bool in = false;
            classes.for_each_class(static_cast<CorpusPos>(p), [&](LexiconId c) {
                classes.for_each_region(c, [&](const Region& rg) {
                    if (in) return;
                    m.positions[0] = m.span_ends[0] = rg.start;
                    if (!match_passes_inline_region_filters(m, lang, no_names)) return;   // not the aligned corpus
                    for (CorpusPos x = rg.start; x <= rg.end && !in; ++x) in = check_conditions(x, cond);
                });
            });
            return in != negated;
        };
        GlobalRegionFilter gf;
        gf.test = std::make_shared<const std::function<bool(int64_t)>>(std::move(test));
        a.global_region_filters.push_back(std::move(gf));
    }
    out = std::move(a);
    return true;
}

size_t QueryExecutor::sampled_total(const TokenQuery& q) {
    constexpr size_t kWant = 4000;     // hits tested, about
    constexpr int64_t kWindows = 1000;
    InnerProgress quiet(progress_);
    TokenQuery base = q;
    std::vector<std::shared_ptr<const std::function<bool(int64_t)>>> tests;
    base.global_region_filters.clear();
    for (const auto& f : q.global_region_filters)
        if (f.test) tests.push_back(f.test);
        else base.global_region_filters.push_back(f);
    const MatchSet all = execute(base, 1, true, 0, 0, 0, 1, nullptr, true);
    const size_t n = all.total_count;
    if (n == 0 || tests.empty()) return n;
    // windows spread evenly over the corpus, together about kWant of base's hits
    const int64_t size = corpus_.size();
    const double share = std::min(1.0, static_cast<double>(kWant) / static_cast<double>(n));
    TokenQuery win = base;
    if (share < 1.0) {
        const int64_t step = std::max<int64_t>(1, size / kWindows);
        const int64_t len = std::max<int64_t>(1, static_cast<int64_t>(share * static_cast<double>(step)));
        auto iv = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
        for (int64_t s = 0; s < size; s += step) iv->push_back({s, std::min(size, s + len) - 1});
        GlobalRegionFilter gf;
        gf.positions = std::move(iv);
        win.global_region_filters.push_back(std::move(gf));
    }
    const MatchSet sample = execute(win, 0, false, 0, 0, 0, 1, nullptr, true);
    if (sample.matches.empty()) return 0;
    size_t pass = 0;
    for (const auto& m : sample.matches)
        if (std::all_of(tests.begin(), tests.end(), [&](const auto& t) { return (*t)(m.first_pos()); })) ++pass;
    if (share >= 1.0 && sample.matches.size() == n) return pass;   // every hit tested: the total itself
    return static_cast<size_t>(static_cast<double>(n) * static_cast<double>(pass) /
                               static_cast<double>(sample.matches.size()) + 0.5);
}

}  // namespace pando
