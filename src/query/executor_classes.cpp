// The class join for an equivalence layer (dev/PROPOSAL-RELATION-LAYERS.md, dev/LINK-LAYERS.md fast path 4):
// `a:[A] =:X b:[B]`, the pairs of distinct tokens that share a class of X, e.g. a word and its translations over
// `tuid`. The generic expansion finds the class mates of each A token one by one; here the classes are walked once
// each. Every class touched by the more selective side is read once (its members, each marked for A and B), so a
// count is Σ_c |A∩c|·|B∩c| − |A∩B∩c| without listing pairs, and a page is the pairs in hit order (by the first
// token, then the second), read off the members of the touched classes.

#include "query/executor.h"
#include "corpus/equivalence_classes.h"
#include "query/executor_aggregate_internal.h"
#include "core/worker_pool.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <functional>
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

// Sorted, disjoint, inclusive position intervals: a side's slice of the corpus.
using Slice = std::vector<std::pair<CorpusPos, CorpusPos>>;

Slice slice_intersect(const Slice& a, const Slice& b) {
    Slice out;
    for (size_t i = 0, j = 0; i < a.size() && j < b.size();) {
        const CorpusPos lo = std::max(a[i].first, b[j].first), hi = std::min(a[i].second, b[j].second);
        if (lo <= hi) out.emplace_back(lo, hi);
        if (a[i].second < b[j].second) ++i;
        else ++j;
    }
    return out;
}

Slice slice_union(const Slice& a, const Slice& b) {
    Slice all(a);
    all.insert(all.end(), b.begin(), b.end());
    std::sort(all.begin(), all.end());
    Slice out;
    for (const auto& [lo, hi] : all) {
        if (!out.empty() && lo <= out.back().second + 1) out.back().second = std::max(out.back().second, hi);
        else out.emplace_back(lo, hi);
    }
    return out;
}

bool in_slice(const Slice& s, CorpusPos p) {
    auto it = std::upper_bound(s.begin(), s.end(), p,
                               [](CorpusPos v, const std::pair<CorpusPos, CorpusPos>& x) { return v < x.first; });
    return it != s.begin() && p <= std::prev(it)->second;
}

size_t slice_tokens(const Slice& s) {
    size_t n = 0;
    for (const auto& [lo, hi] : s) n += static_cast<size_t>(hi - lo + 1);
    return n;
}

// f(lo, hi) for each part of [a, b] inside the slice
template <typename F>
void clip(const Slice& s, CorpusPos a, CorpusPos b, F&& f) {
    auto it = std::lower_bound(s.begin(), s.end(), a,
                               [](const std::pair<CorpusPos, CorpusPos>& x, CorpusPos v) { return x.second < v; });
    for (; it != s.end() && it->first <= b; ++it) f(std::max(a, it->first), std::min(b, it->second));
}

// The tokens of the regions whose attribute (`text_lang`) has exactly this value, as a slice: what
// `[text_lang="cs"]` or `:: b.text_lang="cs"` allows. false when the index cannot give it (not a region
// attribute, no reverse index, a multivalue attribute, the empty value).
bool region_value_slice(const Corpus& corpus, const std::string& attr, const std::string& value, Slice& out) {
    RegionAttrParts parts;
    if (value.empty() || corpus.has_attr(attr) || corpus.is_multivalue(attr) || !split_region_attr_name(attr, parts)
        || !corpus.has_structure(parts.struct_name))
        return false;
    const StructuralAttr& sa = corpus.structure(parts.struct_name);
    const auto key = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
    const int64_t* rs = nullptr;
    size_t n = 0;
    if (!key || !sa.regions_for_value(*key, value, rs, n)) return false;
    Slice iv;
    iv.reserve(n);
    for (size_t k = 0; k < n; ++k) {
        const Region r = sa.get(static_cast<size_t>(rs[k]));
        if (r.start <= r.end) iv.emplace_back(r.start, r.end);
    }
    out = slice_union(iv, {});
    return true;
}

}  // namespace

std::optional<MatchSet> QueryExecutor::execute_class_join(const TokenQuery& q, size_t max_matches, bool count_total,
                                                          size_t max_total_cap, size_t sample_size,
                                                          const std::vector<std::string>* aggregate_by_fields) const {
    if (sample_size > 0 || !fastpaths_enabled()) return std::nullopt;
    if (range_ || windowed() || q.tokens.size() != 2 || q.relations.size() != 1) return std::nullopt;
    const QueryRelation& rel = q.relations[0];
    if (rel.type != RelationType::EQUIV || rel.layer.empty()) return std::nullopt;
    const RelationDecl* decl = corpus_.relation(rel.layer);
    if (!decl || decl->kind != RelationDecl::Kind::Equivalence || !rel.label.empty()) return std::nullopt;
    for (const auto& tok : q.tokens)
        if (tok.has_repetition() || tok.is_anchor() || tok.is_dep_subtree) return std::nullopt;
    // `:: a.X = b.Y` between the two tokens is read by class_join (as part of the classes); other shapes return nullopt
    if (!q.within.empty() || q.not_within || q.within_having || !q.containing_clauses.empty()
        || !q.global_function_filters.empty() || !q.position_orders.empty())
        return std::nullopt;
    // a default within narrower than the layer's scope would cut the classes: the generic path drops it for such
    // layers (layer_crosses_default_within); one as wide or wider holds every class, so the classes stand as they are
    // max_matches 0 is every hit, as in the rest of the executor (a named set, the pairs of an alignment layer)
    return class_join(q, decl->attr, false, max_matches == 0 ? SIZE_MAX : max_matches, max_total_cap,
                      aggregate_by_fields, count_total);
}

// `a:[A] with b:[B] :: a.X = b.X` (one token on either side, the same token or region attribute X): the pairs whose X
// values (their components, for a multivalue X) meet, which are X's classes, without a token paired with itself
// (`with` never pairs a unit with itself, since 2026-10-28). The class join gives them as source | target pairs;
// nullopt for any other shape (the general joins of execute_parallel take it).
std::optional<MatchSet> QueryExecutor::execute_parallel_class_join(const TokenQuery& source, const TokenQuery& target,
                                                                   const std::vector<GlobalAlignmentFilter>& align,
                                                                   size_t max_matches, bool count_total) const {
    if (!fastpaths_enabled() || include_empty_alignment_values_ || align.size() != 1) return std::nullopt;
    if (source.tokens.size() != 1 || target.tokens.size() != 1) return std::nullopt;
    const std::string& sn = source.tokens[0].name;
    const std::string& tn = target.tokens[0].name;
    const GlobalAlignmentFilter& af = align[0];
    const bool forward = af.name1 == sn && af.name2 == tn, backward = af.name1 == tn && af.name2 == sn;
    if (sn.empty() || tn.empty() || sn == tn || (!forward && !backward)) return std::nullopt;
    const std::string attr = normalize_attr(af.attr1);
    if (attr != normalize_attr(af.attr2) || Corpus::is_internal_attr_name(attr)) return std::nullopt;
    TokenQuery q;
    for (const TokenQuery* side : {&source, &target}) {
        if (!side->relations.empty() || !side->within.empty() || side->not_within || side->within_having
            || !side->containing_clauses.empty() || !side->global_function_filters.empty()
            || !side->position_orders.empty())
            return std::nullopt;
        q.tokens.push_back(side->tokens[0]);
        // a `::` filter may come after either side whichever token it names (class_join assigns it by name)
        q.global_region_filters.insert(q.global_region_filters.end(), side->global_region_filters.begin(),
                                       side->global_region_filters.end());
    }
    for (const auto& tok : q.tokens)
        if (tok.has_repetition() || tok.is_anchor() || tok.is_dep_subtree) return std::nullopt;
    // `with` leaves the pair of a token with itself out, as `=:` does (parallel_pair_is_self in executor.cpp)
    auto ms = class_join(q, attr, false, max_matches == 0 ? SIZE_MAX : max_matches, 0, nullptr, count_total);
    if (!ms) return std::nullopt;
    MatchSet out;
    out.parallel = true;
    out.num_tokens = 2;
    out.plan_path = "class_join";
    out.total_count = ms->total_count;
    out.total_exact = ms->total_exact;
    out.pair_sources = ms->pair_sources;
    out.parallel_matches.reserve(ms->matches.size());
    for (const Match& m : ms->matches) {
        Match a, b;
        a.positions = {m.positions[0]};
        a.span_ends = {m.positions[0]};
        b.positions = {m.positions[1]};
        b.span_ends = {m.positions[1]};
        out.parallel_matches.emplace_back(std::move(a), std::move(b));
    }
    return out;
}

bool equiv_query_gives_pairs(const Corpus& corpus, const TokenQuery& q) {
    if (q.tokens.size() != 2 || q.relations.size() != 1) return false;
    const QueryRelation& rel = q.relations[0];
    if (rel.type != RelationType::EQUIV || rel.layer.empty()) return false;
    for (const auto& tok : q.tokens)
        if (tok.is_anchor()) return false;
    const RelationDecl* decl = corpus.relation(rel.layer);
    return decl && decl->kind == RelationDecl::Kind::Equivalence && (decl->scope == "corpus" || decl->scope == "external");
}

void matches_as_pairs(MatchSet& ms) {
    ms.parallel = true;
    ms.parallel_matches.reserve(ms.matches.size());
    for (const Match& m : ms.matches) {
        if (m.positions.size() < 2) continue;
        Match a, b;
        a.positions = {m.positions[0]};
        a.span_ends = {m.span_ends.empty() ? m.positions[0] : m.span_ends[0]};
        b.positions = {m.positions[1]};
        b.span_ends = {m.span_ends.size() < 2 ? m.positions[1] : m.span_ends[1]};
        ms.parallel_matches.emplace_back(std::move(a), std::move(b));
    }
    ms.matches.clear();
}

std::optional<MatchSet> QueryExecutor::class_join(const TokenQuery& q, const std::string& attr, bool with_self,
                                                  size_t max_matches, size_t max_total_cap,
                                                  const std::vector<std::string>* aggregate_by_fields,
                                                  bool count_total) const {
    if (with_self && aggregate_by_fields && !aggregate_by_fields->empty()) return std::nullopt;
    EquivalenceClasses classes;
    if (!classes.open(corpus_, attr)) return std::nullopt;

    // each side: its token conditions and the `::` region filters on its name (a filter naming both, or none, is
    // not of this shape)
    // a `::` filter on a region attribute (`a.text_iso="eng"`) holds for every token of a region or for none: it is
    // evaluated once per region (not for a nested or overlapping structure, where a token is in several)
    struct Filter {
        TokenQuery one;
        const StructuralAttr* regions = nullptr;
        // per region: -1 not yet evaluated, else the outcome (threads reading the members share it: any of them
        // stores the one outcome)
        std::unique_ptr<std::atomic<int8_t>[]> memo;
    };
    // A side's language (sliced search): a region attribute with one value, `[… & text_lang="cs"]` at the top of
    // its token conditions or `:: b.text_lang="cs"`, holds for a few long stretches of the corpus when the corpus
    // keeps a language together (OPUS / TEITOK projects: a folder per language). Such conditions become the side's
    // slice: it is listed interval by interval, its members are tested by interval, and the class members read for
    // it are those inside the slice (an `s_tuid` class holds a sentence in every language; one is wanted).
    struct Side {
        ConditionPtr cond;   // its token conditions, without those taken into the slice
        std::vector<Filter> filters;
        NameIndexMap names;
        std::vector<CorpusPos> listed;   // its candidates, when its conditions narrow it down
        bool has_list = false;
        bool sliced = false;
        Slice slice;
    };
    Side side[2];
    auto narrow = [](Side& s, Slice iv) {
        s.slice = s.sliced ? slice_intersect(s.slice, iv) : std::move(iv);
        s.sliced = true;
    };
    // the region-attribute leaves ANDed at the top of a condition go into the slice; the rest is returned
    std::function<ConditionPtr(const ConditionPtr&, Side&)> take_slices = [&](const ConditionPtr& c,
                                                                                Side& s) -> ConditionPtr {
        if (!c) return c;
        if (c->is_leaf) {
            const AttrCondition& ac = c->leaf;
            Slice iv;
            if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive && !ac.is_nvals
                && !ac.in_positions && !ac.in_regions && region_value_slice(corpus_, normalize_attr(ac.attr), ac.value, iv)) {
                narrow(s, std::move(iv));
                return nullptr;
            }
            return c;
        }
        if (c->is_structural || c->is_count || c->bool_op != BoolOp::AND) return c;
        ConditionPtr l = take_slices(c->left, s), r = take_slices(c->right, s);
        if (!l) return r;
        if (!r) return l;
        if (l == c->left && r == c->right) return c;
        return ConditionNode::make_branch(BoolOp::AND, l, r);
    };
    for (int i = 0; i < 2; ++i) {
        side[i].cond = take_slices(q.tokens[static_cast<size_t>(i)].conditions, side[i]);
        if (!q.tokens[static_cast<size_t>(i)].name.empty()) side[i].names[q.tokens[static_cast<size_t>(i)].name] = 0;
    }
    for (const auto& f : q.global_region_filters) {
        if (f.computed() || f.anchor_name.empty()) return std::nullopt;
        int i = f.anchor_name == q.tokens[0].name ? 0 : f.anchor_name == q.tokens[1].name ? 1 : -1;
        if (i < 0) return std::nullopt;
        if (Slice iv; f.op == CompOp::EQ && !f.regex && region_value_slice(corpus_, f.region_attr, f.value, iv)) {
            narrow(side[i], std::move(iv));
            continue;
        }
        Filter flt;
        flt.one.global_region_filters.push_back(f);
        const size_t us = f.region_attr.find('_');
        const std::string sn = us == std::string::npos ? std::string() : f.region_attr.substr(0, us);
        if (!sn.empty() && corpus_.has_structure(sn) && !corpus_.is_nested(sn) && !corpus_.is_overlapping(sn)) {
            flt.regions = &corpus_.structure(sn);
            const size_t nr = flt.regions->region_count();
            flt.memo.reset(new std::atomic<int8_t>[nr]);
            for (size_t r = 0; r < nr; ++r) flt.memo[r].store(-1, std::memory_order_relaxed);
        }
        side[i].filters.push_back(std::move(flt));
    }
    // `:: a.X = b.Y` between the two tokens, X and Y plain token attributes with one value each (`a.lemma = b.lemma`):
    // a pair holds when the two values are the same, as the generic path's alignment filter compares them (a missing
    // value, "_" or empty, is the same as nothing). The members are then grouped by class and value, and the groups
    // are read below as the classes are: a pair is two tokens of one group. A value is read as an id of X's lexicon
    // (Y's ids looked up in it, once each); any other filter between the tokens is not of this shape.
    struct PairKey {
        const PositionalAttr* pa[2];
        std::vector<LexiconId> mapped;   // Y's id → X's id when Y is another attribute (kUnmapped: not looked up yet)
        std::vector<int8_t> missing[2];  // per side and id: -1 not yet read, else whether the value is missing
    };
    constexpr LexiconId kUnmapped = UNKNOWN_LEX - 1;
    std::vector<PairKey> keys;
    for (const auto& af : q.global_alignment_filters) {
        const int i1 = af.name1 == q.tokens[0].name ? 0 : af.name1 == q.tokens[1].name ? 1 : -1;
        const int i2 = af.name2 == q.tokens[0].name ? 0 : af.name2 == q.tokens[1].name ? 1 : -1;
        if (i1 < 0 || i2 < 0 || i1 == i2 || q.tokens[0].name.empty()) return std::nullopt;
        const std::string an1 = normalize_attr(af.attr1), an2 = normalize_attr(af.attr2);
        for (const std::string* an : {&an1, &an2})
            if (!corpus_.has_attr(*an) || corpus_.is_multivalue(*an) || Corpus::is_internal_attr_name(*an))
                return std::nullopt;
        PairKey k;
        k.pa[i1] = &corpus_.attr(an1);
        k.pa[i2] = &corpus_.attr(an2);
        if (k.pa[0] != k.pa[1]) k.mapped.assign(static_cast<size_t>(k.pa[1]->lexicon().size()), kUnmapped);
        for (int s = 0; s < 2; ++s) k.missing[s].assign(static_cast<size_t>(k.pa[s]->lexicon().size()), -1);
        keys.push_back(std::move(k));
    }
    // the values of the token at p on side s, as ids of the X lexicons; false when one is missing (no pair then)
    auto key_of = [&](int s, CorpusPos p, std::vector<LexiconId>& out) {
        out.clear();
        for (PairKey& k : keys) {
            LexiconId id = k.pa[s]->id_at(p);
            if (id < 0 || static_cast<size_t>(id) >= k.missing[s].size()) return false;
            int8_t& miss = k.missing[s][static_cast<size_t>(id)];
            if (miss < 0) {
                const std::string_view v = k.pa[s]->lexicon().get(id);
                miss = !include_empty_alignment_values_ && (v.empty() || v == "_");
            }
            if (miss) return false;
            if (s == 1 && !k.mapped.empty()) {
                LexiconId& m = k.mapped[static_cast<size_t>(id)];
                if (m == kUnmapped) m = k.pa[0]->lexicon().lookup(k.pa[1]->lexicon().get(id));
                if (m == UNKNOWN_LEX) return false;
                id = m;
            }
            out.push_back(id);
        }
        return true;
    };
    // a side's token conditions: membership in its resolved candidates when it has them (a region attribute as a
    // token condition, `[text_iso="eng"]`, is costly to test token by token), else tested at the token; `ex`: the
    // executor testing them (a thread's own)
    auto passes_by = [&](const QueryExecutor& ex, const Side& s, CorpusPos p) {
        if (s.has_list) {
            if (!std::binary_search(s.listed.begin(), s.listed.end(), p)) return false;
        } else if ((s.sliced && !in_slice(s.slice, p)) || !ex.check_conditions(p, s.cond)) {
            return false;
        }
        for (const Filter& f : s.filters) {
            const int64_t r = f.regions ? f.regions->find_region(p) : -1;
            if (r >= 0) {
                const int8_t known = f.memo[static_cast<size_t>(r)].load(std::memory_order_relaxed);
                if (known >= 0) {
                    if (!known) return false;
                    continue;
                }
            }
            Match m;
            m.positions = {p};
            m.span_ends = {p};
            const bool ok = ex.match_passes_inline_region_filters(m, f.one, s.names);
            if (r >= 0) f.memo[static_cast<size_t>(r)].store(ok, std::memory_order_relaxed);
            if (!ok) return false;
        }
        return true;
    };
    auto passes = [&](const Side& s, CorpusPos p) { return passes_by(*this, s, p); };
    // a side's candidates: from its token conditions, else from a region filter (the tokens of the regions it
    // holds for, `b.text_iso="ces"`); a side with neither has no list
    auto restricted = [&](const Side& s) {
        if (s.cond || s.sliced) return true;
        for (const Filter& f : s.filters)
            if (f.regions) return true;
        return false;
    };
    auto list_side = [&](Side& s) {
        if (s.sliced) {
            // the slice's tokens meeting the rest: the rest's postings read interval by interval (each a binary
            // search into them), or, when the rest is the rarer, its positions kept where they fall in the slice
            const size_t in_slice_n = slice_tokens(s.slice);
            if (s.cond && estimate_cardinality(s.cond) < in_slice_n / 4) {
                for (CorpusPos p : resolve_conditions(s.cond))
                    if (in_slice(s.slice, p)) s.listed.push_back(p);
            } else {
                s.listed.reserve(s.cond ? 0 : in_slice_n);
                for (const auto& [lo, hi] : s.slice) {
                    if (!s.cond) {
                        for (CorpusPos p = lo; p <= hi; ++p) s.listed.push_back(p);
                        continue;
                    }
                    QueryExecutor w(*this, nullptr, progress_);
                    w.operand_window_ = {lo, hi + 1};
                    const std::vector<CorpusPos> part = w.resolve_conditions(s.cond);
                    s.listed.insert(s.listed.end(), part.begin(), part.end());
                }
            }
            if (!s.filters.empty())
                s.listed.erase(std::remove_if(s.listed.begin(), s.listed.end(), [&](CorpusPos p) {
                    for (Filter& f : s.filters) {
                        Match m;
                        m.positions = {p};
                        m.span_ends = {p};
                        if (!match_passes_inline_region_filters(m, f.one, s.names)) return true;
                    }
                    return false;
                }), s.listed.end());
            s.has_list = true;
            return;
        }
        if (s.cond) {
            s.listed = resolve_conditions(s.cond);
            s.has_list = true;
            return;
        }
        for (Filter& f : s.filters) {
            if (!f.regions) continue;
            for (size_t r = 0; r < f.regions->region_count(); ++r) {
                const Region rg = f.regions->get(r);
                if (rg.start > rg.end) continue;
                Match m;
                m.positions = {rg.start};
                m.span_ends = {rg.start};
                const bool ok = match_passes_inline_region_filters(m, f.one, s.names);
                f.memo[r].store(ok, std::memory_order_relaxed);
                if (ok)
                    for (CorpusPos p = rg.start; p <= rg.end; ++p) s.listed.push_back(p);
            }
            s.has_list = true;
            return;
        }
    };
    // the narrower side by the lexicon's counts, before anything is listed (a side with only a region filter counts
    // as every token); only it is listed now
    size_t est[2];
    for (int i = 0; i < 2; ++i) {
        est[i] = side[i].cond ? estimate_cardinality(side[i].cond) : static_cast<size_t>(corpus_.size());
        if (side[i].sliced) est[i] = std::min(est[i], slice_tokens(side[i].slice));
    }
    // the class members worth reading: those in either side's slice, when both sides have one
    const bool clipped = side[0].sliced && side[1].sliced;
    const Slice either = clipped ? slice_union(side[0].slice, side[1].slice) : Slice{};
    auto for_each_member = [&](LexiconId c, const Slice* only, auto&& f) {
        if (only && classes.regions()) {
            classes.for_each_range(c, [&](CorpusPos a, CorpusPos b) {
                clip(*only, a, b, [&](CorpusPos lo, CorpusPos hi) {
                    for (CorpusPos p = lo; p <= hi; ++p) f(p);
                });
            });
            return;
        }
        classes.for_each_member(c, [&](CorpusPos p) {
            if (!only || in_slice(*only, p)) f(p);
        });
    };
    auto members_in = [&](LexiconId c, const Slice* only) {
        if (!only || !classes.regions()) return classes.size(c);
        size_t n = 0;
        classes.for_each_range(c, [&](CorpusPos a, CorpusPos b) {
            clip(*only, a, b, [&](CorpusPos lo, CorpusPos hi) { n += static_cast<size_t>(hi - lo + 1); });
        });
        return n;
    };
    const bool r0 = restricted(side[0]), r1 = restricted(side[1]);
    const int driver = r0 && (!r1 || est[0] <= est[1]) ? 0 : r1 ? 1 : -1;
    if (driver >= 0) list_side(side[driver]);
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
    MatchSet result;
    result.num_tokens = 2;
    result.plan_path = "class_join";

    // page first (no total asked, no `count by`, a page size, and the first token not much the wider side): the
    // first tokens in corpus order, each with the second tokens of its classes, until the page is full; nothing else
    // is read. A second side much narrower than the first would leave most first tokens without a partner: then the
    // count below, which starts from it.
    if (!count_total && !aggregating && max_matches != SIZE_MAX && (driver != 1 || est[0] / 8 <= est[1])) {
        if (driver == 1 && restricted(side[0])) list_side(side[0]);
        std::vector<CorpusPos> bs;
        std::vector<LexiconId> ka, kb;
        bool full = false;
        auto take = [&](CorpusPos a) {
            if (!passes(side[0], a) || !key_of(0, a, ka)) return;
            bs.clear();
            classes.for_each_class(a, [&](LexiconId c) {
                for_each_member(c, side[1].sliced ? &side[1].slice : nullptr, [&](CorpusPos b) {
                    if ((with_self || b != a) && passes(side[1], b) && key_of(1, b, kb) && kb == ka) bs.push_back(b);
                });
            });
            std::sort(bs.begin(), bs.end());
            bs.erase(std::unique(bs.begin(), bs.end()), bs.end());
            for (CorpusPos b : bs) {
                if (result.matches.size() >= max_matches) {
                    full = true;
                    return;
                }
                Match m;
                m.positions = {a, b};
                m.span_ends = {a, b};
                result.matches.push_back(std::move(m));
            }
        };
        if (side[0].has_list) {
            for (size_t i = 0; i < side[0].listed.size() && !full; ++i) take(side[0].listed[i]);
        } else {
            for (CorpusPos a = 0; a < corpus_.size() && !full; ++a) take(a);
        }
        result.total_count = result.matches.size();
        result.total_exact = !full;
        return result;
    }

    if (driver >= 0) {
        for (CorpusPos p : side[driver].listed)
            if (side[driver].filters.empty() || passes(side[driver], p))
                classes.for_each_class(p, [&](LexiconId c) { touched.push_back(c); });
        std::sort(touched.begin(), touched.end());
        touched.erase(std::unique(touched.begin(), touched.end()), touched.end());
    } else {
        for (LexiconId c = 0; c < classes.class_count(); ++c)
            if (classes.is_class(c)) touched.push_back(c);
    }
    size_t scan_cost = 0;
    for (LexiconId c : touched) scan_cost += members_in(c, clipped ? &either : nullptr);
    // the other side listed only when that is cheaper than testing it at the members of the touched classes
    if (driver >= 0 && restricted(side[1 - driver]) && est[1 - driver] < scan_cost) list_side(side[1 - driver]);
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
        // the members of the touched classes, read in slices of about equal size; a large read over threads (the
        // request's budget, else PANDO_CLASS_JOIN_THREADS, else min(usable CPUs, 8); a thread for every
        // PANDO_CLASS_JOIN_MIN members, default 2^19; workers borrowed from the process pool), each slice with its
        // own executor, the slices joined in class order
        const Slice* only = clipped ? &either : nullptr;
        auto read = [&](const QueryExecutor& ex, size_t lo, size_t hi, std::vector<Member>& out) {
            for (size_t t = lo; t < hi; ++t) {
                const LexiconId c = touched[t];
                for_each_member(c, only, [&](CorpusPos p) {
                    const bool a = passes_by(ex, side[0], p), b = passes_by(ex, side[1], p);
                    if (a || b) out.push_back({p, c, a, b});
                });
            }
        };
        static const unsigned env_cap = [] {
            const char* e = std::getenv("PANDO_CLASS_JOIN_THREADS");
            const int v = e && *e ? std::atoi(e) : 0;
            return v > 0 ? static_cast<unsigned>(v) : std::min(8u, usable_cpus());
        }();
        static const size_t per_thread = [] {
            const char* e = std::getenv("PANDO_CLASS_JOIN_MIN");
            const long long v = e && *e ? std::atoll(e) : 0;
            return v > 0 ? static_cast<size_t>(v) : size_t{1} << 19;
        }();
        const unsigned cap = thread_budget_ ? std::min(thread_budget_, env_cap) : env_cap;
        const unsigned nt = static_cast<unsigned>(std::min<size_t>(cap, std::max<size_t>(1, scan_cost / per_thread)));
        if (nt <= 1 || touched.size() < 2) {
            read(*this, 0, touched.size(), members);
        } else {
            // 4 slices per thread, cut where the members read so far reach the next share of scan_cost
            const size_t nslices = std::min<size_t>(static_cast<size_t>(nt) * 4, touched.size());
            std::vector<size_t> cut{0};
            size_t acc = 0;
            for (size_t t = 0; t < touched.size() && cut.size() < nslices; ++t) {
                acc += members_in(touched[t], only);
                if (acc * nslices >= scan_cost * cut.size()) cut.push_back(t + 1);
            }
            if (cut.back() != touched.size()) cut.push_back(touched.size());
            // the conditions compiled here: compiling writes into their nodes, which the threads share
            for (const Side& sd : side) compile_conditions(sd.cond);
            std::vector<std::vector<Member>> parts(cut.size() - 1);
            std::atomic<bool> cancelled{false};
            WorkerPool::global().run(parts.size(), nt - 1, [&](size_t i) {
                if (cancelled.load(std::memory_order_relaxed) || (progress_ && progress_->cancelled())) {
                    cancelled = true;
                    return;
                }
                QueryExecutor ex(*this, nullptr, progress_);
                read(ex, cut[i], cut[i + 1], parts[i]);
            });
            if (cancelled) throw QueryCancelled();
            size_t n = 0;
            for (const auto& part : parts) n += part.size();
            members.reserve(n);
            for (auto& part : parts) members.insert(members.end(), part.begin(), part.end());
        }
    }
    // with `:: a.X = b.Y`: each class split into groups by the members' values, a group numbered like a class (in
    // class order, so the rows stay grouped). A member on both sides whose two values differ (X and Y two attributes)
    // is two rows, one per side; it cannot pair with itself then.
    if (!keys.empty()) {
        const size_t nk = keys.size();
        std::vector<Member> grouped, rows;
        grouped.reserve(members.size());
        std::vector<LexiconId> rkeys;   // the rows' values, nk per row
        std::vector<size_t> order;
        std::vector<LexiconId> ka, kb;
        auto key_less = [&](size_t x, size_t y) {
            return std::lexicographical_compare(rkeys.begin() + static_cast<std::ptrdiff_t>(x * nk),
                                                rkeys.begin() + static_cast<std::ptrdiff_t>((x + 1) * nk),
                                                rkeys.begin() + static_cast<std::ptrdiff_t>(y * nk),
                                                rkeys.begin() + static_cast<std::ptrdiff_t>((y + 1) * nk));
        };
        LexiconId group = 0;
        for (size_t i = 0; i < members.size();) {
            size_t j = i;
            rows.clear();
            rkeys.clear();
            for (; j < members.size() && members[j].cls == members[i].cls; ++j) {
                const Member& m = members[j];
                const bool a = m.in_a && key_of(0, m.pos, ka), b = m.in_b && key_of(1, m.pos, kb);
                if (a && b && ka == kb) {
                    rows.push_back(m);
                    rkeys.insert(rkeys.end(), ka.begin(), ka.end());
                    continue;
                }
                if (a) {
                    rows.push_back({m.pos, m.cls, true, false});
                    rkeys.insert(rkeys.end(), ka.begin(), ka.end());
                }
                if (b) {
                    rows.push_back({m.pos, m.cls, false, true});
                    rkeys.insert(rkeys.end(), kb.begin(), kb.end());
                }
            }
            order.resize(rows.size());
            for (size_t k = 0; k < rows.size(); ++k) order[k] = k;
            std::stable_sort(order.begin(), order.end(), key_less);
            for (size_t k = 0; k < order.size(); ++k) {
                if (k > 0 && key_less(order[k - 1], order[k])) ++group;
                Member m = rows[order[k]];
                m.cls = group;
                grouped.push_back(m);
            }
            if (!rows.empty()) ++group;
            i = j;
        }
        members = std::move(grouped);
    }
    // the count per class; a class without both an A and a B member holds no pair and is dropped
    size_t total = 0;
    // the first tokens with a partner: counted, when a token is in one class only (no repeats to drop)
    const bool single = classes.single();
    std::vector<CorpusPos> sources;
    size_t nsources = 0;
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
                for (size_t k = i; k < j; ++k) {
                    if (members[k].in_a && (with_self || nb > static_cast<size_t>(members[k].in_b))) {
                        if (single) ++nsources;
                        else sources.push_back(members[k].pos);
                    }
                    members[out++] = members[k];
                }
            }
            i = j;
        }
        members.resize(out);
        std::sort(sources.begin(), sources.end());
        sources.erase(std::unique(sources.begin(), sources.end()), sources.end());
        if (!single) nsources = sources.size();
    }
    // a multivalue attribute: a pair sharing k classes was counted k times
    std::unordered_map<CorpusPos, std::vector<size_t>> multi;   // position → its member rows, when in several classes
    if (!single) {
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

    // `with` pairs a token with itself too: once for each token on both sides (in a class)
    std::vector<CorpusPos> selves;
    if (with_self) {
        for (const Member& m : members)
            if (m.in_a && m.in_b) selves.push_back(m.pos);
        std::sort(selves.begin(), selves.end());
        selves.erase(std::unique(selves.begin(), selves.end()), selves.end());
        total += selves.size();
    }

    // the page, in hit order as the generic expansion gives it: by the first token, then the second; the A members
    // in corpus order, each with the B members of its classes
    if (max_matches > 0) {
        std::vector<size_t> order;
        for (size_t i = 0; i < members.size(); ++i)
            if (members[i].in_a) order.push_back(i);
        // only the first of them sorted (a page asks for a few): more when those leave the page short
        auto by_pos = [&](size_t x, size_t y) { return members[x].pos < members[y].pos; };
        size_t sorted = std::min(order.size(), max_matches > order.size() / 2 ? order.size() : 2 * max_matches + 64);
        std::partial_sort(order.begin(), order.begin() + static_cast<std::ptrdiff_t>(sorted), order.end(), by_pos);
        auto reach = [&](size_t i) {   // order[i] in place
            if (i < sorted) return;
            const size_t from = sorted;
            sorted = std::min(order.size(), 2 * sorted);
            std::partial_sort(order.begin() + static_cast<std::ptrdiff_t>(from),
                              order.begin() + static_cast<std::ptrdiff_t>(sorted), order.end(), by_pos);
        };
        std::vector<CorpusPos> bs;
        for (size_t i = 0; i < order.size() && result.matches.size() < max_matches;) {
            reach(i);
            const CorpusPos a = members[order[i]].pos;
            bs.clear();
            for (; i < order.size() && (reach(i), members[order[i]].pos == a); ++i) {
                const auto [lo, hi] = range_of[members[order[i]].cls];
                for (size_t k = lo; k < hi; ++k)
                    if (members[k].in_b && (with_self || members[k].pos != a)) bs.push_back(members[k].pos);
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
    result.pair_sources = nsources;
    if (max_total_cap > 0 && result.total_count > max_total_cap) {
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    return result;
}

// `[A] within L [having [C]]` / `[A] not within L [having [C]]` with L an equivalence layer (`s_tuid`, `tuid`): a class
// as the region. The hit lies in a class (that has a C token); `not within`: it does not (an unaligned token is in no
// class). Rewritten as P4.14 rewrites `within R having C`: the classes in question give position intervals (their
// regions, or their tokens), a filter on the hit's position the other paths read. A one-token query; two tokens in
// one class are `a:[A] =:L b:[B]`. `lazy` (a page, no total): the filter tests each hit's own classes instead, so the
// first hits come without computing every class in question.
bool QueryExecutor::rewrite_within_class(const TokenQuery& q, TokenQuery& out, bool lazy) const {
    if (q.within.empty() || corpus_.has_structure(q.within)) return false;
    const RelationDecl* decl = corpus_.relation(q.within);
    if (!decl || decl->kind != RelationDecl::Kind::Equivalence) return false;
    if (q.tokens.size() != 1 || q.tokens[0].has_repetition() || q.tokens[0].is_anchor())
        throw std::runtime_error("within " + q.within + " takes a one-token query; for two tokens in one class of " +
                                 q.within + ": a:[…] =:" + q.within + " b:[…]");
    EquivalenceClasses classes;
    if (!classes.open(corpus_, decl->attr))
        throw std::runtime_error("within " + q.within + ": the corpus has no attribute '" + decl->attr + "'");
    if (lazy) {
        // per hit: one of its classes has a C token (or: it has a class); `not within`: none
        if (q.within_having) compile_conditions(q.within_having);
        const ConditionPtr having = q.within_having;
        const bool negated = q.not_within;
        auto test = [this, classes, having, negated](int64_t p) {
            bool in = false;
            classes.for_each_class(static_cast<CorpusPos>(p), [&](LexiconId c) {
                if (in) return;
                if (!having) { in = true; return; }
                if (classes.regions())
                    classes.for_each_range(c, [&](CorpusPos a, CorpusPos b) {
                        for (CorpusPos x = a; x <= b && !in; ++x) in = check_conditions(x, having);
                    });
                else
                    classes.for_each_member(c, [&](CorpusPos x) { in = in || check_conditions(x, having); });
            });
            return in != negated;
        };
        out = q;
        out.within.clear();
        out.not_within = false;
        out.within_having = nullptr;
        GlobalRegionFilter gf;
        gf.test = std::make_shared<const std::function<bool(int64_t)>>(std::move(test));
        out.global_region_filters.push_back(std::move(gf));
        return true;
    }
    // the classes in question: those with a C token, or all
    std::vector<LexiconId> cls;
    if (q.within_having) {
        compile_conditions(q.within_having);
        for (CorpusPos p : resolve_conditions(q.within_having))
            classes.for_each_class(p, [&](LexiconId c) { cls.push_back(c); });
        std::sort(cls.begin(), cls.end());
        cls.erase(std::unique(cls.begin(), cls.end()), cls.end());
    } else {
        for (LexiconId c = 0; c < classes.class_count(); ++c)
            if (classes.is_class(c)) cls.push_back(c);
    }
    // their positions as sorted, disjoint intervals
    std::vector<std::pair<int64_t, int64_t>> spans;
    for (LexiconId c : cls) {
        if (classes.regions())
            classes.for_each_region(c, [&](const Region& r) { spans.emplace_back(r.start, r.end); });
        else
            classes.for_each_member(c, [&](CorpusPos p) { spans.emplace_back(p, p); });
    }
    std::sort(spans.begin(), spans.end());
    auto iv = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
    for (const auto& [a, b] : spans) {
        if (!iv->empty() && a <= iv->back().second + 1) iv->back().second = std::max<int64_t>(iv->back().second, b);
        else iv->push_back({a, b});
    }
    if (q.not_within) {   // the complement
        auto co = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
        int64_t from = 0;
        for (const auto& [a, b] : *iv) {
            if (a > from) co->push_back({from, a - 1});
            from = b + 1;
        }
        if (from < corpus_.size()) co->push_back({from, corpus_.size() - 1});
        iv = std::move(co);
    }
    out = q;
    out.within.clear();
    out.not_within = false;
    out.within_having = nullptr;
    GlobalRegionFilter gf;
    gf.positions = std::move(iv);
    out.global_region_filters.push_back(std::move(gf));
    return true;
}

// `… within L1 having [A] within L2 having [B]`: every clause restricts the hits. The first stays as it is; each
// later one becomes a filter on the hit's position: an equivalence layer as rewrite_within_class has it, a region
// (one-token queries: the token lies in such a region) through its regions with a C token, or all of them.
bool QueryExecutor::rewrite_more_withins(const TokenQuery& q, TokenQuery& out, bool lazy) const {
    if (q.more_withins.empty()) return false;
    out = q;
    out.more_withins.clear();
    for (const WithinClause& w : q.more_withins) {
        TokenQuery one;
        one.tokens = q.tokens;
        one.within = w.within;
        one.not_within = w.not_within;
        one.within_having = w.having;
        TokenQuery cq;
        if (rewrite_within_class(one, cq, lazy)) {
            out.global_region_filters.push_back(std::move(cq.global_region_filters.back()));
            continue;
        }
        if (!corpus_.has_structure(w.within))
            throw std::runtime_error("within " + w.within + ": the corpus has no region or class layer of that name");
        if (q.tokens.size() != 1 || q.tokens[0].has_repetition() || q.tokens[0].is_anchor())
            throw std::runtime_error("a second within on regions (" + w.within + ") takes a one-token query; "
                                     "for longer queries: one within " + w.within + " and :: filters");
        const auto& sa = corpus_.structure(w.within);
        std::vector<int64_t> regions;
        if (w.having) {
            compile_conditions(w.having);
            int64_t hint = -1;
            for (CorpusPos p : resolve_conditions(w.having)) {
                const int64_t r = sa.find_region_from(p, hint);
                if (r >= 0) { hint = r; regions.push_back(r); }
            }
            std::sort(regions.begin(), regions.end());
            regions.erase(std::unique(regions.begin(), regions.end()), regions.end());
        } else {
            for (size_t r = 0; r < sa.region_count(); ++r) regions.push_back(static_cast<int64_t>(r));
        }
        std::vector<std::pair<int64_t, int64_t>> spans;
        for (int64_t r : regions) {
            const Region reg = sa.get(static_cast<size_t>(r));
            if (reg.start <= reg.end) spans.emplace_back(reg.start, reg.end);
        }
        std::sort(spans.begin(), spans.end());
        auto iv = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
        for (const auto& [a, b] : spans) {
            if (!iv->empty() && a <= iv->back().second + 1) iv->back().second = std::max<int64_t>(iv->back().second, b);
            else iv->push_back({a, b});
        }
        if (w.not_within) {
            auto co = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>();
            int64_t from = 0;
            for (const auto& [a, b] : *iv) {
                if (a > from) co->push_back({from, a - 1});
                from = b + 1;
            }
            if (from < corpus_.size()) co->push_back({from, corpus_.size() - 1});
            iv = std::move(co);
        }
        GlobalRegionFilter gf;
        gf.positions = std::move(iv);
        out.global_region_filters.push_back(std::move(gf));
    }
    return true;
}

}  // namespace pando
