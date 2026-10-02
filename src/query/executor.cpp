#include "query/executor.h"
#include "core/regex_engine.h"
#include "query/bitmap_expr.h"
#include "query/flat_agg_counter.h"
#include "index/head_attr.h"
#include <tuple>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <queue>
#include <random>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include <mutex>
#include <condition_variable>

namespace pando {

namespace {

static bool regex_eval_sv(std::string_view val, const Regex& re, bool full_match) {
    return re.match(val, full_match);
}

// Upper bound on positions materialised for one complex operand (memory guard
// on multi-billion-token corpora: 256M positions = 1-2 GB). PANDO_MATERIALIZE_MAX
// overrides; above the bound the operand stays on the generic probing path.
static size_t materialize_max() {
    static const size_t v = [] {
        const char* e = std::getenv("PANDO_MATERIALIZE_MAX");
        if (e && *e) return static_cast<size_t>(std::strtoull(e, nullptr, 10));
        return static_cast<size_t>(256) << 20;
    }();
    return v;
}

// Literal prefix every match of `pat` must start with (P1.5). Only for patterns
// anchored at the start (full match, or a leading '^') without alternation; the
// prefix stops at the first metacharacter, and a char followed by a quantifier
// that allows zero repetitions is dropped. *exact is set when the whole pattern
// is that literal (the regex is then an EQ). Lexicon ids are in byte order, so a
// prefix is a contiguous id range: `[lemma="un.*"]` scans only the "un…" entries.
static std::string regex_literal_prefix(const std::string& pat, bool full_match, bool* exact) {
    *exact = false;
    if (pat.find('|') != std::string::npos) return {};
    size_t i = 0;
    const bool caret = !pat.empty() && pat[0] == '^';
    if (caret) i = 1;
    if (!full_match && !caret) return {};
    auto is_meta = [](char c) {
        return c == '.' || c == '[' || c == ']' || c == '(' || c == ')' || c == '*'
            || c == '+' || c == '?' || c == '{' || c == '}' || c == '\\' || c == '^'
            || c == '$';
    };
    std::string lit;
    for (; i < pat.size(); ++i) {
        const char c = pat[i];
        if (c == '$' && i + 1 == pat.size()) {
            *exact = true;
            return lit;
        }
        if (is_meta(c)) {
            // '+' and '{n,…}' (n >= 1) keep the previous char; '*', '?', '{0…'
            // make it optional: drop it (the whole UTF-8 sequence).
            if (c == '*' || c == '?' || pat.compare(i, 2, "{0") == 0) {
                while (!lit.empty() && (static_cast<unsigned char>(lit.back()) & 0xC0) == 0x80)
                    lit.pop_back();
                if (!lit.empty()) lit.pop_back();
            }
            return lit;
        }
        lit += c;
    }
    *exact = full_match;
    return lit;
}

// Longest literal every match of `pat` must contain (prefilter for the lexicon
// scan: memchr/memcmp-speed rejection before the regex engine). Only top-level
// literal runs count (not inside groups or classes); a char made optional by
// '*', '?' or '{0' ends its run without itself. No alternation, no inline flags.
static std::string regex_required_literal(const std::string& pat) {
    if (pat.find('|') != std::string::npos || pat.find("(?") != std::string::npos) return {};
    std::string best, run;
    auto flush = [&] { if (run.size() > best.size()) best = run; run.clear(); };
    int depth = 0;
    for (size_t i = 0; i < pat.size(); ++i) {
        const char c = pat[i];
        if (c == '\\') { flush(); ++i; continue; }
        if (c == '[') {
            flush();
            size_t j = i + 1;
            if (j < pat.size() && pat[j] == '^') ++j;
            if (j < pat.size() && pat[j] == ']') ++j;
            while (j < pat.size() && pat[j] != ']') { if (pat[j] == '\\') ++j; ++j; }
            i = j;
            continue;
        }
        if (c == '(') { flush(); ++depth; continue; }
        if (c == ')') { flush(); if (depth > 0) --depth; continue; }
        if (depth > 0) continue;
        if (c == '*' || c == '?' || (c == '{' && pat.compare(i, 2, "{0") == 0)) {
            while (!run.empty() && (static_cast<unsigned char>(run.back()) & 0xC0) == 0x80)
                run.pop_back();
            if (!run.empty()) run.pop_back();
            flush();
            continue;
        }
        if (c == '.' || c == '+' || c == '{' || c == '}' || c == '^' || c == '$' || c == ']') {
            flush();
            if (c == '{') { while (i < pat.size() && pat[i] != '}') ++i; }
            continue;
        }
        run += c;
    }
    flush();
    return best;
}

// P4.6: `(?i)pattern` → its required literal, lowercased, for an ASCII
// case-insensitive prefilter; empty when the literal is not plain ASCII or has a
// letter with a non-ASCII case variant in Unicode simple case folding (k: U+212A
// KELVIN SIGN, s: U+017F LONG S), so the prefilter can never drop a match.
static std::string regex_required_literal_icase(const std::string& pat) {
    if (pat.compare(0, 4, "(?i)") != 0) return {};
    std::string lit = regex_required_literal(pat.substr(4));
    for (char& c : lit) {
        const unsigned char u = static_cast<unsigned char>(c);
        if (u >= 0x80) return {};
        c = static_cast<char>(std::tolower(u));
        if (c == 'k' || c == 's') return {};
    }
    return lit;
}

// Does `v` contain the lowercase ASCII `lit` ignoring ASCII case?
static bool contains_icase_ascii(std::string_view v, std::string_view lit) {
    if (lit.empty()) return true;
    if (v.size() < lit.size()) return false;
    const char a = lit[0], A = static_cast<char>(std::toupper(static_cast<unsigned char>(a)));
    for (size_t i = 0; i + lit.size() <= v.size(); ++i) {
        if (v[i] != a && v[i] != A) continue;
        size_t k = 1;
        while (k < lit.size() && std::tolower(static_cast<unsigned char>(v[i + k])) == lit[k]) ++k;
        if (k == lit.size()) return true;
    }
    return false;
}

// Lexicon id range [lo, hi) of the entries starting with `prefix`.
static std::pair<LexiconId, LexiconId> lexicon_prefix_range(const Lexicon& lex,
                                                            std::string_view prefix) {
    LexiconId lo = 0, hi = lex.size();
    while (lo < hi) {   // first entry >= prefix
        const LexiconId mid = lo + (hi - lo) / 2;
        if (lex.get(mid) < prefix) lo = mid + 1;
        else hi = mid;
    }
    LexiconId a = lo, b = lex.size();
    while (a < b) {     // first entry whose leading |prefix| bytes are > prefix
        const LexiconId mid = a + (b - a) / 2;
        if (lex.get(mid).substr(0, prefix.size()) <= prefix) a = mid + 1;
        else b = mid;
    }
    return {lo, a};
}

// `within S` when S is declared nested= or overlapping= (see expand_seed caller):
// the full match span [min,max] must be contained in *some* region row of S.
// Flat structures use the cheaper single-`find_region` path instead.
static std::optional<std::pair<CorpusPos, CorpusPos>> match_min_max_span(
    const std::vector<CorpusPos>& pm, size_t n) {
    CorpusPos min_p = NO_HEAD;
    CorpusPos max_p = 0;
    for (size_t i = 0; i < n; ++i) {
        if (pm[i] == NO_HEAD) continue;
        CorpusPos lo = pm[i];
        CorpusPos hi = (pm[n + i] != NO_HEAD) ? pm[n + i] : pm[i];
        if (min_p == NO_HEAD) {
            min_p = lo;
            max_p = hi;
        } else {
            if (lo < min_p) min_p = lo;
            if (hi > max_p) max_p = hi;
        }
    }
    if (min_p == NO_HEAD) return std::nullopt;
    return std::make_pair(min_p, max_p);
}

// Same extent rule as match_min_max_span, for assembled Match objects.
static std::optional<std::pair<CorpusPos, CorpusPos>> match_extent_from_match(
    const Match& m) {
    if (m.positions.empty()) return std::nullopt;
    const size_t n = m.positions.size();
    CorpusPos min_p = NO_HEAD;
    CorpusPos max_p = 0;
    for (size_t i = 0; i < n; ++i) {
        if (m.positions[i] == NO_HEAD) continue;
        CorpusPos lo = m.positions[i];
        CorpusPos hi = (!m.span_ends.empty() && i < m.span_ends.size()
                        && m.span_ends[i] != NO_HEAD)
            ? m.span_ends[i]
            : m.positions[i];
        if (min_p == NO_HEAD) {
            min_p = lo;
            max_p = hi;
        } else {
            if (lo < min_p) min_p = lo;
            if (hi > max_p) max_p = hi;
        }
    }
    if (min_p == NO_HEAD) return std::nullopt;
    return std::make_pair(min_p, max_p);
}

static bool within_span_in_some_region(const StructuralAttr& sa, CorpusPos min_p,
                                       CorpusPos max_p) {
    if (min_p == NO_HEAD || min_p > max_p) return false;
    bool ok = false;
    sa.for_each_region_at(min_p, [&](size_t rgn_idx) -> bool {
        Region r = sa.get(rgn_idx);
        if (r.start <= min_p && max_p <= r.end) {
            ok = true;
            return false;
        }
        return true;
    });
    return ok;
}

// ── Fast-path switch (testing) ───────────────────────────────────────────
//
// PANDO_FASTPATH=on (default) | nomerge | off
//   nomerge: disable .rev shift-merge (S1) and the dep bitset+head join, so the
//            same query runs through the older seed+probe / generic paths;
//   off:     additionally disable the sequence seed+probe fast path.
// Used by test/fastpath_diff.py to check that fast paths return exactly the
// same matches and totals as the generic executor.
enum class FastPathMode : uint8_t { On, NoMerge, Off };

// Dep bitset join is only used when max(|A|,|B|) <= kDepBitsetMaxSkew * min(|A|,|B|).
static constexpr size_t kDepBitsetMaxSkew = 64;

static FastPathMode fastpath_mode() {
    static const FastPathMode mode = [] {
        const char* v = std::getenv("PANDO_FASTPATH");
        if (!v) return FastPathMode::On;
        const std::string s(v);
        if (s == "off" || s == "0") return FastPathMode::Off;
        if (s == "nomerge") return FastPathMode::NoMerge;
        return FastPathMode::On;
    }();
    return mode;
}

// P3.2: PANDO_BITMAPS=on (default) | off | force
//   off:   never use the bitmap kernels (testing: compare with the merge paths);
//   force: use them whenever the query compiles to bitmap expressions, also
//          where the planner would pick a merge (rare operands).
enum class BitmapMode : uint8_t { On, Off, Force };
static BitmapMode bitmap_mode() {
    static const BitmapMode mode = [] {
        const char* v = std::getenv("PANDO_BITMAPS");
        if (!v) return BitmapMode::On;
        const std::string s(v);
        if (s == "off" || s == "0") return BitmapMode::Off;
        if (s == "force") return BitmapMode::Force;
        return BitmapMode::On;
    }();
    return mode;
}

// ── Manatee-style adjacent sequence merge (QUERY-PERF-MERGE S1) ─────────

/// Simple EQ on a non-MV positional attr (post-compile), or empty = wildcard `[]`.
enum class SeqMergeTok : uint8_t { Complex, Wildcard, EqRev };

struct SeqMergeOperand {
    SeqMergeTok kind = SeqMergeTok::Complex;
    RevSpan span{};
    /// P1.4/P1.5: storage for a materialised operand (AND / OR / %c / regex / !=):
    /// `span` then points into this buffer instead of a `.rev` file.
    std::shared_ptr<void> owned;
};

/// Wrap sorted unique positions as a posting span of the given width (same width
/// as the corpus `.rev` files, so the typed kernels apply).
static SeqMergeOperand owned_postings(const std::vector<CorpusPos>& pos, int width) {
    SeqMergeOperand o;
    o.kind = SeqMergeTok::EqRev;
    o.span.width = width;
    o.span.count = pos.size();
    auto fill = [&](auto tag) {
        using T = decltype(tag);
        auto buf = std::make_shared<std::vector<T>>(pos.size());
        for (size_t i = 0; i < pos.size(); ++i) (*buf)[i] = static_cast<T>(pos[i]);
        o.span.data = buf->data();
        o.owned = buf;
    };
    if (width == 2) fill(int16_t{});
    else if (width == 4) fill(int32_t{});
    else { o.span.width = 8; fill(int64_t{}); }
    if (pos.empty()) o.span.data = nullptr;
    return o;
}

/// `--sample N` / /query "sample": the N hits with the smallest hash of (seed, the
/// hit's positions) — a uniform random sample that does not depend on the order in
/// which a path finds the hits, so the same seed gives the same sample on every
/// path, page and thread count. take() returns them in hash order (a random order:
/// the order of a shuffled concordance).
class HitSample {
public:
    HitSample(size_t k, uint32_t seed)
        : k_(k), seed_(mix(seed != 0 ? seed : static_cast<uint64_t>(std::random_device{}()) << 1 | 1)) {
        heap_.reserve(std::min<size_t>(k, 1u << 16));
    }
    static uint64_t mix(uint64_t x) {   // splitmix64 finaliser
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }
    uint64_t hash(const Match& m) const {
        uint64_t h = seed_;
        for (CorpusPos p : m.positions) h = mix(h ^ static_cast<uint64_t>(p));
        for (CorpusPos p : m.span_ends) h = mix(h ^ (static_cast<uint64_t>(p) + 0x5bd1e995ULL));
        return h;
    }
    void offer(Match&& m) {
        if (k_ == 0) return;
        const uint64_t h = hash(m);
        auto cmp = [](const Item& a, const Item& b) { return a.first < b.first; };
        if (heap_.size() < k_) {
            heap_.emplace_back(h, std::move(m));
            std::push_heap(heap_.begin(), heap_.end(), cmp);
        } else if (h < heap_.front().first) {
            std::pop_heap(heap_.begin(), heap_.end(), cmp);
            heap_.back() = Item(h, std::move(m));
            std::push_heap(heap_.begin(), heap_.end(), cmp);
        }
    }
    std::vector<Match> take() {
        std::sort(heap_.begin(), heap_.end(),
                  [](const Item& a, const Item& b) { return a.first < b.first; });
        std::vector<Match> out;
        out.reserve(heap_.size());
        for (auto& it : heap_) out.push_back(std::move(it.second));
        heap_.clear();
        return out;
    }
private:
    using Item = std::pair<uint64_t, Match>;
    size_t k_;
    uint64_t seed_;
    std::vector<Item> heap_;
};

static SeqMergeOperand seq_merge_operand(const Corpus& corpus, const ConditionPtr& cond) {
    SeqMergeOperand out;
    if (!cond) {
        out.kind = SeqMergeTok::Wildcard;
        return out;
    }
    if (!cond->is_leaf) return out;
    const AttrCondition& ac = cond->leaf;
    if (ac.op != CompOp::EQ || ac.case_insensitive || ac.diacritics_insensitive || ac.is_nvals)
        return out;
    if (ac.resolved_id < 0) return out;
    // normalize_attr is on executor; duplicate minimal alias here for form/word.
    std::string name = ac.attr;
    if (name == "word" && !corpus.has_attr("word") && corpus.has_attr("form"))
        name = "form";
    if (!corpus.has_attr(name) || corpus.is_multivalue(name)) return out;
    out.span = corpus.attr(name).rev_span_of_id(static_cast<LexiconId>(ac.resolved_id));
    out.kind = SeqMergeTok::EqRev;
    return out;
}

/// First index j in [lo, B.count) with B.at(j) >= target (B.count if none).
/// Exponential ("galloping") search from `lo`, then binary search inside the
/// bracket: O(log gap) comparisons, and the probes stay near the cursor.
static inline size_t gallop_rev(const RevSpan& B, size_t lo, CorpusPos target) {
    if (B.lazy) return B.lower_bound(lo, target);   // P4.2b: skip table, one block decoded
    const size_t n = B.count;
    if (lo >= n || B.at(lo) >= target) return lo;
    size_t prev = lo;            // invariant: B.at(prev) < target
    size_t step = 1;
    size_t hi = lo + 1;
    while (hi < n && B.at(hi) < target) {
        prev = hi;
        step <<= 1;
        hi = prev + step;
    }
    if (hi > n) hi = n;          // answer in (prev, hi]
    size_t L = prev + 1, R = hi;
    while (L < R) {
        size_t mid = L + ((R - L) >> 1);
        if (B.at(mid) < target) L = mid + 1;
        else R = mid;
    }
    return L;
}

/// Cursor over a *flat* (sorted, non-overlapping) region array for ascending
/// positions: find(pos) returns the region containing pos or -1. Advances by
/// exponential search on region ends, so sparse positions skip whole runs of
/// regions instead of stepping through them (sentence tables are ~5 GB at 6B tokens).
struct FlatRegionCursor {
    const Region* r = nullptr;
    size_t n = 0;
    size_t cur = 0;
    FlatRegionCursor() = default;
    explicit FlatRegionCursor(const StructuralAttr& sa)
        : r(sa.region_data()), n(sa.region_count()) {}
    int64_t find(CorpusPos pos) {
        if (cur < n && r[cur].end < pos) {
            size_t prev = cur, step = 1, hi = cur + 1;   // r[prev].end < pos
            while (hi < n && r[hi].end < pos) {
                prev = hi;
                step <<= 1;
                hi = prev + step;
            }
            if (hi > n) hi = n;
            size_t L = prev + 1, R = hi;
            while (L < R) {
                size_t mid = L + ((R - L) >> 1);
                if (r[mid].end < pos) L = mid + 1;
                else R = mid;
            }
            cur = L;
        }
        if (cur < n && r[cur].start <= pos) return static_cast<int64_t>(cur);
        return -1;
    }
};

/// gallop_ptr on a lazily decoded list: the skip table finds the block.
template<typename T>
static inline size_t gallop_ptr(const LazyAcc<T>& a, size_t n, size_t lo, CorpusPos target) {
    return a.lp->lower_bound(a.base + lo, a.base + n, target) - a.base;
}

/// One list 8x longer than the other: the kernels gallop into the long one, so
/// it is read sparsely (with_accs `sparse`).
static inline bool skewed_lists(const RevSpan& a, const RevSpan& b) {
    return a.count > (b.count << 3) || b.count > (a.count << 3);
}

/// f(a, b) with element accessors of the two spans (same width; false if not).
/// sparse: packed postings as LazyAcc<T> (P4.2b: the kernel decodes only the
/// blocks it reads — region intervals, gallops); otherwise, for kernels that read
/// every posting (often several times), the decoded arrays (`const T*`, whole()):
/// a block-state test per read costs more than decoding everything once.
template <typename F>
static bool with_accs(const RevSpan& A, const RevSpan& B, bool sparse, F&& f) {
    if (A.width != B.width) return false;
    auto go = [&](auto tag) {
        using T = decltype(tag);
        if (!sparse) {
            f(static_cast<const T*>(A.whole()), static_cast<const T*>(B.whole()));
            return;
        }
        A.template with_acc<T>([&](const auto& a) { B.template with_acc<T>([&](const auto& b) { f(a, b); }); });
    };
    switch (A.width) {
        case 2: go(int16_t{}); return true;
        case 4: go(int32_t{}); return true;
        case 8: go(int64_t{}); return true;
        default: return false;
    }
}

/// Balanced two-pointer shift-merge on typed posting arrays (width dispatched once
/// by the caller instead of a switch per element in RevSpan::at).
template<typename AccA, typename AccB, typename Emit>
static bool shift_merge_typed(const AccA& a, size_t na, const AccB& b, size_t nb, int64_t delta,
                              Emit& emit) {
    size_t ia = 0, ib = 0;
    while (ia < na && ib < nb) {
        const CorpusPos av = static_cast<CorpusPos>(a[ia]);
        const CorpusPos need = av + delta;
        const CorpusPos bv = static_cast<CorpusPos>(b[ib]);
        if (need == bv) {
            if (!emit(av)) return false;
            ++ia;
            ++ib;
        } else {
            ia += (need < bv);
            ib += (need > bv);
        }
    }
    return true;
}

/// Emit every `a` in A such that `a + delta` is in B (both sorted, unique positions).
/// `emit(a)` return false to stop. Returns false if stopped early.
template<typename Emit>
static bool shift_merge_rev(const RevSpan& A, const RevSpan& B, int64_t delta, Emit&& emit) {
    if (A.empty() || B.empty()) return true;
    size_t ia = 0, ib = 0;
    const size_t na = A.count, nb = B.count;
    // Gallop when B is much larger: for each a, binary-search a+delta in B.
    if (nb > (na << 3)) {
        size_t lo = 0;
        while (ia < na) {
            CorpusPos need = A.at(ia) + delta;
            size_t L = gallop_rev(B, lo, need);
            if (L >= nb) break;
            lo = L;
            if (B.at(L) == need) {
                if (!emit(A.at(ia))) return false;
                ++lo;
            }
            ++ia;
        }
        return true;
    }
    if (na > (nb << 3)) {
        // Symmetric: walk B, seek a = b - delta in A.
        size_t lo = 0;
        while (ib < nb) {
            CorpusPos b = B.at(ib);
            CorpusPos need = b - delta;
            size_t L = gallop_rev(A, lo, need);
            if (L >= na) break;
            lo = L;
            if (A.at(L) == need) {
                if (!emit(need)) return false;
                ++lo;
            }
            ++ib;
        }
        return true;
    }
    // Balanced two-pointer merge
    {
        bool r = true;
        if (with_accs(A, B, false, [&](const auto& a, const auto& b) { r = shift_merge_typed(a, na, b, nb, delta, emit); }))
            return r;
    }
    while (ia < na && ib < nb) {
        CorpusPos a = A.at(ia);
        CorpusPos b = B.at(ib);
        CorpusPos need = a + delta;
        if (need == b) {
            if (!emit(a)) return false;
            ++ia;
            ++ib;
        } else if (need < b) {
            ++ia;
        } else {
            ++ib;
        }
    }
    return true;
}

/// Fill a corpus-sized bitset from sorted `.rev` postings (dense-side operand for dep joins).
static void rev_span_to_bitset(const RevSpan& span, std::vector<uint64_t>& bits,
                               CorpusPos corpus_size) {
    bits.assign(static_cast<size_t>((corpus_size + 63) / 64), 0);
    uint64_t* b = bits.data();
    auto fill = [&](const auto& p, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            const CorpusPos v = static_cast<CorpusPos>(p[i]);
            if (v < 0 || v >= corpus_size) continue;
            b[static_cast<size_t>(v) >> 6] |= (uint64_t{1} << (static_cast<size_t>(v) & 63));
        }
    };
    if (span.empty()) return;
    switch (span.width) {
        case 2: fill(static_cast<const int16_t*>(span.whole()), span.count); break;
        case 4: fill(static_cast<const int32_t*>(span.whole()), span.count); break;
        default: fill(static_cast<const int64_t*>(span.whole()), span.count); break;
    }
}

static inline bool bitset_test(const std::vector<uint64_t>& bits, CorpusPos p) {
    if (p < 0) return false;
    size_t i = static_cast<size_t>(p) >> 6;
    if (i >= bits.size()) return false;
    return (bits[i] & (uint64_t{1} << (static_cast<size_t>(p) & 63))) != 0;
}

static inline void bitset_set(std::vector<uint64_t>& bits, CorpusPos p) {
    if (p < 0) return;
    size_t i = static_cast<size_t>(p) >> 6;
    if (i >= bits.size()) return;
    bits[i] |= (uint64_t{1} << (static_cast<size_t>(p) & 63));
}

static inline void bitset_and_inplace(std::vector<uint64_t>& a, const std::vector<uint64_t>& b) {
    size_t n = std::min(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) a[i] &= b[i];
}

/// Set every bit in [lo, hi] inclusive (corpus positions).
static void bitset_set_range(std::vector<uint64_t>& bits, CorpusPos lo, CorpusPos hi) {
    if (lo < 0) lo = 0;
    if (hi < lo) return;
    size_t max_bit = bits.size() * 64;
    if (static_cast<size_t>(lo) >= max_bit) return;
    if (static_cast<size_t>(hi) >= max_bit)
        hi = static_cast<CorpusPos>(max_bit - 1);

    size_t a = static_cast<size_t>(lo);
    size_t b = static_cast<size_t>(hi);
    size_t wa = a >> 6, wb = b >> 6;
    if (wa == wb) {
        uint64_t mask = (~uint64_t{0} << (a & 63)) &
                        (~uint64_t{0} >> (63 - (b & 63)));
        bits[wa] |= mask;
        return;
    }
    bits[wa] |= ~uint64_t{0} << (a & 63);
    for (size_t w = wa + 1; w < wb; ++w) bits[w] = ~uint64_t{0};
    bits[wb] |= ~uint64_t{0} >> (63 - (b & 63));
}

/// Result of compiling EQ global region filters (e.g. `within text_langcode="en"`)
/// into a corpus position bitset via structure attr `.rev`.
enum class RegionMaskStatus : uint8_t {
    None,           // no filters — do not use a mask
    Unsatisfiable,  // no matching regions
    Ready,          // `bits` valid
    Unsupported     // non-EQ / no rev — caller must post-filter only
};

/// Positions allowed by EQ global region filters (P1.7 / P1.11).
/// Normally a sorted list of disjoint position intervals (one per matching
/// region, merged, intersected across filters): no corpus-sized memory, and
/// posting lists can be *sliced* to the intervals instead of tested per
/// position. Only when the intervals are many and short (average below
/// kMaskMinAvgInterval tokens) is it turned into a corpus bitset.
struct PosInterval {
    CorpusPos s, e;  // inclusive
};
static constexpr CorpusPos kMaskMinAvgInterval = 512;

struct RegionPosMask {
    RegionMaskStatus status = RegionMaskStatus::None;
    std::vector<PosInterval> iv;   // sorted, disjoint (when Ready)
    bool use_bits = false;         // fine-grained: `bits` also valid
    std::vector<uint64_t> bits;
    /// P4.1: the executor's position range is folded in (a path that applies
    /// this mask to every hit it counts honours the range).
    bool ranged = false;
    /// P4.1: … and there are no region filters: the mask is exactly the range
    /// (paths may slice their operands to it instead of testing positions).
    bool range_only = false;

    bool ready() const { return status == RegionMaskStatus::Ready; }
    /// Membership for positions in arbitrary order.
    bool contains(CorpusPos p) const {
        if (use_bits) return bitset_test(bits, p);
        auto it = std::upper_bound(iv.begin(), iv.end(), p,
                                   [](CorpusPos v, const PosInterval& x) { return v < x.s; });
        if (it == iv.begin()) return false;
        --it;
        return p <= it->e;
    }
};

/// Membership for ascending positions (amortised O(1), galloping over intervals).
struct MaskCursor {
    const RegionPosMask* m = nullptr;
    size_t cur = 0;
    MaskCursor() = default;
    explicit MaskCursor(const RegionPosMask& mm) : m(&mm) {}
    bool contains(CorpusPos p) {
        if (m->use_bits) return bitset_test(m->bits, p);
        const auto& v = m->iv;
        const size_t n = v.size();
        if (cur < n && v[cur].e < p) {
            size_t prev = cur, step = 1, hi = cur + 1;
            while (hi < n && v[hi].e < p) {
                prev = hi;
                step <<= 1;
                hi = prev + step;
            }
            if (hi > n) hi = n;
            size_t L = prev + 1, R = hi;
            while (L < R) {
                size_t mid = L + ((R - L) >> 1);
                if (v[mid].e < p) L = mid + 1;
                else R = mid;
            }
            cur = L;
        }
        return cur < n && v[cur].s <= p;
    }
    /// Interval mode only, after contains(p) == false: no interval at or after p.
    bool exhausted() const { return !m->use_bits && cur >= m->iv.size(); }
    /// Interval mode only, after contains(p) == false: start of the next interval.
    CorpusPos next_start() const { return m->iv[cur].s; }
    /// Interval mode only, after contains(p) == true: the interval containing p.
    const PosInterval& current() const { return m->iv[cur]; }
};

static std::vector<PosInterval> intersect_intervals(const std::vector<PosInterval>& a,
                                                    const std::vector<PosInterval>& b) {
    std::vector<PosInterval> out;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        const CorpusPos lo = std::max(a[i].s, b[j].s);
        const CorpusPos hi = std::min(a[i].e, b[j].e);
        if (lo <= hi) out.push_back({lo, hi});
        if (a[i].e < b[j].e) ++i;
        else ++j;
    }
    return out;
}

static RegionPosMask build_region_eq_position_mask(
        const Corpus& corpus,
        const std::vector<GlobalRegionFilter>& filters) {
    RegionPosMask out;
    if (filters.empty()) return out;

    CorpusPos ntok = corpus.size();
    bool any = false;
    for (const auto& gf : filters) {
        if (gf.op != CompOp::EQ || !gf.anchor_name.empty()) {
            out = RegionPosMask{};
            out.status = RegionMaskStatus::Unsupported;
            return out;
        }
        RegionAttrParts parts;
        if (!split_region_attr_name(gf.region_attr, parts) ||
            !corpus.has_structure(parts.struct_name)) {
            out = RegionPosMask{};
            out.status = RegionMaskStatus::Unsatisfiable;
            return out;
        }
        const auto& sa = corpus.structure(parts.struct_name);
        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        if (!rkey) {
            out = RegionPosMask{};
            out.status = RegionMaskStatus::Unsatisfiable;
            return out;
        }
        const int64_t* regions = nullptr;
        size_t nreg = 0;
        if (!sa.regions_for_value(*rkey, gf.value, regions, nreg)) {
            out = RegionPosMask{};
            out.status = RegionMaskStatus::Unsupported;
            return out;
        }
        std::vector<PosInterval> layer;
        layer.reserve(nreg);
        for (size_t i = 0; i < nreg; ++i) {
            size_t ri = static_cast<size_t>(regions[i]);
            if (ri >= sa.region_count()) continue;
            Region r = sa.get(ri);
            if (r.start > r.end || r.start >= ntok) continue;
            layer.push_back({std::max<CorpusPos>(r.start, 0), std::min(r.end, ntok - 1)});
        }
        std::sort(layer.begin(), layer.end(),
                  [](const PosInterval& x, const PosInterval& y) { return x.s < y.s; });
        std::vector<PosInterval> merged;   // union of this filter's regions
        for (const auto& x : layer) {
            if (!merged.empty() && x.s <= merged.back().e + 1)
                merged.back().e = std::max(merged.back().e, x.e);
            else
                merged.push_back(x);
        }
        out.iv = any ? intersect_intervals(out.iv, merged) : std::move(merged);
        any = true;
        if (out.iv.empty()) {
            out = RegionPosMask{};
            out.status = RegionMaskStatus::Unsatisfiable;
            return out;
        }
    }
    out.status = RegionMaskStatus::Ready;
    // PANDO_MASK_BITS=1 forces the bitset representation (testing both paths).
    static const bool force_bits = [] {
        const char* v = std::getenv("PANDO_MASK_BITS");
        return v && *v && *v != '0';
    }();
    if (force_bits || static_cast<CorpusPos>(out.iv.size()) * kMaskMinAvgInterval > ntok) {
        out.use_bits = true;
        out.bits.assign(static_cast<size_t>((ntok + 63) / 64), 0);
        for (const auto& x : out.iv) bitset_set_range(out.bits, x.s, x.e);
    }
    return out;
}

/// P4.1: fold a partition range [lo, hi) into a start mask. Region filters that
/// cannot be expressed as a mask (Unsupported) are left alone: the path then
/// post-filters over the whole corpus, does not honour the range, and so must
/// not claim to (it checks `ranged`). An unsatisfiable filter stays so (no hit
/// in any range). Always intervals: a corpus-sized bitset per worker would
/// multiply the stop-gap memory by the thread count (PANDO_MASK_BITS=1 still
/// forces bits, for the differential tests).
static void restrict_mask_to_range(RegionPosMask& m, const PosRange& r, CorpusPos ntok) {
    if (m.status == RegionMaskStatus::Unsupported || m.status == RegionMaskStatus::Unsatisfiable)
        return;
    const CorpusPos lo = std::max<CorpusPos>(r.lo, 0);
    const CorpusPos hi = std::min<CorpusPos>(r.hi, ntok);
    const bool was_none = m.status == RegionMaskStatus::None;
    std::vector<PosInterval> riv;
    if (hi > lo) riv.push_back({lo, hi - 1});
    m.iv = was_none ? std::move(riv) : intersect_intervals(m.iv, riv);
    m.ranged = true;
    m.range_only = was_none;
    m.bits.clear();
    m.use_bits = false;
    if (m.iv.empty()) {
        m.status = RegionMaskStatus::Unsatisfiable;
        return;
    }
    m.status = RegionMaskStatus::Ready;
    static const bool force_bits = [] {
        const char* v = std::getenv("PANDO_MASK_BITS");
        return v && *v && *v != '0';
    }();
    if (force_bits) {
        m.use_bits = true;
        m.bits.assign(static_cast<size_t>((ntok + 63) / 64), 0);
        for (const auto& x : m.iv) bitset_set_range(m.bits, x.s, x.e);
    }
}

/// Sub-span [lo, hi) of a `.rev` posting span (zero-copy).
static RevSpan rev_slice(const RevSpan& sp, size_t lo, size_t hi) { return sp.slice(lo, hi); }

/// Typed-array version of gallop_rev: first index >= lo with a[j] >= target.
template<typename T>
static inline size_t gallop_ptr(const T* a, size_t n, size_t lo, CorpusPos target) {
    if (lo >= n || static_cast<CorpusPos>(a[lo]) >= target) return lo;
    size_t prev = lo, step = 1, hi = lo + 1;
    while (hi < n && static_cast<CorpusPos>(a[hi]) < target) {
        prev = hi;
        step <<= 1;
        hi = prev + step;
    }
    if (hi > n) hi = n;
    size_t L = prev + 1, R = hi;
    while (L < R) {
        size_t mid = L + ((R - L) >> 1);
        if (static_cast<CorpusPos>(a[mid]) < target) L = mid + 1;
        else R = mid;
    }
    return L;
}

/// P1.11: fixed-size (32 KB) ring bitset over a sliding position window, for
/// dependency joins. Heads are always within ±32767 tokens of their dependent
/// (sentence-local int16), so a window of 2^18 positions replaces the old
/// corpus-sized bitset (~690 MB per query at 5.5B tokens) and stays in L1/L2.
/// Positions below `dead` are no longer needed; set()/test() require
/// dead <= p < dead + kWindow.
struct SlidingBitset {
    static constexpr CorpusPos kWindow = CorpusPos{1} << 18;
    static constexpr size_t kWords = static_cast<size_t>(kWindow / 64);
    uint64_t w[kWords] = {};
    CorpusPos dead = 0;

    void advance_dead(CorpusPos to) {
        if (to <= dead) return;
        if (to - dead >= kWindow) {
            std::fill(w, w + kWords, uint64_t{0});
        } else {
            clear_slots(dead, to);
        }
        dead = to;
    }
    /// Empty window starting at `lo` (valid for [lo, lo + kWindow)).
    void reset(CorpusPos lo) {
        std::fill(w, w + kWords, uint64_t{0});
        dead = lo;
    }
    void set(CorpusPos p) {
        const size_t b = static_cast<size_t>(p & (kWindow - 1));
        w[b >> 6] |= uint64_t{1} << (b & 63);
    }
    bool test(CorpusPos p) const {
        const size_t b = static_cast<size_t>(p & (kWindow - 1));
        return (w[b >> 6] >> (b & 63)) & 1u;
    }
private:
    // Clear ring slots of positions [a, b), b - a < kWindow.
    void clear_slots(CorpusPos a, CorpusPos b) {
        size_t lo = static_cast<size_t>(a & (kWindow - 1));
        size_t n = static_cast<size_t>(b - a);
        while (n > 0) {
            size_t run = std::min(n, static_cast<size_t>(kWindow) - lo);
            clear_linear(lo, lo + run);
            n -= run;
            lo = 0;
        }
    }
    void clear_linear(size_t a, size_t b) {   // bits [a, b)
        if (a >= b) return;
        size_t wa = a >> 6, wb = (b - 1) >> 6;
        if (wa == wb) {
            uint64_t mask = (~uint64_t{0} << (a & 63)) & (~uint64_t{0} >> (63 - ((b - 1) & 63)));
            w[wa] &= ~mask;
            return;
        }
        w[wa] &= ~(~uint64_t{0} << (a & 63));
        for (size_t k = wa + 1; k < wb; ++k) w[k] = 0;
        w[wb] &= ~(~uint64_t{0} >> (63 - ((b - 1) & 63)));
    }
};
static constexpr CorpusPos kMaxHeadDistance = 32768;   // int16 sentence-local heads

static void collect_token_and_region_labels(const TokenQuery& q,
                                            std::unordered_set<std::string>* token_labels,
                                            std::unordered_set<std::string>* region_labels) {
    for (const auto& t : q.tokens) {
        if (t.name.empty()) continue;
        if (t.is_anchor()) {
            region_labels->insert(t.name);
            for (const auto& cl : t.anchor_region_clauses)
                if (!cl.peer_label.empty()) region_labels->insert(cl.peer_label);
        } else {
            token_labels->insert(t.name);
        }
    }
}

/// `count / freq / group by L.attr`: `L` must label a token or region of the query
/// (an unknown label used to give no buckets at all). `text.langcode` with no `text`
/// label is the common case: the attribute of the containing region is `text_langcode`.
static void check_aggregate_labels(const Corpus& corpus, const TokenQuery& q,
                                   const std::vector<std::string>& fields) {
    std::unordered_set<std::string> t, r;
    collect_token_and_region_labels(q, &t, &r);
    for (const std::string& field : fields) {
        if (field.find('(') != std::string::npos) continue;   // tcnt(…), year(…), …
        const size_t dot = field.find('.');
        if (dot == std::string::npos || dot == 0) continue;
        const std::string label = field.substr(0, dot);
        if (label == "match" || t.count(label) || r.count(label)) continue;
        std::string msg = "by " + field + ": '" + label + "' is not a label in the query";
        if (corpus.has_structure(label))
            msg += "; for the " + label + " containing the hit use " + label + "_" + field.substr(dot + 1);
        throw std::runtime_error(msg);
    }
}

static void expect_token_label(const std::string& name, const std::string& ctx,
                               const std::unordered_set<std::string>& token_labels,
                               const std::unordered_set<std::string>& region_labels) {
    if (name.empty()) return;
    if (token_labels.count(name)) return;
    if (region_labels.count(name)) {
        throw std::runtime_error(
            ctx + ": '" + name +
            "' refers to a region anchor binding; this construct requires a named query token "
            "(e.g. verb:[])");
    }
    throw std::runtime_error(ctx + ": unknown name '" + name + "'");
}

static void expect_alignment_operand_label(const std::string& name, const std::string& ctx,
                                           const std::unordered_set<std::string>& token_labels,
                                           const std::unordered_set<std::string>& region_labels) {
    if (name.empty()) return;
    if (token_labels.count(name) || region_labels.count(name)) return;
    throw std::runtime_error(ctx + ": unknown name '" + name + "' (neither this query nor an earlier "
                             "statement of the program / session binds it)");
}

static const char* struct_rel_keyword(StructRelType t) {
    switch (t) {
        case StructRelType::CHILD: return "child";
        case StructRelType::PARENT: return "parent";
        case StructRelType::SIBLING: return "sibling";
        case StructRelType::DESCENDANT: return "descendant";
        case StructRelType::ANCESTOR: return "ancestor";
    }
    return "structural";
}

static void validate_conditions_token_labels(const ConditionPtr& cond,
                                             const std::unordered_set<std::string>& token_labels,
                                             const std::unordered_set<std::string>& region_labels) {
    if (!cond) return;
    if (cond->is_leaf) return;
    if (cond->is_structural) {
        if (!cond->nested_name.empty()) {
            std::string ctx = std::string(struct_rel_keyword(cond->struct_rel)) + " " + cond->nested_name +
                              ":[…]";
            expect_token_label(cond->nested_name, ctx, token_labels, region_labels);
        }
        validate_conditions_token_labels(cond->nested_conditions, token_labels, region_labels);
        return;
    }
    if (cond->is_count) {
        validate_conditions_token_labels(cond->count_filter, token_labels, region_labels);
        return;
    }
    validate_conditions_token_labels(cond->left, token_labels, region_labels);
    validate_conditions_token_labels(cond->right, token_labels, region_labels);
}

static void append_func_call_token_args(const GlobalFuncCall& fc, std::vector<std::string>* out) {
    switch (fc.func) {
        case GlobalFunctionType::DISTANCE:
        case GlobalFunctionType::DISTABS:
            if (fc.args.size() >= 2) {
                out->push_back(fc.args[0]);
                out->push_back(fc.args[1]);
            }
            break;
        case GlobalFunctionType::STRLEN:
        case GlobalFunctionType::FREQ:
        case GlobalFunctionType::YEAR:
        case GlobalFunctionType::CENTURY:
        case GlobalFunctionType::DECADE:
        case GlobalFunctionType::MONTH:
        case GlobalFunctionType::WEEK:
        case GlobalFunctionType::DAY:
        case GlobalFunctionType::NVALS:
            if (!fc.args.empty()) {
                auto dot = fc.args[0].find('.');
                if (dot != std::string::npos && dot > 0) out->push_back(fc.args[0].substr(0, dot));
            }
            break;
        case GlobalFunctionType::NCHILDREN:
        case GlobalFunctionType::DEPTH:
        case GlobalFunctionType::NDESCENDANTS:
            if (!fc.args.empty()) out->push_back(fc.args[0]);
            break;
        case GlobalFunctionType::CONTAINS:
        case GlobalFunctionType::RCHILD:
        case GlobalFunctionType::RCONTAINS:
            break;
        case GlobalFunctionType::TCNT:
            if (!fc.args.empty()) out->push_back(fc.args[0]);
            break;
    }
}

static void expect_region_label(const std::string& name, const std::string& ctx,
                                const std::unordered_set<std::string>& token_labels,
                                const std::unordered_set<std::string>& region_labels) {
    if (name.empty()) return;
    if (region_labels.count(name)) return;
    if (token_labels.count(name)) {
        throw std::runtime_error(
            ctx + ": '" + name +
            "' is a token label; these filters expect named region bindings (e.g. s:<s> …)");
    }
    throw std::runtime_error(ctx + ": unknown name '" + name + "'");
}

static void validate_global_func_filter_token_labels(const GlobalFunctionFilter& ff,
                                                     const std::unordered_set<std::string>& token_labels,
                                                     const std::unordered_set<std::string>& region_labels) {
    auto check_call = [&](const GlobalFuncCall& fc) {
        if (fc.func == GlobalFunctionType::YEAR || fc.func == GlobalFunctionType::CENTURY
            || fc.func == GlobalFunctionType::DECADE || fc.func == GlobalFunctionType::MONTH
            || fc.func == GlobalFunctionType::WEEK || fc.func == GlobalFunctionType::DAY) {
            if (fc.args.size() != 1) {
                const char* msg = "date function in global filter requires exactly one argument";
                switch (fc.func) {
                    case GlobalFunctionType::YEAR:    msg = "year(...) in global filter requires exactly one argument"; break;
                    case GlobalFunctionType::CENTURY: msg = "century(...) in global filter requires exactly one argument"; break;
                    case GlobalFunctionType::DECADE:  msg = "decade(...) in global filter requires exactly one argument"; break;
                    case GlobalFunctionType::MONTH:   msg = "month(...) in global filter requires exactly one argument"; break;
                    case GlobalFunctionType::WEEK:    msg = "week(...) in global filter requires exactly one argument"; break;
                    case GlobalFunctionType::DAY:     msg = "day(...) in global filter requires exactly one argument"; break;
                    default: break;
                }
                throw std::runtime_error(msg);
            }
            const std::string& spec = fc.args[0];
            auto dot = spec.find('.');
            if (dot != std::string::npos && dot > 0) {
                const std::string prefix = spec.substr(0, dot);
                if (!token_labels.count(prefix) && !region_labels.count(prefix)) {
                    throw std::runtime_error(
                            "Global filter (:: ...): unknown name '" + prefix + "' in date function");
                }
            }
            return;
        }
        if (fc.func == GlobalFunctionType::TCNT) {
            if (fc.args.size() != 1)
                throw std::runtime_error("tcnt(...) in global filter requires exactly one label");
            const std::string& n = fc.args[0];
            if (token_labels.count(n) || region_labels.count(n)) return;
            throw std::runtime_error(
                    "Global filter (:: …): tcnt(...) unknown label '" + n + "'");
        }
        if (fc.func == GlobalFunctionType::CONTAINS || fc.func == GlobalFunctionType::RCHILD
            || fc.func == GlobalFunctionType::RCONTAINS) {
            if (fc.args.size() < 2)
                throw std::runtime_error(
                        "contains(outer, inner), rchild(parent, child), and rcontains(ancestor, descendant) "
                        "require two named region labels");
            const char* ctx = fc.func == GlobalFunctionType::CONTAINS   ? "contains(…)"
                             : fc.func == GlobalFunctionType::RCHILD    ? "rchild(…)"
                                                                        : "rcontains(…)";
            expect_region_label(fc.args[0], ctx, token_labels, region_labels);
            expect_region_label(fc.args[1], ctx, token_labels, region_labels);
            return;
        }
        std::vector<std::string> names;
        append_func_call_token_args(fc, &names);
        for (const auto& n : names)
            expect_token_label(n, "Global filter (:: …)", token_labels, region_labels);
    };
    check_call(ff.lhs);
    if (ff.has_rhs_func) check_call(ff.rhs);
}

/// True iff `inner`'s inclusive token span lies inside `outer`'s (Layer A geometry).
static bool region_span_contains(const Region& outer, const Region& inner) {
    return inner.start >= outer.start && inner.end <= outer.end;
}

static std::optional<int64_t> parse_year_prefix(std::string_view text) {
    size_t i = 0;
    while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
    if (i >= text.size()) return std::nullopt;

    bool neg = false;
    if (text[i] == '+' || text[i] == '-') {
        neg = text[i] == '-';
        ++i;
    }

    if (i + 4 > text.size()) return std::nullopt;
    for (size_t k = 0; k < 4; ++k) {
        if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
            return std::nullopt;
    }
    if (i + 4 < text.size() && std::isdigit(static_cast<unsigned char>(text[i + 4])))
        return std::nullopt;

    int64_t year = 0;
    for (size_t k = 0; k < 4; ++k)
        year = year * 10 + static_cast<int64_t>(text[i + k] - '0');
    if (neg) year = -year;
    return year;
}

static std::optional<int64_t> century_from_year(int64_t year) {
    if (year <= 0) return std::nullopt;
    return ((year - 1) / 100) + 1;
}

static int64_t decade_from_year(int64_t year) {
    return (year / 10) * 10;
}

struct ParsedDateParts {
    int64_t year = 0;
    int month = 0;
    int day = 0;
    bool has_month = false;
    bool has_day = false;
};

static bool is_leap_year(int64_t y) {
    return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));
}

static int days_in_month(int64_t y, int m) {
    static const int kDays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
    if (m == 2) return is_leap_year(y) ? 29 : 28;
    return kDays[m - 1];
}

static std::optional<ParsedDateParts> parse_date_parts_prefix(std::string_view text) {
    size_t i = 0;
    while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
    if (i + 4 > text.size()) return std::nullopt;
    for (size_t k = 0; k < 4; ++k) {
        if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
            return std::nullopt;
    }
    ParsedDateParts out;
    out.year = 0;
    for (size_t k = 0; k < 4; ++k)
        out.year = out.year * 10 + static_cast<int64_t>(text[i + k] - '0');
    i += 4;
    if (i >= text.size() || (text[i] != '-' && text[i] != '/'))
        return out;
    char sep = text[i++];
    if (i + 2 > text.size()) return std::nullopt;
    if (!std::isdigit(static_cast<unsigned char>(text[i]))
        || !std::isdigit(static_cast<unsigned char>(text[i + 1])))
        return std::nullopt;
    out.month = static_cast<int>((text[i] - '0') * 10 + (text[i + 1] - '0'));
    if (out.month < 1 || out.month > 12) return std::nullopt;
    out.has_month = true;
    i += 2;
    if (i >= text.size() || text[i] != sep)
        return out;
    ++i;
    if (i + 2 > text.size()) return std::nullopt;
    if (!std::isdigit(static_cast<unsigned char>(text[i]))
        || !std::isdigit(static_cast<unsigned char>(text[i + 1])))
        return std::nullopt;
    out.day = static_cast<int>((text[i] - '0') * 10 + (text[i + 1] - '0'));
    if (out.day < 1 || out.day > days_in_month(out.year, out.month))
        return std::nullopt;
    out.has_day = true;
    return out;
}

static int weekday_iso(int64_t y, int m, int d) {
    // Sakamoto: 0=Sunday..6=Saturday
    static const int t[12] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
    int64_t yy = y;
    if (m < 3) --yy;
    int w = static_cast<int>((yy + yy/4 - yy/100 + yy/400 + t[m - 1] + d) % 7);
    if (w < 0) w += 7;
    return w == 0 ? 7 : w; // Monday=1..Sunday=7
}

static int iso_weeks_in_year(int64_t y) {
    const int jan1 = weekday_iso(y, 1, 1);
    if (jan1 == 4) return 53;
    if (jan1 == 3 && is_leap_year(y)) return 53;
    return 52;
}

static int day_of_year(int64_t y, int m, int d) {
    int doy = d;
    for (int mm = 1; mm < m; ++mm)
        doy += days_in_month(y, mm);
    return doy;
}

static int iso_week_number(int64_t y, int m, int d) {
    int doy = day_of_year(y, m, d);
    int dow = weekday_iso(y, m, d); // Mon=1..Sun=7
    int week = (doy - dow + 10) / 7;
    if (week < 1) return iso_weeks_in_year(y - 1);
    int wiy = iso_weeks_in_year(y);
    if (week > wiy) return 1;
    return week;
}

static std::optional<Region> named_region_span(const Corpus& corpus, const Match& m,
                                               const std::string& label) {
    auto it = m.named_regions.find(label);
    if (it == m.named_regions.end()) return std::nullopt;
    const RegionRef& rr = it->second;
    if (!corpus.has_structure(rr.struct_name)) return std::nullopt;
    const auto& sa = corpus.structure(rr.struct_name);
    if (rr.region_idx >= sa.region_count()) return std::nullopt;
    return sa.get(rr.region_idx);
}

} // namespace

// RG-5f: Multivalue membership — check if any pipe-separated component of `val`
// equals `target`.  Falls back to direct comparison when no pipe is present.
// True if sorted MV component ids at pos contain want (forward index).
static bool mv_fwd_contains_sorted(const PositionalAttr& pa, CorpusPos pos,
                                   LexiconId want) {
    if (want < 0) return false;
    bool found = false;
    pa.for_each_mv_fwd_at(pos, [&](LexiconId id) {
        if (id == want) {
            found = true;
            return false;
        }
        if (id > want) return false;
        return true;
    });
    return found;
}

// MV-002: non-empty intersection of component sets (string keys), using forward
// index when available else pipe-split / scalar value_at.
static void collect_mv_component_strings(const PositionalAttr& pa, CorpusPos pos,
                                         bool is_mv_declared,
                                         std::unordered_set<std::string>& out) {
    if (!is_mv_declared) {
        std::string_view v = pa.value_at(pos);
        if (!v.empty() && v != "_") out.insert(std::string(v));
        return;
    }
    if (pa.has_mv_fwd()) {
        pa.for_each_mv_fwd_at(pos, [&](LexiconId id) {
            out.insert(std::string(pa.mv_lexicon().get(id)));
            return true;
        });
        return;
    }
    {
        std::string_view val = pa.value_at(pos);
        size_t start = 0;
        while (start <= val.size()) {
            size_t p = val.find('|', start);
            size_t end = (p == std::string_view::npos) ? val.size() : p;
            if (end > start) out.insert(std::string(val.substr(start, end - start)));
            if (p == std::string_view::npos) break;
            start = p + 1;
        }
    }
}

static bool alignment_attr_is_supported(const Corpus& corpus, const std::string& attr) {
    if (corpus.has_attr(attr)) return true;
    RegionAttrParts parts;
    if (!split_region_attr_name(attr, parts)) return false;
    if (!corpus.has_structure(parts.struct_name)) return false;
    const auto& sa = corpus.structure(parts.struct_name);
    return static_cast<bool>(resolve_region_attr_key(sa, parts.struct_name, parts.attr_name));
}

static std::optional<std::string_view> alignment_attr_value_at(
        const Corpus& corpus, const std::string& attr, CorpusPos pos) {
    if (corpus.has_attr(attr))
        return corpus.attr(attr).value_at(pos);

    RegionAttrParts parts;
    if (!split_region_attr_name(attr, parts) || !corpus.has_structure(parts.struct_name))
        return std::nullopt;
    const auto& sa = corpus.structure(parts.struct_name);
    auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
    if (!rkey) return std::nullopt;
    int64_t rgn = sa.find_region(pos);
    if (rgn < 0) return std::nullopt;
    return sa.region_value(*rkey, static_cast<size_t>(rgn));
}

static std::optional<std::string_view> resolve_alignment_operand_value(
        const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
        const std::string& name, const std::string& attr) {
    if (CorpusPos p = resolve_name(m, name_map, name); p != NO_HEAD)
        return alignment_attr_value_at(corpus, attr, p);

    auto nr = m.named_regions.find(name);
    if (nr == m.named_regions.end()) return std::nullopt;
    if (!corpus.has_structure(nr->second.struct_name)) return std::nullopt;
    const auto& anchor_sa = corpus.structure(nr->second.struct_name);
    if (nr->second.region_idx >= anchor_sa.region_count()) return std::nullopt;

    // Region-attribute lookup against the anchor structure itself first.
    if (auto rkey = resolve_region_attr_key(anchor_sa, nr->second.struct_name, attr))
        return anchor_sa.region_value(*rkey, nr->second.region_idx);

    // Explicit struct attr form (e.g. s_tuid, text_id).
    RegionAttrParts parts;
    if (split_region_attr_name(attr, parts) && corpus.has_structure(parts.struct_name)) {
        const auto& sa = corpus.structure(parts.struct_name);
        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        if (!rkey) return std::nullopt;
        if (parts.struct_name == nr->second.struct_name)
            return sa.region_value(*rkey, nr->second.region_idx);
        Region rg = anchor_sa.get(nr->second.region_idx);
        int64_t rgn = sa.find_region(rg.start);
        if (rgn < 0) return std::nullopt;
        return sa.region_value(*rkey, static_cast<size_t>(rgn));
    }

    // Positional fallback for region anchors: read at region start token.
    if (corpus.has_attr(attr)) {
        Region rg = anchor_sa.get(nr->second.region_idx);
        return corpus.attr(attr).value_at(rg.start);
    }

    return std::nullopt;
}

static bool alignment_value_is_missing(std::string_view v) {
    if (v.empty() || v == "_") return true;
    for (char c : v)
        if (c != '|') return false;
    return true;
}

static bool alignment_values_match(const Corpus& corpus,
                                   const std::string& an1,
                                   const std::string& an2,
                                   std::optional<std::string_view> v1,
                                   std::optional<std::string_view> v2,
                                   bool include_empty_alignment_values) {
    if (!v1 || !v2) return false;
    if (!include_empty_alignment_values
        && (alignment_value_is_missing(*v1) || alignment_value_is_missing(*v2)))
        return false;
    bool mv1 = corpus.is_multivalue(an1);
    bool mv2 = corpus.is_multivalue(an2);
    if (!mv1 && !mv2)
        return *v1 == *v2;

    std::unordered_set<std::string> s1, s2;
    auto split_components = [](std::string_view v, std::unordered_set<std::string>& out) {
        size_t start = 0;
        while (start <= v.size()) {
            size_t p = v.find('|', start);
            size_t end = (p == std::string_view::npos) ? v.size() : p;
            if (end > start) out.insert(std::string(v.substr(start, end - start)));
            if (p == std::string_view::npos) break;
            start = p + 1;
        }
    };
    if (mv1 || v1->find('|') != std::string_view::npos) split_components(*v1, s1);
    else s1.insert(std::string(*v1));
    if (mv2 || v2->find('|') != std::string_view::npos) split_components(*v2, s2);
    else s2.insert(std::string(*v2));
    if (!include_empty_alignment_values && (s1.empty() || s2.empty()))
        return false;
    for (const auto& a : s1)
        if (s2.count(a)) return true;
    return false;
}

static void collect_alignment_component_strings(const Corpus& corpus,
                                                const std::string& attr,
                                                CorpusPos pos,
                                                std::unordered_set<std::string>& out) {
    if (corpus.has_attr(attr)) {
        collect_mv_component_strings(corpus.attr(attr), pos, corpus.is_multivalue(attr), out);
        return;
    }
    auto val = alignment_attr_value_at(corpus, attr, pos);
    if (!val || val->empty() || *val == "_")
        return;
    if (corpus.is_multivalue(attr) || val->find('|') != std::string_view::npos) {
        size_t start = 0;
        while (start <= val->size()) {
            size_t p = val->find('|', start);
            size_t end = (p == std::string_view::npos) ? val->size() : p;
            if (end > start) out.insert(std::string(val->substr(start, end - start)));
            if (p == std::string_view::npos) break;
            start = p + 1;
        }
    } else {
        out.insert(std::string(*val));
    }
}

struct ParallelPairHash {
    size_t operator()(const std::pair<size_t, size_t>& p) const noexcept {
        return p.first ^ (p.second + 0x9e3779b97f4a7c15ULL + (p.first << 6) + (p.first >> 2));
    }
};

// MV-002: Source|Target join for a single `:: a.attr = b.attr` filter.
// Uses hash join when both sides are scalar; otherwise inverted index on
// component strings (same semantics as global_alignment_attrs_match).
static bool parallel_alignment_overlap_join_single(
        const Corpus& corpus,
        const std::string& an1,
        const std::string& an2,
        const GlobalAlignmentFilter& af,
        const std::vector<Match>& src,
        const std::vector<Match>& tgt,
        const NameIndexMap& src_names,
        const NameIndexMap& tgt_names,
        bool include_empty_alignment_values,
        std::vector<std::pair<Match, Match>>& out,
        size_t& total_count,
        size_t max_matches,
        bool count_total) {
    const auto& pa1 = corpus.attr(an1);
    const auto& pa2 = corpus.attr(an2);
    bool mv1 = corpus.is_multivalue(an1);
    bool mv2 = corpus.is_multivalue(an2);

    auto emit_pair = [&](size_t i, size_t j) -> bool {
        out.emplace_back(src[i], tgt[j]);
        ++total_count;
        if (max_matches > 0 && total_count >= max_matches && !count_total) return false;
        return true;
    };

    if (!mv1 && !mv2) {
        std::unordered_map<std::string, std::vector<size_t>> src_by_val;
        src_by_val.reserve(src.size() * 2);
        for (size_t i = 0; i < src.size(); ++i) {
            CorpusPos p1 = resolve_name(src[i], src_names, af.name1);
            if (p1 == NO_HEAD) continue;
            std::string_view v = pa1.value_at(p1);
            if (!include_empty_alignment_values && alignment_value_is_missing(v)) continue;
            src_by_val[std::string(v)].push_back(i);
        }
        std::unordered_set<std::pair<size_t, size_t>, ParallelPairHash> seen;
        seen.reserve(std::min(src.size() * tgt.size(), size_t{1024}));
        for (size_t j = 0; j < tgt.size(); ++j) {
            CorpusPos p2 = resolve_name(tgt[j], tgt_names, af.name2);
            if (p2 == NO_HEAD) continue;
            std::string_view v_sv = pa2.value_at(p2);
            if (!include_empty_alignment_values && alignment_value_is_missing(v_sv)) continue;
            std::string v(v_sv);
            auto it = src_by_val.find(v);
            if (it == src_by_val.end()) continue;
            for (size_t i : it->second) {
                if (!seen.insert({i, j}).second) continue;
                if (!emit_pair(i, j)) return true;
            }
        }
        return true;
    }

    std::unordered_map<std::string, std::vector<size_t>> src_by_comp;
    for (size_t i = 0; i < src.size(); ++i) {
        CorpusPos p1 = resolve_name(src[i], src_names, af.name1);
        if (p1 == NO_HEAD) continue;
        std::unordered_set<std::string> comps;
        collect_mv_component_strings(pa1, p1, mv1, comps);
        if (!include_empty_alignment_values && comps.empty()) continue;
        for (const auto& c : comps) src_by_comp[c].push_back(i);
    }
    std::unordered_set<std::pair<size_t, size_t>, ParallelPairHash> seen;
    seen.reserve(std::min(src.size() * tgt.size(), size_t{1024}));
    for (size_t j = 0; j < tgt.size(); ++j) {
        CorpusPos p2 = resolve_name(tgt[j], tgt_names, af.name2);
        if (p2 == NO_HEAD) continue;
        std::unordered_set<std::string> comps;
        collect_mv_component_strings(pa2, p2, mv2, comps);
        if (!include_empty_alignment_values && comps.empty()) continue;
        for (const auto& c : comps) {
            auto it = src_by_comp.find(c);
            if (it == src_by_comp.end()) continue;
            for (size_t i : it->second) {
                if (!seen.insert({i, j}).second) continue;
                if (!emit_pair(i, j)) return true;
            }
        }
    }
    return true;
}

// MV-002: `:: a.attr = b.attr` — scalar compares joined string; multivalue uses
// non-empty intersection of component sets.
static bool global_alignment_attrs_match(const Corpus& corpus,
                                         const std::string& an1,
                                         const std::string& an2,
                                         CorpusPos p1, CorpusPos p2) {
    if (!alignment_attr_is_supported(corpus, an1) || !alignment_attr_is_supported(corpus, an2))
        return false;

    bool pos1 = corpus.has_attr(an1);
    bool pos2 = corpus.has_attr(an2);
    bool mv1 = corpus.is_multivalue(an1);
    bool mv2 = corpus.is_multivalue(an2);
    if (pos1 && pos2 && !mv1 && !mv2) {
        return corpus.attr(an1).value_at(p1) == corpus.attr(an2).value_at(p2);
    }
    std::unordered_set<std::string> s1, s2;
    collect_alignment_component_strings(corpus, an1, p1, s1);
    collect_alignment_component_strings(corpus, an2, p2, s2);
    for (const std::string& a : s1)
        if (s2.count(a)) return true;
    return false;
}

static bool multivalue_eq(std::string_view val, const std::string& target) {
    if (val == target) return true;
    // Fast path: no pipe → single value, already compared above.
    auto pipe = val.find('|');
    if (pipe == std::string_view::npos) return false;
    // Split on '|' and check each component.
    size_t start = 0;
    while (start < val.size()) {
        size_t p = val.find('|', start);
        if (p == std::string_view::npos) p = val.size();
        if (val.substr(start, p - start) == target) return true;
        start = p + 1;
    }
    return false;
}

// Non-empty pipe-separated components (aligned with multivalue_eq / show values explode).
static size_t count_mv_components(std::string_view val) {
    size_t count = 0;
    size_t start = 0;
    while (start <= val.size()) {
        size_t p = val.find('|', start);
        size_t end = (p == std::string_view::npos) ? val.size() : p;
        if (end > start) ++count;
        if (p == std::string_view::npos) break;
        start = p + 1;
    }
    return count;
}

static void insert_mv_components(std::unordered_set<std::string>& out, std::string_view val) {
    size_t start = 0;
    while (start <= val.size()) {
        size_t p = val.find('|', start);
        size_t end = (p == std::string_view::npos) ? val.size() : p;
        if (end > start) out.insert(std::string(val.substr(start, end - start)));
        if (p == std::string_view::npos) break;
        start = p + 1;
    }
}

static int64_t nvals_from_string(std::string_view val, bool is_mv_declared) {
    if (is_mv_declared || val.find('|') != std::string_view::npos)
        return static_cast<int64_t>(count_mv_components(val));
    if (val.empty() || val == "_")
        return 0;
    return 1;
}

static bool compare_nvals_count(int64_t n, CompOp op, int64_t rhs) {
    switch (op) {
        case CompOp::EQ:  return n == rhs;
        case CompOp::NEQ: return n != rhs;
        case CompOp::LT:  return n < rhs;
        case CompOp::GT:  return n > rhs;
        case CompOp::LTE: return n <= rhs;
        case CompOp::GTE: return n >= rhs;
        case CompOp::REGEX:
        case CompOp::IN:  return false;  // IN is position-set only (check_leaf)
    }
    return false;
}

QueryExecutor::QueryExecutor(const Corpus& corpus)
    : corpus_(corpus),
      caches_(std::make_shared<Caches>()),
      fold_map_cache_(caches_->fold_map),
      fold_index_cache_(caches_->fold_index),
      dep_pair_cache_(caches_->dep_pair),
      bitmap_cache_(caches_->bitmap),
      fold_map_mutex_(caches_->mu),
      regex_cache_(caches_->regex),
      regex_cache_mutex_(caches_->regex_mu) {}

QueryExecutor::QueryExecutor(const QueryExecutor& parent, const PosRange* range,
                             ExecProgress* progress)
    : include_empty_alignment_values_(parent.include_empty_alignment_values_),
      anchor_binding_mode_(parent.anchor_binding_mode_),
      corpus_(parent.corpus_),
      range_(range),
      skip_compile_(true),
      caches_(parent.caches_),
      fold_map_cache_(caches_->fold_map),
      fold_index_cache_(caches_->fold_index),
      dep_pair_cache_(caches_->dep_pair),
      progress_(progress),
      thread_budget_(parent.thread_budget_),
      bitmap_cache_(caches_->bitmap),
      fold_map_mutex_(caches_->mu),
      regex_cache_(caches_->regex),
      regex_cache_mutex_(caches_->regex_mu) {}

const FoldMap& QueryExecutor::get_fold_map(const std::string& attr, bool case_fold, bool accent_fold) const {
    std::string key = attr + ":" + (case_fold ? "1" : "0") + (accent_fold ? "1" : "0");
    std::lock_guard<std::mutex> lock(fold_map_mutex_);
    auto it = fold_map_cache_.find(key);
    if (it != fold_map_cache_.end()) return it->second;
    const auto& lex = corpus_.attr(attr).lexicon();
    FoldMap fm;
    if (case_fold && accent_fold) fm = FoldMap::build_lc_no_accents(lex);
    else if (case_fold) fm = FoldMap::build_lowercase(lex);
    else fm = FoldMap::build_no_accents(lex);
    auto [ins, _] = fold_map_cache_.emplace(key, std::move(fm));
    return ins->second;
}

std::vector<LexiconId> QueryExecutor::fold_lookup_ids(const std::string& attr, bool case_fold,
                                                      bool accent_fold,
                                                      const std::string& value) const {
    const FoldMode mode = fold_mode_for(case_fold, accent_fold);
    const std::string folded = fold_string(mode, value);
    const auto& pa = corpus_.attr(attr);
    std::shared_ptr<FoldIndex> idx;
    {
        std::lock_guard<std::mutex> lock(fold_map_mutex_);
        const std::string key = attr + ":" + fold_mode_suffix(mode);
        auto it = fold_index_cache_.find(key);
        if (it == fold_index_cache_.end()) {
            auto fi = std::make_shared<FoldIndex>();
            if (!pa.base_path().empty() && fi->open(pa.base_path(), mode, pa.lexicon().size()))
                it = fold_index_cache_.emplace(key, fi).first;
            else
                it = fold_index_cache_.emplace(key, nullptr).first;
        }
        idx = it->second;
    }
    if (idx) return idx->lookup(pa.lexicon(), folded);
    const auto& ids = get_fold_map(attr, case_fold, accent_fold).lookup(folded);
    std::vector<LexiconId> out(ids.begin(), ids.end());
    std::sort(out.begin(), out.end());
    return out;
}

std::shared_ptr<BitmapIndex> QueryExecutor::bitmap_index(const std::string& attr) const {
    std::lock_guard<std::mutex> lock(fold_map_mutex_);
    auto it = bitmap_cache_.find(attr);
    if (it == bitmap_cache_.end()) {
        std::shared_ptr<BitmapIndex> bi;
        if (corpus_.has_attr(attr) && !corpus_.is_multivalue(attr)) {
            bi = std::make_shared<BitmapIndex>();
            if (!bi->open(corpus_.attr(attr), corpus_.size())) bi.reset();
        }
        it = bitmap_cache_.emplace(attr, bi).first;
    }
    return it->second;
}

std::shared_ptr<BitmapIndex> QueryExecutor::structure_bitmap(const std::string& name) const {
    std::lock_guard<std::mutex> lock(fold_map_mutex_);
    const std::string key = "\x01struct:" + name;
    auto it = bitmap_cache_.find(key);
    if (it == bitmap_cache_.end()) {
        std::shared_ptr<BitmapIndex> bi;
        if (corpus_.has_structure(name) && !corpus_.is_nested(name) && !corpus_.is_overlapping(name)) {
            bi = std::make_shared<BitmapIndex>();
            if (!bi->open_structure(BitmapIndex::structure_base(corpus_.dir(), name), corpus_.size()))
                bi.reset();
        }
        it = bitmap_cache_.emplace(key, bi).first;
    }
    return it->second;
}

std::shared_ptr<DepPairIndex> QueryExecutor::dep_pair_index(const std::string& head_attr,
                                                            const std::string& child_attr) const {
    std::lock_guard<std::mutex> lock(fold_map_mutex_);
    const std::string key = head_attr + "\t" + child_attr;
    auto it = dep_pair_cache_.find(key);
    if (it == dep_pair_cache_.end()) {
        auto di = std::make_shared<DepPairIndex>();
        if (!di->open(corpus_, head_attr, child_attr)) di.reset();
        it = dep_pair_cache_.emplace(key, di).first;
    }
    return it->second;
}

// ── Attribute name normalization ────────────────────────────────────────

std::string normalize_query_attr_name(const Corpus& corpus, const std::string& attr) {
    // Compatibility alias: many external tools/front-ends use `word`, while
    // TEITOK/UD-oriented corpora commonly expose only `form`.
    if (attr == "word" && !corpus.has_attr("word") && corpus.has_attr("form"))
        return "form";

    // UD positional sub-key: feats/Feature — never fold slash to dot; map to materialized split
    // column when present (feats#Feature preferred, feats_Feature legacy).
    if (attr.size() > 6 && attr.compare(0, 6, "feats/") == 0) {
        std::string feat = attr.substr(6);
        if (!feat.empty() && feat.find('/') == std::string::npos && feat.find('.') == std::string::npos) {
            std::string modern = std::string("feats#") + feat;
            if (corpus.has_attr(modern)) return modern;
            std::string legacy = std::string("feats_") + feat;
            if (corpus.has_attr(legacy)) return legacy;
            return attr;
        }
    }
    if (attr.size() > 6 && attr.compare(0, 6, "feats#") == 0 && corpus.has_attr(attr))
        return attr;
    if (attr.size() > 6 && attr.compare(0, 6, "feats_") == 0 && corpus.has_attr(attr))
        return attr;

    std::string a;
    a.reserve(attr.size());
    for (char c : attr) {
        if (c == '/') a += '.';
        else a += c;
    }
    return a;
}

bool QueryExecutor::leaf_regex_eval(std::string_view val, const AttrCondition& ac) const {
    // pando-CQL: %c / %d apply to literal comparisons only (PANDO-CQL.md); a regex
    // is made case-insensitive with (?i) — the CWB dialect adds it for "…"%c
    return regex_eval_sv(val, regex_for(ac.value), ac.regex_full_match);
}

const Regex& QueryExecutor::regex_for(const std::string& pattern) const {
    std::lock_guard<std::mutex> lock(regex_cache_mutex_);
    auto it = regex_cache_.find(pattern);
    if (it == regex_cache_.end()) it = regex_cache_.emplace(pattern, std::make_unique<Regex>(pattern)).first;
    return *it->second;   // stable: the map owns it until the executor's caches go
}

std::string QueryExecutor::normalize_attr(const std::string& attr) const {
    return normalize_query_attr_name(corpus_, attr);
}

// ── Combined feats helpers ──────────────────────────────────────────────
//
// When feats is stored as a single string like "Case=Nom|Number=Sing",
// extract the value for a specific feature name.

bool feats_is_subkey(const std::string& name, std::string& feat_name) {
    if (name.size() > 6 && name.compare(0, 6, "feats/") == 0) {
        feat_name = name.substr(6);
        return !feat_name.empty() && feat_name.find('/') == std::string::npos;
    }
    return false;
}

bool corpus_has_ud_split_feats_column(const Corpus& corpus, const std::string& feat_name) {
    return corpus.has_attr(std::string("feats#") + feat_name)
        || corpus.has_attr(std::string("feats_") + feat_name);
}

std::string feats_extract_value(std::string_view feats,
                                const std::string& feat_name) {
    if (feats == "_" || feats.empty()) return "_";

    std::string prefix = feat_name + "=";
    size_t search = 0;
    while (search < feats.size()) {
        size_t pos = feats.find(prefix, search);
        if (pos == std::string::npos) return "_";
        if (pos == 0 || feats[pos - 1] == '|') {
            size_t val_start = pos + prefix.size();
            size_t val_end = feats.find('|', val_start);
            if (val_end == std::string::npos) val_end = feats.size();
            return std::string(feats.substr(val_start, val_end - val_start));
        }
        search = pos + 1;
    }
    return "_";
}

static bool feats_entry_matches(std::string_view feats,
                                const std::string& feat_name,
                                const std::string& value) {
    return feats_extract_value(feats, feat_name) == value;
}

// ── Condition compilation (#25): pre-resolve EQ values to LexiconIds ────
//
// Walk the condition tree and resolve string EQ values to integer IDs.
// check_leaf can then compare id_at(pos) == resolved_id instead of
// value_at(pos) == string, avoiding a lexicon lookup per position.

static int64_t id_set_count(const PositionalAttr& pa, const std::vector<int32_t>& ids) {
    int64_t total = 0;
    for (int32_t id : ids) total += static_cast<int64_t>(pa.count_of_id(static_cast<LexiconId>(id)));
    return total;
}

void QueryExecutor::compile_conditions(const ConditionPtr& cond) const {
    if (!cond) return;
    if (cond->is_leaf) {
        AttrCondition& ac = const_cast<AttrCondition&>(cond->leaf);
        if (ac.is_nvals || ac.op == CompOp::IN) return;
        {
            const std::string name = normalize_attr(ac.attr);
            std::string feat_name;
            const bool plain = !feats_is_subkey(name, feat_name) && corpus_.has_attr(name)
                               && !corpus_.is_multivalue(name);
            ac.plain_attr = plain ? static_cast<const void*>(&corpus_.attr(name)) : nullptr;
            ac.plain_attr_corpus = plain ? static_cast<const void*>(&corpus_) : nullptr;
        }
        if ((ac.op == CompOp::EQ || ac.op == CompOp::NEQ) && !ac.neq_regex
            && !ac.case_insensitive && !ac.diacritics_insensitive) {
            std::string name = normalize_attr(ac.attr);
            std::string feat_name;
            if (!feats_is_subkey(name, feat_name) && corpus_.has_attr(name)
                && corpus_.is_multivalue(name)) {
                ac.resolved_mv_component_id =
                    corpus_.attr(name).mv_lookup(ac.value);
            }
        }
        // (also `!= /re/`: the ids that match; the NEQ paths take the complement)
        if ((ac.op == CompOp::REGEX || ac.neq_regex) && !ac.id_set_resolved) {
            std::string name = normalize_attr(ac.attr);
            std::string feat_name;
            if (!feats_is_subkey(name, feat_name) && corpus_.has_attr(name)
                && !corpus_.is_multivalue(name)) {
                // One lexicon scan per query; same matcher as check_leaf. A literal
                // regex is a lookup, and a literal prefix narrows the scan to the
                // matching id range (the lexicon is sorted).
                const auto& lex = corpus_.attr(name).lexicon();
                ac.id_set.clear();   // a scan cancelled half-way (QueryCancelled) left a partial set
                ac.id_set_total = -1;
                LexiconId lex_lo = 0, nlex = lex.size();
                bool exact = false;
                // %c / %d do not apply to a regex (leaf_regex_eval): its literal
                // prefix and required literal hold with them too
                const std::string prefix = regex_literal_prefix(ac.value, ac.regex_full_match, &exact);
                if (exact) {
                    const LexiconId id = lex.lookup(prefix);
                    if (id != UNKNOWN_LEX) ac.id_set.push_back(id);
                    ac.id_set_resolved = true;
                    lex_lo = nlex = 0;
                } else if (!prefix.empty()) {
                    std::tie(lex_lo, nlex) = lexicon_prefix_range(lex, prefix);
                }
                const std::string req = regex_required_literal(ac.value);
                const std::string req_ci = req.empty() ? regex_required_literal_icase(ac.value) : std::string();
                // the corpus keeps id sets across queries (page, background total,
                // sort, next page…: one scan)
                std::string cache_key;
                if (!ac.id_set_resolved) {
                    cache_key = name + '\x1f' + ac.value + '\x1f' + (ac.regex_full_match ? '1' : '0')
                                + (ac.case_insensitive ? '1' : '0') + (ac.diacritics_insensitive ? '1' : '0');
                    if (auto hit = corpus_.cached_id_set(cache_key)) {
                        ac.id_set = *hit;
                        ac.id_set_resolved = true;
                        lex_lo = nlex = 0;
                    }
                }
                // P4.6: a large scan is split over threads (ranges of ids, merged in
                // order); each checks the literal prefilters before the regex
                const Regex& re = regex_for(ac.value);   // compiled once (throws on a bad pattern here)
                auto scan = [&](LexiconId a, LexiconId b, std::vector<int32_t>& out,
                                const std::atomic<bool>& stop, const Regex& re) {
                    for (LexiconId id = a; id < b; ++id) {
                        if ((id & 0xFFF) == 0 && stop.load(std::memory_order_relaxed)) return;
                        const std::string_view v = lex.get(id);
                        if (!req.empty() && v.find(req) == std::string_view::npos) continue;
                        if (!req_ci.empty() && !contains_icase_ascii(v, req_ci)) continue;
                        if (regex_eval_sv(v, re, ac.regex_full_match)) out.push_back(id);
                    }
                };
                const LexiconId span = nlex > lex_lo ? nlex - lex_lo : 0;
                // threads: the request's budget (a tier's `threads`), else
                // PANDO_LEXICON_THREADS, else min(usable CPUs, 8); workers are
                // borrowed from the process pool (none when it is busy)
                static const unsigned env_cap = [] {
                    const char* e = std::getenv("PANDO_LEXICON_THREADS");
                    const int v = e && *e ? std::atoi(e) : 0;
                    return v > 0 ? static_cast<unsigned>(v) : std::min(8u, usable_cpus());
                }();
                const unsigned cap = thread_budget_ ? std::min(thread_budget_, env_cap) : env_cap;
                const unsigned nt = static_cast<unsigned>(std::min<LexiconId>(
                    cap, std::max<LexiconId>(1, span / (LexiconId{1} << 18))));
                std::atomic<bool> stop{false};
                if (nt <= 1) {
                    for (LexiconId a = lex_lo; a < nlex; a += 0x1000) {
                        // a scan over a large lexicon can take seconds: honour cancel / timeouts
                        check_cancelled();
                        scan(a, std::min<LexiconId>(nlex, a + 0x1000), ac.id_set, stop, re);
                    }
                } else {
                    // 4 slices per thread, taken in turn; each slice checks cancel /
                    // time limits between its 4096-value blocks
                    const size_t nslices = static_cast<size_t>(nt) * 4;
                    std::vector<std::vector<int32_t>> parts(nslices);
                    const LexiconId step = (span + static_cast<LexiconId>(nslices) - 1) / static_cast<LexiconId>(nslices);
                    std::atomic<bool> cancelled{false};
                    WorkerPool::global().run(nslices, nt - 1, [&](size_t t) {
                        const LexiconId a0 = lex_lo + static_cast<LexiconId>(t) * step;
                        const LexiconId b0 = std::min<LexiconId>(nlex, a0 + step);
                        if (a0 >= b0) return;
                        // one compiled regex per slice: RE2's shared DFA cache is
                        // locked, so threads on one object barely scale
                        const Regex mine(ac.value);
                        for (LexiconId a = a0; a < b0 && !stop.load(std::memory_order_relaxed); a += 0x1000) {
                            if (progress_ && progress_->cancelled()) {
                                cancelled = true;
                                stop = true;
                                return;
                            }
                            scan(a, std::min<LexiconId>(b0, a + 0x1000), parts[t], stop, mine);
                        }
                    });
                    if (cancelled) throw QueryCancelled();
                    for (auto& part : parts) ac.id_set.insert(ac.id_set.end(), part.begin(), part.end());
                }
                if (!ac.id_set_resolved && !cache_key.empty())
                    corpus_.cache_id_set(cache_key, std::make_shared<const Corpus::IdSet>(ac.id_set));
                ac.id_set_resolved = true;
                ac.id_set_total = id_set_count(corpus_.attr(name), ac.id_set);
            }
        }
        if ((ac.op == CompOp::EQ || ac.op == CompOp::NEQ) && !ac.neq_regex
            && (ac.case_insensitive || ac.diacritics_insensitive) && !ac.id_set_resolved) {
            std::string name = normalize_attr(ac.attr);
            std::string feat_name;
            if (!feats_is_subkey(name, feat_name) && corpus_.has_attr(name)
                && !corpus_.is_multivalue(name)) {
                auto ids = fold_lookup_ids(name, ac.case_insensitive,
                                           ac.diacritics_insensitive, ac.value);
                ac.id_set.assign(ids.begin(), ids.end());
                std::sort(ac.id_set.begin(), ac.id_set.end());
                ac.id_set_resolved = true;
                ac.id_set_total = id_set_count(corpus_.attr(name), ac.id_set);
            }
        }
        if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive) {
            std::string name = normalize_attr(ac.attr);
            // Only resolve for positional attrs (not feats sub, not region attrs)
            std::string feat_name;
            if (!feats_is_subkey(name, feat_name) && corpus_.has_attr(name)
                && !corpus_.is_multivalue(name)) {
                ac.resolved_id = corpus_.attr(name).lexicon().lookup(ac.value);
            }
        }
        return;
    }
    if (cond->is_structural) {
        compile_conditions(cond->nested_conditions);
        return;
    }
    if (cond->is_count) {
        compile_conditions(cond->count_filter);
        return;
    }
    compile_conditions(cond->left);
    compile_conditions(cond->right);
}

void QueryExecutor::compile_query(const TokenQuery& query) const {
    for (const auto& tok : query.tokens)
        compile_conditions(tok.conditions);
    compile_conditions(query.within_having);
    for (const auto& cc : query.containing_clauses)
        compile_conditions(cc.subtree_cond);
}

std::vector<std::string> QueryExecutor::query_labels(const TokenQuery& query) {
    std::unordered_set<std::string> t, r;
    collect_token_and_region_labels(query, &t, &r);
    std::vector<std::string> out(t.begin(), t.end());
    out.insert(out.end(), r.begin(), r.end());
    std::sort(out.begin(), out.end());
    return out;
}

size_t QueryExecutor::bind_external_alignment(TokenQuery& query, const LabelLookup& lookup) const {
    if (query.global_alignment_filters.empty()) return 0;
    std::unordered_set<std::string> token_labels, region_labels;
    collect_token_and_region_labels(query, &token_labels, &region_labels);
    auto local = [&](const std::string& n) { return token_labels.count(n) || region_labels.count(n); };

    auto split_into = [](std::string_view v, std::unordered_set<std::string>& out) {
        size_t start = 0;
        while (start <= v.size()) {
            const size_t p = v.find('|', start);
            const size_t end = p == std::string_view::npos ? v.size() : p;
            if (end > start) out.insert(std::string(v.substr(start, end - start)));
            if (p == std::string_view::npos) break;
            start = p + 1;
        }
    };
    const bool with_empty = include_empty_alignment_values_;
    // does a value on the local side share a component with the earlier values?
    auto hits = [&](std::string_view v, const std::unordered_set<std::string>& vals) {
        if (!with_empty && alignment_value_is_missing(v)) return false;
        if (v.find('|') == std::string_view::npos) return vals.count(std::string(v)) > 0;
        std::unordered_set<std::string> comps;
        split_into(v, comps);
        for (const auto& c : comps)
            if (vals.count(c)) return true;
        return false;
    };

    size_t bound = 0;
    std::vector<GlobalAlignmentFilter> keep;
    for (const auto& af : query.global_alignment_filters) {
        const bool l1 = local(af.name1), l2 = local(af.name2);
        if (l1 == l2) {   // both in this query (or neither: validation says so)
            keep.push_back(af);
            continue;
        }
        const std::string& ext = l1 ? af.name2 : af.name1;
        const std::string& ext_attr = l1 ? af.attr2 : af.attr1;
        const std::string& loc = l1 ? af.name1 : af.name2;
        const std::string& loc_attr = l1 ? af.attr1 : af.attr2;
        std::optional<LabelBinding> b = lookup ? lookup(ext) : std::nullopt;
        if (!b || !b->ms) {
            keep.push_back(af);
            continue;
        }
        if (!token_labels.count(loc))
            throw std::runtime_error(":: " + ext + "." + ext_attr + " = " + loc + "." + loc_attr + ": '" + loc
                                     + "' is a region binding; aligning with the earlier statement's '" + ext
                                     + "' needs a token label here (e.g. " + loc + ":[])");
        const std::string ext_an = normalize_attr(ext_attr), loc_an = normalize_attr(loc_attr);
        for (const std::string* an : {&ext_an, &loc_an})
            if (!alignment_attr_is_supported(corpus_, *an))
                throw std::runtime_error(":: alignment filter: no attribute '" + *an + "' in this corpus");

        // the earlier statement's values (components of multivalues)
        std::unordered_set<std::string> vals;
        for (const auto& m : b->ms->matches) {
            auto v = resolve_alignment_operand_value(corpus_, m, b->nm, ext, ext_an);
            if (!v) continue;
            if (!with_empty && alignment_value_is_missing(*v)) continue;
            if (corpus_.is_multivalue(ext_an) || v->find('|') != std::string_view::npos) split_into(*v, vals);
            else vals.insert(std::string(*v));
        }

        // the local tokens that carry one of them
        std::vector<CorpusPos> pos;
        if (!vals.empty()) {
            if (corpus_.has_attr(loc_an)) {
                const PositionalAttr& pa = corpus_.attr(loc_an);
                const bool mv = corpus_.is_multivalue(loc_an) && pa.has_mv();
                for (const auto& v : vals) {
                    if (mv) {
                        const LexiconId id = pa.mv_lookup(v);
                        if (id != UNKNOWN_LEX)
                            pa.for_each_position_mv(id, [&](CorpusPos p) { pos.push_back(p); return true; });
                    } else {
                        const LexiconId id = pa.lexicon().lookup(v);
                        if (id != UNKNOWN_LEX)
                            pa.for_each_position_id(id, [&](CorpusPos p) { pos.push_back(p); return true; });
                    }
                }
            } else {
                RegionAttrParts parts;
                split_region_attr_name(loc_an, parts);
                const StructuralAttr& sa = corpus_.structure(parts.struct_name);
                const std::string key = *resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
                const CorpusPos ntok = corpus_.size();
                auto add_region = [&](size_t ri) {
                    if (ri >= sa.region_count()) return;
                    const Region r = sa.get(ri);
                    for (CorpusPos p = std::max<CorpusPos>(r.start, 0); p <= r.end && p < ntok; ++p) pos.push_back(p);
                };
                if (sa.has_region_value_reverse(key)) {
                    // over the distinct values, then their regions
                    const LexiconId K = sa.region_attr_lex_size(key);
                    for (LexiconId id = 0; id < K; ++id) {
                        if ((id & 0xFFFF) == 0) check_cancelled();
                        const std::string_view v = sa.region_attr_lex_get(key, id);
                        if (!hits(v, vals)) continue;
                        const int64_t* regs = nullptr;
                        size_t nreg = 0;
                        if (sa.regions_for_value(key, std::string(v), regs, nreg))
                            for (size_t i = 0; i < nreg; ++i) add_region(static_cast<size_t>(regs[i]));
                    }
                } else {
                    for (size_t ri = 0; ri < sa.region_count(); ++ri) {
                        if ((ri & 0xFFFF) == 0) check_cancelled();
                        if (hits(sa.region_value(key, ri), vals)) add_region(ri);
                    }
                }
            }
            std::sort(pos.begin(), pos.end());
            pos.erase(std::unique(pos.begin(), pos.end()), pos.end());
        }

        AttrCondition ac;
        ac.attr = loc_attr;
        ac.op = CompOp::IN;
        ac.value = ext + "." + ext_attr;
        ac.in_positions = std::make_shared<const std::vector<CorpusPos>>(std::move(pos));
        ConditionPtr leaf = ConditionNode::make_leaf(std::move(ac));
        for (auto& t : query.tokens) {
            if (t.is_anchor() || t.name != loc) continue;
            t.conditions = t.conditions ? ConditionNode::make_branch(BoolOp::AND, t.conditions, leaf) : leaf;
        }
        ++bound;
    }
    query.global_alignment_filters = std::move(keep);
    return bound;
}

void QueryExecutor::validate_query_name_bindings(const TokenQuery& query,
                                                 const TokenQuery* merge_labels_from) const {
    std::unordered_set<std::string> token_labels;
    std::unordered_set<std::string> region_labels;
    collect_token_and_region_labels(query, &token_labels, &region_labels);
    if (merge_labels_from)
        collect_token_and_region_labels(*merge_labels_from, &token_labels, &region_labels);

    for (const auto& tok : query.tokens) {
        validate_conditions_token_labels(tok.conditions, token_labels, region_labels);
    }
    validate_conditions_token_labels(query.within_having, token_labels, region_labels);
    for (const auto& cc : query.containing_clauses)
        validate_conditions_token_labels(cc.subtree_cond, token_labels, region_labels);

    for (const auto& af : query.global_alignment_filters) {
        expect_alignment_operand_label(af.name1, ":: alignment filter", token_labels, region_labels);
        expect_alignment_operand_label(af.name2, ":: alignment filter", token_labels, region_labels);
    }
    for (const auto& po : query.position_orders) {
        expect_token_label(po.name1, ":: position order", token_labels, region_labels);
        expect_token_label(po.name2, ":: position order", token_labels, region_labels);
    }
    for (const auto& ff : query.global_function_filters)
        validate_global_func_filter_token_labels(ff, token_labels, region_labels);

    const bool single_dep_subtree_only =
        query.tokens.size() == 1 && query.tokens[0].is_dep_subtree
        && std::all_of(query.tokens.begin(), query.tokens.end(),
                       [](const QueryToken& t) { return !t.is_anchor(); });
    for (const auto& tok : query.tokens) {
        if (!tok.is_dep_subtree || tok.dep_subtree_source.empty()) continue;
        if (token_labels.count(tok.dep_subtree_source)) continue;
        if (single_dep_subtree_only) continue;  // CLI resolves src against named query or Last + name map
        expect_token_label(tok.dep_subtree_source, "dep_subtree(...)", token_labels, region_labels);
    }

    size_t real_token_slots = 0;
    for (const auto& tok : query.tokens) {
        if (!tok.is_anchor()) ++real_token_slots;
    }
    for (size_t i = 0; i < query.tokens.size(); ++i) {
        const auto& tok = query.tokens[i];
        if (!tok.is_anchor() || tok.anchor_region_clauses.empty()) continue;
        for (const auto& cl : tok.anchor_region_clauses) {
            const std::string& plab = cl.peer_label;
            bool peer_before = false;
            for (size_t j = 0; j < i; ++j) {
                if (query.tokens[j].is_anchor() && query.tokens[j].name == plab) {
                    peer_before = true;
                    break;
                }
            }
            if (!peer_before) {
                throw std::runtime_error(
                        "Region anchor clause referencing '" + plab
                        + "': an earlier anchor must be labeled '" + plab + "' (e.g. " + plab
                        + ":<node …>)");
            }
        }
        if (real_token_slots == 0) {
            throw std::runtime_error(
                    "Region anchor peer clauses (rchild/contains) require at least one "
                    "non-anchor token (region-only enumeration cannot resolve peers together)");
        }
    }
}

// ── Cardinality estimation ──────────────────────────────────────────────
//
// Uses the rev.idx to get exact counts in O(1) per EQ condition.
// For AND, takes min; for OR, takes sum.  This is a conservative upper
// bound that's free to compute.

size_t QueryExecutor::estimate_leaf(const AttrCondition& ac) const {
    if (ac.op == CompOp::IN) return ac.in_positions ? ac.in_positions->size() : 0;
    std::string name = normalize_attr(ac.attr);
    if (ac.is_nvals)
        return static_cast<size_t>(corpus_.size());
    if (ac.neq_regex) {   // complement of the matching ids (when resolved)
        const size_t N = static_cast<size_t>(corpus_.size());
        if (ac.id_set_resolved && ac.id_set_total >= 0)
            return N - std::min(N, static_cast<size_t>(ac.id_set_total));
        return N;
    }

    // Combined feats mode: scan the feats lexicon for matching entries
    std::string feat_name;
    if (feats_is_subkey(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        size_t total = 0;
        LexiconId n = pa.lexicon().size();
        for (LexiconId id = 0; id < n; ++id) {
            if (feats_entry_matches(pa.lexicon().get(id), feat_name, ac.value))
                total += pa.count_of_id(id);
        }
        if (ac.op == CompOp::NEQ)
            return static_cast<size_t>(corpus_.size()) - total;
        return total;
    }

    if (!corpus_.has_attr(name)) {
        // Try region attribute fallback: name = "struct_region"
        auto us = name.find('_');
        if (us != std::string::npos && us + 1 < name.size()) {
            std::string struct_name = name.substr(0, us);
            std::string region_attr = name.substr(us + 1);
            if (corpus_.has_structure(struct_name)) {
                const auto& sa = corpus_.structure(struct_name);
                auto rkey = resolve_region_attr_key(sa, struct_name, region_attr);
                if (rkey) {
                    if (ac.op == CompOp::EQ && sa.has_region_value_reverse(*rkey)) {
                        size_t spans = sa.token_span_sum_for_attr_eq(*rkey, ac.value);
                        if (spans == SIZE_MAX)
                            return static_cast<size_t>(corpus_.size());
                        return std::min(spans, static_cast<size_t>(corpus_.size()));
                    }
                    return static_cast<size_t>(corpus_.size());
                }
            }
        }
        return 0;
    }
    const auto& pa = corpus_.attr(name);

    if (ac.id_set_resolved && ac.id_set_total >= 0
        && (ac.op == CompOp::REGEX
            || ((ac.case_insensitive || ac.diacritics_insensitive) && ac.op == CompOp::EQ)))
        return static_cast<size_t>(ac.id_set_total);

    // Fold-aware cardinality estimation
    if ((ac.case_insensitive || ac.diacritics_insensitive) && ac.op == CompOp::EQ) {
        const std::vector<LexiconId> ids = ac.id_set_resolved
            ? std::vector<LexiconId>(ac.id_set.begin(), ac.id_set.end())
            : fold_lookup_ids(name, ac.case_insensitive, ac.diacritics_insensitive, ac.value);
        size_t total = 0;
        for (LexiconId id : ids)
            total += pa.count_of_id(id);
        return total;
    }

    // RG-5f: MV-aware cardinality via component reverse index
    if (corpus_.is_multivalue(name) && pa.has_mv() && ac.op == CompOp::EQ) {
        return pa.mv_count_of(ac.value);
    }
    if (corpus_.is_multivalue(name) && pa.has_mv() && ac.op == CompOp::NEQ) {
        size_t eq = pa.mv_count_of(ac.value);
        return static_cast<size_t>(corpus_.size()) - eq;
    }

    if (ac.op == CompOp::REGEX && ac.id_set_resolved) {
        size_t total = 0;
        for (int32_t id : ac.id_set) total += pa.count_of_id(static_cast<LexiconId>(id));
        return total;
    }

    switch (ac.op) {
        case CompOp::EQ:
            return pa.count_of(ac.value);
        case CompOp::NEQ: {
            size_t eq = pa.count_of(ac.value);
            return static_cast<size_t>(corpus_.size()) - eq;
        }
        default:
            return static_cast<size_t>(corpus_.size());
    }
}

size_t QueryExecutor::estimate_cardinality(const ConditionPtr& cond) const {
    if (!cond) return static_cast<size_t>(corpus_.size());
    if (cond->is_leaf) return estimate_leaf(cond->leaf);
    if (cond->is_structural) return static_cast<size_t>(corpus_.size());  // conservative estimate
    if (cond->is_count) return static_cast<size_t>(corpus_.size());       // conservative estimate

    size_t l = estimate_cardinality(cond->left);
    size_t r = estimate_cardinality(cond->right);
    if (cond->bool_op == BoolOp::AND)
        return std::min(l, r);
    return std::min(l + r, static_cast<size_t>(corpus_.size()));
}

// ── Query planning ──────────────────────────────────────────────────────
//
// Picks the lowest-cardinality token as seed, then BFS outward along the
// query chain to determine step order.  This ensures we start from the
// most selective restriction and never materialize the large side.

QueryPlan QueryExecutor::plan_query(const TokenQuery& query) const {
    QueryPlan plan;
    size_t n = query.tokens.size();
    if (n == 0) return plan;

    std::vector<size_t> card(n);
    for (size_t i = 0; i < n; ++i)
        card[i] = query.tokens[i].is_dep_subtree
                      ? std::numeric_limits<size_t>::max()
                      : estimate_cardinality(query.tokens[i].conditions);

    // Pick the lowest-cardinality *non-optional* token as seed.
    // Optional tokens (min_repeat == 0) can match nothing, so they make
    // terrible anchors — seed_len=0 would create a corrupt span (end < start).
    // If every token is optional, fall back to the lowest-cardinality one;
    // the seed_len guard below still protects us.
    //
    // Never seed from the negated side of `!>` / `!<`: `[A] !> [B]` keeps A tokens
    // without a B dependent and `[A] !< [B]` keeps B tokens without an A dependent
    // (wiki: `[upos="DET"] !< [lemma="book"]` = *book* without a determiner).
    // Seeding from the dependent side silently answered a different query
    // (dependent tokens without such a head) whenever that side was rarer.
    std::vector<char> negated(n, 0);
    for (size_t r = 0; r < query.relations.size() && r + 1 < n; ++r) {
        if (query.relations[r].type == RelationType::NOT_GOVERNS) negated[r + 1] = 1;
        else if (query.relations[r].type == RelationType::NOT_GOV_BY) negated[r] = 1;
    }
    auto better = [&](size_t cand, size_t cur) {
        // true when `cand` is a better seed than `cur`
        if (negated[cand] != negated[cur]) return !negated[cand];
        bool cur_optional  = (query.tokens[cur].min_repeat == 0);
        bool cand_optional = (query.tokens[cand].min_repeat == 0);
        // Prefer non-optional over optional; among same optionality, prefer lower cardinality
        return (!cand_optional && cur_optional) ||
               (cand_optional == cur_optional && card[cand] < card[cur]);
    };
    plan.seed = 0;
    for (size_t i = 1; i < n; ++i)
        if (better(i, plan.seed)) plan.seed = i;

    // BFS from seed along the linear chain
    std::vector<bool> visited(n, false);
    visited[plan.seed] = true;
    std::queue<size_t> q;
    q.push(plan.seed);

    while (!q.empty()) {
        size_t cur = q.front();
        q.pop();

        // Left neighbor: tokens[cur-1] connected by relations[cur-1]
        if (cur > 0 && !visited[cur - 1]) {
            visited[cur - 1] = true;
            plan.steps.push_back({cur, cur - 1, cur - 1, /*reversed=*/true});
            q.push(cur - 1);
        }
        // Right neighbor: tokens[cur+1] connected by relations[cur]
        if (cur + 1 < n && !visited[cur + 1]) {
            visited[cur + 1] = true;
            plan.steps.push_back({cur, cur + 1, cur, /*reversed=*/false});
            q.push(cur + 1);
        }
    }

    plan.cardinalities = std::move(card);
    return plan;
}

// ── Per-position condition checking ─────────────────────────────────────
//
// Evaluates a condition tree against a single corpus position.
// Avoids materializing candidate sets for non-seed tokens — just reads
// the per-position attribute value from the .dat file (O(1) array lookup).

std::optional<int64_t> QueryExecutor::nvals_cardinality_at(
        CorpusPos pos, const std::string& name_in) const {
    std::string name = normalize_attr(name_in);

    std::string feat_name;
    if (feats_is_subkey(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        std::string_view feats_str = pa.value_at(pos);
        std::string feat_val = feats_extract_value(feats_str, feat_name);
        int64_t n = (feat_val.empty() || feat_val == "_") ? 0 : 1;
        return n;
    }

    RegionAttrParts rap;
    if (!corpus_.has_attr(name) && split_region_attr_name(name, rap)
        && corpus_.has_structure(rap.struct_name)) {
        const auto& sa = corpus_.structure(rap.struct_name);
        auto rkey = resolve_region_attr_key(sa, rap.struct_name, rap.attr_name);
        if (rkey) {
            const std::string& region_attr = *rkey;
            bool is_multi_region = corpus_.is_overlapping(rap.struct_name)
                                 || corpus_.is_nested(rap.struct_name);
            if (is_multi_region) {
                std::unordered_set<std::string> uniq;
                sa.for_each_region_at(pos, [&](size_t rgn_idx) -> bool {
                    insert_mv_components(uniq, sa.region_value(region_attr, rgn_idx));
                    return true;
                });
                return static_cast<int64_t>(uniq.size());
            }
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0)
                return int64_t{0};
            std::string_view v = sa.region_value(region_attr, static_cast<size_t>(rgn));
            return nvals_from_string(v, corpus_.is_multivalue(name));
        }
    }
    if (!corpus_.has_attr(name))
        return std::nullopt;
    const auto& pa = corpus_.attr(name);
    if (corpus_.is_multivalue(name) && pa.has_mv_fwd())
        return static_cast<int64_t>(pa.mv_fwd_count_at(pos));
    std::string_view val = pa.value_at(pos);
    return nvals_from_string(val, corpus_.is_multivalue(name));
}

bool QueryExecutor::check_leaf(CorpusPos pos, const AttrCondition& ac) const {
    if (ac.op == CompOp::IN)
        return ac.in_positions && std::binary_search(ac.in_positions->begin(), ac.in_positions->end(), pos);
    if (ac.is_nvals) {
        auto n = nvals_cardinality_at(pos, ac.attr);
        if (!n) return false;
        return compare_nvals_count(*n, ac.op, ac.nvals_compare);
    }

    // Plain positional attribute resolved by compile_conditions: id comparisons
    // without the name normalisation / lookups below (same results).
    if (ac.plain_attr && ac.plain_attr_corpus == &corpus_) {
        const auto& pa = *static_cast<const PositionalAttr*>(ac.plain_attr);
        if (ac.resolved_id >= 0) return pa.id_at(pos) == static_cast<LexiconId>(ac.resolved_id);
        if (ac.id_set_resolved) {
            const bool in = std::binary_search(ac.id_set.begin(), ac.id_set.end(),
                                               static_cast<int32_t>(pa.id_at(pos)));
            return ac.op == CompOp::NEQ ? !in : in;
        }
        if (ac.neq_regex) return !leaf_regex_eval(pa.value_at(pos), ac);
        if (!ac.case_insensitive && !ac.diacritics_insensitive) {
            if (ac.op == CompOp::NEQ) return !multivalue_eq(pa.value_at(pos), ac.value);
            if (ac.op == CompOp::EQ) return multivalue_eq(pa.value_at(pos), ac.value);
        }
    }

    std::string name = normalize_attr(ac.attr);

    // Combined feats mode: feats/Number="Sing" → check within combined feats string
    std::string feat_name;
    if (feats_is_subkey(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        std::string_view feats_str = pa.value_at(pos);
        std::string feat_val = feats_extract_value(feats_str, feat_name);
        if (ac.neq_regex)
            return !leaf_regex_eval(feat_val, ac);
        switch (ac.op) {
            case CompOp::EQ:  return feat_val == ac.value;
            case CompOp::NEQ: return feat_val != ac.value;
            default:          return false;
        }
    }

    if (!corpus_.has_attr(name)) {
        // Try region attribute fallback: name = "struct_region"
        auto us = name.find('_');
        if (us != std::string::npos && us + 1 < name.size()) {
            std::string struct_name = name.substr(0, us);
            std::string region_attr = name.substr(us + 1);
            if (corpus_.has_structure(struct_name)) {
                const auto& sa = corpus_.structure(struct_name);
                auto rkey = resolve_region_attr_key(sa, struct_name, region_attr);
                if (rkey) {
                    const std::string& resolved_attr = *rkey;
                    bool is_multi_region = corpus_.is_overlapping(struct_name)
                                         || corpus_.is_nested(struct_name);

                    // For overlapping/nested structures, check ALL regions
                    // containing pos (existential semantics for EQ/REGEX).
                    if (is_multi_region) {
                        bool any_match = false;
                        sa.for_each_region_at(pos, [&](size_t rgn_idx) -> bool {
                            std::string_view val = sa.region_value(resolved_attr, rgn_idx);
                            switch (ac.op) {
                                case CompOp::EQ:
                                    if (multivalue_eq(val, ac.value)) { any_match = true; return false; }
                                    break;
                                case CompOp::NEQ:
                                    // Existential: true if ANY region has val != target
                                    if (ac.neq_regex
                                            ? !leaf_regex_eval(val, ac)
                                            : !multivalue_eq(val, ac.value)) {
                                        any_match = true;
                                        return false;
                                    }
                                    break;
                                case CompOp::REGEX: {
                                    if (leaf_regex_eval(val, ac)) {
                                        any_match = true;
                                        return false;
                                    }
                                    break;
                                }
                                case CompOp::LT:
                                    if (val < ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::GT:
                                    if (val > ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::LTE:
                                    if (val <= ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::GTE:
                                    if (val >= ac.value) { any_match = true; return false; }
                                    break;
                                case CompOp::IN:
                                    break;  // handled at top of check_leaf
                            }
                            return true;  // continue scanning
                        });
                        return any_match;
                    }

                    // Flat structure: single region lookup (original path).
                    int64_t rgn = sa.find_region(pos);
                    if (rgn >= 0) {
                        std::string_view val = sa.region_value(resolved_attr, static_cast<size_t>(rgn));
                        switch (ac.op) {
                            case CompOp::EQ:    return multivalue_eq(val, ac.value);
                            case CompOp::NEQ:
                                if (ac.neq_regex)
                                    return !leaf_regex_eval(val, ac);
                                return !multivalue_eq(val, ac.value);
                            case CompOp::REGEX:
                                return leaf_regex_eval(val, ac);
                            case CompOp::LT:    return val < ac.value;
                            case CompOp::GT:    return val > ac.value;
                            case CompOp::LTE:   return val <= ac.value;
                            case CompOp::GTE:   return val >= ac.value;
                            case CompOp::IN:    return false;  // handled at top of check_leaf
                        }
                    }
                }
            }
        }
        return false;
    }
    const auto& pa = corpus_.attr(name);

    // Stage 1: multivalue EQ/NEQ via sorted .mv.fwd component ids (membership).
    if (corpus_.is_multivalue(name) && pa.has_mv() && pa.has_mv_fwd()
        && (ac.op == CompOp::EQ || ac.op == CompOp::NEQ) && !ac.neq_regex
        && !ac.case_insensitive && !ac.diacritics_insensitive) {
        LexiconId mid = ac.resolved_mv_component_id;
        if (mid >= 0) {
            bool contains = mv_fwd_contains_sorted(pa, pos, mid);
            return ac.op == CompOp::EQ ? contains : !contains;
        }
        // Unknown MV component string: EQ never matches; NEQ always true.
        if (ac.op == CompOp::EQ) return false;
        return true;
    }

    // #25: Fast path — use pre-resolved LexiconId for integer comparison
    if (ac.resolved_id >= 0) {
        // resolved_id is only set for EQ without fold flags
        return pa.id_at(pos) == static_cast<LexiconId>(ac.resolved_id);
    }

    // P1.5 / P1.6: pre-resolved id set (fold / regex) → integer membership test.
    if (ac.id_set_resolved) {
        const bool in = std::binary_search(ac.id_set.begin(), ac.id_set.end(),
                                           static_cast<int32_t>(pa.id_at(pos)));
        return ac.op == CompOp::NEQ ? !in : in;
    }

    std::string_view val = pa.value_at(pos);

    if (ac.neq_regex)
        return !leaf_regex_eval(val, ac);

    // Fold-aware comparison for %c / %d flags
    if ((ac.case_insensitive || ac.diacritics_insensitive) &&
        (ac.op == CompOp::EQ || ac.op == CompOp::NEQ)) {
        std::string folded;
        if (ac.case_insensitive && ac.diacritics_insensitive)
            folded = FoldMap::to_lower_no_accents(val);
        else if (ac.case_insensitive)
            folded = FoldMap::to_lower(val);
        else
            folded = FoldMap::strip_accents(val);
        // The query value should already be folded by the caller (or we fold it here)
        std::string folded_query;
        if (ac.case_insensitive && ac.diacritics_insensitive)
            folded_query = FoldMap::to_lower_no_accents(ac.value);
        else if (ac.case_insensitive)
            folded_query = FoldMap::to_lower(ac.value);
        else
            folded_query = FoldMap::strip_accents(ac.value);
        if (ac.op == CompOp::EQ) return folded == folded_query;
        return folded != folded_query;
    }

    switch (ac.op) {
        case CompOp::EQ:    return multivalue_eq(val, ac.value);
        case CompOp::NEQ:   return !multivalue_eq(val, ac.value);
        case CompOp::REGEX:
            return leaf_regex_eval(val, ac);
        case CompOp::LT:    return val < ac.value;
        case CompOp::GT:    return val > ac.value;
        case CompOp::LTE:   return val <= ac.value;
        case CompOp::GTE:   return val >= ac.value;
        case CompOp::IN:    return false;  // handled at top of check_leaf
    }
    return false;
}

bool QueryExecutor::check_conditions(CorpusPos pos,
                                     const ConditionPtr& cond) const {
    if (!cond) return true;
    if (cond->is_leaf) return check_leaf(pos, cond->leaf);

    if (cond->is_count) {
        if (!corpus_.has_deps())
            throw std::runtime_error("Count conditions require dependency index");
        const auto& deps = corpus_.deps();
        std::vector<CorpusPos> related;
        switch (cond->count_rel) {
            case StructRelType::CHILD:      related = deps.children(pos); break;
            case StructRelType::PARENT:     { auto h = deps.head(pos); if (h != NO_HEAD) related.push_back(h); break; }
            case StructRelType::SIBLING:    { auto h = deps.head(pos); if (h != NO_HEAD) { related = deps.children(h); related.erase(std::remove(related.begin(), related.end(), pos), related.end()); } break; }
            case StructRelType::DESCENDANT: related = deps.subtree(pos); break;
            case StructRelType::ANCESTOR:   related = deps.ancestors(pos); break;
        }
        int64_t cnt = 0;
        for (CorpusPos rp : related) {
            if (!cond->count_filter || check_conditions(rp, cond->count_filter))
                ++cnt;
        }
        switch (cond->count_op) {
            case CompOp::EQ:  return cnt == cond->count_value;
            case CompOp::NEQ: return cnt != cond->count_value;
            case CompOp::LT:  return cnt <  cond->count_value;
            case CompOp::GT:  return cnt >  cond->count_value;
            case CompOp::LTE: return cnt <= cond->count_value;
            case CompOp::GTE: return cnt >= cond->count_value;
            case CompOp::REGEX:
            case CompOp::IN:  return false;
        }
        return false;
    }

    if (cond->is_structural) {
        if (!corpus_.has_deps())
            throw std::runtime_error("Structural conditions require dependency index");
        const auto& deps = corpus_.deps();
        std::vector<CorpusPos> related;
        switch (cond->struct_rel) {
            case StructRelType::CHILD:      related = deps.children(pos); break;
            case StructRelType::PARENT:     { auto h = deps.head(pos); if (h != NO_HEAD) related.push_back(h); break; }
            case StructRelType::SIBLING:    { auto h = deps.head(pos); if (h != NO_HEAD) { related = deps.children(h); related.erase(std::remove(related.begin(), related.end(), pos), related.end()); } break; }
            case StructRelType::DESCENDANT: related = deps.subtree(pos); break;
            case StructRelType::ANCESTOR:   related = deps.ancestors(pos); break;
        }
        bool any_match = false;
        for (CorpusPos rp : related) {
            if (check_conditions(rp, cond->nested_conditions)) {
                any_match = true;
                break;
            }
        }
        return cond->struct_negated ? !any_match : any_match;
    }

    if (cond->bool_op == BoolOp::AND)
        return check_conditions(pos, cond->left) &&
               check_conditions(pos, cond->right);
    else
        return check_conditions(pos, cond->left) ||
               check_conditions(pos, cond->right);
}

// ── Relation traversal ──────────────────────────────────────────────────
//
// Given a position on one side of a relation edge, returns the positions
// on the other side.  For SEQUENCE this is 1 position; for GOVERNS it's
// the children list or the single head; for transitive relations it walks
// the tree (walk-up is O(depth), walk-down is DFS).

static bool is_dep_relation(RelationType rel) {
    return rel != RelationType::SEQUENCE;
}

std::vector<CorpusPos> QueryExecutor::find_related(
        CorpusPos pos, RelationType rel, bool reversed) const {

    std::vector<CorpusPos> out;

    // Defensive: ignore invalid positions to avoid out-of-bounds in deps/attrs
    if (pos < 0 || pos >= corpus_.size())
        return out;

    if (is_dep_relation(rel) && !corpus_.has_deps())
        throw std::runtime_error(
            "Query uses dependency relations but corpus has no dependency index");

    const auto& deps = corpus_.deps();

    // Invert the relation when traversing backward through the edge
    RelationType eff = rel;
    if (reversed) {
        switch (rel) {
            case RelationType::SEQUENCE:       eff = RelationType::SEQUENCE; break;
            case RelationType::GOVERNS:        eff = RelationType::GOVERNED_BY; break;
            case RelationType::GOVERNED_BY:    eff = RelationType::GOVERNS; break;
            case RelationType::TRANS_GOVERNS:  eff = RelationType::TRANS_GOV_BY; break;
            case RelationType::TRANS_GOV_BY:   eff = RelationType::TRANS_GOVERNS; break;
            case RelationType::NOT_GOVERNS:    eff = RelationType::NOT_GOV_BY; break;
            case RelationType::NOT_GOV_BY:     eff = RelationType::NOT_GOVERNS; break;
        }
    }

    switch (eff) {
        case RelationType::SEQUENCE:
            if (reversed) {
                if (pos > 0) out.push_back(pos - 1);
            } else {
                if (pos + 1 < corpus_.size()) out.push_back(pos + 1);
            }
            break;

        case RelationType::GOVERNS:
            out = deps.children(pos);
            break;

        case RelationType::GOVERNED_BY: {
            CorpusPos h = deps.head(pos);
            if (h != NO_HEAD) out.push_back(h);
            break;
        }

        case RelationType::TRANS_GOVERNS:
            out = deps.subtree(pos);
            break;

        case RelationType::TRANS_GOV_BY:
            out = deps.ancestors(pos);
            break;

        case RelationType::NOT_GOVERNS:
        case RelationType::NOT_GOV_BY:
            // Negative relations handled as post-filters in execute()
            break;
    }
    return out;
}

// ── Lazy seed iteration (avoids materializing full position vector for EQ) ─

void QueryExecutor::for_each_seed_position_impl(const ConditionPtr& cond,
                                                std::function<bool(CorpusPos)> f) const {
    if (!cond) {
        for (CorpusPos p = 0; p < corpus_.size(); ++p)
            if (!f(p)) return;
        return;
    }
    if (cond->is_leaf) {
        const AttrCondition& ac = cond->leaf;
        if (ac.is_nvals) {
            for (CorpusPos p = 0; p < corpus_.size(); ++p)
                if (check_leaf(p, ac) && !f(p)) return;
            return;
        }
        std::string name = normalize_attr(ac.attr);
        std::string feat_name;
        if (feats_is_subkey(name, feat_name) && !corpus_.has_attr(name)
            && corpus_.has_attr("feats")) {
            auto vec = resolve_leaf(ac);
            for (CorpusPos p : vec)
                if (!f(p)) return;
            return;
        }
        if (!corpus_.has_attr(name)) {
            RegionAttrParts parts;
            if (split_region_attr_name(name, parts) &&
                corpus_.has_structure(parts.struct_name)) {
                const auto& sa = corpus_.structure(parts.struct_name);
                auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
                if (rkey && ac.op == CompOp::EQ) {
                    // Fast path: iterate only positions inside matching regions
                    const int64_t* rgn_ids = nullptr;
                    size_t rgn_count = 0;
                    if (sa.regions_for_value(*rkey, ac.value,
                                             rgn_ids, rgn_count)) {
                        for (size_t k = 0; k < rgn_count; ++k) {
                            Region r = sa.get(static_cast<size_t>(rgn_ids[k]));
                            for (CorpusPos pos = r.start; pos <= r.end; ++pos)
                                if (!f(pos)) return;
                        }
                        return;
                    }
                    // Fallback: no .rev, materialize
                    auto vec = resolve_leaf(ac);
                    for (CorpusPos p : vec)
                        if (!f(p)) return;
                    return;
                }
                // Non-EQ region attr or no region attr: materialize
                if (rkey) {
                    auto vec = resolve_leaf(ac);
                    for (CorpusPos p : vec)
                        if (!f(p)) return;
                    return;
                }
            }
            return;
        }
        const auto& pa = corpus_.attr(name);
        if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive) {
            // RG-5f: For multivalue attributes, use the .mv.rev component
            // reverse index for O(log V) seed resolution.
            if (corpus_.is_multivalue(name) && pa.has_mv()) {
                LexiconId mv_id = pa.mv_lookup(ac.value);
                if (mv_id != UNKNOWN_LEX)
                    pa.for_each_position_mv(mv_id, f);
                return;
            }
            // #25: Use pre-resolved ID if available, otherwise lookup
            LexiconId id = (ac.resolved_id >= 0)
                ? static_cast<LexiconId>(ac.resolved_id)
                : pa.lexicon().lookup(ac.value);
            if (id == UNKNOWN_LEX) return;
            pa.for_each_position_id(id, f);
            return;
        }
        // NEQ, REGEX, or fold-aware EQ: materialize
        auto vec = resolve_leaf(ac);
        for (CorpusPos p : vec)
            if (!f(p)) return;
        return;
    }
    if (cond->bool_op == BoolOp::AND) {
        size_t left_est  = estimate_cardinality(cond->left);
        size_t right_est = estimate_cardinality(cond->right);
        const ConditionPtr& cheap     = (left_est <= right_est) ? cond->left : cond->right;
        const ConditionPtr& expensive = (left_est <= right_est) ? cond->right : cond->left;
        for_each_seed_position(cheap, [&](CorpusPos p) {
            return check_conditions(p, expensive) ? f(p) : true;
        });
        return;
    }
    // OR: need merged list
    auto left  = resolve_conditions(cond->left);
    auto right = resolve_conditions(cond->right);
    auto merged = unite(left, right);
    for (CorpusPos p : merged)
        if (!f(p)) return;
}

// ── Seed resolution (inverted index lookup) ─────────────────────────────

// Sorted union of the postings of lexicon ids (a regex / %c id set). Postings of
// distinct ids are disjoint. Dense unions — a regex like ".*a.*" matches types
// covering a fifth of the corpus — go through a bitmap of the corpus (N/8 bytes,
// less than the result itself) instead of concatenating and sorting millions of
// positions; sparse ones concatenate and sort. Honours cancellation throughout.
// First index of a sorted posting span with a position >= x.
static size_t span_lower_bound(const RevSpan& sp, CorpusPos x) {
    size_t lo = 0, hi = sp.count;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (sp.at(mid) < x) lo = mid + 1; else hi = mid;
    }
    return lo;
}

// The postings of `sp` within [lo, hi).
static std::vector<CorpusPos> span_window(const RevSpan& sp, CorpusPos lo, CorpusPos hi) {
    std::vector<CorpusPos> out;
    for (size_t i = span_lower_bound(sp, lo); i < sp.count; ++i) {
        const CorpusPos p = sp.at(i);
        if (p >= hi) break;
        out.push_back(p);
    }
    return out;
}

// P6.5e: the union restricted to [lo, hi) — a binary search per id, then only
// the positions inside; a bitmap over the window when dense.
template <class Ids>
static std::vector<CorpusPos> union_id_postings_window(const PositionalAttr& pa, const Ids& ids,
                                                       CorpusPos lo, CorpusPos hi, const QueryExecutor& ex,
                                                       const void* key) {
    std::vector<CorpusPos> out;
    const size_t w = static_cast<size_t>(hi - lo);
    if (ids.size() > w / 16) {
        // many types for a small window (a regex matching half the lexicon):
        // scan the window's ids against an id bitmap instead of seeking each list
        const size_t L = static_cast<size_t>(pa.lexicon().size());
        std::vector<uint64_t> local;
        std::vector<uint64_t>* wp = &local;
        if (auto* cache = ex.window_id_bits(); cache && key) wp = &(*cache)[key];
        std::vector<uint64_t>& want = *wp;
        if (want.size() != (L + 63) / 64) {   // (built once for the ranges of one page)
            want.assign((L + 63) / 64, 0);
            for (auto id : ids)
                if (id >= 0 && static_cast<size_t>(id) < L)
                    want[static_cast<size_t>(id) >> 6] |= uint64_t{1} << (static_cast<size_t>(id) & 63);
        }
        out.reserve(w / 4);
        for (CorpusPos p = lo; p < hi; ++p) {
            if (((p - lo) & 0xFFFF) == 0) ex.check_cancelled();
            const LexiconId id = pa.id_at(p);
            if (id >= 0 && static_cast<size_t>(id) < L
                && (want[static_cast<size_t>(id) >> 6] >> (static_cast<size_t>(id) & 63) & 1))
                out.push_back(p);
        }
        return out;
    }
    std::vector<uint64_t> bits((w + 63) / 64, 0);
    size_t k = 0, n = 0;
    for (auto id : ids) {
        if ((++k & 0x3FF) == 0) ex.check_cancelled();
        const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(id));
        for (size_t i = span_lower_bound(sp, lo); i < sp.count; ++i) {
            const CorpusPos p = sp.at(i);
            if (p >= hi) break;
            const size_t off = static_cast<size_t>(p - lo);
            bits[off >> 6] |= uint64_t{1} << (off & 63);
            ++n;
        }
    }
    ex.check_cancelled();
    out.reserve(n);
    for (size_t b = 0; b < bits.size(); ++b)
        for (uint64_t x = bits[b]; x; x &= x - 1)
            out.push_back(lo + static_cast<CorpusPos>(b * 64 + static_cast<size_t>(__builtin_ctzll(x))));
    return out;
}

template <class Ids>
static std::vector<CorpusPos> union_id_postings(const PositionalAttr& pa, const Ids& ids, CorpusPos n_tokens,
                                                const QueryExecutor& ex, const void* key = nullptr) {
    std::vector<CorpusPos> out;
    if (ids.empty()) return out;
    if (ex.windowed())
        return union_id_postings_window(pa, ids, ex.operand_window().lo,
                                        std::min(ex.operand_window().hi, n_tokens), ex, key);
    if (ids.size() == 1) return pa.positions_of_id(static_cast<LexiconId>(ids[0]));
    size_t total = 0;
    for (auto id : ids) total += pa.count_of_id(static_cast<LexiconId>(id));
    size_t k = 0;
    if (total < static_cast<size_t>(n_tokens) / 64) {
        out.reserve(total);
        for (auto id : ids) {
            if ((++k & 0x3FF) == 0) ex.check_cancelled();
            const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(id));
            for (size_t i = 0; i < sp.count; ++i) out.push_back(sp.at(i));
        }
        ex.check_cancelled();
        std::sort(out.begin(), out.end());
        return out;
    }
    std::vector<uint64_t> bits((static_cast<size_t>(n_tokens) + 63) / 64, 0);
    for (auto id : ids) {
        if ((++k & 0x3FF) == 0) ex.check_cancelled();
        const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(id));
        for (size_t i = 0; i < sp.count; ++i) {
            const CorpusPos p = sp.at(i);
            bits[static_cast<size_t>(p) >> 6] |= uint64_t{1} << (p & 63);
        }
    }
    ex.check_cancelled();
    out.reserve(total);
    for (size_t w = 0; w < bits.size(); ++w)
        for (uint64_t x = bits[w]; x; x &= x - 1)
            out.push_back(static_cast<CorpusPos>(w * 64 + static_cast<size_t>(__builtin_ctzll(x))));
    return out;
}

std::vector<CorpusPos> QueryExecutor::resolve_leaf(
        const AttrCondition& ac) const {
    if (ac.op == CompOp::IN) {
        if (!ac.in_positions) return {};
        const auto& v = *ac.in_positions;
        if (!windowed()) return v;
        auto a = std::lower_bound(v.begin(), v.end(), operand_window_.lo);
        auto b = std::lower_bound(a, v.end(), operand_window_.hi);
        return std::vector<CorpusPos>(a, b);
    }
    std::string name = normalize_attr(ac.attr);

    if (ac.is_nvals) {
        std::vector<CorpusPos> result;
        for (CorpusPos p = 0; p < corpus_.size(); ++p)
            if (check_leaf(p, ac)) result.push_back(p);
        return result;
    }

    // `!= /re/` and `!= "x" %c` with a resolved id set: the complement of the ids'
    // postings; `!= /re/` elsewhere (feats, regions, multivalue): check each position
    if (ac.op == CompOp::NEQ && (ac.neq_regex || ac.id_set_resolved)) {
        const CorpusPos lo = windowed() ? operand_window_.lo : 0;
        const CorpusPos hi = windowed() ? std::min(operand_window_.hi, corpus_.size()) : corpus_.size();
        std::vector<CorpusPos> out;
        if (ac.id_set_resolved && corpus_.has_attr(name) && !corpus_.is_multivalue(name)) {
            const std::vector<CorpusPos> in =
                union_id_postings(corpus_.attr(name), ac.id_set, corpus_.size(), *this, &ac);
            out.reserve(static_cast<size_t>(hi - lo) - std::min(in.size(), static_cast<size_t>(hi - lo)));
            auto it = std::lower_bound(in.begin(), in.end(), lo);
            for (CorpusPos p = lo; p < hi; ++p) {
                while (it != in.end() && *it < p) ++it;
                if (it != in.end() && *it == p) continue;
                out.push_back(p);
            }
            return out;
        }
        for (CorpusPos p = lo; p < hi; ++p) {
            if ((p & 0xFFFF) == 0) check_cancelled();
            if (check_leaf(p, ac)) out.push_back(p);
        }
        return out;
    }

    // Combined feats mode: scan feats lexicon, union matching position lists
    std::string feat_name;
    if (feats_is_subkey(name, feat_name) && !corpus_.has_attr(name)
        && corpus_.has_attr("feats")) {
        const auto& pa = corpus_.attr("feats");
        LexiconId n = pa.lexicon().size();

        std::vector<CorpusPos> result;
        for (LexiconId id = 0; id < n; ++id) {
            bool match = feats_entry_matches(pa.lexicon().get(id),
                                             feat_name, ac.value);
            if ((ac.op == CompOp::EQ && match) ||
                (ac.op == CompOp::NEQ && !match)) {
                auto positions = pa.positions_of_id(id);
                result.insert(result.end(), positions.begin(), positions.end());
            }
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    if (!corpus_.has_attr(name)) {
        // Try region attribute fallback: name = "struct_region"
        auto us = name.find('_');
        if (us != std::string::npos && us + 1 < name.size()) {
            std::string struct_name = name.substr(0, us);
            std::string region_attr = name.substr(us + 1);
            if (corpus_.has_structure(struct_name)) {
                const auto& sa = corpus_.structure(struct_name);
                auto rkey = resolve_region_attr_key(sa, struct_name, region_attr);
                if (rkey) {
                    if (ac.op == CompOp::EQ) {
                        std::vector<CorpusPos> result;
                        // Fast path: use .rev index to get matching region indices,
                        // then emit only positions inside those regions.
                        const int64_t* rgn_ids = nullptr;
                        size_t rgn_count = 0;
                        if (sa.regions_for_value(*rkey, ac.value,
                                                 rgn_ids, rgn_count)) {
                            result.reserve(rgn_count * 64);  // rough estimate
                            for (size_t k = 0; k < rgn_count; ++k) {
                                Region r = sa.get(static_cast<size_t>(rgn_ids[k]));
                                for (CorpusPos pos = r.start; pos <= r.end; ++pos)
                                    result.push_back(pos);
                            }
                            // Region indices from .rev are sorted, so positions
                            // are already in order.
                            return result;
                        }
                        // Fallback: no reverse index, linear scan
                        for (CorpusPos pos = 0; pos < corpus_.size(); ++pos) {
                            int64_t rgn = sa.find_region(pos);
                            if (rgn >= 0) {
                                std::string_view val = sa.region_value(
                                    *rkey, static_cast<size_t>(rgn));
                                if (val == ac.value)
                                    result.push_back(pos);
                            }
                        }
                        return result;
                    }
                    // For other operations on region attrs, return empty (not supported)
                    return {};
                }
            }
        }
        return {};
    }
    const auto& pa = corpus_.attr(name);

    // Fold-aware EQ resolution (fold index file, else in-memory fold map)
    if ((ac.case_insensitive || ac.diacritics_insensitive) && ac.op == CompOp::EQ) {
        const std::vector<LexiconId> ids = ac.id_set_resolved
            ? std::vector<LexiconId>(ac.id_set.begin(), ac.id_set.end())
            : fold_lookup_ids(name, ac.case_insensitive, ac.diacritics_insensitive, ac.value);
        return union_id_postings(pa, ids, corpus_.size(), *this, ac.id_set_resolved ? &ac : nullptr);
    }

    if (windowed() && (ac.op == CompOp::EQ || ac.op == CompOp::NEQ)) {
        // P6.5e: only the operand window (a page found range by range)
        const CorpusPos lo = operand_window_.lo, hi = std::min(operand_window_.hi, corpus_.size());
        const LexiconId id = pa.lexicon().lookup(ac.value);
        std::vector<CorpusPos> in = id == UNKNOWN_LEX ? std::vector<CorpusPos>{}
                                                      : span_window(pa.rev_span_of_id(id), lo, hi);
        if (ac.op == CompOp::EQ) return in;
        std::vector<CorpusPos> out;
        out.reserve(static_cast<size_t>(hi - lo) - in.size());
        size_t j = 0;
        for (CorpusPos p = lo; p < hi; ++p) {
            if (j < in.size() && in[j] == p) { ++j; continue; }
            out.push_back(p);
        }
        return out;
    }
    switch (ac.op) {
        case CompOp::EQ:
            return pa.positions_of(ac.value);
        case CompOp::NEQ:
            return pa.positions_not(ac.value, corpus_.size());
        case CompOp::REGEX: {
            if (ac.id_set_resolved)   // a regex can match millions of types: bitmap union
                return union_id_postings(pa, ac.id_set, corpus_.size(), *this, &ac);
            const Regex& compiled = regex_for(ac.value);
            return pa.positions_matching(compiled, ac.regex_full_match);
        }
        default:
            throw std::runtime_error("Unsupported comparison on positional attr");
    }
}

bool QueryExecutor::may_sink_hits(const TokenQuery& q) const {
    for (const auto& tok : q.tokens)
        if (tok.is_dep_subtree) return false;
    return q.global_region_filters.empty()
           && !(q.within_having && !q.within.empty() && corpus_.has_structure(q.within))
           && !(q.not_within && !q.within.empty() && corpus_.has_structure(q.within))
           && q.containing_clauses.empty() && q.position_orders.empty()
           && q.global_alignment_filters.empty() && q.global_function_filters.empty();
}

std::vector<CorpusPos> QueryExecutor::resolve_conditions(
        const ConditionPtr& cond) const {
    if (!cond) {
        const CorpusPos lo = windowed() ? operand_window_.lo : 0;
        const CorpusPos hi = windowed() ? std::min(operand_window_.hi, corpus_.size()) : corpus_.size();
        std::vector<CorpusPos> all(static_cast<size_t>(hi - lo));
        for (CorpusPos i = lo; i < hi; ++i)
            all[static_cast<size_t>(i - lo)] = i;
        return all;
    }
    if (cond->is_leaf) return resolve_leaf(cond->leaf);

    if (cond->bool_op == BoolOp::AND) {
        // Resolve the cheaper side, filter the results by the expensive side.
        // This avoids materializing huge complement sets for NEQ conditions
        // (e.g., [upos!="PUNCT" & lemma="cat"] only materializes "cat" positions).
        size_t left_est  = estimate_cardinality(cond->left);
        size_t right_est = estimate_cardinality(cond->right);

        const ConditionPtr& cheap     = (left_est <= right_est) ? cond->left : cond->right;
        const ConditionPtr& expensive = (left_est <= right_est) ? cond->right : cond->left;

        auto positions = resolve_conditions(cheap);
        positions.erase(
            std::remove_if(positions.begin(), positions.end(),
                [&](CorpusPos p) { return !check_conditions(p, expensive); }),
            positions.end());
        return positions;
    } else {
        auto left  = resolve_conditions(cond->left);
        auto right = resolve_conditions(cond->right);
        return unite(left, right);
    }
}

// ── Value comparison helper (used by both inline and post-hoc filters) ──

static bool compare_value(CompOp op, const std::string& a, const std::string& b) {
    switch (op) {
        case CompOp::EQ:  return a == b;
        case CompOp::NEQ: return a != b;
        case CompOp::LT:  return a < b;
        case CompOp::GT:  return a > b;
        case CompOp::LTE: return a <= b;
        case CompOp::GTE: return a >= b;
        default: return a == b;
    }
}

static bool compare_value_maybe_mv(CompOp op,
                                   std::string_view stored,
                                   const std::string& query_value,
                                   bool is_multivalue_attr) {
    const bool treat_as_mv = is_multivalue_attr || stored.find('|') != std::string_view::npos;
    if (op == CompOp::EQ && treat_as_mv)
        return multivalue_eq(stored, query_value);
    if (op == CompOp::NEQ && treat_as_mv)
        return !multivalue_eq(stored, query_value);
    return compare_value(op, std::string(stored), query_value);
}

static bool region_attr_is_multivalue(const Corpus& corpus,
                                      const StructuralAttr* sa_ptr,
                                      const std::string& attr_name) {
    if (!sa_ptr) return false;
    for (const auto& sn : corpus.structure_names()) {
        const auto& sa = corpus.structure(sn);
        if (&sa == sa_ptr)
            return corpus.is_multivalue(sn + "_" + attr_name);
    }
    return false;
}

// ── Pre-resolved region filters ──────────────────────────────────────────

std::vector<ResolvedRegionFilter> QueryExecutor::resolve_region_filters(
        const TokenQuery& q) const {
    std::vector<ResolvedRegionFilter> out;
    out.reserve(q.global_region_filters.size());
    for (const auto& gf : q.global_region_filters) {
        ResolvedRegionFilter rf;
        rf.op = gf.op;
        rf.value = gf.value;
        rf.anchor_name = gf.anchor_name;

        RegionAttrParts parts;
        if (!split_region_attr_name(gf.region_attr, parts)) {
            out.push_back(std::move(rf));
            continue;
        }
        if (corpus_.has_structure(parts.struct_name)) {
            const auto& sa = corpus_.structure(parts.struct_name);
            auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
            if (rkey) {
                rf.sa = &sa;
                rf.attr_name = *rkey;
                rf.has_reverse = sa.has_region_value_reverse(*rkey);
            }
        }
        out.push_back(std::move(rf));
    }
    return out;
}

// ── Fast aggregation path ────────────────────────────────────────────────
//
// For single-token queries with aggregation and no complex post-filters,
// bypass Match construction entirely. The hot loop is:
//   seed pos → RegionCursor.find(O(1) amortized) → array index → counter++
//
// Returns true if the fast path was taken; false = fall back to standard path.

bool QueryExecutor::condition_ids_on_attr(const ConditionPtr& c, const PositionalAttr& pa,
                                          std::vector<char>& ids) const {
    const size_t V = static_cast<size_t>(std::max<LexiconId>(0, pa.lexicon().size()));
    if (!c) {
        ids.assign(V, 1);
        return true;
    }
    if (!c->is_leaf) {
        if (c->is_structural || c->is_count || !c->left || !c->right) return false;
        std::vector<char> r;
        if (!condition_ids_on_attr(c->left, pa, ids) || !condition_ids_on_attr(c->right, pa, r))
            return false;
        if (c->bool_op == BoolOp::AND)
            for (size_t i = 0; i < V; ++i) ids[i] = ids[i] && r[i];
        else
            for (size_t i = 0; i < V; ++i) ids[i] = ids[i] || r[i];
        return true;
    }
    // as check_leaf on a plain positional attribute, per lexicon entry
    const AttrCondition& ac = c->leaf;
    if (ac.op == CompOp::IN || ac.is_nvals || ac.plain_attr != &pa || ac.plain_attr_corpus != &corpus_)
        return false;
    if (ac.resolved_id >= 0) {
        ids.assign(V, 0);
        if (static_cast<size_t>(ac.resolved_id) < V) ids[static_cast<size_t>(ac.resolved_id)] = 1;
        return true;
    }
    if (ac.id_set_resolved) {
        const bool neq = ac.op == CompOp::NEQ;
        ids.assign(V, neq ? 1 : 0);
        for (int32_t id : ac.id_set)
            if (id >= 0 && static_cast<size_t>(id) < V) ids[static_cast<size_t>(id)] = neq ? 0 : 1;
        return true;
    }
    const Lexicon& lex = pa.lexicon();
    ids.assign(V, 0);
    if (ac.neq_regex) {
        for (size_t i = 0; i < V; ++i) ids[i] = !leaf_regex_eval(lex.get(static_cast<LexiconId>(i)), ac);
        return true;
    }
    // an unresolved EQ is rare (its tokens are found faster from the postings); NEQ
    // is the common `!=` over (nearly) every token
    if (ac.case_insensitive || ac.diacritics_insensitive || ac.op != CompOp::NEQ)
        return false;
    for (size_t i = 0; i < V; ++i)
        ids[i] = !multivalue_eq(lex.get(static_cast<LexiconId>(i)), ac.value);
    return true;
}

bool QueryExecutor::try_fast_aggregate(
        const TokenQuery& q,
        AggregateBucketData& agg,
        const std::vector<ResolvedRegionFilter>& resolved_filters,
        const std::vector<AnchorConstraint>& token_anchor_constraints,
        size_t max_total_cap,
        MatchSet& result,
        const NameIndexMap& name_map) const {

    // Guard: single-token, non-repeating, no global function filters
    if (q.tokens.size() != 1) return false;
    if (q.tokens[0].min_repeat != 1 || q.tokens[0].max_repeat != 1) return false;
    if (!q.global_function_filters.empty()) return false;

    // ── Anchor constraints (RG-REG-7): only the "simple" subset is supported ─
    //
    // The fast path can resolve anchor constraints inline (no Match construction)
    // iff each constraint:
    //   - binds to the single query token (token_idx == 0)
    //   - is not a region enumeration
    //   - has no peer clauses (rchild/contains)
    //   - targets a non-multi structural type (no nested/overlapping/zerowidth)
    //   - references an existing structure
    //
    // In that case we can, per seed position, ask a RegionCursor for the
    // containing region, verify the boundary (pos == reg.start or reg.end),
    // check attrs_match, and record the region row for any RegionFromBinding
    // aggregate columns that bind to this constraint.
    struct AnchorFastInfo {
        const AnchorConstraint* ac = nullptr;
        const StructuralAttr* sa = nullptr;
        RegionCursor cursor;
    };
    std::vector<AnchorFastInfo> anchor_infos;
    anchor_infos.reserve(token_anchor_constraints.size());
    std::unordered_map<std::string, size_t> binding_to_anchor_idx;
    for (const auto& ac : token_anchor_constraints) {
        if (ac.region_enumeration) return false;
        if (ac.token_idx != 0) return false;
        if (!ac.anchor_region_clauses.empty()) return false;
        if (!corpus_.has_structure(ac.region)) return false;
        // Multi types need RG-REG-5 fan-out (Cartesian over candidate rows) which
        // this per-seed fast path can't express; decline and let the general path run.
        // Flat types have at most one containing row, so fanout/innermost coincide here.
        if (corpus_.is_nested(ac.region) || corpus_.is_overlapping(ac.region)
            || corpus_.is_zerowidth(ac.region))
            return false;
        AnchorFastInfo ai;
        ai.ac = &ac;
        ai.sa = &corpus_.structure(ac.region);
        ai.cursor = RegionCursor(*ai.sa);
        if (!ac.binding_name.empty())
            binding_to_anchor_idx[ac.binding_name] = anchor_infos.size();
        anchor_infos.push_back(std::move(ai));
    }

    for (const auto& col : agg.columns) {
        if (col.kind == AggregateBucketData::Column::Kind::FeatsComposite) return false;
        if (col.date_transform != AggregateBucketData::Column::DateTransform::None) return false;
    }

    // Named anchors in aggregate columns must refer either to this single query token
    // (index 0 — matches fill_aggregate_key / resolve_name) or, for RegionFromBinding,
    // to one of the simple anchor constraints above.
    for (const auto& col : agg.columns) {
        if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding) {
            if (col.named_anchor.empty()) return false;
            auto it = binding_to_anchor_idx.find(col.named_anchor);
            if (it == binding_to_anchor_idx.end()) return false;
            const auto& ai = anchor_infos[it->second];
            if (!resolve_region_attr_key(*ai.sa, ai.ac->region, col.region_attr_name)) return false;
            continue;
        }
        if (col.named_anchor.empty()) continue;
        auto it = name_map.find(col.named_anchor);
        if (it == name_map.end() || it->second != 0) return false;
    }
    for (const auto& rf : resolved_filters) {
        if (!rf.anchor_name.empty()) return false;
    }

    // ── Precompute per-column fast-lookup structures ─────────────────────

    const size_t ncols = agg.columns.size();

    struct ColFastInfo {
        bool is_positional = true;
        const PositionalAttr* pa = nullptr;
        const StructuralAttr* sa = nullptr;
        std::vector<LexiconId> region_to_lex;
        LexiconId lex_size = 0;
        std::string attr_name;
        // >=0 → RegionFromBinding: row comes from anchor_infos[anchor_idx], not from
        //       a per-column RegionCursor. -1 for all other column kinds.
        int anchor_idx = -1;
    };

    std::vector<ColFastInfo> col_info(ncols);
    bool sole_positional_col = (ncols == 1);

    for (size_t i = 0; i < ncols; ++i) {
        const auto& col = agg.columns[i];
        auto& ci = col_info[i];
        if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            ci.is_positional = true;
            ci.pa = col.pa;
        } else if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding) {
            ci.is_positional = false;
            const size_t aidx = binding_to_anchor_idx[col.named_anchor];
            ci.sa = anchor_infos[aidx].sa;
            ci.attr_name = col.region_attr_name;
            ci.region_to_lex = ci.sa->precompute_region_to_lex(col.region_attr_name);
            ci.lex_size = ci.sa->region_attr_lex_size(col.region_attr_name);
            ci.anchor_idx = static_cast<int>(aidx);
            sole_positional_col = false;
            if (ci.region_to_lex.empty()) return false;  // no reverse index
        } else {
            ci.is_positional = false;
            ci.sa = col.sa;
            ci.attr_name = col.region_attr_name;
            ci.region_to_lex = col.sa->precompute_region_to_lex(col.region_attr_name);
            ci.lex_size = col.sa->region_attr_lex_size(col.region_attr_name);
            sole_positional_col = false;
            if (ci.region_to_lex.empty()) return false;  // no reverse index
        }
    }

    // ── Precompute filter cursors ────────────────────────────────────────

    struct FilterCursor {
        const ResolvedRegionFilter* rf = nullptr;
        RegionCursor cursor;
        bool valid = false;
        bool is_mv = false;
        std::vector<char> ok;   // P7.5: the filter's verdict per region, when precomputed
    };

    std::vector<FilterCursor> filter_cursors;
    filter_cursors.reserve(resolved_filters.size());
    const size_t est_seeds = resolved_filters.empty() ? 0 : estimate_cardinality(q.tokens[0].conditions);
    for (const auto& rf : resolved_filters) {
        FilterCursor fc;
        fc.rf = &rf;
        if (rf.sa) {
            fc.cursor = RegionCursor(*rf.sa);
            fc.valid = true;
            fc.is_mv = region_attr_is_multivalue(corpus_, rf.sa, rf.attr_name);
            // one verdict per region instead of a lexicon lookup + posting search
            // (or a string compare) per seed, when there are more seeds than regions
            const size_t nr = rf.sa->region_count();
            if (est_seeds >= nr / 4) {
                fc.ok.assign(nr, 0);
                if (rf.op == CompOp::EQ && rf.has_reverse && !fc.is_mv) {
                    const int64_t* regs = nullptr;
                    size_t cnt = 0;
                    if (rf.sa->regions_for_value(rf.attr_name, rf.value, regs, cnt))
                        for (size_t k = 0; k < cnt; ++k)
                            if (regs[k] >= 0 && static_cast<size_t>(regs[k]) < nr) fc.ok[static_cast<size_t>(regs[k])] = 1;
                }
                for (size_t r = 0; r < nr; ++r)
                    if (!fc.ok[r])
                        fc.ok[r] = compare_value_maybe_mv(rf.op, rf.sa->region_value(rf.attr_name, r), rf.value, fc.is_mv);
            }
        }
        filter_cursors.push_back(std::move(fc));
    }

    // Region cursors for aggregate columns (not used for RegionFromBinding, which
    // reads its row from the matched anchor constraint instead).
    std::vector<RegionCursor> agg_cursors(ncols);
    for (size_t i = 0; i < ncols; ++i) {
        if (!col_info[i].is_positional && col_info[i].anchor_idx < 0)
            agg_cursors[i] = RegionCursor(*col_info[i].sa);
    }

    // ── Special case: single positional column → flat array ─────────────
    // Both special cases require zero per-seed filtering work, so they're only
    // valid when there are no filter cursors AND no anchor constraints.

    if (sole_positional_col && filter_cursors.empty() && anchor_infos.empty()) {
        const auto& pa = *col_info[0].pa;
        LexiconId lex_sz = pa.lexicon().size();
        std::vector<uint64_t> flat(static_cast<size_t>(std::max<LexiconId>(1, lex_sz)), 0);
        size_t total = 0;

        // P7.4: grouped by the attribute the token restricts (`[lemma=".*ness"];
        // count by lemma`, `[upos!="PUNCT"]; count by upos`): the counts are the
        // posting lengths of the ids that satisfy it (in a partition: the part of
        // each posting list in its range), no hit enumerated
        std::vector<char> ids;
        if (max_total_cap == 0 && q.within.empty() && fastpath_mode() == FastPathMode::On
            && condition_ids_on_attr(q.tokens[0].conditions, pa, ids)) {
            for (size_t id = 0; id < ids.size(); ++id) {
                if (!ids[id]) continue;
                size_t c;
                if (range_) {
                    const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(id));
                    const size_t a = gallop_rev(sp, 0, range_->lo);
                    c = gallop_rev(sp, a, range_->hi) - a;
                } else {
                    c = pa.count_of_id(static_cast<LexiconId>(id));
                }
                flat[id] = c;
                total += c;
            }
            if (range_) result.range_ok = true;
            agg.total_hits = total;
            agg.flat_ncols = 1;
            agg.flat_v2 = 1;
            agg.flat_dense = std::move(flat);
            result.total_count = total;
            result.total_exact = true;
            result.plan_path = "single_agg_ids";
            return true;
        }
        auto count_pos = [&](CorpusPos pos) -> bool {
            if (max_total_cap > 0 && total >= max_total_cap) return false;
            ++flat[static_cast<size_t>(pa.id_at(pos))];
            ++total;
            return true;
        };

        // P4.1: a partition counts the hits in its range only — a plain EQ token
        // walks the slice of its posting list, anything else skips the others.
        const ConditionPtr& tc = q.tokens[0].conditions;
        const AttrCondition* eq = (tc && tc->is_leaf && tc->leaf.op == CompOp::EQ
                                   && !tc->leaf.case_insensitive && !tc->leaf.diacritics_insensitive
                                   && tc->leaf.resolved_id >= 0) ? &tc->leaf : nullptr;
        const std::string eq_attr = eq ? normalize_attr(eq->attr) : std::string();
        if (range_ && eq && corpus_.has_attr(eq_attr) && !corpus_.is_multivalue(eq_attr)) {
            const RevSpan sp = corpus_.attr(eq_attr).rev_span_of_id(static_cast<LexiconId>(eq->resolved_id));
            const size_t a = gallop_rev(sp, 0, range_->lo);
            const size_t b = gallop_rev(sp, a, range_->hi);
            for (size_t i = a; i < b; ++i)
                if (!count_pos(sp.at(i))) break;
            result.range_ok = true;
        } else if (range_) {
            const CorpusPos rlo = range_->lo, rhi = range_->hi;
            for_each_seed_position(tc, [&](CorpusPos pos) -> bool {
                if (pos < rlo || pos >= rhi) return true;
                return count_pos(pos);
            });
            result.range_ok = true;
        } else {
            for_each_seed_position(tc, count_pos);
        }

        agg.total_hits = total;
        // P7.2 buckets: the dense array as is (read through for_each_bucket), no
        // vector key per value — and P4.1 partitions merge by adding arrays
        agg.flat_ncols = 1;
        agg.flat_v2 = 1;
        agg.flat_dense = std::move(flat);
        result.total_count = total;
        result.total_exact = !(max_total_cap > 0 && total >= max_total_cap);
        return true;
    }

    // ── Special case: single region column → flat array via region_to_lex ─

    if (ncols == 1 && !col_info[0].is_positional && col_info[0].anchor_idx < 0
        && filter_cursors.empty() && anchor_infos.empty()) {
        const auto& ci = col_info[0];
        LexiconId lex_sz = ci.lex_size;
        if (lex_sz <= 0) return false;
        std::vector<size_t> flat(static_cast<size_t>(lex_sz), 0);
        size_t total = 0;
        RegionCursor cursor(*ci.sa);

        for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
            if (max_total_cap > 0 && total >= max_total_cap) return false;
            int64_t rgn = cursor.find(pos);
            if (rgn < 0) return true;
            LexiconId lid = ci.region_to_lex[static_cast<size_t>(rgn)];
            if (lid == UNKNOWN_LEX) return true;
            ++flat[static_cast<size_t>(lid)];
            ++total;
            return true;
        });

        // Build results using 1-based intern IDs for decode_aggregate_bucket_key
        agg.total_hits = total;
        agg.region_intern.resize(1);
        auto& ri = agg.region_intern[0];
        for (LexiconId id = 0; id < lex_sz; ++id) {
            if (flat[static_cast<size_t>(id)] > 0) {
                int64_t intern_id = static_cast<int64_t>(ri.id_to_str.size() + 1);
                std::string val(ci.sa->region_attr_lex_get(ci.attr_name, id));
                ri.str_to_id[val] = intern_id;
                ri.id_to_str.push_back(val);
                std::vector<int64_t> key = {intern_id};
                agg.counts[std::move(key)] = flat[static_cast<size_t>(id)];
            }
        }
        result.total_count = total;
        result.total_exact = !(max_total_cap > 0 && total >= max_total_cap);
        return true;
    }

    // ── General case: multi-column or with :: filters / anchor constraints ─

    std::vector<int64_t> key_buf(ncols);
    size_t total = 0;
    bool capped = false;

    // P7.5: one or two columns counted on a packed id key (FlatAggCounter's table)
    // instead of a vector key per hit in `counts`
    auto card_of = [&](size_t i) -> uint64_t {
        return col_info[i].is_positional
            ? static_cast<uint64_t>(std::max<LexiconId>(1, col_info[i].pa->lexicon().size()))
            : static_cast<uint64_t>(std::max<LexiconId>(1, col_info[i].lex_size));
    };
    // Region values are keyed 1-based (lex id + 1) over their whole lexicon, which
    // then goes to region_intern as is: the buckets stay packed (flat_*), as the
    // general path's FlatAggCounter hands them over, and partitions key alike.
    FlatAggCounter packed;
    bool packed_ok = ncols >= 1 && ncols <= 2;
    for (size_t i = 0; i < ncols && packed_ok; ++i)
        packed_ok = col_info[i].is_positional
                    || static_cast<size_t>(std::max<LexiconId>(0, col_info[i].lex_size)) <= FlatRegionCol::kMaxValues;
    auto packed_id = [&](size_t i, int64_t id) -> uint64_t {
        return static_cast<uint64_t>(id) + (col_info[i].is_positional ? 0 : 1);
    };
    const uint64_t card1 = ncols == 2 ? card_of(1) + 1 : 1;
    if (packed_ok) {
        const uint64_t card0 = card_of(0) + 1;
        if (card0 <= ~uint64_t{0} / card1 / 2) {
            packed.on = true;
            packed.ncols = static_cast<int>(ncols);
            packed.v1 = card0;
            packed.v2 = card1;
            packed.dense_mode = ncols == 1 && card0 <= FlatAggCounter::kDenseAlways;
            if (packed.dense_mode) packed.dense.assign(static_cast<size_t>(card0), 0);
        }
    }

    // Per-seed scratch: region row resolved for each anchor constraint. Filled by
    // pass_anchors and consumed by RegionFromBinding column extraction.
    std::vector<size_t> anchor_region_rows(anchor_infos.size(), 0);

    auto pass_anchors = [&](CorpusPos pos) -> bool {
        for (size_t ai = 0; ai < anchor_infos.size(); ++ai) {
            auto& info = anchor_infos[ai];
            int64_t rgn = info.cursor.find(pos);
            if (rgn < 0) return false;
            Region reg = info.sa->get(static_cast<size_t>(rgn));
            if (info.ac->is_start) {
                if (pos != reg.start) return false;
            } else {
                if (pos != reg.end) return false;
            }
            for (const auto& [key, val] : info.ac->attrs) {
                auto rk = resolve_region_attr_key(*info.sa, info.ac->region, key);
                if (!rk) return false;
                if (info.sa->region_value(*rk, static_cast<size_t>(rgn)) != val)
                    return false;
            }
            anchor_region_rows[ai] = static_cast<size_t>(rgn);
        }
        return true;
    };

    auto pass_filters = [&](CorpusPos pos) -> bool {
        for (auto& fc : filter_cursors) {
            if (!fc.valid) return false;
            int64_t rgn = fc.cursor.find(pos);
            if (rgn < 0) return false;
            if (!fc.ok.empty()) {
                if (!fc.ok[static_cast<size_t>(rgn)]) return false;
                continue;
            }
            const auto& rf = *fc.rf;
            const bool is_mv = fc.is_mv;
            if (rf.op == CompOp::EQ && rf.has_reverse && !is_mv) {
                if (rf.sa->region_matches_attr_eq_rev(rf.attr_name,
                        static_cast<size_t>(rgn), rf.value))
                    continue;
            }
            std::string_view rval = rf.sa->region_value(rf.attr_name,
                                                         static_cast<size_t>(rgn));
            if (!compare_value_maybe_mv(rf.op, rval, rf.value, is_mv)) return false;
        }
        return true;
    };

    for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
        if (max_total_cap > 0 && total >= max_total_cap) {
            capped = true;
            return false;
        }
        if (!anchor_infos.empty() && !pass_anchors(pos))
            return true;
        if (!filter_cursors.empty() && !pass_filters(pos))
            return true;

        for (size_t i = 0; i < ncols; ++i) {
            const auto& ci = col_info[i];
            if (ci.is_positional) {
                key_buf[i] = static_cast<int64_t>(ci.pa->id_at(pos));
            } else {
                int64_t rgn;
                if (ci.anchor_idx >= 0) {
                    // RegionFromBinding: use row already resolved by pass_anchors.
                    rgn = static_cast<int64_t>(anchor_region_rows[ci.anchor_idx]);
                } else {
                    rgn = agg_cursors[i].find(pos);
                    if (rgn < 0) return true;
                }
                LexiconId lid = ci.region_to_lex[static_cast<size_t>(rgn)];
                if (lid == UNKNOWN_LEX) return true;
                // 0-based lex id; remapped to 1-based intern ID below
                key_buf[i] = static_cast<int64_t>(lid);
            }
        }
        ++total;
        if (packed.on) packed.inc(ncols == 1 ? packed_id(0, key_buf[0])
                                             : packed_id(0, key_buf[0]) * card1 + packed_id(1, key_buf[1]));
        else ++agg.counts[key_buf];
        return true;
    });

    if (packed.on) {
        packed.flush(agg);
        agg.region_intern.resize(ncols);
        for (size_t i = 0; i < ncols; ++i) {
            if (col_info[i].is_positional) continue;
            auto& ri = agg.region_intern[i];
            ri.str_to_id.clear();
            ri.id_to_str.clear();
            for (LexiconId l = 0; l < col_info[i].lex_size; ++l) {
                ri.id_to_str.emplace_back(col_info[i].sa->region_attr_lex_get(col_info[i].attr_name, l));
                ri.str_to_id.emplace(ri.id_to_str.back(), static_cast<int64_t>(l) + 1);
            }
        }
        agg.total_hits = total;
        result.total_count = total;
        result.total_exact = !capped;
        return true;
    }

    agg.total_hits = total;

    // Populate region_intern and remap keys for decode_aggregate_bucket_key.
    // Build per-column lex_id → 1-based intern_id mappings first, then remap
    // all keys in a single pass over the counts map.
    agg.region_intern.resize(ncols);
    std::vector<std::unordered_map<int64_t, int64_t>> lex_to_intern_maps(ncols);
    bool need_remap = false;

    for (size_t i = 0; i < ncols; ++i) {
        if (col_info[i].is_positional) continue;
        need_remap = true;
        const auto& ci = col_info[i];
        auto& ri = agg.region_intern[i];
        auto& lex_to_intern = lex_to_intern_maps[i];

        for (const auto& [key, cnt] : agg.counts) {
            int64_t lid = key[i];
            if (lex_to_intern.count(lid)) continue;
            int64_t intern_id = static_cast<int64_t>(ri.id_to_str.size() + 1);
            std::string val(ci.sa->region_attr_lex_get(ci.attr_name,
                            static_cast<LexiconId>(lid)));
            ri.str_to_id[val] = intern_id;
            ri.id_to_str.push_back(val);
            lex_to_intern[lid] = intern_id;
        }
    }

    if (need_remap) {
        std::unordered_map<std::vector<int64_t>, size_t,
                           AggregateBucketData::VecHash,
                           AggregateBucketData::VecEq> new_counts;
        for (auto& [key, cnt] : agg.counts) {
            std::vector<int64_t> new_key = key;
            for (size_t i = 0; i < ncols; ++i) {
                if (col_info[i].is_positional) continue;
                new_key[i] = lex_to_intern_maps[i][key[i]];
            }
            new_counts[std::move(new_key)] += cnt;
        }
        agg.counts = std::move(new_counts);
    }

    result.total_count = total;
    result.total_exact = !capped;
    return true;
}

// ── Main execution ──────────────────────────────────────────────────────

namespace {

static std::optional<AggregateBucketData::Column::DateTransform> parse_date_transform_prefix(
        const std::string& field, std::string& inner_attr) {
    struct Prefix {
        const char* name;
        AggregateBucketData::Column::DateTransform tx;
    };
    static const Prefix kPrefixes[] = {
        {"year(", AggregateBucketData::Column::DateTransform::Year},
        {"century(", AggregateBucketData::Column::DateTransform::Century},
        {"decade(", AggregateBucketData::Column::DateTransform::Decade},
        {"month(", AggregateBucketData::Column::DateTransform::Month},
        {"week(", AggregateBucketData::Column::DateTransform::Week},
        {"day(", AggregateBucketData::Column::DateTransform::Day},
        {"strlen(", AggregateBucketData::Column::DateTransform::Strlen},
    };
    if (field.size() < 7 || field.back() != ')')
        return std::nullopt;
    for (const auto& p : kPrefixes) {
        const std::string_view pref(p.name);
        if (field.rfind(pref, 0) == 0 && field.size() > pref.size() + 1) {
            inner_attr = field.substr(pref.size(), field.size() - pref.size() - 1);
            if (!inner_attr.empty())
                return p.tx;
        }
    }
    return std::nullopt;
}

static std::string apply_date_transform_bucket(
        std::string_view raw,
        AggregateBucketData::Column::DateTransform transform) {
    if (transform == AggregateBucketData::Column::DateTransform::None)
        return std::string(raw);
    if (transform == AggregateBucketData::Column::DateTransform::Strlen) {
        int64_t cp_count = 0;
        for (unsigned char c : raw)
            if ((c & 0xC0) != 0x80) ++cp_count;
        return std::to_string(cp_count);
    }
    auto parse_year_prefix = [](std::string_view text) -> std::optional<int64_t> {
        size_t i = 0;
        while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
        if (i >= text.size()) return std::nullopt;
        bool neg = false;
        if (text[i] == '+' || text[i] == '-') {
            neg = text[i] == '-';
            ++i;
        }
        if (i + 4 > text.size()) return std::nullopt;
        for (size_t k = 0; k < 4; ++k) {
            if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
                return std::nullopt;
        }
        if (i + 4 < text.size() && std::isdigit(static_cast<unsigned char>(text[i + 4])))
            return std::nullopt;
        int64_t y = 0;
        for (size_t k = 0; k < 4; ++k)
            y = y * 10 + static_cast<int64_t>(text[i + k] - '0');
        return neg ? -y : y;
    };

    auto y = parse_year_prefix(raw);
    if (!y) return "";
    switch (transform) {
        case AggregateBucketData::Column::DateTransform::Year:
            return std::to_string(*y);
        case AggregateBucketData::Column::DateTransform::Century:
            return (*y > 0) ? std::to_string(((*y - 1) / 100) + 1) : "";
        case AggregateBucketData::Column::DateTransform::Decade:
            return (*y > 0) ? std::to_string((*y / 10) * 10) : "";
        case AggregateBucketData::Column::DateTransform::Month:
        case AggregateBucketData::Column::DateTransform::Week:
        case AggregateBucketData::Column::DateTransform::Day:
            return "";
        case AggregateBucketData::Column::DateTransform::Strlen:
            return std::to_string(std::count_if(
                    raw.begin(), raw.end(),
                    [](unsigned char c) { return (c & 0xC0) != 0x80; }));
        case AggregateBucketData::Column::DateTransform::None:
            return std::string(raw);
    }
    return "";
}

bool build_aggregate_plan(const Corpus& corpus, const std::vector<std::string>& fields,
                          AggregateBucketData& out) {
    out.columns.clear();
    out.region_intern.clear();
    out.counts.clear();
    out.flat_ncols = 0;
    out.flat_v2 = 1;
    out.flat_dense.clear();
    out.flat_keys.clear();
    out.flat_vals.clear();
    out.total_hits = 0;
    out.columns.reserve(fields.size());
    out.region_intern.resize(fields.size());
    for (const std::string& field : fields) {
        AggregateBucketData::Column col;
        std::string attr_spec = field;
        std::string date_inner;
        if (auto tx = parse_date_transform_prefix(field, date_inner)) {
            col.date_transform = *tx;
            attr_spec = std::move(date_inner);
        }
        if (attr_spec.rfind("match.", 0) == 0 && attr_spec.size() > 6) {
            attr_spec = attr_spec.substr(6);
        } else {
            auto dot = attr_spec.find('.');
            if (dot != std::string::npos && dot > 0) {
                std::string prefix = attr_spec.substr(0, dot);
                std::string rest = attr_spec.substr(dot + 1);
                std::string rattr = rest;
                if (rattr.size() > 5 && rattr.substr(0, 5) == "feats" && rattr.find('.') != std::string::npos)
                    rattr[rattr.find('.')] = '_';
                // `node.type`, `s.id` — prefix is the structural type (named region binding label).
                if (corpus.has_structure(prefix)) {
                    const auto& sa = corpus.structure(prefix);
                    auto rkey = resolve_region_attr_key(sa, prefix, rattr);
                    if (rkey) {
                        col.kind = AggregateBucketData::Column::Kind::Region;
                        col.sa = &sa;
                        col.region_attr_name = *rkey;
                        col.named_anchor = std::move(prefix);
                        out.columns.push_back(std::move(col));
                        continue;
                    }
                }
                col.named_anchor = std::move(prefix);
                attr_spec = std::move(rest);
            }
        }
        std::string attr = normalize_query_attr_name(corpus, attr_spec);
        std::string feat_name;
        if (feats_is_subkey(attr, feat_name) && corpus.has_attr("feats")) {
            if (!corpus_has_ud_split_feats_column(corpus, feat_name)) {
                col.kind = AggregateBucketData::Column::Kind::FeatsComposite;
                col.pa = &corpus.attr("feats");
                col.feats_sub_key = feat_name;
                out.columns.push_back(std::move(col));
                continue;
            }
        }
        if (corpus.has_attr(attr)) {
            // `n.form`-style: label is not a struct prefix; prefer region row attribute when
            // the short name exists on a structural type (matches read_tabulate_field order).
            if (!col.named_anchor.empty()) {
                bool any_region = false;
                for (const std::string& st : corpus.structure_names()) {
                    if (!corpus.has_structure(st)) continue;
                    const auto& sa = corpus.structure(st);
                    if (resolve_region_attr_key(sa, st, attr)) {
                        any_region = true;
                        break;
                    }
                }
                if (any_region) {
                    // the region's value when the label names a region (`c:<contr>`),
                    // the token's positional attribute when it names a token
                    col.kind = AggregateBucketData::Column::Kind::RegionFromBinding;
                    col.region_attr_name = attr;
                    col.pa = &corpus.attr(attr);
                    col.sa = nullptr;
                    out.columns.push_back(std::move(col));
                    continue;
                }
            }
            col.kind = AggregateBucketData::Column::Kind::Positional;
            col.pa = &corpus.attr(attr);
            out.columns.push_back(std::move(col));
            continue;
        }
        bool found_reg = false;
        for (const auto& ra_name : corpus.region_attr_names()) {
            if (ra_name != attr_spec) continue;
            auto us = ra_name.find('_');
            if (us == std::string::npos || us + 1 >= ra_name.size()) return false;
            std::string sn = ra_name.substr(0, us);
            std::string ran = ra_name.substr(us + 1);
            if (!corpus.has_structure(sn)) return false;
            const auto& sa = corpus.structure(sn);
            auto rkey = resolve_region_attr_key(sa, sn, ran);
            if (!rkey) return false;
            col.kind = AggregateBucketData::Column::Kind::Region;
            col.sa = &sa;
            col.region_attr_name = *rkey;
            out.columns.push_back(std::move(col));
            found_reg = true;
            break;
        }
        if (!found_reg) return false;
    }
    return true;
}

bool fill_aggregate_key(AggregateBucketData& data, const Corpus& corpus, const Match& m,
                        const NameIndexMap& nm, std::vector<int64_t>& key_out) {
    key_out.resize(data.columns.size());
    for (size_t i = 0; i < data.columns.size(); ++i) {
        const auto& col = data.columns[i];
        auto intern_value = [&](std::string value) {
            auto& st = data.region_intern[i];
            auto it = st.str_to_id.find(value);
            if (it != st.str_to_id.end()) {
                key_out[i] = it->second;
            } else {
                int64_t id = static_cast<int64_t>(st.id_to_str.size() + 1);
                st.str_to_id.emplace(value, id);
                st.id_to_str.push_back(std::move(value));
                key_out[i] = id;
            }
        };
        if (col.kind == AggregateBucketData::Column::Kind::FeatsComposite) {
            CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                     : resolve_name(m, nm, col.named_anchor);
            if (pos == NO_HEAD) return false;
            std::string val = std::string(feats_extract_value(col.pa->value_at(pos), col.feats_sub_key));
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                val = apply_date_transform_bucket(val, col.date_transform);
            intern_value(std::move(val));
        } else if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                     : resolve_name(m, nm, col.named_anchor);
            if (pos == NO_HEAD) return false;
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None) {
                std::string val(col.pa->value_at(pos));
                intern_value(apply_date_transform_bucket(val, col.date_transform));
            } else {
                key_out[i] = static_cast<int64_t>(col.pa->id_at(pos));
            }
        } else if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding) {
            auto nr = m.named_regions.find(col.named_anchor);
            if (nr == m.named_regions.end()) {
                // a named token, not a region: its positional attribute
                if (!col.pa) return false;
                CorpusPos pos = resolve_name(m, nm, col.named_anchor);
                if (pos == NO_HEAD) return false;
                std::string val(col.pa->value_at(pos));
                if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                    val = apply_date_transform_bucket(val, col.date_transform);
                intern_value(std::move(val));
                continue;
            }
            const auto& sa = corpus.structure(nr->second.struct_name);
            std::optional<std::string> val;
            if (auto rk = resolve_region_attr_key(sa, nr->second.struct_name, col.region_attr_name)) {
                val = std::string(sa.region_value(*rk, nr->second.region_idx));
            } else {
                val = lookup_super_region_attr_value(
                    corpus, sa, nr->second.struct_name, nr->second.region_idx,
                    col.region_attr_name);
            }
            if (!val) return false;
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                *val = apply_date_transform_bucket(*val, col.date_transform);
            intern_value(std::move(*val));
        } else {
            int64_t rgn = -1;
            if (!col.named_anchor.empty()) {
                auto nr = m.named_regions.find(col.named_anchor);
                if (nr != m.named_regions.end()) {
                    if (&corpus.structure(nr->second.struct_name) != col.sa) return false;
                    rgn = static_cast<int64_t>(nr->second.region_idx);
                }
            }
            if (rgn < 0) {
                CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                         : resolve_name(m, nm, col.named_anchor);
                if (pos == NO_HEAD) return false;
                rgn = col.sa->find_region(pos);
                if (rgn < 0) return false;
            }
            std::string val(col.sa->region_value(col.region_attr_name, static_cast<size_t>(rgn)));
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                val = apply_date_transform_bucket(val, col.date_transform);
            intern_value(std::move(val));
        }
    }
    return true;
}

} // namespace

bool QueryExecutor::match_survives_post_filters_for_aggregate(
        const TokenQuery& query,
        const NameIndexMap& name_map,
        const std::vector<AnchorConstraint>& anchor_constraints,
        MatchSet& scratch,
        const Match& m) const {
    scratch.matches.clear();
    scratch.matches.push_back(m);
    scratch.total_count = 1;
    apply_anchor_filters(anchor_constraints, scratch);
    if (scratch.matches.empty()) return false;
    apply_within_having(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_not_within(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_containing(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_position_orders(query, name_map, scratch);
    if (scratch.matches.empty()) return false;
    apply_global_filters(query, name_map, scratch);
    return !scratch.matches.empty();
}

// `:: text_langcode="nld"` is not part of token cardinality — without this check we would
// still enumerate every token matching [] and reject each in pass_region_filters.
// For EQ, if no region of that type ever carries the value, the result is necessarily empty.
static bool global_region_eq_unsatisfiable(const Corpus& corpus,
                                          const std::vector<GlobalRegionFilter>& filters) {
    for (const auto& gf : filters) {
        if (gf.op != CompOp::EQ) continue;
        size_t us = gf.region_attr.find('_');
        if (us == std::string::npos || us + 1 >= gf.region_attr.size()) continue;
        std::string struct_name = gf.region_attr.substr(0, us);
        std::string attr_name = gf.region_attr.substr(us + 1);
        if (!corpus.has_structure(struct_name)) return true;
        const auto& sa = corpus.structure(struct_name);
        auto rkey = resolve_region_attr_key(sa, struct_name, attr_name);
        if (!rkey) return true;
        const std::string composite = struct_name + "_" + *rkey;
        const bool is_mv = corpus.is_multivalue(composite);
        if (sa.has_region_value_reverse(*rkey) && !is_mv) {
            if (sa.count_regions_with_attr_eq(*rkey, gf.value) > 0)
                continue;
        }
        bool any = false;
        for (size_t ri = 0; ri < sa.region_count(); ++ri) {
            std::string_view rval = sa.region_value(*rkey, ri);
            if (compare_value_maybe_mv(CompOp::EQ, rval, gf.value, is_mv)) {
                any = true;
                break;
            }
        }
        if (!any) return true;
    }
    return false;
}

NameIndexMap QueryExecutor::build_name_map_for_stripped_query(const TokenQuery& query) {
    std::vector<AnchorConstraint> constraints;
    bool has_anchors = false;
    for (const auto& tok : query.tokens) {
        if (tok.is_anchor()) {
            has_anchors = true;
            break;
        }
    }
    if (!has_anchors)
        return build_name_map(query);
    TokenQuery stripped = strip_anchors(query, constraints);
    NameIndexMap map = build_name_map(stripped);
    // `n:<err>;` (and similar) strips to zero tokens; the label still names match slot 0
    // for Phase B token-group matches and `count by n.code`-style projections.
    if (stripped.tokens.empty()) {
        size_t labeled_anchors = 0;
        std::string lone;
        for (const auto& tok : query.tokens) {
            if (tok.is_anchor() && !tok.name.empty()) {
                ++labeled_anchors;
                lone = tok.name;
            }
        }
        if (labeled_anchors == 1)
            map[lone] = 0;
    }
    return map;
}

MatchSet QueryExecutor::execute_impl(const TokenQuery& query,
                                size_t max_matches,
                                bool count_total,
                                size_t max_total_cap,
                                size_t sample_size,
                                uint32_t random_seed,
                                unsigned num_threads,
                                const std::vector<std::string>* aggregate_by_fields,
                                bool skip_name_validation) {
    if (!skip_name_validation)
        validate_query_name_bindings(query);
    wh_cache_ = {};   // (keyed on AST nodes, which a later query may reuse)

    // P1.12: post-filters (`within … having`, `containing`, `not within`, `::`
    // functions / alignment, position orders) run on each hit as it is found
    // (add_resolved_match), so no path counts or pages hits the filters did not
    // see. A page without a total stops once it is full; with a total every hit
    // is enumerated (max_matches 0: no path counts past a full page without
    // seeing the hits) and only the page is kept (store_cap_), so memory stays
    // O(page) instead of the whole hit list.
    const bool has_post = query.within_having || query.not_within
        || !query.containing_clauses.empty() || !query.position_orders.empty()
        || !query.global_alignment_filters.empty() || !query.global_function_filters.empty();
    if (has_post && !post_per_hit_ && !aggregate_by_fields) {
        struct Scope {
            QueryExecutor& ex;
            bool per_hit;
            size_t cap;
            ~Scope() { ex.post_per_hit_ = per_hit; ex.store_cap_ = cap; }
        } scope{*this, post_per_hit_, store_cap_};
        post_per_hit_ = true;
        const size_t page = max_matches;
        const bool enumerate = count_total && page > 0 && sample_size == 0;
        store_cap_ = enumerate ? page : 0;
        MatchSet ms = execute_impl(query, enumerate ? 0 : page, count_total, max_total_cap,
                                   sample_size, random_seed, num_threads, nullptr, true);
        // (paths with their own filter pass — region anchors — keep every hit)
        if (page > 0 && sample_size == 0 && ms.matches.size() > page)
            ms.matches.erase(ms.matches.begin() + static_cast<std::ptrdiff_t>(page), ms.matches.end());
        if (!count_total && page > 0) {
            ms.total_count = ms.matches.size();
            ms.total_exact = false;
        } else if (max_total_cap > 0 && ms.total_count >= max_total_cap) {
            ms.total_count = max_total_cap;
            ms.total_exact = false;
        }
        return ms;
    }

    // Phase A.5: token-group structs (e.g. discontinuous `err`) cover non-contiguous
    // sets of tokens. `within` / `containing` operators assume a contiguous
    // [start..end] interval, so the implicit choice between member positions and
    // envelope would silently mislead users on whichever interpretation they didn't
    // pick. Reject up front with a clear error.
    if (!query.within.empty() && corpus_.is_token_group(query.within)) {
        throw std::runtime_error(
            "'" + query.within + "' is a token-group (discontinuous) struct and cannot "
            "be used with `within`. Use [" + query.within + "_gid=\"…\"] for token-level "
            "filtering, or query the group as a top-level match.");
    }
    for (const auto& cc : query.containing_clauses) {
        if (!cc.region.empty() && corpus_.is_token_group(cc.region)) {
            throw std::runtime_error(
                "'" + cc.region + "' is a token-group (discontinuous) struct and cannot "
                "be used with `containing`. Use [" + cc.region + "_gid=\"…\"] for "
                "token-level filtering, or query the group as a top-level match.");
        }
    }

    // Phase B: standalone token-group anchor query, e.g. `<err code="SPLIT">;`.
    // When the query is exactly one REGION_START anchor whose region is a
    // token-group struct, expand each matching group from the GroupIndex
    // sidecar (`groups/<struct>.jsonl`) into one Match whose positions[] /
    // span_ends[] hold the disjoint sub-spans verbatim. The existing Match
    // representation already supports non-contiguous matches, so KWIC,
    // count, freq, and sample need no changes.
    if (query.tokens.size() == 1
        && query.tokens[0].is_anchor()
        && query.tokens[0].anchor == RegionAnchorType::REGION_START
        && corpus_.is_token_group(query.tokens[0].anchor_region)
        && query.containing_clauses.empty()
        && query.within.empty()
        && query.global_region_filters.empty()
        && query.global_function_filters.empty()
        && query.global_alignment_filters.empty()
        && query.position_orders.empty()) {
        const auto& anchor = query.tokens[0];
        const GroupIndex& gi = corpus_.group_index(anchor.anchor_region);
        MatchSet out;
        out.num_tokens = 1;
        out.total_exact = true;
        // `where MM` filter helper: returns true iff at least one of `rec`'s
        // member positions appears in the sorted `ps`. Member intersection,
        // not envelope — gap tokens don't count.
        auto group_intersects = [](const GroupRecord& rec,
                                   const std::vector<CorpusPos>& ps) {
            if (ps.empty()) return false;
            for (const auto& sp : rec.spans) {
                auto lo = std::lower_bound(ps.begin(), ps.end(), sp.first);
                if (lo != ps.end() && *lo <= sp.second) return true;
            }
            return false;
        };
        for (const auto& rec : gi.records()) {
            // Filter by required prop attrs declared on the anchor.
            bool ok = true;
            for (const auto& kv : anchor.anchor_attrs) {
                bool found = false;
                for (const auto& pv : rec.props) {
                    if (pv.first == kv.first && pv.second == kv.second) {
                        found = true;
                        break;
                    }
                }
                if (!found) { ok = false; break; }
            }
            if (!ok) continue;
            // `where` filter — AND across all referenced match sets.
            for (const auto& ps : anchor.where_positions) {
                if (!group_intersects(rec, ps)) { ok = false; break; }
            }
            if (!ok) continue;
            Match m;
            m.token_group_match = true;
            m.token_group_props = rec.props;
            for (const auto& s : rec.spans) {
                m.positions.push_back(s.first);
                m.span_ends.push_back(s.second);
            }
            out.matches.push_back(std::move(m));
            ++out.total_count;
            if (max_matches > 0 && out.matches.size() >= max_matches && !count_total)
                break;
            if (max_total_cap > 0 && out.total_count >= max_total_cap) {
                out.total_exact = false;
                break;
            }
        }
        return out;
    }

    // Strip region anchors (<s>, </s>) from query, recording constraints
    std::vector<AnchorConstraint> anchor_constraints;
    bool has_anchors = false;
    for (const auto& tok : query.tokens) {
        if (tok.is_anchor()) { has_anchors = true; break; }
    }
    TokenQuery stripped_query;
    if (has_anchors) stripped_query = strip_anchors(query, anchor_constraints);
    const TokenQuery& q = has_anchors ? stripped_query : query;

    if (!skip_compile_) compile_query(q);
    if (global_region_eq_unsatisfiable(corpus_, q.global_region_filters)) {
        MatchSet empty;
        empty.total_exact = true;
        return empty;
    }

    std::vector<AnchorConstraint> token_anchor_constraints;
    token_anchor_constraints.reserve(anchor_constraints.size());
    for (const auto& ac : anchor_constraints) {
        if (!ac.region_enumeration)
            token_anchor_constraints.push_back(ac);
    }

    if (q.tokens.empty()) {
        for (const auto& ac : anchor_constraints) {
            if (ac.region_enumeration) {
                MatchSet re = execute_region_enumeration(anchor_constraints, q, max_matches, count_total,
                                                         max_total_cap, sample_size, random_seed);
                if (aggregate_by_fields && !aggregate_by_fields->empty() && sample_size == 0) {
                    std::shared_ptr<AggregateBucketData> agg_storage =
                        std::make_shared<AggregateBucketData>();
                    if (!build_aggregate_plan(corpus_, *aggregate_by_fields, *agg_storage))
                        return re;
                    // Named anchors (`n:<node>`, `n:<contr>`) must resolve in fill_aggregate_key;
                    // an empty map breaks `count by n.form` (positional) while `n.type` worked
                    // because build_aggregate_plan failed and we fell back to read_tabulate_field.
                    NameIndexMap name_map = build_name_map_for_stripped_query(query);
                    MatchSet out;
                    out.num_tokens = 0;
                    out.total_exact = re.total_exact;
                    for (const auto& m : re.matches) {
                        std::vector<int64_t> akey;
                        if (!fill_aggregate_key(*agg_storage, corpus_, m, name_map, akey))
                            continue;
                        ++agg_storage->total_hits;
                        ++agg_storage->counts[std::move(akey)];
                    }
                    out.aggregate_buckets = std::move(agg_storage);
                    return out;
                }
                return re;
            }
        }
        // Single REGION_START anchor with no stripped tokens (e.g. `<mwe>;` or `<overlay-mwe>;`)
        // yields an empty query and previously returned no matches with no explanation.
        if (query.tokens.size() == 1 && query.tokens[0].is_anchor()
            && query.tokens[0].anchor == RegionAnchorType::REGION_START) {
            const std::string& r = query.tokens[0].anchor_region;
            if (!corpus_.has_structure(r) && !corpus_.is_token_group(r)) {
                std::string msg = "Unknown region anchor '<" + r
                                  + ">' (not a structural region or token-group struct).";
                const auto& tgs = corpus_.token_group_structs();
                if (!tgs.empty()) {
                    msg += " Known token-group names: ";
                    for (size_t i = 0; i < tgs.size(); ++i) {
                        if (i)
                            msg += ", ";
                        msg += tgs[i];
                    }
                }
                msg += " Overlay stand-off groups use merged names `overlay-<layer>-<struct>` (e.g. "
                       "`overlay-mwe-mwe` when layer and struct are both `mwe`).";
                throw std::runtime_error(msg);
            }
        }
        return MatchSet{};
    }

    MatchSet result;
    size_t n = q.tokens.size();
    result.num_tokens = n;

    // P4.1 (see QueryExecutor::range_): the fast paths' start mask with this
    // worker's position range folded in. A path marks its result (range_ok) once
    // it has applied such a mask to every hit it emits or counts.
    auto build_start_mask = [&]() {
        RegionPosMask m = build_region_eq_position_mask(corpus_, q.global_region_filters);
        if (range_) restrict_mask_to_range(m, *range_, corpus_.size());
        return m;
    };
    auto mark_ranged = [&](const RegionPosMask& m) {
        if (range_ && m.ranged) result.range_ok = true;
    };
    NameIndexMap name_map = build_name_map(q);

    std::shared_ptr<AggregateBucketData> agg_storage;
    AggregateBucketData* agg_ptr = nullptr;
    MatchSet post_scratch;
    bool agg_capped = false;
    if (aggregate_by_fields && !aggregate_by_fields->empty() && sample_size == 0) {
        agg_storage = std::make_shared<AggregateBucketData>();
        if (build_aggregate_plan(corpus_, *aggregate_by_fields, *agg_storage))
            agg_ptr = agg_storage.get();
        else
            agg_storage.reset();
    }
    if (agg_ptr)
        post_scratch.matches.reserve(1);

    // Per-match post-filter pass is expensive at millions of hits; skip when only
    // inline :: region filters apply (handled above) and no other post-filters exist.
    const bool agg_per_match_post =
        agg_ptr
        && (!token_anchor_constraints.empty()
            || (q.within_having && !q.within.empty() && corpus_.has_structure(q.within))
            || (q.not_within && !q.within.empty() && corpus_.has_structure(q.within))
            || !q.containing_clauses.empty() || !q.position_orders.empty()
            || !q.global_alignment_filters.empty() || !q.global_function_filters.empty());

    // RG-REG-7: the fast aggregate path can handle simple anchor constraints inline,
    // so don't use `agg_per_match_post` as the fast-path gate — compute a narrower
    // block that excludes anchor constraints (try_fast_aggregate will decline if the
    // constraints turn out to be too complex).
    const bool agg_fast_path_blocked =
        agg_ptr
        && ((q.within_having && !q.within.empty() && corpus_.has_structure(q.within))
            || (q.not_within && !q.within.empty() && corpus_.has_structure(q.within))
            || !q.containing_clauses.empty() || !q.position_orders.empty()
            || !q.global_alignment_filters.empty() || !q.global_function_filters.empty());

    // P7.2 / P7.3: flat counters; with no per-hit filters the fast paths' hits go
    // straight into them (agg_sink) without building a Match.
    FlatAggCounter flat_agg;
    bool agg_sink = false;
    if (agg_ptr && fastpath_mode() == FastPathMode::On && flat_agg.init(*agg_ptr, name_map, n, corpus_, !anchor_constraints.empty())) {
        bool any_dep_subtree = false;
        for (const auto& tok : q.tokens) any_dep_subtree |= tok.is_dep_subtree;
        agg_sink = !agg_per_match_post && token_anchor_constraints.empty()
                   && q.global_region_filters.empty() && !any_dep_subtree;
    }
    auto flush_flat_agg = [&]() {
        if (agg_ptr) flat_agg.flush(*agg_ptr);
    };
    // P7.8: hits straight to the caller's sink (coll / dcoll), same conditions as
    // the aggregation sink: nothing to check per hit after the kernels
    bool sink_on = false;
    if (hit_sink_ && !agg_ptr && sample_size == 0 && fastpath_mode() == FastPathMode::On) {
        sink_on = token_anchor_constraints.empty() && may_sink_hits(q);
    }


    HitSample sampler(sample_size, random_seed);

    // Partial match vectors are of size 2*n: [0..n-1]=starts, [n..2n-1]=ends
    // For non-repeating tokens: pm[i] == pm[n+i]
    auto build_match = [this, n, &q](std::vector<CorpusPos>&& pm) -> Match {
        Match m;
        m.positions.assign(pm.begin(), pm.begin() + n);
        m.span_ends.assign(pm.begin() + n, pm.begin() + 2 * n);
        materialize_dep_subtree_bindings(q, m);
        return m;
    };

    // Post-anchor body: runs once per resolved anchor-binding assignment (RG-REG-5 fan-out).
    auto add_resolved_match = [&](Match& m) -> bool {
        // Apply :: region filters inline so rejected matches don't consume the limit
        if (!q.global_region_filters.empty() && !match_passes_inline_region_filters(m, q, name_map))
            return true;
        if (agg_ptr) {
            if (agg_per_match_post &&
                !match_survives_post_filters_for_aggregate(q, name_map, token_anchor_constraints,
                                                          post_scratch, m))
                return true;
            uint64_t fkey = 0;
            std::vector<int64_t> akey;
            if (flat_agg.on ? !flat_agg.key_of(m.positions.data(), m.positions.size(), fkey)
                            : !fill_aggregate_key(*agg_ptr, corpus_, m, name_map, akey))
                return true;
            if (max_total_cap > 0 && agg_ptr->total_hits >= max_total_cap) {
                agg_capped = true;
                return false;
            }
            if (flat_agg.on) flat_agg.inc(fkey);
            else ++agg_ptr->counts[std::move(akey)];
            ++agg_ptr->total_hits;
            progress_tick(m.first_pos(), agg_ptr->total_hits);
            return true;
        }
        // P1.12: the post-filters, per hit (before it counts toward the page / total)
        if (post_per_hit_ && !passes_post_filters(q, name_map, m, post_scratch))
            return true;
        ++result.total_count;
        progress_tick(m.first_pos(), result.total_count);
        if (sample_size > 0) {
            sampler.offer(std::move(m));
        } else if (store_cap_ > 0 ? result.matches.size() < store_cap_
                                  : (max_matches == 0 || result.matches.size() < max_matches)) {
            result.matches.push_back(std::move(m));
        }
        return true;
    };

    auto add_match = [&](std::vector<CorpusPos>&& positions) {
        if (sink_on) {
            hit_sink_->hit(positions.data(), positions.data() + n, n);
            ++sink_hits_;
            ++result.total_count;
            progress_tick(positions[0] != NO_HEAD ? positions[0] : 0, result.total_count);
            return;
        }
        if (agg_sink) {
            // P7.3: count the hit from its token starts; no Match
            uint64_t fkey = 0;
            if (!flat_agg.key_of(positions.data(), n, fkey)) return;
            if (max_total_cap > 0 && agg_ptr->total_hits >= max_total_cap) {
                agg_capped = true;
                return;
            }
            flat_agg.inc(fkey);
            ++agg_ptr->total_hits;
            // the first token can be an optional one that did not take part
            progress_tick(positions[0] != NO_HEAD ? positions[0] : 0, agg_ptr->total_hits);
            return;
        }
        Match m = build_match(std::move(positions));
        if (token_anchor_constraints.empty()) {
            add_resolved_match(m);
            return;
        }
        // RG-REG-5: fan out one output Match per valid anchor-binding assignment
        // (Cartesian product in Fanout mode; single innermost row in Innermost mode).
        // Each fanned-out match counts independently toward caps / limits.
        expand_anchor_constraints(m, token_anchor_constraints,
                                  [&](Match& r) { return add_resolved_match(r); });
    };

    auto reached_limit = [&]() {
        if (sample_size > 0) return false;  // sampling needs full enumeration
        if (agg_ptr) return false;
        return max_matches > 0 && result.total_count >= max_matches
               && !count_total;
    };
    auto reached_total_cap = [&]() {
        if (agg_ptr) return agg_capped;
        return count_total && max_total_cap > 0 && result.total_count >= max_total_cap;
    };
    // Common exit for every path that fed hits through add_match(): with an
    // aggregation (`…; count by …`) hand back the buckets, otherwise run the
    // post-filters on the materialised hits. Fast paths used to return `result`
    // directly, which dropped the buckets (`[ADJ] [NOUN]; count by lemma` → 0).
    auto finish_query = [&]() -> MatchSet {
        if (sample_size > 0) result.matches = sampler.take();
        if (agg_ptr) {
            flush_flat_agg();
            result.matches.clear();
            result.total_count = agg_ptr->total_hits;
            result.total_exact = !agg_capped;
            result.aggregate_buckets = std::move(agg_storage);
            return std::move(result);
        }
        apply_anchor_filters(token_anchor_constraints, result);
        if (!post_per_hit_) {   // (P1.12: already applied to each hit)
            apply_within_having(q, result);
            apply_not_within(q, result);
            apply_containing(q, result);
            apply_position_orders(q, name_map, result);
            apply_global_filters(q, name_map, result);
        }
        result.total_exact = !reached_limit() && !reached_total_cap();
        return std::move(result);
    };

    // ── Merge operands (P1.4 / P1.5) ─────────────────────────────────────
    // A plain EQ leaf is a zero-copy `.rev` span. Other boolean combinations of
    // leaves (`&`, `|`, `%c`/`%d`, regex, `!=`, token-level region attrs) are
    // materialised once via resolve_conditions() — sorted unique positions — so
    // they stay on the merge / dep fast paths instead of falling back to probing.
    // Skipped when the operand would cover more than half the corpus (dense `!=`).
    const int corpus_rev_width = [&] {
        for (const auto& a : corpus_.attr_names())
            if (corpus_.has_attr(a) && !corpus_.is_multivalue(a))
                return corpus_.attr(a).rev_width();
        return 8;
    }();
    std::function<bool(const ConditionPtr&)> plain_condition = [&](const ConditionPtr& c) -> bool {
        if (!c) return false;
        if (c->is_leaf) {
            const AttrCondition& ac = c->leaf;
            if (ac.op == CompOp::IN) return true;   // a position list already
            if (ac.is_nvals) return false;
            if (ac.op != CompOp::EQ && ac.op != CompOp::NEQ && ac.op != CompOp::REGEX)
                return false;
            std::string name = normalize_attr(ac.attr);
            std::string feat_name;
            if (feats_is_subkey(name, feat_name)) return false;
            if (corpus_.has_attr(name)) return !corpus_.is_multivalue(name);
            // token-level region attribute (`text_langcode="en"` inside [...])
            RegionAttrParts parts;
            return ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive
                && split_region_attr_name(name, parts) && corpus_.has_structure(parts.struct_name);
        }
        if (c->is_structural || c->is_count) return false;
        return plain_condition(c->left) && plain_condition(c->right);
    };
    // (defined below; merge_operand materialises through it when bitmaps apply)
    std::function<std::unique_ptr<BmExpr>(const ConditionPtr&, bool*)> compile_bm;
    bool in_compile_bm = false;
    auto merge_operand = [&](const ConditionPtr& c) -> SeqMergeOperand {
        SeqMergeOperand o = seq_merge_operand(corpus_, c);
        if (o.kind != SeqMergeTok::Complex || fastpath_mode() != FastPathMode::On) return o;
        if (!plain_condition(c)) return o;
        const size_t est = estimate_cardinality(c);
        if (est > static_cast<size_t>(corpus_.size()) / 2) return o;
        if (windowed()) {   // P6.5e: only the window's share is built
            const double f = static_cast<double>(operand_window_.hi - operand_window_.lo)
                             / static_cast<double>(std::max<CorpusPos>(corpus_.size(), 1));
            if (static_cast<double>(est) * f > static_cast<double>(materialize_max())) return o;
        } else if (est > materialize_max()) {
            return o;
        }
        auto materialize = [&]() -> SeqMergeOperand {
            // P3.6: a combination over bitmap attributes / region intervals
            // (`[upos="VERB" & text_langcode="en"]`) is evaluated chunk by chunk
            // instead of resolving and intersecting position lists.
            if (!in_compile_bm && bitmap_mode() != BitmapMode::Off && compile_bm) {
                in_compile_bm = true;
                bool dense = false;
                std::unique_ptr<BmExpr> e = compile_bm(c, &dense);
                in_compile_bm = false;
                if (e && dense) {
                    std::vector<CorpusPos> pos;
                    pos.reserve(std::min(est, e->estimate));
                    const CorpusPos N = windowed() ? std::min(operand_window_.hi, corpus_.size()) : corpus_.size();
                    const CorpusPos W0 = windowed() ? operand_window_.lo : 0;
                    const size_t nch = static_cast<size_t>((N + BitmapIndex::kChunk - 1) >> BitmapIndex::kChunkShift);
                    for (size_t ch = static_cast<size_t>(W0 >> BitmapIndex::kChunkShift); ch < nch; ++ch) {
                        if ((ch & 0xFF) == 0) check_cancelled();
                        const BmChunk v = e->load(ch);
                        if (v.zero) continue;
                        const CorpusPos base = static_cast<CorpusPos>(ch) << BitmapIndex::kChunkShift;
                        for (size_t w = 0; w < BitmapIndex::kWords; ++w)
                            for (uint64_t x = v.w[w]; x; x &= x - 1) {
                                const CorpusPos p = base + static_cast<CorpusPos>(w * 64) + __builtin_ctzll(x);
                                if (p < N && p >= W0) pos.push_back(p);
                            }
                    }
                    return owned_postings(pos, corpus_rev_width);
                }
            }
            std::vector<CorpusPos> pos = resolve_conditions(c);
            if (windowed()) {   // leaves resolved without the window (regions, feats, …)
                const CorpusPos lo = operand_window_.lo, hi = operand_window_.hi;
                pos.erase(std::remove_if(pos.begin(), pos.end(),
                                         [&](CorpusPos p) { return p < lo || p >= hi; }),
                          pos.end());
            }
            return owned_postings(pos, corpus_rev_width);
        };
        // P4.1: the ranges of one partitioned query share the materialised list
        // (the probe range builds it, the others reuse it); a windowed operand
        // (P6.5e) is only this range's part
        Caches& cc = *caches_;
        if (!cc.share_operands || windowed()) return materialize();
        {
            std::lock_guard<std::mutex> lk(cc.operands_mu);
            auto it = cc.operands.find(c.get());
            if (it != cc.operands.end())
                return *std::static_pointer_cast<SeqMergeOperand>(it->second.second);
        }
        SeqMergeOperand r = materialize();
        std::lock_guard<std::mutex> lk(cc.operands_mu);
        cc.operands.emplace(c.get(), std::make_pair(std::shared_ptr<const void>(c),
                                                    std::shared_ptr<void>(std::make_shared<SeqMergeOperand>(r))));
        return r;
    };

    // ── Bitmap expressions (P3.2) ────────────────────────────────────────
    // A token condition → per-chunk bitmap expression: values of attributes with
    // `<attr>.bm` (EQ, id sets from regex / %c, `!=`), `.rev` postings for other
    // attributes (or a materialised merge operand), AND / OR / NOT of those.
    // nullptr = not expressible (feats sub-keys, MV, structural, count, …).
    // *dense is set when a real bitmap or a complement is involved (the cases
    // where the kernel beats the merge paths).
    compile_bm = [&](const ConditionPtr& c, bool* dense) -> std::unique_ptr<BmExpr> {
        auto from_merge_operand = [&]() -> std::unique_ptr<BmExpr> {
            SeqMergeOperand o = merge_operand(c);
            if (o.kind != SeqMergeTok::EqRev) return nullptr;
            return std::make_unique<BmPostings>(o.span, o.owned);
        };
        if (!c) return nullptr;
        if (!c->is_leaf) {
            if (c->is_structural || c->is_count) return nullptr;
            auto a = compile_bm(c->left, dense);
            auto b = a ? compile_bm(c->right, dense) : nullptr;
            if (!a || !b) return from_merge_operand();
            if (c->bool_op == BoolOp::AND) return std::make_unique<BmAnd>(std::move(a), std::move(b));
            return std::make_unique<BmOr>(std::move(a), std::move(b));
        }
        const AttrCondition& ac = c->leaf;
        if (ac.is_nvals) return nullptr;
        const std::string name = normalize_attr(ac.attr);
        if (!corpus_.has_attr(name)) {
            // token-level region attribute: the matching regions' position intervals
            RegionAttrParts parts;
            if (ac.op == CompOp::EQ && !ac.case_insensitive && !ac.diacritics_insensitive
                && split_region_attr_name(name, parts) && corpus_.has_structure(parts.struct_name)) {
                GlobalRegionFilter gf;
                gf.region_attr = name;
                gf.value = ac.value;
                RegionPosMask m = build_region_eq_position_mask(corpus_, {gf});
                if (m.status == RegionMaskStatus::Unsatisfiable)
                    return std::make_unique<BmPostings>(RevSpan{});
                if (m.status == RegionMaskStatus::Ready) {
                    std::vector<std::pair<CorpusPos, CorpusPos>> iv;
                    iv.reserve(m.iv.size());
                    for (const auto& x : m.iv) iv.emplace_back(x.s, x.e);
                    *dense = true;
                    return std::make_unique<BmIntervals>(std::move(iv));
                }
            }
            return from_merge_operand();
        }
        if (corpus_.is_multivalue(name)) return from_merge_operand();
        const PositionalAttr& pa = corpus_.attr(name);
        std::shared_ptr<BitmapIndex> bi = bitmap_index(name);
        auto id_count = [&](int64_t id) { return pa.count_of_id(static_cast<LexiconId>(id)); };
        auto set_expr = [&](const std::vector<int64_t>& ids, int64_t known = -1) -> std::unique_ptr<BmExpr> {
            if (!bi && ids.size() > 64) return nullptr;   // (before counting: ids can be millions)
            size_t cnt = 0;
            if (known >= 0) cnt = static_cast<size_t>(known);
            else for (int64_t id : ids) cnt += id_count(id);
            if (bi) {
                *dense = true;
                if (ids.size() == 1) return std::make_unique<BmValue>(*bi, ids[0], cnt);
                return std::make_unique<BmValueSet>(*bi, ids, cnt);
            }
            if (ids.empty()) return std::make_unique<BmPostings>(RevSpan{});
            if (ids.size() > 64) return nullptr;
            std::unique_ptr<BmExpr> e;
            for (int64_t id : ids) {
                auto p = std::make_unique<BmPostings>(pa.rev_span_of_id(static_cast<LexiconId>(id)));
                if (e) e = std::make_unique<BmOr>(std::move(e), std::move(p));
                else e = std::move(p);
            }
            return e;
        };
        auto negate = [&](std::unique_ptr<BmExpr> e) -> std::unique_ptr<BmExpr> {
            if (!e) return nullptr;
            *dense = true;
            return std::make_unique<BmNot>(std::move(e), static_cast<size_t>(corpus_.size()));
        };
        // No bitmap and too many ids for an OR of postings: a truth table over the
        // lexicon, applied to the `.dat` ids chunk by chunk (dense `!=`, big regexes)
        auto table_expr = [&](const auto& ids, bool negated, int64_t known) -> std::unique_ptr<BmExpr> {
            const size_t V = static_cast<size_t>(pa.lexicon().size());
            auto tab = std::make_shared<std::vector<uint8_t>>(V, negated ? 1 : 0);
            size_t cnt = 0;
            for (auto id : ids) {
                if (id < 0 || static_cast<size_t>(id) >= V) continue;
                (*tab)[static_cast<size_t>(id)] = negated ? 0 : 1;
                if (known < 0) cnt += id_count(static_cast<int64_t>(id));
            }
            if (known >= 0) cnt = static_cast<size_t>(known);
            const size_t N = static_cast<size_t>(corpus_.size());
            if (negated) cnt = N > cnt ? N - cnt : 0;
            *dense = true;
            return std::make_unique<BmTable>(pa, std::move(tab), cnt, corpus_.size());
        };
        if (ac.id_set_resolved
            && (ac.op == CompOp::EQ || ac.op == CompOp::NEQ || ac.op == CompOp::REGEX)) {
            if (!bi && ac.id_set.size() > 64) {
                if (auto e = from_merge_operand()) return e;
                return table_expr(ac.id_set, ac.op == CompOp::NEQ, ac.id_set_total);
            }
            std::vector<int64_t> ids(ac.id_set.begin(), ac.id_set.end());
            auto e = set_expr(ids, ac.id_set_total);
            if (!e) return from_merge_operand();
            return ac.op == CompOp::NEQ ? negate(std::move(e)) : std::move(e);
        }
        if (ac.op == CompOp::EQ && ac.resolved_id >= 0) {
            if (bi) {
                *dense = true;
                return std::make_unique<BmValue>(*bi, ac.resolved_id, id_count(ac.resolved_id));
            }
            return std::make_unique<BmPostings>(pa.rev_span_of_id(static_cast<LexiconId>(ac.resolved_id)));
        }
        // Unresolved EQ / NEQ: the ids whose value matches like check_leaf does
        // (multivalue_eq: the whole value, or one part of a `|`-joined value) —
        // the exact entry plus the matching `|` entries (found once per attribute).
        auto mv_eq_ids = [&]() {
            std::vector<int64_t> ids;
            const Lexicon& lex = pa.lexicon();
            const LexiconId exact = lex.lookup(ac.value);
            if (exact != UNKNOWN_LEX) ids.push_back(exact);
            const std::string key = name + "\x1f|entries";
            auto pipes = corpus_.cached_id_set(key);
            if (!pipes) {
                auto v = lex.ids_containing('|');
                auto set = std::make_shared<Corpus::IdSet>(v.begin(), v.end());
                corpus_.cache_id_set(key, set);
                pipes = set;
            }
            for (int32_t id : *pipes)
                if (id != exact && multivalue_eq(lex.get(id), ac.value)) ids.push_back(id);
            std::sort(ids.begin(), ids.end());
            return ids;
        };
        if ((ac.op == CompOp::EQ || ac.op == CompOp::NEQ) && bi && !ac.neq_regex
            && !ac.case_insensitive && !ac.diacritics_insensitive) {
            auto e = set_expr(mv_eq_ids());
            return ac.op == CompOp::NEQ ? negate(std::move(e)) : std::move(e);
        }
        // The same without a bitmap (`[lemma!="shoe"]`): few ids → NOT of their
        // postings, many → a lexicon truth table.
        if ((ac.op == CompOp::EQ || ac.op == CompOp::NEQ) && !bi && !ac.neq_regex
            && !ac.case_insensitive && !ac.diacritics_insensitive) {
            if (auto e = from_merge_operand()) return e;
            std::vector<int64_t> ids = mv_eq_ids();
            if (ids.size() <= 64) {
                auto e = set_expr(ids);
                return ac.op == CompOp::NEQ ? negate(std::move(e)) : std::move(e);
            }
            return table_expr(ids, ac.op == CompOp::NEQ, -1);
        }
        return from_merge_operand();
    };

    // ── P3.2: bit-parallel kernel on chunked bitmaps ────────────────────
    // Start s matches iff every constrained token t has bit s + t set:
    // per 2^16-position chunk, acc = AND_t (B_t >> t), then the start
    // masks (corpus end, `:: match.…` region filter, `within` — the whole
    // span inside one region). The total past the first page is a popcount.
    // Tokens are ANDed rarest first, so empty chunks of a rare token skip
    // the rest. Used when a token involves a real bitmap (or `!=`) and every
    // token averages at least one hit per chunk; rarer operands stay on the
    // merge paths (PANDO_BITMAPS=force overrides, =off disables).
    // Returns true when it ran (result filled: the caller returns finish_query()).
    auto try_seq_bitmap = [&](const StructuralAttr* within_sa, bool within_span_semantics,
                              const RegionPosMask& region_mask, bool use_region_mask,
                              bool cheap_total_ok) -> bool {
        std::vector<std::shared_ptr<BitmapIndex>> bm_keep;
        if (fastpath_mode() == FastPathMode::On && bitmap_mode() != BitmapMode::Off
            && n <= static_cast<size_t>(kBmMaxShift) + 1 && !within_span_semantics
            && sample_size == 0) {
            struct BmTok { std::unique_ptr<BmExpr> e; int k; };
            std::vector<BmTok> bt;
            bool compiled = true, dense = false;
            size_t min_est = SIZE_MAX;
            for (size_t i = 0; i < n && compiled; ++i) {
                if (!q.tokens[i].conditions) continue;   // [] : no constraint
                auto e = compile_bm(q.tokens[i].conditions, &dense);
                if (!e) { compiled = false; break; }
                min_est = std::min(min_est, e->estimate);
                bt.push_back({std::move(e), static_cast<int>(i)});
            }
            const CorpusPos N = corpus_.size();
            const size_t nchunks = static_cast<size_t>((N + BitmapIndex::kChunk - 1)
                                                       >> BitmapIndex::kChunkShift);
            // Merge / gallop wins while the rarest token has only a few hits per
            // chunk (a chunk costs ~1024 word ANDs per token).
            // a lone `[]` (every position) is a popcount of the start masks
            const bool all_free = compiled && bt.empty() && n == 1;
            const bool use_bm = compiled
                && ((!bt.empty()
                     && (bitmap_mode() == BitmapMode::Force || (dense && min_est >= 8 * nchunks)))
                    || all_free);
            if (use_bm) {
                result.plan_path = "seq_bitmap";
                std::stable_sort(bt.begin(), bt.end(), [](const BmTok& a, const BmTok& b) {
                    return a.e->estimate < b.e->estimate;
                });
                constexpr size_t W = BitmapIndex::kWords;
                std::vector<uint64_t> acc(W), msk(W);
                const CorpusPos L = static_cast<CorpusPos>(n);
                const CorpusPos last_start = N - L;               // starts in [0, last_start]
                size_t ivc = 0;                                   // region-mask interval cursor
                size_t wr = 0;                                    // within region cursor
                const Region* wreg = within_sa ? within_sa->region_data() : nullptr;
                const size_t wn = within_sa ? within_sa->region_count() : 0;
                // P3.6: the structure's boundary bitmaps instead of walking its regions
                std::unique_ptr<BmValue> sC, sE;
                if (within_sa) {
                    const std::string wname = q.within.empty() ? corpus_.default_within() : q.within;
                    if (auto sbi = structure_bitmap(wname)) {
                        sC = std::make_unique<BmValue>(*sbi, BitmapIndex::kStructCovered, 0);
                        sE = std::make_unique<BmValue>(*sbi, BitmapIndex::kStructEnds, 0);
                        bm_keep.push_back(sbi);
                    }
                }
                auto set_range = [&](uint64_t* m, uint32_t a, uint32_t b) {   // inclusive
                    const uint32_t wa = a >> 6, wb = b >> 6;
                    const uint64_t ma = ~uint64_t{0} << (a & 63);
                    const uint64_t mb = ~uint64_t{0} >> (63 - (b & 63));
                    if (wa == wb) { m[wa] |= ma & mb; return; }
                    m[wa] |= ma;
                    for (uint32_t w = wa + 1; w < wb; ++w) m[w] = ~uint64_t{0};
                    m[wb] |= mb;
                };
                bool stop = false, counting = false;
                for (size_t c = 0; c < nchunks && !stop; ++c) {
                    const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
                    progress_tick(base, result.total_count, true);
                    if (base > last_start) break;
                    const CorpusPos top = std::min<CorpusPos>(base + BitmapIndex::kChunk - 1, last_start);
                    const uint32_t hi = static_cast<uint32_t>(top - base);   // last start offset
                    const size_t nw = (hi >> 6) + 1;
                    // `:: match.…` interval filter: skip chunks without an allowed start
                    // before touching any token (tokens' cursors are monotone).
                    if (use_region_mask && !region_mask.use_bits) {
                        const auto& iv = region_mask.iv;
                        while (ivc < iv.size() && iv[ivc].e < base) ++ivc;
                        if (ivc >= iv.size()) break;
                        if (iv[ivc].s > top) continue;
                    }
                    // AND of the shifted token bitmaps
                    bool any = false;
                    if (all_free) {
                        std::fill(acc.begin(), acc.begin() + static_cast<std::ptrdiff_t>(nw), ~uint64_t{0});
                        any = true;
                    }
                    for (size_t t = 0; t < bt.size(); ++t) {
                        const BmChunk v = bt[t].e->load(c);
                        if (v.zero) { any = false; break; }
                        const int k = bt[t].k;
                        uint64_t orr = 0;
                        if (t == 0) {
                            if (k == 0) {
                                for (size_t w = 0; w < nw; ++w) orr |= (acc[w] = v.w[w]);
                            } else {
                                for (size_t w = 0; w < nw; ++w) orr |= (acc[w] = v.shifted(w, k));
                            }
                        } else if (k == 0) {
                            for (size_t w = 0; w < nw; ++w) orr |= (acc[w] &= v.w[w]);
                        } else {
                            for (size_t w = 0; w < nw; ++w) orr |= (acc[w] &= v.shifted(w, k));
                        }
                        any = orr != 0;
                        if (!any) break;
                    }
                    if (!any) continue;
                    // start masks
                    if ((hi & 63) != 63) acc[nw - 1] &= ~uint64_t{0} >> (63 - (hi & 63));
                    if (use_region_mask) {
                        if (region_mask.use_bits) {
                            const size_t b0 = static_cast<size_t>(base >> 6);
                            for (size_t w = 0; w < nw; ++w)
                                acc[w] &= b0 + w < region_mask.bits.size() ? region_mask.bits[b0 + w] : 0;
                        } else {
                            const auto& iv = region_mask.iv;
                            while (ivc < iv.size() && iv[ivc].e < base) ++ivc;
                            std::fill(msk.begin(), msk.begin() + nw, 0);
                            for (size_t j = ivc; j < iv.size() && iv[j].s <= top; ++j) {
                                const CorpusPos a = std::max(iv[j].s, base);
                                const CorpusPos b = std::min(iv[j].e, top);
                                if (b >= a) set_range(msk.data(), static_cast<uint32_t>(a - base),
                                                      static_cast<uint32_t>(b - base));
                            }
                            for (size_t w = 0; w < nw; ++w) acc[w] &= msk[w];
                        }
                    }
                    if (sC) {
                        // start covered, no region end among the first L-1 positions
                        const BmChunk cv = sC->load(c), ev = sE->load(c);
                        for (size_t w = 0; w < nw; ++w) {
                            uint64_t m = cv.w[w];
                            for (int d = 0; d + 1 < static_cast<int>(L) && m; ++d) m &= ~ev.shifted(w, d);
                            acc[w] &= m;
                        }
                    } else if (wreg) {
                        // allowed starts: [r.start, r.end - (L-1)] for every region r
                        while (wr < wn && wreg[wr].end < base) ++wr;
                        std::fill(msk.begin(), msk.begin() + nw, 0);
                        for (size_t j = wr; j < wn && wreg[j].start <= top; ++j) {
                            const CorpusPos a = std::max<CorpusPos>(wreg[j].start, base);
                            const CorpusPos b = std::min<CorpusPos>(wreg[j].end - (L - 1), top);
                            if (b >= a) set_range(msk.data(), static_cast<uint32_t>(a - base),
                                                  static_cast<uint32_t>(b - base));
                        }
                        for (size_t w = 0; w < nw; ++w) acc[w] &= msk[w];
                    }
                    // emit the page, then count
                    for (size_t w = 0; w < nw && !stop; ++w) {
                        uint64_t x = acc[w];
                        if (counting) {
                            result.total_count += static_cast<size_t>(__builtin_popcountll(x));
                            continue;
                        }
                        while (x) {
                            if (max_matches > 0 && result.matches.size() >= max_matches) {
                                if (!count_total) { stop = true; break; }
                                if (cheap_total_ok) {
                                    counting = true;
                                    result.total_count += static_cast<size_t>(__builtin_popcountll(x));
                                    break;
                                }
                            }
                            const CorpusPos p0 = base + static_cast<CorpusPos>(w * 64)
                                               + __builtin_ctzll(x);
                            x &= x - 1;
                            std::vector<CorpusPos> pm(2 * n);
                            for (size_t i = 0; i < n; ++i) pm[i] = pm[n + i] = p0 + static_cast<CorpusPos>(i);
                            add_match(std::move(pm));
                            if (reached_limit() || reached_total_cap()) { stop = true; break; }
                        }
                    }
                    if (counting && max_total_cap > 0 && result.total_count >= max_total_cap) {
                        result.total_count = max_total_cap;
                        stop = true;
                    }
                }
                return true;
            }
        }
        return false;
    };

    // ── Single-token fast path ──────────────────────────────────────────

    if (n == 1) {
        if (q.tokens[0].is_dep_subtree) {
            throw std::runtime_error(
                    "dep_subtree(...) must be paired with its source in one query (e.g. "
                    "`head:[...] sub:dep_subtree(head) [:: ...]`), or define the source first "
                    "as a named query (`head = [...];` then `sub = dep_subtree(head) [:: ...]`).");
        }
        size_t est = estimate_cardinality(q.tokens[0].conditions);
        result.cardinalities = {est};
        result.seed_token = 0;
        result.plan_path = "single";

        // ── Try fast aggregation path (no Match construction) ────────────
        if (agg_ptr && !agg_fast_path_blocked) {
            auto resolved_filters = resolve_region_filters(q);
            if (try_fast_aggregate(q, *agg_ptr, resolved_filters,
                                   token_anchor_constraints,
                                   max_total_cap, result, name_map)) {
                result.aggregate_buckets = std::move(agg_storage);
                return result;
            }
            // Fast path declined — fall through to standard path
        }

        int min_rep = q.tokens[0].min_repeat;
        int max_rep = q.tokens[0].max_repeat;
        const ConditionPtr& tok_cond = q.tokens[0].conditions;

        // ── P3.2: `&` / `|` / `!=` over bitmap attributes (`[upos!="PUNCT"]`,
        // `[upos="NOUN" & deprel="nsubj"]`): per-chunk AND / OR / NOT + popcount
        // instead of materialising or probing every position. Plain EQ / id-set
        // leaves keep their O(1) / k-way paths below.
        if (min_rep == 1 && max_rep == 1 && !q.tokens[0].is_dep_subtree
            && (!tok_cond || !tok_cond->is_leaf || tok_cond->leaf.op == CompOp::NEQ)
            && fastpath_mode() == FastPathMode::On && bitmap_mode() != BitmapMode::Off) {
            const std::string eff_within = q.within.empty() ? corpus_.default_within() : q.within;
            const StructuralAttr* wsa = (!eff_within.empty() && corpus_.has_structure(eff_within))
                ? &corpus_.structure(eff_within) : nullptr;
            const bool wspan = wsa && (corpus_.is_nested(eff_within) || corpus_.is_overlapping(eff_within));
            RegionPosMask rm = build_start_mask();
            if (rm.status == RegionMaskStatus::Unsatisfiable) {
                mark_ranged(rm);
                result.total_exact = true;
                return result;
            }
            const bool urm = rm.ready();
            const bool cheap = (q.global_region_filters.empty() || urm) && token_anchor_constraints.empty();
            if (try_seq_bitmap(wsa, wspan, rm, urm, cheap)) {
                mark_ranged(rm);
                return finish_query();
            }
        }

        // ── Single EQ token: first page straight from `.rev`, the rest only counted
        // (optionally through the EQ region mask). Avoids one Match per hit.
        if (fastpath_mode() == FastPathMode::On && sample_size == 0 && !agg_ptr
            && !q.tokens[0].has_repetition() && token_anchor_constraints.empty()
            && !q.within_having && !q.not_within && q.containing_clauses.empty()
            && q.position_orders.empty() && q.global_alignment_filters.empty()
            && q.global_function_filters.empty()) {
            // ── Single leaf with a resolved id set (regex, %c/%d): no
            // materialisation. First page = k-way merge of the ids' `.rev` spans;
            // total = sum of per-id counts (sliced per interval under a region
            // filter). `[word=".*ness"]` on a 5B-token corpus stays O(#ids + page).
            if (tok_cond && tok_cond->is_leaf && tok_cond->leaf.id_set_resolved
                && tok_cond->leaf.op != CompOp::NEQ) {
                const AttrCondition& ac = tok_cond->leaf;
                const std::string aname = normalize_attr(ac.attr);
                if (corpus_.has_attr(aname) && !corpus_.is_multivalue(aname)) {
                    // O(#ids + page) for any total: not worth partitioning (P4.1),
                    // so the plain region mask — the probe range then computes it all
                    RegionPosMask mask =
                        build_region_eq_position_mask(corpus_, q.global_region_filters);
                    const bool iv_mask = mask.ready() && !mask.use_bits;
                    const auto& pa = corpus_.attr(aname);
                    // P4.6: a large, dense id set (`[form=".*e.*"]`: 1M ids, a
                    // quarter of the tokens): the first page from the token ids
                    // in corpus order (a few thousand positions at most), the total
                    // from the counts — not a heap over a million posting lists
                    const CorpusPos N = corpus_.size();
                    if (mask.status == RegionMaskStatus::None && ac.id_set.size() > 4096
                        && ac.id_set_total > 0 && max_matches > 0
                        && static_cast<double>(ac.id_set_total) * 4096.0 >= static_cast<double>(N)) {
                        result.plan_path = "single_idset";
                        const size_t V = static_cast<size_t>(pa.lexicon().size());
                        std::vector<uint64_t> in((V + 63) / 64, 0);
                        for (int32_t id : ac.id_set)
                            if (id >= 0 && static_cast<size_t>(id) < V)
                                in[static_cast<size_t>(id) >> 6] |= uint64_t{1} << (id & 63);
                        size_t emitted = 0;
                        bool capped = false;
                        CorpusPos p = 0;
                        for (; p < N; ++p) {
                            if (result.matches.size() >= max_matches) break;
                            if ((p & 0xFFFF) == 0) check_cancelled();
                            const auto id = static_cast<size_t>(pa.id_at(p));
                            if (!((in[id >> 6] >> (id & 63)) & 1)) continue;
                            add_match(std::vector<CorpusPos>{p, p});
                            ++emitted;
                            if (reached_total_cap()) { capped = true; break; }
                        }
                        if (count_total && !capped && p < N) {
                            result.total_count += static_cast<size_t>(ac.id_set_total) - emitted;
                            if (max_total_cap > 0 && result.total_count > max_total_cap)
                                result.total_count = max_total_cap;
                        }
                        result.total_exact = !reached_limit() && !reached_total_cap();
                        return result;
                    }
                    std::vector<RevSpan> spans;
                    spans.reserve(ac.id_set.size());
                    for (int32_t id : ac.id_set) {
                        RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(id));
                        if (!sp.empty()) spans.push_back(sp);
                    }
                    const bool mask_ok = mask.status == RegionMaskStatus::None
                        || mask.status == RegionMaskStatus::Unsatisfiable
                        || (iv_mask && spans.size() * mask.iv.size() <= (size_t{1} << 22));
                    if (mask_ok) {
                        result.plan_path = "single_idset";
                        if (mask.status == RegionMaskStatus::Unsatisfiable) {
                            result.total_exact = true;
                            return result;
                        }
                        // first page: ascending k-way merge (from the first allowed start)
                        using HeapItem = std::pair<CorpusPos, size_t>;   // (pos, span)
                        std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<HeapItem>> heap;
                        std::vector<size_t> cur(spans.size(), 0);
                        const CorpusPos first_allowed = iv_mask ? mask.iv.front().s : 0;
                        for (size_t k = 0; k < spans.size(); ++k) {
                            if (first_allowed > 0) cur[k] = gallop_rev(spans[k], 0, first_allowed);
                            if (cur[k] < spans[k].count) heap.push({spans[k].at(cur[k]), k});
                        }
                        MaskCursor mc(mask);
                        size_t emitted = 0;
                        bool capped = false;
                        while (!heap.empty()) {
                            if (max_matches > 0 && result.matches.size() >= max_matches) break;
                            auto [p, k] = heap.top();
                            heap.pop();
                            if (++cur[k] < spans[k].count) heap.push({spans[k].at(cur[k]), k});
                            if (iv_mask && !mc.contains(p)) continue;
                            add_match(std::vector<CorpusPos>{p, p});
                            ++emitted;
                            if (reached_total_cap()) { capped = true; break; }
                        }
                        if (count_total && !capped && !heap.empty()) {
                            size_t tot = 0;
                            if (!iv_mask) {
                                for (const auto& sp : spans) tot += sp.count;
                            } else {
                                for (const auto& sp : spans) {
                                    size_t lo = 0;
                                    for (const auto& I : mask.iv) {
                                        lo = gallop_rev(sp, lo, I.s);
                                        if (lo >= sp.count) break;
                                        const size_t hi = gallop_rev(sp, lo, I.e + 1);
                                        tot += hi - lo;
                                        lo = hi;
                                    }
                                }
                            }
                            result.total_count += tot - emitted;
                            if (max_total_cap > 0 && result.total_count > max_total_cap)
                                result.total_count = max_total_cap;
                        }
                        result.total_exact = !reached_limit() && !reached_total_cap();
                        return result;
                    }
                }
            }
            SeqMergeOperand op = merge_operand(tok_cond);
            if (op.kind == SeqMergeTok::EqRev) {
                // O(#intervals) totals: not partitioned (see single_idset)
                RegionPosMask mask =
                    build_region_eq_position_mask(corpus_, q.global_region_filters);
                if (mask.status == RegionMaskStatus::Unsatisfiable) {
                    result.plan_path = "single_count";
                    result.total_exact = true;
                    return result;
                }
                if (mask.status != RegionMaskStatus::Unsupported) {
                    result.plan_path = "single_count";
                    const size_t cnt = op.span.count;
                    if (mask.ready() && mask.use_bits) {
                        // Fine-grained filter: per-position bit test.
                        size_t i = 0;
                        for (; i < cnt; ++i) {
                            CorpusPos p = op.span.at(i);
                            if (!bitset_test(mask.bits, p)) continue;
                            if (max_matches > 0 && result.matches.size() >= max_matches) break;
                            add_match(std::vector<CorpusPos>{p, p});
                            if (reached_total_cap()) break;
                        }
                        if (count_total && i < cnt && !reached_total_cap()) {
                            for (; i < cnt; ++i)
                                if (bitset_test(mask.bits, op.span.at(i))) ++result.total_count;
                            if (max_total_cap > 0 && result.total_count > max_total_cap)
                                result.total_count = max_total_cap;
                        }
                    } else {
                        // No filter, or interval filter: slice the postings per
                        // interval; the part past the first page is counted as
                        // index differences (O(#intervals * log) for any total).
                        std::vector<PosInterval> whole;
                        const std::vector<PosInterval>* ivs = &mask.iv;
                        if (!mask.ready()) {
                            whole.push_back({0, corpus_.size() - 1});
                            ivs = &whole;
                        }
                        size_t lo = 0;
                        for (const auto& I : *ivs) {
                            lo = gallop_rev(op.span, lo, I.s);
                            if (lo >= cnt) break;
                            const size_t hi = gallop_rev(op.span, lo, I.e + 1);
                            size_t k = lo;
                            bool stop = false;
                            for (; k < hi; ++k) {
                                if (max_matches > 0 && result.matches.size() >= max_matches) break;
                                const CorpusPos p = op.span.at(k);
                                add_match(std::vector<CorpusPos>{p, p});
                                if (reached_total_cap()) { stop = true; break; }
                            }
                            if (stop) break;
                            if (k < hi) {             // page full, rest of this interval
                                if (!count_total) break;
                                result.total_count += hi - k;
                                if (max_total_cap > 0 && result.total_count >= max_total_cap) {
                                    result.total_count = max_total_cap;
                                    break;
                                }
                            }
                            lo = hi;
                        }
                    }
                    result.total_exact = !reached_limit() && !reached_total_cap();
                    return result;
                }
            }
        }

        // Non-repeating single token only: one span [p,p] per matching seed (min=max=1).
        auto try_spans_from = [&](CorpusPos p) {
            for (int len = min_rep; len <= max_rep; ++len) {
                CorpusPos end = p + len - 1;
                if (end >= corpus_.size()) break;
                if (len > 1 && !check_conditions(end, tok_cond)) break;
                bool valid = true;
                if (len == 1) {
                    // already checked by caller
                } else {
                    for (CorpusPos cp = p + 1; cp < end; ++cp) {
                        if (!check_conditions(cp, tok_cond)) {
                            valid = false;
                            break;
                        }
                    }
                }
                if (!valid) break;
                std::vector<CorpusPos> pm = {p, end};
                add_match(std::move(pm));
                if (reached_limit() || reached_total_cap()) return false;
            }
            return !reached_limit() && !reached_total_cap();
        };

        // Repetition (+, *, {n,m}, …): one hit per maximal contiguous stretch of matching
        // tokens (tile if length > max_repeat). Same for * and +; min_repeat==0 only relaxes
        // the final tile (any positive remainder is emitted). Sub-span enumeration is not used.
        // P3.10: the same maximal runs from the token's chunk bitmaps (word-level run
        // detection) instead of check_conditions per position; the hits and their
        // order are the generic loop's below. Past the first page, runs are only
        // counted (tiles per run) when nothing else has to see each hit.
        bool runs_done = false;
        if (q.tokens[0].has_repetition() && fastpath_mode() == FastPathMode::On
            && bitmap_mode() != BitmapMode::Off && !q.tokens[0].is_dep_subtree) {
            bool dense = false;
            std::unique_ptr<BmExpr> e = tok_cond ? compile_bm(tok_cond, &dense) : nullptr;
            if (e || !tok_cond) {
                result.plan_path = "single_runs";
                runs_done = true;
                const CorpusPos N = corpus_.size();
                const bool cheap_count = count_total && sample_size == 0 && !agg_ptr
                    && q.global_region_filters.empty() && token_anchor_constraints.empty();
                bool counting = false, stop = false;
                // tiles of a maximal run [s, e] (see the generic loop below)
                auto tiles_of = [&](int64_t L) -> size_t {
                    if (L < min_rep) return 0;
                    if (L <= max_rep) return 1;
                    const int64_t full = L / max_rep, r = L % max_rep;
                    return static_cast<size_t>(full) + (r > 0 && r >= min_rep ? 1 : 0);
                };
                auto emit_run = [&](CorpusPos rs, CorpusPos re) {
                    const int64_t L = static_cast<int64_t>(re - rs + 1);
                    if (counting) {
                        result.total_count += tiles_of(L);
                        if (max_total_cap > 0 && result.total_count >= max_total_cap) {
                            result.total_count = max_total_cap;
                            stop = true;
                        }
                        return;
                    }
                    if (L < min_rep) return;
                    CorpusPos cur = rs;
                    while (cur <= re) {
                        const int64_t rem = static_cast<int64_t>(re - cur + 1);
                        CorpusPos te;
                        if (L <= max_rep) te = re;
                        else if (rem > max_rep) te = static_cast<CorpusPos>(cur + max_rep - 1);
                        else if (rem >= min_rep) te = re;
                        else break;
                        if (cheap_count && max_matches > 0 && result.matches.size() >= max_matches) {
                            // page full: count this run's remaining tiles, then the rest
                            counting = true;
                            const int64_t R = static_cast<int64_t>(re - cur + 1);
                            result.total_count += (L <= max_rep) ? 1 : tiles_of(R);
                            if (max_total_cap > 0 && result.total_count >= max_total_cap) {
                                result.total_count = max_total_cap;
                                stop = true;
                            }
                            return;
                        }
                        std::vector<CorpusPos> pm = {cur, te};
                        add_match(std::move(pm));
                        if (reached_limit() || reached_total_cap()) { stop = true; return; }
                        if (te == re) break;
                        cur = te + 1;
                    }
                };
                if (!tok_cond) {
                    if (N > 0) emit_run(0, N - 1);
                } else {
                    constexpr size_t W = BitmapIndex::kWords;
                    const size_t nchunks = static_cast<size_t>((N + BitmapIndex::kChunk - 1) >> BitmapIndex::kChunkShift);
                    CorpusPos open_s = -1;   // start of the run still open (-1: none)
                    for (size_t c = 0; c < nchunks && !stop; ++c) {
                        const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
                        progress_tick(base, result.total_count, true);
                        const BmChunk v = e->load(c);
                        if (v.zero || (v.w == bm_zero_words())) {
                            if (open_s >= 0) { emit_run(open_s, base - 1); open_s = -1; }
                            continue;
                        }
                        for (size_t w = 0; w < W && !stop; ++w) {
                            const CorpusPos P = base + static_cast<CorpusPos>(w * 64);
                            if (P >= N) break;
                            uint64_t x = v.w[w];
                            if (N - P < 64) x &= (uint64_t{1} << (N - P)) - 1;   // past the corpus end
                            if (open_s >= 0) {
                                if (x == ~uint64_t{0}) continue;
                                const int t = __builtin_ctzll(~x);          // first 0: the run ends before it
                                emit_run(open_s, P + t - 1);
                                open_s = -1;
                                if (stop) break;
                                x &= ~uint64_t{0} << t;                     // (bits below t were the run)
                            }
                            while (x) {
                                const int st = __builtin_ctzll(x);
                                const uint64_t zeros = ~x & (~uint64_t{0} << st);
                                if (!zeros) { open_s = P + st; break; }     // runs into the next word
                                const int en = __builtin_ctzll(zeros);
                                emit_run(P + st, P + en - 1);
                                if (stop) break;
                                x &= ~uint64_t{0} << en;
                            }
                        }
                    }
                    if (!stop && open_s >= 0) emit_run(open_s, N - 1);
                }
            }
        }
        if (runs_done) {
            // (hits went through add_match like the loop below)
        } else if (q.tokens[0].has_repetition()) {
            CorpusPos i = 0;
            const CorpusPos n = corpus_.size();
            while (i < n) {
                while (i < n && !check_conditions(i, tok_cond)) ++i;
                if (i >= n) break;
                const CorpusPos s = i;
                while (i + 1 < n && check_conditions(i + 1, tok_cond)) ++i;
                const CorpusPos e = i;
                const int64_t L = static_cast<int64_t>(e - s + 1);
                if (L < min_rep) {
                    i = e + 1;
                    continue;
                }
                if (L <= max_rep) {
                    std::vector<CorpusPos> pm = {s, e};
                    add_match(std::move(pm));
                } else {
                    CorpusPos cur = s;
                    while (cur <= e) {
                        const int64_t rem = static_cast<int64_t>(e - cur + 1);
                        if (rem > max_rep) {
                            std::vector<CorpusPos> pm = {cur,
                                                         static_cast<CorpusPos>(cur + max_rep - 1)};
                            add_match(std::move(pm));
                            cur += max_rep;
                            if (reached_limit() || reached_total_cap()) break;
                        } else {
                            if (rem >= min_rep) {
                                std::vector<CorpusPos> pm = {cur, e};
                                add_match(std::move(pm));
                            }
                            break;
                        }
                    }
                }
                if (reached_limit() || reached_total_cap()) break;
                i = e + 1;
            }
        } else {
            bool use_scan = !tok_cond || est > static_cast<size_t>(corpus_.size()) / 2;
            if (use_scan) {
                for (CorpusPos p = 0; p < corpus_.size(); ++p) {
                    if (check_conditions(p, tok_cond)) {
                        if (!try_spans_from(p)) break;
                    }
                }
            } else {
                for_each_seed_position(tok_cond, [&](CorpusPos p) { return try_spans_from(p); });
            }
        }
        if (agg_ptr) {
            flush_flat_agg();
            result.matches.clear();
            result.total_count = agg_ptr->total_hits;
            result.total_exact = !agg_capped;
            result.aggregate_buckets = std::move(agg_storage);
            return result;
        }
        return finish_query();
    }

    // ── Gap / optional / repetition fast path (P2.1–P2.3) ───────────────
    //
    // Linear sequences with exactly one variable-length element V:
    //   prefix(p fixed tokens)  V{min,max}  suffix(q fixed tokens)
    // V is a gap `[]{n,m}`, an optional `[X]?` or a repetition `[X]{n,m}` / `+` / `*`
    // (unbounded = REPEAT_UNBOUNDED, as in the generic executor). With one variable
    // element a (start, end) pair has exactly one segmentation, so the generic
    // executor's hits are exactly the pairs (s, t) with: prefix at s, suffix at t,
    // V = [s+p, t-1] of an allowed length whose tokens all match X. We iterate the
    // rarer fixed side and count the partner side with two gallops per anchor —
    // exact totals without enumerating hits; only the first page builds matches.
    // Semantics = native pando (= Manatee without `within`): every (start, end) pair.
    {
        bool all_seq = true;
        for (const auto& rel : q.relations)
            if (rel.type != RelationType::SEQUENCE) { all_seq = false; break; }
        int var_idx = -1, n_var = 0;
        bool simple = all_seq && n >= 2 && fastpath_mode() == FastPathMode::On;
        for (size_t i = 0; simple && i < n; ++i) {
            const auto& tok = q.tokens[i];
            if (tok.is_anchor() || tok.is_dep_subtree) simple = false;
            if (tok.has_repetition()) { ++n_var; var_idx = static_cast<int>(i); }
        }
        if (simple && n_var == 1) {
            const size_t v = static_cast<size_t>(var_idx);
            const size_t p = v, qn = n - 1 - v;              // prefix / suffix lengths
            const int vmin = q.tokens[v].min_repeat, vmax = q.tokens[v].max_repeat;

            // ── P3.3: the same (start, end) pairs on chunked bitmaps (seq_gap_bitmap) ──
            // For each length L of V the pattern is fixed: prefix at s, X at
            // s+p … s+p+L-1, suffix at s+p+L. Per chunk:
            //   PRE  = AND of the shifted prefix tokens (and the `:: match.…` mask)
            //   R_L  = R_{L-1} & (X >> p+L-1)       (runs of X; R_0 = all)
            //   A_len = A_{len-1} & ~(E >> len-2)   (no region end inside the span;
            //          A_1 = covered positions, E = region ends + the corpus end)
            //   H_L  = PRE & R_L & A_{p+L+q} & AND_j (S_j >> p+L+j)
            // The total is the sum of popcounts; the first page walks starts in
            // order and, per start, the lengths in increasing order.
            if (fastpath_mode() == FastPathMode::On && bitmap_mode() != BitmapMode::Off
                && all_seq && sample_size == 0 && vmax >= vmin && vmax >= 1
                && static_cast<int>(n) - 1 + vmax <= kBmMaxShift) {
                std::string eff_within = q.within.empty() ? corpus_.default_within() : q.within;
                const StructuralAttr* wsa = (!eff_within.empty() && corpus_.has_structure(eff_within))
                    ? &corpus_.structure(eff_within) : nullptr;
                const bool wspan = wsa && (corpus_.is_nested(eff_within) || corpus_.is_overlapping(eff_within));
                bool compiled = !wspan, dense = false, pre_free = true, suf_free = true;
                size_t min_fixed = SIZE_MAX;
                std::vector<std::unique_ptr<BmExpr>> tex(n);   // nullptr = []
                for (size_t i = 0; i < n && compiled; ++i) {
                    if (!q.tokens[i].conditions) continue;
                    tex[i] = compile_bm(q.tokens[i].conditions, &dense);
                    if (!tex[i]) { compiled = false; break; }
                    if (i != v) {
                        min_fixed = std::min(min_fixed, tex[i]->estimate);
                        (i < v ? pre_free : suf_free) = false;
                    }
                }
                const CorpusPos N = corpus_.size();
                const size_t nchunks = static_cast<size_t>((N + BitmapIndex::kChunk - 1) >> BitmapIndex::kChunkShift);
                const bool use_bm = compiled && !(pre_free && suf_free)
                    && (bitmap_mode() == BitmapMode::Force
                        || (dense && min_fixed != SIZE_MAX && min_fixed >= 8 * nchunks
                            // a wildcard / dense V costs one pass per length; runs of a
                            // selective X die out after a few
                            && (vmax - vmin + 1 <= 32
                                || (tex[v] && tex[v]->estimate <= static_cast<size_t>(N) / 2))));
                RegionPosMask bm_mask;
                if (use_bm) {
                    bm_mask = build_start_mask();
                    if (bm_mask.status == RegionMaskStatus::Unsatisfiable) {
                        result.plan_path = "seq_gap_bitmap";
                        mark_ranged(bm_mask);
                        result.total_exact = true;
                        return result;
                    }
                }
                if (use_bm) {
                    result.plan_path = "seq_gap_bitmap";
                    mark_ranged(bm_mask);
                    const size_t p = v, qn = n - 1 - v;
                    const bool use_mask = bm_mask.status == RegionMaskStatus::Ready;
                    const bool cheap_total_ok = (q.global_region_filters.empty() || use_mask)
                                                && token_anchor_constraints.empty();
                    constexpr size_t W = BitmapIndex::kWords;
                    constexpr size_t WX = W + kBmExtra;
                    // fixed tokens, rarest first
                    std::vector<size_t> order;
                    for (size_t i = 0; i < n; ++i)
                        if (i != v && tex[i]) order.push_back(i);
                    std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
                        return tex[a]->estimate < tex[b]->estimate;
                    });
                    std::vector<BmChunk> view(n);
                    std::vector<uint64_t> pre(W), run(W), alw(W), cw(WX), ew(WX);
                    const int nlen = vmax + 1;
                    std::vector<uint64_t> hits(static_cast<size_t>(nlen) * W);
                    std::vector<char> has_len(static_cast<size_t>(nlen));
                    size_t ivc = 0, wr = 0;
                    const Region* wreg = wsa ? wsa->region_data() : nullptr;
                    const size_t wn = wsa ? wsa->region_count() : 0;
                    std::shared_ptr<BitmapIndex> sbi = wsa ? structure_bitmap(eff_within) : nullptr;
                    std::unique_ptr<BmValue> sC, sE;
                    if (sbi) {
                        sC = std::make_unique<BmValue>(*sbi, BitmapIndex::kStructCovered, 0);
                        sE = std::make_unique<BmValue>(*sbi, BitmapIndex::kStructEnds, 0);
                    }
                    auto set_range = [](uint64_t* m, size_t a, size_t b) {   // inclusive bit range
                        const size_t wa = a >> 6, wb = b >> 6;
                        const uint64_t ma = ~uint64_t{0} << (a & 63);
                        const uint64_t mb = ~uint64_t{0} >> (63 - (b & 63));
                        if (wa == wb) { m[wa] |= ma & mb; return; }
                        m[wa] |= ma;
                        for (size_t w = wa + 1; w < wb; ++w) m[w] = ~uint64_t{0};
                        m[wb] |= mb;
                    };
                    auto shifted_raw = [](const std::vector<uint64_t>& a, size_t i, int k) -> uint64_t {
                        const size_t q0 = i + static_cast<size_t>(k >> 6);
                        const int r = k & 63;
                        if (r == 0) return a[q0];
                        return (a[q0] >> r) | (a[q0 + 1] << (64 - r));
                    };
                    bool stop = false, counting = false;
                    for (size_t c = 0; c < nchunks && !stop; ++c) {
                        const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
                        progress_tick(base, result.total_count, true);
                        const CorpusPos top = std::min<CorpusPos>(base + BitmapIndex::kChunk, N) - 1;
                        const size_t nw = static_cast<size_t>((top - base) >> 6) + 1;
                        if (use_mask && !bm_mask.use_bits) {
                            const auto& iv = bm_mask.iv;
                            while (ivc < iv.size() && iv[ivc].e < base) ++ivc;
                            if (ivc >= iv.size()) break;
                            if (iv[ivc].s > top) continue;
                        }
                        bool any = true;
                        for (size_t i : order) {
                            view[i] = tex[i]->load(c);
                            if (view[i].zero) { any = false; break; }
                        }
                        if (!any) continue;
                        // PRE
                        uint64_t orr = 0;
                        for (size_t w = 0; w < nw; ++w) {
                            uint64_t x = ~uint64_t{0};
                            for (size_t i = 0; i < p; ++i)
                                if (tex[i]) x &= view[i].shifted(w, static_cast<int>(i));
                            pre[w] = x;
                        }
                        if ((top - base + 1) & 63) pre[nw - 1] &= (uint64_t{1} << ((top - base + 1) & 63)) - 1;
                        if (use_mask) {
                            if (bm_mask.use_bits) {
                                const size_t b0 = static_cast<size_t>(base >> 6);
                                for (size_t w = 0; w < nw; ++w)
                                    pre[w] &= b0 + w < bm_mask.bits.size() ? bm_mask.bits[b0 + w] : 0;
                            } else {
                                std::fill(alw.begin(), alw.begin() + nw, 0);
                                const auto& iv = bm_mask.iv;
                                for (size_t j = ivc; j < iv.size() && iv[j].s <= top; ++j) {
                                    const CorpusPos a = std::max(iv[j].s, base), b = std::min(iv[j].e, top);
                                    if (b >= a) set_range(alw.data(), static_cast<size_t>(a - base), static_cast<size_t>(b - base));
                                }
                                for (size_t w = 0; w < nw; ++w) pre[w] &= alw[w];
                            }
                        }
                        for (size_t w = 0; w < nw; ++w) orr |= pre[w];
                        if (!orr) continue;
                        // covered positions C and span ends E over the chunk + the extra words
                        std::fill(cw.begin(), cw.end(), 0);
                        std::fill(ew.begin(), ew.end(), 0);
                        const CorpusPos wend = base + static_cast<CorpusPos>(WX * 64);   // exclusive
                        if (sC) {
                            const BmChunk cv = sC->load(c), ev = sE->load(c);
                            std::copy(cv.w, cv.w + W, cw.begin());
                            std::copy(cv.nx, cv.nx + kBmExtra, cw.begin() + W);
                            std::copy(ev.w, ev.w + W, ew.begin());
                            std::copy(ev.nx, ev.nx + kBmExtra, ew.begin() + W);
                        } else if (wreg) {
                            while (wr < wn && wreg[wr].end < base) ++wr;
                            for (size_t j = wr; j < wn && wreg[j].start < wend; ++j) {
                                const CorpusPos a = std::max<CorpusPos>(wreg[j].start, base);
                                const CorpusPos b = std::min<CorpusPos>(wreg[j].end, wend - 1);
                                if (b >= a) set_range(cw.data(), static_cast<size_t>(a - base), static_cast<size_t>(b - base));
                                if (wreg[j].end < wend) {
                                    const size_t o = static_cast<size_t>(wreg[j].end - base);
                                    ew[o >> 6] |= uint64_t{1} << (o & 63);
                                }
                            }
                        } else {
                            const CorpusPos b = std::min<CorpusPos>(N, wend) - 1;
                            set_range(cw.data(), 0, static_cast<size_t>(b - base));
                        }
                        if (N - 1 < wend) {
                            const size_t o = static_cast<size_t>(N - 1 - base);
                            ew[o >> 6] |= uint64_t{1} << (o & 63);
                        }
                        // A for len0 = p + qn (the length at L = 0)
                        const int len0 = static_cast<int>(p + qn);
                        for (size_t w = 0; w < nw; ++w) {
                            uint64_t a = cw[w];
                            for (int d = 0; d <= len0 - 2; ++d) a &= ~shifted_raw(ew, w, d);
                            alw[w] = a;
                        }
                        std::fill(run.begin(), run.begin() + nw, ~uint64_t{0});
                        const bool emit_mode = !counting;
                        for (int L = 0; L <= vmax; ++L) {
                            const int len = len0 + L;
                            uint64_t live = 0;
                            if (L > 0) {
                                const int kx = static_cast<int>(p) + L - 1;
                                for (size_t w = 0; w < nw; ++w) {
                                    if (tex[v]) run[w] &= view[v].shifted(w, kx);
                                    alw[w] &= ~shifted_raw(ew, w, len - 2);
                                    live |= run[w] & alw[w] & pre[w];
                                }
                            } else {
                                if (tex[v]) view[v] = tex[v]->load(c);
                                for (size_t w = 0; w < nw; ++w) live |= alw[w] & pre[w];
                            }
                            if (!live) break;
                            if (L < vmin) continue;
                            uint64_t* h = hits.data() + static_cast<size_t>(L) * W;
                            uint64_t hor = 0;
                            size_t cnt = 0;
                            for (size_t w = 0; w < nw; ++w) {
                                uint64_t x = pre[w] & run[w] & alw[w];
                                for (size_t j = 0; j < qn && x; ++j) {
                                    const size_t ti = v + 1 + j;
                                    if (tex[ti]) x &= view[ti].shifted(w, static_cast<int>(p) + L + static_cast<int>(j));
                                }
                                if (emit_mode) h[w] = x;
                                hor |= x;
                                cnt += static_cast<size_t>(__builtin_popcountll(x));
                            }
                            has_len[static_cast<size_t>(L)] = hor != 0;
                            if (!emit_mode) result.total_count += cnt;
                        }
                        if (emit_mode) {
                            // ordered walk: starts ascending, lengths ascending
                            for (size_t w = 0; w < nw && !stop; ++w) {
                                uint64_t starts = 0;
                                for (int L = vmin; L <= vmax; ++L)
                                    if (has_len[static_cast<size_t>(L)]) starts |= hits[static_cast<size_t>(L) * W + w];
                                while (starts && !stop) {
                                    const int j = __builtin_ctzll(starts);
                                    starts &= starts - 1;
                                    const CorpusPos st = base + static_cast<CorpusPos>(w * 64) + j;
                                    for (int L = vmin; L <= vmax && !stop; ++L) {
                                        if (!has_len[static_cast<size_t>(L)]) continue;
                                        if (!(hits[static_cast<size_t>(L) * W + w] >> j & 1)) continue;
                                        if (counting) { ++result.total_count; continue; }
                                        if (max_matches > 0 && result.matches.size() >= max_matches) {
                                            if (!count_total) { stop = true; break; }
                                            if (cheap_total_ok) { counting = true; ++result.total_count; continue; }
                                        }
                                        std::vector<CorpusPos> pm(2 * n);
                                        for (size_t i = 0; i < p; ++i) pm[i] = pm[n + i] = st + static_cast<CorpusPos>(i);
                                        const CorpusPos vb = st + static_cast<CorpusPos>(p);
                                        if (L == 0) { pm[v] = pm[n + v] = NO_HEAD; }
                                        else { pm[v] = vb; pm[n + v] = vb + L - 1; }
                                        for (size_t jj = 0; jj < qn; ++jj)
                                            pm[v + 1 + jj] = pm[n + v + 1 + jj] = vb + L + static_cast<CorpusPos>(jj);
                                        add_match(std::move(pm));
                                        if (reached_limit() || reached_total_cap()) { stop = true; break; }
                                    }
                                }
                            }
                        }
                        std::fill(has_len.begin(), has_len.end(), 0);
                        if (counting && max_total_cap > 0 && result.total_count >= max_total_cap) {
                            result.total_count = max_total_cap;
                            stop = true;
                        }
                    }
                    return finish_query();
                }
            }
            std::vector<SeqMergeOperand> ops(n);
            bool ok = vmax >= vmin && vmax >= 1;
            for (size_t i = 0; ok && i < n; ++i) {
                ops[i] = merge_operand(q.tokens[i].conditions);
                if (ops[i].kind == SeqMergeTok::Complex) ok = false;
            }
            std::string effective_within = q.within.empty() ? corpus_.default_within() : q.within;
            const bool has_within = !effective_within.empty() && corpus_.has_structure(effective_within);
            if (has_within && (corpus_.is_nested(effective_within) || corpus_.is_overlapping(effective_within)))
                ok = false;
            RegionPosMask region_mask;
            if (ok) {
                region_mask = build_start_mask();
                if (region_mask.status == RegionMaskStatus::Unsatisfiable) {
                    result.plan_path = "seq_gap";
                    mark_ranged(region_mask);
                    result.total_exact = true;
                    return result;
                }
            }
            const CorpusPos corpus_end = corpus_.size();

            // Starts of a fixed-length run of tokens [lo, hi), sorted; `free` = all
            // wildcards. With a single constrained token the start list is that
            // token's `.rev` span shifted by its offset (no copy); starts outside
            // the corpus are skipped by the callers.
            struct FixedSide {
                bool free = true;
                size_t len = 0;
                bool use_span = false;
                RevSpan span{};
                CorpusPos shift = 0;
                std::vector<CorpusPos> starts;
                size_t size() const { return use_span ? span.count : starts.size(); }
                CorpusPos at(size_t i) const { return use_span ? span.at(i) - shift : starts[i]; }
                size_t geq(size_t from, CorpusPos x) const {   // first index >= from with at() >= x
                    if (use_span) return gallop_rev(span, from, x + shift);
                    return static_cast<size_t>(std::lower_bound(starts.begin() + static_cast<std::ptrdiff_t>(from),
                                                                starts.end(), x) - starts.begin());
                }
            };
            // run starts that can take part at all: the prefix starts a match, so
            // it lies in the start mask's hull; the suffix follows it by p + [vmin, vmax]
            // (P4.1 ranges: a materialised side then only covers its range)
            CorpusPos hull_lo = 0, hull_hi = std::numeric_limits<CorpusPos>::max();
            if (region_mask.ready() && !region_mask.iv.empty()) {
                hull_lo = region_mask.iv.front().s;
                hull_hi = region_mask.iv.back().e;
            }
            auto fixed_starts = [&](size_t lo, size_t hi) {
                FixedSide f;
                f.len = hi - lo;
                const bool is_prefix = lo == 0;
                const int64_t smin = is_prefix ? hull_lo : hull_lo + static_cast<int64_t>(p) + vmin;
                const int64_t smax = is_prefix ? hull_hi
                    : (hull_hi == std::numeric_limits<CorpusPos>::max()
                           ? hull_hi : hull_hi + static_cast<int64_t>(p) + vmax);
                size_t seed = SIZE_MAX, n_eq = 0;
                for (size_t i = lo; i < hi; ++i)
                    if (ops[i].kind == SeqMergeTok::EqRev) {
                        ++n_eq;
                        if (seed == SIZE_MAX || ops[i].span.count < ops[seed].span.count) seed = i;
                    }
                if (seed == SIZE_MAX) return f;              // only wildcards (or empty)
                f.free = false;
                if (n_eq == 1) {
                    f.use_span = true;
                    f.span = ops[seed].span;
                    f.shift = static_cast<CorpusPos>(seed - lo);
                    return f;
                }
                const RevSpan& S = ops[seed].span;
                std::vector<size_t> cur(hi - lo, 0);
                const size_t k0 = smin > 0 ? gallop_rev(S, 0, smin + static_cast<CorpusPos>(seed - lo)) : 0;
                for (size_t k = k0; k < S.count; ++k) {
                    const CorpusPos st = S.at(k) - static_cast<CorpusPos>(seed - lo);
                    if (st > smax) break;
                    if (st < 0 || st + static_cast<CorpusPos>(f.len) > corpus_end) continue;
                    bool all = true;
                    for (size_t i = lo; i < hi && all; ++i) {
                        if (i == seed || ops[i].kind != SeqMergeTok::EqRev) continue;
                        const CorpusPos need = st + static_cast<CorpusPos>(i - lo);
                        size_t& c = cur[i - lo];
                        c = gallop_rev(ops[i].span, c, need);
                        all = c < ops[i].span.count && ops[i].span.at(c) == need;
                    }
                    if (all) f.starts.push_back(st);
                }
                return f;
            };
            FixedSide pre, suf;
            if (ok) {
                pre = fixed_starts(0, p);
                suf = fixed_starts(v + 1, n);
                if (pre.free && suf.free) ok = false;        // e.g. [] []* [] — leave to generic
            }
            if (ok) {
                result.plan_path = "seq_gap";
                mark_ranged(region_mask);
                const bool v_wild = ops[v].kind == SeqMergeTok::Wildcard;
                const RevSpan& X = ops[v].span;
                // consecutive X positions starting at x (forward) / ending at y (backward)
                // Callers ask in ascending x / y, so one monotone cursor suffices.
                size_t xcur = 0;
                auto run_fwd = [&](CorpusPos x, int cap) -> int {
                    if (v_wild) return cap;
                    xcur = gallop_rev(X, xcur, x);
                    size_t k = xcur;
                    int r = 0;
                    while (r < cap && k < X.count && X.at(k) == x + r) { ++r; ++k; }
                    return r;
                };
                auto run_back = [&](CorpusPos y, int cap) -> int {
                    if (v_wild) return cap;
                    xcur = gallop_rev(X, xcur, y + 1);         // first > y
                    size_t k = xcur;
                    int r = 0;
                    while (r < cap && k > 0 && X.at(k - 1) == y - r) { ++r; --k; }
                    return r;
                };
                const StructuralAttr* wsa = has_within ? &corpus_.structure(effective_within) : nullptr;
                FlatRegionCursor wcur;
                if (wsa) wcur = FlatRegionCursor(*wsa);
                const bool use_mask = region_mask.status == RegionMaskStatus::Ready;
                const bool cheap_total_ok = (q.global_region_filters.empty() || use_mask)
                                            && token_anchor_constraints.empty();
                bool stop = false;
                auto emit = [&](CorpusPos st, int L, CorpusPos t) -> bool {
                    // t = first suffix position (= V start + L)
                    if (use_mask && !region_mask.contains(st)) return true;
                    if (max_matches > 0 && result.matches.size() >= max_matches) {
                        if (!count_total) return false;
                        if (cheap_total_ok) {
                            ++result.total_count;
                            progress_tick(st, result.total_count);
                            return !reached_total_cap();
                        }
                    }
                    std::vector<CorpusPos> pm(2 * n);
                    for (size_t i = 0; i < p; ++i) pm[i] = pm[n + i] = st + static_cast<CorpusPos>(i);
                    const CorpusPos vb = st + static_cast<CorpusPos>(p);
                    if (L == 0) { pm[v] = pm[n + v] = NO_HEAD; }
                    else { pm[v] = vb; pm[n + v] = vb + L - 1; }
                    for (size_t j = 0; j < qn; ++j) pm[v + 1 + j] = pm[n + v + 1 + j] = t + static_cast<CorpusPos>(j);
                    add_match(std::move(pm));
                    return !(reached_limit() || reached_total_cap());
                };
                // bulk count for a run of pairs that need no per-hit work
                auto bulk = [&](size_t cnt) {
                    result.total_count += cnt;
                    progress_tick(-1, result.total_count);
                    if (max_total_cap > 0 && result.total_count >= max_total_cap) {
                        result.total_count = max_total_cap;
                        stop = true;
                    }
                };
                auto page_full = [&] { return max_matches > 0 && result.matches.size() >= max_matches; };
                const bool anchor_prefix = !pre.free && (suf.free || pre.size() <= suf.size());
                // interval mask (region filter / P4.1 range): jump the anchor list
                // to the next allowed start instead of testing every anchor
                const bool mask_iv = use_mask && !region_mask.use_bits;
                MaskCursor amask(region_mask);
                if (anchor_prefix) {
                    size_t tlo = 0;
                    for (size_t a = 0; a < pre.size() && !stop; ++a) {
                        const CorpusPos st = pre.at(a);
                        if (st < 0) continue;
                        if (mask_iv && !amask.contains(st)) {
                            if (amask.exhausted()) break;
                            const size_t j = pre.geq(a, amask.next_start());
                            if (j >= pre.size()) break;
                            a = j - 1;
                            continue;
                        }
                        const CorpusPos vb = st + static_cast<CorpusPos>(p);
                        CorpusPos limit = corpus_end - 1;             // last allowed match position
                        if (wsa) {
                            const int64_t r = wcur.find(st);
                            if (r < 0) continue;
                            limit = std::min(limit, wcur.r[r].end);
                        }
                        // prefix, V and suffix must end by `limit`: L <= limit - vb + 1 - qn
                        const int64_t room = limit - vb + 1 - static_cast<CorpusPos>(qn);
                        if (room < 0) continue;
                        const int lmax = static_cast<int>(std::min<int64_t>(vmax, room));
                        const int L_hi = run_fwd(vb, lmax);
                        if (L_hi < vmin) continue;
                        if (use_mask && !region_mask.contains(st)) continue;
                        if (qn == 0 || suf.free) {
                            // every L in [vmin, L_hi] is a hit (suffix is wildcards or empty)
                            if (page_full() && count_total && cheap_total_ok) {
                                bulk(static_cast<size_t>(L_hi - vmin + 1));
                                continue;
                            }
                            for (int L = vmin; L <= L_hi && !stop; ++L)
                                if (!emit(st, L, vb + L)) stop = true;
                            continue;
                        }
                        // suffix starts in [vb + vmin, vb + L_hi]
                        tlo = suf.geq(tlo, vb + vmin);
                        if (page_full() && count_total && cheap_total_ok) {
                            bulk(suf.geq(tlo, vb + L_hi + 1) - tlo);
                            continue;
                        }
                        for (size_t k = tlo; k < suf.size() && !stop; ++k) {
                            const CorpusPos t = suf.at(k);
                            if (t > vb + L_hi) break;
                            if (!emit(st, static_cast<int>(t - vb), t)) stop = true;
                        }
                    }
                } else {
                    // anchor on the suffix: t ascending, match end e = t + qn - 1
                    size_t slo = 0;
                    size_t a0 = 0;
                    // P4.1 range [lo, hi): starts st = t - p - L with L in [vmin, vmax],
                    // so only suffixes t in [lo + p + vmin, hi - 1 + p + vmax] can count
                    const bool rng = region_mask.range_only;
                    const int64_t t_first = rng ? region_mask.iv.front().s + static_cast<int64_t>(p) + vmin : 0;
                    const int64_t t_last = rng ? region_mask.iv.back().e + static_cast<int64_t>(p) + vmax
                                               : std::numeric_limits<int64_t>::max();
                    if (rng) a0 = suf.geq(0, t_first);
                    for (size_t a = a0; a < suf.size() && !stop; ++a) {
                        const CorpusPos t = suf.at(a);
                        if (t > t_last) break;
                        const CorpusPos e = t + static_cast<CorpusPos>(qn) - 1;
                        if (t < 0 || e >= corpus_end) continue;
                        CorpusPos first = 0;                          // first allowed match position
                        if (wsa) {
                            const int64_t r = wcur.find(e);
                            if (r < 0) continue;
                            first = wcur.r[r].start;
                            if (t < first) continue;                  // suffix itself crosses a boundary
                        }
                        // the match must start at or after `first`: L <= t - first - p
                        const int64_t room = t - first - static_cast<CorpusPos>(p);
                        if (room < 0) continue;
                        const int lmax = static_cast<int>(std::min<int64_t>(vmax, room));
                        const int L_hi = run_back(t - 1, lmax);
                        if (L_hi < vmin) continue;
                        if (p == 0 || pre.free) {
                            if (page_full() && count_total && cheap_total_ok && !use_mask) {
                                bulk(static_cast<size_t>(L_hi - vmin + 1));
                                continue;
                            }
                            for (int L = vmin; L <= L_hi && !stop; ++L)
                                if (!emit(t - L - static_cast<CorpusPos>(p), L, t)) stop = true;
                            continue;
                        }
                        // prefix starts st with st + p + L = t, L in [vmin, L_hi]
                        const CorpusPos smin = t - L_hi - static_cast<CorpusPos>(p);
                        const CorpusPos smax = t - vmin - static_cast<CorpusPos>(p);
                        slo = pre.geq(slo, smin);                          // smin is ascending
                        const size_t shi = pre.geq(slo, smax + 1);
                        if (page_full() && count_total && cheap_total_ok && !use_mask) {
                            bulk(shi - slo);
                            continue;
                        }
                        for (size_t k = slo; k < shi && !stop; ++k) {
                            const CorpusPos st = pre.at(k);
                            if (!emit(st, static_cast<int>(t - st - static_cast<CorpusPos>(p)), t)) stop = true;
                        }
                    }
                }
                if (max_total_cap > 0 && result.total_count > max_total_cap)
                    result.total_count = max_total_cap;
                return finish_query();
            }
        }
        // ── P2.x: two or more variable-length elements (seq_multi_bitmap) ──
        // `[ADP] [DET]? [ADJ]* [NOUN]`, `[DET] [ADJ]* [NOUN]+`: every assignment of
        // lengths L_i ({0 if min_i = 0} ∪ [max(min_i, 1), max_i]) with token i's L_i
        // positions all matching X_i and the tokens contiguous is one hit — the
        // generic executor's hits (it seeds from a token with min >= 1, so one is
        // required here). Per chunk, a depth-first walk over the tokens and their
        // lengths keeps the set of starts whose prefix matches at the current
        // offset d as a bitmap: acc & (X_i >> d + L - 1) for each extra position,
        // & A_d (the span [s, s+d-1] inside one `within` region: covered start, no
        // region end before its last position; the corpus end counts as one).
        // A branch dies when its bitmap is empty, so runs of a selective X end after
        // a few lengths. Leaves are counted (popcount) or, for the page, walked in
        // start order.
        if (simple && n_var >= 2 && bitmap_mode() != BitmapMode::Off) {
            std::string eff_within = q.within.empty() ? corpus_.default_within() : q.within;
            const StructuralAttr* wsa = (!eff_within.empty() && corpus_.has_structure(eff_within))
                ? &corpus_.structure(eff_within) : nullptr;
            const bool wspan = wsa && (corpus_.is_nested(eff_within) || corpus_.is_overlapping(eff_within));
            int sum_max = 0, wild_var = 0;
            bool any_required = false, lengths_ok = true;
            for (size_t i = 0; i < n; ++i) {
                const auto& tok = q.tokens[i];
                if (tok.max_repeat < 1 || tok.max_repeat < tok.min_repeat) lengths_ok = false;
                sum_max += tok.max_repeat;
                if (tok.min_repeat >= 1) any_required = true;
                if (!tok.conditions && tok.has_repetition()) ++wild_var;
            }
            bool mcompiled = !wspan && lengths_ok && any_required && wild_var <= 1
                             && sum_max - 1 <= kBmMaxShift;
            bool mdense = false;
            std::vector<std::unique_ptr<BmExpr>> mtex(n);   // nullptr = []
            for (size_t i = 0; i < n && mcompiled; ++i) {
                if (!q.tokens[i].conditions) continue;
                mtex[i] = compile_bm(q.tokens[i].conditions, &mdense);
                if (!mtex[i]) mcompiled = false;
            }
            if (mcompiled) {
                RegionPosMask bm_mask = build_start_mask();
                result.plan_path = "seq_multi_bitmap";
                mark_ranged(bm_mask);
                if (bm_mask.status == RegionMaskStatus::Unsatisfiable) {
                    result.total_exact = true;
                    return result;
                }
                const bool use_mask = bm_mask.status == RegionMaskStatus::Ready;
                const bool cheap_total_ok = (q.global_region_filters.empty() || use_mask)
                                            && token_anchor_constraints.empty();
                constexpr size_t W = BitmapIndex::kWords;
                constexpr size_t WX = W + kBmExtra;
                const CorpusPos N = corpus_.size();
                const size_t nchunks = static_cast<size_t>((N + BitmapIndex::kChunk - 1) >> BitmapIndex::kChunkShift);
                // tokens a match cannot do without (min >= 1), rarest first: an empty
                // chunk of one of them (and its first extra words) skips the chunk
                std::vector<size_t> required, others;
                for (size_t i = 0; i < n; ++i) {
                    if (!mtex[i]) continue;
                    (q.tokens[i].min_repeat >= 1 ? required : others).push_back(i);
                }
                std::stable_sort(required.begin(), required.end(), [&](size_t a, size_t b) {
                    return mtex[a]->estimate < mtex[b]->estimate;
                });
                size_t fv = 0;                                       // first variable token
                while (fv < n && !q.tokens[fv].has_repetition()) ++fv;
                std::vector<BmChunk> view(n);
                std::vector<uint64_t> cw(WX), ew(WX), alw(W), acc0(W);
                std::vector<uint64_t> A(static_cast<size_t>(sum_max + 1) * W);
                std::vector<uint64_t> bufs(static_cast<size_t>(n + 1) * W);
                std::vector<int> lens(n, 0);
                struct Leaf { std::vector<int> lens; std::vector<uint64_t> w; };
                std::vector<Leaf> leaves;
                size_t nleaves = 0;
                size_t ivc = 0, wr = 0;
                const Region* wreg = wsa ? wsa->region_data() : nullptr;
                const size_t wn = wsa ? wsa->region_count() : 0;
                std::shared_ptr<BitmapIndex> sbi = wsa ? structure_bitmap(eff_within) : nullptr;
                std::unique_ptr<BmValue> sC, sE;
                if (sbi) {
                    sC = std::make_unique<BmValue>(*sbi, BitmapIndex::kStructCovered, 0);
                    sE = std::make_unique<BmValue>(*sbi, BitmapIndex::kStructEnds, 0);
                }
                auto set_range = [](uint64_t* m, size_t a, size_t b) {   // inclusive bit range
                    const size_t wa = a >> 6, wb = b >> 6;
                    const uint64_t ma = ~uint64_t{0} << (a & 63);
                    const uint64_t mb = ~uint64_t{0} >> (63 - (b & 63));
                    if (wa == wb) { m[wa] |= ma & mb; return; }
                    m[wa] |= ma;
                    for (size_t w = wa + 1; w < wb; ++w) m[w] = ~uint64_t{0};
                    m[wb] |= mb;
                };
                auto shifted_raw = [](const std::vector<uint64_t>& a, size_t i, int k) -> uint64_t {
                    const size_t q0 = i + static_cast<size_t>(k >> 6);
                    const int r = k & 63;
                    if (r == 0) return a[q0];
                    return (a[q0] >> r) | (a[q0 + 1] << (64 - r));
                };
                bool stop = false, counting = false;
                size_t nw = W;
                int a_hi = 1;                                         // A_d computed for d <= a_hi
                auto Aat = [&](int d) -> const uint64_t* {
                    while (a_hi < d) {
                        ++a_hi;
                        uint64_t* dst = A.data() + static_cast<size_t>(a_hi) * W;
                        const uint64_t* prev = dst - W;
                        for (size_t w = 0; w < nw; ++w) dst[w] = prev[w] & ~shifted_raw(ew, w, a_hi - 2);
                    }
                    return A.data() + static_cast<size_t>(d) * W;
                };
                auto leaf = [&](const uint64_t* acc) {
                    if (counting) {
                        size_t cnt = 0;
                        for (size_t w = 0; w < nw; ++w) cnt += static_cast<size_t>(__builtin_popcountll(acc[w]));
                        result.total_count += cnt;
                        return;
                    }
                    if (nleaves == leaves.size()) leaves.push_back(Leaf{std::vector<int>(n), std::vector<uint64_t>(W)});
                    Leaf& lf = leaves[nleaves++];
                    lf.lens = lens;
                    std::copy(acc, acc + nw, lf.w.begin());
                };
                // depth-first over tokens i >= fv; acc: starts whose tokens < i match
                // with the span so far = d positions
                auto dfs = [&](auto& self, size_t i, int d, const uint64_t* acc) -> void {
                    if (i == n) { leaf(acc); return; }
                    const auto& tok = q.tokens[i];
                    const BmExpr* ex = mtex[i].get();
                    uint64_t* r = bufs.data() + i * W;
                    if (!tok.has_repetition()) {
                        const uint64_t* Ad = Aat(d + 1);
                        uint64_t any = 0;
                        for (size_t w = 0; w < nw; ++w) {
                            uint64_t x = acc[w] & Ad[w];
                            if (ex) x &= view[i].shifted(w, d);
                            r[w] = x;
                            any |= x;
                        }
                        if (any) { lens[i] = 1; self(self, i + 1, d + 1, r); }
                        return;
                    }
                    const int mn = tok.min_repeat, mx = tok.max_repeat;
                    if (mn == 0) { lens[i] = 0; self(self, i + 1, d, acc); }
                    std::copy(acc, acc + nw, r);
                    for (int L = 1; L <= mx; ++L) {
                        const uint64_t* Ad = Aat(d + L);
                        uint64_t any = 0;
                        for (size_t w = 0; w < nw; ++w) {
                            uint64_t x = r[w] & Ad[w];
                            if (ex) x &= view[i].shifted(w, d + L - 1);
                            r[w] = x;
                            any |= x;
                        }
                        if (!any) break;
                        if (L >= mn) { lens[i] = L; self(self, i + 1, d + L, r); }
                    }
                };
                for (size_t c = 0; c < nchunks && !stop; ++c) {
                    const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
                    progress_tick(base, result.total_count, true);
                    const CorpusPos top = std::min<CorpusPos>(base + BitmapIndex::kChunk, N) - 1;
                    nw = static_cast<size_t>((top - base) >> 6) + 1;
                    if (use_mask && !bm_mask.use_bits) {
                        const auto& iv = bm_mask.iv;
                        while (ivc < iv.size() && iv[ivc].e < base) ++ivc;
                        if (ivc >= iv.size()) break;
                        if (iv[ivc].s > top) continue;
                    }
                    bool any = true;
                    for (size_t i : required) {
                        view[i] = mtex[i]->load(c);
                        if (view[i].zero) { any = false; break; }
                    }
                    if (!any) continue;
                    for (size_t i : others) view[i] = mtex[i]->load(c);
                    // starts: in the chunk, allowed by the `:: match.…` mask, the fixed prefix
                    uint64_t orr = 0;
                    for (size_t w = 0; w < nw; ++w) {
                        uint64_t x = ~uint64_t{0};
                        for (size_t i = 0; i < fv; ++i)
                            if (mtex[i]) x &= view[i].shifted(w, static_cast<int>(i));
                        acc0[w] = x;
                    }
                    if ((top - base + 1) & 63) acc0[nw - 1] &= (uint64_t{1} << ((top - base + 1) & 63)) - 1;
                    if (use_mask) {
                        if (bm_mask.use_bits) {
                            const size_t b0 = static_cast<size_t>(base >> 6);
                            for (size_t w = 0; w < nw; ++w)
                                acc0[w] &= b0 + w < bm_mask.bits.size() ? bm_mask.bits[b0 + w] : 0;
                        } else {
                            std::fill(alw.begin(), alw.begin() + nw, 0);
                            const auto& iv = bm_mask.iv;
                            for (size_t j = ivc; j < iv.size() && iv[j].s <= top; ++j) {
                                const CorpusPos a = std::max(iv[j].s, base), b = std::min(iv[j].e, top);
                                if (b >= a) set_range(alw.data(), static_cast<size_t>(a - base), static_cast<size_t>(b - base));
                            }
                            for (size_t w = 0; w < nw; ++w) acc0[w] &= alw[w];
                        }
                    }
                    for (size_t w = 0; w < nw; ++w) orr |= acc0[w];
                    if (!orr) continue;
                    // covered positions C and region ends E over the chunk + the extra words
                    std::fill(cw.begin(), cw.end(), 0);
                    std::fill(ew.begin(), ew.end(), 0);
                    const CorpusPos wend = base + static_cast<CorpusPos>(WX * 64);   // exclusive
                    if (sC) {
                        const BmChunk cv = sC->load(c), ev = sE->load(c);
                        std::copy(cv.w, cv.w + W, cw.begin());
                        std::copy(cv.nx, cv.nx + kBmExtra, cw.begin() + W);
                        std::copy(ev.w, ev.w + W, ew.begin());
                        std::copy(ev.nx, ev.nx + kBmExtra, ew.begin() + W);
                    } else if (wreg) {
                        while (wr < wn && wreg[wr].end < base) ++wr;
                        for (size_t j = wr; j < wn && wreg[j].start < wend; ++j) {
                            const CorpusPos a = std::max<CorpusPos>(wreg[j].start, base);
                            const CorpusPos b = std::min<CorpusPos>(wreg[j].end, wend - 1);
                            if (b >= a) set_range(cw.data(), static_cast<size_t>(a - base), static_cast<size_t>(b - base));
                            if (wreg[j].end < wend) {
                                const size_t o = static_cast<size_t>(wreg[j].end - base);
                                ew[o >> 6] |= uint64_t{1} << (o & 63);
                            }
                        }
                    } else {
                        const CorpusPos b = std::min<CorpusPos>(N, wend) - 1;
                        set_range(cw.data(), 0, static_cast<size_t>(b - base));
                    }
                    if (N - 1 < wend) {
                        const size_t o = static_cast<size_t>(N - 1 - base);
                        ew[o >> 6] |= uint64_t{1} << (o & 63);
                    }
                    std::copy(cw.begin(), cw.begin() + nw, A.begin() + W);   // A_1: covered starts
                    a_hi = 1;
                    // the fixed prefix spans fv positions: A_fv
                    if (fv > 0) {
                        const uint64_t* Af = Aat(static_cast<int>(fv));
                        orr = 0;
                        for (size_t w = 0; w < nw; ++w) orr |= (acc0[w] &= Af[w]);
                        if (!orr) continue;
                        for (size_t i = 0; i < fv; ++i) lens[i] = 1;
                    }
                    nleaves = 0;
                    dfs(dfs, fv, static_cast<int>(fv), acc0.data());
                    if (counting || nleaves == 0) {
                        if (counting && max_total_cap > 0 && result.total_count >= max_total_cap) {
                            result.total_count = max_total_cap;
                            stop = true;
                        }
                        continue;
                    }
                    // the page: starts ascending, per start the leaves in walk order
                    for (size_t w = 0; w < nw && !stop; ++w) {
                        uint64_t starts = 0;
                        for (size_t k = 0; k < nleaves; ++k) starts |= leaves[k].w[w];
                        while (starts && !stop) {
                            const int j = __builtin_ctzll(starts);
                            starts &= starts - 1;
                            const CorpusPos st = base + static_cast<CorpusPos>(w * 64) + j;
                            for (size_t k = 0; k < nleaves && !stop; ++k) {
                                if (!(leaves[k].w[w] >> j & 1)) continue;
                                if (counting) { ++result.total_count; continue; }
                                if (max_matches > 0 && result.matches.size() >= max_matches) {
                                    if (!count_total) { stop = true; break; }
                                    if (cheap_total_ok) { counting = true; ++result.total_count; continue; }
                                }
                                std::vector<CorpusPos> pm(2 * n);
                                CorpusPos off = st;
                                for (size_t i = 0; i < n; ++i) {
                                    const int L = leaves[k].lens[i];
                                    if (L == 0) { pm[i] = pm[n + i] = NO_HEAD; continue; }
                                    pm[i] = off;
                                    pm[n + i] = off + L - 1;
                                    off += L;
                                }
                                add_match(std::move(pm));
                                if (reached_limit() || reached_total_cap()) { stop = true; break; }
                            }
                        }
                    }
                    if (counting && max_total_cap > 0 && result.total_count >= max_total_cap) {
                        result.total_count = max_total_cap;
                        stop = true;
                    }
                }
                return finish_query();
            }
        }
    }

    // ── Sequence fast path: bypass plan/step/expand for pure linear sequences ──
    //
    // For queries like [upos="ADJ"] [upos="NOUN"] where all relations are
    // SEQUENCE, no repetitions, no anchors: iterate the cheapest token and
    // check all others at fixed offsets.  No Match vector bookkeeping,
    // no find_related, no partial-match machinery.
    {
        bool all_seq = true;
        for (const auto& rel : q.relations)
            if (rel.type != RelationType::SEQUENCE) { all_seq = false; break; }
        bool no_rep = true;
        bool no_anchor = true;
        bool any_dep_subtree = false;
        for (const auto& tok : q.tokens) {
            if (tok.has_repetition()) no_rep = false;
            if (tok.is_anchor()) no_anchor = false;
            if (tok.is_dep_subtree) any_dep_subtree = true;
        }

        if (all_seq && no_rep && no_anchor && !any_dep_subtree && n >= 2
            && fastpath_mode() != FastPathMode::Off) {
            // Pick cheapest token as seed (cardinalities for debug / fallback)
            size_t seed = 0;
            size_t best_est = SIZE_MAX;
            std::vector<size_t> card(n);
            for (size_t i = 0; i < n; ++i) {
                card[i] = q.tokens[i].is_dep_subtree
                              ? std::numeric_limits<size_t>::max()
                              : estimate_cardinality(q.tokens[i].conditions);
                if (card[i] < best_est) { best_est = card[i]; seed = i; }
            }
            result.seed_token = seed;
            result.cardinalities = card;
            result.plan_path = "seq_probe";

            // Within-region support
            std::string effective_within = q.within.empty()
                ? corpus_.default_within() : q.within;
            bool has_within = !effective_within.empty() &&
                              corpus_.has_structure(effective_within);
            const StructuralAttr* within_sa = has_within
                ? &corpus_.structure(effective_within) : nullptr;
            const bool within_span_semantics =
                has_within && (corpus_.is_nested(effective_within) ||
                               corpus_.is_overlapping(effective_within));

            CorpusPos corpus_end = corpus_.size();
            // Candidates mostly arrive in position order: reuse the last region
            // (find_region_from) instead of a full binary search per candidate.
            int64_t within_hint = -1;
            FlatRegionCursor within_cur;
            if (within_sa && !within_span_semantics) within_cur = FlatRegionCursor(*within_sa);

            RegionPosMask region_mask =
                build_start_mask();
            // every path of this block applies region_mask to the starts it emits / counts
            mark_ranged(region_mask);
            if (region_mask.status == RegionMaskStatus::Unsatisfiable) {
                result.total_exact = true;
                return result;
            }
            const bool use_region_mask =
                region_mask.status == RegionMaskStatus::Ready;
            const bool cheap_total_ok =
                (q.global_region_filters.empty() || use_region_mask)
                && token_anchor_constraints.empty();   // anchors are applied in add_match

            if (try_seq_bitmap(within_sa, within_span_semantics, region_mask, use_region_mask,
                               cheap_total_ok))
                return finish_query();

            // ── S1: Manatee-style .rev shift-merge when every token is EQ or [] ──
            std::vector<SeqMergeOperand> ops(n);
            bool mergeable = true;
            for (size_t i = 0; i < n; ++i) {
                ops[i] = merge_operand(q.tokens[i].conditions);
                if (ops[i].kind == SeqMergeTok::Complex) { mergeable = false; break; }
                if (ops[i].kind == SeqMergeTok::EqRev && ops[i].span.empty()) {
                    // Lex id present but no postings → zero hits
                    result.total_exact = true;
                    return result;
                }
            }
            if (fastpath_mode() != FastPathMode::On) mergeable = false;

            // Merge paths hand accept_start ascending starts: monotone mask cursor.
            MaskCursor mask_cur(region_mask);
            const bool mask_intervals = use_region_mask && !region_mask.use_bits;
            auto accept_start = [&](CorpusPos p0) -> bool {
                CorpusPos pN = p0 + static_cast<int64_t>(n) - 1;
                if (p0 < 0 || pN >= corpus_end) return true;
                if (use_region_mask && !mask_cur.contains(p0))
                    return true;
                if (within_sa) {
                    if (within_span_semantics) {
                        if (!within_span_in_some_region(*within_sa, p0, pN)) return true;
                    } else {
                        // Merge paths emit starts in increasing order: RegionCursor
                        // (amortised O(1), inline) instead of a lookup per candidate.
                        int64_t rgn = within_cur.find(p0);
                        if (rgn < 0) return true;
                        if (pN > within_cur.r[rgn].end) return true;
                    }
                }
                // Past page capacity: count only (no Match alloc) when totaling.
                if (max_matches > 0 && result.matches.size() >= max_matches) {
                    if (!count_total) return false;
                    if (cheap_total_ok) {
                        ++result.total_count;
                        progress_tick(p0, result.total_count);
                        return !reached_total_cap();
                    }
                    // Fall through to add_match for filter-aware counting.
                }
                std::vector<CorpusPos> pm(2 * n);
                for (size_t i = 0; i < n; ++i) {
                    pm[i]     = p0 + static_cast<int64_t>(i);
                    pm[n + i] = p0 + static_cast<int64_t>(i);
                }
                add_match(std::move(pm));
                return !reached_limit() && !reached_total_cap();
            };

            if (mergeable) {
                // Find first EqRev token as merge left; accumulate starts.
                size_t first_eq = n;
                for (size_t i = 0; i < n; ++i) {
                    if (ops[i].kind == SeqMergeTok::EqRev) { first_eq = i; break; }
                }
                bool ok = true;
                if (first_eq == n) {
                    // All wildcards — every aligned window; probe-free but O(corpus).
                    // Fall through to seed path (degenerate).
                    ok = false;
                } else if (n == 2 && first_eq == 0 && ops[1].kind == SeqMergeTok::EqRev) {
                    result.plan_path = "seq_merge2";
                    if (mask_intervals) {
                        // Merge only inside the allowed intervals (token 0 in [s,e],
                        // token 1 in [s+1,e+1]) instead of over the whole corpus.
                        const RevSpan& A = ops[0].span;
                        const RevSpan& B = ops[1].span;
                        size_t la = 0, lb = 0;
                        for (const auto& I : region_mask.iv) {
                            la = gallop_rev(A, la, I.s);
                            if (la >= A.count) break;
                            const size_t ha = gallop_rev(A, la, I.e + 1);
                            lb = gallop_rev(B, lb, I.s + 1);
                            const size_t hb = gallop_rev(B, lb, I.e + 2);
                            if (ha > la && hb > lb
                                && !shift_merge_rev(rev_slice(A, la, ha), rev_slice(B, lb, hb),
                                                    /*delta=*/1, accept_start))
                                break;
                            la = ha;
                            lb = hb;
                        }
                    } else {
                        ok = shift_merge_rev(ops[0].span, ops[1].span, /*delta=*/1, accept_start);
                    }
                } else if (n == 2 && first_eq == 0 && ops[1].kind == SeqMergeTok::Wildcard) {
                    result.plan_path = "seq_merge_wild";
                    const RevSpan& A = ops[0].span;
                    MaskCursor jm(region_mask);
                    for (size_t i = 0; i < A.count; ++i) {
                        const CorpusPos a = A.at(i);
                        if (mask_intervals && !jm.contains(a)) {
                            if (jm.exhausted()) break;
                            const size_t j = gallop_rev(A, i, jm.next_start());
                            if (j >= A.count) break;
                            i = j - 1;
                            continue;
                        }
                        if (!accept_start(a)) { ok = true; break; }
                    }
                } else if (n == 2 && first_eq == 1 && ops[0].kind == SeqMergeTok::Wildcard) {
                    // [][B]: starts at b-1
                    result.plan_path = "seq_merge_wild";
                    const RevSpan& B = ops[1].span;
                    MaskCursor jm(region_mask);
                    for (size_t i = 0; i < B.count; ++i) {
                        CorpusPos b = B.at(i);
                        if (b == 0) continue;
                        if (mask_intervals && !jm.contains(b - 1)) {
                            if (jm.exhausted()) break;
                            const size_t j = gallop_rev(B, i, jm.next_start() + 1);
                            if (j >= B.count) break;
                            i = j - 1;
                            continue;
                        }
                        if (!accept_start(b - 1)) { ok = true; break; }
                    }
                } else {
                    // n>=3 or mixed: leapfrog join driven by the rarest EQ token.
                    // For each position s of the rarest list, gallop every other EQ
                    // list to s - seed + t; emit the start when all agree. Streams
                    // (first page stops early) and costs O(|rarest| * log gap).
                    result.plan_path = "seq_merge_n";
                    size_t seed_eq = first_eq;
                    for (size_t t = 0; t < n; ++t)
                        if (ops[t].kind == SeqMergeTok::EqRev
                            && ops[t].span.count < ops[seed_eq].span.count)
                            seed_eq = t;
                    std::vector<size_t> cur(n, 0);
                    const RevSpan& S = ops[seed_eq].span;
                    // Gallop only into lists much longer than the seed; for similar
                    // sizes a linear advance is cheaper (a few steps per seed).
                    std::vector<char> use_gallop(n, 0);
                    for (size_t t = 0; t < n; ++t)
                        use_gallop[t] = ops[t].kind == SeqMergeTok::EqRev
                                        && ops[t].span.count > (S.count << 3);
                    const int64_t back = static_cast<int64_t>(seed_eq);
                    bool exhausted = false;
                    MaskCursor seed_mask(region_mask);
                    for (size_t i = 0; i < S.count && !exhausted; ++i) {
                        CorpusPos p0 = S.at(i) - back;
                        if (p0 < 0) continue;
                        if (mask_intervals && !seed_mask.contains(p0)) {
                            if (seed_mask.exhausted()) break;
                            // jump the seed list to the next allowed start
                            const size_t j = gallop_rev(S, i, seed_mask.next_start() + back);
                            if (j >= S.count) break;
                            i = j - 1;
                            continue;
                        }
                        bool all = true;
                        for (size_t t = 0; t < n; ++t) {
                            if (t == seed_eq || ops[t].kind != SeqMergeTok::EqRev) continue;
                            const RevSpan& B = ops[t].span;
                            CorpusPos need = p0 + static_cast<int64_t>(t);
                            if (use_gallop[t]) {
                                cur[t] = gallop_rev(B, cur[t], need);
                            } else {
                                size_t c = cur[t];
                                while (c < B.count && B.at(c) < need) ++c;
                                cur[t] = c;
                            }
                            if (cur[t] >= B.count) { exhausted = true; all = false; break; }
                            if (B.at(cur[t]) != need) { all = false; break; }
                        }
                        if (all && !accept_start(p0)) break;
                    }
                }

                if (first_eq != n) {
                    return finish_query();
                }
                (void)ok;
            }

            // ── Fallback: seed + neighbor probe (non-EQ / regex / MV / …) ──
            int64_t seed_offset = static_cast<int64_t>(seed);

            for_each_seed_position(q.tokens[seed].conditions, [&](CorpusPos seed_p) {
                // Compute position of first token from this seed
                CorpusPos p0 = seed_p - seed_offset;
                CorpusPos pN = p0 + static_cast<int64_t>(n) - 1;
                if (p0 < 0 || pN >= corpus_end) return true;  // out of bounds, skip
                if (use_region_mask && !region_mask.contains(p0))
                    return true;

                if (within_sa) {
                    if (within_span_semantics) {
                        if (!within_span_in_some_region(*within_sa, p0, pN)) return true;
                    } else {
                        int64_t rgn = within_sa->find_region_from(p0, within_hint);
                        if (rgn < 0) return true;
                        within_hint = rgn;
                        Region r = within_sa->get(static_cast<size_t>(rgn));
                        if (pN > r.end) return true;
                    }
                }

                // Check all non-seed tokens at their fixed offsets
                bool all_match = true;
                for (size_t i = 0; i < n; ++i) {
                    if (i == seed) continue;
                    if (!check_conditions(p0 + static_cast<int64_t>(i),
                                          q.tokens[i].conditions)) {
                        all_match = false;
                        break;
                    }
                }
                if (!all_match) return true;

                // Build match: [0..n)=starts, [n..2n)=ends
                std::vector<CorpusPos> pm(2 * n);
                for (size_t i = 0; i < n; ++i) {
                    pm[i]     = p0 + static_cast<int64_t>(i);
                    pm[n + i] = p0 + static_cast<int64_t>(i);
                }
                add_match(std::move(pm));
                return !reached_limit() && !reached_total_cap();
            });

            return finish_query();
        }
    }

    // ── Dep EQ fast path: [A] > [B] / [A] < [B] via bitset + head() ─────
    //
    // Dense POS×POS (e.g. VERB > NOUN) used to probe millions of seeds.
    // Build a bitset of the parent side from .rev, stream the child side,
    // test head(child) ∈ parent — sequential .rev + amortized head_from.
    {
        bool no_rep = true, no_anchor = true, any_dep_subtree = false;
        for (const auto& tok : q.tokens) {
            if (tok.has_repetition()) no_rep = false;
            if (tok.is_anchor()) no_anchor = false;
            if (tok.is_dep_subtree) any_dep_subtree = true;
        }
        if (n == 2 && no_rep && no_anchor && !any_dep_subtree
            && q.relations.size() == 1 && corpus_.has_deps()
            && fastpath_mode() == FastPathMode::On) {
            RelationType rt = q.relations[0].type;
            if (rt == RelationType::GOVERNS || rt == RelationType::GOVERNED_BY) {
                SeqMergeOperand left = merge_operand(q.tokens[0].conditions);
                SeqMergeOperand right = merge_operand(q.tokens[1].conditions);
                // The bitset join streams both lists, O(|A| + |B|); when one side is
                // much rarer, probing from the rare side (dep_probe) is far cheaper.
                const bool skewed = left.kind == SeqMergeTok::EqRev
                    && right.kind == SeqMergeTok::EqRev
                    && std::min(left.span.count, right.span.count) * kDepBitsetMaxSkew
                           < std::max(left.span.count, right.span.count);
                if (left.kind == SeqMergeTok::EqRev && right.kind == SeqMergeTok::EqRev
                    && !left.span.empty() && !right.span.empty()) {
                    // GOVERNS: head(right)=left.  GOVERNED_BY: head(left)=right.
                    RevSpan parent_span =
                        (rt == RelationType::GOVERNS) ? left.span : right.span;
                    RevSpan child_span =
                        (rt == RelationType::GOVERNS) ? right.span : left.span;
                    const bool parent_is_token0 = (rt == RelationType::GOVERNS);

                    result.seed_token = parent_is_token0 ? 0 : 1;
                    result.plan_path = skewed ? "dep_probe" : "dep_bitset";
                    result.cardinalities = {
                        estimate_cardinality(q.tokens[0].conditions),
                        estimate_cardinality(q.tokens[1].conditions)};

                    std::string effective_within = q.within.empty()
                        ? corpus_.default_within() : q.within;
                    bool has_within = !effective_within.empty() &&
                                      corpus_.has_structure(effective_within);
                    const StructuralAttr* within_sa = has_within
                        ? &corpus_.structure(effective_within) : nullptr;
                    const bool within_span_semantics =
                        has_within && (corpus_.is_nested(effective_within) ||
                                       corpus_.is_overlapping(effective_within));

                    // `within text_langcode="en"` is functionally the same as
                    // `[… & text_langcode="en"]`, but arrives as a post-join
                    // global filter. Compile EQ region filters into a position
                    // mask and prune parent/child streams before the join —
                    // otherwise we pay for the full-corpus dep join then discard.
                    RegionPosMask region_mask =
                        build_start_mask();
                    mark_ranged(region_mask);
                    if (region_mask.status == RegionMaskStatus::Unsatisfiable) {
                        result.total_exact = true;
                        return result;
                    }
                    // P4.1: a bare partition range (no region filter) slices both
                    // posting lists instead of masking positions: a head and its
                    // dependents share the sentence, which lies inside one range
                    // (ranges are cut at sentence starts), so the unmasked kernels
                    // — with their exact-total hot loops — run on the slices.
                    const bool slice_range = region_mask.range_only;
                    if (slice_range) {
                        const CorpusPos rlo = region_mask.iv.front().s;
                        const CorpusPos rhi = region_mask.iv.back().e + 1;
                        auto cut = [&](const RevSpan& sp) {
                            const size_t a = gallop_rev(sp, 0, rlo);
                            return rev_slice(sp, a, gallop_rev(sp, a, rhi));
                        };
                        parent_span = cut(parent_span);
                        child_span = cut(child_span);
                    }
                    const bool use_region_mask =
                        region_mask.status == RegionMaskStatus::Ready && !slice_range;
                    // Cheap total++ bypass is only safe when every candidate that
                    // reaches it already satisfies global region filters (mask),
                    // or there are no such filters. Otherwise go through add_match.
                    const bool cheap_total_ok =
                        (q.global_region_filters.empty() || use_region_mask)
                && token_anchor_constraints.empty();   // anchors are applied in add_match

                    // ── Kernel (P1.0g / P1.8 / P1.0h / P1.11) ──
                    // Raw arrays, no cross-file calls per child. head = pos + head_rel
                    // when dep.head_rel exists; otherwise sentence start (cursor,
                    // children arrive sorted) + sentence-local head. Parent membership
                    // uses a 32 KB sliding bitset filled from the (sorted) parent
                    // postings just ahead of the child cursor — no corpus-sized bitset.
                    const auto& deps = corpus_.deps();
                    const StructuralAttr& sents = corpus_.structure("s");
                    const Region* sent_r = sents.region_data();
                    FlatRegionCursor sent_cur(sents);
                    const HeadRelView& hrel = deps.head_rel_data();
                    const int16_t* hloc = deps.head_local_data();
                    const CorpusPos corpus_end = corpus_.size();
                    const bool mask_bits = use_region_mask && region_mask.use_bits;
                    const bool mask_iv = use_region_mask && !region_mask.use_bits;
                    MaskCursor child_mask(region_mask), parent_mask(region_mask);
                    // `within s` is implied: a head and its dependent share the sentence
                    // the dependency index is built on.
                    const StructuralAttr* wsa =
                        (within_sa && !within_span_semantics && within_sa == &sents)
                            ? nullptr : within_sa;
                    int64_t within_hint = -1;

                    // Returns false to stop the scan.
                    auto emit = [&](CorpusPos parent, CorpusPos child) -> bool {
                        if (wsa) {
                            CorpusPos lo = parent < child ? parent : child;
                            CorpusPos hi = parent < child ? child : parent;
                            if (within_span_semantics) {
                                if (!within_span_in_some_region(*wsa, lo, hi)) return true;
                            } else {
                                int64_t rgn = wsa->find_region_from(lo, within_hint);
                                if (rgn < 0) return true;
                                within_hint = rgn;
                                if (hi > wsa->get(static_cast<size_t>(rgn)).end) return true;
                            }
                        }
                        if (max_matches > 0 && result.matches.size() >= max_matches) {
                            if (!count_total) return false;
                            if (cheap_total_ok) {
                                ++result.total_count;
                                progress_tick(-1, result.total_count);
                                return !reached_total_cap();
                            }
                            // Filters not pre-applied — must go through add_match.
                        }
                        // pm layout: [t0_start, t1_start, t0_end, t1_end]
                        std::vector<CorpusPos> pm(4);
                        if (parent_is_token0) {
                            pm[0] = parent; pm[1] = child;
                            pm[2] = parent; pm[3] = child;
                        } else {
                            pm[0] = child; pm[1] = parent;
                            pm[2] = child; pm[3] = parent;
                        }
                        add_match(std::move(pm));
                        return !(reached_limit() || reached_total_cap());
                    };

                    // Once the page is full and every remaining hit only needs counting,
                    // count with a local counter (no per-hit call).
                    const bool can_fast_count = count_total && cheap_total_ok && !wsa
                        && max_matches > 0 && !agg_ptr && sample_size == 0;
                    auto head_of = [&](CorpusPos child, CorpusPos& parent) -> bool {
                        if (hrel) {
                            const int16_t d = hrel[child];
                            parent = child + d;
                            return d != 0;
                        }
                        const int16_t l = hloc[child];
                        if (l < 0) return false;
                        const int64_t si = sent_cur.find(child);
                        if (si < 0) return false;
                        parent = sent_r[si].start + l;
                        return parent < corpus_end;
                    };
                    auto bit = [](const std::vector<uint64_t>& b, CorpusPos p) -> bool {
                        return (b[static_cast<size_t>(p) >> 6] >> (static_cast<size_t>(p) & 63)) & 1u;
                    };
                    auto run = [&](const auto& cp, size_t cn, const auto& pp, size_t pn) {
                        SlidingBitset par;
                        size_t pi = 0;            // next parent posting to load
                        size_t cnt = 0;
                        bool counting = false;
                        for (size_t i = 0; i < cn; ++i) {
                            const CorpusPos child = static_cast<CorpusPos>(cp[i]);
                            if (mask_iv && !child_mask.contains(child)) {
                                if (child_mask.exhausted()) break;
                                const size_t j = gallop_ptr(cp, cn, i, child_mask.next_start());
                                if (j >= cn) break;
                                i = j - 1;
                                continue;
                            }
                            if (mask_bits && !bit(region_mask.bits, child)) continue;
                            // Parents lie in [child - D, child + D). The window may lag by
                            // up to W - 2D, so advance it in coarse steps (amortised clears).
                            if (child - kMaxHeadDistance - par.dead > kMaxHeadDistance)
                                par.advance_dead(child - kMaxHeadDistance);
                            const CorpusPos fill_to = child + kMaxHeadDistance;
                            while (pi < pn && static_cast<CorpusPos>(pp[pi]) < fill_to) {
                                const CorpusPos p = static_cast<CorpusPos>(pp[pi++]);
                                if (p < par.dead) continue;
                                if (mask_iv && !parent_mask.contains(p)) continue;
                                if (mask_bits && !bit(region_mask.bits, p)) continue;
                                par.set(p);
                            }
                            CorpusPos parent;
                            if (!head_of(child, parent)) continue;
                            if (parent < par.dead || parent >= fill_to || !par.test(parent)) continue;
                            if (counting) { ++cnt; progress_tick(-1, result.total_count + cnt); continue; }
                            if (!emit(parent, child)) break;
                            if (can_fast_count && result.matches.size() >= max_matches) {
                                counting = true;
                                if (hrel && !use_region_mask) {
                                    // Hot path for exact totals (no mask, relative heads):
                                    // children in chunks of W - 2D positions; per chunk,
                                    // reset the 32 KB window, load the parents of
                                    // [base - D, base + W - D) in one tight loop, then
                                    // count the chunk's children branch-free.
                                    constexpr CorpusPos kChunk =
                                        SlidingBitset::kWindow - 2 * kMaxHeadDistance;
                                    size_t ps = 0;    // first parent of the current window
                                    ++i;
                                    while (i < cn) {
                                        const CorpusPos base = static_cast<CorpusPos>(cp[i]);
                                        const CorpusPos lo = base - kMaxHeadDistance;
                                        const CorpusPos hi = base + kChunk + kMaxHeadDistance;
                                        par.reset(lo);
                                        ps = gallop_ptr(pp, pn, ps, lo);
                                        for (size_t k = ps; k < pn && static_cast<CorpusPos>(pp[k]) < hi; ++k)
                                            par.set(static_cast<CorpusPos>(pp[k]));
                                        const CorpusPos chunk_end = base + kChunk;
                                        for (; i < cn && static_cast<CorpusPos>(cp[i]) < chunk_end; ++i) {
                                            const CorpusPos c = static_cast<CorpusPos>(cp[i]);
                                            const int16_t d = hrel[c];
                                            cnt += static_cast<size_t>((d != 0) & par.test(c + d));
                                        }
                                        progress_tick(chunk_end, result.total_count + cnt, true);
                                    }
                                    break;
                                }
                            }
                        }
                        result.total_count += cnt;
                        if (max_total_cap > 0 && result.total_count > max_total_cap)
                            result.total_count = max_total_cap;
                    };
                    // ── P5.2 edge postings (dep_pair): [H=h] > [C=c] is one key of
                    // dep.pair.<H>.<C> — the child positions; total = key length
                    // (sliced per region interval), page = direct read + head().
                    RevSpan pair_span;
                    bool use_pair = false;
                    {
                        auto leaf_eq = [&](const ConditionPtr& c, std::string& attr,
                                           LexiconId& id) -> bool {
                            if (!c || !c->is_leaf) return false;
                            const AttrCondition& ac = c->leaf;
                            if (ac.op != CompOp::EQ || ac.case_insensitive
                                || ac.diacritics_insensitive || ac.is_nvals || ac.id_set_resolved)
                                return false;
                            attr = normalize_attr(ac.attr);
                            std::string feat_name;
                            if (feats_is_subkey(attr, feat_name) || !corpus_.has_attr(attr)
                                || corpus_.is_multivalue(attr))
                                return false;
                            id = corpus_.attr(attr).lexicon().lookup(ac.value);
                            return id != UNKNOWN_LEX;
                        };
                        std::string ha, ca;
                        LexiconId hid = UNKNOWN_LEX, cid = UNKNOWN_LEX;
                        const ConditionPtr& pc = q.tokens[parent_is_token0 ? 0 : 1].conditions;
                        const ConditionPtr& cc = q.tokens[parent_is_token0 ? 1 : 0].conditions;
                        if (fastpath_mode() == FastPathMode::On && !mask_bits
                            && leaf_eq(pc, ha, hid) && leaf_eq(cc, ca, cid)) {
                            if (auto dpi = dep_pair_index(ha, ca)) {
                                pair_span = dpi->span(hid, cid);
                                use_pair = true;
                            }
                        }
                    }
                    if (use_pair) {
                        result.plan_path = "dep_pair";
                        const RevSpan& S = pair_span;
                        // the edge postings are sliced per interval: the region
                        // filter's, or the P4.1 range (sentence-aligned by construction)
                        const bool pair_iv = mask_iv || slice_range;
                        std::vector<PosInterval> whole;
                        const std::vector<PosInterval>* ivs = &region_mask.iv;
                        if (!pair_iv) {
                            whole.push_back({0, corpus_end - 1});
                            ivs = &whole;
                        }
                        FlatRegionCursor bcur(sents);
                        bool counting = false, stop = false;
                        size_t lo = 0;
                        for (const auto& I : *ivs) {
                            if (S.empty()) break;
                            lo = gallop_rev(S, lo, I.s);
                            if (lo >= S.count) break;
                            const size_t hi = gallop_rev(S, lo, I.e + 1);
                            // Heads of the children in I lie in I when I starts and
                            // ends on sentence boundaries (heads share the sentence).
                            bool aligned = true;
                            if (pair_iv) {
                                const int64_t a = bcur.find(I.s);
                                const int64_t b = bcur.find(I.e);
                                aligned = (a < 0 || sent_r[a].start >= I.s)
                                       && (b < 0 || sent_r[b].end <= I.e);
                            }
                            for (size_t k = lo; k < hi; ++k) {
                                if (counting && aligned) {
                                    result.total_count += hi - k;
                                    break;
                                }
                                const CorpusPos c = S.at(k);
                                CorpusPos par;
                                if (!head_of(c, par)) continue;
                                if (pair_iv && !aligned && !region_mask.contains(par)) continue;
                                if (!emit(par, c)) { stop = true; break; }
                                if (can_fast_count && result.matches.size() >= max_matches)
                                    counting = true;
                            }
                            if (stop) break;
                            if (max_total_cap > 0 && result.total_count >= max_total_cap) {
                                result.total_count = max_total_cap;
                                break;
                            }
                            lo = hi;
                        }
                        if (max_total_cap > 0 && result.total_count > max_total_cap)
                            result.total_count = max_total_cap;
                    } else if (skewed) {
                        // ── Skewed operands (dep_probe): drive from the rare side ──
                        // Rare children: head(c), then gallop the parent postings (heads
                        // lie within ±D of c, so a monotone lower cursor at c - D keeps
                        // every search short). Rare parents: walk the child postings of
                        // the parent's sentence and keep those whose head is the parent.
                        // O(|rare| · log) — the big list is never streamed, and the
                        // operands are the already-resolved postings (no re-evaluation
                        // of materialised AND / regex / fold conditions).
                        const bool rare_child = child_span.count <= parent_span.count;
                        if (rare_child) {
                            const RevSpan& C = child_span;
                            const RevSpan& P = parent_span;
                            size_t plo = 0;
                            for (size_t i = 0; i < C.count; ++i) {
                                const CorpusPos c = C.at(i);
                                if (use_region_mask && !child_mask.contains(c)) {
                                    if (mask_iv && child_mask.exhausted()) break;
                                    continue;
                                }
                                CorpusPos h;
                                if (!head_of(c, h)) continue;
                                plo = gallop_rev(P, plo, c - kMaxHeadDistance);
                                const size_t j = gallop_rev(P, plo, h);
                                if (j >= P.count || P.at(j) != h) continue;
                                if (use_region_mask && !region_mask.contains(h)) continue;
                                if (!emit(h, c)) break;
                            }
                        } else {
                            const RevSpan& C = child_span;
                            const RevSpan& P = parent_span;
                            size_t clo = 0;
                            for (size_t i = 0; i < P.count && clo < C.count; ++i) {
                                const CorpusPos p = P.at(i);
                                if (use_region_mask && !parent_mask.contains(p)) {
                                    if (mask_iv && parent_mask.exhausted()) break;
                                    continue;
                                }
                                const int64_t si = sent_cur.find(p);
                                if (si < 0) continue;
                                const CorpusPos s0 = sent_r[si].start, s1 = sent_r[si].end;
                                clo = gallop_rev(C, clo, s0);
                                bool stop = false;
                                for (size_t k = clo; k < C.count; ++k) {
                                    const CorpusPos c = C.at(k);
                                    if (c > s1) break;
                                    CorpusPos h;
                                    if (hrel) {
                                        const int16_t d = hrel[c];
                                        if (d == 0) continue;
                                        h = c + d;
                                    } else {
                                        const int16_t l = hloc[c];
                                        if (l < 0) continue;
                                        h = s0 + l;
                                    }
                                    if (h != p) continue;
                                    if (use_region_mask && !region_mask.contains(c)) continue;
                                    if (!emit(p, c)) { stop = true; break; }
                                }
                                if (stop) break;
                            }
                        }
                        if (max_total_cap > 0 && result.total_count > max_total_cap)
                            result.total_count = max_total_cap;
                    } else if (child_span.width == parent_span.width) {
                        with_accs(child_span, parent_span, mask_iv || skewed_lists(child_span, parent_span), [&](const auto& c, const auto& p) {
                            run(c, child_span.count, p, parent_span.count);
                        });
                    } else {
                        std::vector<int64_t> c64(child_span.count), p64(parent_span.count);
                        for (size_t i = 0; i < child_span.count; ++i) c64[i] = child_span.at(i);
                        for (size_t i = 0; i < parent_span.count; ++i) p64[i] = parent_span.at(i);
                        run(c64.data(), c64.size(), p64.data(), p64.size());
                    }

                    return finish_query();
                }
            } else if (rt == RelationType::TRANS_GOVERNS || rt == RelationType::TRANS_GOV_BY) {
                // ── Transitive deps (P1.9): sentence-windowed Euler join ──
                // Both operands' postings are walked sentence by sentence (the rarer
                // list drives, the other is galloped to the sentence start); inside a
                // sentence every candidate pair is tested with the Euler interval
                // (in(a) < in(d) && out(a) > out(d)): O(|A| + |B| + pairs per sentence),
                // no subtree()/ancestors() vectors per seed.
                SeqMergeOperand left = merge_operand(q.tokens[0].conditions);
                SeqMergeOperand right = merge_operand(q.tokens[1].conditions);
                if (left.kind == SeqMergeTok::EqRev && right.kind == SeqMergeTok::EqRev
                    && !left.span.empty() && !right.span.empty()
                    && left.span.width == right.span.width) {
                    // TRANS_GOVERNS: token0 is an ancestor of token1; TRANS_GOV_BY: the reverse.
                    const bool anc_is_token0 = (rt == RelationType::TRANS_GOVERNS);
                    const bool driver_is_token0 = left.span.count <= right.span.count;
                    const RevSpan& D = driver_is_token0 ? left.span : right.span;
                    const RevSpan& O = driver_is_token0 ? right.span : left.span;
                    const bool driver_is_anc = (driver_is_token0 == anc_is_token0);

                    result.seed_token = driver_is_token0 ? 0 : 1;
                    result.plan_path = "dep_trans";
                    result.cardinalities = {
                        estimate_cardinality(q.tokens[0].conditions),
                        estimate_cardinality(q.tokens[1].conditions)};

                    std::string effective_within = q.within.empty()
                        ? corpus_.default_within() : q.within;
                    bool has_within = !effective_within.empty() &&
                                      corpus_.has_structure(effective_within);
                    const StructuralAttr* within_sa = has_within
                        ? &corpus_.structure(effective_within) : nullptr;
                    const bool within_span_semantics =
                        has_within && (corpus_.is_nested(effective_within) ||
                                       corpus_.is_overlapping(effective_within));

                    RegionPosMask region_mask =
                        build_start_mask();
                    mark_ranged(region_mask);
                    if (region_mask.status == RegionMaskStatus::Unsatisfiable) {
                        result.total_exact = true;
                        return result;
                    }
                    const bool use_region_mask =
                        region_mask.status == RegionMaskStatus::Ready;
                    const bool cheap_total_ok =
                        (q.global_region_filters.empty() || use_region_mask)
                && token_anchor_constraints.empty();   // anchors are applied in add_match

                    const auto& deps = corpus_.deps();
                    const StructuralAttr& sents = corpus_.structure("s");
                    const Region* sr = sents.region_data();
                    const size_t ns = sents.region_count();
                    const int16_t* ein = deps.euler_in_data();
                    const int16_t* eout = deps.euler_out_data();
                    const bool mask_iv = use_region_mask && !region_mask.use_bits;
                    MaskCursor dmask(region_mask);
                    auto allowed = [&](CorpusPos p) -> bool {   // any order
                        return !use_region_mask || region_mask.contains(p);
                    };
                    // Ancestor and descendant share the sentence the dep index is built
                    // on, so `within s` is implied.
                    const StructuralAttr* wsa =
                        (within_sa && !within_span_semantics && within_sa == &sents)
                            ? nullptr : within_sa;
                    int64_t within_hint = -1;
                    const bool can_fast_count = count_total && cheap_total_ok && !wsa
                        && max_matches > 0 && !agg_ptr && sample_size == 0;

                    // Returns false to stop the scan.
                    auto emit = [&](CorpusPos t0, CorpusPos t1) -> bool {
                        if (wsa) {
                            CorpusPos lo = t0 < t1 ? t0 : t1;
                            CorpusPos hi = t0 < t1 ? t1 : t0;
                            if (within_span_semantics) {
                                if (!within_span_in_some_region(*wsa, lo, hi)) return true;
                            } else {
                                int64_t rgn = wsa->find_region_from(lo, within_hint);
                                if (rgn < 0) return true;
                                within_hint = rgn;
                                if (hi > wsa->get(static_cast<size_t>(rgn)).end) return true;
                            }
                        }
                        if (max_matches > 0 && result.matches.size() >= max_matches) {
                            if (!count_total) return false;
                            if (cheap_total_ok) {
                                ++result.total_count;
                                progress_tick(-1, result.total_count);
                                return !reached_total_cap();
                            }
                        }
                        std::vector<CorpusPos> pm{t0, t1, t0, t1};
                        add_match(std::move(pm));
                        return !(reached_limit() || reached_total_cap());
                    };

                    size_t cnt = 0;           // hits counted after the page is full
                    auto run = [&](const auto& Dp, const auto& Op) {
                        const size_t nd = D.count, no = O.count;
                        const bool gallop_other = no > (nd << 3);
                        size_t id = 0, io = 0, si = 0;
                        if (mask_iv) {   // region filter / P4.1 range: start at the first interval
                            id = gallop_ptr(Dp, nd, 0, region_mask.iv.front().s);
                            io = gallop_ptr(Op, no, 0, region_mask.iv.front().s);
                        }
                        FlatRegionCursor scur(sents);
                        bool counting = false;
                        bool stop = false;
                        while (id < nd && !stop) {
                            const CorpusPos d0 = Dp[id];
                            if (mask_iv && !dmask.contains(d0)) {
                                // skip driver postings up to the next allowed interval
                                if (dmask.exhausted()) break;
                                id = gallop_ptr(Dp, nd, id, dmask.next_start());
                                continue;
                            }
                            // Sentence of d0 (driver positions ascend; galloping cursor).
                            {
                                const int64_t r = scur.find(d0);
                                if (r < 0) {
                                    if (scur.cur >= ns) break;
                                    ++id;
                                    continue;
                                }
                                si = static_cast<size_t>(r);
                            }
                            const CorpusPos st = sr[si].start, en = sr[si].end;
                            size_t jd = id;
                            while (jd < nd && Dp[jd] <= en) ++jd;
                            if (gallop_other) {
                                io = gallop_rev(O, io, st);  // RevSpan view of Op
                            } else {
                                while (io < no && Op[io] < st) ++io;
                            }
                            size_t jo = io;
                            while (jo < no && Op[jo] <= en) ++jo;
                            // Whole sentence inside the allowed interval → no per-position tests.
                            const bool whole = !use_region_mask
                                || (mask_iv && dmask.current().s <= st && en <= dmask.current().e);
                            for (size_t x = id; x < jd && !stop; ++x) {
                                const CorpusPos dp = Dp[x];
                                if (!whole && !allowed(dp)) continue;
                                const int16_t din = ein[dp], dout = eout[dp];
                                if (counting && whole) {
                                    // Branch-free count once the page is full.
                                    size_t c = 0;
                                    if (driver_is_anc) {
                                        for (size_t y = io; y < jo; ++y)
                                            c += (din < ein[Op[y]]) & (dout > eout[Op[y]]);
                                    } else {
                                        for (size_t y = io; y < jo; ++y)
                                            c += (ein[Op[y]] < din) & (eout[Op[y]] > dout);
                                    }
                                    cnt += c;
                                    progress_tick(dp, result.total_count + cnt);
                                    continue;
                                }
                                for (size_t y = io; y < jo; ++y) {
                                    const CorpusPos op = Op[y];
                                    const int16_t oin = ein[op], oout = eout[op];
                                    const bool related = driver_is_anc
                                        ? (din < oin && dout > oout)
                                        : (oin < din && oout > dout);
                                    if (!related) continue;
                                    if (!whole && !allowed(op)) continue;
                                    if (counting) { ++cnt; progress_tick(-1, result.total_count + cnt); continue; }
                                    const CorpusPos t0 = driver_is_token0 ? dp : op;
                                    const CorpusPos t1 = driver_is_token0 ? op : dp;
                                    if (!emit(t0, t1)) { stop = true; break; }
                                    if (can_fast_count && result.matches.size() >= max_matches)
                                        counting = true;
                                }
                            }
                            if (counting && max_total_cap > 0
                                && result.total_count + cnt >= max_total_cap)
                                break;
                            id = jd;
                            io = jo;
                        }
                    };
                    with_accs(D, O, mask_iv || skewed_lists(D, O), [&](const auto& d, const auto& o) { run(d, o); });
                    result.total_count += cnt;
                    if (max_total_cap > 0 && result.total_count > max_total_cap)
                        result.total_count = max_total_cap;

                    return finish_query();
                }
            } else if (rt == RelationType::NOT_GOVERNS || rt == RelationType::NOT_GOV_BY) {
                // ── Negated direct deps (P1.10) ──
                // Same semantics as the generic executor (wiki: `[upos="DET"] !< [lemma="book"]`
                // = *book* without a determiner): the kept token is the governor side,
                // the negated token its would-be dependent.
                //   [A] !> [B]: A tokens with no dependent in B  (match = A, B slot empty)
                //   [A] !< [B]: B tokens with no dependent in A  (match = B, A slot empty)
                // One bitset over heads(dependent side), one pass over the governor side.
                SeqMergeOperand left = merge_operand(q.tokens[0].conditions);
                SeqMergeOperand right = merge_operand(q.tokens[1].conditions);
                // A rare governor side is left to the generic path (seed from it,
                // check its children), which beats a bitset over all dependent heads.
                const bool neg_keep_token0 = (rt == RelationType::NOT_GOVERNS);
                const size_t neg_g = neg_keep_token0 ? left.span.count : right.span.count;
                const size_t neg_d = neg_keep_token0 ? right.span.count : left.span.count;
                if (left.kind == SeqMergeTok::EqRev && right.kind == SeqMergeTok::EqRev
                    && !left.span.empty() && !right.span.empty()
                    && neg_g * kDepBitsetMaxSkew >= neg_d) {
                    result.plan_path = "dep_not";
                    result.cardinalities = {
                        estimate_cardinality(q.tokens[0].conditions),
                        estimate_cardinality(q.tokens[1].conditions)};

                    std::string effective_within = q.within.empty()
                        ? corpus_.default_within() : q.within;
                    bool has_within = !effective_within.empty() &&
                                      corpus_.has_structure(effective_within);
                    const StructuralAttr* within_sa = has_within
                        ? &corpus_.structure(effective_within) : nullptr;
                    const bool within_span_semantics =
                        has_within && (corpus_.is_nested(effective_within) ||
                                       corpus_.is_overlapping(effective_within));

                    RegionPosMask region_mask =
                        build_start_mask();
                    mark_ranged(region_mask);
                    if (region_mask.status == RegionMaskStatus::Unsatisfiable) {
                        result.total_exact = true;
                        return result;
                    }
                    // P4.1: bare partition range → slice both lists, run unmasked
                    // (governor and dependents share a sentence inside the range)
                    const bool slice_range = region_mask.range_only;
                    const bool use_region_mask =
                        region_mask.status == RegionMaskStatus::Ready && !slice_range;
                    const bool cheap_total_ok =
                        (q.global_region_filters.empty() || use_region_mask)
                && token_anchor_constraints.empty();   // anchors are applied in add_match

                    const auto& deps = corpus_.deps();
                    const StructuralAttr& sents = corpus_.structure("s");
                    const Region* sent_r = sents.region_data();
                    const HeadRelView& hrel = deps.head_rel_data();
                    const int16_t* hloc = deps.head_local_data();
                    const CorpusPos corpus_end = corpus_.size();
                    const bool mask_bits = use_region_mask && region_mask.use_bits;
                    const bool mask_iv = use_region_mask && !region_mask.use_bits;
                    MaskCursor gmask(region_mask);
                    int64_t sent_hint = -1;
                    auto head_of = [&](CorpusPos p) -> CorpusPos {
                        if (hrel) {
                            const int16_t d = hrel[p];
                            return d ? p + d : NO_HEAD;
                        }
                        const int16_t l = hloc[p];
                        if (l < 0) return NO_HEAD;
                        const int64_t si = sents.find_region_from(p, sent_hint);
                        if (si < 0) return NO_HEAD;
                        sent_hint = si;
                        const CorpusPos h = sent_r[si].start + l;
                        return h < corpus_end ? h : NO_HEAD;
                    };

                    const bool keep_token0 = (rt == RelationType::NOT_GOVERNS);
                    RevSpan G = keep_token0 ? left.span : right.span;    // governor side
                    RevSpan Dd = keep_token0 ? right.span : left.span;   // dependent side
                    if (slice_range) {
                        const CorpusPos rlo = region_mask.iv.front().s;
                        const CorpusPos rhi = region_mask.iv.back().e + 1;
                        auto cut = [&](const RevSpan& sp) {
                            const size_t a = gallop_rev(sp, 0, rlo);
                            return rev_slice(sp, a, gallop_rev(sp, a, rhi));
                        };
                        G = cut(G);
                        Dd = cut(Dd);
                    }
                    result.seed_token = keep_token0 ? 0 : 1;

                    // Forbidden governors = heads of the dependent side, kept in a
                    // 32 KB window (P1.11) instead of a corpus-sized bitset: governors
                    // are processed in chunks of kWindow positions; a chunk's
                    // forbidden set comes from the dependents within ±D of it.
                    SlidingBitset forbid;

                    const StructuralAttr* wsa =
                        (within_sa && !within_span_semantics && within_sa == &sents)
                            ? nullptr : within_sa;
                    int64_t within_hint = -1;
                    const bool can_fast_count = count_total && cheap_total_ok && !wsa
                        && max_matches > 0 && !agg_ptr && sample_size == 0;
                    auto emit = [&](CorpusPos a) -> bool {
                        if (wsa) {
                            if (within_span_semantics) {
                                if (!within_span_in_some_region(*wsa, a, a)) return true;
                            } else {
                                int64_t rgn = wsa->find_region_from(a, within_hint);
                                if (rgn < 0) return true;
                                within_hint = rgn;
                            }
                        }
                        if (max_matches > 0 && result.matches.size() >= max_matches) {
                            if (!count_total) return false;
                            if (cheap_total_ok) {
                                ++result.total_count;
                                progress_tick(-1, result.total_count);
                                return !reached_total_cap();
                            }
                        }
                        std::vector<CorpusPos> pm = keep_token0
                            ? std::vector<CorpusPos>{a, NO_HEAD, a, NO_HEAD}
                            : std::vector<CorpusPos>{NO_HEAD, a, NO_HEAD, a};
                        add_match(std::move(pm));
                        return !(reached_limit() || reached_total_cap());
                    };
                    size_t cnt = 0;
                    auto run = [&](const auto& Gp, size_t gn, const auto& Dp, size_t dn) {
                        bool counting = false, stop = false;
                        size_t i = 0, ds = 0;
                        while (i < gn && !stop) {
                            const CorpusPos g0 = static_cast<CorpusPos>(Gp[i]);
                            if (mask_iv && !gmask.contains(g0)) {
                                if (gmask.exhausted()) break;
                                i = gallop_ptr(Gp, gn, i, gmask.next_start());
                                continue;
                            }
                            const CorpusPos base = g0;
                            const CorpusPos chunk_end = base + SlidingBitset::kWindow;
                            forbid.reset(base);
                            ds = gallop_ptr(Dp, dn, ds, base - kMaxHeadDistance);
                            const CorpusPos dend = chunk_end + kMaxHeadDistance;
                            for (size_t k = ds; k < dn && static_cast<CorpusPos>(Dp[k]) < dend; ++k) {
                                const CorpusPos h = head_of(static_cast<CorpusPos>(Dp[k]));
                                if (h >= base && h < chunk_end) forbid.set(h);   // NO_HEAD (-1) < base
                            }
                            if (counting && !use_region_mask) {
                                size_t c = 0;
                                for (; i < gn && static_cast<CorpusPos>(Gp[i]) < chunk_end; ++i)
                                    c += !forbid.test(static_cast<CorpusPos>(Gp[i]));
                                cnt += c;
                                progress_tick(chunk_end, result.total_count + cnt, true);
                                continue;
                            }
                            for (; i < gn; ++i) {
                                const CorpusPos g = static_cast<CorpusPos>(Gp[i]);
                                if (g >= chunk_end) break;
                                if (mask_iv && !gmask.contains(g)) break;   // outer round skips ahead
                                if (mask_bits && !bitset_test(region_mask.bits, g)) continue;
                                if (forbid.test(g)) continue;
                                if (counting) { ++cnt; progress_tick(-1, result.total_count + cnt); continue; }
                                if (!emit(g)) { stop = true; break; }
                                if (can_fast_count && result.matches.size() >= max_matches)
                                    counting = true;
                            }
                        }
                    };
                    if (G.width == Dd.width) {
                        with_accs(G, Dd, mask_iv || skewed_lists(G, Dd), [&](const auto& g, const auto& d) { run(g, G.count, d, Dd.count); });
                    } else {
                        std::vector<int64_t> g64(G.count), d64(Dd.count);
                        for (size_t k = 0; k < G.count; ++k) g64[k] = G.at(k);
                        for (size_t k = 0; k < Dd.count; ++k) d64[k] = Dd.at(k);
                        run(g64.data(), g64.size(), d64.data(), d64.size());
                    }
                    result.total_count += cnt;
                    if (max_total_cap > 0 && result.total_count > max_total_cap)
                        result.total_count = max_total_cap;

                    return finish_query();
                }
            }
        }
    }

    // ── Plan: pick seed by cardinality, expand outward ──────────────────

    QueryPlan plan = plan_query(q);
    result.seed_token = plan.seed;
    result.cardinalities = plan.cardinalities;
    result.plan_path = "generic";

    // #9: Use default_within from corpus when query does not specify within
    std::string effective_within = q.within.empty()
        ? corpus_.default_within() : q.within;
    bool has_within = !effective_within.empty() &&
                      corpus_.has_structure(effective_within);
    const StructuralAttr* within_sa = has_within
        ? &corpus_.structure(effective_within) : nullptr;
    const bool within_span_semantics =
        has_within && (corpus_.is_nested(effective_within) ||
                       corpus_.is_overlapping(effective_within));

    // Sequential path: process one seed at a time (lazy when possible)
    // #24: Skip seeds not contained in any within-region (cheap find_region check;
    // full span containment is enforced in expand_seed).
    // P4.1: a partition expands only its own seeds (every match has exactly one
    // seed position, so the ranges split the matches; seeds arrive ascending,
    // so the ranges' concatenated output keeps the single-threaded order).
    const CorpusPos seed_lo = range_ ? range_->lo : 0;
    const CorpusPos seed_hi = range_ ? range_->hi : std::numeric_limits<CorpusPos>::max();
    if (range_) result.range_ok = true;
    for_each_seed_position(q.tokens[plan.seed].conditions, [&](CorpusPos seed_p) {
        if (seed_p < seed_lo || seed_p >= seed_hi) return true;
        // #24: Early within rejection — seed must lie in *some* region (cheap scalar check).
        if (within_sa && within_sa->find_region(seed_p) < 0) return true;
        expand_seed(q, plan, within_sa, seed_p,
                      [&](std::vector<CorpusPos>&& pm) -> bool {
                          add_match(std::move(pm));
                          return !reached_limit() && !reached_total_cap();
                      },
                      within_span_semantics);
        return !reached_limit() && !reached_total_cap();
    });

    if (agg_ptr) {
        flush_flat_agg();
        result.matches.clear();
        result.total_count = agg_ptr->total_hits;
        result.total_exact = !agg_capped;
        result.aggregate_buckets = std::move(agg_storage);
        return result;
    }
    return finish_query();   // (takes the sample)
}

// ── P4.1: query-time range partitioning ─────────────────────────────────

namespace {

/// Smallest partition worth a thread (PANDO_PARTITION_MIN overrides, for tests
/// on small corpora).
CorpusPos partition_min_tokens() {
    static const CorpusPos v = [] {
        if (const char* e = std::getenv("PANDO_PARTITION_MIN")) {
            const long long x = std::atoll(e);
            if (x > 0) return static_cast<CorpusPos>(x);
        }
        return static_cast<CorpusPos>(1) << 20;
    }();
    return v;
}

/// Add one partition's buckets to another's. Positional keys are lexicon ids
/// (global); interned values (region attributes, transforms) get a partition-
/// local id each, so those columns are re-keyed through their strings.
void merge_aggregate_into(AggregateBucketData& dst, AggregateBucketData& src) {
    dst.total_hits += src.total_hits;
    dst.total_exact = dst.total_exact && src.total_exact;

    const size_t nc = std::max(dst.columns.size(), src.region_intern.size());
    if (dst.region_intern.size() < nc) dst.region_intern.resize(nc);
    std::vector<std::vector<int64_t>> remap(src.region_intern.size());
    bool any_remap = false;
    for (size_t i = 0; i < src.region_intern.size(); ++i) {
        const auto& from = src.region_intern[i];
        if (from.id_to_str.empty()) continue;
        auto& to = dst.region_intern[i];
        remap[i].assign(from.id_to_str.size() + 1, 0);
        for (size_t j = 0; j < from.id_to_str.size(); ++j) {
            const std::string& v = from.id_to_str[j];
            auto it = to.str_to_id.find(v);
            int64_t id;
            if (it != to.str_to_id.end()) {
                id = it->second;
            } else {
                id = static_cast<int64_t>(to.id_to_str.size() + 1);
                to.str_to_id.emplace(v, id);
                to.id_to_str.push_back(v);
            }
            remap[i][j + 1] = id;
        }
        any_remap = true;
    }
    for (auto& [k, c] : src.counts) {
        if (!any_remap) {
            dst.counts[k] += c;
            continue;
        }
        std::vector<int64_t> key = k;
        for (size_t i = 0; i < key.size() && i < remap.size(); ++i)
            if (!remap[i].empty() && key[i] > 0 && static_cast<size_t>(key[i]) < remap[i].size())
                key[i] = remap[i][static_cast<size_t>(key[i])];
        dst.counts[std::move(key)] += c;
    }

    // flat counters (P7.2): same plan in every partition, so same ncols / v2
    if (src.flat_ncols == 0) return;
    if (dst.flat_ncols == 0) {
        dst.flat_ncols = src.flat_ncols;
        dst.flat_v2 = src.flat_v2;
    }
    const bool dense = !dst.flat_dense.empty() || !src.flat_dense.empty();
    if (dense) {
        // one column: everything into one dense array (keys are lexicon ids)
        auto& d = dst.flat_dense;
        if (d.size() < src.flat_dense.size()) d.resize(src.flat_dense.size(), 0);
        for (size_t i = 0; i < src.flat_dense.size(); ++i) d[i] += src.flat_dense[i];
        auto fold = [&](const std::vector<uint64_t>& keys, const std::vector<uint64_t>& vals) {
            for (size_t i = 0; i < keys.size(); ++i) {
                if (keys[i] == AggregateBucketData::kFlatEmpty) continue;
                if (keys[i] >= d.size()) d.resize(static_cast<size_t>(keys[i]) + 1, 0);
                d[static_cast<size_t>(keys[i])] += vals[i];
            }
        };
        fold(dst.flat_keys, dst.flat_vals);
        fold(src.flat_keys, src.flat_vals);
        dst.flat_keys.clear();
        dst.flat_vals.clear();
    } else {
        // packed keys: sort both tables' live entries and add equal keys
        std::vector<std::pair<uint64_t, uint64_t>> kv;
        kv.reserve(dst.flat_keys.size() + src.flat_keys.size());
        for (int side = 0; side < 2; ++side) {
            const auto& keys = side ? src.flat_keys : dst.flat_keys;
            const auto& vals = side ? src.flat_vals : dst.flat_vals;
            for (size_t i = 0; i < keys.size(); ++i)
                if (keys[i] != AggregateBucketData::kFlatEmpty) kv.emplace_back(keys[i], vals[i]);
        }
        std::sort(kv.begin(), kv.end());
        dst.flat_keys.clear();
        dst.flat_vals.clear();
        for (const auto& [k, v] : kv) {
            if (!dst.flat_keys.empty() && dst.flat_keys.back() == k) dst.flat_vals.back() += v;
            else { dst.flat_keys.push_back(k); dst.flat_vals.push_back(v); }
        }
    }
}

}  // namespace

// A bare "s" (or /re/) token is [form="s" | contr_form="s"]+: one hit per run over
// the tokens of a contraction. A corpus without contr_form has no contractions:
// it is the one-token [form="s"], as in CQP ("the the" was one hit, and the
// repeated OR took the generic path: 3 s instead of a posting-list count).
static bool simplify_bare_strings(const Corpus& corpus, const TokenQuery& q, TokenQuery& out) {
    bool any = false;
    for (const auto& t : q.tokens) any |= t.bare_string;
    if (!any || corpus.has_attr("contr_form")) return false;
    out = q;
    for (auto& t : out.tokens) {
        if (!t.bare_string) continue;
        t.bare_string = false;
        if (t.conditions && !t.conditions->is_leaf && t.conditions->left) t.conditions = t.conditions->left;
        if (!t.bare_repeat_given) t.min_repeat = t.max_repeat = 1;
    }
    return true;
}

// Head attributes (head#A, HeadAttr): [P] > [C] (or [C] < [P]) is the one-token
// [C & P'] on the dependent, P' = P with every attribute A read as head#A — so the
// one-token fast paths (bitmaps, posting merges, counts) answer it. Only when every
// leaf of P has a head attribute and nothing else refers to the two tokens apart
// (labels in aggregates, function / alignment filters, …). PANDO_HEADATTR=off
// (or PANDO_FASTPATH other than on) keeps the dependency paths.
static bool head_attrs_enabled() {
    static const bool on = [] {
        const char* v = std::getenv("PANDO_HEADATTR");
        return !(v && (std::string(v) == "off" || std::string(v) == "0"));
    }();
    return on && fastpath_mode() == FastPathMode::On;
}

// A region attribute (`text_lang`) of s / text / doc / the default `within`: equal
// for a token and its head (same sentence).
static bool sentence_region_attr(const Corpus& corpus, const std::string& a) {
    if (corpus.has_attr(a)) return false;
    for (const auto& ra : corpus.region_attr_names()) {
        if (ra != a) continue;
        const std::string sn = ra.substr(0, ra.find('_'));
        return (sn == "s" || sn == "text" || sn == "doc" || sn == corpus.default_within())
            && corpus.has_structure(sn) && !corpus.is_nested(sn) && !corpus.is_overlapping(sn);
    }
    return false;
}

static ConditionPtr head_condition(const Corpus& corpus, const ConditionPtr& c, bool& ok) {
    if (!c || !ok) return nullptr;
    if (c->is_leaf) {
        const AttrCondition& ac = c->leaf;
        const std::string a = normalize_query_attr_name(corpus, ac.attr);
        const std::string h = HeadAttr::name_for(a);
        // a region attribute of a structure that holds whole sentences: the head
        // (same sentence) has the dependent's value, so the leaf stays as it is
        if (ac.op != CompOp::IN && !ac.is_nvals && sentence_region_attr(corpus, a)) return c;
        if (ac.op == CompOp::IN || ac.is_nvals || !HeadAttr::source_of(a).empty() || !corpus.has_attr(a)
            || corpus.is_multivalue(a) || !corpus.has_attr(h)) {
            ok = false;
            return nullptr;
        }
        AttrCondition m;   // the query's text of the leaf, read on head#A (ids are resolved again)
        m.attr = h;
        m.op = ac.op;
        m.value = ac.value;
        m.case_insensitive = ac.case_insensitive;
        m.diacritics_insensitive = ac.diacritics_insensitive;
        m.regex_full_match = ac.regex_full_match;
        m.neq_regex = ac.neq_regex;
        return ConditionNode::make_leaf(std::move(m));
    }
    if (c->is_structural || c->is_count || !c->left || !c->right) {
        ok = false;
        return nullptr;
    }
    auto l = head_condition(corpus, c->left, ok);
    auto r = head_condition(corpus, c->right, ok);
    return ok ? ConditionNode::make_branch(c->bool_op, std::move(l), std::move(r)) : nullptr;
}

// False when the condition cannot hold on a token without a head (head#A = kNoHead
// for every A): it requires a literal value other than kNoHead.
static bool may_match_no_head(const ConditionPtr& c) {
    if (!c) return true;
    if (c->is_leaf) {
        const AttrCondition& ac = c->leaf;
        if (ac.op != CompOp::EQ) return true;
        std::string v = ac.value, n = HeadAttr::kNoHead;
        if (ac.case_insensitive) {
            for (auto& ch : v) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            for (auto& ch : n) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        return v == n || ac.diacritics_insensitive;
    }
    if (c->bool_op == BoolOp::AND) return may_match_no_head(c->left) && may_match_no_head(c->right);
    return may_match_no_head(c->left) || may_match_no_head(c->right);
}

// `count by` fields of [P] > [C] for the rewritten one-token query: C.X → X,
// P.X → head#X, region attributes of sentence-holding structures as they are. An
// unlabeled positional field (read at the match start, min(P, C)) has no
// equivalent: false.
static bool head_attr_fields(const Corpus& corpus, const std::vector<std::string>& in,
                             const std::string& pname, const std::string& cname,
                             std::vector<std::string>& out) {
    out.clear();
    for (const auto& f : in) {
        if (f.find('(') != std::string::npos) return false;
        std::string label, rest = f;
        if (const size_t dot = f.find('.'); dot != std::string::npos) {
            label = f.substr(0, dot);
            rest = f.substr(dot + 1);
            if (label.empty() || (label != pname && label != cname)) return false;
        }
        if (sentence_region_attr(corpus, rest)) {
            out.push_back(rest);
            continue;
        }
        if (label.empty()) return false;
        if (label == cname) {
            out.push_back(rest);
            continue;
        }
        const std::string a = normalize_query_attr_name(corpus, rest);
        if (!corpus.has_attr(a) || !corpus.has_attr(HeadAttr::name_for(a))) return false;
        out.push_back(HeadAttr::name_for(a));
    }
    return true;
}

// `parent [P]` in a token's condition (not negated, unnamed, not nested in another
// restriction) is head#P' & has-head on the token itself: an ordinary condition,
// so it works in any query, any token.
// Returns `c` itself when nothing changed.
static ConditionPtr rewrite_parent_restrictions(const Corpus& corpus, const ConditionPtr& c,
                                                bool& changed) {
    if (!c || c->is_leaf || c->is_count) return c;
    if (c->is_structural) {
        if (c->struct_rel == StructRelType::PARENT && !c->struct_negated && c->nested_name.empty()) {
            bool ok = true;
            ConditionPtr hp = head_condition(corpus, c->nested_conditions, ok);
            if (ok && may_match_no_head(c->nested_conditions)) {
                if (corpus.head_attr_names().empty()) return c;
                AttrCondition g;
                g.attr = corpus.head_attr_names().front();
                g.op = CompOp::NEQ;
                g.value = HeadAttr::kNoHead;
                auto gn = ConditionNode::make_leaf(std::move(g));
                hp = hp ? ConditionNode::make_branch(BoolOp::AND, std::move(hp), std::move(gn)) : std::move(gn);
            }
            if (ok && hp) {
                changed = true;
                return hp;
            }
            return c;
        }
        // not inside other restrictions (child [… parent […]]): their nested
        // conditions are checked per related token, where the leaf costs more
        return c;
    }
    bool l = false, r = false;
    auto nl = rewrite_parent_restrictions(corpus, c->left, l);
    auto nr = rewrite_parent_restrictions(corpus, c->right, r);
    if (!l && !r) return c;
    changed = true;
    return ConditionNode::make_branch(c->bool_op, std::move(nl), std::move(nr));
}

static bool rewrite_parent_query(const Corpus& corpus, const TokenQuery& q, TokenQuery& out) {
    if (corpus.head_attr_names().empty()) return false;
    bool any = false;
    std::vector<ConditionPtr> conds;
    for (const auto& t : q.tokens) {
        bool ch = false;
        conds.push_back(rewrite_parent_restrictions(corpus, t.conditions, ch));
        any |= ch;
    }
    if (!any) return false;
    out = q;
    for (size_t i = 0; i < out.tokens.size(); ++i) out.tokens[i].conditions = conds[i];
    return true;
}

static bool rewrite_head_attrs(const Corpus& corpus, const TokenQuery& q, TokenQuery& out,
                               bool& parent_first) {
    if (q.tokens.size() != 2 || q.relations.size() != 1) return false;
    const RelationType rt = q.relations[0].type;
    if (rt != RelationType::GOVERNS && rt != RelationType::GOVERNED_BY) return false;
    if (!corpus.has_deps() || !corpus.deps().head_rel_data()) return false;
    for (const auto& t : q.tokens)
        if (t.has_repetition() || t.is_anchor() || t.is_dep_subtree || t.bare_string || !t.where_refs.empty())
            return false;
    if (q.not_within || q.within_having || !q.containing_clauses.empty()
        || !q.global_alignment_filters.empty() || !q.global_function_filters.empty()
        || !q.position_orders.empty())
        return false;
    parent_first = rt == RelationType::GOVERNS;
    const QueryToken& P = q.tokens[parent_first ? 0 : 1];
    const QueryToken& C = q.tokens[parent_first ? 1 : 0];
    if (!P.name.empty() && P.name == C.name) return false;
    bool ok = true;
    ConditionPtr hp = head_condition(corpus, P.conditions, ok);
    if (!ok) return false;
    if (may_match_no_head(P.conditions)) {
        std::string guard;   // any head attribute: head#A != kNoHead = "has a head"
        for (const auto& a : corpus.head_attr_names()) { guard = a; break; }
        if (guard.empty()) return false;
        AttrCondition g;
        g.attr = guard;
        g.op = CompOp::NEQ;
        g.value = HeadAttr::kNoHead;
        auto gn = ConditionNode::make_leaf(std::move(g));
        hp = hp ? ConditionNode::make_branch(BoolOp::AND, std::move(hp), std::move(gn)) : std::move(gn);
    }
    out = TokenQuery{};
    QueryToken t = C;
    t.conditions = t.conditions ? ConditionNode::make_branch(BoolOp::AND, t.conditions, hp) : hp;
    out.tokens.push_back(std::move(t));
    out.within = q.within;
    // region filters: the head shares the dependent's sentence (as on the dep paths)
    for (auto f : q.global_region_filters) {
        if (!f.anchor_name.empty()) {
            if (f.anchor_name != P.name && f.anchor_name != C.name) return false;
            f.anchor_name = C.name;
        }
        out.global_region_filters.push_back(std::move(f));
    }
    return true;
}

MatchSet QueryExecutor::execute(const TokenQuery& query,
                                size_t max_matches,
                                bool count_total,
                                size_t max_total_cap,
                                size_t sample_size,
                                uint32_t random_seed,
                                unsigned num_threads,
                                const std::vector<std::string>* aggregate_by_fields,
                                bool skip_name_validation) {
    if (TokenQuery simple; simplify_bare_strings(corpus_, query, simple))
        return execute(simple, max_matches, count_total, max_total_cap, sample_size, random_seed, num_threads,
                       aggregate_by_fields, skip_name_validation);
    if (head_attrs_enabled())
        if (TokenQuery pq; rewrite_parent_query(corpus_, query, pq))
            return execute(pq, max_matches, count_total, max_total_cap, sample_size, random_seed,
                           num_threads, aggregate_by_fields, skip_name_validation);
    if (head_attrs_enabled() && !hit_sink_) {
        TokenQuery one;
        bool parent_first = true;
        std::vector<std::string> fields;
        const bool agg = aggregate_by_fields && !aggregate_by_fields->empty();
        if (rewrite_head_attrs(corpus_, query, one, parent_first)
            && (!agg || head_attr_fields(corpus_, *aggregate_by_fields,
                                         query.tokens[parent_first ? 0 : 1].name,
                                         query.tokens[parent_first ? 1 : 0].name, fields))) {
            MatchSet r = execute(one, max_matches, count_total, max_total_cap, sample_size, random_seed,
                                 num_threads, agg ? &fields : nullptr, skip_name_validation);
            const HeadRelView& hrel = corpus_.deps().head_rel_data();
            for (auto& m : r.matches) {
                const CorpusPos c = m.positions[0];
                const CorpusPos h = c + hrel[c];
                m.positions = parent_first ? PosVec{h, c} : PosVec{c, h};
                m.span_ends = m.positions;
            }
            r.num_tokens = 2;
            r.seed_token = parent_first ? 1 : 0;
            r.plan_path = "head_attr:" + r.plan_path;
            return r;
        }
    }
    // Materialised merge operands (regex / %c / AND postings) are kept for the
    // whole query: several fast paths are tried in turn and each asked for the
    // same operands (a two-regex sequence built each list twice), and the ranges
    // of a partitioned query share them. Cleared when the top-level query ends.
    struct OperandScope {
        Caches* c = nullptr;
        explicit OperandScope(Caches& cc, bool top) {
            if (top && !cc.share_operands) { c = &cc; c->share_operands = true; }
        }
        ~OperandScope() {
            if (!c) return;
            c->share_operands = false;
            std::lock_guard<std::mutex> lk(c->operands_mu);
            c->operands.clear();
        }
    } operand_scope(*caches_, !range_);
    if (!range_ && aggregate_by_fields && !aggregate_by_fields->empty())
        check_aggregate_labels(corpus_, query, *aggregate_by_fields);
    if (!range_ && !windowed() && max_matches > 0 && !count_total && sample_size == 0
        && !(aggregate_by_fields && !aggregate_by_fields->empty())) {
        if (auto r = execute_progressive_page(query, max_matches, random_seed, skip_name_validation))
            return std::move(*r);
    }
    if (num_threads > 1 && !range_ && sample_size == 0 && !hit_sink_) {
        if (auto r = execute_partitioned(query, max_matches, count_total, max_total_cap,
                                         random_seed, num_threads, aggregate_by_fields,
                                         skip_name_validation))
            return std::move(*r);
    }
    return execute_impl(query, max_matches, count_total, max_total_cap, sample_size,
                        random_seed, num_threads, aggregate_by_fields, skip_name_validation);
}

std::optional<MatchSet> QueryExecutor::execute_partitioned(
        const TokenQuery& query, size_t max_matches, bool count_total, size_t max_total_cap,
        uint32_t random_seed, unsigned num_threads,
        const std::vector<std::string>* aggregate_by_fields, bool skip_name_validation) {
    const bool agg = aggregate_by_fields && !aggregate_by_fields->empty();
    // A page without a total stops at its first hits: nothing to split. Capped
    // buckets would depend on which hits came first.
    if (!count_total && !agg) return std::nullopt;
    if (agg && max_total_cap > 0) return std::nullopt;
    const CorpusPos N = corpus_.size();
    const CorpusPos min_part = partition_min_tokens();
    if (N / min_part < 2) return std::nullopt;
    const unsigned K = static_cast<unsigned>(
        std::min<CorpusPos>(static_cast<CorpusPos>(num_threads), N / min_part));
    if (K < 2) return std::nullopt;
    // P4.1d: more ranges than threads — up to PANDO_RANGES_PER_THREAD (default 4)
    // per thread, but none below PANDO_RANGE_TARGET tokens (default 2^23: each
    // range has a fixed cost, and its `count by` buckets are merged) — taken in
    // turn by whichever thread is free: a dense range no longer holds up the
    // others, and pool workers that free up later still find work.
    static const unsigned per_thread = [] {
        const char* e = std::getenv("PANDO_RANGES_PER_THREAD");
        const int v = e && *e ? std::atoi(e) : 0;
        return v > 0 ? static_cast<unsigned>(v) : 4u;
    }();
    static const CorpusPos range_target = [] {
        const char* e = std::getenv("PANDO_RANGE_TARGET");
        const long long v = e && *e ? std::atoll(e) : 0;
        return v > 0 ? static_cast<CorpusPos>(v) : CorpusPos{1} << 23;
    }();
    const CorpusPos by_size = std::max<CorpusPos>(min_part, range_target);
    const unsigned R = static_cast<unsigned>(std::clamp<CorpusPos>(
        N / by_size, static_cast<CorpusPos>(K), static_cast<CorpusPos>(K) * per_thread));

    if (!skip_name_validation) validate_query_name_bindings(query);
    // Workers skip compile_query (it writes into the shared condition nodes):
    // compile once here. Anchor stripping copies the tokens, not the nodes.
    compile_query(query);

    // Cut points at bitmap-chunk boundaries moved forward to the next sentence
    // start: a dependency tree never crosses a cut, and the bitmap kernels see
    // whole chunks except at the two ends of a range.
    const StructuralAttr* sents = corpus_.has_structure("s") ? &corpus_.structure("s") : nullptr;
    const bool chunk_align = min_part >= BitmapIndex::kChunk;   // (tests use tiny ranges)
    auto align = [&](CorpusPos x) -> CorpusPos {
        if (chunk_align)
            x = ((x + BitmapIndex::kChunk - 1) >> BitmapIndex::kChunkShift) << BitmapIndex::kChunkShift;
        if (x >= N) return N;
        if (sents) {
            const int64_t r = sents->find_region(x);
            if (r >= 0) {
                const Region reg = sents->get(static_cast<size_t>(r));
                if (reg.start < x) x = reg.end + 1;
            }
        }
        return std::min(x, N);
    };
    // Range 0 is a short probe run first, alone: its result says whether the
    // query's path honours ranges at all. If not, the probe never saw its range
    // and already holds the full answer — no work is wasted either way.
    std::vector<PosRange> ranges;
    CorpusPos prev = 0;
    const CorpusPos probe_end = align(N / (8 * static_cast<CorpusPos>(K)));
    if (probe_end <= 0 || probe_end >= N) return std::nullopt;
    ranges.push_back({0, probe_end});
    prev = probe_end;
    for (unsigned w = 1; w <= R; ++w) {
        const CorpusPos target = probe_end + (N - probe_end) * static_cast<CorpusPos>(w) / R;
        const CorpusPos cut = (w == R) ? N : align(target);
        if (cut > prev) {
            ranges.push_back({prev, cut});
            prev = cut;
        }
    }
    if (ranges.back().hi < N) ranges.back().hi = N;

    // materialised operands are shared by the ranges while this query runs
    // (execute()'s OperandScope)

    auto run_range = [&](const PosRange& r, ExecProgress* prog) {
        QueryExecutor ex(*this, &r, prog);
        return ex.execute_impl(query, max_matches, count_total, max_total_cap, 0, random_seed,
                               1, aggregate_by_fields, true);
    };

    MatchSet result = run_range(ranges[0], progress_);
    if (!result.range_ok) return result;              // the path ignored the range
    if (ranges.size() == 1) return result;

    const size_t nw = ranges.size() - 1;
    std::vector<std::unique_ptr<ExecProgress>> wprog(nw);
    std::vector<std::optional<MatchSet>> parts(nw);
    std::vector<std::exception_ptr> errs(nw);
    // Publish the partitions' progress as one scan: counted = sum; scanned = the
    // number of positions covered so far, as a position in one sequential scan.
    // Called at the workers' checkpoints (on_tick) and after each range.
    const size_t probe_total = result.total_count;
    std::mutex publish_mu;
    auto publish_locked = [&] {
        size_t counted = probe_total;
        int64_t covered = static_cast<int64_t>(ranges[0].hi);
        for (size_t w = 0; w < nw; ++w) {
            counted += wprog[w]->counted.load(std::memory_order_relaxed);
            const int64_t sc = wprog[w]->scanned.load(std::memory_order_relaxed);
            const PosRange& r = ranges[w + 1];
            if (sc >= r.lo) covered += std::min<int64_t>(sc, r.hi - 1) - r.lo + 1;
        }
        progress_->counted.store(counted, std::memory_order_relaxed);
        if (covered - 1 > progress_->scanned.load(std::memory_order_relaxed))
            progress_->scanned.store(covered - 1, std::memory_order_relaxed);
    };
    const std::function<void()> tick = [&] {
        std::unique_lock<std::mutex> lk(publish_mu, std::try_to_lock);
        if (lk.owns_lock()) publish_locked();
    };
    for (auto& p : wprog) {
        p = std::make_unique<ExecProgress>();
        p->parent = progress_;                    // the query's cancel / time limit
        p->on_tick = progress_ ? &tick : nullptr;
    }
    // The calling thread runs ranges itself and borrows idle pool workers (at
    // most K - 1); under load it gets none and goes through the ranges alone.
    WorkerPool::global().run(nw, K - 1, [&](size_t w) {
        if (wprog[w]->cancelled()) {
            errs[w] = std::make_exception_ptr(QueryCancelled());
            return;
        }
        try {
            parts[w] = run_range(ranges[w + 1], wprog[w].get());
        } catch (...) {
            errs[w] = std::current_exception();
            for (auto& p : wprog) p->cancel.store(true, std::memory_order_relaxed);
        }
        if (progress_) tick();
    });
    if (progress_) {
        std::lock_guard<std::mutex> lk(publish_mu);
        publish_locked();
    }

    // errors: a real one first; otherwise the cancellation
    std::exception_ptr cancelled;
    for (auto& e : errs) {
        if (!e) continue;
        try {
            std::rethrow_exception(e);
        } catch (const QueryCancelled&) {
            cancelled = e;
        } catch (...) {
            throw;
        }
    }
    if (cancelled) std::rethrow_exception(cancelled);
    if (progress_ && progress_->cancel.load()) throw QueryCancelled();

    for (size_t w = 0; w < nw; ++w) {
        MatchSet& p = *parts[w];
        if (!p.range_ok) {
            // a range took a different path than the probe (should not happen);
            // be safe: the plain single-threaded run
            return execute_impl(query, max_matches, count_total, max_total_cap, 0, random_seed,
                                1, aggregate_by_fields, true);
        }
        result.total_count += p.total_count;
        result.total_exact = result.total_exact && p.total_exact;
        for (auto& m : p.matches) {
            if (max_matches > 0 && result.matches.size() >= max_matches) break;
            result.matches.push_back(std::move(m));
        }
        if (p.aggregate_buckets) {
            if (!result.aggregate_buckets) result.aggregate_buckets = std::move(p.aggregate_buckets);
            else merge_aggregate_into(*result.aggregate_buckets, *p.aggregate_buckets);
        }
    }
    if (result.aggregate_buckets) {
        result.total_count = result.aggregate_buckets->total_hits;
        result.total_exact = result.total_exact && result.aggregate_buckets->total_exact;
    }
    if (max_total_cap > 0 && result.total_count >= max_total_cap) {
        // as the single-threaded count, which stops (inexact) on reaching the cap
        result.total_count = max_total_cap;
        result.total_exact = false;
    }
    result.partitions = static_cast<unsigned>(ranges.size());
    result.range_ok = false;
    return result;
}

// ── P6.5e: a page found range by range ──────────────────────────────────

namespace {

CorpusPos env_tokens(const char* name, CorpusPos dflt) {
    if (const char* e = std::getenv(name)) {
        const long long x = std::atoll(e);
        if (x > 0) return static_cast<CorpusPos>(x);
    }
    return dflt;
}

}  // namespace

/// A page (first hits, no total) of a query with a huge complex operand — a
/// regex matching millions of tokens, `[word=".*a.*"] [word=".*e.*"]` — would
/// otherwise build that operand for the whole corpus before the first hit. Here
/// the corpus is searched in growing sentence-aligned ranges (64K tokens, then
/// 256K, 1M, …), each range materialising only its own part of the operands
/// (the operand window: the range plus the longest match), until the page is
/// full. The hits of one range are those starting in it, in corpus order, so
/// the concatenation is the page of a plain run. A path that does not honour
/// ranges (range_ok unset) → nullopt: the plain run.
std::optional<MatchSet> QueryExecutor::execute_progressive_page(
        const TokenQuery& query, size_t max_matches, uint32_t random_seed, bool skip_name_validation) {
    if (fastpath_mode() != FastPathMode::On) return std::nullopt;
    const CorpusPos N = corpus_.size();
    static const CorpusPos first = env_tokens("PANDO_PROGRESSIVE_WINDOW", static_cast<CorpusPos>(1) << 16);
    static const CorpusPos min_card = env_tokens("PANDO_PROGRESSIVE_MIN", static_cast<CorpusPos>(1) << 20);
    if (N < 4 * first) return std::nullopt;
    // post-filters and cross-hit constraints see all hits: the plain run
    if (query.within_having || query.not_within || !query.containing_clauses.empty()
        || !query.position_orders.empty() || !query.global_alignment_filters.empty()
        || !query.global_function_filters.empty() || query.tokens.empty())
        return std::nullopt;
    if (!query.within.empty() && corpus_.is_token_group(query.within)) return std::nullopt;

    // Sequences only: the dependency paths order their hits by the seed token
    // (dep_bitset by the dependent, dep_probe by the head) and a range picks its
    // path by the range's cardinalities, so the ranges' pages would not add up to
    // the plain run's page (nor its later pages).
    for (const auto& r : query.relations)
        if (r.type != RelationType::SEQUENCE) return std::nullopt;
    // An optional first token: some paths order by the first mandatory token
    // (seq_gap), others by the match start (seq_gap_bitmap).
    for (const auto& t : query.tokens) {
        if (t.is_anchor()) continue;
        if (t.min_repeat < 1) return std::nullopt;
        break;
    }
    // the longest match: the window padding
    const bool sents = corpus_.has_structure("s");
    CorpusPos span = 1;
    for (const auto& t : query.tokens) {
        if (t.max_repeat >= REPEAT_UNBOUNDED || t.is_dep_subtree) return std::nullopt;
        span += std::max(1, t.max_repeat);
    }
    const CorpusPos pad = span;

    if (!skip_name_validation) validate_query_name_bindings(query);
    // once: workers skip it (it writes into the shared nodes); a fallback to the
    // plain run finds the id sets resolved
    compile_query(query);

    // Worth it when some operand is a huge complex list (a regex over millions of
    // tokens) and no token is rare: a rare token seeds the plain run, which is
    // then fast, while the ranges would scan the corpus for a page that never fills.
    bool huge = false;
    size_t rarest = std::numeric_limits<size_t>::max();
    for (const auto& t : query.tokens) {
        if (t.is_anchor() || !t.conditions) continue;
        const size_t est = estimate_cardinality(t.conditions);
        if (t.min_repeat >= 1) rarest = std::min(rarest, est);
        if (est >= static_cast<size_t>(min_card)
            && seq_merge_operand(corpus_, t.conditions).kind == SeqMergeTok::Complex)
            huge = true;
    }
    if (!huge || rarest < static_cast<size_t>(N / 64)) return std::nullopt;

    const StructuralAttr* sa = sents ? &corpus_.structure("s") : nullptr;
    auto align = [&](CorpusPos x) -> CorpusPos {
        if (x >= N) return N;
        if (sa) {
            const int64_t r = sa->find_region(x);
            if (r >= 0) {
                const Region reg = sa->get(static_cast<size_t>(r));
                if (reg.start < x) x = reg.end + 1;
            }
        }
        return std::min(x, N);
    };

    MatchSet result;
    bool filled = false;
    unsigned nranges = 0;
    std::unordered_map<const void*, std::vector<uint64_t>> id_bits;
    CorpusPos lo = 0, size = first;
    while (lo < N) {
        CorpusPos hi = (N - lo < 2 * size) ? N : align(lo + size);
        if (hi <= lo) hi = N;
        const PosRange r{lo, hi};
        QueryExecutor w(*this, &r, progress_);
        w.operand_window_ = {lo > pad ? lo - pad : 0, std::min(N, hi + pad)};
        w.window_id_bits_ = &id_bits;
        MatchSet part = w.execute_impl(query, max_matches - result.matches.size(), false, 0, 0,
                                       random_seed, 1, nullptr, true);
        if (!part.range_ok) return std::nullopt;   // the path ignored the range
        if (nranges++ == 0) {
            result.num_tokens = part.num_tokens;
            result.seed_token = part.seed_token;
            result.cardinalities = part.cardinalities;
            result.plan_path = part.plan_path;
        }
        for (auto& m : part.matches) {
            if (result.matches.size() >= max_matches) break;
            result.matches.push_back(std::move(m));
        }
        if (result.matches.size() >= max_matches) {
            filled = true;
            break;
        }
        lo = hi;
        size *= 4;
    }
    result.total_count = result.matches.size();
    result.total_exact = !filled;
    result.partitions = nranges;
    result.range_ok = false;
    return result;
}

// ── Shared seed expansion (single source of truth for match logic) ───────

void QueryExecutor::expand_seed(const TokenQuery& query,
                                const QueryPlan& plan,
                                const StructuralAttr* within_sa,
                                CorpusPos seed_p,
                                std::function<bool(std::vector<CorpusPos>&&)> emit,
                                bool within_span_semantics) const {
    size_t n = query.tokens.size();
    int seed_min_rep = std::max(query.tokens[plan.seed].min_repeat, 1); // seed_len=0 is nonsensical
    int seed_max_rep = query.tokens[plan.seed].max_repeat;

    for (int seed_len = seed_min_rep; seed_len <= seed_max_rep; ++seed_len) {
        // Validate the seed span: positions seed_p..seed_p+seed_len-1 must all match
        if (seed_len > 1) {
            CorpusPos end_p = seed_p + seed_len - 1;
            if (end_p >= corpus_.size()) break;
            bool valid = true;
            for (CorpusPos q = seed_p + 1; q <= end_p; ++q) {
                if (!check_conditions(q, query.tokens[plan.seed].conditions)) {
                    valid = false;
                    break;
                }
            }
            if (!valid) break;
        }

        // Partial match vectors are of size 2*n: [0..n-1]=starts, [n..2n-1]=ends
        std::vector<std::vector<CorpusPos>> partial;
        {
            std::vector<CorpusPos> pm(2 * n, NO_HEAD);
            pm[plan.seed] = seed_p;
            pm[n + plan.seed] = seed_p + seed_len - 1;
            partial.push_back(std::move(pm));
        }

        // Expand outward through plan steps
        for (const auto& step : plan.steps) {
            if (step.edge_idx >= query.relations.size())
                throw std::runtime_error("Internal error: query plan edge index out of range");
            RelationType rel = query.relations[step.edge_idx].type;
            const auto& target_cond = query.tokens[step.to].conditions;
            int to_min = query.tokens[step.to].min_repeat;
            int to_max = query.tokens[step.to].max_repeat;
            bool is_negative = (rel == RelationType::NOT_GOVERNS ||
                                rel == RelationType::NOT_GOV_BY);

            std::vector<std::vector<CorpusPos>> new_partial;

            if (query.tokens[step.to].is_dep_subtree) {
                if (!corpus_.has_deps())
                    throw std::runtime_error("dep_subtree requires a corpus with dependency index");
                if (rel != RelationType::SEQUENCE)
                    throw std::runtime_error("dep_subtree must follow SEQUENCE (chain) relation");
                const NameIndexMap nm = build_name_map(query);
                auto src_it = nm.find(query.tokens[step.to].dep_subtree_source);
                if (src_it == nm.end()) {
                    throw std::runtime_error(
                            "dep_subtree: unknown source token '" + query.tokens[step.to].dep_subtree_source
                            + "'");
                }
                const size_t src_idx = src_it->second;
                for (const auto& pm : partial) {
                    CorpusPos head = pm[src_idx];
                    if (head == NO_HEAD) continue;
                    auto extended = pm;
                    extended[step.to] = head;
                    extended[n + step.to] = head;
                    new_partial.push_back(std::move(extended));
                }
                partial = std::move(new_partial);
                if (partial.empty()) break;
                continue;
            }

            if (is_negative) {
                // Negative relation: keep partial only if NO target matches
                RelationType pos_rel = (rel == RelationType::NOT_GOVERNS)
                                       ? RelationType::GOVERNS
                                       : RelationType::GOVERNED_BY;
                for (const auto& pm : partial) {
                    CorpusPos from_pos = pm[step.from];
                    if (from_pos == NO_HEAD) continue;
                    bool any_match = false;
                    for_each_related(from_pos, pos_rel, step.reversed, [&](CorpusPos r) -> bool {
                        if (r >= 0 && r < corpus_.size() &&
                            check_conditions(r, target_cond)) {
                            any_match = true;
                            return false;  // stop early
                        }
                        return true;
                    });
                    if (!any_match)
                        new_partial.push_back(pm);
                }
            } else if (rel == RelationType::SEQUENCE && (to_min != 1 || to_max != 1)) {
                // SEQUENCE with repetition/optional on the target token
                for (const auto& pm : partial) {
                    CorpusPos from_end = pm[n + step.from];
                    CorpusPos from_start = pm[step.from];
                    if (from_end == NO_HEAD) {
                        // From-token was skipped (optional) — walk to nearest placed token
                        CorpusPos fallback = NO_HEAD;
                        if (!step.reversed) {
                            for (int k = static_cast<int>(step.from) - 1; k >= 0; --k) {
                                if (pm[n + k] != NO_HEAD) { fallback = pm[n + k]; break; }
                            }
                        } else {
                            for (size_t k = step.from + 1; k < n; ++k) {
                                if (pm[k] != NO_HEAD) { fallback = pm[k]; break; }
                            }
                        }
                        if (fallback == NO_HEAD) continue;
                        from_end = fallback;
                        from_start = fallback;
                    }

                    CorpusPos base;
                    if (!step.reversed) {
                        base = from_end + 1;    // span starts right after from's end
                    } else {
                        base = from_start - 1;  // span ends right before from's start
                    }

                    // Optional token: min=0 path (skip this token entirely)
                    if (to_min == 0) {
                        auto skipped = pm;
                        new_partial.push_back(std::move(skipped));
                    }

                    // Try spans of length 1..to_max
                    for (int len = 1; len <= to_max; ++len) {
                        CorpusPos p = step.reversed ? (base - len + 1) : (base + len - 1);
                        if (p < 0 || p >= corpus_.size()) break;
                        if (!check_conditions(p, target_cond)) break;

                        if (len >= std::max(to_min, 1)) {
                            auto extended = pm;
                            if (!step.reversed) {
                                extended[step.to] = base;
                                extended[n + step.to] = base + len - 1;
                            } else {
                                extended[step.to] = base - len + 1;
                                extended[n + step.to] = base;
                            }
                            new_partial.push_back(std::move(extended));
                        }
                    }
                }
            } else {
                // Standard non-repeating path (or non-SEQUENCE relation)
                for (const auto& pm : partial) {
                    CorpusPos from_pos;
                    if (rel == RelationType::SEQUENCE) {
                        from_pos = step.reversed ? pm[step.from] : pm[n + step.from];
                    } else {
                        from_pos = pm[step.from];
                    }
                    if (from_pos == NO_HEAD) {
                        // From-token was skipped (optional with min=0).
                        // Walk to nearest placed token for SEQUENCE fallback.
                        if (rel == RelationType::SEQUENCE) {
                            CorpusPos fallback = NO_HEAD;
                            if (!step.reversed) {
                                for (int k = static_cast<int>(step.from) - 1; k >= 0; --k) {
                                    if (pm[n + k] != NO_HEAD) { fallback = pm[n + k]; break; }
                                }
                            } else {
                                for (size_t k = step.from + 1; k < n; ++k) {
                                    if (pm[k] != NO_HEAD) { fallback = pm[k]; break; }
                                }
                            }
                            if (fallback == NO_HEAD) continue;
                            from_pos = fallback;
                        } else {
                            continue;
                        }
                    }

                    // Optional token: min=0 path (skip)
                    if (to_min == 0 && rel == RelationType::SEQUENCE) {
                        auto skipped = pm;
                        new_partial.push_back(std::move(skipped));
                    }

                    for_each_related(from_pos, rel, step.reversed, [&](CorpusPos r) -> bool {
                        if (r >= 0 && r < corpus_.size() &&
                            check_conditions(r, target_cond)) {
                            auto extended = pm;
                            extended[step.to] = r;
                            extended[n + step.to] = r;
                            new_partial.push_back(std::move(extended));
                        }
                        return true;
                    });
                }
            }

            partial = std::move(new_partial);
            if (partial.empty()) break;
        }

        // Within-clause filter + emit
        bool stop = false;
        for (auto& pm : partial) {
            if (within_sa) {
                if (within_span_semantics) {
                    auto ext = match_min_max_span(pm, n);
                    if (!ext || !within_span_in_some_region(*within_sa, ext->first, ext->second))
                        continue;
                } else {
                    CorpusPos anchor = NO_HEAD;
                    for (size_t i = 0; i < n; ++i) {
                        if (pm[i] != NO_HEAD) {
                            anchor = pm[i];
                            break;
                        }
                    }
                    if (anchor == NO_HEAD) continue;
                    int64_t rgn = within_sa->find_region(anchor);
                    if (rgn < 0) continue;
                    bool ok = true;
                    for (size_t i = 0; i < n; ++i) {
                        if (pm[i] == NO_HEAD) continue;
                        if (within_sa->find_region(pm[i]) != rgn) {
                            ok = false;
                            break;
                        }
                        if (pm[n + i] != pm[i] && pm[n + i] != NO_HEAD &&
                            within_sa->find_region(pm[n + i]) != rgn) {
                            ok = false;
                            break;
                        }
                    }
                    if (!ok) continue;
                }
            }
            if (!emit(std::move(pm))) { stop = true; break; }
        }
        if (stop) break;
    }
}

// ── expand_one_seed (convenience wrapper for parallel execution) ──────────

std::vector<Match> QueryExecutor::expand_one_seed(const TokenQuery& query,
                                                  const QueryPlan& plan,
                                                  const StructuralAttr* within_sa,
                                                  CorpusPos seed_p,
                                                  bool within_span_semantics) const {
    std::vector<Match> out;
    size_t n = query.tokens.size();

    expand_seed(query, plan, within_sa, seed_p,
                [&](std::vector<CorpusPos>&& pm) -> bool {
                    Match m;
                    m.positions.assign(pm.begin(), pm.begin() + n);
                    m.span_ends.assign(pm.begin() + n, pm.begin() + 2 * n);
                    materialize_dep_subtree_bindings(query, m);
                    out.push_back(std::move(m));
                    return true;
                },
                within_span_semantics);

    return out;
}

void QueryExecutor::materialize_dep_subtree_bindings(const TokenQuery& query, Match& m) const {
    if (!corpus_.has_deps()) return;
    const auto& deps = corpus_.deps();
    for (size_t i = 0; i < query.tokens.size(); ++i) {
        const auto& tok = query.tokens[i];
        if (!tok.is_dep_subtree || tok.name.empty()) continue;
        if (i >= m.positions.size()) continue;
        CorpusPos head = m.positions[i];
        if (head == NO_HEAD) continue;
        std::vector<CorpusPos> pos;
        pos.push_back(head);
        auto desc = deps.subtree(head);
        pos.insert(pos.end(), desc.begin(), desc.end());
        std::sort(pos.begin(), pos.end());
        pos.erase(std::unique(pos.begin(), pos.end()), pos.end());
        m.named_dep_subtrees[tok.name] = std::move(pos);
    }
}

MatchSet QueryExecutor::execute_dep_subtree_from_named(const TokenQuery& query,
                                                       const MatchSet& source_matches,
                                                       const NameIndexMap& source_name_map,
                                                       size_t max_matches,
                                                       bool count_total,
                                                       size_t max_total_cap) const {
    validate_query_name_bindings(query);
    if (query.tokens.size() != 1 || !query.tokens[0].is_dep_subtree)
        throw std::runtime_error("internal: execute_dep_subtree_from_named expects one dep_subtree token");
    if (!corpus_.has_deps())
        throw std::runtime_error("dep_subtree requires a corpus with dependency index");
    compile_query(query);
    NameIndexMap name_map = build_name_map(query);

    MatchSet out;
    out.num_tokens = 1;
    const std::string& src_label = query.tokens[0].dep_subtree_source;

    for (const auto& src_m : source_matches.matches) {
        CorpusPos head = resolve_name(src_m, source_name_map, src_label);
        if (head == NO_HEAD) {
            if (!src_m.positions.empty()) head = src_m.positions[0];
            else continue;
        }
        Match m;
        m.positions.push_back(head);
        m.span_ends.push_back(head);
        materialize_dep_subtree_bindings(query, m);
        out.matches.push_back(std::move(m));
        if (count_total && max_total_cap > 0 && out.matches.size() >= max_total_cap) break;
    }

    apply_global_filters(query, name_map, out);
    if (max_matches > 0 && !count_total && out.matches.size() > max_matches)
        out.matches.resize(max_matches);
    out.total_count = out.matches.size();
    out.total_exact = true;
    return out;
}

// ── #16 Source | Target parallel execution ──────────────────────────────

MatchSet QueryExecutor::execute_parallel(const TokenQuery& source_query,
                                         const TokenQuery& target_query,
                                         size_t max_matches,
                                         bool count_total) {
    {
        TokenQuery s2, t2;
        const bool a = simplify_bare_strings(corpus_, source_query, s2);
        const bool b = simplify_bare_strings(corpus_, target_query, t2);
        if (a || b)
            return execute_parallel(a ? s2 : source_query, b ? t2 : target_query, max_matches, count_total);
    }
    validate_query_name_bindings(source_query, &target_query);
    validate_query_name_bindings(target_query, &source_query);

    // `parse_token_query` may attach trailing `::` filters to either side; alignment
    // constraints refer to names from *both* halves and must not run inside execute()
    // on a single query (token labels from the other side would be missing).
    std::vector<GlobalAlignmentFilter> merged_alignment;
    merged_alignment.reserve(source_query.global_alignment_filters.size()
                             + target_query.global_alignment_filters.size());
    merged_alignment.insert(merged_alignment.end(),
                              source_query.global_alignment_filters.begin(),
                              source_query.global_alignment_filters.end());
    merged_alignment.insert(merged_alignment.end(),
                              target_query.global_alignment_filters.begin(),
                              target_query.global_alignment_filters.end());

    TokenQuery src_exec = source_query;
    TokenQuery tgt_exec = target_query;
    src_exec.global_alignment_filters.clear();
    tgt_exec.global_alignment_filters.clear();

    MatchSet result;
    result.num_tokens = source_query.tokens.size() + target_query.tokens.size();

    MatchSet source_set = execute(src_exec, 0, true, 0, 0, 0, 1, nullptr, true);
    NameIndexMap src_names = build_name_map(source_query);
    NameIndexMap tgt_names = build_name_map(target_query);

    // Apply region filters (e.g. :: match.text_lang="en") before joining.
    apply_region_filters(source_query, src_names, source_set);

    const auto& filters = merged_alignment;
    auto is_anchor_label = [](const TokenQuery& q, const std::string& name) {
        for (const auto& tok : q.tokens)
            if (tok.name == name) return tok.is_anchor();
        return false;
    };

    // Fast path: single alignment filter with positional attrs, simple one-token target.
    // Instead of materializing the full target query set, pull only target positions whose
    // alignment value appears on the (already computed) source side.
    if (filters.size() == 1 && !include_empty_alignment_values_) {
        const auto& af = filters[0];
        std::string an1 = normalize_attr(af.attr1);
        std::string an2 = normalize_attr(af.attr2);
        const bool target_simple =
            target_query.tokens.size() == 1 &&
            target_query.relations.empty() &&
            !target_query.tokens[0].has_repetition() &&
            !target_query.tokens[0].is_anchor() &&
            target_query.within.empty() &&
            !target_query.not_within &&
            !target_query.within_having &&
            target_query.containing_clauses.empty() &&
            target_query.global_function_filters.empty();
        // (a multivalue target attribute's lexicon holds whole values, not the
        // components the source side collects: the general join handles it)
        if (target_simple
            && corpus_.has_attr(an1) && corpus_.has_attr(an2)
            && !corpus_.is_multivalue(an2)
            && !is_anchor_label(source_query, af.name1)
            && !is_anchor_label(target_query, af.name2)) {
            const auto& pa2 = corpus_.attr(an2);
            std::unordered_set<std::string> wanted_components;
            wanted_components.reserve(std::max<size_t>(source_set.matches.size(), 16));
            for (const auto& s : source_set.matches) {
                CorpusPos p1 = resolve_name(s, src_names, af.name1);
                if (p1 == NO_HEAD) continue;
                collect_alignment_component_strings(corpus_, an1, p1, wanted_components);
            }

            MatchSet target_set;
            target_set.num_tokens = target_query.tokens.size();
            std::unordered_set<CorpusPos> seen_positions;
            seen_positions.reserve(wanted_components.size() * 4 + 16);
            for (const auto& comp : wanted_components) {
                LexiconId id = pa2.lexicon().lookup(comp);
                if (id == UNKNOWN_LEX) continue;
                pa2.for_each_position_id(id, [&](CorpusPos p) {
                    if (!seen_positions.insert(p).second) return true;
                    if (!check_conditions(p, target_query.tokens[0].conditions)) return true;
                    Match m;
                    m.positions.push_back(p);
                    m.span_ends.push_back(p);
                    target_set.matches.push_back(std::move(m));
                    return true;
                });
            }
            apply_region_filters(target_query, tgt_names, target_set);

            parallel_alignment_overlap_join_single(
                    corpus_, an1, an2, af, source_set.matches, target_set.matches,
                    src_names, tgt_names, include_empty_alignment_values_,
                    result.parallel_matches, result.total_count,
                    max_matches, count_total);
            result.total_exact = true;
            return result;
        }
    }

    MatchSet target_set = execute(tgt_exec, 0, true, 0, 0, 0, 1, nullptr, true);
    apply_region_filters(target_query, tgt_names, target_set);

    if (filters.size() == 1) {
        const auto& af = filters[0];
        std::string an1 = normalize_attr(af.attr1);
        std::string an2 = normalize_attr(af.attr2);
        if (corpus_.has_attr(an1) && corpus_.has_attr(an2)
            && !is_anchor_label(source_query, af.name1)
            && !is_anchor_label(target_query, af.name2)) {
            parallel_alignment_overlap_join_single(
                    corpus_, an1, an2, af, source_set.matches, target_set.matches,
                    src_names, tgt_names, include_empty_alignment_values_,
                    result.parallel_matches, result.total_count,
                    max_matches, count_total);
            result.total_exact = true;
            return result;
        }
    }
    for (const auto& s : source_set.matches) {
        for (const auto& t : target_set.matches) {
            bool aligned = true;
            for (const auto& af : filters) {
                std::string an1 = normalize_attr(af.attr1);
                std::string an2 = normalize_attr(af.attr2);
                if (!alignment_attr_is_supported(corpus_, an1)
                    || !alignment_attr_is_supported(corpus_, an2)) {
                    aligned = false;
                    break;
                }
                auto v1 = resolve_alignment_operand_value(corpus_, s, src_names, af.name1, an1);
                auto v2 = resolve_alignment_operand_value(corpus_, t, tgt_names, af.name2, an2);
                if (!alignment_values_match(corpus_, an1, an2, v1, v2,
                                            include_empty_alignment_values_)) {
                    aligned = false;
                    break;
                }
            }
            if (aligned) {
                result.parallel_matches.emplace_back(s, t);
                result.total_count++;
                if (max_matches > 0 && result.total_count >= max_matches && !count_total)
                    goto done;
            }
        }
    }
done:
    result.total_exact = true;
    return result;
}

// ── #12 Global filters ───────────────────────────────────────────────────

void QueryExecutor::apply_region_filters(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const {
    if (query.global_region_filters.empty()) return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (const auto& m : result.matches) {
        bool pass = true;

        for (const auto& gf : query.global_region_filters) {
            std::string struct_name;
            std::string attr_name;
            const StructuralAttr* sa_ptr = nullptr;
            std::optional<std::string> rkey;

            size_t us = gf.region_attr.find('_');
            if (us != std::string::npos && us + 1 < gf.region_attr.size()) {
                struct_name = gf.region_attr.substr(0, us);
                attr_name = gf.region_attr.substr(us + 1);
                if (!corpus_.has_structure(struct_name)) { pass = false; break; }
                sa_ptr = &corpus_.structure(struct_name);
                rkey = resolve_region_attr_key(*sa_ptr, struct_name, attr_name);
            } else if (!gf.anchor_name.empty()) {
                auto nr = m.named_regions.find(gf.anchor_name);
                if (nr != m.named_regions.end() && corpus_.has_structure(nr->second.struct_name)) {
                    struct_name = nr->second.struct_name;
                    attr_name = gf.region_attr;
                    sa_ptr = &corpus_.structure(struct_name);
                    rkey = resolve_region_attr_key(*sa_ptr, struct_name, attr_name);
                } else {
                    CorpusPos pos = m.first_pos();
                    CorpusPos ap = resolve_name(m, name_map, gf.anchor_name);
                    if (ap != NO_HEAD) pos = ap;
                    // If anchor name is a token label, infer its containing structure by probing.
                    for (const auto& sn : corpus_.structure_names()) {
                        const auto& probe_sa = corpus_.structure(sn);
                        auto probe_key = resolve_region_attr_key(probe_sa, sn, gf.region_attr);
                        if (!probe_key) continue;
                        if (probe_sa.find_region(pos) >= 0) {
                            struct_name = sn;
                            attr_name = gf.region_attr;
                            sa_ptr = &probe_sa;
                            rkey = std::move(probe_key);
                            break;
                        }
                    }
                }
            } else {
                pass = false;
                break;
            }
            if (!sa_ptr || !rkey) { pass = false; break; }
            const auto& sa = *sa_ptr;

            int64_t rgn = -1;
            if (!gf.anchor_name.empty()) {
                auto nr = m.named_regions.find(gf.anchor_name);
                if (nr != m.named_regions.end()) {
                    if (nr->second.struct_name != struct_name) { pass = false; break; }
                    rgn = static_cast<int64_t>(nr->second.region_idx);
                }
            }
            if (rgn < 0) {
                CorpusPos pos = m.first_pos();
                if (!gf.anchor_name.empty()) {
                    CorpusPos ap = resolve_name(m, name_map, gf.anchor_name);
                    if (ap != NO_HEAD) pos = ap;
                }
                rgn = sa.find_region(pos);
                if (rgn < 0) { pass = false; break; }
            }
            const std::string composite = struct_name + "_" + *rkey;
            const bool is_mv = corpus_.is_multivalue(composite);
            if (gf.op == CompOp::EQ && sa.has_region_value_reverse(*rkey) && !is_mv) {
                if (sa.region_matches_attr_eq_rev(*rkey, static_cast<size_t>(rgn), gf.value))
                    continue;
            }
            std::string_view rval = sa.region_value(*rkey, static_cast<size_t>(rgn));
            if (!compare_value_maybe_mv(gf.op, rval, gf.value, is_mv)) { pass = false; break; }
        }

        if (pass) kept.push_back(m);
    }

    const size_t before = result.matches.size();
    result.matches = std::move(kept);
    // Fast paths may have already counted a full-corpus total past the page.
    // Only sync total_count from the page when it still reflected page size;
    // otherwise subtract rejected page rows so --total stays accurate.
    if (result.total_count <= before)
        result.total_count = result.matches.size();
    else if (before > result.matches.size())
        result.total_count -= (before - result.matches.size());
}

void QueryExecutor::apply_global_filters(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const {
    if (query.global_region_filters.empty() && query.global_alignment_filters.empty()
        && query.global_function_filters.empty())
        return;

    // First apply region filters
    apply_region_filters(query, name_map, result);
    if (query.global_alignment_filters.empty() && query.global_function_filters.empty())
        return;
    if (result.matches.empty())
        return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (const auto& m : result.matches) {
        bool pass = true;

        // Alignment filters: :: a.attr = b.attr
        for (const auto& af : query.global_alignment_filters) {
            std::string an1 = normalize_attr(af.attr1);
            std::string an2 = normalize_attr(af.attr2);
            if (!alignment_attr_is_supported(corpus_, an1)
                || !alignment_attr_is_supported(corpus_, an2))
                { pass = false; break; }
            auto v1 = resolve_alignment_operand_value(corpus_, m, name_map, af.name1, an1);
            auto v2 = resolve_alignment_operand_value(corpus_, m, name_map, af.name2, an2);
            if (!alignment_values_match(corpus_, an1, an2, v1, v2,
                                        include_empty_alignment_values_)) {
                pass = false;
                break;
            }
        }

        // Function filters: :: distance(a,b) < 5, :: depth(a) > depth(b), etc.
        if (pass) {
            // Evaluate a single GlobalFuncCall against a match.
            // Returns {true, value} on success, {false, 0} on failure (match rejected).
            auto eval_func = [&](const GlobalFuncCall& fc) -> std::pair<bool, int64_t> {
                auto resolve_date_spec = [&](const std::string& spec) -> std::optional<std::string> {
                    auto dot = spec.find('.');
                    if (dot != std::string::npos) {
                        if (dot == 0 || dot + 1 >= spec.size()) return std::nullopt;
                        std::string prefix = spec.substr(0, dot);
                        std::string suffix = spec.substr(dot + 1);

                        CorpusPos p = resolve_name(m, name_map, prefix);
                        std::string attr_name = normalize_attr(suffix);
                        if (p != NO_HEAD && corpus_.has_attr(attr_name)) {
                            return std::string(corpus_.attr(attr_name).value_at(p));
                        }

                        auto nr = m.named_regions.find(prefix);
                        if (nr != m.named_regions.end() && corpus_.has_structure(nr->second.struct_name)) {
                            const auto& sa = corpus_.structure(nr->second.struct_name);
                            auto rkey = resolve_region_attr_key(sa, nr->second.struct_name, suffix);
                            if (!rkey) return std::nullopt;
                            if (nr->second.region_idx >= sa.region_count()) return std::nullopt;
                            return std::string(sa.region_value(*rkey, nr->second.region_idx));
                        }
                        return std::nullopt;
                    }

                    std::string attr_name = normalize_attr(spec);
                    if (corpus_.has_attr(attr_name)) {
                        return std::string(corpus_.attr(attr_name).value_at(m.first_pos()));
                    }

                    RegionAttrParts parts;
                    if (split_region_attr_name(spec, parts) && corpus_.has_structure(parts.struct_name)) {
                        const auto& sa = corpus_.structure(parts.struct_name);
                        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
                        if (!rkey) return std::nullopt;
                        int64_t rgn = sa.find_region(m.first_pos());
                        if (rgn < 0) return std::nullopt;
                        return std::string(sa.region_value(*rkey, static_cast<size_t>(rgn)));
                    }
                    return std::nullopt;
                };

                switch (fc.func) {
                case GlobalFunctionType::DISTANCE:
                case GlobalFunctionType::DISTABS: {
                    if (fc.args.size() < 2) return {false, 0};
                    CorpusPos p1 = resolve_name(m, name_map, fc.args[0]);
                    CorpusPos p2 = resolve_name(m, name_map, fc.args[1]);
                    if (p1 == NO_HEAD || p2 == NO_HEAD) return {false, 0};
                    return {true, std::abs(p1 - p2)};
                }
                case GlobalFunctionType::STRLEN: {
                    if (fc.args.empty()) return {false, 0};
                    const std::string& spec = fc.args[0];
                    auto dot = spec.find('.');
                    if (dot == std::string::npos) return {false, 0};
                    std::string tok_name = spec.substr(0, dot);
                    std::string attr_name = normalize_attr(spec.substr(dot + 1));
                    CorpusPos p = resolve_name(m, name_map, tok_name);
                    if (p == NO_HEAD || !corpus_.has_attr(attr_name)) return {false, 0};
                    std::string_view val = corpus_.attr(attr_name).value_at(p);
                    int64_t cp_count = 0;
                    for (unsigned char c : val)
                        if ((c & 0xC0) != 0x80) ++cp_count;
                    return {true, cp_count};
                }
                case GlobalFunctionType::FREQ: {
                    if (fc.args.empty()) return {false, 0};
                    const std::string& spec = fc.args[0];
                    auto dot = spec.find('.');
                    if (dot == std::string::npos) return {false, 0};
                    std::string tok_name = spec.substr(0, dot);
                    std::string attr_name = normalize_attr(spec.substr(dot + 1));
                    CorpusPos p = resolve_name(m, name_map, tok_name);
                    if (p == NO_HEAD || !corpus_.has_attr(attr_name)) return {false, 0};
                    const auto& pa = corpus_.attr(attr_name);
                    LexiconId lid = pa.id_at(p);
                    if (lid == UNKNOWN_LEX) return {false, 0};
                    return {true, static_cast<int64_t>(pa.count_of_id(lid))};
                }
                case GlobalFunctionType::YEAR: {
                    if (fc.args.size() != 1) return {false, 0};
                    auto val = resolve_date_spec(fc.args[0]);
                    if (!val) return {false, 0};
                    auto y = parse_year_prefix(*val);
                    if (!y) return {false, 0};
                    return {true, *y};
                }
                case GlobalFunctionType::CENTURY: {
                    if (fc.args.size() != 1) return {false, 0};
                    auto val = resolve_date_spec(fc.args[0]);
                    if (!val) return {false, 0};
                    auto y = parse_year_prefix(*val);
                    if (!y) return {false, 0};
                    auto c = century_from_year(*y);
                    if (!c) return {false, 0};
                    return {true, *c};
                }
                case GlobalFunctionType::DECADE: {
                    if (fc.args.size() != 1) return {false, 0};
                    auto val = resolve_date_spec(fc.args[0]);
                    if (!val) return {false, 0};
                    auto y = parse_year_prefix(*val);
                    if (!y) return {false, 0};
                    return {true, decade_from_year(*y)};
                }
                case GlobalFunctionType::MONTH: {
                    if (fc.args.size() != 1) return {false, 0};
                    auto val = resolve_date_spec(fc.args[0]);
                    if (!val) return {false, 0};
                    auto p = parse_date_parts_prefix(*val);
                    if (!p || !p->has_month) return {false, 0};
                    return {true, static_cast<int64_t>(p->month)};
                }
                case GlobalFunctionType::DAY: {
                    if (fc.args.size() != 1) return {false, 0};
                    auto val = resolve_date_spec(fc.args[0]);
                    if (!val) return {false, 0};
                    auto p = parse_date_parts_prefix(*val);
                    if (!p || !p->has_day) return {false, 0};
                    return {true, static_cast<int64_t>(p->day)};
                }
                case GlobalFunctionType::WEEK: {
                    if (fc.args.size() != 1) return {false, 0};
                    auto val = resolve_date_spec(fc.args[0]);
                    if (!val) return {false, 0};
                    auto p = parse_date_parts_prefix(*val);
                    if (!p || !p->has_day) return {false, 0};
                    return {true, static_cast<int64_t>(iso_week_number(p->year, p->month, p->day))};
                }
                case GlobalFunctionType::NCHILDREN: {
                    if (fc.args.empty() || !corpus_.has_deps()) return {false, 0};
                    CorpusPos p = resolve_name(m, name_map, fc.args[0]);
                    if (p == NO_HEAD) return {false, 0};
                    return {true, static_cast<int64_t>(corpus_.deps().children_count(p))};
                }
                case GlobalFunctionType::DEPTH: {
                    if (fc.args.empty() || !corpus_.has_deps()) return {false, 0};
                    CorpusPos p = resolve_name(m, name_map, fc.args[0]);
                    if (p == NO_HEAD) return {false, 0};
                    return {true, static_cast<int64_t>(corpus_.deps().depth(p))};
                }
                case GlobalFunctionType::NDESCENDANTS: {
                    if (fc.args.empty() || !corpus_.has_deps()) return {false, 0};
                    CorpusPos p = resolve_name(m, name_map, fc.args[0]);
                    if (p == NO_HEAD) return {false, 0};
                    int16_t ein = corpus_.deps().euler_in(p);
                    int16_t eout = corpus_.deps().euler_out(p);
                    return {true, static_cast<int64_t>((eout - ein) / 2)};
                }
                case GlobalFunctionType::NVALS: {
                    if (fc.args.size() != 1) return {false, 0};
                    const std::string& spec = fc.args[0];
                    auto dot = spec.find('.');
                    if (dot == std::string::npos) return {false, 0};
                    std::string tok_name = spec.substr(0, dot);
                    std::string attr_raw = spec.substr(dot + 1);
                    CorpusPos p = resolve_name(m, name_map, tok_name);
                    if (p == NO_HEAD) return {false, 0};
                    auto n = nvals_cardinality_at(p, attr_raw);
                    if (!n) return {false, 0};
                    return {true, *n};
                }
                case GlobalFunctionType::CONTAINS: {
                    if (fc.args.size() < 2) return {false, 0};
                    auto outer = named_region_span(corpus_, m, fc.args[0]);
                    auto inner = named_region_span(corpus_, m, fc.args[1]);
                    if (!outer || !inner) return {false, 0};
                    return {true, region_span_contains(*outer, *inner) ? 1 : 0};
                }
                case GlobalFunctionType::RCHILD: {
                    if (fc.args.size() < 2) return {false, 0};
                    auto pit = m.named_regions.find(fc.args[0]);
                    auto cit = m.named_regions.find(fc.args[1]);
                    if (pit == m.named_regions.end() || cit == m.named_regions.end())
                        return {false, 0};
                    const RegionRef& pr = pit->second;
                    const RegionRef& cr = cit->second;
                    if (pr.struct_name != cr.struct_name)
                        return {true, 0};
                    if (!corpus_.has_structure(pr.struct_name)) return {false, 0};
                    const auto& sa = corpus_.structure(pr.struct_name);
                    if (!sa.has_parent_region_id()) return {false, 0};
                    if (pr.region_idx >= sa.region_count() || cr.region_idx >= sa.region_count())
                        return {false, 0};
                    int32_t par = sa.parent_region_id(cr.region_idx);
                    bool ok = par >= 0 && static_cast<size_t>(par) == pr.region_idx;
                    return {true, ok ? 1 : 0};
                }
                case GlobalFunctionType::RCONTAINS: {
                    if (fc.args.size() < 2) return {false, 0};
                    auto ait = m.named_regions.find(fc.args[0]);
                    auto dit = m.named_regions.find(fc.args[1]);
                    if (ait == m.named_regions.end() || dit == m.named_regions.end())
                        return {false, 0};
                    const RegionRef& ar = ait->second;
                    const RegionRef& dr = dit->second;
                    if (ar.struct_name != dr.struct_name)
                        return {true, 0};
                    if (!corpus_.has_structure(ar.struct_name)) return {false, 0};
                    const auto& sa = corpus_.structure(ar.struct_name);
                    if (!sa.has_parent_region_id()) return {false, 0};
                    if (ar.region_idx >= sa.region_count() || dr.region_idx >= sa.region_count())
                        return {false, 0};
                    bool ok = sa.region_is_ancestor_of(ar.region_idx, dr.region_idx);
                    return {true, ok ? 1 : 0};
                }
                case GlobalFunctionType::TCNT: {
                    if (fc.args.size() != 1) return {false, 0};
                    const std::string& label = fc.args[0];
                    auto ds = m.named_dep_subtrees.find(label);
                    if (ds != m.named_dep_subtrees.end())
                        return {true, static_cast<int64_t>(ds->second.size())};
                    auto nr = m.named_regions.find(label);
                    if (nr != m.named_regions.end()) {
                        const RegionRef& rr = nr->second;
                        if (!corpus_.has_structure(rr.struct_name)) return {false, 0};
                        const auto& sa = corpus_.structure(rr.struct_name);
                        Region rg = sa.get(rr.region_idx);
                        if (rg.start > rg.end) return {true, 0};
                        return {true, static_cast<int64_t>(rg.end - rg.start + 1)};
                    }
                    return {false, 0};
                }
                }
                return {false, 0};
            };

            for (const auto& ff : query.global_function_filters) {
                auto [lhs_ok, lhs_val] = eval_func(ff.lhs);
                if (!lhs_ok) { pass = false; break; }

                int64_t rhs_val = ff.int_value;
                if (ff.has_rhs_func) {
                    auto [rhs_ok, rv] = eval_func(ff.rhs);
                    if (!rhs_ok) { pass = false; break; }
                    rhs_val = rv;
                }

                bool ok = false;
                switch (ff.op) {
                    case CompOp::EQ:  ok = (lhs_val == rhs_val); break;
                    case CompOp::NEQ: ok = (lhs_val != rhs_val); break;
                    case CompOp::LT:  ok = (lhs_val <  rhs_val); break;
                    case CompOp::GT:  ok = (lhs_val >  rhs_val); break;
                    case CompOp::LTE: ok = (lhs_val <= rhs_val); break;
                    case CompOp::GTE: ok = (lhs_val >= rhs_val); break;
                    case CompOp::REGEX:
                    case CompOp::IN:  ok = false; break;
                }
                if (!ok) { pass = false; break; }
            }
        }

        if (pass) kept.push_back(m);
    }

    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

// ── Anchor preprocessing and filtering ───────────────────────────────────

TokenQuery QueryExecutor::strip_anchors(const TokenQuery& query,
                                        std::vector<AnchorConstraint>& constraints) {
    TokenQuery cleaned;
    cleaned.within = query.within;
    cleaned.not_within = query.not_within;
    cleaned.within_having = query.within_having;
    cleaned.containing_clauses = query.containing_clauses;
    cleaned.global_region_filters = query.global_region_filters;
    cleaned.global_alignment_filters = query.global_alignment_filters;
    cleaned.global_function_filters = query.global_function_filters;
    cleaned.position_orders = query.position_orders;

    // Map from old token indices to new (after stripping anchors)
    std::vector<int> old_to_new(query.tokens.size(), -1);

    for (size_t i = 0; i < query.tokens.size(); ++i) {
        if (query.tokens[i].is_anchor()) {
            // Record the constraint: bind to the nearest real token
            AnchorConstraint ac;
            ac.region = query.tokens[i].anchor_region;
            ac.is_start = (query.tokens[i].anchor == RegionAnchorType::REGION_START);
            ac.attrs = query.tokens[i].anchor_attrs;
            ac.binding_name = query.tokens[i].name;
            ac.anchor_region_clauses = query.tokens[i].anchor_region_clauses;

            bool bound = false;
            if (ac.is_start) {
                // <s> binds to the next real token
                for (size_t j = i + 1; j < query.tokens.size(); ++j) {
                    if (!query.tokens[j].is_anchor()) {
                        ac.token_idx = j;  // temporarily store old index
                        bound = true;
                        break;
                    }
                }
            } else {
                // </s> binds to the previous real token
                for (int j = static_cast<int>(i) - 1; j >= 0; --j) {
                    if (!query.tokens[static_cast<size_t>(j)].is_anchor()) {
                        ac.token_idx = static_cast<size_t>(j);
                        bound = true;
                        break;
                    }
                }
            }
            // No following token: optional region-only query (`np:<s> :: ...`) enumerates rows.
            if (bound) {
                constraints.push_back(ac);
            } else if (ac.is_start && !ac.binding_name.empty()) {
                ac.region_enumeration = true;
                constraints.push_back(ac);
            }
        } else {
            old_to_new[i] = static_cast<int>(cleaned.tokens.size());
            cleaned.tokens.push_back(query.tokens[i]);
        }
    }

    // Remap constraint token indices from old to new
    for (auto& ac : constraints) {
        if (ac.region_enumeration) continue;  // no token slot; token_idx unused
        int new_idx = old_to_new[ac.token_idx];
        if (new_idx < 0) {
            // Anchor binding to another anchor — shouldn't happen, ignore
            ac.token_idx = 0;
        } else {
            ac.token_idx = static_cast<size_t>(new_idx);
        }
    }

    // Rebuild relations: only keep relations between consecutive real tokens
    // The relation between old tokens[i] and tokens[i+1] maps to the relation
    // between the real tokens they connect.
    // For a chain like <s> [] [] </s>, the relations are:
    //   [<s>→[]] [[]→[]] [[]]→</s>]
    // After stripping, we just need the relation between [] and []
    // Use SEQUENCE for all surviving consecutive pairs (original relations carry over)
    for (size_t i = 0; i + 1 < cleaned.tokens.size(); ++i) {
        // Find the original relation between these tokens
        // Default to SEQUENCE if we can't determine from originals
        cleaned.relations.push_back({RelationType::SEQUENCE});
    }

    // Better: try to find original relations between the real tokens
    // Overwrite with actual relation types where possible
    cleaned.relations.clear();
    std::vector<size_t> real_indices;
    for (size_t i = 0; i < query.tokens.size(); ++i) {
        if (!query.tokens[i].is_anchor())
            real_indices.push_back(i);
    }
    for (size_t k = 0; k + 1 < real_indices.size(); ++k) {
        size_t from = real_indices[k];
        size_t to = real_indices[k + 1];
        // Find the relation on the edge closest to 'from' going toward 'to'
        // The original relations[i] connects tokens[i] and tokens[i+1]
        // Between from and to, pick the first non-anchor relation
        RelationType rel = RelationType::SEQUENCE;  // default
        for (size_t e = from; e < to && e < query.relations.size(); ++e) {
            if (!query.tokens[e].is_anchor() || !query.tokens[e + 1].is_anchor()) {
                rel = query.relations[e].type;
                break;
            }
        }
        cleaned.relations.push_back({rel});
    }

    return cleaned;
}

bool QueryExecutor::match_passes_inline_region_filters(const Match& m, const TokenQuery& q,
                                                       const NameIndexMap& name_map) const {
    for (const auto& gf : q.global_region_filters) {
        std::string struct_name;
        std::string attr_name;
        const StructuralAttr* sa_ptr = nullptr;
        std::optional<std::string> rkey;

        size_t us = gf.region_attr.find('_');
        if (us != std::string::npos && us + 1 < gf.region_attr.size()) {
            struct_name = gf.region_attr.substr(0, us);
            attr_name = gf.region_attr.substr(us + 1);
            if (!corpus_.has_structure(struct_name)) return false;
            sa_ptr = &corpus_.structure(struct_name);
            rkey = resolve_region_attr_key(*sa_ptr, struct_name, attr_name);
        } else if (!gf.anchor_name.empty()) {
            auto nr = m.named_regions.find(gf.anchor_name);
            if (nr != m.named_regions.end() && corpus_.has_structure(nr->second.struct_name)) {
                struct_name = nr->second.struct_name;
                attr_name = gf.region_attr;
                sa_ptr = &corpus_.structure(struct_name);
                rkey = resolve_region_attr_key(*sa_ptr, struct_name, attr_name);
            } else {
                CorpusPos pos = m.first_pos();
                CorpusPos ap = resolve_name(m, name_map, gf.anchor_name);
                if (ap != NO_HEAD) pos = ap;
                for (const auto& sn : corpus_.structure_names()) {
                    const auto& probe_sa = corpus_.structure(sn);
                    auto probe_key = resolve_region_attr_key(probe_sa, sn, gf.region_attr);
                    if (!probe_key) continue;
                    if (probe_sa.find_region(pos) >= 0) {
                        struct_name = sn;
                        attr_name = gf.region_attr;
                        sa_ptr = &probe_sa;
                        rkey = std::move(probe_key);
                        break;
                    }
                }
            }
        } else {
            return false;
        }
        if (!sa_ptr || !rkey) return false;
        const auto& sa = *sa_ptr;

        int64_t rgn = -1;
        if (!gf.anchor_name.empty()) {
            auto nr = m.named_regions.find(gf.anchor_name);
            if (nr != m.named_regions.end()) {
                if (nr->second.struct_name != struct_name) return false;
                rgn = static_cast<int64_t>(nr->second.region_idx);
            }
        }
        if (rgn < 0) {
            CorpusPos pos = m.first_pos();
            if (!gf.anchor_name.empty()) {
                CorpusPos ap = resolve_name(m, name_map, gf.anchor_name);
                if (ap != NO_HEAD) pos = ap;
            }
            rgn = sa.find_region(pos);
            if (rgn < 0) return false;
        }
        const std::string composite = struct_name + "_" + *rkey;
        const bool is_mv = corpus_.is_multivalue(composite);
        if (gf.op == CompOp::EQ && sa.has_region_value_reverse(*rkey) && !is_mv) {
            if (sa.region_matches_attr_eq_rev(*rkey, static_cast<size_t>(rgn), gf.value))
                continue;
        }
        std::string_view rval = sa.region_value(*rkey, static_cast<size_t>(rgn));
        if (!compare_value_maybe_mv(gf.op, rval, gf.value, is_mv)) return false;
    }
    return true;
}

MatchSet QueryExecutor::execute_region_enumeration(const std::vector<AnchorConstraint>& anchor_constraints,
                                                   const TokenQuery& q,
                                                   size_t max_matches,
                                                   bool count_total,
                                                   size_t max_total_cap,
                                                   size_t sample_size,
                                                   uint32_t random_seed) const {
    MatchSet result;
    result.num_tokens = 0;
    result.total_exact = true;

    const AnchorConstraint* enum_ac = nullptr;
    for (const auto& ac : anchor_constraints) {
        if (ac.region_enumeration && !ac.binding_name.empty()) {
            enum_ac = &ac;
            break;
        }
    }
    if (!enum_ac)
        return result;
    if (!corpus_.has_structure(enum_ac->region)) {
        throw std::runtime_error("Unknown region anchor '<" + enum_ac->region
                                 + ">' in labeled region enumeration");
    }

    NameIndexMap name_map;
    const auto& sa = corpus_.structure(enum_ac->region);
    const size_t nreg = sa.region_count();

    HitSample sampler(sample_size, random_seed);

    auto reached_total_cap = [&]() {
        return count_total && max_total_cap > 0 && result.total_count >= max_total_cap;
    };

    for (size_t ri = 0; ri < nreg; ++ri) {
        if (reached_total_cap()) break;

        bool attr_ok = true;
        for (const auto& [key, val] : enum_ac->attrs) {
            const std::string wanted_val = val;
            auto rk = resolve_region_attr_key(sa, enum_ac->region, key);
            if (rk) {
                if (std::string(sa.region_value(*rk, ri)) != wanted_val) {
                    attr_ok = false;
                    break;
                }
                continue;
            }

            // Super-region fallback for region-only anchor enumeration
            // (e.g. b:<s text_lang="Dutch"> where language lives on text).
            auto super_val = lookup_super_region_attr_value(
                corpus_, sa, enum_ac->region, ri, key);
            if (!super_val || *super_val != wanted_val) {
                attr_ok = false;
                break;
            }
        }
        if (!attr_ok) continue;

        Region reg = sa.get(ri);
        Match m;
        m.positions.push_back(reg.start);
        m.span_ends.push_back(reg.end);
        m.named_regions[enum_ac->binding_name] = RegionRef{enum_ac->region, ri};

        if (!q.global_region_filters.empty() && !match_passes_inline_region_filters(m, q, name_map))
            continue;

        ++result.total_count;
        if (sample_size > 0) {
            sampler.offer(std::move(m));
        } else if (max_matches == 0 || result.matches.size() < max_matches) {
            result.matches.push_back(std::move(m));
        }

        if (!count_total && max_matches > 0 && result.matches.size() >= max_matches)
            break;
    }

    if (sample_size > 0) result.matches = sampler.take();

    if (count_total && max_total_cap > 0 && result.total_count > max_total_cap)
        result.total_exact = false;

    apply_within_having(q, result);
    apply_not_within(q, result);
    apply_containing(q, result);
    apply_position_orders(q, name_map, result);
    apply_global_filters(q, name_map, result);
    return result;
}

namespace {
// Evaluate attrs + anchor peer-clauses for a candidate row; see `expand_anchor_constraints`.
struct AnchorRowCheck {
    const Corpus& corpus;
    const StructuralAttr& sa;
    const std::string& region_name;
    const std::vector<std::pair<std::string, std::string>>& attrs;
    const std::vector<AnchorRegionClause>& anchor_region_clauses;
    const Match& m;
    bool attrs_match(size_t ri) const {
        Region cur = sa.get(ri);
        for (const auto& [key, val] : attrs) {
            const std::string wanted_val = val;
            auto rk = resolve_region_attr_key(sa, region_name, key);
            if (rk) {
                if (sa.region_value(*rk, ri) != wanted_val) return false;
                continue;
            }

            // Super-region fallback (e.g. <s text_lang="Dutch"> resolves via
            // containing text.text_lang when s itself has no lang attr).
            auto super_val = lookup_super_region_attr_value(
                corpus, sa, region_name, ri, key);
            if (!super_val || *super_val != wanted_val) return false;
        }
        return true;
    }
    bool clauses_ok(size_t ri) const {
        Region cur = sa.get(ri);
        for (const auto& cl : anchor_region_clauses) {
            if (cl.kind == AnchorRegionClauseKind::RchildOf) {
                auto pit = m.named_regions.find(cl.peer_label);
                if (pit == m.named_regions.end()) return false;
                const RegionRef& pr = pit->second;
                if (pr.struct_name != region_name) return false;
                if (!sa.has_parent_region_id()) return false;
                int32_t par = sa.parent_region_id(ri);
                if (par < 0 || static_cast<size_t>(par) != pr.region_idx) return false;
            } else if (cl.kind == AnchorRegionClauseKind::Contains) {
                auto inner = named_region_span(corpus, m, cl.peer_label);
                if (!inner) return false;
                if (!region_span_contains(cur, *inner)) return false;
            }
        }
        return true;
    }
};
} // namespace

size_t QueryExecutor::expand_anchor_constraints(
        const Match& base,
        const std::vector<AnchorConstraint>& constraints,
        const std::function<bool(Match&)>& emit) const {

    size_t emitted = 0;
    bool stop = false;

    // Recursive Cartesian expansion over `constraints` in order. Peer clauses
    // (rchild / contains) look up `m.named_regions`, so earlier bindings must be
    // established before later constraints are evaluated.
    std::function<void(size_t, Match&)> recurse = [&](size_t idx, Match& m) {
        if (stop) return;
        if (idx == constraints.size()) {
            Match out = m;
            ++emitted;
            if (!emit(out)) stop = true;
            return;
        }
        const auto& ac = constraints[idx];
        if (ac.region_enumeration) { recurse(idx + 1, m); return; }
        if (ac.token_idx >= m.positions.size()) return;
        CorpusPos pos = ac.is_start ? m.positions[ac.token_idx]
                                    : (ac.token_idx < m.span_ends.size()
                                       ? m.span_ends[ac.token_idx]
                                       : m.positions[ac.token_idx]);
        if (!corpus_.has_structure(ac.region)) return;
        const auto& sa = corpus_.structure(ac.region);
        AnchorRowCheck chk{corpus_, sa, ac.region, ac.attrs, ac.anchor_region_clauses, m};

        const bool multi = corpus_.is_nested(ac.region) || corpus_.is_overlapping(ac.region)
                           || corpus_.is_zerowidth(ac.region);

        auto try_bind = [&](size_t ri) {
            if (stop) return;
            if (!chk.attrs_match(ri)) return;
            if (!chk.clauses_ok(ri)) return;
            if (!ac.binding_name.empty()) {
                auto prev = m.named_regions.find(ac.binding_name);
                bool had = prev != m.named_regions.end();
                RegionRef saved{};
                if (had) saved = prev->second;
                m.named_regions[ac.binding_name] = RegionRef{ac.region, ri};
                recurse(idx + 1, m);
                if (had) m.named_regions[ac.binding_name] = saved;
                else m.named_regions.erase(ac.binding_name);
            } else {
                recurse(idx + 1, m);
            }
        };

        if (!multi) {
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0) return;
            Region reg = sa.get(static_cast<size_t>(rgn));
            if (ac.is_start ? (pos != reg.start) : (pos != reg.end)) return;
            try_bind(static_cast<size_t>(rgn));
            return;
        }

        // Multi (nested / overlapping / zerowidth): several rows can share `pos`.
        // Fanout mode: emit every row satisfying attrs + clauses.
        // Innermost mode: pick tightest (smallest span; tiebreak on region_idx).
        if (anchor_binding_mode_ == AnchorBindingMode::Innermost) {
            int64_t best_ri = -1;
            Region best_reg{};
            auto consider = [&](size_t ri) -> bool {
                if (!chk.attrs_match(ri) || !chk.clauses_ok(ri)) return true;
                Region cur = sa.get(ri);
                if (best_ri < 0) { best_ri = static_cast<int64_t>(ri); best_reg = cur; return true; }
                bool better = false;
                if (ac.is_start) {
                    if (cur.end < best_reg.end) better = true;
                    else if (cur.end == best_reg.end &&
                             static_cast<int64_t>(ri) < best_ri) better = true;
                } else {
                    if (cur.start > best_reg.start) better = true;
                    else if (cur.start == best_reg.start &&
                             static_cast<int64_t>(ri) < best_ri) better = true;
                }
                if (better) { best_ri = static_cast<int64_t>(ri); best_reg = cur; }
                return true;
            };
            if (ac.is_start) sa.for_each_region_starting_at(pos, consider);
            else             sa.for_each_region_ending_at(pos, consider);
            if (best_ri >= 0) try_bind(static_cast<size_t>(best_ri));
            return;
        }

        // Fanout: materialize candidate ids first so recursion doesn't
        // interleave with the StructuralAttr enumerator.
        std::vector<size_t> cands;
        auto collect = [&](size_t ri) -> bool { cands.push_back(ri); return true; };
        if (ac.is_start) sa.for_each_region_starting_at(pos, collect);
        else             sa.for_each_region_ending_at(pos, collect);
        for (size_t ri : cands) {
            if (stop) break;
            try_bind(ri);
        }
    };

    Match cur = base;
    cur.named_regions.clear();
    recurse(0, cur);
    return emitted;
}

bool QueryExecutor::resolve_anchor_constraints(Match& m,
                                               const std::vector<AnchorConstraint>& constraints) const {
    if (constraints.empty()) return true;
    bool ok = false;
    Match captured;
    expand_anchor_constraints(m, constraints, [&](Match& r) {
        captured = std::move(r);
        ok = true;
        return false; // single resolution is enough
    });
    if (!ok) return false;
    m.named_regions = std::move(captured.named_regions);
    return true;
}

void QueryExecutor::apply_anchor_filters(const std::vector<AnchorConstraint>& constraints,
                                         MatchSet& result) const {
    if (constraints.empty()) return;

    std::vector<Match> kept;
    kept.reserve(result.matches.size());

    // RG-REG-5: fan out one kept Match per valid anchor-binding assignment.
    for (const auto& m : result.matches) {
        expand_anchor_constraints(m, constraints, [&](Match& r) {
            kept.push_back(std::move(r));
            return true;
        });
    }

    // add_match() already applied the anchor constraints to every hit, including
    // those only counted past the page (--total / --count-only), so adjust the total
    // by what changed on the page instead of resetting it to the page size.
    const size_t before = result.matches.size();
    result.matches = std::move(kept);
    if (result.total_count >= before)
        result.total_count = result.total_count - before + result.matches.size();
    else
        result.total_count = result.matches.size();
}

bool QueryExecutor::passes_within_having(const TokenQuery& query, const Match& m) const {
    const auto& sa = corpus_.structure(query.within);
    const bool span_semantics = corpus_.is_nested(query.within) ||
                                corpus_.is_overlapping(query.within);
    if (!span_semantics) {
        CorpusPos pos = m.first_pos();
        int64_t rgn = sa.find_region(pos);
        if (rgn < 0) return false;
        // hits come in corpus order, many per region (P1.12: one at a time):
        // the last region's answer is reused
        if (wh_cache_.cond == query.within_having.get() && wh_cache_.sa == &sa
            && wh_cache_.rgn == rgn)
            return wh_cache_.found;
        Region reg = sa.get(static_cast<size_t>(rgn));
        bool found = false;
        for (CorpusPos p = reg.start; p <= reg.end; ++p) {
            if (check_conditions(p, query.within_having)) {
                found = true;
                break;
            }
        }
        wh_cache_ = {query.within_having.get(), &sa, rgn, found};
        return found;
    }

    auto ext = match_extent_from_match(m);
    if (!ext) return false;
    CorpusPos ms = ext->first;
    CorpusPos me = ext->second;

    bool found = false;
    sa.for_each_region_at(ms, [&](size_t rgn_idx) -> bool {
        Region reg = sa.get(rgn_idx);
        if (ms < reg.start || me > reg.end) return true;
        for (CorpusPos p = reg.start; p <= reg.end; ++p) {
            if (check_conditions(p, query.within_having)) {
                found = true;
                return false;
            }
        }
        return true;
    });
    return found;
}

bool QueryExecutor::within_having_active(const TokenQuery& query) const {
    return query.within_having && !query.within.empty() && corpus_.has_structure(query.within);
}

bool QueryExecutor::not_within_active(const TokenQuery& query) const {
    return query.not_within && !query.within.empty() && corpus_.has_structure(query.within);
}

void QueryExecutor::apply_within_having(const TokenQuery& query, MatchSet& result) const {
    if (!within_having_active(query)) return;
    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (auto& m : result.matches)
        if (passes_within_having(query, m)) kept.push_back(std::move(m));
    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

// ── Containing / not-within / position-order filters ─────────────────

bool QueryExecutor::passes_containing(const TokenQuery& query, const Match& m) const {
    CorpusPos ms = m.first_pos();
    CorpusPos me = m.last_pos();
    for (const auto& cc : query.containing_clauses) {
        bool found = false;

        if (cc.is_subtree) {
            // Dependency subtree containment: find a token in [ms, me] matching
            // cc.subtree_cond whose full subtree is also within [ms, me].
            if (!corpus_.has_deps()) { found = false; }
            else {
                const auto& deps = corpus_.deps();
                for (CorpusPos p = ms; p <= me; ++p) {
                    if (!check_conditions(p, cc.subtree_cond)) continue;
                    auto sub = deps.subtree(p);
                    bool all_inside = true;
                    for (CorpusPos sp : sub) {
                        if (sp < ms || sp > me) { all_inside = false; break; }
                    }
                    if (all_inside) { found = true; break; }
                }
            }
        } else {
            // Structural region containment: check if any region of the
            // specified type has both start and end within [ms, me].
            if (!corpus_.has_structure(cc.region)) { found = false; }
            else {
                const auto& sa = corpus_.structure(cc.region);
                size_t count = sa.region_count();
                // Linear scan from the region containing ms
                int64_t rgn = sa.find_region(ms);
                if (rgn < 0) rgn = 0;
                for (size_t r = static_cast<size_t>(rgn); r < count; ++r) {
                    Region reg = sa.get(r);
                    if (reg.start > me) break;  // past match end
                    if (reg.start >= ms && reg.end <= me) {
                        found = true;
                        break;
                    }
                }
            }
        }

        if (cc.negated) found = !found;
        if (!found) return false;
    }
    return true;
}

void QueryExecutor::apply_containing(const TokenQuery& query, MatchSet& result) const {
    if (query.containing_clauses.empty()) return;
    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (auto& m : result.matches)
        if (passes_containing(query, m)) kept.push_back(std::move(m));
    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

bool QueryExecutor::passes_not_within(const TokenQuery& query, const Match& m) const {
    const auto& sa = corpus_.structure(query.within);
    const bool span_semantics = corpus_.is_nested(query.within) ||
                                corpus_.is_overlapping(query.within);
    auto ext = match_extent_from_match(m);
    if (!ext) return false;
    CorpusPos ms = ext->first;
    CorpusPos me = ext->second;

    if (!span_semantics) {
        // Flat: at most one region per position; find_region(ms) is the only
        // candidate that could contain the full span.
        int64_t rgn = sa.find_region(ms);
        if (rgn < 0) return true;
        Region reg = sa.get(static_cast<size_t>(rgn));
        return !(reg.start <= ms && me <= reg.end);
    }
    return !within_span_in_some_region(sa, ms, me);
}

void QueryExecutor::apply_not_within(const TokenQuery& query, MatchSet& result) const {
    if (!not_within_active(query)) return;
    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (auto& m : result.matches)
        if (passes_not_within(query, m)) kept.push_back(std::move(m));
    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

bool QueryExecutor::passes_position_orders(const TokenQuery& query, const NameIndexMap& name_map,
                                           const Match& m) const {
    for (const auto& po : query.position_orders) {
        CorpusPos p1 = resolve_name(m, name_map, po.name1);
        CorpusPos p2 = resolve_name(m, name_map, po.name2);
        if (p1 == NO_HEAD || p2 == NO_HEAD) return false;
        bool ok = false;
        switch (po.op) {
            case CompOp::LT:  ok = (p1 < p2); break;
            case CompOp::GT:  ok = (p1 > p2); break;
            default: ok = (p1 < p2); break;
        }
        if (!ok) return false;
    }
    return true;
}

void QueryExecutor::apply_position_orders(const TokenQuery& query, const NameIndexMap& name_map, MatchSet& result) const {
    if (query.position_orders.empty()) return;
    std::vector<Match> kept;
    kept.reserve(result.matches.size());
    for (auto& m : result.matches)
        if (passes_position_orders(query, name_map, m)) kept.push_back(std::move(m));
    result.matches = std::move(kept);
    result.total_count = result.matches.size();
}

/// P1.12: every post-filter on one hit, in apply order (m is moved through the
/// set-based `::` filters and back when it survives).
bool QueryExecutor::passes_post_filters(const TokenQuery& query, const NameIndexMap& name_map,
                                        Match& m, MatchSet& scratch) const {
    if (within_having_active(query) && !passes_within_having(query, m)) return false;
    if (not_within_active(query) && !passes_not_within(query, m)) return false;
    if (!query.containing_clauses.empty() && !passes_containing(query, m)) return false;
    if (!query.position_orders.empty() && !passes_position_orders(query, name_map, m)) return false;
    if (query.global_alignment_filters.empty() && query.global_function_filters.empty())
        return true;   // (region filters: already inline)
    scratch.matches.clear();
    scratch.matches.push_back(std::move(m));
    scratch.total_count = 1;
    apply_global_filters(query, name_map, scratch);
    if (scratch.matches.empty()) return false;
    m = std::move(scratch.matches.front());
    return true;
}

// ── Set operations ──────────────────────────────────────────────────────

// Galloping (exponential) search: find first element >= target in [lo, hi).
// Returns iterator to the first element >= target, or hi if none.
static auto gallop(std::vector<CorpusPos>::const_iterator lo,
                   std::vector<CorpusPos>::const_iterator hi,
                   CorpusPos target) {
    // Exponential jump to find bracket
    size_t step = 1;
    auto it = lo;
    while (it < hi && *it < target) {
        lo = it;
        it += static_cast<ptrdiff_t>(step);
        step <<= 1;
    }
    if (it > hi) it = hi;
    // Binary search within bracket [lo, it)
    return std::lower_bound(lo, it, target);
}

std::vector<CorpusPos> QueryExecutor::intersect(
        const std::vector<CorpusPos>& a,
        const std::vector<CorpusPos>& b) {
    std::vector<CorpusPos> out;
    if (a.empty() || b.empty()) return out;

    // When one list is >8× larger, galloping is faster than linear merge.
    // O(|small| × log(|large| / |small|)) vs O(|small| + |large|).
    const auto& small = (a.size() <= b.size()) ? a : b;
    const auto& large = (a.size() <= b.size()) ? b : a;

    if (large.size() > 8 * small.size()) {
        out.reserve(small.size());
        auto lo = large.begin();
        for (CorpusPos val : small) {
            lo = gallop(lo, large.end(), val);
            if (lo == large.end()) break;
            if (*lo == val) {
                out.push_back(val);
                ++lo;
            }
        }
    } else {
        // Balanced sizes: standard two-pointer merge
        auto ia = a.begin(), ib = b.begin();
        while (ia != a.end() && ib != b.end()) {
            if (*ia == *ib) { out.push_back(*ia); ++ia; ++ib; }
            else if (*ia < *ib) ++ia;
            else ++ib;
        }
    }
    return out;
}

std::vector<CorpusPos> QueryExecutor::unite(
        const std::vector<CorpusPos>& a,
        const std::vector<CorpusPos>& b) {
    std::vector<CorpusPos> out;
    std::merge(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(out));
    auto it = std::unique(out.begin(), out.end());
    out.erase(it, out.end());
    return out;
}

void sort_matches_by_position(std::vector<Match>& matches) {
    std::stable_sort(matches.begin(), matches.end(), [](const Match& a, const Match& b) {
        const CorpusPos fa = a.first_pos(), fb = b.first_pos();
        if (fa != fb) return fa < fb;
        const CorpusPos la = a.last_pos(), lb = b.last_pos();
        if (la != lb) return la < lb;
        return a.positions < b.positions;
    });
}


} // namespace pando
