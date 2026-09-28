// Full-program session API: run_program_json()
// Runs a complete CQL program (queries + commands) and returns JSON output.
// Lives in the pando_api library so both pando CLI and pando-server can use it.

#include "api/query_json.h"
#include "api/group_counts.h"
#include "core/json_utils.h"
#include "core/count_hierarchy_json.h"
#include "query/parser.h"
#include "query/executor.h"
#include "index/positional_attr.h"
#include "index/dependency_index.h"

#include <iostream>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <cmath>
#include <iomanip>
#include <numeric>
#include <stdexcept>

namespace pando {

// ── Session impl: hit sets (P6.1) ───────────────────────────────────────

namespace {

using SessionClock = std::chrono::steady_clock;

// Hits without per-hit extras, flat: hit i is pos[i*stride, +stride) (and ends).
struct CompactHits {
    size_t n = 0;
    size_t stride = 0;
    bool has_ends = false;
    std::vector<CorpusPos> pos, ends;

    static bool fits(const std::vector<Match>& v, size_t& stride, bool& has_ends) {
        stride = v.empty() ? 0 : v[0].positions.size();
        has_ends = !v.empty() && !v[0].span_ends.empty();
        for (const Match& m : v) {
            if (m.positions.size() != stride) return false;
            if (has_ends ? m.span_ends.size() != stride : !m.span_ends.empty()) return false;
            if (!m.named_regions.empty() || !m.named_dep_subtrees.empty() || !m.token_group_props.empty()
                || m.token_group_match)
                return false;
        }
        return true;
    }
    static std::optional<CompactHits> from(const std::vector<Match>& v) {
        CompactHits c;
        if (!fits(v, c.stride, c.has_ends)) return std::nullopt;
        c.n = v.size();
        c.pos.reserve(c.n * c.stride);
        if (c.has_ends) c.ends.reserve(c.n * c.stride);
        for (const Match& m : v) {
            c.pos.insert(c.pos.end(), m.positions.begin(), m.positions.end());
            if (c.has_ends) c.ends.insert(c.ends.end(), m.span_ends.begin(), m.span_ends.end());
        }
        return c;
    }
    void fill(size_t i, Match& m) const {
        const auto b = pos.begin() + static_cast<std::ptrdiff_t>(i * stride);
        m.positions.assign(b, b + static_cast<std::ptrdiff_t>(stride));
        if (has_ends) {
            const auto e = ends.begin() + static_cast<std::ptrdiff_t>(i * stride);
            m.span_ends.assign(e, e + static_cast<std::ptrdiff_t>(stride));
        } else {
            m.span_ends.clear();
        }
    }
    std::vector<Match> expand(size_t from, size_t to) const {
        std::vector<Match> out(to > from ? to - from : 0);
        for (size_t i = from; i < to; ++i) fill(i, out[i - from]);
        return out;
    }
    void permute(const std::vector<size_t>& idx) {
        std::vector<CorpusPos> p2, e2;
        p2.reserve(pos.size());
        if (has_ends) e2.reserve(ends.size());
        for (size_t i : idx) {
            p2.insert(p2.end(), pos.begin() + static_cast<std::ptrdiff_t>(i * stride),
                      pos.begin() + static_cast<std::ptrdiff_t>((i + 1) * stride));
            if (has_ends)
                e2.insert(e2.end(), ends.begin() + static_cast<std::ptrdiff_t>(i * stride),
                          ends.begin() + static_cast<std::ptrdiff_t>((i + 1) * stride));
        }
        pos = std::move(p2);
        ends = std::move(e2);
    }
    size_t bytes() const { return sizeof(*this) + (pos.capacity() + ends.capacity()) * sizeof(CorpusPos); }
};

size_t match_heap_bytes(const Match& m) {
    size_t x = (m.positions.capacity() + m.span_ends.capacity()) * sizeof(CorpusPos);
    x += m.named_regions.size() * 96;
    for (const auto& kv : m.named_dep_subtrees) x += 96 + kv.second.capacity() * sizeof(CorpusPos);
    x += m.token_group_props.capacity() * sizeof(std::pair<std::string, std::string>);
    return x;
}

size_t estimate_bytes(const MatchSet& ms) {
    size_t b = sizeof(MatchSet) + ms.matches.capacity() * sizeof(Match)
             + ms.parallel_matches.capacity() * sizeof(std::pair<Match, Match>);
    for (const auto& m : ms.matches) b += match_heap_bytes(m);
    for (const auto& pm : ms.parallel_matches) b += match_heap_bytes(pm.first) + match_heap_bytes(pm.second);
    return b;
}

// Bytes `n` hits of a `tokens`-token query take as Match objects (+ malloc overhead).
size_t transient_bytes(size_t n, size_t tokens) {
    return n * (sizeof(Match) + 2 * (tokens * sizeof(CorpusPos) + 32));
}

// One stored result: the recipe (query AST + sort steps) and, once a command
// needed every hit, the materialised hits (compact when they are plain).
struct HitSet {
    std::shared_ptr<Program> prog;   // keeps the AST alive (compile state is corpus-bound)
    size_t stmt = 0;                 // the query statement in *prog
    std::string text;                // the /query text (empty for /run statements)
    std::string display;             // shown in results / info
    bool allow_empty_alignment = false;
    std::vector<std::vector<std::string>> sorts;
    NameIndexMap nm, tnm;
    bool total_known = false;
    size_t total = 0;
    bool total_exact = false;
    bool materialised = false;
    std::optional<CompactHits> compact;   // the hits, when plain …
    MatchSet ms;                          // … otherwise here (parallel: parallel_matches)
    size_t bytes = 0;
    SessionClock::time_point last_used = SessionClock::now();

    const Statement& st() const { return (*prog)[stmt]; }
    bool parallel() const { return st().is_parallel; }
    size_t hit_count() const {
        if (compact) return compact->n;
        return parallel() ? ms.parallel_matches.size() : ms.matches.size();
    }
    void touch() { last_used = SessionClock::now(); }
    void know_total(size_t n, bool exact) {
        if (total_known && total_exact && !exact) return;   // an exact total wins
        total_known = true;
        total = n;
        total_exact = exact;
    }
    // Keep every hit (`all` holds all of them, in the set's order).
    void keep(MatchSet&& all) {
        const size_t n = parallel() ? all.parallel_matches.size() : all.matches.size();
        know_total(n, true);
        compact.reset();
        if (!parallel()) compact = CompactHits::from(all.matches);
        if (compact) {
            ms = MatchSet{};
            ms.plan_path = all.plan_path;
            bytes = compact->bytes();
        } else {
            all.total_count = n;
            all.total_exact = true;
            all.aggregate_buckets.reset();
            ms = std::move(all);
            bytes = estimate_bytes(ms);
        }
        materialised = true;
    }
    void drop() {
        compact.reset();
        ms = MatchSet{};
        materialised = false;
        bytes = 0;
    }
    // Every hit as a MatchSet (compact sets are expanded into `tmp`).
    const MatchSet& full_view(std::optional<MatchSet>& tmp) const {
        if (!compact) return ms;
        tmp.emplace();
        tmp->matches = compact->expand(0, compact->n);
        tmp->total_count = compact->n;
        tmp->total_exact = true;
        tmp->plan_path = ms.plan_path;
        return *tmp;
    }
    // `sort … by fields` on the kept hits (stable; the order sort_matches_by_key gives).
    void sort_by(const Corpus& corpus, const std::vector<std::string>& fields) {
        if (!compact) {
            sort_matches_by_key(corpus, ms.matches, nm, fields);
            return;
        }
        std::vector<std::string> keys(compact->n);
        Match m;
        for (size_t i = 0; i < compact->n; ++i) {
            compact->fill(i, m);
            keys[i] = make_group_key(corpus, m, nm, fields);
        }
        std::vector<size_t> idx(compact->n);
        std::iota(idx.begin(), idx.end(), size_t{0});
        std::stable_sort(idx.begin(), idx.end(),
                         [&](size_t a, size_t b) { return compare_group_keys(keys[a], keys[b]); });
        compact->permute(idx);
        bytes = compact->bytes();
    }
    // /query JSON for a page of the kept hits.
    std::string page(const Corpus& corpus, const QueryOptions& opts, std::string_view extra) {
        if (!compact) {
            ms.total_count = hit_count();
            ms.total_exact = true;
            return to_query_result_json(corpus, display, ms, opts, 0.0, extra);
        }
        const size_t from = std::min(opts.offset, compact->n);
        const size_t to = std::min(compact->n, from + opts.limit);
        MatchSet pm;
        pm.matches = compact->expand(from, to);
        pm.total_count = compact->n;
        pm.total_exact = true;
        pm.plan_path = ms.plan_path;
        return to_query_result_json(corpus, display, pm, opts, 0.0, extra, from);
    }
};
using HitSetPtr = std::shared_ptr<HitSet>;

}  // namespace

HitSetTooLarge::HitSetTooLarge(const std::string& name, size_t h, size_t lim)
    : std::runtime_error("hit set " + name + " has " + std::to_string(h)
                         + " hits, more than this session materialises (" + std::to_string(lim) + ")"),
      hits(h), limit(lim) {}

struct ProgramSession::Impl {
    std::map<std::string, HitSetPtr> sets;   // "Last" and the named sets
    size_t max_hits = 0;                     // 0 = no limit
    ProgramSession::AdmitFn admit;

    HitSetPtr find(const std::string& name) const {
        auto it = sets.find(name);
        return it == sets.end() ? nullptr : it->second;
    }
    void bind(const std::string& name, const HitSetPtr& hs) {
        sets["Last"] = hs;
        if (!name.empty() && name != "Last") sets[name] = hs;
    }
    HitSetInfo describe(const std::string& name, const HitSetPtr& hs) const {
        HitSetInfo i;
        i.name = name;
        i.query = hs->display;
        i.parallel = hs->parallel();
        i.materialised = hs->materialised;
        i.hits = hs->materialised ? hs->hit_count() : 0;
        i.total_known = hs->total_known;
        i.total = hs->total;
        i.total_exact = hs->total_exact;
        i.bytes = hs->materialised ? hs->bytes : 0;
        i.sorts = hs->sorts;
        for (const auto& [n, other] : sets)
            if (other == hs && n != name) i.aliases.push_back(n);
        i.idle_s = std::chrono::duration<double>(SessionClock::now() - hs->last_used).count();
        return i;
    }
};

ProgramSession::ProgramSession() : impl_(std::make_unique<Impl>()) {}
ProgramSession::~ProgramSession() = default;
ProgramSession::ProgramSession(ProgramSession&&) noexcept = default;
ProgramSession& ProgramSession::operator=(ProgramSession&&) noexcept = default;

namespace {

void setup_executor(QueryExecutor& ex, const HitSet& hs, ExecProgress* progress) {
    ex.set_include_empty_alignment_values(hs.allow_empty_alignment);
    if (progress) ex.set_progress(progress);
}

// The exact total of a set (counted once, then known).
size_t set_total_count(const Corpus& corpus, HitSet& hs, unsigned threads, ExecProgress* progress) {
    hs.touch();
    if (hs.total_known && hs.total_exact) return hs.total;
    if (hs.materialised) { hs.know_total(hs.hit_count(), true); return hs.total; }
    QueryExecutor ex(corpus);
    setup_executor(ex, hs, progress);
    const Statement& st = hs.st();
    MatchSet ms = st.is_parallel ? ex.execute_parallel(st.query, st.target_query, 1, true)
                                 : ex.execute(st.query, 1, true, 0, 0, 0, std::max(1u, threads));
    hs.know_total(ms.total_count, ms.total_exact);
    return hs.total;
}

// Every hit of `hs` in memory (the query again, then its sort steps). The total
// is counted first when unknown, so the size limit and the admission (the Match
// objects exist until they are compacted) are decided before anything is built.
void materialise(const Corpus& corpus, HitSet& hs, const std::string& name, const ProgramSession::Impl& S,
                 unsigned threads, ExecProgress* progress) {
    hs.touch();
    if (hs.materialised) return;
    const size_t total = set_total_count(corpus, hs, threads, progress);
    if (S.max_hits && total > S.max_hits) throw HitSetTooLarge(name, total, S.max_hits);
    std::shared_ptr<void> token;
    const Statement& st = hs.st();
    if (S.admit) {
        const size_t toks = std::max<size_t>(1, st.query.tokens.size() + (st.is_parallel ? st.target_query.tokens.size() : 0));
        token = S.admit(transient_bytes(total, toks), progress);
    }
    QueryExecutor ex(corpus);
    setup_executor(ex, hs, progress);
    MatchSet ms = st.is_parallel
        ? ex.execute_parallel(st.query, st.target_query, 0, false)
        : ex.execute(st.query, 0, true, 0, 0, 0, std::max(1u, threads));
    for (const auto& keys : hs.sorts)
        sort_matches_by_key(corpus, ms.matches, hs.nm, keys);
    hs.keep(std::move(ms));
}

// `count / freq … by fields` over a set without its hits: the aggregation sink.
MatchSet aggregate(const Corpus& corpus, HitSet& hs, const std::vector<std::string>& fields,
                   unsigned threads, ExecProgress* progress) {
    hs.touch();
    QueryExecutor ex(corpus);
    setup_executor(ex, hs, progress);
    MatchSet ms = ex.execute(hs.st().query, 0, true, 0, 0, 0, std::max(1u, threads), &fields);
    if (ms.aggregate_buckets) hs.know_total(ms.aggregate_buckets->total_hits, true);
    else if (ms.total_exact) hs.know_total(ms.total_count, true);
    return ms;
}

}  // namespace

// ── ProgramSession: hit-set API ─────────────────────────────────────────

void ProgramSession::store_query(const Corpus& corpus, const std::string& name, const std::string& query_text,
                                 const QueryOptions& opts, const MatchSet& result) {
    (void)corpus;
    Parser parser(query_text, ParserOptions{opts.strict_quoted_strings});
    auto prog = std::make_shared<Program>(parser.parse());
    size_t si = 0;
    while (si < prog->size() && !(*prog)[si].has_query) ++si;
    if (si == prog->size()) throw std::runtime_error("not a query: " + query_text);
    auto hs = std::make_shared<HitSet>();
    hs->prog = prog;
    hs->stmt = si;
    hs->text = query_text;
    hs->display = query_text;
    hs->allow_empty_alignment = opts.allow_empty_alignment;
    const Statement& st = hs->st();
    hs->nm = st.is_parallel ? build_name_map(st.query) : QueryExecutor::build_name_map_for_stripped_query(st.query);
    hs->tnm = st.is_parallel ? build_name_map(st.target_query) : NameIndexMap{};
    if (opts.total && result.total_exact) {
        hs->know_total(result.total_count, true);
        // the page held every hit (run_single_query pages from 0): keep them
        if (!st.is_parallel && result.matches.size() == result.total_count && !result.aggregate_buckets) {
            MatchSet all = result;
            hs->keep(std::move(all));
        }
    }
    impl_->bind(name, hs);
}

bool ProgramSession::has(const std::string& name) const { return impl_->find(name) != nullptr; }

std::optional<HitSetInfo> ProgramSession::info(const std::string& name) const {
    auto hs = impl_->find(name);
    if (!hs) return std::nullopt;
    return impl_->describe(name, hs);
}

std::vector<HitSetInfo> ProgramSession::list() const {
    std::vector<HitSetInfo> out;
    for (const auto& [n, hs] : impl_->sets) out.push_back(impl_->describe(n, hs));
    return out;
}

std::string ProgramSession::query_text(const std::string& name) const {
    auto hs = impl_->find(name);
    return hs ? hs->text : std::string();
}

void ProgramSession::set_total(const std::string& name, size_t total, bool exact) {
    if (auto hs = impl_->find(name)) hs->know_total(total, exact);
}

std::string ProgramSession::page_json(const Corpus& corpus, const std::string& name, const QueryOptions& opts,
                                      ExecProgress* progress, std::string_view extra_result_fields,
                                      std::optional<std::pair<size_t, bool>> shown_total) {
    auto hs = impl_->find(name);
    if (!hs) throw UnknownHitSet(name);
    hs->touch();
    const unsigned threads = std::max(1u, opts.threads);
    if (!hs->materialised && (!hs->sorts.empty() || hs->parallel()))
        materialise(corpus, *hs, name, *impl_, threads, progress);
    // A deep page of a lazy set: materialise once (every later page is a slice)
    // instead of running the query to offset+limit again for each page.
    constexpr size_t kDeepPage = 10000;
    if (!hs->materialised && opts.offset >= kDeepPage && opts.limit > 0) {
        try {
            materialise(corpus, *hs, name, *impl_, threads, progress);
        } catch (const HitSetTooLarge&) {
            // too many to keep: page by running the query (below)
        }
    }
    if (hs->materialised) return hs->page(corpus, opts, extra_result_fields);
    if (opts.limit == 0) {   // the total only (a limit-0 run would materialise every hit)
        MatchSet ms;
        if (opts.total && !shown_total) set_total_count(corpus, *hs, threads, progress);
        if (hs->total_known) { ms.total_count = hs->total; ms.total_exact = hs->total_exact; }
        else if (shown_total) { ms.total_count = shown_total->first; ms.total_exact = shown_total->second; }
        else ms.total_exact = false;
        return to_query_result_json(corpus, hs->display, ms, opts, 0.0, extra_result_fields);
    }
    // not materialised, not sorted: the page of the query itself
    QueryExecutor ex(corpus);
    setup_executor(ex, *hs, progress);
    const bool count = opts.total && !(hs->total_known && hs->total_exact) && !shown_total;
    const size_t cap = (count && opts.max_total > 0) ? opts.max_total : 0;
    auto t0 = std::chrono::high_resolution_clock::now();
    MatchSet ms = ex.execute(hs->st().query, opts.offset + opts.limit, count, cap, 0, 0, threads);
    const double elapsed = std::chrono::duration<double, std::milli>(
                               std::chrono::high_resolution_clock::now() - t0).count();
    if (count && ms.total_exact) hs->know_total(ms.total_count, true);
    if (hs->total_known && (opts.total || hs->total_exact)) {
        ms.total_count = hs->total;
        ms.total_exact = hs->total_exact;
    } else if (shown_total) {
        ms.total_count = std::max(shown_total->first, ms.matches.size());
        ms.total_exact = shown_total->second;
    }
    return to_query_result_json(corpus, hs->display, ms, opts, elapsed, extra_result_fields);
}

size_t ProgramSession::cache_bytes() const {
    std::set<const HitSet*> seen;
    size_t b = 0;
    for (const auto& kv : impl_->sets)
        if (kv.second->materialised && seen.insert(kv.second.get()).second) b += kv.second->bytes;
    return b;
}

size_t ProgramSession::drop_caches() {
    size_t freed = 0;
    std::set<const HitSet*> seen;
    for (auto& kv : impl_->sets) {
        HitSet& hs = *kv.second;
        if (!hs.materialised || !seen.insert(&hs).second) continue;
        freed += hs.bytes;
        hs.drop();
    }
    return freed;
}

void ProgramSession::set_max_hits(size_t n) { impl_->max_hits = n; }
void ProgramSession::set_admission(AdmitFn admit) { impl_->admit = std::move(admit); }
size_t ProgramSession::size() const { return impl_->sets.size(); }
void ProgramSession::clear() { impl_->sets.clear(); }

// ── Helpers ──────────────────────────────────────────────────────────────
// build_name_map is already inline in executor.h — just use it directly.

static std::string make_key(const Corpus& corpus, const Match& m,
                            const NameIndexMap& name_map,
                            const std::vector<std::string>& fields) {
    std::string key;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) key += '\t';
        key += read_tabulate_field(corpus, m, name_map, fields[i]);
    }
    return key;
}

static std::unordered_set<LexiconId> build_stoplist_ids(const PositionalAttr& pa, size_t top_n) {
    std::unordered_set<LexiconId> out;
    if (top_n == 0) return out;
    std::vector<std::pair<LexiconId, size_t>> items;
    const auto& lex = pa.lexicon();
    items.reserve(lex.size());
    for (LexiconId id = 0; id < lex.size(); ++id) {
        size_t c = pa.count_of_id(id);
        if (c == 0) continue;
        items.push_back({id, c});
    }
    std::sort(items.begin(), items.end(), [&](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return std::string(lex.get(a.first)) < std::string(lex.get(b.first));
    });
    size_t keep = std::min(top_n, items.size());
    for (size_t i = 0; i < keep; ++i) out.insert(items[i].first);
    return out;
}

enum class FreqDateTransform { None, Year, Century, Decade, Month, Week, Day };

struct FreqSubcorpusSpec {
    const StructuralAttr* sa = nullptr;
    struct FieldSpec {
        std::string region_attr;
        FreqDateTransform transform = FreqDateTransform::None;
    };
    std::vector<FieldSpec> fields;
};

static std::optional<std::string> apply_date_bucket(std::string_view raw, FreqDateTransform t) {
    if (t == FreqDateTransform::None) return std::string(raw);
    struct ParsedDateParts {
        int64_t year = 0;
        int month = 0;
        int day = 0;
        bool has_month = false;
        bool has_day = false;
    };
    auto is_leap_year = [](int64_t y) {
        return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));
    };
    auto days_in_month = [&](int64_t y, int m) {
        static const int kDays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
        if (m == 2) return is_leap_year(y) ? 29 : 28;
        return kDays[m - 1];
    };
    auto parse_date_parts_prefix = [&](std::string_view text) -> std::optional<ParsedDateParts> {
        size_t i = 0;
        while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
        if (i + 4 > text.size()) return std::nullopt;
        for (size_t k = 0; k < 4; ++k) {
            if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
                return std::nullopt;
        }
        ParsedDateParts out;
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
    };
    auto weekday_iso = [](int64_t y, int m, int d) {
        static const int tt[12] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        int64_t yy = y;
        if (m < 3) --yy;
        int w = static_cast<int>((yy + yy/4 - yy/100 + yy/400 + tt[m - 1] + d) % 7);
        if (w < 0) w += 7;
        return w == 0 ? 7 : w;
    };
    auto iso_weeks_in_year = [&](int64_t y) {
        int jan1 = weekday_iso(y, 1, 1);
        if (jan1 == 4) return 53;
        if (jan1 == 3 && is_leap_year(y)) return 53;
        return 52;
    };
    auto day_of_year = [&](int64_t y, int m, int d) {
        int doy = d;
        for (int mm = 1; mm < m; ++mm)
            doy += days_in_month(y, mm);
        return doy;
    };
    auto iso_week_number = [&](int64_t y, int m, int d) {
        int doy = day_of_year(y, m, d);
        int dow = weekday_iso(y, m, d);
        int week = (doy - dow + 10) / 7;
        if (week < 1) return iso_weeks_in_year(y - 1);
        int wiy = iso_weeks_in_year(y);
        if (week > wiy) return 1;
        return week;
    };
    auto p = parse_date_parts_prefix(raw);
    if (!p) return std::nullopt;
    switch (t) {
        case FreqDateTransform::Year:
            return std::to_string(p->year);
        case FreqDateTransform::Century:
            if (p->year <= 0) return std::nullopt;
            return std::to_string(((p->year - 1) / 100) + 1);
        case FreqDateTransform::Decade:
            return std::to_string((p->year / 10) * 10);
        case FreqDateTransform::Month:
            if (!p->has_month) return std::nullopt;
            return std::to_string(p->month);
        case FreqDateTransform::Week:
            if (!p->has_day) return std::nullopt;
            return std::to_string(iso_week_number(p->year, p->month, p->day));
        case FreqDateTransform::Day:
            if (!p->has_day) return std::nullopt;
            return std::to_string(p->day);
        case FreqDateTransform::None:
            break;
    }
    return std::nullopt;
}

static std::optional<FreqSubcorpusSpec> resolve_freq_subcorpus_spec(
        const Corpus& corpus, const std::vector<std::string>& fields) {
    if (fields.empty()) return std::nullopt;
    auto resolve_region = [&](const std::string& attr_spec, FreqDateTransform tr,
                              const StructuralAttr** expected_sa)
            -> std::optional<FreqSubcorpusSpec::FieldSpec> {
        RegionAttrParts parts;
        if (!split_region_attr_name(attr_spec, parts) || !corpus.has_structure(parts.struct_name))
            return std::nullopt;
        const auto& sa = corpus.structure(parts.struct_name);
        if (*expected_sa && *expected_sa != &sa) return std::nullopt;
        *expected_sa = &sa;
        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        if (!rkey) return std::nullopt;
        FreqSubcorpusSpec::FieldSpec fs;
        fs.region_attr = *rkey;
        fs.transform = tr;
        return fs;
    };
    auto parse_wrapped = [&](const std::string& f, const char* name)
            -> std::optional<std::string> {
        const size_t n = std::strlen(name);
        if (f.size() <= n + 2 || f.compare(0, n, name) != 0 || f[n] != '(' || f.back() != ')')
            return std::nullopt;
        std::string inner = f.substr(n + 1, f.size() - n - 2);
        if (inner.rfind("match.", 0) == 0 && inner.size() > 6)
            inner = inner.substr(6);
        return inner;
    };
    auto parse_field = [&](const std::string& f, const StructuralAttr** expected_sa)
            -> std::optional<FreqSubcorpusSpec::FieldSpec> {
        if (auto base = resolve_region(f, FreqDateTransform::None, expected_sa)) return base;
        if (auto inner = parse_wrapped(f, "year"))
            return resolve_region(*inner, FreqDateTransform::Year, expected_sa);
        if (auto inner = parse_wrapped(f, "century"))
            return resolve_region(*inner, FreqDateTransform::Century, expected_sa);
        if (auto inner = parse_wrapped(f, "decade"))
            return resolve_region(*inner, FreqDateTransform::Decade, expected_sa);
        if (auto inner = parse_wrapped(f, "month"))
            return resolve_region(*inner, FreqDateTransform::Month, expected_sa);
        if (auto inner = parse_wrapped(f, "week"))
            return resolve_region(*inner, FreqDateTransform::Week, expected_sa);
        if (auto inner = parse_wrapped(f, "day"))
            return resolve_region(*inner, FreqDateTransform::Day, expected_sa);
        return std::nullopt;
    };

    const StructuralAttr* common_sa = nullptr;
    FreqSubcorpusSpec spec;
    for (const auto& f : fields) {
        auto fs = parse_field(f, &common_sa);
        if (!fs) return std::nullopt;
        spec.fields.push_back(std::move(*fs));
    }
    spec.sa = common_sa;
    if (!spec.sa || spec.fields.empty()) return std::nullopt;
    return spec;
}

static std::unordered_map<std::string, double> build_freq_subcorpus_sizes(
        const FreqSubcorpusSpec& spec,
        const std::vector<std::string>& keys,
        double corpus_size) {
    std::unordered_map<std::string, double> out;
    if (!spec.sa) return out;

    if (spec.fields.size() == 1 && spec.fields[0].transform == FreqDateTransform::None) {
        const auto& f0 = spec.fields[0];
        for (const auto& key : keys) {
            size_t span = spec.sa->token_span_sum_for_attr_eq(f0.region_attr, key);
            out[key] = (span > 0 && span != SIZE_MAX) ? static_cast<double>(span) : corpus_size;
        }
        return out;
    }

    std::unordered_map<std::string, size_t> span_by_key;
    for (size_t ri = 0; ri < spec.sa->region_count(); ++ri) {
        std::string key;
        bool ok = true;
        for (size_t i = 0; i < spec.fields.size(); ++i) {
            const auto& fs = spec.fields[i];
            auto bucket = apply_date_bucket(spec.sa->region_value(fs.region_attr, ri), fs.transform);
            if (!bucket) { ok = false; break; }
            if (i) key.push_back('\t');
            key += *bucket;
        }
        if (!ok) continue;
        Region rg = spec.sa->get(ri);
        if (rg.end < rg.start) continue;
        span_by_key[key] += static_cast<size_t>(rg.end - rg.start + 1);
    }
    for (const auto& key : keys) {
        auto it = span_by_key.find(key);
        size_t span = (it != span_by_key.end()) ? it->second : 0;
        out[key] = (span > 0 && span != SIZE_MAX) ? static_cast<double>(span) : corpus_size;
    }
    return out;
}

static bool aggregate_command_targets_stmt(const Statement& stmt, const GroupCommand& ncmd) {
    if (ncmd.query_name.empty())
        return true;
    if (!stmt.name.empty())
        return ncmd.query_name == stmt.name;
    return ncmd.query_name == "Last";
}

// ── Association measures ────────────────────────────────────────────────

struct CollEntry {
    LexiconId id;
    std::string form;
    size_t obs;
    size_t f_coll;
    size_t f_node;
    size_t N;
};

static double compute_logdice(const CollEntry& e) {
    if (e.f_node == 0 && e.f_coll == 0) return 0;
    double d = 2.0 * static_cast<double>(e.obs) / (static_cast<double>(e.f_node) + static_cast<double>(e.f_coll));
    if (d <= 0) return 0;
    return 14.0 + log2(d);
}

static double compute_mi(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    if (expected == 0) return 0;
    return log2(static_cast<double>(e.obs) / expected);
}

static double compute_mi3(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    if (expected == 0) return 0;
    return log2(static_cast<double>(e.obs) * static_cast<double>(e.obs) * static_cast<double>(e.obs) / expected);
}

static double compute_tscore(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    return (static_cast<double>(e.obs) - expected) / sqrt(static_cast<double>(e.obs));
}

static double compute_ll(const CollEntry& e) {
    double a = static_cast<double>(e.obs);
    double b = static_cast<double>(e.f_coll) - a;
    double c = static_cast<double>(e.f_node) - a;
    double d = static_cast<double>(e.N) - a - b - c;
    if (b < 0) b = 0; if (c < 0) c = 0; if (d < 0) d = 0;
    double n = static_cast<double>(e.N);
    auto xlogx = [](double x, double total) -> double {
        if (x <= 0 || total <= 0) return 0;
        return x * log(x / total);
    };
    double row1 = a + b, row2 = c + d;
    double col1 = a + c, col2 = b + d;
    return 2.0 * (xlogx(a, row1 * col1 / n) + xlogx(b, row1 * col2 / n) +
                  xlogx(c, row2 * col1 / n) + xlogx(d, row2 * col2 / n));
}

static double compute_measure(const std::string& name, const CollEntry& e) {
    if (name == "mi")       return compute_mi(e);
    if (name == "mi3")      return compute_mi3(e);
    if (name == "t" || name == "tscore") return compute_tscore(e);
    if (name == "logdice")  return compute_logdice(e);
    if (name == "ll")       return compute_ll(e);
    if (name == "dice") {
        if (e.f_node == 0 && e.f_coll == 0) return 0;
        return 2.0 * static_cast<double>(e.obs) / (static_cast<double>(e.f_node) + static_cast<double>(e.f_coll));
    }
    return compute_logdice(e);
}

// ── JSON emitters (write to ostream, always JSON) ───────────────────────

static void emit_count_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map, size_t group_limit) {
    if (cmd.fields.empty() && cmd.query_name.empty()) {
        out << "{\"ok\": false, \"error\": \"count/group requires 'by' clause\"}\n";
        return;
    }
    try {
    // P7.1: only the returned rows are decoded (top group_limit by count); the
    // hierarchy for several fields still needs every group.
    const bool need_all = cmd.fields.size() >= 2;
    GroupRows gr = group_rows(corpus, ms, cmd.fields, name_map, need_all ? 0 : group_limit,
                              cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0]));
    const auto& sorted = gr.rows;
    std::map<std::string, size_t> counts;
    if (need_all) counts.insert(gr.rows.begin(), gr.rows.end());
    size_t total = gr.total;
    size_t g_end = (group_limit > 0 && group_limit < sorted.size()) ? group_limit : sorted.size();

    out << "{\"ok\": true, \"operation\": \"count\", \"last_command\": \"count\", \"result\": {\n";
    out << "  \"total_matches\": " << total << ",\n";
    out << "  \"groups\": " << gr.groups << ",\n";
    out << "  \"groups_returned\": " << g_end << ",\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.fields[i]); }
    out << "],\n";
    if (cmd.fields.size() >= 2) {
        emit_count_result_hierarchy_json(out, cmd.fields, counts, total, group_limit);
        out << "\n}}\n";
    } else {
        out << "  \"rows\": [\n";
        for (size_t i = 0; i < g_end; ++i) {
            if (i > 0) out << ",\n";
            double pct = 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total);
            out << "    {\"key\": " << jstr(sorted[i].first) << ", \"count\": " << sorted[i].second
                << ", \"pct\": " << pct << "}";
        }
        out << "\n  ]\n}}\n";
    }
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_stats_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map) {
    if (cmd.stats_metrics.empty()) {
        out << "{\"ok\": false, \"error\": \"stats requires at least one metric\"}\n";
        return;
    }
    std::vector<StatsMetricSpec> metrics;
    metrics.reserve(cmd.stats_metrics.size());
    for (const auto& sm : cmd.stats_metrics) {
        StatsMetricSpec spec;
        spec.kind = (sm.kind == GroupCommand::StatMetric::Kind::AVG)
                        ? StatsMetricSpec::Kind::Avg
                        : StatsMetricSpec::Kind::Median;
        spec.expr = sm.expr;
        metrics.push_back(std::move(spec));
    }
    std::vector<StatsRowResult> rows;
    if (!compute_stats_rows(corpus, ms, name_map, cmd.fields, metrics, rows)) {
        out << "{\"ok\": false, \"error\": \"failed to compute stats\"}\n";
        return;
    }
    auto metric_name = [](const GroupCommand::StatMetric& sm) {
        return std::string(sm.kind == GroupCommand::StatMetric::Kind::AVG ? "avg(" : "median(")
             + sm.expr + ")";
    };
    out << "{\"ok\": true, \"operation\": \"stats\", \"last_command\": \"stats\", \"result\": {\n";
    out << "  \"by\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(cmd.fields[i]);
    }
    out << "],\n  \"metrics\": [";
    for (size_t i = 0; i < cmd.stats_metrics.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(metric_name(cmd.stats_metrics[i]));
    }
    out << "],\n  \"rows\": [\n";
    for (size_t ri = 0; ri < rows.size(); ++ri) {
        if (ri > 0) out << ",\n";
        const auto& row = rows[ri];
        out << "    {\"key\": " << jstr(row.key)
            << ", \"n_total\": " << row.n_total
            << ", \"values\": [";
        for (size_t mi = 0; mi < row.metrics.size(); ++mi) {
            if (mi > 0) out << ", ";
            const auto& mr = row.metrics[mi];
            out << "{\"n_valid\": " << mr.n_valid << ", \"value\": ";
            if (mr.has_value) out << mr.value;
            else out << "null";
            out << "}";
        }
        out << "]}";
    }
    out << "\n  ]\n}}\n";
}

static void freq_build_counts(const Corpus& corpus, const MatchSet& ms,
                              const GroupCommand& cmd, const ProgramOptions& opts,
                              const NameIndexMap& name_map,
                              std::map<std::string, size_t>& counts,
                              size_t& total_matches) {
    counts.clear();
    if (ms.aggregate_buckets) {
        ms.aggregate_buckets->for_each_bucket([&](const int64_t* key, size_t len, size_t c) {
            counts[decode_aggregate_bucket_key(*ms.aggregate_buckets, key, len)] += c;
        });
    } else {
        for (const auto& m : ms.matches) ++counts[make_key(corpus, m, name_map, cmd.fields)];
    }
    if (cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0])) {
        std::map<std::string, size_t> exploded;
        for (const auto& [key, count] : counts) {
            if (key.find('|') != std::string::npos) {
                size_t s = 0;
                while (s < key.size()) {
                    size_t p = key.find('|', s);
                    if (p == std::string::npos) p = key.size();
                    std::string comp = key.substr(s, p - s);
                    if (!comp.empty()) exploded[comp] += count;
                    s = p + 1;
                }
            } else {
                exploded[key] += count;
            }
        }
        counts = std::move(exploded);
    }
    total_matches = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
}

struct FreqSrc {
    std::string label;
    const MatchSet* ms;
    const NameIndexMap* nm;
};

static void emit_freq_compare_json(std::ostream& out, const Corpus& corpus, const std::vector<FreqSrc>& srcs,
                                   const GroupCommand& cmd, const ProgramOptions& opts) {
    if (cmd.fields.empty()) {
        out << "{\"ok\": false, \"error\": \"freq requires 'by' clause\"}\n";
        return;
    }
    std::vector<std::map<std::string, size_t>> counts_per(srcs.size());
    std::vector<size_t> totals(srcs.size());
    for (size_t i = 0; i < srcs.size(); ++i)
        freq_build_counts(corpus, *srcs[i].ms, cmd, opts, *srcs[i].nm, counts_per[i], totals[i]);

    std::set<std::string> all_keys;
    for (const auto& m : counts_per)
        for (const auto& [k, c] : m)
            all_keys.insert(k);
    std::vector<std::string> sorted_keys(all_keys.begin(), all_keys.end());
    std::sort(sorted_keys.begin(), sorted_keys.end(),
              [&](const std::string& a, const std::string& b) {
                  size_t sa = 0, sb = 0;
                  for (const auto& m : counts_per) {
                      auto ia = m.find(a), ib = m.find(b);
                      if (ia != m.end()) sa += ia->second;
                      if (ib != m.end()) sb += ib->second;
                  }
                  if (sa != sb) return sa > sb;
                  return compare_group_keys(a, b);
              });

    double corpus_size = static_cast<double>(corpus.size());
    auto freq_spec = resolve_freq_subcorpus_spec(corpus, cmd.fields);
    const bool use_subcorpus_ipm = freq_spec.has_value();
    std::unordered_map<std::string, double> subcorpus_sizes =
            use_subcorpus_ipm
                ? build_freq_subcorpus_sizes(*freq_spec, sorted_keys, corpus_size)
                : std::unordered_map<std::string, double>{};
    auto ipm_denom = [&](const std::string& key) -> double {
        if (use_subcorpus_ipm) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    out << "{\"ok\": true, \"operation\": \"freq\", \"last_command\": \"freq\", \"result\": {\n";
    out << "  \"compare_queries\": [";
    for (size_t i = 0; i < srcs.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(srcs[i].label);
    }
    out << "],\n  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"freq_mode\": \"compare\",\n";
    out << "  \"per_subcorpus_ipm\": " << (use_subcorpus_ipm ? "true" : "false") << ",\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(cmd.fields[i]);
    }
    out << "],\n  \"totals_per_query\": {";
    for (size_t i = 0; i < srcs.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(srcs[i].label) << ": " << totals[i];
    }
    out << "},\n  \"rows\": [\n";
    for (size_t ri = 0; ri < sorted_keys.size(); ++ri) {
        const std::string& key = sorted_keys[ri];
        if (ri > 0) out << ",\n";
        double denom = ipm_denom(key);
        out << "    {\"key\": " << jstr(key);
        if (use_subcorpus_ipm) out << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
        out << ", \"queries\": {";
        for (size_t qi = 0; qi < srcs.size(); ++qi) {
            if (qi > 0) out << ", ";
            size_t c = 0;
            auto it = counts_per[qi].find(key);
            if (it != counts_per[qi].end()) c = it->second;
            double pct = totals[qi] > 0 ? 100.0 * static_cast<double>(c) / static_cast<double>(totals[qi]) : 0.0;
            double ipm = 1e6 * static_cast<double>(c) / denom;
            double q_ipm = totals[qi] > 0
                ? 1e6 * static_cast<double>(c) / static_cast<double>(totals[qi])
                : 0.0;
            out << jstr(srcs[qi].label) << ": {\"count\": " << c
                      << ", \"pct\": " << pct
                      << ", \"ipm\": " << std::fixed << std::setprecision(2) << ipm
                      << ", \"q_ipm\": " << q_ipm;
            if (use_subcorpus_ipm) {
                double rf_pct = denom > 0 ? 100.0 * static_cast<double>(c) / denom : 0.0;
                out << ", \"rf_pct\": " << std::setprecision(4) << rf_pct;
            }
            out << "}";
        }
        out << "}}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_freq_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                           const GroupCommand& cmd, const ProgramOptions& opts, const NameIndexMap& name_map,
                           const std::string& source_query_name) {
    if (cmd.fields.empty() && cmd.query_name.empty()) {
        out << "{\"ok\": false, \"error\": \"freq requires 'by' clause\"}\n";
        return;
    }
    try {
    // every row (freq is not paged); sorted by count, then key
    GroupRows gr = group_rows(corpus, ms, cmd.fields, name_map, 0,
                              cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0]));
    const auto& sorted = gr.rows;
    const size_t total_matches = gr.total;
    double corpus_size = static_cast<double>(corpus.size());

    // Per-subcorpus IPM: when grouping by a single region attribute, use per-group
    // token counts as IPM denominator for meaningful relative frequencies.
    auto freq_spec = resolve_freq_subcorpus_spec(corpus, cmd.fields);
    const bool use_subcorpus_ipm = freq_spec.has_value();
    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(sorted.size());
    for (const auto& kv : sorted) sorted_keys.push_back(kv.first);
    std::unordered_map<std::string, double> subcorpus_sizes =
            use_subcorpus_ipm
                ? build_freq_subcorpus_sizes(*freq_spec, sorted_keys, corpus_size)
                : std::unordered_map<std::string, double>{};

    auto ipm_denom = [&](const std::string& key) -> double {
        if (use_subcorpus_ipm) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    out << "{\"ok\": true, \"operation\": \"freq\", \"last_command\": \"freq\", \"result\": {\n";
    out << "  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"total_matches\": " << total_matches << ",\n";
    out << "  \"freq_mode\": \"single\",\n";
    out << "  \"source_query\": " << jstr(source_query_name) << ",\n";
    out << "  \"per_subcorpus_ipm\": " << (use_subcorpus_ipm ? "true" : "false") << ",\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.fields[i]); }
    out << "],\n  \"rows\": [\n";
    for (size_t i = 0; i < sorted.size(); ++i) {
        if (i > 0) out << ",\n";
        double denom = ipm_denom(sorted[i].first);
        double ipm = 1e6 * static_cast<double>(sorted[i].second) / denom;
        double pct = total_matches > 0
            ? 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total_matches)
            : 0.0;
        out << "    {\"key\": " << jstr(sorted[i].first) << ", \"count\": " << sorted[i].second
            << ", \"pct\": " << pct
            << ", \"ipm\": " << std::fixed << std::setprecision(2) << ipm;
        if (use_subcorpus_ipm) {
            double rf_pct = denom > 0
                ? 100.0 * static_cast<double>(sorted[i].second) / denom
                : 0.0;
            out << ", \"rf_pct\": " << std::setprecision(4) << rf_pct
                << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
        }
        out << "}";
    }
    out << "\n  ]\n}}\n";
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_size_json(std::ostream& out, const MatchSet& ms) {
    size_t n = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
    out << "{\"ok\": true, \"operation\": \"size\", \"last_command\": \"size\", \"result\": " << n << "}\n";
}

static bool is_multi_value_field(const Corpus& corpus, const std::string& field) {
    if (field.size() >= 5 && field.compare(0, 5, "tcnt(") == 0) return false;
    std::string attr_spec = field;
    if (field.rfind("match.", 0) == 0 && field.size() > 6) {
        attr_spec = field.substr(6);
    } else {
        auto dot = field.find('.');
        if (dot != std::string::npos && dot > 0)
            attr_spec = field.substr(dot + 1);
    }
    // Multivalue positional attr
    if (corpus.is_multivalue(attr_spec))
        return true;
    // Overlapping/nested region attr
    RegionAttrParts parts;
    if (split_region_attr_name(attr_spec, parts) &&
        corpus.has_structure(parts.struct_name)) {
        const auto& sa = corpus.structure(parts.struct_name);
        if (!resolve_region_attr_key(sa, parts.struct_name, parts.attr_name))
            return false;
        return corpus.is_overlapping(parts.struct_name)
            || corpus.is_nested(parts.struct_name);
    }
    return false;
}

static void emit_field_json(std::ostream& out, const std::string& val, bool is_multi) {
    if (is_multi && val.find('|') != std::string::npos) {
        out << '[';
        size_t start = 0;
        bool first = true;
        while (start < val.size()) {
            size_t p = val.find('|', start);
            if (p == std::string::npos) p = val.size();
            if (!first) out << ", ";
            out << jstr(val.substr(start, p - start));
            first = false;
            start = p + 1;
        }
        out << ']';
    } else {
        out << jstr(val);
    }
}

static void emit_tabulate_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                               const GroupCommand& cmd, const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        out << "{\"ok\": false, \"error\": \"tabulate requires at least one field\"}\n";
        return;
    }
    try {
    const size_t n = ms.matches.size();
    const size_t start = std::min(cmd.tabulate_offset, n);
    const size_t end = std::min(start + cmd.tabulate_limit, n);
    const size_t total_hits = ms.total_count > 0 ? ms.total_count : n;

    std::vector<bool> field_is_multi(cmd.fields.size(), false);
    for (size_t f = 0; f < cmd.fields.size(); ++f)
        field_is_multi[f] = is_multi_value_field(corpus, cmd.fields[f]);

    out << "{\"ok\": true, \"operation\": \"tabulate\", \"last_command\": \"tabulate\", \"result\": {\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.fields[i]); }
    out << "],\n  \"total_matches\": " << total_hits << ",\n";
    out << "  \"offset\": " << cmd.tabulate_offset << ",\n";
    out << "  \"limit\": " << cmd.tabulate_limit << ",\n";
    out << "  \"rows_returned\": " << (end - start) << ",\n  \"rows\": [\n";
    for (size_t i = start; i < end; ++i) {
        if (i > start) out << ",\n";
        out << "    [";
        for (size_t f = 0; f < cmd.fields.size(); ++f) {
            if (f > 0) out << ", ";
            std::string val = read_tabulate_field(corpus, ms.matches[i], name_map, cmd.fields[f]);
            emit_field_json(out, val, field_is_multi[f]);
        }
        out << "]";
    }
    out << "\n  ]\n}}\n";
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_describe_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                               const GroupCommand& cmd, const NameIndexMap& name_map) {
    const size_t n = ms.matches.size();
    const size_t start = std::min(cmd.tabulate_offset, n);
    const size_t end = std::min(start + cmd.tabulate_limit, n);
    const size_t total_hits = ms.total_count > 0 ? ms.total_count : n;

    auto is_region_only_match = [&](const Match& m) {
        return m.token_group_match || (!m.named_regions.empty() && name_map.empty());
    };

    out << "{\"ok\": true, \"operation\": \"describe\", \"last_command\": \"describe\", \"result\": {\n";
    out << "  \"total_matches\": " << total_hits << ",\n";
    out << "  \"offset\": " << cmd.tabulate_offset << ",\n";
    out << "  \"limit\": " << cmd.tabulate_limit << ",\n";
    out << "  \"rows_returned\": " << (end - start) << ",\n";
    out << "  \"rows\": [\n";
    for (size_t i = start; i < end; ++i) {
        if (i > start) out << ",\n";
        const auto& m = ms.matches[i];
        const bool region_only = is_region_only_match(m);
        out << "    {\"match_index\": " << i
            << ", \"match_start\": " << m.first_pos()
            << ", \"match_end\": " << m.last_pos()
            << ", \"kind\": " << jstr(region_only ? "region" : "token");

        if (region_only) {
            out << ", \"regions\": [";
            bool first_region = true;
            for (const auto& [label, rr] : m.named_regions) {
                if (!corpus.has_structure(rr.struct_name)) continue;
                const auto& sa = corpus.structure(rr.struct_name);
                if (rr.region_idx >= sa.region_count()) continue;
                if (!first_region) out << ", ";
                first_region = false;
                Region r = sa.get(rr.region_idx);
                out << "{\"label\": " << jstr(label)
                    << ", \"type\": " << jstr(rr.struct_name)
                    << ", \"index\": " << rr.region_idx
                    << ", \"start\": " << r.start
                    << ", \"end\": " << r.end
                    << ", \"attrs\": {";
                const auto& ra = sa.region_attr_names();
                bool first_ra = true;
                for (size_t j = 0; j < ra.size(); ++j) {
                    std::string_view rv = sa.region_value(ra[j], rr.region_idx);
                    if (!describe_emit_attr(ra[j], rv)) continue;
                    if (!first_ra) out << ", ";
                    first_ra = false;
                    out << jstr(ra[j]) << ": " << jstr(std::string(rv));
                }
                out << "}}";
            }
            out << "]";
            if (!m.token_group_props.empty()) {
                out << ", \"token_group_props\": {";
                bool first_prop = true;
                for (size_t j = 0; j < m.token_group_props.size(); ++j) {
                    const auto& [pk, pv] = m.token_group_props[j];
                    if (!describe_emit_attr(pk, pv)) continue;
                    if (!first_prop) out << ", ";
                    first_prop = false;
                    out << jstr(pk) << ": " << jstr(pv);
                }
                out << "}";
            }
        } else {
            out << ", \"tokens\": [";
            bool first_tok = true;
            const auto& attr_names = corpus.attr_names();
            for (size_t t = 0; t < m.positions.size(); ++t) {
                if (m.positions[t] == NO_HEAD) continue;
                CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
                for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                    if (!first_tok) out << ", ";
                    first_tok = false;
                    out << "{\"corpus_pos\": " << p;
                    for (const auto& attr_name : attr_names) {
                        if (!corpus.has_attr(attr_name)) continue;
                        auto val = corpus.attr(attr_name).value_at(p);
                        if (!describe_emit_attr(attr_name, val)) continue;
                        out << ", " << jstr(attr_name) << ": " << jstr(val);
                    }
                    out << "}";
                }
            }
            out << "]";
        }
        out << "}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_raw_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms) {
    const auto& form = corpus.attr("form");
    out << "{\"ok\": true, \"operation\": \"raw\", \"last_command\": \"raw\", \"result\": [\n";
    for (size_t i = 0; i < ms.matches.size(); ++i) {
        if (i > 0) out << ",\n";
        auto positions = ms.matches[i].matched_positions();
        out << "  {\"positions\": [";
        for (size_t j = 0; j < positions.size(); ++j) { if (j > 0) out << ", "; out << positions[j]; }
        out << "], \"tokens\": [";
        for (size_t j = 0; j < positions.size(); ++j) {
            if (j > 0) out << ", ";
            out << jstr(std::string(form.value_at(positions[j])));
        }
        out << "]}";
    }
    out << "\n]}\n";
}

static void emit_coll_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                           const GroupCommand& cmd, const ProgramOptions& opts,
                           const NameIndexMap& name_map, const NameIndexMap* target_name_map) {
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_window_positions = 0;

    auto count_coll_token = [&](CorpusPos p) {
        if (pa.value_at(p).empty()) return;
        ++obs_counts[pa.id_at(p)];
        ++total_window_positions;
    };

    auto add_envelope = [&](const Match& m) {
        auto matched = m.matched_positions();
        std::set<CorpusPos> matched_set(matched.begin(), matched.end());
        CorpusPos first = m.first_pos();
        CorpusPos last = m.last_pos();
        CorpusPos left_start = (first > static_cast<CorpusPos>(opts.coll_left)) ? first - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < first; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
        CorpusPos right_end = std::min(last + static_cast<CorpusPos>(opts.coll_right) + 1,
                                        static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = last + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
    };
    auto add_hub = [&](const std::set<CorpusPos>& matched_set, CorpusPos hub) {
        CorpusPos left_start = (hub > static_cast<CorpusPos>(opts.coll_left)) ? hub - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < hub; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
        CorpusPos right_end = std::min(hub + static_cast<CorpusPos>(opts.coll_right) + 1,
                                        static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = hub + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
    };

    if (!ms.parallel_matches.empty()) {
        for (const auto& [s, t] : ms.parallel_matches) {
            if (cmd.coll_on_label.empty()) {
                add_envelope(s);
            } else {
                CorpusPos hub =
                        resolve_token_label_pair(s, name_map, t, target_name_map, cmd.coll_on_label);
                if (hub == NO_HEAD) continue;
                std::set<CorpusPos> excl;
                for (CorpusPos p : s.matched_positions()) excl.insert(p);
                for (CorpusPos p : t.matched_positions()) excl.insert(p);
                add_hub(excl, hub);
            }
        }
    } else {
        for (const auto& m : ms.matches) {
            if (cmd.coll_on_label.empty()) {
                add_envelope(m);
            } else {
                CorpusPos hub = resolve_name(m, name_map, cmd.coll_on_label);
                if (hub == NO_HEAD) continue;
                auto matched = m.matched_positions();
                std::set<CorpusPos> matched_set(matched.begin(), matched.end());
                add_hub(matched_set, hub);
            }
        }
    }

    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (stop_ids.count(id)) continue;
        if (obs < opts.coll_min_freq) continue;
        std::string w = std::string(pa.lexicon().get(id));
        if (w.empty()) continue;
        entries.push_back({id, std::move(w), obs, pa.count_of_id(id), total_window_positions, N});
    }
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });
    size_t show = std::min(entries.size(), opts.coll_max_items);
    const size_t coll_match_n =
            !ms.parallel_matches.empty() ? ms.parallel_matches.size() : ms.matches.size();

    out << "{\"ok\": true, \"operation\": \"coll\", \"last_command\": \"coll\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(coll_attr) << ",\n";
    out << "  \"window\": [" << opts.coll_left << ", " << opts.coll_right << "],\n";
    if (!cmd.coll_on_label.empty())
        out << "  \"on\": " << jstr(cmd.coll_on_label) << ",\n";
    out << "  \"matches\": " << coll_match_n << ",\n";
    out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
    out << "  \"measures\": [";
    for (size_t i = 0; i < measures.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(measures[i]);
    }
    out << "],\n  \"collocates\": [\n";
    for (size_t i = 0; i < show; ++i) {
        if (i > 0) out << ",\n";
        out << "    {\"word\": " << jstr(entries[i].form) << ", \"obs\": " << entries[i].obs
            << ", \"freq\": " << entries[i].f_coll;
        for (const auto& meas : measures)
            out << ", " << jstr(meas) << ": " << std::fixed << std::setprecision(3)
                << compute_measure(meas, entries[i]);
        out << "}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_dcoll_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map,
                            const NameIndexMap* target_name_map,
                            const ProgramOptions& opts) {
    if (!corpus.has_deps()) {
        out << "{\"ok\": false, \"error\": \"dcoll requires dependency index\"}\n";
        return;
    }
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);
    const auto& deps = corpus.deps();
    bool has_deprel_attr = corpus.has_attr("deprel");
    const PositionalAttr* deprel_pa = has_deprel_attr ? &corpus.attr("deprel") : nullptr;

    bool want_head = false, want_descendants = false, want_all_children = false;
    std::set<std::string> deprel_filter;
    if (cmd.relations.empty()) { want_all_children = true; }
    else {
        for (const auto& rel : cmd.relations) {
            if (rel == "head") want_head = true;
            else if (rel == "descendants") want_descendants = true;
            else if (rel == "children") want_all_children = true;
            else deprel_filter.insert(rel);
        }
    }
    bool want_filtered_children = !deprel_filter.empty();

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_related = 0;

    auto emit_one = [&](const Match& m, const Match* tgt) {
        CorpusPos node_pos = m.first_pos();
        if (!cmd.dcoll_anchor.empty()) {
            CorpusPos ap = tgt ? resolve_token_label_pair(m, name_map, *tgt, target_name_map, cmd.dcoll_anchor)
                               : resolve_name(m, name_map, cmd.dcoll_anchor);
            if (ap != NO_HEAD) node_pos = ap;
        }
        auto count_token = [&](CorpusPos rp) {
            if (rp == node_pos) return;
            ++obs_counts[pa.id_at(rp)]; ++total_related;
        };
        if (want_head) { auto h = deps.head(node_pos); if (h != NO_HEAD) count_token(h); }
        if (want_descendants) for (CorpusPos rp : deps.subtree(node_pos)) count_token(rp);
        if (want_all_children) for (CorpusPos rp : deps.children(node_pos)) count_token(rp);
        if (want_filtered_children) {
            for (CorpusPos rp : deps.children(node_pos)) {
                if (deprel_pa) { std::string dr(deprel_pa->value_at(rp)); if (deprel_filter.count(dr)) count_token(rp); }
            }
        }
    };
    if (!ms.parallel_matches.empty()) {
        for (const auto& [s, t] : ms.parallel_matches)
            emit_one(s, &t);
    } else {
        for (const auto& m : ms.matches)
            emit_one(m, nullptr);
    }

    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (stop_ids.count(id)) continue;
        if (obs < opts.coll_min_freq) continue;
        entries.push_back({id, std::string(pa.lexicon().get(id)), obs, pa.count_of_id(id), total_related, N});
    }
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });
    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"dcoll\", \"last_command\": \"dcoll\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(coll_attr) << ",\n";
    out << "  \"relations\": [";
    for (size_t i = 0; i < cmd.relations.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.relations[i]); }
    out << "],\n";
    if (!cmd.dcoll_anchor.empty()) out << "  \"anchor\": " << jstr(cmd.dcoll_anchor) << ",\n";
    {
        const size_t dcoll_match_n =
                !ms.parallel_matches.empty() ? ms.parallel_matches.size() : ms.matches.size();
        out << "  \"matches\": " << dcoll_match_n << ",\n";
    }
    out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
    out << "  \"measures\": [";
    for (size_t i = 0; i < measures.size(); ++i) { if (i > 0) out << ", "; out << jstr(measures[i]); }
    out << "],\n  \"collocates\": [\n";
    for (size_t i = 0; i < show; ++i) {
        if (i > 0) out << ",\n";
        out << "    {\"word\": " << jstr(entries[i].form) << ", \"obs\": " << entries[i].obs
            << ", \"freq\": " << entries[i].f_coll;
        for (const auto& meas : measures)
            out << ", " << jstr(meas) << ": " << std::fixed << std::setprecision(3) << compute_measure(meas, entries[i]);
        out << "}";
    }
    out << "\n  ]\n}}\n";
}

// ── keyness: subcorpus keyword extraction (#40) ────────────────────────

static double safe_ln(double x) { return x > 0 ? std::log(x) : 0.0; }

static void emit_keyness_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                               const GroupCommand& cmd, const ProgramOptions& opts,
                               const MatchSet* ref_ms = nullptr) {
    std::string attr = "lemma";
    if (!cmd.fields.empty()) attr = cmd.fields[0];
    if (!corpus.has_attr(attr)) attr = "form";
    const auto& pa = corpus.attr(attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);

    std::unordered_map<LexiconId, size_t> focus_counts;
    size_t focus_size = 0;
    for (const auto& m : ms.matches) {
        auto positions = m.matched_positions();
        for (CorpusPos p : positions) {
            ++focus_counts[pa.id_at(p)];
            ++focus_size;
        }
    }

    if (focus_size == 0) {
        out << "{\"ok\": true, \"operation\": \"keyness\", \"last_command\": \"keyness\", \"result\": {\"rows\": []}}\n";
        return;
    }

    // Reference counts: from ref_ms if provided, else rest of corpus
    std::unordered_map<LexiconId, size_t> ref_counts;
    size_t ref_size = 0;
    if (ref_ms) {
        for (const auto& m : ref_ms->matches) {
            auto positions = m.matched_positions();
            for (CorpusPos p : positions) {
                ++ref_counts[pa.id_at(p)];
                ++ref_size;
            }
        }
    } else {
        ref_size = corpus.size() - focus_size;
    }

    double N = static_cast<double>(focus_size + ref_size);

    // Collect all word types
    std::set<LexiconId> all_ids;
    for (const auto& [id, _] : focus_counts) if (!stop_ids.count(id)) all_ids.insert(id);
    if (ref_ms) {
        for (const auto& [id, _] : ref_counts)
            if (!stop_ids.count(id)) all_ids.insert(id);
    }

    struct KE { std::string form; size_t ff; size_t rf; double g2; };
    std::vector<KE> entries;
    for (LexiconId id : all_ids) {
        size_t ffreq = 0;
        auto fit = focus_counts.find(id);
        if (fit != focus_counts.end()) ffreq = fit->second;

        size_t rfreq = 0;
        if (ref_ms) {
            auto rit = ref_counts.find(id);
            if (rit != ref_counts.end()) rfreq = rit->second;
        } else {
            size_t corpus_freq = pa.count_of_id(id);
            rfreq = corpus_freq > ffreq ? corpus_freq - ffreq : 0;
        }

        if (ffreq == 0 && rfreq == 0) continue;

        double E1 = static_cast<double>(focus_size) * static_cast<double>(ffreq + rfreq) / N;
        double E2 = static_cast<double>(ref_size) * static_cast<double>(ffreq + rfreq) / N;
        double g2 = 0.0;
        if (ffreq > 0 && E1 > 0) g2 += static_cast<double>(ffreq) * safe_ln(static_cast<double>(ffreq) / E1);
        if (rfreq > 0 && E2 > 0) g2 += static_cast<double>(rfreq) * safe_ln(static_cast<double>(rfreq) / E2);
        g2 *= 2.0;
        if (static_cast<double>(ffreq) < E1) g2 = -g2;
        entries.push_back({std::string(pa.lexicon().get(id)), ffreq, rfreq, g2});
    }

    std::sort(entries.begin(), entries.end(), [](const KE& a, const KE& b) {
        if (a.g2 != b.g2) return a.g2 > b.g2;
        if (a.ff != b.ff) return a.ff > b.ff;
        return a.form < b.form;
    });

    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"keyness\", \"last_command\": \"keyness\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(attr) << ",\n";
    out << "  \"focus_size\": " << focus_size << ",\n";
    out << "  \"ref_size\": " << ref_size << ",\n";
    out << "  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
    out << "  \"rows\": [\n";
    for (size_t i = 0; i < show; ++i) {
        if (i > 0) out << ",\n";
        out << "    {\"word\": " << jstr(entries[i].form)
            << ", \"focus_freq\": " << entries[i].ff
            << ", \"ref_freq\": " << entries[i].rf
            << ", \"keyness\": " << std::fixed << std::setprecision(2) << entries[i].g2
            << ", \"effect\": " << jstr(entries[i].g2 >= 0 ? "+" : "-")
            << "}";
    }
    out << "\n  ]\n}}\n";
}

static QueryOptions query_options_of(const ProgramOptions& opts) {
    QueryOptions qopts;
    qopts.limit = opts.limit; qopts.offset = opts.offset; qopts.max_total = opts.max_total;
    qopts.context = opts.context; qopts.total = opts.total; qopts.attrs = opts.attrs;
    return qopts;
}

static void emit_query_json(std::ostream& out, const Corpus& corpus, const std::string& query_text,
                            const MatchSet& ms, const ProgramOptions& opts, double elapsed_ms) {
    out << to_query_result_json(corpus, query_text, ms, query_options_of(opts), elapsed_ms);
}

static void emit_show_values_json(std::ostream& out, const Corpus& corpus, const std::string& attr_name,
                                  size_t group_limit) {
    std::string json = to_values_json(corpus, attr_name, group_limit);
    if (json.empty())
        out << "{\"ok\": false, \"error\": \"Unknown attribute: " << attr_name << "\"}\n";
    else
        out << json;
}

static void emit_show_regions_type_json(std::ostream& out, const Corpus& corpus,
                                        const std::string& type_name, size_t group_limit) {
    std::string json = to_regions_json(corpus, type_name, group_limit);
    if (json.empty())
        out << "{\"ok\": false, \"error\": \"Unknown structure type: " << type_name << "\"}\n";
    else
        out << json;
}

static void emit_show_regions_json(std::ostream& out, const Corpus& corpus) {
    out << "{\n  \"ok\": true,\n  \"operation\": \"show_regions\",\n";
    out << "  \"result\": {\n    \"structures\": [";
    const auto& s_names = corpus.structure_names();
    for (size_t i = 0; i < s_names.size(); ++i) {
        if (i > 0) out << ", ";
        const auto& sa = corpus.structure(s_names[i]);
        out << "{\"name\": " << jstr(s_names[i]) << ", \"regions\": " << sa.region_count()
            << ", \"has_values\": " << (sa.has_values() ? "true" : "false") << ", \"attrs\": [";
        const auto& ra = sa.region_attr_names();
        for (size_t j = 0; j < ra.size(); ++j) { if (j > 0) out << ", "; out << jstr(ra[j]); }
        out << "]}";
    }
    out << "],\n    \"region_attrs\": [";
    const auto& ra_all = corpus.region_attr_names();
    for (size_t i = 0; i < ra_all.size(); ++i) { if (i > 0) out << ", "; out << jstr(ra_all[i]); }
    out << "]\n  }\n}\n";
}

static void emit_show_attrs_json(std::ostream& out, const Corpus& corpus) {
    out << "{\"ok\": true, \"operation\": \"show_attrs\", \"result\": [";
    const auto& names = corpus.attr_names();
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) out << ", ";
        out << "{\"name\": " << jstr(names[i]) << ", \"vocab\": " << corpus.attr(names[i]).lexicon().size() << "}";
    }
    out << "]}\n";
}

static void emit_show_info_json(std::ostream& out, const Corpus& corpus) {
    out << to_info_json(corpus);
}

static void emit_show_named_json(std::ostream& out, const ProgramSession& ps) {
    out << "{\"ok\": true, \"operation\": \"show_named\", \"result\": [";
    size_t idx = 0;
    for (const HitSetInfo& i : ps.list()) {
        if (idx++ > 0) out << ", ";
        const size_t n = i.materialised ? i.hits : (i.total_known ? i.total : 0);
        out << "{\"name\": " << jstr(i.name) << ", \"matches\": " << n
            << ", \"materialised\": " << (i.materialised ? "true" : "false")
            << ", \"total_known\": " << (i.total_known ? "true" : "false") << "}";
    }
    out << "]}\n";
}

// ── Main dispatch ───────────────────────────────────────────────────────

std::string run_program_json(Corpus& corpus, ProgramSession& ps,
                             const std::string& cql, ProgramOptions opts) {
    auto& S = *ps.impl_;

    Parser parser(cql, ParserOptions{opts.strict_quoted_strings});
    auto prog = std::make_shared<Program>(parser.parse());
    const unsigned threads = std::max(1u, opts.threads);

    QueryExecutor executor(corpus);
    executor.set_include_empty_alignment_values(opts.allow_empty_alignment);
    if (opts.progress) executor.set_progress(opts.progress);
    std::ostringstream out;

    // `q; count by f` computes the counts while the query runs (aggregation
    // sink): that result serves only the command right after its query; the
    // stored hit set keeps no hits (a later command re-derives them).
    std::optional<MatchSet> immediate;
    size_t immediate_si = 0;

    for (size_t si = 0; si < prog->size(); ++si) {
        auto& stmt = (*prog)[si];
        bool next_is_command = (si + 1 < prog->size() && (*prog)[si + 1].has_command);

        if (stmt.has_query) {
            immediate.reset();
            auto hs = std::make_shared<HitSet>();
            hs->prog = prog;
            hs->stmt = si;
            hs->display = cql;
            hs->allow_empty_alignment = opts.allow_empty_alignment;
            hs->nm = stmt.is_parallel ? build_name_map(stmt.query)
                                      : QueryExecutor::build_name_map_for_stripped_query(stmt.query);
            hs->tnm = stmt.is_parallel ? build_name_map(stmt.target_query) : NameIndexMap{};

            const std::vector<std::string>* aggregate_by = nullptr;
            if (next_is_command && !stmt.is_parallel) {
                const GroupCommand& ncmd = (*prog)[si + 1].command;
                if (!ncmd.fields.empty()
                    && (ncmd.type == CommandType::COUNT || ncmd.type == CommandType::GROUP
                        || ncmd.type == CommandType::FREQ)
                    && aggregate_command_targets_stmt(stmt, ncmd))
                    aggregate_by = &ncmd.fields;
            }

            if (aggregate_by) {
                // counted while the query runs; no hits kept
                MatchSet res = executor.execute(stmt.query, 0, true, 0, 0, 0, threads, aggregate_by);
                if (res.aggregate_buckets) hs->know_total(res.aggregate_buckets->total_hits, true);
                else if (res.total_exact) hs->know_total(res.total_count, true);
                immediate = std::move(res);
                immediate_si = si;
            } else if (!next_is_command) {
                // the page (a named query also gets its total); the hits are
                // materialised only when a command needs them all
                const bool count_t = opts.total || !stmt.name.empty();
                const size_t max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
                const size_t max_m = opts.offset + opts.limit;
                auto t0 = std::chrono::high_resolution_clock::now();
                MatchSet res = stmt.is_parallel
                    ? executor.execute_parallel(stmt.query, stmt.target_query, max_m, count_t)
                    : executor.execute(stmt.query, max_m, count_t, max_total_cap, 0, 0, threads);
                auto t1 = std::chrono::high_resolution_clock::now();
                double query_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
                const size_t n = stmt.is_parallel ? res.parallel_matches.size() : res.matches.size();
                if (count_t && res.total_exact) hs->know_total(res.total_count, true);
                out.str(""); out.clear();
                emit_query_json(out, corpus, cql, res, opts, query_ms);
                if (count_t && res.total_exact && n == res.total_count && max_m > 0)
                    hs->keep(std::move(res));   // the page held every hit
            }
            // else: the next command materialises the set (materialise: size
            // limit, admission) or counts it
            S.bind(stmt.name, hs);
        }

        if (stmt.has_command) {
            // Commands that don't need a MatchSet
            if (stmt.command.type == CommandType::DROP) {
                if (stmt.command.query_name == "all") S.sets.clear();
                else S.sets.erase(stmt.command.query_name);
                out.str(""); out.clear();
                out << "{\"ok\": true, \"operation\": \"drop\"}\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_NAMED) {
                out.str(""); out.clear(); emit_show_named_json(out, ps); continue;
            }
            if (stmt.command.type == CommandType::SHOW_ATTRS) {
                out.str(""); out.clear(); emit_show_attrs_json(out, corpus); continue;
            }
            if (stmt.command.type == CommandType::SHOW_REGIONS) {
                out.str(""); out.clear();
                if (!stmt.command.query_name.empty())
                    emit_show_regions_type_json(out, corpus, stmt.command.query_name, opts.group_limit);
                else
                    emit_show_regions_json(out, corpus);
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_VALUES) {
                out.str(""); out.clear();
                emit_show_values_json(out, corpus, stmt.command.query_name, opts.group_limit);
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_INFO) {
                out.str(""); out.clear(); emit_show_info_json(out, corpus); continue;
            }
            if (stmt.command.type == CommandType::SET) {
                const std::string& name = stmt.command.set_name;
                const std::string& val  = stmt.command.set_value;
                auto split_csv = [](const std::string& s) -> std::vector<std::string> {
                    std::vector<std::string> r;
                    std::string cur;
                    for (char c : s) {
                        if (c == ',' || c == ' ') { if (!cur.empty()) { r.push_back(cur); cur.clear(); } }
                        else cur += c;
                    }
                    if (!cur.empty()) r.push_back(cur);
                    return r;
                };
                auto to_size = [&](size_t& t) { try { t = std::stoull(val); } catch (...) {} };
                auto to_int = [&](int& t) { try { t = std::stoi(val); } catch (...) {} };

                if (name == "limit")          to_size(opts.limit);
                else if (name == "offset")    to_size(opts.offset);
                else if (name == "context")   to_int(opts.context);
                else if (name == "left")      to_int(opts.coll_left);
                else if (name == "right")     to_int(opts.coll_right);
                else if (name == "window")    { to_int(opts.coll_left); opts.coll_right = opts.coll_left; opts.context = opts.coll_left; }
                else if (name == "max-total" || name == "max_total")  to_size(opts.max_total);
                else if (name == "max-items" || name == "max_items")  to_size(opts.coll_max_items);
                else if (name == "min-freq" || name == "min_freq")    to_size(opts.coll_min_freq);
                else if (name == "stoplist") to_size(opts.coll_stoplist);
                else if (name == "group-limit" || name == "group_limit") to_size(opts.group_limit);
                else if (name == "measures")  opts.coll_measures = split_csv(val);
                else if (name == "attrs") {
                    if (val == "all" || val == "*" || val.empty()) opts.attrs.clear();
                    else opts.attrs = split_csv(val);
                }
                else if (name == "total")     opts.total = (val == "true" || val == "1" || val == "on");

                out.str(""); out.clear();
                out << "{\"ok\": true, \"operation\": \"set\", \"setting\": "
                    << jstr(name) << ", \"value\": " << jstr(val) << "}\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_SETTINGS) {
                auto join = [](const std::vector<std::string>& v) {
                    std::string r;
                    for (size_t i = 0; i < v.size(); ++i) { if (i > 0) r += ","; r += v[i]; }
                    return r.empty() ? "(all)" : r;
                };
                out.str(""); out.clear();
                out << "{\"ok\": true, \"operation\": \"show_settings\", \"result\": {\n";
                out << "  \"limit\": " << opts.limit << ",\n";
                out << "  \"offset\": " << opts.offset << ",\n";
                out << "  \"context\": " << opts.context << ",\n";
                out << "  \"left\": " << opts.coll_left << ",\n";
                out << "  \"right\": " << opts.coll_right << ",\n";
                out << "  \"max_total\": " << opts.max_total << ",\n";
                out << "  \"max_items\": " << opts.coll_max_items << ",\n";
                out << "  \"min_freq\": " << opts.coll_min_freq << ",\n";
                out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
                out << "  \"group_limit\": " << opts.group_limit << ",\n";
                out << "  \"total\": " << (opts.total ? "true" : "false") << ",\n";
                out << "  \"measures\": " << jstr(join(opts.coll_measures.empty()
                    ? std::vector<std::string>{"logdice"} : opts.coll_measures)) << ",\n";
                out << "  \"attrs\": " << jstr(join(opts.attrs)) << "\n";
                out << "}}\n";
                continue;
            }
            if (stmt.command.type == CommandType::SIZE && stmt.command.query_name.empty() && !S.find("Last")) {
                out.str(""); out.clear(); emit_show_info_json(out, corpus); continue;
            }

            // Commands on a hit set: the aggregated result of the query just before
            // (built for this command), else the named set (unknown name → Last).
            const bool use_immediate = immediate && immediate_si + 1 == si;
            const std::string& qn = stmt.command.query_name;
            HitSetPtr hs = qn.empty() ? nullptr : S.find(qn);
            std::string set_name = qn;
            if (!hs) { hs = S.find("Last"); set_name = "Last"; }
            if (!hs) {
                out.str(""); out.clear();
                out << "{\"ok\": false, \"error\": \"No query to operate on\"}\n";
                continue;
            }
            hs->touch();
            std::optional<MatchSet> expanded;   // a compact set's hits as Match objects, for this command
            auto full = [&]() -> const MatchSet& {
                materialise(corpus, *hs, set_name, S, threads, opts.progress);
                return hs->full_view(expanded);
            };
            // what `count / freq … by` counts over: the immediate aggregation, the
            // materialised hits, or an aggregation run now
            std::optional<MatchSet> agg_now;
            // (the sink also for a materialised set: it runs partitioned (P4.1) and
            // reads ids, faster than grouping a million Match objects; order-free)
            auto counting = [&]() -> const MatchSet& {
                if (use_immediate) return *immediate;
                if (hs->parallel() || stmt.command.fields.empty()) return full();
                agg_now = aggregate(corpus, *hs, stmt.command.fields, threads, opts.progress);
                return *agg_now;
            };
            const NameIndexMap& nm_to_use = hs->nm;
            const NameIndexMap* nm_tgt_parallel = hs->parallel() ? &hs->tnm : nullptr;

            out.str(""); out.clear();
            switch (stmt.command.type) {
                case CommandType::COUNT:
                case CommandType::GROUP:
                    emit_count_json(out, corpus, counting(), stmt.command, nm_to_use, opts.group_limit);
                    break;
                case CommandType::STATS:
                    emit_stats_json(out, corpus, full(), stmt.command, nm_to_use);
                    break;
                case CommandType::FREQ:
                    if (stmt.command.freq_query_names.size() >= 2) {
                        std::vector<FreqSrc> srcs;
                        std::vector<HitSetPtr> keep;
                        std::vector<std::unique_ptr<std::optional<MatchSet>>> tmps;
                        std::string missing;
                        for (const std::string& fqn : stmt.command.freq_query_names) {
                            HitSetPtr f = S.find(fqn);
                            if (!f) { missing = fqn; break; }
                            materialise(corpus, *f, fqn, S, threads, opts.progress);
                            keep.push_back(f);
                            tmps.push_back(std::make_unique<std::optional<MatchSet>>());
                            srcs.push_back({fqn, &f->full_view(*tmps.back()), &f->nm});
                        }
                        if (!missing.empty())
                            out << "{\"ok\": false, \"error\": " << jstr("Unknown named query: " + missing) << "}\n";
                        else
                            emit_freq_compare_json(out, corpus, srcs, stmt.command, opts);
                    } else {
                        const std::string source_query_name = !qn.empty() ? qn : "Last";
                        emit_freq_json(out, corpus, counting(), stmt.command, opts, nm_to_use, source_query_name);
                    }
                    break;
                case CommandType::SIZE: {
                    size_t n = 0;
                    if (use_immediate && immediate->aggregate_buckets) n = immediate->aggregate_buckets->total_hits;
                    else n = set_total_count(corpus, *hs, threads, opts.progress);
                    out << "{\"ok\": true, \"operation\": \"size\", \"last_command\": \"size\", \"result\": "
                        << n << "}\n";
                    break;
                }
                case CommandType::TABULATE:
                    emit_tabulate_json(out, corpus, full(), stmt.command, nm_to_use);
                    break;
                case CommandType::DESCRIBE:
                    emit_describe_json(out, corpus, full(), stmt.command, nm_to_use);
                    break;
                case CommandType::RAW:
                    emit_raw_json(out, corpus, full());
                    break;
                case CommandType::COLL:
                    emit_coll_json(out, corpus, full(), stmt.command, opts, nm_to_use, nm_tgt_parallel);
                    break;
                case CommandType::DCOLL:
                    emit_dcoll_json(out, corpus, full(), stmt.command, nm_to_use, nm_tgt_parallel, opts);
                    break;
                case CommandType::KEYNESS: {
                    const MatchSet* ref_ms = nullptr;
                    HitSetPtr ref;
                    std::optional<MatchSet> ref_tmp;
                    if (!stmt.command.ref_query_name.empty()) {
                        ref = S.find(stmt.command.ref_query_name);
                        if (!ref) {
                            out << "{\"ok\": false, \"error\": \"Unknown reference query: "
                                << stmt.command.ref_query_name << "\"}\n";
                            break;
                        }
                        materialise(corpus, *ref, stmt.command.ref_query_name, S, threads, opts.progress);
                        ref_ms = &ref->full_view(ref_tmp);
                    }
                    emit_keyness_json(out, corpus, full(), stmt.command, opts, ref_ms);
                    break;
                }
                case CommandType::SORT: {
                    materialise(corpus, *hs, set_name, S, threads, opts.progress);
                    try {
                        if (!stmt.command.fields.empty()) {   // P7.7: one key per hit
                            hs->sort_by(corpus, stmt.command.fields);
                            hs->sorts.push_back(stmt.command.fields);
                        }
                        const std::string saved = hs->display;
                        hs->display = "(sorted)";
                        out << hs->page(corpus, query_options_of(opts), {});
                        hs->display = saved;
                    } catch (const std::exception& e) {
                        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
                    }
                    break;
                }
                default:
                    out << "{\"ok\": true, \"operation\": \"unknown\"}\n";
                    break;
            }
            if (use_immediate) immediate.reset();
        }
    }

    std::string result = out.str();
    if (result.empty())
        return "{\"ok\": true, \"operation\": \"assign\", \"result\": {}}\n";
    return result;
}

} // namespace pando
