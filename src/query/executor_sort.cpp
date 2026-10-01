// P7.10: `sort by …` without keeping the hits (see SortIndex in executor.h).

#include "query/executor.h"
#include "query/executor_aggregate_internal.h"
#include "query/flat_agg_counter.h"

#include <algorithm>
#include <numeric>

namespace pando {

namespace {

/// The flat-counter key of every hit (as `count by fields` computes it), or why not.
struct SortKeys {
    static constexpr uint64_t kDenseMax = uint64_t{1} << 24;
    AggregateBucketData plan;
    FlatAggCounter keys;
    bool ok = false;
    /// packed keys < card(): dense tables instead of hash maps when small enough
    uint64_t card() const { return keys.ncols == 2 ? keys.v1 * keys.v2 : keys.v1; }
    bool dense() const { return keys.v1 <= kDenseMax && (keys.ncols == 1 || keys.v2 <= kDenseMax) && card() <= kDenseMax; }

    SortKeys(const Corpus& corpus, const TokenQuery& q, const std::vector<std::string>& fields) {
        if (fields.empty() || fields.size() > 2) return;
        if (!build_aggregate_plan(corpus, fields, plan)) return;
        const NameIndexMap nm = QueryExecutor::build_name_map_for_stripped_query(q);
        size_t ntok = 0;
        for (const auto& t : q.tokens) ntok += !t.is_anchor();
        ok = keys.init(plan, nm, ntok, corpus, false);
    }
};

/// Pass 1: per distinct key, the hit count and one hit (for its key string).
struct CountSink : HitSink {
    static constexpr uint32_t kNone = ~uint32_t{0};
    FlatAggCounter& keys;
    std::unordered_map<uint64_t, uint32_t> index;   // packed key → distinct key
    std::vector<uint32_t> dense;                    // the same, by packed key (kNone: absent)
    std::vector<uint64_t> keys_seen;                // distinct key → packed key
    std::vector<uint64_t> count;
    std::vector<CorpusPos> rep;   // stride * 2 per key: starts, ends
    size_t stride = 0;
    bool bad = false;
    CountSink(FlatAggCounter& k, bool use_dense, uint64_t card) : keys(k) {
        if (use_dense) dense.assign(static_cast<size_t>(card), kNone);
    }
    void hit(const CorpusPos* s, const CorpusPos* e, size_t n) override {
        if (bad) return;
        if (stride == 0) stride = n;
        uint64_t k = 0;
        if (n != stride || !keys.key_of(s, n, k)) { bad = true; return; }
        uint32_t d;
        if (!dense.empty()) {
            if (k >= dense.size()) { bad = true; return; }
            d = dense[static_cast<size_t>(k)];
            if (d == kNone) d = dense[static_cast<size_t>(k)] = fresh(k, s, e, n);
        } else {
            auto it = index.find(k);
            d = it != index.end() ? it->second : index.emplace(k, fresh(k, s, e, n)).first->second;
        }
        ++count[d];
    }
    uint32_t fresh(uint64_t k, const CorpusPos* s, const CorpusPos* e, size_t n) {
        keys_seen.push_back(k);
        count.push_back(0);
        rep.insert(rep.end(), s, s + n);
        rep.insert(rep.end(), e, e + n);
        return static_cast<uint32_t>(count.size() - 1);
    }
};

/// Pass 2: the hits whose sorted position falls in [from, to).
struct PageSink : HitSink {
    FlatAggCounter& keys;
    const SortIndex& si;
    size_t from, to;
    std::vector<size_t> seen;   // per rank
    struct Kept { size_t at; std::vector<CorpusPos> pos; };
    std::vector<Kept> kept;
    bool bad = false;
    PageSink(FlatAggCounter& k, const SortIndex& s, size_t f, size_t t)
        : keys(k), si(s), from(f), to(t), seen(s.start.size(), 0) {}
    void hit(const CorpusPos* s, const CorpusPos* e, size_t n) override {
        if (bad) return;
        uint64_t k = 0;
        if (!keys.key_of(s, n, k)) { bad = true; return; }
        uint32_t r;
        if (!si.rank_dense.empty()) {
            if (k >= si.rank_dense.size() || !si.rank_dense[static_cast<size_t>(k)]) { bad = true; return; }
            r = si.rank_dense[static_cast<size_t>(k)] - 1;
        } else {
            auto it = si.rank_of.find(k);
            if (it == si.rank_of.end()) { bad = true; return; }
            r = it->second;
        }
        const size_t at = si.start[r] + seen[r]++;
        if (at < from || at >= to) return;
        Kept h{at, {}};
        h.pos.assign(s, s + n);
        h.pos.insert(h.pos.end(), e, e + n);
        kept.push_back(std::move(h));
    }
};

}  // namespace

std::shared_ptr<const SortIndex> QueryExecutor::build_sort_index(const TokenQuery& q,
                                                                const std::vector<std::string>& fields,
                                                                const SortKeyString& key,
                                                                const SortKeyLess& less) {
    if (!may_sink_hits(q)) return nullptr;   // (fast paths off: no sink, sunk == 0 below)
    SortKeys sk(corpus_, q, fields);
    if (!sk.ok) return nullptr;
    CountSink sink(sk.keys, sk.dense(), sk.card());
    set_hit_sink(&sink);
    MatchSet ms;
    try {
        ms = execute(q, 0, true, 0, 0, 0, 1);
    } catch (...) {
        set_hit_sink(nullptr);
        throw;
    }
    const size_t sunk = sink_hits();
    set_hit_sink(nullptr);
    if (sink.bad || !ms.matches.empty() || !ms.total_exact || sunk != ms.total_count) return nullptr;

    // the keys' strings (from one hit each, as the materialised sort reads them), in order
    const size_t nk = sink.count.size(), w = sink.stride;
    std::vector<std::string> str(nk);
    Match m;
    for (size_t d = 0; d < nk; ++d) {
        const CorpusPos* p = sink.rep.data() + d * 2 * w;
        m.positions.assign(p, p + w);
        m.span_ends.assign(p + w, p + 2 * w);
        str[d] = key(m);
    }
    std::vector<uint32_t> order(nk);
    std::iota(order.begin(), order.end(), 0u);
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) { return less(str[a], str[b]); });

    auto si = std::make_shared<SortIndex>();
    si->fields = fields;
    si->total = ms.total_count;
    std::vector<uint32_t> rank_of_key(nk, 0);
    size_t pos = 0;
    for (size_t i = 0; i < nk; ++i) {
        const uint32_t d = order[i];
        if (i == 0 || less(str[order[i - 1]], str[d])) si->start.push_back(pos);   // a new key string
        rank_of_key[d] = static_cast<uint32_t>(si->start.size() - 1);
        pos += sink.count[d];
    }
    if (!sink.dense.empty()) {
        si->rank_dense.assign(sink.dense.size(), 0);
        for (size_t d = 0; d < nk; ++d) si->rank_dense[static_cast<size_t>(sink.keys_seen[d])] = rank_of_key[d] + 1;
    } else {
        si->rank_of.reserve(nk);
        for (size_t d = 0; d < nk; ++d) si->rank_of.emplace(sink.keys_seen[d], rank_of_key[d]);
    }
    return si;
}

std::optional<MatchSet> QueryExecutor::sorted_page(const TokenQuery& q, const SortIndex& si, size_t from,
                                                   size_t to) {
    SortKeys sk(corpus_, q, si.fields);
    if (!sk.ok) return std::nullopt;
    to = std::min(to, si.total);
    PageSink sink(sk.keys, si, from, to);
    set_hit_sink(&sink);
    MatchSet ms;
    try {
        ms = execute(q, 0, true, 0, 0, 0, 1);
    } catch (...) {
        set_hit_sink(nullptr);
        throw;
    }
    const size_t sunk = sink_hits();
    set_hit_sink(nullptr);
    if (sink.bad || !ms.matches.empty() || sunk != si.total || ms.total_count != si.total) return std::nullopt;
    std::sort(sink.kept.begin(), sink.kept.end(), [](const auto& a, const auto& b) { return a.at < b.at; });
    MatchSet out;
    out.total_count = si.total;
    out.total_exact = true;
    out.plan_path = ms.plan_path;
    out.num_tokens = ms.num_tokens;
    out.matches.resize(sink.kept.size());
    for (size_t i = 0; i < sink.kept.size(); ++i) {
        const auto& p = sink.kept[i].pos;
        const size_t w = p.size() / 2;
        out.matches[i].positions.assign(p.begin(), p.begin() + static_cast<std::ptrdiff_t>(w));
        out.matches[i].span_ends.assign(p.begin() + static_cast<std::ptrdiff_t>(w), p.end());
    }
    return out;
}

}  // namespace pando
