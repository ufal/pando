#include "api/hit_steps.h"
#include "query/sort_field.h"

#include <algorithm>
#include <cstdlib>
#include <numeric>
#include <random>

namespace pando {

FilterSpans filter_spans_of(const std::vector<Match>& hits) {
    FilterSpans out;
    out.reserve(hits.size());
    for (const Match& m : hits) out.emplace_back(m.first_pos(), m.last_pos());
    std::sort(out.begin(), out.end());
    return out;
}

bool filter_window_has(const Match& m, const NameIndexMap& nm, const GroupCommand& cmd, const FilterSpans& f) {
    int64_t from = sort_anchor_position(m, nm, cmd.filter_from);
    int64_t to = sort_anchor_position(m, nm, cmd.filter_to);
    if (to < from) std::swap(from, to);
    const int64_t hs = m.first_pos(), he = m.last_pos();
    auto counts = [&](const std::pair<int64_t, int64_t>& s) {
        return !(cmd.filter_excluding_match && s.first <= he && s.second >= hs);
    };
    auto it = std::lower_bound(f.begin(), f.end(), std::make_pair(from, INT64_MIN));
    if (cmd.filter_rank == 0) {
        for (; it != f.end() && it->first == from; ++it)
            if (it->second == to && counts(*it)) return true;
        return false;
    }
    const long need = std::labs(static_cast<long>(cmd.filter_rank));
    long have = 0;
    for (; it != f.end() && it->first <= to; ++it)
        if (it->second <= to && counts(*it) && ++have >= need) return true;
    return false;
}

uint64_t step_seed(const GroupCommand& cmd) {
    if (cmd.has_seed) return cmd.seed;
    std::random_device rd;
    return (static_cast<uint64_t>(rd()) << 32) ^ rd();
}

namespace {
/// a uniform number in [0, n) (rejection sampling: no modulo bias, platform-independent)
uint64_t below(std::mt19937_64& g, uint64_t n) {
    const uint64_t lim = UINT64_MAX - UINT64_MAX % n;
    uint64_t r;
    do r = g(); while (r >= lim);
    return r % n;
}
}  // namespace

std::vector<size_t> shuffled_order(size_t n, uint64_t seed) {
    std::vector<size_t> idx(n);
    std::iota(idx.begin(), idx.end(), size_t{0});
    std::mt19937_64 g(seed);
    for (size_t i = n; i > 1; --i) std::swap(idx[i - 1], idx[static_cast<size_t>(below(g, i))]);
    return idx;
}

std::vector<size_t> sample_indices(size_t n, size_t k, uint64_t seed) {
    if (k >= n) {
        std::vector<size_t> all(n);
        std::iota(all.begin(), all.end(), size_t{0});
        return all;
    }
    // the first k of a partial Fisher–Yates, then in order
    std::vector<size_t> idx(n);
    std::iota(idx.begin(), idx.end(), size_t{0});
    std::mt19937_64 g(seed);
    for (size_t i = 0; i < k; ++i) std::swap(idx[i], idx[i + static_cast<size_t>(below(g, n - i))]);
    idx.resize(k);
    std::sort(idx.begin(), idx.end());
    return idx;
}

}  // namespace pando
