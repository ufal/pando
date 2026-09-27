#include "api/group_counts.h"

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdint>
#include <map>
#include <numeric>

namespace pando {

namespace {

bool parse_i64_strict(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    size_t i = 0;
    bool neg = false;
    if (s[i] == '+' || s[i] == '-') {
        neg = (s[i] == '-');
        ++i;
    }
    if (i >= s.size()) return false;
    int64_t v = 0;
    for (; i < s.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (!std::isdigit(c)) return false;
        int d = c - '0';
        if (v > (INT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    out = neg ? -v : v;
    return true;
}

bool parse_roman_numeral(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    auto val = [](char c) -> int {
        switch (c) {
            case 'I': return 1; case 'V': return 5; case 'X': return 10; case 'L': return 50;
            case 'C': return 100; case 'D': return 500; case 'M': return 1000;
            default: return 0;
        }
    };
    int64_t total = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = static_cast<char>(std::toupper(static_cast<unsigned char>(s[i])));
        int cur = val(c);
        if (cur == 0) return false;
        int next = 0;
        if (i + 1 < s.size()) {
            char n = static_cast<char>(std::toupper(static_cast<unsigned char>(s[i + 1])));
            next = val(n);
            if (next == 0) return false;
        }
        if (next > cur) total -= cur;
        else total += cur;
    }
    out = total;
    return total > 0;
}

// A strict weak order (std::sort needs one): numeric values (integers, Roman
// numerals) first, by value, then bytewise; other values bytewise. The earlier
// "numerically when both are numbers, else bytewise" was not transitive
// ("V" < "1954" < "2.5" < "V"), so the order of equal counts depended on the
// input order — and a non-transitive comparator is undefined behaviour in std::sort.
bool compare_sort_values(std::string_view a, std::string_view b) {
    int64_t ai = 0, bi = 0;
    const bool an = parse_i64_strict(a, ai) || parse_roman_numeral(a, ai);
    const bool bn = parse_i64_strict(b, bi) || parse_roman_numeral(b, bi);
    if (an != bn) return an;
    if (an && ai != bi) return ai < bi;
    return a < b;
}

bool by_count_then_key(const std::pair<std::string, size_t>& a, const std::pair<std::string, size_t>& b) {
    if (a.second != b.second) return a.second > b.second;
    return compare_group_keys(a.first, b.first);
}

}  // namespace

bool compare_group_keys(std::string_view a, std::string_view b) {
    size_t sa = 0, sb = 0;
    while (true) {
        size_t ea = a.find('\t', sa);
        size_t eb = b.find('\t', sb);
        if (ea == std::string_view::npos) ea = a.size();
        if (eb == std::string_view::npos) eb = b.size();
        std::string_view pa = a.substr(sa, ea - sa);
        std::string_view pb = b.substr(sb, eb - sb);
        if (pa != pb) return compare_sort_values(pa, pb);
        if (ea == a.size() && eb == b.size()) return false;
        sa = (ea < a.size()) ? ea + 1 : a.size();
        sb = (eb < b.size()) ? eb + 1 : b.size();
    }
}

std::string make_group_key(const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
                           const std::vector<std::string>& fields) {
    std::string key;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) key += '\t';
        key += read_tabulate_field(corpus, m, name_map, fields[i]);
    }
    return key;
}

GroupRows group_rows(const Corpus& corpus, const MatchSet& ms, const std::vector<std::string>& fields,
                     const NameIndexMap& name_map, size_t limit, bool explode_mv) {
    GroupRows out;
    const AggregateBucketData* agg = ms.aggregate_buckets.get();
    out.total = agg ? agg->total_hits : ms.matches.size();

    if (agg && !explode_mv) {
        // Id keys decode to distinct strings (lexicon ids, interned region / date
        // values), so each bucket is one group: select the top `limit` by count on
        // the ids, decode only those (and the ties at the cut, for the key order).
        // (count, offset of the key in keybuf); keys of 1-2 ids, no allocation per bucket
        std::vector<std::pair<size_t, size_t>> v;
        std::vector<int64_t> keybuf;
        const size_t kl = agg->columns.size();
        v.reserve(agg->bucket_count());
        keybuf.reserve(v.capacity() * kl);
        agg->for_each_bucket([&](const int64_t* key, size_t len, size_t c) {
            v.emplace_back(c, keybuf.size());
            for (size_t i = 0; i < kl; ++i) keybuf.push_back(i < len ? key[i] : 0);
        });
        out.groups = v.size();
        const size_t want = (limit == 0 || limit >= v.size()) ? v.size() : limit;
        auto desc = [](const auto& a, const auto& b) { return a.first > b.first; };
        size_t keep_n = v.size();
        if (want < v.size()) {
            std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(want - 1), v.end(), desc);
            const size_t cut = v[want - 1].first;
            auto mid = std::partition(v.begin(), v.end(), [cut](const auto& x) { return x.first >= cut; });
            keep_n = static_cast<size_t>(mid - v.begin());
        }
        out.rows.reserve(keep_n);
        for (size_t i = 0; i < keep_n; ++i)
            out.rows.emplace_back(decode_aggregate_bucket_key(*agg, keybuf.data() + v[i].second, kl),
                                  v[i].first);
        std::sort(out.rows.begin(), out.rows.end(), by_count_then_key);
        if (out.rows.size() > want) out.rows.resize(want);
        return out;
    }

    std::map<std::string, size_t> counts;
    if (agg) {
        agg->for_each_bucket([&](const int64_t* key, size_t len, size_t c) {
            counts[decode_aggregate_bucket_key(*agg, key, len)] += c;
        });
    } else {
        for (const auto& m : ms.matches) ++counts[make_group_key(corpus, m, name_map, fields)];
    }
    // RG-5f: a single multivalue field — "artist|writer" counts for both components.
    if (explode_mv) {
        std::map<std::string, size_t> exploded;
        for (const auto& [key, count] : counts) {
            if (key.find('|') != std::string::npos) {
                size_t start = 0;
                while (start < key.size()) {
                    size_t p = key.find('|', start);
                    if (p == std::string::npos) p = key.size();
                    std::string comp = key.substr(start, p - start);
                    if (!comp.empty()) exploded[comp] += count;
                    start = p + 1;
                }
            } else {
                exploded[key] += count;
            }
        }
        counts = std::move(exploded);
    }
    out.groups = counts.size();
    out.rows.assign(counts.begin(), counts.end());
    std::sort(out.rows.begin(), out.rows.end(), by_count_then_key);
    if (limit > 0 && out.rows.size() > limit) out.rows.resize(limit);
    return out;
}

void sort_matches_by_key(const Corpus& corpus, std::vector<Match>& matches,
                         const NameIndexMap& name_map, const std::vector<std::string>& fields,
                         bool group_key_order) {
    std::vector<std::string> keys;
    keys.reserve(matches.size());
    for (const auto& m : matches) keys.push_back(make_group_key(corpus, m, name_map, fields));
    std::vector<size_t> idx(matches.size());
    std::iota(idx.begin(), idx.end(), size_t{0});
    if (group_key_order)
        std::stable_sort(idx.begin(), idx.end(),
                         [&](size_t a, size_t b) { return compare_group_keys(keys[a], keys[b]); });
    else
        std::stable_sort(idx.begin(), idx.end(), [&](size_t a, size_t b) { return keys[a] < keys[b]; });
    std::vector<Match> sorted;
    sorted.reserve(matches.size());
    for (size_t i : idx) sorted.push_back(std::move(matches[i]));
    matches = std::move(sorted);
}

}  // namespace pando
