#include "api/group_counts.h"
#include "core/json_utils.h"
#include "query/sort_field.h"

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdint>
#include <functional>
#include <map>
#include <numeric>
#include <unordered_map>

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
        // a field with CQP options (`form %cd`, `form on match[-1]..match[-5]`)
        SortFieldSpec spec;
        if (parse_sort_field(fields[i], spec))
            key += read_sort_field(corpus, m, name_map, spec);
        else
            key += read_tabulate_field(corpus, m, name_map, fields[i]);
    }
    return key;
}

// ── P6.6: count by several fields as a limited tree ─────────────────────

namespace {

/// Buckets as id rows (kl ids each) + counts, and per column a decoder.
struct IdTable {
    size_t kl = 0;
    std::vector<int64_t> keys;      // row r: keys[r*kl .. r*kl+kl)
    std::vector<size_t> counts;
    std::function<std::string(size_t col, int64_t id)> decode;
};

std::string decode_bucket_column(const AggregateBucketData& d, size_t i, int64_t id) {
    const auto& col = d.columns[i];
    if (col.date_transform == AggregateBucketData::Column::DateTransform::None
        && col.kind == AggregateBucketData::Column::Kind::Positional)
        return std::string(col.pa->lexicon().get(static_cast<LexiconId>(id)));
    const auto& st = d.region_intern[i];
    if (id >= 1 && static_cast<size_t>(id) <= st.id_to_str.size()) return st.id_to_str[static_cast<size_t>(id - 1)];
    return std::string();
}

void build_level(const IdTable& t, const std::vector<size_t>& order, size_t lo, size_t hi, size_t level,
                 size_t limit, size_t child_limit, std::vector<GroupNode>& out, size_t& groups) {
    struct Run { size_t count, lo, hi; int64_t id; };
    std::vector<Run> runs;
    for (size_t i = lo; i < hi;) {
        const int64_t id = t.keys[order[i] * t.kl + level];
        size_t j = i, c = 0;
        while (j < hi && t.keys[order[j] * t.kl + level] == id) c += t.counts[order[j++]];
        runs.push_back({c, i, j, id});
        i = j;
    }
    groups = runs.size();
    const size_t want = (limit == 0 || limit >= runs.size()) ? runs.size() : limit;
    size_t keep = runs.size();
    if (want < runs.size()) {   // the top `want` by count, plus the ties at the cut (ordered by value below)
        auto desc = [](const Run& a, const Run& b) { return a.count > b.count; };
        std::nth_element(runs.begin(), runs.begin() + static_cast<std::ptrdiff_t>(want - 1), runs.end(), desc);
        const size_t cut = runs[want - 1].count;
        keep = static_cast<size_t>(std::partition(runs.begin(), runs.end(),
                                                  [cut](const Run& r) { return r.count >= cut; }) - runs.begin());
    }
    std::vector<std::pair<std::string, size_t>> vals;   // (value, run index)
    vals.reserve(keep);
    for (size_t k = 0; k < keep; ++k) vals.emplace_back(t.decode(level, runs[k].id), k);
    std::sort(vals.begin(), vals.end(), [&](const auto& a, const auto& b) {
        if (runs[a.second].count != runs[b.second].count) return runs[a.second].count > runs[b.second].count;
        return compare_sort_values(a.first, b.first);
    });
    if (vals.size() > want) vals.resize(want);
    out.reserve(vals.size());
    for (auto& [v, k] : vals) {
        GroupNode n;
        n.value = std::move(v);
        n.count = runs[k].count;
        if (level + 1 < t.kl)
            build_level(t, order, runs[k].lo, runs[k].hi, level + 1, child_limit, child_limit, n.children, n.groups);
        out.push_back(std::move(n));
    }
}

void emit_nodes(std::ostream& out, const std::vector<std::string>& fields, const std::vector<GroupNode>& nodes,
                size_t depth, size_t total, double parent, int indent) {
    for (size_t i = 0; i < nodes.size(); ++i) {
        const GroupNode& n = nodes[i];
        if (i) out << ",\n";
        for (int s = 0; s < indent; ++s) out << ' ';
        const double pct = total ? 100.0 * static_cast<double>(n.count) / static_cast<double>(total) : 0.0;
        out << "{\"field\": " << jstr(fields[depth]) << ", \"value\": " << jstr(n.value) << ", \"count\": " << n.count
            << ", \"pct\": " << pct;
        if (depth > 0)
            out << ", \"pct_of_parent\": " << (parent > 0 ? 100.0 * static_cast<double>(n.count) / parent : 0.0);
        if (depth + 1 < fields.size()) {
            out << ", \"groups\": " << n.groups << ", \"children\": [";
            if (!n.children.empty()) {
                out << "\n";
                emit_nodes(out, fields, n.children, depth + 1, total, static_cast<double>(n.count), indent + 2);
                out << "\n";
                for (int s = 0; s < indent; ++s) out << ' ';
            }
            out << "]";
        }
        out << "}";
    }
}

}  // namespace

GroupTree group_tree(const Corpus& corpus, const MatchSet& ms, const std::vector<std::string>& fields,
                     const NameIndexMap& name_map, size_t top_limit, size_t child_limit) {
    GroupTree tree;
    IdTable t;
    t.kl = fields.size();
    const AggregateBucketData* agg = ms.aggregate_buckets.get();
    tree.total = agg ? agg->total_hits : ms.matches.size();
    std::vector<std::vector<std::string>> interned;   // the per-match path: values per column
    if (agg && agg->columns.size() == t.kl) {
        agg->for_each_bucket([&](const int64_t* key, size_t len, size_t c) {
            for (size_t i = 0; i < t.kl; ++i) t.keys.push_back(i < len ? key[i] : 0);
            t.counts.push_back(c);
        });
        t.decode = [agg](size_t col, int64_t id) { return decode_bucket_column(*agg, col, id); };
    } else {
        // hits kept: intern each field's values, count per id row
        interned.resize(t.kl);
        std::vector<std::unordered_map<std::string, int64_t>> ids(t.kl);
        std::unordered_map<std::string, size_t> row_of;   // id row (bytes) -> row
        std::vector<int64_t> row(t.kl);
        std::vector<std::string> parts;
        for (const auto& m : ms.matches) {
            const std::string key = make_group_key(corpus, m, name_map, fields);
            parts.clear();
            size_t a = 0;
            for (size_t i = 0; i <= key.size(); ++i)
                if (i == key.size() || key[i] == '\t') { parts.emplace_back(key, a, i - a); a = i + 1; }
            if (parts.size() != t.kl) continue;
            for (size_t i = 0; i < t.kl; ++i) {
                auto [it, fresh] = ids[i].emplace(parts[i], static_cast<int64_t>(interned[i].size()));
                if (fresh) interned[i].push_back(parts[i]);
                row[i] = it->second;
            }
            std::string rk(reinterpret_cast<const char*>(row.data()), row.size() * sizeof(int64_t));
            auto [it, fresh] = row_of.emplace(std::move(rk), t.counts.size());
            if (fresh) {
                t.keys.insert(t.keys.end(), row.begin(), row.end());
                t.counts.push_back(0);
            }
            ++t.counts[it->second];
        }
        t.decode = [&interned](size_t col, int64_t id) { return interned[col][static_cast<size_t>(id)]; };
    }
    tree.groups = t.counts.size();
    std::vector<size_t> order(t.counts.size());
    std::iota(order.begin(), order.end(), size_t{0});
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return std::lexicographical_compare(t.keys.begin() + static_cast<std::ptrdiff_t>(a * t.kl),
                                            t.keys.begin() + static_cast<std::ptrdiff_t>(a * t.kl + t.kl),
                                            t.keys.begin() + static_cast<std::ptrdiff_t>(b * t.kl),
                                            t.keys.begin() + static_cast<std::ptrdiff_t>(b * t.kl + t.kl));
    });
    if (t.kl > 0) build_level(t, order, 0, order.size(), 0, top_limit, child_limit, tree.top, tree.top_groups);
    return tree;
}

void emit_group_tree_json(std::ostream& out, const std::vector<std::string>& fields, const GroupTree& tree) {
    out << "  \"hierarchy\": [\n";
    emit_nodes(out, fields, tree.top, 0, tree.total, static_cast<double>(tree.total), 4);
    out << "\n  ]";
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
