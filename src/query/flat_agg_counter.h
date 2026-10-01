#pragma once

// Flat counters for `count by` (P7.2 / P7.5) and the sort index (P7.10): one or
// two positional / region attribute columns as packed lexicon / value ids.
// Internal to the executor (executor.cpp, executor_sort.cpp).

#include "query/executor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace pando {

/// P7.5: a region attribute as a flat-counter column: region → value id (1-based,
/// the id the bucket keys and region_intern use; 0 = the position is in no region),
/// one table per query so every partition keys the values alike. Positions arrive
/// mostly in corpus order: a cursor, with a binary search when a hit goes back.
struct FlatRegionCol {
    static constexpr size_t kMaxValues = size_t{1} << 18;
    const Region* regions = nullptr;
    size_t n = 0, cur = 0;
    std::vector<uint32_t> rid;
    std::vector<std::string> values;   // value id - 1 → value

    /// The region values as fill_aggregate_key reads them (region_value); false when
    /// the attribute has no reverse index or too many values.
    bool build(const StructuralAttr& sa, const std::string& attr) {
        const LexiconId L = sa.region_attr_lex_size(attr);
        if (L <= 0 || static_cast<size_t>(L) > kMaxValues) return false;
        const std::vector<LexiconId> r2l = sa.precompute_region_to_lex(attr);
        if (r2l.empty()) return false;
        regions = sa.region_data();
        n = sa.region_count();
        values.reserve(static_cast<size_t>(L));
        for (LexiconId i = 0; i < L; ++i) values.emplace_back(sa.region_attr_lex_get(attr, i));
        rid.assign(n, 0);
        std::unordered_map<std::string, uint32_t> extra;
        for (size_t r = 0; r < n && r < r2l.size(); ++r) {
            if (r2l[r] != UNKNOWN_LEX) { rid[r] = static_cast<uint32_t>(r2l[r]) + 1; continue; }
            std::string v(sa.region_value(attr, r));
            const LexiconId l = sa.region_attr_lex_lookup(attr, v);
            if (l != UNKNOWN_LEX) { rid[r] = static_cast<uint32_t>(l) + 1; continue; }
            auto it = extra.find(v);
            if (it == extra.end()) {
                if (values.size() >= kMaxValues) return false;
                values.push_back(v);
                it = extra.emplace(std::move(v), static_cast<uint32_t>(values.size())).first;
            }
            rid[r] = it->second;
        }
        return true;
    }
    uint32_t at(CorpusPos pos) {
        if (cur < n && regions[cur].start <= pos) {
            for (int k = 0; k < 4; ++k) {
                if (pos <= regions[cur].end) return rid[cur];
                if (cur + 1 >= n || regions[cur + 1].start > pos) return 0;   // a gap
                ++cur;
            }
        }
        const Region* it = std::upper_bound(regions, regions + n, pos,
                                            [](CorpusPos p, const Region& r) { return p < r.start; });
        if (it == regions) return 0;
        cur = static_cast<size_t>(it - regions) - 1;
        return pos <= regions[cur].end ? rid[cur] : 0;
    }
    uint64_t card() const { return values.size() + 1; }
};

// ── P7.2: flat counters for `count by` on positional attributes ─────────
// One column: a dense array indexed by lexicon id; two columns: an open-
// addressing table on the packed key id1 * |lexicon 2| + id2. Replaces one
// std::vector<int64_t> key + unordered_map<vector> probe per hit. The compact
// buckets are handed to AggregateBucketData (flat_*) when the query finishes and
// read through for_each_bucket(), without converting them to vector keys.
// One column over a large lexicon starts as a hash table and turns dense once it
// holds 1/8 of the lexicon, so a rare query does not allocate and scan a
// lexicon-sized array.
struct FlatAggCounter {
    bool on = false;
    int ncols = 0;
    int tok[2] = {-1, -1};                 // label's token index; -1 = match start (first_pos)
    const PositionalAttr* pa[2] = {nullptr, nullptr};
    std::unique_ptr<FlatRegionCol> reg[2];   // P7.5: a region attribute column
    uint64_t v2 = 1;
    bool dense_mode = false;
    std::vector<uint64_t> dense;
    std::vector<uint64_t> keys, vals;      // keys[i] == kEmpty: free
    size_t used = 0;
    uint64_t v1 = 0;
    static constexpr uint64_t kEmpty = ~uint64_t{0};
    static constexpr uint64_t kDenseAlways = uint64_t{1} << 20;   // 8 MB of counters
    static constexpr uint64_t kDenseMax = uint64_t{1} << 24;

    static uint64_t mix(uint64_t x) {
        x ^= x >> 33; x *= 0xff51afd7ed558ccdULL; x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL; x ^= x >> 33;
        return x;
    }
    void grow() {
        if (ncols == 1 && v1 <= kDenseMax && used * 8 >= v1) {   // hash → dense
            dense.assign(static_cast<size_t>(std::max<uint64_t>(1, v1)), 0);
            for (size_t i = 0; i < keys.size(); ++i)
                if (keys[i] != kEmpty) dense[static_cast<size_t>(keys[i])] += vals[i];
            keys.clear(); keys.shrink_to_fit(); vals.clear(); vals.shrink_to_fit();
            used = 0;
            dense_mode = true;
            return;
        }
        std::vector<uint64_t> ok = std::move(keys), ov = std::move(vals);
        const size_t cap = ok.empty() ? 1024 : ok.size() * 2;
        keys.assign(cap, kEmpty);
        vals.assign(cap, 0);
        used = 0;
        for (size_t i = 0; i < ok.size(); ++i)
            if (ok[i] != kEmpty) add_key(ok[i], ov[i]);
    }
    void add_key(uint64_t k, uint64_t c) {
        if ((used + 1) * 10 > keys.size() * 7) {
            grow();
            if (dense_mode) { dense[static_cast<size_t>(k)] += c; return; }
        }
        const size_t mask = keys.size() - 1;
        size_t i = static_cast<size_t>(mix(k)) & mask;
        while (keys[i] != kEmpty && keys[i] != k) i = (i + 1) & mask;
        if (keys[i] == kEmpty) { keys[i] = k; ++used; }
        vals[i] += c;
    }
    static bool flat_structure(const Corpus& corpus, const StructuralAttr& sa) {
        for (const std::string& st : corpus.structure_names())
            if (corpus.has_structure(st) && &corpus.structure(st) == &sa)
                return !corpus.is_nested(st) && !corpus.is_overlapping(st) && !corpus.is_zerowidth(st);
        return false;
    }
    /// Set up for `agg` when every column is a plain positional attribute (at most
    /// two, no date / strlen transform) whose label resolves to a query token.
    /// P7.5: also a region attribute of a flat structure (`text_langcode`, or
    /// `b.text_langcode` for a token label) when the hits carry no named regions.
    bool init(const AggregateBucketData& agg, const NameIndexMap& name_map, size_t ntok,
              const Corpus& corpus, bool named_regions) {
        if (agg.columns.empty() || agg.columns.size() > 2) return false;
        for (size_t c = 0; c < agg.columns.size(); ++c) {
            const auto& col = agg.columns[c];
            using K = AggregateBucketData::Column::Kind;
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None) return false;
            if (col.kind == K::Positional) {
                if (!col.pa) return false;
            } else if (col.kind == K::Region) {
                if (!col.sa || named_regions || !flat_structure(corpus, *col.sa)) return false;
            } else {
                return false;
            }
            if (!col.named_anchor.empty()) {
                auto it = name_map.find(col.named_anchor);
                if (it == name_map.end() || it->second >= ntok) return false;
                tok[c] = static_cast<int>(it->second);
            }
            if (col.kind == K::Region) {
                reg[c] = std::make_unique<FlatRegionCol>();
                if (!reg[c]->build(*col.sa, col.region_attr_name)) return false;
            }
            pa[c] = col.pa;
        }
        ncols = static_cast<int>(agg.columns.size());
        auto card = [&](int c) -> uint64_t {
            return reg[c] ? reg[c]->card() : static_cast<uint64_t>(std::max<int64_t>(0, pa[c]->lexicon().size()));
        };
        if (ncols == 2) v2 = std::max<uint64_t>(1, card(1));
        v1 = card(0);
        dense_mode = ncols == 1 && v1 <= kDenseAlways;
        if (dense_mode) dense.assign(static_cast<size_t>(std::max<uint64_t>(1, v1)), 0);
        on = true;
        return true;
    }
    /// Key of one hit from its token starts `starts[0..n)`; false = not counted
    /// (a labelled token that did not take part, as fill_aggregate_key).
    bool key_of(const CorpusPos* starts, size_t n, uint64_t& key) {
        CorpusPos first = NO_HEAD;
        uint64_t id[2] = {0, 0};
        for (int c = 0; c < ncols; ++c) {
            CorpusPos pos;
            if (tok[c] >= 0) {
                if (static_cast<size_t>(tok[c]) >= n) return false;
                pos = starts[tok[c]];
                if (pos == NO_HEAD) return false;
            } else {
                if (first == NO_HEAD) {
                    for (size_t i = 0; i < n; ++i)
                        if (starts[i] != NO_HEAD && (first == NO_HEAD || starts[i] < first)) first = starts[i];
                    if (first == NO_HEAD) first = 0;   // as Match::first_pos()
                }
                pos = first;
            }
            if (reg[c]) {
                id[c] = reg[c]->at(pos);
                if (!id[c]) return false;   // in no region (as fill_aggregate_key)
            } else {
                id[c] = static_cast<uint64_t>(pa[c]->id_at(pos));
            }
        }
        key = ncols == 1 ? id[0] : id[0] * v2 + id[1];
        return true;
    }
    void inc(uint64_t key) {
        if (dense_mode) ++dense[static_cast<size_t>(key)];
        else add_key(key, 1);
    }
    /// Hand the compact buckets to `agg` (read through AggregateBucketData::for_each_bucket).
    void flush(AggregateBucketData& agg) {
        if (!on) return;
        agg.flat_ncols = ncols;
        agg.flat_v2 = v2;
        // region columns: the keys are value ids of the full table (the same in every
        // partition, so merge_aggregate_into re-keys nothing)
        if (agg.region_intern.size() < agg.columns.size()) agg.region_intern.resize(agg.columns.size());
        for (int c = 0; c < ncols; ++c) {
            if (!reg[c]) continue;
            auto& ri = agg.region_intern[static_cast<size_t>(c)];
            ri.str_to_id.clear();
            ri.id_to_str = std::move(reg[c]->values);
            for (size_t j = 0; j < ri.id_to_str.size(); ++j)
                ri.str_to_id.emplace(ri.id_to_str[j], static_cast<int64_t>(j + 1));
            reg[c].reset();
        }
        if (dense_mode) {
            agg.flat_dense = std::move(dense);
        } else {
            static_assert(kEmpty == AggregateBucketData::kFlatEmpty, "same empty marker");
            agg.flat_keys = std::move(keys);
            agg.flat_vals = std::move(vals);
        }
        on = false;
    }
};


} // namespace pando
