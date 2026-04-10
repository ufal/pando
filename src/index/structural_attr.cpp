#include "index/structural_attr.h"
#include <algorithm>
#include <cstddef>
#include <fstream>

namespace pando {

void StructuralAttr::open(const std::string& rgn_path, bool preload) {
    file_ = MmapFile::open(rgn_path, preload);
    par_ = MmapFile{};
    region_attrs_.clear();
    region_value_rev_.clear();
    region_attr_names_.clear();

    // Try to load optional default region values (.val + .val.idx)
    std::string base = rgn_path.substr(0, rgn_path.size() - 4); // strip .rgn
    std::string val_path = base + ".val";
    std::string idx_path = base + ".val.idx";
    std::ifstream probe(val_path);
    if (probe.good()) {
        val_     = MmapFile::open(val_path, preload);
        val_idx_ = MmapFile::open(idx_path, preload);
    }

    // RG-REG-1: optional parent region index (.par), one int32 per .rgn row
    {
        std::string par_path = base + ".par";
        std::ifstream probe_par(par_path);
        if (probe_par.good()) {
            probe_par.close();
            MmapFile par = MmapFile::open(par_path, preload);
            if (par.valid() && par.count<int32_t>() == file_.count<Region>())
                par_ = std::move(par);
        }
    }
}

void StructuralAttr::add_region_attr(const std::string& attr_name,
                                    const std::string& val_path,
                                    const std::string& idx_path,
                                    bool preload) {
    MmapFile v = MmapFile::open(val_path, preload);
    MmapFile i = MmapFile::open(idx_path, preload);
    region_attrs_[attr_name] = std::make_pair(std::move(v), std::move(i));
    region_attr_names_.push_back(attr_name);

    std::string base = val_path;
    if (base.size() >= 4 && base.compare(base.size() - 4, 4, ".val") == 0)
        base.resize(base.size() - 4);
    else
        return;
    std::ifstream probe(base + ".rev.idx");
    if (!probe.good()) return;
    probe.close();

    RegionValueRev rv;
    rv.lex.open(base, preload);
    rv.rev = MmapFile::open(base + ".rev", preload);
    rv.rev_idx = MmapFile::open(base + ".rev.idx", preload);
    if (rv.rev.valid() && rv.rev_idx.valid())
        region_value_rev_[attr_name] = std::move(rv);
}

bool StructuralAttr::has_region_attr(const std::string& attr_name) const {
    return region_attrs_.count(attr_name) != 0;
}

std::string_view StructuralAttr::region_value(const std::string& attr_name, size_t idx) const {
    auto it = region_attrs_.find(attr_name);
    if (it == region_attrs_.end()) return {};
    const MmapFile& v = it->second.first;
    const MmapFile& i = it->second.second;
    if (!v.valid() || !i.valid()) return {};
    const auto* off = i.as<int64_t>();
    if (idx + 1 >= i.count<int64_t>()) return {};
    const char* base = static_cast<const char*>(v.data());
    return std::string_view(base + off[idx],
                            static_cast<size_t>(off[idx + 1] - off[idx] - 1));
}

size_t StructuralAttr::region_count() const {
    return file_.count<Region>();
}

Region StructuralAttr::get(size_t idx) const {
    return file_.as<Region>()[idx];
}

bool StructuralAttr::has_parent_region_id() const {
    return par_.valid() && par_.count<int32_t>() == file_.count<Region>();
}

int32_t StructuralAttr::parent_region_id(size_t region_idx) const {
    if (!has_parent_region_id() || region_idx >= region_count()) return -1;
    return par_.as<int32_t>()[region_idx];
}

bool StructuralAttr::region_is_ancestor_of(size_t ancestor_idx, size_t descendant_idx) const {
    if (!has_parent_region_id()) return false;
    if (ancestor_idx >= region_count() || descendant_idx >= region_count()) return false;
    size_t cur = descendant_idx;
    while (true) {
        if (cur == ancestor_idx) return true;
        int32_t par = parent_region_id(cur);
        if (par < 0) return false;
        cur = static_cast<size_t>(par);
    }
}

int64_t StructuralAttr::find_region(CorpusPos pos) const {
    size_t n = region_count();
    if (n == 0) return -1;
    const Region* r = file_.as<Region>();

    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (r[mid].start <= pos) lo = mid + 1;
        else                     hi = mid;
    }
    if (lo == 0) return -1;
    --lo;
    if (pos >= r[lo].start && pos <= r[lo].end) return static_cast<int64_t>(lo);
    return -1;
}

int64_t StructuralAttr::find_innermost_region(CorpusPos pos) const {
    size_t n = region_count();
    if (n == 0) return -1;
    const Region* r = file_.as<Region>();

    // Binary search: find rightmost index with start <= pos.
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (r[mid].start <= pos) lo = mid + 1;
        else                     hi = mid;
    }
    if (lo == 0) return -1;

    // With (start, -end) sort order, the landing point (lo-1) has the largest
    // start <= pos and — among regions sharing that start — the smallest end.
    // If it contains pos, it's the innermost.  Otherwise scan backwards;
    // the first region containing pos is the innermost candidate.
    for (size_t i = lo; i-- > 0; ) {
        if (r[i].start > r[i].end) continue;  // skip zero-width regions
        if (pos >= r[i].start && pos <= r[i].end)
            return static_cast<int64_t>(i);
    }
    return -1;
}

int64_t StructuralAttr::find_region_from(CorpusPos pos, int64_t hint) const {
    size_t n = region_count();
    if (n == 0) return -1;
    const Region* r = file_.as<Region>();

    // If hint is valid, try linear advance from there
    if (hint >= 0 && static_cast<size_t>(hint) < n) {
        size_t h = static_cast<size_t>(hint);
        // Check if pos is still in the hinted region
        if (pos >= r[h].start && pos <= r[h].end) return hint;
        // Advance forward (pos > r[h].end means we moved past this region)
        if (pos > r[h].end) {
            // Scan forward a few regions (bounded to avoid degenerate cases)
            for (size_t i = h + 1; i < n && i <= h + 8; ++i) {
                if (pos < r[i].start) return -1;  // gap between regions
                if (pos <= r[i].end) return static_cast<int64_t>(i);
            }
        }
    }
    // Fallback to full binary search
    return find_region(pos);
}

bool StructuralAttr::same_region(CorpusPos a, CorpusPos b) const {
    int64_t ra = find_region(a);
    return ra >= 0 && ra == find_region(b);
}

std::string_view StructuralAttr::region_value(size_t idx) const {
    if (!val_.valid() || !val_idx_.valid()) return {};
    const auto* off = val_idx_.as<int64_t>();
    const char* base = static_cast<const char*>(val_.data());
    return std::string_view(base + off[idx],
                            static_cast<size_t>(off[idx + 1] - off[idx] - 1));
}

bool StructuralAttr::has_region_value_reverse(const std::string& attr_name) const {
    return region_value_rev_.count(attr_name) != 0;
}

size_t StructuralAttr::count_regions_with_attr_eq(const std::string& attr_name,
                                                    const std::string& value) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return SIZE_MAX;
    const RegionValueRev& rv = it->second;
    LexiconId id = rv.lex.lookup(value);
    if (id == UNKNOWN_LEX) return 0;
    const auto* idx = rv.rev_idx.as<int64_t>();
    return static_cast<size_t>(idx[static_cast<size_t>(id) + 1] - idx[static_cast<size_t>(id)]);
}

bool StructuralAttr::region_matches_attr_eq_rev(const std::string& attr_name, size_t region_idx,
                                                  const std::string& value) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return false;
    const RegionValueRev& rv = it->second;
    LexiconId vid = rv.lex.lookup(value);
    if (vid == UNKNOWN_LEX) return false;
    const auto* idx = rv.rev_idx.as<int64_t>();
    size_t vi = static_cast<size_t>(vid);
    if (vi + 1 >= rv.rev_idx.count<int64_t>()) return false;
    int64_t lo = idx[vi];
    int64_t hi = idx[vi + 1];
    const int64_t* p = rv.rev.as<int64_t>() + lo;
    size_t n = static_cast<size_t>(hi - lo);
    int64_t key = static_cast<int64_t>(region_idx);
    return std::binary_search(p, p + n, key);
}

size_t StructuralAttr::token_span_sum_for_attr_eq(const std::string& attr_name,
                                                    const std::string& value) const {
    auto it = region_value_rev_.find(attr_name);
    if (it != region_value_rev_.end()) {
        const RegionValueRev& rv = it->second;
        LexiconId vid = rv.lex.lookup(value);
        if (vid != UNKNOWN_LEX) {
            const auto* idx = rv.rev_idx.as<int64_t>();
            size_t vi = static_cast<size_t>(vid);
            if (vi + 1 < rv.rev_idx.count<int64_t>()) {
                int64_t lo = idx[vi];
                int64_t hi = idx[vi + 1];
                const int64_t* p = rv.rev.as<int64_t>() + lo;
                size_t sum = 0;
                for (int64_t k = 0; k < hi - lo; ++k) {
                    size_t ri = static_cast<size_t>(p[k]);
                    Region r = get(ri);
                    sum += static_cast<size_t>(r.end - r.start + 1);
                }
                if (sum > 0) return sum;
            }
        }
        // Lex miss, empty posting, or rev/lex out of sync with .val strings: fall through
        // so freq row keys (from read_tabulate_field / decode_aggregate_bucket_key) still
        // get correct per-value denominators for IPM.
    }
    // No .rev index: still compute per-value token span for freq / IPM denominators by
    // scanning .rgn rows (same row index as .val; O(n_regions) per call).
    if (!has_region_attr(attr_name)) return SIZE_MAX;
    size_t sum = 0;
    size_t n = region_count();
    for (size_t ri = 0; ri < n; ++ri) {
        std::string_view v = region_value(attr_name, ri);
        if (v == value) {
            Region r = get(ri);
            if (r.start <= r.end)
                sum += static_cast<size_t>(r.end - r.start + 1);
        }
    }
    return sum;
}

// ── Fast aggregation support ─────────────────────────────────────────────

LexiconId StructuralAttr::region_attr_lex_lookup(const std::string& attr_name,
                                                  const std::string& value) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return UNKNOWN_LEX;
    return it->second.lex.lookup(value);
}

LexiconId StructuralAttr::region_attr_lex_size(const std::string& attr_name) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return 0;
    return it->second.lex.size();
}

std::string_view StructuralAttr::region_attr_lex_get(const std::string& attr_name,
                                                      LexiconId id) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return {};
    return it->second.lex.get(id);
}

std::vector<LexiconId> StructuralAttr::precompute_region_to_lex(
        const std::string& attr_name) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return {};

    const RegionValueRev& rv = it->second;
    size_t n_regions = region_count();
    std::vector<LexiconId> mapping(n_regions, UNKNOWN_LEX);

    const auto* idx = rv.rev_idx.as<int64_t>();
    LexiconId n_vals = rv.lex.size();

    for (LexiconId vid = 0; vid < n_vals; ++vid) {
        size_t vi = static_cast<size_t>(vid);
        if (vi + 1 >= rv.rev_idx.count<int64_t>()) break;
        int64_t lo = idx[vi];
        int64_t hi = idx[vi + 1];
        const int64_t* p = rv.rev.as<int64_t>() + lo;
        for (int64_t k = 0; k < hi - lo; ++k) {
            size_t ri = static_cast<size_t>(p[k]);
            if (ri < n_regions) mapping[ri] = vid;
        }
    }
    return mapping;
}

bool StructuralAttr::regions_for_value(const std::string& attr_name,
                                        const std::string& value,
                                        const int64_t*& out_regions,
                                        size_t& out_count) const {
    auto it = region_value_rev_.find(attr_name);
    if (it == region_value_rev_.end()) return false;
    const RegionValueRev& rv = it->second;
    LexiconId vid = rv.lex.lookup(value);
    if (vid == UNKNOWN_LEX) { out_count = 0; return true; }
    const auto* idx = rv.rev_idx.as<int64_t>();
    size_t vi = static_cast<size_t>(vid);
    if (vi + 1 >= rv.rev_idx.count<int64_t>()) { out_count = 0; return true; }
    int64_t lo = idx[vi];
    int64_t hi = idx[vi + 1];
    out_regions = rv.rev.as<int64_t>() + lo;
    out_count = static_cast<size_t>(hi - lo);
    return true;
}

} // namespace pando
