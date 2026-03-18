#include "index/structural_attr.h"
#include <fstream>

namespace manatree {

void StructuralAttr::open(const std::string& rgn_path, bool preload) {
    file_ = MmapFile::open(rgn_path, preload);
    region_attrs_.clear();
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
}

void StructuralAttr::add_region_attr(const std::string& attr_name,
                                    const std::string& val_path,
                                    const std::string& idx_path,
                                    bool preload) {
    MmapFile v = MmapFile::open(val_path, preload);
    MmapFile i = MmapFile::open(idx_path, preload);
    region_attrs_[attr_name] = std::make_pair(std::move(v), std::move(i));
    region_attr_names_.push_back(attr_name);
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

} // namespace manatree
