#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>

namespace manatree {

// Read-only structural attribute: stores (start, end) pairs for regions
// (sentences, paragraphs, documents) and supports O(log N) position-to-region
// lookup via binary search on sorted start positions.
//
// Optionally has per-region string values: one default (.val/.val.idx) and/or
// multiple named attributes (e.g. text_id.val, text_year.val) for #8 region attrs.
class StructuralAttr {
public:
    void open(const std::string& rgn_path, bool preload = false);

    // Add a named region attribute (e.g. "id" for text_id.val). Call after open().
    void add_region_attr(const std::string& attr_name,
                         const std::string& val_path,
                         const std::string& idx_path,
                         bool preload = false);

    size_t region_count() const;
    Region get(size_t idx) const;

    int64_t find_region(CorpusPos pos) const;
    bool same_region(CorpusPos a, CorpusPos b) const;

    bool has_values() const { return val_.valid(); }
    std::string_view region_value(size_t idx) const;

    // Named region attributes (#8): e.g. region_value("id", idx) for text_id.
    bool has_region_attr(const std::string& attr_name) const;
    std::string_view region_value(const std::string& attr_name, size_t idx) const;
    const std::vector<std::string>& region_attr_names() const { return region_attr_names_; }

private:
    MmapFile file_;       // .rgn: Region pairs
    MmapFile val_;        // .val: default (unnamed) region value strings
    MmapFile val_idx_;    // .val.idx: int64 byte offsets into .val
    std::unordered_map<std::string, std::pair<MmapFile, MmapFile>> region_attrs_;
    std::vector<std::string> region_attr_names_;
};

} // namespace manatree
