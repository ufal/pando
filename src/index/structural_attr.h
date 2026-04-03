#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/lexicon.h"
#include <cstdint>
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

    // RG-REG-1: Optional per-region parent index (nested structural types only).
    // Parallel to .rgn row order; -1 = root or unknown. Absent if index has no .par file.
    bool has_parent_region_id() const;
    int32_t parent_region_id(size_t region_idx) const;

    // Direct access to the Region array pointer (for cursor-based iteration).
    const Region* region_data() const { return file_.as<Region>(); }

    int64_t find_region(CorpusPos pos) const;

    // SM-ROAD-2: Find the innermost (most deeply nested) region containing pos.
    // Requires regions to be sorted by (start, -end) — i.e. for nested structure
    // types, the builder sorts outermost-first within the same start position.
    // Falls back to scanning backwards from the binary-search landing point.
    int64_t find_innermost_region(CorpusPos pos) const;

    // Find ALL regions containing pos. Calls f(region_idx) for each.
    // f returns true to continue, false to stop early.
    // For overlapping/nested structures a position can be in multiple regions.
    // Scans backwards from the binary-search landing point.
    template<typename F>
    void for_each_region_at(CorpusPos pos, F&& f) const {
        size_t n = region_count();
        if (n == 0) return;
        const Region* r = file_.as<Region>();
        // Binary search: find rightmost index with start <= pos
        size_t lo = 0, hi = n;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (r[mid].start <= pos) lo = mid + 1;
            else                     hi = mid;
        }
        // Scan backwards: all regions with start <= pos are candidates
        for (size_t i = lo; i-- > 0; ) {
            if (r[i].end < pos) continue;      // region ends before pos
            if (r[i].start > r[i].end) continue; // zero-width sentinel
            // r[i] contains pos
            if (!f(i)) return;
        }
    }

    // #28: Cursor-based find_region — start search from hint index.
    // When iterating sorted positions, pass the last returned region index
    // as hint; advances linearly from there, falling back to binary search
    // when the hint is stale. Returns -1 if pos is not in any region.
    int64_t find_region_from(CorpusPos pos, int64_t hint) const;

    bool same_region(CorpusPos a, CorpusPos b) const;

    bool has_values() const { return val_.valid(); }
    std::string_view region_value(size_t idx) const;

    // Named region attributes (#8): e.g. region_value("id", idx) for text_id.
    bool has_region_attr(const std::string& attr_name) const;
    std::string_view region_value(const std::string& attr_name, size_t idx) const;
    const std::vector<std::string>& region_attr_names() const { return region_attr_names_; }

    // Optional reverse index (pando-index): value → sorted region ids in .rev / .lex next to .val.
    bool has_region_value_reverse(const std::string& attr_name) const;
    // O(1) count of regions with this attribute value; 0 if unknown value; SIZE_MAX if no rev index.
    size_t count_regions_with_attr_eq(const std::string& attr_name, const std::string& value) const;

    // When a reverse index exists: O(log K) check that region_idx is in the posting for value.
    // If value is not in the .lex or region_idx is not listed, returns false.
    bool region_matches_attr_eq_rev(const std::string& attr_name, size_t region_idx,
                                   const std::string& value) const;

    // Sum of inclusive token spans for regions with attr == value. 0 if value not in .lex.
    // SIZE_MAX if there is no reverse index for this attr (caller uses a conservative bound).
    size_t token_span_sum_for_attr_eq(const std::string& attr_name, const std::string& value) const;

    // ── Fast aggregation support ─────────────────────────────────────────

    // Look up the lex ID for an attribute value in the reverse index lexicon.
    // Returns UNKNOWN_LEX if no reverse index or value not found.
    LexiconId region_attr_lex_lookup(const std::string& attr_name,
                                     const std::string& value) const;

    // Number of distinct values in the reverse index lexicon for this attr.
    LexiconId region_attr_lex_size(const std::string& attr_name) const;

    // Get the string for a given lex ID from the reverse index lexicon.
    std::string_view region_attr_lex_get(const std::string& attr_name, LexiconId id) const;

    // Build a dense mapping: region_idx → lex_id for a named attribute.
    // Walks the .rev postings once. Regions not covered get UNKNOWN_LEX (-1).
    // O(num_regions) — call once at query plan time.
    std::vector<LexiconId> precompute_region_to_lex(const std::string& attr_name) const;

    // Get raw pointer + count of sorted region indices matching a value.
    // Returns false if no reverse index. The pointer is into mmap'd memory.
    bool regions_for_value(const std::string& attr_name, const std::string& value,
                           const int64_t*& out_regions, size_t& out_count) const;

private:
    struct RegionValueRev {
        Lexicon lex;       // distinct values (.lex)
        MmapFile rev;      // int64_t region indices
        MmapFile rev_idx;  // cumulative offsets per lex id
    };
    MmapFile file_;       // .rgn: Region pairs
    MmapFile par_;        // .par: optional int32 parent region index per row (-1 = root)
    MmapFile val_;        // .val: default (unnamed) region value strings
    MmapFile val_idx_;    // .val.idx: int64 byte offsets into .val
    std::unordered_map<std::string, std::pair<MmapFile, MmapFile>> region_attrs_;
    std::unordered_map<std::string, RegionValueRev> region_value_rev_;
    std::vector<std::string> region_attr_names_;
};

} // namespace manatree
