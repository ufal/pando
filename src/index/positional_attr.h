#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/lexicon.h"
#include "index/packed_postings.h"
#include <atomic>
#include <memory>
#include <string>

#include "core/regex_engine.h"

namespace pando {

/// P4.2b: one id's packed postings (`.rev.pfb`), decoded block by block (128
/// positions) on first access into a buffer of the attribute's width — a page,
/// a gallop or a count over intervals decode only the blocks they touch. Safe for
/// concurrent readers (a block is decoded once; others wait for it).
class LazyPostings {
public:
    LazyPostings(std::shared_ptr<const PackedPostings> pk, int64_t id, size_t count, int width);
    LazyPostings(const LazyPostings&) = delete;
    LazyPostings& operator=(const LazyPostings&) = delete;

    size_t count() const { return count_; }
    int width() const { return width_; }
    CorpusPos at(size_t i) const {
        const size_t b = i / PackedPostings::kBlock;
        if (state_[b].load(std::memory_order_acquire) != kReady) ensure(b);
        switch (width_) {
            case 2: return static_cast<CorpusPos>(reinterpret_cast<const int16_t*>(buf_.get())[i]);
            case 4: return static_cast<CorpusPos>(reinterpret_cast<const int32_t*>(buf_.get())[i]);
            default: return reinterpret_cast<const int64_t*>(buf_.get())[i];
        }
    }
    /// The whole list as a typed array (every block decoded).
    const void* whole() const;
    /// First j in [lo, end) with at(j) >= target (end if none): the skip table
    /// finds the block, so only that block is decoded.
    size_t lower_bound(size_t lo, size_t end, CorpusPos target) const;
    size_t bytes() const { return count_ * static_cast<size_t>(width_); }

private:
    static constexpr uint8_t kNone = 0, kBusy = 1, kReady = 2;
    void ensure(size_t b) const;
    CorpusPos block_first(size_t b) const { return pk_->block_first(id_, count_, b); }

    std::shared_ptr<const PackedPostings> pk_;
    int64_t id_;
    size_t count_;
    int width_;
    size_t nblocks_;
    std::unique_ptr<char[]> buf_;
    std::unique_ptr<std::atomic<uint8_t>[]> state_;
    mutable std::atomic<bool> all_{false};
};

/// Zero-copy view of one lexicon id's sorted `.rev` postings (Manatee-style merge
/// operand): a typed array (`data`, the mmapped `.rev` or a materialised list),
/// or packed postings decoded on demand (`lazy`, P4.2b: positions base..base+count).
struct RevSpan {
    int width = 8;            // 2, 4, or 8
    const void* data = nullptr;
    size_t count = 0;
    /// What `data` / `lazy` point into when it is not the mmapped `.rev`; copies and
    /// slices keep it alive.
    std::shared_ptr<const void> keep;
    const LazyPostings* lazy = nullptr;
    size_t base = 0;          // with `lazy`: index of element 0 in the list

    CorpusPos at(size_t i) const {
        if (lazy) return lazy->at(base + i);
        switch (width) {
            case 2: return static_cast<CorpusPos>(static_cast<const int16_t*>(data)[i]);
            case 4: return static_cast<CorpusPos>(static_cast<const int32_t*>(data)[i]);
            default: return static_cast<const int64_t*>(data)[i];
        }
    }
    /// All `count` elements as a typed array of `width` (decodes a lazy list).
    const void* whole() const {
        if (!lazy) return data;
        return static_cast<const char*>(lazy->whole()) + base * static_cast<size_t>(width);
    }
    /// First j >= lo with at(j) >= target (count if none): galloping on an array,
    /// the skip table on packed postings.
    size_t lower_bound(size_t lo, CorpusPos target) const;
    /// Sub-span [lo, hi) (zero-copy).
    RevSpan slice(size_t lo, size_t hi) const;
    bool empty() const { return count == 0 || (data == nullptr && lazy == nullptr); }
};

// Read-only positional attribute: provides O(log V) lookup from value
// to a sorted position list, and O(1) lookup from position to value.
class PositionalAttr {
public:
    void open(const std::string& base_path, CorpusPos corpus_size, bool preload = false);

    /// Path prefix the attribute was opened from (`<dir>/<name>`), for sidecar files.
    const std::string& base_path() const { return base_path_; }

    /// Sorted postings for `id` without allocating a vector (empty if unknown / OOB).
    /// With packed postings (P4.2) the list is decoded (long lists cached).
    RevSpan rev_span_of_id(LexiconId id) const;
    /// Width of the spans' elements (2, 4 or 8), without touching a list.
    int rev_width() const { return rev_width_; }
    /// P4.2: postings are served from `<attr>.rev.pfb` (PANDO_REV=packed, or no `.rev`).
    bool rev_packed() const { return use_packed_; }
    const PackedPostings& packed_postings() const { return *packed_; }

    // Position → value
    LexiconId id_at(CorpusPos pos) const;
    /// Raw `.dat` ids (dat_width() bytes each: 1 = uint8, 2 = uint16, 4 = int32).
    const void* dat_data() const { return corpus_.data(); }
    int dat_width() const { return dat_width_; }
    std::string_view value_at(CorpusPos pos) const;

    // Value → count (O(1) via rev.idx — no position data touched)
    size_t count_of(const std::string& value) const;
    size_t count_of_id(LexiconId id) const;
    /// Number of postings over all ids (`.rev.idx` end).
    size_t postings_total() const {
        const size_t n = rev_idx_.count<int64_t>();
        return n ? static_cast<size_t>(rev_idx_.as<int64_t>()[n - 1]) : 0;
    }

    // Value → sorted positions (width-aware, returns owned vector)
    std::vector<CorpusPos> positions_of(const std::string& value) const;
    std::vector<CorpusPos> positions_of_id(LexiconId id) const;

    // Lazy iteration: invoke f(pos) for each position in lex id's .rev list (no vector alloc).
    // Returns false to stop early. Use for large EQ conditions to avoid O(n) allocation.
    template<typename F>
    bool for_each_position_id(LexiconId id, F&& f) const {
        const auto* idx = rev_idx_.as<int64_t>();
        int64_t start = idx[id];
        int64_t end   = idx[id + 1];
        const size_t count = static_cast<size_t>(end - start);
        if (use_packed_) return packed_->for_each(id, count, f);
        switch (rev_width_) {
            case 2: {
                const auto* p = rev_.as<int16_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            case 4: {
                const auto* p = rev_.as<int32_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            default: {
                const auto* p = rev_.as<int64_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(p[i])) return false;
                break;
            }
        }
        return true;
    }

    // Regex match: returns owned vector (union of all matching lex entries).
    // full_match: whole lexicon string must match (RE2 FullMatch / std::regex_match); else substring.
    std::vector<CorpusPos> positions_matching(const Regex& re, bool full_match = false) const;

    // Negation: all positions where value != given value
    std::vector<CorpusPos> positions_not(const std::string& value,
                                         CorpusPos corpus_size) const;

    const Lexicon& lexicon() const { return lexicon_; }
    CorpusPos corpus_size() const { return corpus_size_; }

    // RG-5f: Multivalue component reverse index support.
    // Call open_mv() after open() for attrs declared as multivalue.
    void open_mv(const std::string& base_path, bool preload = false);
    bool has_mv() const { return mv_rev_idx_.valid(); }
    const Lexicon& mv_lexicon() const { return mv_lexicon_; }

    // Lookup a single component value (e.g. "artist") in the MV lexicon.
    // Returns UNKNOWN_LEX if not found.
    LexiconId mv_lookup(std::string_view component) const;

    // Count of positions containing this component.
    size_t mv_count_of(const std::string& component) const;
    size_t mv_count_of_id(LexiconId mv_id) const;

    // Lazy iteration over positions for a component MV lex ID.
    template<typename F>
    bool for_each_position_mv(LexiconId mv_id, F&& f) const {
        const auto* idx = mv_rev_idx_.as<int64_t>();
        int64_t start = idx[mv_id];
        int64_t end   = idx[mv_id + 1];
        const size_t count = static_cast<size_t>(end - start);
        switch (mv_rev_width_) {
            case 2: {
                const auto* p = mv_rev_.as<int16_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            case 4: {
                const auto* p = mv_rev_.as<int32_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            default: {
                const auto* p = mv_rev_.as<int64_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(p[i])) return false;
                break;
            }
        }
        return true;
    }

private:
    std::string base_path_;
    Lexicon  lexicon_;
    MmapFile corpus_;      // .dat  — int8/int16/int32 per position
    MmapFile rev_;         // .rev  — int16/int32/int64 sorted positions per lex id
    MmapFile rev_idx_;     // .rev.idx — int64[lex_size+1] element offsets

    CorpusPos corpus_size_ = 0;
    int dat_width_ = 4;    // bytes per element in .dat (1, 2, or 4)
    int rev_width_ = 8;    // bytes per element in .rev (2, 4, or 8)
    /// P4.2 `.rev.pfb` (when present); shared with the lazily decoded lists
    std::shared_ptr<PackedPostings> packed_ = std::make_shared<PackedPostings>();
    bool use_packed_ = false;
    uint64_t serial_ = 0;     // decode-cache key of this attribute

    // RG-5f: MV component reverse index (optional, only for multivalue attrs)
    Lexicon  mv_lexicon_;      // .mv.lex — sorted component strings
    MmapFile mv_rev_;          // .mv.rev — positions per component
    MmapFile mv_rev_idx_;      // .mv.rev.idx — int64 offsets
    int mv_rev_width_ = 8;

    // Stage 1 of PANDO-MULTIVALUE-FIELDS: forward MV index.
    // Spec: dev/PANDO-MVAL-FORMAT.md (v0.2). Optional sidecar; if absent
    // (older corpora) has_mv_fwd() returns false and consumers fall back.
    MmapFile mv_fwd_;          // .mv.fwd     — sorted MV component ids per position
    MmapFile mv_fwd_idx_;      // .mv.fwd.idx — int64[corpus_size+1] offsets
    int mv_fwd_width_ = 4;     // 2 or 4 bytes per element

public:
    // Open the forward MV index. No-op if base.mv.fwd.idx is missing (older corpora);
    // missing path must not throw — MmapFile::open fails on ENOENT otherwise.
    void open_mv_fwd(const std::string& base_path, bool preload = false);
    bool has_mv_fwd() const { return mv_fwd_idx_.valid(); }

    // Number of MV components at this position. Returns 0 when has_mv_fwd()
    // is false or the position carries the empty set (zero-length run).
    size_t mv_fwd_count_at(CorpusPos pos) const;

    // Lazy iteration over the sorted, deduplicated MV component ids at pos.
    // f(LexiconId mv_id) → bool; returning false stops early.
    template<typename F>
    bool for_each_mv_fwd_at(CorpusPos pos, F&& f) const {
        if (!has_mv_fwd()) return true;
        const auto* idx = mv_fwd_idx_.as<int64_t>();
        int64_t start = idx[pos];
        int64_t end   = idx[pos + 1];
        const size_t count = static_cast<size_t>(end - start);
        switch (mv_fwd_width_) {
            case 2: {
                const auto* p = mv_fwd_.as<int16_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<LexiconId>(p[i]))) return false;
                break;
            }
            default: {
                const auto* p = mv_fwd_.as<int32_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<LexiconId>(p[i]))) return false;
                break;
            }
        }
        return true;
    }

    // Convenience: copy MV component ids at pos into a small vector.
    std::vector<LexiconId> mv_fwd_at(CorpusPos pos) const;
};

} // namespace pando
