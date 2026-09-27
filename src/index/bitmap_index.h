#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include <cstdint>
#include <cstring>
#include <string>

namespace pando {

class PositionalAttr;

// P3.1: chunked (Roaring-style) bitmaps for a low-cardinality attribute.
//
// The corpus is cut into chunks of 2^16 positions. For every lexicon value v
// and every chunk where v occurs there is one container:
//
//   card == chunk length   full    (no payload)
//   card >  4096           bitmap  (1024 uint64 words, bit j of word w = position 64w + j)
//   otherwise              array   (card sorted uint16 offsets, padded to 8 bytes)
//
// so a container never takes more than min(16 bits per hit, 1 bit per position)
// (+16 bytes of directory). All `upos` values together are at most ~17 bits per
// token, against 32 (or 64 above 2^31 tokens) per token for `upos.rev`, and the
// kernels read them sequentially, chunk by chunk.
//
// Files (next to the attribute's `.rev`):
//   <attr>.bm.idx  64-byte header, uint64 value_start[V+1], Entry[nentries]
//                  (entries of value v: [value_start[v], value_start[v+1]), by chunk)
//   <attr>.bm      container payload
class BitmapIndex {
public:
    static constexpr int kChunkShift = 16;
    static constexpr CorpusPos kChunk = CorpusPos{1} << kChunkShift;
    static constexpr size_t kWords = static_cast<size_t>(kChunk) / 64;   // 1024
    static constexpr uint32_t kArrayMax = 4096;
    /// `--bitmaps auto`: non-multivalue attributes with at most this many values.
    static constexpr int64_t kAutoMaxValues = 1024;

    struct Entry {
        uint32_t chunk;
        uint32_t card;       // number of set positions in the chunk (>= 1)
        uint64_t offset;     // byte offset of the container in `.bm`
    };
    static_assert(sizeof(Entry) == 16, "Entry layout");

    struct BuildStats {
        uint64_t entries = 0, arrays = 0, bitmaps = 0, fulls = 0, payload_bytes = 0;
    };

    static std::string path(const std::string& attr_base);        // <attr>.bm
    static std::string idx_path(const std::string& attr_base);    // <attr>.bm.idx

    /// Build `<attr>.bm{,.idx}` from the attribute's `.rev` postings.
    static bool build(const PositionalAttr& pa, CorpusPos corpus_size, std::string* err,
                      BuildStats* stats = nullptr);

    /// Open existing files; false when absent or inconsistent with the attribute.
    bool open(const PositionalAttr& pa, CorpusPos corpus_size);

    bool valid() const { return idx_.valid(); }
    CorpusPos corpus_size() const { return corpus_size_; }
    size_t nchunks() const { return nchunks_; }
    int64_t nvalues() const { return nvalues_; }

    /// Positions in chunk `c` (kChunk except for the last chunk).
    uint32_t chunk_len(size_t c) const {
        const CorpusPos b = static_cast<CorpusPos>(c) << kChunkShift;
        const CorpusPos r = corpus_size_ - b;
        return static_cast<uint32_t>(r < kChunk ? r : kChunk);
    }

    const Entry* begin(int64_t v) const { return entries_ + value_start_[v]; }
    const Entry* end(int64_t v) const { return entries_ + value_start_[v + 1]; }

    bool is_full(const Entry& e) const { return e.card == chunk_len(e.chunk); }
    bool is_bitmap(const Entry& e) const { return !is_full(e) && e.card > kArrayMax; }

    const uint64_t* bitmap_words(const Entry& e) const {
        return reinterpret_cast<const uint64_t*>(payload_ + e.offset);
    }
    const uint16_t* array_values(const Entry& e) const {
        return reinterpret_cast<const uint16_t*>(payload_ + e.offset);
    }

    /// OR the container into out[0..kWords) (for full: the chunk's valid bits).
    void or_into(const Entry& e, uint64_t* out) const;
    /// Bits of positions [0, 64) of the container's chunk.
    uint64_t first_word(const Entry& e) const;

private:
    MmapFile idx_;
    MmapFile payload_file_;
    const uint64_t* value_start_ = nullptr;
    const Entry* entries_ = nullptr;
    const uint8_t* payload_ = nullptr;
    CorpusPos corpus_size_ = 0;
    size_t nchunks_ = 0;
    int64_t nvalues_ = 0;
};

/// Monotone cursor over one value's containers (chunks visited in ascending order).
struct BitmapValueCursor {
    const BitmapIndex::Entry* cur = nullptr;
    const BitmapIndex::Entry* end = nullptr;
    BitmapValueCursor() = default;
    BitmapValueCursor(const BitmapIndex& bi, int64_t v) : cur(bi.begin(v)), end(bi.end(v)) {}
    /// Container of chunk `c`, or nullptr when the value does not occur there.
    const BitmapIndex::Entry* at(size_t c) {
        while (cur < end && cur->chunk < c) ++cur;
        return (cur < end && cur->chunk == c) ? cur : nullptr;
    }
    /// Container of chunk c + 1 without advancing past chunk c.
    const BitmapIndex::Entry* peek_next(size_t c) const {
        const BitmapIndex::Entry* p = cur;
        while (p < end && p->chunk <= c) ++p;
        return (p < end && p->chunk == c + 1) ? p : nullptr;
    }
};

} // namespace pando
