#pragma once

// P4.2: block-compressed postings for a positional attribute.
//
// Each lexicon id's sorted positions are cut into blocks of kBlock (128). Within
// a block the positions after the first are stored as gaps (p[i] - p[i-1] - 1),
// bit-packed at the block's own width w (the bits of its largest gap, 0..56; 64 =
// plain 8-byte positions for huge gaps). A block decodes in one branch-free loop;
// a long list has a skip table (each block's first position, offset and width),
// so a position is found by a binary search over it and one block decode instead
// of decoding from the start (Manatee's delta streams are byte-variable and
// sequential). Most ids of a large lexicon occur a few times: their list is a
// varint first position and (for 2..128 postings) a width byte and packed gaps,
// with no skip table.
//
// Files (next to the attribute's `.rev` / `.rev.idx`; the counts per id stay in
// `.rev.idx`, which every index keeps):
//   <attr>.rev.pfb.idx  64-byte header, uint64 base[ceil((nlex+1)/64)],
//                       uint32 rel[nlex+1]: id's data at base[id/64] + rel[id]
//   <attr>.rev.pfb      per id: count <= kBlock: varint first [, width, gaps];
//                       count > kBlock: Skip[nblocks] then each block's gaps
//                       (+ 16 zero bytes, so word loads never run past the end)
// Built by `pando-index --upgrade --packed-rev`; with `--drop-rev` the plain `.rev`
// can then be removed. Older than `.rev.idx` = stale (not opened).

#include "core/types.h"
#include "core/mmap_file.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

namespace pando {

class PositionalAttr;

class PackedPostings {
public:
    static constexpr size_t kBlock = 128;
    static constexpr uint32_t kRawWidth = 64;
    static constexpr size_t kGroup = 64;          // ids per 64-bit offset base
    static constexpr size_t kSkipBytes = 13;      // int64 first, uint32 offset, uint8 width

    struct BuildStats {
        uint64_t postings = 0, blocks = 0, payload_bytes = 0, idx_bytes = 0;
    };

    static std::string path(const std::string& attr_base) { return attr_base + ".rev.pfb"; }
    static std::string idx_path(const std::string& attr_base) { return attr_base + ".rev.pfb.idx"; }

    /// Write `<attr>.rev.pfb{,.idx}` from the attribute's plain postings.
    static bool build(const PositionalAttr& pa, std::string* err, BuildStats* stats = nullptr);

    /// Open existing files: false when absent, stale (older than `<base>.rev.idx`)
    /// or inconsistent with `nlex` ids / `total` postings.
    bool open(const std::string& attr_base, int64_t nlex, int64_t total, bool preload = false);

    bool valid() const { return idx_.valid(); }
    /// Width of the plain `.rev` the files were built from (2, 4 or 8).
    int rev_width() const { return rev_width_; }
    size_t bytes() const { return idx_.size() + payload_file_.size(); }

    /// Every position of `id` (count from `.rev.idx`) into out[0..count), as T.
    template <typename T>
    void decode_all(int64_t id, size_t count, T* out) const {
        for_each_block(id, count, [&](const CorpusPos* b, size_t n, size_t at) {
            for (size_t i = 0; i < n; ++i) out[at + i] = static_cast<T>(b[i]);
            return true;
        });
    }

    /// f(pos) for every position of `id` in order; false from f stops (returns false).
    template <typename F>
    bool for_each(int64_t id, size_t count, F&& f) const {
        return for_each_block(id, count, [&](const CorpusPos* b, size_t n, size_t) {
            for (size_t i = 0; i < n; ++i)
                if (!f(b[i])) return false;
            return true;
        });
    }

    /// g(block, n, index of block[0] in the list) for each decoded block, in order.
    template <typename G>
    bool for_each_block(int64_t id, size_t count, G&& g) const {
        if (count == 0) return true;
        const uint8_t* p = data_of(id);
        CorpusPos buf[kBlock];
        if (count <= kBlock) {
            const CorpusPos first = static_cast<CorpusPos>(read_varint(p));
            uint32_t w = 0;
            if (count > 1) w = *p++;
            unpack(first, w, p, count, buf);
            return g(buf, count, size_t{0});
        }
        const size_t nb = (count + kBlock - 1) / kBlock;
        const uint8_t* skip = p;
        for (size_t b = 0; b < nb; ++b) {
            int64_t first;
            uint32_t off;
            std::memcpy(&first, skip + b * kSkipBytes, 8);
            std::memcpy(&off, skip + b * kSkipBytes + 8, 4);
            const uint32_t w = skip[b * kSkipBytes + 12];
            const size_t n = std::min(kBlock, count - b * kBlock);
            unpack(first, w, p + off, n, buf);
            if (!g(buf, n, b * kBlock)) return false;
        }
        return true;
    }

    /// Blocks of a list of `count` postings.
    static size_t nblocks(size_t count) { return (count + kBlock - 1) / kBlock; }

    /// First position of block b of `id`'s list (no decoding).
    CorpusPos block_first(int64_t id, size_t count, size_t b) const {
        const uint8_t* p = data_of(id);
        if (count <= kBlock) return static_cast<CorpusPos>(read_varint(p));
        int64_t first;
        std::memcpy(&first, p + b * kSkipBytes, 8);
        return first;
    }

    /// Decode block b of `id`'s list into out[0..n); returns n (<= kBlock).
    size_t decode_block(int64_t id, size_t count, size_t b, CorpusPos* out) const {
        const uint8_t* p = data_of(id);
        if (count <= kBlock) {
            const CorpusPos first = static_cast<CorpusPos>(read_varint(p));
            uint32_t w = 0;
            if (count > 1) w = *p++;
            unpack(first, w, p, count, out);
            return count;
        }
        const uint8_t* e = p + b * kSkipBytes;
        int64_t first;
        uint32_t off;
        std::memcpy(&first, e, 8);
        std::memcpy(&off, e + 8, 4);
        const size_t n = std::min(kBlock, count - b * kBlock);
        unpack(first, e[12], p + off, n, out);
        return n;
    }

private:
    const uint8_t* data_of(int64_t id) const {
        return payload_ + base_[static_cast<size_t>(id) / kGroup] + rel_[id];
    }
    static uint64_t read_varint(const uint8_t*& p) {
        uint64_t v = 0;
        unsigned s = 0;
        for (;;) {
            const uint8_t b = *p++;
            v |= static_cast<uint64_t>(b & 0x7F) << s;
            if (!(b & 0x80)) return v;
            s += 7;
        }
    }
    /// n positions from `first` and the gaps at `g` (width w) into out.
    static void unpack(CorpusPos first, uint32_t w, const uint8_t* g, size_t n, CorpusPos* out) {
        CorpusPos p = first;
        out[0] = p;
        if (w == 0) {
            for (size_t i = 1; i < n; ++i) out[i] = ++p;
        } else if (w == kRawWidth) {
            for (size_t i = 1; i < n; ++i) {
                int64_t v;
                std::memcpy(&v, g + (i - 1) * 8, 8);
                out[i] = v;
            }
        } else {
            const uint64_t mask = (uint64_t{1} << w) - 1;
            size_t bit = 0;
            for (size_t i = 1; i < n; ++i, bit += w) {
                uint64_t x;
                std::memcpy(&x, g + (bit >> 3), 8);   // little endian
                p += static_cast<CorpusPos>((x >> (bit & 7)) & mask) + 1;
                out[i] = p;
            }
        }
    }

    MmapFile idx_;
    MmapFile payload_file_;
    const uint64_t* base_ = nullptr;
    const uint32_t* rel_ = nullptr;
    const uint8_t* payload_ = nullptr;
    int rev_width_ = 8;
};

}  // namespace pando
