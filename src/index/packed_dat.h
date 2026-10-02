#pragma once

// P4.3: compact forward index (`<attr>.dat.pk`) with O(1) random access.
//
// `.dat` stores a lexicon id per token in 1, 2 or 4 bytes. Two codings:
//
//   fixed  lexicons below 2^16 values: every id in ceil(log2 V) bits (upos:
//          5 bits instead of 8, deprel: 9 instead of 16). id_at = one unaligned
//          8-byte load, shift, mask.
//   group  larger lexicons (form, lemma): ids replaced by their frequency rank
//          (the most frequent value is 0; `rank → id` table in the file), ranks
//          in blocks of 16 tokens: a 32-bit header with a 2-bit length code per
//          token (1–4 bytes), then the bytes. Block offsets: a u64 base per 1024
//          tokens + a u16 offset per block. id_at: the offset of token k in its
//          block is k + the sum of the first k codes (two popcounts), one 4-byte
//          load and mask, the rank table. On the 38M UD demo: form 4 → 2.3 bytes
//          per token, lemma 4 → 2.1.
//
// File: 64-byte header, then (fixed) the bit-packed ids, or (group) u64
// bases[nsuper], u16 rel[nblocks], int32 rank_to_id[V], payload; 8 zero bytes
// at the end so word loads never read past it. Written by
// `pando-index --upgrade --packed-dat`; `--drop-dat` then removes the `.dat`
// (verified). Older than `.dat` / `.lex` = stale (not opened).

#include "core/types.h"
#include "core/mmap_file.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

namespace pando {

class PackedDat {
public:
    enum class Mode : uint32_t { Fixed = 1, Group = 2 };
    static constexpr size_t kBlock = 16;
    static constexpr size_t kSuper = 1024;   // tokens per u64 base

    struct BuildStats {
        uint64_t bytes = 0;     // the whole file
        Mode mode = Mode::Fixed;
        unsigned bits = 0;      // fixed
    };

    static std::string path(const std::string& attr_base) { return attr_base + ".dat.pk"; }

    /// Write `<attr>.dat.pk` from `n` ids over a lexicon of `nlex` values;
    /// fill(a, b, out) puts the ids of positions [a, b) into out.
    using Fill = std::function<void(int64_t a, int64_t b, LexiconId* out)>;
    static bool build(const std::string& attr_base, int64_t n, int64_t nlex, const Fill& fill,
                      std::string* err, BuildStats* stats = nullptr);

    /// Open; false when absent, stale (older than `<base>.lex` or `<base>.dat` when
    /// that exists) or inconsistent with `n` tokens / `nlex` values.
    bool open(const std::string& attr_base, int64_t n, int64_t nlex, bool preload = false);
    bool valid() const { return file_.valid(); }
    size_t bytes() const { return file_.size(); }

    LexiconId at(CorpusPos p) const {
        if (mode_ == Mode::Fixed) {
            const uint64_t bit = static_cast<uint64_t>(p) * bits_;
            uint64_t x;
            std::memcpy(&x, payload_ + (bit >> 3), 8);
            return static_cast<LexiconId>((x >> (bit & 7)) & mask_);
        }
        const uint64_t b = static_cast<uint64_t>(p) / kBlock;
        const uint8_t* blk = payload_ + bases_[static_cast<uint64_t>(p) / kSuper] + rel_[b];
        uint32_t h;
        std::memcpy(&h, blk, 4);
        const unsigned k = static_cast<unsigned>(p % kBlock);
        const uint32_t m = k ? (h & ((uint32_t{1} << (2 * k)) - 1)) : 0;
        const unsigned off = k + static_cast<unsigned>(__builtin_popcount(m & 0x55555555u))
                           + 2 * static_cast<unsigned>(__builtin_popcount(m & 0xAAAAAAAAu));
        const unsigned len = ((h >> (2 * k)) & 3u) + 1;
        uint32_t v;
        std::memcpy(&v, blk + 4 + off, 4);
        if (len < 4) v &= (uint32_t{1} << (8 * len)) - 1;
        return rank_to_id_[v];
    }

    /// ids of [a, b) into out (sequential: a block at a time).
    void decode(CorpusPos a, CorpusPos b, LexiconId* out) const;
    /// Group coding: the frequency ranks of [a, b) (no rank → id lookups), and the
    /// table that maps them to ids (null in fixed coding: decode() gives ids).
    void decode_ranks(CorpusPos a, CorpusPos b, uint32_t* out) const;
    const int32_t* rank_to_id() const { return mode_ == Mode::Group ? rank_to_id_ : nullptr; }

private:
    MmapFile file_;
    Mode mode_ = Mode::Fixed;
    unsigned bits_ = 0;
    uint64_t mask_ = 0;
    const uint64_t* bases_ = nullptr;
    const uint16_t* rel_ = nullptr;
    const int32_t* rank_to_id_ = nullptr;
    const uint8_t* payload_ = nullptr;
};

}  // namespace pando
