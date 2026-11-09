#pragma once

// EF-12: a subcorpus resolved to one sorted set of corpus positions.
//
// Two encodings, chosen by size when the set is built (Builder::finish):
//   Ranges  sorted, disjoint, inclusive position runs (16 bytes a run): few, long
//           pieces — region-based subcorpora (documents, genres, years);
//   Bitmap  roaring-style chunks of 2^16 positions, each an array of 16-bit
//           offsets (at most 4096 positions) or a 1024-word bitmap: fragmented
//           sets — token-based selections (all nouns).
// Whatever the encoding: tokens() (positions in the set), runs() (its pieces: the
// maximal runs of consecutive positions), contains(p), the runs in order, and
// union / intersection / difference in one pass over the operands' runs.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace pando {

class PositionSet {
public:
    enum class Encoding { Ranges, Bitmap };
    using Run = std::pair<int64_t, int64_t>;   // inclusive
    using Runs = std::vector<Run>;

    static constexpr int kChunkShift = 16;
    static constexpr int64_t kChunk = int64_t{1} << kChunkShift;
    static constexpr size_t kWords = static_cast<size_t>(kChunk) / 64;   // 1024
    static constexpr uint32_t kArrayMax = 4096;

    PositionSet();   // empty (ranges)

    /// Positions in the set, its pieces (maximal runs) and how it is stored.
    uint64_t tokens() const { return tokens_; }
    size_t runs() const { return runs_; }
    bool empty() const { return tokens_ == 0; }
    Encoding encoding() const { return enc_; }
    /// "ranges" / "roaring" (the JSON value of "encoding")
    const char* encoding_name() const { return enc_ == Encoding::Ranges ? "ranges" : "roaring"; }
    /// Memory of the stored set (estimate).
    size_t bytes() const;

    bool contains(int64_t p) const;
    /// Positions of the set in [a, b] (inclusive).
    uint64_t count_in(int64_t a, int64_t b) const;
    bool overlaps(int64_t a, int64_t b) const;

    /// f(start, end) for each maximal run, ascending.
    template <class F> void for_each_run(F&& f) const;
    Runs to_runs() const;
    /// The runs when the set is stored as ranges (nullptr for a bitmap): shared, not copied.
    std::shared_ptr<const Runs> ranges() const { return enc_ == Encoding::Ranges ? ranges_ : nullptr; }

    /// The same positions in the other encoding (tests; PANDO_SUBCORPUS_ENCODING).
    PositionSet reencoded(Encoding e) const;

    static PositionSet unite(const PositionSet& a, const PositionSet& b);
    static PositionSet intersect(const PositionSet& a, const PositionSet& b);
    static PositionSet subtract(const PositionSet& a, const PositionSet& b);

    /// Builds a set from runs in any order (overlapping / adjacent runs merge).
    /// Runs arriving in ascending order are merged as they come; a large number of
    /// pending runs is folded into chunk bitmaps, so memory stays bounded by the
    /// bitmap of the touched chunks, not 16 bytes per run.
    class Builder {
    public:
        Builder();
        ~Builder();
        Builder(Builder&&) noexcept;
        Builder& operator=(Builder&&) noexcept;
        void add(int64_t a, int64_t b);
        void add(int64_t p) { add(p, p); }
        /// The set, in the smaller encoding (or `force`: "ranges" / "roaring").
        PositionSet finish(const std::string& force = {});
    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

private:
    struct Chunk {
        uint32_t key = 0;                 // position >> 16
        uint32_t card = 0;
        std::vector<uint16_t> arr;        // card <= kArrayMax
        std::vector<uint64_t> bits;       // otherwise
    };
    static PositionSet from_sorted_runs(Runs runs);
    static PositionSet from_chunks(std::vector<Chunk> chunks);
    static PositionSet choose(Runs runs, const std::string& force);
    static size_t bitmap_bytes_of(const Runs& runs);
    static std::vector<Chunk> chunks_of(const Runs& runs);
    template <class F> static void chunk_runs(const Chunk& c, F&& f);
    const Chunk* chunk_at(int64_t p) const;
    template <class Op> static PositionSet combine(const PositionSet& a, const PositionSet& b, Op op);

    Encoding enc_ = Encoding::Ranges;
    std::shared_ptr<const Runs> ranges_;
    std::vector<Chunk> chunks_;
    std::vector<int32_t> chunk_index_;   // key -> index in chunks_ + 1 (0: no chunk)
    uint64_t tokens_ = 0;
    size_t runs_ = 0;
};

// ── template members ────────────────────────────────────────────────────

template <class F>
void PositionSet::chunk_runs(const Chunk& c, F&& f) {
    const int64_t base = static_cast<int64_t>(c.key) << kChunkShift;
    if (!c.bits.empty()) {
        constexpr size_t N = kWords * 64;
        // first set / clear bit at or after `from` (N: none)
        auto next = [&](size_t from, bool set) -> size_t {
            size_t w = from / 64;
            if (w >= kWords) return N;
            uint64_t x = (set ? c.bits[w] : ~c.bits[w]) & (~uint64_t{0} << (from % 64));
            while (!x) {
                if (++w >= kWords) return N;
                x = set ? c.bits[w] : ~c.bits[w];
            }
            return w * 64 + static_cast<size_t>(__builtin_ctzll(x));
        };
        for (size_t s = next(0, true); s < N;) {
            const size_t e = next(s, false);
            f(base + static_cast<int64_t>(s), base + static_cast<int64_t>(e) - 1);
            s = next(e, true);
        }
        return;
    }
    size_t i = 0;
    const size_t n = c.arr.size();
    while (i < n) {
        size_t j = i;
        while (j + 1 < n && c.arr[j + 1] == c.arr[j] + 1) ++j;
        f(base + c.arr[i], base + c.arr[j]);
        i = j + 1;
    }
}

template <class F>
void PositionSet::for_each_run(F&& f) const {
    if (enc_ == Encoding::Ranges) {
        if (ranges_)
            for (const auto& [a, b] : *ranges_) f(a, b);
        return;
    }
    // runs that cross a chunk boundary are joined
    bool have = false;
    int64_t cs = 0, ce = 0;
    for (const Chunk& c : chunks_) {
        chunk_runs(c, [&](int64_t s, int64_t e) {
            if (have && s == ce + 1) { ce = e; return; }
            if (have) f(cs, ce);
            have = true;
            cs = s;
            ce = e;
        });
    }
    if (have) f(cs, ce);
}

} // namespace pando
