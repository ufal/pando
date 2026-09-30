#pragma once

// P3.2 / P3.3: per-chunk bitmap expressions for the bit-parallel sequence kernels.
//
// A token's condition is compiled into a tree whose leaves are bitmap values
// (`BitmapIndex`), id sets (OR of values) or plain `.rev` postings (attributes
// without bitmaps, materialised operands); inner nodes are AND / OR / NOT.
// load(c) returns the token's positions in chunk c as kWords uint64 words plus
// the first kBmExtra words of chunk c + 1 (`nx`), so the kernels can shift by
// up to kBmMaxShift positions across the chunk border. Chunks are visited in
// ascending order; every node keeps monotone cursors.

#include "index/bitmap_index.h"
#include "index/positional_attr.h"
#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace pando {

constexpr size_t kBmExtra = 8;                                 // words of chunk c + 1
constexpr int kBmMaxShift = static_cast<int>(64 * (kBmExtra - 1));   // largest token offset

struct BmChunk {
    const uint64_t* w = nullptr;    // kWords words (never null)
    const uint64_t* nx = nullptr;   // kBmExtra words of the next chunk (never null)
    bool zero = false;              // hint: all of w and nx are 0
    uint64_t word(size_t i) const { return i < BitmapIndex::kWords ? w[i] : nx[i - BitmapIndex::kWords]; }
    /// Bit j = position 64*i + k + j of the chunk (0 <= k <= kBmMaxShift, i < kWords).
    uint64_t shifted(size_t i, int k) const {
        const size_t q = i + static_cast<size_t>(k >> 6);
        const int r = k & 63;
        if (r == 0) return word(q);
        return (word(q) >> r) | (word(q + 1) << (64 - r));
    }
};

inline const uint64_t* bm_zero_words() {
    static const std::vector<uint64_t> z(BitmapIndex::kWords + kBmExtra, 0);
    return z.data();
}
inline const uint64_t* bm_ones_words() {
    static const std::vector<uint64_t> o(BitmapIndex::kWords + kBmExtra, ~uint64_t{0});
    return o.data();
}

/// OR the first kBmExtra words of a container into out.
inline void bm_head_words(const BitmapIndex& bi, const BitmapIndex::Entry& e, uint64_t* out) {
    if (bi.is_full(e)) {
        const uint32_t len = bi.chunk_len(e.chunk);
        for (size_t w = 0; w < kBmExtra; ++w) {
            const uint32_t lo = static_cast<uint32_t>(w * 64);
            if (len >= lo + 64) out[w] = ~uint64_t{0};
            else if (len > lo) out[w] |= (uint64_t{1} << (len - lo)) - 1;
        }
    } else if (bi.is_bitmap(e)) {
        const uint64_t* b = bi.bitmap_words(e);
        for (size_t w = 0; w < kBmExtra; ++w) out[w] |= b[w];
    } else {
        const uint16_t* a = bi.array_values(e);
        for (uint32_t k = 0; k < e.card && a[k] < 64 * kBmExtra; ++k)
            out[a[k] >> 6] |= uint64_t{1} << (a[k] & 63);
    }
}

class BmExpr {
public:
    virtual ~BmExpr() = default;
    virtual BmChunk load(size_t c) = 0;
    /// Number of set positions (upper bound for AND, sum for OR); planning only.
    size_t estimate = 0;
protected:
    std::vector<uint64_t> buf_ = std::vector<uint64_t>(BitmapIndex::kWords);
    uint64_t nbuf_[kBmExtra] = {};
    bool nzero() const {
        uint64_t a = 0;
        for (size_t w = 0; w < kBmExtra; ++w) a |= nbuf_[w];
        return a == 0;
    }
};

/// One value of a bitmap-indexed attribute.
class BmValue final : public BmExpr {
public:
    BmValue(const BitmapIndex& bi, int64_t v, size_t count) : bi_(bi), cur_(bi, v) { estimate = count; }
    BmChunk load(size_t c) override {
        const BitmapIndex::Entry* e = cur_.at(c);
        const BitmapIndex::Entry* nx = cur_.peek_next(c);
        BmChunk r;
        bool nz = true;
        if (!nx) {
            r.nx = bm_zero_words();
        } else if (bi_.is_bitmap(*nx)) {
            r.nx = bi_.bitmap_words(*nx);
            nz = false;
        } else {
            std::fill(nbuf_, nbuf_ + kBmExtra, 0);
            bm_head_words(bi_, *nx, nbuf_);
            r.nx = nbuf_;
            nz = nzero();
        }
        if (!e) {
            r.w = bm_zero_words();
            r.zero = nz;
        } else if (bi_.is_bitmap(*e)) {
            r.w = bi_.bitmap_words(*e);
        } else if (bi_.is_full(*e) && bi_.chunk_len(c) == static_cast<uint32_t>(BitmapIndex::kChunk)) {
            r.w = bm_ones_words();
        } else {
            std::fill(buf_.begin(), buf_.end(), 0);
            bi_.or_into(*e, buf_.data());
            r.w = buf_.data();
        }
        return r;
    }
private:
    const BitmapIndex& bi_;
    BitmapValueCursor cur_;
};

/// OR of several values of one bitmap-indexed attribute (regex, %c, |).
class BmValueSet final : public BmExpr {
public:
    BmValueSet(const BitmapIndex& bi, const std::vector<int64_t>& ids, size_t count) : bi_(bi) {
        for (int64_t v : ids) cur_.emplace_back(bi, v);
        estimate = count;
    }
    BmChunk load(size_t c) override {
        BmChunk r;
        bool any = false;
        std::fill(nbuf_, nbuf_ + kBmExtra, 0);
        for (auto& cu : cur_) {
            const BitmapIndex::Entry* e = cu.at(c);
            if (const BitmapIndex::Entry* nx = cu.peek_next(c)) bm_head_words(bi_, *nx, nbuf_);
            if (!e) continue;
            if (!any) { std::fill(buf_.begin(), buf_.end(), 0); any = true; }
            bi_.or_into(*e, buf_.data());
        }
        r.w = any ? buf_.data() : bm_zero_words();
        r.nx = nbuf_;
        r.zero = !any && nzero();
        return r;
    }
private:
    const BitmapIndex& bi_;
    std::vector<BitmapValueCursor> cur_;
};

/// Sorted unique positions (a `.rev` span or a materialised operand).
class BmPostings final : public BmExpr {
public:
    explicit BmPostings(RevSpan s, std::shared_ptr<void> keep = nullptr)
        : s_(s), keep_(std::move(keep)) { estimate = s.count; }
    BmChunk load(size_t c) override {
        const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
        const CorpusPos end = base + BitmapIndex::kChunk;
        // skip positions before this chunk (gallop: rare lists jump far)
        if (i_ < s_.count && s_.at(i_) < base) {
            size_t step = 1, lo = i_, hi = i_ + 1;
            while (hi < s_.count && s_.at(hi) < base) { lo = hi; step <<= 1; hi = lo + step; }
            if (hi > s_.count) hi = s_.count;
            size_t L = lo + 1, R = hi;
            while (L < R) {
                const size_t mid = L + ((R - L) >> 1);
                if (s_.at(mid) < base) L = mid + 1; else R = mid;
            }
            i_ = L;
        }
        BmChunk r;
        size_t j = i_;
        bool any = false;
        while (j < s_.count) {
            const CorpusPos p = s_.at(j);
            if (p >= end) break;
            if (!any) { std::fill(buf_.begin(), buf_.end(), 0); any = true; }
            const uint32_t o = static_cast<uint32_t>(p - base);
            buf_[o >> 6] |= uint64_t{1} << (o & 63);
            ++j;
        }
        i_ = j;
        std::fill(nbuf_, nbuf_ + kBmExtra, 0);
        for (size_t k = j; k < s_.count; ++k) {
            const CorpusPos d = s_.at(k) - end;
            if (d >= static_cast<CorpusPos>(64 * kBmExtra)) break;
            nbuf_[d >> 6] |= uint64_t{1} << (d & 63);
        }
        r.w = any ? buf_.data() : bm_zero_words();
        r.nx = nbuf_;
        r.zero = !any && nzero();
        return r;
    }
private:
    RevSpan s_;
    std::shared_ptr<void> keep_;
    size_t i_ = 0;
};

/// Positions covered by sorted, disjoint inclusive intervals (token-level region
/// attributes: `[text_langcode="en"]`), set range by range per chunk.
class BmIntervals final : public BmExpr {
public:
    explicit BmIntervals(std::vector<std::pair<CorpusPos, CorpusPos>> iv) : iv_(std::move(iv)) {
        for (const auto& x : iv_) estimate += static_cast<size_t>(x.second - x.first + 1);
    }
    BmChunk load(size_t c) override {
        const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
        const CorpusPos end = base + BitmapIndex::kChunk;
        const CorpusPos xend = end + static_cast<CorpusPos>(64 * kBmExtra);
        while (i_ < iv_.size() && iv_[i_].second < base) ++i_;
        BmChunk r;
        bool any = false;
        std::fill(nbuf_, nbuf_ + kBmExtra, 0);
        for (size_t j = i_; j < iv_.size() && iv_[j].first < xend; ++j) {
            const CorpusPos a = std::max(iv_[j].first, base), b = std::min(iv_[j].second, xend - 1);
            if (a < end) {
                if (!any) { std::fill(buf_.begin(), buf_.end(), 0); any = true; }
                set(buf_.data(), static_cast<size_t>(a - base), static_cast<size_t>(std::min(b, end - 1) - base));
            }
            if (b >= end)
                set(nbuf_, static_cast<size_t>(std::max(a, end) - end), static_cast<size_t>(b - end));
        }
        r.w = any ? buf_.data() : bm_zero_words();
        r.nx = nbuf_;
        r.zero = !any && nzero();
        return r;
    }
private:
    static void set(uint64_t* m, size_t a, size_t b) {   // inclusive
        const size_t wa = a >> 6, wb = b >> 6;
        const uint64_t ma = ~uint64_t{0} << (a & 63);
        const uint64_t mb = ~uint64_t{0} >> (63 - (b & 63));
        if (wa == wb) { m[wa] |= ma & mb; return; }
        m[wa] |= ma;
        for (size_t w = wa + 1; w < wb; ++w) m[w] = ~uint64_t{0};
        m[wb] |= mb;
    }
    std::vector<std::pair<CorpusPos, CorpusPos>> iv_;
    size_t i_ = 0;
};

/// A condition on a positional attribute without a bitmap (`[lemma!="shoe"]`,
/// `[lemma=/.*e.*/]` with a large id set): a per-id truth table over the lexicon,
/// applied to the `.dat` ids of each chunk. One table lookup per position — the
/// dense case, where materialising or probing positions costs far more.
class BmTable final : public BmExpr {
public:
    BmTable(const PositionalAttr& pa, std::shared_ptr<const std::vector<uint8_t>> tab,
            size_t count, CorpusPos corpus_size)
        : dat_(pa.dat_data()), width_(pa.dat_width()), tab_(std::move(tab)), n_(corpus_size) {
        estimate = count;
    }
    BmChunk load(size_t c) override {
        const CorpusPos base = static_cast<CorpusPos>(c) << BitmapIndex::kChunkShift;
        const CorpusPos end = std::min<CorpusPos>(base + BitmapIndex::kChunk, n_);
        const CorpusPos xend = std::min<CorpusPos>(base + BitmapIndex::kChunk + 64 * kBmExtra, n_);
        uint64_t any = fill(base, end, buf_.data(), BitmapIndex::kWords);
        std::fill(nbuf_, nbuf_ + kBmExtra, 0);
        if (xend > base + BitmapIndex::kChunk)
            fill(base + BitmapIndex::kChunk, xend, nbuf_, kBmExtra);
        BmChunk r;
        r.w = buf_.data();
        r.nx = nbuf_;
        r.zero = any == 0 && nzero();
        return r;
    }
private:
    /// Bits for positions [a, b) into out (nw words, cleared first); returns the OR.
    uint64_t fill(CorpusPos a, CorpusPos b, uint64_t* out, size_t nw) const {
        std::fill(out, out + nw, 0);
        if (b <= a) return 0;
        const uint8_t* t = tab_->data();
        const size_t tn = tab_->size();
        auto run = [&](auto ptr) {
            uint64_t orr = 0;
            const size_t len = static_cast<size_t>(b - a);
            for (size_t w = 0; w * 64 < len; ++w) {
                const size_t k1 = std::min<size_t>(64, len - w * 64);
                uint64_t x = 0;
                const auto* d = ptr + a + static_cast<CorpusPos>(w * 64);
                for (size_t k = 0; k < k1; ++k) {
                    const size_t id = static_cast<size_t>(d[k]);
                    x |= static_cast<uint64_t>(id < tn ? t[id] : 0) << k;
                }
                out[w] = x;
                orr |= x;
            }
            return orr;
        };
        switch (width_) {
            case 1: return run(static_cast<const uint8_t*>(dat_));
            case 2: return run(static_cast<const uint16_t*>(dat_));
            default: return run(static_cast<const int32_t*>(dat_));
        }
    }
    const void* dat_;
    int width_;
    std::shared_ptr<const std::vector<uint8_t>> tab_;
    CorpusPos n_;
};

class BmAnd final : public BmExpr {
public:
    BmAnd(std::unique_ptr<BmExpr> a, std::unique_ptr<BmExpr> b) : a_(std::move(a)), b_(std::move(b)) {
        // evaluate the sparser side first (it decides most early outs)
        if (b_->estimate < a_->estimate) std::swap(a_, b_);
        estimate = a_->estimate;
    }
    BmChunk load(size_t c) override {
        const BmChunk x = a_->load(c);
        if (x.zero) { b_->load(c); return x; }
        const BmChunk y = b_->load(c);
        if (y.zero) return y;
        for (size_t w = 0; w < kBmExtra; ++w) nbuf_[w] = x.nx[w] & y.nx[w];
        uint64_t any = 0;
        for (size_t w = 0; w < BitmapIndex::kWords; ++w) any |= (buf_[w] = x.w[w] & y.w[w]);
        BmChunk r;
        r.w = buf_.data();
        r.nx = nbuf_;
        r.zero = any == 0 && nzero();
        return r;
    }
private:
    std::unique_ptr<BmExpr> a_, b_;
};

class BmOr final : public BmExpr {
public:
    BmOr(std::unique_ptr<BmExpr> a, std::unique_ptr<BmExpr> b) : a_(std::move(a)), b_(std::move(b)) {
        estimate = a_->estimate + b_->estimate;
    }
    BmChunk load(size_t c) override {
        const BmChunk x = a_->load(c);
        const BmChunk y = b_->load(c);
        if (y.zero) return x;   // children's buffers stay valid until their next load
        if (x.zero) return y;
        for (size_t w = 0; w < kBmExtra; ++w) nbuf_[w] = x.nx[w] | y.nx[w];
        for (size_t w = 0; w < BitmapIndex::kWords; ++w) buf_[w] = x.w[w] | y.w[w];
        BmChunk r;
        r.w = buf_.data();
        r.nx = nbuf_;
        return r;
    }
private:
    std::unique_ptr<BmExpr> a_, b_;
};

/// Complement within the corpus (`!=`). Bits past the corpus end are set; the
/// kernels mask starts / ends to the corpus anyway.
class BmNot final : public BmExpr {
public:
    BmNot(std::unique_ptr<BmExpr> a, size_t corpus_size) : a_(std::move(a)) {
        estimate = corpus_size > a_->estimate ? corpus_size - a_->estimate : 0;
    }
    BmChunk load(size_t c) override {
        const BmChunk x = a_->load(c);
        BmChunk r;
        if (x.zero) {
            r.w = bm_ones_words();
            r.nx = bm_ones_words();
            return r;
        }
        for (size_t w = 0; w < kBmExtra; ++w) nbuf_[w] = ~x.nx[w];
        for (size_t w = 0; w < BitmapIndex::kWords; ++w) buf_[w] = ~x.w[w];
        r.w = buf_.data();
        r.nx = nbuf_;
        return r;
    }
private:
    std::unique_ptr<BmExpr> a_;
};

} // namespace pando
