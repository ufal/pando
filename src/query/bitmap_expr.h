#pragma once

// P3.2: per-chunk bitmap expressions for the bit-parallel sequence kernel.
//
// A token's condition is compiled into a tree whose leaves are bitmap values
// (`BitmapIndex`), id sets (OR of values) or plain `.rev` postings (attributes
// without bitmaps, materialised operands); inner nodes are AND / OR / NOT.
// load(c) returns the token's positions in chunk c as kWords uint64 words plus
// the first word of chunk c + 1 (`next`), so the kernel can shift by up to 63
// positions across the chunk border. Chunks are visited in ascending order;
// every node keeps monotone cursors.

#include "index/bitmap_index.h"
#include "index/positional_attr.h"
#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace pando {

struct BmChunk {
    const uint64_t* w = nullptr;   // kWords words (never null)
    uint64_t next = 0;             // bits of positions [0, 64) of chunk c + 1
    bool zero = false;             // hint: all of w and next are 0
};

inline const uint64_t* bm_zero_words() {
    static const std::vector<uint64_t> z(BitmapIndex::kWords, 0);
    return z.data();
}
inline const uint64_t* bm_ones_words() {
    static const std::vector<uint64_t> o(BitmapIndex::kWords, ~uint64_t{0});
    return o.data();
}

class BmExpr {
public:
    virtual ~BmExpr() = default;
    virtual BmChunk load(size_t c) = 0;
    /// Number of set positions (upper bound for AND, sum for OR); planning only.
    size_t estimate = 0;
protected:
    std::vector<uint64_t> buf_ = std::vector<uint64_t>(BitmapIndex::kWords);
};

/// One value of a bitmap-indexed attribute.
class BmValue final : public BmExpr {
public:
    BmValue(const BitmapIndex& bi, int64_t v, size_t count) : bi_(bi), cur_(bi, v) { estimate = count; }
    BmChunk load(size_t c) override {
        const BitmapIndex::Entry* e = cur_.at(c);
        const BitmapIndex::Entry* nx = cur_.peek_next(c);
        BmChunk r;
        r.next = nx ? bi_.first_word(*nx) : 0;
        if (!e) {
            r.w = bm_zero_words();
            r.zero = r.next == 0;
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
        for (auto& cu : cur_) {
            const BitmapIndex::Entry* e = cu.at(c);
            if (const BitmapIndex::Entry* nx = cu.peek_next(c)) r.next |= bi_.first_word(*nx);
            if (!e) continue;
            if (!any) { std::fill(buf_.begin(), buf_.end(), 0); any = true; }
            bi_.or_into(*e, buf_.data());
        }
        r.w = any ? buf_.data() : bm_zero_words();
        r.zero = !any && r.next == 0;
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
        for (size_t k = j; k < s_.count; ++k) {
            const CorpusPos p = s_.at(k);
            if (p >= end + 64) break;
            r.next |= uint64_t{1} << (p - end);
        }
        r.w = any ? buf_.data() : bm_zero_words();
        r.zero = !any && r.next == 0;
        return r;
    }
private:
    RevSpan s_;
    std::shared_ptr<void> keep_;
    size_t i_ = 0;
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
        BmChunk r;
        r.next = x.next & y.next;
        if (y.zero) { r.w = bm_zero_words(); r.zero = r.next == 0; return r; }
        uint64_t any = 0;
        for (size_t w = 0; w < BitmapIndex::kWords; ++w) any |= (buf_[w] = x.w[w] & y.w[w]);
        r.w = buf_.data();
        r.zero = any == 0 && r.next == 0;
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
        BmChunk r;
        for (size_t w = 0; w < BitmapIndex::kWords; ++w) buf_[w] = x.w[w] | y.w[w];
        r.w = buf_.data();
        r.next = x.next | y.next;
        return r;
    }
private:
    std::unique_ptr<BmExpr> a_, b_;
};

/// Complement within the corpus (`!=`). Bits past the corpus end are set; the
/// kernel masks starts to the corpus anyway.
class BmNot final : public BmExpr {
public:
    BmNot(std::unique_ptr<BmExpr> a, size_t corpus_size) : a_(std::move(a)) {
        estimate = corpus_size > a_->estimate ? corpus_size - a_->estimate : 0;
    }
    BmChunk load(size_t c) override {
        const BmChunk x = a_->load(c);
        BmChunk r;
        r.next = ~x.next;
        if (x.zero) { r.w = bm_ones_words(); return r; }
        for (size_t w = 0; w < BitmapIndex::kWords; ++w) buf_[w] = ~x.w[w];
        r.w = buf_.data();
        return r;
    }
private:
    std::unique_ptr<BmExpr> a_;
};

} // namespace pando
