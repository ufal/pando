#include "subcorpus/position_set.h"

#include <algorithm>
#include <cstdlib>
#include <map>
#include <stdexcept>

namespace pando {

PositionSet::PositionSet() : ranges_(std::make_shared<const Runs>()) {}

// ── building ────────────────────────────────────────────────────────────

/// Sort and merge runs (overlapping or adjacent ones become one).
static void normalise_runs(PositionSet::Runs& r) {
    if (r.empty()) return;
    std::sort(r.begin(), r.end());
    size_t k = 0;
    for (size_t i = 1; i < r.size(); ++i) {
        if (r[i].first <= r[k].second + 1) {
            r[k].second = std::max(r[k].second, r[i].second);
        } else {
            r[++k] = r[i];
        }
    }
    r.resize(k + 1);
}

PositionSet PositionSet::from_sorted_runs(Runs runs) {
    PositionSet s;
    s.enc_ = Encoding::Ranges;
    for (const auto& [a, b] : runs) s.tokens_ += static_cast<uint64_t>(b - a + 1);
    s.runs_ = runs.size();
    runs.shrink_to_fit();
    s.ranges_ = std::make_shared<const Runs>(std::move(runs));
    return s;
}

std::vector<PositionSet::Chunk> PositionSet::chunks_of(const Runs& runs) {
    // per chunk: the positions of the runs (array or bitmap)
    std::vector<Chunk> out;
    auto chunk_for = [&](uint32_t key) -> Chunk& {
        if (out.empty() || out.back().key != key) {
            out.emplace_back();
            out.back().key = key;
        }
        return out.back();
    };
    for (const auto& [a, b] : runs) {
        for (int64_t s = a; s <= b;) {
            const uint32_t key = static_cast<uint32_t>(s >> kChunkShift);
            const int64_t chunk_end = (static_cast<int64_t>(key) + 1) * kChunk - 1;
            const int64_t e = std::min(b, chunk_end);
            Chunk& c = chunk_for(key);
            const uint32_t n = static_cast<uint32_t>(e - s + 1);
            if (c.bits.empty() && c.card + n > kArrayMax) {
                c.bits.assign(kWords, 0);
                for (uint16_t v : c.arr) c.bits[v >> 6] |= uint64_t{1} << (v & 63);
                c.arr.clear();
                c.arr.shrink_to_fit();
            }
            const uint32_t lo = static_cast<uint32_t>(s & (kChunk - 1));
            if (c.bits.empty()) {
                for (uint32_t v = lo; v < lo + n; ++v) c.arr.push_back(static_cast<uint16_t>(v));
            } else {
                for (uint32_t v = lo; v < lo + n;) {
                    if ((v & 63) == 0 && v + 64 <= lo + n) {
                        c.bits[v >> 6] = ~uint64_t{0};
                        v += 64;
                    } else {
                        c.bits[v >> 6] |= uint64_t{1} << (v & 63);
                        ++v;
                    }
                }
            }
            c.card += n;
            s = e + 1;
        }
    }
    return out;
}

PositionSet PositionSet::from_chunks(std::vector<Chunk> chunks) {
    PositionSet s;
    s.enc_ = Encoding::Bitmap;
    s.ranges_.reset();
    uint32_t max_key = 0;
    for (auto& c : chunks) {
        s.tokens_ += c.card;
        max_key = std::max(max_key, c.key);
        c.arr.shrink_to_fit();
    }
    s.chunks_ = std::move(chunks);
    s.chunk_index_.assign(s.chunks_.empty() ? 0 : static_cast<size_t>(max_key) + 1, 0);
    for (size_t i = 0; i < s.chunks_.size(); ++i)
        s.chunk_index_[s.chunks_[i].key] = static_cast<int32_t>(i + 1);
    size_t n = 0;
    s.for_each_run([&](int64_t, int64_t) { ++n; });
    s.runs_ = n;
    return s;
}

size_t PositionSet::bitmap_bytes_of(const Runs& runs) {
    // per chunk: 2 bytes a position up to kArrayMax, else 8 KiB; + directory
    std::map<uint32_t, uint64_t> card;
    for (const auto& [a, b] : runs)
        for (int64_t s = a; s <= b;) {
            const uint32_t key = static_cast<uint32_t>(s >> kChunkShift);
            const int64_t e = std::min(b, (static_cast<int64_t>(key) + 1) * kChunk - 1);
            card[key] += static_cast<uint64_t>(e - s + 1);
            s = e + 1;
        }
    size_t bytes = 0;
    for (const auto& [k, n] : card) bytes += 48 + (n <= kArrayMax ? 2 * n : kWords * 8);
    return bytes;
}

static std::string forced_encoding(const std::string& force) {
    if (!force.empty()) return force;
    const char* e = std::getenv("PANDO_SUBCORPUS_ENCODING");   // tests: both encodings
    return e ? std::string(e) : std::string();
}

PositionSet PositionSet::choose(Runs runs, const std::string& force) {
    const std::string f = forced_encoding(force);
    if (f == "ranges") return from_sorted_runs(std::move(runs));
    if (f == "roaring" || f == "bitmap") return from_chunks(chunks_of(runs));
    // the smaller one: few long runs as ranges, many short ones as a bitmap
    const size_t range_bytes = runs.size() * sizeof(Run);
    if (range_bytes <= 4096 || range_bytes <= bitmap_bytes_of(runs)) return from_sorted_runs(std::move(runs));
    return from_chunks(chunks_of(runs));
}

struct PositionSet::Builder::Impl {
    Runs pending;
    bool sorted = true;
    // runs folded into chunk bitmaps once `pending` grew large (any order)
    std::map<uint32_t, std::vector<uint64_t>> folded;
    static constexpr size_t kFoldAt = size_t{1} << 22;   // 4M runs = 64 MB

    void fold() {
        for (const auto& [a, b] : pending)
            for (int64_t p = a; p <= b; ++p) {
                auto& bits = folded[static_cast<uint32_t>(p >> kChunkShift)];
                if (bits.empty()) bits.assign(kWords, 0);
                const uint64_t off = static_cast<uint64_t>(p & (kChunk - 1));
                bits[off >> 6] |= uint64_t{1} << (off & 63);
            }
        pending.clear();
        sorted = true;
    }
};

PositionSet::Builder::Builder() : impl_(std::make_unique<Impl>()) {}
PositionSet::Builder::~Builder() = default;
PositionSet::Builder::Builder(Builder&&) noexcept = default;
PositionSet::Builder& PositionSet::Builder::operator=(Builder&&) noexcept = default;

void PositionSet::Builder::add(int64_t a, int64_t b) {
    if (a < 0) a = 0;
    if (b < a) return;
    auto& P = impl_->pending;
    if (!P.empty() && impl_->sorted) {
        auto& last = P.back();
        if (a >= last.first && a <= last.second + 1) {   // ascending: merged as they come
            last.second = std::max(last.second, b);
            return;
        }
        if (a < last.first) impl_->sorted = false;
    }
    P.emplace_back(a, b);
    if (P.size() >= Impl::kFoldAt) {
        if (!impl_->sorted) {
            normalise_runs(P);
            impl_->sorted = true;
        }
        if (P.size() >= Impl::kFoldAt / 2) impl_->fold();
    }
}

PositionSet PositionSet::Builder::finish(const std::string& force) {
    Runs runs = std::move(impl_->pending);
    normalise_runs(runs);
    if (!impl_->folded.empty()) {
        // the folded chunks and the pending runs together
        for (const auto& [a, b] : runs)
            for (int64_t p = a; p <= b; ++p) {
                auto& bits = impl_->folded[static_cast<uint32_t>(p >> kChunkShift)];
                if (bits.empty()) bits.assign(kWords, 0);
                const uint64_t off = static_cast<uint64_t>(p & (kChunk - 1));
                bits[off >> 6] |= uint64_t{1} << (off & 63);
            }
        std::vector<Chunk> chunks;
        for (auto& [key, bits] : impl_->folded) {
            Chunk c;
            c.key = key;
            for (uint64_t w : bits) c.card += static_cast<uint32_t>(__builtin_popcountll(w));
            if (c.card == 0) continue;
            if (c.card <= kArrayMax) {
                for (size_t w = 0; w < kWords; ++w)
                    for (uint64_t x = bits[w]; x; x &= x - 1)
                        c.arr.push_back(static_cast<uint16_t>(w * 64 + static_cast<size_t>(__builtin_ctzll(x))));
            } else {
                c.bits = std::move(bits);
            }
            chunks.push_back(std::move(c));
        }
        impl_->folded.clear();
        PositionSet s = from_chunks(std::move(chunks));
        const std::string f = forced_encoding(force);
        if (f == "ranges" || (f.empty() && s.runs() * sizeof(Run) < s.bytes())) return s.reencoded(Encoding::Ranges);
        return s;
    }
    return choose(std::move(runs), force);
}

// ── queries ─────────────────────────────────────────────────────────────

size_t PositionSet::bytes() const {
    if (enc_ == Encoding::Ranges) return (ranges_ ? ranges_->size() : 0) * sizeof(Run);
    size_t b = chunk_index_.size() * sizeof(int32_t);
    for (const auto& c : chunks_) b += sizeof(Chunk) + c.arr.size() * 2 + c.bits.size() * 8;
    return b;
}

const PositionSet::Chunk* PositionSet::chunk_at(int64_t p) const {
    if (p < 0) return nullptr;
    const uint64_t key = static_cast<uint64_t>(p) >> kChunkShift;
    if (key >= chunk_index_.size() || chunk_index_[key] == 0) return nullptr;
    return &chunks_[static_cast<size_t>(chunk_index_[key] - 1)];
}

bool PositionSet::contains(int64_t p) const {
    if (enc_ == Encoding::Ranges) {
        const Runs& r = *ranges_;
        auto it = std::upper_bound(r.begin(), r.end(), p, [](int64_t v, const Run& x) { return v < x.first; });
        if (it == r.begin()) return false;
        --it;
        return p <= it->second;
    }
    const Chunk* c = chunk_at(p);
    if (!c) return false;
    const uint32_t off = static_cast<uint32_t>(p & (kChunk - 1));
    if (!c->bits.empty()) return (c->bits[off >> 6] >> (off & 63)) & 1;
    return std::binary_search(c->arr.begin(), c->arr.end(), static_cast<uint16_t>(off));
}

uint64_t PositionSet::count_in(int64_t a, int64_t b) const {
    if (b < a || tokens_ == 0) return 0;
    if (a < 0) a = 0;
    if (enc_ == Encoding::Ranges) {
        const Runs& r = *ranges_;
        auto it = std::lower_bound(r.begin(), r.end(), a, [](const Run& x, int64_t v) { return x.second < v; });
        uint64_t n = 0;
        for (; it != r.end() && it->first <= b; ++it)
            n += static_cast<uint64_t>(std::min(b, it->second) - std::max(a, it->first) + 1);
        return n;
    }
    uint64_t n = 0;
    for (int64_t s = a; s <= b;) {
        const int64_t key = s >> kChunkShift;
        const int64_t e = std::min(b, (key + 1) * kChunk - 1);
        if (const Chunk* c = chunk_at(s)) {
            const uint32_t lo = static_cast<uint32_t>(s & (kChunk - 1)), hi = static_cast<uint32_t>(e & (kChunk - 1));
            if (lo == 0 && hi == kChunk - 1) {
                n += c->card;
            } else if (!c->bits.empty()) {
                for (uint32_t v = lo; v <= hi;) {
                    if ((v & 63) == 0 && v + 63 <= hi) {
                        n += static_cast<uint64_t>(__builtin_popcountll(c->bits[v >> 6]));
                        v += 64;
                    } else {
                        n += (c->bits[v >> 6] >> (v & 63)) & 1;
                        ++v;
                    }
                }
            } else {
                auto x = std::lower_bound(c->arr.begin(), c->arr.end(), static_cast<uint16_t>(lo));
                auto y = std::upper_bound(x, c->arr.end(), static_cast<uint16_t>(hi));
                n += static_cast<uint64_t>(y - x);
            }
        }
        s = e + 1;
    }
    return n;
}

bool PositionSet::overlaps(int64_t a, int64_t b) const {
    if (b < a) return false;
    if (enc_ == Encoding::Ranges) {
        const Runs& r = *ranges_;
        auto it = std::lower_bound(r.begin(), r.end(), a, [](const Run& x, int64_t v) { return x.second < v; });
        return it != r.end() && it->first <= b;
    }
    return count_in(a, b) > 0;
}

PositionSet::Runs PositionSet::to_runs() const {
    if (enc_ == Encoding::Ranges) return *ranges_;
    Runs out;
    out.reserve(runs_);
    for_each_run([&](int64_t a, int64_t b) { out.emplace_back(a, b); });
    return out;
}

PositionSet PositionSet::reencoded(Encoding e) const {
    if (e == enc_) return *this;
    Runs r = to_runs();
    return e == Encoding::Ranges ? from_sorted_runs(std::move(r)) : from_chunks(chunks_of(r));
}

// ── set operations: one merge over the two run sequences ─────────────────

template <class Op>
PositionSet PositionSet::combine(const PositionSet& a, const PositionSet& b, Op op) {
    const Runs ra = a.to_runs(), rb = b.to_runs();
    // sweep over the boundaries: inside(a), inside(b) between events
    Builder out;
    size_t i = 0, j = 0;
    int64_t pos = 0;
    const int64_t kEnd = INT64_MAX;
    while (true) {
        // the state at `pos`, and the next position where it may change
        bool in_a = false, in_b = false;
        int64_t next = kEnd;
        while (i < ra.size() && ra[i].second < pos) ++i;
        while (j < rb.size() && rb[j].second < pos) ++j;
        if (i < ra.size()) {
            if (ra[i].first <= pos) { in_a = true; next = std::min(next, ra[i].second + 1); }
            else next = std::min(next, ra[i].first);
        }
        if (j < rb.size()) {
            if (rb[j].first <= pos) { in_b = true; next = std::min(next, rb[j].second + 1); }
            else next = std::min(next, rb[j].first);
        }
        if (next == kEnd) break;
        if (op(in_a, in_b)) out.add(pos, next - 1);
        pos = next;
    }
    return out.finish();
}

PositionSet PositionSet::unite(const PositionSet& a, const PositionSet& b) {
    return combine(a, b, [](bool x, bool y) { return x || y; });
}
PositionSet PositionSet::intersect(const PositionSet& a, const PositionSet& b) {
    return combine(a, b, [](bool x, bool y) { return x && y; });
}
PositionSet PositionSet::subtract(const PositionSet& a, const PositionSet& b) {
    return combine(a, b, [](bool x, bool y) { return x && !y; });
}

} // namespace pando
