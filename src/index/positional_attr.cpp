#include "index/positional_attr.h"
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <list>
#include <mutex>
#include <thread>
#include <stdexcept>
#include <unordered_map>
#include <sys/mman.h>

namespace fs = std::filesystem;

namespace pando {

// ── PositionalAttr (read-only) ──────────────────────────────────────────

namespace {

// P4.2: PANDO_REV=packed serves postings from `.rev.pfb` even when `.rev` exists
// (tests, benchmarks); raw = the plain `.rev` when present; auto (default) = the
// plain one when present, else the packed one.
enum class RevMode { Auto, Raw, Packed };
RevMode rev_mode() {
    static const RevMode m = [] {
        const char* v = std::getenv("PANDO_REV");
        if (!v) return RevMode::Auto;
        const std::string s(v);
        if (s == "packed") return RevMode::Packed;
        if (s == "raw") return RevMode::Raw;
        return RevMode::Auto;
    }();
    return m;
}

// Decoded long lists, shared by every attribute of the process, least recently
// used out by bytes (PANDO_REV_CACHE_MB, default 256). Spans keep their list
// alive after eviction.
class DecodeCache {
public:
    static DecodeCache& get() {
        static DecodeCache c;
        return c;
    }
    std::shared_ptr<const void> find(uint64_t serial, LexiconId id) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = index_.find(key(serial, id));
        if (it == index_.end()) return nullptr;
        lru_.splice(lru_.begin(), lru_, it->second);
        return it->second->buf;
    }
    void put(uint64_t serial, LexiconId id, std::shared_ptr<const void> buf, size_t bytes) {
        if (bytes > max_ / 4) return;
        std::lock_guard<std::mutex> lk(mu_);
        const uint64_t k = key(serial, id);
        if (index_.count(k)) return;
        lru_.push_front(Item{k, std::move(buf), bytes});
        index_[k] = lru_.begin();
        bytes_ += bytes;
        while (bytes_ > max_ && !lru_.empty()) {
            bytes_ -= lru_.back().bytes;
            index_.erase(lru_.back().k);
            lru_.pop_back();
        }
    }

private:
    struct Item { uint64_t k; std::shared_ptr<const void> buf; size_t bytes; };
    static uint64_t key(uint64_t serial, LexiconId id) { return serial << 40 | static_cast<uint64_t>(id); }
    DecodeCache() {
        const char* v = std::getenv("PANDO_REV_CACHE_MB");
        max_ = (v ? static_cast<size_t>(std::atoll(v)) : size_t{256}) << 20;
    }
    std::mutex mu_;
    std::list<Item> lru_;
    std::unordered_map<uint64_t, std::list<Item>::iterator> index_;
    size_t bytes_ = 0, max_ = 0;
};

std::atomic<uint64_t> g_attr_serial{0};
constexpr size_t kCacheMinPostings = 4096;   // shorter lists are decoded per call

}  // namespace

// ── LazyPostings (P4.2b) ────────────────────────────────────────────────

LazyPostings::LazyPostings(std::shared_ptr<const PackedPostings> pk, int64_t id, size_t count, int width)
    : pk_(std::move(pk)), id_(id), count_(count), width_(width), nblocks_(PackedPostings::nblocks(count)),
      buf_(new char[count * static_cast<size_t>(width)]),   // not touched until decoded
      state_(new std::atomic<uint8_t>[nblocks_]) {
    for (size_t b = 0; b < nblocks_; ++b) state_[b].store(kNone, std::memory_order_relaxed);
}

void LazyPostings::ensure(size_t b) const {
    uint8_t st = kNone;
    if (state_[b].compare_exchange_strong(st, kBusy, std::memory_order_acquire)) {
        const size_t at = b * PackedPostings::kBlock;   // straight into the list's width
        switch (width_) {
            case 2: pk_->decode_block(id_, count_, b, reinterpret_cast<int16_t*>(buf_.get()) + at); break;
            case 4: pk_->decode_block(id_, count_, b, reinterpret_cast<int32_t*>(buf_.get()) + at); break;
            default: pk_->decode_block(id_, count_, b, reinterpret_cast<int64_t*>(buf_.get()) + at); break;
        }
        state_[b].store(kReady, std::memory_order_release);
        return;
    }
    while (state_[b].load(std::memory_order_acquire) != kReady) std::this_thread::yield();
}

const void* LazyPostings::whole() const {
    if (!all_.load(std::memory_order_acquire)) {
#ifdef MADV_POPULATE_WRITE
        // fault the buffer's pages in one call instead of one fault per page
        // (Linux 5.14+; ~40% of the cost of first touching a fresh 34 MB buffer)
        if (bytes() >= (size_t{1} << 20)) {
            const uintptr_t a = (reinterpret_cast<uintptr_t>(buf_.get()) + 4095) & ~uintptr_t{4095};
            const uintptr_t e = (reinterpret_cast<uintptr_t>(buf_.get()) + bytes()) & ~uintptr_t{4095};
            if (e > a) ::madvise(reinterpret_cast<void*>(a), e - a, MADV_POPULATE_WRITE);
        }
#endif
        for (size_t b = 0; b < nblocks_; ++b)
            if (state_[b].load(std::memory_order_acquire) != kReady) ensure(b);
        all_.store(true, std::memory_order_release);
    }
    return buf_.get();
}

size_t LazyPostings::lower_bound(size_t lo, size_t end, CorpusPos target) const {
    if (lo >= end) return end;
    if (at(lo) >= target) return lo;
    // the last block whose first position is < target, from lo's block (gallop on
    // the skip table: merges probe close ahead, page jumps go far)
    const size_t last_b = (end - 1) / PackedPostings::kBlock;
    size_t b = lo / PackedPostings::kBlock;   // block_first(b) <= at(lo) < target
    if (b < last_b) {
        size_t prev = b, step = 1, hi = b + 1;
        while (hi <= last_b && block_first(hi) < target) {
            prev = hi;
            step <<= 1;
            hi = prev + step;
        }
        if (hi > last_b + 1) hi = last_b + 1;   // answer block in [prev, hi)
        size_t L = prev + 1, R = hi;              // first block with first >= target
        while (L < R) {
            const size_t mid = L + ((R - L) >> 1);
            if (block_first(mid) < target) L = mid + 1;
            else R = mid;
        }
        b = L - 1;
    }
    size_t j = std::max(lo, b * PackedPostings::kBlock);
    const size_t stop = std::min(end, (b + 1) * PackedPostings::kBlock);
    // binary search inside the block (at() decodes it once)
    size_t L = j, R = stop;
    while (L < R) {
        const size_t mid = L + ((R - L) >> 1);
        if (at(mid) < target) L = mid + 1;
        else R = mid;
    }
    return L;   // == stop: the next block starts at or above target (or end)
}

size_t RevSpan::lower_bound(size_t lo, CorpusPos target) const {
    if (lazy) {
        const size_t r = lazy->lower_bound(base + lo, base + count, target);
        return r - base;
    }
    const size_t n = count;
    if (lo >= n || at(lo) >= target) return lo;
    size_t prev = lo, step = 1, hi = lo + 1;
    while (hi < n && at(hi) < target) {
        prev = hi;
        step <<= 1;
        hi = prev + step;
    }
    if (hi > n) hi = n;
    size_t L = prev + 1, R = hi;
    while (L < R) {
        const size_t mid = L + ((R - L) >> 1);
        if (at(mid) < target) L = mid + 1;
        else R = mid;
    }
    return L;
}

RevSpan RevSpan::slice(size_t lo, size_t hi) const {
    RevSpan out;
    out.width = width;
    if (hi > count) hi = count;
    if (hi <= lo) return out;
    out.count = hi - lo;
    out.keep = keep;
    if (lazy) {
        out.lazy = lazy;
        out.base = base + lo;
    } else {
        out.data = static_cast<const char*>(data) + lo * static_cast<size_t>(width);
    }
    return out;
}

void PositionalAttr::open(const std::string& base, CorpusPos corpus_size, bool preload) {
    corpus_size_ = corpus_size;
    base_path_ = base;
    serial_ = ++g_attr_serial;
    lexicon_.open(base, preload);
    derived_ = false;
    // P5.6: a head attribute (head#A) may have no .dat: set_head_source() follows
    if (fs::exists(base + ".dat")) corpus_ = MmapFile::open(base + ".dat", preload);
    rev_idx_ = MmapFile::open(base + ".rev.idx", preload);
    const bool have_rev = fs::exists(base + ".rev");
    const RevMode mode = rev_mode();
    const int64_t nlex = static_cast<int64_t>(rev_idx_.count<int64_t>()) - 1;
    const int64_t total = nlex >= 0 ? rev_idx_.as<int64_t>()[nlex] : 0;
    // P4.2: packed postings, when wanted or when the plain `.rev` was dropped
    if (nlex >= 0 && (mode == RevMode::Packed || !have_rev))
        use_packed_ = packed_->open(base, nlex, total, preload);
    if (use_packed_) {
        rev_width_ = packed_->rev_width();
    } else {
        if (!have_rev)
            throw std::runtime_error("no postings for " + base + " (neither .rev nor a valid .rev.pfb)");
        rev_ = MmapFile::open(base + ".rev", preload);
    }

    // Infer .dat element width from file size
    if (corpus_size_ > 0 && corpus_.size() > 0) {
        dat_width_ = static_cast<int>(corpus_.size() / static_cast<size_t>(corpus_size_));
        if (dat_width_ != 1 && dat_width_ != 2 && dat_width_ != 4)
            throw std::runtime_error("Invalid .dat element width (" +
                                     std::to_string(dat_width_) + ") for " + base);
    }

    // Infer .rev element width from file size
    if (rev_.size() > 0 && corpus_size_ > 0) {
        rev_width_ = static_cast<int>(rev_.size() / static_cast<size_t>(corpus_size_));
        if (rev_width_ != 2 && rev_width_ != 4 && rev_width_ != 8)
            throw std::runtime_error("Invalid .rev element width (" +
                                     std::to_string(rev_width_) + ") for " + base);
    }
}

void PositionalAttr::set_head_source(const PositionalAttr* src, const HeadRelView& hrel, LexiconId none_id) {
    derived_ = true;
    src_ = src;
    hrel_ = hrel;
    none_id_ = none_id;
    shift_ = lexicon_.size() == src->lexicon().size() + 1;
    dat_width_ = 4;
}

LexiconId PositionalAttr::head_id_at(CorpusPos pos) const {
    const int16_t d = hrel_[pos];
    if (d == 0) return none_id_;
    const CorpusPos h = pos + d;
    if (h < 0 || h >= corpus_size_) return none_id_;
    const LexiconId id = src_->id_at(h);
    return shift_ && id >= none_id_ ? id + 1 : id;
}

LexiconId PositionalAttr::id_at(CorpusPos pos) const {
    if (derived_) return head_id_at(pos);
    switch (dat_width_) {
        case 1: return static_cast<LexiconId>(corpus_.as<uint8_t>()[pos]);
        case 2: return static_cast<LexiconId>(corpus_.as<uint16_t>()[pos]);
        default: return corpus_.as<int32_t>()[pos];
    }
}

std::string_view PositionalAttr::value_at(CorpusPos pos) const {
    return lexicon_.get(id_at(pos));
}

size_t PositionalAttr::count_of(const std::string& value) const {
    LexiconId id = lexicon_.lookup(value);
    if (id == UNKNOWN_LEX) return 0;
    return count_of_id(id);
}

size_t PositionalAttr::count_of_id(LexiconId id) const {
    const auto* idx = rev_idx_.as<int64_t>();
    return static_cast<size_t>(idx[id + 1] - idx[id]);
}

std::vector<CorpusPos> PositionalAttr::positions_of(const std::string& value) const {
    LexiconId id = lexicon_.lookup(value);
    if (id == UNKNOWN_LEX) return {};
    return positions_of_id(id);
}

RevSpan PositionalAttr::rev_span_of_id(LexiconId id) const {
    RevSpan span;
    span.width = rev_width_;
    if (!rev_idx_.valid() || id == UNKNOWN_LEX) return span;
    size_t nlex = rev_idx_.count<int64_t>();
    if (nlex < 2 || static_cast<size_t>(id) + 1 >= nlex) return span;
    const auto* idx = rev_idx_.as<int64_t>();
    int64_t start = idx[id];
    int64_t end   = idx[id + 1];
    if (end <= start) return span;
    span.count = static_cast<size_t>(end - start);
    if (use_packed_) {
        // P4.2b: decoded block by block on access; long lists are kept (with the
        // blocks decoded so far) for the next queries
        const bool cache = span.count >= kCacheMinPostings;
        std::shared_ptr<const void> lp;
        if (cache) lp = DecodeCache::get().find(serial_, id);
        if (!lp) {
            lp = std::make_shared<const LazyPostings>(packed_, id, span.count, rev_width_);
            if (cache) DecodeCache::get().put(serial_, id, lp, span.count * static_cast<size_t>(rev_width_));
        }
        span.lazy = static_cast<const LazyPostings*>(lp.get());
        span.keep = std::move(lp);
        return span;
    }
    switch (rev_width_) {
        case 2: span.data = rev_.as<int16_t>() + start; break;
        case 4: span.data = rev_.as<int32_t>() + start; break;
        default: span.data = rev_.as<int64_t>() + start; break;
    }
    return span;
}

std::vector<CorpusPos> PositionalAttr::positions_of_id(LexiconId id) const {
    RevSpan span = rev_span_of_id(id);
    std::vector<CorpusPos> result(span.count);
    for (size_t i = 0; i < span.count; ++i)
        result[i] = span.at(i);
    return result;
}

std::vector<CorpusPos> PositionalAttr::positions_matching(const Regex& re, bool full_match) const {
    std::vector<CorpusPos> result;
    LexiconId n = lexicon_.size();
    for (LexiconId id = 0; id < n; ++id) {
        if (re.match(lexicon_.get(id), full_match)) {
            auto span = positions_of_id(id);
            result.insert(result.end(), span.begin(), span.end());
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

std::vector<CorpusPos> PositionalAttr::positions_not(
        const std::string& value, CorpusPos csz) const {
    LexiconId exclude = lexicon_.lookup(value);
    if (exclude == UNKNOWN_LEX) {
        std::vector<CorpusPos> all(static_cast<size_t>(csz));
        for (CorpusPos i = 0; i < csz; ++i) all[static_cast<size_t>(i)] = i;
        return all;
    }
    auto excluded = positions_of_id(exclude);
    std::vector<CorpusPos> result;
    result.reserve(static_cast<size_t>(csz) - excluded.size());
    size_t ei = 0;
    for (CorpusPos p = 0; p < csz; ++p) {
        if (ei < excluded.size() && excluded[ei] == p) { ++ei; continue; }
        result.push_back(p);
    }
    return result;
}

// ── RG-5f: Multivalue component reverse index ───────────────────────────

void PositionalAttr::open_mv(const std::string& base, bool preload) {
    std::string mv_rev_path = base + ".mv.rev";
    std::string mv_idx_path = base + ".mv.rev.idx";

    // MV files are optional — silently skip if absent
    mv_rev_idx_ = MmapFile::open(mv_idx_path, preload);
    if (!mv_rev_idx_.valid()) return;

    mv_lexicon_.open(base + ".mv", preload);  // opens .mv.lex and .mv.lex.idx
    mv_rev_ = MmapFile::open(mv_rev_path, preload);

    // Infer .mv.rev element width
    if (mv_rev_.size() > 0 && corpus_size_ > 0) {
        // Total entries = rev_idx[mv_lex_size] (last element)
        size_t n_idx = mv_rev_idx_.count<int64_t>();
        if (n_idx >= 2) {
            int64_t total = mv_rev_idx_.as<int64_t>()[n_idx - 1];
            if (total > 0) {
                mv_rev_width_ = static_cast<int>(mv_rev_.size() /
                                                  static_cast<size_t>(total));
                if (mv_rev_width_ != 2 && mv_rev_width_ != 4 && mv_rev_width_ != 8)
                    throw std::runtime_error("Invalid .mv.rev element width (" +
                                             std::to_string(mv_rev_width_) + ") for " + base);
            }
        }
    }
}

LexiconId PositionalAttr::mv_lookup(std::string_view component) const {
    return mv_lexicon_.lookup(component);
}

size_t PositionalAttr::mv_count_of(const std::string& component) const {
    LexiconId id = mv_lexicon_.lookup(component);
    if (id == UNKNOWN_LEX) return 0;
    return mv_count_of_id(id);
}

size_t PositionalAttr::mv_count_of_id(LexiconId id) const {
    const auto* idx = mv_rev_idx_.as<int64_t>();
    return static_cast<size_t>(idx[id + 1] - idx[id]);
}

// ── Stage 1: forward MV index (.mv.fwd / .mv.fwd.idx) ───────────────────
// Spec: dev/PANDO-MVAL-FORMAT.md (v0.2)

void PositionalAttr::open_mv_fwd(const std::string& base, bool preload) {
    std::string fwd_idx_path = base + ".mv.fwd.idx";
    std::string fwd_path     = base + ".mv.fwd";

    // Optional sidecar — silent no-op for older corpora (no file, or empty path).
    // MmapFile::open throws on ENOENT; treat missing index as "no forward MV".
    if (!fs::exists(fwd_idx_path)) return;

    mv_fwd_idx_ = MmapFile::open(fwd_idx_path, preload);
    if (!mv_fwd_idx_.valid()) return;

    // Sanity: idx must be int64[corpus_size + 1].
    size_t expected_idx_bytes =
        (static_cast<size_t>(corpus_size_) + 1) * sizeof(int64_t);
    if (mv_fwd_idx_.size() != expected_idx_bytes) {
        // Reset and bail rather than crash — the corpus.info may be out of
        // sync with files on disk; loaders should treat this as "no fwd".
        mv_fwd_idx_ = MmapFile{};
        throw std::runtime_error(
            "Invalid .mv.fwd.idx size for " + base +
            " (expected " + std::to_string(expected_idx_bytes) +
            " bytes, got " + std::to_string(mv_fwd_idx_.size()) + ")");
    }

    mv_fwd_ = MmapFile::open(fwd_path, preload);

    // Total elements = idx[corpus_size]. Detect width from .mv.fwd file size.
    int64_t total = mv_fwd_idx_.as<int64_t>()[corpus_size_];
    if (total > 0) {
        if (!mv_fwd_.valid())
            throw std::runtime_error(
                ".mv.fwd missing but .mv.fwd.idx says non-empty for " + base);
        size_t per = mv_fwd_.size() / static_cast<size_t>(total);
        if (per != 2 && per != 4)
            throw std::runtime_error(
                "Invalid .mv.fwd element width (" + std::to_string(per) +
                ") for " + base);
        mv_fwd_width_ = static_cast<int>(per);
    } else {
        // Zero-entry case: .mv.fwd may be empty/absent — width is irrelevant.
        mv_fwd_width_ = 4;
    }
}

size_t PositionalAttr::mv_fwd_count_at(CorpusPos pos) const {
    if (!has_mv_fwd()) return 0;
    const auto* idx = mv_fwd_idx_.as<int64_t>();
    return static_cast<size_t>(idx[pos + 1] - idx[pos]);
}

std::vector<LexiconId> PositionalAttr::mv_fwd_at(CorpusPos pos) const {
    std::vector<LexiconId> out;
    out.reserve(mv_fwd_count_at(pos));
    for_each_mv_fwd_at(pos, [&](LexiconId id) {
        out.push_back(id);
        return true;
    });
    return out;
}

} // namespace pando
