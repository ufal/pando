#include "index/bitmap_index.h"
#include "index/positional_attr.h"
#include "index/structural_attr.h"
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace pando {

namespace {

constexpr char kMagic[8] = {'P', 'A', 'N', 'D', 'O', 'B', 'M', '1'};

struct Header {
    char magic[8];
    uint64_t corpus_size;
    uint64_t nvalues;
    uint64_t nchunks;
    uint64_t nentries;
    uint64_t payload_bytes;
    uint64_t reserved[2];
};
static_assert(sizeof(Header) == 64, "Header layout");

}  // namespace

std::string BitmapIndex::path(const std::string& attr_base) { return attr_base + ".bm"; }
std::string BitmapIndex::idx_path(const std::string& attr_base) { return attr_base + ".bm.idx"; }

bool BitmapIndex::build_generic(const std::string& base, CorpusPos corpus_size, int64_t nvalues,
                                const Generator& gen, std::string* err, BuildStats* stats) {
    auto fail = [&](const std::string& m) { if (err) *err = m; return false; };
    const size_t nchunks = static_cast<size_t>((corpus_size + kChunk - 1) >> kChunkShift);
    const std::string pay_tmp = path(base) + ".tmp", idx_tmp = idx_path(base) + ".tmp";

    FILE* pf = std::fopen(pay_tmp.c_str(), "wb");
    if (!pf) return fail("cannot create " + pay_tmp);
    std::vector<char> iobuf(1 << 20);
    std::setvbuf(pf, iobuf.data(), _IOFBF, iobuf.size());

    std::vector<uint64_t> value_start(static_cast<size_t>(nvalues) + 1, 0);
    std::vector<Entry> entries;
    BuildStats st;
    uint64_t off = 0;
    bool io_ok = true, order_ok = true;

    auto chunk_len = [&](size_t c) -> uint32_t {
        const CorpusPos r = corpus_size - (static_cast<CorpusPos>(c) << kChunkShift);
        return static_cast<uint32_t>(r < kChunk ? r : kChunk);
    };
    auto put = [&](const void* p, size_t n) {
        if (n && std::fwrite(p, 1, n, pf) != n) io_ok = false;
        off += n;
    };

    // One chunk under construction: a sorted uint16 list while small, a word
    // bitmap once it has more than kArrayMax positions (or a long range).
    std::vector<uint64_t> words(kWords);
    std::vector<uint16_t> arr;
    arr.reserve(kArrayMax + 8);
    bool in_words = false;
    int64_t cur_chunk = -1;
    uint32_t card = 0;
    CorpusPos last = -1;
    auto to_words = [&] {
        std::fill(words.begin(), words.end(), 0);
        for (uint16_t o : arr) words[o >> 6] |= uint64_t{1} << (o & 63);
        in_words = true;
    };
    auto flush = [&] {
        if (cur_chunk < 0 || card == 0) return;
        const size_t c = static_cast<size_t>(cur_chunk);
        entries.push_back(Entry{static_cast<uint32_t>(c), card, off});
        if (card == chunk_len(c)) {
            ++st.fulls;
        } else if (card > kArrayMax) {
            if (!in_words) to_words();
            put(words.data(), kWords * 8);
            ++st.bitmaps;
        } else {
            if (in_words) {
                arr.clear();
                for (size_t w = 0; w < kWords; ++w)
                    for (uint64_t x = words[w]; x; x &= x - 1)
                        arr.push_back(static_cast<uint16_t>(w * 64 + static_cast<size_t>(__builtin_ctzll(x))));
            }
            const size_t pad = (4 - arr.size() % 4) % 4;
            arr.resize(arr.size() + pad, 0);
            put(arr.data(), arr.size() * 2);
            ++st.arrays;
        }
    };
    auto start_chunk = [&](int64_t c) {
        flush();
        cur_chunk = c;
        card = 0;
        arr.clear();
        in_words = false;
    };
    auto emit = [&](CorpusPos a, CorpusPos b) {   // inclusive, ascending, disjoint
        if (a > b) return;
        if (a <= last || a < 0 || b >= corpus_size) { order_ok = false; return; }
        last = b;
        while (a <= b) {
            const int64_t c = a >> kChunkShift;
            if (c != cur_chunk) start_chunk(c);
            const CorpusPos cbase = static_cast<CorpusPos>(c) << kChunkShift;
            const CorpusPos e = std::min<CorpusPos>(b, cbase + kChunk - 1);
            const uint32_t oa = static_cast<uint32_t>(a - cbase), ob = static_cast<uint32_t>(e - cbase);
            const uint32_t len = ob - oa + 1;
            if (!in_words && (card + len > kArrayMax || len > 64)) to_words();
            if (in_words) {
                for (uint32_t o = oa; o <= ob;) {
                    if ((o & 63) == 0 && o + 63 <= ob) { words[o >> 6] = ~uint64_t{0}; o += 64; continue; }
                    words[o >> 6] |= uint64_t{1} << (o & 63);
                    ++o;
                }
            } else {
                for (uint32_t o = oa; o <= ob; ++o) arr.push_back(static_cast<uint16_t>(o));
            }
            card += len;
            a = e + 1;
        }
    };

    for (int64_t v = 0; v < nvalues && io_ok && order_ok; ++v) {
        value_start[static_cast<size_t>(v)] = entries.size();
        cur_chunk = -1;
        card = 0;
        last = -1;
        arr.clear();
        in_words = false;
        gen(v, emit);
        flush();
    }
    value_start[static_cast<size_t>(nvalues)] = entries.size();
    if (std::fclose(pf) != 0) io_ok = false;
    if (!io_ok || !order_ok) {
        std::remove(pay_tmp.c_str());
        return fail(order_ok ? "cannot write " + pay_tmp
                             : base + ": positions not ascending / out of range");
    }

    Header h{};
    std::memcpy(h.magic, kMagic, 8);
    h.corpus_size = static_cast<uint64_t>(corpus_size);
    h.nvalues = static_cast<uint64_t>(nvalues);
    h.nchunks = nchunks;
    h.nentries = entries.size();
    h.payload_bytes = off;
    FILE* xf = std::fopen(idx_tmp.c_str(), "wb");
    if (!xf) return fail("cannot create " + idx_tmp);
    bool ok = std::fwrite(&h, sizeof h, 1, xf) == 1
        && std::fwrite(value_start.data(), 8, value_start.size(), xf) == value_start.size()
        && (entries.empty()
            || std::fwrite(entries.data(), sizeof(Entry), entries.size(), xf) == entries.size());
    if (std::fclose(xf) != 0) ok = false;
    if (!ok) {
        std::remove(idx_tmp.c_str());
        std::remove(pay_tmp.c_str());
        return fail("cannot write " + idx_tmp);
    }
    std::error_code ec;
    fs::rename(pay_tmp, path(base), ec);
    if (!ec) fs::rename(idx_tmp, idx_path(base), ec);
    if (ec) return fail("cannot rename " + path(base) + ": " + ec.message());
    st.entries = entries.size();
    st.payload_bytes = off;
    if (stats) *stats = st;
    return true;
}

bool BitmapIndex::build(const PositionalAttr& pa, CorpusPos corpus_size, std::string* err,
                        BuildStats* stats) {
    return build_generic(pa.base_path(), corpus_size, pa.lexicon().size(),
        [&](int64_t v, const RangeSink& emit) {
            const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(v));
            for (size_t i = 0; i < sp.count; ++i) {
                const CorpusPos p = sp.at(i);
                emit(p, p);
            }
        }, err, stats);
}

std::string BitmapIndex::structure_base(const std::string& dir, const std::string& name) {
    return dir + "/" + name + ".bnd";
}

bool BitmapIndex::build_structure(const StructuralAttr& sa, const std::string& base,
                                  CorpusPos corpus_size, std::string* err, BuildStats* stats) {
    const Region* r = sa.region_data();
    const size_t n = sa.region_count();
    for (size_t i = 1; i < n; ++i)
        if (r[i].start <= r[i - 1].end) {
            if (err) *err = base + ": regions overlap or are not sorted (flat structures only)";
            return false;
        }
    return build_generic(base, corpus_size, kStructValues,
        [&](int64_t v, const RangeSink& emit) {
            CorpusPos a = -1, b = -2;   // pending covered range (merged across adjacent regions)
            for (size_t i = 0; i < n; ++i) {
                const CorpusPos s = std::max<CorpusPos>(r[i].start, 0);
                const CorpusPos e = std::min<CorpusPos>(r[i].end, corpus_size - 1);
                if (e < s) continue;
                if (v == kStructEnds) { emit(e, e); continue; }
                if (s == b + 1) { b = e; continue; }
                if (a >= 0) emit(a, b);
                a = s;
                b = e;
            }
            if (v == kStructCovered && a >= 0) emit(a, b);
        }, err, stats);
}

bool BitmapIndex::open(const PositionalAttr& pa, CorpusPos corpus_size) {
    return open_generic(pa.base_path(), corpus_size, pa.lexicon().size(), pa.base_path() + ".rev");
}

bool BitmapIndex::open_structure(const std::string& base, CorpusPos corpus_size) {
    const std::string suffix = ".bnd";
    std::string src;
    if (base.size() > suffix.size() && base.compare(base.size() - suffix.size(), suffix.size(), suffix) == 0)
        src = base.substr(0, base.size() - suffix.size()) + ".rgn";
    return open_generic(base, corpus_size, kStructValues, src);
}

bool BitmapIndex::open_generic(const std::string& base, CorpusPos corpus_size, int64_t nvalues,
                               const std::string& source) {
    const std::string ip = idx_path(base), pp = path(base);
    std::error_code ec;
    if (!fs::exists(ip, ec) || !fs::exists(pp, ec)) return false;
    if (!source.empty() && fs::exists(source, ec)) {
        const auto ts = fs::last_write_time(source, ec);
        if (!ec && fs::last_write_time(ip, ec) < ts) return false;   // stale
    }
    MmapFile idx = MmapFile::open(ip, false);
    if (!idx.valid() || idx.size() < sizeof(Header)) return false;
    Header h;
    std::memcpy(&h, idx.data(), sizeof h);
    if (std::memcmp(h.magic, kMagic, 8) != 0) return false;
    const size_t nchunks = static_cast<size_t>((corpus_size + kChunk - 1) >> kChunkShift);
    if (h.corpus_size != static_cast<uint64_t>(corpus_size) || h.nvalues != static_cast<uint64_t>(nvalues)
        || h.nchunks != nchunks)
        return false;
    const size_t want = sizeof(Header) + (h.nvalues + 1) * 8 + h.nentries * sizeof(Entry);
    if (idx.size() != want) return false;
    if (static_cast<uint64_t>(fs::file_size(pp, ec)) != h.payload_bytes || ec) return false;
    const auto* bytes = static_cast<const uint8_t*>(idx.data());
    const auto* vs = reinterpret_cast<const uint64_t*>(bytes + sizeof(Header));
    if (vs[h.nvalues] != h.nentries) return false;
    if (h.payload_bytes > 0) {
        payload_file_ = MmapFile::open(pp, false);
        if (!payload_file_.valid()) return false;
        payload_ = static_cast<const uint8_t*>(payload_file_.data());
    }
    value_start_ = vs;
    entries_ = reinterpret_cast<const Entry*>(bytes + sizeof(Header) + (h.nvalues + 1) * 8);
    corpus_size_ = corpus_size;
    nchunks_ = nchunks;
    nvalues_ = nvalues;
    idx_ = std::move(idx);
    return true;
}

void BitmapIndex::or_into(const Entry& e, uint64_t* out) const {
    if (is_full(e)) {
        const uint32_t len = chunk_len(e.chunk);
        const size_t full_words = len >> 6;
        for (size_t w = 0; w < full_words; ++w) out[w] = ~uint64_t{0};
        if (len & 63) out[full_words] |= (uint64_t{1} << (len & 63)) - 1;
    } else if (e.card > kArrayMax) {
        const uint64_t* b = bitmap_words(e);
        for (size_t w = 0; w < kWords; ++w) out[w] |= b[w];
    } else {
        const uint16_t* a = array_values(e);
        for (uint32_t k = 0; k < e.card; ++k) out[a[k] >> 6] |= uint64_t{1} << (a[k] & 63);
    }
}

uint64_t BitmapIndex::first_word(const Entry& e) const {
    if (is_full(e)) {
        const uint32_t len = chunk_len(e.chunk);
        return len >= 64 ? ~uint64_t{0} : ((uint64_t{1} << len) - 1);
    }
    if (e.card > kArrayMax) return bitmap_words(e)[0];
    const uint16_t* a = array_values(e);
    uint64_t w = 0;
    for (uint32_t k = 0; k < e.card && a[k] < 64; ++k) w |= uint64_t{1} << a[k];
    return w;
}

} // namespace pando
