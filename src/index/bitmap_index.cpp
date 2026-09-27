#include "index/bitmap_index.h"
#include "index/positional_attr.h"
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

bool BitmapIndex::build(const PositionalAttr& pa, CorpusPos corpus_size, std::string* err,
                        BuildStats* stats) {
    auto fail = [&](const std::string& m) { if (err) *err = m; return false; };
    const std::string base = pa.base_path();
    const int64_t nvalues = pa.lexicon().size();
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
    std::vector<uint64_t> words(kWords);
    std::vector<uint16_t> arr;
    arr.reserve(kArrayMax);
    bool io_ok = true;

    auto chunk_len = [&](size_t c) -> uint32_t {
        const CorpusPos r = corpus_size - (static_cast<CorpusPos>(c) << kChunkShift);
        return static_cast<uint32_t>(r < kChunk ? r : kChunk);
    };
    auto put = [&](const void* p, size_t n) {
        if (n && std::fwrite(p, 1, n, pf) != n) io_ok = false;
        off += n;
    };

    for (int64_t v = 0; v < nvalues && io_ok; ++v) {
        value_start[static_cast<size_t>(v)] = entries.size();
        const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(v));
        size_t i = 0;
        while (i < sp.count) {
            const CorpusPos p0 = sp.at(i);
            if (p0 < 0 || p0 >= corpus_size) {
                std::fclose(pf);
                std::remove(pay_tmp.c_str());
                return fail(base + ".rev: position out of range");
            }
            const size_t c = static_cast<size_t>(p0 >> kChunkShift);
            const CorpusPos cbase = static_cast<CorpusPos>(c) << kChunkShift;
            const CorpusPos cend = cbase + kChunk;
            size_t j = i;
            while (j < sp.count && sp.at(j) < cend) ++j;
            const uint32_t card = static_cast<uint32_t>(j - i);
            Entry e{static_cast<uint32_t>(c), card, off};
            if (card == chunk_len(c)) {
                ++st.fulls;
            } else if (card > kArrayMax) {
                std::fill(words.begin(), words.end(), 0);
                for (size_t k = i; k < j; ++k) {
                    const uint32_t o = static_cast<uint32_t>(sp.at(k) - cbase);
                    words[o >> 6] |= uint64_t{1} << (o & 63);
                }
                put(words.data(), kWords * 8);
                ++st.bitmaps;
            } else {
                arr.clear();
                for (size_t k = i; k < j; ++k) arr.push_back(static_cast<uint16_t>(sp.at(k) - cbase));
                const size_t pad = (8 - (arr.size() * 2) % 8) % 8;
                arr.resize(arr.size() + pad / 2, 0);
                put(arr.data(), arr.size() * 2);
                ++st.arrays;
            }
            entries.push_back(e);
            i = j;
        }
    }
    value_start[static_cast<size_t>(nvalues)] = entries.size();
    if (std::fclose(pf) != 0) io_ok = false;
    if (!io_ok) {
        std::remove(pay_tmp.c_str());
        return fail("cannot write " + pay_tmp);
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

bool BitmapIndex::open(const PositionalAttr& pa, CorpusPos corpus_size) {
    const std::string base = pa.base_path();
    const std::string ip = idx_path(base), pp = path(base);
    std::error_code ec;
    if (!fs::exists(ip, ec) || !fs::exists(pp, ec)) return false;
    MmapFile idx = MmapFile::open(ip, false);
    if (!idx.valid() || idx.size() < sizeof(Header)) return false;
    Header h;
    std::memcpy(&h, idx.data(), sizeof h);
    if (std::memcmp(h.magic, kMagic, 8) != 0) return false;
    const int64_t nvalues = pa.lexicon().size();
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
