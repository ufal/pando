#include "index/packed_dat.h"

#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <numeric>
#include <vector>

namespace fs = std::filesystem;

namespace pando {

namespace {

struct Header {
    char magic[8];       // "PNDDATK1"
    uint32_t version;    // 1
    uint32_t mode;       // PackedDat::Mode
    int64_t n;
    int64_t nlex;
    uint32_t bits;       // fixed
    uint32_t reserved;
    uint64_t nsuper;     // group
    uint64_t nblocks;    // group
    uint64_t payload_bytes;
};
static_assert(sizeof(Header) == 64, "Header layout");
constexpr char kMagic[8] = {'P', 'N', 'D', 'D', 'A', 'T', 'K', '1'};

size_t pad8(size_t x) { return (x + 7) & ~size_t{7}; }

bool put(FILE* f, const void* d, size_t n) { return n == 0 || std::fwrite(d, 1, n, f) == n; }

}  // namespace

bool PackedDat::build(const std::string& base, int64_t n, int64_t nlex, const Fill& fill, std::string* err,
                      BuildStats* stats) {
    auto fail = [&](const std::string& m) { if (err) *err = m; return false; };
    const std::string out = path(base), tmp = out + ".tmp", ptmp = out + ".payload.tmp";
    constexpr int64_t kChunk = 1 << 20;
    std::vector<LexiconId> buf(static_cast<size_t>(kChunk));
    Header h{};
    std::memcpy(h.magic, kMagic, 8);
    h.version = 1;
    h.n = n;
    h.nlex = nlex;
    FILE* f = std::fopen(tmp.c_str(), "wb");
    if (!f) return fail("cannot write " + tmp);
    const char zero[8] = {};

    // tests: PANDO_PACKED_DAT_GROUP=a,b forces the group coding for those attributes
    bool force_group = false;
    if (const char* g = std::getenv("PANDO_PACKED_DAT_GROUP")) {
        const std::string name = fs::path(base).filename().string(), list = std::string(",") + g + ",";
        force_group = list.find("," + name + ",") != std::string::npos;
    }
    if (nlex <= 65536 && !force_group) {
        unsigned bits = 1;
        while ((int64_t{1} << bits) < nlex) ++bits;
        h.mode = static_cast<uint32_t>(Mode::Fixed);
        h.bits = bits;
        h.payload_bytes = (static_cast<uint64_t>(n) * bits + 7) / 8;
        bool ok = put(f, &h, sizeof h);
        uint64_t acc = 0;
        unsigned nb = 0;
        std::vector<uint8_t> bytes;
        for (int64_t a = 0; a < n && ok; a += kChunk) {
            const int64_t b = std::min(n, a + kChunk);
            fill(a, b, buf.data());
            bytes.clear();
            for (int64_t i = 0; i < b - a; ++i) {
                acc |= static_cast<uint64_t>(static_cast<uint32_t>(buf[static_cast<size_t>(i)])) << nb;
                nb += bits;
                while (nb >= 8) {
                    bytes.push_back(static_cast<uint8_t>(acc));
                    acc >>= 8;
                    nb -= 8;
                }
            }
            ok = put(f, bytes.data(), bytes.size());
        }
        if (nb) {
            const uint8_t last = static_cast<uint8_t>(acc);
            ok = ok && put(f, &last, 1);
        }
        ok = ok && put(f, zero, 8);
        if (std::fclose(f) != 0 || !ok) return fail("write failed: " + tmp);
        if (stats) {
            stats->mode = Mode::Fixed;
            stats->bits = bits;
        }
    } else {
        // pass 1: frequencies → ranks (most frequent = 0; ties by id)
        std::vector<int64_t> cnt(static_cast<size_t>(nlex), 0);
        for (int64_t a = 0; a < n; a += kChunk) {
            const int64_t b = std::min(n, a + kChunk);
            fill(a, b, buf.data());
            for (int64_t i = 0; i < b - a; ++i) {
                const LexiconId id = buf[static_cast<size_t>(i)];
                if (id < 0 || id >= nlex) {
                    std::fclose(f);
                    return fail(base + ": id " + std::to_string(id) + " outside the lexicon");
                }
                ++cnt[static_cast<size_t>(id)];
            }
        }
        std::vector<int32_t> rank_to_id(static_cast<size_t>(nlex));
        std::iota(rank_to_id.begin(), rank_to_id.end(), 0);
        std::stable_sort(rank_to_id.begin(), rank_to_id.end(),
                         [&](int32_t x, int32_t y) { return cnt[static_cast<size_t>(x)] > cnt[static_cast<size_t>(y)]; });
        std::vector<uint32_t> id_to_rank(static_cast<size_t>(nlex));
        for (size_t r = 0; r < rank_to_id.size(); ++r) id_to_rank[static_cast<size_t>(rank_to_id[r])] = static_cast<uint32_t>(r);
        cnt.clear();
        cnt.shrink_to_fit();

        // pass 2: blocks of 16 into the payload file, offsets in memory
        const uint64_t nblocks = (static_cast<uint64_t>(n) + kBlock - 1) / kBlock;
        const uint64_t nsuper = (static_cast<uint64_t>(n) + kSuper - 1) / kSuper;
        std::vector<uint64_t> bases;
        std::vector<uint16_t> rel;
        bases.reserve(nsuper);
        rel.reserve(nblocks);
        FILE* pf = std::fopen(ptmp.c_str(), "wb");
        if (!pf) {
            std::fclose(f);
            return fail("cannot write " + ptmp);
        }
        uint64_t off = 0;
        bool ok = true;
        std::vector<uint8_t> bytes;
        for (int64_t a = 0; a < n && ok; a += kChunk) {   // kChunk is a multiple of kSuper
            const int64_t b = std::min(n, a + kChunk);
            fill(a, b, buf.data());
            bytes.clear();
            for (int64_t blk = a; blk < b; blk += static_cast<int64_t>(kBlock)) {
                if (blk % static_cast<int64_t>(kSuper) == 0) bases.push_back(off + bytes.size());
                rel.push_back(static_cast<uint16_t>(off + bytes.size() - bases.back()));
                const size_t hat = bytes.size();
                bytes.resize(hat + 4);
                uint32_t hdr = 0;
                const int64_t e = std::min(b, blk + static_cast<int64_t>(kBlock));
                for (int64_t p = blk; p < e; ++p) {
                    const uint32_t r = id_to_rank[static_cast<size_t>(buf[static_cast<size_t>(p - a)])];
                    const unsigned len = r < (1u << 8) ? 1 : r < (1u << 16) ? 2 : r < (1u << 24) ? 3 : 4;
                    hdr |= (len - 1) << (2 * (p - blk));
                    for (unsigned i = 0; i < len; ++i) bytes.push_back(static_cast<uint8_t>(r >> (8 * i)));
                }
                std::memcpy(bytes.data() + hat, &hdr, 4);
            }
            ok = put(pf, bytes.data(), bytes.size());
            off += bytes.size();
        }
        ok = ok && put(pf, zero, 8);
        if (std::fclose(pf) != 0 || !ok) {
            std::fclose(f);
            return fail("write failed: " + ptmp);
        }
        h.mode = static_cast<uint32_t>(Mode::Group);
        h.nsuper = nsuper;
        h.nblocks = nblocks;
        h.payload_bytes = off;
        ok = put(f, &h, sizeof h) && put(f, bases.data(), bases.size() * 8)
            && put(f, rel.data(), rel.size() * 2) && put(f, zero, pad8(rel.size() * 2) - rel.size() * 2)
            && put(f, rank_to_id.data(), rank_to_id.size() * 4)
            && put(f, zero, pad8(rank_to_id.size() * 4) - rank_to_id.size() * 4);
        // the payload after the tables
        FILE* in = std::fopen(ptmp.c_str(), "rb");
        if (!in) ok = false;
        std::vector<char> cp(size_t{4} << 20);
        for (size_t got; ok && (got = std::fread(cp.data(), 1, cp.size(), in)) > 0;) ok = put(f, cp.data(), got);
        if (in) std::fclose(in);
        std::error_code ec;
        fs::remove(ptmp, ec);
        if (std::fclose(f) != 0 || !ok) return fail("write failed: " + tmp);
        if (stats) stats->mode = Mode::Group;
    }
    std::error_code ec;
    fs::rename(tmp, out, ec);
    if (ec) return fail("cannot rename " + tmp + ": " + ec.message());
    if (stats) stats->bytes = fs::file_size(out, ec);
    return true;
}

bool PackedDat::open(const std::string& base, int64_t n, int64_t nlex, bool preload) {
    file_ = MmapFile{};
    const std::string p = path(base);
    std::error_code ec;
    if (!fs::exists(p, ec)) return false;
    const auto t = fs::last_write_time(p, ec);
    for (const char* src : {".lex", ".dat"}) {
        std::error_code e2;
        if (fs::exists(base + src, e2) && fs::last_write_time(base + src, e2) > t) return false;   // stale
    }
    MmapFile f = MmapFile::open(p, preload);
    if (!f.valid() || f.size() < sizeof(Header)) return false;
    Header h;
    std::memcpy(&h, f.data(), sizeof h);
    if (std::memcmp(h.magic, kMagic, 8) != 0 || h.version != 1 || h.n != n || h.nlex != nlex) return false;
    const uint8_t* d = static_cast<const uint8_t*>(f.data()) + sizeof(Header);
    size_t need = sizeof(Header);
    if (h.mode == static_cast<uint32_t>(Mode::Fixed)) {
        if (h.bits == 0 || h.bits > 16) return false;
        need += h.payload_bytes + 8;
        if (f.size() != need) return false;
        mode_ = Mode::Fixed;
        bits_ = h.bits;
        mask_ = (uint64_t{1} << h.bits) - 1;
        payload_ = d;
    } else if (h.mode == static_cast<uint32_t>(Mode::Group)) {
        const size_t sb = h.nsuper * 8, rb = pad8(h.nblocks * 2), tb = pad8(static_cast<size_t>(h.nlex) * 4);
        need += sb + rb + tb + h.payload_bytes + 8;
        if (f.size() != need || h.nblocks != (static_cast<uint64_t>(n) + kBlock - 1) / kBlock
            || h.nsuper != (static_cast<uint64_t>(n) + kSuper - 1) / kSuper)
            return false;
        mode_ = Mode::Group;
        bases_ = reinterpret_cast<const uint64_t*>(d);
        rel_ = reinterpret_cast<const uint16_t*>(d + sb);
        rank_to_id_ = reinterpret_cast<const int32_t*>(d + sb + rb);
        payload_ = d + sb + rb + tb;
    } else {
        return false;
    }
    file_ = std::move(f);
    return true;
}

template <typename Out, typename Map>
static void decode_group(const uint8_t* payload, const uint64_t* bases, const uint16_t* rel, CorpusPos a,
                         CorpusPos b, Out* out, Map map) {
    CorpusPos p = a;
    while (p < b) {   // a block at a time: header once, values in sequence
        const uint64_t blk_i = static_cast<uint64_t>(p) / PackedDat::kBlock;
        const uint8_t* blk = payload + bases[static_cast<uint64_t>(p) / PackedDat::kSuper] + rel[blk_i];
        uint32_t h;
        std::memcpy(&h, blk, 4);
        const unsigned k0 = static_cast<unsigned>(p % PackedDat::kBlock);
        const uint32_t m = k0 ? (h & ((uint32_t{1} << (2 * k0)) - 1)) : 0;
        unsigned off = 4 + k0 + static_cast<unsigned>(__builtin_popcount(m & 0x55555555u))
                     + 2 * static_cast<unsigned>(__builtin_popcount(m & 0xAAAAAAAAu));
        const CorpusPos end = std::min<CorpusPos>(b, static_cast<CorpusPos>((blk_i + 1) * PackedDat::kBlock));
        for (unsigned k = k0; p < end; ++p, ++k) {
            const unsigned len = ((h >> (2 * k)) & 3u) + 1;
            uint32_t v;
            std::memcpy(&v, blk + off, 4);
            if (len < 4) v &= (uint32_t{1} << (8 * len)) - 1;
            *out++ = map(v);
            off += len;
        }
    }
}

void PackedDat::decode(CorpusPos a, CorpusPos b, LexiconId* out) const {
    if (mode_ == Mode::Fixed) {
        for (CorpusPos p = a; p < b; ++p) *out++ = at(p);
        return;
    }
    const int32_t* t = rank_to_id_;
    decode_group(payload_, bases_, rel_, a, b, out, [t](uint32_t v) { return static_cast<LexiconId>(t[v]); });
}

void PackedDat::decode_ranks(CorpusPos a, CorpusPos b, uint32_t* out) const {
    if (mode_ == Mode::Fixed) {
        for (CorpusPos p = a; p < b; ++p) *out++ = static_cast<uint32_t>(at(p));
        return;
    }
    decode_group(payload_, bases_, rel_, a, b, out, [](uint32_t v) { return v; });
}

}  // namespace pando
