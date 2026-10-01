#include "index/packed_postings.h"
#include "index/positional_attr.h"

#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

namespace pando {

namespace {

struct Header {
    char magic[8];          // "PNDPFB2\0"
    uint32_t version;       // 2
    uint32_t block;         // kBlock
    int64_t nlex;
    int64_t total;          // postings
    int64_t nblocks;
    int32_t rev_width;      // of the plain .rev it was built from
    uint32_t group;         // kGroup
    int64_t payload_bytes;
    int64_t reserved;
};
static_assert(sizeof(Header) == 64, "Header layout");

constexpr char kMagic[8] = {'P', 'N', 'D', 'P', 'F', 'B', '2', '\0'};

/// Little-endian bit writer for one block's gaps (w <= 56).
void put_bits(std::vector<uint8_t>& out, const CorpusPos* pos, size_t n, unsigned w) {
    uint64_t acc = 0;
    unsigned nbits = 0;
    for (size_t i = 1; i < n; ++i) {
        acc |= static_cast<uint64_t>(pos[i] - pos[i - 1] - 1) << nbits;   // nbits < 8 here
        nbits += w;
        while (nbits >= 8) {
            out.push_back(static_cast<uint8_t>(acc));
            acc >>= 8;
            nbits -= 8;
        }
    }
    if (nbits) out.push_back(static_cast<uint8_t>(acc));
}

/// The block's width and its gaps appended to out.
unsigned pack_block(std::vector<uint8_t>& out, const CorpusPos* pos, size_t n) {
    uint64_t maxgap = 0;
    for (size_t i = 1; i < n; ++i) maxgap = std::max<uint64_t>(maxgap, static_cast<uint64_t>(pos[i] - pos[i - 1] - 1));
    unsigned w = maxgap ? 64 - static_cast<unsigned>(__builtin_clzll(maxgap)) : 0;
    if (w > 56) {
        for (size_t i = 1; i < n; ++i) {
            const int64_t v = pos[i];
            const auto* b = reinterpret_cast<const uint8_t*>(&v);
            out.insert(out.end(), b, b + 8);
        }
        return PackedPostings::kRawWidth;
    }
    if (w) put_bits(out, pos, n, w);
    return w;
}

void put_varint(std::vector<uint8_t>& out, uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<uint8_t>(v | 0x80));
        v >>= 7;
    }
    out.push_back(static_cast<uint8_t>(v));
}

}  // namespace

bool PackedPostings::build(const PositionalAttr& pa, std::string* err, BuildStats* stats) {
    const std::string base = pa.base_path();
    const int64_t nlex = pa.lexicon().size();
    const std::string ptmp = path(base) + ".tmp", itmp = idx_path(base) + ".tmp";
    std::ofstream pf(ptmp, std::ios::binary | std::ios::trunc);
    if (!pf) {
        if (err) *err = "cannot write " + ptmp;
        return false;
    }
    std::vector<uint64_t> bases;
    std::vector<uint32_t> rel;
    rel.reserve(static_cast<size_t>(nlex) + 1);
    uint64_t offset = 0, total = 0, nblocks = 0;
    std::vector<uint8_t> out, gaps;
    std::vector<CorpusPos> pos;
    auto mark = [&](int64_t id) -> bool {   // the id's data starts at `offset`
        if (static_cast<size_t>(id) % kGroup == 0) bases.push_back(offset);
        const uint64_t r = offset - bases.back();
        if (r > UINT32_MAX) {
            if (err) *err = base + ": a group of " + std::to_string(kGroup) + " ids above 4 GB of packed postings";
            return false;
        }
        rel.push_back(static_cast<uint32_t>(r));
        return true;
    };
    for (int64_t id = 0; id < nlex; ++id) {
        if (!mark(id)) return false;
        const RevSpan sp = pa.rev_span_of_id(static_cast<LexiconId>(id));
        total += sp.count;
        if (sp.count == 0) continue;
        pos.resize(sp.count);
        for (size_t i = 0; i < sp.count; ++i) {
            pos[i] = sp.at(i);
            if (i && pos[i] <= pos[i - 1]) {
                if (err) *err = base + ".rev: postings of id " + std::to_string(id) + " not ascending";
                return false;
            }
        }
        out.clear();
        if (sp.count <= kBlock) {
            put_varint(out, static_cast<uint64_t>(pos[0]));
            if (sp.count > 1) {
                const size_t wat = out.size();
                out.push_back(0);
                out[wat] = static_cast<uint8_t>(pack_block(out, pos.data(), sp.count));
            }
            ++nblocks;
        } else {
            const size_t nb = (sp.count + kBlock - 1) / kBlock;
            out.resize(nb * kSkipBytes);
            gaps.clear();
            for (size_t b = 0; b < nb; ++b) {
                const size_t at = b * kBlock, n = std::min(kBlock, sp.count - at);
                const uint64_t goff = out.size() + gaps.size();   // from the id's data start
                if (goff > UINT32_MAX) {
                    if (err) *err = base + ": the packed postings of one id above 4 GB";
                    return false;
                }
                const unsigned w = pack_block(gaps, pos.data() + at, n);
                const int64_t first = pos[at];
                const uint32_t off = static_cast<uint32_t>(goff);
                std::memcpy(out.data() + b * kSkipBytes, &first, 8);
                std::memcpy(out.data() + b * kSkipBytes + 8, &off, 4);
                out[b * kSkipBytes + 12] = static_cast<uint8_t>(w);
            }
            out.insert(out.end(), gaps.begin(), gaps.end());
            nblocks += nb;
        }
        pf.write(reinterpret_cast<const char*>(out.data()), static_cast<std::streamsize>(out.size()));
        offset += out.size();
    }
    if (!mark(nlex)) return false;
    const char pad[16] = {};
    pf.write(pad, sizeof pad);
    pf.close();
    if (!pf) {
        if (err) *err = "write failed: " + ptmp;
        return false;
    }

    Header h{};
    std::memcpy(h.magic, kMagic, 8);
    h.version = 2;
    h.block = kBlock;
    h.nlex = nlex;
    h.total = static_cast<int64_t>(total);
    h.nblocks = static_cast<int64_t>(nblocks);
    h.rev_width = pa.rev_width();
    h.group = kGroup;
    h.payload_bytes = static_cast<int64_t>(offset);
    std::ofstream xf(itmp, std::ios::binary | std::ios::trunc);
    xf.write(reinterpret_cast<const char*>(&h), sizeof h);
    xf.write(reinterpret_cast<const char*>(bases.data()), static_cast<std::streamsize>(bases.size() * 8));
    xf.write(reinterpret_cast<const char*>(rel.data()), static_cast<std::streamsize>(rel.size() * 4));
    xf.close();
    if (!xf) {
        if (err) *err = "write failed: " + itmp;
        return false;
    }
    std::error_code ec;
    fs::rename(ptmp, path(base), ec);
    if (!ec) fs::rename(itmp, idx_path(base), ec);
    if (ec) {
        if (err) *err = "rename failed: " + ec.message();
        return false;
    }
    if (stats) {
        stats->postings = total;
        stats->blocks = nblocks;
        stats->payload_bytes = offset;
        stats->idx_bytes = sizeof h + bases.size() * 8 + rel.size() * 4;
    }
    return true;
}

bool PackedPostings::open(const std::string& base, int64_t nlex, int64_t total, bool preload) {
    const std::string ip = idx_path(base), pp = path(base);
    std::error_code ec;
    if (!fs::exists(ip, ec) || !fs::exists(pp, ec)) return false;
    const auto src = fs::last_write_time(base + ".rev.idx", ec);
    if (!ec && fs::last_write_time(ip, ec) < src) return false;   // stale
    MmapFile idx = MmapFile::open(ip, preload);
    if (!idx.valid() || idx.size() < sizeof(Header)) return false;
    Header h;
    std::memcpy(&h, idx.data(), sizeof h);
    if (std::memcmp(h.magic, kMagic, 8) != 0 || h.version != 2 || h.block != kBlock || h.group != kGroup)
        return false;
    if (h.nlex != nlex || h.total != total) return false;
    const size_t ngroups = (static_cast<size_t>(nlex) + 1 + kGroup - 1) / kGroup;
    const size_t want = sizeof(Header) + ngroups * 8 + (static_cast<size_t>(nlex) + 1) * 4;
    if (idx.size() != want) return false;
    MmapFile pay = MmapFile::open(pp, preload);
    if (!pay.valid() || pay.size() != static_cast<size_t>(h.payload_bytes) + 16) return false;
    base_ = reinterpret_cast<const uint64_t*>(static_cast<const char*>(idx.data()) + sizeof(Header));
    rel_ = reinterpret_cast<const uint32_t*>(base_ + ngroups);
    payload_ = static_cast<const uint8_t*>(pay.data());
    rev_width_ = h.rev_width;
    idx_ = std::move(idx);
    payload_file_ = std::move(pay);
    return true;
}

}  // namespace pando
