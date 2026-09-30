#include "index/lexicon.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace pando {

// ── Lexicon (read-only) ─────────────────────────────────────────────────

void Lexicon::open(const std::string& base, bool preload) {
    strings_ = MmapFile::open(base + ".lex", preload);
    offsets_ = MmapFile::open(base + ".lex.idx", preload);
}

LexiconId Lexicon::size() const {
    if (!offsets_.valid()) return 0;
    // offsets has lex_size+1 entries
    return static_cast<LexiconId>(offsets_.count<int64_t>()) - 1;
}

std::string_view Lexicon::get(LexiconId id) const {
    if (id < 0 || id >= size()) return {};
    const auto* off = offsets_.as<int64_t>();
    const char* base = static_cast<const char*>(strings_.data());
    return std::string_view(base + off[id],
                            static_cast<size_t>(off[id + 1] - off[id] - 1));
}

LexiconId Lexicon::lookup(std::string_view value) const {
    LexiconId lo = 0, hi = size();
    while (lo < hi) {
        LexiconId mid = lo + (hi - lo) / 2;
        std::string_view entry = get(mid);
        int cmp = value.compare(entry);
        if (cmp == 0) return mid;
        if (cmp < 0) hi = mid;
        else         lo = mid + 1;
    }
    return UNKNOWN_LEX;
}

std::vector<LexiconId> Lexicon::ids_containing(char c) const {
    std::vector<LexiconId> out;
    const LexiconId n = size();
    if (n <= 0) return out;
    const auto* off = offsets_.as<int64_t>();
    const char* base = static_cast<const char*>(strings_.data());
    const char* end = base + off[n];
    const char* p = base;
    while (p < end) {
        const void* hit = std::memchr(p, c, static_cast<size_t>(end - p));
        if (!hit) break;
        const int64_t at = static_cast<const char*>(hit) - base;
        // the entry whose [off[id], off[id+1]) holds `at`
        const LexiconId id = static_cast<LexiconId>(
            std::upper_bound(off, off + n + 1, at) - off) - 1;
        if (id < 0 || id >= n) break;
        if (static_cast<size_t>(at - off[id]) < get(id).size()) out.push_back(id);   // not the terminator
        p = base + off[id + 1];                     // next entry
    }
    return out;
}

} // namespace pando
