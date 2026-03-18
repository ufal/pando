#include "index/lexicon.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace manatree {

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

} // namespace manatree
