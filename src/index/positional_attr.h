#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/lexicon.h"
#include <string>

#ifdef PANDO_USE_RE2
#include <re2/re2.h>
#else
#include <regex>
#endif

namespace manatree {

// Read-only positional attribute: provides O(log V) lookup from value
// to a sorted position list, and O(1) lookup from position to value.
class PositionalAttr {
public:
    void open(const std::string& base_path, CorpusPos corpus_size, bool preload = false);

    // Position → value
    LexiconId id_at(CorpusPos pos) const;
    std::string_view value_at(CorpusPos pos) const;

    // Value → count (O(1) via rev.idx — no position data touched)
    size_t count_of(const std::string& value) const;
    size_t count_of_id(LexiconId id) const;

    // Value → sorted positions (width-aware, returns owned vector)
    std::vector<CorpusPos> positions_of(const std::string& value) const;
    std::vector<CorpusPos> positions_of_id(LexiconId id) const;

    // Lazy iteration: invoke f(pos) for each position in lex id's .rev list (no vector alloc).
    // Returns false to stop early. Use for large EQ conditions to avoid O(n) allocation.
    template<typename F>
    bool for_each_position_id(LexiconId id, F&& f) const {
        const auto* idx = rev_idx_.as<int64_t>();
        int64_t start = idx[id];
        int64_t end   = idx[id + 1];
        const size_t count = static_cast<size_t>(end - start);
        switch (rev_width_) {
            case 2: {
                const auto* p = rev_.as<int16_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            case 4: {
                const auto* p = rev_.as<int32_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            default: {
                const auto* p = rev_.as<int64_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(p[i])) return false;
                break;
            }
        }
        return true;
    }

    // Regex match: returns owned vector (union of all matching lex entries)
#ifdef PANDO_USE_RE2
    std::vector<CorpusPos> positions_matching(const re2::RE2& re) const;
#else
    std::vector<CorpusPos> positions_matching(const std::regex& re) const;
#endif

    // Negation: all positions where value != given value
    std::vector<CorpusPos> positions_not(const std::string& value,
                                         CorpusPos corpus_size) const;

    const Lexicon& lexicon() const { return lexicon_; }
    CorpusPos corpus_size() const { return corpus_size_; }

private:
    Lexicon  lexicon_;
    MmapFile corpus_;      // .dat  — int8/int16/int32 per position
    MmapFile rev_;         // .rev  — int16/int32/int64 sorted positions per lex id
    MmapFile rev_idx_;     // .rev.idx — int64[lex_size+1] element offsets

    CorpusPos corpus_size_ = 0;
    int dat_width_ = 4;    // bytes per element in .dat (1, 2, or 4)
    int rev_width_ = 8;    // bytes per element in .rev (2, 4, or 8)
};

} // namespace manatree
