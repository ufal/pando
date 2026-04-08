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

    // Regex match: returns owned vector (union of all matching lex entries).
    // full_match: whole lexicon string must match (RE2 FullMatch / std::regex_match); else substring.
#ifdef PANDO_USE_RE2
    std::vector<CorpusPos> positions_matching(const re2::RE2& re, bool full_match = false) const;
#else
    std::vector<CorpusPos> positions_matching(const std::regex& re, bool full_match = false) const;
#endif

    // Negation: all positions where value != given value
    std::vector<CorpusPos> positions_not(const std::string& value,
                                         CorpusPos corpus_size) const;

    const Lexicon& lexicon() const { return lexicon_; }
    CorpusPos corpus_size() const { return corpus_size_; }

    // RG-5f: Multivalue component reverse index support.
    // Call open_mv() after open() for attrs declared as multivalue.
    void open_mv(const std::string& base_path, bool preload = false);
    bool has_mv() const { return mv_rev_idx_.valid(); }
    const Lexicon& mv_lexicon() const { return mv_lexicon_; }

    // Lookup a single component value (e.g. "artist") in the MV lexicon.
    // Returns UNKNOWN_LEX if not found.
    LexiconId mv_lookup(std::string_view component) const;

    // Count of positions containing this component.
    size_t mv_count_of(const std::string& component) const;
    size_t mv_count_of_id(LexiconId mv_id) const;

    // Lazy iteration over positions for a component MV lex ID.
    template<typename F>
    bool for_each_position_mv(LexiconId mv_id, F&& f) const {
        const auto* idx = mv_rev_idx_.as<int64_t>();
        int64_t start = idx[mv_id];
        int64_t end   = idx[mv_id + 1];
        const size_t count = static_cast<size_t>(end - start);
        switch (mv_rev_width_) {
            case 2: {
                const auto* p = mv_rev_.as<int16_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            case 4: {
                const auto* p = mv_rev_.as<int32_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<CorpusPos>(p[i]))) return false;
                break;
            }
            default: {
                const auto* p = mv_rev_.as<int64_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(p[i])) return false;
                break;
            }
        }
        return true;
    }

private:
    Lexicon  lexicon_;
    MmapFile corpus_;      // .dat  — int8/int16/int32 per position
    MmapFile rev_;         // .rev  — int16/int32/int64 sorted positions per lex id
    MmapFile rev_idx_;     // .rev.idx — int64[lex_size+1] element offsets

    CorpusPos corpus_size_ = 0;
    int dat_width_ = 4;    // bytes per element in .dat (1, 2, or 4)
    int rev_width_ = 8;    // bytes per element in .rev (2, 4, or 8)

    // RG-5f: MV component reverse index (optional, only for multivalue attrs)
    Lexicon  mv_lexicon_;      // .mv.lex — sorted component strings
    MmapFile mv_rev_;          // .mv.rev — positions per component
    MmapFile mv_rev_idx_;      // .mv.rev.idx — int64 offsets
    int mv_rev_width_ = 8;

    // Stage 1 of PANDO-MULTIVALUE-FIELDS: forward MV index.
    // Spec: dev/PANDO-MVAL-FORMAT.md (v0.2). Optional sidecar; if absent
    // (older corpora) has_mv_fwd() returns false and consumers fall back.
    MmapFile mv_fwd_;          // .mv.fwd     — sorted MV component ids per position
    MmapFile mv_fwd_idx_;      // .mv.fwd.idx — int64[corpus_size+1] offsets
    int mv_fwd_width_ = 4;     // 2 or 4 bytes per element

public:
    // Open the forward MV index. No-op if base.mv.fwd.idx is missing (older corpora);
    // missing path must not throw — MmapFile::open fails on ENOENT otherwise.
    void open_mv_fwd(const std::string& base_path, bool preload = false);
    bool has_mv_fwd() const { return mv_fwd_idx_.valid(); }

    // Number of MV components at this position. Returns 0 when has_mv_fwd()
    // is false or the position carries the empty set (zero-length run).
    size_t mv_fwd_count_at(CorpusPos pos) const;

    // Lazy iteration over the sorted, deduplicated MV component ids at pos.
    // f(LexiconId mv_id) → bool; returning false stops early.
    template<typename F>
    bool for_each_mv_fwd_at(CorpusPos pos, F&& f) const {
        if (!has_mv_fwd()) return true;
        const auto* idx = mv_fwd_idx_.as<int64_t>();
        int64_t start = idx[pos];
        int64_t end   = idx[pos + 1];
        const size_t count = static_cast<size_t>(end - start);
        switch (mv_fwd_width_) {
            case 2: {
                const auto* p = mv_fwd_.as<int16_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<LexiconId>(p[i]))) return false;
                break;
            }
            default: {
                const auto* p = mv_fwd_.as<int32_t>() + start;
                for (size_t i = 0; i < count; ++i)
                    if (!f(static_cast<LexiconId>(p[i]))) return false;
                break;
            }
        }
        return true;
    }

    // Convenience: copy MV component ids at pos into a small vector.
    std::vector<LexiconId> mv_fwd_at(CorpusPos pos) const;
};

} // namespace manatree
