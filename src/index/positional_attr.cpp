#include "index/positional_attr.h"
#include <algorithm>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

namespace manatree {

// ── PositionalAttr (read-only) ──────────────────────────────────────────

void PositionalAttr::open(const std::string& base, CorpusPos corpus_size, bool preload) {
    corpus_size_ = corpus_size;
    lexicon_.open(base, preload);
    corpus_  = MmapFile::open(base + ".dat", preload);
    rev_     = MmapFile::open(base + ".rev", preload);
    rev_idx_ = MmapFile::open(base + ".rev.idx", preload);

    // Infer .dat element width from file size
    if (corpus_size_ > 0) {
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

LexiconId PositionalAttr::id_at(CorpusPos pos) const {
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

std::vector<CorpusPos> PositionalAttr::positions_of_id(LexiconId id) const {
    const auto* idx = rev_idx_.as<int64_t>();
    int64_t start = idx[id];
    int64_t end   = idx[id + 1];
    size_t count = static_cast<size_t>(end - start);

    std::vector<CorpusPos> result(count);

    switch (rev_width_) {
        case 2: {
            auto* p = rev_.as<int16_t>() + start;
            for (size_t i = 0; i < count; ++i) result[i] = static_cast<CorpusPos>(p[i]);
            break;
        }
        case 4: {
            auto* p = rev_.as<int32_t>() + start;
            for (size_t i = 0; i < count; ++i) result[i] = static_cast<CorpusPos>(p[i]);
            break;
        }
        default: {
            auto* p = rev_.as<int64_t>() + start;
            for (size_t i = 0; i < count; ++i) result[i] = p[i];
            break;
        }
    }
    return result;
}

#ifdef PANDO_USE_RE2
std::vector<CorpusPos> PositionalAttr::positions_matching(
        const re2::RE2& re, bool full_match) const {
    std::vector<CorpusPos> result;
    LexiconId n = lexicon_.size();
    for (LexiconId id = 0; id < n; ++id) {
        std::string_view sv = lexicon_.get(id);
        bool ok = full_match ? re2::RE2::FullMatch(sv, re) : re2::RE2::PartialMatch(sv, re);
        if (ok) {
            auto span = positions_of_id(id);
            result.insert(result.end(), span.begin(), span.end());
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}
#else
std::vector<CorpusPos> PositionalAttr::positions_matching(
        const std::regex& re, bool full_match) const {
    std::vector<CorpusPos> result;
    LexiconId n = lexicon_.size();
    for (LexiconId id = 0; id < n; ++id) {
        std::string_view sv = lexicon_.get(id);
        std::string s(sv);
        bool ok = full_match ? std::regex_match(s, re) : std::regex_search(s, re);
        if (ok) {
            auto span = positions_of_id(id);
            result.insert(result.end(), span.begin(), span.end());
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}
#endif

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

} // namespace manatree
