#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/lexicon.h"
#include <string>
#include <string_view>
#include <vector>

namespace pando {

// Case / diacritic folding modes for `%c`, `%d`, `%cd`.
enum class FoldMode : uint8_t { Lower = 0, NoAccents = 1, LowerNoAccents = 2 };

FoldMode fold_mode_for(bool case_fold, bool accent_fold);
const char* fold_mode_suffix(FoldMode m);          // "lc", "na", "lcna"
std::string fold_string(FoldMode m, std::string_view s);

// P1.6: index-time lookup for folded values.
//
// `<attr_base>.fold_<suffix>.v2.perm` holds the attribute's lexicon ids (int32)
// sorted by (fold(string), id): 4 bytes per lexicon entry and mode. A folded
// query value is found by binary search over the permutation (folding the
// ~log2(V) probed entries on the fly), so `[form="the" %c]` no longer builds
// an in-memory fold map over the whole lexicon in every process (~2 s for a
// 3M-type lexicon).
class FoldIndex {
public:
    static std::string path(const std::string& attr_base, FoldMode m);

    /// Write the permutation file for `lex`. Returns false and sets *err on failure.
    static bool build(const std::string& attr_base, const Lexicon& lex, FoldMode m,
                      std::string* err);

    /// Open an existing file; false when absent or not matching `lex_size`.
    bool open(const std::string& attr_base, FoldMode m, LexiconId lex_size);

    /// Lexicon ids whose folded string equals `folded_query` (sorted ascending).
    std::vector<LexiconId> lookup(const Lexicon& lex, std::string_view folded_query) const;

private:
    MmapFile perm_;
    FoldMode mode_ = FoldMode::Lower;
};

} // namespace pando
