#pragma once

#include "core/types.h"
#include "index/lexicon.h"
#include <string>
#include <unordered_map>
#include <vector>

namespace pando {

// FoldMap: maps normalized (lowercased / accent-stripped) strings to sets of
// original LexiconId entries.  Built lazily from a Lexicon; proportional to
// vocabulary size, not corpus size.
//
// Usage:
//   FoldMap fold = FoldMap::build_lowercase(lexicon);
//   auto ids = fold.lookup("the");  // → {id("The"), id("THE"), id("the")}
//
class FoldMap {
public:
    // Build a fold map that lowercases the lexicon.
    static FoldMap build_lowercase(const Lexicon& lex);

    // Build a fold map that strips diacritics (NFD-style, combining marks removed).
    static FoldMap build_no_accents(const Lexicon& lex);

    // Build a fold map that does both lowercase + strip diacritics.
    static FoldMap build_lc_no_accents(const Lexicon& lex);

    // Look up a normalized string → list of original lex IDs.
    // Returns empty span if not found.
    const std::vector<LexiconId>& lookup(const std::string& normalized) const {
        auto it = map_.find(normalized);
        if (it != map_.end()) return it->second;
        return empty_;
    }

    bool empty() const { return map_.empty(); }

    // Normalization helpers (public for use in check_leaf). All UTF-8 aware; a change
    // here changes the order of the <attr>.fold_*.perm files (bump FoldIndex::path).
    static std::string fold(std::string_view s, bool lowercase, bool no_accents);
    static std::string to_lower(std::string_view s);
    static std::string strip_accents(std::string_view s);
    static std::string to_lower_no_accents(std::string_view s);

private:
    std::unordered_map<std::string, std::vector<LexiconId>> map_;
    static const std::vector<LexiconId> empty_;
};

} // namespace pando
