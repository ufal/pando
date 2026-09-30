#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include <string>
#include <string_view>
#include <vector>

namespace pando {

// Read-only lexicon backed by mmap'd .lex and .lex.idx files.
// Supports O(log V) string→ID lookup via binary search on sorted strings.
class Lexicon {
public:
    void open(const std::string& base_path, bool preload = false);

    LexiconId lookup(std::string_view value) const;
    std::string_view get(LexiconId id) const;
    LexiconId size() const;

    /// Sorted ids of the entries that contain byte `c` (one memchr pass over the
    /// string data; e.g. the `|`-joined values that multivalue matching splits).
    std::vector<LexiconId> ids_containing(char c) const;

private:
    MmapFile strings_;       // .lex
    MmapFile offsets_;       // .lex.idx  (int64 byte offsets, size = lex_size+1)
};

} // namespace pando
