#pragma once

#include <cstdint>
#include <vector>
#include <string>

namespace manatree {

using CorpusPos  = int64_t;   // int64: supports corpora beyond 2.1B tokens
using LexiconId  = int32_t;   // int32: vocabularies never approach 2B entries
// Euler times are sentence-local int16 (max sentence ~16K tokens).
// No global EulerTime needed — dependency trees are always within a sentence.

static constexpr CorpusPos  NO_HEAD     = -1;
static constexpr LexiconId  UNKNOWN_LEX = -1;

struct Region {
    CorpusPos start;
    CorpusPos end;       // inclusive
};

// Split a composite region attribute name like "text_langcode" into its
// structure name ("text") and attribute name ("langcode").
// Returns false if the name has no underscore or the attr part is empty.
// Strips an optional "match." prefix first.
struct RegionAttrParts {
    std::string struct_name;
    std::string attr_name;
};

inline bool split_region_attr_name(const std::string& field, RegionAttrParts& out) {
    std::string fld = field;
    if (fld.size() > 6 && fld.compare(0, 6, "match.") == 0)
        fld = fld.substr(6);
    auto us = fld.find('_');
    if (us == std::string::npos || us + 1 >= fld.size())
        return false;
    out.struct_name = fld.substr(0, us);
    out.attr_name = fld.substr(us + 1);
    return true;
}

} // namespace manatree
