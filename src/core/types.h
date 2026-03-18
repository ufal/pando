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

} // namespace manatree
