#pragma once

// CorefUD coreference in CoNLL-U (MISC `Entity=`, CorefUD 1.x): mentions as brackets over tokens,
// `Entity=(e6353-person-2(e6352-person-1)` opens two mentions, `Entity=e6353)` closes one. Each opening names the
// entity and the mention's head, as an offset within the mention (field order from `# global.Entity =
// eid-etype-head-other`). The builder gives every mention head the entity id as the token attribute `coref`
// (multivalue: a token can head several mentions), declared as the equivalence layer `coref`
// (equivalence_layer.h): `[A] =:coref [B]` finds mention heads of the same entity.

#include "core/types.h"

#include <map>
#include <string>
#include <string_view>

namespace pando {

class CorefEntityReader {
public:
    // `# global.Entity = eid-etype-head-other`: the field order (eid and head are required)
    void set_format(std::string_view global_entity);

    // The token at `pos` with its MISC column: records the mentions it opens; returns the entity ids of the
    // mentions this token heads, joined by '|' ("" when none). Tokens must come in corpus order.
    std::string on_token(CorpusPos pos, std::string_view misc);

    bool seen() const { return seen_; }
    size_t mentions() const { return mentions_; }

private:
    int eid_field_ = 0, head_field_ = 2;
    bool seen_ = false;
    size_t mentions_ = 0;
    std::map<CorpusPos, std::string> heads_;   // future head positions → entity ids
};

}  // namespace pando
