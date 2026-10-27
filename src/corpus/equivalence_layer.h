#pragma once

// Equivalence layers (dev/PROPOSAL-RELATION-LAYERS.md): classes of tokens named by the values of an attribute
// (`tuid` across translations, `coref` entities, `err_gid` groups), queried with `[A] =:X [B]`. Nothing is stored
// besides the attribute and its reverse index; the declaration records what the classes are measured to be, which
// decides the join a query can use (within a sentence, a text, or across the corpus).
//
//   relation.coref=equivalence,attr=coref,scope=text,span=212,endpoints=token,source=annotated

#include "index/relation_decl.h"

#include <cstddef>

namespace pando {

class Corpus;

struct EquivalenceStats {
    size_t classes = 0;    // distinct values
    size_t members = 0;    // (token, class) memberships
    size_t singletons = 0; // classes of one token
    size_t largest = 0;    // members of the largest class
};

// `decl` as declared (kind equivalence, attr); returns it with scope (the smallest of s / text / corpus that holds
// every class) and span (the largest distance between a class's first and last member) measured. Throws when the
// attribute is missing.
RelationDecl measure_equivalence(const Corpus& corpus, RelationDecl decl, EquivalenceStats* stats = nullptr);

}  // namespace pando
