#pragma once

// Relation layers (dev/PROPOSAL-RELATION-LAYERS.md): the declared properties of a relation between units of the
// corpus, as attributes have theirs (multivalue, kv_pipe) and regions theirs (nested, overlapping). One line per
// relation in corpus.info:
//
//   relation.head=pointer,functional,tree,acyclic,label=deprel,scope=s,span=37,endpoints=token,source=annotated
//
// The properties decide which query forms apply and which fast path computes them; `span` is measured by the
// indexer (the largest link distance in tokens), the others are declared, or measured for the basic tree.

#include "core/types.h"

#include <optional>
#include <string>
#include <vector>

namespace pando {

class DependencyIndex;
class StructuralAttr;

struct RelationDecl {
    enum class Kind { Pointer, Equivalence };

    std::string name;                 // "head", "deps", "coref", …
    Kind kind = Kind::Pointer;        // pointer: directed; equivalence: symmetric classes
    bool functional = false;          // at most one target per source
    bool tree = false;                // functional, acyclic, one root per scope unit
    bool acyclic = false;             // no cycles (a tree is acyclic)
    std::string label;                // the label: an attribute of the target ("deprel"), "edge", or "" (none)
    std::string scope = "s";          // s | text | corpus | external: the bound no link crosses
    int64_t span = -1;                // the largest link distance in tokens (measured); -1 = unknown
    std::string endpoints = "token";  // token | a region type
    std::string source = "annotated"; // annotated | derived:<rule>
    std::string inherit;              // a layer this one deviates from ("head"): only the deviations are stored
    bool materialized = false;        // stored in full although it inherits (speed over disk)

    // "pointer,functional,…" as written after `relation.<name>=`
    std::string format() const;
};

// The value of a `relation.<name>=` line; nullopt (and *err) when it does not parse.
std::optional<RelationDecl> parse_relation(const std::string& name, const std::string& value, std::string* err = nullptr);

// The basic dependency tree as a relation, measured from the index: functional by construction; span the largest
// |head - token|; tree and acyclic only when every sentence with dependencies has one root and no cycle (an index
// whose data is not a tree is described as it is, never refused: the basic tree predates the declarations).
// `problems` (optional) receives up to a few sentences that are not trees, for a message.
RelationDecl measure_head_relation(const DependencyIndex& deps, const StructuralAttr& sentences,
                                   std::vector<std::string>* problems = nullptr);

}  // namespace pando
