#pragma once

// Pointer layers derived from the basic tree (dev/PROPOSAL-RELATION-LAYERS.md, "Derived layers"), stored as
// `inherit=head` layers (relation_layer.h) and declared `source=derived:<rule>`. Two rules:
//
// effective-head (`eff`): the head a token would have without coordination; it replaces the `conj` edge.
//   1. a conjunct gets its first conjunct's head and relation instead of `conj` ("John and Mary love to walk and
//      talk": Mary nsubj of love, talk xcomp of love), up a chain of nested coordinations; a conjunct of the root
//      becomes a root;
//   2. a predicate conjunct (VERB, ADJ, AUX) without a subject of its own also gets the subjects of the nearest
//      conjunct up the chain that has them, and their conjuncts ("the dog sleeps and snores": dog nsubj of snores).
//      These are added edges: a shared subject has several effective heads.
//
// conj-propagation (`deps` for a corpus without DEPS): enhanced UD's conjunct propagation (Schuster & Manning 2016),
//   which adds edges and keeps `conj`, as UD treebanks with DEPS do: rule 1 as an added edge, not for conjuncts of the
//   root, not inside a nested coordination, not for loose links and function words (parataxis, list, aux, cop, …);
//   rule 2 from the direct first conjunct only. Labels stay the basic ones (no `conj:and`, `obl:in`); relative
//   pronouns (`ref`), controlled subjects (`xsubj`) and empty nodes are left to annotated DEPS.

#include "index/relation_decl.h"

#include <string>

namespace pando {

class Corpus;

enum class DeriveRule { EffectiveHead, ConjPropagation };

// "effective-head" / "conj-propagation"; nullopt-like false when unknown
bool parse_derive_rule(const std::string& s, DeriveRule& out);
const char* derive_rule_name(DeriveRule r);

struct DerivedDepsStats {
    size_t tokens = 0;           // tokens in sentences
    size_t deviating = 0;        // tokens whose edges differ from the basic one
    size_t conj_heads = 0;       // conjuncts given their first conjunct's head (rule 1)
    size_t shared_subjects = 0;  // subject edges added (rule 2)
};

// Writes rel.NAME.* into `dir` (the corpus's own folder) from the corpus's basic tree, `deprel` and `upos` (rule 2
// needs upos); returns the declaration to record. Throws when the corpus has no dependencies or sentences.
RelationDecl derive_deps_layer(const Corpus& corpus, const std::string& dir, const std::string& name, DeriveRule rule,
                               DerivedDepsStats* stats = nullptr);

}  // namespace pando
