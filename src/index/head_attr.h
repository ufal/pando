#pragma once

// Head attributes: for a positional attribute A, `head#A` is a positional
// attribute whose value at token c is A of c's dependency head (KonText / Manatee
// corpora often carry the same as `p_lemma`, `p_upos`, …). Tokens without a head
// (the root, tokens outside the dependency layer) get kNoHead. The head's
// conditions then become conditions on the dependent, where every one-token fast
// path applies: QueryExecutor reads `parent [P]` as head#P', and
// [upos="VERB"] > [deprel="nsubj"] as the one-token [deprel="nsubj" &
// head#upos="VERB"]. N head attributes cover every head × child combination
// (dep.pair.H.C needs one file per pair).
//
// Storage only: files `head#A.{dat,lex,lex.idx,rev,rev.idx}` (A's lexicon plus
// kNoHead), `head_attrs=A,…` in corpus.info (Corpus::head_attr_names(), not among
// attr_names()); the lexer refuses `head#…` in queries. Built by
// `pando-index --upgrade --head-attrs A[,B...]` before folds / bitmaps / packed
// postings, which then cover them like any attribute.

#include <string>

namespace pando {

class Corpus;

struct HeadAttr {
    static constexpr const char* kPrefix = "head#";
    static constexpr const char* kNoHead = "<root>";

    static std::string name_for(const std::string& attr) { return kPrefix + attr; }
    /// "head#lemma" → "lemma"; "" when `name` is not a head attribute name.
    static std::string source_of(const std::string& name);

    /// True when `head#attr` exists and is newer than its sources.
    static bool up_to_date(const Corpus& corpus, const std::string& attr);
    /// Write the files of `head#attr` (needs dep.head_rel) and list it in
    /// corpus.info. The corpus must be re-opened to see it.
    static bool build(const Corpus& corpus, const std::string& attr, std::string* err);
};

}  // namespace pando
