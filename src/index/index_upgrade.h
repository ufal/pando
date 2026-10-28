#pragma once

// What an index gets after it is built (and on `pando-index --upgrade`, without re-indexing): files derived from the
// indexed data that newer versions use when present, and the measured relation declarations. pando-index and the
// builder API (PandoIndexBuilder::finalize) both run it, so an index is the same whichever way it was built.
//   dep.head_rel                 (P1.8)  relative head offsets
//   <attr>.fold_{lc,na,lcna}.v2.perm (P1.6)  %c / %d / %cd lookups (UTF-8 fold)
//   <attr>.bm{,.idx}             (P3.1)  chunked bitmaps, low-cardinality attrs

#include "index/relation_decl.h"

#include <string>
#include <vector>

namespace pando {

// P5.2 default: upos × upos edge postings (when the corpus has deps and upos).
inline constexpr const char* kDefaultDepPairs = "upos:upos";
// P3.1 default: bitmaps for every single-valued attribute with at most
// BitmapIndex::kAutoMaxValues values (upos, deprel, …).
inline constexpr const char* kDefaultBitmaps = "auto";
// P5.6 default: head attributes for upos, deprel and lemma (those the corpus has)
// when it has a dependency index; `--head-attrs none` opts out.
inline constexpr const char* kDefaultHeadAttrs = "auto";
inline constexpr const char* kAutoHeadAttrs[] = {"upos", "deprel", "lemma"};

std::vector<std::string> split_list(const std::string& s);

// The derived files (dep.head_rel / head_rel8, relation.head, head attributes, fold indexes, bitmaps, packed
// postings, edge postings) and `upgraded_with=` in corpus.info. 0 = done, 1 = an error (reported on stderr).
int upgrade_index(const std::string& dir, bool quiet = false, const std::string& dep_pairs = kDefaultDepPairs,
                  const std::string& bitmaps = kDefaultBitmaps, const std::string& packed = "none",
                  bool drop_rev = false, const std::string& head_attrs = kDefaultHeadAttrs, bool compact_deps = false,
                  const std::string& packed_dat = "none", bool drop_dat = false);

// Record `relation.<name>=…` in corpus.info (replacing an earlier line for that relation).
bool record_relation(const std::string& dir, const RelationDecl& r);

// Equivalence layers declared without a measurement: their scope and span measured from the data and recorded.
int measure_declared_equivalences(const std::string& dir, bool quiet);

}  // namespace pando
