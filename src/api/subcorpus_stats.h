#pragma once

// EF-12: statistics over a subcorpus. "Statistics follow the words":
//   `use S`       the subcorpus is the corpus: N (ipm, corpus_size), the collocates' and
//                 keywords' frequencies, the per-group sizes of `freq by <region attribute>`
//                 are counted inside S; keyness compares with the rest of S;
//   `… within S`  a filter: statistics stay those of the whole corpus.
// In both, window collocates (and dependency relatives) outside the restriction do not count.

#include "api/dcoll_totals.h"
#include "subcorpus/subcorpus.h"

#include <cstdint>
#include <memory>
#include <string>

namespace pando {

class Corpus;
class PositionalAttr;

struct SubcorpusStats {
    AppliedSubcorpusPtr use;          // `use`: the corpus is this subcorpus
    ResolvedSubcorpusPtr inside;      // the restriction in effect (use ∩ within)
    AppliedSubcorpusPtr reported;     // what the answer reports as "subcorpus"
    /// The part of a result cache key that depends on it.
    std::string cache_key() const;
};
using SubcorpusStatsPtr = std::shared_ptr<const SubcorpusStats>;

/// N: the subcorpus's tokens under `use`, else the corpus's.
uint64_t stats_corpus_size(const Corpus& corpus, const SubcorpusStats* s);
/// Positions counted as collocates / relatives (nullptr: all).
const PositionSet* stats_inside(const SubcorpusStats* s);
/// Tokens of [a, b] in the corpus (the subcorpus under `use`).
uint64_t stats_span_tokens(const SubcorpusStats* s, int64_t a, int64_t b);

/// The frequency of an attribute's values in the corpus (the subcorpus under `use`).
class StatsFreqs {
public:
    StatsFreqs(const Corpus& corpus, const SubcorpusStats* s, const std::string& attr_name);
    uint64_t of(int32_t id) const;
private:
    const PositionalAttr& pa_;
    std::shared_ptr<const std::vector<uint64_t>> counts_;   // under `use`
};

/// `dcoll` under `use`: the totals its scores compare with, counted inside the subcorpus —
/// relation: every token of the subcorpus a node, its relatives inside (as the command
/// counts them: `relations`, `layer`); corpus: the subcorpus's frequencies and size.
/// Kept with the subcorpus. nullptr when not under `use`.
std::shared_ptr<const PairCounts> stats_dcoll_counts(const Corpus& corpus, const SubcorpusStats* s,
                                                     const std::string& attr, DcollBaseline baseline,
                                                     const std::vector<std::string>& relations,
                                                     const std::string& layer);

} // namespace pando
