#pragma once

// The totals a `dcoll` score compares an observed (node, relation, collocate) count with.
//
// An association score needs two totals in the same space as the observed count: how often the node has any
// relative of this kind (DcollCounter's total), and how often the collocate is a relative of this kind, out of N
// such pairs. For a window collocation that space is token positions, so the collocate's corpus frequency and the
// corpus size are right; for `dcoll` it is the dependency pairs the command asks for (children with a deprel, all
// children, the head, descendants; over the basic tree or a relation layer).
//
//   relation (the default): the collocate's frequency as such a relative, N = all such pairs in the corpus
//   corpus:                 the collocate's corpus frequency, N = the corpus size (pando before 0.4.5)
//
// pando-index stores the relation totals of the basic tree per attribute (index/dcoll_totals_file.h); without that
// file (an older index) and over relation layers they are counted in a pass over the corpus per kind of pair and
// attribute, kept with the attribute (PositionalAttr::cache), so a server pays for them once.

#include "core/types.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace pando {

class Corpus;
class PositionalAttr;
class RelationLayer;

enum class DcollBaseline { Relation, Corpus };

/// "relation" / "corpus"; false for anything else
bool parse_dcoll_baseline(std::string_view s, DcollBaseline& out);
const char* dcoll_baseline_name(DcollBaseline b);

/// One kind of pair: how often a token with an id is the relative; total() = all pairs.
class PairCounts {
public:
    virtual ~PairCounts() = default;
    virtual uint64_t of(LexiconId id) const = 0;
    virtual uint64_t total() const = 0;
};

/// Counted at query time.
struct PairTotals final : PairCounts {
    std::vector<uint64_t> by_id;
    uint64_t sum = 0;
    uint64_t of(LexiconId id) const override {
        return id >= 0 && static_cast<size_t>(id) < by_id.size() ? by_id[static_cast<size_t>(id)] : 0;
    }
    uint64_t total() const override { return sum; }
};

/// The kinds of pair a `dcoll` command counts (several add up, as their observed counts do).
struct DcollPairKinds {
    bool head = false, descendants = false, children = false;
    std::vector<std::string> deprels;   // children with these relations (labels)
    const RelationLayer* layer = nullptr;   // nullptr = the basic tree
    std::string layer_name;
};

class DcollTotals {
public:
    static DcollTotals corpus(const PositionalAttr& pa, size_t corpus_size);
    static DcollTotals relation(const Corpus& corpus, const std::string& attr, const DcollPairKinds& kinds);
    /// Totals counted elsewhere (EF-12: inside a subcorpus): of(id) = part->of(id), N = part->total().
    static DcollTotals counted(DcollBaseline b, std::shared_ptr<const PairCounts> part);

    DcollBaseline baseline() const { return baseline_; }
    /// relation totals read from the index (index/dcoll_totals_file.h), not counted for this query
    bool stored() const { return stored_; }
    /// how often `id` is a relative (relation) or occurs (corpus)
    uint64_t of(LexiconId id) const;
    /// N: all pairs (relation) or the corpus size (corpus)
    uint64_t total() const { return total_; }

private:
    DcollBaseline baseline_ = DcollBaseline::Relation;
    const PositionalAttr* pa_ = nullptr;
    std::vector<std::shared_ptr<const PairCounts>> parts_;
    uint64_t total_ = 0;
    bool stored_ = false;
};

}  // namespace pando
