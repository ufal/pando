#pragma once

// Counting for `coll` (window collocations) and `dcoll` (dependency collocations),
// shared by the CLI and the program API (P7.8). Dense counters indexed by lexicon
// id instead of a hash map per hit, no per-hit allocations, and for `dcoll` on many
// nodes the children are found from the child side (one pass over the candidates'
// heads) instead of rebuilding a sentence's children map per node.

#include "corpus/corpus.h"
#include "query/executor.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pando {

struct CollCounts {
    /// (collocate id, observed count), every id with a count > 0
    std::vector<std::pair<LexiconId, size_t>> items;
    /// all counted positions (window positions / related tokens)
    size_t total = 0;
    /// `dcoll … by attr, attr2`: per (collocate id, attr2 id), how often the related
    /// tokens counted for that collocate had that attr2 value; key = id << 32 | attr2 id
    std::vector<std::pair<uint64_t, size_t>> breakdown;
};

/// `CollCounts::breakdown` per collocate id: (attr2 id, count), most frequent first.
std::unordered_map<LexiconId, std::vector<std::pair<LexiconId, size_t>>>
breakdown_by_collocate(const CollCounts& cc);

/// `coll`: the tokens in a window around each hit.
class CollCounter {
public:
    CollCounter(const PositionalAttr& pa, int left, int right, size_t corpus_size);

    /// A hit spanning [first, last]: the `left` tokens before it and the `right`
    /// tokens after it. Matched positions outside the span (dep_subtree bindings)
    /// can be excluded with `excl`.
    void add_envelope(CorpusPos first, CorpusPos last, const std::vector<CorpusPos>* excl = nullptr);

    /// A window around one token of a hit (`coll on label`), skipping the hit's
    /// own matched positions `excl` (any order).
    void add_hub(CorpusPos hub, const std::vector<CorpusPos>& excl);

    CollCounts finish() const;

private:
    void count(CorpusPos p);
    const PositionalAttr& pa_;
    CorpusPos left_, right_, n_;
    LexiconId empty_id_;
    std::vector<uint32_t> counts_;
    size_t total_ = 0;
};

/// `dcoll`: dependency relatives of each node token. `relations` as in the
/// command: empty = all children; "head", "children", "descendants", else a
/// deprel of the children (several may be given; each adds its own counts).
class DcollCounter {
public:
    DcollCounter(const Corpus& corpus, const PositionalAttr& pa,
                 const std::vector<std::string>& relations);

    void add_node(CorpusPos node) { nodes_.push_back(node); }
    size_t nodes() const { return nodes_.size(); }

    /// `dcoll … by attr, attr2`: also tally attr2 of every counted token (the
    /// relation a collocate comes in, its upos, …); nullptr = none.
    void set_breakdown(const PositionalAttr* attr) { bd_ = attr; }

    /// Does the counting (the nodes are buffered, so the children can be found
    /// from the child side when there are many).
    CollCounts finish();

private:
    void count(CorpusPos p, uint32_t times = 1);
    CollCounts done() const;
    const PositionalAttr* bd_ = nullptr;
    std::unordered_map<uint64_t, size_t> bd_counts_;
    const Corpus& corpus_;
    const PositionalAttr& pa_;
    bool want_head_ = false, want_descendants_ = false, want_all_children_ = false;
    std::vector<LexiconId> deprel_ids_;   // children with these deprels
    bool deprel_filter_ = false;          // a deprel was asked for (even if unknown)
    std::vector<CorpusPos> nodes_;
    std::vector<uint32_t> counts_;
    size_t total_ = 0;
};

/// P7.8: `coll` straight from the executor (QueryExecutor::set_hit_sink).
/// `hub` = the token index of `coll on label` (-1: the hit's envelope; -2: the
/// label is not in the query, every hit is skipped as with resolve_name).
class CollHitSink : public HitSink {
public:
    CollHitSink(CollCounter& c, int hub) : c_(c), hub_(hub) {}
    void hit(const CorpusPos* starts, const CorpusPos* ends, size_t n) override;
private:
    CollCounter& c_;
    int hub_;
    std::vector<CorpusPos> excl_;
};

/// P7.8: `dcoll` straight from the executor; `anchor` = token index of the
/// anchor label (-1: the hit's first position, as when the label is unknown).
class DcollHitSink : public HitSink {
public:
    DcollHitSink(DcollCounter& c, int anchor) : c_(c), anchor_(anchor) {}
    void hit(const CorpusPos* starts, const CorpusPos* ends, size_t n) override;
private:
    DcollCounter& c_;
    int anchor_;
};

} // namespace pando
