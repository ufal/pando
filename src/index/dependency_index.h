#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/structural_attr.h"
#include <string>
#include <vector>

namespace manatree {

// Sentence-local dependency index.
//
// All dependency data is stored as int16 sentence-local values:
//   - head: sentence-local index of syntactic head (-1 = root)
//   - euler_in/out: sentence-local DFS timestamps
//
// Children are derived on the fly by scanning the sentence (~50 int16
// comparisons) — no separate children index needed.
//
// Storage per token: 6 bytes (3 × int16).  For a 5B-token corpus this
// is 30 GB total, down from ~200 GB with absolute int64 positions.
//
// EX-2p: A per-sentence children cache avoids redundant O(sentence_length)
// scans when multiple seeds in the same sentence request children().
class DependencyIndex {
public:
    void open(const std::string& dir, const StructuralAttr& sentences, bool preload = false);

    // Absolute corpus position of the head.  Returns NO_HEAD for root.
    CorpusPos head(CorpusPos pos) const;

    // Children of pos.  First call in a sentence builds and caches the
    // full children map (O(sentence_length)); subsequent calls in the
    // same sentence are O(1) amortized via cache lookup.
    std::vector<CorpusPos> children(CorpusPos pos) const;

    // Count-only variant: no vector allocation, just returns the count.
    size_t children_count(CorpusPos pos) const;

    // All descendants of pos.  Uses the children cache internally.
    // Builds cache + DFS — O(sentence_length).
    std::vector<CorpusPos> subtree(CorpusPos pos) const;

    // All ancestors of pos.  Walks the head chain — O(depth), typ. <15.
    std::vector<CorpusPos> ancestors(CorpusPos pos) const;

    // Count-only variant: walks head chain counting, no allocation.
    size_t depth(CorpusPos pos) const;

    int16_t euler_in(CorpusPos pos) const;
    int16_t euler_out(CorpusPos pos) const;

    // O(1) ancestor check: same-sentence guard + int16 Euler range test.
    bool is_ancestor(CorpusPos ancestor, CorpusPos descendant) const;

    // Invalidate the children cache (e.g. between query executions).
    void clear_children_cache() const { cached_sentence_id_ = -1; }

private:
    const StructuralAttr* sentences_ = nullptr;
    MmapFile head_file_;       // int16[corpus_size]
    MmapFile euler_in_file_;   // int16[corpus_size]
    MmapFile euler_out_file_;  // int16[corpus_size]

    // EX-2p: Sentence-local children cache.
    // Mutable because children() is logically const but populates the cache.
    // Single-entry cache: stores the children map for the most recently
    // queried sentence.  In typical usage (iterating seeds within a sentence),
    // this turns N × O(sent_len) scans into 1 × O(sent_len) + N × O(children).
    mutable int64_t cached_sentence_id_ = -1;
    mutable Region  cached_sentence_;
    mutable std::vector<std::vector<int16_t>> cached_children_map_;  // [local_idx] → children local indices

    // Build (or reuse) the children map for the sentence containing pos.
    // Returns the sentence region and sets cached_children_map_.
    Region ensure_children_cache(CorpusPos pos) const;
};

} // namespace manatree
