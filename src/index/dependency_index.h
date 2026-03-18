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
class DependencyIndex {
public:
    void open(const std::string& dir, const StructuralAttr& sentences, bool preload = false);

    // Absolute corpus position of the head.  Returns NO_HEAD for root.
    CorpusPos head(CorpusPos pos) const;

    // Children of pos.  Scans the sentence — O(sentence_length), typ. ~50.
    std::vector<CorpusPos> children(CorpusPos pos) const;

    // All descendants of pos.  Scans the sentence once to build a local
    // tree, then DFS — O(sentence_length).
    std::vector<CorpusPos> subtree(CorpusPos pos) const;

    // All ancestors of pos.  Walks the head chain — O(depth), typ. <15.
    std::vector<CorpusPos> ancestors(CorpusPos pos) const;

    int16_t euler_in(CorpusPos pos) const;
    int16_t euler_out(CorpusPos pos) const;

    // O(1) ancestor check: same-sentence guard + int16 Euler range test.
    bool is_ancestor(CorpusPos ancestor, CorpusPos descendant) const;

private:
    const StructuralAttr* sentences_ = nullptr;
    MmapFile head_file_;       // int16[corpus_size]
    MmapFile euler_in_file_;   // int16[corpus_size]
    MmapFile euler_out_file_;  // int16[corpus_size]
};

} // namespace manatree
