#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/structural_attr.h"
#include <string>
#include <vector>

namespace pando {

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

    /// P1.8: optional `dep.head_rel` (int16 per token: head - pos, 0 = root / none).
    /// nullptr when the index predates it (see `pando-index --upgrade`).
    /// With it, head(pos) = pos + rel[pos] — no sentence lookup at all.
    const int16_t* head_rel_data() const {
        return head_rel_file_.valid() ? head_rel_file_.as<int16_t>() : nullptr;
    }
    /// Raw sentence-local Euler tour times (int16 per token).
    const int16_t* euler_in_data() const { return euler_in_file_.as<int16_t>(); }
    const int16_t* euler_out_data() const { return euler_out_file_.as<int16_t>(); }
    /// Raw sentence-local heads (int16 per token, -1 = root).
    const int16_t* head_local_data() const { return head_file_.as<int16_t>(); }
    size_t token_count() const { return head_file_.size() / sizeof(int16_t); }

    /// Write `<dir>/dep.head_rel` derived from `dep.head` + sentence regions
    /// (for indexes built before P1.8). Returns false and sets *err on failure.
    static bool write_head_rel_file(const std::string& dir, const StructuralAttr& sentences,
                                    std::string* err);

    /// Like `head`, but reuses `sentence_hint` via `find_region_from` so scanning
    /// sorted child positions is amortized O(1) per call (Manatee-style dep joins).
    CorpusPos head_from(CorpusPos pos, int64_t& sentence_hint) const;

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

    // Invalidate the children cache (e.g. between query executions). The cache
    // is per thread; this clears the calling thread's entry.
    void clear_children_cache() const;

private:
    const StructuralAttr* sentences_ = nullptr;
    MmapFile head_file_;       // int16[corpus_size]
    MmapFile head_rel_file_;   // int16[corpus_size], optional (P1.8)
    MmapFile euler_in_file_;   // int16[corpus_size]
    MmapFile euler_out_file_;  // int16[corpus_size]

    // EX-2p: Sentence-local children cache. Single entry (the most recently
    // queried sentence): iterating seeds within a sentence turns N × O(sent_len)
    // scans into 1 × O(sent_len) + N × O(children). The index is shared by every
    // query on the corpus (pando-server / ServerApi run queries concurrently), so
    // the entry lives in thread-local storage, keyed by this index's generation
    // (unique per open(), so a reopened or reused object never sees stale data).
    struct ChildrenCache {
        uint64_t gen = 0;
        int64_t sentence_id = -1;
        Region sentence{0, -1};
        std::vector<std::vector<int16_t>> children;   // [local_idx] → children local indices
    };
    static uint64_t next_cache_gen();
    static ChildrenCache& tl_children_cache();
    uint64_t cache_gen_ = next_cache_gen();

    // Build (or reuse) the calling thread's children map for the sentence
    // containing pos; `sent` = the sentence ({0, -1} when pos is in none).
    const ChildrenCache& ensure_children_cache(CorpusPos pos) const;
};

} // namespace pando
