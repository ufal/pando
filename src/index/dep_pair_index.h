#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include "index/positional_attr.h"
#include <string>

namespace pando {

class Corpus;

// P5.2: dependency edge postings for a pair of low-cardinality attributes.
//
// `<dir>/dep.pair.<H>.<C>.rev` holds, for every non-root token c, the position
// c under the key  id_H(head(c)) * |C| + id_C(c)  (positions ascending within a
// key); `.rev.idx` holds the int64 key offsets (|H|·|C| + 1 entries). So
// `[upos="VERB"] > [upos="NOUN"]` is one key: the exact total is a `.rev.idx`
// difference and the first page a direct read (+ head() per shown hit), at any
// corpus size; region filters slice the postings per interval.
//
// Storage: one position per non-root token (the same width as the corpus
// `.rev` files) — about one extra `.rev`.
class DepPairIndex {
public:
    /// Largest |H|·|C| accepted (keeps .rev.idx small; upos × upos = 17 × 17).
    static constexpr int64_t kMaxKeys = int64_t{1} << 20;

    static std::string base_path(const std::string& dir, const std::string& head_attr,
                                 const std::string& child_attr);

    /// Build the files for (head_attr, child_attr). Needs dep.head_rel.
    /// Returns false and sets *err on failure (e.g. too many keys).
    static bool build(const Corpus& corpus, const std::string& head_attr,
                      const std::string& child_attr, std::string* err);

    /// Open existing files; false when absent or inconsistent with the corpus.
    bool open(const Corpus& corpus, const std::string& head_attr, const std::string& child_attr);

    bool valid() const { return rev_idx_.valid(); }
    /// Child positions whose head has id `h` (in H) and which have id `c` (in C).
    RevSpan span(LexiconId h, LexiconId c) const;
    size_t count(LexiconId h, LexiconId c) const;

private:
    MmapFile rev_;
    MmapFile rev_idx_;
    int width_ = 8;
    int64_t vh_ = 0, vc_ = 0;
};

} // namespace pando
