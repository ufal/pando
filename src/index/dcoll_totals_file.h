#pragma once

// `<attr>.dcoll`: the totals `dcoll` scores compare with (api/dcoll_totals.h), counted once when the index is built
// (index_upgrade.h) instead of in a pass over the corpus per query. Per value of the attribute, over the basic tree:
//
//   children     how often a token with the value has a head
//   head         how often a token with the value is a head (once per dependent; a token that is its own head not)
//   descendants  the number of ancestors, summed over the value's tokens
//   deprel table how often a token with the value has a head and that deprel, per deprel
//
// and each kind's total (N). The file is a derived file: rebuilt by `pando-index --upgrade` when stale (an attribute,
// the deprels or the heads changed after it), and not used when it does not match the corpus (then the query counts
// the totals itself). Relation layers (`dcoll (via L)`) are counted at query time.
//
// Layout (little-endian): the header (uint64 each: magic, version, tokens, values V, deprels D, the children / head
// / descendants totals), children[V], head[V], descendants[V], deprel_total[D], deprel_start[D + 1] (entry index),
// then the entries {uint32 value, uint32 0, uint64 count} sorted by deprel, then value.

#include "core/mmap_file.h"
#include "core/types.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace pando {

class Corpus;
class DependencyIndex;

/// f(c, ancestors of c) for every token, in corpus order; a head chain that loops (a malformed tree) counts no
/// ancestors beyond the loop. Shared by the file builder and the query-time count.
void for_each_ancestor_count(const DependencyIndex& deps, CorpusPos n,
                             const std::function<void(CorpusPos, uint32_t)>& f);

class DcollTotalsFile {
public:
    struct Entry {
        uint32_t value;
        uint32_t pad;
        uint64_t count;
    };

    static std::string path_for(const Corpus& corpus, const std::string& attr);
    /// Writes the files of `attrs` (single-valued positional attributes) in one pass over the corpus.
    static bool build(const Corpus& corpus, const std::vector<std::string>& attrs, std::string* err);
    /// The file exists, matches the corpus and is newer than what it was counted from.
    static bool up_to_date(const Corpus& corpus, const std::string& attr);

    /// The file of `attr`, or nullptr when there is none or it does not match the corpus.
    static std::shared_ptr<const DcollTotalsFile> open(const Corpus& corpus, const std::string& attr);

    const uint64_t* children() const { return arr(0); }
    const uint64_t* heads() const { return arr(1); }
    const uint64_t* descendants() const { return arr(2); }
    uint64_t children_total() const { return hdr()[5]; }
    uint64_t heads_total() const { return hdr()[6]; }
    uint64_t descendants_total() const { return hdr()[7]; }
    size_t values() const { return static_cast<size_t>(hdr()[3]); }
    size_t deprels() const { return static_cast<size_t>(hdr()[4]); }
    uint64_t deprel_total(LexiconId d) const;
    /// how often a token with `value` has a head and deprel `d`
    uint64_t deprel_count(LexiconId d, LexiconId value) const;

private:
    static constexpr size_t kHeader = 8;
    const uint64_t* hdr() const { return file_.as<uint64_t>(); }
    const uint64_t* arr(size_t i) const { return hdr() + kHeader + i * values(); }
    MmapFile file_;
};

}  // namespace pando
