#pragma once

// What a hit shows between its first and last token. Most hits lie in one stretch of text, shown whole. A hit whose
// tokens lie far apart (an English word and its translation over `tuid`, a coreference chain, any join across texts)
// would show everything in between, up to millions of tokens; it is shown as runs instead: each group of nearby
// matched tokens with its sentence, the runs separated by an elision. Nearby: in the same sentence, or (without
// sentences, or across a sentence boundary) at most kDisplayGap tokens apart.

#include "core/types.h"
#include "corpus/corpus.h"

#include <algorithm>
#include <utility>
#include <vector>

namespace pando {

constexpr CorpusPos kDisplayGap = 50;

// The runs (inclusive position ranges, in corpus order) to show for the matched positions `hit` (sorted, unique),
// within [lo, hi] (the hit's context: the first run starts at lo, the last ends at hi). One run [lo, hi] when the
// tokens are nearby.
inline std::vector<std::pair<CorpusPos, CorpusPos>> display_runs(const Corpus& corpus,
                                                                 const std::vector<CorpusPos>& hit,
                                                                 CorpusPos lo, CorpusPos hi) {
    std::vector<std::pair<CorpusPos, CorpusPos>> runs;
    if (hit.empty()) return runs;
    const StructuralAttr* s = corpus.has_structure("s") ? &corpus.structure("s") : nullptr;
    auto sentence = [&](CorpusPos p) { return s ? s->find_region(p) : int64_t{-1}; };
    // groups of nearby matched tokens
    std::vector<std::pair<CorpusPos, CorpusPos>> groups{{hit.front(), hit.front()}};
    int64_t group_s = sentence(hit.front());
    for (size_t i = 1; i < hit.size(); ++i) {
        const CorpusPos p = hit[i];
        const int64_t ps = sentence(p);
        if (p - groups.back().second <= kDisplayGap || (ps >= 0 && ps == group_s)) {
            groups.back().second = p;
        } else {
            groups.emplace_back(p, p);
            group_s = ps;
        }
    }
    if (groups.size() == 1) return {{lo, hi}};
    // each group with its sentence (at most kDisplayGap tokens either side); the outer ends at lo and hi
    for (size_t g = 0; g < groups.size(); ++g) {
        CorpusPos a = groups[g].first, b = groups[g].second;
        const int64_t sa = sentence(a), sb = sentence(b);
        a = g == 0 ? lo : std::max(sa >= 0 ? s->get(static_cast<size_t>(sa)).start : a, a - kDisplayGap);
        b = g + 1 == groups.size() ? hi : std::min(sb >= 0 ? s->get(static_cast<size_t>(sb)).end : b, b + kDisplayGap);
        if (!runs.empty() && a <= runs.back().second + 1) runs.back().second = std::max(runs.back().second, b);
        else runs.emplace_back(a, b);
    }
    return runs;
}

}  // namespace pando
