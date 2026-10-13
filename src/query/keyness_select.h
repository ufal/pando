#pragma once
// Which keyness rows to show (CLI and server). Rows are sorted by signed keyness,
// highest first: words over-used in the focus at the top (positive), words typical of
// the reference at the bottom (negative, most negative last). A plain "first N" cut
// kept only positive rows whenever there were more than N of them, so the reference
// side never appeared (a volcano plot showed only its right half).
//
// keyness_rows_to_show() keeps up to half of the limit from each side — space one side
// does not need goes to the other — in the same order: the strongest positive rows,
// then the strongest negative rows ending with the most negative one.

#include <algorithm>
#include <cstddef>
#include <vector>

namespace pando {

/// Indices into `sorted` (descending by score(row)) of the rows to show, at most `limit`.
template <typename Row, typename Score>
std::vector<size_t> keyness_rows_to_show(const std::vector<Row>& sorted, size_t limit, Score score) {
    const size_t n = sorted.size();
    size_t npos = 0;
    while (npos < n && score(sorted[npos]) >= 0) ++npos;   // positives come first
    const size_t nneg = n - npos;
    size_t take_neg = std::min(nneg, limit / 2);
    const size_t take_pos = std::min(npos, limit - take_neg);
    take_neg = std::min(nneg, limit - take_pos);            // positives left room over
    std::vector<size_t> out;
    out.reserve(take_pos + take_neg);
    for (size_t i = 0; i < take_pos; ++i) out.push_back(i);
    for (size_t i = n - take_neg; i < n; ++i) out.push_back(i);
    return out;
}

}  // namespace pando
