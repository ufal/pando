// keyness_rows_to_show (keyness_select.h): both sides of a keyness list.
#undef NDEBUG   // the asserts are the test, also in Release builds
#include "query/keyness_select.h"

#include <cassert>
#include <cstdio>
#include <vector>

using pando::keyness_rows_to_show;

static std::vector<double> scores(int pos, int neg) {   // descending, as the callers sort
    std::vector<double> v;
    for (int i = pos; i >= 1; --i) v.push_back(i);
    for (int i = 1; i <= neg; ++i) v.push_back(-i);
    return v;
}

static std::vector<double> pick(const std::vector<double>& v, size_t limit) {
    std::vector<double> out;
    for (size_t i : keyness_rows_to_show(v, limit, [](double x) { return x; })) out.push_back(v[i]);
    return out;
}

int main() {
    using V = std::vector<double>;
    assert((pick(scores(10, 10), 6) == V{10, 9, 8, -8, -9, -10}));   // half each, strongest, order kept
    assert((pick(scores(10, 10), 5) == V{10, 9, 8, -9, -10}));       // odd: the extra one to the focus
    assert((pick(scores(2, 10), 6) == V{2, 1, -7, -8, -9, -10}));    // few positives: room to the reference
    assert((pick(scores(10, 1), 6) == V{10, 9, 8, 7, 6, -1}));       // few negatives: room to the focus
    assert((pick(scores(10, 0), 4) == V{10, 9, 8, 7}));              // one-sided list: as before
    assert((pick(scores(0, 5), 3) == V{-3, -4, -5}));
    assert((pick(scores(3, 3), 50) == V{3, 2, 1, -1, -2, -3}));      // everything fits
    assert((pick(scores(4, 4), 1) == V{4}));
    assert((pick(scores(0, 0), 5).empty()));
    std::puts("keyness_select: ok");
    return 0;
}
