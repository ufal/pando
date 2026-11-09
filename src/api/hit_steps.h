#pragma once

// In-place steps on a set of hits besides `sort`: `filter` (Manatee's positive / negative
// filter, KonText's p/n), `shuffle` and `sample`. The program API (HitSet) and the CLI
// apply them alike; each gives the indices of the hits to keep, in their new order.

#include "query/ast.h"
#include "query/executor.h"

#include <cstdint>
#include <utility>
#include <vector>

namespace pando {

/// The spans [first, last] of a filter query's hits, sorted by start (then end).
using FilterSpans = std::vector<std::pair<int64_t, int64_t>>;

FilterSpans filter_spans_of(const std::vector<Match>& hits);

/// `filter … by F on from..to [rank N] [excluding match]`: whether F has the matches the
/// command asks for in the window of hit `m` (before `not`). The window is [from, to]
/// (inclusive corpus positions, from the anchors as sort's `on` reads them; swapped when
/// to < from); a match of F counts when it lies wholly inside; `rank N` (N >= 1 or <= -1):
/// at least |N| such matches; `rank 0`: a match spanning exactly the window;
/// `excluding match`: matches overlapping the hit do not count.
bool filter_window_has(const Match& m, const NameIndexMap& nm, const GroupCommand& cmd, const FilterSpans& f);

/// The seed of a shuffle / sample step: the command's, else a fresh one (kept with the
/// step, so a replay gives the same order).
uint64_t step_seed(const GroupCommand& cmd);

/// 0..n-1 in a random order (Fisher–Yates over a 64-bit Mersenne twister: the same
/// order for the same seed on every platform).
std::vector<size_t> shuffled_order(size_t n, uint64_t seed);

/// `k` of 0..n-1 chosen at random, ascending (all when k >= n).
std::vector<size_t> sample_indices(size_t n, size_t k, uint64_t seed);

}  // namespace pando
