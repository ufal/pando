#pragma once

// P7.1 / P7.7: grouped counts and sorting shared by the CLI (`pando`) and the
// program API (`/run`, FFI) — `count by`, `group by`, `freq by`, `sort by`.

#include "corpus/corpus.h"
#include "query/executor.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace pando {

/// Order of group keys (tab-separated fields): field by field, numeric (or Roman
/// numeral) values numerically, otherwise bytewise.
bool compare_group_keys(std::string_view a, std::string_view b);

/// The tab-separated key of one match for `fields` (same text as tabulate / decode).
std::string make_group_key(const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
                           const std::vector<std::string>& fields);

struct GroupRows {
    /// Sorted by count (descending), then key (compare_group_keys).
    std::vector<std::pair<std::string, size_t>> rows;
    size_t groups = 0;   // distinct groups in the whole result (not only `rows`)
    size_t total = 0;    // hits counted
};

/// Group `ms` by `fields`. Only the first `limit` rows are built (0 = all): with
/// streamed buckets (`ms.aggregate_buckets`) the counts stay on their id keys and
/// only the rows that are shown (plus the ties at the cut) are decoded to strings,
/// instead of decoding, mapping and sorting every group. `explode_mv`: a single
/// multivalue field whose `a|b` values count for `a` and for `b`.
GroupRows group_rows(const Corpus& corpus, const MatchSet& ms, const std::vector<std::string>& fields,
                     const NameIndexMap& name_map, size_t limit, bool explode_mv);

/// `sort by fields`: one key per match, computed once (not twice per comparison);
/// stable, so equal keys keep their corpus order. `group_key_order`: compare with
/// compare_group_keys (numbers numerically, /run); false = bytewise (CLI).
void sort_matches_by_key(const Corpus& corpus, std::vector<Match>& matches,
                         const NameIndexMap& name_map, const std::vector<std::string>& fields,
                         bool group_key_order = true);

}  // namespace pando
