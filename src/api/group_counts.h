#pragma once

// P7.1 / P7.7: grouped counts and sorting shared by the CLI (`pando`) and the
// program API (`/run`, FFI) — `count by`, `group by`, `freq by`, `sort by`.

#include "corpus/corpus.h"
#include "query/executor.h"

#include <ostream>
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

/// P6.6: `count by A, B, …` as a tree: the top `top_limit` values of A by count,
/// under each the top `child_limit` values of B within it, and so on (0 = all).
/// Built on the id keys of streamed buckets (only the returned values are decoded
/// to strings); order: count descending, then value (numbers numerically).
struct GroupNode {
    std::string value;
    size_t count = 0;
    size_t groups = 0;                 // distinct values of the next field under this one
    std::vector<GroupNode> children;   // the top child_limit of them
};
struct GroupTree {
    std::vector<GroupNode> top;
    size_t top_groups = 0;   // distinct values of the first field
    size_t groups = 0;       // distinct combinations of all fields
    size_t total = 0;        // hits counted
};
GroupTree group_tree(const Corpus& corpus, const MatchSet& ms, const std::vector<std::string>& fields,
                     const NameIndexMap& name_map, size_t top_limit, size_t child_limit);

/// The `"hierarchy": [...]` member of a multi-field count (JSON, no trailing comma):
/// {"field", "value", "count", "pct", ["pct_of_parent"], ["groups", "children": [...]]}.
void emit_group_tree_json(std::ostream& out, const std::vector<std::string>& fields, const GroupTree& tree);

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
