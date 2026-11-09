#pragma once

// EF-12: a query restricted to a subcorpus (the same for `use` and `within`; the modes
// differ only in the statistics, see subcorpus.h).
//
// The hit rule (Manatee's): a hit counts only when all its tokens lie in the set.
//   - every query token gets the condition "position in the set" (an IN leaf on the
//     PositionSet, AttrCondition::in_set), so each token, repeated positions included,
//     must be inside;
//   - a set stored as ranges is also given as a computed position filter
//     (GlobalRegionFilter::positions): the region-mask fast paths slice their operands to
//     it. A one-position query needs only that filter (its one token is its first
//     position), so its fast paths stay as they are;
//   - a query of region anchors only (`t:<s>`, regions as hits): a region counts when part
//     of it is inside; its span as displayed is unchanged.
// The display context is never cut.

#include "query/ast.h"
#include "subcorpus/position_set.h"

#include <memory>

namespace pando {

class Corpus;

void restrict_query_to_subcorpus(const Corpus& corpus, TokenQuery& q, const std::shared_ptr<const PositionSet>& set);
/// The statement's query, and an aligned query's target side.
void restrict_statement_to_subcorpus(const Corpus& corpus, Statement& st, const std::shared_ptr<const PositionSet>& set);

} // namespace pando
