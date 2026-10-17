#pragma once

#include "query/ast.h"
#include "query/dialect/pmltq/pmltq_ast.h"

#include <string>

namespace pando::pmltq {

// One node's PML-TQ conditions as pando conditions on that token: attribute tests (`=` literal,
// `~` regex, `in { … }`), `and` / `or` / `!` (negation pushed down to the tests), and subqueries:
// a related node with occurrences (`0x`, `1+x`, `2x`, `1..3x`, `|`) as `child [...]`,
// `not child [...]` or `count(child [...])` conditions. Inside a subquery every nested node is a
// condition too. References to other nodes ($a.lemma) are not conditions on one token: the
// caller handles them (pmltq_native_lower.cpp). Throws std::runtime_error.

/// `expr` negated (`!`): tests' operators flipped, and / or swapped, occurrences complemented.
PmltqCondExpr negate_condition(const PmltqCondExpr& expr);

/// `expr` as a condition on the token of node `self` ("" when unnamed). In a subquery
/// (`in_subquery`), nested nodes without occurrences count as 1+x.
ConditionPtr lower_condition(const PmltqCondExpr& expr, const std::string& self, bool in_subquery = false);

/// Does `expr` refer to another node than `self` (a `$b.attr` side, or `$a = $b`)?
bool refers_to_other_node(const PmltqCondExpr& expr, const std::string& self);

/// The token is (negated: is not) its sentence's root word: `deprel = "root"` (pando has no
/// a-root node; UD's root word hangs from it).
ConditionPtr lower_test_root(bool negated = false);

/// PML-TQ's relation name as pando's (child, parent, sibling, descendant, ancestor); throws for
/// the others.
StructRelType structural_relation(const std::string& relation);

} // namespace pando::pmltq
