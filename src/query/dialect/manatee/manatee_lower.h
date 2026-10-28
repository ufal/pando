#pragma once

// A Manatee CQL parse tree (manatee_ast.h) as a pando Program, keeping Manatee's meaning:
//   * every quoted value is a regular expression matching the whole value (`==` compares literally); a bare
//     "re" is on the default attribute (`word`);
//   * numeric labels `1:[…]` are the query's names (`_1`), `& 1.lemma = 2.lemma` and `& f(1.word) > 5` its `::`
//     filters;
//   * `<s>` / `</s>` are region anchors; `within` / `!within` / `containing` with a structure (`<s/>`,
//     `<doc file="x"/>`) are pando's within, not within and containing.
// What the grammar accepts but pando does not run is refused with a message naming the construct.

#include "query/ast.h"
#include "query/dialect/manatee/manatee_ast.h"

#include <ostream>

namespace pando::manatee {

Program lower(const Query& q, std::ostream* trace = nullptr);

}  // namespace pando::manatee
