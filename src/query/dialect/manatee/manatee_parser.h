#pragma once

// Manatee CQL into its parse tree (manatee_ast.h): a recursive-descent reading of Manatee's grammar
// (query/cqpeval.y, as a specification). It accepts what that grammar accepts and rejects the rest with a syntax
// error naming the position; whether pando can run a construct is decided later (manatee_lower).

#include "query/dialect/manatee/manatee_ast.h"

#include <string>

namespace pando::manatee {

Query parse(const std::string& query);

}  // namespace pando::manatee
