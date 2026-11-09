#pragma once

#include "query/ast.h"

#include <string>

namespace pando {

// Manatee CQL (the query language of Sketch Engine / NoSketch Engine and KonText) as a pando Program: Manatee's
// grammar (dialect/manatee/manatee_parser.h) and its meaning (manatee_lower.h). Throws std::runtime_error on a
// syntax error, or for a construct pando does not run, naming it. With debug_level > 0 and trace_out, the
// translation is described there.
// `default_attr`: the attribute a bare "re" is on (empty: word, as KonText corpora set Manatee's DEFAULTATTR;
// a front-end's chosen one, e.g. KonText's simple query with a default attribute).
Program translate_manatee_program(const std::string& input, int debug_level, std::string* trace_out,
                                  const std::string& default_attr = "");

}  // namespace pando
