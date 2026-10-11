#pragma once

#include "query/ast.h"
#include "query/parser.h"
#include <string>

namespace pando {

// Thin TIGERSearch-style surface → native ClickCQL (constituency / nested `node` regions).
// Expands line-based macros, then parses with the normal Parser. See dev/TIGER-DIALECT.md.
// `cat_attr`: the node attribute holding the category (`type` in older pando test corpora,
// `cat` in TEITOK node layers; the CLI picks it from the corpus).
// Throws std::runtime_error on unknown lines or parse errors.
Program translate_tiger_program(const std::string& input, int debug_level,
                                std::string* trace_out,
                                ParserOptions parse_opts = {},
                                const std::string& cat_attr = "type");

} // namespace pando
