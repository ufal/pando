#pragma once

#include "query/ast.h"
#include <string>

namespace pando {

/// Parse CWB/CQP-style input using a lexer aligned with IMS CWB parser.l / parser.y.
/// Throws std::runtime_error on syntax errors or unsupported constructs.
/// `default_attr`: the attribute of a bare "pattern" (empty: word).
Program translate_cwb_program_parsed(const std::string& input, int debug_level,
                                       std::string* trace_out, const std::string& default_attr = "");

} // namespace pando
