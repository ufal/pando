#pragma once

#include "query/ast.h"
#include <string>

namespace pando {

// Translate a CWB/CQP-style query subset into native pando Program.
// When trace_out is non-null and debug_level > 0, append a human-readable
// interpretation (literal vs regex per pattern, resulting structure).
//
// Throws std::runtime_error with a clear message for unsupported constructs
// (e.g. merge, unclosed bracket).
// `default_attr`: the attribute a bare "pattern" is on (empty: word, CQP's default).
Program translate_cwb_program(const std::string& input, int debug_level,
                              std::string* trace_out, const std::string& default_attr = "");

} // namespace pando
