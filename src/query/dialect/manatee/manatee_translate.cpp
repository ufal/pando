#include "query/dialect/manatee/manatee_translate.h"

#include "query/dialect/manatee/manatee_lower.h"
#include "query/dialect/manatee/manatee_parser.h"

#include <sstream>

namespace pando {

Program translate_manatee_program(const std::string& input, int debug_level, std::string* trace_out) {
    const manatee::Query q = manatee::parse(input);
    std::ostringstream trace;
    Program p = manatee::lower(q, debug_level > 0 && trace_out ? &trace : nullptr);
    if (!p.empty()) p[0].source = input;
    if (debug_level > 0 && trace_out) *trace_out += trace.str();
    return p;
}

}  // namespace pando
