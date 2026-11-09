
#include "query/dialect/cwb/cwb_translate.h"
#include "query/dialect/cwb/cwb_parser.h"

namespace pando {

Program translate_cwb_program(const std::string& input, int debug_level,
                              std::string* trace_out, const std::string& default_attr) {
    return translate_cwb_program_parsed(input, debug_level, trace_out, default_attr);
}

} // namespace pando
