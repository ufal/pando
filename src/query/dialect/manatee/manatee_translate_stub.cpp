#include "query/dialect/manatee/manatee_translate.h"

#include <stdexcept>
#include <string>

namespace pando {

Program translate_manatee_program(const std::string&, int, std::string*) {
    throw std::runtime_error(
        "The Manatee CQL dialect was not built in this binary. Configure with -DPANDO_MANATEE_DIALECT=ON.");
}

}  // namespace pando
