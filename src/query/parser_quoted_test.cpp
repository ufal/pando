#include "query/parser.h"

int main() {
    using pando::Parser;
    using pando::ParserOptions;
    Parser(R"([form = "th.*"])", {}).parse();
    Parser(R"([form = "the"])", {}).parse();
    ParserOptions strict;
    strict.strict_quoted_strings = true;
    Parser(R"([form = "th.*"])", strict).parse();
    // UD-style feats: feats/Key only (dot form feats.Key is rejected; '.' is name.attr).
    Parser(R"([feats/Definite="Ind"])", {}).parse();
    Parser(R"([feats/Number="Sing"])", {}).parse();
    Parser(R"([form=/foo/])", {}).parse();

    // dep_subtree: inline chain + global tcnt
    Parser(
            R"(barenoun:[upos="NOUN"] longbarenp:dep_subtree(barenoun) :: tcnt(longbarenp) > 2)",
            {})
            .parse();
    Parser(R"(longbarenp = dep_subtree(barenoun) :: tcnt(longbarenp) > 2)", {}).parse();
    return 0;
}
