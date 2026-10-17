// parse_in_dialect (dialect.h): a query in a dialect followed by pando's own steps.
// assert() is the check here: keep it in Release (NDEBUG) builds too
#undef NDEBUG
#include "query/dialect/dialect.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace pando;

static bool throws(const std::string& text, const std::string& dialect) {
    try {
        parse_in_dialect(text, dialect);
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

int main() {
    assert(normalize_dialect("PML-TQ") == "pmltq");
    assert(normalize_dialect("pando-cql") == "native");
    assert(normalize_dialect("xpath").empty());
    assert(throws("[lemma=\"be\"]", "xpath"));

    // a PML-TQ query alone, and followed by pando steps (the query, then the step)
    assert(parse_in_dialect("a-node [ lemma = \"be\" ]", "pmltq").size() == 1);
    assert(parse_in_dialect("a-node [ lemma = \"be\" ]; freq by upos", "pmltq").size() == 2);
    // a `;` inside a string is not where the steps start
    assert(parse_in_dialect("a-node [ lemma = \";\" ]; freq by upos", "pmltq").size() == 2);
    // steps that pando cannot read either: an error
    assert(throws("a-node [ lemma = \"be\" ]; no such step", "pmltq"));

    // CWB's own programs are read whole; a CWB query with pando steps also works
    assert(parse_in_dialect("A = [lemma=\"be\"]; count A by upos", "cwb").size() == 2);
    assert(parse_in_dialect("[lemma=\"be\"]; freq by upos", "cwb").size() == 2);
    std::cerr << "PASS dialect_test\n";
    return 0;
}
