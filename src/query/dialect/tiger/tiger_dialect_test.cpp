// assert() is the check here: keep it in Release (NDEBUG) builds too
#undef NDEBUG
#include "query/dialect/tiger/tiger_translate.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

static void expect_ok(const char* q, std::size_t n_stmt = 1) {
    auto p = pando::translate_tiger_program(q, 0, nullptr);
    assert(p.size() == n_stmt);
}

static void expect_throw(const char* q) {
    try {
        pando::translate_tiger_program(q, 0, nullptr);
        assert(false);
    } catch (const std::runtime_error&) {
    }
}

int main() {
    expect_ok("dom NP VP");
    expect_ok("idom NP VP");
    expect_ok("cat NP");

    {
        auto p = pando::translate_tiger_program("dom S NP", 0, nullptr);
        assert(p.size() == 1);
        assert(p[0].has_query);
        assert(!p[0].query.global_function_filters.empty());
    }

    expect_ok("# comment only line before\n dom NP VP");

    expect_ok("dom NP VP\ncat S", 2);

    expect_throw("dom NP");
    expect_throw("");

    std::cerr << "PASS tiger_dialect_test\n";
    return 0;
}
