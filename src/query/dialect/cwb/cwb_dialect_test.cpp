#include "query/dialect/cwb/cwb_translate.h"

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <string>

static void expect_ok(const char* q, std::size_t n_stmt = 1) {
    auto p = manatree::translate_cwb_program(q, 0, nullptr);
    assert(p.size() == n_stmt);
}

static void expect_throw(const char* q) {
    try {
        manatree::translate_cwb_program(q, 0, nullptr);
        assert(false);
    } catch (const std::runtime_error&) {
    }
}

int main() {
    expect_ok("[lemma=\"the\"]");
    expect_ok("[lemma=\"a\" & lemma=\"b\"]");
    expect_ok("[lemma=\"a\" | lemma=\"b\"]");
    expect_ok("q = [lemma=\"x\"]", 1);
    assert(manatree::translate_cwb_program("q = [lemma=\"x\"]", 0, nullptr)[0].name == "q");

    expect_ok("[lemma=\"a\"][lemma=\"b\"]", 1);
    assert(manatree::translate_cwb_program("[lemma=\"a\"][lemma=\"b\"]", 0, nullptr)[0]
               .query.tokens.size() == 2);

    expect_throw("count [lemma=\"x\"]");
    expect_throw("[lemma=\"a\"] | [lemma=\"b\"]");
    expect_throw("![lemma=\"x\"]");
    expect_throw("[lemma=\"x\"] ::");

    try {
        manatree::translate_cwb_program("count", 0, nullptr);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::strstr(e.what(), "by") != nullptr);
    }

    {
        auto p = manatree::translate_cwb_program("[lemma=\"the\"]; count by form", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].has_command);
        assert(p[1].command.type == manatree::CommandType::COUNT);
        assert(p[1].command.fields.size() == 1);
        assert(p[1].command.fields[0] == "form");
    }

    {
        auto p = manatree::translate_cwb_program("[upos=\"NOUN\"]; group by lemma", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].has_command);
        assert(p[1].command.type == manatree::CommandType::GROUP);
        assert(p[1].command.fields.size() == 1);
        assert(p[1].command.fields[0] == "lemma");
    }

    {
        auto p = manatree::translate_cwb_program("group by match lemma", 0, nullptr);
        assert(p.size() == 1);
        assert(p[0].command.type == manatree::CommandType::GROUP);
        assert(p[0].command.fields.size() == 1);
        assert(p[0].command.fields[0] == "match.lemma");
    }

    {
        auto p = manatree::translate_cwb_program("group by match.lemma", 0, nullptr);
        assert(p.size() == 1);
        assert(p[0].command.fields[0] == "match.lemma");
    }

    {
        auto p = manatree::translate_cwb_program("group by lemma, form", 0, nullptr);
        assert(p[0].command.fields.size() == 2);
        assert(p[0].command.fields[0] == "lemma");
        assert(p[0].command.fields[1] == "form");
    }

    expect_throw("count by lemma.sub");
    expect_throw("count by match");

    expect_throw("group by match");

    return 0;
}
