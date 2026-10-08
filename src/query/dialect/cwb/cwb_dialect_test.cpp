// assert() is the check here: keep it in Release (NDEBUG) builds too
#undef NDEBUG
#include "index/fold_map.h"
#include "query/dialect/cwb/cwb_translate.h"

#include <cassert>
#include <iostream>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

static void expect_ok(const char* q, std::size_t n_stmt = 1) {
    auto p = pando::translate_cwb_program(q, 0, nullptr);
    assert(p.size() == n_stmt);
}

static void expect_throw(const char* q) {
    try {
        pando::translate_cwb_program(q, 0, nullptr);
        assert(false);
    } catch (const std::runtime_error&) {
    }
}

int main() {
    expect_ok("[lemma=\"the\"]");
    expect_ok("[lemma=\"a\" & lemma=\"b\"]");
    expect_ok("[lemma=\"a\" | lemma=\"b\"]");
    expect_ok("q = [lemma=\"x\"]", 1);
    assert(pando::translate_cwb_program("q = [lemma=\"x\"]", 0, nullptr)[0].name == "q");

    expect_ok("[lemma=\"a\"][lemma=\"b\"]", 1);
    assert(pando::translate_cwb_program("[lemma=\"a\"][lemma=\"b\"]", 0, nullptr)[0]
               .query.tokens.size() == 2);

    // repetition suffixes (the '+' used to be lexed as a signed number).
    // CHECK, not assert: this test also has to fail in Release (NDEBUG) builds.
    struct RepCase { const char* q; std::size_t ntok; int rep_tok, min, max; };
    const RepCase rep_cases[] = {
        {"[pos=\"DET\"] []+ [pos=\"NOUN\"]", 3, 1, 1, pando::REPEAT_UNBOUNDED},
        {"[pos=\"DET\"] []+[pos=\"NOUN\"]", 3, 1, 1, pando::REPEAT_UNBOUNDED},
        {"[pos=\"ADJ\"]+ [pos=\"NOUN\"]", 2, 0, 1, pando::REPEAT_UNBOUNDED},
        {"[pos=\"DET\"] []* [pos=\"NOUN\"]", 3, 1, 0, pando::REPEAT_UNBOUNDED},
        {"[pos=\"DET\"] []{0,3} [pos=\"NOUN\"]", 3, 1, 0, 3},
        {"[pos=\"DET\"] [pos=\"ADJ\"]? [pos=\"NOUN\"]", 3, 1, 0, 1},
    };
    for (const auto& c : rep_cases) {
        auto p = pando::translate_cwb_program(c.q, 0, nullptr);
        const auto& toks = p.at(0).query.tokens;
        if (p.size() != 1 || toks.size() != c.ntok
            || toks[static_cast<std::size_t>(c.rep_tok)].min_repeat != c.min
            || toks[static_cast<std::size_t>(c.rep_tok)].max_repeat != c.max) {
            std::fprintf(stderr, "FAIL repetition: %s\n", c.q);
            return 1;
        }
    }

    expect_throw("count [lemma=\"x\"]");
    expect_throw("[lemma=\"a\"] | [lemma=\"b\"]");
    expect_throw("![lemma=\"x\"]");
    expect_throw("[lemma=\"x\"] ::");

    try {
        pando::translate_cwb_program("count", 0, nullptr);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::strstr(e.what(), "by") != nullptr);
    }

    {
        auto p = pando::translate_cwb_program("[lemma=\"the\"]; count by form", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].has_command);
        assert(p[1].command.type == pando::CommandType::COUNT);
        assert(p[1].command.fields.size() == 1);
        assert(p[1].command.fields[0] == "form");
    }

    {
        auto p = pando::translate_cwb_program("[upos=\"NOUN\"]; group by lemma", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].has_command);
        assert(p[1].command.type == pando::CommandType::GROUP);
        assert(p[1].command.fields.size() == 1);
        assert(p[1].command.fields[0] == "lemma");
    }

    {
        auto p = pando::translate_cwb_program("group by match lemma", 0, nullptr);
        assert(p.size() == 1);
        assert(p[0].command.type == pando::CommandType::GROUP);
        assert(p[0].command.fields.size() == 1);
        assert(p[0].command.fields[0] == "match.lemma");
    }

    {
        auto p = pando::translate_cwb_program("group by match.lemma", 0, nullptr);
        assert(p.size() == 1);
        assert(p[0].command.fields[0] == "match.lemma");
    }

    {
        auto p = pando::translate_cwb_program("group by lemma, form", 0, nullptr);
        assert(p[0].command.fields.size() == 2);
        assert(p[0].command.fields[0] == "lemma");
        assert(p[0].command.fields[1] == "form");
    }

    {
        auto p = pando::translate_cwb_program("[lemma=\"the\"]; size", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].has_command);
        assert(p[1].command.type == pando::CommandType::SIZE);
    }

    {
        auto p = pando::translate_cwb_program("[lemma=\"the\"]; sort by lemma", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].command.type == pando::CommandType::SORT);
        assert(p[1].command.fields.size() == 1);
        // CQP: the key is the whole match unless boundaries are given
        assert(p[1].command.fields[0] == "lemma on match..matchend");
    }

    {
        // the named query is sorted (not skipped as a corpus id); flags and boundaries kept
        auto p = pando::translate_cwb_program(
            "Matches = [lemma=\"the\"]; sort Matches by word %cd on match[-1]..match[-5]", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].command.type == pando::CommandType::SORT);
        assert(p[1].command.query_name == "Matches");
        assert(p[1].command.fields.size() == 1);
        assert(p[1].command.fields[0] == "word %cd on match[-1]..match[-5]");
        auto q = pando::translate_cwb_program("[]; sort by word %c on matchend[1]..matchend[5]", 0, nullptr);
        assert(q[1].command.fields[0] == "word %c on matchend[1]..matchend[5]");
        // %c / %d fold UTF-8 (sort keys and query matching share FoldMap::fold)
        assert(pando::FoldMap::fold("ŘEKA Čaj", true, false) == "řeka čaj");
        assert(pando::FoldMap::fold("Žena Émile Ångström", true, true) == "zena emile angstrom");
        assert(pando::FoldMap::fold("Ελλάδα Ёлка", true, true) == "ελλαδα елка");
        assert(pando::FoldMap::fold("Øl Łódź straße", true, true) == "øl łodz straße");
        bool threw = false;
        try {
            pando::translate_cwb_program("[]; sort by word %l", 0, nullptr);
        } catch (const std::exception&) {
            threw = true;
        }
        assert(threw);
    }

    {
        auto p = pando::translate_cwb_program("[lemma=\"the\"]; tabulate 0 5 lemma", 0, nullptr);
        assert(p.size() == 2);
        assert(p[1].command.type == pando::CommandType::TABULATE);
        assert(p[1].command.tabulate_offset == 0);
        assert(p[1].command.tabulate_limit == 5);
        assert(p[1].command.fields.size() == 1);
        assert(p[1].command.fields[0] == "lemma");
    }

    {
        auto p = pando::translate_cwb_program("tabulate lemma", 0, nullptr);
        assert(p.size() == 1);
        assert(p[0].command.tabulate_offset == 0);
        assert(p[0].command.tabulate_limit == 1000);
        assert(p[0].command.fields[0] == "lemma");
    }

    expect_throw("count by lemma.sub");
    expect_throw("count by match");

    expect_throw("group by match");

    {
        auto p = pando::translate_cwb_program(R"([!(lemma="the" | upos="NOUN")] within s)", 0, nullptr);
        assert(p[0].query.within == "s");
        const auto& c = p[0].query.tokens[0].conditions;
        assert(!c->is_leaf && c->bool_op == pando::BoolOp::AND);
    }
    std::cerr << "PASS cwb_dialect_test\n";
    return 0;
}
