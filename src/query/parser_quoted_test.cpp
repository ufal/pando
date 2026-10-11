// assert() is the check here: keep it in Release (NDEBUG) builds too
#undef NDEBUG
#include "query/parser.h"
#include "query/ast.h"
#include <cassert>
#include <iostream>

int main() {
    using pando::CommandType;
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
    // the documented form: a derived binding after `::`
    {
        auto prog = Parser(R"(n:[deprel="nsubj"] :: np:dep_subtree(n) & tcnt(np) > 6)", {}).parse();
        const auto& q = prog[0].query;
        if (q.tokens.size() != 2 || !q.tokens[1].is_dep_subtree || q.tokens[1].name != "np"
            || q.tokens[1].dep_subtree_source != "n" || q.relations.size() != 1
            || q.global_function_filters.size() != 1) {
            std::cerr << "`:: np:dep_subtree(n)` not parsed as a derived binding\n";
            return 1;
        }
    }
    // `Name = dep_subtree(…)` and `:: np = dep_subtree(n)` are errors with a hint
    for (const char* bad : {R"(longbarenp = dep_subtree(barenoun) :: tcnt(longbarenp) > 2)",
                            R"(n:[] :: np = dep_subtree(n))"}) {
        bool threw = false;
        try { Parser(bad, {}).parse(); } catch (const std::exception&) { threw = true; }
        if (!threw) { std::cerr << "expected an error for: " << bad << "\n"; return 1; }
    }
    Parser(R"(a:[]; stats avg(strlen(a.form)), median(strlen(a.form)) by a.text_id)", {}).parse();

    // coll / dcoll: parenthesized match-set name and (on label)
    {
        auto prog = Parser(R"(coll (MySet) (on hub) by lemma)", {}).parse();
        assert(prog.size() == 1 && prog[0].has_command);
        assert(prog[0].command.type == CommandType::COLL);
        assert(prog[0].command.query_name == "MySet");
        assert(prog[0].command.coll_on_label == "hub");
    }
    {
        auto prog = Parser(R"(coll on b by lemma)", {}).parse();
        assert(prog[0].command.query_name.empty());
        assert(prog[0].command.coll_on_label == "b");
    }
    {
        auto prog = Parser(R"(coll LastQuery on b by lemma)", {}).parse();
        assert(prog[0].command.query_name == "LastQuery");
        assert(prog[0].command.coll_on_label == "b");
    }
    {
        auto prog = Parser(R"(dcoll (Saved) a.amod by lemma)", {}).parse();
        assert(prog.size() == 1 && prog[0].has_command);
        assert(prog[0].command.type == CommandType::DCOLL);
        assert(prog[0].command.query_name == "Saved");
        assert(prog[0].command.dcoll_anchor == "a");
        assert(prog[0].command.relations.size() == 1 && prog[0].command.relations[0] == "amod");
    }
    {
        auto prog = Parser(R"(dcoll (on anchor1) by lemma)", {}).parse();
        assert(prog[0].command.dcoll_anchor == "anchor1");
        assert(prog[0].command.relations.empty());
    }
    // Korpora report (2026-09-30): no silent leftovers, `!` inside [ ], within <s/>,
    // `:: a.attr = "v"` on a token
    {
        auto throws = [](const char* q) {
            try { Parser(q, {}).parse(); } catch (const std::exception&) { return true; }
            return false;
        };
        assert(throws(R"([upos="DET"] | [upos="NOUN"])"));
        assert(throws(R"([upos="DET"] ([upos="NOUN"]){1,2})"));
        assert(throws(R"([upos="DET"] ()"));
        auto neg = Parser(R"([!(lemma="the" | upos="NOUN")])", {}).parse();
        const auto& c = neg[0].query.tokens[0].conditions;
        assert(!c->is_leaf && c->bool_op == pando::BoolOp::AND);
        assert(c->left->leaf.op == pando::CompOp::NEQ && c->right->leaf.op == pando::CompOp::NEQ);
        auto negre = Parser(R"([!lemma="be.*"])", {}).parse();
        assert(negre[0].query.tokens[0].conditions->leaf.neq_regex);
        auto w = Parser(R"([lemma="cat"] within <s/>)", {}).parse();
        assert(w[0].query.within == "s");
        auto g = Parser(R"(a:[upos="VERB"] :: a.lemma = "say")", {}).parse();
        assert(g[0].query.global_region_filters.empty());
        assert(!g[0].query.tokens[0].conditions->is_leaf);
        auto r = Parser(R"(a:[upos="VERB"] :: a.text_lang = "en")", {}).parse();
        assert(r[0].query.global_region_filters.size() == 1);
    }
    std::cerr << "PASS parser_quoted_test\n";
    return 0;
}
