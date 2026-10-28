// The Manatee CQL dialect without a corpus: what the grammar accepts and rejects (Manatee's query/cqpeval.y) and the
// shape of the pando query it becomes. Answers against an index: test/manatee_dialect_corpus_test.py.

#include "query/dialect/manatee/manatee_lexer.h"
#include "query/dialect/manatee/manatee_translate.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace pando;

static int failures = 0;

static void check(bool ok, const std::string& what) {
    if (!ok) {
        ++failures;
        std::cerr << "FAIL " << what << "\n";
    }
}

static TokenQuery tq(const std::string& q) {
    Program p = translate_manatee_program(q, 0, nullptr);
    if (p.size() != 1 || !p[0].has_query) throw std::runtime_error("not one query");
    return p[0].query;
}

// the error message of q ("" when it translates)
static std::string err(const std::string& q) {
    try {
        translate_manatee_program(q, 0, nullptr);
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

int main() {
    // lexer: strings are regexps with \" and \\ unescaped, other backslashes kept
    {
        auto t = manatee::lex(R"([word="a\"b\\c\.d"] -3 x_1@y ;)");
        check(t[3].kind == manatee::Tok::Regexp && t[3].text == R"(a"b\c\.d)", "lexer: string unescaping");
        check(t[5].kind == manatee::Tok::NNumber && t[5].num == -3, "lexer: negative number");
        check(t[6].kind == manatee::Tok::Word && t[6].text == "x_1@y", "lexer: word characters");
        check(t.back().kind == manatee::Tok::End, "lexer: closing ;");
    }
    check(err("[word='x']").find("unexpected character") != std::string::npos, "single quotes are not Manatee");
    check(err("[word=\"x\"]\n").find("unexpected character") != std::string::npos, "a newline is not Manatee");

    // tokens: a bare string is the default attribute; values are regexps; == is literal
    {
        TokenQuery q = tq("\"the\"");
        check(q.tokens.size() == 1 && q.tokens[0].conditions->leaf.attr == "word" &&
                  q.tokens[0].conditions->leaf.op == CompOp::EQ, "bare string → word = literal");
        q = tq("[lemma=\"un.*\"]");
        check(q.tokens[0].conditions->leaf.op == CompOp::REGEX && q.tokens[0].conditions->leaf.regex_full_match,
              "a pattern is a whole-value regex");
        q = tq("[lemma==\"a.b\"]");
        check(q.tokens[0].conditions->leaf.op == CompOp::EQ && q.tokens[0].conditions->leaf.value == "a.b", "== literal");
        q = tq("[lemma!=\"x.*\"]");
        check(q.tokens[0].conditions->leaf.op == CompOp::NEQ && q.tokens[0].conditions->leaf.neq_regex, "!= pattern");
        q = tq("[!(lemma=\"a\" & upos=\"NOUN\")]");
        check(!q.tokens[0].conditions->is_leaf && q.tokens[0].conditions->bool_op == BoolOp::OR &&
                  q.tokens[0].conditions->left->leaf.op == CompOp::NEQ, "! pushed down (De Morgan)");
        q = tq("[]");
        check(!q.tokens[0].conditions, "[] is any token");
    }
    // sequences, repetition, labels, groups, alternatives of one token
    {
        TokenQuery q = tq("[upos=\"DET\"] []{0,2} [upos=\"NOUN\"]");
        check(q.tokens.size() == 3 && q.tokens[1].min_repeat == 0 && q.tokens[1].max_repeat == 2, "{0,2}");
        q = tq("[]+ []* []? []{3,} []{,4}");
        check(q.tokens[0].max_repeat == REPEAT_UNBOUNDED && q.tokens[1].min_repeat == 0 && q.tokens[2].max_repeat == 1 &&
                  q.tokens[3].min_repeat == 3 && q.tokens[3].max_repeat == REPEAT_UNBOUNDED &&
                  q.tokens[4].min_repeat == 0 && q.tokens[4].max_repeat == 4, "+ * ? {n,} {,m}");
        q = tq("1:[upos=\"ADJ\"] 2:[upos=\"NOUN\"] & 1.lemma = 2.lemma");
        check(q.tokens[0].name == "_1" && q.global_alignment_filters.size() == 1 &&
                  q.global_alignment_filters[0].attr2 == "lemma", "labels and & 1.a = 2.b");
        q = tq("1:[] & f(1.word) >= 100");
        check(q.global_function_filters.size() == 1 && q.global_function_filters[0].op == CompOp::GTE &&
                  q.global_function_filters[0].int_value == 100, "f(1.word) >= 100");
        q = tq("([upos=\"ADJ\"])+ [upos=\"NOUN\"]");
        check(q.tokens.size() == 2 && q.tokens[0].max_repeat == REPEAT_UNBOUNDED, "a repeated one-token group");
        q = tq("\"a\" | \"the\"");
        check(q.tokens.size() == 1 && q.tokens[0].conditions->bool_op == BoolOp::OR, "| between single tokens");
    }
    // unions: `|` between sequences, repeated groups expanded (a union the executor takes)
    {
        TokenQuery q = tq("[upos=\"DET\"] [upos=\"NOUN\"] | [upos=\"PRON\"]");
        check(q.tokens.size() == 2 && q.union_alternatives.size() == 1 && q.union_alternatives[0].size() == 1,
              "A B | C: two alternatives");
        q = tq("([upos=\"ADJ\"] [upos=\"CCONJ\"])+ [upos=\"NOUN\"]");
        check(q.tokens.size() == 3 && q.union_alternatives.size() == 49 && q.union_alternatives.back().size() == 101,
              "(A B)+ C: 1 … 50 repetitions (Manatee's range limit of 100)");
        q = tq("([upos=\"ADJ\"] [upos=\"ADJ\"]?)? [upos=\"NOUN\"]");
        check(q.tokens.size() == 1 && q.union_alternatives.size() == 1 && q.union_alternatives[0].size() == 3,
              "(A B?)? C: C and A B? C");
        q = tq("([upos=\"ADJ\"] [upos=\"NOUN\"]){2} [upos=\"VERB\"] within <s/>");
        check(q.tokens.size() == 5 && q.union_alternatives.empty() && !q.manatee_within, "(A B){2} C: one sequence");
        q = tq("[upos=\"DET\"] [upos=\"NOUN\"] | [upos=\"PRON\"] within <s/>");
        check(q.manatee_within, "alternatives of different lengths within <s/>: Manatee's merge");
    }
    // structures
    {
        TokenQuery q = tq("[upos=\"ADJ\"] [upos=\"NOUN\"] within <s/>");
        check(q.within == "s" && !q.not_within, "within <s/>");
        q = tq("[upos=\"ADJ\"] !within <s/>");
        check(q.within == "s" && q.not_within, "!within <s/>");
        q = tq("[upos=\"NOUN\"] within <text langcode=\"en\"/>");
        check(q.within == "text" && q.global_region_filters.size() == 1 &&
                  q.global_region_filters[0].region_attr == "text_langcode", "within <text langcode=\"en\"/>");
        q = tq("<s> [upos=\"DET\"]");
        check(q.tokens[0].anchor == RegionAnchorType::REGION_START && q.tokens[0].anchor_region == "s", "<s> anchor");
        q = tq("[upos=\"PUNCT\"] </s>");
        check(q.tokens[1].anchor == RegionAnchorType::REGION_END, "</s> anchor");
        q = tq("[upos=\"VERB\"] containing <s/>");
        check(q.containing_clauses.size() == 1 && q.containing_clauses[0].region == "s", "containing <s/>");
    }
    // syntax errors (Manatee rejects them)
    for (const char* bad : {"[word=\"x\"", "[word]", "a:[word=\"x\"]", "[word=\"x\"] & 1.word=\"x\"",
                            "[word=\"x\"] within", "(meet [word=\"x\"] [word=\"y\"] -3 3 p)", "[word=\"x\"]{2"})
        check(err(bad).find("syntax error") != std::string::npos || err(bad).find("unexpected") != std::string::npos,
              std::string("rejected: ") + bad + " (" + err(bad) + ")");
    // accepted by Manatee, refused by pando with the construct named
    for (const char* q : {"(meet [lemma=\"a\"] [lemma=\"b\"] -3 3)", "(union [lemma=\"a\"] [lemma=\"b\"])",
                          "[lemma=\"a\"] within ces: [lemma=\"b\"]", "~\"house\"", "[#5]",
                          "[lemma=\"a\"] within 3"})
        check(err(q).find("not supported by pando") != std::string::npos, std::string("named as unsupported: ") + q +
              " (" + err(q) + ")");

    if (failures) {
        std::cerr << failures << " failure(s)\n";
        return 1;
    }
    std::cerr << "PASS manatee_dialect_test\n";
    return 0;
}
