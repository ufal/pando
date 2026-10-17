// assert() is the check here: keep it in Release (NDEBUG) builds too
#undef NDEBUG
// Unit tests for PML-TQ lexer, JSON, and native parser (see dev/PMLTQ-ROADMAP.md).

#include "query/dialect/pmltq/pmltq_ast.h"
#include "query/dialect/pmltq/pmltq_gold_json.h"
#include "query/dialect/pmltq/pmltq_lexer.h"
#include "query/dialect/pmltq/pmltq_native_lower.h"
#include "query/dialect/pmltq/pmltq_parser.h"
#include "query/dialect/pmltq/pmltq_test_lower.h"

#include <cassert>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>

using namespace pando;
using namespace pando::pmltq;

namespace {

static void test_lex_sample() {
    const char* src = R"(# comment
[a-node x:=y [ lemma="the" and tag="DT" ]]
>> distinct give a.lemma, a.tag
)";
    auto toks = tokenize_pmltq_all(src);
    assert(!toks.empty());
    assert(toks.back().type == PmltqTok::END);

    size_t i = 0;
    assert(toks[i++].type == PmltqTok::LBRACKET);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "a-node");
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "x");
    assert(toks[i++].type == PmltqTok::COLON_EQ);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "y");
    assert(toks[i++].type == PmltqTok::LBRACKET);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "lemma");
    assert(toks[i++].type == PmltqTok::EQ);
    assert(toks[i++].type == PmltqTok::STRING && toks[i - 1].text == "the");
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "and");
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "tag");
    assert(toks[i++].type == PmltqTok::EQ);
    assert(toks[i++].type == PmltqTok::STRING && toks[i - 1].text == "DT");
    assert(toks[i++].type == PmltqTok::RBRACKET);
    assert(toks[i++].type == PmltqTok::RBRACKET);
    assert(toks[i++].type == PmltqTok::GTGT);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "distinct");
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "give");
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "a");
    assert(toks[i++].type == PmltqTok::DOT);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "lemma");
    assert(toks[i++].type == PmltqTok::COMMA);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "a");
    assert(toks[i++].type == PmltqTok::DOT);
    assert(toks[i++].type == PmltqTok::IDENT && toks[i - 1].text == "tag");
}

static void test_parse_empty() {
    bool threw = false;
    try {
        parse_pmltq_query("  # only comment\n");
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("empty query") != std::string::npos);
    }
    assert(threw);
}

static void test_json_roundtrip() {
    const char* j = R"({"ok":true,"ast":{"type":"query","children":[]},"sql":null})";
    JsonValue v;
    std::string err;
    assert(parse_json(j, v, err));
    const JsonValue* ok = v.get("ok");
    assert(ok && ok->kind == JsonValue::Bool && ok->b_val);
}

static void test_parse_gold_fixture_file() {
    namespace fs = std::filesystem;
    fs::path here(__FILE__);
    for (int i = 0; i < 5; ++i)
        here = here.parent_path();
    here /= "test/data/pmltq_gold_sample.json";
    std::ifstream in(here);
    if (!in) {
        // the fixture comes from the dev/pmltq-gold PEG bridge, which is not in every checkout
        std::cerr << "SKIP test_parse_gold_fixture_file (no " << here.string() << ")\n";
        return;
    }
    std::string json((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    JsonValue root;
    std::string err;
    assert(parse_json(json, root, err));
    assert(err.empty());
    const JsonValue* ast = root.get("ast");
    assert(ast != nullptr);
    assert(ast->kind == JsonValue::Object);
}

static void test_parse_native_simple() {
    PmltqQuery q = parse_pmltq_query(R"(a-node [ lemma = "the" and tag = "DT" ])");
    assert(q.selector.kind == PmltqSelectorKind::ANode);
    assert(q.selector.has_cond);
    assert(q.selector.cond.kind == PmltqCondKind::And);
}

static void test_parse_bracket_shorthand() {
    PmltqQuery q = parse_pmltq_query("tok [ form = \"x\" ]");
    assert(q.selector.kind == PmltqSelectorKind::Tok);
}

// tree_query_grammar.txt: the node type is optional, a nested selector's relation defaults to child
static void test_parse_untyped_nested() {
    PmltqQuery q = parse_pmltq_query("[a-node []]");
    assert(q.selector.type_name.empty());
    assert(q.selector.cond.kind == PmltqCondKind::Nested);
    assert(!q.selector.cond.nested->subquery && q.selector.cond.nested->relation == "child");
}

static bool parse_fails(const std::string& text) {
    try {
        parse_pmltq_query(text);
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

// `,` and `and` join the condition list; after a test, `and` takes a nested selector into the
// test (a subquery); occurrences make one; `!` and `in`
static void test_parse_conditions() {
    PmltqQuery q = parse_pmltq_query(R"(a-node [ lemma = "be", upos = 'AUX', ])");
    assert(q.selector.cond.kind == PmltqCondKind::And && q.selector.cond.children.size() == 2);
    assert(q.selector.cond.children[1].value == "AUX");

    q = parse_pmltq_query(R"(a-node [ upos = "VERB", child a-node [ deprel = "obj" ] ])");
    assert(q.selector.cond.children[1].kind == PmltqCondKind::Nested);
    assert(!q.selector.cond.children[1].nested->subquery);

    q = parse_pmltq_query(R"(a-node [ upos = "VERB" and child a-node [ deprel = "obj" ] ])");
    assert(q.selector.cond.kind == PmltqCondKind::And);
    assert(q.selector.cond.children[1].nested->subquery);

    q = parse_pmltq_query(R"(a-node [ 0|2+x child a-node [], 1..3x descendant [] ])");
    const auto& occ = q.selector.cond.children[0].nested->occurrences;
    assert(occ.size() == 2 && occ[0].min == 0 && occ[0].max == 0 && occ[1].min == 2 && occ[1].max == -1);
    const auto& occ2 = q.selector.cond.children[1].nested->occurrences;
    assert(occ2.size() == 1 && occ2[0].min == 1 && occ2[0].max == 3);

    q = parse_pmltq_query(R"(a-node [ !child a-node [ deprel = "nsubj" ], deprel !in { "a", "b" } ])");
    assert(q.selector.cond.children[0].kind == PmltqCondKind::Not);
    assert(q.selector.cond.children[1].op == "!in" && q.selector.cond.children[1].set_values.size() == 2);

    q = parse_pmltq_query(R"(a-node $v := [ child a-node $o := [ lemma = $v.lemma, $o != $v ] ])");
    const auto& inner = q.selector.cond.nested->node->cond;
    assert(inner.children[0].ref_node == "v" && inner.children[0].ref_attr == "lemma");
    assert(inner.children[1].kind == PmltqCondKind::Same && inner.children[1].op == "!=");

    assert(parse_fails("a-node [ lemma = ]"));
    assert(parse_fails("a-node [ 2 child a-node [] ]"));      // occurrences without x
    assert(parse_fails("a-node [ lower(lemma) = \"x\" ]"));  // functions
}

// the tree as a chain of named tokens, subqueries as conditions, different nodes different tokens
static void test_lower_tree() {
    std::string err;
    Program p = lower_native_pmltq_query(parse_pmltq_query(
        R"(a-node $v := [ upos = "VERB", child a-node [ deprel = "nsubj" ], child a-node [ deprel = "obj" ] ])"), err);
    assert(err.empty());
    const TokenQuery& tq = p.at(0).query;
    assert(tq.tokens.size() == 3 && tq.tokens[1].name == "v");
    assert(tq.relations[0].type == RelationType::GOVERNED_BY && tq.relations[1].type == RelationType::GOVERNS);
    assert(tq.position_orders.size() == 1 && tq.position_orders[0].op == CompOp::NEQ);

    p = lower_native_pmltq_query(parse_pmltq_query(
        R"(a-node [ upos = "VERB", 0x child a-node [ deprel = "nsubj" ] ])"), err);
    assert(err.empty() && p.at(0).query.tokens.size() == 1);

    p = lower_native_pmltq_query(parse_pmltq_query(
        R"(a-node [ deprel = "nsubj", sibling a-node [ deprel = "obj" ] ])"), err);
    assert(err.empty() && p.at(0).query.tokens.size() == 3); // through the shared parent
    assert(p.at(0).query.position_orders.size() == 1);

    p = lower_native_pmltq_query(parse_pmltq_query(
        R"(a-node [ child [], child [], child [] ])"), err);
    assert(err.find("chain") != std::string::npos);
    err.clear();
    p = lower_native_pmltq_query(parse_pmltq_query(R"(a-node [ ?child [] ])"), err);
    assert(err.find("optional") != std::string::npos);
}

// a-root (pando has no root node): its child is the sentence's root word, a condition
static void test_lower_root() {
    std::string err;
    Program p = lower_native_pmltq_query(parse_pmltq_query(
        R"(a-root [ child a-node $p := [ upos = "VERB", child a-node [ deprel = "obj" ] ] ])"), err);
    assert(err.empty());
    assert(p.at(0).query.tokens.size() == 2 && p.at(0).query.tokens[0].name == "p");
    p = lower_native_pmltq_query(parse_pmltq_query(R"(a-node [ upos = "VERB", parent a-root [] ])"), err);
    assert(err.empty() && p.at(0).query.tokens.size() == 1);
    p = lower_native_pmltq_query(parse_pmltq_query(R"(a-node [ 0x parent a-root [] ])"), err);
    assert(err.empty() && p.at(0).query.tokens.size() == 1);
    p = lower_native_pmltq_query(parse_pmltq_query(R"(a-root [])"), err);
    assert(err.find("a-root") != std::string::npos);
    err.clear();
    p = lower_native_pmltq_query(parse_pmltq_query(R"(a-root [ child [], child [] ])"), err);
    assert(err.find("a-root") != std::string::npos);
}

static void test_parse_output_distinct_count() {
    PmltqQuery q = parse_pmltq_query(
        "a-node $a := [ form = \"th.*\" ] >> distinct $a.form, count(over $a.form)");
    assert(q.has_output);
    assert(q.output.saw_distinct_keyword);
    assert(q.output.items.size() == 2u);
    assert(q.output.items[0].kind == PmltqOutputItemKind::FieldRef);
    assert(q.output.items[0].node == "a" && q.output.items[0].attr == "form");
    assert(q.output.items[1].kind == PmltqOutputItemKind::CountOver);
    assert(q.output.items[1].node == "a" && q.output.items[1].attr == "form");
}

} // namespace

// PML-TQ's attribute names as pando's: deprel stays deprel, feats/ values get their capital
static void test_field_names() {
    assert(pmltq_map_field("deprel") == "deprel");
    assert(pmltq_map_field("lemma") == "lemma");
    assert(pmltq_map_field("feats/case") == "feats/Case");
}

int main() {
    test_lex_sample();
    test_field_names();
    test_json_roundtrip();
    test_parse_gold_fixture_file();
    test_parse_empty();
    test_parse_native_simple();
    test_parse_bracket_shorthand();
    test_parse_untyped_nested();
    test_parse_conditions();
    test_lower_tree();
    test_lower_root();
    test_parse_output_distinct_count();
    std::cerr << "PASS pmltq_dialect_test\n";
    return 0;
}
