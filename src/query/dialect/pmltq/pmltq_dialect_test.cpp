// assert() is the check here: keep it in Release (NDEBUG) builds too
#undef NDEBUG
// Unit tests for PML-TQ lexer, JSON, and native parser (see dev/PMLTQ-ROADMAP.md).

#include "query/dialect/pmltq/pmltq_ast.h"
#include "query/dialect/pmltq/pmltq_gold_json.h"
#include "query/dialect/pmltq/pmltq_lexer.h"
#include "query/dialect/pmltq/pmltq_parser.h"

#include <cassert>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>

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

static void test_parse_invalid_nested_bracket() {
    bool threw = false;
    try {
        parse_pmltq_query("[a-node []]");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);
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

int main() {
    test_lex_sample();
    test_json_roundtrip();
    test_parse_gold_fixture_file();
    test_parse_empty();
    test_parse_native_simple();
    test_parse_bracket_shorthand();
    test_parse_invalid_nested_bracket();
    test_parse_output_distinct_count();
    std::cerr << "PASS pmltq_dialect_test\n";
    return 0;
}
