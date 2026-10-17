#include "query/dialect/pmltq/pmltq_test_lower.h"

#include <cctype>
#include <utility>

namespace pando::pmltq {

namespace {

std::string strip_quotes(std::string s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"')
        return s.substr(1, s.size() - 2);
    return s;
}

} // namespace

std::string pmltq_map_field(std::string field) {
    // deprel stays deprel: pando's name for the relation (flexencoder's dep_rel is mapped at indexing)
    if (field == "head")
        return "head_tok_pos";
    if (field == "sentord")
        return "sent_ord";
    if (field == "docpos")
        return "doc_pos";
    const std::string feats_p = "feats/";
    if (field.size() > feats_p.size() && field.compare(0, feats_p.size(), feats_p) == 0) {
        std::string rest = field.substr(feats_p.size());
        if (!rest.empty()) {
            if (rest[0] >= 'a' && rest[0] <= 'z')
                rest[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(rest[0])));
            return feats_p + rest;
        }
    }
    const std::string iset_p = "iset/";
    if (field.size() > iset_p.size() && field.compare(0, iset_p.size(), iset_p) == 0) {
        std::string rest = field.substr(iset_p.size());
        if (!rest.empty()) {
            if (rest[0] >= 'a' && rest[0] <= 'z')
                rest[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(rest[0])));
            return feats_p + rest;
        }
    }
    return field;
}

ConditionPtr lower_pmltq_test_strings(const std::string& field_in, const std::string& op_in,
                                      const std::string& value_in, std::string& err) {
    // PML-TQ's operators (tree_query_grammar.txt CMP): `=` / `!=` compare strings as they are,
    // `~` / `!~` match a regular expression anywhere in the value, `~*` / `!~*` the same
    // ignoring case, `<` `>` `<=` `>=` compare
    std::string op = op_in;
    if (op == "==")
        op = "=";

    AttrCondition ac;
    ac.attr = pmltq_map_field(std::string(field_in));
    ac.value = strip_quotes(value_in);

    if (op == "=") {
        ac.op = CompOp::EQ;
    } else if (op == "!=") {
        ac.op = CompOp::NEQ;
    } else if (op == "~" || op == "~*") {
        ac.op = CompOp::REGEX;
        if (op == "~*") // in the pattern: %c folds the values, not the pattern
            ac.value = "(?i)" + ac.value;
    } else if (op == "!~" || op == "!~*") {
        ac.op = CompOp::NEQ;
        ac.neq_regex = true;
        if (op == "!~*")
            ac.value = "(?i)" + ac.value;
    } else if (op == "<") {
        ac.op = CompOp::LT;
    } else if (op == ">") {
        ac.op = CompOp::GT;
    } else if (op == "<=") {
        ac.op = CompOp::LTE;
    } else if (op == ">=") {
        ac.op = CompOp::GTE;
    } else {
        err = "PMLTQ: unsupported test operator: " + op;
        return nullptr;
    }

    return ConditionNode::make_leaf(std::move(ac));
}

} // namespace pando::pmltq
