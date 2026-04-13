#include "query/dialect/pmltq/pmltq_test_lower.h"

#include <cctype>
#include <string_view>
#include <utility>

namespace pando::pmltq {

namespace {

bool has_regex_metachar(std::string_view s) {
    for (unsigned char uc : s) {
        char ch = static_cast<char>(uc);
        switch (ch) {
        case '.':
        case '*':
        case '+':
        case '?':
        case '[':
        case ']':
        case '(':
        case ')':
        case '{':
        case '}':
        case '|':
        case '^':
        case '$':
        case '\\':
            return true;
        default:
            break;
        }
    }
    return false;
}

std::string strip_quotes(std::string s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"')
        return s.substr(1, s.size() - 2);
    return s;
}

} // namespace

std::string pmltq_map_field(std::string field) {
    if (field == "deprel")
        return "dep_rel";
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
    std::string op = op_in;
    if (op == "==")
        op = "=";

    std::string val = strip_quotes(value_in);
    std::string field = pmltq_map_field(std::string(field_in));

    AttrCondition ac;
    ac.attr = std::move(field);

    if (op == "=") {
        if (has_regex_metachar(val)) {
            ac.op = CompOp::REGEX;
            ac.value = "^" + val + "$";
        } else {
            ac.op = CompOp::EQ;
            ac.value = val;
        }
    } else if (op == "!=") {
        if (has_regex_metachar(val)) {
            err = "PMLTQ: != with regex metacharacters not lowered (use native CQL or literal !=)";
            return nullptr;
        }
        ac.op = CompOp::NEQ;
        ac.value = val;
    } else if (op == "~") {
        ac.op = CompOp::REGEX;
        ac.value = val;
    } else if (op == "!~") {
        err = "PMLTQ: operator !~ not yet lowered (use native CQL or extend lowerer)";
        return nullptr;
    } else {
        err = "PMLTQ: unsupported test operator: " + op;
        return nullptr;
    }

    return ConditionNode::make_leaf(std::move(ac));
}

} // namespace pando::pmltq
