#pragma once

#include "query/ast.h"

#include <stdexcept>
#include <string>
#include <string_view>

namespace pando {

// Same character set as CWB dialect (cwb_parser.cpp): if absent, treat as literal for "=".
inline bool string_has_regex_metachar(std::string_view s) {
    for (char ch : s) {
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

// A value that is a single character with no meaning as a whole-token pattern (`"?"`, `"*"`,
// `"("`): the character itself, so `[form="?"]` finds question marks instead of failing. (`"."`
// keeps its CWB meaning: any one-character token; `"\."` is a full stop.)
inline bool lone_literal_metachar(std::string_view s) {
    if (s.size() != 1) return false;
    switch (s[0]) {
    case '?': case '*': case '+': case '(': case ')': case '[': case ']':
    case '{': case '}': case '|': case '\\':
        return true;
    default:
        return false;
    }
}

// Native `[attr = "value"]` and bare-token `"value"` / `form` shorthand: when strict_quoted_strings
// is false, behave like CWB/Manatee — quoted "=" uses whole-token regex if value looks like a pattern.
// The executor applies RE2::FullMatch / std::regex_match (regex_full_match), not literal ^$ in the pattern.
// `escaped`: the value as written, backslashes kept (Token::raw); a pattern uses it, so `"\."` is
// a full stop and `"\?"` a question mark (the lexer's unescaped text would be `.` / `?`).
inline void interpret_quoted_eq_string(AttrCondition& ac, std::string raw,
                                     bool strict_quoted_strings,
                                     const std::string* escaped = nullptr) {
    ac.regex_full_match = false;
    const std::string& pat = (escaped && !escaped->empty()) ? *escaped : raw;
    if (strict_quoted_strings || lone_literal_metachar(raw) || !string_has_regex_metachar(pat)) {
        ac.op = CompOp::EQ;
        ac.value = std::move(raw);
        return;
    }
    ac.op = CompOp::REGEX;
    ac.regex_full_match = true;
    ac.value = pat;
}

// `[attr != "value"]`: like `=` (CWB/Manatee), a quoted value with regex metacharacters is a
// whole-token pattern, and != is its complement (neq_regex). strict_quoted_strings: literal.
inline void interpret_quoted_neq_string(AttrCondition& ac, std::string raw,
                                        bool strict_quoted_strings,
                                        const std::string* escaped = nullptr) {
    ac.op = CompOp::NEQ;
    ac.regex_full_match = false;
    ac.neq_regex = false;
    const std::string& pat = (escaped && !escaped->empty()) ? *escaped : raw;
    if (!strict_quoted_strings && !lone_literal_metachar(raw) && string_has_regex_metachar(pat)) {
        ac.neq_regex = true;
        ac.regex_full_match = true;
        ac.value = pat;
        return;
    }
    ac.value = std::move(raw);
}

} // namespace pando
