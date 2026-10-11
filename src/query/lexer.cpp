#include "query/lexer.h"
#include <stdexcept>
#include <cctype>

namespace pando {

Lexer::Lexer(const std::string& input) : input_(input) {}

void Lexer::skip_whitespace() {
    while (pos_ < input_.size() && (input_[pos_] == ' ' || input_[pos_] == '\t' ||
           input_[pos_] == '\r' || input_[pos_] == '\n'))
        ++pos_;
}

// From `from` (just after `<`): name ( ws+ ( key[/sub]* = "…" | key = '…' | key = word | name(label) ) )* ws* `/`? `>`.
// On success *end is the index of the closing `>`.
bool Lexer::scan_region_anchor_body(size_t from, size_t* end) const {
    const std::string& s = input_;
    size_t i = from;
    auto ident_char = [&](char ch) { return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '-' || ch == '.'; };
    auto skip_ws = [&]() { while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i; };
    size_t n0 = i;
    while (i < s.size() && ident_char(s[i])) ++i;
    if (i == n0) return false;
    while (true) {
        size_t before = i;
        skip_ws();
        if (i >= s.size()) return false;
        if (s[i] == '/' && i + 1 < s.size() && s[i + 1] == '>') { *end = i + 1; return true; }
        if (s[i] == '>') { *end = i; return true; }
        if (i == before) return false;          // the name/item must be followed by space, `>` or `/>`
        size_t k0 = i;
        while (i < s.size() && (ident_char(s[i]) || s[i] == '/')) ++i;
        if (i == k0) return false;
        if (i < s.size() && s[i] == '(') {       // peer clause: rchild(ip), contains(x)
            ++i;
            size_t l0 = i;
            while (i < s.size() && ident_char(s[i])) ++i;
            if (i == l0 || i >= s.size() || s[i] != ')') return false;
            ++i;
            continue;
        }
        if (i >= s.size() || s[i] != '=') return false;
        ++i;
        if (i < s.size() && (s[i] == '"' || s[i] == '\'')) {
            char q = s[i++];
            while (i < s.size() && s[i] != q) i += (s[i] == '\\' && i + 1 < s.size()) ? 2 : 1;
            if (i >= s.size()) return false;
            ++i;
        } else {
            size_t v0 = i;   // unquoted value: anything up to space or `>` (as the anchor parser reads it)
            while (i < s.size() && !std::isspace(static_cast<unsigned char>(s[i])) && s[i] != '>') ++i;
            if (i == v0) return false;
        }
    }
}

Token Lexer::next() {
    if (has_lookahead_) {
        has_lookahead_ = false;
        return lookahead_;
    }

    skip_whitespace();

    if (pos_ >= input_.size())
        return {TokType::END, "", pos_};

    size_t start = pos_;
    char c = input_[pos_];

    // Region boundary hooks: <ident> or </ident>
    // Must be checked before two-character and single-character tokens
    // to avoid <ident> being parsed as LT + IDENT + GT.
    if (c == '<') {
        // Check for </ident> (region end)
        if (pos_ + 1 < input_.size() && input_[pos_ + 1] == '/') {
            size_t scan = pos_ + 2;
            if (scan < input_.size() && (std::isalpha(input_[scan]) || input_[scan] == '_')) {
                std::string name;
                while (scan < input_.size() && (std::isalnum(input_[scan]) || input_[scan] == '_'))
                    name += input_[scan++];
                if (scan < input_.size() && input_[scan] == '>') {
                    pos_ = scan + 1;
                    return {TokType::REGION_END, name, start};
                }
            }
        }
        // Check for <ident ...> (region start, possibly with attrs) — but NOT <<, <=.
        // Only a well-formed anchor body counts: a name, then `key="value"` pairs and
        // `clause(label)` peer clauses (quote-aware, so `>` inside a value does not end it).
        // Anything else is `<` (comparison / dependency): `:: a <b & distance(a, b) > 1` and
        // `[] <n:[…]` must not be swallowed up to some later `>`.
        if (pos_ + 1 < input_.size() && (std::isalpha(input_[pos_ + 1]) || input_[pos_ + 1] == '_')) {
            size_t end = 0;
            if (scan_region_anchor_body(pos_ + 1, &end)) {
                std::string content(input_.data() + pos_ + 1, end - pos_ - 1);
                pos_ = end + 1;
                return {TokType::REGION_START, content, start};
            }
        }
    }

    // Two-character tokens
    if (pos_ + 1 < input_.size()) {
        char c2 = input_[pos_ + 1];
        if (c == '!' && c2 == '=') { pos_ += 2; return {TokType::NEQ,     "!=", start}; }
        if (c == '!' && c2 == '>') { pos_ += 2; return {TokType::BANG_GT,  "!>", start}; }
        if (c == '!' && c2 == '<') { pos_ += 2; return {TokType::BANG_LT,  "!<", start}; }
        if (c == '<' && c2 == '<') { pos_ += 2; return {TokType::LTLT,     "<<", start}; }
        if (c == '>' && c2 == '>') { pos_ += 2; return {TokType::GTGT,     ">>", start}; }
        if (c == '<' && c2 == '=') { pos_ += 2; return {TokType::LTE,      "<=", start}; }
        if (c == '>' && c2 == '=') { pos_ += 2; return {TokType::GTE,      ">=", start}; }
        if (c == ':' && c2 == ':') { pos_ += 2; return {TokType::DCOLON,   "::", start}; }
    }

    // Single-character tokens
    switch (c) {
        case '[': ++pos_; return {TokType::LBRACKET, "[", start};
        case ']': ++pos_; return {TokType::RBRACKET, "]", start};
        case '(': ++pos_; return {TokType::LPAREN,   "(", start};
        case ')': ++pos_; return {TokType::RPAREN,   ")", start};
        case '=': ++pos_; return {TokType::EQ,       "=", start};
        case '<': ++pos_; return {TokType::LT,       "<", start};
        case '>': ++pos_; return {TokType::GT,       ">", start};
        case '&': ++pos_; return {TokType::AMP,      "&", start};
        case '|': ++pos_; return {TokType::PIPE,     "|", start};
        case ':': ++pos_; return {TokType::COLON,    ":", start};
        case '@': ++pos_; return {TokType::AT,       "@", start};
        case ';': ++pos_; return {TokType::SEMI,     ";", start};
        case '.': ++pos_; return {TokType::DOT,      ".", start};
        case ',': ++pos_; return {TokType::COMMA,    ",", start};
        case '{': ++pos_; return {TokType::LBRACE,   "{", start};
        case '}': ++pos_; return {TokType::RBRACE,   "}", start};
        case '+': ++pos_; return {TokType::PLUS,     "+", start};
        case '*': ++pos_; return {TokType::STAR,     "*", start};
        case '?': ++pos_; return {TokType::QUESTION, "?", start};
        case '%': ++pos_; return {TokType::PERCENT,  "%", start};
        case '!': ++pos_; return {TokType::BANG,     "!", start};
        default: break;
    }

    // String literal "..."
    if (c == '"') {
        ++pos_;
        std::string text, raw;
        while (pos_ < input_.size() && input_[pos_] != '"') {
            if (input_[pos_] == '\\' && pos_ + 1 < input_.size()) {
                raw += input_[pos_];
                ++pos_;
                raw += input_[pos_];
                text += input_[pos_++];
            } else {
                raw += input_[pos_];
                text += input_[pos_++];
            }
        }
        if (pos_ < input_.size()) ++pos_; // consume closing quote
        Token tk{TokType::STRING, text, start};
        tk.raw = std::move(raw);
        return tk;
    }

    // Regex literal /pattern/
    if (c == '/') {
        ++pos_;
        std::string text;
        while (pos_ < input_.size() && input_[pos_] != '/') {
            if (input_[pos_] == '\\' && pos_ + 1 < input_.size()) {
                text += input_[pos_++];
                text += input_[pos_++];
            } else {
                text += input_[pos_++];
            }
        }
        if (pos_ < input_.size()) ++pos_; // consume closing /
        return {TokType::REGEX, text, start};
    }

    // Number
    if (std::isdigit(c) || (c == '-' && pos_ + 1 < input_.size() && std::isdigit(input_[pos_+1]))) {
        std::string text;
        if (c == '-') text += input_[pos_++];
        while (pos_ < input_.size() && std::isdigit(input_[pos_]))
            text += input_[pos_++];
        return {TokType::NUMBER, text, start};
    }

    // Identifier (letters, digits, underscore, hyphen after first char).
    // Composite "namespace/key" paths: one or more segments separated by '/'
    // (e.g. feats/Definite). Leading '/' is still handled as regex below.
    if (std::isalpha(c) || c == '_') {
        std::string text;
        auto read_segment = [&]() {
            while (pos_ < input_.size() &&
                   (std::isalnum(static_cast<unsigned char>(input_[pos_])) ||
                    input_[pos_] == '_' || input_[pos_] == '-' || input_[pos_] == '#'))
                text += input_[pos_++];
        };
        read_segment();
        while (pos_ < input_.size() && input_[pos_] == '/') {
            if (pos_ + 1 >= input_.size()) break;
            char nc = input_[pos_ + 1];
            if (!std::isalnum(static_cast<unsigned char>(nc)) && nc != '_') break;
            text += '/';
            ++pos_;
            read_segment();
        }
        // head#A (HeadAttr) is storage only: the engine reads [P] > [C] through it
        if (text.rfind("head#", 0) == 0)
            throw std::runtime_error("Unknown attribute '" + text + "' at position " + std::to_string(start)
                                     + " (the head's attributes: [X] > [Y] or parent [X])");
        return {TokType::IDENT, text, start};
    }

    throw std::runtime_error(std::string("Unexpected character '") + c +
                             "' at position " + std::to_string(start));
}

Token Lexer::peek() {
    if (!has_lookahead_) {
        lookahead_ = next();
        has_lookahead_ = true;
    }
    return lookahead_;
}

void Lexer::consume() {
    if (has_lookahead_) {
        has_lookahead_ = false;
    } else {
        next();
    }
}

bool Lexer::at_end() const {
    return !has_lookahead_ && pos_ >= input_.size();
}

Token Lexer::expect(TokType type) {
    Token t = next();
    if (t.type != type) {
        throw std::runtime_error(std::string("Expected ") +
            toktype_name(type) + " but got '" +
            t.text + "' at position " + std::to_string(t.pos));
    }
    return t;
}

} // namespace pando
