#include "query/lexer.h"
#include <stdexcept>
#include <cctype>

namespace manatree {

Lexer::Lexer(const std::string& input) : input_(input) {}

void Lexer::skip_whitespace() {
    while (pos_ < input_.size() && (input_[pos_] == ' ' || input_[pos_] == '\t' ||
           input_[pos_] == '\r' || input_[pos_] == '\n'))
        ++pos_;
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
        default: break;
    }

    // String literal "..."
    if (c == '"') {
        ++pos_;
        std::string text;
        while (pos_ < input_.size() && input_[pos_] != '"') {
            if (input_[pos_] == '\\' && pos_ + 1 < input_.size()) {
                ++pos_;
                text += input_[pos_++];
            } else {
                text += input_[pos_++];
            }
        }
        if (pos_ < input_.size()) ++pos_; // consume closing quote
        return {TokType::STRING, text, start};
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

    // Identifier (letters, digits, underscore, hyphen after first char)
    if (std::isalpha(c) || c == '_') {
        std::string text;
        while (pos_ < input_.size() &&
               (std::isalnum(input_[pos_]) || input_[pos_] == '_' || input_[pos_] == '-'))
            text += input_[pos_++];
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

} // namespace manatree
