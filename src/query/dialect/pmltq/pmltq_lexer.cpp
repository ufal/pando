#include "query/dialect/pmltq/pmltq_lexer.h"

#include <cctype>
#include <stdexcept>
#include <vector>

namespace pando::pmltq {

namespace {

bool is_ident_start(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
           static_cast<unsigned char>(c) >= 0x80;
}

bool is_ident_cont(char c) {
    return is_ident_start(c) || (c >= '0' && c <= '9') || c == '-' || c == '_';
}

} // namespace

PmltqLexer::PmltqLexer(std::string_view input) : s_(input) {}

void PmltqLexer::skip_pre_token() {
    const size_t n = s_.size();
    while (i_ < n) {
        unsigned char c = static_cast<unsigned char>(s_[i_]);
        if (std::isspace(c)) {
            ++i_;
            continue;
        }
        if (c == '#') {
            while (i_ < n && s_[i_] != '\n')
                ++i_;
            continue;
        }
        break;
    }
}

PmltqToken PmltqLexer::read_next() {
    skip_pre_token();
    const size_t n = s_.size();
    if (i_ >= n) {
        PmltqToken t;
        t.type = PmltqTok::END;
        t.byte_offset = n;
        return t;
    }

    const size_t start = i_;
    const char c = s_[i_];

    auto make = [&](PmltqTok ty, size_t len, std::string txt = {}) -> PmltqToken {
        PmltqToken tok;
        tok.type = ty;
        tok.byte_offset = start;
        tok.text = txt.empty() ? std::string(s_.substr(start, len)) : std::move(txt);
        i_ = start + len;
        return tok;
    };

    // String "..." or '...'
    if (c == '"' || c == '\'') {
        const char q = c;
        ++i_;
        std::string out;
        while (i_ < n) {
            char d = s_[i_++];
            if (d == '\\' && i_ < n) {
                out += s_[i_++];
                continue;
            }
            if (d == q)
                return make(PmltqTok::STRING, i_ - start, std::move(out));
            out += d;
        }
        throw std::runtime_error("Unterminated string literal starting at byte " + std::to_string(start));
    }

    // Number (optional leading -)
    if (c == '-' && i_ + 1 < n && std::isdigit(static_cast<unsigned char>(s_[i_ + 1]))) {
        size_t j = i_ + 1;
        while (j < n && std::isdigit(static_cast<unsigned char>(s_[j])))
            ++j;
        if (j + 1 < n && s_[j] == '.' && std::isdigit(static_cast<unsigned char>(s_[j + 1]))) {
            ++j;
            while (j < n && std::isdigit(static_cast<unsigned char>(s_[j])))
                ++j;
        }
        if (j < n && (s_[j] == 'e' || s_[j] == 'E')) {
            ++j;
            if (j < n && (s_[j] == '+' || s_[j] == '-'))
                ++j;
            while (j < n && std::isdigit(static_cast<unsigned char>(s_[j])))
                ++j;
        }
        return make(PmltqTok::NUMBER, j - start);
    }

    if (std::isdigit(static_cast<unsigned char>(c)) || (c == '.' && i_ + 1 < n &&
                                                        std::isdigit(static_cast<unsigned char>(s_[i_ + 1])))) {
        size_t j = i_;
        while (j < n && std::isdigit(static_cast<unsigned char>(s_[j])))
            ++j;
        if (j + 1 < n && s_[j] == '.' && std::isdigit(static_cast<unsigned char>(s_[j + 1]))) {
            ++j;
            while (j < n && std::isdigit(static_cast<unsigned char>(s_[j])))
                ++j;
        }
        if (j < n && (s_[j] == 'e' || s_[j] == 'E')) {
            ++j;
            if (j < n && (s_[j] == '+' || s_[j] == '-'))
                ++j;
            while (j < n && std::isdigit(static_cast<unsigned char>(s_[j])))
                ++j;
        }
        return make(PmltqTok::NUMBER, j - start);
    }

    // Identifier (includes NODE_TYPE-like `a-node`)
    if (is_ident_start(c)) {
        size_t j = i_ + 1;
        while (j < n && is_ident_cont(s_[j]))
            ++j;
        return make(PmltqTok::IDENT, j - start);
    }

    // Operators (longest match first)
    if (c == '>' && i_ + 1 < n && s_[i_ + 1] == '>')
        return make(PmltqTok::GTGT, 2);
    if (c == ':' && i_ + 1 < n && s_[i_ + 1] == '=')
        return make(PmltqTok::COLON_EQ, 2);
    if (c == '>' && i_ + 1 < n && s_[i_ + 1] == '=')
        return make(PmltqTok::GTE, 2);
    if (c == '<' && i_ + 1 < n && s_[i_ + 1] == '=')
        return make(PmltqTok::LTE, 2);
    if (c == '!' && i_ + 1 < n && s_[i_ + 1] == '=')
        return make(PmltqTok::NEQ, 2);
    if (c == '=' && i_ + 1 < n && s_[i_ + 1] == '=')
        return make(PmltqTok::EQEQ, 2);
    if (c == '-' && i_ + 1 < n && s_[i_ + 1] == '>')
        return make(PmltqTok::ARROW, 2);
    if (c == '.' && i_ + 1 < n && s_[i_ + 1] == '.')
        return make(PmltqTok::DOTDOT, 2);

    switch (c) {
    case '=':
        return make(PmltqTok::EQ, 1);
    case '<':
        return make(PmltqTok::LT, 1);
    case '>':
        return make(PmltqTok::GT, 1);
    case '!':
        return make(PmltqTok::BANG, 1);
    case '-':
        return make(PmltqTok::MINUS, 1);
    case '|':
        return make(PmltqTok::PIPE, 1);
    case '&':
        return make(PmltqTok::AMP, 1);
    case '+':
        return make(PmltqTok::PLUS, 1);
    case '*':
        return make(PmltqTok::STAR, 1);
    case '?':
        return make(PmltqTok::QMARK, 1);
    case ':':
        return make(PmltqTok::COLON, 1);
    case ';':
        return make(PmltqTok::SEMI, 1);
    case ',':
        return make(PmltqTok::COMMA, 1);
    case '.':
        return make(PmltqTok::DOT, 1);
    case '(':
        return make(PmltqTok::LPAREN, 1);
    case ')':
        return make(PmltqTok::RPAREN, 1);
    case '[':
        return make(PmltqTok::LBRACKET, 1);
    case ']':
        return make(PmltqTok::RBRACKET, 1);
    case '{':
        return make(PmltqTok::LBRACE, 1);
    case '}':
        return make(PmltqTok::RBRACE, 1);
    case '/':
        return make(PmltqTok::SLASH, 1);
    case '\\':
        return make(PmltqTok::BACKSLASH, 1);
    case '$':
        return make(PmltqTok::DOLLAR, 1);
    case '@':
        return make(PmltqTok::AT, 1);
    case '^':
        return make(PmltqTok::CARET, 1);
    case '~':
        return make(PmltqTok::TILDE, 1);
    case '%':
        return make(PmltqTok::PERCENT, 1);
    default:
        return make(PmltqTok::INVALID, 1);
    }
}

PmltqToken PmltqLexer::peek() {
    if (!peeked_) {
        peeked_ = read_next();
    }
    return *peeked_;
}

PmltqToken PmltqLexer::consume() {
    if (peeked_) {
        PmltqToken t = *peeked_;
        peeked_.reset();
        return t;
    }
    return read_next();
}

std::vector<PmltqToken> tokenize_pmltq_all(std::string_view input) {
    PmltqLexer lx(input);
    std::vector<PmltqToken> out;
    for (;;) {
        PmltqToken t = lx.consume();
        out.push_back(t);
        if (t.type == PmltqTok::END)
            break;
    }
    return out;
}

} // namespace pando::pmltq
