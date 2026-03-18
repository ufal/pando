#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace manatree {

enum class TokType {
    END,
    LBRACKET,    // [
    RBRACKET,    // ]
    LPAREN,      // (
    RPAREN,      // )
    EQ,          // =
    NEQ,         // !=
    LT,          // <
    GT,          // >
    LTE,         // <=
    GTE,         // >=
    LTLT,        // <<
    GTGT,        // >>
    BANG_LT,     // !<
    BANG_GT,     // !>
    AMP,         // &
    PIPE,        // |
    STRING,      // "..."
    REGEX,       // /pattern/
    IDENT,       // identifier
    NUMBER,      // integer
    COLON,       // :
    DCOLON,      // ::
    AT,          // @
    SEMI,        // ;
    DOT,         // .
    COMMA,       // ,
    LBRACE,      // {
    RBRACE,      // }
    PLUS,        // +
    STAR,        // *
    QUESTION,    // ?
};

inline const char* toktype_name(TokType t) {
    switch (t) {
        case TokType::END:       return "END";
        case TokType::LBRACKET:  return "'['";
        case TokType::RBRACKET:  return "']'";
        case TokType::LPAREN:    return "'('";
        case TokType::RPAREN:    return "')'";
        case TokType::EQ:        return "'='";
        case TokType::NEQ:       return "'!='";
        case TokType::LT:        return "'<'";
        case TokType::GT:        return "'>'";
        case TokType::LTE:       return "'<='";
        case TokType::GTE:       return "'>='";
        case TokType::LTLT:      return "'<<'";
        case TokType::GTGT:      return "'>>'";
        case TokType::BANG_LT:   return "'!<'";
        case TokType::BANG_GT:   return "'!>'";
        case TokType::AMP:       return "'&'";
        case TokType::PIPE:      return "'|'";
        case TokType::STRING:    return "string";
        case TokType::REGEX:     return "regex";
        case TokType::IDENT:     return "identifier";
        case TokType::NUMBER:    return "number";
        case TokType::COLON:     return "':'";
        case TokType::DCOLON:    return "'::'";
        case TokType::AT:        return "'@'";
        case TokType::SEMI:      return "';'";
        case TokType::DOT:       return "'.'";
        case TokType::COMMA:     return "','";
        case TokType::LBRACE:    return "'{'";
        case TokType::RBRACE:    return "'}'";
        case TokType::PLUS:      return "'+'";
        case TokType::STAR:      return "'*'";
        case TokType::QUESTION:  return "'?'";
    }
    return "unknown";
}

struct Token {
    TokType     type = TokType::END;
    std::string text;
    size_t      pos = 0;    // character position in input
};

class Lexer {
public:
    explicit Lexer(const std::string& input);

    Token next();
    Token peek();
    void  consume();
    bool  at_end() const;

    // Consume and verify the expected token type. Throws on mismatch.
    Token expect(TokType type);

private:
    void skip_whitespace();

    std::string input_;
    size_t      pos_ = 0;
    Token       lookahead_;
    bool        has_lookahead_ = false;
};

} // namespace manatree
