#pragma once

// Manatee CQL (Sketch Engine / NoSketch Engine, as KonText sends it): the tokens of Manatee's query language, as
// manatee-open's lexer defines them (query/cqpeval.y, written from that grammar as a specification; no code taken).
// Only double-quoted strings, every one a regular expression ("…" with \" and \\ unescaped, every other backslash
// kept for the regex); words [A-Za-z_][A-Za-z0-9_@]*, some of them keywords; numbers, `-N` a negative number; only
// spaces and tabs between tokens; a `;` closes the query.

#include <string>
#include <vector>

namespace pando::manatee {

enum class Tok {
    End,
    Word,      // attribute / structure / corpus name
    Regexp,    // "…" (text: unescaped)
    Number,    // 12
    NNumber,   // -3
    LBracket, RBracket, LParen, RParen, LBrace, RBrace,
    Not,       // !
    Eq,        // =
    Neq,       // !=
    Leq,       // <=
    Geq,       // >=
    LStruct,   // <
    RStruct,   // >
    And,       // &
    Or,        // |
    Star, Plus, Quest, Slash,
    PosNum,    // #
    Comma, Colon, Dot,
    Teq,       // ~
    KwMeet, KwUnion, KwWithin, KwContaining, KwWs, KwSwap, KwCcoll, KwFreq, KwTerm,
};

struct Token {
    Tok kind = Tok::End;
    std::string text;   // a word or keyword as written, a regexp unescaped
    long long num = 0;  // Number / NNumber
    size_t pos = 0;     // character offset in the query
};

// The tokens of `query` (ending with Tok::End). Throws std::runtime_error at a character Manatee does not accept.
std::vector<Token> lex(const std::string& query);

// The words that are keywords but may stand where a word is expected (attribute and structure names)
bool is_word_like(Tok t);

const char* tok_name(Tok t);

}  // namespace pando::manatee
