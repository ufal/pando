#include "query/dialect/manatee/manatee_lexer.h"

#include <stdexcept>

namespace pando::manatee {

namespace {

bool is_alpha(char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); }
bool is_digit(char c) { return c >= '0' && c <= '9'; }

}  // namespace

bool is_word_like(Tok t) {
    switch (t) {
    case Tok::Word: case Tok::KwMeet: case Tok::KwUnion: case Tok::KwWithin: case Tok::KwContaining:
    case Tok::KwFreq: case Tok::KwWs: case Tok::KwTerm: case Tok::KwSwap: case Tok::KwCcoll:
        return true;
    default:
        return false;
    }
}

const char* tok_name(Tok t) {
    switch (t) {
    case Tok::End: return "the end of the query";
    case Tok::Word: return "a name";
    case Tok::Regexp: return "a quoted string";
    case Tok::Number: case Tok::NNumber: return "a number";
    case Tok::LBracket: return "'['";
    case Tok::RBracket: return "']'";
    case Tok::LParen: return "'('";
    case Tok::RParen: return "')'";
    case Tok::LBrace: return "'{'";
    case Tok::RBrace: return "'}'";
    case Tok::Not: return "'!'";
    case Tok::Eq: return "'='";
    case Tok::Neq: return "'!='";
    case Tok::Leq: return "'<='";
    case Tok::Geq: return "'>='";
    case Tok::LStruct: return "'<'";
    case Tok::RStruct: return "'>'";
    case Tok::And: return "'&'";
    case Tok::Or: return "'|'";
    case Tok::Star: return "'*'";
    case Tok::Plus: return "'+'";
    case Tok::Quest: return "'?'";
    case Tok::Slash: return "'/'";
    case Tok::PosNum: return "'#'";
    case Tok::Comma: return "','";
    case Tok::Colon: return "':'";
    case Tok::Dot: return "'.'";
    case Tok::Teq: return "'~'";
    case Tok::KwMeet: return "'meet'";
    case Tok::KwUnion: return "'union'";
    case Tok::KwWithin: return "'within'";
    case Tok::KwContaining: return "'containing'";
    case Tok::KwWs: return "'ws'";
    case Tok::KwSwap: return "'swap'";
    case Tok::KwCcoll: return "'ccoll'";
    case Tok::KwFreq: return "'f'";
    case Tok::KwTerm: return "'term'";
    }
    return "?";
}

std::vector<Token> lex(const std::string& q) {
    std::vector<Token> out;
    size_t i = 0;
    const size_t n = q.size();
    auto push = [&](Tok k, size_t at) {
        Token t;
        t.kind = k;
        t.pos = at;
        out.push_back(std::move(t));
    };
    while (i < n) {
        const char c = q[i];
        const char next = i + 1 < n ? q[i + 1] : '\0';
        const size_t at = i;
        if (c == ' ' || c == '\t') {
            ++i;
            continue;
        }
        if (is_digit(c) || (c == '-' && is_digit(next))) {
            const bool neg = c == '-';
            if (neg) ++i;
            long long v = 0;
            while (i < n && is_digit(q[i])) v = v * 10 + (q[i++] - '0');
            Token t;
            t.kind = neg ? Tok::NNumber : Tok::Number;
            t.num = neg ? -v : v;
            t.pos = at;
            out.push_back(std::move(t));
            continue;
        }
        if (is_alpha(c) || c == '_') {
            size_t e = i + 1;
            while (e < n && (is_alpha(q[e]) || q[e] == '_' || q[e] == '@' || is_digit(q[e]))) ++e;
            Token t;
            t.text = q.substr(i, e - i);
            t.pos = at;
            const std::string& w = t.text;
            t.kind = w == "f" ? Tok::KwFreq : w == "meet" ? Tok::KwMeet : w == "union" ? Tok::KwUnion
                   : w == "within" ? Tok::KwWithin : w == "containing" ? Tok::KwContaining
                   : w == "ws" ? Tok::KwWs : w == "term" ? Tok::KwTerm : w == "swap" ? Tok::KwSwap
                   : w == "ccoll" ? Tok::KwCcoll : Tok::Word;
            out.push_back(std::move(t));
            i = e;
            continue;
        }
        ++i;
        switch (c) {
        case '"': {
            // to the next quote not escaped by an odd number of backslashes; \" and \\ unescaped
            std::string raw;
            size_t slashes = 0;
            while (i < n && (q[i] != '"' || slashes % 2 == 1)) {
                slashes = q[i] == '\\' ? slashes + 1 : 0;
                raw += q[i++];
            }
            if (i >= n) throw std::runtime_error("Manatee CQL: a string opened at position " + std::to_string(at) + " is not closed");
            ++i;
            Token t;
            t.kind = Tok::Regexp;
            t.pos = at;
            for (size_t k = 0; k < raw.size(); ++k) {
                if (raw[k] == '\\' && k + 1 < raw.size() && (raw[k + 1] == '"' || raw[k + 1] == '\\')) ++k;
                t.text += raw[k];
            }
            out.push_back(std::move(t));
            break;
        }
        case '[': push(Tok::LBracket, at); break;
        case ']': push(Tok::RBracket, at); break;
        case '=': push(Tok::Eq, at); break;
        case '<':
            if (next == '=') { ++i; push(Tok::Leq, at); } else push(Tok::LStruct, at);
            break;
        case '>':
            if (next == '=') { ++i; push(Tok::Geq, at); } else push(Tok::RStruct, at);
            break;
        case '&': push(Tok::And, at); break;
        case '|': push(Tok::Or, at); break;
        case '!':
            if (next == '=') { ++i; push(Tok::Neq, at); } else push(Tok::Not, at);
            break;
        case '(': push(Tok::LParen, at); break;
        case ')': push(Tok::RParen, at); break;
        case '{': push(Tok::LBrace, at); break;
        case '}': push(Tok::RBrace, at); break;
        case '*': push(Tok::Star, at); break;
        case '+': push(Tok::Plus, at); break;
        case '?': push(Tok::Quest, at); break;
        case '/': push(Tok::Slash, at); break;
        case '#': push(Tok::PosNum, at); break;
        case ',': push(Tok::Comma, at); break;
        case ':': push(Tok::Colon, at); break;
        case '.': push(Tok::Dot, at); break;
        case '~': push(Tok::Teq, at); break;
        case ';':
            if (i == n) {   // a `;` closing the query
                push(Tok::End, at);
                return out;
            }
            [[fallthrough]];
        default:
            throw std::runtime_error(std::string("Manatee CQL: unexpected character ") +
                                     (static_cast<unsigned char>(c) >= 32 && static_cast<unsigned char>(c) < 128
                                          ? std::string("'") + c + "' " : std::string()) +
                                     "at position " + std::to_string(at));
        }
    }
    push(Tok::End, n);
    return out;
}

}  // namespace pando::manatee
