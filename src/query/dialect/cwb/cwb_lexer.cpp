#include "query/dialect/cwb/cwb_lexer.h"

#include <cctype>
#include <stdexcept>
#include <unordered_map>

namespace pando::cwb {

namespace {

const std::unordered_map<std::string, CwbTok>* keyword_map() {
    static const std::unordered_map<std::string, CwbTok> m = {
        {"exit", CwbTok::EXIT_SYM},
        {"cat", CwbTok::CAT_SYM},
        {"define", CwbTok::DEFINE_SYM},
        {"def", CwbTok::DEFINE_SYM},
        {"diff", CwbTok::DIFF_SYM},
        {"difference", CwbTok::DIFF_SYM},
        {"discard", CwbTok::DISCARD_SYM},
        {"inter", CwbTok::INTER_SYM},
        {"intersect", CwbTok::INTER_SYM},
        {"intersection", CwbTok::INTER_SYM},
        {"join", CwbTok::JOIN_SYM},
        {"subset", CwbTok::SUBSET_SYM},
        {"left", CwbTok::LEFT_SYM},
        {"right", CwbTok::RIGHT_SYM},
        {"save", CwbTok::SAVE_SYM},
        {"show", CwbTok::SHOW_SYM},
        {"cd", CwbTok::CD_SYM},
        {"group", CwbTok::GROUP_SYM},
        {"where", CwbTok::WHERE_SYM},
        {"within", CwbTok::WITHIN_SYM},
        {"with", CwbTok::WITH_SYM},
        {"without", CwbTok::WITHOUT_SYM},
        {"delete", CwbTok::DELETE_SYM},
        {"expand", CwbTok::EXPAND_SYM},
        {"to", CwbTok::TO_SYM},
        {"set", CwbTok::SET_SYM},
        {"source", CwbTok::EXEC_SYM},
        {"cut", CwbTok::CUT_SYM},
        {"info", CwbTok::INFO_SYM},
        {"meet", CwbTok::MEET_SYM},
        {"union", CwbTok::UNION_SYM},
        {"MU", CwbTok::MU_SYM},
        {"TAB", CwbTok::TAB_SYM},
        {"sort", CwbTok::SORT_SYM},
        {"count", CwbTok::COUNT_SYM},
        {"by", CwbTok::BY_SYM},
        {"foreach", CwbTok::FOREACH_SYM},
        {"on", CwbTok::ON_SYM},
        {"yes", CwbTok::YES_SYM},
        {"off", CwbTok::OFF_SYM},
        {"no", CwbTok::NO_SYM},
        {"asc", CwbTok::ASC_SYM},
        {"ascending", CwbTok::ASC_SYM},
        {"desc", CwbTok::DESC_SYM},
        {"descending", CwbTok::DESC_SYM},
        {"reverse", CwbTok::REVERSE_SYM},
        {"sleep", CwbTok::SLEEP_SYM},
        {"reduce", CwbTok::REDUCE_SYM},
        {"maximal", CwbTok::MAXIMAL_SYM},
        {"size", CwbTok::SIZE_SYM},
        {"dump", CwbTok::DUMP_SYM},
        {"undump", CwbTok::UNDUMP_SYM},
        {"tabulate", CwbTok::TABULATE_SYM},
        {"not", CwbTok::NOT_SYM},
        {"contains", CwbTok::CONTAINS_SYM},
        {"matches", CwbTok::MATCHES_SYM},
        {"unlock", CwbTok::UNLOCK_SYM},
        {"user", CwbTok::USER_SYM},
        {"host", CwbTok::HOST_SYM},
        {"macro", CwbTok::MACRO_SYM},
        {"randomize", CwbTok::RANDOMIZE_SYM},
        {"from", CwbTok::FROM_SYM},
        {"inclusive", CwbTok::INCLUSIVE_SYM},
        {"exclusive", CwbTok::EXCLUSIVE_SYM},
        {"NULL", CwbTok::NULL_SYM},
        {"match", CwbTok::FIELD},
        {"matchend", CwbTok::FIELD},
        {"target", CwbTok::FIELD},
        {"collocate", CwbTok::FIELD},
        {"keyword", CwbTok::FIELD},
    };
    return &m;
}

bool is_id_start(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
           static_cast<unsigned char>(c) >= 0x80;
}

bool is_id_cont(char c) {
    return is_id_start(c) || (c >= '0' && c <= '9') || c == '-' || c == '_';
}

} // namespace

std::string CwbLexer::strip_quotes(std::string_view raw) {
    if (raw.size() < 2)
        return std::string(raw);
    char d = raw[0];
    if ((d != '"' && d != '\'') || raw.back() != d)
        return std::string(raw);
    std::string out;
    out.reserve(raw.size() - 2);
    for (std::size_t i = 1; i + 1 < raw.size();) {
        char c = raw[i];
        if (c == '\\' && i + 1 < raw.size() - 1) {
            out.push_back(raw[i + 1]);
            i += 2;
            continue;
        }
        if (c == d && i + 1 < raw.size() && raw[i + 1] == d) {
            out.push_back(d);
            i += 2;
            continue;
        }
        out.push_back(c);
        ++i;
    }
    return out;
}

void CwbLexer::skip_ws_and_comments() {
    while (i_ < s_.size()) {
        char c = s_[i_];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            ++i_;
            continue;
        }
        if (c == '#') {
            while (i_ < s_.size() && s_[i_] != '\n')
                ++i_;
            continue;
        }
        break;
    }
}

char CwbLexer::peek_ch(std::size_t j) const {
    std::size_t p = i_ + j;
    return p < s_.size() ? s_[p] : '\0';
}

void CwbLexer::bump_ch(std::size_t n) { i_ += n; }

CwbToken CwbLexer::make_tok(CwbTok k, std::size_t start, std::string t) {
    CwbToken r;
    r.kind = k;
    r.offset = start;
    r.text = std::move(t);
    return r;
}

std::string CwbLexer::read_string_double() {
    std::size_t start = i_;
    bump_ch(1);
    std::string raw;
    raw.push_back('"');
    while (i_ < s_.size()) {
        char c = s_[i_];
        if (c == '\\' && i_ + 1 < s_.size()) {
            raw.push_back(c);
            raw.push_back(s_[i_ + 1]);
            bump_ch(2);
            continue;
        }
        if (c == '"') {
            if (i_ + 1 < s_.size() && s_[i_ + 1] == '"') {
                raw.push_back('"');
                raw.push_back('"');
                bump_ch(2);
                continue;
            }
            raw.push_back('"');
            bump_ch(1);
            return strip_quotes(std::string_view(raw.data(), raw.size()));
        }
        if (c == '\r' || c == '\n')
            throw std::runtime_error("Unterminated string starting near offset " +
                                     std::to_string(start));
        raw.push_back(c);
        bump_ch(1);
    }
    throw std::runtime_error("Unterminated string starting near offset " + std::to_string(start));
}

std::string CwbLexer::read_string_single() {
    std::size_t start = i_;
    bump_ch(1);
    std::string raw;
    raw.push_back('\'');
    while (i_ < s_.size()) {
        char c = s_[i_];
        if (c == '\\' && i_ + 1 < s_.size()) {
            raw.push_back(c);
            raw.push_back(s_[i_ + 1]);
            bump_ch(2);
            continue;
        }
        if (c == '\'') {
            if (i_ + 1 < s_.size() && s_[i_ + 1] == '\'') {
                raw.push_back('\'');
                raw.push_back('\'');
                bump_ch(2);
                continue;
            }
            raw.push_back('\'');
            bump_ch(1);
            return strip_quotes(std::string_view(raw.data(), raw.size()));
        }
        if (c == '\r' || c == '\n')
            throw std::runtime_error("Unterminated string starting near offset " +
                                     std::to_string(start));
        raw.push_back(c);
        bump_ch(1);
    }
    throw std::runtime_error("Unterminated string starting near offset " + std::to_string(start));
}

CwbToken CwbLexer::next() {
    skip_ws_and_comments();
    if (i_ >= s_.size())
        return make_tok(CwbTok::END, i_, {});

    std::size_t start = i_;
    char c = s_[i_];

    // Multi-char operators (longest first)
    if (c == '-' && peek_ch(1) == '>')
        return bump_ch(2), make_tok(CwbTok::IMPLIES, start, "->");
    if (c == ':' && peek_ch(1) == ':')
        return bump_ch(2), make_tok(CwbTok::GCDEL, start, "::");
    if (c == ':' && peek_ch(1) == ']')
        return bump_ch(2), make_tok(CwbTok::LCEND, start, ":]");
    if (c == '!' && peek_ch(1) == '=')
        return bump_ch(2), make_tok(CwbTok::NEQ, start, "!=");
    if (c == '<' && peek_ch(1) == '=')
        return bump_ch(2), make_tok(CwbTok::LET, start, "<=");
    if (c == '>' && peek_ch(1) == '=')
        return bump_ch(2), make_tok(CwbTok::GET, start, ">=");
    if (c == '>' && peek_ch(1) == '>')
        return bump_ch(2), make_tok(CwbTok::APPEND, start, ">>");
    if (c == '<' && peek_ch(1) == '<')
        return bump_ch(2), make_tok(CwbTok::LEFT_APPEND, start, "<<");
    if (c == '+' && peek_ch(1) == '=')
        return bump_ch(2), make_tok(CwbTok::PLUSEQ, start, "+=");
    if (c == '-' && peek_ch(1) == '=')
        return bump_ch(2), make_tok(CwbTok::MINUSEQ, start, "-=");
    if (c == '.' && peek_ch(1) == '.' && peek_ch(2) == '.') {
        bump_ch(3);
        return make_tok(CwbTok::ELLIPSIS, start, "...");
    }
    if (c == '.' && peek_ch(1) == '.') {
        bump_ch(2);
        return make_tok(CwbTok::ELLIPSIS, start, "..");
    }
    if (c == 'R' && peek_ch(1) == 'E' && peek_ch(2) == '(') {
        bump_ch(3);
        return make_tok(CwbTok::RE_PAREN, start, "RE(");
    }
    if (c == '(' && peek_ch(1) == '?') {
        bump_ch(2);
        return make_tok(CwbTok::EXTENSION, start, "(?");
    }
    if (c == '.' && peek_ch(1) == 'E' && peek_ch(2) == 'O' && peek_ch(3) == 'L' && peek_ch(4) == '.' &&
        peek_ch(5) == '.') {
        bump_ch(6);
        return make_tok(CwbTok::EOL_SYM, start, ".EOL.");
    }

    // Strings
    if (c == '"') {
        std::string body = read_string_double();
        return make_tok(CwbTok::STRING, start, std::move(body));
    }
    if (c == '\'') {
        std::string body = read_string_single();
        return make_tok(CwbTok::STRING, start, std::move(body));
    }

    // Variable $name
    if (c == '$') {
        bump_ch(1);
        if (i_ >= s_.size() || !is_id_start(s_[i_]))
            throw std::runtime_error("Malformed variable near offset " + std::to_string(start));
        std::size_t p0 = i_;
        bump_ch(1);
        while (i_ < s_.size() && is_id_cont(s_[i_]))
            bump_ch(1);
        return make_tok(CwbTok::VARIABLE, start, std::string(s_.data() + p0, i_ - p0));
    }

    // Flag %[a-z]+
    if (c == '%') {
        bump_ch(1);
        std::size_t p0 = i_;
        while (i_ < s_.size() && s_[i_] >= 'a' && s_[i_] <= 'z')
            bump_ch(1);
        if (p0 == i_)
            throw std::runtime_error("Empty flag near offset " + std::to_string(start));
        return make_tok(CwbTok::FLAG, start, std::string(s_.data() + p0, i_ - p0));
    }

    // Numbers. A sign only starts a number when a digit follows; otherwise '+'
    // and '-' are operators (`[]+ [x]`, `[]+[x]` are repetition, not "+<num>").
    const bool signed_num = (c == '+' || c == '-') && i_ + 1 < s_.size()
                            && s_[i_ + 1] >= '0' && s_[i_ + 1] <= '9';
    if (signed_num || (c >= '0' && c <= '9')) {
        bool neg = false;
        if (c == '+' || c == '-') {
            neg = (c == '-');
            bump_ch(1);
            if (i_ >= s_.size())
                throw std::runtime_error("Invalid number near offset " + std::to_string(start));
            c = s_[i_];
        }
        if (c < '0' || c > '9')
            throw std::runtime_error("Invalid number near offset " + std::to_string(start));
        std::size_t p0 = i_;
        while (i_ < s_.size() && s_[i_] >= '0' && s_[i_] <= '9')
            bump_ch(1);
        bool is_float = false;
        if (i_ < s_.size() && s_[i_] == '.') {
            is_float = true;
            bump_ch(1);
            while (i_ < s_.size() && s_[i_] >= '0' && s_[i_] <= '9')
                bump_ch(1);
        }
        if (i_ < s_.size() && (s_[i_] == 'e' || s_[i_] == 'E')) {
            is_float = true;
            bump_ch(1);
            if (i_ < s_.size() && (s_[i_] == '+' || s_[i_] == '-'))
                bump_ch(1);
            while (i_ < s_.size() && s_[i_] >= '0' && s_[i_] <= '9')
                bump_ch(1);
        }
        std::string num(s_.data() + p0, i_ - p0);
        if (neg)
            num = "-" + num;
        if (is_float) {
            CwbToken t = make_tok(CwbTok::DOUBLE, start, num);
            t.fval = std::stod(num);
            return t;
        }
        CwbToken t = make_tok(CwbTok::INTEGER, start, num);
        t.ival = std::stoi(num);
        return t;
    }

    // Backtick-quoted ID
    if (c == '`') {
        bump_ch(1);
        std::size_t p0 = i_;
        while (i_ < s_.size() && s_[i_] != '`')
            bump_ch(1);
        if (i_ >= s_.size())
            throw std::runtime_error("Unterminated `...` near offset " + std::to_string(start));
        std::string id(s_.data() + p0, i_ - p0);
        bump_ch(1);
        return make_tok(CwbTok::ID, start, std::move(id));
    }

    // XML / anchor tags
    if (c == '<') {
        static const struct {
            const char* s;
            CwbTok kind;
        } seq[] = {
            {"<match>", CwbTok::ANCHORTAG},
            {"<matchend>", CwbTok::ANCHORTAG},
            {"<target>", CwbTok::ANCHORTAG},
            {"<collocate>", CwbTok::ANCHORTAG},
            {"<keyword>", CwbTok::ANCHORTAG},
            {"</match>", CwbTok::ANCHORENDTAG},
            {"</matchend>", CwbTok::ANCHORENDTAG},
            {"</target>", CwbTok::ANCHORENDTAG},
            {"</collocate>", CwbTok::ANCHORENDTAG},
            {"</keyword>", CwbTok::ANCHORENDTAG},
        };
        for (const auto& e : seq) {
            std::size_t len = std::char_traits<char>::length(e.s);
            if (i_ + len <= s_.size() && s_.compare(i_, len, e.s) == 0) {
                bump_ch(len);
                return make_tok(e.kind, start, std::string(e.s));
            }
        }
        // </id>
        if (peek_ch(1) == '/') {
            bump_ch(2);
            std::size_t p0 = i_;
            while (i_ < s_.size() && is_id_cont(s_[i_]))
                bump_ch(1);
            if (i_ >= s_.size() || s_[i_] != '>')
                throw std::runtime_error("Malformed closing XML tag near offset " +
                                         std::to_string(start));
            std::string name(s_.data() + p0, i_ - p0);
            bump_ch(1);
            return make_tok(CwbTok::TAGEND, start, std::move(name));
        }
        // <id ...> partial TAGSTART
        if (is_id_start(peek_ch(1))) {
            bump_ch(1);
            std::size_t p0 = i_;
            while (i_ < s_.size() && is_id_cont(s_[i_]))
                bump_ch(1);
            std::string name(s_.data() + p0, i_ - p0);
            return make_tok(CwbTok::TAGSTART, start, std::move(name));
        }
        throw std::runtime_error("Unexpected '<' near offset " + std::to_string(start));
    }

    // [::] before [:]
    if (c == '[' && peek_ch(1) == ':' && peek_ch(2) == ':' && peek_ch(3) == ']') {
        bump_ch(4);
        return make_tok(CwbTok::LCMATCHALL, start, "[::]");
    }
    if (c == '[' && peek_ch(1) == ':') {
        bump_ch(2);
        return make_tok(CwbTok::LCSTART, start, "[:");
    }
    if (c == '[' && peek_ch(1) == ']') {
        bump_ch(2);
        return make_tok(CwbTok::MATCHALL, start, "[]");
    }

    // Identifiers and keywords
    if (is_id_start(c)) {
        std::size_t p0 = i_;
        bump_ch(1);
        while (i_ < s_.size() && is_id_cont(s_[i_]))
            bump_ch(1);
        std::string id(s_.data() + p0, i_ - p0);

        // QID id.id
        if (i_ < s_.size() && s_[i_] == '.' && peek_ch(1) != '.' && is_id_start(peek_ch(1))) {
            bump_ch(1);
            std::size_t p1 = i_;
            bump_ch(1);
            while (i_ < s_.size() && is_id_cont(s_[i_]))
                bump_ch(1);
            id += '.';
            id.append(s_.data() + p1, i_ - p1);
            return make_tok(CwbTok::QID, start, std::move(id));
        }

        // label: or fieldlabel:
        if (i_ < s_.size() && s_[i_] == ':' && peek_ch(1) != ':') {
            bump_ch(1);
            static const char* fl[] = {"match", "matchend", "target", "collocate", "keyword"};
            for (const char* f : fl) {
                if (id == f)
                    return make_tok(CwbTok::FIELDLABEL, start, std::move(id));
            }
            return make_tok(CwbTok::LABEL, start, std::move(id));
        }

        auto it = keyword_map()->find(id);
        if (it != keyword_map()->end())
            return make_tok(it->second, start, std::move(id));
        return make_tok(CwbTok::ID, start, std::move(id));
    }

    // Single-char
    bump_ch(1);
    switch (c) {
    case '(':
        return make_tok(CwbTok::LPAREN, start, "(");
    case ')':
        return make_tok(CwbTok::RPAREN, start, ")");
    case '[':
        return make_tok(CwbTok::LBRACK, start, "[");
    case ']':
        return make_tok(CwbTok::RBRACK, start, "]");
    case '{':
        return make_tok(CwbTok::LBRACE, start, "{");
    case '}':
        return make_tok(CwbTok::RBRACE, start, "}");
    case ';':
        return make_tok(CwbTok::SEMI, start, ";");
    case ',':
        return make_tok(CwbTok::COMMA, start, ",");
    case ':':
        return make_tok(CwbTok::COLON, start, ":");
    case '|':
        return make_tok(CwbTok::PIPE, start, "|");
    case '&':
        return make_tok(CwbTok::AMP, start, "&");
    case '!':
        return make_tok(CwbTok::BANG, start, "!");
    case '*':
        return make_tok(CwbTok::STAR, start, "*");
    case '+':
        return make_tok(CwbTok::PLUS, start, "+");
    case '?':
        return make_tok(CwbTok::QUEST, start, "?");
    case '=':
        return make_tok(CwbTok::EQ, start, "=");
    case '<':
        return make_tok(CwbTok::LT, start, "<");
    case '>':
        return make_tok(CwbTok::GT, start, ">");
    case '-':
        return make_tok(CwbTok::MINUS, start, "-");
    case '~':
        return make_tok(CwbTok::TILDE, start, "~");
    case '@':
        return make_tok(CwbTok::AT, start, "@");
    case '/':
        return make_tok(CwbTok::SLASH, start, "/");
    default:
        throw std::runtime_error(std::string("Unexpected character '") + c + "' near offset " +
                                 std::to_string(start));
    }
}

} // namespace pando::cwb
