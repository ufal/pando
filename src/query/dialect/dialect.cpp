// One entry point for pando's query dialects (the CLI's --cql, the server's "dialect"): see dialect.h.
#include "query/dialect/dialect.h"

#include "corpus/corpus.h"
#include "query/dialect/cwb/cwb_translate.h"
#include "query/dialect/manatee/manatee_translate.h"
#include "query/dialect/pmltq/pmltq_translate.h"
#include "query/dialect/tiger/tiger_translate.h"

#include <cctype>
#include <exception>
#include <iterator>
#include <stdexcept>
#include <vector>

namespace pando {

std::string normalize_dialect(const std::string& name) {
    std::string d;
    for (char c : name)
        if (!std::isspace(static_cast<unsigned char>(c))) d += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (d.empty() || d == "native" || d == "pando" || d == "pando-cql") return "native";
    if (d == "cwb" || d == "cwb-cql" || d == "cqp") return "cwb";
    if (d == "pmltq" || d == "pml-tq") return "pmltq";
    if (d == "tiger" || d == "tigersearch") return "tiger";
    if (d == "manatee" || d == "manatee-cql" || d == "sketch-cql") return "manatee";
    return "";
}

std::string take_language_prefix(std::string& text) {
    static const char* const words[] = {"PANDO", "CWB", "CQP", "PMLTQ", "PML-TQ", "TIGER", "MANATEE"};
    size_t b = 0;
    while (b < text.size() && std::isspace(static_cast<unsigned char>(text[b]))) ++b;
    size_t e = b;
    while (e < text.size() && !std::isspace(static_cast<unsigned char>(text[e]))) ++e;
    if (e == text.size()) return ""; // nothing after the word
    const std::string word = text.substr(b, e - b);
    for (const char* w : words) {
        if (word != w) continue;
        size_t r = e;
        while (r < text.size() && std::isspace(static_cast<unsigned char>(text[r]))) ++r;
        if (r == text.size()) return "";
        text.erase(0, r);
        return normalize_dialect(word);
    }
    return "";
}

namespace {

/// The offset of the first `;` outside quotes and brackets (std::string::npos when none).
size_t top_level_semicolon(const std::string& text) {
    int depth = 0;
    char quote = 0;
    for (size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (quote) {
            if (c == '\\') ++i;
            else if (c == quote) quote = 0;
        } else if (c == '"' || c == '\'') quote = c;
        else if (c == '[' || c == '(' || c == '{') ++depth;
        else if (c == ']' || c == ')' || c == '}') --depth;
        else if (c == ';' && depth == 0) return i;
    }
    return std::string::npos;
}

/// The statements of a program, split at its top-level `;`, trimmed, empty ones left out.
std::vector<std::string> split_statements(const std::string& text) {
    std::vector<std::string> out;
    std::string rest = text;
    while (true) {
        const size_t cut = top_level_semicolon(rest);
        std::string piece = rest.substr(0, cut);
        const size_t a = piece.find_first_not_of(" \t\r\n");
        if (a != std::string::npos) {
            const size_t b = piece.find_last_not_of(" \t\r\n");
            out.push_back(piece.substr(a, b - a + 1));
        }
        if (cut == std::string::npos) break;
        rest = rest.substr(cut + 1);
    }
    return out;
}

/// A statement whose first word is a pando command (`count`, `sort`, `filter`, …).
bool is_command_statement(const std::string& piece) {
    size_t e = 0;
    while (e < piece.size() && (std::isalnum(static_cast<unsigned char>(piece[e])) || piece[e] == '_')) ++e;
    if (e == 0) return false;
    // `count = […]` names a query
    size_t r = e;
    while (r < piece.size() && std::isspace(static_cast<unsigned char>(piece[r]))) ++r;
    if (r < piece.size() && piece[r] == '=' && (r + 1 >= piece.size() || piece[r + 1] != '=')) return false;
    return Parser::is_command_keyword(piece.substr(0, e));
}

/// `Name = <query>` → the query (trimmed), `name` set; otherwise the statement itself.
std::string named_query_body(const std::string& piece, std::string& name) {
    name.clear();
    size_t e = 0;
    if (e < piece.size() && (std::isalpha(static_cast<unsigned char>(piece[e])) || piece[e] == '_')) {
        ++e;
        while (e < piece.size() && (std::isalnum(static_cast<unsigned char>(piece[e])) || piece[e] == '_')) ++e;
    }
    if (e == 0) return piece;
    size_t r = e;
    while (r < piece.size() && std::isspace(static_cast<unsigned char>(piece[r]))) ++r;
    if (r >= piece.size() || piece[r] != '=' || (r + 1 < piece.size() && piece[r + 1] == '=')) return piece;
    std::string body = piece.substr(r + 1);
    const size_t a = body.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) throw std::runtime_error("`" + piece + "`: the query after '=' is missing");
    name = piece.substr(0, e);
    return body.substr(a);
}

// An attribute renamed throughout a program: in conditions (also of related and counted words),
// in comparisons between named tokens and in a command's fields (`tag`, `v.tag`).
void rename_attr(const ConditionPtr& c, const std::string& from, const std::string& to) {
    if (!c) return;
    if (c->is_leaf && c->leaf.attr == from) c->leaf.attr = to;
    rename_attr(c->left, from, to);
    rename_attr(c->right, from, to);
    rename_attr(c->nested_conditions, from, to);
    rename_attr(c->count_filter, from, to);
}

void rename_attr(TokenQuery& q, const std::string& from, const std::string& to) {
    for (auto& t : q.tokens) rename_attr(t.conditions, from, to);
    for (auto& f : q.global_alignment_filters) {
        if (f.attr1 == from) f.attr1 = to;
        if (f.attr2 == from) f.attr2 = to;
    }
}

void rename_attr(Program& prog, const std::string& from, const std::string& to) {
    for (auto& st : prog) {
        rename_attr(st.query, from, to);
        rename_attr(st.target_query, from, to);
        for (auto& f : st.command.fields) {
            if (f == from) f = to;
            else if (f.size() > from.size() && f.compare(f.size() - from.size() - 1, std::string::npos, "." + from) == 0)
                f = f.substr(0, f.size() - from.size()) + to;
        }
    }
}

Program parse_dialect_only(const std::string& text, const std::string& d, ParserOptions parse_opts,
                           const Corpus* corpus, int debug_level, std::string* trace_out) {
    if (d == "cwb") return translate_cwb_program(text, debug_level, trace_out);
    if (d == "pmltq") {
        Program p = translate_pmltq_program(text, debug_level, trace_out);
        // Not PML-TQ itself but the UD treebanks on LINDAT's PML-TQ server call the UD part of speech
        // `tag`: so that queries written for them run here, a corpus with `upos` and no `tag` of its
        // own reads `tag` as `upos` (a fallback only; a corpus's own `tag` always wins)
        if (corpus && !corpus->has_attr("tag") && corpus->has_attr("upos"))
            rename_attr(p, "tag", "upos");
        return p;
    }
    if (d == "manatee") return translate_manatee_program(text, debug_level, trace_out);
    if (d == "tiger") {
        // the node layer's category attribute: `cat` (TEITOK) when the corpus has it, else `type`
        std::string cat_attr = "type";
        if (corpus && corpus->has_structure("node") && corpus->structure("node").has_region_attr("cat")
            && !corpus->structure("node").has_region_attr("type"))
            cat_attr = "cat";
        return translate_tiger_program(text, debug_level, trace_out, parse_opts, cat_attr);
    }
    throw std::runtime_error("Unknown query language '" + d + "'");
}

} // namespace

Program parse_in_dialect(const std::string& text, const std::string& dialect, ParserOptions parse_opts,
                         const Corpus* corpus, int debug_level, std::string* trace_out) {
    const std::string d = normalize_dialect(dialect);
    if (d == "native") {
        Parser parser(text, parse_opts);
        return parser.parse();
    }
    if (d.empty())
        throw std::runtime_error("Unknown query language '" + dialect + "' (pando knows pando-cql, cwb-cql, manatee-cql, pmltq, tiger)");
    try {
        return parse_dialect_only(text, d, parse_opts, corpus, debug_level, trace_out);
    } catch (const std::runtime_error&) {
        // a program: its query statements (`[…]`, `Name = […]`) in the dialect, its commands
        // (`count …`, `sort …`, `filter …`) as pando CQL; when the first statement is not a
        // query of the dialect either, the whole text's error
        const std::exception_ptr whole = std::current_exception();
        if (top_level_semicolon(text) == std::string::npos) throw;
        bool first_ok = false;
        try {
            Program prog;
            for (const std::string& piece : split_statements(text)) {
                if (is_command_statement(piece)) {
                    Parser p(piece, parse_opts);
                    Program steps = p.parse();
                    prog.insert(prog.end(), std::make_move_iterator(steps.begin()), std::make_move_iterator(steps.end()));
                } else {
                    std::string name;
                    const std::string query = named_query_body(piece, name);
                    Program q = parse_dialect_only(query, d, parse_opts, corpus, debug_level, trace_out);
                    if (q.empty() || !q.back().has_query)
                        throw std::runtime_error("not a query: " + piece);
                    if (!name.empty()) {
                        q.back().name = name;
                        q.back().source = query;
                    }
                    prog.insert(prog.end(), std::make_move_iterator(q.begin()), std::make_move_iterator(q.end()));
                }
                first_ok = true;
            }
            return prog;
        } catch (const std::runtime_error&) {
            if (first_ok) throw;   // a program after all: the error of its statement
            std::rethrow_exception(whole);
        }
    }
}

} // namespace pando
