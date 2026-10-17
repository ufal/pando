// One entry point for pando's query dialects (the CLI's --cql, the server's "dialect"): see dialect.h.
#include "query/dialect/dialect.h"

#include "corpus/corpus.h"
#include "query/dialect/cwb/cwb_translate.h"
#include "query/dialect/pmltq/pmltq_translate.h"
#include "query/dialect/tiger/tiger_translate.h"

#include <cctype>
#include <exception>
#include <iterator>
#include <stdexcept>

namespace pando {

std::string normalize_dialect(const std::string& name) {
    std::string d;
    for (char c : name)
        if (!std::isspace(static_cast<unsigned char>(c))) d += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (d.empty() || d == "native" || d == "pando" || d == "pando-cql") return "native";
    if (d == "cwb" || d == "cwb-cql" || d == "cqp") return "cwb";
    if (d == "pmltq" || d == "pml-tq") return "pmltq";
    if (d == "tiger" || d == "tigersearch") return "tiger";
    return "";
}

std::string take_language_prefix(std::string& text) {
    static const char* const words[] = {"PANDO", "CWB", "CQP", "PMLTQ", "PML-TQ", "TIGER"};
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

Program parse_dialect_only(const std::string& text, const std::string& d, ParserOptions parse_opts,
                           const Corpus* corpus, int debug_level, std::string* trace_out) {
    if (d == "cwb") return translate_cwb_program(text, debug_level, trace_out);
    if (d == "pmltq") return translate_pmltq_program(text, debug_level, trace_out);
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
        throw std::runtime_error("Unknown query language '" + dialect + "' (pando knows pando-cql, cwb-cql, pmltq, tiger)");
    try {
        return parse_dialect_only(text, d, parse_opts, corpus, debug_level, trace_out);
    } catch (const std::runtime_error&) {
        // a query in the dialect followed by pando's own steps (`a-node [...]; freq by lemma`): the
        // part up to the first top-level `;` in the dialect, the rest as pando CQL; when that does
        // not parse either, the whole text's error
        const std::exception_ptr whole = std::current_exception();
        const size_t cut = top_level_semicolon(text);
        if (cut == std::string::npos) throw;
        try {
            Program prog = parse_dialect_only(text.substr(0, cut), d, parse_opts, corpus, debug_level, trace_out);
            Parser rest(text.substr(cut + 1), parse_opts);
            Program steps = rest.parse();
            prog.insert(prog.end(), std::make_move_iterator(steps.begin()), std::make_move_iterator(steps.end()));
            return prog;
        } catch (const std::runtime_error&) {
            std::rethrow_exception(whole);
        }
    }
}

} // namespace pando
