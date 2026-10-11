#include "query/dialect/tiger/tiger_translate.h"

#include <cctype>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>

namespace pando {

namespace {

std::string trim(std::string s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.erase(0, 1);
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.pop_back();
    return s;
}

// Split `rest` into two non-empty whitespace-separated tokens (categories; no spaces in token).
static bool two_words(const std::string& rest, std::string* a, std::string* b) {
    std::istringstream iss(rest);
    if (!(iss >> *a)) return false;
    if (!(iss >> *b)) return false;
    std::string extra;
    if (iss >> extra) return false;
    return true;
}

// The macros below tie both nodes to the token `[]`, so they only find a pair that *starts on
// the same word* (pando binds a region anchor to the next token). Real dominance between
// independent nodes needs region-to-region joins: dev/REGION-JOINS-PLAN.md (P2/P5).
static std::string expand_dom(const std::string& a, const std::string& parent_cat, const std::string& child_cat) {
    // Geometric containment: parent span ⊇ child span (Layer A).
    return "p:<node " + a + "=\"" + parent_cat + "\"> c:<node " + a + "=\"" + child_cat
           + "\"> [] :: contains(p, c) = 1";
}

static std::string expand_idom(const std::string& a, const std::string& parent_cat, const std::string& child_cat) {
    // Immediate dominance via .par (same as :: rchild(parent, child)).
    return "p:<node " + a + "=\"" + parent_cat + "\"> c:<node " + a + "=\"" + child_cat
           + "\"> [] :: rchild(p, c) = 1";
}

static std::string expand_cat(const std::string& a, const std::string& cat) {
    // One token hit at the start of a node of this category.
    return "n:<node " + a + "=\"" + cat + "\"> []";
}

// One line of tiger dialect → CQL fragment (no trailing newline required).
static std::string expand_line(const std::string& line_in, const std::string& cat_attr, std::string* err) {
    err->clear();
    std::string line = trim(line_in);
    if (line.empty())
        return "";
    if (line[0] == '#')
        return "";

    static const char kDom[] = "dom ";
    static const char kIdom[] = "idom ";
    static const char kCat[] = "cat ";

    auto starts = [](const std::string& s, const char* pfx) -> bool {
        const std::size_t n = std::strlen(pfx);
        return s.size() >= n && s.compare(0, n, pfx) == 0;
    };

    if (starts(line, kDom)) {
        std::string a, b;
        if (!two_words(line.substr(std::strlen(kDom)), &a, &b)) {
            *err = "tiger: expected: dom <parent_cat> <child_cat> (two words)";
            return "";
        }
        return expand_dom(cat_attr, a, b);
    }
    if (starts(line, kIdom)) {
        std::string a, b;
        if (!two_words(line.substr(std::strlen(kIdom)), &a, &b)) {
            *err = "tiger: expected: idom <parent_cat> <child_cat> (two words)";
            return "";
        }
        return expand_idom(cat_attr, a, b);
    }
    if (starts(line, kCat)) {
        std::string rest = trim(line.substr(std::strlen(kCat)));
        std::istringstream iss(rest);
        std::string cat;
        if (!(iss >> cat) || cat.empty()) {
            *err = "tiger: expected: cat <category>";
            return "";
        }
        std::string extra;
        if (iss >> extra) {
            *err = "tiger: cat takes exactly one category token";
            return "";
        }
        return expand_cat(cat_attr, cat);
    }
    // Pass through as native CQL.
    return line;
}

} // namespace

Program translate_tiger_program(const std::string& input, int debug_level,
                                std::string* trace_out, ParserOptions parse_opts,
                                const std::string& cat_attr) {
    if (trace_out)
        trace_out->clear();

    std::ostringstream expanded;
    std::istringstream in(input);
    std::string line;
    bool first = true;
    while (std::getline(in, line)) {
        std::string e;
        std::string frag = expand_line(line, cat_attr, &e);
        if (!e.empty())
            throw std::runtime_error(e);
        if (frag.empty())
            continue;
        if (!first)
            expanded << ';';
        first = false;
        expanded << frag;
    }
    std::string cql = expanded.str();
    if (cql.empty())
        throw std::runtime_error("tiger: empty program after expansion");

    if (debug_level > 0 && trace_out) {
        *trace_out += "tiger dialect: expanded to native CQL:\n" + cql + "\n";
    }

    Parser parser(cql, parse_opts);
    return parser.parse();
}

} // namespace pando
