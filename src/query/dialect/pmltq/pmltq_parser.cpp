#include "query/dialect/pmltq/pmltq_parser.h"

#include "query/dialect/pmltq/pmltq_lexer.h"

#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

// The query part of PML-TQ after perl-pmltq's tree_query_grammar.txt: selector, conditions
// (joined by `,` / `and`), condition (nested_selector before test), test as or_clause /
// and_clause / not_clause / simple_test, subquery with occurrences, predicate. The `>>` part is
// the small subset pando lowers itself (distinct fields, count(over …)); pando's own steps
// (`; freq by …`) follow a query instead (dialect.cpp).

namespace pando::pmltq {

namespace {

const std::unordered_set<std::string>& relation_words() {
    static const std::unordered_set<std::string> r = {
        "child", "parent", "sibling", "descendant", "ancestor", "same-tree-as", "same-document-as",
        "depth-first-precedes", "depth-first-follows", "order-precedes", "order-follows"};
    return r;
}

class PmltqParser {
public:
    explicit PmltqParser(std::string_view input) : lx_(input) { cur_ = lx_.consume(); }

    PmltqQuery parse_query() {
        if (cur_.type == PmltqTok::END)
            throw std::runtime_error("PMLTQ: empty query (no selectors after comments/whitespace)");
        if (cur_.type == PmltqTok::INVALID) {
            std::ostringstream o;
            o << "PMLTQ: invalid character at byte " << cur_.byte_offset;
            throw std::runtime_error(o.str());
        }

        PmltqQuery q;
        if (cur_.type == PmltqTok::PLUS)
            bump(); // `+` (overlapping) on a lone top-level selector changes nothing
        q.selector = parse_selector();
        while (cur_.type == PmltqTok::SEMI)
            bump();

        if (cur_.type == PmltqTok::GTGT) {
            bump();
            q.has_output = true;
            q.output = parse_output_block();
        }

        while (cur_.type == PmltqTok::SEMI)
            bump();

        if (cur_.type != PmltqTok::END) {
            std::ostringstream o;
            o << "PMLTQ: unexpected token at byte " << cur_.byte_offset
              << " (pando reads one top-level selector, optionally followed by >> distinct / count(over …))";
            throw std::runtime_error(o.str());
        }
        return q;
    }

private:
    PmltqLexer lx_;
    PmltqToken cur_;

    void bump() { cur_ = lx_.consume(); }
    PmltqToken peek() { return lx_.peek(); }
    bool is_word(const char* w) const { return cur_.type == PmltqTok::IDENT && cur_.text == w; }

    [[noreturn]] void throw_expected(const char* what) {
        std::ostringstream o;
        o << "PMLTQ: expected " << what << " at byte " << cur_.byte_offset;
        throw std::runtime_error(o.str());
    }

    void expect(PmltqTok t, const char* what) {
        if (cur_.type != t)
            throw_expected(what);
    }

    // ── selectors ────────────────────────────────────────────────────────

    /// Does a selector start here: `[`, `$name :=` or a node type before `[` / `$`?
    bool selector_starts() {
        if (cur_.type == PmltqTok::LBRACKET)
            return true;
        if (cur_.type == PmltqTok::DOLLAR)
            return false; // `$a` alone is a reference, `$a := [` only after a type or at the top
        if (cur_.type == PmltqTok::IDENT || cur_.type == PmltqTok::STAR) {
            const PmltqTok n = peek().type;
            return n == PmltqTok::LBRACKET || n == PmltqTok::DOLLAR;
        }
        return false;
    }

    /// A relation word (child, descendant, …) before a selector.
    bool relation_starts() {
        if (cur_.type != PmltqTok::IDENT || !relation_words().count(cur_.text))
            return false;
        const PmltqTok n = peek().type;
        return n == PmltqTok::IDENT || n == PmltqTok::LBRACKET || n == PmltqTok::STAR ||
               n == PmltqTok::LBRACE || n == PmltqTok::COLON;
    }

    /// A nested selector (join or subquery) starts here: `+`? relation? selector.
    bool nested_starts() { return cur_.type == PmltqTok::PLUS || relation_starts() || selector_starts(); }

    PmltqSelector parse_selector() {
        PmltqSelector s;
        if (cur_.type == PmltqTok::IDENT || cur_.type == PmltqTok::STAR) {
            const PmltqTok n = peek().type;
            if (n == PmltqTok::LBRACKET || n == PmltqTok::DOLLAR) {
                s.type_name = cur_.type == PmltqTok::STAR ? "*" : cur_.text;
                if (s.type_name == "a-root")
                    s.kind = PmltqSelectorKind::ARoot;
                else if (s.type_name == "tok")
                    s.kind = PmltqSelectorKind::Tok;
                bump();
            }
        }
        if (cur_.type == PmltqTok::DOLLAR) {
            bump();
            if (cur_.type != PmltqTok::IDENT)
                throw_expected("variable name after '$'");
            s.bind_name = cur_.text;
            bump();
            expect(PmltqTok::COLON_EQ, "':=' after the node's name");
            bump();
        }
        expect(PmltqTok::LBRACKET, "'['");
        bump();
        if (cur_.type != PmltqTok::RBRACKET) {
            s.has_cond = true;
            s.cond = parse_conditions();
        }
        expect(PmltqTok::RBRACKET, "']' or ',' between conditions");
        bump();
        return s;
    }

    /// conditions: condition ((`,` | `and`) condition)*, a trailing `,` allowed.
    PmltqCondExpr parse_conditions() {
        PmltqCondExpr list;
        list.kind = PmltqCondKind::And;
        list.children.push_back(parse_condition());
        while (cur_.type == PmltqTok::COMMA || is_word("and")) {
            bump();
            if (cur_.type == PmltqTok::RBRACKET)
                break; // trailing comma
            list.children.push_back(parse_condition());
        }
        if (list.children.size() == 1)
            return std::move(list.children.front());
        return list;
    }

    /// condition: `?` nested selector | nested selector (a node of the match) | test.
    PmltqCondExpr parse_condition() {
        if (cur_.type == PmltqTok::QMARK) {
            const size_t at = cur_.byte_offset;
            bump();
            PmltqCondExpr e = parse_nested(false);
            e.nested->optional = true;
            e.byte_offset = at;
            return e;
        }
        if (nested_starts())
            return parse_nested(false);
        return parse_or();
    }

    /// `+`? relation? selector, as a join (subquery = false) or a subquery.
    PmltqCondExpr parse_nested(bool subquery) {
        PmltqCondExpr e;
        e.kind = PmltqCondKind::Nested;
        e.byte_offset = cur_.byte_offset;
        auto n = std::make_shared<PmltqNested>();
        n->byte_offset = cur_.byte_offset;
        n->subquery = subquery;
        bool overlapping = false;
        if (cur_.type == PmltqTok::PLUS) {
            overlapping = true;
            bump();
        }
        if (relation_starts()) {
            n->relation = cur_.text;
            bump();
            if (cur_.type == PmltqTok::LBRACE) { // descendant{1,3}
                n->bounded_length = true;
                while (cur_.type != PmltqTok::RBRACE && cur_.type != PmltqTok::END)
                    bump();
                expect(PmltqTok::RBRACE, "'}'");
                bump();
            }
            if (cur_.type == PmltqTok::COLON && peek().type == PmltqTok::COLON) { // child::
                bump();
                bump();
            }
        }
        if (!selector_starts())
            throw_expected("a node selector (e.g. a-node [ … ]) after the relation");
        n->node = std::make_shared<PmltqSelector>(parse_selector());
        n->node->overlapping = overlapping;
        if (subquery && n->occurrences.empty())
            n->occurrences.push_back(PmltqOccurrence{1, -1});
        e.nested = std::move(n);
        return e;
    }

    // ── tests ────────────────────────────────────────────────────────────

    PmltqCondExpr combine(PmltqCondKind kind, PmltqCondExpr left, PmltqCondExpr right) {
        if (left.kind == kind) {
            left.children.push_back(std::move(right));
            return left;
        }
        PmltqCondExpr c;
        c.kind = kind;
        c.byte_offset = left.byte_offset;
        c.children.push_back(std::move(left));
        c.children.push_back(std::move(right));
        return c;
    }

    PmltqCondExpr parse_or() {
        PmltqCondExpr left = parse_and();
        while (is_word("or")) {
            bump();
            left = combine(PmltqCondKind::Or, std::move(left), parse_and());
        }
        return left;
    }

    /// and_clause: as in the grammar, `and` after a test takes the next test into the clause, so
    /// `lemma = "x" and child a-node […]` is a subquery (one result per node); `?` ends it.
    PmltqCondExpr parse_and() {
        PmltqCondExpr left = parse_not();
        while (is_word("and") && peek().type != PmltqTok::QMARK && peek().type != PmltqTok::PLUS) {
            bump();
            left = combine(PmltqCondKind::And, std::move(left), parse_not());
        }
        return left;
    }

    PmltqCondExpr parse_not() {
        if (cur_.type == PmltqTok::BANG) {
            const size_t at = cur_.byte_offset;
            bump();
            PmltqCondExpr inner = parse_simple();
            PmltqCondExpr n;
            n.kind = PmltqCondKind::Not;
            n.byte_offset = at;
            n.children.push_back(std::move(inner));
            return n;
        }
        return parse_simple();
    }

    /// simple_test: `(` comma_clause `)` | subquery | predicate.
    PmltqCondExpr parse_simple() {
        if (cur_.type == PmltqTok::LPAREN) {
            bump();
            PmltqCondExpr e = parse_or();
            while (cur_.type == PmltqTok::COMMA) {
                bump();
                e = combine(PmltqCondKind::And, std::move(e), parse_or());
            }
            expect(PmltqTok::RPAREN, "')'");
            bump();
            return e;
        }
        if (cur_.type == PmltqTok::NUMBER)
            return parse_counted_subquery();
        if (relation_starts() || selector_starts())
            return parse_nested(true);
        return parse_predicate();
    }

    /// occurrences ('|' occurrences)* 'x' relation? selector.
    PmltqCondExpr parse_counted_subquery() {
        std::vector<PmltqOccurrence> occ;
        for (;;) {
            expect(PmltqTok::NUMBER, "a number of occurrences");
            PmltqOccurrence o;
            o.min = o.max = std::stoll(cur_.text);
            bump();
            if (cur_.type == PmltqTok::DOTDOT) {
                bump();
                expect(PmltqTok::NUMBER, "a number after '..'");
                o.max = std::stoll(cur_.text);
                bump();
            } else if (cur_.type == PmltqTok::PLUS) {
                o.max = -1;
                bump();
            } else if (cur_.type == PmltqTok::MINUS) {
                o.max = o.min;
                o.min = 0;
                bump();
            }
            occ.push_back(o);
            if (cur_.type != PmltqTok::PIPE)
                break;
            bump();
        }
        if (!is_word("x"))
            throw_expected("'x' after the number of occurrences (e.g. 2x, 1+x, 0x)");
        bump();
        PmltqCondExpr e = parse_nested(true);
        e.nested->occurrences = std::move(occ);
        return e;
    }

    /// `$a.attr` or `attr` (with `/` steps: feats/Case): {node, attr}.
    std::pair<std::string, std::string> parse_attribute() {
        std::string node;
        if (cur_.type == PmltqTok::DOLLAR) {
            bump();
            if (cur_.type != PmltqTok::IDENT)
                throw_expected("node name after '$'");
            node = cur_.text;
            bump();
            if (cur_.type != PmltqTok::DOT)
                return {node, ""};
            bump();
        }
        if (cur_.type != PmltqTok::IDENT)
            throw_expected("attribute name");
        std::string attr = cur_.text;
        bump();
        while (cur_.type == PmltqTok::SLASH) {
            bump();
            if (cur_.type != PmltqTok::IDENT)
                throw_expected("attribute name after '/'");
            attr += "/" + cur_.text;
            bump();
        }
        if (cur_.type == PmltqTok::LPAREN) {
            std::ostringstream o;
            o << "PMLTQ: function " << attr << "() in a test at byte " << cur_.byte_offset
              << " (pando reads attribute tests; for counts of related nodes use occurrences, e.g. 2+x child a-node [])";
            throw std::runtime_error(o.str());
        }
        return {node, attr};
    }

    std::string parse_cmp_op() {
        switch (cur_.type) {
        case PmltqTok::EQ:
        case PmltqTok::EQEQ: bump(); return "=";
        case PmltqTok::NEQ: bump(); return "!=";
        case PmltqTok::LT: bump(); return "<";
        case PmltqTok::GT: bump(); return ">";
        case PmltqTok::LTE: bump(); return "<=";
        case PmltqTok::GTE: bump(); return ">=";
        case PmltqTok::TILDE:
            bump();
            if (cur_.type == PmltqTok::STAR) { bump(); return "~*"; }
            return "~";
        case PmltqTok::BANG:
            bump();
            if (is_word("in")) { bump(); return "!in"; }
            expect(PmltqTok::TILDE, "'~' or 'in' after '!'");
            bump();
            if (cur_.type == PmltqTok::STAR) { bump(); return "!~*"; }
            return "!~";
        default:
            if (is_word("in")) { bump(); return "in"; }
            throw_expected("comparison operator (=, !=, ~, !~, ~*, <, >, <=, >=, in)");
        }
    }

    std::string parse_literal() {
        if (cur_.type == PmltqTok::STRING || cur_.type == PmltqTok::NUMBER) {
            std::string v = cur_.text;
            bump();
            return v;
        }
        throw_expected("a string or number");
    }

    /// predicate: `$a = $b`, `attr in { … }`, `attr CMP value` (value: literal or `$n.attr`).
    PmltqCondExpr parse_predicate() {
        PmltqCondExpr t;
        t.kind = PmltqCondKind::Test;
        t.byte_offset = cur_.byte_offset;
        auto [node, attr] = parse_attribute();
        t.node = std::move(node);
        t.field = std::move(attr);
        t.op = parse_cmp_op();
        if (t.op == "in" || t.op == "!in") {
            expect(PmltqTok::LBRACE, "'{' after in");
            bump();
            t.set_values.push_back(parse_literal());
            while (cur_.type == PmltqTok::COMMA) {
                bump();
                t.set_values.push_back(parse_literal());
            }
            expect(PmltqTok::RBRACE, "'}'");
            bump();
            return t;
        }
        if (t.field.empty()) { // `$a = $b`: the same node (or not)
            if (t.op != "=" && t.op != "!=")
                throw_expected("'=' or '!=' between two nodes");
            expect(PmltqTok::DOLLAR, "a node ($b) after a node and '='");
            auto [n2, a2] = parse_attribute();
            if (!a2.empty())
                throw_expected("a node ($b), not an attribute, after a node and '='");
            t.kind = PmltqCondKind::Same;
            t.ref_node = std::move(n2);
            return t;
        }
        if (cur_.type == PmltqTok::DOLLAR) {
            auto [n2, a2] = parse_attribute();
            if (a2.empty())
                throw_expected("an attribute ($b.lemma) to compare with");
            t.ref_node = std::move(n2);
            t.ref_attr = std::move(a2);
            return t;
        }
        t.value = parse_literal();
        return t;
    }

    // ── >> (the subset pando lowers) ─────────────────────────────────────

    PmltqOutputBlock parse_output_block() {
        PmltqOutputBlock ob;
        if (is_word("distinct")) {
            ob.saw_distinct_keyword = true;
            bump();
        }
        ob.items.push_back(parse_output_item());
        while (cur_.type == PmltqTok::COMMA) {
            bump();
            ob.items.push_back(parse_output_item());
        }
        return ob;
    }

    PmltqOutputItem parse_output_item() {
        if (is_word("count")) {
            bump();
            expect(PmltqTok::LPAREN, "'('");
            bump();
            if (!is_word("over"))
                throw_expected("'over' inside count(...)");
            bump();
            std::pair<std::string, std::string> fr = parse_field_ref();
            expect(PmltqTok::RPAREN, "')'");
            bump();
            PmltqOutputItem it;
            it.kind = PmltqOutputItemKind::CountOver;
            it.node = std::move(fr.first);
            it.attr = std::move(fr.second);
            return it;
        }

        std::pair<std::string, std::string> fr = parse_field_ref();
        PmltqOutputItem it;
        it.kind = PmltqOutputItemKind::FieldRef;
        it.node = std::move(fr.first);
        it.attr = std::move(fr.second);
        return it;
    }

    std::pair<std::string, std::string> parse_field_ref() {
        if (cur_.type == PmltqTok::DOLLAR)
            bump();
        if (cur_.type != PmltqTok::IDENT)
            throw_expected("node name in field reference");
        std::string node = cur_.text;
        bump();
        expect(PmltqTok::DOT, "'.' in field reference");
        bump();
        if (cur_.type != PmltqTok::IDENT)
            throw_expected("attribute name in field reference");
        std::string attr = cur_.text;
        bump();
        return {std::move(node), std::move(attr)};
    }
};

} // namespace

PmltqQuery parse_pmltq_query(const std::string& input) {
    PmltqParser p(input);
    return p.parse_query();
}

} // namespace pando::pmltq
