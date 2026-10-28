// The grammar rules are named as in Manatee's query/cqpeval.y; its left-recursive rules are loops here.
//
//   query       := sequence ['&' condExpr] { ['!'] ('within' | 'containing') suffix }
//   suffix      := NUMBER | '!' NUMBER | ['!'] word ':' sequence | sequence
//   sequence    := ['!'] seq { '|' ['!'] seq }
//   seq         := repetition { repetition }
//   repetition  := atom [repOpt] | '<' structure '>' | '<' structure '/' '>' | '<' '/' structure '>'
//   atom        := position | '(' sequence ')' | '(' query-with-within/containing ')'
//   position    := [NUMBER ':'] (REGEXP | '[' attvallist ']' | '[' ']' | thesaurus | '(' meet/union ')')
//   attvallist  := attvaland { '|' attvaland };  attvaland := attval { '&' attval }
//   condExpr    := condConj { '|' condConj };    condConj := condAtom { '&' condAtom }

#include "query/dialect/manatee/manatee_parser.h"
#include "query/dialect/manatee/manatee_lexer.h"

#include <algorithm>
#include <stdexcept>

namespace pando::manatee {

namespace {

class Parser {
public:
    explicit Parser(const std::string& q) : t_(lex(q)) {}

    Query parse_all() {
        Query q = query();
        if (peek().kind != Tok::End) fail("unexpected " + std::string(tok_name(peek().kind)));
        return q;
    }

private:
    std::vector<Token> t_;
    size_t i_ = 0;

    const Token& peek(size_t k = 0) const { return t_[std::min(i_ + k, t_.size() - 1)]; }
    bool at(Tok k, size_t ahead = 0) const { return peek(ahead).kind == k; }
    const Token& next() { return t_[std::min(i_++, t_.size() - 1)]; }
    [[noreturn]] void fail(const std::string& what) const {
        throw std::runtime_error("Manatee CQL syntax error at position " + std::to_string(peek().pos) + ": " + what);
    }
    const Token& expect(Tok k) {
        if (!at(k)) fail(std::string("expected ") + tok_name(k) + ", found " + tok_name(peek().kind));
        return next();
    }
    std::string word() {
        if (!is_word_like(peek().kind)) fail(std::string("expected a name, found ") + tok_name(peek().kind));
        return next().text;
    }

    // a token that can start a repetition
    bool starts_repetition() const {
        switch (peek().kind) {
        case Tok::Regexp: case Tok::LBracket: case Tok::Teq: case Tok::LParen: case Tok::LStruct: return true;
        case Tok::Number: return at(Tok::Colon, 1);
        default: return false;
        }
    }

    // query := sequence ['&' condExpr] { ['!'] ('within' | 'containing') suffix }
    Query query() {
        Query q;
        q.sequence = sequence();
        if (at(Tok::And)) {
            next();
            q.conditions = cond_expr();
        }
        clauses(q);
        return q;
    }

    void clauses(Query& q) {
        while (true) {
            Clause c;
            if (at(Tok::Not) && (at(Tok::KwWithin, 1) || at(Tok::KwContaining, 1))) {
                next();
                c.negated = true;
            } else if (!at(Tok::KwWithin) && !at(Tok::KwContaining)) {
                return;
            }
            c.containing = next().kind == Tok::KwContaining;
            c.suffix = suffix();
            if (c.suffix.aligned_not) {
                if (c.negated) fail("'!' twice");   // ! within ! corpus: …
                c.negated = true;
            }
            q.clauses.push_back(std::move(c));
        }
    }

    // suffix := NUMBER | '!' NUMBER | ['!'] word ':' sequence | sequence
    Suffix suffix() {
        Suffix s;
        s.pos = peek().pos;
        if (at(Tok::Number) && !at(Tok::Colon, 1)) {
            s.kind = Suffix::Kind::Number;
            s.number = next().num;
            return s;
        }
        if (at(Tok::Not) && at(Tok::Number, 1) && !at(Tok::Colon, 2)) {
            next();
            s.kind = Suffix::Kind::NotNumber;
            s.number = next().num;
            return s;
        }
        size_t k = at(Tok::Not) ? 1 : 0;
        if (is_word_like(peek(k).kind) && at(Tok::Colon, k + 1)) {
            if (k) {
                next();
                s.aligned_not = true;
            }
            s.kind = Suffix::Kind::Aligned;
            s.corpus = next().text;
            next();   // ':'
            s.sequence = sequence();
            return s;
        }
        s.kind = Suffix::Kind::Sequence;
        s.sequence = sequence();
        return s;
    }

    // sequence := ['!'] seq { '|' ['!'] seq }
    Sequence sequence() {
        Sequence s;
        s.alternatives.push_back(seq_maybe_negated());
        while (at(Tok::Or)) {
            next();
            s.alternatives.push_back(seq_maybe_negated());
        }
        return s;
    }

    Seq seq_maybe_negated() {
        bool neg = false;
        if (at(Tok::Not)) {
            next();
            neg = true;
        }
        Seq s = seq();
        s.negated = neg;
        return s;
    }

    Seq seq() {
        Seq s;
        if (!starts_repetition()) fail(std::string("expected a token, found ") + tok_name(peek().kind));
        while (starts_repetition()) s.items.push_back(repetition());
        return s;
    }

    Repetition repetition() {
        Repetition r;
        r.pos = peek().pos;
        if (at(Tok::LStruct)) {
            next();
            if (at(Tok::Slash)) {
                next();
                r.kind = Repetition::Kind::StructEnd;
                r.structure = structure();
                expect(Tok::RStruct);
                return r;
            }
            r.structure = structure();
            if (at(Tok::Slash)) {
                next();
                r.kind = Repetition::Kind::StructWhole;
            } else {
                r.kind = Repetition::Kind::StructBegin;
            }
            expect(Tok::RStruct);
            return r;
        }
        if (at(Tok::LParen) && !at(Tok::KwMeet, 1) && !at(Tok::KwUnion, 1)) {
            next();
            r.kind = Repetition::Kind::Group;
            auto g = std::make_shared<Query>(query());
            // ( sequence ) or ( query within / containing … ): global conditions only with a clause
            if (g->conditions && g->clauses.empty()) fail("global conditions ('&') inside parentheses");
            r.group = std::move(g);
            expect(Tok::RParen);
        } else {
            r.kind = Repetition::Kind::Position;
            r.position = position();
        }
        rep_opt(r);
        return r;
    }

    void rep_opt(Repetition& r) {
        switch (peek().kind) {
        case Tok::Star: next(); r.repeated = true; r.min = 0; r.max = -1; return;
        case Tok::Plus: next(); r.repeated = true; r.min = 1; r.max = -1; return;
        case Tok::Quest: next(); r.repeated = true; r.min = 0; r.max = 1; return;
        case Tok::LBrace: break;
        default: return;
        }
        next();
        r.repeated = true;
        if (at(Tok::Comma)) {               // {,m}
            next();
            r.min = 0;
            r.max = expect(Tok::Number).num;
        } else {
            r.min = expect(Tok::Number).num;
            if (at(Tok::Comma)) {
                next();
                r.max = at(Tok::Number) ? next().num : -1;   // {n,m} / {n,}
            } else {
                r.max = r.min;              // {n}
            }
        }
        expect(Tok::RBrace);
    }

    // structure := word [attvallist]
    Structure structure() {
        Structure s;
        s.pos = peek().pos;
        s.name = word();
        if (!at(Tok::RStruct) && !at(Tok::Slash)) s.attrs = attvallist();
        return s;
    }

    // position := [NUMBER ':'] oneposition
    PositionPtr position() {
        auto p = std::make_shared<Position>();
        p->pos = peek().pos;
        if (at(Tok::Number) && at(Tok::Colon, 1)) {
            p->label = next().num;
            next();
        }
        switch (peek().kind) {
        case Tok::Regexp:
            p->kind = Position::Kind::Regexp;
            p->regexp = next().text;
            break;
        case Tok::LBracket:
            next();
            if (at(Tok::RBracket)) {
                p->kind = Position::Kind::Any;
            } else {
                p->kind = Position::Kind::Bracket;
                p->attrs = attvallist();
            }
            expect(Tok::RBracket);
            break;
        case Tok::Teq:
            thesaurus();
            p->kind = Position::Kind::Thesaurus;
            break;
        case Tok::LParen: {
            next();
            const Tok kw = next().kind;   // meet / union (checked by the caller)
            if (kw != Tok::KwMeet && kw != Tok::KwUnion) fail("expected 'meet' or 'union'");
            p->kind = kw == Tok::KwMeet ? Position::Kind::Meet : Position::Kind::Union;
            p->a = position();
            p->b = position();
            if (kw == Tok::KwMeet && (at(Tok::Number) || at(Tok::NNumber))) {
                p->meet_window = true;
                p->from = next().num;
                if (!at(Tok::Number) && !at(Tok::NNumber)) fail("expected a second number in meet");
                p->to = next().num;
            }
            expect(Tok::RParen);
            break;
        }
        default:
            fail(std::string("expected a token, found ") + tok_name(peek().kind));
        }
        return p;
    }

    // thesaurus := '~' ['~'] [NUMBER] REGEXP
    void thesaurus() {
        expect(Tok::Teq);
        if (at(Tok::Teq)) next();
        if (at(Tok::Number)) next();
        expect(Tok::Regexp);
    }

    AttValPtr attvallist() {
        AttValPtr l = attvaland();
        while (at(Tok::Or)) {
            next();
            auto n = std::make_shared<AttVal>();
            n->kind = AttVal::Kind::Or;
            n->left = l;
            n->right = attvaland();
            l = n;
        }
        return l;
    }

    AttValPtr attvaland() {
        AttValPtr l = attval();
        while (at(Tok::And)) {
            next();
            auto n = std::make_shared<AttVal>();
            n->kind = AttVal::Kind::And;
            n->left = l;
            n->right = attval();
            l = n;
        }
        return l;
    }

    AttValPtr attval() {
        auto a = std::make_shared<AttVal>();
        a->pos = peek().pos;
        // functions: ws( …), term( …), swap( …), ccoll( …): a keyword followed by '('
        const Tok k = peek().kind;
        if ((k == Tok::KwWs || k == Tok::KwTerm || k == Tok::KwSwap || k == Tok::KwCcoll) && at(Tok::LParen, 1)) {
            a->kind = AttVal::Kind::Func;
            a->func = next().text;
            next();
            int depth = 1;   // the arguments as Manatee's grammar has them, not used further
            if (k == Tok::KwSwap || k == Tok::KwCcoll) {
                expect(Tok::Number);
                expect(Tok::Comma);
                if (k == Tok::KwCcoll) {
                    expect(Tok::Number);
                    expect(Tok::Comma);
                }
                a->left = attvallist();
                expect(Tok::RParen);
                return a;
            }
            while (depth > 0) {
                if (at(Tok::End)) fail("')' missing");
                const Tok t = next().kind;
                depth += t == Tok::LParen ? 1 : t == Tok::RParen ? -1 : 0;
            }
            return a;
        }
        if (at(Tok::Teq)) {
            thesaurus();
            a->kind = AttVal::Kind::Func;
            a->func = "~";
            return a;
        }
        if (at(Tok::Not)) {
            next();
            a->kind = AttVal::Kind::Not;
            a->left = attval();
            return a;
        }
        if (at(Tok::LParen)) {
            next();
            AttValPtr inner = attvallist();
            expect(Tok::RParen);
            return inner;
        }
        if (at(Tok::PosNum)) {
            next();
            a->kind = AttVal::Kind::PosNum;
            a->num1 = expect(Tok::Number).num;
            a->num2 = at(Tok::NNumber) ? -next().num : a->num1;
            return a;
        }
        a->kind = AttVal::Kind::Cmp;
        a->attr = word();
        switch (peek().kind) {
        case Tok::Eq:
            next();
            if (at(Tok::Eq)) { next(); a->op = AttVal::Op::EqLit; } else a->op = AttVal::Op::Eq;
            break;
        case Tok::Neq:
            next();
            if (at(Tok::Eq)) { next(); a->op = AttVal::Op::NeqLit; } else a->op = AttVal::Op::Neq;
            break;
        case Tok::Leq: next(); a->op = AttVal::Op::Leq; break;
        case Tok::Geq: next(); a->op = AttVal::Op::Geq; break;
        case Tok::Not:
            next();
            if (at(Tok::Leq)) { next(); a->op = AttVal::Op::NotLeq; }
            else if (at(Tok::Geq)) { next(); a->op = AttVal::Op::NotGeq; }
            else fail("expected '<=' or '>=' after '!'");
            break;
        default:
            fail(std::string("expected a comparison after '") + a->attr + "', found " + tok_name(peek().kind));
        }
        a->value = expect(Tok::Regexp).text;
        return a;
    }

    CondPtr cond_expr() {
        CondPtr l = cond_conj();
        while (at(Tok::Or)) {
            next();
            auto n = std::make_shared<Cond>();
            n->kind = Cond::Kind::Or;
            n->left = l;
            n->right = cond_conj();
            l = n;
        }
        return l;
    }

    CondPtr cond_conj() {
        CondPtr l = cond_atom();
        while (at(Tok::And)) {
            next();
            auto n = std::make_shared<Cond>();
            n->kind = Cond::Kind::And;
            n->left = l;
            n->right = cond_atom();
            l = n;
        }
        return l;
    }

    CondPtr cond_atom() {
        auto c = std::make_shared<Cond>();
        c->pos = peek().pos;
        if (at(Tok::LParen)) {
            next();
            CondPtr inner = cond_expr();
            expect(Tok::RParen);
            return inner;
        }
        if (at(Tok::Not)) {
            next();
            expect(Tok::LParen);
            c->kind = Cond::Kind::Not;
            c->left = cond_expr();
            expect(Tok::RParen);
            return c;
        }
        if (at(Tok::KwFreq) && at(Tok::LParen, 1)) {
            next();
            next();
            c->kind = Cond::Kind::Freq;
            c->l1 = expect(Tok::Number).num;
            expect(Tok::Dot);
            c->a1 = word();
            expect(Tok::RParen);
            switch (peek().kind) {
            case Tok::Eq: next(); c->cmp = Cond::Cmp::Eq; break;
            case Tok::Neq: next(); c->cmp = Cond::Cmp::Neq; break;
            case Tok::Leq: next(); c->cmp = Cond::Cmp::Leq; break;
            case Tok::Geq: next(); c->cmp = Cond::Cmp::Geq; break;
            case Tok::LStruct: next(); c->cmp = Cond::Cmp::Lt; break;
            case Tok::RStruct: next(); c->cmp = Cond::Cmp::Gt; break;
            case Tok::Not:
                next();
                if (at(Tok::Geq)) { next(); c->cmp = Cond::Cmp::Lt; }
                else if (at(Tok::Leq)) { next(); c->cmp = Cond::Cmp::Gt; }
                else fail("expected '>=' or '<=' after '!'");
                break;
            default:
                fail("expected a comparison after f(…)");
            }
            c->value = expect(Tok::Number).num;
            return c;
        }
        // N.attr = M.attr / N.attr != M.attr
        c->l1 = expect(Tok::Number).num;
        expect(Tok::Dot);
        c->a1 = word();
        if (at(Tok::Eq)) c->kind = Cond::Kind::AttrEq;
        else if (at(Tok::Neq)) c->kind = Cond::Kind::AttrNeq;
        else fail("expected '=' or '!=' between two labelled attributes (1.lemma = 2.lemma)");
        next();
        c->l2 = expect(Tok::Number).num;
        expect(Tok::Dot);
        c->a2 = word();
        return c;
    }
};

}  // namespace

Query parse(const std::string& query) {
    return Parser(query).parse_all();
}

}  // namespace pando::manatee
