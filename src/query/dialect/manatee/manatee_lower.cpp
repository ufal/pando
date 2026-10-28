#include "query/dialect/manatee/manatee_lower.h"

#include "query/quoted_string_pattern.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>

namespace pando::manatee {

namespace {

constexpr const char* kDefaultAttr = "word";   // Manatee's DEFAULTATTR as KonText corpora set it

[[noreturn]] void unsupported(const std::string& what, size_t pos) {
    throw std::runtime_error("Manatee CQL: " + what + " (at position " + std::to_string(pos) +
                             ") is not supported by pando");
}

std::string label_name(long long n) { return "_" + std::to_string(n); }

// attr = "re": a regex on the whole value; a value without regex characters is the same as a literal comparison
ConditionPtr regex_leaf(const std::string& attr, const std::string& re, bool negated) {
    AttrCondition ac;
    ac.attr = attr;
    if (string_has_regex_metachar(re)) {
        ac.op = negated ? CompOp::NEQ : CompOp::REGEX;
        ac.neq_regex = negated;
        ac.regex_full_match = true;
    } else {
        ac.op = negated ? CompOp::NEQ : CompOp::EQ;
    }
    ac.value = re;
    return ConditionNode::make_leaf(std::move(ac));
}

ConditionPtr literal_leaf(const std::string& attr, const std::string& value, CompOp op) {
    AttrCondition ac;
    ac.attr = attr;
    ac.op = op;
    ac.value = value;
    return ConditionNode::make_leaf(std::move(ac));
}

ConditionPtr branch(BoolOp op, ConditionPtr a, ConditionPtr b) {
    if (!a) return b;
    if (!b) return a;
    return ConditionNode::make_branch(op, std::move(a), std::move(b));
}

// an attvallist as a condition; `negated`: its complement (pushed down to the comparisons, De Morgan)
ConditionPtr lower_attval(const AttVal& a, bool negated) {
    switch (a.kind) {
    case AttVal::Kind::Not:
        return lower_attval(*a.left, !negated);
    case AttVal::Kind::And:
    case AttVal::Kind::Or: {
        const bool and_ = (a.kind == AttVal::Kind::And) != negated;
        return branch(and_ ? BoolOp::AND : BoolOp::OR, lower_attval(*a.left, negated), lower_attval(*a.right, negated));
    }
    case AttVal::Kind::PosNum:
        unsupported("a corpus position (#" + std::to_string(a.num1) + ")", a.pos);
    case AttVal::Kind::Func:
        if (a.func == "ws" || a.func == "term" || a.func == "~")
            unsupported(std::string(a.func == "~" ? "the thesaurus (~)" : a.func == "ws" ? "word sketches (ws)" : "terms (term)") +
                            ", which need Sketch Engine data,", a.pos);
        unsupported(a.func + "(…)", a.pos);
    case AttVal::Kind::Cmp:
        break;
    }
    switch (a.op) {
    case AttVal::Op::Eq: return regex_leaf(a.attr, a.value, negated);
    case AttVal::Op::Neq: return regex_leaf(a.attr, a.value, !negated);
    case AttVal::Op::EqLit: return literal_leaf(a.attr, a.value, negated ? CompOp::NEQ : CompOp::EQ);
    case AttVal::Op::NeqLit: return literal_leaf(a.attr, a.value, negated ? CompOp::EQ : CompOp::NEQ);
    case AttVal::Op::Leq: return literal_leaf(a.attr, a.value, negated ? CompOp::GT : CompOp::LTE);
    case AttVal::Op::NotLeq: return literal_leaf(a.attr, a.value, negated ? CompOp::LTE : CompOp::GT);
    case AttVal::Op::Geq: return literal_leaf(a.attr, a.value, negated ? CompOp::LT : CompOp::GTE);
    case AttVal::Op::NotGeq: return literal_leaf(a.attr, a.value, negated ? CompOp::GTE : CompOp::LT);
    }
    return nullptr;
}

// a structure's attributes as (attr, value) pairs: only `attr="v"` joined by `&`
void structure_pairs(const AttVal& a, std::vector<std::pair<std::string, std::string>>& out, const std::string& what) {
    if (a.kind == AttVal::Kind::And) {
        structure_pairs(*a.left, out, what);
        structure_pairs(*a.right, out, what);
        return;
    }
    if (a.kind != AttVal::Kind::Cmp || (a.op != AttVal::Op::Eq && a.op != AttVal::Op::EqLit))
        unsupported(what + " with conditions other than attr=\"value\" joined by '&'", a.pos);
    out.emplace_back(a.attr, a.value);
}

class Lowering {
public:
    explicit Lowering(std::ostream* trace) : trace_(trace) {}

    Program run(const Query& q) {
        TokenQuery tq;
        std::vector<Tokens> alts = expand_sequence(q.sequence);
        tq.tokens = std::move(alts[0]);
        for (size_t i = 1; i < alts.size(); ++i) tq.union_alternatives.push_back(std::move(alts[i]));
        if (q.conditions) conditions(*q.conditions, tq);
        for (const Clause& c : q.clauses) clause(c, tq);
        Statement st;
        st.query = std::move(tq);
        st.has_query = true;
        Program p;
        p.push_back(std::move(st));
        return p;
    }

private:
    std::ostream* trace_;
    using Tokens = std::vector<QueryToken>;
    // Manatee's limit on a repeated range (MAX_RANGE_SIZE): an unbounded group repeats while it fits in it
    static constexpr size_t kMaxRange = 100;
    // a query that expands into more sequences than this is refused
    static constexpr size_t kMaxAlternatives = 2000;

    static void check_size(size_t n, size_t pos) {
        if (n > kMaxAlternatives)
            unsupported("a query that expands into more than " + std::to_string(kMaxAlternatives) + " sequences", pos);
    }

    // A sequence as the token sequences it stands for. `A | B` where every alternative is one plain token is one
    // token (their conditions joined by |); other alternatives, and repeated groups, give several sequences, whose
    // union the executor takes.
    std::vector<Tokens> expand_sequence(const Sequence& s) {
        if (s.alternatives.size() > 1) {
            bool single = true;
            for (const Seq& seq : s.alternatives)
                single = single && seq.items.size() == 1 && seq.items[0].kind == Repetition::Kind::Position &&
                         !seq.items[0].repeated && seq.items[0].position->label < 0;
            if (single) return {{alternatives_as_token(s)}};
        }
        std::vector<Tokens> out;
        for (const Seq& seq : s.alternatives) {
            for (auto& t : expand_seq(seq)) out.push_back(std::move(t));
            check_size(out.size(), seq.items.empty() ? 0 : seq.items[0].pos);
        }
        return out;
    }

    std::vector<Tokens> expand_seq(const Seq& seq) {
        if (seq.negated) {
            if (seq.items.size() != 1 || seq.items[0].kind != Repetition::Kind::Position || seq.items[0].repeated)
                unsupported("'!' before a sequence of more than one token", seq.items[0].pos);
            return {{position_token(*seq.items[0].position, true)}};
        }
        std::vector<Tokens> out{{}};
        for (const Repetition& r : seq.items) {
            const std::vector<Tokens> opts = expand_repetition(r);
            std::vector<Tokens> next;
            for (const Tokens& head : out)
                for (const Tokens& tail : opts) {
                    Tokens t = head;
                    t.insert(t.end(), tail.begin(), tail.end());
                    next.push_back(std::move(t));
                }
            check_size(next.size(), r.pos);
            out = std::move(next);
        }
        return out;
    }

    // A | B | …: one token, each alternative one plain token
    QueryToken alternatives_as_token(const Sequence& s) {
        ConditionPtr any;
        bool matches_all = false;
        for (const Seq& seq : s.alternatives) {
            QueryToken t = position_token(*seq.items[0].position, seq.negated);
            if (!t.conditions) matches_all = true;
            any = branch(BoolOp::OR, any, t.conditions);
        }
        QueryToken out;
        if (!matches_all) out.conditions = any;
        return out;
    }

    // the token sequences one repetition stands for (one, but for a group)
    std::vector<Tokens> expand_repetition(const Repetition& r) {
        switch (r.kind) {
        case Repetition::Kind::Position: {
            QueryToken t = position_token(*r.position, false);
            repeat(t, r);
            return {{std::move(t)}};
        }
        case Repetition::Kind::Group: {
            const Query& g = *r.group;
            if (g.conditions || !g.clauses.empty()) unsupported("within / containing inside parentheses", r.pos);
            std::vector<Tokens> inner = expand_sequence(g.sequence);
            if (!r.repeated) return inner;
            // one token without a repetition of its own: the repetition is the token's
            if (inner.size() == 1 && inner[0].size() == 1 && !inner[0][0].has_repetition() && !inner[0][0].is_anchor()) {
                repeat(inner[0][0], r);
                return inner;
            }
            // k = min … max occurrences one after the other (unbounded: while they fit in Manatee's range limit)
            size_t shortest = SIZE_MAX;
            for (const Tokens& t : inner) {
                size_t len = 0;
                for (const QueryToken& q : t) len += q.is_anchor() ? 0 : static_cast<size_t>(q.min_repeat);
                shortest = std::min(shortest, std::max<size_t>(len, 1));
            }
            const size_t max_k = r.max >= 0 ? static_cast<size_t>(r.max) : std::max<size_t>(1, kMaxRange / shortest);
            std::vector<Tokens> out;
            if (r.min == 0) out.push_back({});
            std::vector<Tokens> k_times{{}};
            for (size_t k = 1; k <= max_k; ++k) {
                std::vector<Tokens> next;
                for (const Tokens& head : k_times)
                    for (const Tokens& tail : inner) {
                        Tokens t = head;
                        t.insert(t.end(), tail.begin(), tail.end());
                        next.push_back(std::move(t));
                    }
                check_size(out.size() + next.size(), r.pos);
                k_times = std::move(next);
                if (k >= static_cast<size_t>(std::max<long long>(r.min, 1)))
                    for (const Tokens& t : k_times) out.push_back(t);
            }
            return out;
        }
        case Repetition::Kind::StructBegin:
        case Repetition::Kind::StructEnd: {
            QueryToken t;
            t.anchor = r.kind == Repetition::Kind::StructBegin ? RegionAnchorType::REGION_START : RegionAnchorType::REGION_END;
            t.anchor_region = r.structure.name;
            if (r.structure.attrs) structure_pairs(*r.structure.attrs, t.anchor_attrs, "<" + r.structure.name + ">");
            return {{std::move(t)}};
        }
        case Repetition::Kind::StructWhole:
            unsupported("a whole structure (<" + r.structure.name + "/>) as part of a query", r.pos);
        }
        return {{}};
    }

    static void repeat(QueryToken& t, const Repetition& r) {
        if (!r.repeated) return;
        if (r.max >= 0 && r.max < r.min) unsupported("a repetition {n,m} with m < n", r.pos);
        t.min_repeat = static_cast<int>(r.min);
        t.max_repeat = r.max < 0 ? REPEAT_UNBOUNDED : static_cast<int>(r.max);
    }

    QueryToken position_token(const Position& p, bool negated) {
        QueryToken t;
        if (p.label >= 0) t.name = label_name(p.label);
        switch (p.kind) {
        case Position::Kind::Regexp:
            t.conditions = regex_leaf(kDefaultAttr, p.regexp, negated);
            break;
        case Position::Kind::Bracket:
            t.conditions = lower_attval(*p.attrs, negated);
            break;
        case Position::Kind::Any:
            if (negated) unsupported("'!' before []", p.pos);
            break;
        case Position::Kind::Thesaurus:
            unsupported("the thesaurus (~), which needs Sketch Engine data,", p.pos);
        case Position::Kind::Meet:
            unsupported("meet", p.pos);
        case Position::Kind::Union:
            unsupported("union", p.pos);
        }
        return t;
    }

    // & conditions: a conjunction of 1.a = 2.b and f(1.a) cmp N
    void conditions(const Cond& c, TokenQuery& tq) {
        switch (c.kind) {
        case Cond::Kind::And:
            conditions(*c.left, tq);
            conditions(*c.right, tq);
            return;
        case Cond::Kind::AttrEq: {
            GlobalAlignmentFilter af;
            af.name1 = label_name(c.l1);
            af.attr1 = c.a1;
            af.name2 = label_name(c.l2);
            af.attr2 = c.a2;
            tq.global_alignment_filters.push_back(std::move(af));
            return;
        }
        case Cond::Kind::Freq: {
            GlobalFunctionFilter ff;
            ff.lhs.func = GlobalFunctionType::FREQ;
            ff.lhs.args.push_back(label_name(c.l1) + "." + c.a1);
            ff.int_value = c.value;
            ff.dbl_value = static_cast<double>(c.value);
            switch (c.cmp) {
            case Cond::Cmp::Eq: ff.op = CompOp::EQ; break;
            case Cond::Cmp::Neq: ff.op = CompOp::NEQ; break;
            case Cond::Cmp::Leq: ff.op = CompOp::LTE; break;
            case Cond::Cmp::Geq: ff.op = CompOp::GTE; break;
            case Cond::Cmp::Lt: ff.op = CompOp::LT; break;
            case Cond::Cmp::Gt: ff.op = CompOp::GT; break;
            }
            tq.global_function_filters.push_back(std::move(ff));
            return;
        }
        case Cond::Kind::AttrNeq:
            unsupported("'!=' between labelled attributes", c.pos);
        case Cond::Kind::Or:
            unsupported("'|' between global conditions", c.pos);
        case Cond::Kind::Not:
            unsupported("'!(…)' in global conditions", c.pos);
        }
    }

    // within / containing a structure: <s/>, <doc file="x" & id="y"/>
    void clause(const Clause& c, TokenQuery& tq) {
        const Suffix& s = c.suffix;
        const char* kw = c.containing ? "containing" : "within";
        if (s.kind == Suffix::Kind::Aligned)
            unsupported(std::string(kw) + " an aligned corpus (" + s.corpus + ": …)", s.pos);
        if (s.kind != Suffix::Kind::Sequence)
            unsupported(std::string(kw) + " a number of tokens", s.pos);
        const auto& alts = s.sequence.alternatives;
        if (alts.size() != 1 || alts[0].negated || alts[0].items.size() != 1 ||
            alts[0].items[0].kind != Repetition::Kind::StructWhole)
            unsupported(std::string(kw) + " a query (only a structure such as <s/> is)", s.pos);
        const Structure& st = alts[0].items[0].structure;
        std::vector<std::pair<std::string, std::string>> attrs;
        if (st.attrs) structure_pairs(*st.attrs, attrs, std::string(kw) + " <" + st.name + " …/>");
        if (c.containing) {
            if (!attrs.empty()) unsupported("containing a structure with attributes", s.pos);
            ContainingClause cc;
            cc.region = st.name;
            cc.negated = c.negated;
            tq.containing_clauses.push_back(std::move(cc));
            return;
        }
        if (!tq.within.empty() && (tq.within != st.name || tq.not_within != c.negated))
            unsupported("a second within with another structure", s.pos);
        if (c.negated && !attrs.empty()) unsupported("!within a structure with attributes", s.pos);
        tq.within = st.name;
        tq.not_within = c.negated;
        // Manatee's merge drops hits only where a hit can run past its region while a later one does not: a
        // pattern of variable length
        if (!c.negated) {
            auto span = [](const std::vector<QueryToken>& ts, bool& fixed) {
                long long n = 0;
                for (const auto& t : ts) {
                    if (t.is_anchor()) continue;
                    if (t.min_repeat != t.max_repeat) fixed = false;
                    n += t.min_repeat;
                }
                return n;
            };
            bool fixed = true;
            const long long len = span(tq.tokens, fixed);
            for (const auto& alt : tq.union_alternatives)
                if (span(alt, fixed) != len) fixed = false;
            tq.manatee_within = !fixed;
        }
        for (const auto& [attr, value] : attrs) {
            GlobalRegionFilter f;
            f.region_attr = st.name + "_" + attr;
            f.op = CompOp::EQ;
            f.value = value;
            if (string_has_regex_metachar(value)) {
                f.regex = true;
                f.regex_full = true;
            }
            tq.global_region_filters.push_back(std::move(f));
        }
    }
};

}  // namespace

Program lower(const Query& q, std::ostream* trace) {
    return Lowering(trace).run(q);
}

}  // namespace pando::manatee
