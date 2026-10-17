#include "query/dialect/pmltq/pmltq_cond_lower.h"

#include "query/dialect/pmltq/pmltq_test_lower.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_map>

namespace pando::pmltq {

namespace {

[[noreturn]] void fail(const std::string& msg, size_t at) {
    throw std::runtime_error("PMLTQ: " + msg + " at byte " + std::to_string(at));
}

/// Occurrence intervals merged, sorted, without empty ones.
std::vector<PmltqOccurrence> normalize(std::vector<PmltqOccurrence> occ) {
    occ.erase(std::remove_if(occ.begin(), occ.end(),
                             [](const PmltqOccurrence& o) { return o.max >= 0 && o.max < o.min; }),
              occ.end());
    std::sort(occ.begin(), occ.end(), [](const PmltqOccurrence& a, const PmltqOccurrence& b) { return a.min < b.min; });
    std::vector<PmltqOccurrence> out;
    for (const auto& o : occ) {
        if (!out.empty() && (out.back().max < 0 || out.back().max + 1 >= o.min)) {
            if (out.back().max >= 0 && (o.max < 0 || o.max > out.back().max))
                out.back().max = o.max;
            continue;
        }
        out.push_back(o);
    }
    return out;
}

/// The numbers of occurrences not in `occ` (over 0, 1, 2, …).
std::vector<PmltqOccurrence> complement(const std::vector<PmltqOccurrence>& occ) {
    std::vector<PmltqOccurrence> out;
    int64_t next = 0;
    for (const auto& o : normalize(occ)) {
        if (o.min > next)
            out.push_back({next, o.min - 1});
        if (o.max < 0)
            return out;
        next = o.max + 1;
    }
    out.push_back({next, -1});
    return out;
}

ConditionPtr all_of(ConditionPtr a, ConditionPtr b, BoolOp op) {
    if (!a)
        return b;
    if (!b)
        return a;
    return ConditionNode::make_branch(op, std::move(a), std::move(b));
}

/// A related node with these occurrences as a condition on the outer token.
ConditionPtr lower_subquery(const PmltqNested& n, size_t at) {
    if (n.optional)
        fail("an optional node (?) inside a test", at);
    if (n.bounded_length)
        fail(n.relation + "{…} with a length", at);
    const StructRelType rel = structural_relation(n.relation);
    const PmltqSelector& sel = *n.node;
    ConditionPtr inner = sel.has_cond ? lower_condition(sel.cond, sel.bind_name, true) : nullptr;
    ConditionPtr any;
    for (const auto& o : normalize(n.occurrences)) {
        ConditionPtr c;
        if (o.min == 0 && o.max == 0)
            c = inner ? ConditionNode::make_structural(rel, inner, "", true)
                      : ConditionNode::make_count(rel, nullptr, CompOp::EQ, 0);
        else if (o.min == 1 && o.max < 0)
            c = inner ? ConditionNode::make_structural(rel, inner)
                      : ConditionNode::make_count(rel, nullptr, CompOp::GTE, 1);
        else if (o.min == o.max)
            c = ConditionNode::make_count(rel, inner, CompOp::EQ, o.min);
        else {
            if (o.min > 0)
                c = ConditionNode::make_count(rel, inner, CompOp::GTE, o.min);
            if (o.max >= 0)
                c = all_of(c, ConditionNode::make_count(rel, inner, CompOp::LTE, o.max), BoolOp::AND);
        }
        any = all_of(any, c, BoolOp::OR);
    }
    if (!any) // no number of occurrences at all (e.g. 2..1x): nothing matches
        any = ConditionNode::make_count(rel, nullptr, CompOp::LT, 0);
    return any;
}

ConditionPtr lower_test(const PmltqCondExpr& t) {
    if (t.op == "in" || t.op == "!in") {
        const bool neg = t.op == "!in";
        ConditionPtr acc;
        for (const auto& v : t.set_values) {
            std::string err;
            ConditionPtr leaf = lower_pmltq_test_strings(t.field, neg ? "!=" : "=", v, err);
            if (!leaf)
                fail(err.substr(err.rfind("PMLTQ: ") == 0 ? 7 : 0), t.byte_offset);
            acc = all_of(acc, leaf, neg ? BoolOp::AND : BoolOp::OR);
        }
        return acc;
    }
    std::string err;
    ConditionPtr leaf = lower_pmltq_test_strings(t.field, t.op, t.value, err);
    if (!leaf)
        fail(err.substr(err.rfind("PMLTQ: ") == 0 ? 7 : 0), t.byte_offset);
    return leaf;
}

} // namespace

StructRelType structural_relation(const std::string& relation) {
    static const std::unordered_map<std::string, StructRelType> m = {
        {"child", StructRelType::CHILD},          {"parent", StructRelType::PARENT},
        {"sibling", StructRelType::SIBLING},      {"descendant", StructRelType::DESCENDANT},
        {"ancestor", StructRelType::ANCESTOR}};
    auto it = m.find(relation);
    if (it == m.end())
        throw std::runtime_error("PMLTQ: the relation " + relation +
                                 " (pando reads child, parent, sibling, descendant and ancestor)");
    return it->second;
}

PmltqCondExpr negate_condition(const PmltqCondExpr& e) {
    static const std::unordered_map<std::string, std::string> flip = {
        {"=", "!="}, {"!=", "="}, {"~", "!~"}, {"!~", "~"}, {"~*", "!~*"}, {"!~*", "~*"},
        {"<", ">="}, {">=", "<"}, {">", "<="}, {"<=", ">"}, {"in", "!in"}, {"!in", "in"}};
    PmltqCondExpr n = e;
    switch (e.kind) {
    case PmltqCondKind::Test:
    case PmltqCondKind::Same:
        n.op = flip.at(e.op);
        return n;
    case PmltqCondKind::Not:
        return e.children.front();
    case PmltqCondKind::And:
    case PmltqCondKind::Or:
        n.kind = e.kind == PmltqCondKind::And ? PmltqCondKind::Or : PmltqCondKind::And;
        for (auto& c : n.children)
            c = negate_condition(c);
        return n;
    case PmltqCondKind::Nested: {
        auto copy = std::make_shared<PmltqNested>(*e.nested);
        copy->subquery = true;
        copy->occurrences = complement(e.nested->subquery ? e.nested->occurrences
                                                          : std::vector<PmltqOccurrence>{{1, -1}});
        n.nested = std::move(copy);
        return n;
    }
    }
    return n;
}

bool refers_to_other_node(const PmltqCondExpr& e, const std::string& self) {
    switch (e.kind) {
    case PmltqCondKind::Same:
        return true;
    case PmltqCondKind::Test:
        return (!e.node.empty() && e.node != self) || !e.ref_node.empty();
    case PmltqCondKind::Nested:
        return false; // a subquery's own nodes are checked when it is lowered
    default:
        for (const auto& c : e.children)
            if (refers_to_other_node(c, self))
                return true;
        return false;
    }
}

ConditionPtr lower_condition(const PmltqCondExpr& e, const std::string& self, bool in_subquery) {
    switch (e.kind) {
    case PmltqCondKind::Test:
        if (refers_to_other_node(e, self))
            fail(in_subquery ? "a reference to another node inside a subquery"
                             : "a reference to another node inside or / !",
                 e.byte_offset);
        return lower_test(e);
    case PmltqCondKind::Same:
        fail(in_subquery ? "a comparison of two nodes inside a subquery"
                         : "a comparison of two nodes inside or / !",
             e.byte_offset);
    case PmltqCondKind::Not:
        return lower_condition(negate_condition(e.children.front()), self, in_subquery);
    case PmltqCondKind::And:
    case PmltqCondKind::Or: {
        ConditionPtr acc;
        for (const auto& c : e.children)
            acc = all_of(acc, lower_condition(c, self, in_subquery),
                         e.kind == PmltqCondKind::And ? BoolOp::AND : BoolOp::OR);
        return acc;
    }
    case PmltqCondKind::Nested: {
        if (e.nested->subquery)
            return lower_subquery(*e.nested, e.byte_offset);
        if (!in_subquery)
            fail("a related node of the match inside or / ! (write it in the condition list, or give it a count: 1+x)",
                 e.byte_offset);
        PmltqNested exists = *e.nested;
        exists.subquery = true;
        exists.occurrences = {{1, -1}};
        return lower_subquery(exists, e.byte_offset);
    }
    }
    return nullptr;
}

} // namespace pando::pmltq
