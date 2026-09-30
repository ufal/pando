#pragma once

#include "query/ast.h"

#include <memory>
#include <stdexcept>

namespace pando {

// `!cond` inside [ ]: pushed down to the leaves (De Morgan), so the AST needs no NOT
// node: `=` ↔ `!=`, a pattern ↔ its complement, AND ↔ OR.
inline ConditionPtr negate_condition(const ConditionPtr& c) {
    if (!c) throw std::runtime_error("'!' needs a condition");
    if (c->is_leaf) {
        auto n = std::make_shared<ConditionNode>(*c);
        AttrCondition& ac = n->leaf;
        if (ac.is_nvals)
            throw std::runtime_error("'!' is not supported on nvals()");
        switch (ac.op) {
            case CompOp::EQ:    ac.op = CompOp::NEQ; ac.neq_regex = false; break;
            case CompOp::REGEX: ac.op = CompOp::NEQ; ac.neq_regex = true; break;
            case CompOp::NEQ:
                ac.op = ac.neq_regex ? CompOp::REGEX : CompOp::EQ;
                ac.neq_regex = false;
                break;
            case CompOp::LT:  ac.op = CompOp::GTE; break;
            case CompOp::GTE: ac.op = CompOp::LT;  break;
            case CompOp::GT:  ac.op = CompOp::LTE; break;
            case CompOp::LTE: ac.op = CompOp::GT;  break;
            default: throw std::runtime_error("'!' is not supported on this condition");
        }
        return n;
    }
    if (c->is_structural || c->is_count || !c->left || !c->right)
        throw std::runtime_error("'!' is not supported on structural conditions (use `not child`, …)");
    return ConditionNode::make_branch(c->bool_op == BoolOp::AND ? BoolOp::OR : BoolOp::AND,
                                      negate_condition(c->left), negate_condition(c->right));
}

} // namespace pando
