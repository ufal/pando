#include "query/dialect/pmltq/pmltq_native_lower.h"

#include "query/dialect/pmltq/pmltq_cond_lower.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// A PML-TQ query as pando's AST. The selector and its nested nodes without occurrences are
// the nodes of the match (PML-TQ counts one result per combination of them), so they become
// named tokens of one TokenQuery, related as in the tree:
//
//   a-node $v := [ upos = "VERB", child a-node [ deprel = "nsubj" ], child a-node [ deprel = "obj" ] ]
//   → n1:[deprel="nsubj"] < v:[upos="VERB"] > n2:[deprel="obj"]  (+ n1 and n2 different tokens)
//
// pando relates each token to the next one, so the nodes must form a chain (every node related
// to at most two others); a sibling is reached through a token for the shared parent. Nodes
// with occurrences, and the conditions, are conditions on their token (pmltq_cond_lower.cpp).
// Different nodes are different tokens (PML-TQ; `+` lets a node overlap): directly related
// nodes are anyway, the others get a `!=` position order.

namespace pando::pmltq {

namespace {

[[noreturn]] void fail(const std::string& msg) { throw std::runtime_error("PMLTQ: " + msg); }

struct QNode {
    std::string name;
    std::vector<ConditionPtr> conds;
    bool hidden = false;      // the shared parent of two siblings (not a PML-TQ node)
    bool overlapping = false; // `+`
};

/// A dependency edge between two nodes: `head` governs `dep` (directly or, trans, at a distance).
struct Edge {
    size_t head, dep;
    bool trans;
};

/// A reference to another node, handled once all nodes are known.
struct Pending {
    size_t owner;
    PmltqCondExpr expr;
};

class TreeBuilder {
public:
    std::vector<QNode> nodes;
    std::vector<Edge> edges;
    std::vector<Pending> pending;

    size_t add(const PmltqSelector& sel) {
        const size_t idx = nodes.size();
        QNode q;
        q.name = sel.bind_name;
        q.overlapping = sel.overlapping;
        nodes.push_back(std::move(q));
        if (sel.has_cond)
            add_conditions(idx, sel.cond, sel.bind_name);
        return idx;
    }

private:
    void add_conditions(size_t idx, const PmltqCondExpr& e, const std::string& self) {
        if (e.kind == PmltqCondKind::And) {
            for (const auto& c : e.children)
                add_conditions(idx, c, self);
            return;
        }
        if (e.kind == PmltqCondKind::Nested && !e.nested->subquery) {
            add_related(idx, *e.nested);
            return;
        }
        if (e.kind == PmltqCondKind::Same ||
            (e.kind == PmltqCondKind::Test && refers_to_other_node(e, self))) {
            pending.push_back({idx, e});
            return;
        }
        nodes[idx].conds.push_back(lower_condition(e, self));
    }

    void add_related(size_t parent, const PmltqNested& n) {
        if (n.node->kind == PmltqSelectorKind::ARoot) { // no node: a condition (pando has no root node)
            if (n.node->has_cond)
                fail("a-root with conditions or related nodes at byte " + std::to_string(n.byte_offset) +
                     " (pando has no root node; write the sentence's root word: a-node [ deprel = \"root\" ])");
            if (n.relation == "parent")
                nodes[parent].conds.push_back(lower_test_root());
            else if (n.relation != "ancestor")
                fail(n.relation + " a-root at byte " + std::to_string(n.byte_offset) + " (a-root is only a parent or an ancestor)");
            return;
        }
        if (n.optional)
            fail("an optional node (?" + n.relation + ") at byte " + std::to_string(n.byte_offset) +
                 " (pando matches the nodes that are there; for 'with or without' leave the node out, for 'without' use 0x)");
        if (n.bounded_length)
            fail(n.relation + "{…} with a length at byte " + std::to_string(n.byte_offset));
        structural_relation(n.relation); // a relation pando knows
        const size_t child = add(*n.node);
        if (n.relation == "child")
            edges.push_back({parent, child, false});
        else if (n.relation == "parent")
            edges.push_back({child, parent, false});
        else if (n.relation == "descendant")
            edges.push_back({parent, child, true});
        else if (n.relation == "ancestor")
            edges.push_back({child, parent, true});
        else { // sibling: both depend on one head
            QNode h;
            h.hidden = true;
            nodes.push_back(std::move(h));
            const size_t head = nodes.size() - 1;
            edges.push_back({head, parent, false});
            edges.push_back({head, child, false});
        }
    }
};

/// Names for the nodes without one (n1, n2, …; p1, … for siblings' parents), none taken twice.
void name_nodes(std::vector<QNode>& nodes) {
    std::unordered_set<std::string> used;
    for (const auto& n : nodes) {
        if (n.name.empty())
            continue;
        if (!used.insert(n.name).second)
            fail("the name $" + n.name + " is given to two nodes");
    }
    int next_n = 1, next_p = 1;
    for (auto& n : nodes) {
        if (!n.name.empty())
            continue;
        do {
            n.name = n.hidden ? "p" + std::to_string(next_p++) : "n" + std::to_string(next_n++);
        } while (used.count(n.name));
        used.insert(n.name);
    }
}

/// The nodes in chain order, or an error naming the node related to more than two others.
std::vector<size_t> chain_order(const std::vector<QNode>& nodes, const std::vector<Edge>& edges) {
    std::vector<std::vector<size_t>> adj(nodes.size());
    for (const auto& e : edges) {
        adj[e.head].push_back(e.dep);
        adj[e.dep].push_back(e.head);
    }
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (adj[i].size() > 2) {
            const std::string who = nodes[i].hidden ? "the shared parent of two siblings" : "$" + nodes[i].name;
            fail(who + " is related to " + std::to_string(adj[i].size()) +
                 " other nodes of the match; pando matches nodes that form a chain (each related to at "
                 "most two others). Give the others a number of occurrences to make them conditions, "
                 "e.g. 1+x child a-node [ … ]");
        }
    }
    size_t start = 0; // the selector itself when it is an end of the chain, else the first end
    if (adj[0].size() > 1) {
        for (size_t i = 0; i < nodes.size(); ++i)
            if (adj[i].size() <= 1) {
                start = i;
                break;
            }
    }
    std::vector<size_t> order{start};
    size_t prev = nodes.size(), cur = start;
    while (order.size() < nodes.size()) {
        size_t next = nodes.size();
        for (size_t nb : adj[cur])
            if (nb != prev)
                next = nb;
        if (next == nodes.size())
            break;
        order.push_back(next);
        prev = cur;
        cur = next;
    }
    return order;
}

RelationType relation_between(const std::vector<Edge>& edges, size_t a, size_t b) {
    for (const auto& e : edges) {
        if (e.head == a && e.dep == b)
            return e.trans ? RelationType::TRANS_GOVERNS : RelationType::GOVERNS;
        if (e.head == b && e.dep == a)
            return e.trans ? RelationType::TRANS_GOV_BY : RelationType::GOVERNED_BY;
    }
    fail("internal: no relation between two neighbouring nodes");
}

std::vector<std::string> fields_from_output_block(const PmltqOutputBlock& ob) {
    std::vector<std::string> out;
    auto add = [&](const std::string& n, const std::string& a) {
        const std::string f = n + "." + a;
        if (std::find(out.begin(), out.end(), f) == out.end())
            out.push_back(f);
    };
    for (const auto& it : ob.items) {
        if (it.kind == PmltqOutputItemKind::CountOver)
            add(it.node, it.attr);
    }
    if (!out.empty())
        return out;
    for (const auto& it : ob.items) {
        if (it.kind == PmltqOutputItemKind::FieldRef)
            add(it.node, it.attr);
    }
    return out;
}

/// References between nodes: `$b.lemma = "x"` (a test on b), `lemma = $b.lemma` (the same value
/// on both tokens), `$a != $b` (different tokens).
void apply_references(const std::vector<Pending>& pending, std::vector<QNode>& nodes, TokenQuery& tq) {
    std::unordered_map<std::string, size_t> by_name;
    for (size_t i = 0; i < nodes.size(); ++i)
        by_name[nodes[i].name] = i;
    auto node_of = [&](const std::string& name, size_t owner, size_t at) -> size_t {
        if (name.empty())
            return owner;
        auto it = by_name.find(name);
        if (it == by_name.end())
            fail("no node is called $" + name + " (byte " + std::to_string(at) + ")");
        return it->second;
    };
    for (const auto& p : pending) {
        const PmltqCondExpr& e = p.expr;
        if (e.kind == PmltqCondKind::Same) {
            const size_t a = node_of(e.node, p.owner, e.byte_offset), b = node_of(e.ref_node, p.owner, e.byte_offset);
            if (e.op == "!=") {
                if (a == b)
                    fail("$" + nodes[a].name + " != $" + nodes[a].name + " matches nothing");
                tq.position_orders.push_back({nodes[a].name, nodes[b].name, CompOp::NEQ});
            } else if (a != b) {
                fail("$" + nodes[a].name + " = $" + nodes[b].name +
                     ": two nodes of the match are different tokens (write one node)");
            }
            continue;
        }
        const size_t a = node_of(e.node, p.owner, e.byte_offset);
        if (e.ref_node.empty()) { // a test on another node
            PmltqCondExpr t = e;
            t.node.clear();
            nodes[a].conds.push_back(lower_condition(t, ""));
            continue;
        }
        const size_t b = node_of(e.ref_node, p.owner, e.byte_offset);
        if (e.op != "=" || a == b)
            fail("pando compares attributes of two different nodes with = (byte " + std::to_string(e.byte_offset) + ")");
        GlobalAlignmentFilter f;
        f.name1 = nodes[a].name;
        f.attr1 = e.field;
        f.name2 = nodes[b].name;
        f.attr2 = e.ref_attr;
        tq.global_alignment_filters.push_back(std::move(f));
    }
}

/// The top selector. `a-root [ child a-node X ]` (pando has no root node): X, the sentence's root
/// word; `a-root [ descendant a-node X ]`: X, any word.
void add_selector(TreeBuilder& tb, const PmltqSelector& sel) {
    if (sel.kind != PmltqSelectorKind::ARoot) {
        tb.add(sel);
        return;
    }
    const std::string how = " (pando has no root node: a-root [ child a-node [ … ] ] is the sentence's root word, "
                            "a-root [ descendant a-node [ … ] ] any word)";
    if (!sel.has_cond)
        fail("a-root on its own" + how);
    const PmltqCondExpr& c = sel.cond;
    if (c.kind != PmltqCondKind::Nested || c.nested->subquery || c.nested->optional ||
        (c.nested->relation != "child" && c.nested->relation != "descendant") ||
        c.nested->node->kind == PmltqSelectorKind::ARoot)
        fail("a-root with conditions other than one child or descendant node" + how);
    const size_t idx = tb.add(*c.nested->node);
    if (c.nested->relation == "child")
        tb.nodes[idx].conds.push_back(lower_test_root());
}

} // namespace

Program lower_native_pmltq_query(const PmltqQuery& q, std::string& err) {
    Program prog;
    try {
        TreeBuilder tb;
        add_selector(tb, q.selector);
        const bool single = tb.nodes.size() == 1;
        if (!single)
            name_nodes(tb.nodes);

        Statement stmt;
        stmt.has_query = true;
        TokenQuery& tq = stmt.query;
        apply_references(tb.pending, tb.nodes, tq);

        const std::vector<size_t> order = chain_order(tb.nodes, tb.edges);
        std::vector<size_t> slot(tb.nodes.size());
        for (size_t i = 0; i < order.size(); ++i) {
            QNode& n = tb.nodes[order[i]];
            slot[order[i]] = i;
            QueryToken qt;
            qt.name = n.name;
            for (auto& c : n.conds)
                qt.conditions = qt.conditions ? ConditionNode::make_branch(BoolOp::AND, qt.conditions, c) : c;
            tq.tokens.push_back(std::move(qt));
            if (i > 0)
                tq.relations.push_back({relation_between(tb.edges, order[i - 1], order[i])});
        }
        // different nodes, different tokens (neighbours in the chain are)
        for (size_t a = 0; a < tb.nodes.size(); ++a)
            for (size_t b = a + 1; b < tb.nodes.size(); ++b) {
                const QNode &na = tb.nodes[a], &nb = tb.nodes[b];
                if (na.hidden || nb.hidden || na.overlapping || nb.overlapping)
                    continue;
                if (slot[a] + 1 == slot[b] || slot[b] + 1 == slot[a])
                    continue;
                tq.position_orders.push_back({na.name, nb.name, CompOp::NEQ});
            }
        prog.push_back(std::move(stmt));

        if (q.has_output) {
            std::unordered_set<std::string> names;
            for (const auto& n : tb.nodes)
                names.insert(n.name);
            for (const auto& it : q.output.items)
                if (!names.count(it.node) && !(single && q.selector.bind_name.empty()))
                    fail("no node is called $" + it.node + " (in >>)");
            std::vector<std::string> fields = fields_from_output_block(q.output);
            if (fields.empty())
                fail(">> block needs field references or count(over node.attr)");
            Statement cmd;
            cmd.has_command = true;
            cmd.command.type = CommandType::COUNT;
            cmd.command.fields = std::move(fields);
            prog.push_back(std::move(cmd));
        }
    } catch (const std::runtime_error& e) {
        err = e.what();
    }
    return prog;
}

} // namespace pando::pmltq
