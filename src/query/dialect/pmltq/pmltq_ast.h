#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace pando::pmltq {

// The PML-TQ query part as pando reads it (tree_query_grammar.txt of perl-pmltq: selector,
// conditions, nested_selector, subquery, test). A selector's conditions are a list joined by
// `,` / `and`; a nested selector in that list is a node of the match (a join), a nested
// selector inside a test (after `and`, under `or` / `!`, or with occurrences `Nx`) is a
// subquery: a condition on the outer node.

struct PmltqSpan {
    size_t begin = 0;
    size_t end = 0;
};

enum class PmltqSelectorKind {
    ANode,  // any node type (a-node, t-node, node, *, or none given): a token
    ARoot,
    Tok,
};

enum class PmltqCondKind {
    Test,   // field op value
    And,
    Or,
    Not,    // `!` (children[0])
    Nested, // a nested selector: a join (no occurrences) or a subquery (occurrences)
    Same,   // `$a = $b` / `$a != $b`: the same node or not (node = a, ref_node = b, op)
};

struct PmltqNested;

/// One side of a test that refers to a node: `$a.lemma` (node "a"); "" = the node itself.
struct PmltqCondExpr {
    PmltqCondKind kind = PmltqCondKind::Test;
    std::string node;      // Test: the node of `field` ("" = this node); Same: the first node
    std::string field;     // attribute (`lemma`, `feats/Case`)
    std::string op;        // = != ~ !~ ~* !~* < > <= >= in !in
    std::string value;     // a literal (string or number), unless ref_node is set
    std::string ref_node;  // value is `$ref_node.ref_attr` (Test), or the second node (Same)
    std::string ref_attr;
    std::vector<std::string> set_values; // op in / !in
    size_t byte_offset = 0;
    std::vector<PmltqCondExpr> children;
    std::shared_ptr<PmltqNested> nested;
};

struct PmltqSelector {
    PmltqSelectorKind kind = PmltqSelectorKind::ANode;
    std::string type_name;   // as written (a-node, t-node, …; "" when none)
    std::string bind_name;   // `$a :=`
    bool overlapping = false; // `+`: may be the same token as another node
    bool has_cond = false;
    PmltqCondExpr cond;      // And of the condition list (or one condition)
};

/// Occurrences of a subquery (`2x`, `2+x`, `2-x`, `1..3x`, alternatives with `|`): intervals,
/// max -1 = no upper bound. A subquery without occurrences has {1, -1}.
struct PmltqOccurrence {
    int64_t min = 1;
    int64_t max = -1;
};

struct PmltqNested {
    std::string relation = "child"; // child parent sibling descendant ancestor …
    bool bounded_length = false;    // descendant{1,3} and the like
    bool optional = false;          // `?`
    bool subquery = false;          // a condition (occurrences) rather than a node of the match
    std::vector<PmltqOccurrence> occurrences;
    std::shared_ptr<PmltqSelector> node;
    size_t byte_offset = 0;
};

// `>>` return block (subset): comma-separated `distinct` field refs and/or `count(over $node.attr)`.
enum class PmltqOutputItemKind {
    FieldRef,  // $a.form or a.form (from distinct list)
    CountOver, // count(over $a.form)
};

struct PmltqOutputItem {
    PmltqOutputItemKind kind = PmltqOutputItemKind::FieldRef;
    std::string node;
    std::string attr;
};

struct PmltqOutputBlock {
    bool saw_distinct_keyword = false;
    std::vector<PmltqOutputItem> items;
};

struct PmltqQuery {
    PmltqSelector selector;
    bool has_output = false;
    PmltqOutputBlock output;
};

} // namespace pando::pmltq
