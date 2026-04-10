#pragma once

#include "core/types.h"
#include <string>
#include <vector>
#include <memory>
#include <variant>

namespace pando {

// ── Relation types between query tokens ─────────────────────────────────

enum class RelationType {
    SEQUENCE,        // [] []   — adjacent in text
    GOVERNS,         // [] > [] — head governs dependent
    GOVERNED_BY,     // [] < [] — dependent governed by head
    TRANS_GOVERNS,   // [] >> []
    TRANS_GOV_BY,    // [] << []
    NOT_GOVERNS,     // [] !> []
    NOT_GOV_BY,      // [] !< []
};

// ── Comparison operators ────────────────────────────────────────────────

enum class CompOp {
    EQ,       // =
    NEQ,      // !=
    LT,       // <
    GT,       // >
    LTE,      // <=
    GTE,      // >=
    REGEX,    // = /pattern/
};

// ── Structural relation types for nested conditions ───────────────────────

enum class StructRelType {
    CHILD,       // direct child (head→dependent)
    PARENT,      // direct parent (dependent→head)
    SIBLING,     // same head
    DESCENDANT,  // transitive child
    ANCESTOR,    // transitive parent
};

// ── A single attribute condition inside [] ──────────────────────────────

struct AttrCondition {
    std::string attr;        // e.g. "upos", "lemma", "feats.Number"
    CompOp op = CompOp::EQ;
    std::string value;       // string/regex value
    bool case_insensitive = false;       // %c flag
    bool diacritics_insensitive = false; // %d flag

    // #25: Pre-resolved lexicon ID for EQ comparisons (populated by compile_conditions).
    // When >= 0, check_leaf uses integer comparison instead of string comparison.
    int32_t resolved_id = -1;  // -1 = UNKNOWN_LEX = not resolved

    // RG-5f / Stage 1: multivalue component id (mv_lookup on .mv.lex) for EQ/NEQ.
    // When >= 0 and .mv.fwd is loaded, check_leaf tests membership in the sorted
    // forward set; when == UNKNOWN_LEX (-1), mv_lookup missed (unknown component).
    int32_t resolved_mv_component_id = -1;

    // nvals(attr) op N — cardinality of explicit pipe-separated MV values (and region overlap).
    // When true, `op` compares the computed count to `nvals_compare` (integer RHS).
    bool is_nvals = false;
    int64_t nvals_compare = 0;

    // When op==REGEX: if true, match the whole token string (RE2::FullMatch / std::regex_match);
    // if false, substring match (RE2::PartialMatch / std::regex_search). Quoted-string CWB-style
    // heuristics set this instead of wrapping the pattern in ^$.
    bool regex_full_match = false;
};

// ── Boolean combination of conditions ───────────────────────────────────

enum class BoolOp { AND, OR };

struct ConditionNode;
using ConditionPtr = std::shared_ptr<ConditionNode>;

struct ConditionNode {
    // Leaf: single attribute condition
    // Branch: boolean combination
    // Structural: nested condition on related positions
    bool is_leaf = true;

    // Leaf fields
    AttrCondition leaf;

    // Branch fields
    BoolOp       bool_op = BoolOp::AND;
    ConditionPtr left;
    ConditionPtr right;

    // Structural relation condition fields (is_leaf=false, left=nullptr, right=nullptr)
    bool is_structural = false;
    bool struct_negated = false;      // true for [not child [...]]
    StructRelType struct_rel;
    ConditionPtr  nested_conditions;  // conditions that the related token must satisfy
    std::string   nested_name;        // optional name for the nested token (for :: constraints)

    // Count condition fields (is_leaf=false, is_structural=false, is_count=true)
    // count(child[upos="ADJ"]) >= 3  — count related positions matching a filter
    bool is_count = false;
    StructRelType count_rel;          // which relation to count over (CHILD, DESCENDANT, ...)
    ConditionPtr  count_filter;       // optional filter on the related positions (null = count all)
    CompOp        count_op = CompOp::EQ;
    int64_t       count_value = 0;

    static ConditionPtr make_leaf(AttrCondition c) {
        auto n = std::make_shared<ConditionNode>();
        n->is_leaf = true;
        n->leaf = std::move(c);
        return n;
    }
    static ConditionPtr make_branch(BoolOp op, ConditionPtr l, ConditionPtr r) {
        auto n = std::make_shared<ConditionNode>();
        n->is_leaf = false;
        n->bool_op = op;
        n->left = std::move(l);
        n->right = std::move(r);
        return n;
    }
    static ConditionPtr make_structural(StructRelType rel, ConditionPtr nested, const std::string& name = "", bool negated = false) {
        auto n = std::make_shared<ConditionNode>();
        n->is_leaf = false;
        n->is_structural = true;
        n->struct_negated = negated;
        n->struct_rel = rel;
        n->nested_conditions = std::move(nested);
        n->nested_name = name;
        return n;
    }
    static ConditionPtr make_count(StructRelType rel, ConditionPtr filter, CompOp op, int64_t value) {
        auto n = std::make_shared<ConditionNode>();
        n->is_leaf = false;
        n->is_count = true;
        n->count_rel = rel;
        n->count_filter = std::move(filter);
        n->count_op = op;
        n->count_value = value;
        return n;
    }
};

// ── Repetition constants ────────────────────────────────────────────────

static constexpr int REPEAT_UNBOUNDED = 100;  // practical cap for {n,} and +

// ── A single query token ────────────────────────────────────────────────

// ── Region anchor types ──────────────────────────────────────────────────

enum class RegionAnchorType {
    NONE,          // normal token — occupies a position
    REGION_START,  // <s> — zero-width, binds to next token's position
    REGION_END,    // </s> — zero-width, binds to previous token's position
};

/// Suffix clauses on `<region ...>` (any order, whitespace-separated): rchild(vp), rcontains(vp), contains(vp).
/// (`child` is reserved for dependency relations in `[]`, not region-tree parent.)
enum class AnchorRegionClauseKind {
    RchildOf,    // rchild(vp) — immediate parent row == region bound as vp (.par)
    RcontainsOf, // rcontains(vp) — vp is in this row's subtree (.par ancestry)
    Contains,    // contains(vp) — this row's span geometrically contains vp's span
};

struct AnchorRegionClause {
    AnchorRegionClauseKind kind = AnchorRegionClauseKind::RchildOf;
    std::string            peer_label;
};

struct QueryToken {
    std::string    name;              // optional label (e.g. "verb:")
    ConditionPtr   conditions;        // may be null (empty token [])
    int            min_repeat = 1;    // {min,max} repetition; default 1,1 = exact single token
    int            max_repeat = 1;    // REPEAT_UNBOUNDED for {n,} and +

    // Region boundary anchor (zero-width, does not consume a position)
    RegionAnchorType anchor = RegionAnchorType::NONE;
    std::string      anchor_region;   // region name, e.g. "s", "text", "np"
    std::vector<std::pair<std::string, std::string>> anchor_attrs;  // <text genre="book"> → {{"genre","book"}}
    /// Optional peer clauses: <node contains(vp) type="NP" rchild(pp)> — AND of all.
    std::vector<AnchorRegionClause> anchor_region_clauses;
    /// `<err …> where MM, NN`: names of previously-bound match sets that this
    /// anchor must intersect. AND semantics. Resolved by the CLI driver before
    /// the query is handed to the executor — the executor sees `where_positions`
    /// (one sorted vector of token positions per ref).
    std::vector<std::string> where_refs;
    std::vector<std::vector<CorpusPos>> where_positions;

    bool has_repetition() const { return min_repeat != 1 || max_repeat != 1; }
    bool is_anchor() const { return anchor != RegionAnchorType::NONE; }
};

// ── A relation edge between two tokens in the query chain ───────────────

struct QueryRelation {
    RelationType type;
};

// ── Global filter (#12): :: match.region_attr op value, or a.attr = b.attr ───

struct GlobalRegionFilter {
    std::string anchor_name;   // token name to resolve position (empty → match.first_pos)
    std::string region_attr;   // e.g. "text_year" (region type + attr)
    CompOp      op = CompOp::EQ;
    std::string value;
};

struct GlobalAlignmentFilter {
    std::string name1, attr1;
    std::string name2, attr2;
};

// :: distance(a, b) < 5, :: f(a.lemma) >= 100, :: depth(a) > depth(b), :: contains(s, np) = 1
enum class GlobalFunctionType {
    DISTANCE,
    DISTABS,
    STRLEN,
    FREQ,
    NCHILDREN,
    DEPTH,
    NDESCENDANTS,
    NVALS,
    /// Layer A: B's token span ⊆ A's span (both args = named region bindings).
    CONTAINS,
    /// Layer B (nested + `.par`): child row's parent id == parent's region index.
    RCHILD,
    /// Layer B: parent dominates child on `.par` tree (transitive; reflexive).
    RCONTAINS
};

// A single function call: func(args...)
struct GlobalFuncCall {
    GlobalFunctionType func;
    std::vector<std::string> args;  // token names or name.attr specs
};

struct GlobalFunctionFilter {
    GlobalFuncCall lhs;                     // left-hand side: always a function call
    CompOp op = CompOp::EQ;
    int64_t int_value = 0;                  // numeric RHS (used when !rhs_func)
    bool has_rhs_func = false;              // true when RHS is a function call
    GlobalFuncCall rhs;                     // right-hand side function (when has_rhs_func)
};

// ── Structural containment operators ──────────────────────────────────────

struct ContainingClause {
    std::string region;          // structural region name (e.g. "s", "np") — empty if subtree mode
    bool is_subtree = false;     // "containing subtree [cond]" — dependency subtree containment
    ConditionPtr subtree_cond;   // condition for subtree root token
    bool negated = false;        // "not containing ..."
};

// ── The full token query ────────────────────────────────────────────────

struct TokenQuery {
    std::vector<QueryToken>    tokens;
    std::vector<QueryRelation> relations;  // relations[i] is between tokens[i] and tokens[i+1]
    std::string                within;     // "s", "p", "text" or empty
    bool                       not_within = false;  // "not within s"
    ConditionPtr               within_having;  // "within s having [cond]": existential check on region
    std::vector<ContainingClause>     containing_clauses;  // "containing s", "containing subtree [cond]"
    std::vector<GlobalRegionFilter>   global_region_filters;    // :: match.text_year > 2000
    std::vector<GlobalAlignmentFilter> global_alignment_filters; // :: a.tuid = b.tuid
    std::vector<GlobalFunctionFilter>  global_function_filters;   // :: distance(a,b) < 5

    // Global position ordering constraints: :: a < b
    struct PositionOrder {
        std::string name1, name2;
        CompOp op;  // LT or GT
    };
    std::vector<PositionOrder> position_orders;
};

// ── Display / grouping commands ─────────────────────────────────────────

enum class CommandType {
    CONCORDANCE,   // default: show matches
    COUNT,
    GROUP,
    SORT,
    FREQ,
    COLL,
    DCOLL,
    CAT,
    SIZE,
    RAW,
    SHOW_ATTRS,
    SHOW_REGIONS,
    SHOW_NAMED,
    SHOW_INFO,
    SHOW_VALUES,   // show values <attr> — unique values + counts
    TABULATE,
    KEYNESS,       // keyness by <attr> — subcorpus keyword extraction (#40)
    SET,           // set <name> <value> — change runtime option (#41)
    SHOW_SETTINGS, // show settings — display all runtime options (#41)
    DROP,          // drop <name> or drop all
};

struct GroupCommand {
    CommandType type;
    std::string query_name;          // which named query to operate on
    std::vector<std::string> query_names; // multiple named queries for comparison (freq Q1, Q2 by attr)
    std::vector<std::string> fields; // fields to group/sort/coll by

    // dcoll-specific: unified relation list
    // Special names: "head" (go up), "descendants" (full subtree)
    // Any other name is a deprel filter on children (e.g. "amod", "nsubj")
    // Empty = all children (default)
    std::vector<std::string> relations;

    // dcoll anchor: named token to start from (empty = first_pos)
    std::string dcoll_anchor;

    // keyness: optional reference query name for "keyness M vs N by attr"
    // When empty, reference = rest of corpus (default).
    std::string ref_query_name;

    // freq only: "freq Q1, Q2 by …" — compare grouped frequencies across named queries.
    // When empty, freq uses the last result (or query_name is ignored for freq; use this list).
    std::vector<std::string> freq_query_names;

    // set: setting name and value for "set <name> <value>"
    std::string set_name;
    std::string set_value;

    // tabulate: CWB-style optional window [offset limit] before fields (default limit 1000)
    size_t tabulate_offset = 0;
    size_t tabulate_limit = 1000;
};

// ── Top-level statement ─────────────────────────────────────────────────

struct Statement {
    std::string name;                // if named query ("Nouns = ...")
    TokenQuery  query;               // the token query
    bool        has_query = false;

    // #16: Source | Target parallel query (when true, query is source, target_query is target)
    bool        is_parallel = false;
    TokenQuery  target_query;        // target side when is_parallel

    GroupCommand command;
    bool         has_command = false;
};

using Program = std::vector<Statement>;

} // namespace pando
