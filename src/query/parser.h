#pragma once

#include "query/ast.h"
#include "query/lexer.h"
#include <string>

namespace pando {

struct ParserOptions {
    /// When true: only `/pattern/` is regex inside `[...]`; quoted strings are always literal
    /// (legacy pando behavior). When false (default): `attr = "x"` uses the same heuristic as
    /// the CWB dialect — if `x` contains no regex metacharacters, literal equality; otherwise
    /// anchored whole-token regex `^x$`.
    bool strict_quoted_strings = false;
    /// The attribute a bare "string" stands for in the Manatee and CWB dialects (`word` when empty):
    /// a front-end's chosen default attribute (KonText's simple query, Manatee's DEFAULTATTR), the
    /// query itself unchanged.
    std::string default_attr;
};

// Recursive-descent parser for ClickCQL.
//
// Parses semicolon-separated statements.  Each statement is either:
//   - A token query (possibly named)
//   - A grouping command (count, group, sort, freq, coll, dcoll, cat, size, ...)
//
// The parser handles the ambiguity between dependency operators (> <) and
// comparison operators by context: inside [] they are comparison, outside
// they are dependency relations.
class Parser {
public:
    explicit Parser(const std::string& input, ParserOptions opts = {});

    Program parse();

    /// The filters after a `::` (the text without the `::`), added to `tq` (its tokens already parsed): for a
    /// dialect that reads CQP's global constraint with pando's own rules. Anything left over is an error.
    static void parse_global_filter_text(const std::string& text, TokenQuery& tq, ParserOptions opts = {});

    /// A command keyword (count, sort, filter, size, show, …): a statement starting with it is a command.
    static bool is_command_keyword(const std::string& text);

private:
    Statement parse_statement();
    Statement parse_statement_with_source();
    std::string input_;

    GroupCommand parse_command();

    TokenQuery  parse_token_query();
    QueryToken  parse_token_expr();
    ConditionPtr parse_conditions();
    ConditionPtr parse_or_condition();
    ConditionPtr parse_and_condition();
    ConditionPtr parse_primary_condition();

    // Parse optional repetition quantifier after a token: {n,m}, {n,}, {n}, +, ?
    void parse_repetition(QueryToken& qt);

    // Try to parse a relation operator between tokens.
    // Returns true and fills `rel` if a relation is found.
    // in_filter: after `::`, where `!=` without a layer compares positions (`:: a != b`)
    bool try_parse_relation(QueryRelation& rel, bool in_filter = false);

    // Parse the within clause: "within s", "within s_tuid='X'", "within s having [cond]"
    void parse_within_clause(TokenQuery& tq, bool negated = false);

    // Parse "containing s", "containing subtree [cond]"
    void parse_containing_clause(TokenQuery& tq, bool negated);

    // #12: Parse :: global_region_filter and/or global_alignment_filter [ & ... ]*
    void parse_global_filters(TokenQuery& tq);
    /// The value of a region filter (`within x op V`, `:: match.x op V`, `:: x op V`): a
    /// string, number, word or `/pattern/`; a quoted value with metacharacters is a pattern,
    /// exactly as for token attributes (CQL-REGION-PATTERN).
    void read_region_filter_value(GlobalRegionFilter& gf);

    Lexer lexer_;
    ParserOptions opts_;
};

} // namespace pando
