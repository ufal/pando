#include "query/dialect/cwb/cwb_parser.h"

#include "query/dialect/cwb/cwb_lexer.h"
#include "query/quoted_string_pattern.h"

#include <cctype>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace manatree {

namespace {

using namespace cwb;

struct Repeat {
    int min = 1;
    int max = 1;
};

struct TokStream {
    const std::vector<CwbToken>* v = nullptr;
    std::size_t i = 0;

    const CwbToken& peek() const { return (*v)[i]; }
    void bump() { ++i; }
    bool eof() const {
        return v == nullptr || i >= v->size() || (*v)[i].kind == CwbTok::END;
    }
};

void set_attr_pattern(AttrCondition& ac, const std::string& raw, std::ostringstream* trace) {
    interpret_quoted_eq_string(ac, raw, /*strict_quoted_strings=*/false);
    if (trace) {
        if (ac.op == CompOp::EQ)
            *trace << "        (literal → EQ)\n";
        else
            *trace << "        (regex → REGEX /^...$/ whole-token)\n";
    }
}

void apply_string_flags(AttrCondition& ac, const std::string& flags) {
    for (char f : flags) {
        switch (f) {
        case 'c':
            ac.case_insensitive = true;
            break;
        case 'd':
            ac.diacritics_insensitive = true;
            break;
        case 'l':
            ac.op = CompOp::EQ;
            ac.value = ac.value; // literal — already set
            break;
        default:
            break;
        }
    }
}

std::vector<CwbToken> tokenize_all(std::string_view s) {
    CwbLexer lx(s);
    std::vector<CwbToken> out;
    for (;;) {
        CwbToken t = lx.next();
        out.push_back(std::move(t));
        if (out.back().kind == CwbTok::END)
            break;
    }
    return out;
}

std::vector<std::string> split_cwb_segments(const std::string& input) {
    std::vector<std::string> out;
    std::string cur;
    cur.reserve(input.size());
    bool in_str = false;
    for (std::size_t i = 0; i < input.size(); ++i) {
        char c = input[i];
        if (c == '"') {
            in_str = !in_str;
            cur.push_back(c);
            continue;
        }
        if (!in_str && c == ';') {
            out.push_back(std::move(cur));
            cur.clear();
            continue;
        }
        cur.push_back(c);
    }
    out.push_back(std::move(cur));
    return out;
}

static bool is_cqp_shell_keyword(CwbTok k) {
    switch (k) {
    case CwbTok::GROUP_SYM:
    case CwbTok::SORT_SYM:
    case CwbTok::CAT_SYM:
    case CwbTok::SAVE_SYM:
    case CwbTok::SHOW_SYM:
    case CwbTok::SET_SYM:
    case CwbTok::TABULATE_SYM:
    case CwbTok::DISCARD_SYM:
    case CwbTok::INTER_SYM:
    case CwbTok::UNION_SYM:
    case CwbTok::DIFF_SYM:
    case CwbTok::JOIN_SYM:
    case CwbTok::SUBSET_SYM:
    case CwbTok::INFO_SYM:
    case CwbTok::DUMP_SYM:
    case CwbTok::UNDUMP_SYM:
    case CwbTok::SIZE_SYM:
    case CwbTok::CUT_SYM:
    case CwbTok::MU_SYM:
    case CwbTok::TAB_SYM:
    case CwbTok::EXEC_SYM:
    case CwbTok::EXIT_SYM:
    case CwbTok::CD_SYM:
    case CwbTok::DEFINE_SYM:
    case CwbTok::DELETE_SYM:
    case CwbTok::REDUCE_SYM:
    case CwbTok::MAXIMAL_SYM:
    case CwbTok::SLEEP_SYM:
    case CwbTok::UNLOCK_SYM:
    case CwbTok::USER_SYM:
    case CwbTok::HOST_SYM:
    case CwbTok::MACRO_SYM:
    case CwbTok::RANDOMIZE_SYM:
    case CwbTok::MEET_SYM:
        return true;
    default:
        return false;
    }
}

[[noreturn]] void shell_keyword_error(const std::string& name, std::size_t off) {
    throw std::runtime_error(
        "CQP command '" + name +
        "' is not implemented in the CWB dialect yet (offset " + std::to_string(off) +
        "). Supported CWB-style commands here include `count by <attr>` and `group by <field>[, ...]` "
        "(see IMS SortCmd). "
        "Use `--cql native` for full pando commands.");
}

CwbTok peek2_kind(const TokStream& ts) {
    if (ts.i + 1 >= ts.v->size())
        return CwbTok::END;
    return (*ts.v)[ts.i + 1].kind;
}

void expect_tok(TokStream& ts, CwbTok k, const char* ctx) {
    if (ts.eof() || ts.peek().kind != k) {
        std::string got = ts.eof() ? "end" : "unexpected token";
        throw std::runtime_error(std::string("Expected ") + ctx + " (" + got + ")");
    }
    ts.bump();
}

ConditionPtr parse_bool_expr_implies(TokStream& ts, std::ostringstream* trace);
ConditionPtr parse_rel_expr(TokStream& ts, std::ostringstream* trace);

std::string parse_rel_lhs(TokStream& ts) {
    if (ts.eof())
        throw std::runtime_error("Unexpected end inside [ ]");
    CwbTok k = ts.peek().kind;
    if (k == CwbTok::TILDE) {
        ts.bump();
        throw std::runtime_error("Unsupported: '~' label auto-delete in attribute reference");
    }
    if (k == CwbTok::FIELD) {
        ts.bump();
        throw std::runtime_error(
            "Unsupported: bare match/matchend/target field reference inside [ ] for pando CWB dialect");
    }
    if (k == CwbTok::ID || k == CwbTok::QID) {
        std::string attr = ts.peek().text;
        ts.bump();
        if (!ts.eof() && ts.peek().kind == CwbTok::LPAREN)
            throw std::runtime_error("Unsupported: function call inside [ ]");
        return attr;
    }
    throw std::runtime_error("Expected attribute name or comparison inside [ ]");
}

bool is_rel_op(CwbTok k) {
    switch (k) {
    case CwbTok::EQ:
    case CwbTok::NEQ:
    case CwbTok::LT:
    case CwbTok::GT:
    case CwbTok::LET:
    case CwbTok::GET:
        return true;
    default:
        return false;
    }
}

CompOp to_comp_op(CwbTok k) {
    switch (k) {
    case CwbTok::EQ:
        return CompOp::EQ;
    case CwbTok::NEQ:
        return CompOp::NEQ;
    case CwbTok::LT:
        return CompOp::LT;
    case CwbTok::GT:
        return CompOp::GT;
    case CwbTok::LET:
        return CompOp::LTE;
    case CwbTok::GET:
        return CompOp::GTE;
    default:
        return CompOp::EQ;
    }
}

ConditionPtr parse_rel_expr(TokStream& ts, std::ostringstream* trace) {
    std::string attr = parse_rel_lhs(ts);
    if (ts.eof())
        throw std::runtime_error(
            "Incomplete relation expression: expected operator and value after attribute");

    CwbTok k = ts.peek().kind;
    if (k == CwbTok::CONTAINS_SYM || k == CwbTok::MATCHES_SYM)
        throw std::runtime_error(
            "Unsupported: contains/matches on multi-valued attributes (not mapped to pando yet)");
    if (k == CwbTok::NOT_SYM &&
        (peek2_kind(ts) == CwbTok::CONTAINS_SYM || peek2_kind(ts) == CwbTok::MATCHES_SYM))
        throw std::runtime_error(
            "Unsupported: not contains / not matches (multi-valued attributes)");

    if (!is_rel_op(k))
        throw std::runtime_error(
            "Unsupported: bare attribute or relation fragment without comparison operator "
            "(use e.g. lemma = \"x\")");

    CompOp op = to_comp_op(k);
    ts.bump();

    AttrCondition ac;
    ac.attr = std::move(attr);

    if (ts.eof())
        throw std::runtime_error("Expected value after comparison operator");

    if (ts.peek().kind == CwbTok::STRING) {
        ac.value = ts.peek().text;
        ts.bump();
        std::string flags;
        while (!ts.eof() && ts.peek().kind == CwbTok::FLAG) {
            flags += ts.peek().text;
            ts.bump();
        }
        if (flags.find('l') != std::string::npos) {
            ac.op = op;
            ac.value = ac.value;
        } else if (op == CompOp::EQ) {
            set_attr_pattern(ac, ac.value, trace);
        } else if (op == CompOp::NEQ) {
            validate_neq_quoted_string(ac.value, /*strict_quoted_strings=*/false);
            ac.op = CompOp::NEQ;
        } else {
            ac.op = op;
        }
        apply_string_flags(ac, flags);
        return ConditionNode::make_leaf(std::move(ac));
    }

    if (ts.peek().kind == CwbTok::INTEGER) {
        ac.op = op;
        ac.value = std::to_string(ts.peek().ival);
        ts.bump();
        return ConditionNode::make_leaf(std::move(ac));
    }

    if (ts.peek().kind == CwbTok::DOUBLE) {
        ac.op = op;
        ac.value = ts.peek().text;
        ts.bump();
        return ConditionNode::make_leaf(std::move(ac));
    }

    throw std::runtime_error(
        "Unsupported: right-hand side of comparison (expected string or number literal)");
}

ConditionPtr parse_bool_unary(TokStream& ts, std::ostringstream* trace) {
    if (ts.eof())
        throw std::runtime_error("Unexpected end inside [ ]");
    if (ts.peek().kind == CwbTok::BANG) {
        ts.bump();
        throw std::runtime_error(
            "Unsupported: boolean negation (!) inside [ ] (not represented in pando condition AST)");
    }
    if (ts.peek().kind == CwbTok::LPAREN) {
        ts.bump();
        ConditionPtr inner = parse_bool_expr_implies(ts, trace);
        expect_tok(ts, CwbTok::RPAREN, "')' to close parenthesised boolean expression");
        return inner;
    }
    return parse_rel_expr(ts, trace);
}

ConditionPtr parse_bool_and(TokStream& ts, std::ostringstream* trace) {
    ConditionPtr left = parse_bool_unary(ts, trace);
    while (!ts.eof() && ts.peek().kind == CwbTok::AMP) {
        ts.bump();
        ConditionPtr right = parse_bool_unary(ts, trace);
        left = ConditionNode::make_branch(BoolOp::AND, std::move(left), std::move(right));
    }
    return left;
}

ConditionPtr parse_bool_or(TokStream& ts, std::ostringstream* trace) {
    ConditionPtr left = parse_bool_and(ts, trace);
    while (!ts.eof() && ts.peek().kind == CwbTok::PIPE) {
        ts.bump();
        ConditionPtr right = parse_bool_and(ts, trace);
        left = ConditionNode::make_branch(BoolOp::OR, std::move(left), std::move(right));
    }
    return left;
}

ConditionPtr parse_bool_expr_implies(TokStream& ts, std::ostringstream* trace) {
    ConditionPtr left = parse_bool_or(ts, trace);
    if (!ts.eof() && ts.peek().kind == CwbTok::IMPLIES)
        throw std::runtime_error(
            "Unsupported: boolean implication (->) inside [ ] (not mapped to pando)");
    return left;
}

QueryToken parse_wordform_pattern(TokStream& ts, std::ostringstream* trace) {
    if (ts.eof())
        throw std::runtime_error("Unexpected end in token pattern");
    CwbTok k = ts.peek().kind;
    if (k == CwbTok::MATCHALL) {
        ts.bump();
        QueryToken qt;
        qt.conditions = nullptr;
        return qt;
    }
    if (k == CwbTok::LCMATCHALL || k == CwbTok::LCSTART)
        throw std::runtime_error(
            "Unsupported: lookahead constraints [: … :] / [::] (not implemented)");
    if (k == CwbTok::STRING) {
        ts.bump();
        throw std::runtime_error(
            "Bare string token pattern is CQP ExtConstraint; use an explicit attribute, e.g. "
            "[lemma=\"…\"] or [word=\"…\"]");
    }
    if (k == CwbTok::VARIABLE) {
        ts.bump();
        throw std::runtime_error("Unsupported: CQP variable reference in token pattern");
    }
    if (k == CwbTok::LBRACK) {
        ts.bump();
        if (ts.eof())
            throw std::runtime_error("Unclosed '[' in token pattern");
        if (ts.peek().kind == CwbTok::RBRACK) {
            ts.bump();
            QueryToken qt;
            qt.conditions = nullptr;
            return qt;
        }
        ConditionPtr cond = parse_bool_expr_implies(ts, trace);
        expect_tok(ts, CwbTok::RBRACK, "']' after boolean expression");
        QueryToken qt;
        qt.conditions = std::move(cond);
        return qt;
    }
    throw std::runtime_error("Malformed WordformPattern");
}

bool starts_reg_wordf_power(const CwbToken& t) {
    switch (t.kind) {
    case CwbTok::LPAREN:
    case CwbTok::STRING:
    case CwbTok::VARIABLE:
    case CwbTok::MATCHALL:
    case CwbTok::LCMATCHALL:
    case CwbTok::LCSTART:
    case CwbTok::LABEL:
    case CwbTok::FIELDLABEL:
    case CwbTok::AT:
    case CwbTok::TAGSTART:
    case CwbTok::ANCHORTAG:
    case CwbTok::ANCHORENDTAG:
    case CwbTok::LEFT_APPEND:
    case CwbTok::LBRACK:
        return true;
    case CwbTok::MU_SYM:
    case CwbTok::TAB_SYM:
        return true;
    default:
        return false;
    }
}

Repeat parse_repeat_suffix(TokStream& ts) {
    if (ts.eof())
        return {1, 1};
    CwbTok k = ts.peek().kind;
    if (k == CwbTok::STAR) {
        ts.bump();
        return {0, static_cast<int>(REPEAT_UNBOUNDED)};
    }
    if (k == CwbTok::PLUS) {
        ts.bump();
        return {1, static_cast<int>(REPEAT_UNBOUNDED)};
    }
    if (k == CwbTok::QUEST) {
        ts.bump();
        return {0, 1};
    }
    if (k == CwbTok::LBRACE) {
        ts.bump();
        if (ts.eof())
            throw std::runtime_error("Unclosed '{' in repetition");
        if (ts.peek().kind == CwbTok::COMMA) {
            ts.bump();
            if (ts.eof() || ts.peek().kind != CwbTok::INTEGER)
                throw std::runtime_error("Expected integer in {,m} repetition");
            int m = ts.peek().ival;
            ts.bump();
            expect_tok(ts, CwbTok::RBRACE, "'}'");
            return {0, m};
        }
        if (ts.peek().kind != CwbTok::INTEGER)
            throw std::runtime_error("Expected integer in {} repetition");
        int n = ts.peek().ival;
        ts.bump();
        if (ts.eof())
            throw std::runtime_error("Unclosed '{' in repetition");
        if (ts.peek().kind == CwbTok::RBRACE) {
            ts.bump();
            return {n, n};
        }
        if (ts.peek().kind == CwbTok::COMMA) {
            ts.bump();
            if (ts.eof())
                throw std::runtime_error("Unclosed '{' in repetition");
            if (ts.peek().kind == CwbTok::RBRACE) {
                ts.bump();
                return {n, static_cast<int>(REPEAT_UNBOUNDED)};
            }
            if (ts.peek().kind != CwbTok::INTEGER)
                throw std::runtime_error("Expected integer after ',' in repetition");
            int m = ts.peek().ival;
            ts.bump();
            expect_tok(ts, CwbTok::RBRACE, "'}'");
            if (m < n)
                throw std::runtime_error("Invalid repetition range (max < min)");
            return {n, m};
        }
        throw std::runtime_error("Malformed {} repetition");
    }
    return {1, 1};
}

TokenQuery parse_reg_wordf_expr(TokStream& ts, std::ostringstream* trace);

TokenQuery parse_reg_wordf_power_inner(TokStream& ts, std::ostringstream* trace) {
    if (ts.eof())
        throw std::runtime_error("Unexpected end in token pattern");
    CwbTok k = ts.peek().kind;
    if (k == CwbTok::MU_SYM || k == CwbTok::TAB_SYM)
        throw std::runtime_error("Unsupported: MU / TAB query forms (not implemented)");

    if (k == CwbTok::TAGSTART || k == CwbTok::ANCHORTAG || k == CwbTok::ANCHORENDTAG)
        throw std::runtime_error(
            "Unsupported: XML / anchor tags in token pattern (not mapped to pando)");
    if (k == CwbTok::LEFT_APPEND)
        throw std::runtime_error("Unsupported: region append syntax (<<) in token pattern");

    if (k == CwbTok::AT)
        throw std::runtime_error(
            "Unsupported: @ target / keyword markers in NamedWfPattern (not implemented)");

    if (k == CwbTok::LPAREN) {
        ts.bump();
        TokenQuery inner = parse_reg_wordf_expr(ts, trace);
        expect_tok(ts, CwbTok::RPAREN, "')' after grouped token pattern");
        return inner;
    }

    if (k == CwbTok::LABEL || k == CwbTok::FIELDLABEL) {
        std::string name = ts.peek().text;
        ts.bump();
        QueryToken qt = parse_wordform_pattern(ts, trace);
        qt.name = std::move(name);
        TokenQuery tq;
        tq.tokens.push_back(std::move(qt));
        return tq;
    }

    QueryToken qt = parse_wordform_pattern(ts, trace);
    TokenQuery tq;
    tq.tokens.push_back(std::move(qt));
    return tq;
}

TokenQuery parse_reg_wordf_factor_chunk(TokStream& ts, std::ostringstream* trace) {
    TokenQuery iq = parse_reg_wordf_power_inner(ts, trace);
    Repeat r = parse_repeat_suffix(ts);
    if (iq.tokens.empty())
        throw std::runtime_error("Empty token pattern");
    if (iq.tokens.size() > 1) {
        if (r.min != 1 || r.max != 1)
            throw std::runtime_error(
                "Unsupported: repetition suffix on a parenthesised multi-token group "
                "(not expressible as a single pando QueryToken chain)");
        return iq;
    }
    iq.tokens[0].min_repeat = r.min;
    iq.tokens[0].max_repeat = r.max;
    return iq;
}

TokenQuery parse_reg_wordf_term(TokStream& ts, std::ostringstream* trace) {
    TokenQuery tq;
    if (!starts_reg_wordf_power(ts.peek()))
        throw std::runtime_error("Expected token pattern (e.g. `[lemma=\"x\"]`)");
    for (;;) {
        TokenQuery chunk = parse_reg_wordf_factor_chunk(ts, trace);
        if (!tq.tokens.empty()) {
            QueryRelation rel;
            rel.type = RelationType::SEQUENCE;
            tq.relations.push_back(rel);
        }
        for (std::size_t ti = 0; ti < chunk.tokens.size(); ++ti) {
            if (ti > 0) {
                QueryRelation rel;
                rel.type = RelationType::SEQUENCE;
                tq.relations.push_back(rel);
            }
            tq.tokens.push_back(std::move(chunk.tokens[ti]));
        }
        if (ts.eof() || !starts_reg_wordf_power(ts.peek()))
            break;
    }
    return tq;
}

TokenQuery parse_reg_wordf_expr(TokStream& ts, std::ostringstream* trace) {
    TokenQuery tq = parse_reg_wordf_term(ts, trace);
    if (!ts.eof() && ts.peek().kind == CwbTok::PIPE) {
        throw std::runtime_error(
            "Unsupported: alternation (|) between token-sequence patterns at RegWordfExpr level "
            "(split queries or use native CQL for disjunction)");
    }
    return tq;
}

void parse_search_pattern_tail(TokStream& ts) {
    if (ts.eof() || ts.peek().kind == CwbTok::END || ts.peek().kind == CwbTok::SEMI)
        return;
    if (ts.peek().kind == CwbTok::GCDEL)
        throw std::runtime_error(
            "Unsupported: global constraint (:: …) after pattern (alignment / multi-corpus)");
    if (ts.peek().kind == CwbTok::WITHIN_SYM)
        throw std::runtime_error(
            "Unsupported: within clause after pattern (not mapped to pando TokenQuery yet)");
    if (ts.peek().kind == CwbTok::CUT_SYM)
        throw std::runtime_error("Unsupported: cut clause after pattern");
    if (ts.peek().kind == CwbTok::SHOW_SYM)
        throw std::runtime_error("Unsupported: show match … ellipsis clause after pattern");
}

// One field in `group by` clauses: undotted attr, dotted QID, or FIELD + ID (CWB space → dot).
static std::string parse_cwb_by_field(TokStream& ts, const char* ctx_after_by) {
    if (ts.eof())
        throw std::runtime_error(std::string("Expected field name ") + ctx_after_by);
    switch (ts.peek().kind) {
    case CwbTok::QID: {
        std::string s = ts.peek().text;
        ts.bump();
        return s;
    }
    case CwbTok::ID: {
        std::string s = ts.peek().text;
        ts.bump();
        return s;
    }
    case CwbTok::FIELD: {
        std::string anchor = ts.peek().text;
        ts.bump();
        if (ts.eof() || ts.peek().kind != CwbTok::ID)
            throw std::runtime_error(
                "CWB `group by` expected attribute after anchor '" + anchor +
                "' (e.g. `group by match lemma` or `group by match.lemma`). "
                "Use `--cql native` for other field syntax.");
        std::string attr = ts.peek().text;
        ts.bump();
        return anchor + "." + attr;
    }
    default:
        throw std::runtime_error(std::string("Expected attribute or field path ") + ctx_after_by);
    }
}

static void parse_cwb_sort_cmd_tail(TokStream& ts, std::ostringstream* trace, const char* cmd_label) {
    if (!ts.eof() && ts.peek().kind == CwbTok::FLAG) {
        if (trace)
            *trace << "  (CWB " << cmd_label << ": ignoring % flags on field — not mapped yet)\n";
        ts.bump();
    }

    if (!ts.eof() && ts.peek().kind == CwbTok::ON_SYM)
        throw std::runtime_error(
            "Unsupported in CWB dialect: sort/group boundaries (`on match` / anchor ranges). "
            "Use `--cql native` for richer sort/group options.");

    if (!ts.eof() &&
        (ts.peek().kind == CwbTok::ASC_SYM || ts.peek().kind == CwbTok::DESC_SYM)) {
        if (trace)
            *trace << "  (CWB " << cmd_label << ": ignoring asc/desc — not applied to pando output)\n";
        ts.bump();
    }
    if (!ts.eof() && ts.peek().kind == CwbTok::REVERSE_SYM) {
        if (trace)
            *trace << "  (CWB " << cmd_label << ": ignoring reverse — not applied to pando output)\n";
        ts.bump();
    }

    if (!ts.eof() && ts.peek().kind == CwbTok::CUT_SYM)
        throw std::runtime_error(std::string("Unsupported: `cut` in CWB ") + cmd_label + " command");

    if (!ts.eof() && ts.peek().kind == CwbTok::GT)
        throw std::runtime_error(std::string("Unsupported: shell redirection (`>`) in CWB ") + cmd_label);
}

// IMS parser.y: SortCmd → COUNT_SYM OptionalCID SortClause CutStatement OptionalRedir
// SortClause → BY_SYM ID OptionalFlag SortBoundaries SortDirection OptReverse
Statement parse_cwb_count_cmd(TokStream& ts, std::ostringstream* trace) {
    expect_tok(ts, CwbTok::COUNT_SYM, "count");

    // OptionalCID: a single identifier immediately before 'by' (corpus / NQR name in CQP).
    if (!ts.eof() && ts.peek().kind == CwbTok::ID &&
        peek2_kind(ts) == CwbTok::BY_SYM) {
        if (trace)
            *trace << "  (CWB count: corpus id '" << ts.peek().text
                   << "' skipped — pando uses the open corpus)\n";
        ts.bump();
    }

    expect_tok(ts, CwbTok::BY_SYM, "'by' after count");

    if (ts.eof())
        throw std::runtime_error("Expected attribute name after 'count by'");
    if (ts.peek().kind == CwbTok::FIELD)
        throw std::runtime_error(
            "CWB `count by` expects a positional attribute name (parser.y: SortClause uses ID), "
            "not anchor keywords like 'match' or 'target'. Example: `count by form` or `count by lemma`. "
            "Use `--cql native` for `count by match.<attr>`.");
    if (ts.peek().kind == CwbTok::QID)
        throw std::runtime_error(
            "CWB `count by` takes a single undotted attribute name (e.g. `lemma`, `form`). "
            "Dotted paths / token labels are not in IMS SortClause. Use `--cql native` for `a.attr` fields.");
    if (ts.peek().kind != CwbTok::ID)
        throw std::runtime_error("Expected attribute identifier after 'count by'");

    Statement stmt;
    stmt.has_command = true;
    stmt.command.type = CommandType::COUNT;
    stmt.command.fields.push_back(ts.peek().text);
    ts.bump();

    parse_cwb_sort_cmd_tail(ts, trace, "count");

    if (trace)
        *trace << "  statement: CWB count by " << stmt.command.fields[0] << "\n";

    return stmt;
}

// IMS: group mirrors SortCmd-style `by` lists; map to native GroupCommand with dotted fields.
Statement parse_cwb_group_cmd(TokStream& ts, std::ostringstream* trace) {
    expect_tok(ts, CwbTok::GROUP_SYM, "group");

    if (!ts.eof() && ts.peek().kind == CwbTok::ID && peek2_kind(ts) == CwbTok::BY_SYM) {
        if (trace)
            *trace << "  (CWB group: corpus id '" << ts.peek().text
                   << "' skipped — pando uses the open corpus)\n";
        ts.bump();
    }

    expect_tok(ts, CwbTok::BY_SYM, "'by' after group");

    Statement stmt;
    stmt.has_command = true;
    stmt.command.type = CommandType::GROUP;
    stmt.command.fields.push_back(parse_cwb_by_field(ts, "after 'group by'"));

    while (!ts.eof() && ts.peek().kind == CwbTok::COMMA) {
        ts.bump();
        stmt.command.fields.push_back(parse_cwb_by_field(ts, "after ',' in group by"));
    }

    parse_cwb_sort_cmd_tail(ts, trace, "group");

    if (trace) {
        *trace << "  statement: CWB group by";
        for (const std::string& f : stmt.command.fields)
            *trace << " " << f;
        *trace << "\n";
    }

    return stmt;
}

Statement parse_statement_tokens(std::vector<CwbToken>& toks, std::ostringstream* trace) {
    TokStream ts;
    ts.v = &toks;
    ts.i = 0;
    if (ts.eof())
        throw std::runtime_error("Empty CWB statement");

    if (ts.peek().kind == CwbTok::COUNT_SYM) {
        Statement st = parse_cwb_count_cmd(ts, trace);
        if (!ts.eof() && ts.peek().kind != CwbTok::END && ts.peek().kind != CwbTok::SEMI)
            throw std::runtime_error("Unexpected tokens after CWB count command");
        return st;
    }

    if (ts.peek().kind == CwbTok::GROUP_SYM) {
        Statement st = parse_cwb_group_cmd(ts, trace);
        if (!ts.eof() && ts.peek().kind != CwbTok::END && ts.peek().kind != CwbTok::SEMI)
            throw std::runtime_error("Unexpected tokens after CWB group command");
        return st;
    }

    if (is_cqp_shell_keyword(ts.peek().kind))
        shell_keyword_error(ts.peek().text, ts.peek().offset);

    Statement stmt;
    if (ts.peek().kind == CwbTok::ID && ts.i + 1 < toks.size() &&
        toks[ts.i + 1].kind == CwbTok::EQ) {
        stmt.name = ts.peek().text;
        ts.bump();
        ts.bump();
        if (trace)
            *trace << "  statement: named query \"" << stmt.name << "\"\n";
        stmt.has_query = true;
        stmt.query = parse_reg_wordf_expr(ts, trace);
        parse_search_pattern_tail(ts);
        if (!ts.eof() && ts.peek().kind != CwbTok::END && ts.peek().kind != CwbTok::SEMI)
            throw std::runtime_error("Unexpected tokens after CWB query pattern");
        return stmt;
    }

    if (trace)
        *trace << "  statement: anonymous token query\n";
    stmt.has_query = true;
    stmt.query = parse_reg_wordf_expr(ts, trace);
    parse_search_pattern_tail(ts);
    if (!ts.eof() && ts.peek().kind != CwbTok::END && ts.peek().kind != CwbTok::SEMI)
        throw std::runtime_error("Unexpected tokens after CWB query pattern");
    return stmt;
}

} // namespace

Program translate_cwb_program_parsed(const std::string& input, int debug_level,
                                   std::string* trace_out) {
    std::ostringstream trace;
    std::ostringstream* tp = (debug_level > 0) ? &trace : nullptr;

    if (tp)
        *tp << "[cwb] dialect front-end (parser.l / parser.y–aligned lexer & recursive descent)\n";

    Program prog;
    for (const std::string& seg : split_cwb_segments(input)) {
        std::string_view sv(seg);
        while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.front())))
            sv.remove_prefix(1);
        while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.back())))
            sv.remove_suffix(1);
        if (sv.empty())
            continue;

        std::vector<CwbToken> toks = tokenize_all(sv);
        Statement st = parse_statement_tokens(toks, tp);
        prog.push_back(std::move(st));
        if (tp)
            *tp << "  ---\n";
    }

    if (prog.empty())
        throw std::runtime_error("Empty CWB query");

    if (trace_out && debug_level > 0) {
        *trace_out += trace.str();
        *trace_out += "[cwb] statements: " + std::to_string(prog.size()) + "\n";
    }

    return prog;
}

} // namespace manatree
