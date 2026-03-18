#include "query/parser.h"
#include <stdexcept>
#include <algorithm>

namespace manatree {

Parser::Parser(const std::string& input) : lexer_(input) {}

Program Parser::parse() {
    Program prog;
    prog.push_back(parse_statement());
    while (lexer_.peek().type == TokType::SEMI) {
        lexer_.consume();
        if (lexer_.peek().type == TokType::END) break;
        prog.push_back(parse_statement());
    }
    return prog;
}

bool Parser::is_command_keyword(const std::string& text) const {
    static const std::vector<std::string> cmds = {
        "count", "group", "sort", "freq", "coll", "dcoll",
        "cat", "size", "raw", "show", "tabulate"
    };
    std::string lower = text;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    return std::find(cmds.begin(), cmds.end(), lower) != cmds.end();
}

Statement Parser::parse_statement() {
    Statement stmt;
    Token t = lexer_.peek();

    // Check for command keywords
    if (t.type == TokType::IDENT && is_command_keyword(t.text)) {
        stmt.has_command = true;
        stmt.command = parse_command();
        return stmt;
    }

    // #16: Source : query | Target : query [:: filters]
    if (t.type == TokType::IDENT) {
        std::string lower = t.text;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        if (lower == "source") {
            lexer_.consume();
            lexer_.expect(TokType::COLON);
            stmt.has_query = true;
            stmt.is_parallel = true;
            stmt.query = parse_token_query();
            if (lexer_.peek().type != TokType::PIPE)
                throw std::runtime_error("Expected '|' after Source query");
            lexer_.consume();
            Token target_kw = lexer_.next();
            if (target_kw.type != TokType::IDENT) throw std::runtime_error("Expected 'Target'");
            std::string target_lower = target_kw.text;
            std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);
            if (target_lower != "target") throw std::runtime_error("Expected 'Target'");
            lexer_.expect(TokType::COLON);
            stmt.target_query = parse_token_query();
            if (lexer_.peek().type == TokType::DCOLON) {
                lexer_.consume();
                parse_global_filters(stmt.query);
            }
            return stmt;
        }
    }

    // Check for named query: IDENT "=" query
    // We need to look ahead: IDENT followed by "=" (not "==" which doesn't exist,
    // but "=" when not inside brackets is assignment)
    if (t.type == TokType::IDENT) {
        // Save state — peek ahead
        Token t1 = lexer_.next();   // consume IDENT
        Token t2 = lexer_.peek();
        if (t2.type == TokType::EQ) {
            // Check it's not an attribute condition by looking if next-next is
            // "[" or "@" or '"' or IDENT:  — these start a token expression.
            // Actually, named query is: IDENT = <token_query>
            // A token query starts with [, @, ", or IDENT (label)
            lexer_.consume(); // consume =
            Token t3 = lexer_.peek();
            if (t3.type == TokType::LBRACKET || t3.type == TokType::AT ||
                t3.type == TokType::STRING || t3.type == TokType::IDENT) {
                stmt.name = t1.text;
                stmt.has_query = true;
                stmt.query = parse_token_query();
                return stmt;
            }
            // Otherwise, put tokens back conceptually — this is tricky.
            // For simplicity, treat as error or try reparse.
            throw std::runtime_error("Unexpected token after '=' at position " +
                                     std::to_string(t3.pos));
        }
        // Not a named query — it's a label:token_expr or just a token query
        // We consumed t1, but the lexer doesn't support pushback of consumed tokens.
        // Workaround: create a synthetic token and inject it.
        // Actually, let's restructure: re-construct state.
        // The IDENT we consumed might be a label for a token, or part of a command.
        // Since we already checked commands above, this must be a label.
        // We'll handle it in parse_token_query.

        // We need to "unconsume" t1. Since our lexer is simple, we'll
        // reconstruct by treating t1 as the first part of the token query.
        // Let's just handle the label case directly here.
        stmt.has_query = true;

        // t1 is an IDENT, t2 could be ":" (label) or "[" (start of expression after whitespace)
        if (t2.type == TokType::COLON) {
            // Label: IDENT ":" token_expr ...
            lexer_.consume(); // consume ":"
            QueryToken qt = parse_token_expr();
            qt.name = t1.text;
            stmt.query.tokens.push_back(std::move(qt));
        } else {
            // The IDENT might be a bare string? Not in CQL.
            // More likely: it was just whitespace separation and we need a [ next.
            throw std::runtime_error("Unexpected identifier '" + t1.text +
                                     "' at position " + std::to_string(t1.pos));
        }

        // Continue parsing the rest of the chain
        QueryRelation rel;
        while (try_parse_relation(rel)) {
            stmt.query.relations.push_back(rel);
            stmt.query.tokens.push_back(parse_token_expr());
        }

        // Within clause
        if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "within") {
            lexer_.consume();
            stmt.query.within = lexer_.expect(TokType::IDENT).text;
        }

        // #12: Global filters
        if (lexer_.peek().type == TokType::DCOLON) {
            lexer_.consume();
            parse_global_filters(stmt.query);
        }

        return stmt;
    }

    // Regular token query
    stmt.has_query = true;
    stmt.query = parse_token_query();
    return stmt;
}

GroupCommand Parser::parse_command() {
    GroupCommand cmd;
    Token t = lexer_.next();
    std::string kw = t.text;
    std::transform(kw.begin(), kw.end(), kw.begin(), ::tolower);

    if (kw == "count") cmd.type = CommandType::COUNT;
    else if (kw == "group") cmd.type = CommandType::GROUP;
    else if (kw == "sort") cmd.type = CommandType::SORT;
    else if (kw == "freq") cmd.type = CommandType::FREQ;
    else if (kw == "coll") cmd.type = CommandType::COLL;
    else if (kw == "dcoll") cmd.type = CommandType::DCOLL;
    else if (kw == "cat") cmd.type = CommandType::CAT;
    else if (kw == "size") cmd.type = CommandType::SIZE;
    else if (kw == "raw") cmd.type = CommandType::RAW;
    else if (kw == "tabulate") cmd.type = CommandType::TABULATE;
    else if (kw == "show") {
        Token what = lexer_.next();
        if (what.text == "attributes" || what.text == "attrs") cmd.type = CommandType::SHOW_ATTRS;
        else if (what.text == "regions") cmd.type = CommandType::SHOW_REGIONS;
        else if (what.text == "named") cmd.type = CommandType::SHOW_NAMED;
        else throw std::runtime_error("Unknown show target: " + what.text);
        return cmd;
    }
    else throw std::runtime_error("Unknown command: " + kw);

    // Optional query name
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text != "by") {
        cmd.query_name = lexer_.next().text;
    }

    // "by" field_list
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "by") {
        lexer_.consume();
        // Parse comma-separated field list: field[.subfield], ...
        std::string field;
        Token ft = lexer_.next();
        field = ft.text;
        if (lexer_.peek().type == TokType::DOT) {
            lexer_.consume();
            field += "." + lexer_.expect(TokType::IDENT).text;
        }
        cmd.fields.push_back(field);

        while (lexer_.peek().type == TokType::COMMA) {
            lexer_.consume();
            ft = lexer_.next();
            field = ft.text;
            if (lexer_.peek().type == TokType::DOT) {
                lexer_.consume();
                field += "." + lexer_.expect(TokType::IDENT).text;
            }
            cmd.fields.push_back(field);
        }
    }

    return cmd;
}

TokenQuery Parser::parse_token_query() {
    TokenQuery tq;
    tq.tokens.push_back(parse_token_expr());

    QueryRelation rel;
    while (try_parse_relation(rel)) {
        tq.relations.push_back(rel);
        tq.tokens.push_back(parse_token_expr());
    }

    // Within clause
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "within") {
        lexer_.consume();
        tq.within = lexer_.expect(TokType::IDENT).text;
    }

    // #12: Global filters :: [match.region_attr op value | a.attr = b.attr] [& ...]*
    if (lexer_.peek().type == TokType::DCOLON) {
        lexer_.consume();
        parse_global_filters(tq);
    }

    return tq;
}

QueryToken Parser::parse_token_expr() {
    QueryToken qt;

    // Optional label: IDENT ":"
    Token t = lexer_.peek();
    if (t.type == TokType::IDENT) {
        // Look ahead for ":"
        Token t1 = lexer_.next();
        if (lexer_.peek().type == TokType::COLON) {
            lexer_.consume();
            qt.name = t1.text;
        } else {
            // Not a label — error or bare ident
            throw std::runtime_error("Expected ':' or '[' after identifier '" +
                                     t1.text + "'");
        }
    }

    // Optional target marker @
    if (lexer_.peek().type == TokType::AT) {
        lexer_.consume();
        qt.is_target = true;
    }

    // Token expression: "[" conditions "]" or "string"
    t = lexer_.peek();
    if (t.type == TokType::LBRACKET) {
        lexer_.consume();
        if (lexer_.peek().type != TokType::RBRACKET) {
            qt.conditions = parse_conditions();
        }
        lexer_.expect(TokType::RBRACKET);
    } else if (t.type == TokType::STRING) {
        lexer_.consume();
        AttrCondition ac;
        ac.attr = "form";
        ac.op = CompOp::EQ;
        ac.value = t.text;
        qt.conditions = ConditionNode::make_leaf(std::move(ac));
    } else {
        throw std::runtime_error("Expected '[' or string at position " +
                                 std::to_string(t.pos));
    }

    // Optional repetition quantifier: {n,m}, {n,}, {n}, +, ?
    parse_repetition(qt);

    return qt;
}

void Parser::parse_repetition(QueryToken& qt) {
    Token t = lexer_.peek();

    if (t.type == TokType::PLUS) {
        lexer_.consume();
        qt.min_repeat = 1;
        qt.max_repeat = REPEAT_UNBOUNDED;
        return;
    }
    if (t.type == TokType::STAR) {
        lexer_.consume();
        qt.min_repeat = 0;
        qt.max_repeat = REPEAT_UNBOUNDED;
        return;
    }
    if (t.type == TokType::QUESTION) {
        lexer_.consume();
        qt.min_repeat = 0;
        qt.max_repeat = 1;
        return;
    }
    if (t.type != TokType::LBRACE) return;

    lexer_.consume();  // consume {

    Token n1 = lexer_.expect(TokType::NUMBER);
    int min_val = std::stoi(n1.text);
    if (min_val < 0)
        throw std::runtime_error("Repetition minimum must be >= 0 at position " +
                                 std::to_string(n1.pos));

    if (lexer_.peek().type == TokType::RBRACE) {
        // {n} — exact count
        lexer_.consume();
        qt.min_repeat = min_val;
        qt.max_repeat = min_val;
        return;
    }

    lexer_.expect(TokType::COMMA);

    if (lexer_.peek().type == TokType::RBRACE) {
        // {n,} — n or more
        lexer_.consume();
        qt.min_repeat = min_val;
        qt.max_repeat = REPEAT_UNBOUNDED;
        return;
    }

    Token n2 = lexer_.expect(TokType::NUMBER);
    int max_val = std::stoi(n2.text);
    if (max_val < min_val)
        throw std::runtime_error("Repetition maximum (" + std::to_string(max_val) +
                                 ") must be >= minimum (" + std::to_string(min_val) +
                                 ") at position " + std::to_string(n2.pos));
    lexer_.expect(TokType::RBRACE);

    qt.min_repeat = min_val;
    qt.max_repeat = max_val;
}

ConditionPtr Parser::parse_conditions() {
    return parse_or_condition();
}

ConditionPtr Parser::parse_or_condition() {
    auto left = parse_and_condition();
    while (lexer_.peek().type == TokType::PIPE) {
        lexer_.consume();
        auto right = parse_and_condition();
        left = ConditionNode::make_branch(BoolOp::OR, left, right);
    }
    return left;
}

ConditionPtr Parser::parse_and_condition() {
    auto left = parse_primary_condition();
    while (lexer_.peek().type == TokType::AMP) {
        lexer_.consume();
        auto right = parse_primary_condition();
        left = ConditionNode::make_branch(BoolOp::AND, left, right);
    }
    return left;
}

ConditionPtr Parser::parse_primary_condition() {
    // Parenthesized group
    if (lexer_.peek().type == TokType::LPAREN) {
        lexer_.consume();
        auto cond = parse_conditions();
        lexer_.expect(TokType::RPAREN);
        return cond;
    }

    // attr op value
    AttrCondition ac;

    // Attribute name, possibly with dot (feats.Number)
    Token t = lexer_.expect(TokType::IDENT);
    ac.attr = t.text;
    if (lexer_.peek().type == TokType::DOT) {
        lexer_.consume();
        ac.attr += "." + lexer_.expect(TokType::IDENT).text;
    }

    // Operator
    Token op = lexer_.next();
    switch (op.type) {
        case TokType::EQ: {
            // Check for regex: = /pattern/
            Token val = lexer_.peek();
            if (val.type == TokType::REGEX) {
                lexer_.consume();
                ac.op = CompOp::REGEX;
                ac.value = val.text;
            } else {
                ac.op = CompOp::EQ;
                Token v = lexer_.next();
                ac.value = v.text;
            }
            break;
        }
        case TokType::NEQ:  ac.op = CompOp::NEQ;  ac.value = lexer_.next().text; break;
        case TokType::LT:   ac.op = CompOp::LT;   ac.value = lexer_.next().text; break;
        case TokType::GT:   ac.op = CompOp::GT;    ac.value = lexer_.next().text; break;
        case TokType::LTE:  ac.op = CompOp::LTE;   ac.value = lexer_.next().text; break;
        case TokType::GTE:  ac.op = CompOp::GTE;   ac.value = lexer_.next().text; break;
        default:
            throw std::runtime_error("Expected comparison operator at position " +
                                     std::to_string(op.pos));
    }

    return ConditionNode::make_leaf(std::move(ac));
}

bool Parser::try_parse_relation(QueryRelation& rel) {
    Token t = lexer_.peek();
    switch (t.type) {
        case TokType::GT:      rel.type = RelationType::GOVERNS;       break;
        case TokType::LT:      rel.type = RelationType::GOVERNED_BY;   break;
        case TokType::GTGT:    rel.type = RelationType::TRANS_GOVERNS; break;
        case TokType::LTLT:    rel.type = RelationType::TRANS_GOV_BY;  break;
        case TokType::BANG_GT: rel.type = RelationType::NOT_GOVERNS;   break;
        case TokType::BANG_LT: rel.type = RelationType::NOT_GOV_BY;    break;

        case TokType::LBRACKET:
        case TokType::AT:
        case TokType::STRING:
            // Implicit SEQUENCE relation (adjacency)
            rel.type = RelationType::SEQUENCE;
            return true;   // don't consume — the token expr will consume it

        case TokType::IDENT:
            // Could be a label for next token, or "within" keyword
            if (t.text == "within") return false;
            // Check if IDENT is followed by ":" (label) — treat as sequence
            rel.type = RelationType::SEQUENCE;
            return true;

        default:
            return false;
    }

    // Consume the explicit relation operator
    lexer_.consume();
    return true;
}

void Parser::parse_global_filters(TokenQuery& tq) {
    auto parse_one = [this, &tq]() {
        Token t = lexer_.next();
        if (t.type != TokType::IDENT)
            throw std::runtime_error("Expected identifier in global filter");
        if (t.text == "match") {
            lexer_.expect(TokType::DOT);
            GlobalRegionFilter gf;
            gf.region_attr = lexer_.expect(TokType::IDENT).text;
            if (lexer_.peek().type == TokType::DOT) {
                lexer_.consume();
                gf.region_attr += "." + lexer_.expect(TokType::IDENT).text;
            }
            Token op = lexer_.next();
            switch (op.type) {
                case TokType::EQ:   gf.op = CompOp::EQ;  break;
                case TokType::NEQ:  gf.op = CompOp::NEQ; break;
                case TokType::LT:   gf.op = CompOp::LT;  break;
                case TokType::GT:   gf.op = CompOp::GT;  break;
                case TokType::LTE:  gf.op = CompOp::LTE; break;
                case TokType::GTE:  gf.op = CompOp::GTE; break;
                default: throw std::runtime_error("Expected comparison in match filter");
            }
            if (lexer_.peek().type == TokType::STRING)
                gf.value = lexer_.next().text;
            else if (lexer_.peek().type == TokType::NUMBER)
                gf.value = lexer_.next().text;
            else
                gf.value = lexer_.expect(TokType::IDENT).text;
            tq.global_region_filters.push_back(std::move(gf));
        } else {
            GlobalAlignmentFilter af;
            af.name1 = t.text;
            lexer_.expect(TokType::DOT);
            af.attr1 = lexer_.expect(TokType::IDENT).text;
            lexer_.expect(TokType::EQ);
            af.name2 = lexer_.expect(TokType::IDENT).text;
            lexer_.expect(TokType::DOT);
            af.attr2 = lexer_.expect(TokType::IDENT).text;
            tq.global_alignment_filters.push_back(std::move(af));
        }
    };

    parse_one();
    while (lexer_.peek().type == TokType::AMP) {
        lexer_.consume();
        parse_one();
    }
}

} // namespace manatree
