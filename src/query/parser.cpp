#include "query/parser.h"
#include "query/quoted_string_pattern.h"
#include <cctype>
#include <stdexcept>
#include <algorithm>

namespace manatree {

Parser::Parser(const std::string& input, ParserOptions opts)
    : lexer_(input), opts_(opts) {}

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
        "cat", "size", "raw", "show", "tabulate", "keyness", "set", "drop"
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

    // Check for named query: IDENT "=" query
    // We need to look ahead: IDENT followed by "=" (not "==" which doesn't exist,
    // but "=" when not inside brackets is assignment)
    if (t.type == TokType::IDENT) {
        // Save state — peek ahead
        Token t1 = lexer_.next();   // consume IDENT
        Token t2 = lexer_.peek();
        if (t2.type == TokType::EQ) {
            // Check it's not an attribute condition by looking if next-next is
            // "[" or '"' or IDENT:  — these start a token expression.
            // Actually, named query is: IDENT = <token_query>
            // A token query starts with [, ", or IDENT (label)
            lexer_.consume(); // consume =
            Token t3 = lexer_.peek();
            if (t3.type == TokType::LBRACKET || t3.type == TokType::STRING
                || t3.type == TokType::IDENT || t3.type == TokType::REGION_START) {
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

        // Structural clauses: within, not within, containing, not containing
        while (lexer_.peek().type == TokType::IDENT) {
            const std::string& kw = lexer_.peek().text;
            if (kw == "within") {
                lexer_.consume();
                parse_within_clause(stmt.query);
            } else if (kw == "not") {
                Token not_tok = lexer_.next();
                Token next = lexer_.peek();
                if (next.type == TokType::IDENT && next.text == "within") {
                    lexer_.consume();
                    stmt.query.not_within = true;
                    parse_within_clause(stmt.query);
                } else if (next.type == TokType::IDENT && next.text == "containing") {
                    lexer_.consume();
                    parse_containing_clause(stmt.query, true);
                } else {
                    throw std::runtime_error("Expected 'within' or 'containing' after 'not'");
                }
            } else if (kw == "containing") {
                lexer_.consume();
                parse_containing_clause(stmt.query, false);
            } else {
                break;
            }
        }

        // #12: Global filters
        if (lexer_.peek().type == TokType::DCOLON) {
            lexer_.consume();
            parse_global_filters(stmt.query);
        }

        // Check for parallel query: query1 with query2
        if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "with") {
            lexer_.consume();
            stmt.is_parallel = true;
            stmt.target_query = parse_token_query();
            if (lexer_.peek().type == TokType::DCOLON) {
                lexer_.consume();
                parse_global_filters(stmt.query);
            }
        }

        return stmt;
    }

    // Regular token query
    stmt.has_query = true;
    stmt.query = parse_token_query();

    // Check for parallel query: query1 with query2 [:: filters]
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "with") {
        lexer_.consume();
        stmt.is_parallel = true;
        stmt.target_query = parse_token_query();
        // Global filters (:: a.attr = b.attr) after `with` apply to source query
        if (lexer_.peek().type == TokType::DCOLON) {
            lexer_.consume();
            parse_global_filters(stmt.query);
        }
    }

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
    else if (kw == "keyness") cmd.type = CommandType::KEYNESS;
    else if (kw == "dcoll") {
        cmd.type = CommandType::DCOLL;
        // Syntax: dcoll [QueryName] [anchor.] rel1[, rel2, ...] by field[, field, ...]
        // Relations: "head" (go up), "descendants" (subtree), any other = deprel on children
        // Empty relations = all children (default)
        //
        // Disambiguation: if first IDENT is followed by DOT and then "by"/comma/relation,
        // it's an anchor (e.g. "a.head" → anchor=a, rel=head).
        // If first IDENT is followed by another IDENT that is "by", it's a query name.
        // Otherwise it's a relation name.

        // Step 1: try to parse optional query name and/or anchor
        if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text != "by") {
            std::string first = lexer_.next().text;
            if (lexer_.peek().type == TokType::DOT) {
                // Could be anchor.relation (a.head) or query_name followed by something
                // Peek past DOT to see if it's a relation or "by"
                lexer_.consume(); // consume DOT
                if (lexer_.peek().type == TokType::IDENT) {
                    // a.head or a.amod — first is the anchor, next is a relation
                    cmd.dcoll_anchor = first;
                    cmd.relations.push_back(lexer_.next().text);
                } else {
                    // Malformed — treat first as query name, put DOT back? Can't.
                    // Just treat as query name and hope for the best
                    cmd.query_name = first;
                }
            } else if (lexer_.peek().type == TokType::IDENT &&
                       lexer_.peek().text == "by") {
                // "dcoll QueryName by ..." — first is a query name
                cmd.query_name = first;
            } else if (lexer_.peek().type == TokType::COMMA ||
                       lexer_.peek().type == TokType::IDENT) {
                // "dcoll nsubj, obj by ..." — first is a relation
                cmd.relations.push_back(first);
            } else {
                // "dcoll head;" — first is a relation, nothing follows
                cmd.relations.push_back(first);
            }
        }

        // Step 2: parse remaining comma-separated relations before "by"
        while (lexer_.peek().type == TokType::COMMA ||
               (lexer_.peek().type == TokType::IDENT && lexer_.peek().text != "by")) {
            if (lexer_.peek().type == TokType::COMMA) lexer_.consume();
            if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text != "by") {
                cmd.relations.push_back(lexer_.next().text);
            } else break;
        }

        // Step 3: parse optional "by" field_list
        if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "by") {
            lexer_.consume();
            std::string field = lexer_.next().text;
            if (lexer_.peek().type == TokType::DOT) {
                lexer_.consume();
                field += "." + lexer_.expect(TokType::IDENT).text;
            }
            cmd.fields.push_back(field);
            while (lexer_.peek().type == TokType::COMMA) {
                lexer_.consume();
                field = lexer_.next().text;
                if (lexer_.peek().type == TokType::DOT) {
                    lexer_.consume();
                    field += "." + lexer_.expect(TokType::IDENT).text;
                }
                cmd.fields.push_back(field);
            }
        }
        return cmd;
    }
    else if (kw == "cat") cmd.type = CommandType::CAT;
    else if (kw == "size") cmd.type = CommandType::SIZE;
    else if (kw == "raw") cmd.type = CommandType::RAW;
    else if (kw == "tabulate") {
        cmd.type = CommandType::TABULATE;
        // CWB-style: tabulate [QueryName] offset limit field1[, field2, ...]
        // offset/limit optional; default limit 1000 when omitted (see ast.h).
        cmd.tabulate_offset = 0;
        cmd.tabulate_limit = 1000;

        auto parse_field_tail = [&]() {
            while (lexer_.peek().type == TokType::IDENT || lexer_.peek().type == TokType::COMMA) {
                if (lexer_.peek().type == TokType::COMMA) lexer_.consume();
                if (lexer_.peek().type != TokType::IDENT) break;
                std::string first = lexer_.next().text;
                std::string low = first;
                std::transform(low.begin(), low.end(), low.begin(), ::tolower);
                if (low == "tcnt" && lexer_.peek().type == TokType::LPAREN) {
                    lexer_.consume();
                    std::string inner = lexer_.expect(TokType::IDENT).text;
                    lexer_.expect(TokType::RPAREN);
                    cmd.fields.push_back("tcnt(" + inner + ")");
                    continue;
                }
                std::string field = std::move(first);
                if (lexer_.peek().type == TokType::DOT) {
                    lexer_.consume();
                    field += "." + lexer_.expect(TokType::IDENT).text;
                }
                cmd.fields.push_back(field);
            }
        };

        // tabulate 0 100 a.form  (implicit Last)
        if (lexer_.peek().type == TokType::NUMBER) {
            cmd.tabulate_offset = std::stoull(lexer_.next().text);
            cmd.tabulate_limit = std::stoull(lexer_.expect(TokType::NUMBER).text);
            parse_field_tail();
            return cmd;
        }

        if (lexer_.peek().type == TokType::IDENT) {
            std::string first = lexer_.next().text;
            {
                std::string low = first;
                std::transform(low.begin(), low.end(), low.begin(), ::tolower);
                if (low == "tcnt" && lexer_.peek().type == TokType::LPAREN) {
                    lexer_.consume();
                    std::string inner = lexer_.expect(TokType::IDENT).text;
                    lexer_.expect(TokType::RPAREN);
                    cmd.fields.push_back("tcnt(" + inner + ")");
                    parse_field_tail();
                    return cmd;
                }
            }
            if (lexer_.peek().type == TokType::DOT) {
                lexer_.consume();
                cmd.fields.push_back(first + "." + lexer_.expect(TokType::IDENT).text);
                parse_field_tail();
                return cmd;
            }
            if (lexer_.peek().type == TokType::NUMBER) {
                cmd.query_name = first;
                cmd.tabulate_offset = std::stoull(lexer_.next().text);
                cmd.tabulate_limit = std::stoull(lexer_.expect(TokType::NUMBER).text);
                parse_field_tail();
                return cmd;
            }
            if (lexer_.peek().type == TokType::IDENT) {
                std::string second = lexer_.next().text;
                if (lexer_.peek().type == TokType::DOT) {
                    cmd.query_name = first;
                    lexer_.consume();
                    cmd.fields.push_back(second + "." + lexer_.expect(TokType::IDENT).text);
                    parse_field_tail();
                    return cmd;
                }
                {
                    std::string low2 = second;
                    std::transform(low2.begin(), low2.end(), low2.begin(), ::tolower);
                    if (low2 == "tcnt" && lexer_.peek().type == TokType::LPAREN) {
                        cmd.query_name = first;
                        lexer_.consume();
                        std::string inner = lexer_.expect(TokType::IDENT).text;
                        lexer_.expect(TokType::RPAREN);
                        cmd.fields.push_back("tcnt(" + inner + ")");
                        parse_field_tail();
                        return cmd;
                    }
                }
                cmd.fields.push_back(first);
                cmd.fields.push_back(second);
                parse_field_tail();
                return cmd;
            }
            cmd.fields.push_back(first);
            parse_field_tail();
            return cmd;
        }

        parse_field_tail();
        return cmd;
    }
    else if (kw == "set") {
        cmd.type = CommandType::SET;
        // Syntax: set <name> <value>
        // value can be a number, ident, or comma-separated list
        Token name_tok = lexer_.next();
        cmd.set_name = name_tok.text;
        // Collect the rest as value: concatenate tokens separated by commas/spaces
        std::string val;
        while (lexer_.peek().type != TokType::END && lexer_.peek().type != TokType::SEMI) {
            if (lexer_.peek().type == TokType::COMMA) {
                lexer_.consume();
                if (!val.empty()) val += ",";
                continue;
            }
            Token vt = lexer_.next();
            if (!val.empty() && val.back() != ',') val += " ";
            val += vt.text;
        }
        cmd.set_value = val;
        return cmd;
    }
    else if (kw == "drop") {
        cmd.type = CommandType::DROP;
        Token what = lexer_.next();
        cmd.query_name = what.text;  // "all" or a named query name
        return cmd;
    }
    else if (kw == "show") {
        Token what = lexer_.next();
        if (what.text == "attributes" || what.text == "attrs") cmd.type = CommandType::SHOW_ATTRS;
        else if (what.text == "settings") {
            cmd.type = CommandType::SHOW_SETTINGS;
        }
        else if (what.text == "regions") {
            cmd.type = CommandType::SHOW_REGIONS;
            // Optional type name: "show regions text" → list all regions of type "text"
            if (lexer_.peek().type == TokType::IDENT) {
                cmd.query_name = lexer_.next().text;
            }
        }
        else if (what.text == "named") cmd.type = CommandType::SHOW_NAMED;
        else if (what.text == "info") cmd.type = CommandType::SHOW_INFO;
        else if (what.text == "values") {
            cmd.type = CommandType::SHOW_VALUES;
            // Required attribute name: "show values upos" or "show values text_genre"
            Token attr_tok = lexer_.next();
            cmd.query_name = attr_tok.text;
        }
        else throw std::runtime_error("Unknown show target: " + what.text);
        return cmd;
    }
    else throw std::runtime_error("Unknown command: " + kw);

    // Optional query name
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text != "by"
        && lexer_.peek().text != "vs") {
        cmd.query_name = lexer_.next().text;
    }

    // keyness: optional "vs RefName"
    if (cmd.type == CommandType::KEYNESS &&
        lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "vs") {
        lexer_.consume();  // eat "vs"
        cmd.ref_query_name = lexer_.expect(TokType::IDENT).text;
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

    // Structural clauses: within, not within, containing, not containing
    // These can be chained: "within s containing np not containing subtree [upos='VERB']"
    while (lexer_.peek().type == TokType::IDENT) {
        const std::string& kw = lexer_.peek().text;
        if (kw == "within") {
            lexer_.consume();
            parse_within_clause(tq);
        } else if (kw == "not") {
            // Look ahead: "not within" or "not containing"
            Token not_tok = lexer_.next();
            Token next = lexer_.peek();
            if (next.type == TokType::IDENT && next.text == "within") {
                lexer_.consume();
                tq.not_within = true;
                parse_within_clause(tq);
            } else if (next.type == TokType::IDENT && next.text == "containing") {
                lexer_.consume();
                parse_containing_clause(tq, /*negated=*/true);
            } else {
                throw std::runtime_error("Expected 'within' or 'containing' after 'not' at position " +
                                         std::to_string(not_tok.pos));
            }
        } else if (kw == "containing") {
            lexer_.consume();
            parse_containing_clause(tq, /*negated=*/false);
        } else {
            break;
        }
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

    // Region boundary anchors: <s>, </s>, <text genre="book"> etc.
    if (lexer_.peek().type == TokType::REGION_START) {
        Token rs = lexer_.next();
        qt.anchor = RegionAnchorType::REGION_START;
        qt.conditions = nullptr;

        // Parse token text: "s" or "text genre=\"book\" id=\"doc42\""
        // Extract region name (first word) and optional key="value" pairs
        const std::string& content = rs.text;
        size_t i = 0;
        // Region name
        while (i < content.size() && !std::isspace(static_cast<unsigned char>(content[i]))) ++i;
        qt.anchor_region = content.substr(0, i);

        auto try_parse_clause = [&](size_t& pos, AnchorRegionClause* out) -> bool {
            while (pos < content.size() && std::isspace(static_cast<unsigned char>(content[pos]))) ++pos;
            if (pos >= content.size()) return false;
            static const struct {
                const char* pref;
                size_t len;
                AnchorRegionClauseKind kind;
            } kClauses[] = {
                {"contains(", 9, AnchorRegionClauseKind::Contains},
                {"rchild(", 7, AnchorRegionClauseKind::RchildOf},
            };
            for (const auto& c : kClauses) {
                if (pos + c.len > content.size() || content.compare(pos, c.len, c.pref, c.len) != 0)
                    continue;
                pos += c.len;
                size_t id0 = pos;
                while (pos < content.size()
                       && (std::isalnum(static_cast<unsigned char>(content[pos]))
                           || content[pos] == '_'))
                    ++pos;
                if (pos == id0)
                    throw std::runtime_error(std::string(c.pref) + " in region anchor requires a label");
                out->peer_label = content.substr(id0, pos - id0);
                out->kind = c.kind;
                if (pos >= content.size() || content[pos] != ')')
                    throw std::runtime_error(std::string(c.pref) + " in region anchor expected ')'");
                ++pos;
                return true;
            }
            return false;
        };

        // Interleaved: key=value attrs and rchild(...)/contains(...) (not "child" — UD reserved)
        while (true) {
            while (i < content.size() && std::isspace(static_cast<unsigned char>(content[i]))) ++i;
            if (i >= content.size()) break;
            AnchorRegionClause cl;
            if (try_parse_clause(i, &cl)) {
                qt.anchor_region_clauses.push_back(std::move(cl));
                continue;
            }
            size_t key_start = i;
            while (i < content.size() && content[i] != '=' && !std::isspace(static_cast<unsigned char>(content[i])))
                ++i;
            std::string key = content.substr(key_start, i - key_start);
            if (key.empty())
                throw std::runtime_error("Malformed region anchor after '" + qt.anchor_region + "'");
            while (i < content.size() && std::isspace(static_cast<unsigned char>(content[i]))) ++i;
            if (i >= content.size() || content[i] != '=')
                throw std::runtime_error("Expected '=' in region anchor attribute or rchild/contains(...)");
            ++i;
            while (i < content.size() && std::isspace(static_cast<unsigned char>(content[i]))) ++i;
            std::string value;
            if (i < content.size() && content[i] == '"') {
                ++i;
                while (i < content.size() && content[i] != '"') value += content[i++];
                if (i < content.size()) ++i;
            } else {
                size_t val_start = i;
                while (i < content.size() && !std::isspace(static_cast<unsigned char>(content[i]))) ++i;
                value = content.substr(val_start, i - val_start);
            }
            qt.anchor_attrs.emplace_back(std::move(key), std::move(value));
        }

        // `<err code="SPLIT"> where MM, NN` — discontinuous-aware filter on
        // a previously bound match set. AND semantics across refs. Resolved
        // by the CLI driver before execute().
        if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "where") {
            lexer_.consume();
            while (true) {
                Token ref = lexer_.expect(TokType::IDENT);
                qt.where_refs.push_back(ref.text);
                if (lexer_.peek().type == TokType::COMMA) {
                    lexer_.consume();
                    continue;
                }
                break;
            }
        }

        return qt;
    }
    if (lexer_.peek().type == TokType::REGION_END) {
        Token re = lexer_.next();
        qt.anchor = RegionAnchorType::REGION_END;
        qt.anchor_region = re.text;
        qt.conditions = nullptr;
        return qt;
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
        // "aux" → [form="aux" | contr_form="aux"]+
        // + matches maximal contiguous runs (one hit per run); see executor single-token path.
        lexer_.consume();
        std::string raw = t.text;
        AttrCondition ac;
        ac.attr = "form";
        interpret_quoted_eq_string(ac, raw, opts_.strict_quoted_strings);
        auto form_node = ConditionNode::make_leaf(std::move(ac));
        AttrCondition ac_contr;
        ac_contr.attr = "contr_form";
        interpret_quoted_eq_string(ac_contr, std::move(raw), opts_.strict_quoted_strings);
        auto contr_node = ConditionNode::make_leaf(std::move(ac_contr));
        qt.conditions = ConditionNode::make_branch(BoolOp::OR, std::move(form_node), std::move(contr_node));
        qt.min_repeat = 1;
        qt.max_repeat = REPEAT_UNBOUNDED;
    } else if (t.type == TokType::REGEX) {
        // /pattern/ → [form=/pattern/ | contr_form=/pattern/]+
        lexer_.consume();
        AttrCondition ac;
        ac.attr = "form";
        ac.op = CompOp::REGEX;
        ac.value = t.text;
        auto form_node = ConditionNode::make_leaf(std::move(ac));
        AttrCondition ac_contr;
        ac_contr.attr = "contr_form";
        ac_contr.op = CompOp::REGEX;
        ac_contr.value = t.text;
        auto contr_node = ConditionNode::make_leaf(std::move(ac_contr));
        qt.conditions = ConditionNode::make_branch(BoolOp::OR, std::move(form_node), std::move(contr_node));
        qt.min_repeat = 1;
        qt.max_repeat = REPEAT_UNBOUNDED;
    } else {
        throw std::runtime_error("Expected '[' or string or /regex/ at position " +
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

    // Count condition: count(child[upos="ADJ"]) >= 3
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "count") {
        Token count_tok = lexer_.next();  // consume "count"
        if (lexer_.peek().type == TokType::LPAREN) {
            lexer_.consume();  // consume (

            // Parse relation keyword
            Token rel_tok = lexer_.expect(TokType::IDENT);
            std::string rel_lower = rel_tok.text;
            std::transform(rel_lower.begin(), rel_lower.end(), rel_lower.begin(), ::tolower);
            StructRelType srt;
            if (rel_lower == "child")           srt = StructRelType::CHILD;
            else if (rel_lower == "parent")     srt = StructRelType::PARENT;
            else if (rel_lower == "sibling")    srt = StructRelType::SIBLING;
            else if (rel_lower == "descendant") srt = StructRelType::DESCENDANT;
            else if (rel_lower == "ancestor")   srt = StructRelType::ANCESTOR;
            else throw std::runtime_error("Expected relation keyword (child/parent/sibling/descendant/ancestor) in count() at position " + std::to_string(rel_tok.pos));

            // Optional filter: [conditions]
            ConditionPtr filter;
            if (lexer_.peek().type == TokType::LBRACKET) {
                lexer_.consume();
                if (lexer_.peek().type != TokType::RBRACKET)
                    filter = parse_conditions();
                lexer_.expect(TokType::RBRACKET);
            }

            lexer_.expect(TokType::RPAREN);  // closing )

            // Comparison operator
            CompOp op;
            Token op_tok = lexer_.next();
            switch (op_tok.type) {
                case TokType::EQ:   op = CompOp::EQ;  break;
                case TokType::NEQ:  op = CompOp::NEQ; break;
                case TokType::LT:   op = CompOp::LT;  break;
                case TokType::GT:   op = CompOp::GT;  break;
                case TokType::LTE:  op = CompOp::LTE; break;
                case TokType::GTE:  op = CompOp::GTE; break;
                default: throw std::runtime_error("Expected comparison operator after count() at position " + std::to_string(op_tok.pos));
            }

            int64_t value = std::stoll(lexer_.expect(TokType::NUMBER).text);
            return ConditionNode::make_count(srt, filter, op, value);
        }
        // count not followed by ( — not a count expression
        throw std::runtime_error("Expected '(' after 'count' at position " + std::to_string(count_tok.pos));
    }

    // Check for [not] structural relation keyword
    if (lexer_.peek().type == TokType::IDENT) {
        std::string lower = lexer_.peek().text;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

        // Check for "not child [...]" etc.
        bool negated = false;
        if (lower == "not") {
            // Peek further: if next is a structural keyword, this is negated structural
            Token not_tok = lexer_.next();  // consume "not"
            if (lexer_.peek().type == TokType::IDENT) {
                std::string next_lower = lexer_.peek().text;
                std::transform(next_lower.begin(), next_lower.end(), next_lower.begin(), ::tolower);
                if (next_lower == "child" || next_lower == "parent" || next_lower == "sibling" ||
                    next_lower == "descendant" || next_lower == "ancestor") {
                    negated = true;
                    lower = next_lower;
                } else {
                    // Not a structural keyword after "not" — error, "not" isn't a valid attr name
                    throw std::runtime_error("Expected structural keyword (child/parent/sibling/descendant/ancestor) after 'not' at position " + std::to_string(not_tok.pos));
                }
            } else {
                throw std::runtime_error("Expected structural keyword after 'not' at position " + std::to_string(not_tok.pos));
            }
        }

        StructRelType srt;
        bool is_struct = true;
        if (lower == "child") srt = StructRelType::CHILD;
        else if (lower == "parent") srt = StructRelType::PARENT;
        else if (lower == "sibling") srt = StructRelType::SIBLING;
        else if (lower == "descendant") srt = StructRelType::DESCENDANT;
        else if (lower == "ancestor") srt = StructRelType::ANCESTOR;
        else is_struct = false;

        if (is_struct) {
            lexer_.consume();  // consume the keyword

            // Optional name: child b:[...]
            std::string nested_name;
            if (lexer_.peek().type == TokType::IDENT) {
                // Could be name:[] - check for colon
                Token name_tok = lexer_.next();
                if (lexer_.peek().type == TokType::COLON) {
                    lexer_.consume();
                    nested_name = name_tok.text;
                } else {
                    // Not a name, error - we expected [ after keyword
                    throw std::runtime_error("Expected '[' or name:[] after structural keyword at position " + std::to_string(name_tok.pos));
                }
            }

            lexer_.expect(TokType::LBRACKET);
            ConditionPtr nested;
            if (lexer_.peek().type != TokType::RBRACKET) {
                nested = parse_conditions();
            }
            lexer_.expect(TokType::RBRACKET);
            return ConditionNode::make_structural(srt, nested, nested_name, negated);
        }
    }

    // nvals(attr) op NUMBER — multivalue cardinality (pipe-separated components)
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "nvals") {
        lexer_.consume();
        lexer_.expect(TokType::LPAREN);
        AttrCondition ac;
        ac.is_nvals = true;
        Token t = lexer_.expect(TokType::IDENT);
        ac.attr = t.text;
        if (lexer_.peek().type == TokType::DOT) {
            lexer_.consume();
            ac.attr += "." + lexer_.expect(TokType::IDENT).text;
        }
        lexer_.expect(TokType::RPAREN);
        Token op_tok = lexer_.next();
        switch (op_tok.type) {
            case TokType::EQ:  ac.op = CompOp::EQ;  break;
            case TokType::NEQ: ac.op = CompOp::NEQ; break;
            case TokType::LT:  ac.op = CompOp::LT;  break;
            case TokType::GT:  ac.op = CompOp::GT;  break;
            case TokType::LTE: ac.op = CompOp::LTE; break;
            case TokType::GTE: ac.op = CompOp::GTE; break;
            default:
                throw std::runtime_error("Expected comparison operator after nvals(...) at position " +
                                         std::to_string(op_tok.pos));
        }
        ac.nvals_compare = std::stoll(lexer_.expect(TokType::NUMBER).text);
        return ConditionNode::make_leaf(std::move(ac));
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
                Token v = lexer_.next();
                if (v.type == TokType::STRING) {
                    interpret_quoted_eq_string(ac, std::string(v.text), opts_.strict_quoted_strings);
                } else if (v.type == TokType::NUMBER) {
                    ac.op = CompOp::EQ;
                    ac.value = v.text;
                } else {
                    throw std::runtime_error("Expected string or number after '=' at position " +
                                             std::to_string(v.pos));
                }
            }
            break;
        }
        case TokType::NEQ: {
            Token v = lexer_.next();
            ac.op = CompOp::NEQ;
            ac.value = v.text;
            if (v.type == TokType::STRING)
                validate_neq_quoted_string(ac.value, opts_.strict_quoted_strings);
            break;
        }
        case TokType::LT:   ac.op = CompOp::LT;   ac.value = lexer_.next().text; break;
        case TokType::GT:   ac.op = CompOp::GT;    ac.value = lexer_.next().text; break;
        case TokType::LTE:  ac.op = CompOp::LTE;   ac.value = lexer_.next().text; break;
        case TokType::GTE:  ac.op = CompOp::GTE;   ac.value = lexer_.next().text; break;
        default:
            throw std::runtime_error("Expected comparison operator at position " +
                                     std::to_string(op.pos));
    }

    // Optional %c (case-insensitive), %d (diacritics-insensitive) flags
    while (lexer_.peek().type == TokType::PERCENT) {
        lexer_.consume();
        Token flag = lexer_.expect(TokType::IDENT);
        if (flag.text == "c") ac.case_insensitive = true;
        else if (flag.text == "d") ac.diacritics_insensitive = true;
        else throw std::runtime_error("Unknown flag '%" + flag.text + "' (expected %c or %d)");
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
        case TokType::STRING:
        case TokType::REGION_START:
        case TokType::REGION_END:
            // Implicit SEQUENCE relation (adjacency)
            rel.type = RelationType::SEQUENCE;
            return true;   // don't consume — the token expr will consume it

        case TokType::IDENT:
            // Could be a label for next token, or structural keyword
            if (t.text == "within" || t.text == "with" || t.text == "not" || t.text == "containing") return false;
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

void Parser::parse_within_clause(TokenQuery& tq) {
    // Expects "within" keyword already consumed. Parses:
    //   within s
    //   within s_tuid="XXX"
    //   within s having [cond]
    Token struct_tok = lexer_.expect(TokType::IDENT);
    std::string full_name = struct_tok.text;

    // Check if this is shorthand: within s_tuid="XXX"
    Token next = lexer_.peek();
    if (next.type == TokType::EQ || next.type == TokType::NEQ ||
        next.type == TokType::LT || next.type == TokType::GT ||
        next.type == TokType::LTE || next.type == TokType::GTE) {
        // Shorthand: decompose region_attr into structure + attr
        auto us = full_name.find('_');
        if (us == std::string::npos || us + 1 >= full_name.size())
            throw std::runtime_error("within shorthand requires region_attr format (e.g., s_tuid)");
        tq.within = full_name.substr(0, us);

        // Parse operator and value, add as global region filter
        GlobalRegionFilter gf;
        gf.region_attr = full_name;
        Token op = lexer_.next();
        switch (op.type) {
            case TokType::EQ:   gf.op = CompOp::EQ;  break;
            case TokType::NEQ:  gf.op = CompOp::NEQ; break;
            case TokType::LT:   gf.op = CompOp::LT;  break;
            case TokType::GT:   gf.op = CompOp::GT;   break;
            case TokType::LTE:  gf.op = CompOp::LTE; break;
            case TokType::GTE:  gf.op = CompOp::GTE; break;
            default: break;
        }
        if (lexer_.peek().type == TokType::STRING)
            gf.value = lexer_.next().text;
        else if (lexer_.peek().type == TokType::NUMBER)
            gf.value = lexer_.next().text;
        else
            gf.value = lexer_.expect(TokType::IDENT).text;
        tq.global_region_filters.push_back(std::move(gf));
    } else {
        // Regular within: store the structure name
        tq.within = full_name;
    }

    // Check for "having [cond]" — works after both plain and shorthand within
    if (lexer_.peek().type == TokType::IDENT && lexer_.peek().text == "having") {
        lexer_.consume();
        lexer_.expect(TokType::LBRACKET);
        if (lexer_.peek().type != TokType::RBRACKET) {
            tq.within_having = parse_conditions();
        }
        lexer_.expect(TokType::RBRACKET);
    }
}

void Parser::parse_containing_clause(TokenQuery& tq, bool negated) {
    // "containing" keyword already consumed.  Parses:
    //   containing s                    — structural: match encloses a full region
    //   containing subtree [cond]       — dependency: match encloses full subtree of token matching cond
    //   not containing s / not containing subtree [cond]  — negated forms

    ContainingClause cc;
    cc.negated = negated;

    Token t = lexer_.peek();
    if (t.type == TokType::IDENT && t.text == "subtree") {
        lexer_.consume();
        cc.is_subtree = true;
        lexer_.expect(TokType::LBRACKET);
        if (lexer_.peek().type != TokType::RBRACKET)
            cc.subtree_cond = parse_conditions();
        lexer_.expect(TokType::RBRACKET);
    } else if (t.type == TokType::IDENT) {
        lexer_.consume();
        cc.region = t.text;
    } else {
        throw std::runtime_error("Expected region name or 'subtree' after 'containing'");
    }

    tq.containing_clauses.push_back(std::move(cc));
}

void Parser::parse_global_filters(TokenQuery& tq) {
    auto parse_one = [this, &tq]() {
        Token t = lexer_.next();
        if (t.type != TokType::IDENT)
            throw std::runtime_error("Expected identifier in global filter");

        // Function call: distance(a, b) < 5, depth(a) > depth(b), etc.
        if (lexer_.peek().type == TokType::LPAREN) {
            // Helper lambda: parse func_name(args...) into a GlobalFuncCall.
            // Assumes the name token has been consumed; LPAREN is next.
            auto parse_func_call = [&](const std::string& name) -> GlobalFuncCall {
                GlobalFuncCall fc;
                if (name == "distance")          fc.func = GlobalFunctionType::DISTANCE;
                else if (name == "distabs")      fc.func = GlobalFunctionType::DISTABS;
                else if (name == "strlen")       fc.func = GlobalFunctionType::STRLEN;
                else if (name == "f")            fc.func = GlobalFunctionType::FREQ;
                else if (name == "nchildren")    fc.func = GlobalFunctionType::NCHILDREN;
                else if (name == "depth")        fc.func = GlobalFunctionType::DEPTH;
                else if (name == "ndescendants") fc.func = GlobalFunctionType::NDESCENDANTS;
                else if (name == "nvals")         fc.func = GlobalFunctionType::NVALS;
                else if (name == "contains")      fc.func = GlobalFunctionType::CONTAINS;
                else if (name == "rchild")        fc.func = GlobalFunctionType::RCHILD;
                else throw std::runtime_error("Unknown function in global filter: " + name);
                lexer_.consume(); // consume LPAREN
                while (lexer_.peek().type != TokType::RPAREN) {
                    std::string arg = lexer_.expect(TokType::IDENT).text;
                    if (lexer_.peek().type == TokType::DOT) {
                        lexer_.consume();
                        arg += "." + lexer_.expect(TokType::IDENT).text;
                    }
                    fc.args.push_back(std::move(arg));
                    if (lexer_.peek().type == TokType::COMMA) lexer_.consume();
                }
                lexer_.expect(TokType::RPAREN);
                return fc;
            };

            GlobalFunctionFilter ff;
            ff.lhs = parse_func_call(t.text);

            // Read comparison operator
            Token op_tok = lexer_.next();
            switch (op_tok.type) {
                case TokType::EQ:   ff.op = CompOp::EQ;  break;
                case TokType::NEQ:  ff.op = CompOp::NEQ; break;
                case TokType::LT:   ff.op = CompOp::LT;  break;
                case TokType::GT:   ff.op = CompOp::GT;  break;
                case TokType::LTE:  ff.op = CompOp::LTE; break;
                case TokType::GTE:  ff.op = CompOp::GTE; break;
                default: throw std::runtime_error("Expected comparison after function in global filter");
            }

            // RHS: either a number or another function call
            if (lexer_.peek().type == TokType::NUMBER) {
                ff.int_value = std::stoll(lexer_.next().text);
            } else if (lexer_.peek().type == TokType::IDENT) {
                std::string rhs_name = lexer_.next().text;
                if (lexer_.peek().type != TokType::LPAREN)
                    throw std::runtime_error("Expected '(' or number after comparison in global filter");
                ff.has_rhs_func = true;
                ff.rhs = parse_func_call(rhs_name);
            } else {
                throw std::runtime_error("Expected number or function after comparison in global filter");
            }

            tq.global_function_filters.push_back(std::move(ff));
            return;
        }

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
            // Look ahead: "a < b" (position order), "a.attr op value" (anchored region),
            // "a.attr = b.attr" (alignment), or shorthand "text_lang = \"Dutch\"" (same as
            // match.text_lang — struct_attr must contain '_').
            Token next = lexer_.peek();
            if ((next.type == TokType::EQ || next.type == TokType::NEQ ||
                 next.type == TokType::LT || next.type == TokType::GT ||
                 next.type == TokType::LTE || next.type == TokType::GTE)
                && t.text.find('_') != std::string::npos) {
                // Shorthand region filter on match start (implicit anchor)
                GlobalRegionFilter gf;
                gf.region_attr = t.text;
                Token op = lexer_.next();
                switch (op.type) {
                    case TokType::EQ:   gf.op = CompOp::EQ;  break;
                    case TokType::NEQ:  gf.op = CompOp::NEQ; break;
                    case TokType::LT:   gf.op = CompOp::LT;  break;
                    case TokType::GT:   gf.op = CompOp::GT;  break;
                    case TokType::LTE:  gf.op = CompOp::LTE; break;
                    case TokType::GTE:  gf.op = CompOp::GTE; break;
                    default: throw std::runtime_error("Expected comparison in global region shorthand");
                }
                if (lexer_.peek().type == TokType::STRING)
                    gf.value = lexer_.next().text;
                else if (lexer_.peek().type == TokType::NUMBER)
                    gf.value = lexer_.next().text;
                else
                    gf.value = lexer_.expect(TokType::IDENT).text;
                tq.global_region_filters.push_back(std::move(gf));
            } else if (next.type == TokType::LT || next.type == TokType::GT) {
                // Positional ordering: :: a < b or :: a > b (no '_' in first name — not struct_attr)
                CompOp op = (next.type == TokType::LT) ? CompOp::LT : CompOp::GT;
                lexer_.consume();
                Token name2 = lexer_.expect(TokType::IDENT);
                tq.position_orders.push_back({t.text, name2.text, op});
            } else if (next.type == TokType::DOT) {
                // a.attr ... — could be anchored region filter or alignment
                lexer_.consume(); // consume DOT
                std::string attr1 = lexer_.expect(TokType::IDENT).text;
                // Compose full region attr (handle dotted sub-attrs like feats.Number)
                if (lexer_.peek().type == TokType::DOT) {
                    lexer_.consume();
                    attr1 += "." + lexer_.expect(TokType::IDENT).text;
                }
                // Read comparison operator
                Token op_tok = lexer_.next();
                CompOp op;
                switch (op_tok.type) {
                    case TokType::EQ:   op = CompOp::EQ;  break;
                    case TokType::NEQ:  op = CompOp::NEQ; break;
                    case TokType::LT:   op = CompOp::LT;  break;
                    case TokType::GT:   op = CompOp::GT;  break;
                    case TokType::LTE:  op = CompOp::LTE; break;
                    case TokType::GTE:  op = CompOp::GTE; break;
                    default: throw std::runtime_error("Expected comparison operator in global filter");
                }
                // RHS: string/number → anchored region filter; IDENT → alignment
                Token rhs = lexer_.peek();
                if (rhs.type == TokType::STRING || rhs.type == TokType::NUMBER) {
                    // Anchored region filter: :: a.text_lang = "French"
                    GlobalRegionFilter gf;
                    gf.anchor_name = t.text;
                    gf.region_attr = attr1;
                    gf.op = op;
                    gf.value = lexer_.next().text;
                    tq.global_region_filters.push_back(std::move(gf));
                } else {
                    // Alignment: :: a.upos = b.upos
                    GlobalAlignmentFilter af;
                    af.name1 = t.text;
                    af.attr1 = attr1;
                    af.name2 = lexer_.expect(TokType::IDENT).text;
                    lexer_.expect(TokType::DOT);
                    af.attr2 = lexer_.expect(TokType::IDENT).text;
                    tq.global_alignment_filters.push_back(std::move(af));
                }
            } else {
                throw std::runtime_error("Expected '<', '>', or '.' after identifier in global filter");
            }
        }
    };

    parse_one();
    while (lexer_.peek().type == TokType::AMP) {
        lexer_.consume();
        parse_one();
    }
}

} // namespace manatree
