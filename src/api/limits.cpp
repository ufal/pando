#include "api/limits.h"

#include "core/json_utils.h"

#include <algorithm>

namespace pando {

QueryLimits parse_query_limits(const std::string& obj) {
    QueryLimits l;
    l.timeout_ms = json_extract_num(obj, "timeout_ms", 0);
    l.total_timeout_ms = json_extract_num(obj, "total_timeout_ms", 0);
    l.max_count_hits = json_extract_num(obj, "max_count_hits", 0);
    l.max_hits = json_extract_num(obj, "max_hits", 0);
    l.threads = static_cast<unsigned>(json_extract_num(obj, "threads", 0));
    l.deny = json_extract_str_array(obj, "deny");
    return l;
}

std::string query_limits_json(const QueryLimits& l) {
    std::string deny = "[";
    for (size_t i = 0; i < l.deny.size(); ++i) deny += (i ? ", " : "") + jstr(l.deny[i]);
    deny += "]";
    return "{\"timeout_ms\": " + std::to_string(l.timeout_ms)
        + ", \"total_timeout_ms\": " + std::to_string(l.total_timeout_ms)
        + ", \"max_count_hits\": " + std::to_string(l.max_count_hits)
        + ", \"max_hits\": " + std::to_string(l.max_hits)
        + ", \"threads\": " + std::to_string(l.threads) + ", \"deny\": " + deny + "}";
}

const std::vector<std::string>& limit_features() {
    static const std::vector<std::string> f = {"transitive", "unbounded_repeat", "regex_no_prefix",
                                               "regex", "parallel", "negated_relation"};
    return f;
}

size_t capped_timeout(size_t cap, size_t server_default, size_t requested) {
    const size_t base = cap ? cap : server_default;
    if (base == 0) return requested;
    return requested ? std::min(requested, base) : base;
}

namespace {

struct Uses {
    bool transitive = false, unbounded = false, regex = false, regex_no_prefix = false,
         parallel = false, negated_relation = false;
};

bool regex_has_literal_start(const std::string& pat) {
    size_t i = (!pat.empty() && pat[0] == '^') ? 1 : 0;
    if (i >= pat.size()) return false;
    static const std::string meta = ".[]()*+?{}|\\^$";
    if (meta.find(pat[i]) != std::string::npos) return false;
    // "a*…" / "a?…": the first char may repeat zero times
    if (i + 1 < pat.size() && (pat[i + 1] == '*' || pat[i + 1] == '?' || pat[i + 1] == '{')) return false;
    return pat.find('|') == std::string::npos;
}

void walk_cond(const ConditionPtr& c, Uses& u) {
    if (!c) return;
    if (c->is_leaf) {
        if (c->leaf.op == CompOp::REGEX) {
            u.regex = true;
            if (!regex_has_literal_start(c->leaf.value)) u.regex_no_prefix = true;
        }
        return;
    }
    if (c->is_structural) {
        if (c->struct_rel == StructRelType::DESCENDANT || c->struct_rel == StructRelType::ANCESTOR)
            u.transitive = true;
        walk_cond(c->nested_conditions, u);
        return;
    }
    if (c->is_count) {
        if (c->count_rel == StructRelType::DESCENDANT || c->count_rel == StructRelType::ANCESTOR)
            u.transitive = true;
        walk_cond(c->count_filter, u);
        return;
    }
    walk_cond(c->left, u);
    walk_cond(c->right, u);
}

void walk_query(const TokenQuery& q, Uses& u) {
    for (const auto& t : q.tokens) {
        walk_cond(t.conditions, u);
        if (t.max_repeat >= REPEAT_UNBOUNDED) u.unbounded = true;
    }
    for (const auto& r : q.relations) {
        if (r.type == RelationType::TRANS_GOVERNS || r.type == RelationType::TRANS_GOV_BY) u.transitive = true;
        if (r.type == RelationType::NOT_GOVERNS || r.type == RelationType::NOT_GOV_BY) u.negated_relation = true;
    }
    walk_cond(q.within_having, u);
    for (const auto& cc : q.containing_clauses) walk_cond(cc.subtree_cond, u);
}

}  // namespace

std::string denied_feature(const Program& prog, const std::vector<std::string>& deny) {
    if (deny.empty()) return {};
    Uses u;
    for (const auto& st : prog) {
        if (!st.has_query) continue;
        walk_query(st.query, u);
        if (st.is_parallel) {
            u.parallel = true;
            walk_query(st.target_query, u);
        }
    }
    for (const auto& f : deny) {
        if ((f == "transitive" && u.transitive) || (f == "unbounded_repeat" && u.unbounded)
            || (f == "regex" && u.regex) || (f == "regex_no_prefix" && u.regex_no_prefix)
            || (f == "parallel" && u.parallel) || (f == "negated_relation" && u.negated_relation))
            return f;
    }
    return {};
}

}  // namespace pando
