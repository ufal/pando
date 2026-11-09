#include "subcorpus/subcorpus.h"

#include "core/json_utils.h"
#include "corpus/corpus.h"
#include "index/positional_attr.h"
#include "query/dialect/dialect.h"
#include "query/executor.h"

#include <cctype>
#include <list>
#include <unordered_map>

namespace pando {

// ── parsing ─────────────────────────────────────────────────────────────

static std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && (std::isspace(static_cast<unsigned char>(s[b - 1])) || s[b - 1] == ';')) --b;
    return s.substr(a, b - a);
}

static bool ident_start(char c) { return std::isalpha(static_cast<unsigned char>(c)) || c == '_'; }
static bool ident_char(char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; }

bool is_subcorpus_name(const std::string& s) {
    if (s.empty() || !ident_start(s[0])) return false;
    for (char c : s)
        if (!ident_char(c)) return false;
    return true;
}

namespace {

struct ExprParser {
    const std::string& s;
    size_t i = 0;

    void ws() { while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i; }
    [[noreturn]] void fail(const std::string& what) const {
        throw SubcorpusError("invalid subcorpus definition: " + what + " at character " + std::to_string(i + 1) +
                             " of: " + s);
    }

    // the end of a parenthesised group starting at s[i] == '(' (quotes and brackets skipped)
    size_t group_end() const {
        int depth = 0;
        char quote = 0;
        for (size_t k = i; k < s.size(); ++k) {
            const char c = s[k];
            if (quote) {
                if (c == '\\') { ++k; continue; }
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '"' || c == '\'') { quote = c; continue; }
            if (c == '(' || c == '[' || c == '{') ++depth;
            else if (c == ')' || c == ']' || c == '}') {
                if (--depth == 0) {
                    if (c != ')') return std::string::npos;
                    return k;
                }
            }
        }
        return std::string::npos;
    }

    SubcorpusExprPtr operand() {
        ws();
        if (i >= s.size()) fail("an operand expected");
        if (s[i] == '(') {
            const size_t e = group_end();
            if (e == std::string::npos) fail("unbalanced '('");
            const std::string inner = s.substr(i + 1, e - i - 1);
            i = e + 1;
            return parse_subcorpus_definition(inner);
        }
        if (ident_start(s[i])) {
            const size_t a = i;
            while (i < s.size() && ident_char(s[i])) ++i;
            auto n = std::make_shared<SubcorpusExpr>();
            n->kind = SubcorpusExpr::Kind::Name;
            n->text = s.substr(a, i - a);
            return n;
        }
        fail("'(' or a name expected");
    }
    static SubcorpusExprPtr op_node(char op, SubcorpusExprPtr l, SubcorpusExprPtr r) {
        auto n = std::make_shared<SubcorpusExpr>();
        n->kind = SubcorpusExpr::Kind::Op;
        n->op = op;
        n->left = std::move(l);
        n->right = std::move(r);
        return n;
    }
    SubcorpusExprPtr term() {   // operand ('&' operand)*
        SubcorpusExprPtr l = operand();
        for (ws(); i < s.size() && s[i] == '&'; ws()) {
            ++i;
            l = op_node('&', l, operand());
        }
        return l;
    }
    SubcorpusExprPtr expr() {   // term (('|' | '-') term)*
        SubcorpusExprPtr l = term();
        for (ws(); i < s.size() && (s[i] == '|' || s[i] == '-'); ws()) {
            const char op = s[i++];
            l = op_node(op, l, term());
        }
        ws();
        if (i < s.size()) fail(std::string("unexpected '") + s[i] + "' (operators: & | -)");
        return l;
    }
};

}  // namespace

bool is_subcorpus_set_expression(const std::string& text) {
    const std::string t = trim(text);
    if (t.empty()) return false;
    if (t[0] == '(') return true;
    if (!ident_start(t[0])) return false;
    size_t k = 0;
    while (k < t.size() && ident_char(t[k])) ++k;
    while (k < t.size() && std::isspace(static_cast<unsigned char>(t[k]))) ++k;
    return k < t.size() && (t[k] == '&' || t[k] == '|' || t[k] == '-');
}

SubcorpusExprPtr parse_subcorpus_definition(const std::string& text) {
    const std::string t = trim(text);
    if (t.empty()) throw SubcorpusError("empty subcorpus definition");
    if (is_subcorpus_set_expression(t)) {
        ExprParser p{t};
        return p.expr();
    }
    auto n = std::make_shared<SubcorpusExpr>();
    if (is_subcorpus_name(t)) {
        n->kind = SubcorpusExpr::Kind::Name;   // `(A)`: a name
    } else {
        n->kind = SubcorpusExpr::Kind::Query;
    }
    n->text = t;
    return n;
}

// ── canonical form (names expanded) ─────────────────────────────────────

static SubcorpusExprPtr expand_names(const SubcorpusExprPtr& e, const SubcorpusLookup& lookup, int depth) {
    if (depth > 32) throw SubcorpusError("subcorpus definitions refer to each other in a cycle");
    switch (e->kind) {
        case SubcorpusExpr::Kind::Query: return e;
        case SubcorpusExpr::Kind::Name: {
            std::optional<std::string> def = lookup ? lookup(e->text) : std::nullopt;
            if (!def) throw SubcorpusError("unknown subcorpus '" + e->text + "'");
            return expand_names(parse_subcorpus_definition(*def), lookup, depth + 1);
        }
        case SubcorpusExpr::Kind::Op: {
            auto n = std::make_shared<SubcorpusExpr>(*e);
            n->left = expand_names(e->left, lookup, depth);
            n->right = expand_names(e->right, lookup, depth);
            return n;
        }
    }
    return e;
}

static std::string canonical(const SubcorpusExpr& e) {
    if (e.kind == SubcorpusExpr::Kind::Op)
        return "(" + canonical(*e.left) + " " + e.op + " " + canonical(*e.right) + ")";
    return "(" + e.text + ")";
}

// ── the cache ───────────────────────────────────────────────────────────

namespace {

struct Cache {
    std::mutex mu;
    size_t max_bytes = size_t{256} << 20;
    size_t bytes = 0;
    std::list<std::pair<std::string, ResolvedSubcorpusPtr>> lru;   // most recent first
    std::unordered_map<std::string, decltype(lru)::iterator> by_key;

    ResolvedSubcorpusPtr get(const std::string& k) {
        std::lock_guard<std::mutex> l(mu);
        auto it = by_key.find(k);
        if (it == by_key.end()) return nullptr;
        lru.splice(lru.begin(), lru, it->second);
        return it->second->second;
    }
    void put(const std::string& k, const ResolvedSubcorpusPtr& v) {
        std::lock_guard<std::mutex> l(mu);
        if (by_key.count(k)) return;
        lru.emplace_front(k, v);
        by_key[k] = lru.begin();
        bytes += v->set().bytes();
        while (bytes > max_bytes && lru.size() > 1) {
            auto& last = lru.back();
            bytes -= last.second->set().bytes();
            by_key.erase(last.first);
            lru.pop_back();
        }
    }
};

Cache& cache() {
    static Cache c;
    return c;
}

/// Positions of a definition query's hits: each token's span (a region anchor's hit is the region).
struct SpanSink : HitSink {
    PositionSet::Builder& b;
    explicit SpanSink(PositionSet::Builder& bb) : b(bb) {}
    void hit(const CorpusPos* starts, const CorpusPos* ends, size_t n) override {
        for (size_t i = 0; i < n; ++i)
            if (starts[i] != NO_HEAD) b.add(starts[i], ends[i] == NO_HEAD ? starts[i] : ends[i]);
    }
};

std::shared_ptr<const PositionSet> resolve_query(const Corpus& corpus, const std::string& text, ExecProgress* progress) {
    std::string q = text;
    const std::string dialect = take_language_prefix(q);
    Program prog;
    try {
        prog = parse_in_dialect(q, dialect, ParserOptions{}, &corpus);
    } catch (const std::exception& e) {
        throw SubcorpusError("invalid subcorpus definition: " + std::string(e.what()) + " (in: " + text + ")");
    }
    if (prog.size() != 1 || !prog[0].has_query || prog[0].has_command || !prog[0].name.empty())
        throw SubcorpusError("invalid subcorpus definition: one query expected (no names, commands or ';'): " + text);
    if (prog[0].is_parallel)
        throw SubcorpusError("invalid subcorpus definition: an aligned (`with`) query cannot define a subcorpus: " + text);
    PositionSet::Builder b;
    QueryExecutor ex(corpus);
    if (progress) ex.set_progress(progress);
    SpanSink sink(b);
    ex.set_hit_sink(&sink);
    MatchSet ms;
    try {
        ms = ex.execute(prog[0].query, 0, true, 0, 0, 0, 1);
    } catch (const QueryCancelled&) {
        throw;
    } catch (const SubcorpusError&) {
        throw;
    } catch (const std::exception& e) {
        throw SubcorpusError("invalid subcorpus definition: " + std::string(e.what()) + " (in: " + text + ")");
    }
    ex.set_hit_sink(nullptr);
    for (const Match& m : ms.matches) {   // the hits that did not go to the sink
        for (size_t i = 0; i < m.positions.size(); ++i) {
            if (m.positions[i] == NO_HEAD) continue;
            const CorpusPos e = (!m.span_ends.empty() && m.span_ends[i] != NO_HEAD) ? m.span_ends[i] : m.positions[i];
            b.add(m.positions[i], e);
        }
        for (const auto& kv : m.named_dep_subtrees)
            for (CorpusPos p : kv.second) b.add(p);
    }
    return std::make_shared<const PositionSet>(b.finish());
}

std::shared_ptr<const PositionSet> evaluate(const Corpus& corpus, const SubcorpusExpr& e, const std::string& id,
                                            ExecProgress* progress);

std::shared_ptr<const PositionSet> evaluate_cached(const Corpus& corpus, const SubcorpusExpr& e, const std::string& id,
                                                   ExecProgress* progress) {
    const std::string k = id + "\x1f" + canonical(e);
    if (auto hit = cache().get(k)) return hit->set_ptr();
    auto set = evaluate(corpus, e, id, progress);
    cache().put(k, std::make_shared<const ResolvedSubcorpus>(canonical(e), id, set));
    return set;
}

std::shared_ptr<const PositionSet> evaluate(const Corpus& corpus, const SubcorpusExpr& e, const std::string& id,
                                            ExecProgress* progress) {
    if (e.kind == SubcorpusExpr::Kind::Query) return resolve_query(corpus, e.text, progress);
    if (e.kind != SubcorpusExpr::Kind::Op) throw SubcorpusError("unknown subcorpus '" + e.text + "'");
    auto l = evaluate_cached(corpus, *e.left, id, progress);
    auto r = evaluate_cached(corpus, *e.right, id, progress);
    switch (e.op) {
        case '&': return std::make_shared<const PositionSet>(PositionSet::intersect(*l, *r));
        case '|': return std::make_shared<const PositionSet>(PositionSet::unite(*l, *r));
        default: return std::make_shared<const PositionSet>(PositionSet::subtract(*l, *r));
    }
}

}  // namespace

ResolvedSubcorpusPtr resolve_subcorpus(const Corpus& corpus, const std::string& definition,
                                       const SubcorpusResolveOptions& opts) {
    SubcorpusExprPtr e = expand_names(parse_subcorpus_definition(definition), opts.lookup, 0);
    const std::string id = corpus.index_identity();
    const std::string key = canonical(*e);
    const std::string k = id + "\x1f" + key;
    if (auto hit = cache().get(k)) return hit;
    auto set = evaluate(corpus, *e, id, opts.progress);
    auto r = std::make_shared<const ResolvedSubcorpus>(key, id, set);
    cache().put(k, r);
    return cache().get(k);   // the one in the cache (another thread may have put it first)
}

ResolvedSubcorpusPtr intersect_subcorpora(const Corpus& corpus, const ResolvedSubcorpusPtr& a,
                                          const ResolvedSubcorpusPtr& b) {
    const std::string key = "(" + a->key() + " & " + b->key() + ")";
    const std::string k = a->index_id() + "\x1f" + key;
    if (auto hit = cache().get(k)) return hit;
    (void)corpus;
    auto r = std::make_shared<const ResolvedSubcorpus>(
        key, a->index_id(), std::make_shared<const PositionSet>(PositionSet::intersect(a->set(), b->set())));
    cache().put(k, r);
    return cache().get(k);
}

void set_subcorpus_cache_bytes(size_t bytes) {
    std::lock_guard<std::mutex> l(cache().mu);
    cache().max_bytes = bytes;
}

void clear_subcorpus_cache() {
    std::lock_guard<std::mutex> l(cache().mu);
    cache().lru.clear();
    cache().by_key.clear();
    cache().bytes = 0;
}

std::shared_ptr<const std::vector<uint64_t>> ResolvedSubcorpus::id_counts(const std::string& attr_name,
                                                                           const PositionalAttr& pa) const {
    {
        std::lock_guard<std::mutex> l(mu_);
        auto it = id_counts_.find(attr_name);
        if (it != id_counts_.end()) return it->second;
    }
    auto v = std::make_shared<std::vector<uint64_t>>(static_cast<size_t>(std::max<int64_t>(pa.lexicon().size(), 0)), 0);
    set_->for_each_run([&](int64_t a, int64_t b) {
        for (int64_t p = a; p <= b; ++p) {
            const LexiconId id = pa.id_at(p);
            if (id >= 0 && static_cast<size_t>(id) < v->size()) ++(*v)[static_cast<size_t>(id)];
        }
    });
    std::lock_guard<std::mutex> l(mu_);
    auto [it, ins] = id_counts_.emplace(attr_name, std::move(v));
    return it->second;
}

std::shared_ptr<const void> ResolvedSubcorpus::memo(const std::string& key,
                                                    const std::function<std::shared_ptr<const void>()>& make) const {
    {
        std::lock_guard<std::mutex> l(mu_);
        auto it = memo_.find(key);
        if (it != memo_.end()) return it->second;
    }
    std::shared_ptr<const void> v = make();
    std::lock_guard<std::mutex> l(mu_);
    return memo_.emplace(key, std::move(v)).first->second;
}

std::string subcorpus_json_member(const AppliedSubcorpus& a) {
    std::string m = "\"subcorpus\": {\"name\": ";
    m += a.name.empty() ? std::string("null") : jstr(a.name);
    m += ", \"mode\": " + jstr(a.mode);
    m += ", \"tokens\": " + std::to_string(a.sc ? a.sc->set().tokens() : 0);
    m += ", \"ranges\": " + std::to_string(a.sc ? a.sc->set().runs() : 0);
    m += ", \"encoding\": " + jstr(a.sc ? a.sc->set().encoding_name() : "ranges");
    m += "}";
    return m;
}

std::string with_subcorpus_member(std::string json, const AppliedSubcorpus& a) {
    const size_t brace = json.find('{');
    if (brace == std::string::npos) return json;
    json.insert(brace + 1, subcorpus_json_member(a) + ", ");
    return json;
}

} // namespace pando
