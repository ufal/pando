#include "api/program_subcorpora.h"

#include "corpus/corpus.h"
#include "subcorpus/restrict.h"

#include <cctype>

namespace pando {

namespace {

std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

/// Top-level statements (`;` outside quotes and brackets), as written.
std::vector<std::string> statements_of(const std::string& cql) {
    std::vector<std::string> out;
    int depth = 0;
    char quote = 0;
    size_t start = 0;
    for (size_t i = 0; i < cql.size(); ++i) {
        const char c = cql[i];
        if (quote) {
            if (c == '\\') { ++i; continue; }
            if (c == quote) quote = 0;
            continue;
        }
        if (c == '"' || c == '\'') quote = c;
        else if (c == '[' || c == '(' || c == '{') ++depth;
        else if ((c == ']' || c == ')' || c == '}') && depth > 0) --depth;
        else if (c == ';' && depth == 0) {
            out.push_back(cql.substr(start, i - start));
            start = i + 1;
        }
    }
    if (start < cql.size()) out.push_back(cql.substr(start));
    return out;
}

/// `use`, `use NAME`, `use (…)`: the definition after `use` ("" = reset); false for other statements.
bool use_statement(const std::string& t, std::string& def) {
    if (t.compare(0, 3, "use") != 0) return false;
    if (t.size() == 3) { def.clear(); return true; }
    if (!std::isspace(static_cast<unsigned char>(t[3])) && t[3] != '(') return false;
    def = trim(t.substr(3));
    return def.empty() || def[0] != '=';   // `use = …` is a hit set called use
}

/// `NAME = <set expression>`
bool define_statement(const std::string& t, std::string& name, std::string& def) {
    const size_t eq = t.find('=');
    if (eq == std::string::npos || eq == 0) return false;
    const std::string n = trim(t.substr(0, eq));
    if (!is_subcorpus_name(n)) return false;
    if (eq + 1 < t.size() && t[eq + 1] == '=') return false;
    const std::string rhs = trim(t.substr(eq + 1));
    if (!is_subcorpus_set_expression(rhs)) return false;
    name = n;
    def = rhs;
    return true;
}

}  // namespace

std::vector<ProgramPiece> split_subcorpus_statements(const std::string& cql) {
    std::vector<ProgramPiece> out;
    std::string plain;
    auto flush = [&]() {
        if (trim(plain).empty()) { plain.clear(); return; }
        out.push_back({ProgramPiece::Kind::Plain, plain, ""});
        plain.clear();
    };
    for (const std::string& raw : statements_of(cql)) {
        const std::string t = trim(raw);
        std::string def, name;
        if (use_statement(t, def)) {
            flush();
            if (def.empty()) {
                out.push_back({ProgramPiece::Kind::UseReset, "", ""});
            } else {
                std::string inner = def;
                while (inner.size() >= 2 && inner.front() == '(' && inner.back() == ')' &&
                       is_subcorpus_name(trim(inner.substr(1, inner.size() - 2))))
                    inner = trim(inner.substr(1, inner.size() - 2));
                out.push_back({ProgramPiece::Kind::Use, def, is_subcorpus_name(inner) ? inner : ""});
            }
        } else if (define_statement(t, name, def)) {
            flush();
            out.push_back({ProgramPiece::Kind::Define, def, name});
        } else {
            if (!t.empty()) plain += raw + ";";
        }
    }
    flush();
    return out;
}

bool has_subcorpus_statements(const std::string& cql) {
    for (const auto& p : split_subcorpus_statements(cql))
        if (p.kind != ProgramPiece::Kind::Plain) return true;
    return false;
}

namespace {

/// `within NAME` / `not within NAME` naming a subcorpus (no structure of the corpus): taken out.
AppliedSubcorpusPtr take_within_name(const Corpus& corpus, TokenQuery& q, const SubcorpusResolveOptions& ro) {
    if (q.within.empty() || corpus.has_structure(q.within) || corpus.is_token_group(q.within)) return nullptr;
    if (!is_subcorpus_name(q.within) || !ro.lookup || !ro.lookup(q.within)) return nullptr;
    if (q.within_having)
        throw SubcorpusError("within " + q.within + " having …: " + q.within + " is a subcorpus, not a structure");
    auto a = std::make_shared<AppliedSubcorpus>();
    a->mode = "within";
    ResolvedSubcorpusPtr sc = resolve_subcorpus(corpus, q.within, ro);
    if (q.not_within) {
        PositionSet::Builder all;
        if (corpus.size() > 0) all.add(0, corpus.size() - 1);
        auto rest = std::make_shared<const PositionSet>(PositionSet::subtract(all.finish(), sc->set()));
        sc = std::make_shared<const ResolvedSubcorpus>("(not " + sc->key() + ")", sc->index_id(), rest);
        a->name = "not " + q.within;
    } else {
        a->name = q.within;
    }
    a->sc = sc;
    q.within.clear();
    q.not_within = false;
    return a;
}

}  // namespace

SubcorpusStatsPtr restrict_statement(const Corpus& corpus, Statement& st, const AppliedSubcorpusPtr& use,
                                     const AppliedSubcorpusPtr& request_within, const SubcorpusResolveOptions& ro) {
    if (!st.has_query) return nullptr;
    AppliedSubcorpusPtr within = take_within_name(corpus, st.query, ro);
    if (st.is_parallel) {
        // the target side's own `within NAME`: a filter on it alone
        if (AppliedSubcorpusPtr tw = take_within_name(corpus, st.target_query, ro))
            restrict_query_to_subcorpus(corpus, st.target_query, tw->sc->set_ptr());
    }
    AppliedSubcorpusPtr w = within;
    if (request_within) {
        if (!w) {
            w = request_within;
        } else {
            auto both = std::make_shared<AppliedSubcorpus>(*within);
            both->sc = intersect_subcorpora(corpus, within->sc, request_within->sc);
            w = both;
        }
    }
    if (!use && !w) return nullptr;
    auto s = std::make_shared<SubcorpusStats>();
    s->use = use;
    if (use && w) s->inside = intersect_subcorpora(corpus, use->sc, w->sc);
    else s->inside = use ? use->sc : w->sc;
    s->reported = use ? use : w;
    restrict_statement_to_subcorpus(corpus, st, s->inside->set_ptr());
    return s;
}

} // namespace pando
