#include "index/relation_decl.h"

#include "index/dependency_index.h"
#include "index/structural_attr.h"

#include <cstdlib>
#include <sstream>

namespace pando {

std::string RelationDecl::format() const {
    std::ostringstream o;
    o << (kind == Kind::Pointer ? "pointer" : "equivalence");
    if (functional) o << ",functional";
    if (tree) o << ",tree";
    if (acyclic) o << ",acyclic";
    if (!attr.empty()) o << ",attr=" << attr;
    if (!label.empty()) o << ",label=" << label;
    o << ",scope=" << scope;
    if (span >= 0) o << ",span=" << span;
    o << ",endpoints=" << endpoints << ",source=" << source;
    if (!inherit.empty()) o << ",inherit=" << inherit;
    if (materialized) o << ",materialized";
    return o.str();
}

std::optional<RelationDecl> parse_relation(const std::string& name, const std::string& value, std::string* err) {
    auto fail = [&](const std::string& m) -> std::optional<RelationDecl> {
        if (err) *err = "relation." + name + ": " + m;
        return std::nullopt;
    };
    if (name.empty()) return fail("no name");
    RelationDecl r;
    r.name = name;
    std::stringstream ss(value);
    bool kind_seen = false;
    for (std::string item; std::getline(ss, item, ',');) {
        if (item.empty()) continue;
        const size_t eq = item.find('=');
        const std::string key = item.substr(0, eq), val = eq == std::string::npos ? "" : item.substr(eq + 1);
        if (key == "pointer" || key == "equivalence") {
            r.kind = key == "pointer" ? RelationDecl::Kind::Pointer : RelationDecl::Kind::Equivalence;
            kind_seen = true;
        } else if (key == "functional") r.functional = true;
        else if (key == "tree") r.tree = true;
        else if (key == "acyclic") r.acyclic = true;
        else if (key == "label") r.label = val;
        else if (key == "attr") r.attr = val;
        else if (key == "scope") r.scope = val;
        else if (key == "span") {
            char* end = nullptr;
            const long long n = std::strtoll(val.c_str(), &end, 10);
            if (val.empty() || *end || n < 0) return fail("span is not a number: " + val);
            r.span = n;
        } else if (key == "endpoints") r.endpoints = val;
        else if (key == "source") r.source = val;
        else if (key == "inherit") r.inherit = val;
        else if (key == "materialized") r.materialized = true;
        else return fail("unknown property " + key);
    }
    if (!kind_seen) return fail("pointer or equivalence missing");
    if (r.tree) r.functional = r.acyclic = true;   // a tree is both
    if (r.kind == RelationDecl::Kind::Equivalence && r.attr.empty())
        return fail("an equivalence names the attribute of its classes (attr=…)");
    return r;
}

RelationDecl measure_head_relation(const DependencyIndex& deps, const StructuralAttr& sentences,
                                   std::vector<std::string>* problems) {
    RelationDecl r;
    r.name = "head";
    r.kind = RelationDecl::Kind::Pointer;
    r.functional = true;   // one head per token by construction
    r.label = "deprel";
    r.scope = "s";
    r.endpoints = "token";
    r.source = "annotated";
    int64_t span = 0;
    bool tree = true, acyclic = true;
    auto note = [&](size_t si, const std::string& what) {
        if (problems && problems->size() < 5)
            problems->push_back("sentence " + std::to_string(si) + ": " + what);
    };
    std::vector<int32_t> state;   // per token of the sentence: 0 unvisited, 1 on the current walk, 2 done
    const size_t n = sentences.region_count();
    for (size_t si = 0; si < n; ++si) {
        const Region s = sentences.get(si);
        if (s.end < s.start) continue;
        const auto len = static_cast<size_t>(s.end - s.start + 1);
        size_t roots = 0, with_head = 0;
        for (CorpusPos p = s.start; p <= s.end; ++p) {
            const CorpusPos h = deps.head(p);
            if (h == NO_HEAD) { ++roots; continue; }
            ++with_head;
            span = std::max<int64_t>(span, h > p ? h - p : p - h);
            if (h < s.start || h > s.end) { tree = acyclic = false; note(si, "a head outside the sentence"); }
        }
        if (with_head == 0) continue;   // a sentence without dependencies says nothing about the tree
        if (roots != 1) { tree = false; note(si, std::to_string(roots) + " roots"); }
        // cycles: walk each token's head chain within the sentence
        state.assign(len, 0);
        for (size_t i = 0; i < len && acyclic; ++i) {
            std::vector<size_t> path;
            bool cycle = false;
            for (size_t j = i;;) {
                if (state[j] == 2) break;                    // reaches a chain already checked
                if (state[j] == 1) { cycle = true; break; }  // back on the current walk
                state[j] = 1;
                path.push_back(j);
                const CorpusPos h = deps.head(s.start + static_cast<CorpusPos>(j));
                if (h == NO_HEAD || h < s.start || h > s.end) break;
                j = static_cast<size_t>(h - s.start);
            }
            if (cycle) {
                acyclic = tree = false;
                note(si, "a cycle");
            }
            for (size_t k : path) state[k] = 2;
        }
    }
    r.span = span;
    r.acyclic = acyclic;
    r.tree = tree && acyclic;
    return r;
}

}  // namespace pando
