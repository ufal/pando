#include "corpus/derive_deps.h"

#include "corpus/corpus.h"
#include "index/relation_layer.h"

#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace pando {

namespace {

std::string_view base_of(std::string_view rel) {
    const size_t colon = rel.find(':');
    return colon == std::string_view::npos ? rel : rel.substr(0, colon);
}

bool is_subject(std::string_view rel) {
    const std::string_view b = base_of(rel);
    return b == "nsubj" || b == "csubj";
}

// relations a conjunct does not inherit from the first conjunct: loose clause links and function words (measured on
// English-EWT's DEPS: parataxis 4%, list 11%, aux 11%, cop 25% of such edges there, against 91-100% for the others)
bool propagates(std::string_view rel) {
    const std::string_view b = base_of(rel);
    return b != "parataxis" && b != "list" && b != "aux" && b != "cop" && b != "punct" && b != "reparandum"
           && b != "orphan" && b != "discourse";
}

bool is_predicate(std::string_view upos) { return upos == "VERB" || upos == "ADJ" || upos == "AUX"; }

}  // namespace

bool parse_derive_rule(const std::string& s, DeriveRule& out) {
    if (s == "effective-head") out = DeriveRule::EffectiveHead;
    else if (s == "conj-propagation") out = DeriveRule::ConjPropagation;
    else return false;
    return true;
}

const char* derive_rule_name(DeriveRule r) {
    return r == DeriveRule::EffectiveHead ? "effective-head" : "conj-propagation";
}

RelationDecl derive_deps_layer(const Corpus& corpus, const std::string& dir, const std::string& name, DeriveRule rule,
                               DerivedDepsStats* stats) {
    if (!corpus.has_deps() || !corpus.has_attr("deprel"))
        throw std::runtime_error("deriving " + name + " needs a dependency tree with deprel");
    if (!corpus.has_structure("s")) throw std::runtime_error("deriving " + name + " needs sentences (s)");
    const auto& deps = corpus.deps();
    const auto& deprel = corpus.attr("deprel");
    const PositionalAttr* upos = corpus.has_attr("upos") ? &corpus.attr("upos") : nullptr;
    const auto& sentences = corpus.structure("s");

    DerivedDepsStats st;
    RelationLayerWriter writer(dir, name);
    std::vector<RelationLayerWriter::Token> toks;
    std::vector<int> head;                     // 1-based within the sentence, 0 = root, -1 = none
    std::vector<std::string_view> rel;
    std::vector<std::set<std::pair<int, std::string>>> edges;
    std::vector<std::vector<int>> children;
    for (size_t r = 0; r < sentences.region_count(); ++r) {
        const Region rg = sentences.get(r);
        if (rg.end < rg.start) continue;
        const int n = static_cast<int>(rg.end - rg.start + 1);
        head.assign(static_cast<size_t>(n), -1);
        rel.assign(static_cast<size_t>(n), std::string_view());
        edges.assign(static_cast<size_t>(n), {});
        children.assign(static_cast<size_t>(n) + 1, {});
        for (int i = 0; i < n; ++i) {
            const CorpusPos p = rg.start + i;
            const CorpusPos h = deps.head(p);
            head[i] = h == NO_HEAD ? 0 : (h >= rg.start && h <= rg.end ? static_cast<int>(h - rg.start + 1) : -1);
            rel[i] = deprel.value_at(p);
            if (head[i] >= 0) {
                edges[i].emplace(head[i], std::string(rel[i]));
                children[static_cast<size_t>(head[i])].push_back(i + 1);
            }
        }
        const bool eff = rule == DeriveRule::EffectiveHead;
        auto has_subject = [&](int c) {
            for (int k : children[static_cast<size_t>(c)])
                if (is_subject(rel[k - 1])) return true;
            return false;
        };
        // 1. conjuncts: the first conjunct's head and relation (UD attaches every conjunct to the first conjunct; a
        //    conjunct of a conjunct is a nested coordination, "write …, but he is either fined or sent")
        for (int i = 0; i < n; ++i) {
            if (base_of(rel[i]) != "conj" || head[i] <= 0) continue;
            int f = head[i];
            if (eff) {
                // replaces `conj`, up the chain of nested coordinations; a conjunct of the root is a root
                for (int guard = n; base_of(rel[f - 1]) == "conj" && head[f - 1] > 0 && guard > 0; --guard)
                    f = head[f - 1];
                if (head[f - 1] < 0) continue;
                edges[i] = {{head[f - 1], std::string(rel[f - 1])}};
                ++st.conj_heads;
            } else if (base_of(rel[f - 1]) != "conj" && head[f - 1] > 0 && propagates(rel[f - 1])
                       && edges[i].emplace(head[f - 1], std::string(rel[f - 1])).second) {
                ++st.conj_heads;   // added next to `conj`, as enhanced UD
            }
        }
        // 2. shared subjects of predicate conjuncts without their own (after 1, which replaces edges)
        for (int i = 0; i < n; ++i) {
            if (base_of(rel[i]) != "conj" || head[i] <= 0) continue;
            if (!upos || !is_predicate(upos->value_at(rg.start + i)) || has_subject(i + 1)) continue;
            int f = head[i];
            // eff: the nearest conjunct up the chain that has subjects ("Ann sings and either dances or jumps")
            if (eff)
                for (int guard = n; !has_subject(f) && base_of(rel[f - 1]) == "conj" && head[f - 1] > 0 && guard > 0;
                     --guard)
                    f = head[f - 1];
            for (int s : children[static_cast<size_t>(f)]) {
                if (!is_subject(rel[s - 1])) continue;
                std::vector<int> subjects{s};
                for (int c : children[static_cast<size_t>(s)])
                    if (base_of(rel[c - 1]) == "conj") subjects.push_back(c);
                for (int sj : subjects)
                    if (sj != i + 1 && edges[sj - 1].emplace(i + 1, std::string(rel[s - 1])).second)
                        ++st.shared_subjects;
            }
        }
        toks.assign(static_cast<size_t>(n), {});
        for (int i = 0; i < n; ++i) {
            auto& t = toks[static_cast<size_t>(i)];
            t.basic_head = head[i];
            t.basic_label = std::string(rel[i]);
            for (const auto& [h, l] : edges[i]) t.deps += (t.deps.empty() ? "" : "|") + std::to_string(h) + ":" + l;
            if (t.deps.empty()) t.deps = "_";
        }
        writer.add_sentence(rg.start, toks);
    }
    writer.finish();
    st.tokens = writer.tokens();
    st.deviating = writer.deviating();
    if (stats) *stats = st;

    RelationDecl d;
    d.name = name;
    d.kind = RelationDecl::Kind::Pointer;
    d.label = "edge";
    d.scope = "s";
    d.span = writer.span();
    d.endpoints = "token";
    d.source = std::string("derived:") + derive_rule_name(rule);
    d.inherit = "head";
    return d;
}

}  // namespace pando
