#include "corpus/equivalence_layer.h"

#include "corpus/corpus.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace pando {

RelationDecl measure_equivalence(const Corpus& corpus, RelationDecl decl, EquivalenceStats* stats) {
    if (decl.kind != RelationDecl::Kind::Equivalence || decl.attr.empty())
        throw std::runtime_error("relation." + decl.name + ": an equivalence with attr=…");
    if (!corpus.has_attr(decl.attr))
        throw std::runtime_error("relation." + decl.name + ": the corpus has no attribute '" + decl.attr + "'");
    const auto& attr = corpus.attr(decl.attr);
    const bool mv = corpus.is_multivalue(decl.attr);
    const StructuralAttr* sents = corpus.has_structure("s") ? &corpus.structure("s") : nullptr;
    // the document region: TEITOK / VRT `text`, else CoNLL-U `# newdoc` (`doc`)
    const std::string doc_struct = corpus.has_structure("text") ? "text" : corpus.has_structure("doc") ? "doc" : "";
    const StructuralAttr* texts = doc_struct.empty() ? nullptr : &corpus.structure(doc_struct);

    struct Class {
        CorpusPos first = 0, last = 0;
        int64_t s = -1, text = -1;
        bool one_s = true, one_text = true;
        size_t n = 0;
    };
    std::unordered_map<std::string, Class> classes;
    int64_t s_hint = 0, t_hint = 0;
    for (CorpusPos p = 0; p < corpus.size(); ++p) {
        const std::string_view v = attr.value_at(p);
        if (v.empty() || v == "_") continue;
        const int64_t si = sents ? sents->find_region_from(p, s_hint) : -1;
        const int64_t ti = texts ? texts->find_region_from(p, t_hint) : -1;
        if (si >= 0) s_hint = si;
        if (ti >= 0) t_hint = ti;
        size_t a = 0;
        while (a <= v.size()) {
            size_t b = mv ? v.find('|', a) : std::string_view::npos;
            if (b == std::string_view::npos) b = v.size();
            const std::string_view id = v.substr(a, b - a);
            a = b + 1;
            if (id.empty() || id == "_") continue;
            auto [it, fresh] = classes.try_emplace(std::string(id));
            Class& c = it->second;
            if (fresh) {
                c.first = p;
                c.s = si;
                c.text = ti;
            } else {
                c.one_s = c.one_s && si == c.s && si >= 0;
                c.one_text = c.one_text && ti == c.text && ti >= 0;
            }
            c.last = p;
            ++c.n;
        }
    }
    EquivalenceStats st;
    bool in_s = sents != nullptr, in_text = texts != nullptr;
    int64_t span = 0;
    for (const auto& [id, c] : classes) {
        ++st.classes;
        st.members += c.n;
        if (c.n == 1) ++st.singletons;
        st.largest = std::max(st.largest, c.n);
        in_s = in_s && c.one_s;
        in_text = in_text && c.one_text;
        span = std::max<int64_t>(span, c.last - c.first);
    }
    decl.scope = in_s ? "s" : in_text ? doc_struct : "corpus";
    decl.span = span;
    if (stats) *stats = st;
    return decl;
}

}  // namespace pando
