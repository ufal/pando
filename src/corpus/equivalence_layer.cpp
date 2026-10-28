#include "corpus/equivalence_layer.h"

#include "corpus/corpus.h"
#include "corpus/equivalence_classes.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

namespace pando {

RelationDecl measure_equivalence(const Corpus& corpus, RelationDecl decl, EquivalenceStats* stats) {
    if (decl.kind != RelationDecl::Kind::Equivalence || decl.attr.empty())
        throw std::runtime_error("relation." + decl.name + ": an equivalence with attr=…");
    EquivalenceClasses classes;
    if (!classes.open(corpus, decl.attr))
        throw std::runtime_error("relation." + decl.name + ": the corpus has no attribute '" + decl.attr +
                                 "' (a token attribute, or a region attribute such as s_tuid)");
    const StructuralAttr* sents = corpus.has_structure("s") ? &corpus.structure("s") : nullptr;
    // the document regions: TEITOK / VRT `text`, CoNLL-U `# newdoc` (`doc`); with both (UD files holding several
    // documents), the scope is the narrower one that holds every class
    const StructuralAttr* texts = corpus.has_structure("text") ? &corpus.structure("text") : nullptr;
    const StructuralAttr* docs = corpus.has_structure("doc") ? &corpus.structure("doc") : nullptr;

    // per class, from its members: first and last position, the one sentence / document holding them all (if any)
    struct Class {
        CorpusPos first = 0, last = 0;
        int64_t s = -1, text = -1, doc = -1;
        bool one_s = true, one_text = true, one_doc = true;
        size_t n = 0;
    };
    std::vector<Class> cls(static_cast<size_t>(std::max<LexiconId>(classes.class_count(), 0)));
    int64_t s_hint = 0, t_hint = 0, d_hint = 0;
    for (CorpusPos p = 0; p < corpus.size(); ++p) {
        bool any = false;
        int64_t si = -1, ti = -1, di = -1;
        classes.for_each_class(p, [&](LexiconId id) {
            if (!any) {
                si = sents ? sents->find_region_from(p, s_hint) : -1;
                ti = texts ? texts->find_region_from(p, t_hint) : -1;
                di = docs ? docs->find_region_from(p, d_hint) : -1;
                if (si >= 0) s_hint = si;
                if (ti >= 0) t_hint = ti;
                if (di >= 0) d_hint = di;
                any = true;
            }
            Class& c = cls[static_cast<size_t>(id)];
            if (c.n == 0) {
                c.first = p;
                c.s = si;
                c.text = ti;
                c.doc = di;
            } else {
                c.one_s = c.one_s && si == c.s && si >= 0;
                c.one_text = c.one_text && ti == c.text && ti >= 0;
                c.one_doc = c.one_doc && di == c.doc && di >= 0;
            }
            c.last = p;
            ++c.n;
        });
    }
    EquivalenceStats st;
    bool in_s = sents != nullptr, in_text = texts != nullptr, in_doc = docs != nullptr;
    int64_t span = 0;
    for (const Class& c : cls) {
        if (c.n == 0) continue;
        ++st.classes;
        st.members += c.n;
        if (c.n == 1) ++st.singletons;
        st.largest = std::max(st.largest, c.n);
        in_s = in_s && c.one_s;
        in_text = in_text && c.one_text;
        in_doc = in_doc && c.one_doc;
        span = std::max<int64_t>(span, c.last - c.first);
    }
    const bool doc_narrower = in_doc && (!in_text || docs->region_count() > texts->region_count());
    decl.scope = in_s ? "s" : doc_narrower ? "doc" : in_text ? "text" : "corpus";
    decl.span = span;
    if (stats) *stats = st;
    return decl;
}

}  // namespace pando
