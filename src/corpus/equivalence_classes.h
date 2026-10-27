#pragma once

// The classes of an equivalence layer (equivalence_layer.h) as integer ids: a class is one value of the layer's
// attribute, its members the tokens carrying that value. Both directions come from the attribute's own index, so
// nothing is stored for the layer: token → classes is the forward index (the multivalue forward index for a
// multivalue attribute), class → members its reverse index. The class join (executor_classes.cpp) walks these.

#include "core/types.h"
#include "index/positional_attr.h"

namespace pando {

class EquivalenceClasses {
public:
    // false when the attribute cannot be read this way (a multivalue attribute without its forward index)
    bool open(const PositionalAttr& attr, bool multivalue) {
        attr_ = &attr;
        mv_ = multivalue;
        if (mv_ && (!attr.has_mv() || !attr.has_mv_fwd())) return false;
        const Lexicon& lex = mv_ ? attr.mv_lexicon() : attr.lexicon();
        null1_ = lex.lookup("");
        null2_ = lex.lookup("_");
        return true;
    }

    // a lexicon id that names a class (not the empty or `_` value)
    bool is_class(LexiconId id) const { return id >= 0 && id != null1_ && id != null2_; }

    // f(class id) for each class of the token at pos (none for an empty or `_` value)
    template <typename F>
    void for_each_class(CorpusPos pos, F&& f) const {
        if (mv_) {
            attr_->for_each_mv_fwd_at(pos, [&](LexiconId id) {
                if (is_class(id)) f(id);
                return true;
            });
            return;
        }
        const LexiconId id = attr_->id_at(pos);
        if (is_class(id)) f(id);
    }

    // the number of members of a class
    size_t size(LexiconId cls) const { return mv_ ? attr_->mv_count_of_id(cls) : attr_->rev_span_of_id(cls).count; }

    // f(member position) for each token of the class, in corpus order
    template <typename F>
    void for_each_member(LexiconId cls, F&& f) const {
        if (mv_) {
            attr_->for_each_position_mv(cls, [&](CorpusPos p) {
                f(p);
                return true;
            });
            return;
        }
        const RevSpan span = attr_->rev_span_of_id(cls);
        for (size_t i = 0; i < span.count; ++i) f(span.at(i));
    }

private:
    const PositionalAttr* attr_ = nullptr;
    bool mv_ = false;
    LexiconId null1_ = UNKNOWN_LEX, null2_ = UNKNOWN_LEX;
};

}  // namespace pando
