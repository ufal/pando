#pragma once

// The classes of an equivalence layer (equivalence_layer.h) as integer ids: a class is one value of the layer's
// attribute. Both directions come from the attribute's own index, so nothing is stored for the layer.
//   * a token attribute (`tuid`): the members are the tokens carrying the value; token → classes is the forward
//     index (the multivalue forward index for a multivalue attribute), class → members its reverse index;
//   * a region attribute (`s_tuid`): a class is a set of regions, possibly far apart (a sentence and its
//     translations); the members are the tokens of those regions, token → class the value of its region,
//     class → regions the region attribute's reverse index.
// The class join (executor_classes.cpp), the generic expansion and the declaration's measurement read these.

#include "core/types.h"
#include "corpus/corpus.h"
#include "index/positional_attr.h"
#include "index/structural_attr.h"

#include <memory>
#include <string>
#include <vector>

namespace pando {

class EquivalenceClasses {
public:
    // A token attribute; false when it cannot be read this way (a multivalue attribute without its forward index)
    bool open(const PositionalAttr& attr, bool multivalue) {
        attr_ = &attr;
        regions_ = nullptr;
        mv_ = multivalue;
        if (mv_ && (!attr.has_mv() || !attr.has_mv_fwd())) return false;
        const Lexicon& lex = mv_ ? attr.mv_lexicon() : attr.lexicon();
        null1_ = lex.lookup("");
        null2_ = lex.lookup("_");
        nclasses_ = lex.size();
        return true;
    }

    // A region attribute `key` of `regions` (single-valued, with its reverse index)
    bool open(const StructuralAttr& regions, const std::string& key) {
        attr_ = nullptr;
        regions_ = &regions;
        key_ = key;
        region_lex_ = regions.region_to_lex_shared(key);
        if (!region_lex_ || region_lex_->size() != regions.region_count()) return false;
        null1_ = regions.region_attr_lex_lookup(key, "");
        null2_ = regions.region_attr_lex_lookup(key, "_");
        nclasses_ = regions.region_attr_lex_size(key);
        return true;
    }

    // The classes of an attribute by name: a token attribute (`tuid`) or a region attribute (`s_tuid`). False when
    // the corpus has neither or it cannot be read as classes (a multivalue region attribute).
    bool open(const Corpus& corpus, const std::string& name) {
        if (corpus.has_attr(name)) return open(corpus.attr(name), corpus.is_multivalue(name));
        RegionAttrParts parts;
        if (!split_region_attr_name(name, parts) || !corpus.has_structure(parts.struct_name)) return false;
        if (corpus.is_multivalue(name)) return false;
        const StructuralAttr& sa = corpus.structure(parts.struct_name);
        const auto key = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        return key && sa.has_region_value_reverse(*key) && open(sa, *key);
    }

    bool regions() const { return regions_ != nullptr; }

    // the class ids are 0 … class_count() - 1 (some of them not a class: is_class)
    LexiconId class_count() const { return nclasses_; }

    // a lexicon id that names a class (not the empty or `_` value)
    bool is_class(LexiconId id) const { return id >= 0 && id != null1_ && id != null2_; }

    // f(class id) for each class of the token at pos (none for an empty or `_` value)
    template <typename F>
    void for_each_class(CorpusPos pos, F&& f) const {
        if (regions_) {
            const int64_t r = regions_->find_region(pos);
            if (r >= 0) {
                const LexiconId id = (*region_lex_)[static_cast<size_t>(r)];
                if (is_class(id)) f(id);
            }
            return;
        }
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
    size_t size(LexiconId cls) const {
        if (regions_) {
            size_t n = 0;
            for_each_region(cls, [&](const Region& r) { n += static_cast<size_t>(r.end - r.start + 1); });
            return n;
        }
        return mv_ ? attr_->mv_count_of_id(cls) : attr_->rev_span_of_id(cls).count;
    }

    // f(member position) for each token of the class, in corpus order (regions of a class in corpus order)
    template <typename F>
    void for_each_member(LexiconId cls, F&& f) const {
        if (regions_) {
            for_each_region(cls, [&](const Region& r) {
                for (CorpusPos p = r.start; p <= r.end; ++p) f(p);
            });
            return;
        }
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

    // f(region) for each region of a class (a region attribute), in corpus order
    template <typename F>
    void for_each_region(LexiconId cls, F&& f) const {
        const int64_t* rs = nullptr;
        size_t n = 0;
        if (!regions_ || !regions_->regions_for_id(key_, cls, rs, n)) return;
        for (size_t i = 0; i < n; ++i) {
            const Region r = regions_->get(static_cast<size_t>(rs[i]));
            if (r.start <= r.end) f(r);
        }
    }

private:
    const PositionalAttr* attr_ = nullptr;
    const StructuralAttr* regions_ = nullptr;
    std::string key_;
    std::shared_ptr<const std::vector<LexiconId>> region_lex_;
    bool mv_ = false;
    LexiconId null1_ = UNKNOWN_LEX, null2_ = UNKNOWN_LEX;
    LexiconId nclasses_ = 0;
};

}  // namespace pando
