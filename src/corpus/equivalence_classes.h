#pragma once

// The classes of an equivalence layer (equivalence_layer.h) as integer ids: a class is one value of the layer's
// attribute. Both directions come from the attribute's own index, so nothing is stored for the layer.
//   * a token attribute (`tuid`): the members are the tokens carrying the value; token → classes is the forward
//     index (the multivalue forward index for a multivalue attribute), class → members its reverse index;
//   * a region attribute (`s_tuid`): a class is a set of regions, possibly far apart (a sentence and its
//     translations); the members are the tokens of those regions, token → class the value of its region,
//     class → regions the region attribute's reverse index.
// A multivalue attribute (as its source declares it: TEITOK settings for a `tuid` holding several ids) puts its
// token / region in the class of each of its ids ('|', the multivalue separator): a token aligned with two
// words, a sentence merging two. For a token attribute the multivalue index has the ids; for a region attribute the
// value → ids table is made once and kept with the attribute (Composite). An attribute not declared multivalue is
// read as it is: its whole values are the classes.
// The class join (executor_classes.cpp), the generic expansion and the declaration's measurement read these.

#include "core/types.h"
#include "corpus/corpus.h"
#include "index/positional_attr.h"
#include "index/structural_attr.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pando {

// The ids of an attribute whose values hold several of them: value (lexicon id) → its classes, class → its values
struct EquivalenceComposite {
    std::vector<std::vector<LexiconId>> value_classes;   // per lexicon id
    std::vector<std::vector<LexiconId>> class_values;    // per class id
};

// `tuid` and `*_tuid`: translation unit ids, multivalue
inline bool is_alignment_id_attr(const std::string& name) {
    return name == "tuid" || (name.size() > 5 && name.compare(name.size() - 5, 5, "_tuid") == 0);
}

// the multivalue separator
inline const char* id_separators(const std::string&) { return "|"; }

namespace equivalence_detail {

inline bool is_separator(char c, const char* seps) {
    for (const char* s = seps; *s; ++s)
        if (c == *s) return true;
    return false;
}

// nullptr when no value holds a separator (the classes are the values themselves)
inline std::shared_ptr<const EquivalenceComposite> build_composite(LexiconId n,
                                                                    const std::function<std::string_view(LexiconId)>& get,
                                                                    const char* seps) {
    bool any = false;
    for (LexiconId v = 0; v < n && !any; ++v)
        for (char c : get(v))
            if (is_separator(c, seps)) { any = true; break; }
    if (!any) return nullptr;
    auto comp = std::make_shared<EquivalenceComposite>();
    comp->value_classes.resize(static_cast<size_t>(n));
    std::unordered_map<std::string, LexiconId> ids;
    for (LexiconId v = 0; v < n; ++v) {
        const std::string_view s = get(v);
        size_t a = 0;
        while (a <= s.size()) {
            size_t b = a;
            while (b < s.size() && !is_separator(s[b], seps)) ++b;
            const std::string_view part = s.substr(a, b - a);
            if (!part.empty() && part != "_") {
                auto [it, fresh] = ids.try_emplace(std::string(part), static_cast<LexiconId>(comp->class_values.size()));
                if (fresh) comp->class_values.emplace_back();
                auto& vc = comp->value_classes[static_cast<size_t>(v)];
                if (std::find(vc.begin(), vc.end(), it->second) == vc.end()) {
                    vc.push_back(it->second);
                    comp->class_values[static_cast<size_t>(it->second)].push_back(v);
                }
            }
            a = b + 1;
        }
    }
    return comp;
}

}  // namespace equivalence_detail

class EquivalenceClasses {
public:
    // A token attribute; false when it cannot be read this way (a multivalue attribute without its forward index).
    // `seps`: the characters joining several ids in a value (id_separators)
    bool open(const PositionalAttr& attr, bool multivalue, const char* seps = "|") {
        attr_ = &attr;
        regions_ = nullptr;
        comp_.reset();
        mv_ = multivalue;
        if (mv_ && (!attr.has_mv() || !attr.has_mv_fwd())) return false;
        const Lexicon& lex = mv_ ? attr.mv_lexicon() : attr.lexicon();
        null1_ = lex.lookup("");
        null2_ = lex.lookup("_");
        nclasses_ = lex.size();
        (void)seps;
        return true;
    }

    // A region attribute `key` of `regions` (with its reverse index); `stacked`: the regions nest or overlap, so a
    // token can be in several (TEITOK sentences inside sentences); `multivalue`: a value's ids ('|') are its classes
    bool open(const StructuralAttr& regions, const std::string& key, bool stacked = false, bool multivalue = false,
              const char* seps = "|") {
        attr_ = nullptr;
        regions_ = &regions;
        mv_ = false;
        stacked_ = stacked;
        key_ = key;
        comp_.reset();
        region_lex_ = regions.region_to_lex_shared(key);
        if (!region_lex_ || region_lex_->size() != regions.region_count()) return false;
        null1_ = regions.region_attr_lex_lookup(key, "");
        null2_ = regions.region_attr_lex_lookup(key, "_");
        nclasses_ = regions.region_attr_lex_size(key);
        if (!multivalue) return true;
        const LexiconId n = nclasses_;
        comp_ = regions.cache().get<EquivalenceComposite>("equivalence-composite:" + key + ":" + seps,
                                                          [&regions, &key, n, seps] {
            return equivalence_detail::build_composite(n, [&regions, &key](LexiconId v) {
                return regions.region_attr_lex_get(key, v);
            }, seps);
        });
        if (comp_) nclasses_ = static_cast<LexiconId>(comp_->class_values.size());
        return true;
    }

    // The classes of an attribute by name: a token attribute (`tuid`) or a region attribute (`s_tuid`), multivalue as
    // the index declares it. False when the corpus has neither.
    bool open(const Corpus& corpus, const std::string& name) {
        if (corpus.has_attr(name)) return open(corpus.attr(name), corpus.is_multivalue(name), id_separators(name));
        RegionAttrParts parts;
        if (!split_region_attr_name(name, parts) || !corpus.has_structure(parts.struct_name)) return false;
        const StructuralAttr& sa = corpus.structure(parts.struct_name);
        const auto key = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        return key && sa.has_region_value_reverse(*key) &&
               open(sa, *key, corpus.is_nested(parts.struct_name) || corpus.is_overlapping(parts.struct_name),
                    corpus.is_multivalue(name), id_separators(name));
    }

    bool regions() const { return regions_ != nullptr; }
    // a token is in at most one class (not a multivalue attribute, nor regions that nest or overlap)
    bool single() const { return !mv_ && !comp_ && !(regions_ && stacked_); }
    // a multivalue region attribute: the classes are the ids of its values
    bool composite() const { return comp_ != nullptr; }

    // the class ids are 0 … class_count() - 1 (some of them not a class: is_class)
    LexiconId class_count() const { return nclasses_; }

    // an id that names a class (not the empty or `_` value)
    bool is_class(LexiconId id) const {
        if (comp_) return id >= 0 && id < nclasses_;
        return id >= 0 && id != null1_ && id != null2_;
    }

    // f(class id) for each class of the token at pos (none for an empty or `_` value)
    template <typename F>
    void for_each_class(CorpusPos pos, F&& f) const {
        LexiconId v = UNKNOWN_LEX;
        if (regions_ && stacked_) {
            // every region holding pos, each class once
            std::vector<LexiconId> cls;
            regions_->for_each_region_at(pos, [&](size_t r) {
                const LexiconId rv = (*region_lex_)[r];
                if (comp_) {
                    if (rv >= 0 && static_cast<size_t>(rv) < comp_->value_classes.size())
                        cls.insert(cls.end(), comp_->value_classes[static_cast<size_t>(rv)].begin(),
                                   comp_->value_classes[static_cast<size_t>(rv)].end());
                } else if (is_class(rv)) {
                    cls.push_back(rv);
                }
                return true;
            });
            std::sort(cls.begin(), cls.end());
            cls.erase(std::unique(cls.begin(), cls.end()), cls.end());
            for (LexiconId c : cls) f(c);
            return;
        }
        if (regions_) {
            const int64_t r = regions_->find_region(pos);
            if (r < 0) return;
            v = (*region_lex_)[static_cast<size_t>(r)];
        } else if (mv_) {
            attr_->for_each_mv_fwd_at(pos, [&](LexiconId id) {
                if (is_class(id)) f(id);
                return true;
            });
            return;
        } else {
            v = attr_->id_at(pos);
        }
        if (comp_) {
            if (v >= 0 && static_cast<size_t>(v) < comp_->value_classes.size())
                for (LexiconId c : comp_->value_classes[static_cast<size_t>(v)]) f(c);
            return;
        }
        if (is_class(v)) f(v);
    }

    // the number of members of a class
    size_t size(LexiconId cls) const {
        if (regions_) {
            size_t n = 0;
            for_each_range(cls, [&](CorpusPos a, CorpusPos b) { n += static_cast<size_t>(b - a + 1); });
            return n;
        }
        if (comp_) {
            size_t n = 0;
            for (LexiconId v : comp_->class_values[static_cast<size_t>(cls)]) n += attr_->rev_span_of_id(v).count;
            return n;
        }
        return mv_ ? attr_->mv_count_of_id(cls) : attr_->rev_span_of_id(cls).count;
    }

    // f(member position) for each token of the class, in corpus order (regions of a class in corpus order)
    template <typename F>
    void for_each_member(LexiconId cls, F&& f) const {
        if (regions_) {
            for_each_range(cls, [&](CorpusPos a, CorpusPos b) {
                for (CorpusPos p = a; p <= b; ++p) f(p);
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
        if (comp_) {
            const auto& vals = comp_->class_values[static_cast<size_t>(cls)];
            if (vals.size() == 1) {
                const RevSpan span = attr_->rev_span_of_id(vals[0]);
                for (size_t i = 0; i < span.count; ++i) f(span.at(i));
                return;
            }
            std::vector<CorpusPos> ps;
            for (LexiconId v : vals) {
                const RevSpan span = attr_->rev_span_of_id(v);
                for (size_t i = 0; i < span.count; ++i) ps.push_back(span.at(i));
            }
            std::sort(ps.begin(), ps.end());
            for (CorpusPos p : ps) f(p);
            return;
        }
        const RevSpan span = attr_->rev_span_of_id(cls);
        for (size_t i = 0; i < span.count; ++i) f(span.at(i));
    }

    // f(region) for each region of a class (a region attribute), in corpus order
    template <typename F>
    void for_each_region(LexiconId cls, F&& f) const {
        if (!regions_) return;
        auto regions_of_value = [&](LexiconId v, std::vector<int64_t>& out) {
            const int64_t* rs = nullptr;
            size_t n = 0;
            if (regions_->regions_for_id(key_, v, rs, n)) out.insert(out.end(), rs, rs + n);
        };
        std::vector<int64_t> idx;
        if (comp_) {
            for (LexiconId v : comp_->class_values[static_cast<size_t>(cls)]) regions_of_value(v, idx);
            std::sort(idx.begin(), idx.end());
        } else {
            regions_of_value(cls, idx);
        }
        for (int64_t i : idx) {
            const Region r = regions_->get(static_cast<size_t>(i));
            if (r.start <= r.end) f(r);
        }
    }

    // f(first, last) for the token ranges of a class (a region attribute): its regions, nested or overlapping ones
    // merged, so each token comes once, in corpus order
    template <typename F>
    void for_each_range(LexiconId cls, F&& f) const {
        bool open_range = false;
        CorpusPos a = 0, b = 0;
        for_each_region(cls, [&](const Region& r) {
            if (open_range && r.start <= b + 1) {
                b = std::max(b, r.end);
                return;
            }
            if (open_range) f(a, b);
            a = r.start;
            b = r.end;
            open_range = true;
        });
        if (open_range) f(a, b);
    }

private:
    const PositionalAttr* attr_ = nullptr;
    bool stacked_ = false;
    const StructuralAttr* regions_ = nullptr;
    std::string key_;
    std::shared_ptr<const std::vector<LexiconId>> region_lex_;
    std::shared_ptr<const EquivalenceComposite> comp_;
    bool mv_ = false;
    LexiconId null1_ = UNKNOWN_LEX, null2_ = UNKNOWN_LEX;
    LexiconId nclasses_ = 0;
};

}  // namespace pando
