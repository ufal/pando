#include "api/aligned_pieces.h"

#include "core/json_utils.h"
#include "corpus/equivalence_classes.h"

#include <algorithm>
#include <stdexcept>

namespace pando {

namespace {

constexpr CorpusPos kWindow = 10;   // around an aligned token, without sentences

// the region attribute value at pos (`text_iso`: the `iso` of pos's `text` region), "" when none
std::string_view region_value_at(const Corpus& corpus, const std::string& name, CorpusPos pos) {
    RegionAttrParts parts;
    if (!split_region_attr_name(name, parts) || !corpus.has_structure(parts.struct_name)) return {};
    const StructuralAttr& sa = corpus.structure(parts.struct_name);
    const auto key = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
    const int64_t r = key ? sa.find_region(pos) : -1;
    return r < 0 ? std::string_view() : sa.region_value(*key, static_cast<size_t>(r));
}

}  // namespace

AlignedWhere parse_aligned_where(const std::string& spec) {
    AlignedWhere w;
    if (spec.empty()) return w;
    const size_t eq = spec.find('=');
    if (eq == std::string::npos || eq == 0 || eq + 1 == spec.size())
        throw std::runtime_error("aligned where: ATTR=VALUE or ATTR=V1|V2 (e.g. text_iso=ces|deu), not '" + spec + "'");
    w.region_attr = spec.substr(0, eq);
    size_t a = eq + 1;
    while (a <= spec.size()) {
        size_t b = spec.find('|', a);
        if (b == std::string::npos) b = spec.size();
        if (b > a) w.values.push_back(spec.substr(a, b - a));
        a = b + 1;
    }
    return w;
}

std::vector<AlignedPiece> aligned_pieces(const Corpus& corpus, const Match& m, const std::string& layer,
                                         const AlignedWhere& where, size_t max_pieces) {
    const RelationDecl* decl = corpus.relation(layer);
    const std::string attr = decl && decl->kind == RelationDecl::Kind::Equivalence ? decl->attr : layer;
    EquivalenceClasses classes;
    if (!classes.open(corpus, attr))
        throw std::runtime_error("aligned: '" + layer + "' is not an equivalence layer or an attribute of this "
                                 "corpus (e.g. tuid, s_tuid)");
    const StructuralAttr* sents = corpus.has_structure("s") ? &corpus.structure("s") : nullptr;
    const std::vector<CorpusPos> hit = m.matched_positions();
    // the hit's own sentences: no piece of its own
    std::vector<int64_t> own;
    for (CorpusPos p : hit)
        if (const int64_t r = sents ? sents->find_region(p) : -1; r >= 0) own.push_back(r);
    std::sort(own.begin(), own.end());
    own.erase(std::unique(own.begin(), own.end()), own.end());
    auto keep = [&](CorpusPos p) {
        if (where.region_attr.empty()) return true;
        const std::string_view v = region_value_at(corpus, where.region_attr, p);
        return std::find(where.values.begin(), where.values.end(), v) != where.values.end();
    };

    std::vector<AlignedPiece> out;
    if (classes.regions()) {
        // a region layer: the other regions of the classes of the hit's tokens
        std::vector<LexiconId> cls;
        for (CorpusPos p : hit) classes.for_each_class(p, [&](LexiconId c) { cls.push_back(c); });
        std::sort(cls.begin(), cls.end());
        cls.erase(std::unique(cls.begin(), cls.end()), cls.end());
        for (LexiconId c : cls)
            classes.for_each_region(c, [&](const Region& r) {
                const bool mine = std::any_of(hit.begin(), hit.end(),
                                              [&](CorpusPos p) { return p >= r.start && p <= r.end; });
                if (!mine && keep(r.start)) out.push_back({r.start, r.end, {}});
            });
    } else {
        // a token layer: the tokens aligned with the hit's tokens, by sentence
        std::vector<CorpusPos> mates;
        for (CorpusPos p : hit)
            classes.for_each_class(p, [&](LexiconId c) {
                classes.for_each_member(c, [&](CorpusPos q) {
                    if (!std::binary_search(hit.begin(), hit.end(), q)) mates.push_back(q);
                });
            });
        std::sort(mates.begin(), mates.end());
        mates.erase(std::unique(mates.begin(), mates.end()), mates.end());
        for (CorpusPos q : mates) {
            const int64_t r = sents ? sents->find_region(q) : -1;
            if (r >= 0 && std::binary_search(own.begin(), own.end(), r)) continue;
            if (!keep(q)) continue;
            CorpusPos a = std::max<CorpusPos>(0, q - kWindow), b = std::min<CorpusPos>(corpus.size() - 1, q + kWindow);
            if (r >= 0) {
                const Region rg = sents->get(static_cast<size_t>(r));
                a = rg.start;
                b = rg.end;
            }
            if (!out.empty() && a <= out.back().end) {   // the same sentence (or an overlapping window)
                out.back().end = std::max(out.back().end, b);
                out.back().marked.push_back(q);
            } else {
                out.push_back({a, b, {q}});
            }
        }
    }
    std::sort(out.begin(), out.end(), [](const AlignedPiece& x, const AlignedPiece& y) { return x.start < y.start; });
    if (max_pieces > 0 && out.size() > max_pieces) out.resize(max_pieces);
    return out;
}

std::vector<std::pair<std::string, std::string>> aligned_piece_doc_attrs(const Corpus& corpus, CorpusPos pos) {
    std::vector<std::pair<std::string, std::string>> out;
    const std::string doc = corpus.has_structure("text") ? "text" : corpus.has_structure("doc") ? "doc" : "";
    if (doc.empty()) return out;
    const StructuralAttr& sa = corpus.structure(doc);
    const int64_t r = sa.find_region(pos);
    if (r < 0) return out;
    for (const auto& an : sa.region_attr_names()) {
        const std::string_view v = sa.region_value(an, static_cast<size_t>(r));
        if (!v.empty() && v != "_") out.emplace_back(an, std::string(v));
    }
    return out;
}

void write_aligned_json(std::ostream& out, const Corpus& corpus, const Match& m, const std::string& layer,
                        const std::string& where, size_t max_pieces) {
    if (layer.empty()) return;
    const auto pieces = aligned_pieces(corpus, m, layer, parse_aligned_where(where), max_pieces);
    out << ", \"aligned\": [";
    for (size_t i = 0; i < pieces.size(); ++i) {
        const AlignedPiece& pc = pieces[i];
        const std::string_view doc_id = lookup_doc_id(corpus, pc.start);
        out << (i ? ", " : "") << "{\"start\": " << pc.start << ", \"end\": " << pc.end
            << ", \"doc_id\": " << (doc_id.empty() ? "null" : jstr(doc_id)) << ", \"doc\": {";
        const auto attrs = aligned_piece_doc_attrs(corpus, pc.start);
        for (size_t k = 0; k < attrs.size(); ++k)
            out << (k ? ", " : "") << jstr(attrs[k].first) << ": " << jstr(attrs[k].second);
        out << "}, \"text\": " << jstr(aligned_piece_text(corpus, pc, "", "")) << ", \"marked\": [";
        for (size_t k = 0; k < pc.marked.size(); ++k) out << (k ? ", " : "") << pc.marked[k];
        out << "]}";
    }
    out << "]";
}

std::string aligned_piece_text(const Corpus& corpus, const AlignedPiece& piece, const std::string& mark_open,
                               const std::string& mark_close) {
    const auto& form = corpus.attr("form");
    std::string text;
    for (CorpusPos p = piece.start; p <= piece.end; ++p) {
        const bool mark = !mark_open.empty() && std::binary_search(piece.marked.begin(), piece.marked.end(), p);
        if (!text.empty()) text += ' ';
        if (mark) text += mark_open;
        text += form.value_at(p);
        if (mark) text += mark_close;
    }
    return text;
}

}  // namespace pando
