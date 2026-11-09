#include "query/sort_field.h"
#include "index/fold_map.h"

#include <stdexcept>
#include <string>
#include <vector>

namespace pando {

namespace {

CorpusPos anchor_position(const Match& m, const NameIndexMap& name_map, const std::string& anchor) {
    if (anchor == "match") return m.first_pos();
    if (anchor == "matchend") return m.last_pos();
    const CorpusPos p = resolve_name(m, name_map, anchor);
    if (p == NO_HEAD)
        throw std::runtime_error("sort range: no token labelled '" + anchor + "' in the query");
    return p;
}

/// `anchor` or `anchor[n]`
void parse_anchor(const std::string& s, std::string& anchor, int& off) {
    const size_t lb = s.find('[');
    if (lb == std::string::npos) {
        anchor = s;
        off = 0;
    } else {
        if (s.back() != ']') throw std::runtime_error("sort range: malformed anchor '" + s + "'");
        anchor = s.substr(0, lb);
        try {
            off = std::stoi(s.substr(lb + 1, s.size() - lb - 2));
        } catch (...) {
            throw std::runtime_error("sort range: malformed offset in '" + s + "'");
        }
    }
    if (anchor.empty()) throw std::runtime_error("sort range: empty anchor in '" + s + "'");
}

std::string anchor_text(const std::string& anchor, int off) {
    return off == 0 ? anchor : anchor + "[" + std::to_string(off) + "]";
}

}  // namespace

std::string fold_sort_text(std::string_view s, bool fold_case, bool fold_accents) {
    return FoldMap::fold(s, fold_case, fold_accents);
}

std::string reverse_utf8(std::string_view s) {
    std::string out(s.size(), '\0');
    size_t w = s.size();
    for (size_t i = 0; i < s.size();) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        size_t len = c < 0x80 ? 1 : (c >> 5) == 0x6 ? 2 : (c >> 4) == 0xE ? 3 : (c >> 3) == 0x1E ? 4 : 1;
        if (i + len > s.size()) len = 1;
        for (size_t k = 1; k < len; ++k)
            if ((static_cast<unsigned char>(s[i + k]) & 0xC0) != 0x80) { len = 1; break; }
        w -= len;
        out.replace(w, len, s.substr(i, len));
        i += len;
    }
    return out;
}

int64_t sort_anchor_position(const Match& m, const NameIndexMap& name_map, const std::string& anchor) {
    std::string a;
    int off = 0;
    parse_anchor(anchor, a, off);
    return static_cast<int64_t>(anchor_position(m, name_map, a)) + off;
}

namespace {
/// one token's value as the field's flags compare it
std::string sort_value(std::string_view v, const SortFieldSpec& spec) {
    std::string f = fold_sort_text(v, spec.fold_case, spec.fold_accents);
    return spec.retrograde ? reverse_utf8(f) : f;
}
}  // namespace

bool parse_sort_field(const std::string& field, SortFieldSpec& out) {
    const size_t sp = field.find(' ');
    if (sp == std::string::npos) return false;
    out = SortFieldSpec{};
    out.attr = field.substr(0, sp);
    std::vector<std::string> parts;
    for (size_t i = sp; i < field.size();) {
        while (i < field.size() && field[i] == ' ') ++i;
        const size_t j = field.find(' ', i);
        const size_t e = j == std::string::npos ? field.size() : j;
        if (e > i) parts.push_back(field.substr(i, e - i));
        i = e;
    }
    for (size_t k = 0; k < parts.size(); ++k) {
        const std::string& p = parts[k];
        if (p.size() > 1 && p[0] == '%') {
            for (size_t c = 1; c < p.size(); ++c) {
                if (p[c] == 'c') out.fold_case = true;
                else if (p[c] == 'd') out.fold_accents = true;
                else if (p[c] == 'r') out.retrograde = true;
                else throw std::runtime_error("sort field '" + field + "': unsupported flag %" + p.substr(c, 1));
            }
        } else if (p == "on" && k + 1 < parts.size()) {
            const std::string& r = parts[++k];
            const size_t dd = r.find("..");
            out.range = true;
            if (dd == std::string::npos) {
                parse_anchor(r, out.from_anchor, out.from_off);
                out.to_anchor = out.from_anchor;
                out.to_off = out.from_off;
            } else {
                parse_anchor(r.substr(0, dd), out.from_anchor, out.from_off);
                parse_anchor(r.substr(dd + 2), out.to_anchor, out.to_off);
            }
        } else {
            throw std::runtime_error("sort field '" + field + "': unexpected '" + p + "'");
        }
    }
    return true;
}

bool fields_have_options(const std::vector<std::string>& fields) {
    for (const auto& f : fields) {
        SortFieldSpec spec;
        if (parse_sort_field(f, spec)) return true;
    }
    return false;
}

std::string sort_field_text(const SortFieldSpec& spec) {
    std::string t = spec.attr;
    if (spec.fold_case || spec.fold_accents || spec.retrograde) {
        t += " %";
        if (spec.fold_case) t += 'c';
        if (spec.fold_accents) t += 'd';
        if (spec.retrograde) t += 'r';
    }
    if (spec.range)
        t += " on " + anchor_text(spec.from_anchor, spec.from_off) + ".."
             + anchor_text(spec.to_anchor, spec.to_off);
    return t;
}

std::string read_sort_field(const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
                            const SortFieldSpec& spec) {
    if (!spec.range) return sort_value(read_tabulate_field(corpus, m, name_map, spec.attr), spec);
    std::string attr_spec = spec.attr;
    if (attr_spec.rfind("match.", 0) == 0) attr_spec = attr_spec.substr(6);
    const std::string attr = normalize_query_attr_name(corpus, attr_spec);
    if (!corpus.has_attr(attr))
        throw std::runtime_error("sort range: '" + spec.attr + "' is not a token attribute");
    const auto& pa = corpus.attr(attr);
    // CQP: the tokens from the first boundary to the second, read backwards when the
    // second comes first (the left context: match[-1] is compared first)
    // (boundaries beyond the corpus are clamped to its first / last token, as CQP does)
    const int64_t size = static_cast<int64_t>(corpus.size());
    if (size == 0) return {};
    auto clamp = [size](int64_t p) { return p < 0 ? int64_t{0} : p >= size ? size - 1 : p; };
    const int64_t from = clamp(static_cast<int64_t>(anchor_position(m, name_map, spec.from_anchor)) + spec.from_off);
    const int64_t to = clamp(static_cast<int64_t>(anchor_position(m, name_map, spec.to_anchor)) + spec.to_off);
    const int64_t step = from <= to ? 1 : -1;
    std::string out;
    for (int64_t p = from;; p += step) {
        if (p != from) out += ' ';
        out += sort_value(pa.value_at(static_cast<CorpusPos>(p)), spec);
        if (p == to) break;
    }
    return out;
}

}  // namespace pando
