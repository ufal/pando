#include "query/executor.h"
#include "query/geo_fields.h"
#include <cctype>
#include <stdexcept>
#include <string>
#include <optional>

namespace pando {

std::string read_tabulate_field(const Corpus& corpus, const Match& m,
                                const NameIndexMap& name_map,
                                const std::string& field) {
    // lat(), lon(), geodist(), geocell() of a location attribute (query/geo_fields.cpp)
    if (auto geo = evaluate_geo_tabulate_field(corpus, m, name_map, field)) return *geo;
    // a date part (core/dates): year(text_date) … day(), earliest(), latest(), precision()
    std::optional<DatePart> date_part;
    std::string requested = field;
    if (!field.empty() && field.back() == ')') {
        const size_t open = field.find('(');
        if (open != std::string::npos && open > 0 && open + 2 < field.size()) {
            if (auto p = date_part_of_function(std::string_view(field).substr(0, open))) {
                date_part = p;
                requested = field.substr(open + 1, field.size() - open - 2);
            }
        }
    }
    auto render_date_part = [&](const std::string& raw) -> std::string {
        if (!date_part) return raw;
        return date_part_label(raw, *date_part).value_or("");
    };

    if (auto tc = evaluate_tcnt_tabulate_field(corpus, m, name_map, field))
        return render_date_part(*tc);
    if (auto fs = evaluate_forms_tabulate_field(corpus, m, name_map, field))
        return render_date_part(*fs);
    if (auto sp = evaluate_spellout_tabulate_field(corpus, m, name_map, field))
        return render_date_part(*sp);

    CorpusPos pos = m.first_pos();
    std::string attr_spec = requested;
    std::optional<std::string> named_token_label;

    if (requested.rfind("match.", 0) == 0 && requested.size() > 6) {
        attr_spec = requested.substr(6);
    } else {
        auto dot = requested.find('.');
        if (dot != std::string::npos && dot > 0) {
            std::string name = requested.substr(0, dot);
            std::string rest = requested.substr(dot + 1);

            auto nr = m.named_regions.find(name);
            if (nr != m.named_regions.end()) {
                const RegionRef& rr = nr->second;
                if (!corpus.has_structure(rr.struct_name)) {
                    throw std::runtime_error(
                        "Tabulate field '" + field + "': region binding '" + name
                        + "' refers to unknown structure '" + rr.struct_name + "'");
                }
                const auto& sa = corpus.structure(rr.struct_name);
                std::string rattr = rest;
                if (rattr.size() > 5 && rattr.substr(0, 5) == "feats" && rattr.find('.') != std::string::npos)
                    rattr[rattr.find('.')] = '_';
                auto rkey = resolve_region_attr_key(sa, rr.struct_name, rattr);
                if (rkey)
                    return render_date_part(std::string(sa.region_value(*rkey, rr.region_idx)));
                // `t.cpos`: where the region starts (as `a.cpos` for a token), unless the
                // region has an attribute of that name; with `tcnt(t)` its extent, e.g. to
                // place hits within their text (dispersion plots)
                if (rattr == "cpos") return std::to_string(sa.get(rr.region_idx).start);
                if (auto super_val = lookup_super_region_attr_value(
                        corpus, sa, rr.struct_name, rr.region_idx, rattr)) {
                    return render_date_part(*super_val);
                }
                throw std::runtime_error(
                    "Tabulate field '" + field + "': no region attribute '" + rattr
                    + "' on structure '" + rr.struct_name + "' (binding '" + name + "')");
            }

            CorpusPos np = resolve_name(m, name_map, name);
            if (np != NO_HEAD) {
                pos = np;
                attr_spec = rest;
                named_token_label = name;
            } else {
                attr_spec = field;
            }
        }
    }

    // Stand-off token-group props (e.g. err `code` from groups/err.jsonl): `count by code`,
    // `n.code` when `n` labels `<err>`, etc. Map `type` → `code` when `type` is absent (parallel
    // to `node.type` naming).
    if (!m.token_group_props.empty()) {
        auto tg_val = [](const Match& mm, std::string_view key) -> std::optional<std::string> {
            for (const auto& kv : mm.token_group_props) {
                if (kv.first == key) return kv.second;
            }
            return std::nullopt;
        };
        if (auto v = tg_val(m, attr_spec)) return render_date_part(*v);
        if (attr_spec == "type" && !tg_val(m, "type") && tg_val(m, "code"))
            return render_date_part(*tg_val(m, "code"));
    }

    // `cpos` (CWB's term): the token's corpus position, unless the corpus has an attribute
    // of that name. `tabulate h.cpos, d.cpos` gives positions for computing distances,
    // word order, … outside pando.
    if (attr_spec == "cpos" && !corpus.has_attr("cpos"))
        return std::to_string(pos);
    std::string attr = normalize_query_attr_name(corpus, attr_spec);
    std::string feat_name;
    if (feats_is_subkey(attr, feat_name) && corpus.has_attr("feats")) {
        if (!corpus_has_ud_split_feats_column(corpus, feat_name)) {
            const auto& pa = corpus.attr("feats");
            return render_date_part(std::string(feats_extract_value(pa.value_at(pos), feat_name)));
        }
    }
    if (corpus.has_attr(attr))
        return render_date_part(std::string(corpus.attr(attr).value_at(pos)));

    RegionAttrParts parts;
    if (split_region_attr_name(attr_spec, parts)) {
        if (!corpus.has_structure(parts.struct_name)) {
            // e.g. typo `no_such_attr` splits to struct "no" — not a missing region *type*,
            // treat as unknown field unless this is `token.struct_attr` projection.
            if (named_token_label.has_value()) {
                throw std::runtime_error(
                    "Tabulate field '" + field + "': unknown token attribute '" + attr_spec
                    + "' for named token '" + *named_token_label + "'");
            }
            throw std::runtime_error(
                "Tabulate field '" + field + "': unknown token or region attribute");
        }
        const auto& sa = corpus.structure(parts.struct_name);
        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        if (!rkey) {
            throw std::runtime_error(
                "Tabulate field '" + field + "': no region attribute '" + parts.attr_name
                + "' on structure '" + parts.struct_name + "'");
        }
        bool multi = corpus.is_overlapping(parts.struct_name)
                   || corpus.is_nested(parts.struct_name);
        if (multi) {
            std::string result;
            sa.for_each_region_at(pos, [&](size_t rgn_idx) -> bool {
                std::string_view v = sa.region_value(*rkey, rgn_idx);
                if (v.empty()) return true;
                std::string vs(v);
                if (result.empty()) {
                    result = vs;
                } else if (result.find(vs) == std::string::npos) {
                    result += '|';
                    result += vs;
                }
                return true;
            });
            return render_date_part(result);
        }
        int64_t rgn = sa.find_region(pos);
        if (rgn < 0) return render_date_part("");
        return render_date_part(std::string(sa.region_value(*rkey, static_cast<size_t>(rgn))));
    }

    if (named_token_label.has_value()) {
        throw std::runtime_error(
            "Tabulate field '" + field + "': unknown token attribute '" + attr_spec
            + "' for named token '" + *named_token_label + "'");
    }
    throw std::runtime_error(
        "Tabulate field '" + field + "': unknown token or region attribute");
}

size_t AggregateBucketData::VecHash::operator()(const std::vector<int64_t>& v) const noexcept {
    size_t h = v.size();
    for (int64_t x : v) {
        uint64_t ux = static_cast<uint64_t>(x);
        h ^= std::hash<uint64_t>{}(ux + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
    }
    return h;
}

std::string decode_aggregate_bucket_key(const AggregateBucketData& data,
                                        const std::vector<int64_t>& key) {
    return decode_aggregate_bucket_key(data, key.data(), key.size());
}

std::string decode_aggregate_bucket_key(const AggregateBucketData& data,
                                        const int64_t* key, size_t key_len) {
    std::string out;
    for (size_t i = 0; i < key_len && i < data.columns.size(); ++i) {
        if (i > 0) out += '\t';
        const auto& col = data.columns[i];
        if (col.date_transform != AggregateBucketData::Column::DateTransform::None) {
            int64_t id = key[i];
            const auto& st = data.region_intern[i];
            if (id >= 1 && static_cast<size_t>(id) <= st.id_to_str.size())
                out += st.id_to_str[static_cast<size_t>(id - 1)];
        } else if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            LexiconId lid = static_cast<LexiconId>(key[i]);
            out += col.pa->lexicon().get(lid);
        } else {
            int64_t id = key[i];
            const auto& st = data.region_intern[i];
            if (id >= 1 && static_cast<size_t>(id) <= st.id_to_str.size())
                out += st.id_to_str[static_cast<size_t>(id - 1)];
        }
    }
    return out;
}

} // namespace pando
