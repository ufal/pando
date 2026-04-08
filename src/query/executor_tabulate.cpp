#include "query/executor.h"
#include <stdexcept>
#include <string>
#include <optional>

namespace manatree {

std::string read_tabulate_field(const Corpus& corpus, const Match& m,
                                const NameIndexMap& name_map,
                                const std::string& field) {
    if (auto tc = evaluate_tcnt_tabulate_field(corpus, m, name_map, field))
        return *tc;

    CorpusPos pos = m.first_pos();
    std::string attr_spec = field;
    std::optional<std::string> named_token_label;

    if (field.rfind("match.", 0) == 0 && field.size() > 6) {
        attr_spec = field.substr(6);
    } else {
        auto dot = field.find('.');
        if (dot != std::string::npos && dot > 0) {
            std::string name = field.substr(0, dot);
            std::string rest = field.substr(dot + 1);

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
                if (sa.has_region_attr(rattr))
                    return std::string(sa.region_value(rattr, rr.region_idx));
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
        if (auto v = tg_val(m, attr_spec)) return *v;
        if (attr_spec == "type" && !tg_val(m, "type") && tg_val(m, "code")) return *tg_val(m, "code");
    }

    std::string attr = attr_spec;
    if (attr.size() > 5 && attr.substr(0, 5) == "feats" && attr.find('.') != std::string::npos)
        attr[attr.find('.')] = '_';
    if (corpus.has_attr(attr))
        return std::string(corpus.attr(attr).value_at(pos));

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
        if (!sa.has_region_attr(parts.attr_name)) {
            throw std::runtime_error(
                "Tabulate field '" + field + "': no region attribute '" + parts.attr_name
                + "' on structure '" + parts.struct_name + "'");
        }
        bool multi = corpus.is_overlapping(parts.struct_name)
                   || corpus.is_nested(parts.struct_name);
        if (multi) {
            std::string result;
            sa.for_each_region_at(pos, [&](size_t rgn_idx) -> bool {
                std::string_view v = sa.region_value(parts.attr_name, rgn_idx);
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
            return result;
        }
        int64_t rgn = sa.find_region(pos);
        if (rgn < 0) return "";
        return std::string(sa.region_value(parts.attr_name, static_cast<size_t>(rgn)));
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
    std::string out;
    for (size_t i = 0; i < key.size() && i < data.columns.size(); ++i) {
        if (i > 0) out += '\t';
        const auto& col = data.columns[i];
        if (col.kind == AggregateBucketData::Column::Kind::Positional) {
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

} // namespace manatree
