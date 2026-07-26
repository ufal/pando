#include "query/executor.h"
#include <cctype>
#include <stdexcept>
#include <string>
#include <optional>

namespace pando {

std::string read_tabulate_field(const Corpus& corpus, const Match& m,
                                const NameIndexMap& name_map,
                                const std::string& field) {
    enum class DatePartKind { None, Year, Century, Decade, Month, Week, Day };
    DatePartKind date_part = DatePartKind::None;
    std::string requested = field;
    if (field.size() > 6 && field.back() == ')') {
        if (field.rfind("year(", 0) == 0) {
            date_part = DatePartKind::Year;
            requested = field.substr(5, field.size() - 6);
        } else if (field.rfind("century(", 0) == 0) {
            date_part = DatePartKind::Century;
            requested = field.substr(8, field.size() - 9);
        } else if (field.rfind("decade(", 0) == 0) {
            date_part = DatePartKind::Decade;
            requested = field.substr(7, field.size() - 8);
        } else if (field.rfind("month(", 0) == 0) {
            date_part = DatePartKind::Month;
            requested = field.substr(6, field.size() - 7);
        } else if (field.rfind("week(", 0) == 0) {
            date_part = DatePartKind::Week;
            requested = field.substr(5, field.size() - 6);
        } else if (field.rfind("day(", 0) == 0) {
            date_part = DatePartKind::Day;
            requested = field.substr(4, field.size() - 5);
        }
    }

    auto parse_year_prefix = [](std::string_view text) -> std::optional<int64_t> {
        size_t i = 0;
        while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
        if (i >= text.size()) return std::nullopt;
        bool neg = false;
        if (text[i] == '+' || text[i] == '-') {
            neg = text[i] == '-';
            ++i;
        }
        if (i + 4 > text.size()) return std::nullopt;
        for (size_t k = 0; k < 4; ++k) {
            if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
                return std::nullopt;
        }
        if (i + 4 < text.size() && std::isdigit(static_cast<unsigned char>(text[i + 4])))
            return std::nullopt;
        int64_t y = 0;
        for (size_t k = 0; k < 4; ++k)
            y = y * 10 + static_cast<int64_t>(text[i + k] - '0');
        if (neg) y = -y;
        return y;
    };
    auto render_date_part = [&](const std::string& raw) -> std::string {
        if (date_part == DatePartKind::None) return raw;
        struct ParsedDateParts {
            int64_t year = 0;
            int month = 0;
            int day = 0;
            bool has_month = false;
            bool has_day = false;
        };
        auto is_leap_year = [](int64_t y) {
            return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));
        };
        auto days_in_month = [&](int64_t y, int m) {
            static const int kDays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
            if (m == 2) return is_leap_year(y) ? 29 : 28;
            return kDays[m - 1];
        };
        auto parse_date_parts_prefix = [&](std::string_view text) -> std::optional<ParsedDateParts> {
            size_t i = 0;
            while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
            if (i + 4 > text.size()) return std::nullopt;
            for (size_t k = 0; k < 4; ++k) {
                if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
                    return std::nullopt;
            }
            ParsedDateParts out;
            for (size_t k = 0; k < 4; ++k)
                out.year = out.year * 10 + static_cast<int64_t>(text[i + k] - '0');
            i += 4;
            if (i >= text.size() || (text[i] != '-' && text[i] != '/'))
                return out;
            char sep = text[i++];
            if (i + 2 > text.size()) return std::nullopt;
            if (!std::isdigit(static_cast<unsigned char>(text[i]))
                || !std::isdigit(static_cast<unsigned char>(text[i + 1])))
                return std::nullopt;
            out.month = static_cast<int>((text[i] - '0') * 10 + (text[i + 1] - '0'));
            if (out.month < 1 || out.month > 12) return std::nullopt;
            out.has_month = true;
            i += 2;
            if (i >= text.size() || text[i] != sep)
                return out;
            ++i;
            if (i + 2 > text.size()) return std::nullopt;
            if (!std::isdigit(static_cast<unsigned char>(text[i]))
                || !std::isdigit(static_cast<unsigned char>(text[i + 1])))
                return std::nullopt;
            out.day = static_cast<int>((text[i] - '0') * 10 + (text[i + 1] - '0'));
            if (out.day < 1 || out.day > days_in_month(out.year, out.month))
                return std::nullopt;
            out.has_day = true;
            return out;
        };
        auto weekday_iso = [](int64_t y, int m, int d) {
            static const int t[12] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
            int64_t yy = y;
            if (m < 3) --yy;
            int w = static_cast<int>((yy + yy/4 - yy/100 + yy/400 + t[m - 1] + d) % 7);
            if (w < 0) w += 7;
            return w == 0 ? 7 : w;
        };
        auto iso_weeks_in_year = [&](int64_t y) {
            const int jan1 = weekday_iso(y, 1, 1);
            if (jan1 == 4) return 53;
            if (jan1 == 3 && is_leap_year(y)) return 53;
            return 52;
        };
        auto day_of_year = [&](int64_t y, int m, int d) {
            int doy = d;
            for (int mm = 1; mm < m; ++mm)
                doy += days_in_month(y, mm);
            return doy;
        };
        auto iso_week_number = [&](int64_t y, int m, int d) {
            int doy = day_of_year(y, m, d);
            int dow = weekday_iso(y, m, d);
            int week = (doy - dow + 10) / 7;
            if (week < 1) return iso_weeks_in_year(y - 1);
            int wiy = iso_weeks_in_year(y);
            if (week > wiy) return 1;
            return week;
        };

        auto y = parse_year_prefix(raw);
        if (!y) return "";
        if (date_part == DatePartKind::Year) return std::to_string(*y);
        if (*y <= 0) return "";
        if (date_part == DatePartKind::Century)
            return std::to_string(((*y - 1) / 100) + 1);
        if (date_part == DatePartKind::Decade)
            return std::to_string((*y / 10) * 10);
        auto p = parse_date_parts_prefix(raw);
        if (!p) return "";
        if (date_part == DatePartKind::Month) {
            if (!p->has_month) return "";
            return std::to_string(p->month);
        }
        if (date_part == DatePartKind::Day) {
            if (!p->has_day) return "";
            return std::to_string(p->day);
        }
        if (date_part == DatePartKind::Week) {
            if (!p->has_day) return "";
            return std::to_string(iso_week_number(p->year, p->month, p->day));
        }
        return "";
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
    std::string out;
    for (size_t i = 0; i < key.size() && i < data.columns.size(); ++i) {
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
