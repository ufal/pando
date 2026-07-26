#include "query/executor.h"
#include "query/executor_aggregate_internal.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <map>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace pando {

#if 0
bool QueryExecutor::try_fast_aggregate(
        const TokenQuery& q,
        AggregateBucketData& agg,
        const std::vector<ResolvedRegionFilter>& resolved_filters,
        const std::vector<AnchorConstraint>& token_anchor_constraints,
        size_t max_total_cap,
        MatchSet& result,
        const NameIndexMap& name_map) const {

    // Guard: single-token, non-repeating, no global function filters
    if (q.tokens.size() != 1) return false;
    if (q.tokens[0].min_repeat != 1 || q.tokens[0].max_repeat != 1) return false;
    if (!q.global_function_filters.empty()) return false;

    for (const auto& col : agg.columns) {
        if (col.kind == AggregateBucketData::Column::Kind::FeatsComposite) return false;
        if (col.date_transform != AggregateBucketData::Column::DateTransform::None) return false;
    }

    // ── Anchor constraints (RG-REG-7): only the "simple" subset is supported ─
    struct AnchorFastInfo {
        const AnchorConstraint* ac = nullptr;
        const StructuralAttr* sa = nullptr;
        RegionCursor cursor;
    };
    std::vector<AnchorFastInfo> anchor_infos;
    anchor_infos.reserve(token_anchor_constraints.size());
    std::unordered_map<std::string, size_t> binding_to_anchor_idx;
    for (const auto& ac : token_anchor_constraints) {
        if (ac.region_enumeration) return false;
        if (ac.token_idx != 0) return false;
        if (!ac.anchor_region_clauses.empty()) return false;
        if (!corpus_.has_structure(ac.region)) return false;
        if (corpus_.is_nested(ac.region) || corpus_.is_overlapping(ac.region)
            || corpus_.is_zerowidth(ac.region))
            return false;
        AnchorFastInfo ai;
        ai.ac = &ac;
        ai.sa = &corpus_.structure(ac.region);
        ai.cursor = RegionCursor(*ai.sa);
        if (!ac.binding_name.empty())
            binding_to_anchor_idx[ac.binding_name] = anchor_infos.size();
        anchor_infos.push_back(std::move(ai));
    }

    for (const auto& col : agg.columns) {
        if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding) {
            if (col.named_anchor.empty()) return false;
            auto it = binding_to_anchor_idx.find(col.named_anchor);
            if (it == binding_to_anchor_idx.end()) return false;
            const auto& ai = anchor_infos[it->second];
            if (!resolve_region_attr_key(*ai.sa, ai.ac->region, col.region_attr_name)) return false;
            continue;
        }
        if (col.named_anchor.empty()) continue;
        auto it = name_map.find(col.named_anchor);
        if (it == name_map.end() || it->second != 0) return false;
    }
    for (const auto& rf : resolved_filters) {
        if (!rf.anchor_name.empty()) return false;
    }

    const size_t ncols = agg.columns.size();
    struct ColFastInfo {
        bool is_positional = true;
        const PositionalAttr* pa = nullptr;
        const StructuralAttr* sa = nullptr;
        std::vector<LexiconId> region_to_lex;
        LexiconId lex_size = 0;
        std::string attr_name;
        int anchor_idx = -1;
    };

    std::vector<ColFastInfo> col_info(ncols);
    bool sole_positional_col = (ncols == 1);
    for (size_t i = 0; i < ncols; ++i) {
        const auto& col = agg.columns[i];
        auto& ci = col_info[i];
        if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            ci.is_positional = true;
            ci.pa = col.pa;
        } else if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding) {
            ci.is_positional = false;
            const size_t aidx = binding_to_anchor_idx[col.named_anchor];
            ci.sa = anchor_infos[aidx].sa;
            ci.attr_name = col.region_attr_name;
            ci.region_to_lex = ci.sa->precompute_region_to_lex(col.region_attr_name);
            ci.lex_size = ci.sa->region_attr_lex_size(col.region_attr_name);
            ci.anchor_idx = static_cast<int>(aidx);
            sole_positional_col = false;
            if (ci.region_to_lex.empty()) return false;
        } else {
            ci.is_positional = false;
            ci.sa = col.sa;
            ci.attr_name = col.region_attr_name;
            ci.region_to_lex = col.sa->precompute_region_to_lex(col.region_attr_name);
            ci.lex_size = col.sa->region_attr_lex_size(col.region_attr_name);
            sole_positional_col = false;
            if (ci.region_to_lex.empty()) return false;
        }
    }

    struct FilterCursor {
        const ResolvedRegionFilter* rf = nullptr;
        RegionCursor cursor;
        bool valid = false;
    };
    std::vector<FilterCursor> filter_cursors;
    filter_cursors.reserve(resolved_filters.size());
    for (const auto& rf : resolved_filters) {
        FilterCursor fc;
        fc.rf = &rf;
        if (rf.sa) {
            fc.cursor = RegionCursor(*rf.sa);
            fc.valid = true;
        }
        filter_cursors.push_back(std::move(fc));
    }

    std::vector<RegionCursor> agg_cursors(ncols);
    for (size_t i = 0; i < ncols; ++i) {
        if (!col_info[i].is_positional && col_info[i].anchor_idx < 0)
            agg_cursors[i] = RegionCursor(*col_info[i].sa);
    }

    if (sole_positional_col && filter_cursors.empty() && anchor_infos.empty()) {
        const auto& pa = *col_info[0].pa;
        LexiconId lex_sz = pa.lexicon().size();
        std::vector<size_t> flat(static_cast<size_t>(lex_sz), 0);
        size_t total = 0;
        for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
            if (max_total_cap > 0 && total >= max_total_cap) return false;
            ++flat[static_cast<size_t>(pa.id_at(pos))];
            ++total;
            return true;
        });
        agg.total_hits = total;
        for (LexiconId id = 0; id < lex_sz; ++id) {
            if (flat[static_cast<size_t>(id)] > 0) {
                std::vector<int64_t> key = {static_cast<int64_t>(id)};
                agg.counts[std::move(key)] = flat[static_cast<size_t>(id)];
            }
        }
        result.total_count = total;
        result.total_exact = !(max_total_cap > 0 && total >= max_total_cap);
        return true;
    }

    if (ncols == 1 && !col_info[0].is_positional && col_info[0].anchor_idx < 0
        && filter_cursors.empty() && anchor_infos.empty()) {
        const auto& ci = col_info[0];
        LexiconId lex_sz = ci.lex_size;
        if (lex_sz <= 0) return false;
        std::vector<size_t> flat(static_cast<size_t>(lex_sz), 0);
        size_t total = 0;
        RegionCursor cursor(*ci.sa);
        for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
            if (max_total_cap > 0 && total >= max_total_cap) return false;
            int64_t rgn = cursor.find(pos);
            if (rgn < 0) return true;
            LexiconId lid = ci.region_to_lex[static_cast<size_t>(rgn)];
            if (lid == UNKNOWN_LEX) return true;
            ++flat[static_cast<size_t>(lid)];
            ++total;
            return true;
        });
        agg.total_hits = total;
        agg.region_intern.resize(1);
        auto& ri = agg.region_intern[0];
        for (LexiconId id = 0; id < lex_sz; ++id) {
            if (flat[static_cast<size_t>(id)] > 0) {
                int64_t intern_id = static_cast<int64_t>(ri.id_to_str.size() + 1);
                std::string val(ci.sa->region_attr_lex_get(ci.attr_name, id));
                ri.str_to_id[val] = intern_id;
                ri.id_to_str.push_back(val);
                std::vector<int64_t> key = {intern_id};
                agg.counts[std::move(key)] = flat[static_cast<size_t>(id)];
            }
        }
        result.total_count = total;
        result.total_exact = !(max_total_cap > 0 && total >= max_total_cap);
        return true;
    }

    std::vector<int64_t> key_buf(ncols);
    size_t total = 0;
    bool capped = false;
    std::vector<size_t> anchor_region_rows(anchor_infos.size(), 0);
    auto pass_anchors = [&](CorpusPos pos) -> bool {
        for (size_t ai = 0; ai < anchor_infos.size(); ++ai) {
            auto& info = anchor_infos[ai];
            int64_t rgn = info.cursor.find(pos);
            if (rgn < 0) return false;
            Region reg = info.sa->get(static_cast<size_t>(rgn));
            if (info.ac->is_start) { if (pos != reg.start) return false; }
            else { if (pos != reg.end) return false; }
            for (const auto& [key, val] : info.ac->attrs) {
                auto rk = resolve_region_attr_key(*info.sa, info.ac->region, key);
                if (!rk) return false;
                if (info.sa->region_value(*rk, static_cast<size_t>(rgn)) != val) return false;
            }
            anchor_region_rows[ai] = static_cast<size_t>(rgn);
        }
        return true;
    };
    auto pass_filters = [&](CorpusPos pos) -> bool {
        for (auto& fc : filter_cursors) {
            if (!fc.valid) return false;
            int64_t rgn = fc.cursor.find(pos);
            if (rgn < 0) return false;
            const auto& rf = *fc.rf;
            if (rf.op == CompOp::EQ && rf.has_reverse) {
                if (!rf.sa->region_matches_attr_eq_rev(rf.attr_name,
                        static_cast<size_t>(rgn), rf.value))
                    return false;
                continue;
            }
            std::string_view rval = rf.sa->region_value(rf.attr_name, static_cast<size_t>(rgn));
            std::string rval_s(rval);
            switch (rf.op) {
                case CompOp::EQ:  if (rval_s != rf.value) return false; break;
                case CompOp::NEQ: if (rval_s == rf.value) return false; break;
                case CompOp::LT:  if (!(rval_s < rf.value)) return false; break;
                case CompOp::GT:  if (!(rval_s > rf.value)) return false; break;
                case CompOp::LTE: if (!(rval_s <= rf.value)) return false; break;
                case CompOp::GTE: if (!(rval_s >= rf.value)) return false; break;
                default: return false;
            }
        }
        return true;
    };

    for_each_seed_position(q.tokens[0].conditions, [&](CorpusPos pos) -> bool {
        if (max_total_cap > 0 && total >= max_total_cap) { capped = true; return false; }
        if (!anchor_infos.empty() && !pass_anchors(pos)) return true;
        if (!filter_cursors.empty() && !pass_filters(pos)) return true;
        for (size_t i = 0; i < ncols; ++i) {
            const auto& ci = col_info[i];
            if (ci.is_positional) {
                key_buf[i] = static_cast<int64_t>(ci.pa->id_at(pos));
            } else {
                int64_t rgn;
                if (ci.anchor_idx >= 0) rgn = static_cast<int64_t>(anchor_region_rows[ci.anchor_idx]);
                else {
                    rgn = agg_cursors[i].find(pos);
                    if (rgn < 0) return true;
                }
                LexiconId lid = ci.region_to_lex[static_cast<size_t>(rgn)];
                if (lid == UNKNOWN_LEX) return true;
                key_buf[i] = static_cast<int64_t>(lid);
            }
        }
        ++total;
        ++agg.counts[key_buf];
        return true;
    });

    agg.total_hits = total;
    agg.region_intern.resize(ncols);
    std::vector<std::unordered_map<int64_t, int64_t>> lex_to_intern_maps(ncols);
    bool need_remap = false;
    for (size_t i = 0; i < ncols; ++i) {
        if (col_info[i].is_positional) continue;
        need_remap = true;
        const auto& ci = col_info[i];
        auto& ri = agg.region_intern[i];
        auto& lex_to_intern = lex_to_intern_maps[i];
        for (const auto& [key, cnt] : agg.counts) {
            (void)cnt;
            int64_t lid = key[i];
            if (lex_to_intern.count(lid)) continue;
            int64_t intern_id = static_cast<int64_t>(ri.id_to_str.size() + 1);
            std::string val(ci.sa->region_attr_lex_get(ci.attr_name, static_cast<LexiconId>(lid)));
            ri.str_to_id[val] = intern_id;
            ri.id_to_str.push_back(val);
            lex_to_intern[lid] = intern_id;
        }
    }
    if (need_remap) {
        std::unordered_map<std::vector<int64_t>, size_t,
                           AggregateBucketData::VecHash,
                           AggregateBucketData::VecEq> new_counts;
        for (auto& [key, cnt] : agg.counts) {
            std::vector<int64_t> new_key = key;
            for (size_t i = 0; i < ncols; ++i) {
                if (col_info[i].is_positional) continue;
                new_key[i] = lex_to_intern_maps[i][key[i]];
            }
            new_counts[std::move(new_key)] += cnt;
        }
        agg.counts = std::move(new_counts);
    }

    result.total_count = total;
    result.total_exact = !capped;
    return true;
}
#endif

namespace {

static std::optional<AggregateBucketData::Column::DateTransform> parse_date_transform_prefix(
        const std::string& field, std::string& inner_attr) {
    struct Prefix {
        const char* name;
        AggregateBucketData::Column::DateTransform tx;
    };
    static const Prefix kPrefixes[] = {
        {"year(", AggregateBucketData::Column::DateTransform::Year},
        {"century(", AggregateBucketData::Column::DateTransform::Century},
        {"decade(", AggregateBucketData::Column::DateTransform::Decade},
        {"month(", AggregateBucketData::Column::DateTransform::Month},
        {"week(", AggregateBucketData::Column::DateTransform::Week},
        {"day(", AggregateBucketData::Column::DateTransform::Day},
        {"strlen(", AggregateBucketData::Column::DateTransform::Strlen},
    };
    if (field.size() < 7 || field.back() != ')')
        return std::nullopt;
    for (const auto& p : kPrefixes) {
        const std::string_view pref(p.name);
        if (field.rfind(pref, 0) == 0 && field.size() > pref.size() + 1) {
            inner_attr = field.substr(pref.size(), field.size() - pref.size() - 1);
            if (!inner_attr.empty())
                return p.tx;
        }
    }
    return std::nullopt;
}

static std::string apply_date_transform_bucket(
        std::string_view raw,
        AggregateBucketData::Column::DateTransform transform) {
    if (transform == AggregateBucketData::Column::DateTransform::None)
        return std::string(raw);
    if (transform == AggregateBucketData::Column::DateTransform::Strlen) {
        int64_t cp_count = 0;
        for (unsigned char c : raw)
            if ((c & 0xC0) != 0x80) ++cp_count;
        return std::to_string(cp_count);
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
        return neg ? -y : y;
    };

    auto y = parse_year_prefix(raw);
    if (!y) return "";
    switch (transform) {
        case AggregateBucketData::Column::DateTransform::Year:
            return std::to_string(*y);
        case AggregateBucketData::Column::DateTransform::Century:
            return (*y > 0) ? std::to_string(((*y - 1) / 100) + 1) : "";
        case AggregateBucketData::Column::DateTransform::Decade:
            return (*y > 0) ? std::to_string((*y / 10) * 10) : "";
        case AggregateBucketData::Column::DateTransform::Month:
        case AggregateBucketData::Column::DateTransform::Week:
        case AggregateBucketData::Column::DateTransform::Day:
            return "";
        case AggregateBucketData::Column::DateTransform::Strlen:
            return std::to_string(std::count_if(
                    raw.begin(), raw.end(),
                    [](unsigned char c) { return (c & 0xC0) != 0x80; }));
        case AggregateBucketData::Column::DateTransform::None:
            return std::string(raw);
    }
    return "";
}

bool build_aggregate_plan_impl(const Corpus& corpus, const std::vector<std::string>& fields,
                               AggregateBucketData& out) {
    out.columns.clear();
    out.region_intern.clear();
    out.counts.clear();
    out.total_hits = 0;
    out.columns.reserve(fields.size());
    out.region_intern.resize(fields.size());
    for (const std::string& field : fields) {
        AggregateBucketData::Column col;
        std::string attr_spec = field;
        std::string date_inner;
        if (auto tx = parse_date_transform_prefix(field, date_inner)) {
            col.date_transform = *tx;
            attr_spec = std::move(date_inner);
        }
        if (attr_spec.rfind("match.", 0) == 0 && attr_spec.size() > 6) {
            attr_spec = attr_spec.substr(6);
        } else {
            auto dot = attr_spec.find('.');
            if (dot != std::string::npos && dot > 0) {
                std::string prefix = attr_spec.substr(0, dot);
                std::string rest = attr_spec.substr(dot + 1);
                std::string rattr = rest;
                if (rattr.size() > 5 && rattr.substr(0, 5) == "feats" && rattr.find('.') != std::string::npos)
                    rattr[rattr.find('.')] = '_';
                if (corpus.has_structure(prefix)) {
                    const auto& sa = corpus.structure(prefix);
                    auto rkey = resolve_region_attr_key(sa, prefix, rattr);
                    if (rkey) {
                        col.kind = AggregateBucketData::Column::Kind::Region;
                        col.sa = &sa;
                        col.region_attr_name = *rkey;
                        col.named_anchor = std::move(prefix);
                        out.columns.push_back(std::move(col));
                        continue;
                    }
                }
                col.named_anchor = std::move(prefix);
                attr_spec = std::move(rest);
            }
        }
        std::string attr = normalize_query_attr_name(corpus, attr_spec);
        std::string feat_name;
        if (feats_is_subkey(attr, feat_name) && corpus.has_attr("feats")) {
            if (!corpus_has_ud_split_feats_column(corpus, feat_name)) {
                col.kind = AggregateBucketData::Column::Kind::FeatsComposite;
                col.pa = &corpus.attr("feats");
                col.feats_sub_key = feat_name;
                out.columns.push_back(std::move(col));
                continue;
            }
        }
        if (corpus.has_attr(attr)) {
            if (!col.named_anchor.empty()) {
                bool any_region = false;
                for (const std::string& st : corpus.structure_names()) {
                    if (!corpus.has_structure(st)) continue;
                    const auto& sa = corpus.structure(st);
                    if (resolve_region_attr_key(sa, st, attr)) { any_region = true; break; }
                }
                if (any_region) {
                    col.kind = AggregateBucketData::Column::Kind::RegionFromBinding;
                    col.region_attr_name = attr;
                    col.pa = nullptr;
                    col.sa = nullptr;
                    out.columns.push_back(std::move(col));
                    continue;
                }
            }
            col.kind = AggregateBucketData::Column::Kind::Positional;
            col.pa = &corpus.attr(attr);
            out.columns.push_back(std::move(col));
            continue;
        }
        bool found_reg = false;
        for (const auto& ra_name : corpus.region_attr_names()) {
            if (ra_name != attr_spec) continue;
            auto us = ra_name.find('_');
            if (us == std::string::npos || us + 1 >= ra_name.size()) return false;
            std::string sn = ra_name.substr(0, us);
            std::string ran = ra_name.substr(us + 1);
            if (!corpus.has_structure(sn)) return false;
            const auto& sa = corpus.structure(sn);
            auto rkey = resolve_region_attr_key(sa, sn, ran);
            if (!rkey) return false;
            col.kind = AggregateBucketData::Column::Kind::Region;
            col.sa = &sa;
            col.region_attr_name = *rkey;
            out.columns.push_back(std::move(col));
            found_reg = true;
            break;
        }
        if (!found_reg) return false;
    }
    return true;
}

bool fill_aggregate_key_impl(AggregateBucketData& data, const Corpus& corpus, const Match& m,
                             const NameIndexMap& nm, std::vector<int64_t>& key_out) {
    key_out.resize(data.columns.size());
    for (size_t i = 0; i < data.columns.size(); ++i) {
        const auto& col = data.columns[i];
        auto intern_value = [&](std::string value) {
            auto& st = data.region_intern[i];
            auto it = st.str_to_id.find(value);
            if (it != st.str_to_id.end()) {
                key_out[i] = it->second;
            } else {
                int64_t id = static_cast<int64_t>(st.id_to_str.size() + 1);
                st.str_to_id.emplace(value, id);
                st.id_to_str.push_back(std::move(value));
                key_out[i] = id;
            }
        };
        if (col.kind == AggregateBucketData::Column::Kind::FeatsComposite) {
            CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                     : resolve_name(m, nm, col.named_anchor);
            if (pos == NO_HEAD) return false;
            std::string val = std::string(feats_extract_value(col.pa->value_at(pos), col.feats_sub_key));
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                val = apply_date_transform_bucket(val, col.date_transform);
            intern_value(std::move(val));
        } else if (col.kind == AggregateBucketData::Column::Kind::Positional) {
            CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                     : resolve_name(m, nm, col.named_anchor);
            if (pos == NO_HEAD) return false;
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None) {
                std::string val(col.pa->value_at(pos));
                intern_value(apply_date_transform_bucket(val, col.date_transform));
            } else {
                key_out[i] = static_cast<int64_t>(col.pa->id_at(pos));
            }
        } else if (col.kind == AggregateBucketData::Column::Kind::RegionFromBinding) {
            auto nr = m.named_regions.find(col.named_anchor);
            if (nr == m.named_regions.end()) return false;
            const auto& sa = corpus.structure(nr->second.struct_name);
            std::optional<std::string> val;
            if (auto rkey = resolve_region_attr_key(sa, nr->second.struct_name, col.region_attr_name)) {
                val = std::string(sa.region_value(*rkey, nr->second.region_idx));
            } else {
                val = lookup_super_region_attr_value(
                    corpus, sa, nr->second.struct_name, nr->second.region_idx,
                    col.region_attr_name);
            }
            if (!val) return false;
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                *val = apply_date_transform_bucket(*val, col.date_transform);
            intern_value(std::move(*val));
        } else {
            int64_t rgn = -1;
            if (!col.named_anchor.empty()) {
                auto nr = m.named_regions.find(col.named_anchor);
                if (nr != m.named_regions.end()) {
                    if (&corpus.structure(nr->second.struct_name) != col.sa) return false;
                    rgn = static_cast<int64_t>(nr->second.region_idx);
                }
            }
            if (rgn < 0) {
                CorpusPos pos = col.named_anchor.empty() ? m.first_pos()
                                                         : resolve_name(m, nm, col.named_anchor);
                if (pos == NO_HEAD) return false;
                rgn = col.sa->find_region(pos);
                if (rgn < 0) return false;
            }
            std::string val(col.sa->region_value(col.region_attr_name, static_cast<size_t>(rgn)));
            if (col.date_transform != AggregateBucketData::Column::DateTransform::None)
                val = apply_date_transform_bucket(val, col.date_transform);
            intern_value(std::move(val));
        }
    }
    return true;
}

} // namespace

bool build_aggregate_plan(const Corpus& corpus, const std::vector<std::string>& fields,
                          AggregateBucketData& out) {
    return build_aggregate_plan_impl(corpus, fields, out);
}

bool fill_aggregate_key(AggregateBucketData& data, const Corpus& corpus, const Match& m,
                        const NameIndexMap& nm, std::vector<int64_t>& key_out) {
    return fill_aggregate_key_impl(data, corpus, m, nm, key_out);
}

bool compute_stats_rows(const Corpus& corpus, const MatchSet& ms, const NameIndexMap& name_map,
                        const std::vector<std::string>& by_fields,
                        const std::vector<StatsMetricSpec>& metrics,
                        std::vector<StatsRowResult>& out_rows) {
    struct MetricAcc {
        double sum = 0.0;
        size_t n_valid = 0;
        std::vector<double> values;
    };
    struct RowAcc {
        size_t n_total = 0;
        std::vector<MetricAcc> metrics;
    };
    auto utf8_cp_len = [](std::string_view raw) -> size_t {
        size_t cp_count = 0;
        for (unsigned char c : raw)
            if ((c & 0xC0) != 0x80) ++cp_count;
        return cp_count;
    };

    auto parse_number = [](const std::string& text, double& out) -> bool {
        if (text.empty()) return false;
        errno = 0;
        char* end = nullptr;
        const double val = std::strtod(text.c_str(), &end);
        if (end == text.c_str()) return false;
        while (*end != '\0' && std::isspace(static_cast<unsigned char>(*end))) ++end;
        if (*end != '\0' || errno == ERANGE) return false;
        out = val;
        return true;
    };
    auto eval_numeric_expr = [&](const Match& m, const std::string& expr, double& out) -> bool {
        if (expr.size() > 9 && expr.rfind("strlen(", 0) == 0 && expr.back() == ')') {
            const std::string inner = expr.substr(7, expr.size() - 8);
            std::string raw;
            try {
                raw = read_tabulate_field(corpus, m, name_map, inner);
            } catch (...) {
                return false;
            }
            out = static_cast<double>(utf8_cp_len(raw));
            return true;
        }
        std::string rendered;
        try {
            rendered = read_tabulate_field(corpus, m, name_map, expr);
        } catch (...) {
            return false;
        }
        return parse_number(rendered, out);
    };

    // Fast path: by single region attr + metrics only on strlen(positional_attr)
    // (no token-label anchors). This avoids read_tabulate_field/string key work per match.
    struct StrlenMetricPlan {
        StatsMetricSpec::Kind kind = StatsMetricSpec::Kind::Avg;
        const PositionalAttr* pa = nullptr;
    };
    auto try_fast_strlen_grouped = [&]() -> bool {
        if (by_fields.size() != 1 || metrics.empty()) return false;
        std::string by = by_fields[0];
        if (by.rfind("match.", 0) == 0 && by.size() > 6) by = by.substr(6);
        auto us = by.find('_');
        if (us == std::string::npos || us == 0 || us + 1 >= by.size()) return false;
        const std::string struct_name = by.substr(0, us);
        const std::string attr_name = by.substr(us + 1);
        if (!corpus.has_structure(struct_name)) return false;
        const auto& sa = corpus.structure(struct_name);
        auto rkey = resolve_region_attr_key(sa, struct_name, attr_name);
        if (!rkey) return false;

        std::vector<StrlenMetricPlan> plans;
        plans.reserve(metrics.size());
        for (const auto& m : metrics) {
            const std::string& expr = m.expr;
            if (!(expr.size() > 9 && expr.rfind("strlen(", 0) == 0 && expr.back() == ')'))
                return false;
            std::string inner = expr.substr(7, expr.size() - 8);
            auto dot = inner.find('.');
            if (dot != std::string::npos) {
                std::string prefix = inner.substr(0, dot);
                if (prefix == "match") inner = inner.substr(dot + 1);
                else return false;  // named-token anchored: fall back
            }
            std::string attr = normalize_query_attr_name(corpus, inner);
            if (!corpus.has_attr(attr)) return false;
            plans.push_back(StrlenMetricPlan{m.kind, &corpus.attr(attr)});
        }

        std::unordered_map<size_t, RowAcc> grouped_by_region;
        grouped_by_region.reserve(1024);
        for (const auto& m : ms.matches) {
            CorpusPos pos = m.first_pos();
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0) continue;
            auto& row = grouped_by_region[static_cast<size_t>(rgn)];
            ++row.n_total;
            if (row.metrics.empty()) row.metrics.resize(metrics.size());
            for (size_t i = 0; i < plans.size(); ++i) {
                const auto* pa = plans[i].pa;
                double num = static_cast<double>(utf8_cp_len(pa->value_at(pos)));
                auto& acc = row.metrics[i];
                acc.sum += num;
                ++acc.n_valid;
                if (plans[i].kind == StatsMetricSpec::Kind::Median)
                    acc.values.push_back(num);
            }
        }

        std::vector<std::pair<std::string, RowAcc>> materialized;
        materialized.reserve(grouped_by_region.size());
        for (auto& [ridx, row] : grouped_by_region) {
            materialized.emplace_back(std::string(sa.region_value(*rkey, ridx)), std::move(row));
        }
        std::sort(materialized.begin(), materialized.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        out_rows.clear();
        out_rows.reserve(materialized.size());
        for (auto& [key, row] : materialized) {
            StatsRowResult out;
            out.key = std::move(key);
            out.n_total = row.n_total;
            out.metrics.resize(metrics.size());
            for (size_t i = 0; i < metrics.size(); ++i) {
                auto& dst = out.metrics[i];
                auto& src = row.metrics[i];
                dst.n_valid = src.n_valid;
                if (src.n_valid == 0) continue;
                dst.has_value = true;
                if (metrics[i].kind == StatsMetricSpec::Kind::Avg) {
                    dst.value = src.sum / static_cast<double>(src.n_valid);
                } else {
                    std::sort(src.values.begin(), src.values.end());
                    const size_t n = src.values.size();
                    dst.value = (n % 2 == 1)
                                    ? src.values[n / 2]
                                    : (src.values[n / 2 - 1] + src.values[n / 2]) / 2.0;
                }
            }
            out_rows.push_back(std::move(out));
        }
        return true;
    };
    if (try_fast_strlen_grouped())
        return true;

    auto build_group_key = [&](const Match& m) {
        std::string key;
        for (size_t i = 0; i < by_fields.size(); ++i) {
            if (i) key.push_back('\t');
            key += read_tabulate_field(corpus, m, name_map, by_fields[i]);
        }
        return key;
    };

    std::map<std::string, RowAcc> grouped;
    for (const auto& m : ms.matches) {
        std::string key;
        if (!by_fields.empty())
            key = build_group_key(m);
        auto& row = grouped[key];
        ++row.n_total;
        if (row.metrics.empty()) row.metrics.resize(metrics.size());
        for (size_t i = 0; i < metrics.size(); ++i) {
            double num = 0.0;
            if (!eval_numeric_expr(m, metrics[i].expr, num)) continue;
            auto& acc = row.metrics[i];
            acc.sum += num;
            ++acc.n_valid;
            if (metrics[i].kind == StatsMetricSpec::Kind::Median)
                acc.values.push_back(num);
        }
    }

    out_rows.clear();
    out_rows.reserve(grouped.size());
    for (auto& [key, row] : grouped) {
        StatsRowResult out;
        out.key = key;
        out.n_total = row.n_total;
        out.metrics.resize(metrics.size());
        for (size_t i = 0; i < metrics.size(); ++i) {
            auto& dst = out.metrics[i];
            auto& src = row.metrics[i];
            dst.n_valid = src.n_valid;
            if (src.n_valid == 0) continue;
            if (metrics[i].kind == StatsMetricSpec::Kind::Avg) {
                dst.has_value = true;
                dst.value = src.sum / static_cast<double>(src.n_valid);
            } else {
                std::sort(src.values.begin(), src.values.end());
                const size_t n = src.values.size();
                dst.has_value = true;
                if (n % 2 == 1) dst.value = src.values[n / 2];
                else dst.value = (src.values[n / 2 - 1] + src.values[n / 2]) / 2.0;
            }
        }
        out_rows.push_back(std::move(out));
    }
    return true;
}

#if 0
bool QueryExecutor::match_survives_post_filters_for_aggregate(
        const TokenQuery& query,
        const NameIndexMap& name_map,
        const std::vector<AnchorConstraint>& anchor_constraints,
        MatchSet& scratch,
        const Match& m) const {
    scratch.matches.clear();
    scratch.matches.push_back(m);
    scratch.total_count = 1;
    apply_anchor_filters(anchor_constraints, scratch);
    if (scratch.matches.empty()) return false;
    apply_within_having(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_not_within(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_containing(query, scratch);
    if (scratch.matches.empty()) return false;
    apply_position_orders(query, name_map, scratch);
    if (scratch.matches.empty()) return false;
    apply_global_filters(query, name_map, scratch);
    return !scratch.matches.empty();
}
#endif

} // namespace pando
