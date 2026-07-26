// Full-program session API: run_program_json()
// Runs a complete CQL program (queries + commands) and returns JSON output.
// Lives in the pando_api library so both pando CLI and pando-server can use it.

#include "api/query_json.h"
#include "core/json_utils.h"
#include "core/count_hierarchy_json.h"
#include "query/parser.h"
#include "query/executor.h"
#include "index/positional_attr.h"
#include "index/dependency_index.h"

#include <iostream>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <cmath>
#include <iomanip>
#include <stdexcept>

namespace pando {

// ── Session impl ────────────────────────────────────────────────────────

struct ProgramSession::Impl {
    std::map<std::string, MatchSet> named_results;
    std::map<std::string, NameIndexMap> named_name_maps;
    std::map<std::string, NameIndexMap> named_target_name_maps;
    MatchSet last_ms;
    NameIndexMap last_name_map;
    NameIndexMap last_target_name_map;
    bool has_last = false;
};

ProgramSession::ProgramSession() : impl_(std::make_unique<Impl>()) {}
ProgramSession::~ProgramSession() = default;
ProgramSession::ProgramSession(ProgramSession&&) noexcept = default;
ProgramSession& ProgramSession::operator=(ProgramSession&&) noexcept = default;

// ── Helpers ──────────────────────────────────────────────────────────────
// build_name_map is already inline in executor.h — just use it directly.

static std::string make_key(const Corpus& corpus, const Match& m,
                            const NameIndexMap& name_map,
                            const std::vector<std::string>& fields) {
    std::string key;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) key += '\t';
        key += read_tabulate_field(corpus, m, name_map, fields[i]);
    }
    return key;
}

static bool parse_i64_strict(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    size_t i = 0;
    bool neg = false;
    if (s[i] == '+' || s[i] == '-') {
        neg = (s[i] == '-');
        ++i;
    }
    if (i >= s.size()) return false;
    int64_t v = 0;
    for (; i < s.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (!std::isdigit(c)) return false;
        int d = c - '0';
        if (v > (INT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    out = neg ? -v : v;
    return true;
}

static bool parse_roman_numeral(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    auto val = [](char c) -> int {
        switch (c) {
            case 'I': return 1; case 'V': return 5; case 'X': return 10; case 'L': return 50;
            case 'C': return 100; case 'D': return 500; case 'M': return 1000;
            default: return 0;
        }
    };
    int64_t total = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = static_cast<char>(std::toupper(static_cast<unsigned char>(s[i])));
        int cur = val(c);
        if (cur == 0) return false;
        int next = 0;
        if (i + 1 < s.size()) {
            char n = static_cast<char>(std::toupper(static_cast<unsigned char>(s[i + 1])));
            next = val(n);
            if (next == 0) return false;
        }
        if (next > cur) total -= cur;
        else total += cur;
    }
    out = total;
    return total > 0;
}

static bool compare_sort_values(std::string_view a, std::string_view b) {
    int64_t ai = 0, bi = 0;
    bool an = parse_i64_strict(a, ai) || parse_roman_numeral(a, ai);
    bool bn = parse_i64_strict(b, bi) || parse_roman_numeral(b, bi);
    if (an && bn && ai != bi) return ai < bi;
    return a < b;
}

static bool compare_group_keys(std::string_view a, std::string_view b) {
    size_t sa = 0, sb = 0;
    while (true) {
        size_t ea = a.find('\t', sa);
        size_t eb = b.find('\t', sb);
        if (ea == std::string_view::npos) ea = a.size();
        if (eb == std::string_view::npos) eb = b.size();
        std::string_view pa = a.substr(sa, ea - sa);
        std::string_view pb = b.substr(sb, eb - sb);
        if (pa != pb) return compare_sort_values(pa, pb);
        if (ea == a.size() && eb == b.size()) return false;
        sa = (ea < a.size()) ? ea + 1 : a.size();
        sb = (eb < b.size()) ? eb + 1 : b.size();
    }
}

static std::unordered_set<LexiconId> build_stoplist_ids(const PositionalAttr& pa, size_t top_n) {
    std::unordered_set<LexiconId> out;
    if (top_n == 0) return out;
    std::vector<std::pair<LexiconId, size_t>> items;
    const auto& lex = pa.lexicon();
    items.reserve(lex.size());
    for (LexiconId id = 0; id < lex.size(); ++id) {
        size_t c = pa.count_of_id(id);
        if (c == 0) continue;
        items.push_back({id, c});
    }
    std::sort(items.begin(), items.end(), [&](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return std::string(lex.get(a.first)) < std::string(lex.get(b.first));
    });
    size_t keep = std::min(top_n, items.size());
    for (size_t i = 0; i < keep; ++i) out.insert(items[i].first);
    return out;
}

enum class FreqDateTransform { None, Year, Century, Decade, Month, Week, Day };

struct FreqSubcorpusSpec {
    const StructuralAttr* sa = nullptr;
    struct FieldSpec {
        std::string region_attr;
        FreqDateTransform transform = FreqDateTransform::None;
    };
    std::vector<FieldSpec> fields;
};

static std::optional<std::string> apply_date_bucket(std::string_view raw, FreqDateTransform t) {
    if (t == FreqDateTransform::None) return std::string(raw);
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
        static const int tt[12] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        int64_t yy = y;
        if (m < 3) --yy;
        int w = static_cast<int>((yy + yy/4 - yy/100 + yy/400 + tt[m - 1] + d) % 7);
        if (w < 0) w += 7;
        return w == 0 ? 7 : w;
    };
    auto iso_weeks_in_year = [&](int64_t y) {
        int jan1 = weekday_iso(y, 1, 1);
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
    auto p = parse_date_parts_prefix(raw);
    if (!p) return std::nullopt;
    switch (t) {
        case FreqDateTransform::Year:
            return std::to_string(p->year);
        case FreqDateTransform::Century:
            if (p->year <= 0) return std::nullopt;
            return std::to_string(((p->year - 1) / 100) + 1);
        case FreqDateTransform::Decade:
            return std::to_string((p->year / 10) * 10);
        case FreqDateTransform::Month:
            if (!p->has_month) return std::nullopt;
            return std::to_string(p->month);
        case FreqDateTransform::Week:
            if (!p->has_day) return std::nullopt;
            return std::to_string(iso_week_number(p->year, p->month, p->day));
        case FreqDateTransform::Day:
            if (!p->has_day) return std::nullopt;
            return std::to_string(p->day);
        case FreqDateTransform::None:
            break;
    }
    return std::nullopt;
}

static std::optional<FreqSubcorpusSpec> resolve_freq_subcorpus_spec(
        const Corpus& corpus, const std::vector<std::string>& fields) {
    if (fields.empty()) return std::nullopt;
    auto resolve_region = [&](const std::string& attr_spec, FreqDateTransform tr,
                              const StructuralAttr** expected_sa)
            -> std::optional<FreqSubcorpusSpec::FieldSpec> {
        RegionAttrParts parts;
        if (!split_region_attr_name(attr_spec, parts) || !corpus.has_structure(parts.struct_name))
            return std::nullopt;
        const auto& sa = corpus.structure(parts.struct_name);
        if (*expected_sa && *expected_sa != &sa) return std::nullopt;
        *expected_sa = &sa;
        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        if (!rkey) return std::nullopt;
        FreqSubcorpusSpec::FieldSpec fs;
        fs.region_attr = *rkey;
        fs.transform = tr;
        return fs;
    };
    auto parse_wrapped = [&](const std::string& f, const char* name)
            -> std::optional<std::string> {
        const size_t n = std::strlen(name);
        if (f.size() <= n + 2 || f.compare(0, n, name) != 0 || f[n] != '(' || f.back() != ')')
            return std::nullopt;
        std::string inner = f.substr(n + 1, f.size() - n - 2);
        if (inner.rfind("match.", 0) == 0 && inner.size() > 6)
            inner = inner.substr(6);
        return inner;
    };
    auto parse_field = [&](const std::string& f, const StructuralAttr** expected_sa)
            -> std::optional<FreqSubcorpusSpec::FieldSpec> {
        if (auto base = resolve_region(f, FreqDateTransform::None, expected_sa)) return base;
        if (auto inner = parse_wrapped(f, "year"))
            return resolve_region(*inner, FreqDateTransform::Year, expected_sa);
        if (auto inner = parse_wrapped(f, "century"))
            return resolve_region(*inner, FreqDateTransform::Century, expected_sa);
        if (auto inner = parse_wrapped(f, "decade"))
            return resolve_region(*inner, FreqDateTransform::Decade, expected_sa);
        if (auto inner = parse_wrapped(f, "month"))
            return resolve_region(*inner, FreqDateTransform::Month, expected_sa);
        if (auto inner = parse_wrapped(f, "week"))
            return resolve_region(*inner, FreqDateTransform::Week, expected_sa);
        if (auto inner = parse_wrapped(f, "day"))
            return resolve_region(*inner, FreqDateTransform::Day, expected_sa);
        return std::nullopt;
    };

    const StructuralAttr* common_sa = nullptr;
    FreqSubcorpusSpec spec;
    for (const auto& f : fields) {
        auto fs = parse_field(f, &common_sa);
        if (!fs) return std::nullopt;
        spec.fields.push_back(std::move(*fs));
    }
    spec.sa = common_sa;
    if (!spec.sa || spec.fields.empty()) return std::nullopt;
    return spec;
}

static std::unordered_map<std::string, double> build_freq_subcorpus_sizes(
        const FreqSubcorpusSpec& spec,
        const std::vector<std::string>& keys,
        double corpus_size) {
    std::unordered_map<std::string, double> out;
    if (!spec.sa) return out;

    if (spec.fields.size() == 1 && spec.fields[0].transform == FreqDateTransform::None) {
        const auto& f0 = spec.fields[0];
        for (const auto& key : keys) {
            size_t span = spec.sa->token_span_sum_for_attr_eq(f0.region_attr, key);
            out[key] = (span > 0 && span != SIZE_MAX) ? static_cast<double>(span) : corpus_size;
        }
        return out;
    }

    std::unordered_map<std::string, size_t> span_by_key;
    for (size_t ri = 0; ri < spec.sa->region_count(); ++ri) {
        std::string key;
        bool ok = true;
        for (size_t i = 0; i < spec.fields.size(); ++i) {
            const auto& fs = spec.fields[i];
            auto bucket = apply_date_bucket(spec.sa->region_value(fs.region_attr, ri), fs.transform);
            if (!bucket) { ok = false; break; }
            if (i) key.push_back('\t');
            key += *bucket;
        }
        if (!ok) continue;
        Region rg = spec.sa->get(ri);
        if (rg.end < rg.start) continue;
        span_by_key[key] += static_cast<size_t>(rg.end - rg.start + 1);
    }
    for (const auto& key : keys) {
        auto it = span_by_key.find(key);
        size_t span = (it != span_by_key.end()) ? it->second : 0;
        out[key] = (span > 0 && span != SIZE_MAX) ? static_cast<double>(span) : corpus_size;
    }
    return out;
}

static bool aggregate_command_targets_stmt(const Statement& stmt, const GroupCommand& ncmd) {
    if (ncmd.query_name.empty())
        return true;
    if (!stmt.name.empty())
        return ncmd.query_name == stmt.name;
    return ncmd.query_name == "Last";
}

// ── Association measures ────────────────────────────────────────────────

struct CollEntry {
    LexiconId id;
    std::string form;
    size_t obs;
    size_t f_coll;
    size_t f_node;
    size_t N;
};

static double compute_logdice(const CollEntry& e) {
    if (e.f_node == 0 && e.f_coll == 0) return 0;
    double d = 2.0 * static_cast<double>(e.obs) / (static_cast<double>(e.f_node) + static_cast<double>(e.f_coll));
    if (d <= 0) return 0;
    return 14.0 + log2(d);
}

static double compute_mi(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    if (expected == 0) return 0;
    return log2(static_cast<double>(e.obs) / expected);
}

static double compute_mi3(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    if (expected == 0) return 0;
    return log2(static_cast<double>(e.obs) * static_cast<double>(e.obs) * static_cast<double>(e.obs) / expected);
}

static double compute_tscore(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    return (static_cast<double>(e.obs) - expected) / sqrt(static_cast<double>(e.obs));
}

static double compute_ll(const CollEntry& e) {
    double a = static_cast<double>(e.obs);
    double b = static_cast<double>(e.f_coll) - a;
    double c = static_cast<double>(e.f_node) - a;
    double d = static_cast<double>(e.N) - a - b - c;
    if (b < 0) b = 0; if (c < 0) c = 0; if (d < 0) d = 0;
    double n = static_cast<double>(e.N);
    auto xlogx = [](double x, double total) -> double {
        if (x <= 0 || total <= 0) return 0;
        return x * log(x / total);
    };
    double row1 = a + b, row2 = c + d;
    double col1 = a + c, col2 = b + d;
    return 2.0 * (xlogx(a, row1 * col1 / n) + xlogx(b, row1 * col2 / n) +
                  xlogx(c, row2 * col1 / n) + xlogx(d, row2 * col2 / n));
}

static double compute_measure(const std::string& name, const CollEntry& e) {
    if (name == "mi")       return compute_mi(e);
    if (name == "mi3")      return compute_mi3(e);
    if (name == "t" || name == "tscore") return compute_tscore(e);
    if (name == "logdice")  return compute_logdice(e);
    if (name == "ll")       return compute_ll(e);
    if (name == "dice") {
        if (e.f_node == 0 && e.f_coll == 0) return 0;
        return 2.0 * static_cast<double>(e.obs) / (static_cast<double>(e.f_node) + static_cast<double>(e.f_coll));
    }
    return compute_logdice(e);
}

// ── JSON emitters (write to ostream, always JSON) ───────────────────────

static void emit_count_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map, size_t group_limit) {
    if (cmd.fields.empty() && cmd.query_name.empty()) {
        out << "{\"ok\": false, \"error\": \"count/group requires 'by' clause\"}\n";
        return;
    }
    try {
    std::map<std::string, size_t> counts;
    if (ms.aggregate_buckets) {
        for (const auto& [k, c] : ms.aggregate_buckets->counts)
            counts[decode_aggregate_bucket_key(*ms.aggregate_buckets, k)] += c;
    } else {
        for (const auto& m : ms.matches) ++counts[make_key(corpus, m, name_map, cmd.fields)];
    }
    // RG-5f: Explode multivalue keys for single-column grouping.
    if (cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0])) {
        std::map<std::string, size_t> exploded;
        for (const auto& [key, count] : counts) {
            if (key.find('|') != std::string::npos) {
                size_t s = 0;
                while (s < key.size()) {
                    size_t p = key.find('|', s);
                    if (p == std::string::npos) p = key.size();
                    std::string comp = key.substr(s, p - s);
                    if (!comp.empty()) exploded[comp] += count;
                    s = p + 1;
                }
            } else {
                exploded[key] += count;
            }
        }
        counts = std::move(exploded);
    }
    std::vector<std::pair<std::string, size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return compare_group_keys(a.first, b.first);
    });
    size_t total = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
    size_t g_end = (group_limit > 0 && group_limit < sorted.size()) ? group_limit : sorted.size();

    out << "{\"ok\": true, \"operation\": \"count\", \"last_command\": \"count\", \"result\": {\n";
    out << "  \"total_matches\": " << total << ",\n";
    out << "  \"groups\": " << sorted.size() << ",\n";
    out << "  \"groups_returned\": " << g_end << ",\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.fields[i]); }
    out << "],\n";
    if (cmd.fields.size() >= 2) {
        emit_count_result_hierarchy_json(out, cmd.fields, counts, total, group_limit);
        out << "\n}}\n";
    } else {
        out << "  \"rows\": [\n";
        for (size_t i = 0; i < g_end; ++i) {
            if (i > 0) out << ",\n";
            double pct = 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total);
            out << "    {\"key\": " << jstr(sorted[i].first) << ", \"count\": " << sorted[i].second
                << ", \"pct\": " << pct << "}";
        }
        out << "\n  ]\n}}\n";
    }
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_stats_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map) {
    if (cmd.stats_metrics.empty()) {
        out << "{\"ok\": false, \"error\": \"stats requires at least one metric\"}\n";
        return;
    }
    std::vector<StatsMetricSpec> metrics;
    metrics.reserve(cmd.stats_metrics.size());
    for (const auto& sm : cmd.stats_metrics) {
        StatsMetricSpec spec;
        spec.kind = (sm.kind == GroupCommand::StatMetric::Kind::AVG)
                        ? StatsMetricSpec::Kind::Avg
                        : StatsMetricSpec::Kind::Median;
        spec.expr = sm.expr;
        metrics.push_back(std::move(spec));
    }
    std::vector<StatsRowResult> rows;
    if (!compute_stats_rows(corpus, ms, name_map, cmd.fields, metrics, rows)) {
        out << "{\"ok\": false, \"error\": \"failed to compute stats\"}\n";
        return;
    }
    auto metric_name = [](const GroupCommand::StatMetric& sm) {
        return std::string(sm.kind == GroupCommand::StatMetric::Kind::AVG ? "avg(" : "median(")
             + sm.expr + ")";
    };
    out << "{\"ok\": true, \"operation\": \"stats\", \"last_command\": \"stats\", \"result\": {\n";
    out << "  \"by\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(cmd.fields[i]);
    }
    out << "],\n  \"metrics\": [";
    for (size_t i = 0; i < cmd.stats_metrics.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(metric_name(cmd.stats_metrics[i]));
    }
    out << "],\n  \"rows\": [\n";
    for (size_t ri = 0; ri < rows.size(); ++ri) {
        if (ri > 0) out << ",\n";
        const auto& row = rows[ri];
        out << "    {\"key\": " << jstr(row.key)
            << ", \"n_total\": " << row.n_total
            << ", \"values\": [";
        for (size_t mi = 0; mi < row.metrics.size(); ++mi) {
            if (mi > 0) out << ", ";
            const auto& mr = row.metrics[mi];
            out << "{\"n_valid\": " << mr.n_valid << ", \"value\": ";
            if (mr.has_value) out << mr.value;
            else out << "null";
            out << "}";
        }
        out << "]}";
    }
    out << "\n  ]\n}}\n";
}

static void freq_build_counts(const Corpus& corpus, const MatchSet& ms,
                              const GroupCommand& cmd, const ProgramOptions& opts,
                              const NameIndexMap& name_map,
                              std::map<std::string, size_t>& counts,
                              size_t& total_matches) {
    counts.clear();
    if (ms.aggregate_buckets) {
        for (const auto& [k, c] : ms.aggregate_buckets->counts)
            counts[decode_aggregate_bucket_key(*ms.aggregate_buckets, k)] += c;
    } else {
        for (const auto& m : ms.matches) ++counts[make_key(corpus, m, name_map, cmd.fields)];
    }
    if (cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0])) {
        std::map<std::string, size_t> exploded;
        for (const auto& [key, count] : counts) {
            if (key.find('|') != std::string::npos) {
                size_t s = 0;
                while (s < key.size()) {
                    size_t p = key.find('|', s);
                    if (p == std::string::npos) p = key.size();
                    std::string comp = key.substr(s, p - s);
                    if (!comp.empty()) exploded[comp] += count;
                    s = p + 1;
                }
            } else {
                exploded[key] += count;
            }
        }
        counts = std::move(exploded);
    }
    total_matches = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
}

static bool session_lookup_ms(ProgramSession::Impl& session, const std::string& qn,
                              MatchSet*& ms, NameIndexMap*& nm) {
    if (qn == "Last") {
        if (!session.has_last) return false;
        ms = &session.last_ms;
        nm = &session.last_name_map;
        return true;
    }
    auto it = session.named_results.find(qn);
    if (it == session.named_results.end()) return false;
    ms = &it->second;
    auto nm_it = session.named_name_maps.find(qn);
    nm = (nm_it != session.named_name_maps.end()) ? &nm_it->second : &session.last_name_map;
    return true;
}

static void emit_freq_compare_json(std::ostream& out, const Corpus& corpus, ProgramSession& ps,
                                   const GroupCommand& cmd, const ProgramOptions& opts) {
    if (cmd.fields.empty()) {
        out << "{\"ok\": false, \"error\": \"freq requires 'by' clause\"}\n";
        return;
    }
    auto& session = *ps.impl_;
    struct Src {
        std::string label;
        MatchSet* ms;
        NameIndexMap* nm;
    };
    std::vector<Src> srcs;
    srcs.reserve(cmd.freq_query_names.size());
    for (const std::string& qn : cmd.freq_query_names) {
        MatchSet* ms = nullptr;
        NameIndexMap* nm = nullptr;
        if (!session_lookup_ms(session, qn, ms, nm)) {
            out << "{\"ok\": false, \"error\": " << jstr("Unknown named query: " + qn) << "}\n";
            return;
        }
        srcs.push_back({qn, ms, nm});
    }

    std::vector<std::map<std::string, size_t>> counts_per(srcs.size());
    std::vector<size_t> totals(srcs.size());
    for (size_t i = 0; i < srcs.size(); ++i)
        freq_build_counts(corpus, *srcs[i].ms, cmd, opts, *srcs[i].nm, counts_per[i], totals[i]);

    std::set<std::string> all_keys;
    for (const auto& m : counts_per)
        for (const auto& [k, c] : m)
            all_keys.insert(k);
    std::vector<std::string> sorted_keys(all_keys.begin(), all_keys.end());
    std::sort(sorted_keys.begin(), sorted_keys.end(),
              [&](const std::string& a, const std::string& b) {
                  size_t sa = 0, sb = 0;
                  for (const auto& m : counts_per) {
                      auto ia = m.find(a), ib = m.find(b);
                      if (ia != m.end()) sa += ia->second;
                      if (ib != m.end()) sb += ib->second;
                  }
                  if (sa != sb) return sa > sb;
                  return compare_group_keys(a, b);
              });

    double corpus_size = static_cast<double>(corpus.size());
    auto freq_spec = resolve_freq_subcorpus_spec(corpus, cmd.fields);
    const bool use_subcorpus_ipm = freq_spec.has_value();
    std::unordered_map<std::string, double> subcorpus_sizes =
            use_subcorpus_ipm
                ? build_freq_subcorpus_sizes(*freq_spec, sorted_keys, corpus_size)
                : std::unordered_map<std::string, double>{};
    auto ipm_denom = [&](const std::string& key) -> double {
        if (use_subcorpus_ipm) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    out << "{\"ok\": true, \"operation\": \"freq\", \"last_command\": \"freq\", \"result\": {\n";
    out << "  \"compare_queries\": [";
    for (size_t i = 0; i < srcs.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(srcs[i].label);
    }
    out << "],\n  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"freq_mode\": \"compare\",\n";
    out << "  \"per_subcorpus_ipm\": " << (use_subcorpus_ipm ? "true" : "false") << ",\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(cmd.fields[i]);
    }
    out << "],\n  \"totals_per_query\": {";
    for (size_t i = 0; i < srcs.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(srcs[i].label) << ": " << totals[i];
    }
    out << "},\n  \"rows\": [\n";
    for (size_t ri = 0; ri < sorted_keys.size(); ++ri) {
        const std::string& key = sorted_keys[ri];
        if (ri > 0) out << ",\n";
        double denom = ipm_denom(key);
        out << "    {\"key\": " << jstr(key);
        if (use_subcorpus_ipm) out << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
        out << ", \"queries\": {";
        for (size_t qi = 0; qi < srcs.size(); ++qi) {
            if (qi > 0) out << ", ";
            size_t c = 0;
            auto it = counts_per[qi].find(key);
            if (it != counts_per[qi].end()) c = it->second;
            double pct = totals[qi] > 0 ? 100.0 * static_cast<double>(c) / static_cast<double>(totals[qi]) : 0.0;
            double ipm = 1e6 * static_cast<double>(c) / denom;
            double q_ipm = totals[qi] > 0
                ? 1e6 * static_cast<double>(c) / static_cast<double>(totals[qi])
                : 0.0;
            out << jstr(srcs[qi].label) << ": {\"count\": " << c
                      << ", \"pct\": " << pct
                      << ", \"ipm\": " << std::fixed << std::setprecision(2) << ipm
                      << ", \"q_ipm\": " << q_ipm;
            if (use_subcorpus_ipm) {
                double rf_pct = denom > 0 ? 100.0 * static_cast<double>(c) / denom : 0.0;
                out << ", \"rf_pct\": " << std::setprecision(4) << rf_pct;
            }
            out << "}";
        }
        out << "}}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_freq_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                           const GroupCommand& cmd, const ProgramOptions& opts, const NameIndexMap& name_map,
                           const std::string& source_query_name) {
    if (cmd.fields.empty() && cmd.query_name.empty()) {
        out << "{\"ok\": false, \"error\": \"freq requires 'by' clause\"}\n";
        return;
    }
    try {
    std::map<std::string, size_t> counts;
    size_t total_matches = 0;
    freq_build_counts(corpus, ms, cmd, opts, name_map, counts, total_matches);
    std::vector<std::pair<std::string, size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return compare_group_keys(a.first, b.first);
    });
    double corpus_size = static_cast<double>(corpus.size());

    // Per-subcorpus IPM: when grouping by a single region attribute, use per-group
    // token counts as IPM denominator for meaningful relative frequencies.
    auto freq_spec = resolve_freq_subcorpus_spec(corpus, cmd.fields);
    const bool use_subcorpus_ipm = freq_spec.has_value();
    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(sorted.size());
    for (const auto& kv : sorted) sorted_keys.push_back(kv.first);
    std::unordered_map<std::string, double> subcorpus_sizes =
            use_subcorpus_ipm
                ? build_freq_subcorpus_sizes(*freq_spec, sorted_keys, corpus_size)
                : std::unordered_map<std::string, double>{};

    auto ipm_denom = [&](const std::string& key) -> double {
        if (use_subcorpus_ipm) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    out << "{\"ok\": true, \"operation\": \"freq\", \"last_command\": \"freq\", \"result\": {\n";
    out << "  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"total_matches\": " << total_matches << ",\n";
    out << "  \"freq_mode\": \"single\",\n";
    out << "  \"source_query\": " << jstr(source_query_name) << ",\n";
    out << "  \"per_subcorpus_ipm\": " << (use_subcorpus_ipm ? "true" : "false") << ",\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.fields[i]); }
    out << "],\n  \"rows\": [\n";
    for (size_t i = 0; i < sorted.size(); ++i) {
        if (i > 0) out << ",\n";
        double denom = ipm_denom(sorted[i].first);
        double ipm = 1e6 * static_cast<double>(sorted[i].second) / denom;
        double pct = total_matches > 0
            ? 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total_matches)
            : 0.0;
        out << "    {\"key\": " << jstr(sorted[i].first) << ", \"count\": " << sorted[i].second
            << ", \"pct\": " << pct
            << ", \"ipm\": " << std::fixed << std::setprecision(2) << ipm;
        if (use_subcorpus_ipm) {
            double rf_pct = denom > 0
                ? 100.0 * static_cast<double>(sorted[i].second) / denom
                : 0.0;
            out << ", \"rf_pct\": " << std::setprecision(4) << rf_pct
                << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
        }
        out << "}";
    }
    out << "\n  ]\n}}\n";
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_size_json(std::ostream& out, const MatchSet& ms) {
    size_t n = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
    out << "{\"ok\": true, \"operation\": \"size\", \"last_command\": \"size\", \"result\": " << n << "}\n";
}

static bool is_multi_value_field(const Corpus& corpus, const std::string& field) {
    if (field.size() >= 5 && field.compare(0, 5, "tcnt(") == 0) return false;
    std::string attr_spec = field;
    if (field.rfind("match.", 0) == 0 && field.size() > 6) {
        attr_spec = field.substr(6);
    } else {
        auto dot = field.find('.');
        if (dot != std::string::npos && dot > 0)
            attr_spec = field.substr(dot + 1);
    }
    // Multivalue positional attr
    if (corpus.is_multivalue(attr_spec))
        return true;
    // Overlapping/nested region attr
    RegionAttrParts parts;
    if (split_region_attr_name(attr_spec, parts) &&
        corpus.has_structure(parts.struct_name)) {
        const auto& sa = corpus.structure(parts.struct_name);
        if (!resolve_region_attr_key(sa, parts.struct_name, parts.attr_name))
            return false;
        return corpus.is_overlapping(parts.struct_name)
            || corpus.is_nested(parts.struct_name);
    }
    return false;
}

static void emit_field_json(std::ostream& out, const std::string& val, bool is_multi) {
    if (is_multi && val.find('|') != std::string::npos) {
        out << '[';
        size_t start = 0;
        bool first = true;
        while (start < val.size()) {
            size_t p = val.find('|', start);
            if (p == std::string::npos) p = val.size();
            if (!first) out << ", ";
            out << jstr(val.substr(start, p - start));
            first = false;
            start = p + 1;
        }
        out << ']';
    } else {
        out << jstr(val);
    }
}

static void emit_tabulate_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                               const GroupCommand& cmd, const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        out << "{\"ok\": false, \"error\": \"tabulate requires at least one field\"}\n";
        return;
    }
    try {
    const size_t n = ms.matches.size();
    const size_t start = std::min(cmd.tabulate_offset, n);
    const size_t end = std::min(start + cmd.tabulate_limit, n);
    const size_t total_hits = ms.total_count > 0 ? ms.total_count : n;

    std::vector<bool> field_is_multi(cmd.fields.size(), false);
    for (size_t f = 0; f < cmd.fields.size(); ++f)
        field_is_multi[f] = is_multi_value_field(corpus, cmd.fields[f]);

    out << "{\"ok\": true, \"operation\": \"tabulate\", \"last_command\": \"tabulate\", \"result\": {\n";
    out << "  \"fields\": [";
    for (size_t i = 0; i < cmd.fields.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.fields[i]); }
    out << "],\n  \"total_matches\": " << total_hits << ",\n";
    out << "  \"offset\": " << cmd.tabulate_offset << ",\n";
    out << "  \"limit\": " << cmd.tabulate_limit << ",\n";
    out << "  \"rows_returned\": " << (end - start) << ",\n  \"rows\": [\n";
    for (size_t i = start; i < end; ++i) {
        if (i > start) out << ",\n";
        out << "    [";
        for (size_t f = 0; f < cmd.fields.size(); ++f) {
            if (f > 0) out << ", ";
            std::string val = read_tabulate_field(corpus, ms.matches[i], name_map, cmd.fields[f]);
            emit_field_json(out, val, field_is_multi[f]);
        }
        out << "]";
    }
    out << "\n  ]\n}}\n";
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_describe_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                               const GroupCommand& cmd, const NameIndexMap& name_map) {
    const size_t n = ms.matches.size();
    const size_t start = std::min(cmd.tabulate_offset, n);
    const size_t end = std::min(start + cmd.tabulate_limit, n);
    const size_t total_hits = ms.total_count > 0 ? ms.total_count : n;

    auto is_region_only_match = [&](const Match& m) {
        return m.token_group_match || (!m.named_regions.empty() && name_map.empty());
    };

    out << "{\"ok\": true, \"operation\": \"describe\", \"last_command\": \"describe\", \"result\": {\n";
    out << "  \"total_matches\": " << total_hits << ",\n";
    out << "  \"offset\": " << cmd.tabulate_offset << ",\n";
    out << "  \"limit\": " << cmd.tabulate_limit << ",\n";
    out << "  \"rows_returned\": " << (end - start) << ",\n";
    out << "  \"rows\": [\n";
    for (size_t i = start; i < end; ++i) {
        if (i > start) out << ",\n";
        const auto& m = ms.matches[i];
        const bool region_only = is_region_only_match(m);
        out << "    {\"match_index\": " << i
            << ", \"match_start\": " << m.first_pos()
            << ", \"match_end\": " << m.last_pos()
            << ", \"kind\": " << jstr(region_only ? "region" : "token");

        if (region_only) {
            out << ", \"regions\": [";
            bool first_region = true;
            for (const auto& [label, rr] : m.named_regions) {
                if (!corpus.has_structure(rr.struct_name)) continue;
                const auto& sa = corpus.structure(rr.struct_name);
                if (rr.region_idx >= sa.region_count()) continue;
                if (!first_region) out << ", ";
                first_region = false;
                Region r = sa.get(rr.region_idx);
                out << "{\"label\": " << jstr(label)
                    << ", \"type\": " << jstr(rr.struct_name)
                    << ", \"index\": " << rr.region_idx
                    << ", \"start\": " << r.start
                    << ", \"end\": " << r.end
                    << ", \"attrs\": {";
                const auto& ra = sa.region_attr_names();
                bool first_ra = true;
                for (size_t j = 0; j < ra.size(); ++j) {
                    std::string_view rv = sa.region_value(ra[j], rr.region_idx);
                    if (!describe_emit_attr(ra[j], rv)) continue;
                    if (!first_ra) out << ", ";
                    first_ra = false;
                    out << jstr(ra[j]) << ": " << jstr(std::string(rv));
                }
                out << "}}";
            }
            out << "]";
            if (!m.token_group_props.empty()) {
                out << ", \"token_group_props\": {";
                bool first_prop = true;
                for (size_t j = 0; j < m.token_group_props.size(); ++j) {
                    const auto& [pk, pv] = m.token_group_props[j];
                    if (!describe_emit_attr(pk, pv)) continue;
                    if (!first_prop) out << ", ";
                    first_prop = false;
                    out << jstr(pk) << ": " << jstr(pv);
                }
                out << "}";
            }
        } else {
            out << ", \"tokens\": [";
            bool first_tok = true;
            const auto& attr_names = corpus.attr_names();
            for (size_t t = 0; t < m.positions.size(); ++t) {
                if (m.positions[t] == NO_HEAD) continue;
                CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
                for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                    if (!first_tok) out << ", ";
                    first_tok = false;
                    out << "{\"corpus_pos\": " << p;
                    for (const auto& attr_name : attr_names) {
                        if (!corpus.has_attr(attr_name)) continue;
                        auto val = corpus.attr(attr_name).value_at(p);
                        if (!describe_emit_attr(attr_name, val)) continue;
                        out << ", " << jstr(attr_name) << ": " << jstr(val);
                    }
                    out << "}";
                }
            }
            out << "]";
        }
        out << "}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_raw_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms) {
    const auto& form = corpus.attr("form");
    out << "{\"ok\": true, \"operation\": \"raw\", \"last_command\": \"raw\", \"result\": [\n";
    for (size_t i = 0; i < ms.matches.size(); ++i) {
        if (i > 0) out << ",\n";
        auto positions = ms.matches[i].matched_positions();
        out << "  {\"positions\": [";
        for (size_t j = 0; j < positions.size(); ++j) { if (j > 0) out << ", "; out << positions[j]; }
        out << "], \"tokens\": [";
        for (size_t j = 0; j < positions.size(); ++j) {
            if (j > 0) out << ", ";
            out << jstr(std::string(form.value_at(positions[j])));
        }
        out << "]}";
    }
    out << "\n]}\n";
}

static void emit_coll_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                           const GroupCommand& cmd, const ProgramOptions& opts,
                           const NameIndexMap& name_map, const NameIndexMap* target_name_map) {
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_window_positions = 0;

    auto count_coll_token = [&](CorpusPos p) {
        if (pa.value_at(p).empty()) return;
        ++obs_counts[pa.id_at(p)];
        ++total_window_positions;
    };

    auto add_envelope = [&](const Match& m) {
        auto matched = m.matched_positions();
        std::set<CorpusPos> matched_set(matched.begin(), matched.end());
        CorpusPos first = m.first_pos();
        CorpusPos last = m.last_pos();
        CorpusPos left_start = (first > static_cast<CorpusPos>(opts.coll_left)) ? first - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < first; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
        CorpusPos right_end = std::min(last + static_cast<CorpusPos>(opts.coll_right) + 1,
                                        static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = last + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
    };
    auto add_hub = [&](const std::set<CorpusPos>& matched_set, CorpusPos hub) {
        CorpusPos left_start = (hub > static_cast<CorpusPos>(opts.coll_left)) ? hub - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < hub; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
        CorpusPos right_end = std::min(hub + static_cast<CorpusPos>(opts.coll_right) + 1,
                                        static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = hub + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
    };

    if (!ms.parallel_matches.empty()) {
        for (const auto& [s, t] : ms.parallel_matches) {
            if (cmd.coll_on_label.empty()) {
                add_envelope(s);
            } else {
                CorpusPos hub =
                        resolve_token_label_pair(s, name_map, t, target_name_map, cmd.coll_on_label);
                if (hub == NO_HEAD) continue;
                std::set<CorpusPos> excl;
                for (CorpusPos p : s.matched_positions()) excl.insert(p);
                for (CorpusPos p : t.matched_positions()) excl.insert(p);
                add_hub(excl, hub);
            }
        }
    } else {
        for (const auto& m : ms.matches) {
            if (cmd.coll_on_label.empty()) {
                add_envelope(m);
            } else {
                CorpusPos hub = resolve_name(m, name_map, cmd.coll_on_label);
                if (hub == NO_HEAD) continue;
                auto matched = m.matched_positions();
                std::set<CorpusPos> matched_set(matched.begin(), matched.end());
                add_hub(matched_set, hub);
            }
        }
    }

    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (stop_ids.count(id)) continue;
        if (obs < opts.coll_min_freq) continue;
        std::string w = std::string(pa.lexicon().get(id));
        if (w.empty()) continue;
        entries.push_back({id, std::move(w), obs, pa.count_of_id(id), total_window_positions, N});
    }
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });
    size_t show = std::min(entries.size(), opts.coll_max_items);
    const size_t coll_match_n =
            !ms.parallel_matches.empty() ? ms.parallel_matches.size() : ms.matches.size();

    out << "{\"ok\": true, \"operation\": \"coll\", \"last_command\": \"coll\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(coll_attr) << ",\n";
    out << "  \"window\": [" << opts.coll_left << ", " << opts.coll_right << "],\n";
    if (!cmd.coll_on_label.empty())
        out << "  \"on\": " << jstr(cmd.coll_on_label) << ",\n";
    out << "  \"matches\": " << coll_match_n << ",\n";
    out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
    out << "  \"measures\": [";
    for (size_t i = 0; i < measures.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(measures[i]);
    }
    out << "],\n  \"collocates\": [\n";
    for (size_t i = 0; i < show; ++i) {
        if (i > 0) out << ",\n";
        out << "    {\"word\": " << jstr(entries[i].form) << ", \"obs\": " << entries[i].obs
            << ", \"freq\": " << entries[i].f_coll;
        for (const auto& meas : measures)
            out << ", " << jstr(meas) << ": " << std::fixed << std::setprecision(3)
                << compute_measure(meas, entries[i]);
        out << "}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_dcoll_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map,
                            const NameIndexMap* target_name_map,
                            const ProgramOptions& opts) {
    if (!corpus.has_deps()) {
        out << "{\"ok\": false, \"error\": \"dcoll requires dependency index\"}\n";
        return;
    }
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);
    const auto& deps = corpus.deps();
    bool has_deprel_attr = corpus.has_attr("deprel");
    const PositionalAttr* deprel_pa = has_deprel_attr ? &corpus.attr("deprel") : nullptr;

    bool want_head = false, want_descendants = false, want_all_children = false;
    std::set<std::string> deprel_filter;
    if (cmd.relations.empty()) { want_all_children = true; }
    else {
        for (const auto& rel : cmd.relations) {
            if (rel == "head") want_head = true;
            else if (rel == "descendants") want_descendants = true;
            else if (rel == "children") want_all_children = true;
            else deprel_filter.insert(rel);
        }
    }
    bool want_filtered_children = !deprel_filter.empty();

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_related = 0;

    auto emit_one = [&](const Match& m, const Match* tgt) {
        CorpusPos node_pos = m.first_pos();
        if (!cmd.dcoll_anchor.empty()) {
            CorpusPos ap = tgt ? resolve_token_label_pair(m, name_map, *tgt, target_name_map, cmd.dcoll_anchor)
                               : resolve_name(m, name_map, cmd.dcoll_anchor);
            if (ap != NO_HEAD) node_pos = ap;
        }
        auto count_token = [&](CorpusPos rp) {
            if (rp == node_pos) return;
            ++obs_counts[pa.id_at(rp)]; ++total_related;
        };
        if (want_head) { auto h = deps.head(node_pos); if (h != NO_HEAD) count_token(h); }
        if (want_descendants) for (CorpusPos rp : deps.subtree(node_pos)) count_token(rp);
        if (want_all_children) for (CorpusPos rp : deps.children(node_pos)) count_token(rp);
        if (want_filtered_children) {
            for (CorpusPos rp : deps.children(node_pos)) {
                if (deprel_pa) { std::string dr(deprel_pa->value_at(rp)); if (deprel_filter.count(dr)) count_token(rp); }
            }
        }
    };
    if (!ms.parallel_matches.empty()) {
        for (const auto& [s, t] : ms.parallel_matches)
            emit_one(s, &t);
    } else {
        for (const auto& m : ms.matches)
            emit_one(m, nullptr);
    }

    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (stop_ids.count(id)) continue;
        if (obs < opts.coll_min_freq) continue;
        entries.push_back({id, std::string(pa.lexicon().get(id)), obs, pa.count_of_id(id), total_related, N});
    }
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });
    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"dcoll\", \"last_command\": \"dcoll\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(coll_attr) << ",\n";
    out << "  \"relations\": [";
    for (size_t i = 0; i < cmd.relations.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.relations[i]); }
    out << "],\n";
    if (!cmd.dcoll_anchor.empty()) out << "  \"anchor\": " << jstr(cmd.dcoll_anchor) << ",\n";
    {
        const size_t dcoll_match_n =
                !ms.parallel_matches.empty() ? ms.parallel_matches.size() : ms.matches.size();
        out << "  \"matches\": " << dcoll_match_n << ",\n";
    }
    out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
    out << "  \"measures\": [";
    for (size_t i = 0; i < measures.size(); ++i) { if (i > 0) out << ", "; out << jstr(measures[i]); }
    out << "],\n  \"collocates\": [\n";
    for (size_t i = 0; i < show; ++i) {
        if (i > 0) out << ",\n";
        out << "    {\"word\": " << jstr(entries[i].form) << ", \"obs\": " << entries[i].obs
            << ", \"freq\": " << entries[i].f_coll;
        for (const auto& meas : measures)
            out << ", " << jstr(meas) << ": " << std::fixed << std::setprecision(3) << compute_measure(meas, entries[i]);
        out << "}";
    }
    out << "\n  ]\n}}\n";
}

// ── keyness: subcorpus keyword extraction (#40) ────────────────────────

static double safe_ln(double x) { return x > 0 ? std::log(x) : 0.0; }

static void emit_keyness_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                               const GroupCommand& cmd, const ProgramOptions& opts,
                               const MatchSet* ref_ms = nullptr) {
    std::string attr = "lemma";
    if (!cmd.fields.empty()) attr = cmd.fields[0];
    if (!corpus.has_attr(attr)) attr = "form";
    const auto& pa = corpus.attr(attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);

    std::unordered_map<LexiconId, size_t> focus_counts;
    size_t focus_size = 0;
    for (const auto& m : ms.matches) {
        auto positions = m.matched_positions();
        for (CorpusPos p : positions) {
            ++focus_counts[pa.id_at(p)];
            ++focus_size;
        }
    }

    if (focus_size == 0) {
        out << "{\"ok\": true, \"operation\": \"keyness\", \"last_command\": \"keyness\", \"result\": {\"rows\": []}}\n";
        return;
    }

    // Reference counts: from ref_ms if provided, else rest of corpus
    std::unordered_map<LexiconId, size_t> ref_counts;
    size_t ref_size = 0;
    if (ref_ms) {
        for (const auto& m : ref_ms->matches) {
            auto positions = m.matched_positions();
            for (CorpusPos p : positions) {
                ++ref_counts[pa.id_at(p)];
                ++ref_size;
            }
        }
    } else {
        ref_size = corpus.size() - focus_size;
    }

    double N = static_cast<double>(focus_size + ref_size);

    // Collect all word types
    std::set<LexiconId> all_ids;
    for (const auto& [id, _] : focus_counts) if (!stop_ids.count(id)) all_ids.insert(id);
    if (ref_ms) {
        for (const auto& [id, _] : ref_counts)
            if (!stop_ids.count(id)) all_ids.insert(id);
    }

    struct KE { std::string form; size_t ff; size_t rf; double g2; };
    std::vector<KE> entries;
    for (LexiconId id : all_ids) {
        size_t ffreq = 0;
        auto fit = focus_counts.find(id);
        if (fit != focus_counts.end()) ffreq = fit->second;

        size_t rfreq = 0;
        if (ref_ms) {
            auto rit = ref_counts.find(id);
            if (rit != ref_counts.end()) rfreq = rit->second;
        } else {
            size_t corpus_freq = pa.count_of_id(id);
            rfreq = corpus_freq > ffreq ? corpus_freq - ffreq : 0;
        }

        if (ffreq == 0 && rfreq == 0) continue;

        double E1 = static_cast<double>(focus_size) * static_cast<double>(ffreq + rfreq) / N;
        double E2 = static_cast<double>(ref_size) * static_cast<double>(ffreq + rfreq) / N;
        double g2 = 0.0;
        if (ffreq > 0 && E1 > 0) g2 += static_cast<double>(ffreq) * safe_ln(static_cast<double>(ffreq) / E1);
        if (rfreq > 0 && E2 > 0) g2 += static_cast<double>(rfreq) * safe_ln(static_cast<double>(rfreq) / E2);
        g2 *= 2.0;
        if (static_cast<double>(ffreq) < E1) g2 = -g2;
        entries.push_back({std::string(pa.lexicon().get(id)), ffreq, rfreq, g2});
    }

    std::sort(entries.begin(), entries.end(), [](const KE& a, const KE& b) {
        if (a.g2 != b.g2) return a.g2 > b.g2;
        if (a.ff != b.ff) return a.ff > b.ff;
        return a.form < b.form;
    });

    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"keyness\", \"last_command\": \"keyness\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(attr) << ",\n";
    out << "  \"focus_size\": " << focus_size << ",\n";
    out << "  \"ref_size\": " << ref_size << ",\n";
    out << "  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
    out << "  \"rows\": [\n";
    for (size_t i = 0; i < show; ++i) {
        if (i > 0) out << ",\n";
        out << "    {\"word\": " << jstr(entries[i].form)
            << ", \"focus_freq\": " << entries[i].ff
            << ", \"ref_freq\": " << entries[i].rf
            << ", \"keyness\": " << std::fixed << std::setprecision(2) << entries[i].g2
            << ", \"effect\": " << jstr(entries[i].g2 >= 0 ? "+" : "-")
            << "}";
    }
    out << "\n  ]\n}}\n";
}

static void emit_query_json(std::ostream& out, const Corpus& corpus, const std::string& query_text,
                            const MatchSet& ms, const ProgramOptions& opts, double elapsed_ms) {
    QueryOptions qopts;
    qopts.limit = opts.limit; qopts.offset = opts.offset; qopts.max_total = opts.max_total;
    qopts.context = opts.context; qopts.total = opts.total; qopts.attrs = opts.attrs;
    out << to_query_result_json(corpus, query_text, ms, qopts, elapsed_ms);
}

static void emit_show_values_json(std::ostream& out, const Corpus& corpus, const std::string& attr_name,
                                  size_t group_limit) {
    std::string json = to_values_json(corpus, attr_name, group_limit);
    if (json.empty())
        out << "{\"ok\": false, \"error\": \"Unknown attribute: " << attr_name << "\"}\n";
    else
        out << json;
}

static void emit_show_regions_type_json(std::ostream& out, const Corpus& corpus,
                                        const std::string& type_name, size_t group_limit) {
    std::string json = to_regions_json(corpus, type_name, group_limit);
    if (json.empty())
        out << "{\"ok\": false, \"error\": \"Unknown structure type: " << type_name << "\"}\n";
    else
        out << json;
}

static void emit_show_regions_json(std::ostream& out, const Corpus& corpus) {
    out << "{\n  \"ok\": true,\n  \"operation\": \"show_regions\",\n";
    out << "  \"result\": {\n    \"structures\": [";
    const auto& s_names = corpus.structure_names();
    for (size_t i = 0; i < s_names.size(); ++i) {
        if (i > 0) out << ", ";
        const auto& sa = corpus.structure(s_names[i]);
        out << "{\"name\": " << jstr(s_names[i]) << ", \"regions\": " << sa.region_count()
            << ", \"has_values\": " << (sa.has_values() ? "true" : "false") << ", \"attrs\": [";
        const auto& ra = sa.region_attr_names();
        for (size_t j = 0; j < ra.size(); ++j) { if (j > 0) out << ", "; out << jstr(ra[j]); }
        out << "]}";
    }
    out << "],\n    \"region_attrs\": [";
    const auto& ra_all = corpus.region_attr_names();
    for (size_t i = 0; i < ra_all.size(); ++i) { if (i > 0) out << ", "; out << jstr(ra_all[i]); }
    out << "]\n  }\n}\n";
}

static void emit_show_attrs_json(std::ostream& out, const Corpus& corpus) {
    out << "{\"ok\": true, \"operation\": \"show_attrs\", \"result\": [";
    const auto& names = corpus.attr_names();
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) out << ", ";
        out << "{\"name\": " << jstr(names[i]) << ", \"vocab\": " << corpus.attr(names[i]).lexicon().size() << "}";
    }
    out << "]}\n";
}

static void emit_show_info_json(std::ostream& out, const Corpus& corpus) {
    out << to_info_json(corpus);
}

static void emit_show_named_json(std::ostream& out, const ProgramSession::Impl& session) {
    out << "{\"ok\": true, \"operation\": \"show_named\", \"result\": [";
    size_t idx = 0;
    for (const auto& [name, ms] : session.named_results) {
        if (idx++ > 0) out << ", ";
        out << "{\"name\": " << jstr(name) << ", \"matches\": " << ms.matches.size() << "}";
    }
    out << "]}\n";
}

// ── Main dispatch ───────────────────────────────────────────────────────

std::string run_program_json(Corpus& corpus, ProgramSession& ps,
                             const std::string& cql, ProgramOptions opts) {
    auto& S = *ps.impl_;

    Parser parser(cql, ParserOptions{opts.strict_quoted_strings});
    Program prog = parser.parse();

    QueryExecutor executor(corpus);
    executor.set_include_empty_alignment_values(opts.allow_empty_alignment);
    std::ostringstream out;

    for (size_t si = 0; si < prog.size(); ++si) {
        auto& stmt = prog[si];
        bool next_is_command = (si + 1 < prog.size() && prog[si + 1].has_command);

        if (stmt.has_query) {
            size_t max_m = 0;
            bool count_t = false;
            size_t max_total_cap = 0;
            if (!next_is_command) {
                // See query_main.cpp: do not page-limit named assignments (stored for freq/count).
                if (!stmt.name.empty()) {
                    max_m = 0;
                    count_t = opts.total;
                    max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
                } else {
                    max_m = opts.offset + opts.limit;
                    count_t = opts.total;
                    max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
                }
            }

            const std::vector<std::string>* aggregate_by = nullptr;
            if (next_is_command && !stmt.is_parallel) {
                const GroupCommand& ncmd = prog[si + 1].command;
                if (!ncmd.fields.empty()
                    && (ncmd.type == CommandType::COUNT || ncmd.type == CommandType::GROUP
                        || ncmd.type == CommandType::FREQ)
                    && aggregate_command_targets_stmt(stmt, ncmd))
                    aggregate_by = &ncmd.fields;
            }

            auto t0 = std::chrono::high_resolution_clock::now();
            if (stmt.is_parallel)
                S.last_ms = executor.execute_parallel(stmt.query, stmt.target_query, max_m, count_t);
            else
                S.last_ms = executor.execute(stmt.query, max_m, count_t, max_total_cap, 0, 0, 1,
                                             aggregate_by);
            auto t1 = std::chrono::high_resolution_clock::now();
            double query_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

            S.has_last = true;
            S.last_name_map = stmt.is_parallel
                ? build_name_map(stmt.query)
                : QueryExecutor::build_name_map_for_stripped_query(stmt.query);
            S.last_target_name_map =
                    stmt.is_parallel ? build_name_map(stmt.target_query) : NameIndexMap{};
            S.named_results["Last"] = S.last_ms;
            S.named_name_maps["Last"] = S.last_name_map;
            S.named_target_name_maps["Last"] = S.last_target_name_map;
            if (!stmt.name.empty()) {
                S.named_results[stmt.name] = S.last_ms;
                S.named_name_maps[stmt.name] = S.last_name_map;
                S.named_target_name_maps[stmt.name] = S.last_target_name_map;
            }

            if (!next_is_command) {
                out.str(""); out.clear();
                emit_query_json(out, corpus, cql, S.last_ms, opts, query_ms);
            }
        }

        if (stmt.has_command) {
            // Commands that don't need a MatchSet
            if (stmt.command.type == CommandType::DROP) {
                if (stmt.command.query_name == "all") {
                    S.named_results.clear();
                    S.named_name_maps.clear();
                    S.named_target_name_maps.clear();
                }  else {
                    S.named_results.erase(stmt.command.query_name);
                    S.named_name_maps.erase(stmt.command.query_name);
                    S.named_target_name_maps.erase(stmt.command.query_name);
                }
                out.str(""); out.clear();
                out << "{\"ok\": true, \"operation\": \"drop\"}\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_NAMED) {
                out.str(""); out.clear(); emit_show_named_json(out, S); continue;
            }
            if (stmt.command.type == CommandType::SHOW_ATTRS) {
                out.str(""); out.clear(); emit_show_attrs_json(out, corpus); continue;
            }
            if (stmt.command.type == CommandType::SHOW_REGIONS) {
                out.str(""); out.clear();
                if (!stmt.command.query_name.empty())
                    emit_show_regions_type_json(out, corpus, stmt.command.query_name, opts.group_limit);
                else
                    emit_show_regions_json(out, corpus);
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_VALUES) {
                out.str(""); out.clear();
                emit_show_values_json(out, corpus, stmt.command.query_name, opts.group_limit);
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_INFO) {
                out.str(""); out.clear(); emit_show_info_json(out, corpus); continue;
            }
            if (stmt.command.type == CommandType::SET) {
                const std::string& name = stmt.command.set_name;
                const std::string& val  = stmt.command.set_value;
                auto split_csv = [](const std::string& s) -> std::vector<std::string> {
                    std::vector<std::string> r;
                    std::string cur;
                    for (char c : s) {
                        if (c == ',' || c == ' ') { if (!cur.empty()) { r.push_back(cur); cur.clear(); } }
                        else cur += c;
                    }
                    if (!cur.empty()) r.push_back(cur);
                    return r;
                };
                auto to_size = [&](size_t& t) { try { t = std::stoull(val); } catch (...) {} };
                auto to_int = [&](int& t) { try { t = std::stoi(val); } catch (...) {} };

                if (name == "limit")          to_size(opts.limit);
                else if (name == "offset")    to_size(opts.offset);
                else if (name == "context")   to_int(opts.context);
                else if (name == "left")      to_int(opts.coll_left);
                else if (name == "right")     to_int(opts.coll_right);
                else if (name == "window")    { to_int(opts.coll_left); opts.coll_right = opts.coll_left; opts.context = opts.coll_left; }
                else if (name == "max-total" || name == "max_total")  to_size(opts.max_total);
                else if (name == "max-items" || name == "max_items")  to_size(opts.coll_max_items);
                else if (name == "min-freq" || name == "min_freq")    to_size(opts.coll_min_freq);
                else if (name == "stoplist") to_size(opts.coll_stoplist);
                else if (name == "group-limit" || name == "group_limit") to_size(opts.group_limit);
                else if (name == "measures")  opts.coll_measures = split_csv(val);
                else if (name == "attrs") {
                    if (val == "all" || val == "*" || val.empty()) opts.attrs.clear();
                    else opts.attrs = split_csv(val);
                }
                else if (name == "total")     opts.total = (val == "true" || val == "1" || val == "on");

                out.str(""); out.clear();
                out << "{\"ok\": true, \"operation\": \"set\", \"setting\": "
                    << jstr(name) << ", \"value\": " << jstr(val) << "}\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_SETTINGS) {
                auto join = [](const std::vector<std::string>& v) {
                    std::string r;
                    for (size_t i = 0; i < v.size(); ++i) { if (i > 0) r += ","; r += v[i]; }
                    return r.empty() ? "(all)" : r;
                };
                out.str(""); out.clear();
                out << "{\"ok\": true, \"operation\": \"show_settings\", \"result\": {\n";
                out << "  \"limit\": " << opts.limit << ",\n";
                out << "  \"offset\": " << opts.offset << ",\n";
                out << "  \"context\": " << opts.context << ",\n";
                out << "  \"left\": " << opts.coll_left << ",\n";
                out << "  \"right\": " << opts.coll_right << ",\n";
                out << "  \"max_total\": " << opts.max_total << ",\n";
                out << "  \"max_items\": " << opts.coll_max_items << ",\n";
                out << "  \"min_freq\": " << opts.coll_min_freq << ",\n";
                out << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
                out << "  \"group_limit\": " << opts.group_limit << ",\n";
                out << "  \"total\": " << (opts.total ? "true" : "false") << ",\n";
                out << "  \"measures\": " << jstr(join(opts.coll_measures.empty()
                    ? std::vector<std::string>{"logdice"} : opts.coll_measures)) << ",\n";
                out << "  \"attrs\": " << jstr(join(opts.attrs)) << "\n";
                out << "}}\n";
                continue;
            }
            if (stmt.command.type == CommandType::SIZE && stmt.command.query_name.empty() && !S.has_last) {
                out.str(""); out.clear(); emit_show_info_json(out, corpus); continue;
            }

            // Commands that need a MatchSet
            MatchSet* ms_to_use = nullptr;
            const NameIndexMap* nm_to_use = nullptr;
            if (!stmt.command.query_name.empty()) {
                auto it = S.named_results.find(stmt.command.query_name);
                if (it != S.named_results.end()) {
                    ms_to_use = &it->second;
                    auto nm_it = S.named_name_maps.find(stmt.command.query_name);
                    nm_to_use = (nm_it != S.named_name_maps.end()) ? &nm_it->second : &S.last_name_map;
                } else if (S.has_last) {
                    ms_to_use = &S.last_ms; nm_to_use = &S.last_name_map;
                }
            } else if (S.has_last) {
                ms_to_use = &S.last_ms; nm_to_use = &S.last_name_map;
            }

            if (!ms_to_use) {
                out.str(""); out.clear();
                out << "{\"ok\": false, \"error\": \"No query to operate on\"}\n";
                continue;
            }

            const NameIndexMap* nm_tgt_parallel = nullptr;
            if (!ms_to_use->parallel_matches.empty()) {
                if (!stmt.command.query_name.empty()) {
                    auto tit = S.named_target_name_maps.find(stmt.command.query_name);
                    nm_tgt_parallel =
                            (tit != S.named_target_name_maps.end()) ? &tit->second : nullptr;
                } else {
                    nm_tgt_parallel = &S.last_target_name_map;
                }
            }

            out.str(""); out.clear();
            switch (stmt.command.type) {
                case CommandType::COUNT:
                case CommandType::GROUP:
                    emit_count_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use, opts.group_limit);
                    break;
                case CommandType::STATS:
                    emit_stats_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use);
                    break;
                case CommandType::FREQ:
                    if (stmt.command.freq_query_names.size() >= 2) {
                        emit_freq_compare_json(out, corpus, ps, stmt.command, opts);
                    } else {
                        const std::string source_query_name =
                            !stmt.command.query_name.empty() ? stmt.command.query_name : "Last";
                        emit_freq_json(out, corpus, *ms_to_use, stmt.command, opts, *nm_to_use, source_query_name);
                    }
                    break;
                case CommandType::SIZE:
                    emit_size_json(out, *ms_to_use);
                    break;
                case CommandType::TABULATE:
                    emit_tabulate_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use);
                    break;
                case CommandType::DESCRIBE:
                    emit_describe_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use);
                    break;
                case CommandType::RAW:
                    emit_raw_json(out, corpus, *ms_to_use);
                    break;
                case CommandType::COLL:
                    emit_coll_json(out, corpus, *ms_to_use, stmt.command, opts, *nm_to_use,
                                   nm_tgt_parallel);
                    break;
                case CommandType::DCOLL:
                    emit_dcoll_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use,
                                    nm_tgt_parallel, opts);
                    break;
                case CommandType::KEYNESS: {
                    const MatchSet* ref_ms = nullptr;
                    if (!stmt.command.ref_query_name.empty()) {
                        auto rit = S.named_results.find(stmt.command.ref_query_name);
                        if (rit == S.named_results.end()) {
                            out.str(""); out.clear();
                            out << "{\"ok\": false, \"error\": \"Unknown reference query: "
                                << stmt.command.ref_query_name << "\"}\n";
                            break;
                        }
                        ref_ms = &rit->second;
                    }
                    emit_keyness_json(out, corpus, *ms_to_use, stmt.command, opts, ref_ms);
                    break;
                }
                case CommandType::SORT: {
                    try {
                        if (!stmt.command.fields.empty()) {
                            std::sort(ms_to_use->matches.begin(), ms_to_use->matches.end(),
                                      [&](const Match& a, const Match& b) {
                                          return compare_group_keys(
                                              make_key(corpus, a, *nm_to_use, stmt.command.fields),
                                              make_key(corpus, b, *nm_to_use, stmt.command.fields));
                                      });
                        }
                        emit_query_json(out, corpus, "(sorted)", *ms_to_use, opts, 0);
                    } catch (const std::exception& e) {
                        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
                    }
                    break;
                }
                default:
                    out << "{\"ok\": true, \"operation\": \"unknown\"}\n";
                    break;
            }
        }
    }

    std::string result = out.str();
    if (result.empty())
        return "{\"ok\": true, \"operation\": \"assign\", \"result\": {}}\n";
    return result;
}

} // namespace pando
