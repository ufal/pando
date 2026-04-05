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
#include <map>
#include <unordered_map>
#include <set>
#include <cmath>
#include <iomanip>
#include <stdexcept>

namespace manatree {

// ── Session impl ────────────────────────────────────────────────────────

struct ProgramSession::Impl {
    std::map<std::string, MatchSet> named_results;
    std::map<std::string, NameIndexMap> named_name_maps;
    MatchSet last_ms;
    NameIndexMap last_name_map;
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
    if (cmd.fields.empty()) {
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
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
    size_t total = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
    size_t g_end = (group_limit > 0 && group_limit < sorted.size()) ? group_limit : sorted.size();

    out << "{\"ok\": true, \"operation\": \"count\", \"result\": {\n";
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

static void emit_freq_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                           const GroupCommand& cmd, const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        out << "{\"ok\": false, \"error\": \"freq requires 'by' clause\"}\n";
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
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
    double corpus_size = static_cast<double>(corpus.size());
    size_t total_matches = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();

    // Per-subcorpus IPM: when grouping by a single region attribute, use per-group
    // token counts as IPM denominator for meaningful relative frequencies.
    const StructuralAttr* freq_sa = nullptr;
    std::string freq_region_attr;
    if (cmd.fields.size() == 1) {
        RegionAttrParts parts;
        if (split_region_attr_name(cmd.fields[0], parts) &&
            corpus.has_structure(parts.struct_name)) {
            const auto& sa = corpus.structure(parts.struct_name);
            if (sa.has_region_attr(parts.attr_name)) {
                freq_sa = &sa;
                freq_region_attr = parts.attr_name;
            }
        }
    }

    std::unordered_map<std::string, double> subcorpus_sizes;
    if (freq_sa) {
        for (const auto& [key, count] : sorted) {
            size_t span = freq_sa->token_span_sum_for_attr_eq(freq_region_attr, key);
            subcorpus_sizes[key] = (span > 0 && span != SIZE_MAX)
                ? static_cast<double>(span) : corpus_size;
        }
    }

    auto ipm_denom = [&](const std::string& key) -> double {
        if (freq_sa) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    out << "{\"ok\": true, \"operation\": \"freq\", \"result\": {\n";
    out << "  \"corpus_size\": " << corpus.size() << ",\n";
    out << "  \"total_matches\": " << total_matches << ",\n";
    out << "  \"per_subcorpus_ipm\": " << (freq_sa ? "true" : "false") << ",\n";
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
        if (freq_sa)
            out << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
        out << "}";
    }
    out << "\n  ]\n}}\n";
    } catch (const std::exception& e) {
        out << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
    }
}

static void emit_size_json(std::ostream& out, const MatchSet& ms) {
    size_t n = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
    out << "{\"ok\": true, \"operation\": \"size\", \"result\": " << n << "}\n";
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

    out << "{\"ok\": true, \"operation\": \"tabulate\", \"result\": {\n";
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

static void emit_raw_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms) {
    const auto& form = corpus.attr("form");
    out << "{\"ok\": true, \"operation\": \"raw\", \"result\": [\n";
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
                           const GroupCommand& cmd, const ProgramOptions& opts) {
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_window_positions = 0;

    for (const auto& m : ms.matches) {
        auto matched = m.matched_positions();
        std::set<CorpusPos> matched_set(matched.begin(), matched.end());
        CorpusPos first = m.first_pos();
        CorpusPos last = m.last_pos();

        CorpusPos left_start = (first > static_cast<CorpusPos>(opts.coll_left)) ? first - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < first; ++p) {
            if (matched_set.count(p)) continue;
            ++obs_counts[pa.id_at(p)]; ++total_window_positions;
        }
        CorpusPos right_end = std::min(last + static_cast<CorpusPos>(opts.coll_right) + 1,
                                        static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = last + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            ++obs_counts[pa.id_at(p)]; ++total_window_positions;
        }
    }

    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (obs < opts.coll_min_freq) continue;
        entries.push_back({id, std::string(pa.lexicon().get(id)), obs, pa.count_of_id(id), total_window_positions, N});
    }
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });
    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"coll\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(coll_attr) << ",\n";
    out << "  \"window\": [" << opts.coll_left << ", " << opts.coll_right << "],\n";
    out << "  \"matches\": " << ms.matches.size() << ",\n";
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

static void emit_dcoll_json(std::ostream& out, const Corpus& corpus, const MatchSet& ms,
                            const GroupCommand& cmd, const NameIndexMap& name_map,
                            const ProgramOptions& opts) {
    if (!corpus.has_deps()) {
        out << "{\"ok\": false, \"error\": \"dcoll requires dependency index\"}\n";
        return;
    }
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
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

    for (const auto& m : ms.matches) {
        CorpusPos node_pos = m.first_pos();
        if (!cmd.dcoll_anchor.empty()) {
            CorpusPos ap = resolve_name(m, name_map, cmd.dcoll_anchor);
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
    }

    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (obs < opts.coll_min_freq) continue;
        entries.push_back({id, std::string(pa.lexicon().get(id)), obs, pa.count_of_id(id), total_related, N});
    }
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });
    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"dcoll\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(coll_attr) << ",\n";
    out << "  \"relations\": [";
    for (size_t i = 0; i < cmd.relations.size(); ++i) { if (i > 0) out << ", "; out << jstr(cmd.relations[i]); }
    out << "],\n";
    if (!cmd.dcoll_anchor.empty()) out << "  \"anchor\": " << jstr(cmd.dcoll_anchor) << ",\n";
    out << "  \"matches\": " << ms.matches.size() << ",\n";
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
        out << "{\"ok\": true, \"operation\": \"keyness\", \"result\": {\"rows\": []}}\n";
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
    for (const auto& [id, _] : focus_counts) all_ids.insert(id);
    if (ref_ms) { for (const auto& [id, _] : ref_counts) all_ids.insert(id); }

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
        return std::abs(a.g2) > std::abs(b.g2);
    });

    size_t show = std::min(entries.size(), opts.coll_max_items);

    out << "{\"ok\": true, \"operation\": \"keyness\", \"result\": {\n";
    out << "  \"attribute\": " << jstr(attr) << ",\n";
    out << "  \"focus_size\": " << focus_size << ",\n";
    out << "  \"ref_size\": " << ref_size << ",\n";
    out << "  \"corpus_size\": " << corpus.size() << ",\n";
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

    Parser parser(cql);
    Program prog = parser.parse();

    QueryExecutor executor(corpus);
    std::ostringstream out;

    for (size_t si = 0; si < prog.size(); ++si) {
        auto& stmt = prog[si];
        bool next_is_command = (si + 1 < prog.size() && prog[si + 1].has_command);

        if (stmt.has_query) {
            size_t max_m = 0;
            bool count_t = false;
            size_t max_total_cap = 0;
            if (!next_is_command) {
                max_m = opts.offset + opts.limit;
                count_t = opts.total;
                max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
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
            S.named_results["Last"] = S.last_ms;
            S.named_name_maps["Last"] = S.last_name_map;
            if (!stmt.name.empty()) {
                S.named_results[stmt.name] = S.last_ms;
                S.named_name_maps[stmt.name] = S.last_name_map;
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
                    S.named_results.clear(); S.named_name_maps.clear();
                }  else {
                    S.named_results.erase(stmt.command.query_name);
                    S.named_name_maps.erase(stmt.command.query_name);
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

            out.str(""); out.clear();
            switch (stmt.command.type) {
                case CommandType::COUNT:
                case CommandType::GROUP:
                    emit_count_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use, opts.group_limit);
                    break;
                case CommandType::FREQ:
                    emit_freq_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use);
                    break;
                case CommandType::SIZE:
                    emit_size_json(out, *ms_to_use);
                    break;
                case CommandType::TABULATE:
                    emit_tabulate_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use);
                    break;
                case CommandType::RAW:
                    emit_raw_json(out, corpus, *ms_to_use);
                    break;
                case CommandType::COLL:
                    emit_coll_json(out, corpus, *ms_to_use, stmt.command, opts);
                    break;
                case CommandType::DCOLL:
                    emit_dcoll_json(out, corpus, *ms_to_use, stmt.command, *nm_to_use, opts);
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
                                          return make_key(corpus, a, *nm_to_use, stmt.command.fields)
                                               < make_key(corpus, b, *nm_to_use, stmt.command.fields);
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

} // namespace manatree
