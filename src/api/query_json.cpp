#include "api/query_json.h"
#include "core/json_utils.h"
#include "query/parser.h"
#include <sstream>
#include <chrono>
#include <algorithm>
#include <map>
#include <iostream>
#include <set>
#include <cmath>
#include <unordered_map>
#include <iomanip>
#include <string_view>

namespace manatree {

// Same pipe boundaries as query/executor multivalue_eq (RG-5f).
static void add_mv_counts(std::string_view stored, size_t weight,
                          std::unordered_map<std::string, size_t>& counts) {
    if (stored.find('|') == std::string_view::npos) {
        counts[std::string(stored)] += weight;
        return;
    }
    size_t start = 0;
    while (start < stored.size()) {
        size_t p = stored.find('|', start);
        if (p == std::string_view::npos) p = stored.size();
        std::string_view seg = stored.substr(start, p - start);
        if (!seg.empty())
            counts[std::string(seg)] += weight;
        start = p + 1;
    }
}

std::vector<std::pair<std::string, size_t>> positional_attr_show_values_mv(const PositionalAttr& pa,
                                                                           bool split_mv) {
    std::unordered_map<std::string, size_t> counts;
    const auto& lex = pa.lexicon();
    for (LexiconId id = 0; id < lex.size(); ++id) {
        size_t cnt = pa.count_of_id(id);
        if (cnt == 0) continue;
        if (split_mv)
            add_mv_counts(lex.get(id), cnt, counts);
        else
            counts[std::string(lex.get(id))] += cnt;
    }
    std::vector<std::pair<std::string, size_t>> entries(counts.begin(), counts.end());
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    return entries;
}

std::vector<std::pair<std::string, size_t>> region_attr_show_values_mv(const StructuralAttr& sa,
                                                                         const std::string& region_attr,
                                                                         bool split_mv) {
    std::unordered_map<std::string, size_t> counts;
    size_t n = sa.region_count();
    for (size_t i = 0; i < n; ++i) {
        std::string_view v = sa.region_value(region_attr, i);
        if (split_mv)
            add_mv_counts(v, 1, counts);
        else
            counts[std::string(v)] += 1;
    }
    std::vector<std::pair<std::string, size_t>> entries(counts.begin(), counts.end());
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    return entries;
}

std::pair<MatchSet, double> run_single_query(const Corpus& corpus,
                                            const std::string& query_text,
                                            const QueryOptions& opts) {
    Parser parser(query_text);
    Program prog = parser.parse();
    if (prog.empty() || !prog[0].has_query)
        return {MatchSet{}, 0.0};

    QueryExecutor executor(corpus);
    size_t max_m = opts.offset + opts.limit;
    bool count_t = opts.total;
    size_t max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;

    auto t0 = std::chrono::high_resolution_clock::now();
    MatchSet ms = executor.execute(prog[0].query, max_m, count_t, max_total_cap);
    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double, std::milli>(t1 - t0).count();
    return {std::move(ms), elapsed};
}

std::string to_query_result_json(const Corpus& corpus,
                                 const std::string& query_text,
                                 const MatchSet& ms,
                                 const QueryOptions& opts,
                                 double elapsed_ms) {
    std::ostringstream out;
    size_t stored = ms.matches.size();
    size_t start = std::min(opts.offset, stored);
    size_t end   = std::min(start + opts.limit, stored);
    size_t returned = end - start;

    out << "{\n";
    out << "  \"ok\": true,\n";
    out << "  \"backend\": \"manatree\",\n";
    out << "  \"operation\": \"query\",\n";
    out << "  \"result\": {\n";
    out << "    \"query\": {\"language\": \"clickcql\", \"text\": " << jstr(query_text) << "},\n";
    out << "    \"page\": {\"start\": " << start << ", \"size\": " << opts.limit
        << ", \"returned\": " << returned << ", \"total\": " << ms.total_count
        << ", \"total_exact\": " << (ms.total_exact ? "true" : "false") << "},\n";
    out << "    \"hits\": [\n";

    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];
        CorpusPos match_start = m.first_pos();
        CorpusPos match_end   = m.last_pos();
        auto doc_id = lookup_doc_id(corpus, match_start);
        auto ctx = build_context(corpus, m, opts.context);

        if (i > start) out << ",\n";
        out << "      {";
        out << "\"doc_id\": " << (doc_id.empty() ? "null" : jstr(doc_id));
        out << ", \"match_start\": " << match_start << ", \"match_end\": " << match_end;
        out << ", \"context\": {\"left\": " << jstr(ctx.left)
            << ", \"match\": " << jstr(ctx.match) << ", \"right\": " << jstr(ctx.right) << "}";
        out << ", \"tokens\": [";
        const auto& attr_names = opts.attrs.empty()
            ? corpus.attr_names() : opts.attrs;
        bool first_tok = true;
        for (size_t t = 0; t < m.positions.size(); ++t) {
            if (m.positions[t] == NO_HEAD) continue;
            CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
            for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                if (!first_tok) out << ", ";
                first_tok = false;
                out << "{\"pos\": " << p;
                for (const auto& attr_name : attr_names) {
                    if (!corpus.has_attr(attr_name)) continue;
                    auto val = corpus.attr(attr_name).value_at(p);
                    if (val == "_") continue;
                    out << ", " << jstr(attr_name) << ": " << jstr(val);
                }
                out << "}";
            }
        }
        out << "]}";
    }
    out << "\n    ]";

    if (opts.debug) {
        out << ",\n    \"debug\": {\n";
        out << "      \"corpus_size\": " << corpus.size() << ",\n";
        out << "      \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
        out << "      \"elapsed_ms\": " << elapsed_ms << ",\n";
        out << "      \"seed_token\": " << ms.seed_token << ",\n";
        out << "      \"cardinalities\": [";
        for (size_t i = 0; i < ms.cardinalities.size(); ++i) {
            if (i > 0) out << ", ";
            out << ms.cardinalities[i];
        }
        out << "]\n    }";
    }
    out << "\n  }\n}\n";
    return out.str();
}

std::string to_info_json(const Corpus& corpus) {
    std::ostringstream out;
    out << "{\n  \"ok\": true,\n  \"operation\": \"info\",\n";
    out << "  \"result\": {\n";
    out << "    \"size\": " << corpus.size() << ",\n";
    out << "    \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
    out << "    \"attributes\": [";
    const auto& names = corpus.attr_names();
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(names[i]);
    }
    out << "],\n";
    out << "    \"structures\": [";
    const auto& s_names = corpus.structure_names();
    for (size_t i = 0; i < s_names.size(); ++i) {
        if (i > 0) out << ", ";
        const auto& sa = corpus.structure(s_names[i]);
        out << "{\"name\": " << jstr(s_names[i])
            << ", \"regions\": " << sa.region_count();
        if (corpus.is_nested(s_names[i]))     out << ", \"nested\": true";
        if (corpus.is_overlapping(s_names[i])) out << ", \"overlapping\": true";
        if (corpus.is_zerowidth(s_names[i]))   out << ", \"zerowidth\": true";
        out << "}";
    }
    out << "]\n  }\n}\n";
    return out.str();
}

std::string to_values_json(const Corpus& corpus, const std::string& attr_name, size_t limit) {
    std::ostringstream out;

    // Try positional attribute first
    if (corpus.has_attr(attr_name)) {
        const auto& pa = corpus.attr(attr_name);
        bool is_mv = corpus.is_multivalue(attr_name);
        std::vector<std::pair<std::string, size_t>> entries = positional_attr_show_values_mv(pa, is_mv);

        size_t cap = (limit > 0) ? std::min(entries.size(), limit) : entries.size();
        out << "{\n  \"ok\": true,\n  \"operation\": \"values\",\n";
        out << "  \"result\": {\n";
        out << "    \"attr\": " << jstr(attr_name) << ",\n";
        out << "    \"type\": \"positional\",\n";
        out << "    \"unique\": " << entries.size() << ",\n";
        out << "    \"returned\": " << cap << ",\n";
        out << "    \"values\": [\n";
        for (size_t i = 0; i < cap; ++i) {
            if (i > 0) out << ",\n";
            out << "      {\"value\": " << jstr(entries[i].first)
                << ", \"count\": " << entries[i].second << "}";
        }
        out << "\n    ]\n  }\n}\n";
        return out.str();
    }

    // Try region attribute: split text_genre → struct "text", attr "genre"
    auto us = attr_name.find('_');
    if (us != std::string::npos) {
        std::string struct_name = attr_name.substr(0, us);
        std::string region_attr = attr_name.substr(us + 1);
        if (corpus.has_structure(struct_name)) {
            const auto& sa = corpus.structure(struct_name);
            if (sa.has_region_attr(region_attr)) {
                bool is_mv = corpus.is_multivalue(attr_name);
                std::vector<std::pair<std::string, size_t>> entries =
                    region_attr_show_values_mv(sa, region_attr, is_mv);

                size_t cap = (limit > 0) ? std::min(entries.size(), limit) : entries.size();
                out << "{\n  \"ok\": true,\n  \"operation\": \"values\",\n";
                out << "  \"result\": {\n";
                out << "    \"attr\": " << jstr(attr_name) << ",\n";
                out << "    \"type\": \"region\",\n";
                out << "    \"structure\": " << jstr(struct_name) << ",\n";
                out << "    \"region_attr\": " << jstr(region_attr) << ",\n";
                out << "    \"unique\": " << entries.size() << ",\n";
                out << "    \"returned\": " << cap << ",\n";
                out << "    \"values\": [\n";
                for (size_t i = 0; i < cap; ++i) {
                    if (i > 0) out << ",\n";
                    out << "      {\"value\": " << jstr(entries[i].first)
                        << ", \"count\": " << entries[i].second << "}";
                }
                out << "\n    ]\n  }\n}\n";
                return out.str();
            }
        }
    }

    return {};  // not found
}

std::string to_regions_json(const Corpus& corpus, const std::string& type_name, size_t limit) {
    if (!corpus.has_structure(type_name)) return {};

    const auto& sa = corpus.structure(type_name);
    const auto& ra_names = sa.region_attr_names();
    size_t n = sa.region_count();
    size_t cap = (limit > 0) ? std::min(n, limit) : n;

    std::ostringstream out;
    out << "{\n  \"ok\": true,\n  \"operation\": \"regions\",\n";
    out << "  \"result\": {\n";
    out << "    \"type\": " << jstr(type_name) << ",\n";
    out << "    \"total\": " << n << ",\n";
    out << "    \"returned\": " << cap << ",\n";
    out << "    \"regions\": [\n";
    for (size_t i = 0; i < cap; ++i) {
        Region rgn = sa.get(i);
        if (i > 0) out << ",\n";
        out << "      {\"index\": " << i
            << ", \"start\": " << rgn.start
            << ", \"end\": " << rgn.end
            << ", \"tokens\": " << (rgn.end - rgn.start + 1);
        for (const auto& attr : ra_names) {
            std::string_view v = sa.region_value(attr, i);
            out << ", " << jstr(type_name + "_" + attr) << ": " << jstr(std::string(v));
        }
        out << "}";
    }
    out << "\n    ]\n  }\n}\n";
    return out.str();
}

} // namespace manatree
