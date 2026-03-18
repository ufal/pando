#include "corpus/corpus.h"
#include "core/json_utils.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/executor.h"
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <map>
#include <iomanip>
#include <vector>

using namespace manatree;

// ── CLI options ─────────────────────────────────────────────────────────

struct Options {
    std::string corpus_dir;
    std::string query;
    bool json    = false;
    // Debug verbosity: 0 = off, 1 = basic (plan, timing), 2+ = more detail in future
    int  debug_level = 0;
    size_t limit  = 20;
    size_t offset = 0;
    size_t max_total = 0;  // 0 = no cap; with --total, stop counting at this (e.g. 10000)
    int context   = 5;
    bool interactive = false;
    bool total    = false;
    std::vector<std::string> attrs;  // empty = all attributes in JSON tokens; else only these
    bool count_only = false;  // print only total (for benchmarking)
    bool timing     = false;  // print open_sec, query_sec, fetch_sec, total, returned to stderr
    size_t sample   = 0;      // return N randomly sampled matches (reservoir sampling)
    uint32_t sample_seed = 0; // RNG seed for --sample (0 = non-deterministic)
    unsigned threads = 1;    // parallel seed processing when > 1 (multi-token queries)
    bool preload    = false; // load all mmap'd pages into RAM at open (slower open, faster first query)
    int  max_gap    = REPEAT_UNBOUNDED; // cap for + and * quantifiers (--max-gap)
    // For aggregation commands (count/group/freq), cap number of output rows; 0 = no cap.
    size_t group_limit = 1000;
    // API mode: like --json but with cleaner, single-object responses for programmatic use
    bool api = false;
};

struct QueryTiming {
    double open_sec = 0, query_sec = 0, fetch_sec = 0;
    size_t total = 0, returned = 0;
};

// ── JSON output ─────────────────────────────────────────────────────────

static void emit_json(const Corpus& corpus, const std::string& query_text,
                      const MatchSet& ms, const Options& opts,
                      double elapsed_ms) {
    // #16: Parallel (Source | Target) result
    if (!ms.parallel_matches.empty()) {
        size_t stored = ms.parallel_matches.size();
        size_t start = std::min(opts.offset, stored);
        size_t end   = std::min(start + opts.limit, stored);
        std::cout << "{\n  \"ok\": true,\n  \"backend\": \"manatree\",\n  \"operation\": \"query\",\n";
        std::cout << "  \"result\": {\n    \"parallel\": true,\n";
        std::cout << "    \"page\": {\"start\": " << start << ", \"size\": " << opts.limit
                  << ", \"returned\": " << (end - start) << ", \"total\": " << ms.total_count
                  << ", \"total_exact\": " << (ms.total_exact ? "true" : "false") << "},\n";
        std::cout << "    \"pairs\": [\n";
        for (size_t i = start; i < end; ++i) {
            const auto& [s, t] = ms.parallel_matches[i];
            auto doc_s = lookup_doc_id(corpus, s.first_pos());
            auto doc_t = lookup_doc_id(corpus, t.first_pos());
            CorpusPos ms_s = s.first_pos();
            CorpusPos me_s = s.last_pos();
            CorpusPos ms_t = t.first_pos();
            CorpusPos me_t = t.last_pos();
            if (i > start) std::cout << ",\n";
            std::cout << "      {\"source\": {\"doc_id\": " << (doc_s.empty() ? "null" : jstr(doc_s))
                      << ", \"match_start\": " << ms_s << ", \"match_end\": " << me_s << "}"
                      << ", \"target\": {\"doc_id\": " << (doc_t.empty() ? "null" : jstr(doc_t))
                      << ", \"match_start\": " << ms_t << ", \"match_end\": " << me_t << "}}";
        }
        std::cout << "\n    ]\n  }\n}\n";
        return;
    }

    size_t stored = ms.matches.size();
    size_t start = std::min(opts.offset, stored);
    size_t end   = std::min(start + opts.limit, stored);
    size_t returned = end - start;

    std::cout << "{\n";
    std::cout << "  \"ok\": true,\n";
    std::cout << "  \"backend\": \"manatree\",\n";
    std::cout << "  \"operation\": \"query\",\n";
    std::cout << "  \"result\": {\n";
    std::cout << "    \"query\": {\"language\": \"clickcql\", \"text\": "
              << jstr(query_text) << "},\n";
    std::cout << "    \"page\": {\"start\": " << start
              << ", \"size\": " << opts.limit
              << ", \"returned\": " << returned
              << ", \"total\": " << ms.total_count
              << ", \"total_exact\": " << (ms.total_exact ? "true" : "false")
              << "},\n";

    std::cout << "    \"hits\": [\n";
    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];
        CorpusPos match_start = m.first_pos();
        CorpusPos match_end   = m.last_pos();
        auto doc_id = lookup_doc_id(corpus, match_start);
        auto ctx = build_context(corpus, m, opts.context);

        if (i > start) std::cout << ",\n";
        std::cout << "      {";
        std::cout << "\"doc_id\": " << (doc_id.empty() ? "null" : jstr(doc_id));
        std::cout << ", \"match_start\": " << match_start;
        std::cout << ", \"match_end\": " << match_end;
        std::cout << ", \"context\": {"
                  << "\"left\": " << jstr(ctx.left)
                  << ", \"match\": " << jstr(ctx.match)
                  << ", \"right\": " << jstr(ctx.right) << "}";

        std::cout << ", \"tokens\": [";
        const auto& attr_names = opts.attrs.empty()
            ? corpus.attr_names() : opts.attrs;
        bool first_tok = true;
        for (size_t t = 0; t < m.positions.size(); ++t) {
            if (m.positions[t] == NO_HEAD) continue;
            CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
            for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                if (!first_tok) std::cout << ", ";
                first_tok = false;
                std::cout << "{\"pos\": " << p;
                for (const auto& attr_name : attr_names) {
                    if (!corpus.has_attr(attr_name)) continue;
                    auto val = corpus.attr(attr_name).value_at(p);
                    if (val == "_") continue;
                    std::cout << ", " << jstr(attr_name) << ": " << jstr(val);
                }
                std::cout << "}";
            }
        }
        std::cout << "]";

        std::cout << "}";
    }
    std::cout << "\n    ]";

    if (opts.debug_level > 0) {
        std::cout << ",\n    \"debug\": {\n";
        std::cout << "      \"corpus_size\": " << corpus.size() << ",\n";
        std::cout << "      \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
        std::cout << "      \"elapsed_ms\": " << elapsed_ms << ",\n";
        std::cout << "      \"seed_token\": " << ms.seed_token << ",\n";
        std::cout << "      \"cardinalities\": [";
        for (size_t i = 0; i < ms.cardinalities.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << ms.cardinalities[i];
        }
        std::cout << "],\n";

        std::cout << "      \"attributes\": [";
        const auto& names = corpus.attr_names();
        for (size_t i = 0; i < names.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(names[i]);
        }
        std::cout << "],\n";

        std::cout << "      \"structures\": [";
        bool first_s = true;
        for (const auto& sn : {"s", "text"}) {
            if (corpus.has_structure(sn)) {
                if (!first_s) std::cout << ", ";
                const auto& sa = corpus.structure(sn);
                std::cout << "{\"name\": " << jstr(sn)
                          << ", \"regions\": " << sa.region_count()
                          << ", \"has_values\": "
                          << (sa.has_values() ? "true" : "false") << "}";
                first_s = false;
            }
        }
        std::cout << "]\n";
        std::cout << "    }";
    }

    std::cout << "\n  }\n}\n";
}

// ── KWIC text output (default) ──────────────────────────────────────────

static void emit_kwic(const Corpus& corpus, const std::string& query_text,
                      const MatchSet& ms, const Options& opts,
                      double elapsed_ms) {
    if (!ms.parallel_matches.empty()) {
        size_t start = std::min(opts.offset, ms.parallel_matches.size());
        size_t end   = std::min(start + opts.limit, ms.parallel_matches.size());
        const auto& form = corpus.attr("form");
        for (size_t i = start; i < end; ++i) {
            const auto& [s, t] = ms.parallel_matches[i];
            std::string src_str, tgt_str;
            for (CorpusPos p : s.positions) src_str += (src_str.empty() ? "" : " ") + std::string(form.value_at(p));
            for (CorpusPos p : t.positions) tgt_str += (tgt_str.empty() ? "" : " ") + std::string(form.value_at(p));
            std::cout << src_str << " | " << tgt_str << "\n";
        }
        std::cout << "Total: " << ms.total_count << " aligned pairs\n";
        return;
    }

    size_t stored = ms.matches.size();
    size_t start = std::min(opts.offset, stored);
    size_t end   = std::min(start + opts.limit, stored);

    if (opts.debug_level > 0) {
        std::cerr << "Plan: seed=token[" << ms.seed_token << "]";
        for (size_t i = 0; i < ms.cardinalities.size(); ++i)
            std::cerr << (i == 0 ? " cardinalities=[" : ", ")
                      << ms.cardinalities[i];
        if (!ms.cardinalities.empty()) std::cerr << "]";
        std::cerr << "\n";
        std::cerr << "Time: " << elapsed_ms << " ms, "
                  << ms.total_count << (ms.total_exact ? "" : "+")
                  << " total matches\n";
    }

    if (ms.total_exact)
        std::cout << ms.total_count << " matches";
    else
        std::cout << ms.total_count << "+ matches";
    if (start > 0 || end < stored)
        std::cout << " (showing " << start << "-" << (end - 1) << ")";
    std::cout << "\n";

    const auto& form   = corpus.attr("form");
    bool has_upos   = corpus.has_attr("upos");
    bool has_deprel = corpus.has_attr("deprel");

    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];
        auto ctx = build_context(corpus, m, opts.context);

        auto doc_id = lookup_doc_id(corpus, m.first_pos());

        if (!doc_id.empty())
            std::cout << "[" << doc_id << "] ";

        std::cout << ctx.left << " <<< ";
        bool first_tok = true;
        for (size_t t = 0; t < m.positions.size(); ++t) {
            if (m.positions[t] == NO_HEAD) continue;
            CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
            for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                if (!first_tok) std::cout << " ";
                first_tok = false;
                std::cout << form.value_at(p);
                if (has_upos) std::cout << "/" << corpus.attr("upos").value_at(p);
                if (has_deprel) std::cout << "/" << corpus.attr("deprel").value_at(p);
            }
        }
        std::cout << " >>> " << ctx.right << "\n";
    }

    if (end < stored) {
        if (ms.total_exact)
            std::cout << "... (" << (ms.total_count - end) << " more)\n";
        else
            std::cout << "... (more available, use --total for exact count)\n";
    }
}

// ── Command handling ────────────────────────────────────────────────────

static void emit_info_json(const Corpus& corpus) {
    std::cout << "{\n  \"ok\": true,\n  \"operation\": \"info\",\n";
    std::cout << "  \"result\": {\n";
    std::cout << "    \"size\": " << corpus.size() << ",\n";
    std::cout << "    \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
    std::cout << "    \"attributes\": [";
    const auto& names = corpus.attr_names();
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << jstr(names[i]);
    }
    std::cout << "],\n";
    std::cout << "    \"structures\": [";
    const auto& s_names = corpus.structure_names();
    for (size_t i = 0; i < s_names.size(); ++i) {
        const auto& s = s_names[i];
        if (i > 0) std::cout << ", ";
        const auto& sa = corpus.structure(s);
        std::cout << "{"
                  << "\"name\": " << jstr(s) << ", "
                  << "\"regions\": " << sa.region_count() << ", "
                  << "\"has_values\": " << (sa.has_values() ? "true" : "false")
                  << "}";
    }
    std::cout << "],\n";
    std::cout << "    \"region_attrs\": [";
    const auto& ra = corpus.region_attr_names();
    for (size_t i = 0; i < ra.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << jstr(ra[i]);
    }
    std::cout << "]\n  }\n}\n";
}

// ── Aggregation helpers ─────────────────────────────────────────────────

static std::string read_field(const Corpus& corpus, const Match& m,
                              const std::string& field) {
    // #15: field can be:
    //   - "upos", "feats.Number" (positional)
    //   - "a.upos" (named token)
    //   - "match.text_langcode" (region attr)
    //   - "a.text_langcode" (region attr anchored at named token)

    CorpusPos pos = m.first_pos();
    std::string attr_spec = field;

    // Special-case "match." prefix for region attrs: match.<region_attr>
    if (field.rfind("match.", 0) == 0 && field.size() > 6) {
        attr_spec = field.substr(6);
    } else {
        // Named token prefix: a.attr
        auto dot = field.find('.');
        if (dot != std::string::npos && dot > 0) {
            std::string name = field.substr(0, dot);
            auto it = m.name_to_position.find(name);
            if (it != m.name_to_position.end()) {
                pos = it->second;
                attr_spec = field.substr(dot + 1);
            } else {
                attr_spec = field;
            }
        }
    }

    // Fast path (6g): if attr_spec is a known positional attribute, skip
    // region_attr scan — avoids O(region_attrs) work per match for count by lemma etc.
    std::string attr = attr_spec;
    if (attr.size() > 5 && attr.substr(0, 5) == "feats" && attr.find('.') != std::string::npos)
        attr[attr.find('.')] = '_';
    if (corpus.has_attr(attr)) {
        return std::string(corpus.attr(attr).value_at(pos));
    }

    // Region attribute? attr_spec matches one of corpus.region_attr_names(),
    // e.g. "text_langcode" or "par_id".
    const auto& ra_all = corpus.region_attr_names();
    for (const auto& ra_name : ra_all) {
        if (ra_name == attr_spec) {
            auto us = ra_name.find('_');
            if (us == std::string::npos || us + 1 >= ra_name.size())
                return "";
            std::string struct_name = ra_name.substr(0, us);
            std::string region_attr = ra_name.substr(us + 1);
            if (!corpus.has_structure(struct_name))
                return "";
            const auto& sa = corpus.structure(struct_name);
            if (!sa.has_region_attr(region_attr))
                return "";
            int64_t rgn = sa.find_region(pos);
            if (rgn < 0)
                return "";
            return std::string(sa.region_value(region_attr, static_cast<size_t>(rgn)));
        }
    }

    // Not a region attr; attr already normalized above, but has_attr was false
    return "";
}

static std::string make_key(const Corpus& corpus, const Match& m,
                            const std::vector<std::string>& fields) {
    std::string key;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) key += '\t';
        key += read_field(corpus, m, fields[i]);
    }
    return key;
}

static void emit_count(const Corpus& corpus, const MatchSet& ms,
                       const GroupCommand& cmd, const Options& opts) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: count/group requires 'by' clause\n";
        return;
    }

    std::map<std::string, size_t> counts;
    for (const auto& m : ms.matches)
        ++counts[make_key(corpus, m, cmd.fields)];

    // Sort by count descending
    std::vector<std::pair<std::string, size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    size_t total = ms.matches.size();

    // Pagination for groups: default to opts.group_limit (1000) when set; else all.
    size_t g_start = 0;
    size_t g_end   = sorted.size();
    if (opts.group_limit > 0 && opts.group_limit < g_end)
        g_end = opts.group_limit;

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"count\", \"result\": {\n";
        std::cout << "  \"total_matches\": " << total << ",\n";
        std::cout << "  \"groups\": " << sorted.size() << ",\n";
        std::cout << "  \"groups_returned\": " << (g_end - g_start) << ",\n";
        std::cout << "  \"fields\": [";
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(cmd.fields[i]);
        }
        std::cout << "],\n  \"rows\": [\n";
        for (size_t i = g_start; i < g_end; ++i) {
            if (i > g_start) std::cout << ",\n";
            double pct = 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total);
            std::cout << "    {\"key\": " << jstr(sorted[i].first)
                      << ", \"count\": " << sorted[i].second
                      << ", \"pct\": " << pct << "}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        // Header
        for (const auto& f : cmd.fields)
            std::cout << f << "\t";
        std::cout << "count\t%\n";

        size_t shown = 0;
        for (size_t i = g_start; i < g_end; ++i) {
            const auto& key = sorted[i].first;
            size_t count = sorted[i].second;
            double pct = 100.0 * static_cast<double>(count) / static_cast<double>(total);
            std::cout << key << "\t" << count << "\t";
            std::cout << std::fixed;
            std::cout.precision(1);
            std::cout << pct << "%\n";
            ++shown;
        }
        std::cout << "Total: " << total << " matches, " << sorted.size()
                  << " groups (showing " << shown << ")\n";
    }
}

static void emit_sort(const Corpus& corpus, MatchSet& ms,
                      const GroupCommand& cmd, const Options& opts) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: sort requires 'by' clause\n";
        return;
    }

    std::sort(ms.matches.begin(), ms.matches.end(),
              [&](const Match& a, const Match& b) {
                  return make_key(corpus, a, cmd.fields) < make_key(corpus, b, cmd.fields);
              });

    emit_kwic(corpus, "(sorted)", ms, opts, 0);
}

static void emit_size(const MatchSet& ms, const Options& opts) {
    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"size\", \"result\": "
                  << ms.matches.size() << "}\n";
    } else {
        std::cout << ms.matches.size() << " matches\n";
    }
}

// ── Query dispatch ──────────────────────────────────────────────────────

static void run_query(const Corpus& corpus, const std::string& input,
                      const Options& opts, QueryTiming* out_timing = nullptr) {
    Parser parser(input);
    Program prog = parser.parse();

    // Apply --max-gap: clamp unbounded repetition to user-configured cap
    if (opts.max_gap != REPEAT_UNBOUNDED) {
        for (auto& stmt : prog) {
            for (auto& tok : stmt.query.tokens) {
                if (tok.max_repeat == REPEAT_UNBOUNDED)
                    tok.max_repeat = opts.max_gap;
            }
            if (stmt.is_parallel) {
                for (auto& tok : stmt.target_query.tokens)
                    if (tok.max_repeat == REPEAT_UNBOUNDED)
                        tok.max_repeat = opts.max_gap;
            }
        }
    }

    QueryExecutor executor(corpus);

    MatchSet last_ms;
    bool has_last = false;

    for (size_t si = 0; si < prog.size(); ++si) {
        auto& stmt = prog[si];
        bool next_is_command = (si + 1 < prog.size() && prog[si + 1].has_command);

        if (stmt.has_query) {
            size_t max_m = 0;
            bool count_t = false;
            size_t max_total_cap = 0;
            if (opts.count_only) {
                max_m = 0;
                count_t = true;
            } else if (opts.sample > 0) {
                max_m = 0;
                count_t = true;  // need full count for reservoir sampling
            } else if (!next_is_command) {
                max_m = opts.offset + opts.limit;
                count_t = opts.total;
                max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
            }

            auto t0 = std::chrono::high_resolution_clock::now();
            if (stmt.is_parallel) {
                last_ms = executor.execute_parallel(stmt.query, stmt.target_query, max_m, count_t);
            } else {
                last_ms = executor.execute(stmt.query, max_m, count_t, max_total_cap,
                                          opts.sample, opts.sample_seed, opts.threads);
            }
            has_last = true;
            auto t1 = std::chrono::high_resolution_clock::now();
            double query_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
            if (out_timing) {
                out_timing->query_sec += query_ms / 1000.0;
                out_timing->total = last_ms.total_count;
                out_timing->returned = last_ms.matches.size();
            }

            if (!next_is_command) {
                if (opts.count_only) {
                    std::cout << last_ms.total_count << "\n";
                    return;
                }
                if (out_timing) {
                    auto t2 = std::chrono::high_resolution_clock::now();
                    if (opts.json)
                        emit_json(corpus, input, last_ms, opts, query_ms);
                    else
                        emit_kwic(corpus, input, last_ms, opts, query_ms);
                    auto t3 = std::chrono::high_resolution_clock::now();
                    out_timing->fetch_sec = std::chrono::duration<double, std::milli>(t3 - t2).count() / 1000.0;
                } else {
                    if (opts.json)
                        emit_json(corpus, input, last_ms, opts, query_ms);
                    else
                        emit_kwic(corpus, input, last_ms, opts, query_ms);
                }
            }
        }

        if (stmt.has_command) {
            switch (stmt.command.type) {
                case CommandType::COUNT:
                case CommandType::GROUP:
                    if (!has_last) { std::cerr << "No query to count\n"; break; }
                    emit_count(corpus, last_ms, stmt.command, opts);
                    break;

                case CommandType::SORT:
                    if (!has_last) { std::cerr << "No query to sort\n"; break; }
                    emit_sort(corpus, last_ms, stmt.command, opts);
                    break;

                case CommandType::SIZE:
                    if (has_last)
                        emit_size(last_ms, opts);
                    else if (opts.json)
                        emit_info_json(corpus);
                    else
                        std::cout << "Corpus size: " << corpus.size() << " tokens\n";
                    break;

                case CommandType::SHOW_ATTRS:
                    std::cout << "Positional attributes:\n";
                    for (const auto& a : corpus.attr_names())
                        std::cout << "  " << a << "\n";
                    break;

                case CommandType::SHOW_REGIONS:
                    if (opts.json) {
                        std::cout << "{\n  \"ok\": true,\n  \"operation\": \"show_regions\",\n";
                        std::cout << "  \"result\": {\n";
                        std::cout << "    \"structures\": [";
                        const auto& s_names = corpus.structure_names();
                        for (size_t i = 0; i < s_names.size(); ++i) {
                            const auto& s = s_names[i];
                            if (i > 0) std::cout << ", ";
                            const auto& sa = corpus.structure(s);
                            std::cout << "{"
                                      << "\"name\": " << jstr(s) << ", "
                                      << "\"regions\": " << sa.region_count() << ", "
                                      << "\"has_values\": " << (sa.has_values() ? "true" : "false") << ", "
                                      << "\"attrs\": [";
                            const auto& ra = sa.region_attr_names();
                            for (size_t j = 0; j < ra.size(); ++j) {
                                if (j > 0) std::cout << ", ";
                                std::cout << jstr(ra[j]);
                            }
                            std::cout << "]}";
                        }
                        std::cout << "],\n";
                        std::cout << "    \"region_attrs\": [";
                        const auto& ra_all = corpus.region_attr_names();
                        for (size_t i = 0; i < ra_all.size(); ++i) {
                            if (i > 0) std::cout << ", ";
                            std::cout << jstr(ra_all[i]);
                        }
                        std::cout << "]\n  }\n}\n";
                    } else {
                        std::cout << "Structural attributes:\n";
                        const auto& s_names = corpus.structure_names();
                        for (const auto& s : s_names) {
                            const auto& sa = corpus.structure(s);
                            std::cout << "  " << s << " (regions=" << sa.region_count()
                                      << ", has_values=" << (sa.has_values() ? "yes" : "no") << ")\n";
                            const auto& ra = sa.region_attr_names();
                            if (!ra.empty()) {
                                std::cout << "    attrs:";
                                for (const auto& a : ra)
                                    std::cout << " " << a;
                                std::cout << "\n";
                            }
                        }
                    }
                    break;

                default:
                    std::cout << "(command not yet implemented)\n";
                    break;
            }
        }
    }
}

// ── Argument parsing ────────────────────────────────────────────────────

static Options parse_args(int argc, char* argv[]) {
    Options opts;
    std::vector<std::string> positional;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--json")        { opts.json = true; }
        else if (arg == "--api")    { opts.api = true; opts.json = true; }
        // --debug  (optional level), e.g. --debug, --debug 2, or --debug=2
        else if (arg == "--debug")  {
            // bare --debug → level 1 (unless already higher)
            if (opts.debug_level < 1) opts.debug_level = 1;
            // If next arg looks like a number, treat it as level
            if (i + 1 < argc && argv[i + 1][0] >= '0' && argv[i + 1][0] <= '9') {
                try {
                    opts.debug_level = std::stoi(argv[++i]);
                } catch (...) {
                    // ignore parse error, keep level 1
                }
            }
        }
        else if (arg.rfind("--debug=", 0) == 0) {
            std::string lvl = arg.substr(8);
            try {
                opts.debug_level = std::stoi(lvl);
            } catch (...) {
                opts.debug_level = 1;
            }
        }
        else if (arg == "--total")  { opts.total = true; }
        else if (arg == "--max-total" && i + 1 < argc) { opts.max_total = std::stoul(argv[++i]); }
        else if (arg == "--limit"  && i + 1 < argc) { opts.limit  = std::stoul(argv[++i]); }
        else if (arg == "--offset" && i + 1 < argc) { opts.offset = std::stoul(argv[++i]); }
        else if (arg == "--context" && i + 1 < argc) { opts.context = std::stoi(argv[++i]); }
        else if (arg == "--count-only") { opts.count_only = true; }
        else if (arg == "--timing")     { opts.timing = true; }
        else if (arg == "--sample" && i + 1 < argc) { opts.sample = std::stoul(argv[++i]); }
        else if (arg == "--seed" && i + 1 < argc)  { opts.sample_seed = static_cast<uint32_t>(std::stoul(argv[++i])); }
        else if (arg == "--threads" && i + 1 < argc) { opts.threads = static_cast<unsigned>(std::stoul(argv[++i])); }
        else if (arg == "--preload") { opts.preload = true; }
        else if (arg == "--max-gap" && i + 1 < argc) { opts.max_gap = std::stoi(argv[++i]); }
        else if (arg == "--attrs" && i + 1 < argc) {
            std::string list = argv[++i];
            opts.attrs.clear();
            for (size_t pos = 0; ; ) {
                size_t comma = list.find(',', pos);
                std::string part = list.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                while (!part.empty() && part.front() == ' ') part.erase(0, 1);
                while (!part.empty() && part.back() == ' ') part.erase(part.size() - 1, 1);
                if (!part.empty()) opts.attrs.push_back(part);
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        }
        else if (arg.substr(0, 2) == "--") {
            std::cerr << "Unknown option: " << arg << "\n";
        } else {
            positional.push_back(arg);
        }
    }

    if (positional.empty()) {
        std::cerr << "Usage: pando [options] <corpus_dir> [query]\n\n"
                  << "Options:\n"
                  << "  --json           Output as JSON (human-/tool-friendly)\n"
                  << "  --api            API mode: JSON only, single-object responses\n"
                  << "  --debug[=N]      Include debug info (plan, timing, cardinalities); N>=1 for verbosity\n"
                  << "  --total          Compute exact total match count even with --limit\n"
                  << "  --max-total N    Cap total count at N when using --total (fast UI total)\n"
                  << "  --limit N        Max hits to return (default: 20)\n"
                  << "  --offset N       Skip first N hits (default: 0)\n"
                  << "  --context N      Context width in tokens (default: 5)\n"
                  << "  --attrs A,B,...  JSON token attributes only (default: all)\n"
                  << "  --count-only     Print only total match count (for benchmarking)\n"
                  << "  --timing         Print open_sec, query_sec, fetch_sec, total, returned to stderr\n"
                  << "  --sample N       Return N randomly sampled matches (reservoir sampling)\n"
                  << "  --seed N         RNG seed for --sample (reproducible runs)\n"
                  << "  --threads N      Parallel seed processing for multi-token queries (default: 1)\n"
                  << "  --max-gap N      Cap for + and * quantifiers (default: " << REPEAT_UNBOUNDED << ")\n";
        std::exit(1);
    }

    opts.corpus_dir = positional[0];
    if (positional.size() > 1) {
        for (size_t i = 1; i < positional.size(); ++i) {
            if (i > 1) opts.query += " ";
            opts.query += positional[i];
        }
    } else {
        opts.interactive = true;
    }

    return opts;
}

// ── Main ────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    Options opts = parse_args(argc, argv);

    QueryTiming timing;
    if (opts.timing)
        timing.open_sec = 0;  // set below

    Corpus corpus;
    try {
        auto t_open0 = std::chrono::high_resolution_clock::now();
        corpus.open(opts.corpus_dir, opts.preload);
        if (opts.timing)
            timing.open_sec = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - t_open0).count();
        // In API/JSON modes, don't print human-facing corpus-loaded line
        if (!opts.json && !opts.api && !opts.count_only)
            std::cerr << "Corpus loaded: " << corpus.size() << " tokens, "
                      << corpus.attr_names().size() << " attributes\n";
    } catch (const std::exception& e) {
        if (opts.json || opts.api) {
            std::cout << "{\"ok\": false, \"error\": {\"stage\": \"open\", \"message\": "
                      << jstr(e.what()) << "}}\n";
        } else {
            std::cerr << "Error loading corpus: " << e.what() << "\n";
        }
        return 1;
    }

    if (!opts.interactive) {
        try {
            run_query(corpus, opts.query, opts, opts.timing ? &timing : nullptr);
            if (opts.timing)
                std::cerr << "open_sec=" << std::fixed << std::setprecision(3) << timing.open_sec
                          << " query_sec=" << timing.query_sec
                          << " fetch_sec=" << timing.fetch_sec
                          << " total=" << timing.total
                          << " returned=" << timing.returned << "\n";
        } catch (const std::exception& e) {
            if (opts.json || opts.api) {
                std::cout << "{\"ok\": false, \"error\": {\"stage\": \"query\", \"message\": "
                          << jstr(e.what()) << "}}\n";
            } else {
                std::cerr << "Query error: " << e.what() << "\n";
            }
            return 1;
        }
    } else {
        std::string line;
        std::cout << "manatree> " << std::flush;
        while (std::getline(std::cin, line)) {
            if (line.empty() || line == "quit" || line == "exit") break;
            try {
                run_query(corpus, line, opts);
            } catch (const std::exception& e) {
                if (opts.json || opts.api) {
                    std::cout << "{\"ok\": false, \"error\": {\"stage\": \"query\", \"message\": "
                              << jstr(e.what()) << "}}\n";
                } else {
                    std::cerr << "Error: " << e.what() << "\n";
                }
            }
            if (!opts.json && !opts.api) std::cout << "manatree> " << std::flush;
        }
    }

    return 0;
}
