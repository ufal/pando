#include "api/query_json.h"
#include "core/json_utils.h"
#include "query/parser.h"
#include <sstream>
#include <chrono>

namespace manatree {

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
    out << "]\n  }\n}\n";
    return out.str();
}

} // namespace manatree
