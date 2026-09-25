// Pando HTTP API server — POST /query, GET /info, GET /health
// Uses a thread pool so multiple requests are handled in parallel.

#include "api/query_json.h"
#include "core/json_utils.h"
#include "corpus/corpus.h"
#include <httplib.h>
#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <thread>

using namespace pando;

static unsigned default_thread_pool_size() {
    unsigned n = std::thread::hardware_concurrency();
    return (n > 0) ? std::max(2u, n) : 4u;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: pando-server <corpus_dir> [port] [threads] [--preload]\n";
        std::cerr << "  Default port: 8765, threads: " << default_thread_pool_size() << "\n";
        std::cerr << "  Open matches CLI (lazy mmap). Pass --preload to warm all pages at startup.\n";
        return 1;
    }
    std::string corpus_dir = argv[1];
    int port = 8765;
    unsigned nthreads = default_thread_pool_size();
    bool preload = false;
    int positional = 0;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--preload") {
            preload = true;
            continue;
        }
        if (a == "--no-preload") {
            preload = false;
            continue;
        }
        if (a.rfind("--", 0) == 0) {
            std::cerr << "Unknown option: " << a << "\n";
            return 1;
        }
        if (positional == 0) {
            port = std::atoi(a.c_str());
            ++positional;
        } else if (positional == 1) {
            int t = std::atoi(a.c_str());
            if (t > 0) nthreads = static_cast<unsigned>(t);
            ++positional;
        }
    }

    Corpus corpus;
    try {
        // Default: same as CLI (lazy mmap). Full preload is optional and can take
        // tens of seconds / GBs RSS on mid-size UD; queries are already fast without it.
        corpus.open(corpus_dir, preload);
    } catch (const std::exception& e) {
        std::cerr << "Failed to open corpus at " << corpus_dir << ": " << e.what() << "\n";
        return 1;
    }

    httplib::Server svr;
    svr.new_task_queue = [nthreads]() {
        return new httplib::ThreadPool(static_cast<size_t>(nthreads));
    };

    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{\"ok\":true,\"status\":\"ok\"}\n", "application/json");
    });

    svr.Get("/info", [&corpus](const httplib::Request&, httplib::Response& res) {
        res.set_content(to_info_json(corpus), "application/json");
    });

    // GET /values/:attr — unique values + counts for a positional or region attribute
    svr.Get(R"(/values/(\w+))", [&corpus](const httplib::Request& req, httplib::Response& res) {
        std::string attr_name = req.matches[1];
        size_t limit = 0;
        if (req.has_param("limit"))
            limit = static_cast<size_t>(std::strtoull(req.get_param_value("limit").c_str(), nullptr, 10));
        std::string json = to_values_json(corpus, attr_name, limit);
        if (json.empty()) {
            res.status = 404;
            res.set_content("{\"ok\":false,\"error\":\"Unknown attribute: " + attr_name + "\"}\n",
                            "application/json");
        } else {
            res.set_content(json, "application/json");
        }
    });

    // GET /regions/:type — list all regions of a given type with their attributes
    svr.Get(R"(/regions/(\w+))", [&corpus](const httplib::Request& req, httplib::Response& res) {
        std::string type_name = req.matches[1];
        size_t limit = 0;
        if (req.has_param("limit"))
            limit = static_cast<size_t>(std::strtoull(req.get_param_value("limit").c_str(), nullptr, 10));
        std::string json = to_regions_json(corpus, type_name, limit);
        if (json.empty()) {
            res.status = 404;
            res.set_content("{\"ok\":false,\"error\":\"Unknown structure type: " + type_name + "\"}\n",
                            "application/json");
        } else {
            res.set_content(json, "application/json");
        }
    });

    // GET /context?pos=&left=&right=&sentence= — KWIC window around one position
    // (KonText widectx; Manatee CorpRegion cannot address Pando toknums).
    svr.Get("/context", [&corpus](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("pos")) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"error\":\"missing 'pos'\"}\n", "application/json");
            return;
        }
        CorpusPos pos = static_cast<CorpusPos>(
            std::strtoull(req.get_param_value("pos").c_str(), nullptr, 10));
        if (pos >= corpus.size()) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"error\":\"pos out of range\"}\n", "application/json");
            return;
        }
        int left = 40;
        int right = 40;
        if (req.has_param("left"))
            left = static_cast<int>(std::strtol(req.get_param_value("left").c_str(), nullptr, 10));
        if (req.has_param("right"))
            right = static_cast<int>(std::strtol(req.get_param_value("right").c_str(), nullptr, 10));
        bool sentence = false;
        if (req.has_param("sentence")) {
            std::string s = req.get_param_value("sentence");
            sentence = (s == "1" || s == "true" || s == "yes");
        }
        if (left < 0) left = 0;
        if (right < 0) right = 0;
        KwicContext ctx = build_context_at(corpus, pos, left, right, sentence);
        std::ostringstream out;
        out << "{\"ok\":true,\"pos\":" << pos
            << ",\"left\":" << jstr(ctx.left)
            << ",\"match\":" << jstr(ctx.match)
            << ",\"right\":" << jstr(ctx.right)
            << ",\"corpus_size\":" << corpus.size() << "}\n";
        res.set_content(out.str(), "application/json");
    });

    // POST /run — run a full CQL program (queries + commands), session-aware
    // Body: {"cql": "...", "limit": 20, "offset": 0, ...}
    // Maintains per-server session for named queries.
    ProgramSession program_session;
    svr.Post("/run", [&corpus, &program_session](const httplib::Request& req, httplib::Response& res) {
        const std::string& body = req.body;

        std::string cql = json_extract_str(body, "cql");
        if (cql.empty()) cql = json_extract_str(body, "query");
        if (cql.empty()) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"error\":\"missing 'cql' field\"}\n", "application/json");
            return;
        }

        ProgramOptions opts;
        opts.limit      = json_extract_num(body, "limit", 20);
        opts.offset     = json_extract_num(body, "offset", 0);
        opts.max_total  = json_extract_num(body, "max_total", 0);
        opts.context    = static_cast<int>(json_extract_num(body, "context", 5));
        opts.total      = json_extract_bool(body, "total", false);
        opts.group_limit = json_extract_num(body, "group_limit", 1000);
        opts.strict_quoted_strings = json_extract_bool(body, "strict_quoted_strings", false);

        std::string json = run_program_json(corpus, program_session, cql, opts);
        res.set_content(json, "application/json");
    });

    svr.Post("/query", [&corpus](const httplib::Request& req, httplib::Response& res) {
        // Expect JSON body with optional: query, limit, offset, total, max_total, context, debug
        // Whitespace-tolerant (Python json.dumps emits spaces after ':' / ',').
        const std::string& body = req.body;
        QueryOptions opts;

        std::string q = json_extract_str(body, "query");
        std::string query_text = q.empty() ? "[]" : q;
        opts.limit     = json_extract_num(body, "limit", 20);
        opts.offset    = json_extract_num(body, "offset", 0);
        opts.max_total = json_extract_num(body, "max_total", 0);
        opts.total     = json_extract_bool(body, "total", false);
        opts.context   = static_cast<int>(json_extract_num(body, "context", 5));
        opts.debug     = json_extract_bool(body, "debug", false);
        opts.sentence  = json_extract_bool(body, "sentence", false);
        opts.strict_quoted_strings = json_extract_bool(body, "strict_quoted_strings", false);
        std::string attrs_str = json_extract_str(body, "attrs");
        opts.attrs.clear();
        if (!attrs_str.empty()) {
            for (size_t pos = 0; ; ) {
                size_t comma = attrs_str.find(',', pos);
                std::string part = attrs_str.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                while (!part.empty() && part.front() == ' ') part.erase(0, 1);
                while (!part.empty() && part.back() == ' ') part.erase(part.size() - 1, 1);
                if (!part.empty()) opts.attrs.push_back(part);
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        }

        try {
            auto [ms, elapsed] = run_single_query(corpus, query_text, opts);
            std::string json = to_query_result_json(corpus, query_text, ms, opts, elapsed);
            res.set_content(json, "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"error\":\"" + json_escape(e.what()) + "\"}\n",
                            "application/json");
        }
    });

    std::cerr << "Pando server: corpus " << corpus_dir << ", port " << port
              << ", threads " << nthreads
              << (preload ? ", preload=on" : ", preload=off (lazy mmap)") << "\n";
    if (!svr.listen("0.0.0.0", static_cast<int>(port))) {
        std::cerr << "Failed to listen on port " << port << "\n";
        return 1;
    }
    return 0;
}
