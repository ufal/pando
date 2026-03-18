// Pando HTTP API server — POST /query, GET /info, GET /health
// Uses a thread pool so multiple requests are handled in parallel.

#include "api/query_json.h"
#include "core/json_utils.h"
#include "corpus/corpus.h"
#include <httplib.h>
#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>

using namespace manatree;

static unsigned default_thread_pool_size() {
    unsigned n = std::thread::hardware_concurrency();
    return (n > 0) ? std::max(2u, n) : 4u;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: pando-server <corpus_dir> [port] [threads]\n";
        std::cerr << "  Default port: 8765, threads: " << default_thread_pool_size() << "\n";
        return 1;
    }
    std::string corpus_dir = argv[1];
    int port = argc >= 3 ? std::atoi(argv[2]) : 8765;
    unsigned nthreads = default_thread_pool_size();
    if (argc >= 4) {
        int t = std::atoi(argv[3]);
        if (t > 0) nthreads = static_cast<unsigned>(t);
    }

    Corpus corpus;
    try {
        // Preload by default: server keeps corpus open and serves many requests; warm pages at startup.
        corpus.open(corpus_dir, true);
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

    svr.Post("/query", [&corpus](const httplib::Request& req, httplib::Response& res) {
        // Expect JSON body with optional: query, limit, offset, total, max_total, context, debug
        std::string query_text = "[]";
        QueryOptions opts;
        // Minimal JSON parsing: look for "query":"...", "limit":N, etc.
        const std::string& body = req.body;
        auto extract_str = [&body](const char* key) -> std::string {
            std::string search = std::string("\"") + key + "\":\"";
            auto pos = body.find(search);
            if (pos == std::string::npos) return "";
            pos += search.size();
            std::string val;
            while (pos < body.size()) {
                char c = body[pos++];
                if (c == '"') break;
                if (c == '\\' && pos < body.size()) c = body[pos++];
                val += c;
            }
            return val;
        };
        auto extract_num = [&body](const char* key, size_t default_val) -> size_t {
            std::string search = std::string("\"") + key + "\":";
            auto pos = body.find(search);
            if (pos == std::string::npos) return default_val;
            pos += search.size();
            return static_cast<size_t>(std::strtoull(body.c_str() + pos, nullptr, 10));
        };
        auto extract_bool = [&body](const char* key, bool default_val) -> bool {
            std::string search = std::string("\"") + key + "\":";
            auto pos = body.find(search);
            if (pos == std::string::npos) return default_val;
            pos += search.size();
            return body.substr(pos, 4) == "true";
        };

        std::string q = extract_str("query");
        if (!q.empty()) query_text = q;
        opts.limit     = extract_num("limit", 20);
        opts.offset    = extract_num("offset", 0);
        opts.max_total = extract_num("max_total", 0);
        opts.total     = extract_bool("total", false);
        opts.context   = static_cast<int>(extract_num("context", 5));
        opts.debug     = extract_bool("debug", false);
        std::string attrs_str = extract_str("attrs");
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

    std::cerr << "Manatree server: corpus " << corpus_dir << ", port " << port
              << ", threads " << nthreads << "\n";
    if (!svr.listen("0.0.0.0", static_cast<int>(port))) {
        std::cerr << "Failed to listen on port " << port << "\n";
        return 1;
    }
    return 0;
}
