// Pando HTTP API server — POST /query, GET /status, POST /cancel, GET /jobs,
// GET /info, GET /health, …
// Uses a thread pool so multiple requests are handled in parallel; exact
// totals can be computed in the background ("total": "async", P6.2).

#include "api/server_api.h"
#include "core/build_info.h"
#include "core/json_utils.h"
#include "corpus/corpus.h"
#include <httplib.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <thread>
#include <algorithm>
#include <chrono>
#include <map>
#include <optional>
#include <cstdio>
#include <ctime>

using namespace pando;

static unsigned default_thread_pool_size() {
    unsigned n = std::thread::hardware_concurrency();
    return (n > 0) ? std::max(2u, n) : 4u;
}

int main(int argc, char* argv[]) {
    if (argc == 2 && (std::string(argv[1]) == "--version" || std::string(argv[1]) == "-V")) {
        std::cout << "pando-server " << build_string() << "\n";
        return 0;
    }
    if (argc < 2) {
        std::cerr << "Usage: pando-server <corpus_dir> [port] [threads] [--preload] [options]\n";
        std::cerr << "  Default port: 8765, threads: " << default_thread_pool_size() << "\n";
        std::cerr << "  Open matches CLI (lazy mmap). Pass --preload to warm all pages at startup.\n";
        std::cerr << "  Background totals (/query \"total\": \"async\", /status, /cancel):\n"
                  << "    --total-workers N       concurrent background counts (default 2)\n"
                  << "    --result-cache N        cached query results (default 512)\n"
                  << "    --result-ttl SEC        drop an unused finished result after SEC (default 3600)\n"
                  << "    --abandon-after SEC     cancel a count nobody polled for SEC (default 120; 0 = never)\n"
                  << "    --debug-total-delay MS  testing: reveal every total gradually over MS\n"
                  << "  --cache-mb MB               recent pages, count / freq / coll results and sort indexes\n"
                  << "                              kept for repeated requests (default 128; 0 = off)\n"
                  << "  --query-timeout MS          default /query time limit (0 = none; per request \"timeout_ms\")\n"
                  << "  --query-threads N           count a total / count by over N position ranges in parallel\n"
                  << "                              (default 1: one thread per query)\n"
                  << "  Client sessions (POST /session, \"session_id\" on /run and /query):\n"
                  << "    --session-ttl SEC       close a session unused for SEC (default 1800)\n"
                  << "    --max-sessions N        open sessions (default 256; the least recently used idle one makes room)\n"
                  << "    --session-memory MB     materialised hits over all sessions (default 2048; 0 = no limit)\n"
                  << "    --session-max-hits N    hits one stored set may materialise (default 5000000; 0 = no limit)\n"
                  << "  Limits by tier (\"tier\" per request: visitor / user / admin, …):\n"
                  << "    --limits FILE           JSON server options: {\"tiers\": {\"visitor\": {\"timeout_ms\": …,\n"
                  << "                            \"max_count_hits\": …, \"deny\": [\"transitive\"]}, …},\n"
                  << "                            \"default_tier\": \"visitor\", \"trust_tier\": true} (wiki: CLI reference)\n"
                  << "    --trust-tier            honour the request's \"tier\" (only behind a front-end that sets it)\n"
                  << "    --warm hot|all|none     read the index files most queries touch (hot) or all\n"
                  << "                            into the page cache in the background (POST /warm later)\n";
        return 1;
    }
    std::string corpus_dir = argv[1];
    int port = 8765;
    unsigned nthreads = default_thread_pool_size();
    bool preload = false;
    int positional = 0;
    QueryJobConfig job_cfg;
    size_t query_timeout_ms = 0;
    unsigned query_threads = 1;
    SessionConfig sess_cfg;
    size_t cache_bytes = ServerConfig{}.cache_bytes;
    std::string limits_file;
    bool trust_tier = false;
    WarmLevel warm = WarmLevel::None;
    bool warm_given = false;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        auto num_arg = [&](long long& out) -> bool {
            if (i + 1 >= argc) {
                std::cerr << a << " needs a value\n";
                return false;
            }
            out = std::atoll(argv[++i]);
            return true;
        };
        long long v = 0;
        if (a == "--total-workers" || a == "--result-cache" || a == "--result-ttl"
            || a == "--abandon-after" || a == "--debug-total-delay" || a == "--query-timeout"
            || a == "--query-threads" || a == "--session-ttl" || a == "--max-sessions"
            || a == "--session-memory" || a == "--session-memory-bytes" || a == "--session-max-hits"
            || a == "--cache-mb") {
            if (!num_arg(v) || v < 0) return 1;
            if (a == "--total-workers") job_cfg.workers = static_cast<unsigned>(std::max(1LL, v));
            else if (a == "--result-cache") job_cfg.max_entries = static_cast<size_t>(std::max(1LL, v));
            else if (a == "--result-ttl") job_cfg.ttl = std::chrono::seconds(v);
            else if (a == "--abandon-after") job_cfg.abandon = std::chrono::seconds(v);
            else if (a == "--query-timeout") query_timeout_ms = static_cast<size_t>(v);
            else if (a == "--query-threads") query_threads = static_cast<unsigned>(std::max(1LL, v));
            else if (a == "--session-ttl") sess_cfg.ttl = std::chrono::seconds(std::max(1LL, v));
            else if (a == "--max-sessions") sess_cfg.max_sessions = static_cast<size_t>(std::max(1LL, v));
            else if (a == "--session-memory") sess_cfg.memory_budget = static_cast<size_t>(v) << 20;
            else if (a == "--session-memory-bytes") sess_cfg.memory_budget = static_cast<size_t>(v);   // tests
            else if (a == "--session-max-hits") sess_cfg.max_hits = static_cast<size_t>(v);
            else if (a == "--cache-mb") cache_bytes = static_cast<size_t>(v) << 20;
            else job_cfg.debug_delay = std::chrono::milliseconds(v);
            continue;
        }
        if (a == "--preload") {
            preload = true;
            continue;
        }
        if (a == "--warm") {
            if (i + 1 >= argc || !parse_warm_level(argv[i + 1], warm)) {
                std::cerr << "--warm needs hot, all or none\n";
                return 1;
            }
            ++i;
            warm_given = true;
            continue;
        }
        if (a == "--limits") {
            if (i + 1 >= argc) { std::cerr << "--limits needs a file\n"; return 1; }
            limits_file = argv[++i];
            continue;
        }
        if (a == "--trust-tier") {
            trust_tier = true;
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

    ServerConfig cfg;
    cfg.jobs = job_cfg;
    cfg.threads = nthreads;
    cfg.preload = preload;
    cfg.query_timeout_ms = query_timeout_ms;
    cfg.query_threads = query_threads;
    cfg.sessions = sess_cfg;
    cfg.cache_bytes = cache_bytes;
    if (!limits_file.empty()) {
        std::ifstream in(limits_file);
        if (!in) {
            std::cerr << "cannot read " << limits_file << "\n";
            return 1;
        }
        std::stringstream buf;
        buf << in.rdbuf();
        cfg = parse_server_options(buf.str(), cfg);
    }
    if (trust_tier) cfg.trust_tier = true;
    if (warm_given) cfg.warm = warm;
    for (const auto& [name, lim] : cfg.tiers)
        for (const auto& f : lim.deny)
            if (std::find(limit_features().begin(), limit_features().end(), f) == limit_features().end())
                std::cerr << "warning: tier " << name << ": unknown deny feature '" << f << "'\n";
    ServerApi api(corpus, cfg);

    httplib::Server svr;
    svr.new_task_queue = [nthreads]() {
        return new httplib::ThreadPool(static_cast<size_t>(nthreads));
    };
    // Every route lives in ServerApi (src/api/server_api.cpp), shared with the C ABI
    // (server_capi.h) used by embedders: this is only the HTTP transport.
    auto handler = [&api](const httplib::Request& req, httplib::Response& res) {
        std::map<std::string, std::string> params;
        for (const auto& [k, v] : req.params) params.emplace(k, v);   // first value wins
        ServerResponse r = api.handle(req.method, req.path, params, req.body);
        res.status = r.status;
        res.set_content(r.body, r.content_type);
    };
    svr.Get(".*", handler);
    svr.Post(".*", handler);

    std::cerr << "Pando server " << build_string() << ": corpus " << corpus_dir << ", port " << port
              << ", threads " << nthreads
              << (preload ? ", preload=on" : ", preload=off (lazy mmap)")
              << ", background totals: " << job_cfg.workers << " workers"
              << (query_threads > 1 ? ", query threads " + std::to_string(query_threads) : std::string())
              << (query_timeout_ms ? ", query timeout " + std::to_string(query_timeout_ms) + " ms" : std::string())
              << "\n";
    if (!svr.listen("0.0.0.0", static_cast<int>(port))) {
        std::cerr << "Failed to listen on port " << port << "\n";
        return 1;
    }
    return 0;
}
