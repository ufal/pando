// Pando HTTP API server — POST /query, GET /status, POST /cancel, GET /jobs,
// GET /info, GET /health, …
// Uses a thread pool so multiple requests are handled in parallel; exact
// totals can be computed in the background ("total": "async", P6.2).

#include "api/query_json.h"
#include "api/query_jobs.h"
#include "core/json_utils.h"
#include "corpus/corpus.h"
#include <httplib.h>
#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <thread>
#include <algorithm>
#include <chrono>
#include <optional>

using namespace pando;

static unsigned default_thread_pool_size() {
    unsigned n = std::thread::hardware_concurrency();
    return (n > 0) ? std::max(2u, n) : 4u;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: pando-server <corpus_dir> [port] [threads] [--preload] [options]\n";
        std::cerr << "  Default port: 8765, threads: " << default_thread_pool_size() << "\n";
        std::cerr << "  Open matches CLI (lazy mmap). Pass --preload to warm all pages at startup.\n";
        std::cerr << "  Background totals (/query \"total\": \"async\", /status, /cancel):\n"
                  << "    --total-workers N       concurrent background counts (default 2)\n"
                  << "    --result-cache N        cached query results (default 512)\n"
                  << "    --result-ttl SEC        drop an unused finished result after SEC (default 3600)\n"
                  << "    --abandon-after SEC     cancel a count nobody polled for SEC (default 120; 0 = never)\n"
                  << "    --debug-total-delay MS  testing: reveal every total gradually over MS\n";
        return 1;
    }
    std::string corpus_dir = argv[1];
    int port = 8765;
    unsigned nthreads = default_thread_pool_size();
    bool preload = false;
    int positional = 0;
    QueryJobConfig job_cfg;
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
            || a == "--abandon-after" || a == "--debug-total-delay") {
            if (!num_arg(v) || v < 0) return 1;
            if (a == "--total-workers") job_cfg.workers = static_cast<unsigned>(std::max(1LL, v));
            else if (a == "--result-cache") job_cfg.max_entries = static_cast<size_t>(std::max(1LL, v));
            else if (a == "--result-ttl") job_cfg.ttl = std::chrono::seconds(v);
            else if (a == "--abandon-after") job_cfg.abandon = std::chrono::seconds(v);
            else job_cfg.debug_delay = std::chrono::milliseconds(v);
            continue;
        }
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

    QueryJobManager jobs(corpus, job_cfg);

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
        std::string_view doc = lookup_doc_id(corpus, pos);
        std::ostringstream out;
        out << "{\"ok\":true,\"pos\":" << pos
            << ",\"left\":" << jstr(ctx.left)
            << ",\"match\":" << jstr(ctx.match)
            << ",\"right\":" << jstr(ctx.right)
            << ",\"doc_id\":" << jstr(doc)
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

    // POST /query — body: query, limit, offset, total, max_total, context, sentence,
    // attrs, debug, strict_quoted_strings.
    //   "total": false    page only (page.total = hits on the page, total_exact false)
    //   "total": true     page + exact total (reuses a cached total for the same query)
    //   "total": "async"  page now; the exact total is counted in the background:
    //                     result.job = {id, state, finished, total, counted, progress,
    //                     estimate, …}; poll GET /status?job=<id>.
    svr.Post("/query", [&corpus, &jobs](const httplib::Request& req, httplib::Response& res) {
        // Whitespace-tolerant (Python json.dumps emits spaces after ':' / ',').
        const std::string& body = req.body;
        QueryOptions opts;

        std::string q = json_extract_str(body, "query");
        std::string query_text = q.empty() ? "[]" : q;
        opts.limit     = json_extract_num(body, "limit", 20);
        opts.offset    = json_extract_num(body, "offset", 0);
        opts.max_total = json_extract_num(body, "max_total", 0);
        const bool total_async = json_extract_str(body, "total") == "async";
        opts.total     = total_async || json_extract_bool(body, "total", false);
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
            std::string extra;
            if (!opts.total) {
                auto [ms, elapsed] = run_single_query(corpus, query_text, opts);
                res.set_content(to_query_result_json(corpus, query_text, ms, opts, elapsed),
                                "application/json");
                return;
            }
            // A cached total (finished job) → only the page is computed.
            std::optional<QueryJobStatus> known = jobs.lookup(query_text, opts);
            if (known && !known->finished()) {
                if (total_async) {
                    known = jobs.ensure(query_text, opts);    // restarts a cancelled / failed one
                } else {
                    known.reset();                            // synchronous: count here
                }
            }
            if (known && known->finished()) {
                QueryOptions page_opts = opts;
                page_opts.total = false;
                auto [ms, elapsed] = run_single_query(corpus, query_text, page_opts);
                ms.total_count = known->total;
                ms.total_exact = known->total_exact;
                extra = "\"job\": " + job_status_json(*known);
                res.set_content(to_query_result_json(corpus, query_text, ms, opts, elapsed, extra),
                                "application/json");
                return;
            }
            if (!total_async) {
                auto [ms, elapsed] = run_single_query(corpus, query_text, opts);
                QueryJobStatus st = jobs.record_finished(query_text, opts, ms.total_count, ms.total_exact);
                extra = "\"job\": " + job_status_json(st);
                res.set_content(to_query_result_json(corpus, query_text, ms, opts, elapsed, extra),
                                "application/json");
                return;
            }
            // async: the page first; when it already held every hit the total is exact
            QueryOptions page_opts = opts;
            page_opts.total = false;
            auto [ms, elapsed] = run_single_query(corpus, query_text, page_opts);
            QueryJobStatus st;
            if (ms.total_exact) {
                st = jobs.record_finished(query_text, opts, ms.total_count, true);
            } else {
                st = known ? *known : jobs.ensure(query_text, opts);
                if (st.finished()) {
                    ms.total_count = st.total;
                    ms.total_exact = st.total_exact;
                } else {
                    ms.total_count = std::max(ms.total_count, st.counted);
                    ms.total_exact = false;
                }
            }
            extra = "\"job\": " + job_status_json(st);
            res.set_content(to_query_result_json(corpus, query_text, ms, opts, elapsed, extra),
                            "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content("{\"ok\":false,\"error\":\"" + json_escape(e.what()) + "\"}\n",
                            "application/json");
        }
    });

    // GET /status?job=<id> — background total: state, count so far, progress, estimate.
    svr.Get("/status", [&jobs](const httplib::Request& req, httplib::Response& res) {
        const std::string id = req.has_param("job") ? req.get_param_value("job") : std::string();
        auto st = jobs.status(id);
        if (!st) {
            res.status = 404;
            res.set_content("{\"ok\":false,\"error\":\"unknown job (expired or never started): "
                            + json_escape(id) + "\"}\n", "application/json");
            return;
        }
        res.set_content("{\"ok\":true,\"job\":" + job_status_json(*st) + "}\n", "application/json");
    });

    // POST /cancel?job=<id> (or body {"job": "<id>"}) — stop a queued / running count.
    svr.Post("/cancel", [&jobs](const httplib::Request& req, httplib::Response& res) {
        std::string id = req.has_param("job") ? req.get_param_value("job") : json_extract_str(req.body, "job");
        const bool ok = jobs.cancel(id);
        res.set_content(std::string("{\"ok\":true,\"job\":") + jstr(id) + ",\"cancelled\":"
                        + (ok ? "true" : "false") + "}\n", "application/json");
    });

    // GET /jobs — all cached results and running counts (debugging / monitoring).
    svr.Get("/jobs", [&jobs](const httplib::Request&, httplib::Response& res) {
        std::string out = "{\"ok\":true,\"jobs\":[";
        bool first = true;
        for (const auto& st : jobs.list()) {
            if (!first) out += ",";
            first = false;
            out += "\n  {\"query\": " + jstr(st.query) + ", \"job\": " + job_status_json(st) + "}";
        }
        out += "\n]}\n";
        res.set_content(out, "application/json");
    });

    std::cerr << "Pando server: corpus " << corpus_dir << ", port " << port
              << ", threads " << nthreads
              << (preload ? ", preload=on" : ", preload=off (lazy mmap)")
              << ", background totals: " << job_cfg.workers << " workers\n";
    if (!svr.listen("0.0.0.0", static_cast<int>(port))) {
        std::cerr << "Failed to listen on port " << port << "\n";
        return 1;
    }
    return 0;
}
