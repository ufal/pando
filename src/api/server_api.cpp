#include "api/server_api.h"

#include "core/build_info.h"
#include "core/json_utils.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <optional>
#include <sstream>

namespace pando {

namespace {

// `"job_id": "<id>", "job": {...}` inside a /query result.
std::string job_fields(const QueryJobStatus& st) {
    return "\"job_id\": " + jstr(st.id) + ",\n    \"job\": " + job_status_json(st);
}

ServerResponse json_error(int status, const std::string& msg, std::string_view extra = {}) {
    ServerResponse r;
    r.status = status;
    r.body = "{\"ok\":false,\"error\":\"" + json_escape(msg) + "\"";
    if (!extra.empty()) r.body += std::string(",") + std::string(extra);
    r.body += "}\n";
    return r;
}

ServerResponse json_ok(std::string body) {
    ServerResponse r;
    r.body = std::move(body);
    return r;
}

const std::string* param(const std::map<std::string, std::string>& p, const char* key) {
    auto it = p.find(key);
    return it == p.end() ? nullptr : &it->second;
}

int hexval(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string url_decode(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '+') {
            out += ' ';
        } else if (s[i] == '%' && i + 2 < s.size() && hexval(s[i + 1]) >= 0 && hexval(s[i + 2]) >= 0) {
            out += static_cast<char>(hexval(s[i + 1]) * 16 + hexval(s[i + 2]));
            i += 2;
        } else {
            out += s[i];
        }
    }
    return out;
}

bool is_word_segment(std::string_view s) {   // httplib's (\w+)
    if (s.empty()) return false;
    for (char c : s) {
        const unsigned char u = static_cast<unsigned char>(c);
        if (!(std::isalnum(u) || u == '_')) return false;
    }
    return true;
}

}  // namespace

std::map<std::string, std::string> parse_query_string(std::string_view qs) {
    std::map<std::string, std::string> out;
    if (!qs.empty() && qs.front() == '?') qs.remove_prefix(1);
    size_t pos = 0;
    while (pos <= qs.size()) {
        size_t amp = qs.find('&', pos);
        if (amp == std::string_view::npos) amp = qs.size();
        std::string_view kv = qs.substr(pos, amp - pos);
        if (!kv.empty()) {
            size_t eq = kv.find('=');
            std::string k = url_decode(kv.substr(0, eq));
            std::string v = eq == std::string_view::npos ? std::string() : url_decode(kv.substr(eq + 1));
            out.emplace(std::move(k), std::move(v));   // first value wins (as httplib get_param_value)
        }
        pos = amp + 1;
    }
    return out;
}

// ── construction ─────────────────────────────────────────────────────────

struct ServerApi::Deadline {
    ExecProgress* prog = nullptr;
    std::multimap<std::chrono::steady_clock::time_point, Deadline*>::iterator it;
    std::chrono::steady_clock::time_point when;
    bool armed = false;
    bool fired = false;
};

ServerApi::ServerApi(Corpus& corpus, ServerConfig cfg)
    : corpus_(corpus), cfg_(std::move(cfg)), jobs_(corpus, cfg_.jobs),
      started_(std::chrono::system_clock::now()), started_steady_(std::chrono::steady_clock::now()) {
    last_request_ns_.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               started_steady_.time_since_epoch()).count());
    char buf[32];
    const std::time_t t = std::chrono::system_clock::to_time_t(started_);
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&t));
    started_iso_ = buf;
    watchdog_ = std::thread([this] { watchdog_loop(); });
}

ServerApi::~ServerApi() {
    {
        std::lock_guard<std::mutex> lock(wd_mu_);
        wd_stop_ = true;
    }
    wd_cv_.notify_all();
    if (watchdog_.joinable()) watchdog_.join();
    // jobs_ (declared after cfg_) is destroyed next: cancels and joins its workers
}

const std::vector<ServerApi::Route>& ServerApi::routes() {
    static const std::vector<Route> r = {
        {"GET", "/health"},  {"GET", "/version"}, {"GET", "/info"},    {"GET", "/values/*"},
        {"GET", "/regions/*"}, {"GET", "/context"}, {"POST", "/run"}, {"POST", "/query"},
        {"GET", "/status"},  {"POST", "/cancel"}, {"GET", "/jobs"},
    };
    return r;
}

const std::vector<std::string>& ServerApi::features() {
    // What this build can do; clients check this instead of probing for errors.
    static const std::vector<std::string> f = {
        "query", "run", "context", "values", "regions",
        "async_total",      // /query "total": "async", /status, /cancel, /jobs (P6.2)
        "result_cache",     // totals reused across requests for the same query
        "limit0_total",     // "limit": 0 with a total = the total only
        "bitmaps",          // <attr>.bm / <struct>.bnd.bm kernels when the index has them (P3)
        "dep_pairs",        // dep.pair.* edge postings (P5.2)
        "fold_index",       // %c / %d via <attr>.fold_*.perm (P1.6)
        "sentence_context", // /query "sentence": true
        "version",          // GET /version, version fields in /health and /info
        "query_timeout",    // /query "timeout_ms" (and a server default) → 408
    };
    return f;
}

size_t ServerApi::busy() {
    size_t n = in_flight_.load();
    for (const auto& st : jobs_.list())
        if (st.state == QueryJobStatus::State::Queued || st.state == QueryJobStatus::State::Running) ++n;
    return n;
}

double ServerApi::idle_seconds() const {
    const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count();
    return static_cast<double>(now - last_request_ns_.load()) / 1e9;
}

std::string ServerApi::server_fields() const {
    const double up = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_steady_).count();
    char ups[32];
    std::snprintf(ups, sizeof ups, "%.0f", up);
    std::string features_json = "[";
    const auto& f = features();
    for (size_t i = 0; i < f.size(); ++i) features_json += std::string(i ? ", " : "") + "\"" + f[i] + "\"";
    features_json += "]";
    std::string s = build_json_fields() + ", \"build_string\": " + jstr(build_string())
        + ", \"features\": " + features_json + ", \"started\": \"" + started_iso_ + "\""
        + ", \"uptime_s\": " + ups + ", \"corpus\": " + jstr(corpus_.dir())
        + ", \"threads\": " + std::to_string(cfg_.threads)
        + ", \"total_workers\": " + std::to_string(cfg_.jobs.workers);
    if (!cfg_.extra_server_fields.empty()) s += ", " + cfg_.extra_server_fields;
    return s;
}

// ── dispatch ─────────────────────────────────────────────────────────────

ServerResponse ServerApi::handle(std::string_view method_in, std::string_view path,
                                 const std::map<std::string, std::string>& params, const std::string& body) {
    // method case-insensitive; "/query/" == "/query"
    std::string method_up(method_in);
    for (char& c : method_up) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    const std::string_view method = method_up;
    while (path.size() > 1 && path.back() == '/') path.remove_suffix(1);
    struct InFlight {
        std::atomic<size_t>& n;
        explicit InFlight(std::atomic<size_t>& c) : n(c) { ++n; }
        ~InFlight() { --n; }
    } guard(in_flight_);
    last_request_ns_.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now().time_since_epoch()).count());
    try {
        const bool get = method == "GET";
        const bool post = method == "POST";
        auto route_is = [&](const char* m, std::string_view p) -> int {   // 1 = match, -1 = wrong method
            if (path != p) return 0;
            return method == m ? 1 : -1;
        };
        int r = 0;
        if ((r = route_is("GET", "/health"))) { if (r > 0) return health(); }
        else if ((r = route_is("GET", "/version"))) { if (r > 0) return version(); }
        else if ((r = route_is("GET", "/info"))) { if (r > 0) return info(); }
        else if ((r = route_is("GET", "/context"))) { if (r > 0) return context(params); }
        else if ((r = route_is("POST", "/run"))) { if (r > 0) return run(body); }
        else if ((r = route_is("POST", "/query"))) { if (r > 0) return query(body); }
        else if ((r = route_is("GET", "/status"))) { if (r > 0) return status(params); }
        else if ((r = route_is("POST", "/cancel"))) { if (r > 0) return cancel(params, body); }
        else if ((r = route_is("GET", "/jobs"))) { if (r > 0) return list_jobs(); }
        else {
            for (const char* prefix : {"/values/", "/regions/"}) {
                const std::string_view pre(prefix);
                if (path.size() > pre.size() && path.substr(0, pre.size()) == pre
                    && is_word_segment(path.substr(pre.size()))) {
                    if (!get) { r = -1; break; }
                    const std::string name(path.substr(pre.size()));
                    return pre == "/values/" ? values(name, params) : regions(name, params);
                }
            }
        }
        (void)post;
        if (r < 0)
            return json_error(405, "method " + std::string(method) + " not allowed for " + std::string(path));
        return json_error(404, "unknown route: " + std::string(method) + " " + std::string(path));
    } catch (const QueryCancelled&) {
        return json_error(408, "query cancelled", "\"timed_out\":true");
    } catch (const std::exception& e) {
        return json_error(500, std::string("internal error: ") + e.what());
    } catch (...) {
        return json_error(500, "internal error");
    }
}

// ── routes ───────────────────────────────────────────────────────────────

ServerResponse ServerApi::health() {
    return json_ok("{\"ok\":true,\"status\":\"ok\", " + server_fields() + "}\n");
}

// GET /version — the pando build answering (and what it supports).
ServerResponse ServerApi::version() {
    return json_ok("{\"ok\":true, " + server_fields() + "}\n");
}

ServerResponse ServerApi::info() {
    return json_ok(to_info_json(corpus_, "info", "\"server\": {" + server_fields() + "}"));
}

// GET /values/:attr — unique values + counts for a positional or region attribute
ServerResponse ServerApi::values(const std::string& attr_name, const std::map<std::string, std::string>& params) {
    size_t limit = 0;
    if (auto v = param(params, "limit")) limit = static_cast<size_t>(std::strtoull(v->c_str(), nullptr, 10));
    std::string json = to_values_json(corpus_, attr_name, limit);
    if (json.empty()) {
        ServerResponse r;
        r.status = 404;
        r.body = "{\"ok\":false,\"error\":\"Unknown attribute: " + attr_name + "\"}\n";
        return r;
    }
    return json_ok(std::move(json));
}

// GET /regions/:type — list all regions of a given type with their attributes
ServerResponse ServerApi::regions(const std::string& type_name, const std::map<std::string, std::string>& params) {
    size_t limit = 0;
    if (auto v = param(params, "limit")) limit = static_cast<size_t>(std::strtoull(v->c_str(), nullptr, 10));
    std::string json = to_regions_json(corpus_, type_name, limit);
    if (json.empty()) {
        ServerResponse r;
        r.status = 404;
        r.body = "{\"ok\":false,\"error\":\"Unknown structure type: " + type_name + "\"}\n";
        return r;
    }
    return json_ok(std::move(json));
}

// GET /context?pos=&left=&right=&sentence= — KWIC window around one position
// (KonText widectx; Manatee CorpRegion cannot address Pando toknums).
ServerResponse ServerApi::context(const std::map<std::string, std::string>& params) {
    const std::string* pos_s = param(params, "pos");
    if (!pos_s) return json_error(400, "missing 'pos'");
    CorpusPos pos = static_cast<CorpusPos>(std::strtoull(pos_s->c_str(), nullptr, 10));
    if (pos >= corpus_.size()) return json_error(400, "pos out of range");
    int left = 40;
    int right = 40;
    if (auto v = param(params, "left")) left = static_cast<int>(std::strtol(v->c_str(), nullptr, 10));
    if (auto v = param(params, "right")) right = static_cast<int>(std::strtol(v->c_str(), nullptr, 10));
    bool sentence = false;
    if (auto v = param(params, "sentence")) sentence = (*v == "1" || *v == "true" || *v == "yes");
    if (left < 0) left = 0;
    if (right < 0) right = 0;
    KwicContext ctx = build_context_at(corpus_, pos, left, right, sentence);
    std::string_view doc = lookup_doc_id(corpus_, pos);
    std::ostringstream out;
    out << "{\"ok\":true,\"pos\":" << pos
        << ",\"left\":" << jstr(ctx.left)
        << ",\"match\":" << jstr(ctx.match)
        << ",\"right\":" << jstr(ctx.right)
        << ",\"doc_id\":" << jstr(doc)
        << ",\"corpus_size\":" << corpus_.size() << "}\n";
    return json_ok(out.str());
}

// POST /run — run a full CQL program (queries + commands), session-aware.
// Body: {"cql": "...", "limit": 20, "offset": 0, ...}. The named-query session is
// shared by all requests, so programs run one at a time.
ServerResponse ServerApi::run(const std::string& body) {
    std::string cql = json_extract_str(body, "cql");
    if (cql.empty()) cql = json_extract_str(body, "query");
    if (cql.empty()) return json_error(400, "missing 'cql' field");

    ProgramOptions opts;
    opts.limit      = json_extract_num(body, "limit", 20);
    opts.offset     = json_extract_num(body, "offset", 0);
    opts.max_total  = json_extract_num(body, "max_total", 0);
    opts.context    = static_cast<int>(json_extract_num(body, "context", 5));
    opts.total      = json_extract_bool(body, "total", false);
    opts.group_limit = json_extract_num(body, "group_limit", 1000);
    opts.strict_quoted_strings = json_extract_bool(body, "strict_quoted_strings", false);

    std::lock_guard<std::mutex> lock(program_mu_);
    try {
        return json_ok(run_program_json(corpus_, program_session_, cql, opts));
    } catch (const QueryCancelled&) {
        throw;
    } catch (const std::exception& e) {
        // a parse error in the program (pando-server used to drop the connection: 500, empty body)
        return json_error(400, e.what());
    }
}

// POST /query — body: query, limit, offset, total, max_total, context, sentence,
// attrs, debug, strict_quoted_strings, timeout_ms.
//   "total": false    page only (page.total = hits on the page, total_exact false)
//   "total": true     page + exact total (reuses a cached total for the same query)
//   "total": "async"  page now; the exact total is counted in the background:
//                     result.job = {id, state, finished, total, counted, progress,
//                     estimate, …}; poll GET /status?job=<id>.
ServerResponse ServerApi::query(const std::string& body) {
    // Whitespace-tolerant (Python json.dumps emits spaces after ':' / ',').
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
    const size_t timeout_ms = json_extract_num(body, "timeout_ms", cfg_.query_timeout_ms);
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

    // the page / synchronous count of this request (not the background jobs)
    ExecProgress prog;
    ExecProgress* progress = nullptr;
    Deadline deadline;
    struct Disarm {
        ServerApi* api; Deadline* d;
        ~Disarm() { if (d) api->disarm(*d); }
    } disarm_guard{this, nullptr};
    if (timeout_ms > 0) {
        progress = &prog;
        deadline.prog = &prog;
        deadline.when = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        arm(deadline);
        disarm_guard.d = &deadline;
    }
    auto run_q = [&](const QueryOptions& o) { return run_single_query(corpus_, query_text, o, progress); };
    auto ok = [&](const MatchSet& ms, double elapsed, std::string_view extra = {}) {
        return json_ok(to_query_result_json(corpus_, query_text, ms, opts, elapsed, extra));
    };

    try {
        if (!opts.total) {
            auto [ms, elapsed] = run_q(opts);
            return ok(ms, elapsed);
        }
        // limit 0 with a total: the total only (no page; limit 0 would otherwise
        // mean "all hits" and materialise every match).
        if (opts.limit == 0) {
            MatchSet ms;
            QueryJobStatus st;
            std::optional<QueryJobStatus> have = jobs_.lookup(query_text, opts);
            if (have && have->finished()) {
                st = *have;
            } else if (total_async) {
                st = jobs_.ensure(query_text, opts);
            } else {
                QueryOptions count_opts = opts;
                count_opts.limit = 1;
                count_opts.offset = 0;
                auto [cms, cel] = run_q(count_opts);
                (void)cel;
                st = jobs_.record_finished(query_text, opts, cms.total_count, cms.total_exact);
            }
            ms.total_count = st.finished() ? st.total : st.counted;
            ms.total_exact = st.finished() && st.total_exact;
            return ok(ms, 0.0, job_fields(st));
        }
        // A cached total (finished job) → only the page is computed.
        std::optional<QueryJobStatus> known = jobs_.lookup(query_text, opts);
        if (known && !known->finished()) {
            if (total_async) {
                known = jobs_.ensure(query_text, opts);    // restarts a cancelled / failed one
            } else {
                known.reset();                             // synchronous: count here
            }
        }
        if (known && known->finished()) {
            QueryOptions page_opts = opts;
            page_opts.total = false;
            auto [ms, elapsed] = run_q(page_opts);
            ms.total_count = known->total;
            ms.total_exact = known->total_exact;
            return ok(ms, elapsed, job_fields(*known));
        }
        if (!total_async) {
            auto [ms, elapsed] = run_q(opts);
            QueryJobStatus st = jobs_.record_finished(query_text, opts, ms.total_count, ms.total_exact);
            return ok(ms, elapsed, job_fields(st));
        }
        // async: the page first; when it already held every hit the total is exact
        QueryOptions page_opts = opts;
        page_opts.total = false;
        auto [ms, elapsed] = run_q(page_opts);
        QueryJobStatus st;
        if (ms.total_exact) {
            st = jobs_.record_finished(query_text, opts, ms.total_count, true);
        } else {
            st = known ? *known : jobs_.ensure(query_text, opts);
            if (st.finished()) {
                ms.total_count = st.total;
                ms.total_exact = st.total_exact;
            } else {
                ms.total_count = std::max(ms.total_count, st.counted);
                ms.total_exact = false;
            }
        }
        return ok(ms, elapsed, job_fields(st));
    } catch (const QueryCancelled&) {
        return json_error(408, "query timed out after " + std::to_string(timeout_ms) + " ms",
                          "\"timed_out\":true");
    } catch (const std::exception& e) {
        return json_error(400, e.what());
    }
}

// GET /status?job=<id> — background total: state, count so far, progress, estimate.
ServerResponse ServerApi::status(const std::map<std::string, std::string>& params) {
    const std::string* idp = param(params, "job");
    const std::string id = idp ? *idp : std::string();
    auto st = jobs_.status(id);
    if (!st) return json_error(404, "unknown job (expired or never started): " + id);
    // `result` (same envelope as /query) and `job` carry the same object
    const std::string js = job_status_json(*st);
    return json_ok("{\"ok\":true,\"result\":" + js + ",\"job\":" + js + "}\n");
}

// POST /cancel?job=<id> (or body {"job": "<id>"}) — stop a queued / running count.
ServerResponse ServerApi::cancel(const std::map<std::string, std::string>& params, const std::string& body) {
    const std::string* idp = param(params, "job");
    std::string id = idp ? *idp : json_extract_str(body, "job");
    const bool ok = jobs_.cancel(id);
    return json_ok(std::string("{\"ok\":true,\"job\":") + jstr(id) + ",\"cancelled\":"
                   + (ok ? "true" : "false") + "}\n");
}

// GET /jobs — all cached results and running counts (debugging / monitoring).
ServerResponse ServerApi::list_jobs() {
    std::string out = "{\"ok\":true,\"jobs\":[";
    bool first = true;
    for (const auto& st : jobs_.list()) {
        if (!first) out += ",";
        first = false;
        out += "\n  {\"query\": " + jstr(st.query) + ", \"job\": " + job_status_json(st) + "}";
    }
    out += "\n]}\n";
    return json_ok(std::move(out));
}

// ── deadlines ────────────────────────────────────────────────────────────

void ServerApi::arm(Deadline& d) {
    {
        std::lock_guard<std::mutex> lock(wd_mu_);
        d.it = deadlines_.emplace(d.when, &d);
        d.armed = true;
    }
    wd_cv_.notify_all();
}

void ServerApi::disarm(Deadline& d) {
    std::lock_guard<std::mutex> lock(wd_mu_);
    if (d.armed) deadlines_.erase(d.it);
    d.armed = false;
}

void ServerApi::watchdog_loop() {
    std::unique_lock<std::mutex> lock(wd_mu_);
    while (!wd_stop_) {
        if (deadlines_.empty()) {
            wd_cv_.wait(lock);
            continue;
        }
        const auto next = deadlines_.begin()->first;
        if (std::chrono::steady_clock::now() < next) {
            wd_cv_.wait_until(lock, next);
            continue;
        }
        while (!deadlines_.empty() && deadlines_.begin()->first <= std::chrono::steady_clock::now()) {
            Deadline* d = deadlines_.begin()->second;
            d->prog->cancel.store(true);
            d->fired = true;
            d->armed = false;
            deadlines_.erase(deadlines_.begin());
        }
    }
}

} // namespace pando
