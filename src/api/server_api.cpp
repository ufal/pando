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

// A top-level member first in a JSON object body ("{…" → "{"k": v, …").
std::string with_member(std::string body, const std::string& member) {
    const size_t brace = body.find('{');
    if (brace == std::string::npos) return body;
    body.insert(brace + 1, member + ", ");
    return body;
}

std::string session_member(const std::string& id, const std::string& set) {
    std::string m = "\"session_id\": " + jstr(id);
    if (!set.empty()) m += ", \"hitset\": " + jstr(set);
    return m;
}

ServerResponse unknown_session(const std::string& id) {
    return json_error(404, "unknown session (expired, closed or never created): " + id,
                      "\"unknown_session\":true,\"session_id\":" + jstr(id));
}

ServerResponse unknown_hitset(const std::string& id, const std::string& name) {
    return json_error(404, "unknown hit set " + name + " in session " + id,
                      "\"unknown_hitset\":true,\"session_id\":" + jstr(id) + ",\"hitset\":" + jstr(name));
}

ServerResponse too_large(const HitSetTooLarge& e) {
    return json_error(413, e.what(), "\"too_large\":true,\"hits\":" + std::to_string(e.hits)
                      + ",\"max_hits\":" + std::to_string(e.limit));
}

bool valid_set_name(const std::string& n) {   // a CQL query name
    if (n.empty() || !(std::isalpha(static_cast<unsigned char>(n[0])) || n[0] == '_')) return false;
    for (char c : n)
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) return false;
    return true;
}

std::string json_str_array(const std::vector<std::string>& v) {
    std::string o = "[";
    for (size_t i = 0; i < v.size(); ++i) o += (i ? ", " : "") + jstr(v[i]);
    return o + "]";
}

std::string fmt_s(double v) {
    char b[32];
    std::snprintf(b, sizeof b, "%.1f", v);
    return b;
}

std::string hitset_json(const HitSetInfo& i) {
    std::string sorts = "[";
    for (size_t k = 0; k < i.sorts.size(); ++k) sorts += (k ? ", " : "") + json_str_array(i.sorts[k]);
    sorts += "]";
    return "{\"name\": " + jstr(i.name) + ", \"query\": " + jstr(i.query)
        + ", \"materialised\": " + (i.materialised ? "true" : "false")
        + ", \"hits\": " + std::to_string(i.hits)
        + ", \"total\": " + (i.total_known ? std::to_string(i.total) : std::string("null"))
        + ", \"total_exact\": " + (i.total_known && i.total_exact ? "true" : "false")
        + ", \"bytes\": " + std::to_string(i.bytes)
        + ", \"sorts\": " + sorts + ", \"aliases\": " + json_str_array(i.aliases)
        + ", \"parallel\": " + (i.parallel ? "true" : "false")
        + ", \"idle_s\": " + fmt_s(i.idle_s) + "}";
}

std::string session_summary_json(const SessionManager::Summary& su) {
    return "{\"session_id\": " + jstr(su.id) + ", \"sets\": " + std::to_string(su.sets)
        + ", \"bytes\": " + std::to_string(su.bytes) + ", \"age_s\": " + fmt_s(su.age_s)
        + ", \"idle_s\": " + fmt_s(su.idle_s) + ", \"ttl_s\": " + std::to_string(su.ttl_s)
        + ", \"requests\": " + std::to_string(su.requests)
        + ", \"in_use\": " + (su.in_use ? "true" : "false") + "}";
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
    : corpus_(corpus), cfg_(std::move(cfg)),
      jobs_(corpus, [this] {
          QueryJobConfig j = cfg_.jobs;
          j.count_threads = std::max({1u, j.count_threads, cfg_.query_threads});
          return j;
      }()),
      sessions_(cfg_.sessions),
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
        {"POST", "/session"}, {"GET", "/session"}, {"POST", "/session/close"}, {"GET", "/sessions"},
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
        "query_timeout",    // /query and /run "timeout_ms" (and a server default) → 408
        "sessions",         // POST /session; "session_id" on /run, /query ("name", "from") (P6.1)
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
        + ", \"query_threads\": " + std::to_string(std::max(1u, cfg_.query_threads))
        + ", \"total_workers\": " + std::to_string(cfg_.jobs.workers)
        + ", \"sessions\": {\"open\": " + std::to_string(sessions_.count())
        + ", \"bytes\": " + std::to_string(sessions_.bytes())
        + ", \"ttl_s\": " + std::to_string(cfg_.sessions.ttl.count())
        + ", \"max_sessions\": " + std::to_string(cfg_.sessions.max_sessions)
        + ", \"memory_budget\": " + std::to_string(cfg_.sessions.memory_budget)
        + ", \"max_hits\": " + std::to_string(cfg_.sessions.max_hits) + "}";
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
        else if (path == "/session") {
            if (post) return session_create(body);
            if (get) return session_info(params, body);
            r = -1;
        }
        else if ((r = route_is("POST", "/session/close"))) { if (r > 0) return session_close(params, body); }
        else if ((r = route_is("GET", "/sessions"))) { if (r > 0) return list_sessions(); }
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
    } catch (const HitSetTooLarge& e) {
        return too_large(e);
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
// Body: {"cql": "...", "limit": 20, "offset": 0, "session_id": …, "timeout_ms": …}.
// Without "session_id" the program runs in the one shared session (requests one
// at a time); with it, in that client session (P6.1: its named sets and Last).
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
    opts.threads = std::max(1u, cfg_.query_threads);
    const std::string sid = json_extract_str(body, "session_id");
    const size_t timeout_ms = json_extract_num(body, "timeout_ms", cfg_.query_timeout_ms);

    ExecProgress prog;
    Deadline deadline;
    struct Disarm {
        ServerApi* api; Deadline* d;
        ~Disarm() { if (d) api->disarm(*d); }
    } disarm_guard{this, nullptr};
    if (timeout_ms > 0) {
        opts.progress = &prog;
        deadline.prog = &prog;
        deadline.when = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        arm(deadline);
        disarm_guard.d = &deadline;
    }
    auto exec = [&](ProgramSession& ps) -> ServerResponse {
        try {
            return json_ok(run_program_json(corpus_, ps, cql, opts));
        } catch (const QueryCancelled&) {
            return json_error(408, "program timed out after " + std::to_string(timeout_ms) + " ms",
                              "\"timed_out\":true");
        } catch (const HitSetTooLarge& e) {
            return too_large(e);
        } catch (const std::exception& e) {
            // a parse error in the program (pando-server used to drop the connection: 500, empty body)
            return json_error(400, e.what());
        }
    };
    if (sid.empty()) {
        std::lock_guard<std::mutex> lock(program_mu_);
        return exec(program_session_);
    }
    SessionManager::Lease lease = sessions_.acquire(sid);
    if (!lease) return unknown_session(sid);
    lease.lock();
    ServerResponse r = exec(lease.ps());
    if (r.status == 200) r.body = with_member(std::move(r.body), session_member(sid, ""));
    return r;
}

// POST /query — body: query, limit, offset, total, max_total, context, sentence,
// attrs, debug, strict_quoted_strings, timeout_ms; session_id, name, from (P6.1).
//   "total": false    page only (page.total = hits on the page, total_exact false)
//   "total": true     page + exact total (reuses a cached total for the same query)
//   "total": "async"  page now; the exact total is counted in the background:
//                     result.job = {id, state, finished, total, counted, progress,
//                     estimate, …}; poll GET /status?job=<id>.
// With "session_id": the result is stored in that session as hit set "name"
// (default: only Last) for later /run commands and pages. With "from": <set>
// (and "session_id"), no query runs: the page comes from the stored set (sorted
// sets in their sorted order); "query" is ignored.
ServerResponse ServerApi::query(const std::string& body) {
    const std::string sid = json_extract_str(body, "session_id");
    const std::string set_name = json_extract_str(body, "name");
    const std::string from = json_extract_str(body, "from");
    SessionManager::Lease lease;
    if (!sid.empty()) {
        lease = sessions_.acquire(sid);
        if (!lease) return unknown_session(sid);
    } else if (!set_name.empty() || !from.empty()) {
        return json_error(400, "'name' and 'from' need a 'session_id' (POST /session)");
    }
    if (!set_name.empty() && !valid_set_name(set_name))
        return json_error(400, "bad hit set name (letters, digits, '_'): " + set_name);

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
    opts.threads = std::max(1u, cfg_.query_threads);
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
    if (!from.empty()) return query_from(opts, total_async, timeout_ms, progress, from, lease);

    auto run_q = [&](const QueryOptions& o) { return run_single_query(corpus_, query_text, o, progress); };
    auto ok = [&](const MatchSet& ms, double elapsed, std::string_view extra = {}) {
        std::string js = to_query_result_json(corpus_, query_text, ms, opts, elapsed, extra);
        if (lease) {
            lease.lock();
            lease.ps().store_query(corpus_, set_name, query_text, opts, ms);
            js = with_member(std::move(js), session_member(sid, set_name.empty() ? "Last" : set_name));
        }
        return json_ok(std::move(js));
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

// /query "from": a page of a stored hit set. A set deposited by /query with
// "total": "async" takes its total from that background count once it is done
// (or shows the count so far); other sets count their total when asked.
ServerResponse ServerApi::query_from(const QueryOptions& opts, bool total_async, size_t timeout_ms,
                                     ExecProgress* progress, const std::string& from,
                                     SessionManager::Lease& lease) {
    lease.lock();
    ProgramSession& ps = lease.ps();
    const std::string& sid = lease.id();
    std::optional<HitSetInfo> info = ps.info(from);
    if (!info) return unknown_hitset(sid, from);
    std::string extra;
    std::optional<std::pair<size_t, bool>> shown;
    const std::string text = ps.query_text(from);
    if (opts.total && !(info->total_known && info->total_exact) && !info->materialised
        && info->sorts.empty() && !text.empty()) {
        std::optional<QueryJobStatus> st = jobs_.lookup(text, opts);
        if (st && st->finished()) {
            ps.set_total(from, st->total, st->total_exact);
            extra = job_fields(*st);
        } else if (total_async) {
            QueryJobStatus js = jobs_.ensure(text, opts);
            if (js.finished()) ps.set_total(from, js.total, js.total_exact);
            else shown = std::make_pair(js.counted, false);
            extra = job_fields(js);
        }
    }
    try {
        std::string js = ps.page_json(corpus_, from, opts, progress, extra, shown);
        return json_ok(with_member(std::move(js), session_member(sid, from)));
    } catch (const QueryCancelled&) {
        return json_error(408, "query timed out after " + std::to_string(timeout_ms) + " ms",
                          "\"timed_out\":true");
    } catch (const HitSetTooLarge& e) {
        return too_large(e);
    } catch (const UnknownHitSet&) {
        return unknown_hitset(sid, from);
    } catch (const std::exception& e) {
        return json_error(400, e.what());
    }
}

// POST /session — body {"session_id": optional (reuse / choose one), "ttl_s": optional}.
ServerResponse ServerApi::session_create(const std::string& body) {
    const std::string want = json_extract_str(body, "session_id");
    const auto ttl = std::chrono::seconds(json_extract_num(body, "ttl_s", 0));
    std::string id;
    const auto res = sessions_.create(want, ttl, &id);
    if (res == SessionManager::CreateResult::BadId)
        return json_error(400, "bad session_id (1-128 of A-Z a-z 0-9 _ - . :): " + want);
    if (res == SessionManager::CreateResult::Full)
        return json_error(503, "too many sessions in use (max_sessions "
                          + std::to_string(cfg_.sessions.max_sessions) + ")");
    auto su = sessions_.summary(id);
    return json_ok("{\"ok\":true,\"session_id\":" + jstr(id) + ",\"created\":"
                   + (res == SessionManager::CreateResult::Created ? "true" : "false")
                   + ",\"ttl_s\":" + std::to_string(su ? su->ttl_s : 0) + "}\n");
}

// GET /session?session_id= — the session's hit sets.
ServerResponse ServerApi::session_info(const std::map<std::string, std::string>& params, const std::string& body) {
    const std::string* idp = param(params, "session_id");
    const std::string id = idp ? *idp : json_extract_str(body, "session_id");
    if (id.empty()) return json_error(400, "missing 'session_id'");
    SessionManager::Lease lease = sessions_.acquire(id);
    if (!lease) return unknown_session(id);
    lease.lock();
    ProgramSession& ps = lease.ps();
    std::string sets = "[";
    bool first = true;
    for (HitSetInfo& i : ps.list()) {
        const std::string text = ps.query_text(i.name);
        if (!i.total_known && !text.empty()) {   // a background count finished since
            if (auto st = jobs_.lookup(text, QueryOptions{}); st && st->finished()) {
                ps.set_total(i.name, st->total, st->total_exact);
                i.total_known = true;
                i.total = st->total;
                i.total_exact = st->total_exact;
            }
        }
        sets += std::string(first ? "\n  " : ",\n  ") + hitset_json(i);
        first = false;
    }
    sets += "]";
    auto su = sessions_.summary(id);
    std::string out = "{\"ok\":true,\"session_id\":" + jstr(id);
    if (su) out += ",\"ttl_s\":" + std::to_string(su->ttl_s) + ",\"age_s\":" + fmt_s(su->age_s)
                   + ",\"requests\":" + std::to_string(su->requests);
    out += ",\"bytes\":" + std::to_string(ps.cache_bytes()) + ",\"sets\":" + sets + "}\n";
    return json_ok(std::move(out));
}

// POST /session/close?session_id= (or body) — forget the session and its sets.
ServerResponse ServerApi::session_close(const std::map<std::string, std::string>& params, const std::string& body) {
    const std::string* idp = param(params, "session_id");
    const std::string id = idp ? *idp : json_extract_str(body, "session_id");
    if (id.empty()) return json_error(400, "missing 'session_id'");
    const bool closed = sessions_.close(id);
    return json_ok("{\"ok\":true,\"session_id\":" + jstr(id) + ",\"closed\":"
                   + (closed ? "true" : "false") + "}\n");
}

// GET /sessions — every open session (monitoring).
ServerResponse ServerApi::list_sessions() {
    std::string out = "{\"ok\":true,\"sessions\":[";
    bool first = true;
    for (const auto& su : sessions_.list()) {
        out += std::string(first ? "\n  " : ",\n  ") + session_summary_json(su);
        first = false;
    }
    out += "],\"open\":" + std::to_string(sessions_.count()) + ",\"bytes\":" + std::to_string(sessions_.bytes())
           + ",\"memory_budget\":" + std::to_string(cfg_.sessions.memory_budget)
           + ",\"materialising_bytes\":" + std::to_string(sessions_.materialising_bytes()) + "}\n";
    return json_ok(std::move(out));
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
