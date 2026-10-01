#pragma once

// The pando-server HTTP API without the HTTP: one corpus, its background-total
// jobs and its /run session behind `ServerApi::handle(method, path, params, body)`.
//
// pando-server is a thin httplib shell around this; embedders (the flexicorp
// adapter / FQS, other language bindings through the C ABI in server_capi.h)
// call the same code, so every transport answers with the same JSON.
//
// Thread safety: handle() may be called from any number of threads at once.
// Queries run concurrently on the shared Corpus. /run without a session_id is
// serialised (one shared named-query session); requests on one client session
// (P6.1, sessions.h) are serialised per session, different sessions run at once.
//
// Lifetime: the Corpus must outlive the ServerApi. Destroying a ServerApi
// cancels its background counts and joins their workers; it must not race with
// a handle() still running (see busy()).

#include "api/corpus_warm.h"
#include "api/query_jobs.h"
#include "api/limits.h"
#include "api/query_json.h"
#include "api/sessions.h"
#include "api/result_cache.h"
#include "corpus/corpus.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

namespace pando {

struct ServerConfig {
    QueryJobConfig jobs;              // background totals (P6.2)
    unsigned threads = 0;             // reported in /health (the transport's request threads)
    /// P4.1: a counting query (a /query total, a background total, a /run
    /// `count by`) is split over this many position ranges counted in parallel.
    /// 1 = one thread per query (the default: a busy server already runs many).
    unsigned query_threads = 1;
    bool preload = false;             // reported: the corpus was opened with preload
    /// Default per-request time limit for /query in ms (0 = none); a request can
    /// set its own with "timeout_ms". An expired query answers 408.
    size_t query_timeout_ms = 0;
    /// P6.1 client sessions (POST /session, "session_id" on /run and /query).
    SessionConfig sessions;
    /// Resource limits by tier (limits.h). A request's "tier" is honoured only
    /// with trust_tier (the host sets it; clients must not reach the server
    /// directly); otherwise, and for an unknown tier, default_tier applies. No
    /// tiers = no per-request limits beyond query_timeout_ms / sessions.
    std::map<std::string, QueryLimits> tiers;
    std::string default_tier;
    bool trust_tier = false;
    /// P6.4: bytes of recent results kept for repeated requests (pages, `count` /
    /// `freq` / `coll` / … results, sort indexes) across requests and sessions;
    /// 0 = off.
    size_t cache_bytes = size_t{128} << 20;
    /// Raw JSON members added to /health, /version and /info "server" (no braces,
    /// no leading comma), e.g. `"embedded_in": "fqs 0.4"`.
    std::string extra_server_fields;
    /// Read the index files most queries touch (WarmLevel::Hot) or all of them
    /// into the page cache in the background when the server starts; POST /warm
    /// starts it later (e.g. when a front-end selects the corpus).
    WarmLevel warm = WarmLevel::None;
};

struct ServerResponse {
    int status = 200;
    std::string body;                 // JSON, newline-terminated
    std::string content_type = "application/json";
};

/// Server options as JSON (the C ABI's options_json, pando-server --limits FILE):
/// preload, total_workers, result_cache, result_ttl, abandon_after, query_timeout_ms, cache_mb,
/// threads, query_threads, session_ttl, max_sessions, session_memory_mb,
/// session_max_hits, embedded_in, debug_total_delay_ms, warm ("hot" / "all"), and
/// "tiers": {"<name>": {limits.h members}, …}, "default_tier", "trust_tier".
/// Members that are absent keep the values of `base`.
ServerConfig parse_server_options(const std::string& json, ServerConfig base = {});

/// URL query string ("a=1&b=x%20y") → first value per key, percent-decoded, '+' = space.
std::map<std::string, std::string> parse_query_string(std::string_view qs);

class ServerApi {
public:
    ServerApi(Corpus& corpus, ServerConfig cfg = {});
    ~ServerApi();
    ServerApi(const ServerApi&) = delete;
    ServerApi& operator=(const ServerApi&) = delete;

    /// Dispatch one request. `method` "GET" / "POST"; `path` without the query
    /// string; `params` the decoded query parameters; `body` the request body
    /// (JSON for POST routes). Never throws: engine errors become 4xx / 5xx JSON.
    ServerResponse handle(std::string_view method, std::string_view path,
                          const std::map<std::string, std::string>& params, const std::string& body);

    /// The routes and methods handle() knows, for transports that register routes.
    struct Route { const char* method; const char* path; };   // path "/values/*": one \w+ segment
    static const std::vector<Route>& routes();
    /// What this build can do (the /version "features" list).
    static const std::vector<std::string>& features();

    /// Requests in flight + background counts queued or running. An embedder that
    /// evicts idle corpora keeps a ServerApi while this is non-zero (or accepts
    /// that destroying it cancels those counts).
    size_t busy();
    /// Seconds since the last handle() call started (for idle eviction).
    double idle_seconds() const;

    QueryJobManager& jobs() { return jobs_; }
    const Corpus& corpus() const { return corpus_; }

private:
    ServerResponse health();
    ServerResponse version();
    ServerResponse info();
    ServerResponse values(const std::string& attr, const std::map<std::string, std::string>& params);
    ServerResponse regions(const std::string& type, const std::map<std::string, std::string>& params);
    ServerResponse context(const std::map<std::string, std::string>& params);
    ServerResponse run(const std::string& body);
    ServerResponse query(const std::string& body);
    ServerResponse query_from(const QueryOptions& opts, bool total_async, size_t timeout_ms, ExecProgress* progress,
                              const std::string& from, SessionManager::Lease& lease, const std::string& tier,
                              std::chrono::milliseconds job_limit);
    ServerResponse session_create(const std::string& body);
    ServerResponse session_info(const std::map<std::string, std::string>& params, const std::string& body);
    ServerResponse session_close(const std::map<std::string, std::string>& params, const std::string& body);
    ServerResponse list_sessions();
    ServerResponse status(const std::map<std::string, std::string>& params);
    ServerResponse cancel(const std::map<std::string, std::string>& params, const std::string& body);
    ServerResponse list_jobs();
    ServerResponse warm(const std::string& body, bool start);
    std::string server_fields() const;

    /// The tier of a request and what it may do (ServerConfig::tiers).
    struct RequestLimits {
        std::string tier;          // "" when no tiers are configured
        QueryLimits lim;
        size_t timeout_ms = 0;     // effective: the tier cap lowered by the request's own
        unsigned threads = 1;
    };
    RequestLimits request_limits(const std::string& body) const;
    /// 403 when the query / program uses a feature the tier denies (nullopt = allowed
    /// or unparsable: the normal path reports parse errors).
    std::optional<ServerResponse> check_denied(const std::string& text, bool strict,
                                               const RequestLimits& rl) const;

    // per-request deadlines (query_timeout_ms / "timeout_ms"): one watchdog thread
    struct Deadline;
    void watchdog_loop();
    void arm(Deadline& d);
    void disarm(Deadline& d);

    Corpus& corpus_;
    ServerConfig cfg_;
    QueryJobManager jobs_;
    std::mutex program_mu_;
    ResultCache cache_;                    // P6.4 (before the sessions that point to it)
    ProgramSession program_session_;       // /run without a session_id (shared, unlimited)
    SessionManager sessions_;
    std::atomic<size_t> in_flight_{0};
    std::atomic<int64_t> last_request_ns_;
    std::chrono::system_clock::time_point started_;
    std::chrono::steady_clock::time_point started_steady_;
    std::string started_iso_;

    std::mutex wd_mu_;
    std::condition_variable wd_cv_;
    std::multimap<std::chrono::steady_clock::time_point, Deadline*> deadlines_;
    bool wd_stop_ = false;
    std::thread watchdog_;
    CorpusWarmer warmer_{corpus_};   // last: stopped first
};

} // namespace pando
