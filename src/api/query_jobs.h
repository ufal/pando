#pragma once

// P6.2: background exact totals and a result cache for pando-server.
//
// KonText shows the first concordance page while Manatee is still counting
// and polls for the growing total. The server side of that:
//
//   * /query with "total": "async" returns the page at once and starts (or
//     reuses) a background count job; the page carries the job's status;
//   * /status?job=ID reports the count so far, a progress estimate and, once
//     finished, the exact total;
//   * finished totals are cached per (query, options), so the several requests
//     KonText makes for one concordance (query_submit, view, status polls,
//     later pages) do not recount.
//
// Jobs run on a small worker pool; a job nobody has asked about for a while is
// cancelled (ExecProgress::cancel, checked at the executor's checkpoints).

#include "api/query_json.h"
#include "corpus/corpus.h"
#include "query/executor.h"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace pando {

struct QueryJobConfig {
    unsigned workers = 2;                        // concurrent background counts
    size_t max_entries = 512;                    // cached results (finished ones evicted LRU)
    std::chrono::seconds ttl{3600};              // idle time before a finished result is dropped
    std::chrono::seconds abandon{120};           // cancel a count nobody polled for this long (0 = never)
    std::chrono::milliseconds debug_delay{0};    // testing: reveal each total gradually over this long
};

struct QueryJobStatus {
    enum class State { Queued, Running, Finished, Cancelled, Failed };
    std::string id;
    std::string query;
    State state = State::Queued;
    size_t total = 0;          // Finished: the total (exact unless capped by max_total)
    bool total_exact = false;
    size_t counted = 0;        // hits counted so far (== total when finished)
    double progress = -1;      // share of the corpus scanned, 0..1; < 0 = unknown
    size_t estimate = 0;       // extrapolated total while running; 0 = unknown
    double elapsed_ms = 0;     // since the job started running (queue time excluded)
    std::string error;
    bool finished() const { return state == State::Finished; }
};

const char* job_state_name(QueryJobStatus::State s);
/// `{"id": ..., "state": ..., "finished": ..., "total": ..., ...}`
std::string job_status_json(const QueryJobStatus& st);

class QueryJobManager {
public:
    explicit QueryJobManager(const Corpus& corpus, QueryJobConfig cfg = {});
    ~QueryJobManager();
    QueryJobManager(const QueryJobManager&) = delete;
    QueryJobManager& operator=(const QueryJobManager&) = delete;

    /// Identity of a result set: the query text and the options that change its hits.
    static std::string key_for(const std::string& query, const QueryOptions& opts);
    /// Stable job id for a key (the same query gets the same id across requests).
    static std::string id_for(const std::string& key);

    /// Start the background exact count for `query` unless it is cached or running.
    QueryJobStatus ensure(const std::string& query, const QueryOptions& opts);
    /// Store a total computed elsewhere (synchronous /query, a page that held every hit).
    QueryJobStatus record_finished(const std::string& query, const QueryOptions& opts,
                                   size_t total, bool exact);
    /// The job for `query`, if known (does not start one).
    std::optional<QueryJobStatus> lookup(const std::string& query, const QueryOptions& opts);
    /// Status by id; counts as interest (keeps a running job from being abandoned).
    std::optional<QueryJobStatus> status(const std::string& id);
    /// Ask a queued / running job to stop. False when unknown or already done.
    bool cancel(const std::string& id);
    std::vector<QueryJobStatus> list();

private:
    struct Job;
    using Clock = std::chrono::steady_clock;

    QueryJobStatus snapshot(const Job& j) const;
    void worker_loop();
    void reaper_loop();
    void run_job(Job& j);
    void evict_locked(Clock::time_point now);

    const Corpus& corpus_;
    QueryJobConfig cfg_;
    std::mutex mu_;
    std::condition_variable cv_;           // workers: queue not empty / stop
    std::condition_variable reaper_cv_;
    std::unordered_map<std::string, std::shared_ptr<Job>> jobs_;
    std::deque<std::shared_ptr<Job>> queue_;
    std::vector<std::thread> workers_;
    std::thread reaper_;
    bool stop_ = false;
};

} // namespace pando
