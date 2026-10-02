#pragma once

// P4.1d: the CPUs a process may use, and one worker pool per process.
//
// usable_cpus(): the affinity mask (taskset, a pinned VM / container) and the
// cgroup CPU quota (`cpu.max` in cgroup v2, `cpu.cfs_quota_us` in v1: a
// Kubernetes CPU limit, a systemd CPUQuota=), whichever is smaller.
// std::thread::hardware_concurrency() sees neither and reports the host's CPUs.
//
// WorkerPool::global(): shared by every corpus the process has open (FQS keeps
// many warm in one process), so the extra threads of all queries together stay
// within `size()`. A parallel step — the position ranges of a query (P4.1), a
// lexicon scan (P4.6) — is run(n tasks, max_helpers, fn): the calling thread
// works through the tasks itself and *borrows* idle pool workers, at most
// max_helpers; a worker that becomes free while tasks remain joins then. It
// never waits for a worker: under load a step simply runs on its caller alone.
// The CPU used is then at most (threads calling in) + size(); the first term is
// the host's to bound (FQS admission).
//
// Size: configure(n) (pando-server `--pool-threads`, server option
// `pool_threads`; 0 = auto); the first explicit value wins for the process, a
// later different one is ignored (configure() returns false). Default and auto:
// PANDO_POOL_THREADS, else usable_cpus(). Workers start lazily, when a step
// first asks for helpers.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <list>
#include <string>
#include <mutex>
#include <thread>
#include <vector>

namespace pando {

/// CPUs this process may run on (≥ 1): min(affinity mask, cgroup quota).
unsigned usable_cpus();

class WorkerPool {
public:
    static WorkerPool& global();

    WorkerPool() = default;
    explicit WorkerPool(unsigned size);   // tests: a private pool of this size
    ~WorkerPool();
    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    /// Set the size (0 = auto). False when an earlier explicit size differs (kept).
    bool configure(unsigned threads);
    unsigned size() const;

    /// fn(i) for every i in [0, n), on the calling thread and up to
    /// `max_helpers` pool workers; returns when all are done. An exception from
    /// fn is rethrown here (the first one) after the others finished; fn
    /// should make the remaining tasks stop early itself if that matters.
    void run(size_t n, unsigned max_helpers, const std::function<void(size_t)>& fn);

    struct Stats {
        unsigned threads = 0;          // size()
        unsigned started = 0;          // workers running (lazily started)
        unsigned busy = 0;             // workers inside a step now
        uint64_t steps = 0;            // run() calls that asked for helpers
        uint64_t helpers_wanted = 0;   // helpers asked for (capped by the tasks)
        uint64_t helpers_granted = 0;  // helpers that joined
    };
    Stats stats() const;
    /// {"threads": …, "started": …, "busy": …, "steps": …, "helpers_wanted": …, "helpers_granted": …}
    std::string stats_json() const;

private:
    struct Step {
        size_t n = 0;
        std::atomic<size_t> next{0};
        unsigned max_helpers = 0;
        unsigned joined = 0;           // under mu_
        unsigned active = 0;           // helpers inside, under mu_
        const std::function<void(size_t)>* fn = nullptr;
        std::exception_ptr err;        // under err_mu
        std::mutex err_mu;
    };
    static void work(Step& s);
    void worker_loop();
    unsigned size_locked() const;

    mutable std::mutex mu_;
    std::condition_variable cv_;       // workers: a step wants helpers / stop
    std::condition_variable done_cv_;  // callers: a helper left a step
    std::list<Step*> steps_;           // steps that may take helpers
    std::vector<std::thread> threads_;
    unsigned size_ = 0;                // 0 = not set (auto)
    bool explicit_ = false;
    unsigned idle_ = 0;
    unsigned busy_ = 0;
    bool stop_ = false;
    uint64_t n_steps_ = 0, wanted_ = 0, granted_ = 0;
};

/// The thread budget of the request this thread is serving (a tier's
/// `threads`; 0 = no cap). Set by the server around a request; read by
/// QueryExecutor when it is created (and passed on to its range workers), so
/// lexicon scans keep to the request's tier.
unsigned current_thread_budget();
class ThreadBudgetScope {
public:
    explicit ThreadBudgetScope(unsigned budget);
    ~ThreadBudgetScope();
    ThreadBudgetScope(const ThreadBudgetScope&) = delete;
    ThreadBudgetScope& operator=(const ThreadBudgetScope&) = delete;
private:
    unsigned prev_;
};

}  // namespace pando
