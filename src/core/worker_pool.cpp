#include "core/worker_pool.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#ifdef __linux__
#include <sched.h>
#endif

namespace pando {

namespace {

#ifdef __linux__
/// ceil(quota / period) of one cgroup `cpu.max` ("max 100000" = none), 0 = none.
unsigned cpu_max_file(const std::string& path) {
    std::ifstream in(path);
    std::string q, p;
    if (!(in >> q >> p) || q == "max") return 0;
    const double quota = std::atof(q.c_str()), period = std::atof(p.c_str());
    if (quota <= 0 || period <= 0) return 0;
    return static_cast<unsigned>(std::max(1.0, std::ceil(quota / period)));
}

unsigned cgroup_cpus() {
    unsigned best = 0;
    auto take = [&](unsigned v) { if (v && (!best || v < best)) best = v; };
    // cgroup v2: this process's group and its parents up to the root
    std::ifstream cg("/proc/self/cgroup");
    std::string line, rel;
    while (std::getline(cg, line))
        if (line.rfind("0::", 0) == 0) rel = line.substr(3);
    if (!rel.empty()) {
        std::string dir = "/sys/fs/cgroup" + (rel == "/" ? std::string() : rel);
        for (;;) {
            take(cpu_max_file(dir + "/cpu.max"));
            if (dir == "/sys/fs/cgroup") break;
            const size_t slash = dir.rfind('/');
            if (slash == std::string::npos || slash < std::string("/sys/fs/cgroup").size()) dir = "/sys/fs/cgroup";
            else dir = dir.substr(0, slash);
        }
    }
    // cgroup v1 (the hierarchy as mounted in a container)
    for (const char* base : {"/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu,cpuacct"}) {
        std::ifstream qf(std::string(base) + "/cpu.cfs_quota_us"), pf(std::string(base) + "/cpu.cfs_period_us");
        long long quota = 0, period = 0;
        if ((qf >> quota) && (pf >> period) && quota > 0 && period > 0)
            take(static_cast<unsigned>(std::max(1.0, std::ceil(static_cast<double>(quota) / period))));
    }
    return best;
}
#endif

unsigned auto_size() {
    static const unsigned v = [] {
        if (const char* e = std::getenv("PANDO_POOL_THREADS")) {
            const int x = std::atoi(e);
            if (x > 0) return static_cast<unsigned>(x);
        }
        return usable_cpus();
    }();
    return v;
}

thread_local unsigned tl_budget = 0;

}  // namespace

unsigned usable_cpus() {
    static const unsigned v = [] {
        unsigned n = std::max(1u, std::thread::hardware_concurrency());
#ifdef __linux__
        cpu_set_t set;
        CPU_ZERO(&set);
        if (sched_getaffinity(0, sizeof set, &set) == 0) {
            const int c = CPU_COUNT(&set);
            if (c > 0) n = std::min(n, static_cast<unsigned>(c));
        }
        if (const unsigned q = cgroup_cpus()) n = std::min(n, q);
#endif
        return std::max(1u, n);
    }();
    return v;
}

WorkerPool& WorkerPool::global() {
    static WorkerPool* pool = new WorkerPool();   // never destroyed: workers may outlive statics
    return *pool;
}

WorkerPool::WorkerPool(unsigned size) : size_(std::max(1u, size)), explicit_(true) {}

WorkerPool::~WorkerPool() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : threads_) if (t.joinable()) t.join();
}

unsigned WorkerPool::size_locked() const { return explicit_ ? size_ : auto_size(); }

unsigned WorkerPool::size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return size_locked();
}

bool WorkerPool::configure(unsigned threads) {
    std::lock_guard<std::mutex> lk(mu_);
    const unsigned v = threads ? threads : auto_size();
    if (explicit_) return v == size_;
    size_ = v;
    explicit_ = true;
    return true;
}

void WorkerPool::work(Step& s) {
    for (;;) {
        const size_t i = s.next.fetch_add(1, std::memory_order_relaxed);
        if (i >= s.n) return;
        try {
            (*s.fn)(i);
        } catch (...) {
            std::lock_guard<std::mutex> lk(s.err_mu);
            if (!s.err) s.err = std::current_exception();
        }
    }
}

void WorkerPool::worker_loop() {
    std::unique_lock<std::mutex> lk(mu_);
    for (;;) {
        Step* pick = nullptr;
        for (Step* s : steps_)
            if (s->joined < s->max_helpers && s->next.load(std::memory_order_relaxed) < s->n) { pick = s; break; }
        if (pick) {
            ++pick->joined;
            ++pick->active;
            --idle_;
            ++busy_;
            lk.unlock();
            work(*pick);
            lk.lock();
            --pick->active;
            --busy_;
            ++idle_;
            done_cv_.notify_all();
            continue;
        }
        if (stop_) return;
        cv_.wait(lk);
    }
}

void WorkerPool::run(size_t n, unsigned max_helpers, const std::function<void(size_t)>& fn) {
    if (n == 0) return;
    const unsigned want = static_cast<unsigned>(std::min<size_t>(max_helpers, n - 1));
    if (want == 0) {
        for (size_t i = 0; i < n; ++i) fn(i);
        return;
    }
    Step s;
    s.n = n;
    s.max_helpers = want;
    s.fn = &fn;
    {
        std::lock_guard<std::mutex> lk(mu_);
        const unsigned sz = size_locked();
        ++n_steps_;
        wanted_ += want;
        while (idle_ < want && threads_.size() < sz) {
            threads_.emplace_back([this] { worker_loop(); });
            ++idle_;
        }
        steps_.push_back(&s);
    }
    cv_.notify_all();
    work(s);
    {
        std::unique_lock<std::mutex> lk(mu_);
        steps_.remove(&s);
        granted_ += s.joined;
        done_cv_.wait(lk, [&] { return s.active == 0; });
    }
    if (s.err) std::rethrow_exception(s.err);
}

WorkerPool::Stats WorkerPool::stats() const {
    std::lock_guard<std::mutex> lk(mu_);
    Stats st;
    st.threads = size_locked();
    st.started = static_cast<unsigned>(threads_.size());
    st.busy = busy_;
    st.steps = n_steps_;
    st.helpers_wanted = wanted_;
    st.helpers_granted = granted_;
    return st;
}

std::string WorkerPool::stats_json() const {
    const Stats st = stats();
    return "{\"threads\": " + std::to_string(st.threads) + ", \"started\": " + std::to_string(st.started)
        + ", \"busy\": " + std::to_string(st.busy) + ", \"steps\": " + std::to_string(st.steps)
        + ", \"helpers_wanted\": " + std::to_string(st.helpers_wanted)
        + ", \"helpers_granted\": " + std::to_string(st.helpers_granted)
        + ", \"usable_cpus\": " + std::to_string(usable_cpus()) + "}";
}

unsigned current_thread_budget() { return tl_budget; }

ThreadBudgetScope::ThreadBudgetScope(unsigned budget) : prev_(tl_budget) { tl_budget = budget; }
ThreadBudgetScope::~ThreadBudgetScope() { tl_budget = prev_; }

}  // namespace pando
