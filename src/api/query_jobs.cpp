#include "api/query_jobs.h"
#include "core/json_utils.h"
#include "query/parser.h"

#include <algorithm>
#include <cstdio>
#include <sstream>

namespace pando {

struct QueryJobManager::Job {
    std::string id, key, query;
    QueryOptions opts;
    ExecProgress prog;
    QueryJobStatus::State state = QueryJobStatus::State::Queued;   // guarded by mu_
    size_t total = 0;
    bool exact = false;
    std::string error;
    Clock::time_point created, started, done, last_access;
    bool has_started = false, has_done = false;
};

const char* job_state_name(QueryJobStatus::State s) {
    switch (s) {
        case QueryJobStatus::State::Queued:    return "queued";
        case QueryJobStatus::State::Running:   return "running";
        case QueryJobStatus::State::Finished:  return "finished";
        case QueryJobStatus::State::Cancelled: return "cancelled";
        case QueryJobStatus::State::Failed:    return "failed";
    }
    return "unknown";
}

std::string job_status_json(const QueryJobStatus& st) {
    std::ostringstream out;
    out << "{\"id\": " << jstr(st.id)
        << ", \"state\": \"" << job_state_name(st.state) << "\""
        << ", \"finished\": " << (st.finished() ? "true" : "false")
        << ", \"total\": " << (st.finished() ? st.total : st.counted)
        << ", \"total_exact\": " << (st.finished() && st.total_exact ? "true" : "false")
        << ", \"counted\": " << st.counted;
    out << ", \"progress\": ";
    if (st.progress >= 0) {
        char buf[32];
        std::snprintf(buf, sizeof buf, "%.4f", st.progress);
        out << buf;
    } else {
        out << "null";
    }
    out << ", \"estimate\": ";
    if (st.estimate > 0) out << st.estimate; else out << "null";
    char ms[32];
    std::snprintf(ms, sizeof ms, "%.1f", st.elapsed_ms);
    out << ", \"elapsed_ms\": " << ms;
    if (!st.error.empty()) out << ", \"error\": " << jstr(st.error);
    out << "}";
    return out.str();
}

std::string QueryJobManager::key_for(const std::string& query, const QueryOptions& opts) {
    std::string k = query;
    k += '\x1f';
    k += opts.strict_quoted_strings ? '1' : '0';
    k += opts.allow_empty_alignment ? '1' : '0';
    k += '\x1f';
    k += std::to_string(opts.max_total);
    return k;
}

std::string QueryJobManager::id_for(const std::string& key) {
    uint64_t h = 1469598103934665603ull;   // FNV-1a 64
    for (unsigned char c : key) {
        h ^= c;
        h *= 1099511628211ull;
    }
    char buf[17];
    std::snprintf(buf, sizeof buf, "%016llx", static_cast<unsigned long long>(h));
    return buf;
}

QueryJobManager::QueryJobManager(const Corpus& corpus, QueryJobConfig cfg)
    : corpus_(corpus), cfg_(cfg) {
    const unsigned n = std::max(1u, cfg_.workers);
    for (unsigned i = 0; i < n; ++i) workers_.emplace_back([this] { worker_loop(); });
    reaper_ = std::thread([this] { reaper_loop(); });
}

QueryJobManager::~QueryJobManager() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
        for (auto& [id, j] : jobs_) j->prog.cancel.store(true);
    }
    cv_.notify_all();
    reaper_cv_.notify_all();
    for (auto& t : workers_) t.join();
    reaper_.join();
}

QueryJobStatus QueryJobManager::snapshot(const Job& j) const {
    QueryJobStatus st;
    st.id = j.id;
    st.query = j.query;
    st.state = j.state;
    st.total = j.total;
    st.total_exact = j.exact;
    st.error = j.error;
    const CorpusPos n = corpus_.size();
    if (j.state == QueryJobStatus::State::Finished) {
        st.counted = j.total;
        st.progress = 1.0;
    } else {
        st.counted = j.prog.counted.load(std::memory_order_relaxed);
        const int64_t scanned = j.prog.scanned.load(std::memory_order_relaxed);
        if (scanned >= 0 && n > 0) {
            st.progress = std::min(1.0, static_cast<double>(scanned + 1) / static_cast<double>(n));
            if (st.counted > 0 && st.progress > 0)
                st.estimate = std::max(st.counted,
                                       static_cast<size_t>(static_cast<double>(st.counted) / st.progress));
        }
    }
    if (j.has_started) {
        const auto end = j.has_done ? j.done : Clock::now();
        st.elapsed_ms = std::chrono::duration<double, std::milli>(end - j.started).count();
    }
    return st;
}

QueryJobStatus QueryJobManager::ensure(const std::string& query, const QueryOptions& opts) {
    const std::string key = key_for(query, opts);
    const std::string id = id_for(key);
    std::lock_guard<std::mutex> lock(mu_);
    const auto now = Clock::now();
    auto it = jobs_.find(id);
    if (it != jobs_.end() && it->second->key == key
        && it->second->state != QueryJobStatus::State::Cancelled
        && it->second->state != QueryJobStatus::State::Failed) {
        it->second->last_access = now;
        return snapshot(*it->second);
    }
    auto j = std::make_shared<Job>();
    j->id = id;
    j->key = key;
    j->query = query;
    j->opts = opts;
    j->created = j->last_access = now;
    jobs_[id] = j;
    queue_.push_back(j);
    evict_locked(now);
    cv_.notify_one();
    return snapshot(*j);
}

QueryJobStatus QueryJobManager::record_finished(const std::string& query, const QueryOptions& opts,
                                                size_t total, bool exact) {
    const std::string key = key_for(query, opts);
    const std::string id = id_for(key);
    std::lock_guard<std::mutex> lock(mu_);
    const auto now = Clock::now();
    auto& slot = jobs_[id];
    if (slot && slot->key == key && slot->state == QueryJobStatus::State::Finished) {
        slot->last_access = now;
        return snapshot(*slot);
    }
    if (slot) {
        // a queued / running count for the same set is no longer needed
        slot->prog.cancel.store(true);
        if (slot->state == QueryJobStatus::State::Queued) slot->state = QueryJobStatus::State::Cancelled;
    }
    auto j = std::make_shared<Job>();
    j->id = id;
    j->key = key;
    j->query = query;
    j->opts = opts;
    j->created = j->started = j->done = j->last_access = now;
    j->has_started = j->has_done = true;
    j->state = QueryJobStatus::State::Finished;
    j->total = total;
    j->exact = exact;
    slot = j;
    QueryJobStatus st = snapshot(*j);
    evict_locked(now);
    return st;
}

std::optional<QueryJobStatus> QueryJobManager::lookup(const std::string& query, const QueryOptions& opts) {
    const std::string key = key_for(query, opts);
    std::lock_guard<std::mutex> lock(mu_);
    auto it = jobs_.find(id_for(key));
    if (it == jobs_.end() || it->second->key != key) return std::nullopt;
    it->second->last_access = Clock::now();
    return snapshot(*it->second);
}

std::optional<QueryJobStatus> QueryJobManager::status(const std::string& id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return std::nullopt;
    it->second->last_access = Clock::now();
    return snapshot(*it->second);
}

bool QueryJobManager::cancel(const std::string& id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return false;
    Job& j = *it->second;
    if (j.state == QueryJobStatus::State::Queued) {
        j.state = QueryJobStatus::State::Cancelled;
        j.prog.cancel.store(true);
        return true;
    }
    if (j.state == QueryJobStatus::State::Running) {
        j.prog.cancel.store(true);   // the worker marks it cancelled at the next checkpoint
        return true;
    }
    return false;
}

std::vector<QueryJobStatus> QueryJobManager::list() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<QueryJobStatus> out;
    out.reserve(jobs_.size());
    for (const auto& [id, j] : jobs_) out.push_back(snapshot(*j));
    return out;
}

void QueryJobManager::evict_locked(Clock::time_point now) {
    // drop idle finished / failed / cancelled results; then the least recently
    // used ones above max_entries. Queued and running jobs stay.
    auto idle_done = [&](const Job& j) {
        return j.state != QueryJobStatus::State::Queued && j.state != QueryJobStatus::State::Running;
    };
    for (auto it = jobs_.begin(); it != jobs_.end();) {
        if (idle_done(*it->second) && now - it->second->last_access > cfg_.ttl) it = jobs_.erase(it);
        else ++it;
    }
    if (jobs_.size() <= cfg_.max_entries) return;
    std::vector<std::pair<Clock::time_point, std::string>> cand;
    for (const auto& [id, j] : jobs_)
        if (idle_done(*j)) cand.emplace_back(j->last_access, id);
    std::sort(cand.begin(), cand.end());
    for (const auto& c : cand) {
        if (jobs_.size() <= cfg_.max_entries) break;
        jobs_.erase(c.second);
    }
}

void QueryJobManager::worker_loop() {
    for (;;) {
        std::shared_ptr<Job> j;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
            if (stop_) return;
            j = queue_.front();
            queue_.pop_front();
            if (j->state != QueryJobStatus::State::Queued) continue;   // cancelled meanwhile
            j->state = QueryJobStatus::State::Running;
            j->started = Clock::now();
            j->has_started = true;
        }
        run_job(*j);
    }
}

void QueryJobManager::run_job(Job& j) {
    QueryJobStatus::State final_state = QueryJobStatus::State::Finished;
    size_t total = 0;
    bool exact = false;
    std::string error;
    try {
        Parser parser(j.query, ParserOptions{j.opts.strict_quoted_strings});
        Program prog = parser.parse();
        if (prog.empty() || !prog[0].has_query) throw std::runtime_error("not a query");
        QueryExecutor ex(corpus_);
        ex.set_include_empty_alignment_values(j.opts.allow_empty_alignment);
        // with --debug-total-delay only the gradual reveal below publishes (a real
        // count reaching 100 % first and then restarting would confuse the client)
        ExecProgress hidden;
        ex.set_progress(cfg_.debug_delay.count() > 0 ? &hidden : &j.prog);
        // count only: one match materialised, the rest counted (cheap paths / popcounts)
        MatchSet ms = ex.execute(prog[0].query, 1, true, j.opts.max_total);
        total = ms.total_count;
        exact = ms.total_exact;
        if (cfg_.debug_delay.count() > 0) {
            // testing aid: publish the total in steps, like a slow count would
            const int steps = 20;
            const CorpusPos n = corpus_.size();
            for (int s = 1; s <= steps; ++s) {
                std::this_thread::sleep_for(cfg_.debug_delay / steps);
                if (j.prog.cancel.load()) throw QueryCancelled();
                j.prog.counted.store(total * static_cast<size_t>(s) / steps);
                j.prog.scanned.store(n * s / steps - 1);
            }
        }
    } catch (const QueryCancelled&) {
        final_state = QueryJobStatus::State::Cancelled;
    } catch (const std::exception& e) {
        final_state = QueryJobStatus::State::Failed;
        error = e.what();
    }
    std::lock_guard<std::mutex> lock(mu_);
    j.state = final_state;
    j.total = total;
    j.exact = exact;
    j.error = error;
    j.done = Clock::now();
    j.has_done = true;
}

void QueryJobManager::reaper_loop() {
    std::unique_lock<std::mutex> lock(mu_);
    while (!stop_) {
        reaper_cv_.wait_for(lock, std::chrono::seconds(1));
        if (stop_) break;
        const auto now = Clock::now();
        if (cfg_.abandon.count() > 0) {
            for (auto& [id, j] : jobs_) {
                if ((j->state == QueryJobStatus::State::Running || j->state == QueryJobStatus::State::Queued)
                    && now - j->last_access > cfg_.abandon) {
                    j->prog.cancel.store(true);
                    if (j->state == QueryJobStatus::State::Queued) j->state = QueryJobStatus::State::Cancelled;
                }
            }
        }
        evict_locked(now);
    }
}

} // namespace pando
