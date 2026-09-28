#pragma once

// P6.1: client sessions of pando-server / ServerApi — each a ProgramSession
// (named hit sets + `Last`) that later requests reuse: sort, count, freq, coll
// and paging operate on the stored set instead of running the query again.
//
// Sessions are soft state: they expire after `ttl` without a request, the
// least recently used idle one is closed when `max_sessions` is reached, and
// under memory pressure the materialised hits are dropped (the sets keep their
// recipe and are re-derived on demand, sorted as before). A client that gets
// "unknown session" (expired, closed, server restarted) creates a new one and
// runs its query again.
//
// Thread safety: every member may be called from any thread. Requests on one
// session are serialised by its mutex (Lease::lock); different sessions run
// concurrently.

#include "api/query_json.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace pando {

struct SessionConfig {
    std::chrono::seconds ttl{1800};        // idle time before a session expires (default for create)
    std::chrono::seconds max_ttl{86400};   // upper bound on a client-requested ttl
    size_t max_sessions = 256;
    /// Materialised hits over all sessions, bytes (0 = no limit). Above it the
    /// hits of the least recently used sessions are dropped (re-derived on demand).
    size_t memory_budget = size_t(2) << 30;
    /// One set may not materialise more hits than this (0 = no limit): 413.
    size_t max_hits = 5'000'000;
};

class SessionManager {
public:
    struct Session {
        std::string id;
        std::mutex mu;                               // one request at a time
        ProgramSession ps;                           // guarded by mu
        std::chrono::seconds ttl{0};
        // guarded by the manager's mutex:
        std::chrono::steady_clock::time_point created, last_used;
        size_t users = 0;
        size_t requests = 0;
        std::atomic<size_t> bytes{0};                // cache bytes after the last request
    };

    /// A session in use: keeps it from expiring / being closed under the request.
    /// lock() takes the session's mutex (released with the lease).
    class Lease {
    public:
        Lease() = default;
        Lease(Lease&& o) noexcept;
        Lease& operator=(Lease&& o) noexcept;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease();
        explicit operator bool() const { return s_ != nullptr; }
        void lock();
        Session& session() { return *s_; }
        ProgramSession& ps() { return s_->ps; }
        const std::string& id() const { return s_->id; }
    private:
        friend class SessionManager;
        Lease(SessionManager* m, std::shared_ptr<Session> s) : m_(m), s_(std::move(s)) {}
        void release();
        SessionManager* m_ = nullptr;
        std::shared_ptr<Session> s_;
        std::unique_lock<std::mutex> lk_;
    };

    struct Summary {
        std::string id;
        size_t sets = 0;             // names (Last included)
        size_t bytes = 0;
        double age_s = 0, idle_s = 0;
        long long ttl_s = 0;
        size_t requests = 0;
        bool in_use = false;
    };

    explicit SessionManager(SessionConfig cfg = {});

    enum class CreateResult { Created, Existing, BadId, Full };
    /// Create a session (`id` empty = a new random id) or, when `id` exists, touch
    /// it. `ttl` 0 = the configured default (capped at max_ttl). `*out_id` = its id.
    CreateResult create(const std::string& id, std::chrono::seconds ttl, std::string* out_id);
    /// The live session `id` (unknown / expired → an empty Lease), not locked.
    Lease acquire(const std::string& id);
    bool close(const std::string& id);
    std::vector<Summary> list();
    std::optional<Summary> summary(const std::string& id);
    /// Close expired idle sessions (at most once per second unless forced).
    void sweep(bool force = false);
    /// Admission for one materialisation of `bytes` (transient Match objects):
    /// waits while other materialisations hold the memory budget (one always may
    /// run); a set `progress->cancel` stops the wait with QueryCancelled. The
    /// token releases the bytes. Installed as every session's AdmitFn.
    std::shared_ptr<void> admit(size_t bytes, ExecProgress* progress);
    size_t materialising_bytes() const;
    size_t count() const;
    size_t bytes() const;
    const SessionConfig& config() const { return cfg_; }

    static bool valid_id(const std::string& id);

private:
    void released(const std::shared_ptr<Session>& s);
    void enforce_budget(const Session* current);
    Summary summarise(const Session& s, std::chrono::steady_clock::time_point now) const;
    std::string new_id();

    SessionConfig cfg_;
    mutable std::mutex mu_;
    std::map<std::string, std::shared_ptr<Session>> sessions_;
    std::chrono::steady_clock::time_point last_sweep_{};
    std::mutex budget_mu_;
    mutable std::mutex gate_mu_;
    std::condition_variable gate_cv_;
    size_t gate_bytes_ = 0;       // bytes of the materialisations admitted and running
    size_t gate_running_ = 0;
};

}  // namespace pando
