#include "api/sessions.h"

#include <algorithm>
#include <cctype>
#include <optional>
#include <random>

namespace pando {

using Clock = std::chrono::steady_clock;

// ── Lease ────────────────────────────────────────────────────────────────

SessionManager::Lease::Lease(Lease&& o) noexcept
    : m_(o.m_), s_(std::move(o.s_)), lk_(std::move(o.lk_)) {
    o.m_ = nullptr;
}

SessionManager::Lease& SessionManager::Lease::operator=(Lease&& o) noexcept {
    if (this != &o) {
        release();
        m_ = o.m_;
        s_ = std::move(o.s_);
        lk_ = std::move(o.lk_);
        o.m_ = nullptr;
    }
    return *this;
}

SessionManager::Lease::~Lease() { release(); }

void SessionManager::Lease::lock() {
    if (s_ && !lk_.owns_lock()) lk_ = std::unique_lock<std::mutex>(s_->mu);
}

void SessionManager::Lease::release() {
    if (!s_) return;
    if (lk_.owns_lock()) {
        s_->bytes.store(s_->ps.cache_bytes());
        lk_.unlock();
    }
    std::shared_ptr<Session> s = std::move(s_);
    s_.reset();
    if (m_) m_->released(s);
    m_ = nullptr;
}

// ── manager ──────────────────────────────────────────────────────────────

SessionManager::SessionManager(SessionConfig cfg) : cfg_(cfg) {}

bool SessionManager::valid_id(const std::string& id) {
    if (id.empty() || id.size() > 128) return false;
    for (char c : id) {
        const unsigned char u = static_cast<unsigned char>(c);
        if (!(std::isalnum(u) || c == '_' || c == '-' || c == '.' || c == ':')) return false;
    }
    return true;
}

std::string SessionManager::new_id() {
    static thread_local std::mt19937_64 rng([] {
        std::random_device rd;
        std::seed_seq seq{rd(), rd(), rd(), rd()};
        std::mt19937_64 g(seq);
        return g;
    }());
    static const char* hex = "0123456789abcdef";
    std::string id = "s";
    for (int w = 0; w < 2; ++w) {
        uint64_t v = rng();
        for (int i = 0; i < 16; ++i) { id += hex[v & 15]; v >>= 4; }
    }
    return id;
}

SessionManager::CreateResult SessionManager::create(const std::string& id_in, std::chrono::seconds ttl,
                                                    std::string* out_id) {
    if (!id_in.empty() && !valid_id(id_in)) return CreateResult::BadId;
    sweep();
    if (ttl.count() <= 0) ttl = cfg_.ttl;
    if (cfg_.max_ttl.count() > 0) ttl = std::min(ttl, cfg_.max_ttl);
    const auto now = Clock::now();
    std::lock_guard<std::mutex> lock(mu_);
    if (!id_in.empty()) {
        auto it = sessions_.find(id_in);
        if (it != sessions_.end()) {
            it->second->last_used = now;
            if (out_id) *out_id = id_in;
            return CreateResult::Existing;
        }
    }
    if (cfg_.max_sessions > 0 && sessions_.size() >= cfg_.max_sessions) {
        // soft state: the least recently used idle session makes room
        auto victim = sessions_.end();
        for (auto it = sessions_.begin(); it != sessions_.end(); ++it)
            if (it->second->users == 0 && (victim == sessions_.end() || it->second->last_used < victim->second->last_used))
                victim = it;
        if (victim == sessions_.end()) return CreateResult::Full;
        sessions_.erase(victim);
    }
    std::string id = id_in;
    if (id.empty())
        do id = new_id(); while (sessions_.count(id));
    auto s = std::make_shared<Session>();
    s->id = id;
    s->ttl = ttl;
    s->created = s->last_used = now;
    s->ps.set_max_hits(cfg_.max_hits);
    s->ps.set_admission([this](size_t b, ExecProgress* p) { return admit(b, p); });
    sessions_.emplace(id, std::move(s));
    if (out_id) *out_id = id;
    return CreateResult::Created;
}

SessionManager::Lease SessionManager::acquire(const std::string& id) {
    sweep();
    std::lock_guard<std::mutex> lock(mu_);
    auto it = sessions_.find(id);
    if (it == sessions_.end()) return Lease();
    const auto now = Clock::now();
    Session& s = *it->second;
    if (s.users == 0 && now - s.last_used > s.ttl) {
        sessions_.erase(it);
        return Lease();
    }
    ++s.users;
    ++s.requests;
    s.last_used = now;
    return Lease(this, it->second);
}

void SessionManager::released(const std::shared_ptr<Session>& s) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (s->users > 0) --s->users;
        s->last_used = Clock::now();
    }
    enforce_budget(s.get());
}

bool SessionManager::close(const std::string& id) {
    std::lock_guard<std::mutex> lock(mu_);
    return sessions_.erase(id) > 0;   // a request still running keeps it alive until it ends
}

void SessionManager::sweep(bool force) {
    const auto now = Clock::now();
    std::lock_guard<std::mutex> lock(mu_);
    if (!force && now - last_sweep_ < std::chrono::seconds(1)) return;
    last_sweep_ = now;
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        const Session& s = *it->second;
        if (s.users == 0 && now - s.last_used > s.ttl) it = sessions_.erase(it);
        else ++it;
    }
}

void SessionManager::enforce_budget(const Session* current) {
    if (cfg_.memory_budget == 0) return;
    std::lock_guard<std::mutex> one_at_a_time(budget_mu_);
    std::vector<std::shared_ptr<Session>> order;
    size_t total = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : sessions_) {
            total += kv.second->bytes.load();
            order.push_back(kv.second);
        }
    }
    if (total <= cfg_.memory_budget) return;
    // least recently used first; the session that just ran last of all
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::sort(order.begin(), order.end(), [&](const auto& a, const auto& b) {
            const bool ca = a.get() == current, cb = b.get() == current;
            if (ca != cb) return cb;
            return a->last_used < b->last_used;
        });
    }
    for (const auto& s : order) {
        if (total <= cfg_.memory_budget) break;
        const size_t b = s->bytes.load();
        if (b == 0) continue;
        std::unique_lock<std::mutex> lk(s->mu, std::try_to_lock);   // busy sessions are skipped
        if (!lk.owns_lock()) continue;
        s->ps.drop_caches();
        s->bytes.store(0);
        total -= std::min(total, b);
    }
}

std::shared_ptr<void> SessionManager::admit(size_t bytes, ExecProgress* progress) {
    if (cfg_.memory_budget == 0) return nullptr;
    std::unique_lock<std::mutex> lk(gate_mu_);
    while (gate_running_ > 0 && gate_bytes_ + bytes > cfg_.memory_budget) {
        if (progress && progress->cancel.load()) throw QueryCancelled();
        gate_cv_.wait_for(lk, std::chrono::milliseconds(50));
    }
    gate_bytes_ += bytes;
    ++gate_running_;
    return std::shared_ptr<void>(nullptr, [this, bytes](void*) {
        {
            std::lock_guard<std::mutex> g(gate_mu_);
            gate_bytes_ -= bytes;
            --gate_running_;
        }
        gate_cv_.notify_all();
    });
}

size_t SessionManager::materialising_bytes() const {
    std::lock_guard<std::mutex> g(gate_mu_);
    return gate_bytes_;
}

SessionManager::Summary SessionManager::summarise(const Session& s, Clock::time_point now) const {
    Summary out;
    out.id = s.id;
    out.bytes = s.bytes.load();
    out.age_s = std::chrono::duration<double>(now - s.created).count();
    out.idle_s = std::chrono::duration<double>(now - s.last_used).count();
    out.ttl_s = static_cast<long long>(s.ttl.count());
    out.requests = s.requests;
    out.in_use = s.users > 0;
    return out;
}

std::vector<SessionManager::Summary> SessionManager::list() {
    sweep();
    std::vector<std::shared_ptr<Session>> all;
    std::vector<Summary> out;
    const auto now = Clock::now();
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& kv : sessions_) {
            all.push_back(kv.second);
            out.push_back(summarise(*kv.second, now));
        }
    }
    for (size_t i = 0; i < all.size(); ++i) {
        std::unique_lock<std::mutex> lk(all[i]->mu, std::try_to_lock);
        if (lk.owns_lock()) out[i].sets = all[i]->ps.size();
    }
    return out;
}

std::optional<SessionManager::Summary> SessionManager::summary(const std::string& id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = sessions_.find(id);
    if (it == sessions_.end()) return std::nullopt;
    return summarise(*it->second, Clock::now());
}

size_t SessionManager::count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return sessions_.size();
}

size_t SessionManager::bytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    size_t b = 0;
    for (const auto& kv : sessions_) b += kv.second->bytes.load();
    return b;
}

}  // namespace pando
