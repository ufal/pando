#include "api/result_cache.h"

namespace pando {

std::string cache_key(std::initializer_list<std::string_view> parts) {
    std::string k;
    for (std::string_view p : parts) {
        k.append(p);
        k.push_back('\x1f');
    }
    return k;
}

size_t ResultCache::bytes_of(const MatchSet& ms) {
    size_t b = sizeof(MatchSet) + ms.matches.capacity() * sizeof(Match);
    for (const Match& m : ms.matches) {
        b += m.positions.heap_bytes() + m.span_ends.heap_bytes();
        b += m.named_regions.size() * 96 + m.token_group_props.size() * 64;
        for (const auto& kv : m.named_dep_subtrees) b += 96 + kv.second.size() * sizeof(CorpusPos);
    }
    b += ms.parallel_matches.size() * 2 * (sizeof(Match) + 64);
    return b;
}

size_t ResultCache::bytes_of(const SortIndex& si) {
    return sizeof(SortIndex) + si.rank_dense.capacity() * sizeof(uint32_t)
           + si.rank_of.size() * 32 + si.start.capacity() * sizeof(size_t);
}

std::shared_ptr<const ResultCache::Entry> ResultCache::get(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) {
        ++misses_;
        return nullptr;
    }
    lru_.splice(lru_.begin(), lru_, it->second);
    ++hits_;
    return it->second->second;
}

void ResultCache::put(const std::string& key, Entry e) {
    if (max_bytes_ == 0) return;
    if (e.bytes == 0) {
        e.bytes = e.json.size();
        if (e.page) e.bytes += bytes_of(*e.page);
        if (e.sort) e.bytes += bytes_of(*e.sort);
    }
    e.bytes += key.size() + 128;
    if (e.bytes > max_bytes_ / 8) return;
    auto entry = std::make_shared<const Entry>(std::move(e));
    std::lock_guard<std::mutex> lk(mu_);
    auto it = index_.find(key);
    if (it != index_.end()) {
        bytes_ -= it->second->second->bytes;
        lru_.erase(it->second);
        index_.erase(it);
    }
    lru_.emplace_front(key, entry);
    index_.emplace(key, lru_.begin());
    bytes_ += entry->bytes;
    ++stored_;
    while (bytes_ > max_bytes_ && !lru_.empty()) {
        bytes_ -= lru_.back().second->bytes;
        index_.erase(lru_.back().first);
        lru_.pop_back();
        ++evicted_;
    }
}

void ResultCache::clear() {
    std::lock_guard<std::mutex> lk(mu_);
    lru_.clear();
    index_.clear();
    bytes_ = 0;
}

ResultCache::Stats ResultCache::stats() const {
    std::lock_guard<std::mutex> lk(mu_);
    Stats s;
    s.entries = index_.size();
    s.bytes = bytes_;
    s.max_bytes = max_bytes_;
    s.hits = hits_;
    s.misses = misses_;
    s.stored = stored_;
    s.evicted = evicted_;
    return s;
}

}  // namespace pando
