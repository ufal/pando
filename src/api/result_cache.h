#pragma once

// P6.4: recent results by what determines them, shared by every request and
// session of one server (one corpus): KonText asks for the same concordance,
// frequency list or sort several times (submit, view, status, each page).
//
// A key is built from the query / program text, the parser options and the
// output options that change the result; values are a JSON result (`count`,
// `freq`, `coll`, …), a page of hits (re-rendered for the request) or a sort
// index (P7.10). Least recently used entries go first once the byte budget is
// reached; an entry larger than an eighth of the budget is not kept.

#include "query/executor.h"

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace pando {

class ResultCache {
public:
    struct Entry {
        std::string json;                           // a command's JSON result
        std::shared_ptr<const MatchSet> page;       // or a page of hits
        std::shared_ptr<const SortIndex> sort;      // or a sort index
        size_t bytes = 0;                           // estimated (key included)
    };
    struct Stats {
        size_t entries = 0, bytes = 0, max_bytes = 0;
        size_t hits = 0, misses = 0, stored = 0, evicted = 0;
    };

    explicit ResultCache(size_t max_bytes) : max_bytes_(max_bytes) {}

    /// nullptr when absent (counted as a miss); a hit becomes the most recent.
    std::shared_ptr<const Entry> get(const std::string& key);
    /// Store (replace) an entry; `bytes` is computed when 0.
    void put(const std::string& key, Entry e);
    void clear();
    Stats stats() const;
    size_t max_bytes() const { return max_bytes_; }

    static size_t bytes_of(const MatchSet& ms);
    static size_t bytes_of(const SortIndex& si);

private:
    using Item = std::pair<std::string, std::shared_ptr<const Entry>>;
    mutable std::mutex mu_;
    size_t max_bytes_;
    size_t bytes_ = 0;
    std::list<Item> lru_;   // front = most recent
    std::unordered_map<std::string, std::list<Item>::iterator> index_;
    size_t hits_ = 0, misses_ = 0, stored_ = 0, evicted_ = 0;
};

/// The "\x1f"-separated key of the parts (a separator that CQL text does not use).
std::string cache_key(std::initializer_list<std::string_view> parts);

}  // namespace pando
