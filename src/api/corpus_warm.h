#pragma once

// Warming a corpus: read the index files most queries touch into the OS page
// cache in the background, so the first queries after a corpus is opened (or
// selected in a front-end, as KonText does) do not wait for the disk.
//
//   hot  the small, shared files: lexicons and their indexes, `.rev.idx`,
//        bitmaps (`.bm`, `.bnd.bm`), fold permutations, region files (`.rgn`,
//        region values and their postings), `dep.head_rel`, the indexes of
//        packed and edge postings — not the per-position `.dat` files or the
//        large posting lists, which are read as queries need them;
//   all  every file of the corpus directory (what `--preload` does at open,
//        but in the background).
//
// Files are read sequentially with read() (the OS reads ahead at full speed);
// the corpus' mmaps then find the pages in the cache. Smallest files first.

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace pando {

class Corpus;

enum class WarmLevel { None, Hot, All };

/// "none" / "hot" / "all" (false for anything else).
bool parse_warm_level(const std::string& s, WarmLevel& out);
const char* warm_level_name(WarmLevel l);

/// The files `level` reads, smallest first (paths in the corpus directory).
std::vector<std::string> warm_files(const Corpus& corpus, WarmLevel level, uint64_t* total_bytes = nullptr);

class CorpusWarmer {
public:
    explicit CorpusWarmer(const Corpus& corpus) : corpus_(corpus) {}
    ~CorpusWarmer();
    CorpusWarmer(const CorpusWarmer&) = delete;
    CorpusWarmer& operator=(const CorpusWarmer&) = delete;

    /// Start warming `level` in a background thread. A running warm-up of the
    /// same or a higher level, or a finished one, is left as it is; a lower
    /// running level is extended (the files already read are not read again).
    void start(WarmLevel level);

    /// {"state": "idle"|"running"|"done", "level", "files", "files_done",
    ///  "bytes", "bytes_done", "seconds"}.
    std::string status_json() const;

private:
    void run(std::vector<std::string> files);

    const Corpus& corpus_;
    std::mutex start_mu_;
    mutable std::mutex mu_;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> running_{false};
    WarmLevel level_ = WarmLevel::None;      // the highest level asked for
    bool done_ = false;
    std::vector<std::string> read_;          // files already read (sorted)
    size_t files_ = 0;
    std::atomic<size_t> files_done_{0};
    uint64_t bytes_ = 0;
    std::atomic<uint64_t> bytes_done_{0};
    std::atomic<int64_t> start_ns_{0}, end_ns_{0};
};

}  // namespace pando
