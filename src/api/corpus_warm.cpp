#include "api/corpus_warm.h"
#include "core/json_utils.h"
#include "corpus/corpus.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace pando {

bool parse_warm_level(const std::string& s, WarmLevel& out) {
    if (s == "none" || s.empty()) out = WarmLevel::None;
    else if (s == "hot") out = WarmLevel::Hot;
    else if (s == "all") out = WarmLevel::All;
    else return false;
    return true;
}

const char* warm_level_name(WarmLevel l) {
    switch (l) {
        case WarmLevel::Hot: return "hot";
        case WarmLevel::All: return "all";
        default: return "none";
    }
}

namespace {

bool ends_with(const std::string& s, const char* suf) {
    const size_t n = std::char_traits<char>::length(suf);
    return s.size() >= n && s.compare(s.size() - n, n, suf) == 0;
}

bool is_hot(const Corpus& corpus, const std::string& name) {
    if (name == "corpus.info" || name == "dep.head_rel") return true;
    for (const char* suf : {".lex", ".lex.idx", ".rev.idx", ".bm", ".bm.idx", ".perm", ".rgn", ".par",
                            ".val", ".val.idx", ".pfb.idx"})
        if (ends_with(name, suf)) return true;
    if (ends_with(name, ".rev")) {   // region attribute values' postings (small); not positional ones
        const std::string base = name.substr(0, name.size() - 4);
        const auto& ra = corpus.region_attr_names();
        return std::find(ra.begin(), ra.end(), base) != ra.end();
    }
    return false;
}

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

}  // namespace

std::vector<std::string> warm_files(const Corpus& corpus, WarmLevel level, uint64_t* total_bytes) {
    std::vector<std::pair<uint64_t, std::string>> found;
    if (level != WarmLevel::None) {
        std::error_code ec;
        for (const auto& e : fs::directory_iterator(corpus.dir(), ec)) {
            std::error_code ec2;
            if (!e.is_regular_file(ec2)) continue;   // follows symlinks
            const std::string name = e.path().filename().string();
            if (level == WarmLevel::Hot && !is_hot(corpus, name)) continue;
            const uint64_t sz = e.file_size(ec2);
            if (ec2 || sz == 0) continue;
            found.emplace_back(sz, e.path().string());
        }
    }
    std::sort(found.begin(), found.end());
    std::vector<std::string> out;
    uint64_t total = 0;
    for (auto& [sz, p] : found) {
        total += sz;
        out.push_back(std::move(p));
    }
    if (total_bytes) *total_bytes = total;
    return out;
}

CorpusWarmer::~CorpusWarmer() {
    stop_ = true;
    if (thread_.joinable()) thread_.join();
}

void CorpusWarmer::start(WarmLevel level) {
    std::lock_guard<std::mutex> start_lk(start_mu_);   // one start() at a time
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (level == WarmLevel::None || level <= level_) return;
        stop_ = true;                  // a lower level still running: extend it
    }
    if (thread_.joinable()) thread_.join();   // run() takes mu_: not held here
    std::lock_guard<std::mutex> lk(mu_);
    stop_ = false;
    level_ = level;
    done_ = false;
    uint64_t total = 0;
    std::vector<std::string> files = warm_files(corpus_, level, &total);
    std::vector<std::string> todo;
    uint64_t already = 0;
    for (auto& f : files) {
        if (std::binary_search(read_.begin(), read_.end(), f)) {
            std::error_code ec;
            already += fs::file_size(f, ec);
            continue;
        }
        todo.push_back(f);
    }
    files_ = files.size();
    files_done_ = files.size() - todo.size();
    bytes_ = total;
    bytes_done_ = already;
    if (start_ns_ == 0) start_ns_ = now_ns();
    end_ns_ = 0;
    running_ = true;
    thread_ = std::thread([this, todo = std::move(todo)]() mutable { run(std::move(todo)); });
}

void CorpusWarmer::run(std::vector<std::string> files) {
    std::vector<char> buf(size_t{4} << 20);
    for (const auto& f : files) {
        if (stop_) break;
        const int fd = ::open(f.c_str(), O_RDONLY);
        if (fd >= 0) {
#ifdef POSIX_FADV_SEQUENTIAL
            ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
            for (;;) {
                if (stop_) break;
                const ssize_t n = ::read(fd, buf.data(), buf.size());
                if (n <= 0) break;
                bytes_done_ += static_cast<uint64_t>(n);
            }
            ::close(fd);
        }
        if (stop_) break;
        ++files_done_;
        std::lock_guard<std::mutex> lk(mu_);
        read_.insert(std::upper_bound(read_.begin(), read_.end(), f), f);
    }
    std::lock_guard<std::mutex> lk(mu_);
    if (!stop_) {
        done_ = true;
        end_ns_ = now_ns();
    }
    running_ = false;
}

std::string CorpusWarmer::status_json() const {
    std::lock_guard<std::mutex> lk(mu_);
    const char* state = running_ ? "running" : done_ ? "done" : "idle";
    const int64_t s = start_ns_, e = end_ns_ ? end_ns_.load() : (s ? now_ns() : 0);
    char secs[32];
    std::snprintf(secs, sizeof secs, "%.3f", s ? static_cast<double>(e - s) / 1e9 : 0.0);
    return std::string("{\"state\": \"") + state + "\", \"level\": \"" + warm_level_name(level_)
        + "\", \"files\": " + std::to_string(files_) + ", \"files_done\": " + std::to_string(files_done_.load())
        + ", \"bytes\": " + std::to_string(bytes_) + ", \"bytes_done\": " + std::to_string(bytes_done_.load())
        + ", \"seconds\": " + secs + "}";
}

}  // namespace pando
