#include "index/index_publish.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <random>
#include <system_error>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace pando {

std::string IndexPublish::new_index_id() {
    // milliseconds: publishes of one root are serialised (lock) and take longer
    // than that, so ids sort in publish order
    const auto now = std::chrono::system_clock::now();
    const std::time_t secs = std::chrono::system_clock::to_time_t(now);
    const long ms = static_cast<long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);
    char ts[32];
    std::strftime(ts, sizeof ts, "%Y%m%dT%H%M%S", std::gmtime(&secs));
    std::random_device rd;
    const unsigned r = (static_cast<unsigned>(rd()) << 16) ^ static_cast<unsigned>(rd());
    char buf[64];
    std::snprintf(buf, sizeof buf, "%s%03ldZ-%08x", ts, ms, r);
    return buf;
}

bool IndexPublish::set_index_id(const std::string& dir, const std::string& id, std::string* err) {
    const std::string path = dir + "/corpus.info", tmp = path + ".tmp";
    std::ifstream in(path);
    if (!in) {
        if (err) *err = "cannot read " + path;
        return false;
    }
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);)
        if (line.rfind("index_id=", 0) != 0) lines.push_back(line);
    in.close();
    lines.push_back("index_id=" + id);
    {
        std::ofstream out(tmp);
        for (const auto& l : lines) out << l << "\n";
        if (!out) {
            if (err) *err = "cannot write " + tmp;
            return false;
        }
    }
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec && err) *err = "cannot replace " + path + ": " + ec.message();
    return !ec;
}

std::string IndexPublish::read_index_id(const std::string& dir) {
    std::ifstream in(dir + "/corpus.info");
    std::string id;
    for (std::string line; std::getline(in, line);)
        if (line.rfind("index_id=", 0) == 0) id = line.substr(9);
    return id;
}

bool IndexPublish::is_root(const std::string& path) {
    std::error_code ec;
    return fs::is_symlink(fs::path(path) / "current", ec) || fs::is_directory(fs::path(path) / "versions", ec);
}

std::string IndexPublish::root_of(const std::string& dir) {
    std::error_code ec;
    fs::path p(dir);
    while (!p.empty() && p.filename().empty()) p = p.parent_path();   // trailing '/'
    if (p.filename() == "current" && fs::is_symlink(p, ec)) return p.parent_path().string();
    const fs::path canon = fs::weakly_canonical(p, ec);
    if (ec) return "";
    const fs::path parent = canon.parent_path();
    if (parent.filename() == "versions" && is_root(parent.parent_path().string()))
        return parent.parent_path().string();
    return "";
}

std::string IndexPublish::current_version(const std::string& root) {
    std::error_code ec;
    const fs::path cur = fs::path(root) / "current";
    if (!fs::is_symlink(cur, ec)) return "";
    const fs::path c = fs::canonical(cur, ec);
    return ec ? std::string() : c.string();
}

IndexPublish::Session::Session(const std::string& root) : root_(root) {
    std::error_code ec;
    const fs::path r(root_);
    const fs::path cur = r / "current";
    if (fs::exists(cur, ec) && !fs::is_symlink(cur, ec)) {
        err_ = cur.string() + " exists and is not a symlink (not a published root)";
        return;
    }
    if (fs::exists(r, ec) && !fs::is_directory(r, ec)) {
        err_ = root_ + " is not a directory";
        return;
    }
    if (fs::exists(r / "corpus.info", ec)) {
        err_ = root_ + " is an index directory itself; --publish takes the corpus' root "
               "(the index goes to ROOT/versions/<id>, ROOT/current points at it)";
        return;
    }
    fs::create_directories(r / "versions", ec);
    if (ec) {
        err_ = "cannot create " + (r / "versions").string() + ": " + ec.message();
        return;
    }
#ifndef _WIN32
    const std::string lock = (r / ".publish.lock").string();
    lock_fd_ = ::open(lock.c_str(), O_RDWR | O_CREAT, 0644);
    if (lock_fd_ < 0 || ::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        err_ = "another publish of " + root_ + " is running (" + lock + ")";
        if (lock_fd_ >= 0) ::close(lock_fd_);
        lock_fd_ = -1;
        return;
    }
#endif
    // building directories left by an interrupted publish (none can be running: we hold the lock)
    for (const auto& e : fs::directory_iterator(r / "versions", ec))
        if (e.path().filename().string().rfind(".building-", 0) == 0) fs::remove_all(e.path(), ec);
    id_ = new_index_id();
    building_ = (r / "versions" / (".building-" + id_)).string();
    fs::create_directory(building_, ec);
    if (ec) {
        err_ = "cannot create " + building_ + ": " + ec.message();
        return;
    }
    ok_ = true;
}

IndexPublish::Session::~Session() {
    std::error_code ec;
    if (!committed_ && !building_.empty()) fs::remove_all(building_, ec);
#ifndef _WIN32
    if (lock_fd_ >= 0) {
        ::flock(lock_fd_, LOCK_UN);
        ::close(lock_fd_);
    }
#endif
}

bool IndexPublish::Session::copy_current() {
    const std::string cur = current_version(root_);
    if (cur.empty()) {
        err_ = root_ + " has no current version to upgrade";
        return false;
    }
    // file by file, keeping modification times: staleness checks compare them
    // (a packed or derived file older than its source is not used)
    std::error_code ec;
    const fs::path src(cur), dst(building_);
    for (auto it = fs::recursive_directory_iterator(src, ec); !ec && it != fs::recursive_directory_iterator();
         it.increment(ec)) {
        const fs::path rel = fs::relative(it->path(), src, ec);
        if (ec) break;
        const fs::path to = dst / rel;
        if (it->is_directory(ec)) {
            fs::create_directories(to, ec);
            continue;
        }
        fs::copy_file(it->path(), to, fs::copy_options::overwrite_existing, ec);
        if (ec) break;
        fs::last_write_time(to, fs::last_write_time(it->path(), ec), ec);
        if (ec) break;
    }
    if (ec) {
        err_ = "cannot copy " + cur + " to " + building_ + ": " + ec.message();
        return false;
    }
    return true;
}

bool IndexPublish::Session::commit(int keep, std::vector<std::string>* removed) {
    if (!ok_) return false;
    std::error_code ec;
    const fs::path r(root_);
    if (!set_index_id(building_, id_, &err_)) return false;
    const fs::path final_dir = r / "versions" / id_;
    fs::rename(building_, final_dir, ec);
    if (ec) {
        err_ = "cannot rename " + building_ + " to " + final_dir.string() + ": " + ec.message();
        return false;
    }
    committed_ = true;
    const std::string previous = current_version(root_);
    // point `current` at the new version: a new symlink renamed over the old one
    const fs::path tmp = r / (".current.tmp-" + id_);
    fs::remove(tmp, ec);
    fs::create_directory_symlink(fs::path("versions") / id_, tmp, ec);
    if (ec) {
        err_ = "cannot create symlink " + tmp.string() + ": " + ec.message();
        return false;
    }
    fs::rename(tmp, r / "current", ec);
    if (ec) {
        err_ = "cannot switch " + (r / "current").string() + ": " + ec.message();
        fs::remove(tmp, ec);
        return false;
    }
    // prune: keep the newest `keep` versions (at least 2), never the new or the previous current
    keep = std::max(keep, 2);
    std::vector<std::string> versions;
    for (const auto& e : fs::directory_iterator(r / "versions", ec)) {
        const std::string name = e.path().filename().string();
        if (!name.empty() && name[0] != '.' && e.is_directory(ec)) versions.push_back(name);
    }
    std::sort(versions.rbegin(), versions.rend());   // newest first (ids sort by time)
    const std::string prev_name = previous.empty() ? std::string() : fs::path(previous).filename().string();
    for (size_t i = static_cast<size_t>(keep); i < versions.size(); ++i) {
        if (versions[i] == id_ || versions[i] == prev_name) continue;
        fs::remove_all(r / "versions" / versions[i], ec);
        if (!ec && removed) removed->push_back(versions[i]);
    }
    return true;
}

}  // namespace pando
