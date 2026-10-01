#include "index/dependency_index.h"

#include <atomic>
#include <stdexcept>
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>

namespace pando {

// ── DependencyIndex (read-only) ─────────────────────────────────────────

int16_t HeadRelView::escaped(CorpusPos pos) const {
    size_t lo = 0, hi = nexc;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (exc[mid].pos < pos) lo = mid + 1;
        else hi = mid;
    }
    return lo < nexc && exc[lo].pos == pos ? static_cast<int16_t>(exc[lo].d) : 0;
}

static bool readable(const std::string& path) { return std::ifstream(path).good(); }

bool DependencyIndex::have_head_rel8(const std::string& dir, size_t tokens) {
    std::error_code ec;
    return readable(dir + "/dep.head_rel8") && readable(dir + "/dep.head_rel8.exc")
        && std::filesystem::file_size(dir + "/dep.head_rel8", ec) == tokens;
}

void DependencyIndex::open(const std::string& dir,
                           const StructuralAttr& sentences,
                           bool preload) {
    sentences_       = &sentences;
    head_file_       = MmapFile{};
    head_rel_file_   = MmapFile{};
    head_rel8_file_  = MmapFile{};
    head_rel8_exc_   = MmapFile{};
    head_rel_        = HeadRelView{};
    euler_in_file_   = MmapFile::open(dir + "/dep.euler_in", preload);
    euler_out_file_  = MmapFile::open(dir + "/dep.euler_out", preload);
    n_ = euler_in_file_.size() / sizeof(int16_t);
    // dep.head (sentence-local heads) may be absent once dep.head_rel8 has it all
    // (P4.3b, pando-index --upgrade --compact-deps)
    if (readable(dir + "/dep.head")) {
        head_file_ = MmapFile::open(dir + "/dep.head", preload);
        if (head_file_.size() != n_ * sizeof(int16_t)) head_file_ = MmapFile{};
    }
    if (have_head_rel8(dir, n_)) {
        head_rel8_file_ = MmapFile::open(dir + "/dep.head_rel8", preload);
        head_rel8_exc_ = MmapFile::open(dir + "/dep.head_rel8.exc", preload);
        head_rel_.r8 = head_rel8_file_.as<int8_t>();
        head_rel_.exc = head_rel8_exc_.size() ? head_rel8_exc_.as<HeadRelException>() : nullptr;
        head_rel_.nexc = head_rel8_exc_.size() / sizeof(HeadRelException);
    } else if (readable(dir + "/dep.head_rel")) {
        MmapFile rel = MmapFile::open(dir + "/dep.head_rel", preload);
        if (rel.valid() && rel.size() == n_ * sizeof(int16_t)) {   // size mismatch → ignore stale file
            head_rel_file_ = std::move(rel);
            head_rel_.r16 = head_rel_file_.as<int16_t>();
        }
    }
    if (!head_file_.valid() && !head_rel_)
        throw std::runtime_error("dependency index in " + dir + ": neither dep.head nor dep.head_rel8");
    cache_gen_ = next_cache_gen();  // invalidates every thread's cache entry for this index
}

bool DependencyIndex::write_head_rel8(const std::string& dir, std::string* err) const {
    std::vector<int8_t> r(n_, 0);
    std::vector<HeadRelException> exc;
    int64_t hint = -1;
    for (size_t p = 0; p < n_; ++p) {
        const CorpusPos h = head_from(static_cast<CorpusPos>(p), hint);
        if (h == NO_HEAD) continue;
        const int64_t d = h - static_cast<CorpusPos>(p);
        if (d == 0) continue;
        if (d >= -127 && d <= 127) {
            r[p] = static_cast<int8_t>(d);
        } else {
            r[p] = HeadRelView::kEscape;
            exc.push_back({static_cast<int64_t>(p), d});
        }
    }
    auto put = [&](const std::string& name, const void* data, size_t bytes) -> bool {
        const std::string tmp = dir + "/" + name + ".tmp";
        FILE* f = std::fopen(tmp.c_str(), "wb");
        if (!f || (bytes && std::fwrite(data, 1, bytes, f) != bytes)) {
            if (f) std::fclose(f);
            if (err) *err = "cannot write " + tmp;
            return false;
        }
        if (std::fclose(f) != 0 || std::rename(tmp.c_str(), (dir + "/" + name).c_str()) != 0) {
            if (err) *err = "cannot write " + dir + "/" + name;
            return false;
        }
        return true;
    };
    // the exceptions first: dep.head_rel8 (checked by have_head_rel8) completes the pair
    return put("dep.head_rel8.exc", exc.data(), exc.size() * sizeof(HeadRelException))
        && put("dep.head_rel8", r.data(), r.size());
}

CorpusPos DependencyIndex::head(CorpusPos pos) const {
    int64_t hint = -1;
    return head_from(pos, hint);
}

CorpusPos DependencyIndex::head_from(CorpusPos pos, int64_t& sentence_hint) const {
    // Defensive bounds check to avoid OOB access on malformed queries/plans
    if (pos < 0 || static_cast<size_t>(pos) >= n_)
        return NO_HEAD;
    if (head_rel_) {
        const int16_t d = head_rel_[pos];
        return d ? pos + d : NO_HEAD;
    }

    int16_t local = head_file_.as<int16_t>()[pos];
    if (local == -1) return NO_HEAD;
    int64_t ri = sentences_->find_region_from(pos, sentence_hint);
    if (ri < 0) return NO_HEAD;
    sentence_hint = ri;
    Region sent = sentences_->get(static_cast<size_t>(ri));
    return sent.start + static_cast<CorpusPos>(local);
}

bool DependencyIndex::write_head_rel_file(const std::string& dir, const StructuralAttr& sentences,
                                          std::string* err) {
    MmapFile head = MmapFile::open(dir + "/dep.head", false);
    if (!head.valid()) {
        if (err) *err = "cannot open " + dir + "/dep.head";
        return false;
    }
    const size_t n = head.size() / sizeof(int16_t);
    const int16_t* h = head.as<int16_t>();
    std::vector<int16_t> rel(n, 0);
    const size_t ns = sentences.region_count();
    const Region* r = sentences.region_data();
    for (size_t si = 0; si < ns; ++si) {
        const CorpusPos s = r[si].start, e = r[si].end;
        if (s < 0 || e < s) continue;
        for (CorpusPos p = s; p <= e && static_cast<size_t>(p) < n; ++p) {
            const int16_t local = h[p];
            if (local < 0) continue;
            const CorpusPos hp = s + local;
            if (hp < 0 || static_cast<size_t>(hp) >= n) continue;
            const int64_t d = hp - p;
            if (d == 0 || d < INT16_MIN || d > INT16_MAX) continue;
            rel[static_cast<size_t>(p)] = static_cast<int16_t>(d);
        }
    }
    const std::string tmp = dir + "/dep.head_rel.tmp";
    FILE* f = std::fopen(tmp.c_str(), "wb");
    if (!f || std::fwrite(rel.data(), sizeof(int16_t), n, f) != n) {
        if (f) std::fclose(f);
        if (err) *err = "cannot write " + tmp;
        return false;
    }
    std::fclose(f);
    if (std::rename(tmp.c_str(), (dir + "/dep.head_rel").c_str()) != 0) {
        if (err) *err = "cannot rename " + tmp;
        return false;
    }
    return true;
}

// EX-2p: Build or reuse the sentence-local children map.
// Single-entry cache: when seeds arrive in corpus order (the common case),
// consecutive calls land in the same sentence, so a single cached sentence
// captures most of the reuse.  Cache miss cost = one O(sentence_length) scan,
// same as the old per-call scan.
namespace {
uint64_t next_cache_gen_impl() {
    static std::atomic<uint64_t> g{0};
    return ++g;
}
}  // namespace

uint64_t DependencyIndex::next_cache_gen() { return next_cache_gen_impl(); }

void DependencyIndex::clear_children_cache() const {
    tl_children_cache().sentence_id = -1;
}

DependencyIndex::ChildrenCache& DependencyIndex::tl_children_cache() {
    thread_local ChildrenCache c;
    return c;
}

const DependencyIndex::ChildrenCache& DependencyIndex::ensure_children_cache(CorpusPos pos) const {
    ChildrenCache& c = tl_children_cache();
    if (c.gen != cache_gen_) {
        c.gen = cache_gen_;
        c.sentence_id = -1;
    }
    int64_t ri = sentences_->find_region(pos);
    if (ri < 0) {
        // pos outside any sentence — an invalid region, caller checks
        c.sentence_id = -1;
        c.sentence = {0, -1};
        c.children.clear();
        return c;
    }
    if (ri == c.sentence_id) return c;

    Region sent = sentences_->get(static_cast<size_t>(ri));
    size_t sent_len = static_cast<size_t>(sent.end - sent.start + 1);

    c.children.assign(sent_len, {});
    const int16_t* heads = head_local_data();
    for (size_t i = 0; i < sent_len; ++i) {
        const CorpusPos p = sent.start + static_cast<CorpusPos>(i);
        int64_t h;
        if (heads) {
            h = heads[p];
        } else {   // from the head offsets (no dep.head)
            const int16_t d = head_rel_[p];
            h = d ? p + d - sent.start : -1;
        }
        if (h >= 0 && static_cast<size_t>(h) < sent_len)
            c.children[static_cast<size_t>(h)].push_back(static_cast<int16_t>(i));
    }

    c.sentence_id = ri;
    c.sentence = sent;
    return c;
}

std::vector<CorpusPos> DependencyIndex::children(CorpusPos pos) const {
    const ChildrenCache& c = ensure_children_cache(pos);
    const Region sent = c.sentence;
    if (sent.end < sent.start) return {};  // invalid sentence

    int16_t my_local = static_cast<int16_t>(pos - sent.start);
    if (my_local < 0 || static_cast<size_t>(my_local) >= c.children.size())
        return {};

    const auto& ch = c.children[static_cast<size_t>(my_local)];
    std::vector<CorpusPos> result;
    result.reserve(ch.size());
    for (int16_t k : ch)
        result.push_back(sent.start + static_cast<CorpusPos>(k));
    return result;
}

size_t DependencyIndex::children_count(CorpusPos pos) const {
    const ChildrenCache& c = ensure_children_cache(pos);
    const Region sent = c.sentence;
    if (sent.end < sent.start) return 0;

    int16_t my_local = static_cast<int16_t>(pos - sent.start);
    if (my_local < 0 || static_cast<size_t>(my_local) >= c.children.size())
        return 0;

    return c.children[static_cast<size_t>(my_local)].size();
}

std::vector<CorpusPos> DependencyIndex::subtree(CorpusPos pos) const {
    const ChildrenCache& c = ensure_children_cache(pos);
    const Region sent = c.sentence;
    if (sent.end < sent.start) return {};

    int16_t my_local = static_cast<int16_t>(pos - sent.start);
    if (my_local < 0 || static_cast<size_t>(my_local) >= c.children.size())
        return {};

    // DFS from my_local using the cached children map
    std::vector<CorpusPos> result;
    const auto& root_ch = c.children[static_cast<size_t>(my_local)];
    std::vector<int16_t> stack(root_ch.begin(), root_ch.end());
    while (!stack.empty()) {
        int16_t cur = stack.back();
        stack.pop_back();
        result.push_back(sent.start + static_cast<CorpusPos>(cur));
        if (static_cast<size_t>(cur) < c.children.size()) {
            for (int16_t k : c.children[static_cast<size_t>(cur)])
                stack.push_back(k);
        }
    }
    return result;
}

std::vector<CorpusPos> DependencyIndex::ancestors(CorpusPos pos) const {
    std::vector<CorpusPos> result;
    CorpusPos cur = head(pos);
    while (cur != NO_HEAD) {
        result.push_back(cur);
        cur = head(cur);
    }
    return result;
}

size_t DependencyIndex::depth(CorpusPos pos) const {
    size_t d = 0;
    CorpusPos cur = head(pos);
    while (cur != NO_HEAD) {
        ++d;
        cur = head(cur);
    }
    return d;
}

int16_t DependencyIndex::euler_in(CorpusPos pos) const {
    return euler_in_file_.as<int16_t>()[pos];
}

int16_t DependencyIndex::euler_out(CorpusPos pos) const {
    return euler_out_file_.as<int16_t>()[pos];
}

bool DependencyIndex::is_ancestor(CorpusPos anc, CorpusPos desc) const {
    int64_t sa = sentences_->find_region(anc);
    int64_t sd = sentences_->find_region(desc);
    if (sa != sd) return false;
    return euler_in(anc) < euler_in(desc) && euler_out(anc) > euler_out(desc);
}

} // namespace pando
