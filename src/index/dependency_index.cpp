#include "index/dependency_index.h"

#include <atomic>
#include <stdexcept>
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <vector>

namespace pando {

// ── DependencyIndex (read-only) ─────────────────────────────────────────

void DependencyIndex::open(const std::string& dir,
                           const StructuralAttr& sentences,
                           bool preload) {
    sentences_       = &sentences;
    head_file_       = MmapFile::open(dir + "/dep.head", preload);
    euler_in_file_   = MmapFile::open(dir + "/dep.euler_in", preload);
    euler_out_file_  = MmapFile::open(dir + "/dep.euler_out", preload);
    head_rel_file_   = MmapFile{};
    {
        std::ifstream probe(dir + "/dep.head_rel");
        if (probe.good()) {
            MmapFile rel = MmapFile::open(dir + "/dep.head_rel", preload);
            if (rel.valid() && rel.size() == head_file_.size())
                head_rel_file_ = std::move(rel);   // size mismatch → ignore stale file
        }
    }
    cache_gen_ = next_cache_gen();  // invalidates every thread's cache entry for this index
}

CorpusPos DependencyIndex::head(CorpusPos pos) const {
    int64_t hint = -1;
    return head_from(pos, hint);
}

CorpusPos DependencyIndex::head_from(CorpusPos pos, int64_t& sentence_hint) const {
    // Defensive bounds check to avoid OOB access on malformed queries/plans
    size_t n = head_file_.size() / sizeof(int16_t);
    if (pos < 0 || static_cast<size_t>(pos) >= n)
        return NO_HEAD;
    if (const int16_t* rel = head_rel_data()) {
        int16_t d = rel[pos];
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
    const int16_t* heads = head_file_.as<int16_t>();
    for (size_t i = 0; i < sent_len; ++i) {
        int16_t h = heads[sent.start + static_cast<CorpusPos>(i)];
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
