#include "index/dependency_index.h"
#include <stdexcept>
#include <algorithm>

namespace manatree {

// ── DependencyIndex (read-only) ─────────────────────────────────────────

void DependencyIndex::open(const std::string& dir,
                           const StructuralAttr& sentences,
                           bool preload) {
    sentences_       = &sentences;
    head_file_       = MmapFile::open(dir + "/dep.head", preload);
    euler_in_file_   = MmapFile::open(dir + "/dep.euler_in", preload);
    euler_out_file_  = MmapFile::open(dir + "/dep.euler_out", preload);
}

CorpusPos DependencyIndex::head(CorpusPos pos) const {
    // Defensive bounds check to avoid OOB access on malformed queries/plans
    size_t n = head_file_.size() / sizeof(int16_t);
    if (pos < 0 || static_cast<size_t>(pos) >= n)
        return NO_HEAD;

    int16_t local = head_file_.as<int16_t>()[pos];
    if (local == -1) return NO_HEAD;
    int64_t ri = sentences_->find_region(pos);
    if (ri < 0) return NO_HEAD;
    Region sent = sentences_->get(static_cast<size_t>(ri));
    return sent.start + static_cast<CorpusPos>(local);
}

std::vector<CorpusPos> DependencyIndex::children(CorpusPos pos) const {
    int64_t ri = sentences_->find_region(pos);
    if (ri < 0) return {};
    Region sent = sentences_->get(static_cast<size_t>(ri));
    int16_t my_local = static_cast<int16_t>(pos - sent.start);

    std::vector<CorpusPos> result;
    const int16_t* heads = head_file_.as<int16_t>();
    for (CorpusPos p = sent.start; p <= sent.end; ++p) {
        if (heads[p] == my_local)
            result.push_back(p);
    }
    return result;
}

std::vector<CorpusPos> DependencyIndex::subtree(CorpusPos pos) const {
    int64_t ri = sentences_->find_region(pos);
    if (ri < 0) return {};
    Region sent = sentences_->get(static_cast<size_t>(ri));
    int16_t my_local = static_cast<int16_t>(pos - sent.start);
    int sent_len = static_cast<int>(sent.end - sent.start + 1);

    // One scan to build local children map
    const int16_t* heads = head_file_.as<int16_t>();
    std::vector<std::vector<int16_t>> ch(static_cast<size_t>(sent_len));
    for (int i = 0; i < sent_len; ++i) {
        int16_t h = heads[sent.start + i];
        if (h >= 0)
            ch[static_cast<size_t>(h)].push_back(static_cast<int16_t>(i));
    }

    // DFS from my_local
    std::vector<CorpusPos> result;
    std::vector<int16_t> stack(ch[static_cast<size_t>(my_local)].begin(),
                               ch[static_cast<size_t>(my_local)].end());
    while (!stack.empty()) {
        int16_t cur = stack.back();
        stack.pop_back();
        result.push_back(sent.start + static_cast<CorpusPos>(cur));
        for (int16_t c : ch[static_cast<size_t>(cur)])
            stack.push_back(c);
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

} // namespace manatree
