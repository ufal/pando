#include "index/dep_pair_index.h"
#include "corpus/corpus.h"
#include <cstdio>
#include <filesystem>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace pando {

std::string DepPairIndex::base_path(const std::string& dir, const std::string& head_attr,
                                    const std::string& child_attr) {
    return dir + "/dep.pair." + head_attr + "." + child_attr;
}

static int corpus_rev_width(const Corpus& corpus) {
    for (const auto& a : corpus.attr_names())
        if (corpus.has_attr(a) && !corpus.is_multivalue(a))
            return corpus.attr(a).rev_span_of_id(0).width;
    const CorpusPos n = corpus.size();
    return n <= 32767 ? 2 : (n <= 2147483647 ? 4 : 8);
}

bool DepPairIndex::build(const Corpus& corpus, const std::string& head_attr,
                         const std::string& child_attr, std::string* err) {
    auto fail = [&](const std::string& m) { if (err) *err = m; return false; };
    if (!corpus.has_deps()) return fail("corpus has no dependency index");
    const int16_t* hrel = corpus.deps().head_rel_data();
    if (!hrel) return fail("dep.head_rel missing (run pando-index --upgrade first)");
    if (!corpus.has_attr(head_attr) || corpus.is_multivalue(head_attr))
        return fail("no single-valued positional attribute '" + head_attr + "'");
    if (!corpus.has_attr(child_attr) || corpus.is_multivalue(child_attr))
        return fail("no single-valued positional attribute '" + child_attr + "'");
    const PositionalAttr& H = corpus.attr(head_attr);
    const PositionalAttr& C = corpus.attr(child_attr);
    const int64_t vh = H.lexicon().size(), vc = C.lexicon().size();
    if (vh * vc > kMaxKeys)
        return fail("dep.pair." + head_attr + "." + child_attr + ": " + std::to_string(vh) + " x "
                    + std::to_string(vc) + " keys exceed the limit (low-cardinality attributes only)");
    const CorpusPos n = corpus.size();
    const int width = corpus_rev_width(corpus);
    const std::string base = base_path(corpus.dir(), head_attr, child_attr);

    auto key_of = [&](CorpusPos c) -> int64_t {
        const int16_t d = hrel[c];
        if (d == 0) return -1;
        const CorpusPos h = c + d;
        if (h < 0 || h >= n) return -1;
        return static_cast<int64_t>(H.id_at(h)) * vc + C.id_at(c);
    };

    // pass 1: counts → offsets
    std::vector<int64_t> idx(static_cast<size_t>(vh * vc) + 1, 0);
    for (CorpusPos c = 0; c < n; ++c) {
        const int64_t k = key_of(c);
        if (k >= 0) ++idx[static_cast<size_t>(k) + 1];
    }
    for (size_t k = 1; k < idx.size(); ++k) idx[k] += idx[k - 1];
    const int64_t total = idx.back();

    // pass 2: fill .rev through a writable mapping
    const std::string rev_tmp = base + ".rev.tmp", idx_tmp = base + ".rev.idx.tmp";
    const size_t bytes = static_cast<size_t>(total) * static_cast<size_t>(width);
    int fd = ::open(rev_tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return fail("cannot create " + rev_tmp);
    if (bytes > 0) {
        if (ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
            ::close(fd);
            return fail("cannot size " + rev_tmp);
        }
        void* raw = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (raw == MAP_FAILED) {
            ::close(fd);
            return fail("cannot mmap " + rev_tmp);
        }
        std::vector<int64_t> cur(idx.begin(), idx.end() - 1);
        for (CorpusPos c = 0; c < n; ++c) {
            const int64_t k = key_of(c);
            if (k < 0) continue;
            const int64_t at = cur[static_cast<size_t>(k)]++;
            switch (width) {
                case 2: static_cast<int16_t*>(raw)[at] = static_cast<int16_t>(c); break;
                case 4: static_cast<int32_t*>(raw)[at] = static_cast<int32_t>(c); break;
                default: static_cast<int64_t*>(raw)[at] = static_cast<int64_t>(c); break;
            }
        }
        msync(raw, bytes, MS_SYNC);
        munmap(raw, bytes);
    }
    ::close(fd);
    try {
        write_vec(idx_tmp, idx);
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    std::error_code ec;
    fs::rename(rev_tmp, base + ".rev", ec);
    if (!ec) fs::rename(idx_tmp, base + ".rev.idx", ec);
    if (ec) return fail("cannot rename " + base + ": " + ec.message());
    return true;
}

bool DepPairIndex::open(const Corpus& corpus, const std::string& head_attr,
                        const std::string& child_attr) {
    if (!corpus.has_attr(head_attr) || !corpus.has_attr(child_attr)) return false;
    const std::string base = base_path(corpus.dir(), head_attr, child_attr);
    if (!fs::exists(base + ".rev") || !fs::exists(base + ".rev.idx")) return false;
    const int64_t vh = corpus.attr(head_attr).lexicon().size();
    const int64_t vc = corpus.attr(child_attr).lexicon().size();
    MmapFile idx = MmapFile::open(base + ".rev.idx", false);
    if (!idx.valid() || idx.count<int64_t>() != static_cast<size_t>(vh * vc + 1)) return false;
    const int64_t total = idx.as<int64_t>()[vh * vc];
    const int width = corpus_rev_width(corpus);
    if (static_cast<int64_t>(fs::file_size(base + ".rev")) != total * width) return false;
    if (total > 0) {
        rev_ = MmapFile::open(base + ".rev", false);
        if (!rev_.valid()) return false;
    }
    rev_idx_ = std::move(idx);
    width_ = width;
    vh_ = vh;
    vc_ = vc;
    return true;
}

RevSpan DepPairIndex::span(LexiconId h, LexiconId c) const {
    RevSpan s;
    s.width = width_;
    if (!valid() || h < 0 || c < 0 || h >= vh_ || c >= vc_) return s;
    const int64_t k = static_cast<int64_t>(h) * vc_ + c;
    const auto* idx = rev_idx_.as<int64_t>();
    const int64_t a = idx[k], b = idx[k + 1];
    if (b <= a) return s;
    s.count = static_cast<size_t>(b - a);
    const char* base = static_cast<const char*>(rev_.data());
    s.data = base + static_cast<size_t>(a) * static_cast<size_t>(width_);
    return s;
}

size_t DepPairIndex::count(LexiconId h, LexiconId c) const {
    return span(h, c).count;
}

} // namespace pando
