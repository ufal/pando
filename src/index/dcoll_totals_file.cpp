#include "index/dcoll_totals_file.h"

#include "corpus/corpus.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <unordered_map>

namespace fs = std::filesystem;

namespace pando {

namespace {

constexpr uint64_t kMagic = 0x314C4C4F43444450ull;   // "PDDCOLL1"
constexpr uint64_t kVersion = 1;
// a token's ancestors are followed at most this far
constexpr size_t kMaxDepth = 4096;

bool newer_or_same(const std::string& a, const std::string& b) {
    std::error_code ec;
    const auto ta = fs::last_write_time(a, ec);
    if (ec) return false;
    const auto tb = fs::last_write_time(b, ec);
    return ec || ta >= tb;   // a source that is not there (dep.head after --compact-deps) is no reason
}

const PositionalAttr* deprel_attr(const Corpus& corpus) {
    return corpus.has_attr("deprel") && !corpus.is_multivalue("deprel") ? &corpus.attr("deprel") : nullptr;
}

// what the file is counted from
std::vector<std::string> sources(const Corpus& corpus, const std::string& attr) {
    const std::string d = corpus.dir();
    std::vector<std::string> s{corpus.attr(attr).base_path() + ".lex", corpus.attr(attr).base_path() + ".dat",
                               d + "/dep.head", d + "/dep.head_rel", d + "/dep.head_rel8"};
    if (const PositionalAttr* dr = deprel_attr(corpus)) {
        s.push_back(dr->base_path() + ".lex");
        s.push_back(dr->base_path() + ".dat");
    }
    return s;
}

}  // namespace

void for_each_ancestor_count(const DependencyIndex& deps, CorpusPos n,
                             const std::function<void(CorpusPos, uint32_t)>& f) {
    constexpr uint32_t kOnPath = UINT32_MAX;
    std::unordered_map<CorpusPos, uint32_t> depth;   // tokens near the current one (heads lie in its sentence)
    std::vector<CorpusPos> path;
    int64_t hint = -1;
    for (CorpusPos c = 0; c < n; ++c) {
        if (depth.size() > 4096) depth.clear();
        path.clear();
        CorpusPos p = c;
        uint32_t base = 0;
        for (;;) {
            auto it = depth.find(p);
            if (it != depth.end()) {
                base = it->second == kOnPath ? 0 : it->second;   // a loop: no ancestors beyond it
                break;
            }
            const CorpusPos h = deps.head_from(p, hint);
            if (h == NO_HEAD || h == p || path.size() >= kMaxDepth) {
                depth[p] = 0;
                break;
            }
            depth[p] = kOnPath;
            path.push_back(p);
            p = h;
        }
        // path = c, its head, …: each has one ancestor more than the next
        uint32_t d = base;
        for (auto i = path.rbegin(); i != path.rend(); ++i) depth[*i] = ++d;
        f(c, depth[c]);
    }
}

std::string DcollTotalsFile::path_for(const Corpus& corpus, const std::string& attr) {
    return corpus.attr(attr).base_path() + ".dcoll";
}

bool DcollTotalsFile::build(const Corpus& corpus, const std::vector<std::string>& attrs, std::string* err) {
    auto fail = [&](const std::string& m) { if (err) *err = m; return false; };
    if (!corpus.has_deps()) return fail("dcoll totals: the corpus has no dependency index");
    struct Acc {
        std::string name;
        const PositionalAttr* pa;
        std::vector<uint64_t> children, heads, desc;
        uint64_t tc = 0, th = 0, td = 0;
        std::vector<uint64_t> deprel_total, deprel_start{0};
        std::vector<Entry> entries;
    };
    std::vector<Acc> acc;
    for (const auto& a : attrs) {
        if (!corpus.has_attr(a) || corpus.is_multivalue(a))
            return fail("dcoll totals: no single-valued positional attribute '" + a + "'");
        const PositionalAttr& pa = corpus.attr(a);
        const size_t V = pa.lexicon().size();
        acc.push_back({a, &pa, std::vector<uint64_t>(V), std::vector<uint64_t>(V), std::vector<uint64_t>(V)});
    }
    if (acc.empty()) return true;
    const DependencyIndex& deps = corpus.deps();
    const CorpusPos n = corpus.size();
    auto id_of = [](const Acc& x, CorpusPos p) -> int64_t {
        const LexiconId id = x.pa->id_at(p);
        return id >= 0 && static_cast<size_t>(id) < x.children.size() ? id : -1;
    };

    // children and heads: one pass in corpus order
    int64_t hint = -1;
    for (CorpusPos c = 0; c < n; ++c) {
        const CorpusPos h = deps.head_from(c, hint);
        if (h == NO_HEAD) continue;
        for (auto& x : acc) {
            if (const int64_t y = id_of(x, c); y >= 0) { ++x.children[y]; ++x.tc; }
            if (h != c)
                if (const int64_t y = id_of(x, h); y >= 0) { ++x.heads[y]; ++x.th; }
        }
    }
    // descendants: each token once per ancestor
    for_each_ancestor_count(deps, n, [&](CorpusPos c, uint32_t k) {
        if (!k) return;
        for (auto& x : acc)
            if (const int64_t y = id_of(x, c); y >= 0) { x.desc[y] += k; x.td += k; }
    });
    // per deprel: its tokens with a head, counted by value (dense, reset from the values touched)
    if (const PositionalAttr* dr = deprel_attr(corpus)) {
        const size_t D = dr->lexicon().size();
        std::vector<std::vector<uint64_t>> cnt(acc.size());
        std::vector<std::vector<uint32_t>> touched(acc.size());
        for (size_t i = 0; i < acc.size(); ++i) cnt[i].assign(acc[i].children.size(), 0);
        for (size_t d = 0; d < D; ++d) {
            int64_t dh = -1;
            dr->for_each_position_id(static_cast<LexiconId>(d), [&](CorpusPos c) {
                if (deps.head_from(c, dh) == NO_HEAD) return true;
                for (size_t i = 0; i < acc.size(); ++i)
                    if (const int64_t y = id_of(acc[i], c); y >= 0)
                        if (cnt[i][y]++ == 0) touched[i].push_back(static_cast<uint32_t>(y));
                return true;
            });
            for (size_t i = 0; i < acc.size(); ++i) {
                auto& x = acc[i];
                std::sort(touched[i].begin(), touched[i].end());
                uint64_t total = 0;
                for (uint32_t y : touched[i]) {
                    x.entries.push_back({y, 0, cnt[i][y]});
                    total += cnt[i][y];
                    cnt[i][y] = 0;
                }
                touched[i].clear();
                x.deprel_total.push_back(total);
                x.deprel_start.push_back(x.entries.size());
            }
        }
    }

    for (auto& x : acc) {
        const uint64_t V = x.children.size(), D = x.deprel_total.size();
        std::vector<uint64_t> words{kMagic, kVersion, static_cast<uint64_t>(n), V, D, x.tc, x.th, x.td};
        for (const auto* v : {&x.children, &x.heads, &x.desc, &x.deprel_total}) words.insert(words.end(), v->begin(), v->end());
        if (D) words.insert(words.end(), x.deprel_start.begin(), x.deprel_start.end());
        else words.push_back(0);   // deprel_start[0]
        const std::string path = path_for(corpus, x.name), tmp = path + ".tmp";
        std::FILE* out = std::fopen(tmp.c_str(), "wb");
        if (!out) return fail("cannot write " + tmp);
        bool ok = std::fwrite(words.data(), sizeof(uint64_t), words.size(), out) == words.size();
        if (!x.entries.empty())
            ok = ok && std::fwrite(x.entries.data(), sizeof(Entry), x.entries.size(), out) == x.entries.size();
        ok = std::fclose(out) == 0 && ok;
        std::error_code ec;
        if (ok) fs::rename(tmp, path, ec);
        if (!ok || ec) {
            fs::remove(tmp, ec);
            return fail("cannot write " + path);
        }
    }
    return true;
}

std::shared_ptr<const DcollTotalsFile> DcollTotalsFile::open(const Corpus& corpus, const std::string& attr) {
    if (!corpus.has_attr(attr) || corpus.is_multivalue(attr) || !corpus.has_deps()) return nullptr;
    const std::string path = path_for(corpus, attr);
    for (const auto& s : sources(corpus, attr))
        if (!newer_or_same(path, s)) return nullptr;
    auto f = std::make_shared<DcollTotalsFile>();
    f->file_ = MmapFile::open(path, false);
    if (!f->file_.valid() || f->file_.size() < kHeader * sizeof(uint64_t)) return nullptr;
    const uint64_t* h = f->hdr();
    const PositionalAttr* dr = deprel_attr(corpus);
    if (h[0] != kMagic || h[1] != kVersion || h[2] != static_cast<uint64_t>(corpus.size())
        || h[3] != corpus.attr(attr).lexicon().size() || h[4] != (dr ? dr->lexicon().size() : 0))
        return nullptr;
    const uint64_t V = h[3], D = h[4];
    const uint64_t words = kHeader + 3 * V + D + (D ? D + 1 : 1);
    if (f->file_.size() < words * sizeof(uint64_t)) return nullptr;
    const uint64_t entries = (f->file_.size() - words * sizeof(uint64_t)) / sizeof(Entry);
    if (D && h[words - 1] != entries) return nullptr;
    return f;
}

bool DcollTotalsFile::up_to_date(const Corpus& corpus, const std::string& attr) {
    return open(corpus, attr) != nullptr;
}

uint64_t DcollTotalsFile::deprel_total(LexiconId d) const {
    if (d < 0 || static_cast<size_t>(d) >= deprels()) return 0;
    return arr(3)[d];
}

uint64_t DcollTotalsFile::deprel_count(LexiconId d, LexiconId value) const {
    if (d < 0 || static_cast<size_t>(d) >= deprels() || value < 0) return 0;
    const uint64_t* start = arr(3) + deprels();
    const uint64_t words = kHeader + 3 * values() + 2 * deprels() + 1;
    const Entry* e = reinterpret_cast<const Entry*>(hdr() + words);
    const Entry* lo = e + start[d];
    const Entry* hi = e + start[d + 1];
    const Entry* it = std::lower_bound(lo, hi, static_cast<uint32_t>(value),
                                       [](const Entry& a, uint32_t v) { return a.value < v; });
    return it != hi && it->value == static_cast<uint32_t>(value) ? it->count : 0;
}

}  // namespace pando
