#include "index/head_attr.h"
#include "corpus/corpus.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace pando {

std::string HeadAttr::source_of(const std::string& name) {
    const std::string p = kPrefix;
    return name.size() > p.size() && name.compare(0, p.size(), p) == 0 ? name.substr(p.size()) : "";
}

namespace {

bool newer_or_same(const std::string& a, const std::string& b) {
    std::error_code ec;
    const auto ta = fs::last_write_time(a, ec);
    if (ec) return false;
    const auto tb = fs::last_write_time(b, ec);
    return ec || ta >= tb;
}

bool listed_in_info(const std::string& dir, const std::string& name) {
    std::ifstream in(dir + "/corpus.info");
    for (std::string line; std::getline(in, line);) {
        if (line.rfind("positional=", 0) != 0) continue;
        size_t from = 11;
        while (from <= line.size()) {
            size_t to = line.find(',', from);
            if (to == std::string::npos) to = line.size();
            if (line.compare(from, to - from, name) == 0) return true;
            from = to + 1;
        }
    }
    return false;
}

bool add_to_info(const std::string& dir, const std::string& name, std::string* err) {
    if (listed_in_info(dir, name)) return true;
    const std::string path = dir + "/corpus.info", tmp = path + ".tmp";
    std::ifstream in(path);
    if (!in) {
        if (err) *err = "cannot read " + path;
        return false;
    }
    std::vector<std::string> lines;
    bool done = false;
    for (std::string line; std::getline(in, line);) {
        if (!done && line.rfind("positional=", 0) == 0) {
            line += (line.size() > 11 ? "," : "") + name;
            done = true;
        }
        lines.push_back(line);
    }
    in.close();
    if (!done) lines.push_back("positional=" + name);
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
    if (ec && err) *err = "cannot rename " + tmp + ": " + ec.message();
    return !ec;
}

template <typename T>
bool write_vec_ok(const std::string& path, const std::vector<T>& v) {
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) return false;
    const bool ok = v.empty() || std::fwrite(v.data(), sizeof(T), v.size(), f) == v.size();
    return std::fclose(f) == 0 && ok;
}

}  // namespace

bool HeadAttr::up_to_date(const Corpus& corpus, const std::string& attr) {
    const std::string name = name_for(attr);
    if (!corpus.has_attr(name) || !listed_in_info(corpus.dir(), name)) return false;
    const std::string b = corpus.dir() + "/" + name;
    const std::string src = corpus.attr(attr).base_path();
    if (!newer_or_same(b + ".rev.idx", src + ".rev.idx") || !newer_or_same(b + ".rev.idx", src + ".dat")
        || !newer_or_same(b + ".rev.idx", corpus.dir() + "/dep.head_rel"))
        return false;
    const auto nsrc = corpus.attr(attr).lexicon().size(), nh = corpus.attr(name).lexicon().size();
    return nh == nsrc || nh == nsrc + 1;
}

bool HeadAttr::build(const Corpus& corpus, const std::string& attr, std::string* err) {
    auto fail = [&](const std::string& m) { if (err) *err = m; return false; };
    if (!corpus.has_deps()) return fail("corpus has no dependency index");
    const int16_t* hrel = corpus.deps().head_rel_data();
    if (!hrel) return fail("dep.head_rel missing (run pando-index --upgrade first)");
    if (!source_of(attr).empty()) return fail("--head-attrs " + attr + ": already a head attribute");
    if (!corpus.has_attr(attr) || corpus.is_multivalue(attr))
        return fail("--head-attrs " + attr + ": no single-valued positional attribute '" + attr + "'");
    const PositionalAttr& A = corpus.attr(attr);
    const Lexicon& lex = A.lexicon();
    const LexiconId na = lex.size();
    const std::string name = name_for(attr);
    const std::string base = corpus.dir() + "/" + name;
    {   // a rebuild: derived files of the old one (folds, bitmaps, packed postings) go
        std::error_code ec;
        const std::string pre = name + ".";
        for (const auto& e : fs::directory_iterator(corpus.dir(), ec))
            if (e.path().filename().string().rfind(pre, 0) == 0) fs::remove(e.path(), ec);
    }

    // Lexicon: A's sorted values with kNoHead inserted in order (ids at or above
    // the insertion point move up by one).
    const std::string_view none = kNoHead;
    LexiconId ins = 0, none_id = -1;
    while (ins < na && lex.get(ins) < none) ++ins;
    if (ins < na && lex.get(ins) == none) none_id = ins;
    const bool inserted = none_id < 0;
    if (inserted) none_id = ins;
    const LexiconId nh = na + (inserted ? 1 : 0);
    auto new_id = [&](LexiconId id) { return inserted && id >= ins ? id + 1 : id; };
    {
        std::vector<int64_t> off;
        off.reserve(static_cast<size_t>(nh) + 1);
        FILE* f = std::fopen((base + ".lex.tmp").c_str(), "wb");
        if (!f) return fail("cannot write " + base + ".lex.tmp");
        int64_t at = 0;
        auto put = [&](std::string_view s) {
            off.push_back(at);
            std::fwrite(s.data(), 1, s.size(), f);
            std::fputc('\0', f);
            at += static_cast<int64_t>(s.size()) + 1;
        };
        for (LexiconId id = 0; id < na; ++id) {
            if (inserted && id == ins) put(none);
            put(lex.get(id));
        }
        if (inserted && ins == na) put(none);
        off.push_back(at);
        if (std::fclose(f) != 0) return fail("write failed: " + base + ".lex.tmp");
        if (!write_vec_ok(base + ".lex.idx.tmp", off)) return fail("cannot write " + base + ".lex.idx.tmp");
    }

    const CorpusPos n = corpus.size();
    auto head_value = [&](CorpusPos c) -> LexiconId {
        const int16_t d = hrel[c];
        if (d == 0) return none_id;
        const CorpusPos h = c + d;
        if (h < 0 || h >= n) return none_id;
        return new_id(A.id_at(h));
    };

    // .dat (width from the lexicon size) and counts per value
    const int dat_width = nh < 256 ? 1 : (nh < 65536 ? 2 : 4);
    std::vector<int64_t> idx(static_cast<size_t>(nh) + 1, 0);
    {
        FILE* f = std::fopen((base + ".dat.tmp").c_str(), "wb");
        if (!f) return fail("cannot write " + base + ".dat.tmp");
        constexpr size_t kChunk = 1 << 20;
        std::vector<uint8_t> buf;
        buf.reserve(kChunk * 4);
        for (CorpusPos c = 0; c < n; ++c) {
            const LexiconId v = head_value(c);
            ++idx[static_cast<size_t>(v) + 1];
            if (dat_width == 1) buf.push_back(static_cast<uint8_t>(v));
            else if (dat_width == 2) {
                const uint16_t x = static_cast<uint16_t>(v);
                buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&x), reinterpret_cast<const uint8_t*>(&x) + 2);
            } else {
                const int32_t x = v;
                buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(&x), reinterpret_cast<const uint8_t*>(&x) + 4);
            }
            if (buf.size() >= kChunk * 4) {
                std::fwrite(buf.data(), 1, buf.size(), f);
                buf.clear();
            }
        }
        std::fwrite(buf.data(), 1, buf.size(), f);
        if (std::fclose(f) != 0) return fail("write failed: " + base + ".dat.tmp");
    }
    for (size_t i = 1; i < idx.size(); ++i) idx[i] += idx[i - 1];
    if (!write_vec_ok(base + ".rev.idx.tmp", idx)) return fail("cannot write " + base + ".rev.idx.tmp");

    // .rev: positions per value (same width as the source attribute's postings)
    const int rw = A.rev_width();
    const size_t bytes = static_cast<size_t>(n) * static_cast<size_t>(rw);
    {
        const std::string rp = base + ".rev.tmp";
        const int fd = ::open(rp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) return fail("cannot write " + rp);
        if (bytes && ::ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
            ::close(fd);
            return fail("cannot size " + rp);
        }
        void* m = bytes ? ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : nullptr;
        if (bytes && m == MAP_FAILED) {
            ::close(fd);
            return fail("cannot mmap " + rp);
        }
        std::vector<int64_t> cur(idx.begin(), idx.end() - 1);
        for (CorpusPos c = 0; c < n; ++c) {
            const int64_t at = cur[static_cast<size_t>(head_value(c))]++;
            if (rw == 2) static_cast<int16_t*>(m)[at] = static_cast<int16_t>(c);
            else if (rw == 4) static_cast<int32_t*>(m)[at] = static_cast<int32_t>(c);
            else static_cast<int64_t*>(m)[at] = c;
        }
        if (bytes) {
            ::msync(m, bytes, MS_SYNC);
            ::munmap(m, bytes);
        }
        ::close(fd);
    }
    // .rev.idx last: it dates the attribute (packed postings / up_to_date compare to it)
    for (const char* ext : {".lex", ".lex.idx", ".dat", ".rev", ".rev.idx"}) {
        std::error_code ec;
        fs::rename(base + ext + ".tmp", base + ext, ec);
        if (ec) return fail("cannot rename " + base + ext + ".tmp: " + ec.message());
    }
    return add_to_info(corpus.dir(), name, err);
}

}  // namespace pando
