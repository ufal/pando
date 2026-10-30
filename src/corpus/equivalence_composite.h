#pragma once

// The ids of a multivalue region attribute (`s_tuid` holding `a|b`): value (lexicon id) → its classes and class → its
// values, both as offsets + ids (CSR). Built from the attribute's lexicon (build), or read from `<index>/<attr>.mvc`,
// which the index writes when it measures the equivalence layer (save), so a process does not build it again.
//
// .mvc: 8-byte magic "PANDOMVC", then uint64 version, value count, class count; uint64 value offsets [values + 1],
// uint64 class offsets [classes + 1]; int32 class ids of the values; int32 value ids of the classes.

#include "core/mmap_file.h"
#include "core/types.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace pando {

class EquivalenceComposite {
public:
    struct Ids {
        const LexiconId* p = nullptr;
        size_t n = 0;
        const LexiconId* begin() const { return p; }
        const LexiconId* end() const { return p + n; }
        size_t size() const { return n; }
        LexiconId operator[](size_t i) const { return p[i]; }
    };

    size_t value_count() const { return nv_; }
    size_t class_count() const { return nc_; }
    Ids classes_of(LexiconId v) const {
        if (v < 0 || static_cast<size_t>(v) >= nv_) return {};
        return {vc_ + vo_[v], static_cast<size_t>(vo_[v + 1] - vo_[v])};
    }
    Ids values_of(LexiconId c) const {
        if (c < 0 || static_cast<size_t>(c) >= nc_) return {};
        return {cv_ + co_[c], static_cast<size_t>(co_[c + 1] - co_[c])};
    }

    // nullptr when no value holds a separator (the classes are the values themselves)
    static std::shared_ptr<const EquivalenceComposite> build(LexiconId n,
                                                             const std::function<std::string_view(LexiconId)>& get,
                                                             const char* seps) {
        auto is_sep = [seps](char c) { return c != 0 && std::strchr(seps, c) != nullptr; };
        bool any = false;
        for (LexiconId v = 0; v < n && !any; ++v)
            for (char c : get(v))
                if (is_sep(c)) { any = true; break; }
        if (!any) return nullptr;
        auto comp = std::make_shared<EquivalenceComposite>();
        std::unordered_map<std::string, LexiconId> ids;
        std::vector<uint64_t>& vo = comp->own_vo_;
        std::vector<LexiconId>& vc = comp->own_vc_;
        std::vector<uint32_t> class_n;   // values per class
        vo.reserve(static_cast<size_t>(n) + 1);
        vo.push_back(0);
        for (LexiconId v = 0; v < n; ++v) {
            const std::string_view s = get(v);
            const size_t first = vc.size();
            size_t a = 0;
            while (a <= s.size()) {
                size_t b = a;
                while (b < s.size() && !is_sep(s[b])) ++b;
                const std::string_view part = s.substr(a, b - a);
                if (!part.empty() && part != "_") {
                    auto [it, fresh] = ids.try_emplace(std::string(part), static_cast<LexiconId>(class_n.size()));
                    if (fresh) class_n.push_back(0);
                    bool dup = false;
                    for (size_t i = first; i < vc.size() && !dup; ++i) dup = vc[i] == it->second;
                    if (!dup) {
                        vc.push_back(it->second);
                        ++class_n[static_cast<size_t>(it->second)];
                    }
                }
                a = b + 1;
            }
            vo.push_back(vc.size());
        }
        // class → values, values ascending
        std::vector<uint64_t>& co = comp->own_co_;
        co.assign(class_n.size() + 1, 0);
        for (size_t c = 0; c < class_n.size(); ++c) co[c + 1] = co[c] + class_n[c];
        comp->own_cv_.resize(co.back());
        std::vector<uint64_t> fill(co.begin(), co.end() - 1);
        for (LexiconId v = 0; v < n; ++v)
            for (uint64_t i = vo[v]; i < vo[v + 1]; ++i) comp->own_cv_[fill[vc[i]]++] = v;
        comp->point(static_cast<size_t>(n), class_n.size(), vo.data(), co.data(), vc.data(), comp->own_cv_.data());
        return comp;
    }

    // `path` written next to the attribute; false when it cannot be written
    bool save(const std::string& path) const {
        const std::string tmp = path + ".tmp";
        {
            std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
            if (!f) return false;
            const uint64_t head[3] = {kVersion, nv_, nc_};
            f.write(kMagic, 8);
            f.write(reinterpret_cast<const char*>(head), sizeof head);
            f.write(reinterpret_cast<const char*>(vo_), static_cast<std::streamsize>((nv_ + 1) * sizeof(uint64_t)));
            f.write(reinterpret_cast<const char*>(co_), static_cast<std::streamsize>((nc_ + 1) * sizeof(uint64_t)));
            f.write(reinterpret_cast<const char*>(vc_), static_cast<std::streamsize>(vo_[nv_] * sizeof(LexiconId)));
            f.write(reinterpret_cast<const char*>(cv_), static_cast<std::streamsize>(co_[nc_] * sizeof(LexiconId)));
            if (!f) return false;
        }
        std::error_code ec;
        std::filesystem::rename(tmp, path, ec);
        return !ec;
    }

    // The table in `path` when it was written for this lexicon (`values` ids) and is not older than `lex_path`;
    // nullptr otherwise (missing, from an earlier build, or damaged), and the caller builds it.
    static std::shared_ptr<const EquivalenceComposite> load(const std::string& path, const std::string& lex_path,
                                                            LexiconId values) {
        namespace fs = std::filesystem;
        std::error_code ec;
        if (!fs::exists(path, ec)) return nullptr;
        const auto t = fs::last_write_time(path, ec);
        const auto tl = fs::last_write_time(lex_path, ec);
        if (ec || t < tl) return nullptr;
        auto comp = std::make_shared<EquivalenceComposite>();
        try {
            comp->map_ = MmapFile::open(path);
        } catch (const std::exception&) {
            return nullptr;
        }
        const size_t size = comp->map_.size();
        const char* b = static_cast<const char*>(comp->map_.data());
        constexpr size_t H = 8 + 3 * sizeof(uint64_t);
        if (size < H || std::memcmp(b, kMagic, 8) != 0) return nullptr;
        uint64_t head[3];
        std::memcpy(head, b + 8, sizeof head);
        if (head[0] != kVersion || head[1] != static_cast<uint64_t>(values)) return nullptr;
        const size_t nv = head[1], nc = head[2];
        const size_t offs = H + (nv + 1 + nc + 1) * sizeof(uint64_t);
        if (size < offs) return nullptr;
        const auto* vo = reinterpret_cast<const uint64_t*>(b + H);
        const auto* co = vo + nv + 1;
        if (vo[0] != 0 || co[0] != 0 || size != offs + (vo[nv] + co[nc]) * sizeof(LexiconId)) return nullptr;
        const auto* vc = reinterpret_cast<const LexiconId*>(b + offs);
        comp->point(nv, nc, vo, co, vc, vc + vo[nv]);
        return comp;
    }

private:
    static constexpr char kMagic[9] = "PANDOMVC";
    static constexpr uint64_t kVersion = 1;

    void point(size_t nv, size_t nc, const uint64_t* vo, const uint64_t* co, const LexiconId* vc,
               const LexiconId* cv) {
        nv_ = nv;
        nc_ = nc;
        vo_ = vo;
        co_ = co;
        vc_ = vc;
        cv_ = cv;
    }

    size_t nv_ = 0, nc_ = 0;
    const uint64_t* vo_ = nullptr;
    const uint64_t* co_ = nullptr;
    const LexiconId* vc_ = nullptr;
    const LexiconId* cv_ = nullptr;
    std::vector<uint64_t> own_vo_, own_co_;   // built
    std::vector<LexiconId> own_vc_, own_cv_;
    MmapFile map_;                            // or read
};

}  // namespace pando
