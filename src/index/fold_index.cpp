#include "index/fold_index.h"
#include "index/fold_map.h"
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <numeric>

namespace pando {

FoldMode fold_mode_for(bool case_fold, bool accent_fold) {
    if (case_fold && accent_fold) return FoldMode::LowerNoAccents;
    if (case_fold) return FoldMode::Lower;
    return FoldMode::NoAccents;
}

const char* fold_mode_suffix(FoldMode m) {
    switch (m) {
        case FoldMode::Lower: return "lc";
        case FoldMode::NoAccents: return "na";
        case FoldMode::LowerNoAccents: return "lcna";
    }
    return "lc";
}

std::string fold_string(FoldMode m, std::string_view s) {
    switch (m) {
        case FoldMode::Lower: return FoldMap::to_lower(s);
        case FoldMode::NoAccents: return FoldMap::strip_accents(s);
        case FoldMode::LowerNoAccents: return FoldMap::to_lower_no_accents(s);
    }
    return std::string(s);
}

std::string FoldIndex::path(const std::string& attr_base, FoldMode m) {
    // ".v2": sorted by the UTF-8 fold (FoldMap::fold). Files of the earlier ASCII /
    // Latin-1 fold (".fold_<m>.perm") are ordered differently and are not opened: %c / %d
    // then fold in memory until `pando-index --upgrade` writes these.
    return attr_base + ".fold_" + fold_mode_suffix(m) + ".v2.perm";
}

bool FoldIndex::build(const std::string& attr_base, const Lexicon& lex, FoldMode m,
                      std::string* err) {
    const LexiconId n = lex.size();
    // Folded strings in one buffer (offsets), then sort ids by (folded, id).
    std::string buf;
    std::vector<int64_t> off(static_cast<size_t>(n) + 1, 0);
    for (LexiconId id = 0; id < n; ++id) {
        buf += fold_string(m, lex.get(id));
        off[static_cast<size_t>(id) + 1] = static_cast<int64_t>(buf.size());
    }
    auto folded = [&](int32_t id) {
        return std::string_view(buf.data() + off[static_cast<size_t>(id)],
                                static_cast<size_t>(off[static_cast<size_t>(id) + 1]
                                                    - off[static_cast<size_t>(id)]));
    };
    std::vector<int32_t> perm(static_cast<size_t>(n));
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](int32_t a, int32_t b) {
        const auto fa = folded(a), fb = folded(b);
        if (fa != fb) return fa < fb;
        return a < b;
    });
    const std::string p = path(attr_base, m);
    const std::string tmp = p + ".tmp";
    try {
        write_vec(tmp, perm);
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        return false;
    }
    if (std::rename(tmp.c_str(), p.c_str()) != 0) {
        if (err) *err = "cannot rename " + tmp;
        return false;
    }
    return true;
}

bool FoldIndex::open(const std::string& attr_base, FoldMode m, LexiconId lex_size) {
    const std::string p = path(attr_base, m);
    {
        std::ifstream probe(p);
        if (!probe.good()) return false;
    }
    MmapFile f = MmapFile::open(p, false);
    if (!f.valid() || f.count<int32_t>() != static_cast<size_t>(lex_size)) return false;
    perm_ = std::move(f);
    mode_ = m;
    return true;
}

std::vector<LexiconId> FoldIndex::lookup(const Lexicon& lex, std::string_view q) const {
    std::vector<LexiconId> out;
    const int32_t* p = perm_.as<int32_t>();
    const size_t n = perm_.count<int32_t>();
    size_t lo = 0, hi = n;
    while (lo < hi) {   // first entry with fold(entry) >= q
        const size_t mid = lo + ((hi - lo) >> 1);
        if (fold_string(mode_, lex.get(p[mid])) < q) lo = mid + 1;
        else hi = mid;
    }
    for (size_t i = lo; i < n; ++i) {
        if (fold_string(mode_, lex.get(p[i])) != q) break;
        out.push_back(p[i]);
    }
    std::sort(out.begin(), out.end());
    return out;
}

} // namespace pando
