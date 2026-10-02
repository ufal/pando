#include "corpus/builder.h"
#include "core/types.h"
#include "index/dependency_index.h"
#include "index/structural_attr.h"
#include "index/fold_index.h"
#include "index/head_attr.h"
#include "index/dep_pair_index.h"
#include "index/bitmap_index.h"
#include "corpus/corpus.h"
#include "core/build_info.h"
#include "index/index_publish.h"
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

namespace fs = std::filesystem;

static std::vector<std::string> find_conllu_files(const std::string& path) {
    std::vector<std::string> files;

    if (fs::is_regular_file(path)) {
        files.push_back(path);
    } else if (fs::is_directory(path)) {
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == ".conllu")
                files.push_back(entry.path().string());
        }
        std::sort(files.begin(), files.end());
    } else {
        throw std::runtime_error("Not a file or directory: " + path);
    }

    if (files.empty())
        throw std::runtime_error("No .conllu files found in " + path);

    return files;
}

static std::vector<std::string> find_vertical_files(const std::string& path) {
    std::vector<std::string> files;
    if (fs::is_regular_file(path)) {
        files.push_back(path);
    } else if (fs::is_directory(path)) {
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            if (entry.is_regular_file()) {
                auto ext = entry.path().extension().string();
                if (ext == ".vrt" || ext == ".vert" || ext == ".txt")
                    files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
    } else {
        throw std::runtime_error("Not a file or directory: " + path);
    }
    if (files.empty())
        throw std::runtime_error("No .vrt/.vert/.txt files found in " + path);
    return files;
}

static pando::CorpusPos read_corpus_size_from_info(const std::string& main_dir) {
    std::ifstream in(main_dir + "/corpus.info");
    if (!in) throw std::runtime_error("Cannot open " + main_dir + "/corpus.info");
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("size=", 0) == 0) {
            long long n = std::stoll(line.substr(5));
            if (n < 0) throw std::runtime_error("Invalid size= in corpus.info");
            return static_cast<pando::CorpusPos>(n);
        }
    }
    throw std::runtime_error("corpus.info missing size= line in " + main_dir);
}

static void write_overlay_info(const std::string& overlay_dir,
                               const std::string& main_dir,
                               const std::string& input_path) {
    std::string path = overlay_dir + "/overlay.info";
    std::ofstream out(path);
    if (!out) throw std::runtime_error("Cannot create " + path);
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
#if defined(_WIN32)
    gmtime_s(&tm_buf, &t);
#else
    gmtime_r(&t, &tm_buf);
#endif
    char iso[32];
    if (std::strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &tm_buf) == 0)
        std::snprintf(iso, sizeof(iso), "(unknown)");
    out << "indexed_at=" << iso << "\n";
    out << "main_corpus=" << main_dir << "\n";
    out << "input_jsonl=" << input_path << "\n";
    {
        namespace fs = std::filesystem;
        fs::path p(overlay_dir);
        if (p.has_filename())
            out << "layer_id=" << p.filename().string() << "\n";
    }
    out << "note=standoff-only overlay; merge at query time with main index (see dev/USER-OVERLAY-ANNOTATIONS.md)\n";
}

// `pando-index --upgrade <corpus_dir>`: add derived files that newer versions
// use when present, without re-indexing. Also run after every build.
//   dep.head_rel                 (P1.8)  relative head offsets
//   <attr>.fold_{lc,na,lcna}.perm (P1.6)  %c / %d / %cd lookups
//   <attr>.bm{,.idx}             (P3.1)  chunked bitmaps, low-cardinality attrs
// P5.2 default: upos × upos edge postings (when the corpus has deps and upos).
static const char* const kDefaultDepPairs = "upos:upos";
// P3.1 default: bitmaps for every single-valued attribute with at most
// BitmapIndex::kAutoMaxValues values (upos, deprel, …).
static const char* const kDefaultBitmaps = "auto";
// P5.6 default: head attributes for upos, deprel and lemma (those the corpus has)
// when it has a dependency index; `--head-attrs none` opts out.
static const char* const kDefaultHeadAttrs = "auto";
static const char* const kAutoHeadAttrs[] = {"upos", "deprel", "lemma"};

static std::vector<std::string> split_list(const std::string& s) {
    std::vector<std::string> out;
    size_t from = 0;
    while (from <= s.size()) {
        size_t to = s.find(',', from);
        if (to == std::string::npos) to = s.size();
        if (to > from) out.push_back(s.substr(from, to - from));
        from = to + 1;
    }
    return out;
}

static int upgrade_bitmaps(const pando::Corpus& corpus, const std::string& spec, bool quiet,
                           const std::function<double()>& secs) {
    if (spec == "none") return 0;
    const bool is_auto = spec == "auto";
    std::vector<std::string> names = is_auto ? corpus.attr_names() : split_list(spec);
    if (is_auto) {
        for (const auto& h : corpus.head_attr_names()) names.push_back(h);
        for (const auto& st : corpus.structure_names()) names.push_back(st);
    }
    for (const auto& name : names) {
        // P3.6: flat structures → <dir>/<name>.bnd.bm (covered positions + region ends)
        if (!corpus.has_attr(name) && corpus.has_structure(name)) {
            if (corpus.is_nested(name) || corpus.is_overlapping(name) || corpus.is_zerowidth(name)) {
                if (!is_auto) {
                    std::cerr << "Error: --bitmaps " << name << ": only flat structures\n";
                    return 1;
                }
                continue;
            }
            const std::string base = pando::BitmapIndex::structure_base(corpus.dir(), name);
            pando::BitmapIndex probe;
            if (probe.open_structure(base, corpus.size())) {
                if (!quiet) std::cerr << "Bitmaps " << pando::BitmapIndex::path(base) << " up to date\n";
                continue;
            }
            std::string err;
            pando::BitmapIndex::BuildStats st;
            if (!pando::BitmapIndex::build_structure(corpus.structure(name), base, corpus.size(), &err, &st)) {
                std::cerr << (is_auto ? "Note: " : "Error: ") << err << "\n";
                if (!is_auto) return 1;
                continue;
            }
            if (!quiet)
                std::cerr << "Wrote " << pando::BitmapIndex::path(base) << " (" << st.entries
                          << " containers; " << (st.payload_bytes >> 20) << " MB; " << secs() << " s)\n";
            continue;
        }
        if (!corpus.has_attr(name) || corpus.is_multivalue(name)) {
            if (!is_auto) {
                std::cerr << "Error: --bitmaps " << name
                          << ": no single-valued positional attribute of that name\n";
                return 1;
            }
            continue;
        }
        const auto& pa = corpus.attr(name);
        if (is_auto && pa.lexicon().size() > pando::BitmapIndex::kAutoMaxValues) continue;
        pando::BitmapIndex probe;
        if (probe.open(pa, corpus.size())) {
            if (!quiet) std::cerr << "Bitmaps " << pando::BitmapIndex::path(pa.base_path())
                                  << " up to date\n";
            continue;
        }
        std::string err;
        pando::BitmapIndex::BuildStats st;
        if (!pando::BitmapIndex::build(pa, corpus.size(), &err, &st)) {
            std::cerr << "Error: " << err << "\n";
            return 1;
        }
        if (!quiet)
            std::cerr << "Wrote " << pando::BitmapIndex::path(pa.base_path()) << " ("
                      << pa.lexicon().size() << " values, " << st.entries << " containers: "
                      << st.bitmaps << " bitmap, " << st.arrays << " array, " << st.fulls
                      << " full; " << (st.payload_bytes >> 20) << " MB; " << secs() << " s)\n";
    }
    return 0;
}

// P4.2: `--packed-rev auto|none|A[,B...]` writes <attr>.rev.pfb (block-compressed
// postings; auto = every single-valued attribute); `--drop-rev` then removes the
// plain .rev of those attributes once every list decodes to the same positions.
static int upgrade_packed(const pando::Corpus& corpus, const std::string& spec, bool drop, bool quiet,
                          const std::function<double()>& secs) {
    if (spec == "none") return 0;
    std::vector<std::string> names;
    if (spec == "auto") {
        for (const auto& n : corpus.attr_names())
            if (corpus.has_attr(n) && !corpus.is_multivalue(n)) names.push_back(n);
        for (const auto& h : corpus.head_attr_names()) names.push_back(h);
    } else {
        names = split_list(spec);
    }
    for (const auto& name : names) {
        if (!corpus.has_attr(name) || corpus.is_multivalue(name)) {
            std::cerr << "Error: --packed-rev " << name << ": not a single-valued positional attribute\n";
            return 1;
        }
        const auto& pa = corpus.attr(name);
        const std::string base = pa.base_path();
        pando::PackedPostings probe;
        const bool have = probe.open(base, pa.lexicon().size(), static_cast<int64_t>(pa.postings_total()));
        const bool plain = fs::exists(base + ".rev");
        if (have) {
            if (!quiet) std::cerr << "Packed postings " << pando::PackedPostings::path(base) << " up to date\n";
        } else {
            if (!plain) {
                std::cerr << "Error: " << base << ": no .rev to pack and no valid .rev.pfb\n";
                return 1;
            }
            std::string err;
            pando::PackedPostings::BuildStats st;
            if (!pando::PackedPostings::build(pa, &err, &st)) {
                std::cerr << "Error: " << err << "\n";
                return 1;
            }
            if (!quiet) {
                const double raw = static_cast<double>(st.postings) * pa.rev_width();
                const double packed = static_cast<double>(st.payload_bytes + st.idx_bytes);
                std::cerr << "Wrote " << pando::PackedPostings::path(base) << " (" << st.postings << " postings, "
                          << (static_cast<uint64_t>(packed) >> 20) << " MB = "
                          << (raw > 0 ? static_cast<int>(100.0 * packed / raw + 0.5) : 0) << "% of .rev, "
                          << secs() << " s)\n";
            }
        }
        if (drop && plain) {
            // only when the packed lists give back exactly the plain ones
            pando::PackedPostings pk;
            if (!pk.open(base, pa.lexicon().size(), static_cast<int64_t>(pa.postings_total()))) {
                std::cerr << "Error: " << base << ".rev.pfb does not open; .rev kept\n";
                return 1;
            }
            std::vector<pando::CorpusPos> buf;
            for (pando::LexiconId id = 0; id < pa.lexicon().size(); ++id) {
                const pando::RevSpan sp = pa.rev_span_of_id(id);
                buf.resize(sp.count);
                pk.decode_all(id, sp.count, buf.data());
                for (size_t i = 0; i < sp.count; ++i)
                    if (buf[i] != sp.at(i)) {
                        std::cerr << "Error: " << base << ".rev.pfb differs from .rev (id " << id << "); .rev kept\n";
                        return 1;
                    }
            }
            std::error_code ec;
            fs::remove(base + ".rev", ec);
            if (ec) {
                std::cerr << "Error: cannot remove " << base << ".rev: " << ec.message() << "\n";
                return 1;
            }
            if (!quiet) std::cerr << "Removed " << base << ".rev (packed postings verified)\n";
        }
    }
    return 0;
}

// P4.3: `--packed-dat [auto|A,B]` writes <attr>.dat.pk (compact ids, O(1) access);
// `--drop-dat` then removes the plain .dat once every id reads back the same.
static int upgrade_packed_dat(const pando::Corpus& corpus, const std::string& spec, bool drop, bool quiet,
                              const std::function<double()>& secs) {
    if (spec == "none") return 0;
    std::vector<std::string> names;
    if (spec == "auto") {
        for (const auto& n : corpus.attr_names())
            if (corpus.has_attr(n) && !corpus.attr(n).derived()) names.push_back(n);
    } else {
        names = split_list(spec);
    }
    for (const auto& name : names) {
        if (!corpus.has_attr(name) || corpus.attr(name).derived()) {
            std::cerr << "Error: --packed-dat " << name << ": not a positional attribute with ids\n";
            return 1;
        }
        const auto& pa = corpus.attr(name);
        const std::string base = pa.base_path();
        const int64_t n = corpus.size(), nlex = pa.lexicon().size();
        pando::PackedDat probe;
        const bool plain = fs::exists(base + ".dat");
        if (probe.open(base, n, nlex)) {
            if (!quiet) std::cerr << "Packed ids " << pando::PackedDat::path(base) << " up to date\n";
        } else {
            if (!plain) {
                std::cerr << "Error: " << base << ": no .dat to pack and no valid .dat.pk\n";
                return 1;
            }
            std::string err;
            pando::PackedDat::BuildStats st;
            auto fill = [&](int64_t a, int64_t b, pando::LexiconId* out) { pa.ids_at(a, b, out); };
            if (!pando::PackedDat::build(base, n, nlex, fill, &err, &st)) {
                std::cerr << "Error: " << err << "\n";
                return 1;
            }
            if (!quiet) {
                const double raw = static_cast<double>(fs::file_size(base + ".dat"));
                std::cerr << "Wrote " << pando::PackedDat::path(base) << " ("
                          << (st.mode == pando::PackedDat::Mode::Fixed ? std::to_string(st.bits) + " bits per id"
                                                                       : std::string("ranks in blocks of 16"))
                          << ", " << (st.bytes >> 20) << " MB = "
                          << (raw > 0 ? static_cast<int>(100.0 * static_cast<double>(st.bytes) / raw + 0.5) : 0)
                          << "% of .dat, " << secs() << " s)\n";
            }
        }
        if (drop && plain) {
            pando::PackedDat pk;
            if (!pk.open(base, n, nlex)) {
                std::cerr << "Error: " << base << ".dat.pk does not open; .dat kept\n";
                return 1;
            }
            std::vector<pando::LexiconId> a(size_t{1} << 20), b(size_t{1} << 20);
            for (int64_t p = 0; p < n; p += static_cast<int64_t>(a.size())) {
                const int64_t e = std::min<int64_t>(n, p + static_cast<int64_t>(a.size()));
                pa.ids_at(p, e, a.data());
                pk.decode(p, e, b.data());
                if (!std::equal(a.begin(), a.begin() + (e - p), b.begin())) {
                    std::cerr << "Error: " << base << ".dat.pk differs from .dat near token " << p << "; .dat kept\n";
                    return 1;
                }
            }
            std::error_code ec;
            fs::remove(base + ".dat", ec);
            if (ec) {
                std::cerr << "Error: cannot remove " << base << ".dat: " << ec.message() << "\n";
                return 1;
            }
            if (!quiet) std::cerr << "Removed " << base << ".dat (packed ids verified)\n";
        }
    }
    return 0;
}

// Record `upgraded_with=<build> <UTC time>` in corpus.info (replacing an earlier one).
static void record_upgrade(const std::string& dir) {
    const std::string path = dir + "/corpus.info", tmp = path + ".tmp";
    std::ifstream in(path);
    if (!in) return;
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);)
        if (line.rfind("upgraded_with=", 0) != 0 && line.rfind("index_id=", 0) != 0) lines.push_back(line);
    in.close();
    char ts[32];
    const std::time_t now = std::time(nullptr);
    std::strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&now));
    lines.push_back("upgraded_with=" + pando::build_string() + " " + ts);
    // a new identity: the files may have changed (hosts reopen on a new index_id)
    lines.push_back("index_id=" + pando::IndexPublish::new_index_id());
    {
        std::ofstream out(tmp);
        if (!out) return;
        for (const auto& l : lines) out << l << "\n";
        if (!out) return;
    }
    std::error_code ec;
    fs::rename(tmp, path, ec);
}

static int upgrade_index(const std::string& dir, bool quiet = false,
                         const std::string& dep_pairs = kDefaultDepPairs,
                         const std::string& bitmaps = kDefaultBitmaps,
                         const std::string& packed = "none", bool drop_rev = false,
                         const std::string& head_attrs = kDefaultHeadAttrs, bool compact_deps = false,
                         const std::string& packed_dat = "none", bool drop_dat = false) {
    auto t0 = std::chrono::steady_clock::now();
    auto secs = [&] {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    };
    if (fs::exists(dir + "/dep.head") && fs::exists(dir + "/s.rgn")) {
        const bool have = fs::exists(dir + "/dep.head_rel")
            && fs::file_size(dir + "/dep.head_rel") == fs::file_size(dir + "/dep.head");
        if (!have) {
            pando::StructuralAttr sentences;
            sentences.open(dir + "/s.rgn", false);
            std::string err;
            if (!pando::DependencyIndex::write_head_rel_file(dir, sentences, &err)) {
                std::cerr << "Error: " << err << "\n";
                return 1;
            }
            if (!quiet) std::cerr << "Wrote " << dir << "/dep.head_rel (" << secs() << " s)\n";
        } else if (!quiet) {
            std::cerr << dir << "/dep.head_rel up to date\n";
        }
    }
    // P4.3b: dep.head_rel8 (1 byte per token + exceptions), which readers prefer;
    // --compact-deps then drops dep.head and dep.head_rel (2 bytes per token each)
    if ((fs::exists(dir + "/dep.head") || fs::exists(dir + "/dep.head_rel8")) && fs::exists(dir + "/s.rgn")) {
        try {
            pando::StructuralAttr sentences;
            sentences.open(dir + "/s.rgn", false);
            pando::DependencyIndex before;
            before.open(dir, sentences);
            const size_t n = before.token_count();
            std::error_code ec;
            const bool stale = fs::exists(dir + "/dep.head")
                && fs::last_write_time(dir + "/dep.head_rel8", ec) < fs::last_write_time(dir + "/dep.head", ec);
            if (!pando::DependencyIndex::have_head_rel8(dir, n) || stale) {
                std::string err;
                if (!before.write_head_rel8(dir, &err)) {
                    std::cerr << "Error: " << err << "\n";
                    return 1;
                }
                if (!quiet) std::cerr << "Wrote " << dir << "/dep.head_rel8 (" << secs() << " s)\n";
            } else if (!quiet) {
                std::cerr << dir << "/dep.head_rel8 up to date\n";
            }
            if (compact_deps && (fs::exists(dir + "/dep.head") || fs::exists(dir + "/dep.head_rel"))) {
                pando::DependencyIndex after;   // reads dep.head_rel8
                after.open(dir, sentences);
                for (size_t p = 0; p < n; ++p)
                    if (before.head(static_cast<pando::CorpusPos>(p)) != after.head(static_cast<pando::CorpusPos>(p))) {
                        std::cerr << "Error: dep.head_rel8 differs at token " << p << "; dep.head kept\n";
                        return 1;
                    }
                for (const char* f : {"/dep.head", "/dep.head_rel"}) {
                    if (!fs::exists(dir + f)) continue;
                    fs::remove(dir + f, ec);
                    if (ec) {
                        std::cerr << "Error: cannot remove " << dir << f << ": " << ec.message() << "\n";
                        return 1;
                    }
                    if (!quiet) std::cerr << "Removed " << dir << f << " (dep.head_rel8 verified)\n";
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << "\n";
            return 1;
        }
    }
    try {
        // `--head-attrs none` is remembered (an empty head_attrs= line in corpus.info):
        // later upgrades with the default (auto) leave the corpus without them
        bool opted_out = false;
        {
            std::ifstream in(dir + "/corpus.info");
            for (std::string line; std::getline(in, line);)
                if (line == "head_attrs=") opted_out = true;
        }
        if (head_attrs == "none" && !opted_out) {
            pando::Corpus probe;
            probe.open(dir);
            if (probe.head_attr_names().empty()) {
                std::ofstream out(dir + "/corpus.info", std::ios::app);
                out << "head_attrs=\n";
            }
        }
        if (head_attrs != "none" && !head_attrs.empty() && !(head_attrs == "auto" && opted_out)) {
            // before the corpus is opened for folds / bitmaps / packed postings,
            // which then cover the head attributes too
            pando::Corpus src;
            src.open(dir);
            const bool is_auto = head_attrs == "auto";
            const bool deps = src.has_deps() && src.deps().head_rel_data();
            if (!deps && !is_auto) {
                std::cerr << "Error: --head-attrs: the corpus has no dependency index\n";
                return 1;
            }
            std::vector<std::string> names;
            if (!deps) {
                // auto without dependencies: none
            } else if (is_auto) {
                for (const char* a : kAutoHeadAttrs)
                    if (src.has_attr(a) && !src.is_multivalue(a)) names.push_back(a);
            } else {
                names = split_list(head_attrs);
            }
            for (const auto& a : names) {
                if (pando::HeadAttr::up_to_date(src, a)) {
                    if (!quiet) std::cerr << "Head attribute " << pando::HeadAttr::name_for(a) << " up to date\n";
                    continue;
                }
                std::string err;
                if (!pando::HeadAttr::build(src, a, &err)) {
                    std::cerr << "Error: " << err << "\n";
                    return 1;
                }
                if (!quiet)
                    std::cerr << "Wrote " << dir << "/" << pando::HeadAttr::name_for(a) << " (" << secs() << " s)\n";
            }
        }
        pando::Corpus corpus;
        corpus.open(dir);
        const pando::FoldMode modes[] = {pando::FoldMode::Lower, pando::FoldMode::NoAccents,
                                         pando::FoldMode::LowerNoAccents};
        std::vector<std::string> fold_names = corpus.attr_names();
        for (const auto& h : corpus.head_attr_names()) fold_names.push_back(h);
        for (const auto& name : fold_names) {
            if (corpus.is_multivalue(name)) continue;
            const auto& pa = corpus.attr(name);
            for (auto m : modes) {
                pando::FoldIndex probe;
                if (probe.open(pa.base_path(), m, pa.lexicon().size())) continue;
                std::string err;
                if (!pando::FoldIndex::build(pa.base_path(), pa.lexicon(), m, &err)) {
                    std::cerr << "Error: " << err << "\n";
                    return 1;
                }
            }
        }
        if (!quiet) std::cerr << "Fold indexes up to date (" << secs() << " s)\n";
        if (upgrade_bitmaps(corpus, bitmaps, quiet, secs) != 0) return 1;
        if (upgrade_packed(corpus, packed, drop_rev, quiet, secs) != 0) return 1;
        if (upgrade_packed_dat(corpus, packed_dat, drop_dat, quiet, secs) != 0) return 1;
        // P5.6: head attributes keep only packed postings (verified; their plain
        // .rev is 4 bytes per token, the packed lists 20-50% of that)
        if (!corpus.head_attr_names().empty()) {
            std::string hs;
            for (const auto& h : corpus.head_attr_names()) hs += (hs.empty() ? "" : ",") + h;
            if (upgrade_packed(corpus, hs, true, quiet, secs) != 0) return 1;
        }
        if (!quiet && !corpus.has_deps())
            std::cerr << "No dependency index: no dep.head_rel / edge postings\n";
        if (corpus.has_deps() && corpus.deps().head_rel_data() && dep_pairs != "none") {
            size_t from = 0;
            while (from <= dep_pairs.size()) {
                size_t to = dep_pairs.find(',', from);
                if (to == std::string::npos) to = dep_pairs.size();
                const std::string item = dep_pairs.substr(from, to - from);
                from = to + 1;
                if (item.empty()) continue;
                const size_t colon = item.find(':');
                const std::string h = item.substr(0, colon);
                const std::string c = colon == std::string::npos ? h : item.substr(colon + 1);
                const bool explicit_list = dep_pairs != kDefaultDepPairs;
                if (!corpus.has_attr(h) || !corpus.has_attr(c)) {
                    if (explicit_list) {
                        std::cerr << "Error: --dep-pairs " << item << ": unknown attribute\n";
                        return 1;
                    }
                    continue;
                }
                pando::DepPairIndex probe;
                if (probe.open(corpus, h, c)) {
                    if (!quiet)
                        std::cerr << "Edge postings " << pando::DepPairIndex::base_path(dir, h, c)
                                  << ".rev up to date\n";
                    continue;
                }
                std::string err;
                if (!pando::DepPairIndex::build(corpus, h, c, &err)) {
                    std::cerr << (explicit_list ? "Error: " : "Note: ") << err << "\n";
                    if (explicit_list) return 1;
                    continue;
                }
                if (!quiet)
                    std::cerr << "Wrote " << pando::DepPairIndex::base_path(dir, h, c)
                              << ".rev (" << secs() << " s)\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    record_upgrade(dir);
    return 0;
}

// In-place changes to a published version (or its root) would rewrite files a
// server may have open: refuse, and say what to do instead.
static bool refuse_published_in_place(const std::string& dir, bool upgrade) {
    std::string root;
    if (pando::IndexPublish::is_root(dir)) root = dir;
    else root = pando::IndexPublish::root_of(dir);
    if (root.empty()) return false;
    std::cerr << "Error: " << dir << " is " << (root == dir ? "a published corpus root" : "a published version of " + root)
              << "; a server may have it open, so it is not changed in place.\n"
              << (upgrade ? "  Use: pando-index --upgrade " + root + " --publish [options]  (upgrades a copy, then switches)\n"
                          : "  Use: pando-index [options] <input> --publish " + root + "  (builds a new version, then switches)\n");
    return true;
}

static int report_publish(pando::IndexPublish::Session& pub, int keep) {
    std::vector<std::string> removed;
    if (!pub.commit(keep, &removed)) {
        std::cerr << "Error: publish failed: " << pub.error() << "\n";
        return 1;
    }
    std::cerr << "Published version " << pub.id() << " (current -> versions/" << pub.id() << ")\n";
    for (const auto& r : removed) std::cerr << "Removed old version " << r << "\n";
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc == 2 && (std::string(argv[1]) == "--version" || std::string(argv[1]) == "-V")) {
        std::cout << "pando-index " << pando::build_string() << "\n";
        return 0;
    }
    if (argc >= 3 && std::string(argv[1]) == "--upgrade") {
        std::string pairs = kDefaultDepPairs;
        std::string bitmaps = kDefaultBitmaps;
        std::string packed = "none";
        bool drop_rev = false;
        std::string head_attrs = kDefaultHeadAttrs;
        bool compact_deps = false;
        std::string packed_dat = "none";
        bool drop_dat = false;
        bool publish = false;
        int keep = 3;
        for (int i = 3; i < argc; ++i) {
            const std::string a = argv[i];
            if (a == "--publish") { publish = true; continue; }
            if (a == "--keep" && i + 1 < argc) { keep = std::atoi(argv[++i]); continue; }
            if (a == "--packed-rev" && i + 1 < argc && argv[i + 1][0] != '-') { packed = argv[++i]; continue; }
            if (a == "--packed-rev") { packed = "auto"; continue; }
            if (a.rfind("--packed-rev=", 0) == 0) { packed = a.substr(13); continue; }
            if (a == "--drop-rev") { drop_rev = true; continue; }
            if (a == "--head-attrs" && i + 1 < argc) { head_attrs = argv[++i]; continue; }
            if (a == "--compact-deps") { compact_deps = true; continue; }
            if (a == "--packed-dat" && i + 1 < argc && argv[i + 1][0] != '-') { packed_dat = argv[++i]; continue; }
            if (a == "--packed-dat") { packed_dat = "auto"; continue; }
            if (a.rfind("--packed-dat=", 0) == 0) { packed_dat = a.substr(13); continue; }
            if (a == "--drop-dat") { drop_dat = true; continue; }
            if (a.rfind("--head-attrs=", 0) == 0) { head_attrs = a.substr(13); continue; }
            if (a == "--dep-pairs" && i + 1 < argc) pairs = argv[++i];
            else if (a.rfind("--dep-pairs=", 0) == 0) pairs = a.substr(12);
            else if (a == "--bitmaps" && i + 1 < argc) bitmaps = argv[++i];
            else if (a.rfind("--bitmaps=", 0) == 0) bitmaps = a.substr(10);
            else {
                std::cerr << "Error: unknown --upgrade option '" << a << "'\n";
                return 1;
            }
        }
        if (drop_rev && packed == "none") packed = "auto";
        if (drop_dat && packed_dat == "none") packed_dat = "auto";
        if (publish) {
            // a copy of ROOT/current is upgraded, then becomes the current version
            std::string root = argv[2];
            if (!pando::IndexPublish::is_root(root)) {
                const std::string r = pando::IndexPublish::root_of(root);
                if (r.empty()) {
                    std::cerr << "Error: " << root << " is not a published corpus root (ROOT/current, ROOT/versions/)\n";
                    return 1;
                }
                root = r;
            }
            pando::IndexPublish::Session pub(root);
            if (!pub.ok()) {
                std::cerr << "Error: " << pub.error() << "\n";
                return 1;
            }
            std::cerr << "Copying " << pando::IndexPublish::current_version(root) << " to " << pub.building_dir() << "\n";
            if (!pub.copy_current()) {
                std::cerr << "Error: " << pub.error() << "\n";
                return 1;
            }
            if (upgrade_index(pub.building_dir(), false, pairs, bitmaps, packed, drop_rev, head_attrs, compact_deps,
                              packed_dat, drop_dat) != 0)
                return 1;
            return report_publish(pub, keep);
        }
        if (refuse_published_in_place(argv[2], true)) return 1;
        return upgrade_index(argv[2], false, pairs, bitmaps, packed, drop_rev, head_attrs, compact_deps,
                             packed_dat, drop_dat);
    }
    bool split_feats = false;
    bool format_vertical = false;
    bool format_jsonl = false;
    bool overlay_index = false;
    std::string index_dir;
    std::string build_head_attrs = kDefaultHeadAttrs;
    std::string publish_root;
    int keep = 3;

    // Collect flags
    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--split-feats") split_feats = true;
        else if (a == "--overlay-index") overlay_index = true;
        else if (a == "--head-attrs" && i + 1 < argc) build_head_attrs = argv[++i];
        else if (a.rfind("--head-attrs=", 0) == 0) build_head_attrs = a.substr(13);
        else if (a == "--index-dir" && i + 1 < argc) index_dir = argv[++i];
        else if (a == "--publish" && i + 1 < argc) publish_root = argv[++i];
        else if (a == "--keep" && i + 1 < argc) keep = std::atoi(argv[++i]);
        else if (a == "--format" && i + 1 < argc) {
            std::string fmt = argv[++i];
            if (fmt == "vertical") format_vertical = true;
            else if (fmt == "jsonl") format_jsonl = true;
            else {
                std::cerr << "Error: unknown --format value '" << fmt
                          << "'. Use 'jsonl' (JSONL events) or 'vertical' (VRT/vert).\n";
                return 1;
            }
        } else if (a == "--format") {
            /* missing arg */
        } else {
            args.push_back(a);
        }
    }

    if (args.size() < (publish_root.empty() ? 2u : 1u)) {
        std::cerr << "Usage: pando-index [options] <input> <output_dir>\n\n"
                  << "  input: .conllu file(s), directory (recursive), or '-' for JSONL stdin;\n"
                  << "         with --format vertical: .vrt/.vert/.txt files;\n"
                  << "         with --format jsonl: JSONL events as in dev/PANDO-INDEX-INTEGRATION.md\n\n"
                  << "  For each region attribute (e.g. text_langcode.val), the indexer also writes\n"
                  << "  .lex / .rev / .rev.idx (value → region ids) for fast :: metadata filters.\n\n"
                  << "  --split-feats     Split FEATS into feats#Feature columns (default: combined)\n"
                  << "  --head-attrs auto|none|A[,B...]  head attributes (default auto: upos, deprel,\n"
                  << "                    lemma when there are dependencies; see --upgrade)\n"
                  << "  --format vertical Read CWB-style vertical (one token/line, <s> </s>);\n"
                  << "                    optional <!-- positional-attributes: ... --> (Korp/Kielipankki)\n"
                  << "  --format jsonl    Read streaming JSONL events (tokens/regions)\n"
                  << "  --overlay-index   Standoff-only JSONL: emit token-group columns + groups/ into\n"
                  << "                    output_dir (no full corpus). Requires --format jsonl and\n"
                  << "                    --index-dir <main_corpus_dir> (must contain corpus.info).\n"
                  << "  --index-dir       Main indexed corpus directory (for overlay size / stamp)\n"
                  << "  --publish ROOT    instead of <output_dir>: build into ROOT/versions/<id>, then point\n"
                  << "                    ROOT/current at it (servers hot-swap; nothing is rewritten in place)\n"
                  << "  --keep N          with --publish: versions kept (default 3; the current and the\n"
                  << "                    previous one always)\n"
                  << "\n  pando-index --upgrade <corpus_dir>\n"
                  << "                    Add derived files to an existing index\n"
                  << "                    (dep.head_rel, <attr>.fold_*.perm for %c/%d,\n"
                  << "                    dep.pair.<H>.<C>.rev edge postings for [H=..] > [C=..],\n"
                  << "                    <attr>.bm bitmaps)\n"
                  << "    --dep-pairs H:C[,H:C...]  edge postings to build (default upos:upos;\n"
                  << "                    'none' to skip). Low-cardinality attributes only.\n"
                  << "    --bitmaps auto|none|A[,B...]  chunked bitmaps (<attr>.bm) for fast dense\n"
                  << "                    token patterns, <struct>.bnd.bm for `within` (default auto:\n"
                  << "                    attributes with <= " << pando::BitmapIndex::kAutoMaxValues
                  << " values, flat structures)\n"
                  << "    --packed-rev [auto|none|A[,B...]]  block-compressed postings (<attr>.rev.pfb,\n"
                  << "                    P4.2; auto = every single-valued attribute; default none)\n"
                  << "    --drop-rev      remove the plain <attr>.rev once its packed postings are\n"
                  << "                    verified (implies --packed-rev auto; queries then decode)\n"
                  << "    --head-attrs auto|none|A[,B...]  head attributes head#A (A of each\n"
                  << "                    token's dependency head): [X] > [Y], [Y] < [X] and parent [X]\n"
                  << "                    become one-token queries on the dependent (default auto =\n"
                  << "                    upos, deprel, lemma when the corpus has dependencies)\n"
                  << "    --packed-dat [auto|none|A[,B...]]  compact ids (<attr>.dat.pk: small lexicons\n"
                  << "                    bit-packed, large ones as frequency ranks in 1-4 bytes; default none)\n"
                  << "    --drop-dat      remove the plain <attr>.dat once its packed ids are verified\n"
                  << "                    (implies --packed-dat auto)\n"
                  << "    --compact-deps  remove dep.head and dep.head_rel once dep.head_rel8 (1 byte\n"
                  << "                    per token, always written) is verified to give the same heads\n"
                  << "    --publish       <corpus_dir> is a published root: upgrade a copy of its current\n"
                  << "                    version, then switch current to it (--keep N as for builds)\n";
        return 1;
    }

    std::string input_path = args[0];
    std::string output_dir = publish_root.empty() ? args[1] : std::string();
    if (!publish_root.empty() && args.size() > 1) {
        std::cerr << "Error: --publish ROOT replaces <output_dir>\n";
        return 1;
    }
    std::unique_ptr<pando::IndexPublish::Session> pub;
    if (!publish_root.empty()) {
        pub = std::make_unique<pando::IndexPublish::Session>(publish_root);
        if (!pub->ok()) {
            std::cerr << "Error: " << pub->error() << "\n";
            return 1;
        }
        output_dir = pub->building_dir();
        std::cerr << "Building version " << pub->id() << " in " << output_dir << "\n";
    } else if (!overlay_index && refuse_published_in_place(output_dir, false)) {
        return 1;
    }

    if (overlay_index && !format_jsonl) {
        std::cerr << "Error: --overlay-index requires --format jsonl\n";
        return 1;
    }
    if (overlay_index && index_dir.empty()) {
        std::cerr << "Error: --overlay-index requires --index-dir <main_corpus_dir>\n";
        return 1;
    }

    try {
        if (overlay_index) {
            pando::CorpusPos main_size = read_corpus_size_from_info(index_dir);
            std::cerr << "Overlay index: main corpus size=" << main_size << " (from "
                      << index_dir << "/corpus.info)\n";
            std::cerr << "Reading overlay JSONL from " << input_path << "\n";
            pando::CorpusBuilder builder(output_dir, true);
            builder.read_jsonl_overlay(input_path, main_size);
            builder.finalize();
            write_overlay_info(output_dir, index_dir, input_path);
            std::cerr << "Wrote overlay manifest " << output_dir << "/overlay.info\n";
            return 0;
        }

        pando::CorpusBuilder builder(output_dir);
        builder.set_split_feats(split_feats);

        auto t0 = std::chrono::steady_clock::now();
        int64_t prev_tokens = 0;
        auto prev_time = t0;
        if (format_jsonl) {
            // JSONL: single stream from file or stdin ("-").
            std::cerr << "Reading JSONL from " << input_path << "\n";
            builder.read_jsonl(input_path);
        } else {
            std::vector<std::string> files;

            if (format_vertical) {
                files = find_vertical_files(input_path);
                std::cerr << "Found " << files.size() << " vertical file"
                          << (files.size() != 1 ? "s" : "") << "\n";
            } else {
                files = find_conllu_files(input_path);
                std::cerr << "Found " << files.size() << " .conllu file"
                          << (files.size() != 1 ? "s" : "") << "\n";
            }

            for (size_t i = 0; i < files.size(); ++i) {
                std::cerr << "[" << (i + 1) << "/" << files.size() << "] "
                          << files[i];

                if (format_vertical)
                    builder.read_vertical(files[i]);
                else
                    builder.read_conllu(files[i]);

                int64_t cur_tokens = builder.builder().corpus_size();
                auto now = std::chrono::steady_clock::now();
                double secs = std::chrono::duration<double>(now - prev_time).count();
                if (secs > 0.001) {
                    double ktps = static_cast<double>(cur_tokens - prev_tokens) / secs / 1000.0;
                    std::cerr << "  (" << (cur_tokens - prev_tokens) << " tok, "
                              << static_cast<int>(ktps) << " ktok/s)";
                }
                std::cerr << "\n";
                prev_tokens = cur_tokens;
                prev_time = now;
            }
        }

        auto t1 = std::chrono::steady_clock::now();
        double total_secs = std::chrono::duration<double>(t1 - t0).count();
        int64_t total_tokens = builder.builder().corpus_size();
        std::cerr << "Corpus: " << total_tokens << " tokens in "
                  << static_cast<int>(total_secs) << "s ("
                  << static_cast<int>(total_tokens / total_secs / 1000) << " ktok/s avg)\n";
        std::cerr << "Finalizing...\n";
        builder.finalize();
        if (upgrade_index(output_dir, false, kDefaultDepPairs, kDefaultBitmaps, "none", false, build_head_attrs) != 0)
            return 1;
        if (pub) return report_publish(*pub, keep);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
