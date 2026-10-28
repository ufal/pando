#include "index/index_upgrade.h"

#include "core/build_info.h"
#include "corpus/corpus.h"
#include "corpus/equivalence_layer.h"
#include "index/bitmap_index.h"
#include "index/dep_pair_index.h"
#include "index/dependency_index.h"
#include "index/fold_index.h"
#include "index/head_attr.h"
#include "index/index_publish.h"
#include "index/structural_attr.h"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>

namespace pando {

namespace fs = std::filesystem;

std::vector<std::string> split_list(const std::string& s) {
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

static int upgrade_bitmaps(const Corpus& corpus, const std::string& spec, bool quiet,
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
            const std::string base = BitmapIndex::structure_base(corpus.dir(), name);
            BitmapIndex probe;
            if (probe.open_structure(base, corpus.size())) {
                if (!quiet) std::cerr << "Bitmaps " << BitmapIndex::path(base) << " up to date\n";
                continue;
            }
            std::string err;
            BitmapIndex::BuildStats st;
            if (!BitmapIndex::build_structure(corpus.structure(name), base, corpus.size(), &err, &st)) {
                std::cerr << (is_auto ? "Note: " : "Error: ") << err << "\n";
                if (!is_auto) return 1;
                continue;
            }
            if (!quiet)
                std::cerr << "Wrote " << BitmapIndex::path(base) << " (" << st.entries
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
        if (is_auto && pa.lexicon().size() > BitmapIndex::kAutoMaxValues) continue;
        BitmapIndex probe;
        if (probe.open(pa, corpus.size())) {
            if (!quiet) std::cerr << "Bitmaps " << BitmapIndex::path(pa.base_path())
                                  << " up to date\n";
            continue;
        }
        std::string err;
        BitmapIndex::BuildStats st;
        if (!BitmapIndex::build(pa, corpus.size(), &err, &st)) {
            std::cerr << "Error: " << err << "\n";
            return 1;
        }
        if (!quiet)
            std::cerr << "Wrote " << BitmapIndex::path(pa.base_path()) << " ("
                      << pa.lexicon().size() << " values, " << st.entries << " containers: "
                      << st.bitmaps << " bitmap, " << st.arrays << " array, " << st.fulls
                      << " full; " << (st.payload_bytes >> 20) << " MB; " << secs() << " s)\n";
    }
    return 0;
}

// P4.2: `--packed-rev auto|none|A[,B...]` writes <attr>.rev.pfb (block-compressed
// postings; auto = every single-valued attribute); `--drop-rev` then removes the
// plain .rev of those attributes once every list decodes to the same positions.
static int upgrade_packed(const Corpus& corpus, const std::string& spec, bool drop, bool quiet,
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
        PackedPostings probe;
        const bool have = probe.open(base, pa.lexicon().size(), static_cast<int64_t>(pa.postings_total()));
        const bool plain = fs::exists(base + ".rev");
        if (have) {
            if (!quiet) std::cerr << "Packed postings " << PackedPostings::path(base) << " up to date\n";
        } else {
            if (!plain) {
                std::cerr << "Error: " << base << ": no .rev to pack and no valid .rev.pfb\n";
                return 1;
            }
            std::string err;
            PackedPostings::BuildStats st;
            if (!PackedPostings::build(pa, &err, &st)) {
                std::cerr << "Error: " << err << "\n";
                return 1;
            }
            if (!quiet) {
                const double raw = static_cast<double>(st.postings) * pa.rev_width();
                const double packed = static_cast<double>(st.payload_bytes + st.idx_bytes);
                std::cerr << "Wrote " << PackedPostings::path(base) << " (" << st.postings << " postings, "
                          << (static_cast<uint64_t>(packed) >> 20) << " MB = "
                          << (raw > 0 ? static_cast<int>(100.0 * packed / raw + 0.5) : 0) << "% of .rev, "
                          << secs() << " s)\n";
            }
        }
        if (drop && plain) {
            // only when the packed lists give back exactly the plain ones
            PackedPostings pk;
            if (!pk.open(base, pa.lexicon().size(), static_cast<int64_t>(pa.postings_total()))) {
                std::cerr << "Error: " << base << ".rev.pfb does not open; .rev kept\n";
                return 1;
            }
            std::vector<CorpusPos> buf;
            for (LexiconId id = 0; id < pa.lexicon().size(); ++id) {
                const RevSpan sp = pa.rev_span_of_id(id);
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
static int upgrade_packed_dat(const Corpus& corpus, const std::string& spec, bool drop, bool quiet,
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
        PackedDat probe;
        const bool plain = fs::exists(base + ".dat");
        if (probe.open(base, n, nlex)) {
            if (!quiet) std::cerr << "Packed ids " << PackedDat::path(base) << " up to date\n";
        } else {
            if (!plain) {
                std::cerr << "Error: " << base << ": no .dat to pack and no valid .dat.pk\n";
                return 1;
            }
            std::string err;
            PackedDat::BuildStats st;
            auto fill = [&](int64_t a, int64_t b, LexiconId* out) { pa.ids_at(a, b, out); };
            if (!PackedDat::build(base, n, nlex, fill, &err, &st)) {
                std::cerr << "Error: " << err << "\n";
                return 1;
            }
            if (!quiet) {
                const double raw = static_cast<double>(fs::file_size(base + ".dat"));
                std::cerr << "Wrote " << PackedDat::path(base) << " ("
                          << (st.mode == PackedDat::Mode::Fixed ? std::to_string(st.bits) + " bits per id"
                                                                       : std::string("ranks in blocks of 16"))
                          << ", " << (st.bytes >> 20) << " MB = "
                          << (raw > 0 ? static_cast<int>(100.0 * static_cast<double>(st.bytes) / raw + 0.5) : 0)
                          << "% of .dat, " << secs() << " s)\n";
            }
        }
        if (drop && plain) {
            PackedDat pk;
            if (!pk.open(base, n, nlex)) {
                std::cerr << "Error: " << base << ".dat.pk does not open; .dat kept\n";
                return 1;
            }
            std::vector<LexiconId> a(size_t{1} << 20), b(size_t{1} << 20);
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

// Record `relation.<name>=…` in corpus.info (replacing an earlier line for that relation).
bool record_relation(const std::string& dir, const RelationDecl& r) {
    const std::string path = dir + "/corpus.info", tmp = path + ".tmp", key = "relation." + r.name + "=";
    std::ifstream in(path);
    if (!in) return false;
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);)
        if (line.rfind(key, 0) != 0) lines.push_back(line);
    in.close();
    lines.push_back(key + r.format());
    {
        std::ofstream out(tmp);
        if (!out) return false;
        for (const auto& l : lines) out << l << "\n";
        if (!out) return false;
    }
    std::error_code ec;
    fs::rename(tmp, path, ec);
    return !ec;
}

// Equivalence layers declared without a measurement (span unknown, e.g. `coref` from CorefUD `Entity=`): measure
// scope and span from the data (equivalence_layer.h) and record them.
int measure_declared_equivalences(const std::string& dir, bool quiet) {
    std::vector<RelationDecl> todo;
    {
        Corpus corpus;
        corpus.open(dir);
        for (const auto& r : corpus.relations())
            if (r.kind == RelationDecl::Kind::Equivalence && r.span < 0) {
                EquivalenceStats st;
                todo.push_back(measure_equivalence(corpus, r, &st));
                if (!quiet)
                    std::cerr << "Relation " << r.name << ": " << todo.back().format() << "\n  " << st.classes
                              << " classes (" << st.singletons << " of one token, the largest " << st.largest
                              << "), " << st.members << " memberships\n";
            }
    }
    for (const auto& d : todo)
        if (!record_relation(dir, d)) {
            std::cerr << "Error: cannot record relation." << d.name << " in " << dir << "/corpus.info\n";
            return 1;
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
    lines.push_back("upgraded_with=" + build_string() + " " + ts);
    // a new identity: the files may have changed (hosts reopen on a new index_id)
    lines.push_back("index_id=" + IndexPublish::new_index_id());
    {
        std::ofstream out(tmp);
        if (!out) return;
        for (const auto& l : lines) out << l << "\n";
        if (!out) return;
    }
    std::error_code ec;
    fs::rename(tmp, path, ec);
}

int upgrade_index(const std::string& dir, bool quiet, const std::string& dep_pairs, const std::string& bitmaps,
                  const std::string& packed, bool drop_rev, const std::string& head_attrs, bool compact_deps,
                  const std::string& packed_dat, bool drop_dat) {
    auto t0 = std::chrono::steady_clock::now();
    auto secs = [&] {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    };
    if (fs::exists(dir + "/dep.head") && fs::exists(dir + "/s.rgn")) {
        const bool have = fs::exists(dir + "/dep.head_rel")
            && fs::file_size(dir + "/dep.head_rel") == fs::file_size(dir + "/dep.head");
        if (!have) {
            StructuralAttr sentences;
            sentences.open(dir + "/s.rgn", false);
            std::string err;
            if (!DependencyIndex::write_head_rel_file(dir, sentences, &err)) {
                std::cerr << "Error: " << err << "\n";
                return 1;
            }
            if (!quiet) std::cerr << "Wrote " << dir << "/dep.head_rel (" << secs() << " s)\n";
        } else if (!quiet) {
            std::cerr << dir << "/dep.head_rel up to date\n";
        }
    }
    bool heads_rewritten = false;   // an existing dep.head_rel8 replaced: rebuild what was derived from it
    // P4.3b: dep.head_rel8 (1 byte per token + exceptions), which readers prefer;
    // --compact-deps then drops dep.head and dep.head_rel (2 bytes per token each)
    if ((fs::exists(dir + "/dep.head") || fs::exists(dir + "/dep.head_rel8")) && fs::exists(dir + "/s.rgn")) {
        try {
            StructuralAttr sentences;
            sentences.open(dir + "/s.rgn", false);
            DependencyIndex before;
            before.open(dir, sentences);
            const size_t n = before.token_count();
            std::error_code ec;
            const bool stale = fs::exists(dir + "/dep.head")
                && fs::last_write_time(dir + "/dep.head_rel8", ec) < fs::last_write_time(dir + "/dep.head", ec);
            // the content too: file times do not survive every copy, and a rebuild into the same
            // folder left the old head_rel8 (iswoc, 2026-10-26: 3% of the heads from an earlier tree)
            const bool have8 = DependencyIndex::have_head_rel8(dir, n);
            const bool differs = have8 && !stale && !before.head_rel8_matches_head();
            if (differs && !quiet)
                std::cerr << dir << "/dep.head_rel8 differs from dep.head: rewritten, with the head attributes "
                             "and edge postings derived from it\n";
            if (!have8 || stale || differs) {
                std::string err;
                if (!before.write_head_rel8(dir, &err)) {
                    std::cerr << "Error: " << err << "\n";
                    return 1;
                }
                heads_rewritten = have8;
                if (!quiet) std::cerr << "Wrote " << dir << "/dep.head_rel8 (" << secs() << " s)\n";
            } else if (!quiet) {
                std::cerr << dir << "/dep.head_rel8 up to date\n";
            }
            if (compact_deps && (fs::exists(dir + "/dep.head") || fs::exists(dir + "/dep.head_rel"))) {
                DependencyIndex after;   // reads dep.head_rel8
                after.open(dir, sentences);
                for (size_t p = 0; p < n; ++p)
                    if (before.head(static_cast<CorpusPos>(p)) != after.head(static_cast<CorpusPos>(p))) {
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
            // relation.head: the basic tree as a relation layer (relation_decl.h), measured from the heads
            // as they are now (dep.head_rel8 may just have been rewritten)
            {
                DependencyIndex now;
                now.open(dir, sentences);
                std::vector<std::string> problems;
                const RelationDecl rel = measure_head_relation(now, sentences, &problems);
                if (!record_relation(dir, rel)) {
                    std::cerr << "Error: cannot record relation.head in " << dir << "/corpus.info\n";
                    return 1;
                }
                if (!quiet) {
                    std::cerr << "Relation head: " << rel.format() << "\n";
                    for (const auto& pr : problems) std::cerr << "  not a tree: " << pr << "\n";
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
            Corpus probe;
            probe.open(dir);
            if (probe.head_attr_names().empty()) {
                std::ofstream out(dir + "/corpus.info", std::ios::app);
                out << "head_attrs=\n";
            }
        }
        if (head_attrs != "none" && !head_attrs.empty() && !(head_attrs == "auto" && opted_out)) {
            // before the corpus is opened for folds / bitmaps / packed postings,
            // which then cover the head attributes too
            Corpus src;
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
                if (!heads_rewritten && HeadAttr::up_to_date(src, a)) {
                    if (!quiet) std::cerr << "Head attribute " << HeadAttr::name_for(a) << " up to date\n";
                    continue;
                }
                std::string err;
                if (!HeadAttr::build(src, a, &err)) {
                    std::cerr << "Error: " << err << "\n";
                    return 1;
                }
                if (!quiet)
                    std::cerr << "Wrote " << dir << "/" << HeadAttr::name_for(a) << " (" << secs() << " s)\n";
            }
        }
        Corpus corpus;
        corpus.open(dir);
        const FoldMode modes[] = {FoldMode::Lower, FoldMode::NoAccents,
                                         FoldMode::LowerNoAccents};
        std::vector<std::string> fold_names = corpus.attr_names();
        for (const auto& h : corpus.head_attr_names()) fold_names.push_back(h);
        for (const auto& name : fold_names) {
            if (corpus.is_multivalue(name)) continue;
            const auto& pa = corpus.attr(name);
            for (auto m : modes) {
                FoldIndex probe;
                if (probe.open(pa.base_path(), m, pa.lexicon().size())) continue;
                std::string err;
                if (!FoldIndex::build(pa.base_path(), pa.lexicon(), m, &err)) {
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
                DepPairIndex probe;
                if (!heads_rewritten && probe.open(corpus, h, c)) {
                    if (!quiet)
                        std::cerr << "Edge postings " << DepPairIndex::base_path(dir, h, c)
                                  << ".rev up to date\n";
                    continue;
                }
                std::string err;
                if (!DepPairIndex::build(corpus, h, c, &err)) {
                    std::cerr << (explicit_list ? "Error: " : "Note: ") << err << "\n";
                    if (explicit_list) return 1;
                    continue;
                }
                if (!quiet)
                    std::cerr << "Wrote " << DepPairIndex::base_path(dir, h, c)
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

}  // namespace pando
