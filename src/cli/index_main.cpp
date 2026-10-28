#include "corpus/builder.h"
#include "core/types.h"
#include "index/dependency_index.h"
#include "index/structural_attr.h"
#include "index/fold_index.h"
#include "index/head_attr.h"
#include "index/dep_pair_index.h"
#include "index/relation_decl.h"
#include "index/bitmap_index.h"
#include "corpus/corpus.h"
#include "core/build_info.h"
#include "index/index_publish.h"
#include "index/index_upgrade.h"
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include "corpus/derive_deps.h"
#include "corpus/equivalence_layer.h"
#include <iostream>
#include <optional>
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
    if (argc == 4 && std::string(argv[1]) == "--relation") {
        // `pando-index --relation NAME=equivalence,attr=ATTR <corpus_dir>`: declare an equivalence layer whose
        // classes are an attribute's values (equivalence_layer.h); scope and span are measured
        const std::string spec = argv[2], dir = argv[3];
        const size_t eq = spec.find('=');
        std::string err;
        auto decl = eq == std::string::npos ? std::nullopt
                                            : pando::parse_relation(spec.substr(0, eq), spec.substr(eq + 1), &err);
        if (!decl) {
            std::cerr << "Error: " << (err.empty() ? "--relation NAME=equivalence,attr=ATTR" : err) << "\n";
            return 1;
        }
        if (decl->kind != pando::RelationDecl::Kind::Equivalence) {
            std::cerr << "Error: --relation declares equivalence layers; pointer layers come from the data (DEPS) "
                         "or --derive\n";
            return 1;
        }
        try {
            pando::Corpus corpus;
            corpus.open(dir);
            if (const pando::RelationDecl* old = corpus.relation(decl->name);
                old && old->kind != pando::RelationDecl::Kind::Equivalence) {
                std::cerr << "Error: " << dir << " has a pointer relation '" << decl->name << "'\n";
                return 1;
            }
            pando::EquivalenceStats st;
            const pando::RelationDecl d = pando::measure_equivalence(corpus, *decl, &st);
            if (!pando::record_relation(dir, d)) {
                std::cerr << "Error: cannot record relation." << d.name << " in " << dir << "/corpus.info\n";
                return 1;
            }
            std::cerr << "Relation " << d.name << ": " << d.format() << "\n"
                      << "  " << st.classes << " classes (" << st.singletons << " of one token, the largest "
                      << st.largest << "), " << st.members << " memberships\n";
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << "\n";
            return 1;
        }
    }
    if ((argc == 4 || argc == 6) && std::string(argv[1]) == "--derive") {
        // `pando-index --derive NAME <corpus_dir> [--rule R]`: a derived pointer layer from the basic tree
        // (derive_deps.h): `eff` (effective heads, the default rule), or `deps` for a corpus without DEPS
        // (conj-propagation, the default for that name)
        const std::string name = argv[2], dir = argv[3];
        pando::DeriveRule rule = name == "deps" ? pando::DeriveRule::ConjPropagation : pando::DeriveRule::EffectiveHead;
        if (argc == 6 && (std::string(argv[4]) != "--rule" || !pando::parse_derive_rule(argv[5], rule))) {
            std::cerr << "Error: --rule effective-head or --rule conj-propagation\n";
            return 1;
        }
        try {
            pando::Corpus corpus;
            corpus.open(dir);
            if (const pando::RelationDecl* old = corpus.relation(name);
                old && old->source.rfind("derived:", 0) != 0) {
                std::cerr << "Error: " << dir << " has an annotated relation '" << name
                          << "'; derive under another name (pando-index --derive eff " << dir << ")\n";
                return 1;
            }
            if (name == "head") {
                std::cerr << "Error: 'head' is the basic tree\n";
                return 1;
            }
            // a layer derived before may be mapped by `corpus`: unlink, then write new files
            std::error_code ec;
            for (const char* ext : {".src", ".off", ".tgt", ".lab", ".labels", ".rtgt", ".rsrc", ".rlab"})
                fs::remove(dir + "/rel." + name + ext, ec);
            pando::DerivedDepsStats st;
            pando::RelationDecl d = pando::derive_deps_layer(corpus, dir, name, rule, &st);
            if (const pando::RelationDecl* h = corpus.relation("head"); h && h->span > d.span) d.span = h->span;
            if (!pando::record_relation(dir, d)) {
                std::cerr << "Error: cannot record relation." << name << " in " << dir << "/corpus.info\n";
                return 1;
            }
            std::cerr << "Relation " << name << ": " << d.format() << "\n"
                      << "  " << st.deviating << " of " << st.tokens << " tokens differ from the basic tree: "
                      << st.conj_heads << " conjunct heads, " << st.shared_subjects << " shared subjects\n";
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << "\n";
            return 1;
        }
    }
    if (argc >= 3 && std::string(argv[1]) == "--upgrade") {
        std::string pairs = pando::kDefaultDepPairs;
        std::string bitmaps = pando::kDefaultBitmaps;
        std::string packed = "none";
        bool drop_rev = false;
        std::string head_attrs = pando::kDefaultHeadAttrs;
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
            if (pando::upgrade_index(pub.building_dir(), false, pairs, bitmaps, packed, drop_rev, head_attrs, compact_deps,
                              packed_dat, drop_dat) != 0)
                return 1;
            return report_publish(pub, keep);
        }
        if (refuse_published_in_place(argv[2], true)) return 1;
        return pando::upgrade_index(argv[2], false, pairs, bitmaps, packed, drop_rev, head_attrs, compact_deps,
                             packed_dat, drop_dat);
    }
    bool split_feats = false;
    std::optional<std::vector<std::string>> misc_keys;   // --misc-keys
    std::vector<std::string> multivalue;   // --multivalue a,b: declared multivalue (values joined by '|')
    bool format_vertical = false;
    bool format_jsonl = false;
    bool overlay_index = false;
    std::string index_dir;
    std::string build_head_attrs = pando::kDefaultHeadAttrs;
    std::string publish_root;
    int keep = 3;

    // Collect flags
    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--split-feats") split_feats = true;
        else if ((a == "--misc-keys" && i + 1 < argc) || a.rfind("--misc-keys=", 0) == 0) {
            std::string v = a == "--misc-keys" ? argv[++i] : a.substr(12);
            misc_keys.emplace();
            if (v != "none")
                for (size_t from = 0; from <= v.size();) {
                    size_t comma = v.find(',', from);
                    if (comma == std::string::npos) comma = v.size();
                    if (comma > from) misc_keys->push_back(v.substr(from, comma - from));
                    from = comma + 1;
                }
        }
        else if ((a == "--multivalue" && i + 1 < argc) || a.rfind("--multivalue=", 0) == 0) {
            const std::string v = a == "--multivalue" ? argv[++i] : a.substr(13);
            for (size_t from = 0; from <= v.size();) {
                size_t comma = v.find(',', from);
                if (comma == std::string::npos) comma = v.size();
                if (comma > from) multivalue.push_back(v.substr(from, comma - from));
                from = comma + 1;
            }
        }
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
                  << "  --multivalue A,B  Attributes whose values hold several values joined by '|' (a token\n"
                  << "                    attribute, or a region one as s_tuid): A=\"x|y\" matches \"x\" and \"y\";\n"
                  << "                    a.A = b.A when they share one (as TEITOK settings' multivalue)\n"
                  << "  --misc-keys none|K1,K2  MISC keys indexed as attributes of their own (default:\n"
                  << "                    tuid,Translit,Vform,LTranslt,Root,CorrectForm,Gloss; none: no MISC)\n"
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
                  << "\n  pando-index --relation NAME=equivalence,attr=ATTR <corpus_dir>\n"
                  << "                    an equivalence layer: tokens with the same ATTR value form a class\n"
                  << "                    (tuid, coref, …); scope and span measured; query with [A] =:NAME [B]\n"
                  << "\n  pando-index --derive NAME <corpus_dir> [--rule effective-head|conj-propagation]\n"
                  << "                    a pointer layer NAME derived from the basic tree, queried with >:NAME:\n"
                  << "                    effective-head (default; e.g. eff): conjuncts get their first conjunct's\n"
                  << "                    head instead of conj, shared subjects are added; conj-propagation (default\n"
                  << "                    for deps): enhanced UD's added edges, for a corpus without DEPS\n"
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
        if (misc_keys) builder.set_misc_keys(*misc_keys);
        for (const auto& m : multivalue) builder.declare_multivalue(m);

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
        if (pando::upgrade_index(output_dir, false, pando::kDefaultDepPairs, pando::kDefaultBitmaps, "none", false, build_head_attrs) != 0)
            return 1;
        if (pando::measure_declared_equivalences(output_dir, false) != 0) return 1;
        if (pub) return report_publish(*pub, keep);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
