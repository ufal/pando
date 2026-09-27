#include "corpus/builder.h"
#include "core/types.h"
#include "index/dependency_index.h"
#include "index/structural_attr.h"
#include "index/fold_index.h"
#include "index/dep_pair_index.h"
#include "index/bitmap_index.h"
#include "corpus/corpus.h"
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
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
    if (is_auto)
        for (const auto& st : corpus.structure_names()) names.push_back(st);
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

static int upgrade_index(const std::string& dir, bool quiet = false,
                         const std::string& dep_pairs = kDefaultDepPairs,
                         const std::string& bitmaps = kDefaultBitmaps) {
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
    try {
        pando::Corpus corpus;
        corpus.open(dir);
        const pando::FoldMode modes[] = {pando::FoldMode::Lower, pando::FoldMode::NoAccents,
                                         pando::FoldMode::LowerNoAccents};
        for (const auto& name : corpus.attr_names()) {
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
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc >= 3 && std::string(argv[1]) == "--upgrade") {
        std::string pairs = kDefaultDepPairs;
        std::string bitmaps = kDefaultBitmaps;
        for (int i = 3; i < argc; ++i) {
            const std::string a = argv[i];
            if (a == "--dep-pairs" && i + 1 < argc) pairs = argv[++i];
            else if (a.rfind("--dep-pairs=", 0) == 0) pairs = a.substr(12);
            else if (a == "--bitmaps" && i + 1 < argc) bitmaps = argv[++i];
            else if (a.rfind("--bitmaps=", 0) == 0) bitmaps = a.substr(10);
            else {
                std::cerr << "Error: unknown --upgrade option '" << a << "'\n";
                return 1;
            }
        }
        return upgrade_index(argv[2], false, pairs, bitmaps);
    }
    bool split_feats = false;
    bool format_vertical = false;
    bool format_jsonl = false;
    bool overlay_index = false;
    std::string index_dir;

    // Collect flags
    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--split-feats") split_feats = true;
        else if (a == "--overlay-index") overlay_index = true;
        else if (a == "--index-dir" && i + 1 < argc) index_dir = argv[++i];
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

    if (args.size() < 2) {
        std::cerr << "Usage: pando-index [options] <input> <output_dir>\n\n"
                  << "  input: .conllu file(s), directory (recursive), or '-' for JSONL stdin;\n"
                  << "         with --format vertical: .vrt/.vert/.txt files;\n"
                  << "         with --format jsonl: JSONL events as in dev/PANDO-INDEX-INTEGRATION.md\n\n"
                  << "  For each region attribute (e.g. text_langcode.val), the indexer also writes\n"
                  << "  .lex / .rev / .rev.idx (value → region ids) for fast :: metadata filters.\n\n"
                  << "  --split-feats     Split FEATS into feats#Feature columns (default: combined)\n"
                  << "  --format vertical Read CWB-style vertical (one token/line, <s> </s>);\n"
                  << "                    optional <!-- positional-attributes: ... --> (Korp/Kielipankki)\n"
                  << "  --format jsonl    Read streaming JSONL events (tokens/regions)\n"
                  << "  --overlay-index   Standoff-only JSONL: emit token-group columns + groups/ into\n"
                  << "                    output_dir (no full corpus). Requires --format jsonl and\n"
                  << "                    --index-dir <main_corpus_dir> (must contain corpus.info).\n"
                  << "  --index-dir       Main indexed corpus directory (for overlay size / stamp)\n"
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
                  << " values, flat structures)\n";
        return 1;
    }

    std::string input_path = args[0];
    std::string output_dir = args[1];

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
        if (upgrade_index(output_dir) != 0) return 1;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
