#include "corpus/builder.h"
#include "core/types.h"
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
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

static manatree::CorpusPos read_corpus_size_from_info(const std::string& main_dir) {
    std::ifstream in(main_dir + "/corpus.info");
    if (!in) throw std::runtime_error("Cannot open " + main_dir + "/corpus.info");
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("size=", 0) == 0) {
            long long n = std::stoll(line.substr(5));
            if (n < 0) throw std::runtime_error("Invalid size= in corpus.info");
            return static_cast<manatree::CorpusPos>(n);
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

int main(int argc, char* argv[]) {
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
                  << "  --split-feats     Split FEATS into feats_X (default: combined)\n"
                  << "  --format vertical Read CWB-style vertical (one token/line, <s> </s>)\n"
                  << "  --format jsonl    Read streaming JSONL events (tokens/regions)\n"
                  << "  --overlay-index   Standoff-only JSONL: emit token-group columns + groups/ into\n"
                  << "                    output_dir (no full corpus). Requires --format jsonl and\n"
                  << "                    --index-dir <main_corpus_dir> (must contain corpus.info).\n"
                  << "  --index-dir       Main indexed corpus directory (for overlay size / stamp)\n";
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
            manatree::CorpusPos main_size = read_corpus_size_from_info(index_dir);
            std::cerr << "Overlay index: main corpus size=" << main_size << " (from "
                      << index_dir << "/corpus.info)\n";
            std::cerr << "Reading overlay JSONL from " << input_path << "\n";
            manatree::CorpusBuilder builder(output_dir, true);
            builder.read_jsonl_overlay(input_path, main_size);
            builder.finalize();
            write_overlay_info(output_dir, index_dir, input_path);
            std::cerr << "Wrote overlay manifest " << output_dir << "/overlay.info\n";
            return 0;
        }

        manatree::CorpusBuilder builder(output_dir);
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

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
