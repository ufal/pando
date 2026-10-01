// pando-check: verify corpus directory consistency (ROADMAP 6b).

#include "corpus/corpus.h"
#include "core/types.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cstdint>

namespace pando {

// Minimal corpus.info reader (duplicated from corpus.cpp to avoid loading full index).
struct CorpusInfoLite {
    CorpusPos size = 0;
    std::vector<std::string> positional_attrs;
    std::vector<std::string> structural_attrs;
};

static CorpusInfoLite read_info(const std::string& path) {
    CorpusInfoLite info;
    std::ifstream in(path);
    if (!in) {
        std::cerr << "Error: Cannot open " << path << "\n";
        return info;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        if (key == "size") {
            info.size = static_cast<CorpusPos>(std::stol(val));
        } else if (key == "positional") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.positional_attrs.push_back(tok);
        } else if (key == "structural") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.structural_attrs.push_back(tok);
        }
    }
    return info;
}

static bool file_exists(const std::string& path) {
    std::ifstream f(path);
    return f.good();
}

static size_t file_size(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    return f ? static_cast<size_t>(f.tellg()) : 0;
}

static int run_check(const std::string& dir) {
    std::string info_path = dir + "/corpus.info";
    if (!file_exists(info_path)) {
        std::cerr << "Error: " << info_path << " not found.\n";
        return 1;
    }

    CorpusInfoLite info = read_info(info_path);
    if (info.size < 0) {
        std::cerr << "Error: invalid size in corpus.info.\n";
        return 1;
    }
    if (info.positional_attrs.empty()) {
        std::cerr << "Error: no positional attributes in corpus.info.\n";
        return 1;
    }

    int errors = 0;
    const size_t corpus_sz = static_cast<size_t>(info.size);

    for (const auto& name : info.positional_attrs) {
        std::string base = dir + "/" + name;
        std::string lex_path = base + ".lex";
        std::string lex_idx_path = base + ".lex.idx";
        std::string dat_path = base + ".dat";
        std::string rev_path = base + ".rev";
        std::string rev_idx_path = base + ".rev.idx";

        if (!file_exists(lex_path)) { std::cerr << "Missing: " << lex_path << "\n"; ++errors; }
        if (!file_exists(lex_idx_path)) { std::cerr << "Missing: " << lex_idx_path << "\n"; ++errors; }
        if (!file_exists(dat_path)) { std::cerr << "Missing: " << dat_path << "\n"; ++errors; }
        if (!file_exists(rev_path)) { std::cerr << "Missing: " << rev_path << "\n"; ++errors; }
        if (!file_exists(rev_idx_path)) { std::cerr << "Missing: " << rev_idx_path << "\n"; ++errors; }

        if (info.size > 0 && file_exists(dat_path)) {
            size_t dat_sz = file_size(dat_path);
            size_t expected = corpus_sz * 4; // max width
            if (dat_sz % corpus_sz != 0) {
                std::cerr << "Invalid " << dat_path << ": size not multiple of corpus size.\n";
                ++errors;
            } else {
                int w = static_cast<int>(dat_sz / corpus_sz);
                if (w != 1 && w != 2 && w != 4) {
                    std::cerr << "Invalid " << dat_path << ": element width " << w << " (expected 1, 2, or 4).\n";
                    ++errors;
                }
            }
        }
    }

    for (const auto& name : info.structural_attrs) {
        std::string rgn_path = dir + "/" + name + ".rgn";
        if (!file_exists(rgn_path)) {
            std::cerr << "Missing: " << rgn_path << "\n";
            ++errors;
        }
    }

    bool has_s = false;
    for (const auto& s : info.structural_attrs) if (s == "s") { has_s = true; break; }
    if (has_s) {
        std::string dep_head = dir + "/dep.head";
        std::string dep_in = dir + "/dep.euler_in";
        std::string dep_out = dir + "/dep.euler_out";
        if (!file_exists(dep_head) && !file_exists(dir + "/dep.head_rel8")) { std::cerr << "Missing: " << dep_head << " (sentence structure present).\n"; ++errors; }
        if (!file_exists(dep_in))  { std::cerr << "Missing: " << dep_in << "\n"; ++errors; }
        if (!file_exists(dep_out)) { std::cerr << "Missing: " << dep_out << "\n"; ++errors; }
        if (file_exists(dep_head) && !file_exists(dir + "/dep.head_rel") && !file_exists(dir + "/dep.head_rel8"))
            std::cerr << "Note: no dep.head_rel (index predates it); run `pando-index --upgrade "
                      << dir << "` for faster dependency queries.\n";
    }

    if (errors > 0) {
        std::cerr << "Total: " << errors << " error(s).\n";
        return 1;
    }

    // Optional: full load and consistency (dat IDs in range, rev sorted)
    try {
        Corpus corpus;
        corpus.open(dir);
        std::cerr << "OK: corpus " << info.size << " tokens, "
                  << info.positional_attrs.size() << " positional, "
                  << info.structural_attrs.size() << " structural.\n";
    } catch (const std::exception& e) {
        std::cerr << "Load check failed: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

} // namespace pando

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: pando-check <corpus_dir>\n";
        return 1;
    }
    return pando::run_check(argv[1]);
}
