#include "core/types.h"
#include "corpus/index_layout.h"
#include "core/mmap_file.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include <fstream>

using namespace pando;
namespace fs = std::filesystem;

static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, delim))
        if (!tok.empty()) out.push_back(tok);
    return out;
}

// Scan a null-separated string file and build int64 byte-offset index.
static void build_lex_idx(const std::string& lex_path,
                          const std::string& idx_path) {
    auto f = MmapFile::open(lex_path);
    const char* data = static_cast<const char*>(f.data());
    std::vector<int64_t> offsets;
    offsets.push_back(0);
    for (size_t i = 0; i < f.size(); ++i) {
        if (data[i] == '\0')
            offsets.push_back(static_cast<int64_t>(i) + 1);
    }
    write_vec(idx_path, offsets);
    std::cerr << "  lexicon: " << (offsets.size() - 1) << " entries\n";
}

// Read a CWB .corpus file (int32 lex IDs) and build the Manatree
// reverse index (.rev + .rev.idx) with int64 positions.
// Two-pass: count, then fill — O(corpus_size) time, minimal memory.
static void build_rev_index(const std::string& dat_path,
                            const std::string& rev_path,
                            const std::string& rev_idx_path,
                            LexiconId lex_size) {
    auto corpus = MmapFile::open(dat_path);
    const auto* data = corpus.as<int32_t>();
    auto n = static_cast<CorpusPos>(corpus.count<int32_t>());

    // Pass 1: count positions per lex ID
    std::vector<int64_t> cnt(static_cast<size_t>(lex_size), 0);
    for (CorpusPos p = 0; p < n; ++p)
        ++cnt[static_cast<size_t>(data[p])];

    // Build rev.idx (cumulative offsets)
    std::vector<int64_t> rev_idx(static_cast<size_t>(lex_size) + 1);
    rev_idx[0] = 0;
    for (LexiconId id = 0; id < lex_size; ++id)
        rev_idx[static_cast<size_t>(id) + 1] =
            rev_idx[static_cast<size_t>(id)] + cnt[static_cast<size_t>(id)];

    // Pass 2: fill sorted position lists
    std::vector<CorpusPos> rev_flat(static_cast<size_t>(n));
    std::vector<int64_t> cursor(rev_idx.begin(), rev_idx.end() - 1);
    for (CorpusPos p = 0; p < n; ++p) {
        auto id = static_cast<size_t>(data[p]);
        rev_flat[static_cast<size_t>(cursor[id]++)] = p;
    }

    write_vec(rev_path, rev_flat);
    write_vec(rev_idx_path, rev_idx);
    std::cerr << "  rev index: " << n << " positions\n";
}

// Convert CWB .avx (int32 start,end pairs) to Manatree .rgn (int64 pairs).
static size_t convert_structure(const std::string& avx_path,
                                const std::string& rgn_path) {
    auto f = MmapFile::open(avx_path);
    size_t n_regions = f.size() / (2 * sizeof(int32_t));
    const auto* pairs = f.as<int32_t>();

    std::vector<Region> regions(n_regions);
    for (size_t i = 0; i < n_regions; ++i) {
        regions[i].start = static_cast<CorpusPos>(pairs[i * 2]);
        regions[i].end   = static_cast<CorpusPos>(pairs[i * 2 + 1]);
    }

    write_file(rgn_path, regions.data(), regions.size() * sizeof(Region));
    return n_regions;
}

static void import_attribute(const std::string& cwb_dir,
                             const std::string& out_dir,
                             const std::string& attr) {
    std::cerr << "Attribute: " << attr << "\n";

    std::string src_lex = cwb_dir + "/" + attr + ".lexicon";
    std::string src_dat = cwb_dir + "/" + attr + ".corpus";
    std::string dst_lex = out_dir + "/" + attr + ".lex";
    std::string dst_idx = out_dir + "/" + attr + ".lex.idx";
    std::string dst_dat = out_dir + "/" + attr + ".dat";
    std::string dst_rev = out_dir + "/" + attr + ".rev";
    std::string dst_ridx = out_dir + "/" + attr + ".rev.idx";

    if (!fs::exists(src_lex))
        throw std::runtime_error("Missing " + src_lex);
    if (!fs::exists(src_dat))
        throw std::runtime_error("Missing " + src_dat);

    // Lexicon: byte-identical, just copy + build int64 offset index
    fs::copy_file(src_lex, dst_lex, fs::copy_options::overwrite_existing);
    build_lex_idx(dst_lex, dst_idx);

    // Count lex entries for the rev index builder
    auto idx_file = MmapFile::open(dst_idx);
    LexiconId lex_size = static_cast<LexiconId>(idx_file.count<int64_t>()) - 1;

    // Corpus data: byte-identical (int32 lex IDs), just copy
    fs::copy_file(src_dat, dst_dat, fs::copy_options::overwrite_existing);

    // Rebuild reverse index with int64 positions from corpus data
    build_rev_index(dst_dat, dst_rev, dst_ridx, lex_size);
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Import a CWB-indexed corpus into Manatree format.\n\n"
                  << "Usage: cwb-import <cwb_data_dir> <output_dir> "
                  << "<attr1,attr2,...> [struct1,struct2,...]\n\n"
                  << "CWB files read per attribute:\n"
                  << "  ATTR.lexicon  (null-separated strings → copied)\n"
                  << "  ATTR.corpus   (int32 lex IDs     → copied as .dat)\n"
                  << "  (reverse index rebuilt from .corpus data)\n\n"
                  << "CWB files read per structure:\n"
                  << "  STRUCT.avx    (int32 start,end pairs → widened to int64)\n\n"
                  << "Example:\n"
                  << "  cwb-import /path/to/cwb/data ./my_index "
                  << "word,lemma,pos,deprel s,text\n";
        return 1;
    }

    std::string cwb_dir  = argv[1];
    std::string out_dir  = argv[2];
    auto attrs = split(argv[3], ',');
    auto structs = (argc > 4) ? split(argv[4], ',') : std::vector<std::string>{};

    try {
        fs::create_directories(out_dir);

        CorpusPos corpus_size = 0;

        for (const auto& attr : attrs) {
            import_attribute(cwb_dir, out_dir, attr);
            if (corpus_size == 0) {
                auto f = MmapFile::open(out_dir + "/" + attr + ".dat");
                corpus_size = static_cast<CorpusPos>(f.count<int32_t>());
            }
        }

        for (const auto& st : structs) {
            std::string avx = cwb_dir + "/" + st + ".avx";
            if (!fs::exists(avx)) {
                std::cerr << "Warning: " << avx << " not found, skipping\n";
                continue;
            }
            std::string rgn = out_dir + "/" + st + ".rgn";
            size_t n = convert_structure(avx, rgn);
            std::cerr << "Structure " << st << ": " << n << " regions\n";
        }

        // Write corpus.info
        {
            std::ofstream info(out_dir + "/corpus.info");
            info << "size=" << corpus_size << "\n";
            info << "layout_version=" << pando::kIndexLayoutVersion << "\n";
            info << "positional=";
            for (size_t i = 0; i < attrs.size(); ++i) {
                if (i > 0) info << ",";
                info << attrs[i];
            }
            info << "\n";
            if (!structs.empty()) {
                info << "structural=";
                for (size_t i = 0; i < structs.size(); ++i) {
                    if (i > 0) info << ",";
                    info << structs[i];
                }
                info << "\n";
            }
        }

        std::cerr << "Done. Corpus: " << corpus_size << " tokens, "
                  << attrs.size() << " attributes.\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
