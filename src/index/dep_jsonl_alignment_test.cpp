// Dependency ingest parity for JSONL v2 sentence regions.
// Ensures head_tok_id mapping remains correct even when `s` region events carry
// mismatched coordinates (for example 1-based start/end from exporters).

#include "corpus/builder.h"
#include "corpus/corpus.h"
#include "query/executor.h"
#include "query/parser.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace pando;

namespace {

fs::path make_scratch(const std::string& tag) {
    fs::path base = fs::temp_directory_path() /
        ("pando_dep_jsonl_align_" + tag + "_" + std::to_string(::getpid()));
    if (fs::exists(base)) fs::remove_all(base);
    fs::create_directories(base);
    return base;
}

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            std::exit(1); \
        } \
    } while (0)

void write_fixture_jsonl(const fs::path& jsonl_path) {
    std::ofstream out(jsonl_path);
    CHECK(static_cast<bool>(out));
    out
        << "{\"type\":\"header\",\"version\":2,\"positional\":[\"form\",\"upos\",\"deprel\"],"
           "\"default_within\":\"s\",\"split_feats\":false}\n"
        << "{\"type\":\"token\",\"tok_id\":\"w-1\",\"head_tok_id\":\"w-2\",\"form\":\"A\",\"upos\":\"DET\",\"deprel\":\"det\"}\n"
        << "{\"type\":\"token\",\"tok_id\":\"w-2\",\"head_tok_id\":\"w-3\",\"form\":\"B\",\"upos\":\"NOUN\",\"deprel\":\"obj\"}\n"
        << "{\"type\":\"token\",\"tok_id\":\"w-3\",\"head\":0,\"form\":\"C\",\"upos\":\"VERB\",\"deprel\":\"root\"}\n"
        // Deliberately 1-based and mismatched vs buffered sentence boundaries (0..2).
        << "{\"type\":\"region\",\"struct\":\"s\",\"start_pos\":1,\"end_pos\":3,\"attrs\":{\"id\":\"s-1\"}}\n";
}

void test_jsonl_s_region_mismatch_does_not_break_deps() {
    auto dir = make_scratch("core");
    auto jsonl = dir / "fixture.jsonl";
    write_fixture_jsonl(jsonl);

    CorpusBuilder b(dir.string());
    b.read_jsonl(jsonl.string());
    b.finalize();

    Corpus corpus;
    corpus.open(dir.string(), false);

    const auto& s = corpus.structure("s");
    CHECK(s.region_count() == 1u);
    Region rg = s.get(0);
    CHECK(rg.start == 0);
    CHECK(rg.end == 2);

    QueryExecutor ex(corpus);
    Parser p1(R"([form="B" & child[upos="DET"]];)");
    MatchSet m1 = ex.execute(p1.parse()[0].query, 50, false);
    CHECK(m1.matches.size() == 1u);

    Parser p2(R"([form="B" & not child[upos="DET"]];)");
    MatchSet m2 = ex.execute(p2.parse()[0].query, 50, false);
    CHECK(m2.matches.empty());

    Parser p3(R"([form="B"] > [upos="DET"];)");
    MatchSet m3 = ex.execute(p3.parse()[0].query, 50, false);
    CHECK(m3.matches.size() == 1u);

    // Euler/subtree sanity for the same toy tree: C governs descendants A and B.
    Parser p4(R"([form="C"] >> [form="A"];)");
    MatchSet m4 = ex.execute(p4.parse()[0].query, 50, false);
    CHECK(m4.matches.size() == 1u);

    fs::remove_all(dir);
    std::cerr << "  PASS test_jsonl_s_region_mismatch_does_not_break_deps\n";
}

}  // namespace

int main() {
    std::cerr << "running dep_jsonl_alignment_test\n";
    test_jsonl_s_region_mismatch_does_not_break_deps();
    std::cerr << "all dep_jsonl_alignment tests passed\n";
    return 0;
}

