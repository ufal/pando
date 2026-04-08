// Stage 1 (PANDO-MULTIVALUE-FIELDS): executor uses .mv.fwd for MV membership,
// MV-002 overlap for :: alignment, and nvals cardinality from forward count.
//
// Run: cmake --build build --target mv_stage1_executor_test && ./build/mv_stage1_executor_test

#include "corpus/corpus.h"
#include "corpus/streaming_builder.h"
#include "query/executor.h"
#include "query/parser.h"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace manatree;

namespace {

fs::path make_scratch(const std::string& tag) {
    fs::path base = fs::temp_directory_path() /
        ("pando_mv_stage1_exec_" + tag + "_" + std::to_string(::getpid()));
    if (fs::exists(base)) fs::remove_all(base);
    fs::create_directories(base);
    return base;
}

// Same shape as mv_fwd_test basics corpus: domain is multivalue.
void build_sample(const fs::path& dir) {
    StreamingBuilder b(dir.string());
    b.declare_multivalue("domain");
    std::vector<std::string> vals = {
        "Hunting",
        "Hunting|Fishing",
        "zebra|apple",
        "a|a|b",
        "",
        "Fishing",
    };
    for (size_t i = 0; i < vals.size(); ++i) {
        std::unordered_map<std::string, std::string> attrs;
        attrs["form"] = "tok" + std::to_string(i);
        attrs["domain"] = vals[i];
        b.add_token(attrs, -1);
    }
    b.end_sentence();
    b.finalize();
}

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            std::exit(1); \
        } \
    } while (0)

void test_mv_eq_membership() {
    auto dir = make_scratch("eq");
    build_sample(dir);

    Corpus corpus;
    corpus.open(dir.string(), false);
    QueryExecutor ex(corpus);

    Parser p1(R"([domain="Hunting"];)");
    Program prog1 = p1.parse();
    CHECK(prog1.size() == 1u);
    CHECK(prog1[0].has_query);
    MatchSet m1 = ex.execute(prog1[0].query, 100, false);
    // Positions 0 and 1 contain component Hunting.
    CHECK(m1.matches.size() == 2u);

    Parser p2(R"([domain="Fishing"];)");
    Program prog2 = p2.parse();
    MatchSet m2 = ex.execute(prog2[0].query, 100, false);
    CHECK(m2.matches.size() == 2u);

    Parser p3(R"([domain="zebra"];)");
    Program prog3 = p3.parse();
    MatchSet m3 = ex.execute(prog3[0].query, 100, false);
    CHECK(m3.matches.size() == 1u);

    fs::remove_all(dir);
    std::cerr << "  PASS test_mv_eq_membership\n";
}

void test_alignment_overlap() {
    auto dir = make_scratch("align");
    build_sample(dir);

    Corpus corpus;
    corpus.open(dir.string(), false);
    QueryExecutor ex(corpus);

    // Adjacent tokens at 0 and 1 share Hunting — joined strings differ, overlap must pass.
    Parser p(R"(a:[form="tok0"] b:[form="tok1"] :: a.domain = b.domain;)");
    Program prog = p.parse();
    CHECK(prog.size() == 1u);
    MatchSet m = ex.execute(prog[0].query, 100, false);
    CHECK(!m.matches.empty());

    fs::remove_all(dir);
    std::cerr << "  PASS test_alignment_overlap\n";
}

void test_parallel_with_mv002_join() {
    auto dir = make_scratch("parallel");
    build_sample(dir);

    Corpus corpus;
    corpus.open(dir.string(), false);
    QueryExecutor ex(corpus);

    // Source|Target with :: a.domain = b.domain — MV-002 overlap join (inverted index path).
    Parser p(R"(a:[form="tok0"] with b:[form="tok1"] :: a.domain = b.domain;)");
    Program prog = p.parse();
    CHECK(prog.size() == 1u);
    CHECK(prog[0].is_parallel);
    MatchSet m = ex.execute_parallel(prog[0].query, prog[0].target_query, 100, false);
    CHECK(m.parallel_matches.size() == 1u);

    fs::remove_all(dir);
    std::cerr << "  PASS test_parallel_with_mv002_join\n";
}

void test_nvals_uses_forward_count() {
    auto dir = make_scratch("nvals");
    build_sample(dir);

    Corpus corpus;
    corpus.open(dir.string(), false);
    QueryExecutor ex(corpus);

    Parser p(R"([nvals(domain) = 2];)");
    Program prog = p.parse();
    MatchSet m = ex.execute(prog[0].query, 100, false);
    // Positions with exactly two MV components: 1 (Hunting|Fishing), 2 (apple|zebra), 3 (a|b)
    CHECK(m.matches.size() == 3u);

    fs::remove_all(dir);
    std::cerr << "  PASS test_nvals_uses_forward_count\n";
}

}  // namespace

int main() {
    std::cerr << "running mv_stage1_executor_test\n";
    test_mv_eq_membership();
    test_alignment_overlap();
    test_parallel_with_mv002_join();
    test_nvals_uses_forward_count();
    std::cerr << "all mv_stage1_executor tests passed\n";
    return 0;
}
