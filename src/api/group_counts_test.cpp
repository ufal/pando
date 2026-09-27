// P7.1 / P7.7: group_rows (top-K from streamed buckets) and the group key order.
//
// * compare_group_keys is a strict weak order (std::sort requires one; the old
//   "numeric when both are numbers, else bytewise" had cycles);
// * group_rows(limit k) on streamed buckets == the first k rows of the full
//   list, and == the rows built from materialised matches;
// * sort_matches_by_key: keys non-decreasing, equal keys keep corpus order.
//
// Run: cmake --build build --target group_counts_test && ./build/group_counts_test

#include "api/group_counts.h"
#include "corpus/corpus.h"
#include "corpus/streaming_builder.h"
#include "query/executor.h"
#include "query/parser.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace pando;

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            std::exit(1); \
        } \
    } while (0)

static void test_order() {
    const std::vector<std::string> v = {"V", "1954", "2.5", "10", "9", "a", "MIX", "", "-3", "b\tx",
                                        "b\t10", "b\t9", "IV", "iv", "0", "+7", "Zeta", "abc"};
    for (const auto& a : v) {
        CHECK(!compare_group_keys(a, a));   // irreflexive
        for (const auto& b : v) {
            if (compare_group_keys(a, b)) CHECK(!compare_group_keys(b, a));   // asymmetric
            for (const auto& c : v)
                if (compare_group_keys(a, b) && compare_group_keys(b, c)) CHECK(compare_group_keys(a, c));
        }
    }
    CHECK(compare_group_keys("9", "10"));
    CHECK(compare_group_keys("b\t9", "b\t10"));
    CHECK(compare_group_keys("10", "a"));   // numbers first
    std::cerr << "  PASS test_order\n";
}

static void build_sample(const fs::path& dir) {
    StreamingBuilder b(dir.string());
    // a Zipf-ish value distribution with many ties
    for (int i = 0; i < 20000; ++i) {
        std::unordered_map<std::string, std::string> attrs;
        const int v = (i * 7919) % 997;
        attrs["form"] = "w" + std::to_string(v % (1 + v % 37));
        attrs["pos"] = (i % 3 == 0) ? "A" : ((i % 3 == 1) ? "B" : "C");
        attrs["num"] = std::to_string(v % 23);
        b.add_token(attrs, -1);
        if (i % 10 == 9) b.end_sentence();
    }
    b.finalize();
}

static void test_group_rows(const Corpus& corpus) {
    QueryExecutor ex(corpus);
    for (const char* cql : {"[pos=\"A\"]; count by form;", "[pos=\"A\"] [pos=\"B\"]; count by form;",
                            "a:[pos=\"B\"] b:[]; count by a.num, b.form;", "[]; count by num;"}) {
        Parser p(cql);
        Program prog = p.parse();
        CHECK(prog.size() == 2u && prog[1].has_command);
        const auto& fields = prog[1].command.fields;
        NameIndexMap nm = QueryExecutor::build_name_map_for_stripped_query(prog[0].query);
        MatchSet streamed = ex.execute(prog[0].query, 0, false, 0, 0, 0, 1, &fields);
        CHECK(streamed.aggregate_buckets != nullptr);
        MatchSet listed = ex.execute(prog[0].query, 0, false);
        GroupRows all = group_rows(corpus, streamed, fields, nm, 0, false);
        GroupRows all_listed = group_rows(corpus, listed, fields, nm, 0, false);
        CHECK(all.rows == all_listed.rows);
        CHECK(all.groups == all.rows.size() && all.groups == all_listed.groups);
        CHECK(all.total == all_listed.total && all.total == listed.matches.size());
        for (size_t k : {size_t{1}, size_t{2}, size_t{5}, size_t{17}, all.rows.size() - 1, all.rows.size(),
                         all.rows.size() + 3}) {
            if (k == 0) continue;
            GroupRows top = group_rows(corpus, streamed, fields, nm, k, false);
            CHECK(top.groups == all.groups && top.total == all.total);
            CHECK(top.rows.size() == std::min(k, all.rows.size()));
            for (size_t i = 0; i < top.rows.size(); ++i) CHECK(top.rows[i] == all.rows[i]);
        }
        std::cerr << "  PASS group_rows " << cql << " (" << all.groups << " groups)\n";
    }
}

static void test_sort(const Corpus& corpus) {
    QueryExecutor ex(corpus);
    Parser p("[pos=\"A\"];");
    Program prog = p.parse();
    MatchSet ms = ex.execute(prog[0].query, 0, false);
    NameIndexMap nm = QueryExecutor::build_name_map_for_stripped_query(prog[0].query);
    const std::vector<std::string> fields = {"num"};
    for (bool group_order : {true, false}) {
        std::vector<Match> m = ms.matches;
        sort_matches_by_key(corpus, m, nm, fields, group_order);
        CHECK(m.size() == ms.matches.size());
        for (size_t i = 1; i < m.size(); ++i) {
            const std::string a = make_group_key(corpus, m[i - 1], nm, fields);
            const std::string b = make_group_key(corpus, m[i], nm, fields);
            CHECK(group_order ? !compare_group_keys(b, a) : !(b < a));
            if (a == b) CHECK(m[i - 1].first_pos() < m[i].first_pos());   // stable
        }
    }
    std::cerr << "  PASS test_sort\n";
}

int main() {
    test_order();
    fs::path dir = fs::temp_directory_path() / ("pando_group_counts_test_" + std::to_string(::getpid()));
    if (fs::exists(dir)) fs::remove_all(dir);
    fs::create_directories(dir);
    build_sample(dir);
    {
        Corpus corpus;
        corpus.open(dir.string(), false);
        test_group_rows(corpus);
        test_sort(corpus);
    }
    fs::remove_all(dir);
    std::cerr << "group_counts_test: all passed\n";
    return 0;
}
