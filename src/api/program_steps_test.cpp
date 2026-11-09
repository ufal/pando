// /run program steps KonText needs after a query (as Manatee does them):
//
// * `sort` of hits is bytewise per field (as the CLI, CQP and Manatee): `!` < `1789` < `vi`
//   (not numbers / Roman numerals first, as count tables order their rows); `%r` retrograde;
// * `count by form on match[-1]..match[-1]` (a context position; ranges read from..to,
//   joined with a space; several fields; positions outside the corpus clamped);
// * Manatee-dialect programs with named queries (`A = …; F = …; filter A by F …; size A`);
// * `filter M [not] by F on from..to [rank N] [excluding match]` in place, chained with sort,
//   kept across drop_caches (replayed), seen by /query "from" (page_json); a redefinition of
//   a filtered name with the same query is the query's full result again;
// * `shuffle M [seed N]`, `sample M N [seed S]` in place, reproducible with the seed;
// * the session's size limit applies to the filter query, with an error naming it;
// * `set coll-count hits`: collocates counted once per hit (Manatee).
//
// Run: cmake --build build --target program_steps_test && ./build/program_steps_test

#include "api/query_json.h"
#include "corpus/corpus.h"
#include "corpus/streaming_builder.h"
#include "query/dialect/dialect.h"
#include "query/parser.h"
#include "query/sort_field.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace pando;

static int g_fail = 0;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            ++g_fail;                                                                \
        }                                                                            \
    } while (0)

// Self-authored sentences (form lemma upos); `dog` hits at every sentence's NOUN.
static void build_sample(const fs::path& dir) {
    const std::vector<std::vector<std::vector<std::string>>> sents = {
        {{"!", "!", "PUNCT"}, {"dog", "dog", "NOUN"}, {"barks", "bark", "VERB"}},
        {{"vi", "vi", "X"}, {"dog", "dog", "NOUN"}, {"sleeps", "sleep", "VERB"}},
        {{"1789", "1789", "NUM"}, {"dog", "dog", "NOUN"}, {"ate", "eat", "VERB"}},
        {{"the", "the", "DET"}, {"big", "big", "ADJ"}, {"dog", "dog", "NOUN"}, {"runs", "run", "VERB"}},
        {{"a", "a", "DET"}, {"small", "small", "ADJ"}, {"old", "old", "ADJ"}, {"dog", "dog", "NOUN"}},
        {{"the", "the", "DET"}, {"dog", "dog", "NOUN"}},
        {{"big", "big", "ADJ"}, {"dogs", "dog", "NOUN"}, {"bark", "bark", "VERB"}},
    };
    StreamingBuilder b(dir.string());
    for (const auto& s : sents) {
        for (const auto& t : s) {
            std::unordered_map<std::string, std::string> a{{"form", t[0]}, {"lemma", t[1]}, {"upos", t[2]}};
            b.add_token(a, -1);
        }
        b.end_sentence();
    }
    b.finalize();
}

static std::vector<std::string> lefts(const std::string& js) {
    std::vector<std::string> out;
    static const std::regex re("\"left\": \"([^\"]*)\"");
    for (auto it = std::sregex_iterator(js.begin(), js.end(), re); it != std::sregex_iterator(); ++it) {
        std::string l = (*it)[1];
        const size_t sp = l.rfind(' ');
        out.push_back(sp == std::string::npos ? l : l.substr(sp + 1));   // the word left of the hit
    }
    return out;
}

static std::vector<std::string> starts(const std::string& js) {
    std::vector<std::string> out;
    static const std::regex re("\"match_start\": (\\d+)");
    for (auto it = std::sregex_iterator(js.begin(), js.end(), re); it != std::sregex_iterator(); ++it)
        out.push_back((*it)[1]);
    return out;
}

static long size_of(const std::string& js) {
    std::smatch m;
    static const std::regex re("\"operation\": \"size\".*\"result\": (\\d+)");
    return std::regex_search(js, m, re) ? std::stol(m[1]) : -1;
}

static bool has(const std::string& s, const std::string& n) { return s.find(n) != std::string::npos; }

static void test_parse() {
    Program p = Parser("filter A not by F on match[-5]..matchend[5] rank -2 excluding match; shuffle seed 4; "
                       "sample A 10 seed 2; sample 3; count by form %cr on match[-1]..match[-3], lemma")
                    .parse();
    CHECK(p.size() == 5u);
    const GroupCommand& f = p[0].command;
    CHECK(f.type == CommandType::FILTER && f.query_name == "A" && f.filter_query == "F" && f.filter_negative);
    CHECK(f.filter_from == "match[-5]" && f.filter_to == "matchend[5]" && f.filter_rank == -2 && f.filter_excluding_match);
    CHECK(p[1].command.type == CommandType::SHUFFLE && p[1].command.query_name.empty() && p[1].command.has_seed
          && p[1].command.seed == 4u);
    CHECK(p[2].command.type == CommandType::SAMPLE && p[2].command.query_name == "A" && p[2].command.sample_n == 10u
          && p[2].command.seed == 2u);
    CHECK(p[3].command.type == CommandType::SAMPLE && p[3].command.sample_n == 3u && !p[3].command.has_seed);
    CHECK(p[4].command.fields.size() == 2u && p[4].command.fields[0] == "form %cr on match[-1]..match[-3]");
    SortFieldSpec spec;
    CHECK(parse_sort_field("form %cr on match[-1]..match[-3]", spec) && spec.retrograde && spec.fold_case);
    CHECK(sort_field_text(spec) == "form %cr on match[-1]..match[-3]");
    CHECK(reverse_utf8("a\xc3\xb1" "b") == "b\xc3\xb1" "a");   // añb → bña
    std::cerr << "  PASS test_parse\n";
}

static void test_dialect_program(const Corpus& corpus) {
    Program p = parse_in_dialect("A = [lemma=\"dog\"]; F = [upos=\"ADJ\"]; filter A by F on match[-1]..match[-1]; size A",
                                 "manatee-cql", ParserOptions{}, &corpus);
    CHECK(p.size() == 4u);
    if (p.size() == 4u) {
        CHECK(p[0].has_query && p[0].name == "A" && p[0].source == "[lemma=\"dog\"]");
        CHECK(p[1].has_query && p[1].name == "F" && p[1].source == "[upos=\"ADJ\"]");
        CHECK(p[2].has_command && p[2].command.type == CommandType::FILTER);
        CHECK(p[3].has_command && p[3].command.type == CommandType::SIZE);
    }
    // a single query is read as before
    CHECK(parse_in_dialect("[lemma=\"dog\"]", "manatee-cql", ParserOptions{}, &corpus).size() == 1u);
    // a statement that is not Manatee CQL: its own error, not the whole text's
    bool threw = false;
    try {
        parse_in_dialect("A = [lemma=\"dog\"]; B = [lemma=", "manatee-cql", ParserOptions{}, &corpus);
    } catch (const std::exception&) { threw = true; }
    CHECK(threw);
    std::cerr << "  PASS test_dialect_program\n";
}

static void test_sort_bytewise(Corpus& corpus) {
    ProgramSession ps;
    const std::string js = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; sort A by form on match[-1]..match[-1]");
    const std::vector<std::string> want = {"!", "1789", "big", "big", "old", "the", "vi"};
    CHECK(lefts(js) == want);
    // a second sort (of the kept hits) also bytewise
    const std::string js2 = run_program_json(corpus, ps, "sort A by lemma; sort A by form on match[-1]");
    CHECK(lefts(js2) == want);
    // %r: each value read backwards (! 9871 dlo eht gib gib iv)
    const std::string js3 = run_program_json(corpus, ps, "B = [lemma=\"dog\"]; sort B by form %r on match[-1]");
    CHECK((lefts(js3) == std::vector<std::string>{"!", "1789", "old", "the", "big", "big", "vi"}));
    std::cerr << "  PASS test_sort_bytewise\n";
}

static void test_count_range(Corpus& corpus) {
    ProgramSession ps;
    std::string js = run_program_json(corpus, ps, "[lemma=\"dog\"]; count by form on match[-1]..match[-1]");
    CHECK(has(js, "\"fields\": [\"form on match[-1]..match[-1]\"]"));
    CHECK(has(js, "{\"key\": \"big\", \"count\": 2"));
    CHECK(has(js, "\"total_matches\": 7"));
    // a range: left to right, joined with a space
    js = run_program_json(corpus, ps, "[lemma=\"dog\"]; count by form on match[-2]..match[-1]");
    CHECK(has(js, "{\"key\": \"small old\", \"count\": 1"));
    // written backwards: read backwards (as sort)
    js = run_program_json(corpus, ps, "[lemma=\"dog\"]; count by form on match[-1]..match[-2]");
    CHECK(has(js, "{\"key\": \"old small\", \"count\": 1"));
    // before the corpus: clamped to its first token
    js = run_program_json(corpus, ps, "[lemma=\"dog\"]; count by form on match[-5]..match[-5]");
    CHECK(has(js, "\"total_matches\": 7"));
    // with another field (the usual two-field shape) and on a named set; group and freq too
    js = run_program_json(corpus, ps, "D = [lemma=\"dog\"]; count D by form on match[-1]..match[-1], lemma");
    CHECK(has(js, "\"fields\": [\"form on match[-1]..match[-1]\", \"lemma\"]") && has(js, "\"value\": \"big\""));
    js = run_program_json(corpus, ps, "freq D by form %c on match[-1]");
    CHECK(has(js, "\"ok\": true") && has(js, "big"));
    std::cerr << "  PASS test_count_range\n";
}

static void test_filter(Corpus& corpus) {
    ProgramSession ps;
    ProgramOptions mo;
    mo.dialect = "manatee-cql";
    auto size_after = [&](const std::string& filter) {
        return size_of(run_program_json(corpus, ps, "A = [lemma=\"dog\"]; F = [upos=\"ADJ\"]; FF = [upos=\"ADJ\"] [upos=\"ADJ\"]; "
                                                    "N = [lemma=\"dog\"]; " + filter + "; size A", mo));
    };
    CHECK(size_after("filter A by F on match[-1]..match[-1]") == 3);
    CHECK(size_after("filter A not by F on match[-1]..match[-1]") == 4);
    CHECK(size_after("filter A by F on match[-2]..match[-1] rank 2") == 1);
    CHECK(size_after("filter A by F on match[-2]..match[-1] rank -2") == 1);
    CHECK(size_after("filter A by FF on match[-2]..match[-1] rank 0") == 1);
    CHECK(size_after("filter A by FF on match[-3]..match[-1] rank 0") == 0);
    CHECK(size_after("filter A by FF on match[-3]..match[-1]") == 1);
    CHECK(size_after("filter A by N on match..matchend") == 7);
    CHECK(size_after("filter A by N on match..matchend excluding match") == 0);
    // windows cross sentence boundaries (as Manatee's): ADJ at 10, 14, 15, 19; hits 1 4 7 11 16 18 20
    CHECK(size_after("filter A by F on match[-3]..matchend[3] excluding match") == 5);
    // a redefinition of the filtered name with the same query: the whole result again
    CHECK(size_of(run_program_json(corpus, ps, "A = [lemma=\"dog\"]; size A", mo)) == 7);

    // the step answers with the set's page (total exact); steps chain
    std::string js = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; F = [upos=\"ADJ\"]; filter A by F on match[-1]..match[-1]");
    CHECK(has(js, "\"total\": 3, \"total_exact\": true"));
    js = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; F = [upos=\"ADJ\"]; G = [lemma=\"small\"]; "
                                      "filter A by F on match[-1]; filter A not by G on match[-2]; sort A by form on match[-1]; cat A 0 9");
    CHECK((lefts(js) == std::vector<std::string>{"big", "big"}));
    // dropped hits are built again with the steps; /query "from" sees them
    ps.drop_caches();
    QueryOptions qo;
    qo.limit = 10;
    qo.total = true;
    js = ps.page_json(corpus, "A", qo);
    CHECK((lefts(js) == std::vector<std::string>{"big", "big"}));
    CHECK(size_of(run_program_json(corpus, ps, "size A")) == 2);
    js = run_program_json(corpus, ps, "count A by form on match[-1]");
    CHECK(has(js, "\"total_matches\": 2"));
    // a sorted set is not reused either
    run_program_json(corpus, ps, "A = [lemma=\"dog\"]; sort A by form; size A");
    CHECK(size_of(run_program_json(corpus, ps, "A = [lemma=\"dog\"]; F = [upos=\"ADJ\"]; size A")) == 7);

    // an unknown filter query; the size limit on the filter query names it
    js = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; filter A by Nope on match[-1]");
    CHECK(has(js, "\"ok\": false") && has(js, "no query named Nope"));
    ProgramSession small;
    small.set_max_hits(4);
    bool named = false;
    try {
        run_program_json(corpus, small, "A = [lemma=\"dog\"]; F = [upos=\"ADJ\"|upos=\"VERB\"]; filter A by F on match[-1]; size A");
    } catch (const std::exception& e) {
        named = has(e.what(), "filter query") && has(e.what(), "more than this session materialises");
    }
    CHECK(named);
    std::cerr << "  PASS test_filter\n";
}

static void test_shuffle_sample(Corpus& corpus) {
    ProgramSession ps;
    const std::string a = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; shuffle A seed 7");
    const std::string b = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; shuffle A seed 7");
    CHECK(starts(a) == starts(b) && starts(a).size() == 7u);
    CHECK(has(a, "\"total\": 7, \"total_exact\": true"));
    std::vector<std::string> s = starts(a), sorted = s;
    std::sort(sorted.begin(), sorted.end(), [](const std::string& x, const std::string& y) { return std::stol(x) < std::stol(y); });
    CHECK(s != sorted);   // (seed 7 does not give corpus order)
    // sample: N hits, in the set's order; all when fewer; reproducible; replayed
    std::string js = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; sample A 3 seed 1");
    CHECK(has(js, "\"total\": 3, \"total_exact\": true"));
    std::vector<std::string> st = starts(js), st_sorted = st;
    std::sort(st_sorted.begin(), st_sorted.end(), [](const std::string& x, const std::string& y) { return std::stol(x) < std::stol(y); });
    CHECK(st == st_sorted);
    ps.drop_caches();
    QueryOptions qo;
    qo.limit = 10;
    CHECK(starts(ps.page_json(corpus, "A", qo)) == st);
    CHECK(size_of(run_program_json(corpus, ps, "A = [lemma=\"dog\"]; sample A 100; size A")) == 7);
    // sample then sort; shuffle then filter
    js = run_program_json(corpus, ps, "A = [lemma=\"dog\"]; sample A 3 seed 1; sort A by form on match[-1]");
    std::vector<std::string> l = lefts(js);
    CHECK(l.size() == 3u && std::is_sorted(l.begin(), l.end()));
    CHECK(size_of(run_program_json(corpus, ps, "A = [lemma=\"dog\"]; F = [upos=\"ADJ\"]; shuffle A; filter A by F on match[-1]; size A")) == 3);
    std::cerr << "  PASS test_shuffle_sample\n";
}

// `set coll-count hits`: a collocate counts once per hit (Manatee's CollocItems). Lemma `dog`
// around the dog hits ±3: 8 occurrences, in 6 hits (hits 4 and 18 have two each).
static void test_coll_per_hit(Corpus& corpus) {
    ProgramSession ps;
    const std::string base = "[lemma=\"dog\"]; set left 3; set right 3; set min-freq 1; set limit 50; ";
    std::string js = run_program_json(corpus, ps, base + "coll by lemma");
    CHECK(std::regex_search(js, std::regex("\"word\": \"dog\", \"obs\": 8\\b")));
    js = run_program_json(corpus, ps, base + "set coll-count hits; coll by lemma");
    CHECK(std::regex_search(js, std::regex("\"word\": \"dog\", \"obs\": 6\\b")));
    // a named set whose hits are kept counts the same way
    js = run_program_json(corpus, ps, "D = [lemma=\"dog\"]; sort D by form; set left 3; set right 3; set min-freq 1; "
                                      "set coll-count hits; coll D by lemma");
    CHECK(std::regex_search(js, std::regex("\"word\": \"dog\", \"obs\": 6\\b")));
    js = run_program_json(corpus, ps, "set coll-count often");
    CHECK(has(js, "\"ok\": false"));
    std::cerr << "  PASS test_coll_per_hit\n";
}

int main() {
    test_parse();
    fs::path dir = fs::temp_directory_path() / ("pando_program_steps_test_" + std::to_string(::getpid()));
    if (fs::exists(dir)) fs::remove_all(dir);
    fs::create_directories(dir);
    build_sample(dir);
    {
        Corpus corpus;
        corpus.open(dir.string(), false);
        test_dialect_program(corpus);
        test_sort_bytewise(corpus);
        test_count_range(corpus);
        test_filter(corpus);
        test_shuffle_sample(corpus);
        test_coll_per_hit(corpus);
    }
    fs::remove_all(dir);
    if (g_fail) {
        std::cerr << "program_steps_test: " << g_fail << " failure(s)\n";
        return 1;
    }
    std::cerr << "program_steps_test: all passed\n";
    return 0;
}
