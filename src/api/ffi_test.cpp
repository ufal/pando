// The plain C API (pando_ffi.h): the options pando_query / pando_run read, and pando_cancel.
//
//   ffi_test <pando-index> <conllu>
//
// Builds an index from the CoNLL-U file and checks:
//   * open errors: NULL + pando_last_error();
//   * pando_query: sentence / fragment, sample + seed (the same seed, the same hits), shuffle;
//   * pando_run: a sorted page and `cat` with sentence / fragment (EF-2, EF-3);
//   * pando_cancel from another thread stops a running call ("cancelled": true).

#include "api/pando_ffi.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

static int g_fail = 0;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            ++g_fail;                                                                \
        }                                                                            \
    } while (0)

static std::string take(char* js) {
    std::string s = js ? js : "";
    pando_free_string(js);
    return s;
}
static bool has(const std::string& s, const char* needle) { return s.find(needle) != std::string::npos; }
static std::vector<long> starts(const std::string& body) {
    std::vector<long> v;
    static const std::regex re("\"match_start\": ?([0-9]+)");
    for (auto it = std::sregex_iterator(body.begin(), body.end(), re); it != std::sregex_iterator(); ++it)
        v.push_back(std::stol((*it)[1]));
    return v;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: ffi_test <pando-index> <conllu>\n";
        return 2;
    }
    const fs::path tmp = fs::temp_directory_path() / ("pando_ffi_test_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string dir = (tmp / "idx").string();
    const std::string cmd = std::string("\"") + argv[1] + "\" \"" + argv[2] + "\" \"" + dir + "\" >/dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) {
        std::cerr << "pando-index failed: " << cmd << "\n";
        return 2;
    }

    CHECK(pando_open("/nonexistent/pando/corpus", 0) == nullptr && std::strlen(pando_last_error()) > 0);
    pando_handle_t h = pando_open(dir.c_str(), 0);
    if (!h) {
        std::cerr << "open failed: " << pando_last_error() << "\n";
        return 1;
    }
    const char* nouns = R"([upos="NOUN"])";

    // pando_query: sentence / fragment
    {
        const std::string plain = take(pando_query(h, nouns, R"({"limit": 3, "context": 1})"));
        const std::string rich = take(pando_query(h, nouns, R"({"limit": 3, "context": 1, "sentence": true, "fragment": true})"));
        CHECK(!has(plain, "\"fragment\"") && has(rich, "\"fragment\"") && has(rich, "\"highlight_map\""));
        CHECK(starts(plain).size() == 3 && starts(plain) == starts(rich));
        CHECK(rich.size() > plain.size());
    }
    // sample + seed: 5 hits, the same ones for the same seed; shuffle
    {
        const std::string a = take(pando_query(h, nouns, R"({"limit": 20, "sample": 5, "seed": 7})"));
        const std::string b = take(pando_query(h, nouns, R"({"limit": 20, "sample": 5, "seed": 7})"));
        const std::string all = take(pando_query(h, nouns, R"({"limit": 5})"));
        CHECK(starts(a).size() == 5 && starts(a) == starts(b));
        const std::string sh = take(pando_query(h, nouns, R"({"limit": 50, "shuffle": true, "seed": 3})"));
        auto s = starts(sh);
        CHECK(s.size() == 50 && !std::is_sorted(s.begin(), s.end()));
        (void)all;
    }
    // pando_run: sorted page and cat with the page options
    {
        const std::string sorted = take(pando_run(h, R"(A = [upos="NOUN"]; sort A by lemma;)", R"({"limit": 3, "offset": 2})"));
        const std::string cat = take(pando_run(h, R"(A = [upos="NOUN"]; sort A by lemma; cat A 2 4)", R"({"fragment": true})"));
        CHECK(starts(sorted).size() == 3 && starts(cat) == starts(sorted));
        CHECK(has(cat, "\"fragment\"") && !has(cat, "not yet implemented"));
    }
    // pando_cancel from another thread: a count of ~30 s stops at once
    {
        std::string out;
        const auto t0 = std::chrono::steady_clock::now();
        std::thread t([&] { out = take(pando_query(h, "[]{1,60} []{0,60} []{0,60}", R"({"limit": 1, "total": true})")); });
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        pando_cancel(h);
        t.join();
        const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        CHECK(has(out, "\"cancelled\":true") && secs < 15);
        // the next call on the handle runs normally
        CHECK(starts(take(pando_query(h, nouns, R"({"limit": 2})"))).size() == 2);
    }

    pando_close(h);
    fs::remove_all(tmp);
    if (g_fail) {
        std::cerr << "ffi_test: FAILED (" << g_fail << ")\n";
        return 1;
    }
    std::cerr << "ffi_test: all passed\n";
    return 0;
}
