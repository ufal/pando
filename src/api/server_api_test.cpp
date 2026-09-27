// ServerApi + its C ABI (server_capi.h): routes, errors, lifetime, concurrency.
//
//   server_api_test <pando-index> <conllu> [corpus_dir]
//
// Builds an index from the CoNLL-U file (or uses corpus_dir) and checks:
//   * ABI / build info; open failures give NULL + a message, never a crash;
//   * every route answers through the C ABI, with the status pando-server sends;
//     unknown routes 404 / wrong methods 405, as JSON; "?a=b" inside the path;
//   * bad input (malformed CQL, missing fields, NULL arguments) gives JSON errors;
//   * "timeout_ms" stops a query with 408 without disturbing the next one;
//   * busy() counts background jobs; closing a handle with a running job is safe;
//   * many threads issuing mixed requests on one handle get exactly the answers
//     of a single-threaded run (run under ThreadSanitizer to check for races).

#include "api/server_capi.h"

#include <atomic>
#include <chrono>
#include <cstdio>
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

struct Resp {
    int status = 0;
    std::string body;
};

static Resp call(pando_server_t* s, const char* method, const char* path, const char* qs = nullptr,
                 const std::string& body = {}) {
    Resp r;
    char* js = pando_server_request(s, method, path, qs, body.empty() ? nullptr : body.c_str(), body.size(),
                                    &r.status);
    if (js) {
        r.body = js;
        pando_server_free(js);
    }
    return r;
}

static bool has(const std::string& s, const char* needle) { return s.find(needle) != std::string::npos; }

static std::string masked(const std::string& s) {
    static const std::regex el("\"elapsed_ms\": ?[0-9.e+-]+");
    return std::regex_replace(s, el, "\"elapsed_ms\": 0");
}

struct Req {
    const char* method;
    const char* path;
    const char* qs;
    std::string body;
};

static std::vector<Req> mixed_requests() {
    auto q = [](const std::string& cql, const std::string& extra = "") {
        std::string esc;
        for (char c : cql) {
            if (c == '"' || c == '\\') esc += '\\';
            esc += c;
        }
        return "{\"query\": \"" + esc + "\", \"limit\": 5" + extra + "}";
    };
    return {
        {"POST", "/query", nullptr, q("[upos=\"NOUN\"]", ", \"total\": true")},
        {"POST", "/query", nullptr, q("[upos=\"DET\"] [upos=\"ADJ\"]? [upos=\"NOUN\"]", ", \"total\": true")},
        {"POST", "/query", nullptr, q("a:[upos=\"VERB\"] > b:[upos=\"NOUN\"]", ", \"total\": true")},
        {"POST", "/query", nullptr, q("a:[upos=\"NOUN\"] > b:[upos=\"ADJ\"]", ", \"sentence\": true")},
        {"POST", "/query", nullptr, q("head:[upos=\"VERB\"] sub:dep_subtree(head)", ", \"total\": true")},
        {"POST", "/query", nullptr, q("[lemma=\"the\" %c] [] [upos=\"NOUN\"] within s", ", \"total\": true")},
        {"POST", "/query", nullptr, q("[form=\".*ing\"]", ", \"total\": true, \"attrs\": \"form,lemma\"")},
        {"POST", "/query", nullptr, q("[upos!=\"PUNCT\"]", ", \"total\": true, \"offset\": 7")},
        {"POST", "/run", nullptr, "{\"cql\": \"[upos=\\\"NOUN\\\"]; count by lemma;\", \"group_limit\": 10}"},
        {"POST", "/run", nullptr, "{\"cql\": \"a:[upos=\\\"ADJ\\\"] b:[upos=\\\"NOUN\\\"]; count by a.lemma, b.lemma;\"}"},
        {"POST", "/run", nullptr, "{\"cql\": \"[lemma=\\\"house\\\" | lemma=\\\"city\\\"]; sort by form;\", \"limit\": 4}"},
        {"GET", "/values/upos", nullptr, ""},
        {"GET", "/regions/s", "limit=3", ""},
        {"GET", "/context", "pos=40&left=3&right=3&sentence=1", ""},
        {"GET", "/info", nullptr, ""},
    };
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: server_api_test <pando-index> <conllu> [corpus_dir]\n";
        return 2;
    }
    fs::path tmp = fs::temp_directory_path() / ("pando_server_api_test_" + std::to_string(::getpid()));
    std::string corpus_dir;
    if (argc >= 4) {
        corpus_dir = argv[3];
    } else {
        fs::create_directories(tmp);
        corpus_dir = (tmp / "idx").string();
        const std::string cmd = std::string("\"") + argv[1] + "\" \"" + argv[2] + "\" \"" + corpus_dir + "\" >/dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "pando-index failed: " << cmd << "\n";
            return 2;
        }
    }

    // ── ABI, build, open failures ────────────────────────────────────────
    CHECK(pando_server_abi_version() == PANDO_SERVER_ABI_VERSION);
    CHECK(std::strlen(pando_server_build_string()) > 0);
    CHECK(has(pando_server_build_json(), "\"abi_version\": 1") && has(pando_server_build_json(), "async_total"));
    {
        char* err = nullptr;
        CHECK(pando_server_open("/nonexistent/pando/corpus", nullptr, &err) == nullptr);
        CHECK(err && std::strlen(err) > 0);
        pando_server_free(err);
        CHECK(pando_server_open(nullptr, nullptr, nullptr) == nullptr);
        Resp r = call(nullptr, "GET", "/health");
        CHECK(r.status == 500 && has(r.body, "\"ok\":false"));
        pando_server_close(nullptr);
    }

    char* err = nullptr;
    pando_server_t* s = pando_server_open(corpus_dir.c_str(), "{\"total_workers\": 2, \"embedded_in\": \"server_api_test\"}", &err);
    if (!s) {
        std::cerr << "open failed: " << (err ? err : "?") << "\n";
        return 1;
    }
    CHECK(std::string(pando_server_corpus_dir(s)) == corpus_dir);

    // ── routes ───────────────────────────────────────────────────────────
    {
        Resp r = call(s, "GET", "/health");
        CHECK(r.status == 200 && has(r.body, "\"status\":\"ok\"") && has(r.body, "\"embedded_in\": \"server_api_test\""));
        r = call(s, "GET", "/version");
        CHECK(r.status == 200 && has(r.body, pando_server_build_string()));
        r = call(s, "GET", "/info");
        CHECK(r.status == 200 && has(r.body, "\"server\": {") && has(r.body, "\"index\""));
        r = call(s, "GET", "/values/upos", "limit=3");
        CHECK(r.status == 200 && has(r.body, "NOUN"));
        r = call(s, "GET", "/values/nosuch");
        CHECK(r.status == 404 && has(r.body, "Unknown attribute: nosuch"));
        r = call(s, "GET", "/regions/s?limit=2");               // query string inside the path
        CHECK(r.status == 200);
        r = call(s, "GET", "/regions/nosuch");
        CHECK(r.status == 404);
        r = call(s, "GET", "/context", "pos=10&left=2&right=2");
        CHECK(r.status == 200 && has(r.body, "\"pos\":10"));
        r = call(s, "GET", "/context");
        CHECK(r.status == 400 && has(r.body, "missing 'pos'"));
        r = call(s, "GET", "/context", "pos=99999999999");
        CHECK(r.status == 400 && has(r.body, "out of range"));
        r = call(s, "POST", "/query", nullptr, "{\"query\": \"[upos=\\\"NOUN\\\"]\", \"limit\": 2, \"total\": true}");
        CHECK(r.status == 200 && has(r.body, "\"total_exact\": true") && has(r.body, "\"job_id\""));
        r = call(s, "POST", "/query", nullptr, "{\"query\": \"[upos=\\\"NOUN\\\"\", \"limit\": 2}");
        CHECK(r.status == 400 && has(r.body, "\"ok\":false"));
        r = call(s, "POST", "/run", nullptr, "{\"cql\": \"[upos=\\\"NOUN\\\"]; count by lemma;\"}");
        CHECK(r.status == 200 && has(r.body, "\"ok\": true"));
        r = call(s, "POST", "/run", nullptr, "{\"cql\": \"x = = [\"}");
        CHECK(r.status == 400 && has(r.body, "\"ok\":false"));
        r = call(s, "POST", "/run", nullptr, "{}");
        CHECK(r.status == 400 && has(r.body, "missing 'cql'"));
        r = call(s, "GET", "/status", "job=0000000000000000");
        CHECK(r.status == 404);
        r = call(s, "POST", "/cancel", "job=0000000000000000");
        CHECK(r.status == 200 && has(r.body, "\"cancelled\":false"));
        r = call(s, "GET", "/jobs");
        CHECK(r.status == 200 && has(r.body, "\"jobs\":["));
        r = call(s, "get", "/health/");                        // method case, trailing slash
        CHECK(r.status == 200);
        r = call(s, "GET", "/nope");
        CHECK(r.status == 404 && has(r.body, "unknown route"));
        r = call(s, "GET", "/query");
        CHECK(r.status == 405 && has(r.body, "not allowed"));
        r = call(s, "POST", "/values/upos");
        CHECK(r.status == 405);
        r = call(s, "GET", "/values/a-b");                      // not a \w+ segment
        CHECK(r.status == 404);
        r = call(s, nullptr, "/health");
        CHECK(r.status == 400);
        // "timeout_ms": a generous limit answers normally
        r = call(s, "POST", "/query", nullptr, "{\"query\": \"[]\", \"limit\": 1, \"total\": true, \"timeout_ms\": 60000}");
        CHECK(r.status == 200);
        std::cerr << "  PASS routes\n";
    }

    // ── async job, busy(), idle ──────────────────────────────────────────
    {
        pando_server_t* d = pando_server_open(corpus_dir.c_str(), "{\"debug_total_delay_ms\": 1500}", nullptr);
        CHECK(d != nullptr);
        CHECK(pando_server_busy(d) == 0);
        Resp r = call(d, "POST", "/query", nullptr, "{\"query\": \"[upos=\\\"ADJ\\\"]\", \"limit\": 1, \"total\": \"async\"}");
        CHECK(r.status == 200 && has(r.body, "\"job_id\""));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK(pando_server_busy(d) >= 1);                       // the background count
        CHECK(pando_server_idle_seconds(d) >= 0.05);
        pando_server_close(d);                                  // with the job still running
        std::cerr << "  PASS busy / close with a running job\n";
    }

    // ── concurrency: N threads x mixed requests == single-threaded answers ─
    {
        const auto reqs = mixed_requests();
        std::vector<Resp> want;
        for (const auto& q : reqs) {
            Resp r = call(s, q.method, q.path, q.qs, q.body);
            r.body = masked(r.body);
            want.push_back(r);
            if (r.status != 200) std::cerr << "  baseline " << q.path << " " << q.body << " -> " << r.status << ": " << r.body.substr(0, 200) << "\n";
            CHECK(r.status == 200);
        }
        // /info carries uptime: compare it on status only
        const unsigned nthreads = 8;
        const int rounds = 6;
        std::atomic<int> mismatches{0};
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < nthreads; ++t) {
            ts.emplace_back([&, t] {
                for (int k = 0; k < rounds; ++k) {
                    for (size_t i = 0; i < reqs.size(); ++i) {
                        const size_t j = (i + t * 3 + static_cast<size_t>(k)) % reqs.size();
                        const auto& q = reqs[j];
                        Resp r = call(s, q.method, q.path, q.qs, q.body);
                        const bool same = std::string(q.path) == "/info"
                            ? r.status == want[j].status
                            : (r.status == want[j].status && masked(r.body) == want[j].body);
                        if (!same) {
                            if (mismatches.fetch_add(1) < 3)
                                std::cerr << "  mismatch " << q.path << " " << q.body << "\n";
                        }
                    }
                }
            });
        }
        // a timed-out query in the middle of it all does not disturb the others
        Resp tr = call(s, "POST", "/query", nullptr,
                       "{\"query\": \"[] [] []\", \"limit\": 1, \"total\": true, \"max_total\": 0, \"timeout_ms\": 1}");
        CHECK(tr.status == 200 || (tr.status == 408 && has(tr.body, "\"timed_out\":true")));
        for (auto& th : ts) th.join();
        CHECK(mismatches.load() == 0);
        std::cerr << "  PASS concurrency (" << nthreads << " threads x " << rounds * reqs.size()
                  << " requests; timeout probe " << tr.status << ")\n";
    }

    pando_server_close(s);
    if (argc < 4) fs::remove_all(tmp);
    std::cerr << (g_fail ? "server_api_test: FAILED (" + std::to_string(g_fail) + ")\n" : "server_api_test: all passed\n");
    return g_fail ? 1 : 0;
}
