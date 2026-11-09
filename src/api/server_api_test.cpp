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

    // an index with a layout this build does not read is refused with what to do (EF-6)
    {
        const fs::path copy = tmp / "layout";
        fs::remove_all(copy);
        fs::create_directories(tmp);
        // with modification times (fs::copy gives new ones, and derived files would look stale)
        const std::string cp = "cp -Rp \"" + corpus_dir + "\" \"" + copy.string() + "\"";
        CHECK(std::system(cp.c_str()) == 0);
        auto open_with = [&](const std::string& layout) {
            const fs::path info = copy / "corpus.info";
            const auto mtime = fs::last_write_time(info);   // kept: derived files are checked against it
            std::FILE* f = std::fopen(info.string().c_str(), "a");
            std::fprintf(f, "layout_version=%s\n", layout.c_str());   // the last line wins
            std::fclose(f);
            fs::last_write_time(info, mtime);
            char* e = nullptr;
            pando_server_t* h = pando_server_open(copy.string().c_str(), nullptr, &e);
            std::string msg = e ? e : "";
            pando_server_free(e);
            pando_server_close(h);
            return std::make_pair(h != nullptr, msg);
        };
        auto [newer_ok, newer] = open_with("99");
        CHECK(!newer_ok && has(newer, "layout version 99") && has(newer, "use a newer pando"));
        auto [older_ok, older] = open_with("0");
        CHECK(!older_ok && has(older, "reindex needed"));
        auto [cur_ok, cur] = open_with("1");
        CHECK(cur_ok && cur.empty());
        fs::remove_all(copy);
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
        CHECK(r.status == 200 && has(r.body, "\"server\": {") && has(r.body, "\"index\"")
              && has(r.body, "\"layout_version\": 1"));
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
        CHECK(!has(r.body, "\"regions\""));                     // only with regions=1 (additive)
        r = call(s, "GET", "/context", "pos=10&left=2&right=2&regions=1");
        CHECK(r.status == 200 && has(r.body, "\"regions\":{") && has(r.body, "\"sent_id\":\"sample-en_p1-s1\""));
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

    // ── /run pages: sentence, fragment (EF-2); cat (EF-3) ────────────────
    {
        // the match_start of every hit on a page, in order
        auto starts = [](const std::string& body) {
            std::vector<long> v;
            static const std::regex re("\"match_start\": ?([0-9]+)");
            for (auto it = std::sregex_iterator(body.begin(), body.end(), re); it != std::sregex_iterator(); ++it)
                v.push_back(std::stol((*it)[1]));
            return v;
        };
        auto lefts = [](const std::string& body) {
            std::vector<std::string> v;
            static const std::regex re("\"left\": ?\"((?:[^\"\\\\]|\\\\.)*)\"");
            for (auto it = std::sregex_iterator(body.begin(), body.end(), re); it != std::sregex_iterator(); ++it)
                v.push_back((*it)[1]);
            return v;
        };
        const std::string nouns = "[upos=\\\"NOUN\\\"]";

        // a sorted page without the options: as before (no fragment, the token window)
        Resp plain = call(s, "POST", "/run", nullptr,
                          "{\"cql\": \"A = " + nouns + "; sort A by lemma;\", \"limit\": 3, \"context\": 1}");
        CHECK(plain.status == 200 && !has(plain.body, "\"fragment\"") && starts(plain.body).size() == 3);
        // with them: the same hits, the sentence as context, each with its fragment
        Resp rich = call(s, "POST", "/run", nullptr,
                         "{\"cql\": \"A = " + nouns + "; sort A by lemma;\", \"limit\": 3, \"context\": 1, "
                         "\"sentence\": true, \"fragment\": true}");
        CHECK(rich.status == 200 && has(rich.body, "\"fragment\"") && has(rich.body, "\"highlight_map\""));
        CHECK(starts(rich.body) == starts(plain.body));
        {
            const auto lp = lefts(plain.body), lr = lefts(rich.body);
            CHECK(lp.size() == 3 && lr.size() == 3);
            size_t longer = 0;
            for (size_t i = 0; i < lp.size() && i < lr.size(); ++i) longer += lr[i].size() > lp[i].size();
            CHECK(longer > 0);   // one-token windows grew to their sentences
        }
        // a plain query page of a program carries them too
        Resp qpage = call(s, "POST", "/run", nullptr,
                          "{\"cql\": \"" + nouns + "\", \"limit\": 2, \"fragment\": true}");
        CHECK(qpage.status == 200 && has(qpage.body, "\"fragment\""));

        // cat: the set in corpus order = the query's own page
        Resp q = call(s, "POST", "/query", nullptr, "{\"query\": \"" + nouns + "\", \"limit\": 4}");
        Resp c = call(s, "POST", "/run", nullptr, "{\"cql\": \"B = " + nouns + "; cat B\", \"limit\": 4}");
        CHECK(c.status == 200 && !has(c.body, "not yet implemented") && has(c.body, "\"operation\": \"query\""));
        CHECK(starts(c.body).size() == 4 && starts(c.body) == starts(q.body));
        // cat after sort: the sorted order, paged by offset / limit
        Resp sorted = call(s, "POST", "/run", nullptr,
                           "{\"cql\": \"C = " + nouns + "; sort C by lemma;\", \"limit\": 3, \"offset\": 2}");
        Resp cs = call(s, "POST", "/run", nullptr,
                       "{\"cql\": \"C = " + nouns + "; sort C by lemma; cat C\", \"limit\": 3, \"offset\": 2}");
        CHECK(starts(cs.body).size() == 3 && starts(cs.body) == starts(sorted.body));
        // the CQP window `cat C 2 4` = hits 2..4, whatever the limit
        Resp cw = call(s, "POST", "/run", nullptr,
                       "{\"cql\": \"C = " + nouns + "; sort C by lemma; cat C 2 4\", \"limit\": 20}");
        CHECK(starts(cw.body) == starts(sorted.body));
        CHECK(has(cw.body, "\"start\": 2"));
        // `cat` alone: Last
        Resp cl = call(s, "POST", "/run", nullptr, "{\"cql\": \"" + nouns + "; cat\", \"limit\": 4}");
        CHECK(starts(cl.body) == starts(q.body));
        // with sentence / fragment
        Resp cf = call(s, "POST", "/run", nullptr,
                       "{\"cql\": \"C = " + nouns + "; sort C by lemma; cat C 2 4\", \"fragment\": true}");
        CHECK(has(cf.body, "\"fragment\"") && starts(cf.body) == starts(sorted.body));
        // a window that ends before it starts is an error
        Resp bad = call(s, "POST", "/run", nullptr, "{\"cql\": \"C = " + nouns + "; cat C 4 2\"}");
        CHECK(bad.status == 400 && has(bad.body, "ends before it starts"));
        // the same as /query "from" in a session
        Resp sess = call(s, "POST", "/session", nullptr, "{}");
        std::smatch m;
        const std::string sb = sess.body;
        CHECK(std::regex_search(sb, m, std::regex("\"session_id\": ?\"([^\"]+)\"")));
        if (m.size() > 1) {
            const std::string sid = m[1];
            Resp r1 = call(s, "POST", "/run", nullptr,
                           "{\"session_id\": \"" + sid + "\", \"cql\": \"D = " + nouns + "; sort D by lemma; cat D 2 4\"}");
            Resp r2 = call(s, "POST", "/query", nullptr,
                           "{\"session_id\": \"" + sid + "\", \"from\": \"D\", \"offset\": 2, \"limit\": 3}");
            CHECK(r1.status == 200 && r2.status == 200 && starts(r1.body) == starts(r2.body)
                  && starts(r1.body) == starts(sorted.body));
            call(s, "POST", "/session/close", nullptr, "{\"session_id\": \"" + sid + "\"}");
        }
        std::cerr << "  PASS /run pages (sentence, fragment, cat)\n";
    }

    // ── cancelling a running request: "request_id" + POST /cancel {"request"} (EF-13) ──
    {
        Resp none = call(s, "POST", "/cancel", nullptr, "{\"request\": \"no-such-request\"}");
        CHECK(none.status == 200 && has(none.body, "\"cancelled\":false"));
        // ~30 s to count on the sample corpus: only a cancel ends it in time
        const std::string slow = "\"query\": \"[]{1,60} []{0,60} []{0,60}\", \"limit\": 1, \"total\": true";
        for (const char* route : {"/query", "/run"}) {
            const bool is_run = std::string(route) == "/run";
            const std::string body = is_run
                ? "{\"cql\": \"[]{1,60} []{0,60} []{0,60}; size\", \"request_id\": \"cx-run\"}"
                : "{" + slow + ", \"request_id\": \"cx-query\"}";
            const char* rid = is_run ? "cx-run" : "cx-query";
            Resp slow_r;
            const auto t0 = std::chrono::steady_clock::now();
            std::thread t([&] { slow_r = call(s, "POST", route, nullptr, body); });
            bool cancelled = false, busy_checked = false;
            for (int i = 0; i < 200 && !cancelled; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                if (!busy_checked) {   // the same id while it runs: refused
                    Resp dup = call(s, "POST", route, nullptr, body);
                    if (dup.status == 409) busy_checked = true;
                }
                Resp c = call(s, "POST", "/cancel", ("request=" + std::string(rid)).c_str());
                cancelled = has(c.body, "\"cancelled\":true");
            }
            t.join();
            const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
            CHECK(cancelled && busy_checked);
            CHECK(slow_r.status == 499 && has(slow_r.body, "\"cancelled\":true") && has(slow_r.body, rid));
            CHECK(secs < 15);
            // the id is free again, and the handle answers normally
            Resp after = call(s, "POST", "/query", nullptr,
                              "{\"query\": \"[upos=\\\"NOUN\\\"]\", \"limit\": 1, \"request_id\": \"" + std::string(rid) + "\"}");
            CHECK(after.status == 200);
        }
        std::cerr << "  PASS cancel by request_id\n";
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
