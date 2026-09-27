// server_capi.cpp — C ABI over ServerApi (see server_capi.h).

#include "api/server_capi.h"

#include "api/server_api.h"
#include "core/build_info.h"
#include "core/json_utils.h"
#include "corpus/corpus.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <string>

using namespace pando;

struct pando_server {
    std::unique_ptr<Corpus> corpus;
    std::unique_ptr<ServerApi> api;   // destroyed before the corpus
    std::string dir;
    ~pando_server() { api.reset(); }
};

namespace {

char* dup_c(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (!p) return nullptr;
    std::memcpy(p, s.data(), s.size());
    p[s.size()] = '\0';
    return p;
}

char* error_body(int status, const std::string& msg, int* status_out) {
    if (status_out) *status_out = status;
    return dup_c("{\"ok\":false,\"error\":\"" + json_escape(msg) + "\"}\n");
}

bool has_key(const std::string& json, const char* key) {
    return json.find(std::string("\"") + key + "\"") != std::string::npos;
}

}  // namespace

extern "C" {

PANDO_API int pando_server_abi_version(void) { return PANDO_SERVER_ABI_VERSION; }

PANDO_API const char* pando_server_build_string(void) {
    static const std::string s = build_string();
    return s.c_str();
}

PANDO_API const char* pando_server_build_json(void) {
    static const std::string s = [] {
        std::string f = "[";
        const auto& feats = ServerApi::features();
        for (size_t i = 0; i < feats.size(); ++i) f += std::string(i ? ", " : "") + "\"" + feats[i] + "\"";
        f += "]";
        return "{" + build_json_fields() + ", \"build_string\": " + jstr(build_string())
               + ", \"abi_version\": " + std::to_string(PANDO_SERVER_ABI_VERSION) + ", \"features\": " + f + "}";
    }();
    return s.c_str();
}

PANDO_API pando_server_t* pando_server_open(const char* corpus_dir, const char* options_json, char** error_out) {
    if (error_out) *error_out = nullptr;
    auto fail = [&](const std::string& msg) -> pando_server_t* {
        if (error_out) *error_out = dup_c(msg);
        return nullptr;
    };
    if (!corpus_dir || !*corpus_dir) return fail("pando_server_open: no corpus directory");
    try {
        const std::string opts = options_json ? options_json : "";
        ServerConfig cfg;
        cfg.preload = json_extract_bool(opts, "preload", false);
        if (has_key(opts, "total_workers"))
            cfg.jobs.workers = static_cast<unsigned>(std::max<size_t>(1, json_extract_num(opts, "total_workers", 2)));
        if (has_key(opts, "result_cache"))
            cfg.jobs.max_entries = std::max<size_t>(1, json_extract_num(opts, "result_cache", 512));
        if (has_key(opts, "result_ttl"))
            cfg.jobs.ttl = std::chrono::seconds(json_extract_num(opts, "result_ttl", 3600));
        if (has_key(opts, "abandon_after"))
            cfg.jobs.abandon = std::chrono::seconds(json_extract_num(opts, "abandon_after", 120));
        cfg.query_timeout_ms = json_extract_num(opts, "query_timeout_ms", 0);
        cfg.jobs.debug_delay = std::chrono::milliseconds(json_extract_num(opts, "debug_total_delay_ms", 0));
        cfg.threads = static_cast<unsigned>(json_extract_num(opts, "threads", 0));
        const std::string emb = json_extract_str(opts, "embedded_in");
        if (!emb.empty()) cfg.extra_server_fields = "\"embedded_in\": " + jstr(emb);

        auto s = std::make_unique<pando_server>();
        s->dir = corpus_dir;
        s->corpus = std::make_unique<Corpus>();
        s->corpus->open(s->dir, cfg.preload);
        s->api = std::make_unique<ServerApi>(*s->corpus, std::move(cfg));
        return s.release();
    } catch (const std::exception& e) {
        return fail(std::string("cannot open corpus at ") + corpus_dir + ": " + e.what());
    } catch (...) {
        return fail(std::string("cannot open corpus at ") + corpus_dir);
    }
}

PANDO_API char* pando_server_request(pando_server_t* s, const char* method, const char* path,
                                     const char* query_string, const char* body, size_t body_len,
                                     int* status_out) {
    try {
        if (!s || !s->api) return error_body(500, "pando_server_request: no handle", status_out);
        if (!method || !path) return error_body(400, "pando_server_request: method and path are required", status_out);
        std::string p = path;
        std::string qs = query_string ? query_string : "";
        if (!query_string) {
            const size_t q = p.find('?');
            if (q != std::string::npos) {
                qs = p.substr(q + 1);
                p.resize(q);
            }
        }
        const std::string b = body ? std::string(body, body_len ? body_len : std::strlen(body)) : std::string();
        ServerResponse r = s->api->handle(method, p, parse_query_string(qs), b);
        if (status_out) *status_out = r.status;
        return dup_c(r.body);
    } catch (const std::exception& e) {
        return error_body(500, std::string("internal error: ") + e.what(), status_out);
    } catch (...) {
        return error_body(500, "internal error", status_out);
    }
}

PANDO_API size_t pando_server_busy(pando_server_t* s) {
    try {
        return (s && s->api) ? s->api->busy() : 0;
    } catch (...) {
        return 1;   // unknown: treat as busy
    }
}

PANDO_API double pando_server_idle_seconds(pando_server_t* s) {
    return (s && s->api) ? s->api->idle_seconds() : 0.0;
}

PANDO_API const char* pando_server_corpus_dir(pando_server_t* s) {
    return s ? s->dir.c_str() : "";
}

PANDO_API void pando_server_close(pando_server_t* s) {
    try {
        delete s;
    } catch (...) {
    }
}

PANDO_API void pando_server_free(char* p) { std::free(p); }

}  // extern "C"
