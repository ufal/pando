/*
 * pando_ffi.cpp — C API implementation for libpando
 */

#include "api/pando_ffi.h"
#include "api/query_json.h"
#include "corpus/corpus.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <memory>

// ── Tiny JSON helpers (no dependency on a JSON lib) ──────────────────────

namespace {

// Extract a string value for a given key from a flat JSON object.
// Handles "key": "value" and "key": number/true/false.
// Returns empty string if key not found.
std::string json_get(const std::string& json, const std::string& key) {
    std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return {};
    pos += needle.size();
    // Skip whitespace and colon
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':' || json[pos] == '\t'))
        ++pos;
    if (pos >= json.size()) return {};
    if (json[pos] == '"') {
        // String value
        ++pos;
        auto end = json.find('"', pos);
        if (end == std::string::npos) return {};
        return json.substr(pos, end - pos);
    }
    // Numeric or boolean value
    auto end = json.find_first_of(",} \t\n\r", pos);
    if (end == std::string::npos) end = json.size();
    return json.substr(pos, end - pos);
}

// Extract a JSON array of strings: "key": ["a", "b"]
std::vector<std::string> json_get_string_array(const std::string& json, const std::string& key) {
    std::vector<std::string> result;
    std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return result;
    pos += needle.size();
    while (pos < json.size() && json[pos] != '[') ++pos;
    if (pos >= json.size()) return result;
    ++pos; // skip '['
    while (pos < json.size() && json[pos] != ']') {
        if (json[pos] == '"') {
            ++pos;
            auto end = json.find('"', pos);
            if (end == std::string::npos) break;
            result.push_back(json.substr(pos, end - pos));
            pos = end + 1;
        } else {
            ++pos;
        }
    }
    return result;
}

// Duplicate a std::string onto the heap as a C string.
char* to_c_string(const std::string& s) {
    char* out = static_cast<char*>(std::malloc(s.size() + 1));
    if (out) {
        std::memcpy(out, s.data(), s.size());
        out[s.size()] = '\0';
    }
    return out;
}

// Parse QueryOptions from an opts_json string (or defaults if null/empty).
pando::QueryOptions parse_query_opts(const char* opts_json) {
    pando::QueryOptions opts;
    if (!opts_json || opts_json[0] == '\0') return opts;
    std::string j(opts_json);
    auto v = json_get(j, "limit");   if (!v.empty()) try { opts.limit    = std::stoull(v); } catch (...) {}
    v = json_get(j, "offset");       if (!v.empty()) try { opts.offset   = std::stoull(v); } catch (...) {}
    v = json_get(j, "max_total");    if (!v.empty()) try { opts.max_total = std::stoull(v); } catch (...) {}
    v = json_get(j, "context");      if (!v.empty()) try { opts.context  = std::stoi(v); }  catch (...) {}
    v = json_get(j, "total");        if (v == "true" || v == "1") opts.total = true;
    v = json_get(j, "debug");        if (v == "true" || v == "1") opts.debug = true;
    v = json_get(j, "strict_quoted_strings");
    if (v == "true" || v == "1")
        opts.strict_quoted_strings = true;
    opts.attrs = json_get_string_array(j, "attrs");
    return opts;
}

// Parse ProgramOptions from an opts_json string.
pando::ProgramOptions parse_program_opts(const char* opts_json) {
    pando::ProgramOptions opts;
    if (!opts_json || opts_json[0] == '\0') return opts;
    std::string j(opts_json);
    auto v = json_get(j, "limit");      if (!v.empty()) try { opts.limit    = std::stoull(v); } catch (...) {}
    v = json_get(j, "offset");          if (!v.empty()) try { opts.offset   = std::stoull(v); } catch (...) {}
    v = json_get(j, "max_total");       if (!v.empty()) try { opts.max_total = std::stoull(v); } catch (...) {}
    v = json_get(j, "context");         if (!v.empty()) try { opts.context  = std::stoi(v); }  catch (...) {}
    v = json_get(j, "total");           if (v == "true" || v == "1") opts.total = true;
    v = json_get(j, "strict_quoted_strings");
    if (v == "true" || v == "1")
        opts.strict_quoted_strings = true;
    v = json_get(j, "group_limit");     if (!v.empty()) try { opts.group_limit = std::stoull(v); } catch (...) {}
    v = json_get(j, "coll_left");       if (!v.empty()) try { opts.coll_left = std::stoi(v); } catch (...) {}
    v = json_get(j, "coll_right");      if (!v.empty()) try { opts.coll_right = std::stoi(v); } catch (...) {}
    v = json_get(j, "coll_min_freq");   if (!v.empty()) try { opts.coll_min_freq = std::stoull(v); } catch (...) {}
    v = json_get(j, "coll_max_items");  if (!v.empty()) try { opts.coll_max_items = std::stoull(v); } catch (...) {}
    opts.attrs = json_get_string_array(j, "attrs");
    opts.coll_measures = json_get_string_array(j, "measures");
    return opts;
}

struct PandoHandle {
    pando::Corpus corpus;
    pando::ProgramSession session;
    /// progress / cancel block of the call running on this handle (pando_cancel)
    pando::ExecProgress progress;
};

/// why the last pando_open on this thread failed (pando_last_error)
thread_local std::string g_last_error;

std::string json_escape(const std::string& s) {
    std::string o;
    o.reserve(s.size() + 2);
    for (unsigned char c : s) {
        switch (c) {
            case '"':  o += "\\\""; break;
            case '\\': o += "\\\\"; break;
            case '\n': o += "\\n"; break;
            case '\r': o += "\\r"; break;
            case '\t': o += "\\t"; break;
            default:
                if (c < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o += b; }
                else o += static_cast<char>(c);
        }
    }
    return o;
}

char* error_json(const std::string& msg, bool cancelled = false) {
    std::string j = "{\"ok\":false,\"error\":\"" + json_escape(msg) + "\"";
    if (cancelled) j += ",\"cancelled\":true";
    j += "}";
    return to_c_string(j);
}

void reset_progress(pando::ExecProgress& p) {
    p.cancel.store(false);
    p.counted.store(0);
    p.scanned.store(-1);
}

} // namespace

// ── C API implementation ─────────────────────────────────────────────────

extern "C" {

pando_handle_t pando_open(const char* corpus_dir, int preload) {
    g_last_error.clear();
    if (!corpus_dir) { g_last_error = "corpus_dir is NULL"; return nullptr; }
    try {
        auto h = std::make_unique<PandoHandle>();
        h->corpus.open(corpus_dir, preload != 0);
        return h.release();
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return nullptr;
    } catch (...) {
        g_last_error = "unknown error opening the corpus";
        return nullptr;
    }
}

const char* pando_last_error(void) {
    return g_last_error.c_str();
}

void pando_cancel(pando_handle_t handle) {
    if (!handle) return;
    static_cast<PandoHandle*>(handle)->progress.cancel.store(true);
}

char* pando_query(pando_handle_t handle, const char* cql, const char* opts_json) {
    if (!handle || !cql) return nullptr;
    try {
        auto* h = static_cast<PandoHandle*>(handle);
        auto opts = parse_query_opts(opts_json);
        reset_progress(h->progress);
        auto [ms, elapsed] = pando::run_single_query(h->corpus, cql, opts, &h->progress);
        std::string json = pando::to_query_result_json(h->corpus, cql, ms, opts, elapsed);
        return to_c_string(json);
    } catch (const pando::QueryCancelled&) {
        return error_json("query cancelled", true);
    } catch (const std::exception& e) {
        return error_json(e.what());
    } catch (...) {
        return error_json("query execution failed");
    }
}

char* pando_run(pando_handle_t handle, const char* cql, const char* opts_json) {
    if (!handle || !cql) return nullptr;
    try {
        auto* h = static_cast<PandoHandle*>(handle);
        auto opts = parse_program_opts(opts_json);
        reset_progress(h->progress);
        opts.progress = &h->progress;
        std::string json = pando::run_program_json(h->corpus, h->session, cql, opts);
        return to_c_string(json);
    } catch (const pando::QueryCancelled&) {
        return error_json("query cancelled", true);
    } catch (const std::exception& e) {
        return error_json(e.what());
    } catch (...) {
        return error_json("program execution failed");
    }
}

char* pando_info(pando_handle_t handle) {
    if (!handle) return nullptr;
    try {
        auto* h = static_cast<PandoHandle*>(handle);
        std::string json = pando::to_info_json(h->corpus);
        return to_c_string(json);
    } catch (...) {
        return to_c_string("{\"ok\":false,\"error\":\"info failed\"}");
    }
}

void pando_free_string(char* s) {
    std::free(s);
}

void pando_close(pando_handle_t handle) {
    if (!handle) return;
    delete static_cast<PandoHandle*>(handle);
}

} // extern "C"
