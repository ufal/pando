#include "core/build_info.h"

namespace pando {

static std::string json_quote(const std::string& s) {
    std::string o = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\') o += '\\';
        o += c;
    }
    return o + "\"";
}

std::string build_string() {
    const std::string v = build_version(), d = build_describe(), b = build_branch();
    if (d == v || d == "v" + v) return v;
    std::string s = v + " (" + d;
    if (!b.empty()) s += ", " + b;
    return s + ")";
}

const char* build_regex_engine() {
#ifdef PANDO_USE_RE2
    return "re2";
#else
    return "std";
#endif
}

std::string build_json_fields() {
    return "\"version\": " + json_quote(build_version()) + ", \"build\": " + json_quote(build_describe())
        + ", \"commit\": " + json_quote(build_commit()) + ", \"branch\": " + json_quote(build_branch())
        + ", \"regex\": " + json_quote(build_regex_engine());
}

}  // namespace pando
