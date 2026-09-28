#include "core/regex_engine.h"

#include <mutex>
#include <stdexcept>

#ifdef PANDO_USE_RE2
#include <re2/re2.h>
#endif

namespace pando {

Regex::Regex(const std::string& pattern) {
#ifdef PANDO_USE_RE2
    RE2::Options o;
    o.set_log_errors(false);   // unsupported constructs fall back to std::regex quietly
    o.set_encoding(RE2::Options::EncodingUTF8);
    auto r = std::make_unique<re2::RE2>(pattern, o);
    if (r->ok()) {
        re2_ = std::move(r);
        return;
    }
#endif
    // libstdc++'s ctype<char>::narrow fills a lazy cache while std::regex compiles
    // (GCC PR 77704: a benign race, but a race): compile one pattern at a time
    static std::mutex std_compile_mu;
    std::lock_guard<std::mutex> lock(std_compile_mu);
    try {
        // std::regex (ECMAScript) has no inline flags; a leading (?i) — what KonText
        // sends for case-insensitive queries, and what RE2 understands — is its icase
        if (pattern.rfind("(?i)", 0) == 0)
            std_ = std::make_unique<std::regex>(pattern.substr(4), std::regex::ECMAScript | std::regex::icase);
        else
            std_ = std::make_unique<std::regex>(pattern);
    } catch (const std::regex_error& e) {
        throw std::runtime_error("invalid regular expression /" + pattern + "/: " + e.what());
    }
}

Regex::~Regex() = default;

bool Regex::match(std::string_view value, bool full) const {
#ifdef PANDO_USE_RE2
    if (re2_) return full ? RE2::FullMatch(value, *re2_) : RE2::PartialMatch(value, *re2_);
#endif
    if (full) return std::regex_match(value.begin(), value.end(), *std_);
    return std::regex_search(value.begin(), value.end(), *std_);
}

const char* Regex::engine() const {
#ifdef PANDO_USE_RE2
    if (re2_) return "re2";
#endif
    return "std";
}

const char* regex_engine_name() {
#ifdef PANDO_USE_RE2
    return "re2";
#else
    return "std";
#endif
}

}  // namespace pando
