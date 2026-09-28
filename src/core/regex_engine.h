#pragma once

// One compiled regex for attribute values (`[lemma=".*ness"]`, `= /…/`).
//
// Built with RE2 (PANDO_USE_RE2), a pattern is compiled with RE2 in UTF-8 mode —
// `.` is one character (not one byte), `(?i)` works, matching is linear-time
// and 3-4x faster over a lexicon — and falls back to std::regex (ECMAScript)
// only for the constructs RE2 does not support (backreferences, lookaround),
// so every pattern that worked before still works. Without RE2, std::regex
// (a leading `(?i)` becomes its icase flag, so `(?i)` works with both engines).
//
// Matching is const and thread-safe for both engines.

#include <memory>
#include <string>
#include <string_view>

#ifdef PANDO_USE_RE2
namespace re2 { class RE2; }
#endif
#include <regex>

namespace pando {

class Regex {
public:
    /// Throws std::runtime_error when the pattern compiles in neither engine.
    explicit Regex(const std::string& pattern);
    ~Regex();
    Regex(const Regex&) = delete;
    Regex& operator=(const Regex&) = delete;

    /// `full`: the whole value must match (CQL `[attr="…"]`); else a partial match.
    bool match(std::string_view value, bool full) const;
    /// "re2" or "std" — which engine this pattern uses.
    const char* engine() const;

private:
#ifdef PANDO_USE_RE2
    std::unique_ptr<re2::RE2> re2_;
#endif
    std::unique_ptr<std::regex> std_;
};

/// Which engine the build prefers ("re2" / "std"), for build info.
const char* regex_engine_name();

}  // namespace pando
