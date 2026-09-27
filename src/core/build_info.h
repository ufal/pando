#pragma once

#include <string>

namespace pando {

// Build identity, generated on every build by cmake/build_info.cmake.
const char* build_version();    // CMake project VERSION, e.g. "0.1.20"
const char* build_describe();   // `git describe --tags --always --dirty`, e.g.
                                // "v0.1.20-23-g2a000ce-dirty"; the version outside git
const char* build_commit();     // full commit hash, or "unknown"
const char* build_branch();     // branch name; "" when detached / unknown

/// "0.1.20" for a clean tagged build, else "0.1.20 (v0.1.20-23-g2a000ce, perf/phase1)".
std::string build_string();

/// `"version": "…", "build": "…", "commit": "…", "branch": "…"` (JSON members, no braces).
std::string build_json_fields();

}  // namespace pando
