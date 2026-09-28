#pragma once

// Per-request resource limits for pando-server / ServerApi, in tiers.
//
// A host (FQS, or KonText in front of pando-server) knows who is asking and
// puts the request in a tier — e.g. "visitor" (not logged in), "user",
// "admin" (a corpus administrator) — by sending "tier": "<name>" in the
// request body. ServerApi only honours that field when the host is trusted
// to set it (ServerConfig::trust_tier); otherwise, and for requests without a
// tier, the default tier applies. What a tier limits:
//
//   timeout_ms        a /query or /run stops with 408 after this long (a request's
//                     own "timeout_ms" can only lower it)
//   total_timeout_ms  a background ("async") count stops after this long
//   max_count_hits    count / group / freq / stats / coll / dcoll / keyness /
//                     tabulate / describe / sort over more hits → 413 (the hits
//                     are counted first, up to the limit, before anything is built)
//   max_hits          hits one stored set may materialise (sessions, sorted pages)
//   threads           position ranges a counting query is split over (P4.1)
//   deny              features refused with 403: "transitive" (>> <<, descendant /
//                     ancestor conditions), "unbounded_repeat" (+ * {n,}),
//                     "regex_no_prefix" (a regex that must scan the whole lexicon:
//                     no literal start), "regex" (any), "parallel" (aligned queries),
//                     "negated_relation" (!> !<)
//
// 0 / empty = no limit from the tier (the server-wide setting applies).

#include "query/ast.h"

#include <cstddef>
#include <string>
#include <vector>

namespace pando {

struct QueryLimits {
    size_t timeout_ms = 0;
    size_t total_timeout_ms = 0;
    size_t max_count_hits = 0;
    size_t max_hits = 0;
    unsigned threads = 0;
    std::vector<std::string> deny;
};

/// A tier object `{"timeout_ms": 30000, "deny": ["transitive"], …}`. Unknown
/// members are ignored; unknown deny features are kept (and never match).
QueryLimits parse_query_limits(const std::string& obj_json);
/// The same object, for /health.
std::string query_limits_json(const QueryLimits& l);
/// The deny features this build knows.
const std::vector<std::string>& limit_features();
/// The first feature in `deny` that `prog` uses, or "" when none does.
std::string denied_feature(const Program& prog, const std::vector<std::string>& deny);

/// A request's timeout: the tier cap (or, without one, `server_default`), lowered
/// by the request's own value; 0 = none.
size_t capped_timeout(size_t cap, size_t server_default, size_t requested);

}  // namespace pando
