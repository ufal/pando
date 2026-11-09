#pragma once

// EF-12: the per-request subcorpus of /query, /run (and the C APIs, which pass the same JSON)
// and the POST /subcorpus route.
//
//   "subcorpus": {"definition": "<text>", "mode": "use" | "within" (default "use"), "name": "<optional>"}
//   (or "subcorpus": "<definition>", mode "use")
//
// Answers carry "subcorpus": {"name", "mode", "tokens", "ranges", "encoding"} (subcorpus.h).
// POST /subcorpus {"definition": "…"} resolves (and caches) a definition and answers
// {"ok": true, "operation": "subcorpus", "tokens", "ranges", "encoding", "index_id"}.

#include "subcorpus/subcorpus.h"

#include <optional>
#include <string>

namespace pando {

class Corpus;

struct SubcorpusRequest {
    std::string definition;
    std::string mode = "use";
    std::string name;
};

/// The request's "subcorpus" (nullopt when absent or null); `body_out` = the body without it,
/// so that flat lookups of the other members (`name`, …) cannot hit its members. Throws
/// SubcorpusError for a malformed member or an unknown mode.
std::optional<SubcorpusRequest> take_subcorpus_request(const std::string& body, std::string& body_out);

/// Resolve a request's subcorpus (cached; names through opts.lookup).
AppliedSubcorpusPtr applied_subcorpus(const Corpus& corpus, const SubcorpusRequest& req,
                                      const SubcorpusResolveOptions& opts = {});

/// The answer of POST /subcorpus. Throws SubcorpusError (missing or invalid definition).
std::string subcorpus_route_json(const Corpus& corpus, const std::string& body, const SubcorpusResolveOptions& opts = {});

} // namespace pando
