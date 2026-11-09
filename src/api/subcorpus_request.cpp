#include "api/subcorpus_request.h"

#include "core/json_utils.h"
#include "corpus/corpus.h"

namespace pando {

std::optional<SubcorpusRequest> take_subcorpus_request(const std::string& body, std::string& body_out) {
    body_out = body;
    const std::string raw = json_extract_raw(body, "subcorpus");
    if (raw.empty() || raw == "null") return std::nullopt;
    body_out = json_without_member(body, "subcorpus");
    SubcorpusRequest r;
    if (raw[0] == '"') {
        r.definition = json_decode_string_at(raw, 0);
    } else if (raw[0] == '{') {
        for (const auto& [k, v] : json_object_members(raw)) {
            if (v.empty() || v == "null") continue;
            if (v[0] != '"')
                throw SubcorpusError("\"subcorpus\": \"" + k + "\" must be a string");
            const std::string s = json_decode_string_at(v, 0);
            if (k == "definition") r.definition = s;
            else if (k == "mode") r.mode = s;
            else if (k == "name") r.name = s;
            else throw SubcorpusError("\"subcorpus\": unknown member \"" + k + "\" (definition, mode, name)");
        }
    } else {
        throw SubcorpusError("\"subcorpus\" must be an object {\"definition\", \"mode\", \"name\"} or a definition string");
    }
    if (r.mode.empty()) r.mode = "use";
    if (r.mode != "use" && r.mode != "within")
        throw SubcorpusError("\"subcorpus\": mode is \"use\" or \"within\", not \"" + r.mode + "\"");
    if (r.definition.empty()) throw SubcorpusError("\"subcorpus\": the definition is missing");
    return r;
}

AppliedSubcorpusPtr applied_subcorpus(const Corpus& corpus, const SubcorpusRequest& req,
                                      const SubcorpusResolveOptions& opts) {
    auto a = std::make_shared<AppliedSubcorpus>();
    a->name = req.name;
    a->mode = req.mode;
    a->sc = resolve_subcorpus(corpus, req.definition, opts);
    return a;
}

std::string subcorpus_route_json(const Corpus& corpus, const std::string& body, const SubcorpusResolveOptions& opts) {
    std::string def = json_extract_str(body, "definition");
    if (def.empty()) def = json_extract_str(body, "subcorpus");
    if (def.empty()) throw SubcorpusError("missing 'definition'");
    ResolvedSubcorpusPtr sc = resolve_subcorpus(corpus, def, opts);
    const PositionSet& s = sc->set();
    return "{\"ok\": true, \"operation\": \"subcorpus\", \"tokens\": " + std::to_string(s.tokens())
        + ", \"ranges\": " + std::to_string(s.runs()) + ", \"encoding\": " + jstr(s.encoding_name())
        + ", \"index_id\": " + jstr(sc->index_id()) + ", \"corpus_size\": " + std::to_string(corpus.size())
        + ", \"definition\": " + jstr(def) + "}\n";
}

} // namespace pando
