#pragma once

// Shared JSON helpers used by query_main.cpp, query_json.cpp, and server_main.cpp.

#include "core/types.h"
#include "query/executor.h"
#include "corpus/corpus.h"
#include <string>
#include <string_view>
#include <cstdio>

namespace pando {

inline std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 4);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

inline std::string jstr(std::string_view s) {
    return "\"" + json_escape(s) + "\"";
}

/// `describe` output: omit `""` always; omit `_` except on `form`/`lemma` (may be `_`).
inline bool describe_emit_attr(std::string_view attr_name, std::string_view val) {
    if (val.empty()) return false;
    if (val == "_")
        return attr_name == "form" || attr_name == "lemma";
    return true;
}

struct KwicContext {
    std::string left;
    std::string match;
    std::string right;
};

inline bool match_is_full_sentence_span(const Corpus& corpus, const Match& m) {
    if (!corpus.has_structure("s")) return false;
    const auto& s = corpus.structure("s");
    CorpusPos first = m.first_pos();
    CorpusPos last = m.last_pos();
    int64_t ri = s.find_region(first);
    if (ri < 0) return false;
    Region sr = s.get(static_cast<size_t>(ri));
    return sr.start == first && sr.end == last;
}

inline KwicContext build_context(const Corpus& corpus, const Match& m, int ctx_width) {
    const auto& form = corpus.attr("form");
    KwicContext ctx;

    CorpusPos first = m.first_pos();
    CorpusPos last  = m.last_pos();

    CorpusPos left_start = std::max(CorpusPos(0), first - ctx_width);
    for (CorpusPos p = left_start; p < first; ++p) {
        if (!ctx.left.empty()) ctx.left += ' ';
        ctx.left += form.value_at(p);
    }

    // Match text: full span from first to last in corpus order.
    // For discontinuous matches this includes gap tokens; the JSON
    // "tokens" array has the per-token detail for consumers that need it.
    for (CorpusPos p = first; p <= last; ++p) {
        if (!ctx.match.empty()) ctx.match += ' ';
        ctx.match += form.value_at(p);
    }

    CorpusPos right_end = std::min(corpus.size() - 1, last + ctx_width);
    for (CorpusPos p = last + 1; p <= right_end; ++p) {
        if (!ctx.right.empty()) ctx.right += ' ';
        ctx.right += form.value_at(p);
    }

    return ctx;
}

inline std::string_view lookup_doc_id(const Corpus& corpus, CorpusPos pos) {
    if (!corpus.has_structure("text")) return {};
    const auto& text = corpus.structure("text");
    int64_t ri = text.find_region(pos);
    if (ri < 0) return {};
    const size_t region_idx = static_cast<size_t>(ri);
    auto present = [](std::string_view v) {
        return !v.empty() && v != "_";
    };

    // Prefer the default text value when present.
    if (text.has_values()) {
        std::string_view v = text.region_value(region_idx);
        if (present(v)) return v;
    }

    // Fallback to common named text-region identifiers used by TEITOK indexes.
    for (const auto& attr : {"id", "text_id", "tuid", "text_tuid"}) {
        if (!text.has_region_attr(attr)) continue;
        std::string_view v = text.region_value(attr, region_idx);
        if (present(v)) return v;
    }
    return {};
}

} // namespace pando
