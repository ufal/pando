#pragma once

// Shared JSON helpers used by query_main.cpp, query_json.cpp, and server_main.cpp.

#include "core/types.h"
#include "query/executor.h"
#include "corpus/corpus.h"
#include <string>
#include <string_view>
#include <cstdio>
#include <cstdlib>

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

/// Skip JSON insignificant whitespace.
inline void json_skip_ws(const std::string& s, size_t& pos) {
    while (pos < s.size() &&
           (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r'))
        ++pos;
}

/// After finding `"key"`, skip whitespace + `:` + whitespace; return value start, or npos.
inline size_t json_value_pos(const std::string& body, const char* key) {
    const std::string needle = std::string("\"") + key + "\"";
    size_t search_from = 0;
    while (true) {
        size_t pos = body.find(needle, search_from);
        if (pos == std::string::npos) return std::string::npos;
        size_t after = pos + needle.size();
        json_skip_ws(body, after);
        if (after < body.size() && body[after] == ':') {
            ++after;
            json_skip_ws(body, after);
            return after;
        }
        search_from = pos + 1;
    }
}

/// Extract a JSON string value for `key` (handles `\"` / `\\`). Empty if missing.
inline std::string json_extract_str(const std::string& body, const char* key) {
    size_t pos = json_value_pos(body, key);
    if (pos == std::string::npos || pos >= body.size() || body[pos] != '"') return {};
    ++pos;
    std::string val;
    while (pos < body.size()) {
        char c = body[pos++];
        if (c == '"') break;
        if (c == '\\' && pos < body.size()) c = body[pos++];
        val += c;
    }
    return val;
}

inline size_t json_extract_num(const std::string& body, const char* key, size_t default_val) {
    size_t pos = json_value_pos(body, key);
    if (pos == std::string::npos) return default_val;
    return static_cast<size_t>(std::strtoull(body.c_str() + pos, nullptr, 10));
}

inline bool json_extract_bool(const std::string& body, const char* key, bool default_val) {
    size_t pos = json_value_pos(body, key);
    if (pos == std::string::npos) return default_val;
    if (body.compare(pos, 4, "true") == 0) return true;
    if (body.compare(pos, 5, "false") == 0) return false;
    return default_val;
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

inline KwicContext build_context(const Corpus& corpus, const Match& m, int ctx_width,
                                 bool sentence = false) {
    const auto& form = corpus.attr("form");
    KwicContext ctx;

    CorpusPos first = m.first_pos();
    CorpusPos last  = m.last_pos();

    CorpusPos left_start = std::max(CorpusPos(0), first - ctx_width);
    CorpusPos right_end = std::min(corpus.size() - 1, last + ctx_width);

    // Sentence view: expand left/right to the enclosing ``s`` region(s).
    // Match span stays [first, last] so KWIC highlighting remains on the hit.
    if (sentence && corpus.has_structure("s")) {
        const auto& s = corpus.structure("s");
        int64_t ri_first = s.find_region(first);
        int64_t ri_last = s.find_region(last);
        if (ri_first >= 0) {
            Region sr = s.get(static_cast<size_t>(ri_first));
            left_start = sr.start;
            right_end = sr.end;
        }
        if (ri_last >= 0 && ri_last != ri_first) {
            Region sr2 = s.get(static_cast<size_t>(ri_last));
            right_end = sr2.end;
            if (ri_first < 0)
                left_start = sr2.start;
        }
    }

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

    for (CorpusPos p = last + 1; p <= right_end; ++p) {
        if (!ctx.right.empty()) ctx.right += ' ';
        ctx.right += form.value_at(p);
    }

    return ctx;
}

/// Build left/match/right around a single corpus position (for KonText widectx).
inline KwicContext build_context_at(const Corpus& corpus, CorpusPos pos, int left_ctx, int right_ctx,
                                    bool sentence = false) {
    Match m;
    m.positions = {pos};
    m.span_ends = {pos};
    if (sentence) {
        return build_context(corpus, m, std::max(left_ctx, right_ctx), true);
    }
    const auto& form = corpus.attr("form");
    KwicContext ctx;
    CorpusPos left_start = (pos > static_cast<CorpusPos>(left_ctx))
        ? pos - static_cast<CorpusPos>(left_ctx) : CorpusPos(0);
    for (CorpusPos p = left_start; p < pos; ++p) {
        if (!ctx.left.empty()) ctx.left += ' ';
        ctx.left += form.value_at(p);
    }
    ctx.match = std::string(form.value_at(pos));
    CorpusPos right_end = std::min(corpus.size() - 1, pos + static_cast<CorpusPos>(right_ctx));
    for (CorpusPos p = pos + 1; p <= right_end; ++p) {
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
