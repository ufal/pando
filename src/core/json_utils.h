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

/// End of the JSON value starting at `pos` (one past it): strings, objects and
/// arrays are matched (quotes and escapes respected), other values end at `,`
/// `}` `]` or whitespace. npos when unterminated.
inline size_t json_value_end(const std::string& s, size_t pos) {
    if (pos >= s.size()) return std::string::npos;
    if (s[pos] == '"') {
        for (size_t i = pos + 1; i < s.size(); ++i) {
            if (s[i] == '\\') { ++i; continue; }
            if (s[i] == '"') return i + 1;
        }
        return std::string::npos;
    }
    if (s[pos] == '{' || s[pos] == '[') {
        int depth = 0;
        for (size_t i = pos; i < s.size(); ++i) {
            const char c = s[i];
            if (c == '"') {
                const size_t e = json_value_end(s, i);
                if (e == std::string::npos) return e;
                i = e - 1;
            } else if (c == '{' || c == '[') {
                ++depth;
            } else if (c == '}' || c == ']') {
                if (--depth == 0) return i + 1;
            }
        }
        return std::string::npos;
    }
    size_t i = pos;
    while (i < s.size() && s[i] != ',' && s[i] != '}' && s[i] != ']' && s[i] != ' ' && s[i] != '\n'
           && s[i] != '\t' && s[i] != '\r')
        ++i;
    return i;
}

/// The raw JSON text of `key`'s value (first occurrence, as json_value_pos). Empty if missing.
inline std::string json_extract_raw(const std::string& body, const char* key) {
    const size_t pos = json_value_pos(body, key);
    if (pos == std::string::npos) return {};
    const size_t end = json_value_end(body, pos);
    if (end == std::string::npos) return {};
    return body.substr(pos, end - pos);
}

/// `body` without `key` and its value (first occurrence), so that flat lookups
/// of the remaining top-level keys cannot hit members nested inside it.
inline std::string json_without_member(const std::string& body, const char* key) {
    const std::string needle = std::string("\"") + key + "\"";
    const size_t k = body.find(needle);
    const size_t v = json_value_pos(body, key);
    if (k == std::string::npos || v == std::string::npos) return body;
    const size_t end = json_value_end(body, v);
    if (end == std::string::npos) return body;
    return body.substr(0, k) + "\"_\":0" + body.substr(end);
}

/// Members of a JSON object text `{"a": 1, "b": {...}}` → (key, raw value), in order.
inline std::vector<std::pair<std::string, std::string>> json_object_members(const std::string& obj) {
    std::vector<std::pair<std::string, std::string>> out;
    size_t i = 0;
    json_skip_ws(obj, i);
    if (i >= obj.size() || obj[i] != '{') return out;
    ++i;
    for (;;) {
        json_skip_ws(obj, i);
        if (i >= obj.size() || obj[i] == '}') break;
        if (obj[i] != '"') break;
        const size_t kend = json_value_end(obj, i);
        if (kend == std::string::npos) break;
        std::string key;
        for (size_t j = i + 1; j + 1 < kend; ++j) {
            if (obj[j] == '\\' && j + 2 < kend) ++j;
            key += obj[j];
        }
        i = kend;
        json_skip_ws(obj, i);
        if (i >= obj.size() || obj[i] != ':') break;
        ++i;
        json_skip_ws(obj, i);
        const size_t vend = json_value_end(obj, i);
        if (vend == std::string::npos) break;
        out.emplace_back(std::move(key), obj.substr(i, vend - i));
        i = vend;
        json_skip_ws(obj, i);
        if (i < obj.size() && obj[i] == ',') ++i;
    }
    return out;
}

/// A JSON array of strings for `key` (`["a", "b"]`); a single string counts as one.
inline std::vector<std::string> json_extract_str_array(const std::string& body, const char* key) {
    std::vector<std::string> out;
    const std::string raw = json_extract_raw(body, key);
    if (raw.empty()) return out;
    if (raw[0] == '"') { out.push_back(json_extract_str(body, key)); return out; }
    if (raw[0] != '[') return out;
    for (size_t i = 1; i < raw.size();) {
        if (raw[i] == '"') {
            const size_t e = json_value_end(raw, i);
            if (e == std::string::npos) break;
            std::string v;
            for (size_t j = i + 1; j + 1 < e; ++j) {
                if (raw[j] == '\\' && j + 2 < e) ++j;
                v += raw[j];
            }
            out.push_back(std::move(v));
            i = e;
        } else {
            ++i;
        }
    }
    return out;
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
