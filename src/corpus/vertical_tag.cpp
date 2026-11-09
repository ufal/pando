#include "corpus/vertical_tag.h"

#include <cstdint>

namespace pando {

namespace {

bool is_space(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }

void append_utf8(std::string& out, uint32_t cp) {
    if (cp < 0x80) {
        out += static_cast<char>(cp);
    } else if (cp < 0x800) {
        out += static_cast<char>(0xC0 | (cp >> 6));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp < 0x10000) {
        out += static_cast<char>(0xE0 | (cp >> 12));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else {
        out += static_cast<char>(0xF0 | (cp >> 18));
        out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    }
}

// The entity starting at s[i] == '&': appended to out, length consumed returned (0 = not an entity).
size_t decode_entity(std::string_view s, size_t i, std::string& out) {
    const size_t semi = s.find(';', i + 1);
    if (semi == std::string_view::npos || semi - i > 10) return 0;
    const std::string_view name = s.substr(i + 1, semi - i - 1);
    if (name == "quot") out += '"';
    else if (name == "amp") out += '&';
    else if (name == "lt") out += '<';
    else if (name == "gt") out += '>';
    else if (name == "apos") out += '\'';
    else if (name.size() >= 2 && name[0] == '#') {
        const bool hex = name[1] == 'x' || name[1] == 'X';
        const std::string_view digits = name.substr(hex ? 2 : 1);
        if (digits.empty()) return 0;
        uint32_t cp = 0;
        for (char c : digits) {
            uint32_t d;
            if (c >= '0' && c <= '9') d = static_cast<uint32_t>(c - '0');
            else if (hex && c >= 'a' && c <= 'f') d = static_cast<uint32_t>(c - 'a' + 10);
            else if (hex && c >= 'A' && c <= 'F') d = static_cast<uint32_t>(c - 'A' + 10);
            else return 0;
            cp = cp * (hex ? 16 : 10) + d;
            if (cp > 0x10FFFF) return 0;
        }
        if (cp == 0) return 0;
        append_utf8(out, cp);
    } else {
        return 0;
    }
    return semi - i + 1;
}

} // namespace

std::string decode_vertical_value(std::string_view raw) {
    std::string out;
    out.reserve(raw.size());
    for (size_t i = 0; i < raw.size(); ++i) {
        const char c = raw[i];
        if (c == '\\' && i + 1 < raw.size() && (raw[i + 1] == '"' || raw[i + 1] == '\'' || raw[i + 1] == '\\')) {
            out += raw[++i];
        } else if (c == '&') {
            const size_t n = decode_entity(raw, i, out);
            if (n == 0) out += c;
            else i += n - 1;
        } else {
            out += c;
        }
    }
    return out;
}

bool parse_vertical_tag(std::string_view line, VerticalTag& out) {
    out = VerticalTag{};
    while (!line.empty() && is_space(line.back())) line.remove_suffix(1);
    // as encodevert: a line from '<' to '>' (a token "<" or "<3" stays a token)
    if (line.size() < 3 || line.front() != '<' || line.back() != '>') return false;
    size_t p = 0;
    ++p;
    if (p < line.size() && line[p] == '/') {
        out.kind = VerticalTag::Kind::Close;
        ++p;
    }
    const size_t name_start = p;
    while (p < line.size() && !is_space(line[p]) && line[p] != '>' && line[p] != '/') ++p;
    out.name = std::string(line.substr(name_start, p - name_start));
    if (out.name.empty()) return false;
    if (out.kind == VerticalTag::Kind::Close) return true;

    while (p < line.size()) {
        while (p < line.size() && is_space(line[p])) ++p;
        if (p >= line.size() || line[p] == '>') break;
        if (line[p] == '/') {   // "/>" (or a stray '/')
            out.kind = VerticalTag::Kind::Empty;
            ++p;
            continue;
        }
        const size_t key_start = p;
        while (p < line.size() && !is_space(line[p]) && line[p] != '=' && line[p] != '>' && line[p] != '/') ++p;
        std::string key(line.substr(key_start, p - key_start));
        while (p < line.size() && is_space(line[p])) ++p;
        if (p >= line.size() || line[p] != '=') continue;   // an attribute without a value: skipped
        ++p;
        while (p < line.size() && is_space(line[p])) ++p;
        if (p >= line.size()) break;
        std::string_view raw;
        if (line[p] == '"' || line[p] == '\'') {
            const char q = line[p++];
            const size_t v0 = p;
            while (p < line.size() && line[p] != q) {
                if (line[p] == '\\' && p + 1 < line.size()) ++p;   // \" \' \\ do not end the value
                ++p;
            }
            raw = line.substr(v0, p - v0);
            if (p < line.size()) ++p;   // the closing quote
        } else {                        // unquoted: up to white space or the tag's end
            const size_t v0 = p;
            while (p < line.size() && !is_space(line[p]) && line[p] != '>' &&
                   !(line[p] == '/' && p + 1 < line.size() && line[p + 1] == '>'))
                ++p;
            raw = line.substr(v0, p - v0);
        }
        if (!key.empty()) out.attrs.emplace_back(std::move(key), decode_vertical_value(raw));
    }
    return true;
}

} // namespace pando
