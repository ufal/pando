#include "index/fold_map.h"
#include <algorithm>
#include <cstdint>
#include <cctype>

namespace pando {

const std::vector<LexiconId> FoldMap::empty_;

namespace {

// ── UTF-8 ────────────────────────────────────────────────────────────────

/// The code point at s[i] (advances i); an invalid byte comes back as itself.
uint32_t next_cp(std::string_view s, size_t& i) {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    size_t n = c < 0x80 ? 1 : (c >> 5) == 0x6 ? 2 : (c >> 4) == 0xE ? 3 : (c >> 3) == 0x1E ? 4 : 0;
    if (n == 0 || i + n > s.size()) { ++i; return c; }
    uint32_t cp = n == 1 ? c : n == 2 ? (c & 0x1F) : n == 3 ? (c & 0x0F) : (c & 0x07);
    for (size_t k = 1; k < n; ++k) {
        const unsigned char cc = static_cast<unsigned char>(s[i + k]);
        if ((cc >> 6) != 0x2) { ++i; return c; }
        cp = (cp << 6) | (cc & 0x3F);
    }
    i += n;
    return cp;
}

void put_cp(std::string& out, uint32_t cp) {
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

// ── %d: the base letter of a letter with a canonical decomposition ───────
// '-': none (Æ, Ø, Đ, Ł, ß, … have no decomposition and stay as they are, as in NFD)

constexpr const char* kLatin1Base =      // U+00C0 … U+00FF
    "AAAAAA-CEEEEIIII"
    "-NOOOOO--UUUUY--"
    "aaaaaa-ceeeeiiii"
    "-nooooo--uuuuy-y";
constexpr const char* kLatinExtABase =   // U+0100 … U+017F
    "AaAaAaCcCcCcCcDd"
    "--EeEeEeEeEeGgGg"
    "GgGgHh--IiIiIiIi"
    "I---JjKk-LlLlLl-"
    "---NnNnNn---OoOo"
    "Oo--RrRrRrSsSsSs"
    "SsTtTt--UuUuUuUu"
    "UuUuWwYyYZzZzZz-";

uint32_t strip_accent(uint32_t cp) {
    if (cp >= 0xC0 && cp <= 0xFF) {
        const char b = kLatin1Base[cp - 0xC0];
        return b == '-' ? cp : static_cast<uint32_t>(b);
    }
    if (cp >= 0x100 && cp <= 0x17F) {
        const char b = kLatinExtABase[cp - 0x100];
        return b == '-' ? cp : static_cast<uint32_t>(b);
    }
    switch (cp) {
    // Latin Extended-B: pinyin (Ǎ …), Vietnamese (Ơ Ư), Romanian (Ș Ț)
    case 0x1CD: return 'A'; case 0x1CE: return 'a'; case 0x1CF: return 'I'; case 0x1D0: return 'i';
    case 0x1D1: return 'O'; case 0x1D2: return 'o';
    case 0x1D3: case 0x1D5: case 0x1D7: case 0x1D9: case 0x1DB: return 'U';
    case 0x1D4: case 0x1D6: case 0x1D8: case 0x1DA: case 0x1DC: return 'u';
    case 0x1A0: return 'O'; case 0x1A1: return 'o'; case 0x1AF: return 'U'; case 0x1B0: return 'u';
    case 0x218: return 'S'; case 0x219: return 's'; case 0x21A: return 'T'; case 0x21B: return 't';
    // Greek tonos / dialytika
    case 0x386: return 0x391; case 0x388: return 0x395; case 0x389: return 0x397; case 0x38A: return 0x399;
    case 0x38C: return 0x39F; case 0x38E: return 0x3A5; case 0x38F: return 0x3A9; case 0x390: return 0x3B9;
    case 0x3AA: return 0x399; case 0x3AB: return 0x3A5; case 0x3AC: return 0x3B1; case 0x3AD: return 0x3B5;
    case 0x3AE: return 0x3B7; case 0x3AF: return 0x3B9; case 0x3B0: return 0x3C5; case 0x3CA: return 0x3B9;
    case 0x3CB: return 0x3C5; case 0x3CC: return 0x3BF; case 0x3CD: return 0x3C5; case 0x3CE: return 0x3C9;
    // Cyrillic Ё Й Ї Ў and lowercase
    case 0x401: return 0x415; case 0x419: return 0x418; case 0x407: return 0x406; case 0x40E: return 0x423;
    case 0x451: return 0x435; case 0x439: return 0x438; case 0x457: return 0x456; case 0x45E: return 0x443;
    default: return cp;
    }
}

bool is_combining_mark(uint32_t cp) {
    return (cp >= 0x300 && cp <= 0x36F) || (cp >= 0x1AB0 && cp <= 0x1AFF) || (cp >= 0x1DC0 && cp <= 0x1DFF)
        || (cp >= 0x20D0 && cp <= 0x20FF);
}

// ── %c: lowercase (simple case mapping of the scripts above) ─────────────

uint32_t lower(uint32_t cp) {
    if (cp < 0x80) return (cp >= 'A' && cp <= 'Z') ? cp + 32 : cp;
    if (cp >= 0xC0 && cp <= 0xDE && cp != 0xD7) return cp + 32;
    if (cp >= 0x100 && cp <= 0x17F) {
        if (cp == 0x130) return 'i';
        if (cp == 0x178) return 0xFF;
        if ((cp >= 0x139 && cp <= 0x148) || (cp >= 0x179 && cp <= 0x17E)) return (cp & 1) ? cp + 1 : cp;
        if (cp == 0x131 || cp == 0x138 || cp == 0x149 || cp == 0x17F) return cp;
        return (cp & 1) ? cp : cp + 1;
    }
    if ((cp >= 0x1CD && cp <= 0x1DC)) return (cp & 1) ? cp + 1 : cp;
    if (cp == 0x1A0 || cp == 0x1AF || (cp >= 0x218 && cp <= 0x21B && !(cp & 1))) return cp + 1;
    if (cp >= 0x391 && cp <= 0x3AB && cp != 0x3A2) return cp + 32;
    switch (cp) {
    case 0x386: return 0x3AC; case 0x388: return 0x3AD; case 0x389: return 0x3AE; case 0x38A: return 0x3AF;
    case 0x38C: return 0x3CC; case 0x38E: return 0x3CD; case 0x38F: return 0x3CE;
    default: break;
    }
    if (cp >= 0x410 && cp <= 0x42F) return cp + 32;
    if (cp >= 0x400 && cp <= 0x40F) return cp + 80;
    if ((cp >= 0x460 && cp <= 0x481) || (cp >= 0x48A && cp <= 0x4BF)) return (cp & 1) ? cp : cp + 1;
    return cp;
}

}  // namespace

// %c / %d on UTF-8: simple lowercase mapping and NFD-style accent removal for Latin
// (Latin-1, Extended-A, the common Extended-B letters), Greek and Cyrillic; combining
// marks are dropped with %d. Other characters, and invalid UTF-8 bytes, stay as they are.
std::string FoldMap::fold(std::string_view s, bool lowercase, bool no_accents) {
    if (!lowercase && !no_accents) return std::string(s);
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size();) {
        uint32_t cp = next_cp(s, i);
        if (no_accents) {
            if (is_combining_mark(cp)) continue;
            cp = strip_accent(cp);
        }
        if (lowercase) cp = lower(cp);
        put_cp(out, cp);
    }
    return out;
}

std::string FoldMap::to_lower(std::string_view s) { return fold(s, true, false); }

std::string FoldMap::strip_accents(std::string_view s) { return fold(s, false, true); }

std::string FoldMap::to_lower_no_accents(std::string_view s) { return fold(s, true, true); }

FoldMap FoldMap::build_lowercase(const Lexicon& lex) {
    FoldMap fm;
    LexiconId n = lex.size();
    for (LexiconId id = 0; id < n; ++id) {
        std::string folded = to_lower(lex.get(id));
        fm.map_[folded].push_back(id);
    }
    return fm;
}

FoldMap FoldMap::build_no_accents(const Lexicon& lex) {
    FoldMap fm;
    LexiconId n = lex.size();
    for (LexiconId id = 0; id < n; ++id) {
        std::string folded = strip_accents(lex.get(id));
        fm.map_[folded].push_back(id);
    }
    return fm;
}

FoldMap FoldMap::build_lc_no_accents(const Lexicon& lex) {
    FoldMap fm;
    LexiconId n = lex.size();
    for (LexiconId id = 0; id < n; ++id) {
        std::string folded = to_lower_no_accents(lex.get(id));
        fm.map_[folded].push_back(id);
    }
    return fm;
}

} // namespace pando
