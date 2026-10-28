#include "corpus/builder.h"
#include <fstream>
#include <stdexcept>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <string_view>
#include <cctype>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pando {

namespace {

// Keys become corpus.info / s_<key>.* — must be a single identifier token.
static bool is_region_attr_key_token(std::string_view s) {
    if (s.empty()) return false;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '_') continue;
        return false;
    }
    return true;
}

// Structural region values for generic # key = val (skip UD translation sentences).
static bool is_simple_region_value(std::string_view s) {
    if (s.empty()) return false;
    for (unsigned char c : s) {
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') return false;
    }
    return true;
}

// Full comment keys that always mean VRT-style text region metadata (not UD text_<lang> translation).
static bool is_reserved_text_structural_key(const std::string& key) {
    static const std::unordered_set<std::string> k{
        "text_id",       "text_lang",     "text_langcode", "text_treebank",
        "text_genre",    "text_century",
    };
    return k.count(key) > 0;
}

// UD sentence comment # text_en = … / # text_cs = … (translation), not text-region attribute "en".
// Only in sentence comment context: before the first token line in the file, or after a blank until tokens.
// Outside that zone, text_xx = … is VRT-style (region text, attribute xx).
static bool is_ud_sentence_translation_key(const std::string& key) {
    if (is_reserved_text_structural_key(key)) return false;
    if (key.size() < 7 || key.compare(0, 5, "text_") != 0) return false;   // text_en: 7
    std::string suf = key.substr(5);
    // 2–3 letter lowercase language codes (en, cs, got, la, …); exclude a few non-lang triples.
    if (suf.size() < 2 || suf.size() > 3) return false;
    for (char c : suf) {
        if (c < 'a' || c > 'z') return false;
    }
    static const std::unordered_set<std::string> not_lang{
        "src", "ref", "dom", "uri", "url", "num", "cat", "new", "old",
    };
    if (not_lang.count(suf)) return false;
    return true;
}

// Every text region must supply the same named attrs (indexer requires equal-length vectors).
static void pad_text_region_attrs(std::vector<std::pair<std::string, std::string>>& attrs) {
    auto has = [&](const char* k) {
        for (const auto& p : attrs)
            if (p.first == k) return true;
        return false;
    };
    if (!has("id")) attrs.push_back({"id", "_"});
    if (!has("genre")) attrs.push_back({"genre", "_"});
    if (!has("lang")) attrs.push_back({"lang", "_"});
    if (!has("langcode")) attrs.push_back({"langcode", "_"});
    if (!has("treebank")) attrs.push_back({"treebank", "_"});
    if (!has("century")) attrs.push_back({"century", "_"});
}

static void upsert_region_attr(std::vector<std::pair<std::string, std::string>>& attrs,
                               const std::string& key, const std::string& val) {
    for (auto& p : attrs) {
        if (p.first == key) {
            p.second = val;
            return;
        }
    }
    attrs.push_back({key, val});
}

static void pad_doc_region_attrs(std::vector<std::pair<std::string, std::string>>& attrs) {
    auto has = [&](const char* k) {
        for (const auto& p : attrs)
            if (p.first == k) return true;
        return false;
    };
    if (!has("id")) attrs.push_back({"id", "_"});
    if (!has("lang")) attrs.push_back({"lang", "_"});
    if (!has("langcode")) attrs.push_back({"langcode", "_"});
}

static std::string trim_sv(std::string_view sv) {
    while (!sv.empty() && (sv.front() == ' ' || sv.front() == '\t'))
        sv.remove_prefix(1);
    while (!sv.empty() && (sv.back() == ' ' || sv.back() == '\r' || sv.back() == '\t'))
        sv.remove_suffix(1);
    return std::string(sv);
}

template <class RegionStack>
static bool close_last_region_of_type(RegionStack& stack,
                                      const std::string& type,
                                      CorpusPos end,
                                      StreamingBuilder& builder) {
    for (auto it = stack.rbegin(); it != stack.rend(); ++it) {
        if (it->type != type) continue;
        if (end >= it->start)
            builder.add_region(it->type, it->start, end, it->attrs);
        stack.erase(std::next(it).base());
        return true;
    }
    return false;
}

} // namespace

CorpusBuilder::CorpusBuilder(const std::string& output_dir, bool overlay_standoff_only)
    : builder_(output_dir, overlay_standoff_only), overlay_standoff_only_(overlay_standoff_only),
      output_dir_(output_dir) {}

void CorpusBuilder::read_jsonl_overlay(const std::string& path, CorpusPos expected_tokens) {
    if (!overlay_standoff_only_)
        throw std::runtime_error("read_jsonl_overlay requires CorpusBuilder(output_dir, true)");
    read_jsonl(path, &expected_tokens);
}

void CorpusBuilder::close_text_region_if_open() {
    if (!has_text_region_) return;
    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
    if (end >= text_region_start_) {
        pad_text_region_attrs(text_region_attrs_);
        builder_.add_region("text", text_region_start_, end, text_region_attrs_);
    }
    has_text_region_ = false;
    text_region_attrs_.clear();
}

void CorpusBuilder::close_doc_region_if_open() {
    if (!has_doc_region_) return;
    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
    if (end >= doc_region_start_) {
        pad_doc_region_attrs(doc_region_attrs_);
        builder_.add_region("doc", doc_region_start_, end, doc_region_attrs_);
    }
    has_doc_region_ = false;
    doc_region_attrs_.clear();
}

void CorpusBuilder::parse_misc(const std::string& misc_str,
        std::unordered_map<std::string, std::string>& attrs) {
    if (misc_str.empty() || misc_str == "_" || misc_keys_.empty()) return;
    // Only index a fixed set (set_misc_keys): some corpora encode open-ended key=value MISC that
    // would explode the attribute namespace if imported wholesale.
    auto misc_key_allowed = [this](std::string_view key) -> bool {
        for (const auto& a : misc_keys_) {
            if (key == a) return true;
        }
        return false;
    };
    size_t start = 0;
    while (start < misc_str.size()) {
        size_t pipe = misc_str.find('|', start);
        size_t end = (pipe == std::string::npos) ? misc_str.size() : pipe;
        std::string_view piece(misc_str.data() + start, end - start);
        auto eq = piece.find('=');
        if (eq != std::string_view::npos) {
            std::string key = trim_sv(piece.substr(0, eq));
            if (!key.empty() && misc_key_allowed(key))
                attrs[std::move(key)] = trim_sv(piece.substr(eq + 1));
        }
        start = end + 1;
    }
}

void CorpusBuilder::parse_feats(const std::string& feats_str,
        std::unordered_map<std::string, std::string>& attrs) {
    if (split_feats_) {
        if (feats_str.size() == 1 && feats_str[0] == '_') return;
        if (feats_str.empty()) return;

        size_t start = 0;
        while (start < feats_str.size()) {
            size_t pipe = feats_str.find('|', start);
            size_t end = (pipe == std::string::npos) ? feats_str.size() : pipe;

            size_t eq = feats_str.find('=', start);
            if (eq != std::string::npos && eq < end) {
                attrs[std::string("feats#") + feats_str.substr(start, eq - start)] =
                    feats_str.substr(eq + 1, end - eq - 1);
            }
            start = end + 1;
        }
    } else {
        attrs["feats"] = feats_str;
    }
}

void CorpusBuilder::read_conllu(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot open " + path);

    std::string line;
    std::unordered_map<std::string, std::string> attrs;
    attrs.reserve(32);

    // Sentence-level # comments: after a blank line, or before the first token line in the file (CoNLL-U
    // often has no blank before the first sentence). UD # text_en = … is translation here, not VRT text_en.
    bool pending_sentence_comments = false;
    bool seen_any_token_line = false;

    while (std::getline(in, line)) {
        if (line.empty()) {
            if (in_sentence_) {
                builder_.end_sentence(sent_region_attrs_);
                sent_region_attrs_.clear();
                pending_sent_tuid_.clear();
                in_sentence_ = false;
            }
            pending_sentence_comments = true;
            continue;
        }
        if (line[0] == '#') {
            const bool sentence_comment_context =
                !seen_any_token_line || pending_sentence_comments;
            // CoNLL-U comments: # newdoc id (structural "doc"), # newregion text, # endregion text,
            // CQP # text_* = …, # tuid = …, # newpar id = …
            std::string_view sv(line);
            // strip leading "# "
            if (sv.size() > 1 && sv[1] == ' ')
                sv.remove_prefix(2);
            else
                sv.remove_prefix(1);
            auto starts_with = [&](std::string_view p) {
                return sv.size() >= p.size() && sv.substr(0, p.size()) == p;
            };
            auto parse_id_after_eq = [&]() -> std::string {
                auto pos = sv.find('=');
                if (pos == std::string_view::npos) return {};
                std::string_view val = sv.substr(pos + 1);
                while (!val.empty() && val.front() == ' ') val.remove_prefix(1);
                while (!val.empty() && (val.back() == ' ' || val.back() == '\r')) val.remove_suffix(1);
                return std::string(val);
            };

            // # newregion X  — open a structural region span
            if (starts_with("newregion")) {
                size_t j = 9; // "newregion"
                while (j < sv.size() && (sv[j] == ' ' || sv[j] == '\t')) ++j;
                std::string rname = trim_sv(sv.substr(j));
                if (rname == "s") {
                    // Sentence region is written by end_sentence(); keep extension as no-op marker.
                } else if (rname == "text") {
                    close_text_region_if_open();
                    text_region_start_ = builder_.corpus_size();
                    text_region_attrs_.clear();
                    has_text_region_ = true;
                } else if (!rname.empty()) {
                    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
                    close_last_region_of_type(conllu_region_stack_, rname, end, builder_);
                    OpenRegion r;
                    r.type = rname;
                    r.start = builder_.corpus_size();
                    conllu_region_stack_.push_back(std::move(r));
                }
                continue;
            }
            // # endregion X  — close before the next # newregion (optional if regions chain)
            if (starts_with("endregion")) {
                size_t j = 8; // "endregion"
                while (j < sv.size() && (sv[j] == ' ' || sv[j] == '\t')) ++j;
                std::string rname = trim_sv(sv.substr(j));
                if (rname == "text") {
                    close_text_region_if_open();
                } else if (!rname.empty() && rname != "s") {
                    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
                    close_last_region_of_type(conllu_region_stack_, rname, end, builder_);
                }
                continue;
            }

            // a keyword alone or before its `= value` (`# newpar`, `# newdoc id = …`)
            auto keyword = [&](std::string_view w) {
                return starts_with(w) && (sv.size() == w.size() || sv[w.size()] == ' ' || sv[w.size()] == '\t' ||
                                          sv[w.size()] == '=');
            };
            if (keyword("newdoc")) {
                // CoNLL-U document boundary — structural region "doc", not "text".
                // Do not close "text": file-level # newregion text can span the same tokens as doc
                // so queries can use text_lang (etc.) alongside doc_id.
                close_doc_region_if_open();
                doc_id_ = parse_id_after_eq();   // `# newdoc id = …`, or a bare `# newdoc`
                doc_region_start_ = builder_.corpus_size();
                doc_region_attrs_.clear();
                doc_region_attrs_.push_back({"id", doc_id_.empty() ? "_" : doc_id_});
                has_doc_region_ = true;
            } else if (starts_with("tuid") &&
                       (sv.size() == 4 || sv[4] == ' ' || sv[4] == '=')) {
                std::string t = parse_id_after_eq();
                upsert_region_attr(sent_region_attrs_, "tuid", t);
                pending_sent_tuid_ = t;
            } else if (starts_with("meta::")) {
                // `# meta::KEY = value`: an attribute of the document (CorefUD, GUM)
                const size_t eqp = sv.find('=');
                const std::string key = eqp == std::string_view::npos ? "" : trim_sv(sv.substr(6, eqp - 6));
                if (has_doc_region_ && is_region_attr_key_token(key))
                    upsert_region_attr(doc_region_attrs_, key, parse_id_after_eq());
            } else if (keyword("newpar")) {
                if (has_par_region_) {
                    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
                    if (end >= par_start_)
                        builder_.add_region("par", par_start_, end,
                                            std::vector<std::pair<std::string, std::string>>{{"id", par_id_}});
                }
                par_id_ = parse_id_after_eq();   // `# newpar id = …`, or a bare `# newpar`
                if (par_id_.empty()) par_id_ = "_";
                par_start_ = builder_.corpus_size();
                has_par_region_ = true;
            } else {
                // Generic: CQP-style region attrs (# text_genre = …), UD metadata.
                // Split on first '=' or ':' so # text_en: … is not misparsed when '=' appears in the value.
                auto eqp = sv.find_first_of("=:");
                if (eqp != std::string_view::npos) {
                    std::string key = trim_sv(sv.substr(0, eqp));
                    std::string val = trim_sv(sv.substr(eqp + 1));
                    if (!is_region_attr_key_token(key))
                        ;
                    else if (key == "parallel_id") {
                        if (is_simple_region_value(val)) {
                            upsert_region_attr(sent_region_attrs_, "tuid", val);
                            pending_sent_tuid_ = val;
                        }
                    } else if (key == "sent_id") {
                        if (is_simple_region_value(val))
                            upsert_region_attr(sent_region_attrs_, "sent_id", val);
                    } else if (key != "tuid") {
                        if (key == "text") {
                            // # text = primary surface (sentence); not a structural attribute
                        } else if (sentence_comment_context
                                   && is_ud_sentence_translation_key(key)) {
                            // UD: translation line for this sentence — not VRT text_<lang> = region attr.
                            // Does not require # newregion text (bare UD corpora).
                            upsert_region_attr(sent_region_attrs_, key, val);
                        } else if (!is_simple_region_value(val)) {
                            // Skip non-token structural values (long prose) except handled above.
                        } else {
                            size_t us = key.find('_');
                            if (us != std::string::npos && us > 0 && us + 1 < key.size()) {
                                std::string rname = key.substr(0, us);
                                std::string attr = key.substr(us + 1);
                                if (!attr.empty() && is_region_attr_key_token(rname)
                                    && is_region_attr_key_token(attr)) {
                                    if (rname == "s") {
                                        upsert_region_attr(sent_region_attrs_, attr, val);
                                        if (attr == "tuid")
                                            pending_sent_tuid_ = val;
                                    } else if (rname == "text") {
                                        if (has_text_region_) {
                                            // VRT text_xx = … unless UD # text_<lang> = in sentence comment block.
                                            if (!(sentence_comment_context
                                                  && is_ud_sentence_translation_key(key)))
                                                upsert_region_attr(text_region_attrs_, attr, val);
                                        }
                                    } else if (rname == "doc" && has_doc_region_) {
                                        upsert_region_attr(doc_region_attrs_, attr, val);
                                    } else {
                                        for (auto it = conllu_region_stack_.rbegin(); it != conllu_region_stack_.rend(); ++it) {
                                            if (it->type == rname) {
                                                upsert_region_attr(it->attrs, attr, val);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            continue;
        }

        // Fast tab-split: CoNLL-U has 10 columns (ID … MISC); need 9 tabs for full row.
        size_t tab[9];  // positions of the 9 tab characters
        size_t pos = 0;
        int found = 0;
        while (found < 9 && pos < line.size()) {
            size_t t = line.find('\t', pos);
            if (t == std::string::npos) break;
            tab[found++] = t;
            pos = t + 1;
        }
        if (found < 7) continue;  // malformed line

        // Field boundaries: field[i] starts at (i==0 ? 0 : tab[i-1]+1), ends at tab[i] or EOL
        auto field = [&](int i) -> std::string_view {
            size_t s = (i == 0) ? 0 : tab[i - 1] + 1;
            size_t e = (i < found) ? tab[i] : line.size();
            return std::string_view(line.data() + s, e - s);
        };

        auto id_sv = field(0);
        // Check for multi-word token range (e.g. "1-2") or empty word (e.g. "1.1")
        {
            bool is_range = false, is_empty_word = false;
            for (char c : id_sv) {
                if (c == '-') is_range = true;
                if (c == '.') is_empty_word = true;
            }
            if (is_empty_word) goto next_line;  // skip empty words (1.1, etc.)
            if (is_range) {
                // MWT range line: parse "start-end" and capture form
                size_t dash = id_sv.find('-');
                int start_id = 0, end_id = 0;
                for (size_t i = 0; i < dash; ++i)
                    start_id = start_id * 10 + (id_sv[i] - '0');
                for (size_t i = dash + 1; i < id_sv.size(); ++i)
                    end_id = end_id * 10 + (id_sv[i] - '0');
                mwt_form_ = std::string(field(1));
                mwt_start_ = builder_.corpus_size();
                mwt_remaining_ = end_id - start_id + 1;
                goto next_line;  // don't add a token for the range line itself
            }
        }

        {
            attrs.clear();
            attrs.emplace("form",   std::string(field(1)));
            attrs.emplace("lemma",  std::string(field(2)));
            attrs.emplace("upos",   std::string(field(3)));
            attrs.emplace("xpos",   std::string(field(4)));
            attrs.emplace("deprel", std::string(field(7)));

            parse_feats(std::string(field(5)), attrs);

            if (!pending_sent_tuid_.empty())
                attrs.emplace("tuid", pending_sent_tuid_);
            // Word-level alignment (TEITOK-style): MISC column tuid=... overrides sentence default.
            if (found >= 9)
                parse_misc(std::string(field(9)), attrs);

            int head_id = 0;
            auto head_sv = field(6);
            for (char c : head_sv) {
                if (c < '0' || c > '9') { head_id = -1; break; }
                head_id = head_id * 10 + (c - '0');
            }

            if (found >= 8) builder_.set_token_deps(field(8));   // enhanced dependencies (relation layer deps)
            builder_.add_token(attrs, head_id);
            in_sentence_ = true;
            pending_sentence_comments = false;
            seen_any_token_line = true;

            // MWT contraction region: after the last sub-token, emit "contr" region.
            if (mwt_remaining_ > 0) {
                --mwt_remaining_;
                if (mwt_remaining_ == 0) {
                    CorpusPos mwt_end = builder_.corpus_size() - 1;
                    builder_.add_region("contr", mwt_start_, mwt_end,
                                        std::vector<std::pair<std::string, std::string>>{{"form", mwt_form_}});
                }
            }
        }
        next_line:;
    }

    if (in_sentence_) {
        builder_.end_sentence(sent_region_attrs_);
        sent_region_attrs_.clear();
        pending_sent_tuid_.clear();
        in_sentence_ = false;
    }
    // Close any open paragraph/doc regions at end of file
    if (has_par_region_) {
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= par_start_)
            builder_.add_region("par", par_start_, end,
                                std::vector<std::pair<std::string, std::string>>{{"id", par_id_}});
        has_par_region_ = false;
    }
    close_text_region_if_open();
    close_doc_region_if_open();
    // Close any still-open generic CoNLL-U regions.
    while (!conllu_region_stack_.empty()) {
        OpenRegion r = std::move(conllu_region_stack_.back());
        conllu_region_stack_.pop_back();
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= r.start)
            builder_.add_region(r.type, r.start, end, r.attrs);
    }
}

// Korp / Kielipankki VRT: <!-- #vrt positional-attributes: word ref lemma pos ... -->
// (IMS CWB itself usually takes -P from the command line; this in-file convention is common
// in Korp pipelines and some downloadable VRT.)
static bool try_parse_vrt_positional_header(const std::string& line,
                                            std::vector<std::string>* names_out) {
    constexpr const char* kMarker = "positional-attributes:";
    size_t pos = line.find(kMarker);
    if (pos == std::string::npos) return false;
    pos += sizeof("positional-attributes:") - 1;
    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) ++pos;
    size_t end = line.rfind("-->");
    if (end == std::string::npos || end <= pos) return false;
    std::string body = line.substr(pos, end - pos);
    names_out->clear();
    std::string cur;
    for (char c : body) {
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            if (!cur.empty()) {
                names_out->push_back(cur);
                cur.clear();
            }
        } else
            cur += c;
    }
    if (!cur.empty()) names_out->push_back(cur);
    return !names_out->empty();
}

static std::string lower_ascii_copy(std::string_view s) {
    std::string o;
    o.reserve(s.size());
    for (unsigned char c : s) o.push_back(static_cast<char>(std::tolower(c)));
    return o;
}

// Map VRT column label → Pando token attribute; empty = drop column (e.g. CWB duplicate "lc").
static std::string map_vrt_col_to_token_attr(const std::string& raw) {
    std::string n = lower_ascii_copy(raw);
    if (n == "word") return "form";
    if (n == "lc") return "";  // lowercase copy of word; flexiconv skips
    if (n == "lemma") return "lemma";
    if (n == "upos") return "upos";
    if (n == "xpos") return "xpos";
    if (n == "pos") return "xpos";  // often PTB in CWB; use "upos" in header for UD
    if (n == "deprel") return "deprel";
    if (n == "msd" || n == "feats") return "feats";
    if (is_region_attr_key_token(n)) return n;
    return "";
}

void CorpusBuilder::read_vertical(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot open " + path);

    std::string line;
    std::unordered_map<std::string, std::string> attrs;
    attrs.reserve(8);
    bool in_sentence = false;
    std::vector<std::string> vrt_column_names;

    while (std::getline(in, line)) {
        // Trim trailing CR
        if (!line.empty() && line.back() == '\r') line.pop_back();

        // XML / VRT preamble (must run before generic "<" handling: <!-- is not an element tag).
        if (!line.empty() && line.rfind("<!--", 0) == 0) {
            try_parse_vrt_positional_header(line, &vrt_column_names);
            continue;
        }
        if (!line.empty() && line.rfind("<?", 0) == 0) {
            // XML declaration or processing instruction (ignored by CWB with -x).
            continue;
        }
        // Hash comment line (CWB -n / -N style); avoid treating tabular data as comments.
        if (!line.empty() && line[0] == '#' && line.find('\t') == std::string::npos) continue;

        // XML-style region tags: generic <tag ...> and </tag>
        if (!line.empty() && line[0] == '<') {
            // Closing tag: </tag>
            if (line.size() >= 3 && line[1] == '/') {
                size_t end_name = line.find_first_of(" >", 2);
                std::string type = line.substr(2, end_name == std::string::npos ? std::string::npos : end_name - 2);
                // Close matching region from stack (last with same type)
                for (auto it = vrt_region_stack_.rbegin(); it != vrt_region_stack_.rend(); ++it) {
                    if (it->type == type) {
                        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
                        if (end >= it->start)
                            builder_.add_region(type, it->start, end, it->attrs);
                        vrt_region_stack_.erase(std::next(it).base());
                        break;
                    }
                }
                continue;
            }
            // Opening tag: <tag ...>
            if (line.size() >= 2 && line[1] != '/') {
                size_t name_start = 1;
                size_t name_end = line.find_first_of(" >", name_start);
                std::string type = line.substr(name_start, name_end == std::string::npos ? std::string::npos : name_end - name_start);

                OpenRegion r;
                r.type = type;
                r.start = builder_.corpus_size();

                // Parse attributes key="value"
                size_t pos = line.find(' ', name_end);
                while (pos != std::string::npos && pos < line.size()) {
                    while (pos < line.size() && line[pos] == ' ') ++pos;
                    if (pos >= line.size() || line[pos] == '>' || line[pos] == '/') break;
                    size_t eq = line.find('=', pos);
                    if (eq == std::string::npos) break;
                    std::string key = line.substr(pos, eq - pos);
                    // trim trailing spaces from key
                    while (!key.empty() && key.back() == ' ') key.pop_back();
                    size_t quote1 = line.find('"', eq + 1);
                    if (quote1 == std::string::npos) break;
                    size_t quote2 = line.find('"', quote1 + 1);
                    if (quote2 == std::string::npos) break;
                    std::string val = line.substr(quote1 + 1, quote2 - quote1 - 1);
                    if (!key.empty())
                        r.attrs.push_back({key, val});
                    pos = quote2 + 1;
                }

                vrt_region_stack_.push_back(std::move(r));
                continue;
            }
        }

        if (line.empty()) {
            if (in_sentence) { builder_.end_sentence(); in_sentence = false; }
            continue;
        }
        // Sentence boundaries (CWB-style)
        if (line == "<s>" || line == "<s />" || line.find("<s ") == 0) {
            if (in_sentence) builder_.end_sentence();
            in_sentence = true;
            continue;
        }
        if (line == "</s>") {
            if (in_sentence) { builder_.end_sentence(); in_sentence = false; }
            continue;
        }

        // Token line: tab-separated. For plain vertical we expect:
        //   form [lemma [upos]]
        // For UD-style VRT we often get:
        //   form id lemma upos ...
        attrs.clear();
        std::vector<std::string> cols;
        {
            size_t pos = 0;
            while (true) {
                size_t tab = line.find('\t', pos);
                if (tab == std::string::npos) {
                    cols.push_back(line.substr(pos));
                    break;
                }
                cols.push_back(line.substr(pos, tab - pos));
                pos = tab + 1;
            }
        }

        if (cols.empty()) continue;

        if (!vrt_column_names.empty()) {
            for (size_t i = 0; i < vrt_column_names.size() && i < cols.size(); ++i) {
                std::string key = map_vrt_col_to_token_attr(vrt_column_names[i]);
                if (key.empty()) continue;
                std::string val = cols[i];
                if (val.empty()) val = "_";
                attrs[key] = std::move(val);
            }
            if (!attrs.count("form") && !cols.empty()) {
                std::string form = cols[0];
                attrs["form"] = form.empty() ? "_" : std::move(form);
            }
            if (!attrs.count("lemma"))
                attrs["lemma"] = attrs["form"];
            if (!attrs.count("upos")) attrs["upos"] = "_";
        } else {
            std::string form = cols[0];
            attrs["form"] = form;

            // Heuristic for UD-style VRT (form, id, lemma, UPOS, ...):
            auto looks_like_upos = [](const std::string& s) {
                if (s.empty()) return false;
                for (char c : s)
                    if (!(c >= 'A' && c <= 'Z')) return false;
                return true;
            };

            if (cols.size() >= 4 && looks_like_upos(cols[3])) {
                // form, id, lemma, upos, ...
                std::string lemma = cols[2];
                std::string upos  = cols[3];
                attrs["lemma"] = lemma.empty() ? form : lemma;
                attrs["upos"]  = upos.empty()  ? "_"  : upos;
            } else if (cols.size() >= 3) {
                // form, lemma, upos
                std::string lemma = cols[1];
                std::string upos  = cols[2];
                attrs["lemma"] = lemma.empty() ? form : lemma;
                attrs["upos"]  = upos.empty()  ? "_"  : upos;
            } else if (cols.size() == 2) {
                std::string lemma = cols[1];
                attrs["lemma"] = lemma.empty() ? form : lemma;
                attrs["upos"]  = "_";
            } else {
                attrs["lemma"] = form;
                attrs["upos"]  = "_";
            }
        }
        if (!in_sentence) in_sentence = true;
        builder_.add_token(attrs, -1);  // no dependency info
    }
    if (in_sentence) builder_.end_sentence();

    // Close any open regions at EOF
    for (auto& r : vrt_region_stack_) {
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= r.start)
            builder_.add_region(r.type, r.start, end, r.attrs);
    }
    vrt_region_stack_.clear();
}

// ── JSONL reader (flexicorp integration) ───────────────────────────────────
//
// This is a minimal, streaming JSONL reader that understands the subset of
// events described in dev/PANDO-INDEX-INTEGRATION.md. It is NOT a general
// JSON parser; it assumes well-formed, flat objects with string values for
// the fields we care about.
//
// Supported events:
//   { "type": "token", ... }
//   { "type": "sentence_end" }
//   { "type": "region_start", ... }
//   { "type": "region_end", ... }
//   { "type": "region", "struct": "err", "spans": [[a,b],[c,d]], "attrs": {...} }
//   — for token_groups (e.g. err), one event can list disjoint spans; same attrs apply.
//   token events may carry "deps": "4:nsubj|7:nsubj:xsubj" (CoNLL-U DEPS, heads numbered as "head" is): the
//   enhanced dependencies, stored as the relation layer `deps`.
//   { "type": "relation", "layer": "rst", "source": 15, "target": 8, "label": "causal-cause" }
//   — an edge of a pointer layer declared in the header ("relations": {"rst": "pointer,label=rel,scope=doc"}),
//   corpus positions; equivalence layers are declared there too ("coref": "equivalence,attr=coref").
//   Header token_group_prop_attrs: { "err": ["code"] } + positional err_code + multivalue err_code
//   indexes group attrs (e.g. code → err_code) on tokens for MV search.
//
// For now, region events are optional; when present, they are interpreted
// as implicit [start = current corpus_size, end = corpus_size-1] spans.

namespace {

// JSON strings with their escapes (\" \\ \/ \b \f \n \r \t \uXXXX, surrogate pairs): a value may hold quotes, braces
// and commas (a token `"`, a sentence text quoting someone), so every scan below steps over whole strings.

// the closing quote of the string opening at q1 (npos if unterminated)
static size_t json_string_end(const std::string& s, size_t q1) {
    for (size_t i = q1 + 1; i < s.size(); ++i) {
        if (s[i] == '\\') ++i;
        else if (s[i] == '"') return i;
    }
    return std::string::npos;
}

static void json_append_utf8(std::string& out, uint32_t cp) {
    if (cp < 0x80) out += static_cast<char>(cp);
    else if (cp < 0x800) {
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

// the string between quotes q1 and q2, unescaped
static std::string json_unescape(const std::string& s, size_t q1, size_t q2) {
    std::string out;
    out.reserve(q2 - q1);
    auto hex4 = [&](size_t at, uint32_t& v) {
        if (at + 4 > q2) return false;
        v = 0;
        for (size_t k = at; k < at + 4; ++k) {
            const char c = s[k];
            v <<= 4;
            if (c >= '0' && c <= '9') v |= static_cast<uint32_t>(c - '0');
            else if (c >= 'a' && c <= 'f') v |= static_cast<uint32_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') v |= static_cast<uint32_t>(c - 'A' + 10);
            else return false;
        }
        return true;
    };
    for (size_t i = q1 + 1; i < q2; ++i) {
        if (s[i] != '\\' || i + 1 >= q2) {
            out += s[i];
            continue;
        }
        const char e = s[++i];
        switch (e) {
            case 'b': out += '\b'; break;
            case 'f': out += '\f'; break;
            case 'n': out += '\n'; break;
            case 'r': out += '\r'; break;
            case 't': out += '\t'; break;
            case 'u': {
                uint32_t cp = 0, lo = 0;
                if (!hex4(i + 1, cp)) { out += "\\u"; break; }
                i += 4;
                if (cp >= 0xD800 && cp < 0xDC00 && i + 6 < q2 && s[i + 1] == '\\' && s[i + 2] == 'u' && hex4(i + 3, lo) &&
                    lo >= 0xDC00 && lo < 0xE000) {
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                    i += 6;
                }
                json_append_utf8(out, cp);
                break;
            }
            default: out += e;   // \" \\ \/
        }
    }
    return out;
}

static size_t json_skip_ws(const std::string& s, size_t p) {
    while (p < s.size() && (s[p] == ' ' || s[p] == '\t')) ++p;
    return p;
}

// one past the value starting at p (a string, an object or array with whatever strings they hold, a scalar)
static size_t json_value_end(const std::string& s, size_t p) {
    if (p >= s.size()) return p;
    if (s[p] == '"') {
        const size_t q = json_string_end(s, p);
        return q == std::string::npos ? s.size() : q + 1;
    }
    if (s[p] == '{' || s[p] == '[') {
        int depth = 0;
        for (size_t i = p; i < s.size(); ++i) {
            if (s[i] == '"') {
                i = json_string_end(s, i);
                if (i == std::string::npos) return s.size();
            } else if (s[i] == '{' || s[i] == '[') ++depth;
            else if ((s[i] == '}' || s[i] == ']') && --depth == 0) return i + 1;
        }
        return s.size();
    }
    while (p < s.size() && s[p] != ',' && s[p] != '}' && s[p] != ']') ++p;
    return p;
}

// Walk the members of the object opening at `brace`: f(key, value start) for each, returning false to stop.
template <typename F>
static void json_for_each_member(const std::string& s, size_t brace, F&& f) {
    size_t p = brace + 1;
    while (true) {
        p = json_skip_ws(s, p);
        while (p < s.size() && (s[p] == ',' || s[p] == ' ' || s[p] == '\t')) ++p;
        if (p >= s.size() || s[p] != '"') return;
        const size_t kq2 = json_string_end(s, p);
        if (kq2 == std::string::npos) return;
        std::string key = json_unescape(s, p, kq2);
        p = json_skip_ws(s, kq2 + 1);
        if (p >= s.size() || s[p] != ':') return;
        p = json_skip_ws(s, p + 1);
        if (!f(key, p)) return;
        p = json_value_end(s, p);
    }
}

// start of the value of the line's top-level member `key` (npos if absent)
static size_t json_find_member(const std::string& line, const std::string& key) {
    const size_t brace = line.find('{');
    if (brace == std::string::npos) return std::string::npos;
    size_t at = std::string::npos;
    json_for_each_member(line, brace, [&](const std::string& k, size_t v) {
        if (k != key) return true;
        at = v;
        return false;
    });
    return at;
}

static std::string json_get_string(const std::string& line,
                                   const std::string& key) {
    const size_t v = json_find_member(line, key);
    if (v == std::string::npos || v >= line.size() || line[v] != '"') return {};
    const size_t q2 = json_string_end(line, v);
    if (q2 == std::string::npos) return {};
    return json_unescape(line, v, q2);
}

// the string members of the object `key` ("attrs": {"id": "s1", ...}); other members are skipped
static void json_get_attrs_object(const std::string& line,
                                  const std::string& key,
                                  std::vector<std::pair<std::string, std::string>>& out) {
    const size_t v = json_find_member(line, key);
    if (v == std::string::npos || v >= line.size() || line[v] != '{') return;
    json_for_each_member(line, v, [&](const std::string& k, size_t p) {
        if (p < line.size() && line[p] == '"') {
            const size_t q2 = json_string_end(line, p);
            if (q2 == std::string::npos) return false;
            out.push_back({k, json_unescape(line, p, q2)});
        }
        return true;
    });
}

} // namespace

// ── JSONL v2 helpers ────────────────────────────────────────────────────

// Extract a JSON array of strings:  "key": ["a","b","c"]
// the strings of the array starting at p
static std::vector<std::string> json_strings_of_array(const std::string& s, size_t p) {
    std::vector<std::string> out;
    if (p >= s.size() || s[p] != '[') return out;
    const size_t end = json_value_end(s, p);
    for (size_t i = p + 1; i < end; ++i) {
        if (s[i] != '"') continue;
        const size_t q2 = json_string_end(s, i);
        if (q2 == std::string::npos) break;
        out.push_back(json_unescape(s, i, q2));
        i = q2;
    }
    return out;
}

static std::vector<std::string> json_get_string_array(const std::string& line,
                                                       const std::string& key) {
    const size_t v = json_find_member(line, key);
    if (v == std::string::npos) return {};
    return json_strings_of_array(line, v);
}

// "spans": [[10,12],[40,40]] — disjoint token spans for one token_group region (JSONL v2).
static std::vector<std::pair<CorpusPos, CorpusPos>> json_get_spans_array(
    const std::string& line, const std::string& key) {
    std::vector<std::pair<CorpusPos, CorpusPos>> out;
    size_t p = json_find_member(line, key);
    if (p == std::string::npos || p >= line.size() || line[p] != '[') return out;
    ++p;
    while (p < line.size()) {
        while (p < line.size() && (line[p] == ' ' || line[p] == '\t' || line[p] == ',')) ++p;
        if (p < line.size() && line[p] == ']') break;
        if (p >= line.size() || line[p] != '[') return {};
        ++p;
        while (p < line.size() && (line[p] == ' ' || line[p] == '\t')) ++p;
        size_t q = p;
        while (q < line.size() && line[q] >= '0' && line[q] <= '9') ++q;
        if (q == p) return {};
        CorpusPos a = static_cast<CorpusPos>(std::stoll(line.substr(p, q - p)));
        p = q;
        while (p < line.size() && (line[p] == ' ' || line[p] == '\t')) ++p;
        if (p >= line.size() || line[p] != ',') return {};
        ++p;
        while (p < line.size() && (line[p] == ' ' || line[p] == '\t')) ++p;
        q = p;
        while (q < line.size() && line[q] >= '0' && line[q] <= '9') ++q;
        if (q == p) return {};
        CorpusPos b = static_cast<CorpusPos>(std::stoll(line.substr(p, q - p)));
        p = q;
        while (p < line.size() && (line[p] == ' ' || line[p] == '\t')) ++p;
        if (p >= line.size() || line[p] != ']') return {};
        ++p;
        out.push_back({a, b});
    }
    return out;
}

// "token_group_prop_attrs": { "err": ["code","severity"] }
static std::unordered_map<std::string, std::vector<std::string>> json_get_object_string_arrays(
    const std::string& line, const std::string& key) {
    std::unordered_map<std::string, std::vector<std::string>> out;
    const size_t v = json_find_member(line, key);
    if (v == std::string::npos || v >= line.size() || line[v] != '{') return out;
    json_for_each_member(line, v, [&](const std::string& k, size_t p) {
        if (p < line.size() && line[p] == '[') out[k] = json_strings_of_array(line, p);
        return true;
    });
    return out;
}

static bool json_get_bool(const std::string& line, const std::string& key, bool dflt) {
    const size_t p = json_find_member(line, key);
    if (p < line.size() && line[p] == 't') return true;
    if (p < line.size() && line[p] == 'f') return false;
    return dflt;
}

// Extract all top-level "key": "value" pairs from a JSON line (string values only).
static void json_get_all_string_pairs(const std::string& line,
                                       std::vector<std::pair<std::string, std::string>>& out) {
    const size_t brace = line.find('{');
    if (brace == std::string::npos) return;
    json_for_each_member(line, brace, [&](const std::string& k, size_t p) {
        if (p < line.size() && line[p] == '"') {
            const size_t q2 = json_string_end(line, p);
            if (q2 == std::string::npos) return false;
            out.push_back({k, json_unescape(line, p, q2)});
        }
        return true;
    });
}

// ── JSONL reader ────────────────────────────────────────────────────────

void CorpusBuilder::read_jsonl(const std::string& path, CorpusPos* overlay_expected_size) {
    // Overlay mode: caller passed an expected main-corpus size and the
    // builder was constructed in standoff-only mode. In this mode the JSONL
    // stream must be header + "region" events only (no token / sentence /
    // region_start / region_end lines); see USER-OVERLAY-ANNOTATIONS.md.
    const bool in_overlay_mode = overlay_expected_size != nullptr;
    if (in_overlay_mode && !overlay_standoff_only_)
        throw std::runtime_error("read_jsonl with overlay size requires CorpusBuilder(output_dir, true)");

    std::istream* in_ptr = nullptr;
    std::ifstream file;
    if (path == "-") {
        in_ptr = &std::cin;
    } else {
        file.open(path);
        if (!file) throw std::runtime_error("Cannot open " + path);
        in_ptr = &file;
    }
    std::istream& in = *in_ptr;

    std::string line;

    // ── JSONL v2 header state ──────────────────────────────────────────
    bool have_header = false;
    std::vector<std::string> positional_list;        // ordered, for compact mode
    std::unordered_set<std::string> positional_set;  // fast lookup, for verbose mode
    // Reserved token keys that are not positional attributes
    static const std::unordered_set<std::string> reserved_keys{
        "type", "tok_id", "head_tok_id", "head", "deps", "sent_id", "doc_id", "v", "tok_pos"
    };

    // ── Common state ───────────────────────────────────────────────────
    struct PendingToken {
        std::unordered_map<std::string, std::string> attrs;
        std::string tok_id;
        std::string head_tok_id;
        long long explicit_head = 0; // 0 = root
        std::string deps;             // enhanced dependencies, CoNLL-U DEPS (`4:nsubj|7:nsubj:xsubj`); empty = none given
        bool has_deps = false;
    };
    std::vector<PendingToken> sentence_buf;

    struct JsonRegion {
        std::string struct_name;   // the actual struct type (e.g. "s", "text")
        CorpusPos start = 0;
        std::vector<std::pair<std::string, std::string>> attrs;
    };
    // Keyed by region_id when present, else by struct name (v1-compatible).
    std::unordered_map<std::string, JsonRegion> open_regions;

    auto get_num_field = [](const std::string& ln, const std::string& key) -> long long {
        const size_t p = json_find_member(ln, key);
        if (p == std::string::npos) return 0;
        size_t q = p;
        while (q < ln.size() && (ln[q] >= '0' && ln[q] <= '9')) ++q;
        if (q == p) return 0;
        return std::stoll(ln.substr(p, q - p));
    };

    auto flush_sentence = [&](const std::vector<std::pair<std::string, std::string>>& sent_attrs
                              = std::vector<std::pair<std::string, std::string>>{}) {
        if (sentence_buf.empty()) return;
        std::unordered_map<std::string, int> tok_id_to_local;
        tok_id_to_local.reserve(sentence_buf.size());
        for (size_t i = 0; i < sentence_buf.size(); ++i) {
            if (!sentence_buf[i].tok_id.empty())
                tok_id_to_local[sentence_buf[i].tok_id] = static_cast<int>(i);
        }
        for (size_t i = 0; i < sentence_buf.size(); ++i) {
            auto& pt = sentence_buf[i];
            int sentence_head_id = 0;
            if (pt.explicit_head > 0) {
                sentence_head_id = static_cast<int>(pt.explicit_head);
            } else if (!pt.head_tok_id.empty()) {
                auto it = tok_id_to_local.find(pt.head_tok_id);
                if (it != tok_id_to_local.end())
                    sentence_head_id = it->second + 1;
            }
            if (pt.has_deps) builder_.set_token_deps(pt.deps);
            builder_.add_token(pt.attrs, sentence_head_id);
        }
        builder_.end_sentence(sent_attrs);
        sentence_buf.clear();
    };

    // ── Parse region events ────────────────────────────────────────────
    // In v2 mode, "s" regions trigger flush_sentence() (dependency resolution).
    // In v1 mode, "s" regions are just ordinary regions (sentence_end handles flush).

    auto emit_struct_span = [&](const std::string& sname, CorpusPos start, CorpusPos end,
                                const std::vector<std::pair<std::string, std::string>>& attrs) {
        if (builder_.is_token_group_struct(sname))
            builder_.apply_token_group_annotation(sname, start, end, attrs);
        else if (attrs.empty())
            builder_.add_region(sname, start, end);
        else
            builder_.add_region(sname, start, end, attrs);
    };

    auto handle_region_start = [&](const std::string& ln) {
        std::string sname = json_get_string(ln, "struct");
        if (sname.empty()) return;
        std::string rid = json_get_string(ln, "region_id");
        std::string key = rid.empty() ? sname : rid;
        JsonRegion r;
        r.struct_name = sname;
        r.start = builder_.corpus_size();
        json_get_attrs_object(ln, "attrs", r.attrs);
        open_regions[key] = std::move(r);
    };

    auto handle_region_end = [&](const std::string& ln) {
        std::string sname = json_get_string(ln, "struct");
        std::string rid = json_get_string(ln, "region_id");

        // Look up by region_id first, then fall back to struct name
        auto it = open_regions.end();
        if (!rid.empty())
            it = open_regions.find(rid);
        if (it == open_regions.end() && !sname.empty())
            it = open_regions.find(sname);
        if (it == open_regions.end()) return;

        const std::string& actual_struct = it->second.struct_name.empty()
                                           ? sname : it->second.struct_name;

        // In v2 mode, closing an "s" region finalizes the sentence buffer and uses
        // the region attrs as sentence attrs; avoid adding a duplicate s span row.
        if (have_header && actual_struct == "s") {
            flush_sentence(it->second.attrs);
            open_regions.erase(it);
            return;
        }
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= it->second.start)
            emit_struct_span(actual_struct, it->second.start, end, it->second.attrs);
        open_regions.erase(it);
    };

    auto get_num_from_line = [](const std::string& ln, const std::string& key) -> CorpusPos {
        const size_t p = json_find_member(ln, key);
        if (p == std::string::npos) return -1;
        size_t q = p;
        while (q < ln.size() && (ln[q] >= '0' && ln[q] <= '9')) ++q;
        if (q == p) return -1;
        return static_cast<CorpusPos>(std::stoll(ln.substr(p, q - p)));
    };

    auto handle_region = [&](const std::string& ln) {
        std::string sname = json_get_string(ln, "struct");
        if (sname.empty()) return;
        std::vector<std::pair<CorpusPos, CorpusPos>> spans = json_get_spans_array(ln, "spans");
        if (!spans.empty()) {
            if (!builder_.is_token_group_struct(sname)) {
                static bool warned_spans = false;
                if (!warned_spans) {
                    std::cerr << "Warning: \"spans\" on region is only supported for token_groups "
                                 "(declared in header); ignoring spans for struct '" << sname
                              << "'.\n";
                    warned_spans = true;
                }
                spans.clear();
            } else {
                std::vector<std::pair<std::string, std::string>> attrs;
                json_get_attrs_object(ln, "attrs", attrs);
                for (const auto& ab : spans)
                    emit_struct_span(sname, ab.first, ab.second, attrs);
                return;
            }
        }
        CorpusPos start = builder_.corpus_size();
        CorpusPos end   = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        CorpusPos sp = get_num_from_line(ln, "start_pos");
        CorpusPos ep = get_num_from_line(ln, "end_pos");
        if (sp >= 0) start = sp;
        if (ep >= 0) end = ep;
        std::vector<std::pair<std::string, std::string>> attrs;
        json_get_attrs_object(ln, "attrs", attrs);
        // In v2 mode, a single-shot "s" region finalizes the sentence buffer and uses
        // attrs on that event as sentence attrs. Sentence span coordinates come from
        // buffered tokens, not from event start/end (which may be 1-based in exports).
        // Warn if the region appears to be placed late (far behind the token stream),
        // which indicates the writer is deferring sentence regions to the end of file.
        if (have_header && sname == "s") {
            CorpusPos cur = builder_.corpus_size();
            if (ep >= 0 && cur > 0 && static_cast<CorpusPos>(cur - 1) > end + 1000) {
                static bool warned_late_s = false;
                if (!warned_late_s) {
                    std::cerr << "Warning: single-shot 's' region (end_pos=" << end
                              << ") is far behind current position (" << cur
                              << "). Sentence regions should appear inline, "
                              << "immediately after their last token.\n";
                    warned_late_s = true;
                }
            }
            if (!sentence_buf.empty()) {
                CorpusPos expected_start = cur;
                CorpusPos expected_end = cur + static_cast<CorpusPos>(sentence_buf.size()) - 1;
                if (sp >= 0 && ep >= 0 && (sp != expected_start || ep != expected_end)) {
                    static bool warned_mismatch_s = false;
                    if (!warned_mismatch_s) {
                        std::cerr << "Warning: single-shot 's' coordinates (" << sp << ".." << ep
                                  << ") do not match buffered sentence tokens (" << expected_start
                                  << ".." << expected_end << "); using buffered token boundaries.\n";
                        warned_mismatch_s = true;
                    }
                }
            }
            flush_sentence(attrs);
            return;
        }
        // Accept both normal regions (end >= start) and zero-width regions (start > end).
        // Zero-width regions use the convention start_pos > end_pos to signal zero width.
        // StreamingBuilder::detect_structure_mode already handles them.
        emit_struct_span(sname, start, end, attrs);
    };

    // ── Process header (first non-empty line) if present ───────────────
    auto try_parse_header = [&](const std::string& ln) -> bool {
        std::string t = json_get_string(ln, "type");
        if (t != "header") return false;

        // positional attrs
        positional_list = json_get_string_array(ln, "positional");
        if (positional_list.empty()) {
            std::cerr << "JSONL v2 header has no positional attributes; falling back to v1.\n";
            return false;
        }
        for (const auto& a : positional_list)
            positional_set.insert(a);

        // split_feats: OR with current value so CLI --split-feats (set before read_jsonl)
        // is not overwritten when the header declares "split_feats": false.
        split_feats_ = split_feats_ || json_get_bool(ln, "split_feats", false);

        // default_within
        std::string dw = json_get_string(ln, "default_within");
        if (!dw.empty()) builder_.set_default_within(dw);

        // structure mode declarations
        for (const auto& s : json_get_string_array(ln, "nested"))
            builder_.declare_nested(s);
        for (const auto& s : json_get_string_array(ln, "overlapping"))
            builder_.declare_overlapping(s);
        for (const auto& s : json_get_string_array(ln, "zerowidth"))
            builder_.declare_zerowidth(s);
        // relation layers: "relations": {"rst": "pointer,label=rel,scope=doc", "coref": "equivalence,attr=coref"}
        {
            std::vector<std::pair<std::string, std::string>> rels;
            json_get_attrs_object(ln, "relations", rels);
            for (const auto& [name, spec] : rels) builder_.declare_relation(name, spec);
        }
        std::unordered_set<std::string> multivalue_set;
        for (const auto& s : json_get_string_array(ln, "multivalue")) {
            builder_.declare_multivalue(s);
            multivalue_set.insert(s);
        }

        for (const auto& s : json_get_string_array(ln, "kv_pipe")) {
            if (positional_set.count(s) == 0)
                throw std::runtime_error(
                        "JSONL header: kv_pipe attribute '" + s +
                        "' must be listed in positional.");
            if (multivalue_set.count(s))
                throw std::runtime_error(
                        "JSONL header: attribute '" + s +
                        "' cannot be both multivalue and kv_pipe.");
            builder_.declare_kv_pipe(s);
        }

        // REQ-TOKEN-GROUPS: TEITOK-style standoff — region type does not get .rgn rows.
        {
            std::vector<std::string> tgs = json_get_string_array(ln, "token_groups");
            std::vector<std::string> tgms = json_get_string_array(ln, "token_group_membership");
            auto tg_props = json_get_object_string_arrays(ln, "token_group_prop_attrs");
            for (size_t i = 0; i < tgs.size(); ++i) {
                std::string memb =
                        (i < tgms.size() && !tgms[i].empty()) ? tgms[i] : (tgs[i] + "_gid");
                std::vector<std::string> prop_keys;
                auto pit = tg_props.find(tgs[i]);
                if (pit != tg_props.end())
                    prop_keys = pit->second;
                // Collision check: derived '<struct>_<key>' columns must not shadow a
                // positional column the user defined for some other purpose. The
                // expected denormalized columns (those *in* prop_keys) are exempt — they
                // are required to appear in `positional` so the MV index can be built.
                std::unordered_set<std::string> expected_derived;
                for (const auto& pk : prop_keys) {
                    if (pk.empty() || pk == "id" || pk == "group_id") continue;
                    expected_derived.insert(tgs[i] + "_" + pk);
                }
                for (const auto& pos_attr : positional_list) {
                    if (pos_attr.size() <= tgs[i].size() + 1) continue;
                    if (pos_attr.compare(0, tgs[i].size(), tgs[i]) != 0) continue;
                    if (pos_attr[tgs[i].size()] != '_') continue;
                    if (expected_derived.count(pos_attr)) continue;
                    if (pos_attr == memb) continue;
                    throw std::runtime_error(
                        "JSONL header: positional column '" + pos_attr +
                        "' collides with the namespace reserved for token-group '" +
                        tgs[i] + "' derived prop columns. Either rename the positional "
                        "column or declare it under token_group_prop_attrs.");
                }
                builder_.declare_token_group_layer(tgs[i], memb, prop_keys);
            }
        }

        return true;
    };

    // ── v2 token handler: supports verbose object mode and compact "v" array mode ──
    auto handle_token_v2 = [&](const std::string& ln) {
        PendingToken pt;
        pt.attrs.reserve(positional_list.size());

        // Check for compact mode: presence of "v" key with array value
        auto compact_vals = json_get_string_array(ln, "v");
        if (!compact_vals.empty()) {
            // Compact mode: "v" array aligned with positional_list
            for (size_t i = 0; i < compact_vals.size() && i < positional_list.size(); ++i) {
                const auto& v = compact_vals[i];
                if (v.empty() || v == "_") continue;
                const auto& attr_name = positional_list[i];
                if (attr_name == "feats") {
                    parse_feats(v, pt.attrs);
                } else {
                    pt.attrs[attr_name] = v;
                }
            }
        } else {
            // Verbose mode: named keys on the JSON object
            std::vector<std::pair<std::string, std::string>> all_pairs;
            all_pairs.reserve(16);
            json_get_all_string_pairs(ln, all_pairs);

            for (auto& [k, v] : all_pairs) {
                if (k == "tok_id") { pt.tok_id = v; continue; }
                if (k == "head_tok_id") { pt.head_tok_id = v; continue; }
                if (reserved_keys.count(k)) continue;
                if (k == "feats") {
                    if (!v.empty()) parse_feats(v, pt.attrs);
                    continue;
                }
                if (positional_set.count(k) && !v.empty())
                    pt.attrs[k] = v;
            }
        }

        // tok_id / head_tok_id / head are always top-level string/number keys
        // (present in both compact and verbose modes)
        if (pt.tok_id.empty())
            pt.tok_id = json_get_string(ln, "tok_id");
        if (pt.head_tok_id.empty())
            pt.head_tok_id = json_get_string(ln, "head_tok_id");
        pt.explicit_head = get_num_field(ln, "head");
        // "deps": CoNLL-U DEPS, heads numbered as `head` is (1-based within the sentence)
        if (json_find_member(ln, "deps") != std::string::npos) {
            pt.deps = json_get_string(ln, "deps");
            pt.has_deps = true;
        }

        sentence_buf.push_back(std::move(pt));
    };

    // ── v1 token handler: hardcoded six fields ─────────────────────────
    auto handle_token_v1 = [&](const std::string& ln) {
        PendingToken pt;
        pt.attrs.reserve(8);

        std::string form   = json_get_string(ln, "form");
        std::string lemma  = json_get_string(ln, "lemma");
        std::string upos   = json_get_string(ln, "upos");
        std::string xpos   = json_get_string(ln, "xpos");
        std::string deprel = json_get_string(ln, "deprel");
        std::string feats  = json_get_string(ln, "feats");

        if (!form.empty())   pt.attrs["form"]   = form;
        if (!lemma.empty())  pt.attrs["lemma"]  = lemma;
        if (!upos.empty())   pt.attrs["upos"]   = upos;
        if (!xpos.empty())   pt.attrs["xpos"]   = xpos;
        if (!deprel.empty()) pt.attrs["deprel"] = deprel;
        if (!feats.empty())  parse_feats(feats, pt.attrs);

        pt.tok_id      = json_get_string(ln, "tok_id");
        pt.head_tok_id = json_get_string(ln, "head_tok_id");
        pt.explicit_head = get_num_field(ln, "head");

        sentence_buf.push_back(std::move(pt));
    };

    // ── Main read loop ─────────────────────────────────────────────────
    bool first_line = true;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        if (first_line) {
            first_line = false;
            if (try_parse_header(line)) {
                have_header = true;
                std::cerr << "JSONL v2 header: " << positional_set.size()
                          << " positional attributes.\n";
                if (overlay_expected_size) {
                    long long tc = get_num_field(line, "token_count");
                    if (tc > 0 &&
                        static_cast<CorpusPos>(tc) != *overlay_expected_size) {
                        throw std::runtime_error(
                                "JSONL overlay header token_count=" + std::to_string(tc) +
                                " does not match main corpus size=" +
                                std::to_string(
                                        static_cast<unsigned long long>(*overlay_expected_size)));
                    }
                    if (!builder_.has_token_group_layers())
                        throw std::runtime_error(
                                "JSONL overlay: header must declare token_groups=…");
                    builder_.bootstrap_overlay_corpus_size(*overlay_expected_size);
                }
                continue;  // header consumed, next line
            }
            if (in_overlay_mode)
                throw std::runtime_error(
                        "JSONL overlay: first line must be a v2 header with token_groups");
            // Not a header — fall through to process as data line in v1 mode
            std::cerr << "No JSONL header found; using v1 compatibility mode.\n";
        }

        std::string t = json_get_string(line, "type");
        if (t == "token") {
            if (in_overlay_mode)
                throw std::runtime_error("JSONL overlay: token lines are not allowed");
            if (have_header) handle_token_v2(line);
            else             handle_token_v1(line);
        } else if (t == "sentence_end") {
            // v1 only: explicit sentence boundary.  v2 uses "s" regions instead.
            if (!in_overlay_mode && !have_header) flush_sentence();
        } else if (t == "region_start") {
            if (!in_overlay_mode) handle_region_start(line);
        } else if (t == "region_end") {
            if (!in_overlay_mode) handle_region_end(line);
        } else if (t == "region") {
            handle_region(line);
        } else if (t == "relation") {
            // an edge of a pointer layer declared in the header: corpus positions (0-based, as region start_pos)
            const long long src = get_num_field(line, "source"), tgt = get_num_field(line, "target");
            const std::string layer = json_get_string(line, "layer");
            if (layer.empty() || json_find_member(line, "source") == std::string::npos ||
                json_find_member(line, "target") == std::string::npos)
                throw std::runtime_error("JSONL relation event needs \"layer\", \"source\" and \"target\": " + line);
            builder_.add_relation_edge(layer, static_cast<CorpusPos>(src), static_cast<CorpusPos>(tgt),
                                       json_get_string(line, "label"));
        }
    }

    // Close any open sentence at EOF
    flush_sentence();

    // Close any open regions at EOF
    for (auto& kv : open_regions) {
        const std::string& sname = kv.second.struct_name.empty()
                                   ? kv.first : kv.second.struct_name;
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= kv.second.start)
            emit_struct_span(sname, kv.second.start, end, kv.second.attrs);
    }
    open_regions.clear();
}

void CorpusBuilder::finalize() {
    builder_.finalize();
}

} // namespace pando
