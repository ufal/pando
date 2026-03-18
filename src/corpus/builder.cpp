#include "corpus/builder.h"
#include <fstream>
#include <stdexcept>
#include <cstdlib>
#include <sstream>
#include <iostream>

namespace manatree {

CorpusBuilder::CorpusBuilder(const std::string& output_dir)
    : builder_(output_dir) {}

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
                attrs["feats_" + feats_str.substr(start, eq - start)] =
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

    while (std::getline(in, line)) {
        if (line.empty()) {
            if (in_sentence_) {
                builder_.end_sentence();
                in_sentence_ = false;
            }
            continue;
        }
        if (line[0] == '#') {
            // CoNLL-U comments: capture doc/paragraph IDs etc.
            // Examples:
            //   # newdoc id = zakon.iso-003
            //   # newpar id = zakon.iso-003-p18
            //   # sent_id = zakon.iso-003-p18s5
            //   # text = ...
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
                // trim spaces
                while (!val.empty() && val.front() == ' ') val.remove_prefix(1);
                while (!val.empty() && (val.back() == ' ' || val.back() == '\r')) val.remove_suffix(1);
                return std::string(val);
            };

            if (starts_with("newdoc id")) {
                // Close previous doc region if any
                if (has_doc_region_) {
                    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
                    if (end >= doc_start_)
                        builder_.add_region("text", doc_start_, end,
                                            std::vector<std::pair<std::string, std::string>>{{"id", doc_id_}});
                }
                doc_id_ = parse_id_after_eq();
                doc_start_ = builder_.corpus_size();
                has_doc_region_ = true;
            } else if (starts_with("newpar id")) {
                // Close previous paragraph if any
                if (has_par_region_) {
                    CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
                    if (end >= par_start_)
                        builder_.add_region("par", par_start_, end,
                                            std::vector<std::pair<std::string, std::string>>{{"id", par_id_}});
                }
                par_id_ = parse_id_after_eq();
                par_start_ = builder_.corpus_size();
                has_par_region_ = true;
            }
            continue;
        }

        // Fast tab-split: find the 10 CoNLL-U fields without allocating
        // We need fields 0-7 (ID, FORM, LEMMA, UPOS, XPOS, FEATS, HEAD, DEPREL)
        size_t tab[8];  // positions of first 8 tab characters
        size_t pos = 0;
        int found = 0;
        while (found < 8 && pos < line.size()) {
            size_t t = line.find('\t', pos);
            if (t == std::string::npos) break;
            tab[found++] = t;
            pos = t + 1;
        }
        if (found < 7) continue;  // malformed line

        // Field boundaries: field[i] starts at (i==0 ? 0 : tab[i-1]+1), ends at tab[i]
        auto field = [&](int i) -> std::string_view {
            size_t s = (i == 0) ? 0 : tab[i - 1] + 1;
            size_t e = (i < found) ? tab[i] : line.size();
            return std::string_view(line.data() + s, e - s);
        };

        auto id_sv = field(0);
        // Skip multi-word tokens (e.g. "1-2") and empty words (e.g. "1.1")
        for (char c : id_sv) {
            if (c == '-' || c == '.') goto next_line;
        }

        {
            attrs.clear();
            attrs.emplace("form",   std::string(field(1)));
            attrs.emplace("lemma",  std::string(field(2)));
            attrs.emplace("upos",   std::string(field(3)));
            attrs.emplace("xpos",   std::string(field(4)));
            attrs.emplace("deprel", std::string(field(7)));

            parse_feats(std::string(field(5)), attrs);

            int head_id = 0;
            auto head_sv = field(6);
            for (char c : head_sv) {
                if (c < '0' || c > '9') { head_id = -1; break; }
                head_id = head_id * 10 + (c - '0');
            }

            builder_.add_token(attrs, head_id);
            in_sentence_ = true;
        }
        next_line:;
    }

    if (in_sentence_) {
        builder_.end_sentence();
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
    if (has_doc_region_) {
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= doc_start_)
            builder_.add_region("text", doc_start_, end,
                                std::vector<std::pair<std::string, std::string>>{{"id", doc_id_}});
        has_doc_region_ = false;
    }
}

void CorpusBuilder::read_vertical(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot open " + path);

    std::string line;
    std::unordered_map<std::string, std::string> attrs;
    attrs.reserve(8);
    bool in_sentence = false;

    while (std::getline(in, line)) {
        // Trim trailing CR
        if (!line.empty() && line.back() == '\r') line.pop_back();

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
//
// For now, region events are optional; when present, they are interpreted
// as implicit [start = current corpus_size, end = corpus_size-1] spans.

namespace {

static std::string json_get_string(const std::string& line,
                                   const std::string& key) {
    std::string pattern = "\"" + key + "\"";
    size_t kpos = line.find(pattern);
    if (kpos == std::string::npos) return {};
    size_t colon = line.find(':', kpos + pattern.size());
    if (colon == std::string::npos) return {};
    size_t q1 = line.find('"', colon + 1);
    if (q1 == std::string::npos) return {};
    size_t q2 = line.find('"', q1 + 1);
    if (q2 == std::string::npos) return {};
    return line.substr(q1 + 1, q2 - q1 - 1);
}

static void json_get_attrs_object(const std::string& line,
                                  const std::string& key,
                                  std::vector<std::pair<std::string, std::string>>& out) {
    std::string pattern = "\"" + key + "\"";
    size_t kpos = line.find(pattern);
    if (kpos == std::string::npos) return;
    size_t colon = line.find(':', kpos + pattern.size());
    if (colon == std::string::npos) return;
    size_t brace1 = line.find('{', colon + 1);
    if (brace1 == std::string::npos) return;
    size_t brace2 = line.find('}', brace1 + 1);
    if (brace2 == std::string::npos) return;
    std::string obj = line.substr(brace1 + 1, brace2 - brace1 - 1);

    size_t pos = 0;
    while (pos < obj.size()) {
        size_t kq1 = obj.find('"', pos);
        if (kq1 == std::string::npos) break;
        size_t kq2 = obj.find('"', kq1 + 1);
        if (kq2 == std::string::npos) break;
        std::string k = obj.substr(kq1 + 1, kq2 - kq1 - 1);
        size_t colon2 = obj.find(':', kq2 + 1);
        if (colon2 == std::string::npos) break;
        size_t vq1 = obj.find('"', colon2 + 1);
        if (vq1 == std::string::npos) break;
        size_t vq2 = obj.find('"', vq1 + 1);
        if (vq2 == std::string::npos) break;
        std::string v = obj.substr(vq1 + 1, vq2 - vq1 - 1);
        out.push_back({k, v});
        pos = vq2 + 1;
    }
}

} // namespace

void CorpusBuilder::read_jsonl(const std::string& path) {
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

    struct PendingToken {
        std::unordered_map<std::string, std::string> attrs;
        std::string tok_id;
        std::string head_tok_id;
        long long explicit_head = 0; // 0 = root
    };
    std::vector<PendingToken> sentence_buf;

    // Simple stack for open regions keyed by struct name
    struct JsonRegion {
        CorpusPos start = 0;
        std::vector<std::pair<std::string, std::string>> attrs;
    };
    std::unordered_map<std::string, JsonRegion> open_regions;

    auto get_num_field = [](const std::string& line, const std::string& key) -> long long {
        std::string pattern = "\"" + key + "\"";
        size_t kpos = line.find(pattern);
        if (kpos == std::string::npos) return 0;
        size_t colon = line.find(':', kpos + pattern.size());
        if (colon == std::string::npos) return 0;
        size_t p = colon + 1;
        while (p < line.size() && (line[p] == ' ' || line[p] == '\t')) ++p;
        size_t q = p;
        while (q < line.size() && (line[q] >= '0' && line[q] <= '9')) ++q;
        if (q == p) return 0;
        return std::stoll(line.substr(p, q - p));
    };

    auto flush_sentence = [&]() {
        if (sentence_buf.empty()) return;

        // Build tok_id → local index map
        std::unordered_map<std::string, int> tok_id_to_local;
        tok_id_to_local.reserve(sentence_buf.size());
        for (size_t i = 0; i < sentence_buf.size(); ++i) {
            if (!sentence_buf[i].tok_id.empty())
                tok_id_to_local[sentence_buf[i].tok_id] = static_cast<int>(i);
        }

        // Emit tokens to StreamingBuilder with resolved heads
        for (size_t i = 0; i < sentence_buf.size(); ++i) {
            auto& pt = sentence_buf[i];
            int sentence_head_id = 0; // 0 = root
            if (pt.explicit_head > 0) {
                sentence_head_id = static_cast<int>(pt.explicit_head);
            } else if (!pt.head_tok_id.empty()) {
                auto it = tok_id_to_local.find(pt.head_tok_id);
                if (it != tok_id_to_local.end())
                    sentence_head_id = it->second + 1; // CoNLL-U style (1-based)
            }
            builder_.add_token(pt.attrs, sentence_head_id);
        }

        builder_.end_sentence();
        sentence_buf.clear();
    };

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::string t = json_get_string(line, "type");
        if (t == "token") {
            PendingToken pt;
            pt.attrs.reserve(8);

            std::string form   = json_get_string(line, "form");
            std::string lemma  = json_get_string(line, "lemma");
            std::string upos   = json_get_string(line, "upos");
            std::string xpos   = json_get_string(line, "xpos");
            std::string deprel = json_get_string(line, "deprel");
            std::string feats  = json_get_string(line, "feats");

            if (!form.empty())   pt.attrs["form"]   = form;
            if (!lemma.empty())  pt.attrs["lemma"]  = lemma;
            if (!upos.empty())   pt.attrs["upos"]   = upos;
            if (!xpos.empty())   pt.attrs["xpos"]   = xpos;
            if (!deprel.empty()) pt.attrs["deprel"] = deprel;
            if (!feats.empty())  parse_feats(feats, pt.attrs);

            pt.tok_id      = json_get_string(line, "tok_id");
            pt.head_tok_id = json_get_string(line, "head_tok_id");
            pt.explicit_head = get_num_field(line, "head"); // 0..N

            sentence_buf.push_back(std::move(pt));
        } else if (t == "sentence_end") {
            flush_sentence();
        } else if (t == "region_start") {
            std::string sname = json_get_string(line, "struct");
            if (sname.empty()) continue;
            JsonRegion r;
            r.start = builder_.corpus_size();
            json_get_attrs_object(line, "attrs", r.attrs);
            open_regions[sname] = std::move(r);
        } else if (t == "region_end") {
            std::string sname = json_get_string(line, "struct");
            auto it = open_regions.find(sname);
            if (it == open_regions.end()) continue;
            CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
            if (end >= it->second.start)
                builder_.add_region(sname, it->second.start, end, it->second.attrs);
            open_regions.erase(it);
        } else if (t == "region") {
            // Optional single-shot region with explicit start_pos/end_pos and attrs
            std::string sname = json_get_string(line, "struct");
            if (sname.empty()) continue;
            // Parse start_pos/end_pos as integers if present
            CorpusPos start = builder_.corpus_size();
            CorpusPos end   = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
            // Very minimal numeric parsing
            auto get_num = [&](const std::string& key) -> CorpusPos {
                std::string pattern = "\"" + key + "\"";
                size_t kpos = line.find(pattern);
                if (kpos == std::string::npos) return -1;
                size_t colon = line.find(':', kpos + pattern.size());
                if (colon == std::string::npos) return -1;
                size_t p = colon + 1;
                while (p < line.size() && (line[p] == ' ' || line[p] == '\t')) ++p;
                size_t q = p;
                while (q < line.size() && (line[q] >= '0' && line[q] <= '9')) ++q;
                if (q == p) return -1;
                return static_cast<CorpusPos>(std::stoll(line.substr(p, q - p)));
            };
            CorpusPos sp = get_num("start_pos");
            CorpusPos ep = get_num("end_pos");
            if (sp >= 0) start = sp;
            if (ep >= 0) end = ep;
            std::vector<std::pair<std::string, std::string>> attrs;
            json_get_attrs_object(line, "attrs", attrs);
            if (end >= start)
                builder_.add_region(sname, start, end, attrs);
        }
    }

    // Close any open sentence at EOF
    flush_sentence();

    // Close any open regions at EOF
    for (auto& kv : open_regions) {
        CorpusPos end = builder_.corpus_size() > 0 ? builder_.corpus_size() - 1 : 0;
        if (end >= kv.second.start)
            builder_.add_region(kv.first, kv.second.start, end, kv.second.attrs);
    }
    open_regions.clear();
}

void CorpusBuilder::finalize() {
    builder_.finalize();
}

} // namespace manatree
