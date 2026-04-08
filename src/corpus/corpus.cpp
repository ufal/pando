#include "corpus/corpus.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace manatree {

// Minimal JSON-like parser for corpus.info (avoids external dependency).
// Format:  key=value lines, with list values comma-separated.
static CorpusInfo read_info(const std::string& path) {
    CorpusInfo info;
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot open " + path);

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);

        if (key == "size") {
            info.size = static_cast<CorpusPos>(std::stol(val));
        } else if (key == "positional") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.positional_attrs.push_back(tok);
        } else if (key == "structural") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.structural_attrs.push_back(tok);
        } else if (key == "region_attrs") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.region_attrs.push_back(tok);
        } else if (key == "default_within") {
            info.default_within = val;
        } else if (key == "nested") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.nested_structs.push_back(tok);
        } else if (key == "overlapping") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.overlapping_structs.push_back(tok);
        } else if (key == "zerowidth") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.zerowidth_structs.push_back(tok);
        } else if (key == "multivalue") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.multivalue_attrs.push_back(tok);
        } else if (key == "token_groups") {
            std::istringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ','))
                if (!tok.empty()) info.token_group_structs.push_back(tok);
        }
    }
    return info;
}

namespace {

std::string hyphenize_token(const std::string& s) {
    std::string o;
    o.reserve(s.size());
    for (char c : s) {
        if (c == '_') o += '-';
        else o += c;
    }
    return o;
}

/// Public merged name: overlay-<layer>-<attr> with underscores in `attr` → hyphens.
std::string overlay_merged_attr(const std::string& layer_id, const std::string& name) {
    return "overlay-" + hyphenize_token(layer_id) + "-" + hyphenize_token(name);
}

std::string overlay_merged_struct(const std::string& layer_id, const std::string& st) {
    return "overlay-" + hyphenize_token(layer_id) + "-" + hyphenize_token(st);
}

std::string read_overlay_layer_id(const std::string& overlay_dir) {
    std::ifstream in(overlay_dir + "/overlay.info");
    if (in) {
        std::string line;
        while (std::getline(in, line)) {
            if (line.rfind("layer_id=", 0) == 0) {
                std::string v = line.substr(9);
                if (!v.empty()) return v;
            }
        }
    }
    namespace fs = std::filesystem;
    fs::path p(overlay_dir);
    if (p.has_filename()) {
        std::string fn = p.filename().string();
        if (!fn.empty()) return fn;
    }
    return "overlay";
}

bool overlay_attr_is_multivalue(const CorpusInfo& oi, const std::string& name) {
    for (const auto& m : oi.multivalue_attrs)
        if (m == name) return true;
    return false;
}

} // namespace

void Corpus::open(const std::string& dir, bool preload,
                  const std::vector<std::string>& overlay_dirs) {
    group_index_path_override_.clear();
    dir_ = dir;
    info_ = read_info(dir + "/corpus.info");

    std::unordered_set<std::string> pos_seen;
    for (const auto& name : info_.positional_attrs) pos_seen.insert(name);

    // Open a positional attribute from `src_dir` (file basename `src_name`)
    // and register it in attrs_ under `registered_name`. Shared between main
    // corpus loading and overlay loading (they differ only in source path and
    // public name). `is_mv` controls whether the MV sidecars are opened.
    // Stage 1 of PANDO-MULTIVALUE-FIELDS: forward MV index sidecar.
    // open_mv_fwd is a no-op when .mv.fwd.idx is absent (older corpora).
    auto open_positional_into = [&](const std::string& src_dir,
                                    const std::string& src_name,
                                    const std::string& registered_name,
                                    bool is_mv) {
        auto pa = std::make_unique<PositionalAttr>();
        pa->open(src_dir + "/" + src_name, info_.size, preload);
        if (is_mv) {
            pa->open_mv(src_dir + "/" + src_name, preload);
            pa->open_mv_fwd(src_dir + "/" + src_name, preload);
        }
        attrs_[registered_name] = std::move(pa);
    };

    for (const auto& name : info_.positional_attrs)
        open_positional_into(dir, name, name, is_multivalue(name));

    for (const auto& name : info_.structural_attrs) {
        auto sa = std::make_unique<StructuralAttr>();
        sa->open(dir + "/" + name + ".rgn", preload);
        std::string prefix = name + "_";
        for (const auto& ra : info_.region_attrs) {
            if (ra.size() > prefix.size() && ra.compare(0, prefix.size(), prefix) == 0) {
                std::string attr_name = ra.substr(prefix.size());
                std::string vpath = dir + "/" + ra + ".val";
                std::string ipath = dir + "/" + ra + ".val.idx";
                std::ifstream probe(vpath);
                if (probe.good())
                    sa->add_region_attr(attr_name, vpath, ipath, preload);
            }
        }
        structs_[name] = std::move(sa);
    }

    if (structs_.count("s")) {
        std::string dep_head = dir + "/dep.head";
        std::ifstream probe(dep_head);
        if (probe.good()) {
            deps_.open(dir, *structs_.at("s"), preload);
            has_deps_ = true;
        }
    }

    for (const std::string& odir : overlay_dirs) {
        CorpusInfo oi = read_info(odir + "/corpus.info");
        if (oi.size != info_.size) {
            throw std::runtime_error("Overlay " + odir + " size=" + std::to_string(oi.size) +
                                     " does not match main corpus size=" +
                                     std::to_string(info_.size));
        }
        const std::string layer = read_overlay_layer_id(odir);
        for (const auto& name : oi.positional_attrs) {
            const std::string merged = overlay_merged_attr(layer, name);
            if (pos_seen.count(merged))
                throw std::runtime_error("Merged overlay attribute already exists: " + merged);
            pos_seen.insert(merged);
            const bool is_mv = overlay_attr_is_multivalue(oi, name);
            open_positional_into(odir, name, merged, is_mv);
            info_.positional_attrs.push_back(merged);
            if (is_mv &&
                std::find(info_.multivalue_attrs.begin(), info_.multivalue_attrs.end(), merged) ==
                    info_.multivalue_attrs.end())
                info_.multivalue_attrs.push_back(merged);
        }
        for (const auto& st : oi.token_group_structs) {
            const std::string ms = overlay_merged_struct(layer, st);
            if (std::find(info_.token_group_structs.begin(), info_.token_group_structs.end(), ms) ==
                info_.token_group_structs.end())
                info_.token_group_structs.push_back(ms);
            group_index_path_override_[ms] = odir + "/groups/" + st + ".jsonl";
        }
    }
}

const PositionalAttr& Corpus::attr(const std::string& name) const {
    auto it = attrs_.find(name);
    if (it == attrs_.end())
        throw std::runtime_error("Unknown attribute: " + name);
    return *it->second;
}

bool Corpus::has_attr(const std::string& name) const {
    return attrs_.count(name) > 0;
}

const StructuralAttr& Corpus::structure(const std::string& name) const {
    auto it = structs_.find(name);
    if (it == structs_.end())
        throw std::runtime_error("Unknown structure: " + name);
    return *it->second;
}

bool Corpus::has_structure(const std::string& name) const {
    return structs_.count(name) > 0;
}

bool Corpus::is_nested(const std::string& name) const {
    for (const auto& s : info_.nested_structs)
        if (s == name) return true;
    return false;
}

bool Corpus::is_overlapping(const std::string& name) const {
    for (const auto& s : info_.overlapping_structs)
        if (s == name) return true;
    return false;
}

bool Corpus::is_zerowidth(const std::string& name) const {
    for (const auto& s : info_.zerowidth_structs)
        if (s == name) return true;
    return false;
}

// ── Phase A: GroupIndex loader (parses groups/<struct>.jsonl) ───────────
//
// The format is line-delimited JSON written by StreamingBuilder::
// write_token_group_indexes(). Strings are escaped only for `"` and `\`,
// so a tiny hand-rolled parser is enough — bringing in a JSON dependency
// just for this would be overkill.

namespace {

// Read a JSON-escaped string starting at the opening '"' index. Sets *next
// to one past the closing '"'. Throws on unterminated input.
std::string parse_json_string(const std::string& s, size_t pos, size_t* next) {
    if (pos >= s.size() || s[pos] != '"')
        throw std::runtime_error("groups jsonl: expected '\"'");
    ++pos;
    std::string out;
    while (pos < s.size() && s[pos] != '"') {
        if (s[pos] == '\\' && pos + 1 < s.size()) {
            out += s[pos + 1];
            pos += 2;
        } else {
            out += s[pos++];
        }
    }
    if (pos >= s.size())
        throw std::runtime_error("groups jsonl: unterminated string");
    *next = pos + 1;
    return out;
}

long long parse_json_int(const std::string& s, size_t pos, size_t* next) {
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) ++pos;
    size_t start = pos;
    if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) ++pos;
    while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') ++pos;
    if (pos == start)
        throw std::runtime_error("groups jsonl: expected number");
    *next = pos;
    return std::stoll(s.substr(start, pos - start));
}

// Find the position of `"key":` in a JSONL line. Returns std::string::npos
// if missing. Cheap-and-cheerful (no nested-string awareness needed for
// our flat schema).
size_t find_key(const std::string& s, const std::string& key) {
    std::string needle = "\"" + key + "\":";
    return s.find(needle);
}

} // namespace

void GroupIndex::load(const std::string& path) {
    std::ifstream in(path);
    if (!in) { loaded_ = true; return; }
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        GroupRecord rec;

        size_t kpos = find_key(line, "gid");
        if (kpos == std::string::npos)
            throw std::runtime_error(path + ": record missing 'gid'");
        size_t next;
        rec.gid = parse_json_string(line, kpos + 6, &next);

        kpos = find_key(line, "first");
        if (kpos != std::string::npos)
            rec.first = static_cast<CorpusPos>(
                parse_json_int(line, kpos + 8, &next));

        kpos = find_key(line, "last");
        if (kpos != std::string::npos)
            rec.last = static_cast<CorpusPos>(
                parse_json_int(line, kpos + 7, &next));

        kpos = find_key(line, "spans");
        if (kpos != std::string::npos) {
            size_t p = kpos + 8;  // past "spans":
            if (p < line.size() && line[p] == '[') {
                ++p;
                while (p < line.size() && line[p] != ']') {
                    while (p < line.size() && (line[p] == ' ' || line[p] == ',')) ++p;
                    if (p >= line.size() || line[p] != '[') break;
                    ++p;
                    long long a = parse_json_int(line, p, &p);
                    while (p < line.size() && (line[p] == ' ' || line[p] == ',')) ++p;
                    long long b = parse_json_int(line, p, &p);
                    while (p < line.size() && line[p] != ']') ++p;
                    if (p < line.size()) ++p;  // past ']'
                    rec.spans.emplace_back(static_cast<CorpusPos>(a),
                                           static_cast<CorpusPos>(b));
                }
            }
        }

        kpos = find_key(line, "props");
        if (kpos != std::string::npos) {
            size_t p = kpos + 8;  // past "props":
            if (p < line.size() && line[p] == '{') {
                ++p;
                while (p < line.size() && line[p] != '}') {
                    while (p < line.size() && (line[p] == ' ' || line[p] == ',')) ++p;
                    if (p >= line.size() || line[p] != '"') break;
                    std::string k = parse_json_string(line, p, &p);
                    while (p < line.size() && (line[p] == ' ' || line[p] == ':')) ++p;
                    std::string v = parse_json_string(line, p, &p);
                    rec.props.emplace_back(std::move(k), std::move(v));
                }
            }
        }

        records_.push_back(std::move(rec));
    }
    loaded_ = true;
}

const GroupIndex& Corpus::group_index(const std::string& name) const {
    auto it = group_indexes_.find(name);
    if (it != group_indexes_.end()) return *it->second;
    auto gi = std::make_unique<GroupIndex>();
    std::string path = dir_ + "/groups/" + name + ".jsonl";
    auto pit = group_index_path_override_.find(name);
    if (pit != group_index_path_override_.end()) path = pit->second;
    gi->load(path);
    auto& ref = *gi;
    group_indexes_[name] = std::move(gi);
    return ref;
}

bool Corpus::is_multivalue(const std::string& name) const {
    // Support both bare "wsd" and named-token-qualified "a.wsd" forms.
    // Strip the prefix up to the first '.' if present.
    std::string_view bare = name;
    auto dot = bare.find('.');
    if (dot != std::string_view::npos)
        bare = bare.substr(dot + 1);
    for (const auto& s : info_.multivalue_attrs)
        if (bare == s) return true;
    return false;
}

} // namespace manatree
