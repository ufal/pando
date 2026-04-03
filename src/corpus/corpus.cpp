#include "corpus/corpus.h"
#include <fstream>
#include <sstream>
#include <stdexcept>

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
        }
    }
    return info;
}

void Corpus::open(const std::string& dir, bool preload) {
    dir_ = dir;
    info_ = read_info(dir + "/corpus.info");

    for (const auto& name : info_.positional_attrs) {
        auto pa = std::make_unique<PositionalAttr>();
        pa->open(dir + "/" + name, info_.size, preload);
        if (is_multivalue(name))
            pa->open_mv(dir + "/" + name, preload);
        attrs_[name] = std::move(pa);
    }

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
