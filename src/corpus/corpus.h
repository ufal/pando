#pragma once

#include "core/types.h"
#include "index/positional_attr.h"
#include "index/structural_attr.h"
#include "index/dependency_index.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

namespace manatree {

struct CorpusInfo {
    CorpusPos size = 0;
    std::vector<std::string> positional_attrs;
    std::vector<std::string> structural_attrs;
    std::vector<std::string> region_attrs;   // e.g. "text_id", "text_year" (#8)
    std::string default_within;              // e.g. "text" (#9); empty = none

    // Structure mode metadata (SM-ROAD-1): per-type flags for non-flat structures.
    // A type listed here has the declared (or auto-detected) property.
    std::vector<std::string> nested_structs;      // e.g. "syn"
    std::vector<std::string> overlapping_structs;  // e.g. "entity","error"
    std::vector<std::string> zerowidth_structs;    // e.g. "trace","pause"

    // RG-5f: Attributes declared as pipe-separated multivalue.
    std::vector<std::string> multivalue_attrs;       // e.g. "wsd","vowels"
};

// Loads and provides access to all indexes for a corpus directory.
class Corpus {
public:
    void open(const std::string& dir, bool preload = false);

    CorpusPos size() const { return info_.size; }

    // Positional attributes
    const PositionalAttr& attr(const std::string& name) const;
    bool has_attr(const std::string& name) const;
    const std::vector<std::string>& attr_names() const { return info_.positional_attrs; }

    // Structural attributes
    const StructuralAttr& structure(const std::string& name) const;
    bool has_structure(const std::string& name) const;
    const std::vector<std::string>& structure_names() const { return info_.structural_attrs; }
    const std::vector<std::string>& region_attr_names() const { return info_.region_attrs; }
    const std::string& default_within() const { return info_.default_within; }

    // Structure mode queries (SM-ROAD-1)
    bool is_nested(const std::string& name) const;
    bool is_overlapping(const std::string& name) const;
    bool is_zerowidth(const std::string& name) const;

    // RG-5f: multivalue attribute queries
    bool is_multivalue(const std::string& name) const;
    const std::vector<std::string>& multivalue_attrs() const { return info_.multivalue_attrs; }

    // Dependency index (may be absent for CWB-imported corpora)
    bool has_deps() const { return has_deps_; }
    const DependencyIndex& deps() const { return deps_; }

    const std::string& dir() const { return dir_; }

private:
    std::string dir_;
    CorpusInfo info_;
    std::unordered_map<std::string, std::unique_ptr<PositionalAttr>> attrs_;
    std::unordered_map<std::string, std::unique_ptr<StructuralAttr>> structs_;
    DependencyIndex deps_;
    bool has_deps_ = false;
};

} // namespace manatree
