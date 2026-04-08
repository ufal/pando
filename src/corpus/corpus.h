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

    // REQ-TOKEN-GROUPS: structural names routed to standoff (no .rgn index).
    std::vector<std::string> token_group_structs;
};

// Phase A: in-memory representation of one record from `groups/<struct>.jsonl`.
// `spans` lists the disjoint sub-spans (insertion order); `props` carries
// declared prop attrs captured at annotation time. `first`/`last` is the
// envelope (used for doc-order iteration and KWIC context windows).
struct GroupRecord {
    std::string gid;
    CorpusPos first = 0;
    CorpusPos last  = 0;
    std::vector<std::pair<CorpusPos, CorpusPos>> spans;
    std::vector<std::pair<std::string, std::string>> props;
};

class GroupIndex {
public:
    void load(const std::string& path);
    const std::vector<GroupRecord>& records() const { return records_; }
    bool loaded() const { return loaded_; }
private:
    std::vector<GroupRecord> records_;
    bool loaded_ = false;
};

// Loads and provides access to all indexes for a corpus directory.
class Corpus {
public:
    /// Load main index from `dir`. Optional `overlay_dirs` are stand-off mini-indices (from
    /// `pando-index --overlay-index`); their attributes and token-group structs are merged
    /// with names prefixed `overlay-<layer>-…` (see USER-OVERLAY-ANNOTATIONS.md).
    void open(const std::string& dir, bool preload = false,
              const std::vector<std::string>& overlay_dirs = {});

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

    // REQ-TOKEN-GROUPS: token-group structs are non-contiguous standoff annotations
    // and must be rejected by operators that assume contiguous regions.
    bool is_token_group(const std::string& name) const {
        for (const auto& s : info_.token_group_structs)
            if (s == name) return true;
        return false;
    }
    const std::vector<std::string>& token_group_structs() const { return info_.token_group_structs; }

    // Lazily loaded `groups/<struct>.jsonl` sidecar (Phase A) — exposes the
    // disjoint sub-spans + props of every group, indexed by doc order.
    const GroupIndex& group_index(const std::string& name) const;

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
    mutable std::unordered_map<std::string, std::unique_ptr<GroupIndex>> group_indexes_;
    /// Merged overlay token-group name → path to `groups/<orig>.jsonl` in overlay dir.
    std::unordered_map<std::string, std::string> group_index_path_override_;
};

} // namespace manatree
