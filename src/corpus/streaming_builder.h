#pragma once

#include "core/types.h"
#include "core/mmap_file.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <cstdio>
#include <limits>
#include <utility>

namespace manatree {

// Streaming corpus builder: writes index files incrementally as tokens
// arrive, then sorts lexicons and builds reverse indexes at finalize.
//
// Memory usage: only the unique strings per attribute (vocabulary) are
// held in memory — typically ~1M entries.  Token data streams directly
// to .dat files on disk.  Dependency data is flushed per sentence.
//
// Usage:
//   StreamingBuilder builder("./output");
//   builder.add_token({{"form","The"}, {"lemma","the"}, ...}, /*head=*/2);
//   builder.add_token({{"form","cat"}, ...}, /*head=*/0);
//   builder.end_sentence();
//   builder.finalize();  // sort lexicons, remap .dat, build .rev
class StreamingBuilder {
public:
    /// When `overlay_standoff_only` is true, do not create `s.rgn` or dep files; only token-group
    /// standoff columns and `groups/*.jsonl` are emitted (for `--overlay-index` mini-trees).
    explicit StreamingBuilder(const std::string& output_dir, bool overlay_standoff_only = false);

    /// After JSONL header declares token_groups, set corpus length to match the main index and
    /// pre-fill standoff memory columns with `_`. Only valid in overlay_standoff_only mode.
    void bootstrap_overlay_corpus_size(CorpusPos n);
    ~StreamingBuilder();

    StreamingBuilder(const StreamingBuilder&) = delete;
    StreamingBuilder& operator=(const StreamingBuilder&) = delete;

    // Feed a token.  `attrs` maps attribute names to values.
    // `sentence_head_id`: 1-based head within sentence (0 = root, -1 = no dep info).
    void add_token(const std::unordered_map<std::string, std::string>& attrs,
                   int sentence_head_id = -1);

    /// Optional per-sentence structural attributes (e.g. `tuid` → `s_tuid` in corpus.info).
    void end_sentence(const std::vector<std::pair<std::string, std::string>>& sent_region_attrs = {});

    // Add a non-sentence structural region (e.g. "text", "p").
    void add_region(const std::string& type, CorpusPos start, CorpusPos end);

    // Add a region with one value (stored as attr "id"; backward compat).
    void add_region(const std::string& type, CorpusPos start, CorpusPos end,
                    const std::string& value);

    // Add a region with multiple named attributes (#8): e.g. {{"id","doc.xml"}, {"year","2020"}}.
    void add_region(const std::string& type, CorpusPos start, CorpusPos end,
                    const std::vector<std::pair<std::string, std::string>>& attrs);

    // Sort lexicons, remap .dat IDs, build reverse indexes.
    void finalize();

    // #9: Set default within-structure (e.g. "text"); written to corpus.info.
    void set_default_within(const std::string& structure) { default_within_ = structure; }

    // JSONL v2: Pre-declare structure modes from header (merged with auto-detected at finalize).
    void declare_nested(const std::string& s)      { declared_nested_.insert(s); }
    void declare_overlapping(const std::string& s)  { declared_overlapping_.insert(s); }
    void declare_zerowidth(const std::string& s)    { declared_zerowidth_.insert(s); }
    void declare_multivalue(const std::string& s)   { declared_multivalue_.insert(s); }

    /// REQ-TOKEN-GROUPS / TEITOK standoff: `struct_name` region events in JSONL are **not**
    /// indexed as StructuralAttr intervals; they assign multivalued `membership_attr` on tokens
    /// and append rows to `group_table.jsonl`. Call from JSONL header (before tokens).
    /// `prop_attr_keys`: group-level attrs to copy onto each covered token as `{struct}_{key}`
    /// (e.g. code → err_code) for indexed search; must also appear in JSONL positional + multivalue.
    void declare_token_group_layer(
        const std::string& struct_name,
        const std::string& membership_attr,
        const std::vector<std::string>& prop_attr_keys = {});

    /// True if `struct_name` was declared with declare_token_group_layer.
    bool is_token_group_struct(const std::string& struct_name) const {
        return token_group_struct_.count(struct_name) != 0;
    }

    bool has_token_group_layers() const { return !token_group_struct_.empty(); }

    /// Apply one standoff annotation covering global token positions [start, end] inclusive.
    void apply_token_group_annotation(const std::string& struct_name, CorpusPos start,
                                      CorpusPos end,
                                      const std::vector<std::pair<std::string, std::string>>& attrs);

    CorpusPos corpus_size() const { return corpus_size_; }

private:
    struct AttrState {
        std::unordered_map<std::string, int32_t> str_to_id;
        std::vector<std::string> id_to_str;
        FILE* dat_file = nullptr;
        /// When set, token values are accumulated here until finalize (standoff group attrs).
        std::vector<std::string>* memory_strings = nullptr;
        CorpusPos written = 0;
        int32_t placeholder_id = -1;  // cached ID for "_"

        int32_t get_or_assign(const std::string& value);
        int32_t get_placeholder();     // fast path for "_"
        ~AttrState();
    };

    void ensure_attr(const std::string& name);
    void ensure_memory_attr(const std::string& name);
    void backfill_attr(AttrState& state);
    void open_dep_files();

    void finalize_attribute(const std::string& name, AttrState& state);
    void remap_dat(const std::string& dat_path, const std::vector<int32_t>& remap,
                   int target_width);
    void build_reverse_index(const std::string& base, int32_t lex_size,
                             int rev_width);
    void build_mv_reverse_index(const std::string& base, int32_t lex_size,
                                int rev_width);
    // Stage 1 of PANDO-MULTIVALUE-FIELDS: forward MV index .mv.fwd / .mv.fwd.idx.
    // Spec: dev/PANDO-MVAL-FORMAT.md (v0.2). Built unconditionally for any
    // multivalue= attr right after build_mv_reverse_index() so it can reuse the
    // joined-string lex parse rather than re-reading .lex.
    void build_mv_forward_index(const std::string& base, int32_t lex_size);

    std::string output_dir_;
    CorpusPos corpus_size_ = 0;
    bool finalized_ = false;
    bool overlay_standoff_only_ = false;
    bool has_deps_ = false;

    std::unordered_map<std::string, std::unique_ptr<AttrState>> attrs_;
    std::vector<std::string> attr_order_;
    std::unordered_set<std::string> attr_set_;

    // Current sentence
    struct SentToken { int sentence_head_id; };
    std::vector<SentToken> sent_buf_;
    CorpusPos sent_start_ = 0;

    // Dep files (streamed per sentence)
    FILE* dep_head_file_ = nullptr;
    FILE* dep_euler_in_file_ = nullptr;
    FILE* dep_euler_out_file_ = nullptr;
    CorpusPos dep_written_ = 0;

    // Sentence region file (streamed)
    FILE* sent_rgn_file_ = nullptr;
    // One entry per sentence region (parallel to s.rgn), for named attrs like tuid
    std::vector<std::vector<std::pair<std::string, std::string>>> sentence_region_attr_rows_;

    // Other structural regions (buffered — typically few)
    std::unordered_map<std::string, std::vector<Region>> regions_;
    std::unordered_map<std::string, std::vector<std::string>> region_values_;  // legacy: one value per region
    // #8: multiple named attrs per region type: region_attr_values_[type][attr_name] = values per region
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string>>> region_attr_values_;
    std::unordered_set<std::string> struct_set_;
    std::string default_within_;  // #9: e.g. "text"

    // SM-ROAD-6: Per-type streaming detection of non-flat structure properties.
    // Populated incrementally by add_region(); summarized at finalize().
    struct StructModeDetection {
        bool nesting_seen = false;
        bool overlap_seen = false;
        bool zerowidth_seen = false;
        size_t nesting_count = 0;
        size_t overlap_count = 0;
        size_t zerowidth_count = 0;
    };
    std::unordered_map<std::string, StructModeDetection> struct_mode_detected_;

    // JSONL v2: Pre-declared structure modes (from header).
    std::unordered_set<std::string> declared_nested_;
    std::unordered_set<std::string> declared_overlapping_;
    std::unordered_set<std::string> declared_zerowidth_;
    std::unordered_set<std::string> declared_multivalue_;

    // REQ-TOKEN-GROUPS: struct type → positional multivalue column (e.g. err → err_gid).
    std::unordered_set<std::string> token_group_struct_;
    std::unordered_map<std::string, std::string> token_group_membership_attr_;
    std::unordered_set<std::string> token_group_membership_attrs_;
    /// Group attr keys (e.g. "code") denormalized to err_code on tokens for MV indexing.
    std::unordered_map<std::string, std::vector<std::string>> token_group_prop_attrs_;
    /// Membership + denormalized prop columns — memory-backed standoff attrs.
    std::unordered_set<std::string> token_group_standoff_memory_attrs_;
    std::unordered_map<std::string, std::vector<std::string>> token_group_memory_column_;

    FILE* group_table_file_ = nullptr;
    // One row per (struct, group id); discontinuous spans call apply_token_group_annotation twice.
    std::unordered_set<std::string> group_table_row_keys_;
    void append_group_table_row(const std::string& struct_name,
                                const std::string& group_id,
                                const std::vector<std::pair<std::string, std::string>>& attrs);

    // Phase A: per-group records accumulated in apply_token_group_annotation, then
    // serialized to `groups/<struct>.jsonl` at finalize. Each record holds the
    // group's sub-spans (in insertion order), an envelope (first/last pos), and
    // any declared prop attrs captured directly from the annotation events (so
    // multi-group tokens with different prop values stay disambiguated, unlike
    // the per-token MV column that just unions everything).
    struct GroupRecord {
        std::string gid;
        CorpusPos first_pos = std::numeric_limits<CorpusPos>::max();
        CorpusPos last_pos  = 0;
        std::vector<std::pair<CorpusPos, CorpusPos>> spans;
        std::vector<std::pair<std::string, std::string>> props;
    };
    std::unordered_map<std::string, std::vector<GroupRecord>> token_group_records_;
    std::unordered_map<std::string, std::unordered_map<std::string, size_t>> token_group_record_idx_;
    void write_token_group_indexes();

    void detect_structure_mode(const std::string& type, CorpusPos start, CorpusPos end);
};

} // namespace manatree
