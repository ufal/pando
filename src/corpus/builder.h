#pragma once

#include "core/types.h"
#include "corpus/streaming_builder.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace manatree {

// Convenience wrapper: reads CoNLL-U and feeds StreamingBuilder.
class CorpusBuilder {
public:
    /// When `overlay_standoff_only` is true, only `read_jsonl_overlay` is supported (standoff-only JSONL).
    explicit CorpusBuilder(const std::string& output_dir, bool overlay_standoff_only = false);

    void read_conllu(const std::string& path);
    /// Read CWB-style vertical: one token per line, tab-separated; <s> </s> for sentence boundaries.
    /// Columns: form [lemma [upos]]; no dependency info.
    void read_vertical(const std::string& path);

    /// Read JSONL event stream (from file or "-" = stdin) and feed StreamingBuilder.
    /// Events follow the schema in dev/PANDO-INDEX-INTEGRATION.md.
    /// If `overlay_expected_size` is non-null, only token-group `region` events are read (no tokens);
    /// corpus length is fixed to `*overlay_expected_size` (overlay index build).
    void read_jsonl(const std::string& path, CorpusPos* overlay_expected_size = nullptr);

    /// Convenience: `read_jsonl(path, &n)` after constructing `CorpusBuilder(dir, true)`.
    void read_jsonl_overlay(const std::string& path, CorpusPos expected_tokens);
    void finalize();

    // When true, split feats into individual feats_X attributes (old behavior).
    // When false (default), store feats as a single combined string.
    void set_split_feats(bool v) { split_feats_ = v; }

    StreamingBuilder& builder() { return builder_; }

private:
    void parse_feats(const std::string& feats_str,
                     std::unordered_map<std::string, std::string>& attrs);
    /// CoNLL-U MISC column: only whitelisted keys (tuid, Translit, Vform, LTranslt, Root, CorrectForm, Gloss).
    void parse_misc(const std::string& misc_str,
                    std::unordered_map<std::string, std::string>& attrs);

    void close_text_region_if_open();
    void close_doc_region_if_open();

    StreamingBuilder builder_;
    bool split_feats_ = false;
    bool overlay_standoff_only_ = false;

    // Region tracking for CoNLL-U input (3b/4c):
    bool        in_sentence_ = false;
    bool        has_text_region_ = false;
    bool        has_doc_region_ = false;
    bool        has_par_region_ = false;
    CorpusPos   text_region_start_ = 0;
    CorpusPos   doc_region_start_ = 0;
    CorpusPos   par_start_ = 0;
    std::string doc_id_;
    std::string par_id_;
    /// Current # newregion text span + CQP # text_* = …
    std::vector<std::pair<std::string, std::string>> text_region_attrs_;
    /// Current # newdoc id span (corpus structural region "doc", not "text").
    std::vector<std::pair<std::string, std::string>> doc_region_attrs_;
    /// Sentence-level attrs for the current CoNLL-U sentence (e.g. # tuid = ...).
    std::vector<std::pair<std::string, std::string>> sent_region_attrs_;
    /// Copied onto each token until the sentence ends (TEITOK-style alignment id).
    std::string pending_sent_tuid_;

    // Region tracking for vertical/VRT input: stack of open regions
    // (<text ...>, <p ...>, <s ...>, <bla ...>), each with attributes.
    struct OpenRegion {
        std::string type;
        CorpusPos   start = 0;
        std::vector<std::pair<std::string, std::string>> attrs;
    };
    // Region stack for CoNLL-U comment extensions: # newregion X / # endregion X.
    std::vector<OpenRegion> conllu_region_stack_;
    std::vector<OpenRegion> vrt_region_stack_;

    // Multi-word token (MWT) tracking for CoNLL-U contraction regions.
    // When a range ID line (e.g. "1-2\taux\t...") is seen, we record the
    // orthographic form and count how many sub-tokens remain. After the last
    // sub-token is added, we emit a "contr" region spanning them.
    std::string mwt_form_;
    CorpusPos   mwt_start_ = 0;
    int         mwt_remaining_ = 0;  // sub-tokens left before closing the contr region
};

} // namespace manatree
