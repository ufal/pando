#pragma once

#include "core/types.h"
#include "corpus/streaming_builder.h"
#include <string>
#include <unordered_map>
#include <vector>

// Thin C++ API for external indexers (e.g. flexencoder).
//
// This wraps StreamingBuilder in a stable, minimal surface so external
// callers don't have to depend on internal details or parsing logic.

namespace pando {

class PandoIndexBuilder {
public:
    // Create a new Pando corpus index under `output_dir`.
    explicit PandoIndexBuilder(const std::string& output_dir);

    // Non-copyable.
    PandoIndexBuilder(const PandoIndexBuilder&) = delete;
    PandoIndexBuilder& operator=(const PandoIndexBuilder&) = delete;

    // Feed a token.
    //
    // - attrs: map from attribute name → value; typically includes
    //   "form", "lemma", "upos", "xpos", "deprel", "feats" or feats#* / feats_* (split).
    // - sentence_head_id: 1-based head index within the current sentence
    //   (0 = root, -1 = no dependency info).
    //
    // Call in sentence order; PandoIndexBuilder will stream data directly
    // to disk and update dependency files per sentence.
    void add_token(const std::unordered_map<std::string, std::string>& attrs,
                   int sentence_head_id = -1);

    // Mark the end of the current sentence. Safe to call even if no tokens
    // have been added since the last end_sentence.
    void end_sentence();

    // Add a structural region with multiple named attributes, e.g.:
    //   type = "text", attrs = {{"id","doc.xml"}, {"langcode","en"}}
    //
    // start/end are absolute token indices (0-based, inclusive).
    void add_region(const std::string& type,
                    CorpusPos start,
                    CorpusPos end,
                    const std::vector<std::pair<std::string, std::string>>& attrs);

    // Set default within-structure (e.g. "text"); written to corpus.info.
    void set_default_within(const std::string& structure);

    // Finalize the index: sort lexicons, remap .dat IDs, build reverse
    // indexes and write corpus.info. Must be called exactly once when
    // all tokens and regions have been added.
    void finalize();

    // Current corpus size (number of tokens) seen so far.
    CorpusPos corpus_size() const;

private:
    StreamingBuilder builder_;
};

} // namespace pando

