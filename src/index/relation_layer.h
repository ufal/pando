#pragma once

// A pointer relation layer stored as deviations from the basic tree (relation_decl.h, `inherit=head`): enhanced UD
// `deps` repeats the basic edge for most tokens, so only the tokens whose edge set differs are stored, with all their
// edges. A token's edges are its own when it deviates, else the basic edge (head, deprel).
//
// Files (NAME = the layer, e.g. "deps"):
//   rel.NAME.src     int64  the deviating tokens' positions, ascending
//   rel.NAME.off     int64  per deviating token the first edge, plus the total (src count + 1)
//   rel.NAME.tgt     int64  per edge the target position (the head), -1 for the root
//   rel.NAME.lab     int32  per edge the label id
//   rel.NAME.labels  text   the labels, one per line, by id

#include "core/mmap_file.h"
#include "core/types.h"

#include <cstdio>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pando {

class DependencyIndex;
class PositionalAttr;

// Writes a layer sentence by sentence (the builder's streaming order).
class RelationLayerWriter {
public:
    RelationLayerWriter(const std::string& dir, const std::string& name);
    ~RelationLayerWriter();

    // One token of a sentence: its basic edge (1-based head within the sentence, 0 = root, -1 = none) and label,
    // and its layer edges as in CoNLL-U DEPS (`4:nsubj|7:nsubj:xsubj`, `_` = none given: the basic edge holds).
    struct Token {
        int basic_head = -1;
        std::string basic_label;
        std::string deps;
    };
    // The tokens of one sentence starting at `start`; edges to or from empty nodes (`5.1`) are left out and counted.
    void add_sentence(CorpusPos start, const std::vector<Token>& tokens);

    // Writes the label list and closes the files.
    void finish();

    size_t tokens() const { return tokens_; }
    size_t deviating() const { return deviating_; }
    size_t edges() const { return edges_; }
    size_t skipped_empty() const { return skipped_empty_; }
    int64_t span() const { return span_; }

private:
    int32_t label_id(std::string_view label);

    std::string base_;
    FILE* src_ = nullptr;
    FILE* off_ = nullptr;
    FILE* tgt_ = nullptr;
    FILE* lab_ = nullptr;
    int64_t written_edges_ = 0;
    std::unordered_map<std::string, int32_t> label_ids_;
    std::vector<std::string> labels_;
    size_t tokens_ = 0, deviating_ = 0, edges_ = 0, skipped_empty_ = 0;
    int64_t span_ = 0;
    bool finished_ = false;
};

// Reads a layer: the heads (targets) of a token, its own edges or the inherited basic edge.
class RelationLayer {
public:
    // false when the files are missing or inconsistent
    bool open(const std::string& dir, const std::string& name, const DependencyIndex* base, const PositionalAttr* base_label);
    bool valid() const { return valid_; }

    // f(head position, label) for every edge of `pos` (a root edge has head NO_HEAD); f returns false to stop
    template <typename F>
    void for_each_head(CorpusPos pos, F&& f) const;

    size_t deviating() const { return nsrc_; }
    const std::string& label(int32_t id) const { return labels_[static_cast<size_t>(id)]; }

private:
    bool valid_ = false;
    MmapFile src_, off_, tgt_, lab_;
    size_t nsrc_ = 0;
    std::vector<std::string> labels_;
    const DependencyIndex* base_ = nullptr;
    const PositionalAttr* base_label_ = nullptr;

    // index of pos among the deviating tokens, or -1
    int64_t find(CorpusPos pos) const;
    CorpusPos base_head(CorpusPos pos) const;
    std::string_view base_label(CorpusPos pos) const;
};

template <typename F>
void RelationLayer::for_each_head(CorpusPos pos, F&& f) const {
    const int64_t i = find(pos);
    if (i < 0) {
        f(base_head(pos), base_label(pos));
        return;
    }
    const auto* off = off_.as<int64_t>();
    const auto* tgt = tgt_.as<int64_t>();
    const auto* lab = lab_.as<int32_t>();
    for (int64_t e = off[i]; e < off[i + 1]; ++e)
        if (!f(static_cast<CorpusPos>(tgt[e]), std::string_view(labels_[static_cast<size_t>(lab[e])]))) return;
}

}  // namespace pando
