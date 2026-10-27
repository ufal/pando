#pragma once

// What a hit is aligned with, over an equivalence layer (equivalence_classes.h): `--aligned s_tuid` shows with each
// hit the other sentences of its sentence's class (its translations), `--aligned tuid` the sentences of the tokens
// aligned with the hit's tokens, those tokens marked. A piece is a sentence (without sentences: the aligned tokens
// with a few tokens around them); the hit's own sentences are left out. `where` (`text_iso=ces|deu`) keeps the
// pieces whose region attribute has one of the values (the languages to show).

#include "core/types.h"
#include "corpus/corpus.h"
#include "query/executor.h"

#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace pando {

struct AlignedPiece {
    CorpusPos start = 0, end = 0;     // the piece's tokens
    std::vector<CorpusPos> marked;    // the aligned tokens in it (a token layer), sorted
};

struct AlignedWhere {
    std::string region_attr;          // e.g. text_iso
    std::vector<std::string> values;  // any of these
};

// `spec` "text_iso=ces|deu" (or ""); throws on a malformed one
AlignedWhere parse_aligned_where(const std::string& spec);

// The pieces aligned with `m` over `layer` (a declared equivalence layer, or a token / region attribute), in corpus
// order, at most `max_pieces` (0 = all). Throws when the corpus has no such layer.
std::vector<AlignedPiece> aligned_pieces(const Corpus& corpus, const Match& m, const std::string& layer,
                                         const AlignedWhere& where, size_t max_pieces = 0);

// `, "aligned": [{"start": …, "end": …, "doc_id": …, "doc": {attrs}, "text": "…", "marked": [pos, …]}, …]`
// for a hit (nothing when `layer` is empty), as /query and the CLI's JSON write it
void write_aligned_json(std::ostream& out, const Corpus& corpus, const Match& m, const std::string& layer,
                        const std::string& where, size_t max_pieces);

// A piece as text, its aligned tokens between `mark_open` and `mark_close`
std::string aligned_piece_text(const Corpus& corpus, const AlignedPiece& piece, const std::string& mark_open,
                               const std::string& mark_close);

// The region attributes of a piece's document region (`text`, else `doc`): its language and the like, for a label
std::vector<std::pair<std::string, std::string>> aligned_piece_doc_attrs(const Corpus& corpus, CorpusPos pos);

}  // namespace pando
