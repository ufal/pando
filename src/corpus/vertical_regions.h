#pragma once

// Structure regions of a vertical file, as Manatee's encodevert makes them:
//   <x …> … </x>   a region over the tokens between (none between: zero-width)
//   <x …/>         a zero-width region at the next token (start > end, dev/PANDO-JSONL-V2.md §5.4)
//   <x> never closed: ends right before the next <x> that opened while it was open, or at the end
//                  of the corpus (encodevert's "auto-closing structure"); warned about, with lines.
// Properly nested <x><x></x></x> stays nested. A </x> without an open <x> is counted and warned about.

#include "core/types.h"
#include "corpus/streaming_builder.h"
#include "corpus/vertical_tag.h"

#include <string>
#include <utility>
#include <vector>

namespace pando {

class VerticalRegions {
public:
    /// `source`: the file name used in warnings.
    VerticalRegions(StreamingBuilder& builder, std::string source)
        : builder_(builder), source_(std::move(source)) {}

    /// <x …>: `pos` = the corpus position of the next token, `line` = 1-based input line.
    void open(VerticalTag tag, CorpusPos pos, size_t line);
    /// </x>
    void close(const std::string& type, CorpusPos pos, size_t line);
    /// <x …/>
    void empty(VerticalTag tag, CorpusPos pos, size_t line);
    /// End of input: close what is still open (see above), print the warnings.
    void finish(CorpusPos corpus_size);

private:
    struct Open {
        std::string type;
        CorpusPos start = 0;
        std::vector<std::pair<std::string, std::string>> attrs;
        size_t line = 0;
        std::string outlived;        // the first enclosing region that closed while this one was open
        size_t outlived_line = 0;    // its </…> line (0 = none did)
        CorpusPos cut = -1;          // where the next <type> opened while this one was open (-1 = none)
        size_t cut_line = 0;
    };
    void note_same_type_open(const std::string& type, CorpusPos pos, size_t line);
    void emit(const std::string& type, CorpusPos start, CorpusPos end,
              const std::vector<std::pair<std::string, std::string>>& attrs);

    StreamingBuilder& builder_;
    std::string source_;
    std::vector<Open> stack_;
    size_t stray_closes_ = 0;
    size_t first_stray_line_ = 0;
    std::string first_stray_type_;
};

} // namespace pando
