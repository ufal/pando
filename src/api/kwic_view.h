#pragma once
// KonText's KWIC lines for a hit (`/query` and `/run` option `"kwic_view"`).
//
// KonText builds its concordance page from what Manatee's KWICLines gives per line (lib/kwiclib):
// the left context, the KWIC and the right context as strings and classes alternating, the refs,
// with KonText's context spec (`-40#` / `40#`: characters, `-5` / `5`: tokens, `-1:s` / `1:s`:
// structures), attributes (`word,lemma`: the first one the text, the others as "attr" chunks
// `\x1Fv1\x1Fv2`, the delimiter of ÚČNK's manatee-open that KonText runs; upstream has `/`),
// structures to show in the context (`s,text.id`: `<s>` … `</s>` chunks of class `strc`) and refs (`=text.id`, `text.id`, `s`, `#`). This is that algorithm over a pando index
// (manatee-open concord/concget.cc, concctx.cc), so that KonText shows a pando corpus the way it
// shows a Manatee one; FQS gives the same object for native Manatee (libfqs_manatee, KWICLines
// itself).
//
// Differences from Manatee that come from what a pando index has: no registry, so no DISPLAYTAG /
// DISPLAYCLASS / DISPLAYBEGIN / DISPLAYEND (every structure is shown as its tags), no ref LABELs
// (`text.id` is `text.id=v`), MAXCONTEXT only when the request gives `maxctx`; no numbered labels
// (`1:[…]`, Manatee's coll classes); nested structures of one type count as not nested.

#include "corpus/corpus.h"
#include <ostream>
#include <string>

namespace pando {

struct KwicViewSpec {
    bool on = false;
    std::string left = "-5", right = "5";   // KonText's kwiclib defaults
    std::string attrs = "word", ctxattrs;    // ctxattrs empty: as attrs
    std::string structs, refs;
    int maxctx = 0;                          // Manatee's MAXCONTEXT (0: none)
};

/// The spec from a request's `"kwic_view"` object (raw JSON; empty: off).
KwicViewSpec parse_kwic_view(const std::string& raw_object);

/// `"kwic_view": {pos, kwiclen, ctxbeg, ctxend, linegroup, refs, left, kwic, right}` (with the
/// leading `, `) for the hit [kwbeg, kwend) (kwend exclusive, as Manatee's).
void write_kwic_view(std::ostream& out, const Corpus& corpus, const KwicViewSpec& spec, CorpusPos kwbeg,
                     CorpusPos kwend);

}  // namespace pando
