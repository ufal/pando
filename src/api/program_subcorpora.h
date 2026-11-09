#pragma once

// EF-12: subcorpora in a pando-CQL program (/run, pando_run, the CLI with a subcorpus).
//
//   S = (t:<text> :: t.text_genre = "fiction") & (y:<text year="1990">);   define (set expression)
//   S = t:<text> :: t.text_genre = "fiction";   a named query: also usable as a subcorpus
//   X = [lemma="go"] within S;                  filter: statistics of the whole corpus
//   use S;  …  use;                             session-wide: S is the corpus (reset with `use;`)
//   use A;  X = […] within B;                   N of A, B a filter inside A
//
// The statements `use …` and `NAME = <set expression>` are taken out of the program before it
// is parsed (the parser knows neither); `within NAME` is recognised on the parsed query when
// NAME is no structure of the corpus but a subcorpus or hit set of the session.

#include "api/subcorpus_stats.h"
#include "query/ast.h"
#include "subcorpus/subcorpus.h"

#include <map>
#include <string>
#include <vector>

namespace pando {

class Corpus;

struct ProgramPiece {
    enum class Kind { Plain, Define, Use, UseReset } kind = Kind::Plain;
    std::string text;   // Plain: its statements; Define / Use: the definition
    std::string name;   // Define: the name; Use: the name when the definition is one
};

/// The program cut at its subcorpus statements; consecutive other statements stay one Plain piece.
std::vector<ProgramPiece> split_subcorpus_statements(const std::string& cql);
/// The program has a `use` or `NAME = <set expression>` statement.
bool has_subcorpus_statements(const std::string& cql);

/// A session's subcorpora: definitions by name and the `use` in effect.
struct SessionSubcorpora {
    std::map<std::string, std::string> defs;
    AppliedSubcorpusPtr use;
};

/// The restriction of one query statement: `use` (the session's or the request's), a
/// request's `within`, and the statement's own `within NAME` / `not within NAME` (taken out
/// of the query). Restricts the statement (subcorpus/restrict.h) and returns what its
/// statistics and answer need; nullptr when nothing applies.
SubcorpusStatsPtr restrict_statement(const Corpus& corpus, Statement& st, const AppliedSubcorpusPtr& use,
                                     const AppliedSubcorpusPtr& request_within, const SubcorpusResolveOptions& ro);

} // namespace pando
