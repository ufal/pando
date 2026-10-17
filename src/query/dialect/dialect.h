#pragma once

#include "query/ast.h"
#include "query/parser.h"
#include <string>

namespace pando {

class Corpus;

/// A query-language name as front-ends and FQS give it, as one of pando's dialects: "native"
/// (pando CQL: "", "native", "pando", "pando-cql"), "cwb" ("cwb", "cwb-cql", "cqp"), "pmltq"
/// ("pmltq", "pml-tq"), "tiger" ("tiger", "tigersearch"); "" for a name pando does not know.
std::string normalize_dialect(const std::string& name);

/// A query that names its language as its first word, in capitals and followed by white space
/// (`PMLTQ a-node [ lemma = "be" ]`; PANDO, CWB, CQP, PMLTQ, PML-TQ, TIGER — FQS's list without
/// MANATEE): the word is taken off `text` and returned as the dialect's name; "" (text unchanged)
/// for any other query. For front ends without a language choice (KonText's CQL field).
std::string take_language_prefix(std::string& text);

/// Parse a query (program) written in a dialect into a native Program: pando CQL as it is, the
/// others through their translators (dev/CQL-DIALECT-LICENSING.md). `corpus` (may be null) lets
/// TIGER pick its category attribute (`cat` in TEITOK node layers, else `type`). Throws
/// std::runtime_error for an unknown dialect and for queries the dialect cannot parse.
Program parse_in_dialect(const std::string& text, const std::string& dialect, ParserOptions parse_opts = {},
                         const Corpus* corpus = nullptr, int debug_level = 0, std::string* trace_out = nullptr);

} // namespace pando
