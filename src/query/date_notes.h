#pragma once
// What the dates a query reads are: notes for its answer's warnings (Result-Format.md). Dates are read
// by core/dates (EDTF, else the old reading); the notes say, per attribute and over its distinct values,
// how many were read the old way, how many are no date, and per date function how many do not
// determine what it asks for (year() of 1712/1734). Nothing is said when every value is EDTF and
// determines it.

#include "core/dates.h"
#include "corpus/corpus.h"
#include "query/ast.h"

#include <string>
#include <vector>

namespace pando {

struct DateUse {
    std::string spec;   // as written: text_date, match.text_date, a.text_date
    DatePart part;
};

/// The date functions of a program: in its queries' :: filters and its commands' fields.
std::vector<DateUse> date_uses(const Program& prog);

/// Notes on the values those functions read; empty when there is nothing to say. An attribute
/// the corpus does not have, or a region binding's attribute, is left out.
std::vector<std::string> date_value_notes(const Corpus& corpus, const std::vector<DateUse>& uses);

/// An answer's JSON with these notes added to result.warnings (the list it has, or a new one; when the
/// result is no object, the answer's own warnings). Unchanged when there are none.
std::string with_result_warnings(std::string json, const std::vector<std::string>& notes);

}  // namespace pando
