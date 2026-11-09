#pragma once

// Sort / count fields with CQP options: `form %cd`, `form on match[-1]..match[-5]`.
//
// A field keeps its options in its own text (the canonical form below), so sort
// steps, cache keys and the CLI carry them like any field:
//
//     <attr>[ %<flags>][ on <anchor>[<n>]..<anchor>[<n>]]
//
// flags: c (case-insensitive), d (diacritic-insensitive), r (retrograde: each token's value
// read backwards, by code points, to sort by word endings). Anchors: match, matchend
// or a token label of the query. A range from a later to an earlier position is
// read backwards (CQP's left-context sort: `on match[-1]..match[-5]`).

#include "query/executor.h"

#include <string>
#include <string_view>
#include <vector>

namespace pando {

struct SortFieldSpec {
    std::string attr;              ///< the field as read_tabulate_field takes it
    bool fold_case = false;        ///< %c
    bool fold_accents = false;     ///< %d
    bool retrograde = false;       ///< %r (each token's value reversed)
    bool range = false;            ///< `on from..to`
    std::string from_anchor, to_anchor;
    int from_off = 0, to_off = 0;
    bool whole_match() const {     ///< `on match..matchend`
        return range && from_anchor == "match" && from_off == 0 && to_anchor == "matchend" && to_off == 0;
    }
};

/// Split `field` into its parts; false for a plain field (no flags, no range).
/// Throws for a malformed option part.
bool parse_sort_field(const std::string& field, SortFieldSpec& out);

/// Some field has options (flags or a range): such fields are keyed hit by hit (make_group_key),
/// not by the aggregation sink.
bool fields_have_options(const std::vector<std::string>& fields);

/// The canonical text of `spec` (parse_sort_field reads it back).
std::string sort_field_text(const SortFieldSpec& spec);

/// `s` with letters lowercased (%c) and/or diacritics removed (%d): Latin (Latin-1,
/// Extended-A, the common Extended-B letters), Greek and Cyrillic; combining marks
/// are dropped with %d.
std::string fold_sort_text(std::string_view s, bool fold_case, bool fold_accents);

/// `s` read backwards by UTF-8 code points (%r; invalid bytes stay single bytes).
std::string reverse_utf8(std::string_view s);

/// The corpus position `anchor` (`match`, `matchend`, `match[-1]`, a token label, `lab[2]`)
/// gives for match `m`: the anchor's token plus the offset, not clamped. Throws for an unknown
/// label or a malformed anchor.
int64_t sort_anchor_position(const Match& m, const NameIndexMap& name_map, const std::string& anchor);

/// The value of one sort field with options for match `m` (see make_group_key).
std::string read_sort_field(const Corpus& corpus, const Match& m, const NameIndexMap& name_map,
                            const SortFieldSpec& spec);

}  // namespace pando
