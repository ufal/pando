# Overlapping and nested regions

**See also:** [Constituency grammar and nested regions](Constituency-and-Nested-Regions.md) — phrase-structure trees as nested `node` regions.

Structural regions (sentences, texts, highlights, syntax nodes, etc.) are stored as ranges over token positions. The corpus declares two orthogonal ideas:

1. **Nested** (`nested` in the JSONL header / index): regions of the same type can nest (e.g. syntax `node` inside `node`). For nested types, **`find_innermost_region`** is used where the query language needs “the” region at a token — so a token inside several layers of `node` resolves to the innermost region.
2. **Overlapping** (`overlapping` in the header): regions of the same type may **overlap without nesting** (e.g. `<hi>` spans in TEI). A token may be covered by multiple regions of that type; predicates like `[node_type="NP"]` mean “the token lies in **some** NP region,” not “unique parent.”

## Zero-width regions

Types listed under **`zerowidth`** (e.g. `del` for editorial marks) can have `start_pos == end_pos` (or inverted ranges per convention) so they represent **gaps** between tokens. Queries can attach to those boundaries (see CQL doc zero-width section).

## Query interpretation

| Situation | Typical behavior |
| --- | --- |
| Nested `node` | Innermost region wins for attribute lookup at a position |
| Overlapping `hi` | Token matches if **any** overlapping region satisfies the predicate |
| `within` | Match span must fit **inside** a region satisfying the constraint |

For the exact table of region-attribute restrictions, see [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) (Regions / Within).

## See also

- [Constituency grammar and nested regions](Constituency-and-Nested-Regions.md) — full constituency as nested phrase-structure regions
- [Multivalue attributes](Multivalue-Attributes.md) — region attrs like `text_genre` can also be multivalue
- [Index and corpus layout](Index-and-Corpus-Layout.md) — `*.rgn`, `*_attr.val`
