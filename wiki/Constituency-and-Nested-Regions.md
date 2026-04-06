# Constituency grammar and nested regions

Pando does **not** run a constituent parser. It **indexes** structural regions you supply. When those regions encode **phrase-structure (constituency) trees** as **nested spans** over tokens, you can query that structure with the same CQL tools as other region types.

This page explains that modeling story and how it connects to the `nested` flag and **innermost** region resolution.

## What “constituency” means here

A **constituent tree** is usually drawn as nested phrases (S → NP VP → …). In Pando, each constituent is a **region**: a half-open span over corpus positions `(start, end)` with a **label** stored as a region attribute (e.g. category `NP`, `VP`, `S`).

That matches common exports from:

- bracketed or **Penn Treebank**-style annotations converted to spans
- XML/TEI phrase markup flattened to token ranges
- any pipeline that emits **one region per node** and **nesting** when one constituent lies inside another

So “using full constituency grammar” in Pando means: **your corpus encodes the full tree as nested `node` (or similarly named) regions** with consistent labels—not that the engine parses raw text into trees.

## Index and header contract

Typical JSONL / `corpus.info` ingredients:

| Idea | Example |
| --- | --- |
| Structural type for constituents | `structural` includes `node` |
| Node label | `region_attrs` maps `node` → `type` (indexed as **`node_type`** in queries: `struct_attr`) |
| Nesting | `nested` includes `node` so the engine treats regions of this type as **properly nested** |

With `nested`, lookups at a token position use the **innermost** region that contains that position (see `StructuralAttr::find_innermost_region` in the indexer). For a token inside several nested `node` regions, attribute conditions like `[node_type="NP"]` consult that **innermost** node unless you use semantics that intentionally scan all covering regions (see [Overlapping and nested regions](Overlapping-and-Nested-Regions.md)).

**Nested vs overlapping:** Phrase-structure trees are **nested** (no crossing branches). If two regions of the **same** type **cross** without containment, that is **overlapping** mode and uses different semantics (e.g. `hi` highlights). Declare `nested` for constituency-style `node` regions; reserve `overlapping` for types that need it.

## Query patterns

Normative syntax and the multivalue / region table is in [PANDO-CQL.md](PANDO-CQL.md) (“Multivalue fields and overlapping regions”). Typical constituency-style patterns:

| Pattern | Meaning |
| --- | --- |
| `[node_type="NP"]` | Token lies in some `node` region whose type is `NP` (with nested semantics, innermost drives default attribute resolution) |
| `within node_type="NP"` | The whole match span must stay inside a region that satisfies the `node` type constraint |

Use **named tokens** and global `::` filters when you need to relate positions or add extra constraints; see the main CQL doc.

## Dependencies vs constituents

**Dependency** (UD-style) links are a **separate** index: governors, dependents, `child`/`parent`, `dcoll`, etc. See [Dependency queries](Dependency-Queries.md).

A corpus can carry **both**: UD tokens + `deprel`/`head` for syntax-as-graph, and **nested `node` regions** for phrase-structure. They answer different questions; neither replaces the other.

## Example corpus

The JSONL emitted by **`python scripts/gen_sample_rich_jsonl.py`** declares `nested: ["node"]` and includes several `node` regions with `attrs.type` (e.g. `NP`, `S`, `VP`, `CLAUSE`) over the first sentence. Index that file and use it to align the header fields and the `region` records with this documentation.

## Modeling scope

- **`nvals(node_type)`** (see [PANDO-CQL.md](PANDO-CQL.md)) reports the number of distinct pipe-separated **components** across covering `node` regions at a token when the structure is nested or overlapping. For **geometric** containment (“inside an NP”), `within` / `containing` over region spans match how Pando represents constituency.

- **Discontinuous constituents** (one logical node over non-adjacent tokens) are not a single contiguous span in the index; model them with **multiple** regions or another annotation layer that fits your pipeline.

## See also

- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md) — `nested` vs `overlapping`, zero-width
- [Index and corpus layout](Index-and-Corpus-Layout.md) — `*.rgn`, `*_attr.val`
- [Multivalue attributes](Multivalue-Attributes.md) — when labels are **sets** on tokens or regions, not tree nodes
