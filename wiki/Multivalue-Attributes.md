# Multivalue attributes

Multivalue (MV) attributes store **sets** of atomic values as a single string using **`|`** as separator (e.g. `Poem|Song`, `artist|writer`). The corpus header (`corpus.info` / JSONL header) lists which attributes are multivalue.

## Semantics

- **Equality / inequality**: `[wsd="artist"]` matches if the token’s `wsd` field contains **artist** as a component (not only the full string `artist|writer`). The same logic applies to region attributes like `text_genre` when declared multivalue: comparisons use component membership, not exact string equality.
- **Main lexicon vs components**: Stored token values may be the full pipe-joined string. The engine uses a **component lexicon** and **`.mv.rev`** postings (RG-5f) for efficient seed resolution on EQ queries.
- **Reverse indexes**: For positional MV attrs, optional `attr.mv.lex`, `attr.mv.rev`, … speed up “find all positions with component X”.
- **`nvals(attr)`**: In token conditions, **`[nvals(wsd)>1]`** compares the number of non-empty `|`-separated components to an integer (see [PANDO-CQL.md](../docs/PANDO-CQL.md)). Equality uses a single **`=`** (e.g. **`nvals(wsd)=2`**), not `==`.

## Indexing

`pando-index` / `StreamingBuilder` writes:

- `corpus.info` line `multivalue=attr1,attr2,...`
- For each listed positional MV attr, MV sidecar files next to the main lexicon.

The indexer writes these files for each declared multivalue attribute; queries resolve EQ and component lookups through them.

## `count` / `freq` and pipe explosion

For **single-column** `count` / `freq` on a **multivalue** positional attribute, the CLI **splits** decoded bucket keys on `|` after aggregation so each row is a **component** (RG-5f-style reporting). This applies to both `count by wsd` and `count by a.wsd` — the corpus strips the `token.` prefix when checking multivalue. Totals and percentages refer to **match rows**; component totals can exceed token count when one token has multiple components.

### Composite keys (debugging)

- **`--no-mv-explode`**: skip that explosion; keep **lexicon** keys as returned from the bucket decoder (often pipe-joined strings).

## See also

- [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) — “Multivalue fields and overlapping regions”
- [CLI reference](CLI-Reference.md) — `pando` flags
