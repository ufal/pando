# Multivalue attributes

Multivalue (MV) attributes store **sets** of atomic values as a single string using **`|`** as separator (e.g. `Poem|Song`, `artist|writer`). The corpus header (`corpus.info` / JSONL header) lists which attributes are multivalue.

**Not the same as UD `feats`:** the pipe character is also used in **morphological feature bundles** (`Case=Nom|Number=Sing`), but semantics and indexing differ. See [KV pipe attributes (UD `feats`)](#kv-pipe-attributes-ud-feats) below.

## Semantics

- **Equality / inequality**: `[wsd="artist"]` matches if the token’s `wsd` field contains **artist** as a component (not only the full string `artist|writer`). The same logic applies to region attributes like `text_genre` when declared multivalue: comparisons use component membership, not exact string equality.
- **Main lexicon vs components**: Stored token values may be the full pipe-joined string. The engine uses a **component lexicon** and **`.mv.rev`** postings (RG-5f) for efficient seed resolution on EQ queries.
- **Reverse indexes**: For positional MV attrs, optional `attr.mv.lex`, `attr.mv.rev`, … speed up “find all positions with component X”.
- **`nvals(attr)`**: In token conditions, **`[nvals(wsd)>1]`** compares the number of non-empty `|`-separated components to an integer (see [PANDO-CQL.md](PANDO-CQL.md)). Equality uses a single **`=`** (e.g. **`nvals(wsd)=2`**), not `==`.

## Indexing

`pando-index` / `StreamingBuilder` writes:

- `corpus.info` line `multivalue=attr1,attr2,...`
- For each listed positional MV attr, MV sidecar files next to the main lexicon.

The indexer writes these files for each declared multivalue attribute; queries resolve EQ and component lookups through them.

## `count` / `freq` and pipe explosion

For **single-column** `count` / `freq` on a **multivalue** positional attribute, the CLI **splits** decoded bucket keys on `|` after aggregation so each row is a **component** (RG-5f-style reporting). This applies to both `count by wsd` and `count by a.wsd` — the corpus strips the `token.` prefix when checking multivalue. Totals and percentages refer to **match rows**; component totals can exceed token count when one token has multiple components.

### Composite keys (debugging)

- **`--no-mv-explode`**: skip that explosion; keep **lexicon** keys as returned from the bucket decoder (often pipe-joined strings).

## KV pipe attributes (UD `feats`)

**KV pipe** fields store **Universal Dependencies**-style morphological bundles: several **`Name=Value`** pairs in one string, separated by **`|`** (e.g. `Number=Plur|Mood=Ind|Tense=Pres`). That is **not** MV “pick one sense from a set”: the pipe **joins features**, not alternate labels.

### Declaration

- **`corpus.info`**: `kv_pipe=feats` (comma-separated if several columns use this encoding).
- **JSONL v2 header**: `"kv_pipe": ["feats"]` — each name must appear in `positional` and must **not** appear in `multivalue` (see [PANDO-JSONL-V2.md](../dev/PANDO-JSONL-V2.md)).

### Combined `feats` (default)

With **`split_feats` false** (the usual case for UD), the indexer keeps a **single** positional attribute **`feats`**. On disk you get the normal positional family: `feats.dat`, `feats.lex`, `feats.rev`, … — one lexicon entry per **full** bundle string.

Queries use **`feats/Feature`** only — **`.`** is reserved for **`name.attr`** (token label or region binding), not UD sub-keys (see [PANDO-CQL.md](PANDO-CQL.md)). For those sub-keys the engine **does not** require a separate indexed column per feature: it resolves the value **at query time** by parsing the stored UD string on each candidate token (and uses the same logic for **`freq` / `tabulate` / aggregates** on `feats/Feature`). There are **no** `feats_Number.mv.*` style component indexes like MV: the “special filenames” story is different from multivalue sidecars.

### Split feats (optional)

With **`pando-index --split-feats`** or JSONL **`split_feats: true`**, the indexer **decomposes** each bundle into **separate positional attributes** named **`feats#<FeatureName>`** (e.g. `feats#Number`, `feats#Mood`). That keeps them distinct from **region** attributes flattened as **`feats_<Field>`** (struct `feats`, field `Number` → `feats_Number`). Each split column gets its **own** `.dat` / `.lex` / `.rev` files. **Legacy** indexes may still use **`feats_<FeatureName>`** for UD split columns; the query normalizer accepts **`feats/Feature`** → `feats#Feature` or legacy `feats_Feature` when present.

### Relation to multivalue

- **`feats`** must **not** be listed under **`multivalue`**: the pipe means **feature pairs**, not a multivalue set.
- MV **`|`** = unordered set of components with **`.mv.*`** indexes; KV **`|`** = ordered **Key=Val** pairs inside one morphological field.

### Key-only segments (future)

Some UD-style columns (notably **MISC** in CoNLL-U) can contain **flags without `=value`**. The query-time parser currently expects **`FeatureName=`** to locate a value; a bare **`|Key|`** between pipes is **not** interpreted as **`Key=True`** yet. A reasonable future rule would be to treat such tokens as **`Key=True`** (or another sentential truth value); **left unspecified for now**.

## See also

- [PANDO-CQL.md](PANDO-CQL.md) — `feats/…` syntax and aggregates
- [Index and corpus layout](Index-and-Corpus-Layout.md) — `corpus.info` keys
- [CLI reference](CLI-Reference.md) — `pando` flags, `pando-index --split-feats`
