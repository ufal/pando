# Index and corpus layout

A **Pando corpus** is a **directory** containing:

- **`corpus.info`** — key/value metadata: corpus size, positional attribute list, structural types, region attribute names, `multivalue`, **`kv_pipe`** (UD-style `Key=Val|…` columns, distinct from multivalue), `nested`, `overlapping`, `zerowidth`, default `within`, etc.
- **Memory-mapped data files** — one family per positional attribute (`form.dat`, `form.lex`, `form.rev`, …; optional `form.mv.*` for **multivalue** attrs only); `*.rgn` for structural spans; `*_attr.val` for region attributes; `dep.*` when dependencies exist. Combined UD **`feats`** uses the single `feats.*` family; **`--split-feats`** adds separate `feats#<Feature>.*` families (legacy: `feats_<Feature>.*`) — see [Multivalue attributes](Multivalue-Attributes.md#kv-pipe-attributes-ud-feats) (KV pipe section; contrasts with MV indexing).

## Building

- **`pando-index`** (CLI) reads JSONL streams and writes the directory layout.
- **Streaming integration** (e.g. flexicorp) feeds the same `StreamingBuilder` API; orchestration is documented alongside the flexicorp / TEITOK stack (see [TEITOK integration](TEITOK-Integration.md)).

## Reverse indexes

Optional **`.lex` / `.rev`** sidecars on region attributes support fast equality and membership for `::` filters and query planning.

## Further reading

- [Multivalue attributes](Multivalue-Attributes.md) — multivalue **`.mv.*`** indexes vs **KV pipe** `feats` (combined vs split columns)
- [TEITOK integration](TEITOK-Integration.md) — project layout, `pando/` folder, flexicorp
- [README](../README.md) — benchmark harness and repository layout
