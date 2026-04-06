# Index and corpus layout

A **Pando corpus** is a **directory** containing:

- **`corpus.info`** — key/value metadata: corpus size, positional attribute list, structural types, region attribute names, `multivalue`, `nested`, `overlapping`, `zerowidth`, default `within`, etc.
- **Memory-mapped data files** — one family per positional attribute (`form.dat`, `form.lex`, `form.rev`, …; optional `form.mv.*` for multivalue); `*.rgn` for structural spans; `*_attr.val` for region attributes; `dep.*` when dependencies exist.

## Building

- **`pando-index`** (CLI) reads JSONL streams and writes the directory layout.
- **Streaming integration** (e.g. flexicorp) feeds the same `StreamingBuilder` API; orchestration is documented alongside the flexicorp / TEITOK stack (see [TEITOK integration](TEITOK-Integration.md)).

## Reverse indexes

Optional **`.lex` / `.rev`** sidecars on region attributes support fast equality and membership for `::` filters and query planning.

## Further reading

- [TEITOK integration](TEITOK-Integration.md) — project layout, `pando/` folder, flexicorp
- [README](../README.md) — benchmark harness and repository layout
