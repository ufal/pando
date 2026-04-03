# Index and corpus layout

A **Pando corpus** is a **directory** containing:

- **`corpus.info`** — key/value metadata: corpus size, positional attribute list, structural types, region attribute names, `multivalue`, `nested`, `overlapping`, `zerowidth`, default `within`, etc.
- **Memory-mapped data files** — one family per positional attribute (`form.dat`, `form.lex`, `form.rev`, …; optional `form.mv.*` for multivalue); `*.rgn` for structural spans; `*_attr.val` for region attributes; `dep.*` when dependencies exist.

## Building

- **`pando-index`** (CLI) reads JSONL streams and writes the directory layout.
- **Streaming integration** (e.g. flexicorp) feeds the same `StreamingBuilder` API; see `dev/PANDO-INDEX-INTEGRATION.md`.

## Reverse indexes

Region attributes may have **`.lex` / `.rev`** sidecars for fast equality on `::` filters and planning. Older corpora without them still answer queries but may use slower paths.

## Further reading

- `dev/PANDO-INDEX-INTEGRATION.md` — integration modes and event API
- [TEITOK integration](TEITOK-Integration.md) — TEITOK project layout, `pando/` folder, flexicorp
- `dev/PANDO-SEGMENTS.md` — segment design (if present)
- [README](../README.md) — benchmark and dev layout
