# Index and corpus layout

A **Pando corpus** is a **directory** containing:

- **`corpus.info`** — key/value metadata: corpus size, positional attribute list, structural types, region attribute names, `multivalue`, **`kv_pipe`** (UD-style `Key=Val|…` columns, distinct from multivalue), `nested`, `overlapping`, `zerowidth`, default `within`, etc.
- **Memory-mapped data files** — one family per positional attribute (`form.dat`, `form.lex`, `form.rev`, …; optional `form.mv.*` for **multivalue** attrs only); `*.rgn` for structural spans; `*_attr.val` for region attributes; `dep.*` when dependencies exist. Combined UD **`feats`** uses the single `feats.*` family; **`--split-feats`** adds separate `feats#<Feature>.*` families (legacy: `feats_<Feature>.*`) — see [Multivalue attributes](Multivalue-Attributes.md#kv-pipe-attributes-ud-feats) (KV pipe section; contrasts with MV indexing).

## Building

- **`pando-index`** (CLI) reads JSONL streams and writes the directory layout.
- **Streaming integration** (e.g. flexicorp) feeds the same `StreamingBuilder` API; orchestration is documented alongside the flexicorp / TEITOK stack (see [TEITOK integration](TEITOK-Integration.md)).

## Reverse indexes

Optional **`.lex` / `.rev`** sidecars on region attributes support fast equality and membership for `::` filters and query planning.

## Packed postings (`.rev.pfb`)

The plain `<attr>.rev` stores every position of every value as a fixed-width
integer (2, 4 or 8 bytes), next to `<attr>.rev.idx` (the start of each value's
list). `pando-index --upgrade <dir> --packed-rev` adds a block-compressed copy
of the postings of every single-valued positional attribute (or only the ones
named: `--packed-rev form,lemma`):

- `<attr>.rev.pfb.idx` — a 64-byte header and a two-level offset table (one
  64-bit base per 64 values, one 32-bit offset per value);
- `<attr>.rev.pfb` — per value, its sorted positions cut into blocks of 128, each
  stored as the gaps between positions bit-packed at the block's own width. A
  value that occurs at most 128 times is a varint first position plus one width
  byte; a longer list starts with a skip table (each block's first position,
  offset and width), so a block can be reached without decoding the ones before it.

The counts per value stay in `<attr>.rev.idx`, which every index keeps. On the
38M-token UD demo corpus the packed files are 57% of `form.rev`, 48% of
`lemma.rev`, 21% of `upos.rev` and 24% of `deprel.rev` (217 MB instead of 582 MB);
building them takes a few seconds.

Which postings are read:

| Situation | Read from |
|---|---|
| `.rev` present, `PANDO_REV` unset or `auto` / `raw` | `.rev` (mmapped, no decoding) |
| `.rev` present, `PANDO_REV=packed` and an up-to-date `.rev.pfb` | `.rev.pfb` |
| `.rev` removed (`--drop-rev`) | `.rev.pfb` (required) |

`--drop-rev` (implies `--packed-rev auto`) first checks that every value decodes
to exactly the positions in `.rev`, then deletes the `.rev`. A `.rev.pfb` older
than its `.rev.idx` is stale and is not opened (rebuild with `--packed-rev`).

From packed postings a value's list is decoded block by block (128 positions)
as a query touches it: the first page of `[upos="NOUN"]` decodes one block, a
search for a position (a merge driven by a rare token, a count inside region
intervals) finds the block through the skip table and decodes only that one, and
a count needs no decoding (the counts are in `.rev.idx`). Kernels that scan a
whole list (transitive dependencies, gap sequences, some dependency joins)
decode all of it, about 2–3 ns per position including the memory it is decoded
into. Lists of 4096 or more positions are kept, with the blocks decoded so far,
in a process-wide cache (`PANDO_REV_CACHE_MB`, default 256 MB; a list larger
than a quarter of it is not kept), so in `pando-server` a block is decoded once.
On the 38M demo the whole perf benchmark (166 measurements) takes as long from
packed postings as from `.rev` (17.9 s both); single one-shot CLI queries that
scan a frequent list are up to 20–40 ms slower. Region attribute `.rev` files,
`contr_form` and the dependency edge postings (`dep.pair.*.rev`) are not packed.

## Packed ids (`.dat.pk`)

`<attr>.dat` holds the lexicon id of every token in 1, 2 or 4 bytes; KWIC,
`count by`, `freq`, `coll`, sorting and every per-token check read it.
`pando-index --upgrade <dir> --packed-dat` (or `--packed-dat form,lemma`) adds
a compact copy with constant-time access to any token:

- lexicons below 65,536 values: every id in ⌈log₂ V⌉ bits (upos 5 bits instead
  of 8, deprel 9 instead of 16);
- larger lexicons (form, lemma): each id replaced by its frequency rank (a
  `rank → id` table in the file), ranks in blocks of 16 tokens with a 2-bit
  length (1–4 bytes) per token and a block index.

On the 38M demo: form 65% of `.dat`, lemma 57%, deprel 56%, upos 63% — 253 MB
instead of 420 MB, built in a few seconds. `PANDO_DAT=packed` reads the ids
from it although `.dat` exists; `--drop-dat` (implies `--packed-dat auto`)
checks that every id reads back the same and then removes the `.dat`.

It is a trade of memory for time: reading an id costs about 3× as much (one
token at random ~80 ns instead of ~17 ns, in order ~3 ns instead of ~1 ns per
token), so queries that read millions of ids get slower — on the demo
`[upos="NOUN"]; count by lemma` 0.10 → 0.19 s, `sort by lemma` over 8.5M hits
1.2 → 1.6 s, `[upos="ADJ"] [upos="NOUN"]; coll by lemma` 0.48 → 0.71 s —
while searches that do not (most token, sequence and dependency queries) are
unchanged. Worth it when the corpus would otherwise not fit in memory (the
`.dat` of form and lemma are 8 GB per billion tokens, packed about 5 GB): a
token read from disk costs far more than its decoding.

## Versioned index directories (hot swap)

A corpus that servers keep open is published in versions, so a new build or
upgrade never touches files a server has mapped:

```
ROOT/
  current -> versions/<id>     relative symlink, replaced atomically (rename)
  versions/<id>/               a complete index; corpus.info index_id=<id>
  versions/.building-<id>/     a publish in progress
  .publish.lock                one publish per ROOT at a time
```

`pando-index <input> --publish ROOT` builds a new version,
`pando-index --upgrade ROOT --publish …` upgrades a copy of the current one
(modification times kept, so the staleness checks still hold); both switch
`current` only once the new version is complete, and keep the newest `--keep`
versions (default 3). The symlink is relative, so `ROOT` can be mounted at a
different path on each machine (shared storage, local copies). Servers and
hosts open `ROOT/current`; a handle keeps the version it opened (symlinks are
resolved at open) until the host swaps it — see
[Embedding the server](Embedding-the-Server.md) ("hot swap"). Every build and
every `--upgrade` writes a new `index_id=` to `corpus.info`, so hosts can tell
versions apart also for unpublished directories.

## Further reading

- [Multivalue attributes](Multivalue-Attributes.md) — multivalue **`.mv.*`** indexes vs **KV pipe** `feats` (combined vs split columns)
- [TEITOK integration](TEITOK-Integration.md) — project layout, `pando/` folder, flexicorp
- [README](../README.md) — benchmark harness and repository layout
