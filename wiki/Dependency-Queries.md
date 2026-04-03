# Dependency queries

Dependency search requires a corpus with:

- Sentence structure **`s`** (regions in `s.rgn`), and
- Dependency arrays built from token head indices during indexing (`dep.head`, `dep.euler_in`, `dep.euler_out`, etc.).

## Between-token operators

Operators between tokens include:

- **`>`** / **`<`**: head / dependent (direct)
- **`>>` / `<<`**: transitive descendants / ancestors (tree traversal)
- Negated forms (e.g. `!<`) where supported

See [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) for the full operator set and examples.

## Inside-token restrictions

PML-TQ-style restrictions embed a subtree on the token, e.g.:

```text
[upos="NOUN" & child [upos="DET"]]
```

`child`, `parent`, `ancestor`, `descendant`, `sibling` (and negations) are implemented as token restrictions.

## Dependency collocations

`dcoll` walks dependency edges (specific labels, `head`, `children`, `descendants`, …). Options such as `--window` do not apply the same way as linear `coll`; see CQL doc and `pando --help`.

## Index

Without `dep.*` files, dependency queries are unavailable or degraded. Run `pando-check` and verify `corpus.info` / file list after indexing.

## See also

- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md) — `containing subtree`
- [Index and corpus layout](Index-and-Corpus-Layout.md)
