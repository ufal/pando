# Dependency queries

Dependency search requires a corpus with:

- Sentence structure **`s`** (regions in `s.rgn`), and
- Dependency arrays built from token head indices during indexing (`dep.head`, `dep.euler_in`, `dep.euler_out`, etc.).

## Between-token operators

Operators between tokens include:

- **`>`** / **`<`**: head / dependent (direct)
- **`>>` / `<<`**: transitive descendants / ancestors (tree traversal)
- Negated forms (e.g. `!<`) where supported

See [PANDO-CQL.md](PANDO-CQL.md) for the full operator set and examples.

## Inside-token restrictions

PML-TQ-style restrictions embed a subtree on the token, e.g.:

```text
[upos="NOUN" & child [upos="DET"]]
```

`child`, `parent`, `ancestor`, `descendant`, `sibling` (and negations) are implemented as token restrictions.

## Dependency collocations

`dcoll` walks dependency edges (specific labels, `head`, `children`, `descendants`, …). Options such as `--window` do not apply the same way as linear `coll`; see CQL doc and `pando --help`.

## Head attributes (`head#lemma`)

`pando-index --upgrade <dir> --head-attrs upos,deprel,lemma` adds, for each
listed attribute A, a positional attribute `head#A`: the value of A on the
token's dependency head (`<root>` for tokens without a head). KonText /
Manatee corpora often carry the same thing as `p_lemma`, `p_upos`. They can be
queried, shown and counted like any attribute (`head/lemma` is the same name):

```text
[deprel="nsubj" & head#upos="VERB"]
[upos="NOUN"]; count by head#lemma
```

With head attributes the engine also reads a two-token dependency query
`[P] > [C]` (or `[C] < [P]`) as the one-token query on the dependent,
`[C & P']` with every attribute A of P read as `head#A`, whenever every
attribute in P has a head attribute (region attributes of `s` / `text` / `doc`
are allowed: a head shares its dependent's sentence) and nothing else
separates the two tokens (`count by` fields must name the tokens:
`a.lemma` → `head#lemma`). The answers are the same; the one-token fast paths
(bitmaps, posting merges) make it cheap cold as well as warm: on a 38M-token
UD corpus `[upos="VERB"] > [deprel="nsubj"]` reads 15 MB from disk instead of
108 MB, and `[upos!="VERB"] > [deprel="nsubj"]` takes 25 ms instead of 350 ms.
Unlike the edge postings (`--dep-pairs H:C`, one file per pair of
attributes), N head attributes cover every combination. `>>`, `!>` and
queries of more than two tokens keep the dependency paths.
`PANDO_HEADATTR=off` disables the rewrite (tests, benchmarks).

## Index

Without `dep.*` files, dependency queries are unavailable or degraded. Run `pando-check` and verify `corpus.info` / file list after indexing.

## See also

- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md) — `containing subtree`
- [Index and corpus layout](Index-and-Corpus-Layout.md)
