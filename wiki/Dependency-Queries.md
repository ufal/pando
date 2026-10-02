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

## Head attributes

`pando-index` (when it builds an index, and `--upgrade`) stores, for `upos`,
`deprel` and `lemma` by default (`--head-attrs none` opts out, `--head-attrs
A,B` chooses), the value of each attribute A on every token's dependency head
(KonText / Manatee corpora often carry the same as `p_lemma`, `p_upos`). These
are storage only: they are not listed with the attributes, not shown in hits
and not named in queries (`head#lemma` is refused). A head attribute has no per-token
file of its own (its value at a token is A's at the token's head, through
`dep.head_rel`): it is a lexicon, packed postings (`.rev.pfb`, verified, the
plain `.rev` dropped) and, for upos / deprel, bitmaps — on the 38M demo 287 MB
for upos, deprel and lemma together (about 7.5 bytes per token). The engine uses them to
answer the head's conditions on the dependent itself:

- `[deprel="nsubj" & parent [upos="VERB"]]`: a `parent [P]` restriction (not
  negated, unlabeled, not inside another restriction) becomes a condition on
  the token, so the one-token fast paths (bitmaps, posting merges) apply, in
  any query and any token: `[upos="ADJ"] [upos="NOUN" & parent [upos!="VERB"]]`.
- `[upos="VERB"] > [deprel="nsubj"]` (or `[deprel="nsubj"] < [upos="VERB"]`):
  a two-token query with one `>` / `<` is answered as the one-token query on
  the dependent, and the head is put back into each hit — the hits, their two
  positions, labels and order are the same. `count by` works when its fields
  name the tokens: `a:[upos="VERB"] > b:[deprel="obj"]; count by a.lemma, b.lemma`.

Both apply when every attribute in P has a head attribute (region attributes
of `s` / `text` / `doc` may appear too: a head shares its dependent's
sentence). On a 38M-token UD corpus `[upos="VERB"] > [deprel="nsubj"]` reads
15 MB from disk instead of 108 MB (cold 30 ms instead of 640 ms), and
`[deprel="nsubj" & parent [upos="VERB"]]` takes 6 ms instead of 200 ms.
Unlike the edge postings (`--dep-pairs H:C`, one file per pair of
attributes), N head attributes cover every combination. `>>`, `!>`,
`not parent [ ]` and longer chains of relations keep the dependency paths.
`PANDO_HEADATTR=off` disables both rewrites (tests, benchmarks).

## Index

Without `dep.*` files, dependency queries are unavailable or degraded. Run `pando-check` and verify `corpus.info` / file list after indexing.

Files: `dep.head` (sentence-local head, int16 per token), `dep.euler_in` /
`dep.euler_out` (Euler tour times for `>>` / descendants, int16 each),
`dep.head_rel` (head − position, int16) and `dep.head_rel8` (the same in one
byte per token, with the few offsets beyond ±127 in `dep.head_rel8.exc`; 1891
of 38M tokens in the UD demo). `pando-index --upgrade` writes the derived
files; readers use `dep.head_rel8` when it is there.
`pando-index --upgrade <dir> --compact-deps` then removes `dep.head` and
`dep.head_rel` after checking that `dep.head_rel8` gives the same head for
every token: 8 → 5 bytes per token of dependency data (on the demo 305 → 191
MB), at the same query speed. An index compacted this way needs this pando
version or later.

## See also

- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md) — `containing subtree`
- [Index and corpus layout](Index-and-Corpus-Layout.md)
