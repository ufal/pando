# Collocations and keyness

Pando-CQL can run **collocation** analysis (words typical in the **window** or **dependency** neighbourhood of a hit set) and **keyness** (items statistically over- or under-represented in a **focus** subcorpus vs a **reference**). Both follow the same pattern as `count` / `freq`: define a **named query** (or use `Last`), then issue a **`coll`**, **`dcoll`**, or **`keyness`** command.

An introduction is in [PANDO-CQL.md](PANDO-CQL.md#counting-and-comparing); commands and settings are in the [reference](PANDO-CQL-Reference.md#commands).

---

## Collocations (`coll`)

After a query that selects **node** tokens (e.g. a lemma), **`coll`** gathers **surrounding** tokens inside a left/right **window** and ranks **collocates** by association strength.

```text
[upos="VERB" & lemma="book"]; coll by lemma
```

- **`by lemma`** (or `form`, `upos`, …) chooses which attribute to bucket collocates with.
- Default window and association measures come from the CLI; override with flags such as **`--window`**, **`--left`**, **`--right`**, **`--measures`**, **`--min-freq`**, **`--max-items`** (see [CLI reference](CLI-Reference.md) and `pando --help`).
- Typical measures include **logdice**, **MI**, **MI3**, **t-score**, **log-likelihood (ll)**, **dice**; the default is **logdice** when none are set.

Use a **named query** when you need a stable handle for later commands:

```text
Hits = [upos="VERB" & lemma="book"]; coll Hits by lemma;
```

---

## Dependency collocations (`dcoll`)

If the corpus has a **dependency index** (sentence structure `s` plus `dep.*` data), **`dcoll`** collects neighbours along **dependency edges** instead of linear distance. That is to say, the most typical nominal subject or adjectival modifier.

```text
[upos="NOUN" & lemma="book"]; dcoll amod by lemma
```

- Relations can be **deprel** labels (`amod`, `nsubj`, …), or **`head`**, **`children`**, **`descendants`**.
- Combine relations: `dcoll head, amod by lemma`.
- For multi-token matches, **anchor** with a named token: `a:[upos="DET"] [upos="NOUN" & lemma="book"]; dcoll a.amod by lemma`.
- A second attribute after `by` is tallied per collocate without changing the measures: `dcoll children by lemma, deprel` adds, for each collocate, how often it came in each relation (`de` → `case:27 det:12`); `dcoll head by lemma, upos` shows the word class of the heads. In JSON this is `"breakdown": {"case": 27, "det": 12}` per collocate (most frequent first) and `"breakdown_attribute"` on the result; an unknown attribute is an error.

### What `dcoll` scores compare with

A score compares the observed count (node, relation, collocate) with what chance would give: from how often the node has any relative of this kind, how often the collocate is such a relative, and how many such pairs there are. In a window collocation every token can fill a window position, so the collocate's corpus frequency and the corpus size are the right totals. In `dcoll` the observed count is of dependency pairs, so the other totals count the same pairs:

| `dcoll …` | the collocate's frequency (`relation_freq`) | N (`relation_total`) |
| --- | --- | --- |
| a deprel (`amod`) | how often it is an `amod` dependent | all `amod` edges |
| `children` | how often it has a head | all edges |
| `head` | how often it is a head (once per dependent) | all edges |
| `descendants` | the number of its ancestors, summed over its tokens | all (ancestor, descendant) pairs |

Several relations add up (`dcoll head, amod` adds both); over a relation layer (`dcoll (via eff) …`) the layer's edges count. A collocate that is often used in other ways (*iron* mostly as a noun) is then not ranked down as a modifier for that; the score says how much it goes with the node *when* it is used in this relation, as word sketches do.

This is the default (`"baseline": "relation"`). `set dcoll-baseline corpus` (CLI `--dcoll-baseline corpus`, FFI option `dcoll_baseline`) gives the scores of pando before 0.4.5: the collocate's corpus frequency and the corpus size. pando-index stores these totals for the usual attributes (`lemma`, `form`, `word`, `upos`, `xpos`, `deprel`; see [dcoll totals](Index-and-Corpus-Layout.md#dcoll-totals-attrdcoll)); for other attributes, older indexes and relation layers the first `dcoll` of a kind of relation counts them in a pass over the corpus, which pando-server then keeps.

See [Dependency queries](Dependency-Queries.md) for how dependencies are encoded in queries.

---

## Keyness

**Keyness** compares a **focus** query to a **reference** — either the **rest of the corpus** (complement) or another **named** query via **`vs`**.

**Focus vs complement** (keywords in French vs everything else):

```text
[text_lang="French"]; keyness by lemma
```

**Two explicit subcorpora**:

```text
Fr = [text_lang="French"]; En = [text_lang="English"]; keyness Fr vs En by lemma
```

The grouping attribute (`lemma`, `upos`, …) is the dimension along which over- and under-use is scored. Output ordering and display options are controlled from the CLI; the engine uses a **log-likelihood G²**-style contrast for ranked rows (see JSON / text tables from `pando`).

---

## See also

- [PANDO-CQL.md](PANDO-CQL.md#counting-and-comparing) — introduction; [reference](PANDO-CQL-Reference.md#commands) — commands, measures and settings
- [CLI reference](CLI-Reference.md) — `pando` flags for windows, measures, limits
- [Dependency queries](Dependency-Queries.md) — `dcoll` prerequisites
