# Collocations and keyness

Pando-CQL can run **collocation** analysis (words typical in the **window** or **dependency** neighbourhood of a hit set) and **keyness** (items statistically over- or under-represented in a **focus** subcorpus vs a **reference**). Both follow the same pattern as `count` / `freq`: define a **named query** (or use `Last`), then issue a **`coll`**, **`dcoll`**, or **`keyness`** command.

The full tutorial, syntax, and edge cases are in [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) — sections [Collocations](../docs/PANDO-CQL.md#collocations) and [Keyness](../docs/PANDO-CQL.md#keyness).

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

If the corpus has a **dependency index** (sentence structure `s` plus `dep.*` data), **`dcoll`** collects neighbours along **dependency edges** instead of linear distance.

```text
[upos="NOUN" & lemma="book"]; dcoll amod by lemma
```

- Relations can be **deprel** labels (`amod`, `nsubj`, …), or **`head`**, **`children`**, **`descendants`**.
- Combine relations: `dcoll head, amod by lemma`.
- For multi-token matches, **anchor** with a named token: `a:[upos="DET"] [upos="NOUN" & lemma="book"]; dcoll a.amod by lemma`.

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

- [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) — [Collocations](../docs/PANDO-CQL.md#collocations), [Keyness](../docs/PANDO-CQL.md#keyness), [Named queries and frequencies](../docs/PANDO-CQL.md#named-queries-and-frequencies)
- [CLI reference](CLI-Reference.md) — `pando` flags for windows, measures, limits
- [Dependency queries](Dependency-Queries.md) — `dcoll` prerequisites
