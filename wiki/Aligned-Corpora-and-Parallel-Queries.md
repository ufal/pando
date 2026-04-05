# Aligned corpora and parallel queries

This page explains **how to model parallel / bitext data** in Pando, how **translation unit ids (`tuid`)** work, and how **n:n (many-to-many) alignment** is represented. It complements [TEITOK integration](TEITOK-Integration.md) (CoNLL-U comments, MISC) and the CQL tutorial in [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) (“Named tokens and aligned corpora”, “Named queries and frequencies”).

---

## 1. Concepts

### Translation units

A **translation unit** is whatever you treat as the minimal link between two sides of a corpus (sentence, clause, paragraph, token, …). Pando does not invent ids: **you** assign stable strings (or numbers as strings) that identify “this bit of L1 corresponds to this bit of L2”.

Typical **levels**:

| Level | Typical use | Region / attribute names (examples) |
| --- | --- | --- |
| **Sentence** | Bitext at `s` | `s` region attribute `tuid` → CQL `s_tuid` (e.g. `eng.s_tuid`) |
| **Paragraph / document** | Higher alignment | e.g. `p_tuid`, `doc`-level attrs (if indexed) |
| **Token** | Word alignment | Positional attribute `tuid` on each token |

Indexing from CoNLL-U is described in [TEITOK integration](TEITOK-Integration.md): sentence-level `# tuid = …`, optional per-token override in **MISC** `tuid=…`.

### 1:1 vs 1:n vs n:1 vs n:n

- **1:1** — One sentence on the left matches exactly one sentence on the right; a **single** shared id is enough.
- **1:n** or **n:1** — One sentence on one side corresponds to **several** on the other (merge/split). The same **logical** translation unit should still be identifiable: either one id repeated on all participating sentences, or **multiple ids** on one side and one on the other (see below).
- **n:n** — **Many-to-many**: one L1 sentence relates to several L2 sentences *and* one L2 sentence relates to several L1 sentences. This is common in **resegmented** or **multi-reference** bitexts.

**Guideline:** treat alignment as a **relation between translation units**, not only between sentences. If the relation is many-to-many, **ids are naturally multi-valued**: one segment may carry **several** partner identifiers.

---

## 2. Multi-valued `tuid` (pipe-separated)

### Encoding

Store **multiple** translation unit ids in one attribute as a **pipe-separated** list, same convention as other multivalue fields:

```text
s001|s002|s007
```

- Component order inside a pipe list does not affect **set-style** matching; for reporting or exports that compare whole stored strings, use a **canonical ordering** (e.g. sorted ids) if you need deterministic string equality outside the query engine.
- In the JSONL / index header, list `tuid` (and `s_tuid` / region attrs as needed) in **`multivalue`** so the index builds multivalue sidecars and queries use **component / set** semantics (see [Multivalue attributes](Multivalue-Attributes.md)).

### Semantics (query side)

For attributes declared **multivalue**, Pando-CQL uses **set-style** interpretation for many constructs (see [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md), “Multivalue fields and overlapping regions”):

- `[s_tuid="s001"]` — the sentence’s `tuid` **set** contains `s001`.
- For comparisons **between** two bindings `a.X` and `b.Y` on multivalue fields, the engine uses **non-empty intersection** of component sets. So `a.s_tuid = b.s_tuid` holds when the two sides share at least one translation-unit id.

**Scalar** attributes remain single-valued. For attributes declared **`multivalue`**, leaf conditions, `::` filters, aggregates, and parallel / `with` alignment all follow the same **multivalue table** in [PANDO-CQL.md](../docs/PANDO-CQL.md) (set / component semantics).

---

## 3. Modeling patterns (examples)

### 3.1 Simple 1:1 sentence alignment

Every aligned pair shares one id:

| Side | `s` region `tuid` |
| --- | --- |
| English sentence | `tu-00042` |
| Dutch sentence | `tu-00042` |

CQL (illustrative; language attrs depend on your index). Often you run a **first** query that names `eng`, then a **second** query in the same session that names `nld` and ties alignment (persistent names — see CQL doc):

```text
eng:[lemma="property"] :: eng.text_lang = "English"
```

```text
nld:[upos="NOUN"] :: match.text_lang = "Dutch" & eng.s_tuid = nld.s_tuid
```

Here `eng` / `nld` are **named tokens**; `s_tuid` is the sentence-level attribute (region naming: struct `s`, attribute `tuid` → `s_tuid` in queries).

### 3.2 Word-level alignment (token `tuid`)

MISC / indexer copies **token** `tuid` (possibly overriding sentence default). Use when you need **word-aligned** bitext:

```text
nld:[] :: match.text_lang = "Dutch" & eng.tuid = nld.tuid
```

(With appropriate `eng` defined in a previous named query or the same query, depending on session usage.)

### 3.3 One-to-many (one L1 sentence → two L2 sentences)

**Option A — Repeat the same canonical id** on both L2 sentences:

- L2a: `s_tuid = tu-100`
- L2b: `s_tuid = tu-100`

**Option B — Multivalue on one side:**

- L1: `s_tuid = tu-L2a|tu-L2b`
- L2a: `s_tuid = tu-L2a`
- L2b: `s_tuid = tu-L2b`

Option B is closer to “explicit partners”; intersection-based joins match **any** shared id.

### 3.4 n:n (many-to-many)

Example: L1 sentence **S** aligns to L2 sentences **A** and **B**; L2 sentence **A** also aligns to L1 sentences **S** and **T**.

A workable encoding is:

- **S**: `s_tuid = id-S|id-A|id-B` (or only the minimal set that encodes your graph)
- **T**: `s_tuid = id-T|id-A`
- **A**: `s_tuid = id-S|id-T|id-A` …

The **guideline** is: **every id that participates in a link appears on every segment that belongs to that link’s clique**, so that **intersection** tests recover “these two segments co-occur in the alignment graph”. When you want one shared key for every segment in a clique, a **synthetic bundle id** (repeated on all those segments) is a common modeling choice.

---

## 4. `query1 with query2` (sentence-aligned shortcut)

For **sentence-aligned** search without naming tokens, you can write:

```text
[form="property"] with [form="bezit"]
```

The parser builds a **parallel** query; the engine runs **source** and **target** subqueries and **pairs** matches subject to **`::`** alignment constraints (typically equality on `tuid` / `s_tuid` or other attrs — see [PANDO-CQL.md](../docs/PANDO-CQL.md) and `pando --help`).

Use **`with`** for straightforward sentence-aligned search. For **n:n** graphs, **named tokens** and explicit `::` conditions over `s_tuid` / `tuid` (multivalue intersection as usual) express the alignment you need.

**JSON:** parallel results use **source/target** spans (`parallel` in JSON output) alongside or instead of a single KWIC row.

---

## 5. Aggregations: `count`, `freq`, and translation equivalents

As in [Named queries and frequencies](../docs/PANDO-CQL.md#named-queries-and-frequencies), **query names persist** across statements in a session, so you can anchor an **English (source) slice** and then **aggregate over aligned Dutch (target) tokens**—the same idea as `Matches = …; count Matches by a.form;`, but with `eng` / `nld` and `s_tuid` / `tuid` alignment.

**Sentence alignment** — after you have run a query that binds the name `eng` to English hits for *property*, count **how often each Dutch form** appears in sentences that share a `tuid` with those English sentences:

```text
eng:[lemma="property"] :: eng.text_lang = "English"
```

```text
DutchWords = nld:[] :: match.text_lang = "Dutch" & eng.s_tuid = nld.s_tuid; count DutchWords by nld.form;
```

The most frequent row in that table answers “most common Dutch **surface** form in the aligned sentences” (e.g. *bezit*, *eigendom*, … depending on the corpus). Use **`nld.lemma`** instead of **`nld.form`** for lemma frequencies.

**Token alignment** — when word-level `tuid` links translation units, restrict to Dutch tokens aligned to the same unit as the English *property* token:

```text
eng:[lemma="property"] :: eng.text_lang = "English"
```

```text
DutchAligned = nld:[] :: match.text_lang = "Dutch" & eng.tuid = nld.tuid; count DutchAligned by nld.form;
```

**Multi-attribute breakdowns** work the same as elsewhere: e.g. `count DutchWords by nld.lemma, nld.upos` for a cross-tab of lemma and part of speech. Use **`freq`** instead of **`count`** when you want normalized frequencies. Output sorting and limits are controlled from the CLI (see [CLI reference](CLI-Reference.md)).

---

## 6. Checklist for indexers

1. **Decide levels** — sentence-only, token-only, or both.
2. **Choose id scheme** — stable, unique per translation unit you care about.
3. **n:n** — use **pipe-separated** lists; add `tuid` / `s_tuid` (and similar) to **`multivalue`** in the corpus header / `corpus.info`.
4. **CoNLL-U** — sentence `# tuid = …`; token overrides `tuid=a|b` in MISC (see [TEITOK integration](TEITOK-Integration.md)).
5. **Validate** — `pando-check`, spot-check a few ids in `pando` / `--json` output.

---

## 7. See also

- [TEITOK integration](TEITOK-Integration.md) — TEITOK layout, flexicorp, CoNLL-U ingestion
- [Multivalue attributes](Multivalue-Attributes.md) — pipe semantics, indexes, `count` / `freq`
- [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) — full CQL, multivalue table, [named queries and frequencies](../docs/PANDO-CQL.md#named-queries-and-frequencies)
- [Index and corpus layout](Index-and-Corpus-Layout.md) — `corpus.info`, region attrs
