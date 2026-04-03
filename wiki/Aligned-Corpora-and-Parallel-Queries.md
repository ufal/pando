# Aligned corpora and parallel queries

This page explains **how to model parallel / bitext data** in Pando, how **translation unit ids (`tuid`)** work, and how **n:n (many-to-many) alignment** is represented. It complements [TEITOK integration](TEITOK-Integration.md) (CoNLL-U comments, MISC) and the CQL tutorial in [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) (“Named tokens and aligned corpora”).

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

- Order may be arbitrary, but **for any join that compares full strings**, both sides should use the **same canonical ordering** (e.g. sorted ids) if they must match **exactly**.
- In the JSONL / index header, list `tuid` (and `s_tuid` / region attrs as needed) in **`multivalue`** so the index knows to build multivalue indexes and apply **component** semantics where applicable (see [Multivalue attributes](Multivalue-Attributes.md)).

### Semantics (query side)

For attributes declared **multivalue**, Pando-CQL uses **set-style** interpretation for many constructs (see [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md), “Multivalue fields and overlapping regions”):

- `[s_tuid="s001"]` — the sentence’s `tuid` **set** contains `s001`.
- For comparisons **between** two bindings `a.X` and `b.Y` on multivalue fields, the tutorial describes **non-empty intersection** (e.g. `a.genre = b.genre`).

**Practical rule:** two regions/tokens are “alignment-compatible” if their **sets of ids overlap** — i.e. they share **at least one** translation unit id.

### Implementation note (exact equality paths)

Some **global** filters that tie two named anchors together compare **stored strings** with **exact equality** in the current engine (e.g. `:: a.tuid = b.tuid` in alignment-filter form, and the join inside **`query1 with query2`**). For those paths, **both sides must carry the same string** if the join is expressed that way.

**Guidelines when you rely on those paths:**

1. Prefer a **single canonical alignment id** shared **verbatim** on every token/sentence in the same equivalence class (simplest for `with` and strict joins).
2. If you **must** store multiple ids per side, use **identical** pipe-strings on both sides when a strict equality join is required, **or**
3. Prefer **query structure** that evaluates alignment through **multivalue-aware** condition checks (token/region restrictions) as in the CQL spec table, **or** run **two** queries in a session and filter by hand for exploratory work.

As the engine evolves, re-check behavior for your corpus; the **data model** (pipe-separated ids + `multivalue` declaration) stays the same.

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

The **guideline** is: **every id that participates in a link appears on every segment that belongs to that link’s clique**, so that **intersection** tests recover “these two segments co-occur in the alignment graph”. For strict string-equality joins, introduce a **single synthetic id** `bundle-001` duplicated on all four segments instead.

---

## 4. `query1 with query2` (sentence-aligned shortcut)

For **sentence-aligned** search without naming tokens, you can write:

```text
[form="property"] with [form="bezit"]
```

The parser builds a **parallel** query; the engine runs **source** and **target** subqueries and **pairs** matches subject to **`::`** alignment constraints (typically `tuid` equality on anchors you provide — see parser / CLI help for the exact form your build expects).

Use this when alignment is **simple** (often 1:1 or shared canonical id per pair). For **complex n:n**, prefer **named tokens** + explicit `::` conditions or multivalue-aware expressions.

**JSON:** parallel results may expose **source/target** spans (`parallel` in JSON output) rather than a single KWIC row.

---

## 5. Checklist for indexers

1. **Decide levels** — sentence-only, token-only, or both.
2. **Choose id scheme** — stable, unique per translation unit you care about.
3. **n:n** — use **pipe-separated** lists; add `tuid` / `s_tuid` (and similar) to **`multivalue`** in the corpus header / `corpus.info`.
4. **CoNLL-U** — sentence `# tuid = …`; token overrides `tuid=a|b` in MISC (see [TEITOK integration](TEITOK-Integration.md)).
5. **Validate** — `pando-check`, spot-check a few ids in `pando` / `--json` output.

---

## 6. See also

- [TEITOK integration](TEITOK-Integration.md) — TEITOK layout, flexicorp, CoNLL-U ingestion
- [Multivalue attributes](Multivalue-Attributes.md) — pipe semantics, indexes, `count` / `freq`
- [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) — full CQL, multivalue table, named queries
- [Index and corpus layout](Index-and-Corpus-Layout.md) — `corpus.info`, region attrs
