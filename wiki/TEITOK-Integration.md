# TEITOK integration

**TEITOK** is a web-based corpus environment; **Pando** is designed to sit alongside **Manatee/CWB** as another query backend, fed by the same corpus preparation pipelines (notably **flexicorp**). This page describes how TEITOK-oriented layouts and conventions map to Pando—without duplicating TEITOK’s own documentation.

## Role in the stack

Typical data flow:

```text
TEITOK (PHP UI)  →  flexicorp (export / indexing CLI)  →  JSONL or API  →  pando-index / StreamingBuilder  →  pando/ (mmap corpus)
```

The same conceptual events (tokens, sentence boundaries, regions) can also drive **Manatee** and **CWB** indexes from a **single corpus walk**. Details of that orchestration live in **`dev/PANDO-INDEX-INTEGRATION.md`** (flexicorp goals, streaming API, JSONL fallback).

**Principle:** Pando remains **engine-generic**. TEITOK-specific paths (PHP, legacy CWB sockets, xidx coupling) belong in flexicorp/TEITOK, not in the core C++ tree.

## Directory layout: `pando/` next to other engines

Many TEITOK deployments keep **multiple index folders** under one project root, for example:

```text
/path/to/teitok/project/
  pando/          ← mmap corpus (corpus.info + data files)
  manatee/
  cqp/
  ...
```

The **`pando` CLI** resolves the corpus path in a TEITOK-friendly way:

1. If the first argument is a directory containing **`corpus.info`**, that directory is the corpus (absolute or relative path).
2. Otherwise, if **`./pando`** exists and contains `corpus.info`, the corpus defaults to **`pando`** and all arguments are treated as the query string.
3. Otherwise, if **`./corpus.info`** exists in the current directory, the corpus is **`.`** (run from inside the index).

So from the TEITOK project root you can run:

```bash
pando '[lemma="example"]' --limit 5
```

when `./pando/corpus.info` is present, without passing `./pando` explicitly. See `pando --help` for the exact resolution order.

The benchmark harness **`dev/run_benchmark.py`** uses the same idea: `--corpus-root` points at a tree that may contain `pando/`, `manatee/`, `cqp/` side by side (see [README](../README.md) “Benchmark harness”).

## Alignment: `tuid` (translation unit id)

TEITOK models **parallel texts** with shared identifiers. In Universal Dependencies / CoNLL-U imports, Pando’s **CoNLL-U reader** supports:

- **Sentence-level `tuid`**: from sentence comments (e.g. CQP-style `# tuid = …` / TEITOK conventions). The value is stored on the **`s`** region and copied onto each token in that sentence as a positional attribute **`tuid`** until the sentence ends.
- **Word-level override**: if the **MISC** column (field 10) contains `tuid=…`, that value overrides the sentence default for that token (TEITOK-style word alignment).

After indexing, CQL can relate named tokens across languages via those attributes, e.g. `eng.s_tuid = nld.s_tuid` or token-level `eng.tuid = nld.tuid`. For **many-to-many** alignment, ids are often **pipe-separated multivalues**; see the detailed guide [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md) and the CQL tutorial in [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) (“Named tokens and aligned corpora”).

## Contractions and multiword tokens

TEITOK and UD often represent **contractions** with multiple analysis layers. Pando does **not** duplicate that layered token graph: it indexes **surface tokens** and can emit a reserved **`contr`** region spanning sub-tokens with a **`form`** attribute. Shorthand queries like `"aux"` expand to token or contraction matches as described in [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) (“Raw queries and contractions”).

## Default `within` and text structure

TEITOK corpora are often segmented by **text** / **document**. Pando’s **`default_within`** (in `corpus.info` / JSONL header) controls implicit scoping for queries; TEITOK-driven imports typically set this to match how the UI expects subcorpus boundaries. See [Index and corpus layout](Index-and-Corpus-Layout.md).

## Overlap and standoff

TEITOK can represent **overlapping or crossing** spans (e.g. standoff layers). Pando supports **overlapping** region *types* when declared in the corpus header; full standoff parity is a broader topic (see **`extensions.md`** in the repo for design notes). For overlap semantics in queries, see [Overlapping and nested regions](Overlapping-and-Nested-Regions.md).

## Related wiki pages

| Topic | Page |
| --- | --- |
| Building indexes from events / JSONL | [Index and corpus layout](Index-and-Corpus-Layout.md) |
| Alignment queries | [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md) |
| CLI corpus path resolution | [CLI reference](CLI-Reference.md) |

## Deeper design notes (repository)

- **`dev/PANDO-INDEX-INTEGRATION.md`** — flexicorp integration, streaming builder, JSONL path.
- **`extensions.md`** — TEITOK standoff / crossing regions (future or partial scope).
