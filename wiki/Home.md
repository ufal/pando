# Pando

**Pando** is a C++ corpus query engine related to Manatee, CWB/CQP, and similar tools: positional and structural (region) attributes, dependency-aware search, multivalue fields, overlapping regions where declared, and parallel / aligned-corpus queries.

If you build from source, the main binaries are `pando` (query CLI), `pando-index` (build corpora), `pando-check`, and `pando-server` (HTTP API). See [Installation](Installation.md).

## Commands (overview)

| Tool | Purpose |
| --- | --- |
| `pando` | Run CQL queries (batch or interactive REPL), KWIC, `count` / `freq` / `coll`, etc. |
| `pando-index` | Build a mmap corpus from JSONL (or via streaming integration) |
| `pando-check` | Validate corpus consistency |
| `pando-server` | Serve queries over HTTP/JSON (when built) |

Use `pando --help` for the current option list. Important query options include `--json`, `--total`, `--no-mv-explode` (see [Multivalue attributes](Multivalue-Attributes.md)), and collocation flags (`--window`, `--measures`, …).

## Key features

- **Pando CQL**: CWB-style token sequences and region constraints, extended with dependencies, named tokens, global `::` filters, `within` / `containing`, collocations, keyness, and more. Full tutorial: [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md).
- **Dependency index**: When the corpus has sentence structure `s` and dependency data, queries can use governors/dependents, `child`/`parent`/tree restrictions, and `dcoll`. See [Dependency queries](Dependency-Queries.md).
- **Multivalue attributes**: Pipe-separated values at index time; EQ/NEQ match any component; reverse indexes for components (RG-5f). See [Multivalue attributes](Multivalue-Attributes.md).
- **Structural regions**: Texts, sentences, docs, and custom region types; optional nested vs overlapping behavior per type. See [Overlapping and nested regions](Overlapping-and-Nested-Regions.md). Phrase-structure (constituency) trees as nested regions: [Constituency grammar and nested regions](Constituency-and-Nested-Regions.md).
- **Aligned corpora**: Shared translation-unit ids (`s_tuid`, token `tuid`); pipe-separated **multivalue** ids for n:n bitext; `query1 with query2` for simple sentence-aligned search. See [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md).
- **On-disk format**: Memory-mapped files under one directory; `corpus.info` describes attributes and flags. See [Index and corpus layout](Index-and-Corpus-Layout.md).
- **TEITOK-style deployments**: a `pando/` index directory beside `manatee/` / `cqp/`, flexicorp-driven indexing, and `tuid`-based alignment. See [TEITOK integration](TEITOK-Integration.md).

## Wiki pages

- [Installation](Installation.md)
- [Quick Start](Quick-Start.md)
- [Query language (Pando CQL)](Query-Language-and-CQL.md)
- [Multivalue attributes](Multivalue-Attributes.md)
- [Overlapping and nested regions](Overlapping-and-Nested-Regions.md)
- [Constituency grammar and nested regions](Constituency-and-Nested-Regions.md)
- [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md)
- [Dependency queries](Dependency-Queries.md)
- [Index and corpus layout](Index-and-Corpus-Layout.md)
- [CLI reference](CLI-Reference.md)
- [Contributing](Contributing.md)
- [TEITOK integration](TEITOK-Integration.md) — project layout, `tuid`, flexicorp pipeline

## Status

The engine is under active development; CLI flags, JSON field names, and on-disk layouts may change. The [README](../README.md) describes scope and companion repositories (flexipipe, flexiconv, flexicorp).
