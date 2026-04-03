# Pando wiki

Documentation for the Pando corpus query engine. All pages are Markdown in this folder (same convention as **flexipipe** / **flexiconv** sibling repos).

**Preview locally:** from the repo root run `python scripts/serve-wiki-preview.py` and open http://localhost:8765/ to browse before pushing. Links to `../docs/` from wiki pages resolve in GitHub and editors; the local preview server only serves `wiki/`, so open files under `docs/` separately when needed.

| Page | Description |
| --- | --- |
| [Home](Home.md) | Overview, binaries, features, links to all pages |
| [Installation](Installation.md) | Build requirements, outputs, optional install path |
| [Quick Start](Quick-Start.md) | First corpus, first query, pointers to samples |
| [Query language (Pando CQL)](Query-Language-and-CQL.md) | Where the full CQL spec lives; dialects |
| [Multivalue attributes](Multivalue-Attributes.md) | Pipe values, indexes, queries, count/freq, CLI flags |
| [Overlapping and nested regions](Overlapping-and-Nested-Regions.md) | Structural types, lookup, zero-width |
| [Constituency grammar and nested regions](Constituency-and-Nested-Regions.md) | Phrase-structure trees as nested `node` regions |
| [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md) | `tuid`, alignment joins, `with` |
| [Dependency queries](Dependency-Queries.md) | Operators, token restrictions, `dep.*` index |
| [Index and corpus layout](Index-and-Corpus-Layout.md) | `corpus.info`, mmap files, streaming build |
| [CLI reference](CLI-Reference.md) | Common `pando` flags and related tools |
| [Contributing](Contributing.md) | Repo layout, deeper design docs in `dev/` |
| [TEITOK integration](TEITOK-Integration.md) | TEITOK layout, flexicorp, `tuid`, `pando/` next to other engines |

Start with [Home](Home.md), or [Installation](Installation.md) + [Quick Start](Quick-Start.md).

The canonical **language reference** remains [../docs/PANDO-CQL.md](../docs/PANDO-CQL.md) in the tree (long-form tutorial). Wiki pages summarize behavior and link there where appropriate.
