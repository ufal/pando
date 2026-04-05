# Query language (Pando CQL)

Pando’s native language is **Pando CQL**, modeled on CWB-CQP with extensions (dependencies, named tokens, multivalue, overlapping regions, parallel queries, collocations, keyness, etc.).

## Canonical reference

The full specification and tutorial live in the repo as a single long document:

**[../docs/PANDO-CQL.md](../docs/PANDO-CQL.md)**

Keep that file as the source of truth for syntax tables, examples, and edge cases.

## Topic guides (wiki)

These pages summarize **how the engine behaves** for specific features and point back to the CQL doc:

| Topic | Page |
| --- | --- |
| Pipe-separated values, indexes, aggregation | [Multivalue attributes](Multivalue-Attributes.md) |
| Region overlap vs nesting | [Overlapping and nested regions](Overlapping-and-Nested-Regions.md) |
| Phrase-structure / constituency as nested regions | [Constituency grammar and nested regions](Constituency-and-Nested-Regions.md) |
| Alignment, `with`, `tuid` | [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md) |
| TEITOK project layout, flexicorp, `pando/` beside other engines | [TEITOK integration](TEITOK-Integration.md) |
| `>`, `<`, `child`, … | [Dependency queries](Dependency-Queries.md) |
| `coll`, `dcoll`, `keyness` | [Collocations and keyness](Collocations-and-Keyness.md) |

## Optional dialects

The `pando` CLI may support alternate front-ends via `--cql` (e.g. PML-TQ export, optional CWB dialect), depending on CMake options. See `pando --help` and `dev/CQL-DIALECT-ROADMAP.md` in the repository.
