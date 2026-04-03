# Pando

Pando is a C++ corpus query engine in the same family as Manatee and CWB/CQP: it supports familiar positional and structural-attribute style querying, adds search over dependency trees, and supports aligned corpora and parallel queries. The native query language is **Pando CQL** (see [`PANDO-CQL.md`](PANDO-CQL.md)).

**Documentation:** the full wiki-style docs live under [`wiki/`](../wiki/README.md) in the repository. Preview: `python scripts/serve-wiki-preview.py` → http://localhost:8765/ — TEITOK / flexicorp: [`wiki/TEITOK-Integration.md`](../wiki/TEITOK-Integration.md).

## Installation (build)

From the **repository root**:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(sysctl -n hw.ncpu 2>/dev/null || nproc || echo 4)
```

This builds the main programs:

| Binary | Role |
| --- | --- |
| `pando` | Interactive / batch query CLI |
| `pando-index` | Build a mmap corpus from JSONL (or streaming integration) |
| `pando-check` | Consistency checks on a corpus directory |
| `pando-server` | Optional HTTP/JSON API (see CMake options if disabled) |

To install copies into `/usr/local/bin` (optional):

```bash
sudo cp build/pando build/pando-index build/pando-check build/pando-server /usr/local/bin
```

## Differences from CWB

The syntax is modeled after CWB/CQP (and similar engines), with extensions including:

- **Dependency relations**: e.g. `[] > []`, `[ child [] ]`, and tree-aware `within` / `containing subtree`.
- **Persistent token names**: `a:[]` names a token for use in later clauses or sessions (see CQL doc).
- **Named region**: `a:<s>` names a region for use in later clauses or sessions
- **Multivalue attributes**: pipe-separated values (e.g. `genre="Poem|Song"`) with dedicated index and query semantics.
- **Overlapping regions**: structures of the same type may overlap when declared in the corpus header (e.g. `hi`); nested structures use innermost-region resolution where applicable.
- **Aligned corpora**: shared identifiers (e.g. `tuid` on sentences or tokens) and the `query1 with query2` parallel form.

## Usage

All data for a **Pando corpus** live in a single directory (mmap files + `corpus.info`). Point the `pando` CLI at that directory and pass a CQL query string, or run interactively with no query argument. See [`SAMPLE-CORPUS.md`](SAMPLE-CORPUS.md) for a minimal end-to-end example, and the [wiki Quick Start](../wiki/Quick-Start.md) for a short walkthrough.
