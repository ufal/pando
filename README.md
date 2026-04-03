Pando
=====

Pando is a C++ corpus query engine in the same family as Manatee and CWB/CQP: it supports familiar positional/s-attribute style querying, adds the ability to search over dependency trees, and has extensive support for aligned corpora. The repository currently focuses on:

- **Core engine**: query parsing, execution, and indexing (`src/query`, `src/index`, `src/corpus`).
- **CLI tools**: binaries for querying, indexing, checking, and serving HTTP APIs (`src/cli`, `src/api`).
- **Dev tooling**: internal benchmark harnesses and design notes under `dev/` (not installed or shipped).

This README is intentionally minimal and documents how to build and run the current prototype for private/use-at-your-own-risk purposes.

**Documentation:** see [docs/PANDO.md](docs/PANDO.md) for a short overview, [docs/PANDO-CQL.md](docs/PANDO-CQL.md) for the query language, and the **[wiki/](wiki/README.md)** for topic pages (same layout as flexipipe/flexiconv). Preview the wiki locally: `python scripts/serve-wiki-preview.py` → http://localhost:8765/

## Status

- **Scope**: Internal / experimental. APIs, on-disk formats, and CLI flags may change without notice.
- **Maturity**: The engine and CLI are usable enough to run real benchmarks and experiments, but not yet ready for public release or long-term format stability guarantees.
- **Companion repos**: Pando is designed to work with:
  - `flexipipe` – corpus preparation pipelines.
  - `flexiconv` – query conversion and related tooling.
  - `flexicorp` – auxiliary tools and integration with other backends.

## Prerequisites

- **OS**: Linux or macOS (development currently on macOS).
- **Toolchain**:
  - CMake (>= 3.16 recommended).
  - A C++17 (or newer) compiler (e.g. Clang, GCC).
  - Python 3.9+ for dev scripts and benchmarks.

All C++ library dependencies are pulled in via CMake (e.g. `httplib` vendored through `add_subdirectory`/FetchContent in `CMakeLists.txt`), so you typically do not need system-wide packages beyond a standard build toolchain.

## Quick start: build

From the repository root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(sysctl -n hw.ncpu 2>/dev/null || nproc || echo 4)
```

This should produce several binaries in `build/`, including:

- `pando` – main query CLI.
- `pando-index` – corpus indexing tool.
- `pando-check` – consistency / sanity checker.
- `pando-server` – HTTP/JSON API server (if enabled in CMake).

Exact binary names and options may evolve; run each with `--help` for the current CLI.

## Running a simple query

To index and query the bundled UD sample (`test/data/sample.conllu`) on your machine, see [`docs/SAMPLE-CORPUS.md`](docs/SAMPLE-CORPUS.md).

Assuming you already have a Pando-formatted corpus at `/path/to/corpus` and have built the project:

```bash
./build/pando /path/to/corpus '[word="example"]' --count-only --timing
```

This should print total match count and timing information to stdout/stderr. The specifics of the corpus layout and configuration are still in flux; for now, see the design notes under `dev/`:

- `dev/PANDO-INDEX-INTEGRATION.md`
- `dev/PANDO-SEGMENTS.md`
- `dev/QUERY-COMPAT.md`

## Benchmark harness

The script `dev/run_benchmark.py` runs a battery of queries across multiple engines (Pando, Manatee, CWB/CQP, etc.) and summarizes timing results.

Basic usage (TEITOK-style layout with `pando/`, `manatee/`, `cqp/` under one root):

```bash
python3 dev/run_benchmark.py \
  --corpus-root /path/to/teitok/root \
  --pando-bin ./build/pando \
  [--manatee-script /path/to/manatee_query_cli.py] \
  [--corpus-name tt_ud217]
```

Or you can explicitly configure each engine and the query file; see the docstring at the top of `dev/run_benchmark.py` for full details.

## Development notes

Useful design and integration notes live in `dev/`:

- `dev/ROADMAP.md` – rough feature roadmap and priorities (full history + design narrative).
- `dev/ROADMAP-TODO.md` – **consolidated open backlog** and current priority order (subsidiary docs may lag).
- `dev/CODE-REVIEW.md` – internal code review and refactoring notes.
- `dev/PANDO-INDEX-INTEGRATION.md` – how Pando’s index layout integrates with other tools.
- `dev/PANDO-SEGMENTS.md` – segment-level structure and design.
- `dev/QUERY-COMPAT.md` – query language compatibility notes vs. other engines.
- `dev/CQL-DIALECT-ROADMAP.md` – implementation roadmap for optional CWB / Manatee / PML-TQ dialect front-ends.
- `dev/CQL-DIALECT-LICENSING.md` – GPL / packaging / `PANDO_CWB_DIALECT` for the CWB dialect module.
- `dev/CLICKCQL-TODO.md` – internal notes on possible ClickHouse / CQL integration.

These documents reflect the current state of thinking and may lag behind the code slightly.

## Python/dev dependencies

The core engine is pure C++ and does not require Python to build or run. Python is used for:

- Benchmarking (`dev/run_benchmark.py`).
- Integration scripts and experiments (primarily within `dev/`).

At present, these scripts only rely on the Python standard library. If that changes, `requirements.txt` will be updated accordingly.

## License

This repository is currently private and experimental. Choose and add a proper license (e.g. MIT, Apache 2.0) before making the project public or sharing it more broadly.

