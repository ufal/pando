# Installation

## Requirements

- **OS**: Linux or macOS (development is often on macOS).
- **CMake** ≥ 3.16 (recommended).
- **C++17** toolchain (Clang or GCC).
- **Python 3.9+** is only needed for dev scripts and benchmarks, not for building the C++ binaries.

Dependencies are pulled in via CMake (see root `CMakeLists.txt`); you typically only need CMake and a compiler.

## Build

From the repository root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(sysctl -n hw.ncpu 2>/dev/null || nproc || echo 4)
```

Artifacts appear under `build/`:

| Binary | Purpose |
| --- | --- |
| `pando` | Query CLI |
| `pando-index` | Index JSONL (or streaming pipeline) into a corpus directory |
| `pando-check` | Sanity-check a corpus |
| `pando-server` | HTTP JSON API server |

Optional CMake flags (see `CMakeLists.txt`) may enable or disable dialect modules (e.g. CWB, PML-TQ) or bundled libraries.

## Optional: install on PATH

```bash
sudo cp build/pando build/pando-index build/pando-check build/pando-server /usr/local/bin
```

Or add `build/` to your `PATH` during development.

## Next steps

- [Quick Start](Quick-Start.md) — index a sample and run a query.
- [Index and corpus layout](Index-and-Corpus-Layout.md) — what gets written on disk.
