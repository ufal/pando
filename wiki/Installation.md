# Installation

## Requirements

- **OS**: Linux or macOS (development is often on macOS).
- **CMake** ≥ 3.16 (recommended).
- **C++17** toolchain (Clang or GCC).
- **Python 3.9+** is only needed for dev scripts and benchmarks, not for building the C++ binaries.

Dependencies are pulled in via CMake (see root `CMakeLists.txt`); you typically only need CMake and a compiler.

**Recommended: RE2** for regular expressions (`brew install re2` on macOS, `apt install libre2-dev`
on Debian / Ubuntu). CMake uses it when it finds it (`-DPANDO_USE_RE2=AUTO`, the default; `ON`
requires it, `OFF` uses std::regex only). With RE2, `.` is one UTF-8 character (not one byte —
`[word="h.t"]` finds *hát*), `(?i)` works, and lexicon scans are 3-4× faster; patterns RE2 does
not support (backreferences, lookaround) still run on std::regex. The configure step prints
`pando regex engine: ON|OFF`. A build directory configured before 2026-09-28 keeps its old
`PANDO_USE_RE2=OFF`: reconfigure with `-DPANDO_USE_RE2=AUTO` to switch.

**macOS deployment target:** binaries run on macOS 14 and newer by default
(`CMAKE_OSX_DEPLOYMENT_TARGET`, set in `CMakeLists.txt`); without it the compiler targets the
build machine's own macOS version, which older Macs cannot run. Change it with
`-DCMAKE_OSX_DEPLOYMENT_TARGET=15.0` (a new build directory, or delete `CMakeCache.txt`).

## Build and install

From the repository root:

```bash
./install.sh            # Release build, installs pando, pando-index, pando-check,
                        # pando-server, pando-build-ud into ~/.local/bin
./install.sh --ud       # … and downloads + indexes the latest UD release (~/ud-data/pando_idx)
```

Options: `--prefix DIR` (e.g. `/usr/local`, may need sudo), `--no-install` (build only),
`--test` (run ctest), `--ud-dir DIR`, `--build-dir DIR`. The script checks for CMake and a
compiler, uses RE2 when it is installed, and says when `~/.local/bin` is not on your `PATH`.

The same by hand:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(sysctl -n hw.ncpu 2>/dev/null || nproc || echo 4)
cmake --install build --prefix ~/.local     # optional: bin/pando, bin/pando-index, …
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
