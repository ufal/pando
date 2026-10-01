# Installation

## Requirements

- **OS**: Linux or macOS (development is often on macOS).
- **CMake** ≥ 3.16 (recommended).
- **C++17** toolchain (Clang or GCC).
- **Python 3.9+** is only needed for dev scripts and benchmarks, not for building the C++ binaries.

Dependencies are pulled in via CMake (see root `CMakeLists.txt`); you typically only need CMake and a compiler.

**Required: RE2** for regular expressions (`brew install re2` on macOS, `apt install libre2-dev`
on Debian / Ubuntu, `dnf install re2-devel` on Fedora / RHEL). Configure fails without it
(`-DPANDO_USE_RE2=ON`, the default since 2026-10-02): on std::regex a regex without a fixed
prefix scans the lexicon about 300× slower (`[form=".*ung"]` on a 38M-token corpus with 3.1M
word forms: 11.4 s instead of 33 ms). `-DPANDO_USE_RE2=AUTO` falls back to std::regex with a
warning, `OFF` uses std::regex only (deliberately, e.g. for a quick local build). With RE2, `.`
is one UTF-8 character (not one byte — `[word="h.t"]` finds *hát*) and `(?i)` works; patterns
RE2 does not support (backreferences, lookaround) still run on std::regex. The configure step
prints `pando regex engine: ON|OFF`; `GET /version` (and `/health`, `/info`) report
`"regex": "re2"` or `"std"`, and a `pando-server` without RE2 prints a warning at startup. A
build directory configured earlier keeps its cached `PANDO_USE_RE2` (`AUTO` or `OFF`):
reconfigure with `-DPANDO_USE_RE2=ON` to make RE2 required there too.

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
compiler, stops when RE2 is missing (`--allow-std-regex` builds with std::regex anyway), and says
when `~/.local/bin` is not on your `PATH`.

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
