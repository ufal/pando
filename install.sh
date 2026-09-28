#!/usr/bin/env bash
# Build pando and install its commands (pando, pando-index, pando-check,
# pando-server, pando-build-ud). Run from the repository root.
#
#   ./install.sh                      build (Release) and install into ~/.local/bin
#   ./install.sh --prefix /usr/local  install elsewhere (may need sudo)
#   ./install.sh --no-install         build only: ./build/pando, ./build/pando-index, …
#   ./install.sh --ud                 also download the latest Universal Dependencies
#                                     release and index it (~/ud-data/pando_idx)
#   ./install.sh --ud-dir DIR         … into DIR instead (implies --ud)
#   ./install.sh --test               run the test suite after building
#
# Needs CMake >= 3.16 and a C++17 compiler; RE2 is used when installed
# (recommended: brew install re2 / apt install libre2-dev), else std::regex.
set -euo pipefail

cd "$(dirname "$0")"

PREFIX="${HOME}/.local"
INSTALL=1
UD=0
UD_DIR=""
TEST=0
BUILD_DIR=build
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --no-install) INSTALL=0; shift ;;
    --ud) UD=1; shift ;;
    --ud-dir) UD=1; UD_DIR="$2"; shift 2 ;;
    --test) TEST=1; shift ;;
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1 (see ./install.sh --help)" >&2; exit 2 ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "error: $1 not found — $2" >&2; exit 1; }; }
need cmake "install CMake (brew install cmake / apt install cmake)"
if ! command -v c++ >/dev/null 2>&1 && ! command -v clang++ >/dev/null 2>&1 && ! command -v g++ >/dev/null 2>&1; then
  echo "error: no C++ compiler — install Xcode command line tools (xcode-select --install) or build-essential" >&2
  exit 1
fi

jobs() { sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4; }

CMAKE_ARGS=(-DCMAKE_BUILD_TYPE=Release -DPANDO_USE_RE2=AUTO)
if command -v brew >/dev/null 2>&1; then
  CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=$(brew --prefix)")
  brew list re2 >/dev/null 2>&1 || echo "note: RE2 not installed — regexes will use std::regex (slower, byte-wise '.'); brew install re2"
elif command -v pkg-config >/dev/null 2>&1 && ! pkg-config --exists re2; then
  echo "note: RE2 not found — regexes will use std::regex (slower, byte-wise '.'); apt install libre2-dev"
fi

echo "==> configure (${BUILD_DIR})"
mkdir -p "$BUILD_DIR"
if ! cmake -S . -B "$BUILD_DIR" "${CMAKE_ARGS[@]}" > "$BUILD_DIR/configure.log" 2>&1; then
  tail -30 "$BUILD_DIR/configure.log" >&2
  echo "error: configure failed (full log: $BUILD_DIR/configure.log)" >&2
  exit 1
fi
grep -E 'pando regex engine' "$BUILD_DIR/configure.log" || true
echo "==> build"
cmake --build "$BUILD_DIR" -j"$(jobs)"

if [[ $TEST -eq 1 ]]; then
  echo "==> test"
  (cd "$BUILD_DIR" && ctest -j"$(jobs)" --output-on-failure) || echo "warning: some tests failed"
fi

BIN="$PWD/$BUILD_DIR"
if [[ $INSTALL -eq 1 ]]; then
  echo "==> install into ${PREFIX}"
  cmake --install "$BUILD_DIR" --prefix "$PREFIX" >/dev/null
  BIN="$PREFIX/bin"
  case ":$PATH:" in
    *":$BIN:"*) ;;
    *) echo "note: $BIN is not on your PATH — add it, e.g.: echo 'export PATH=\"$BIN:\$PATH\"' >> ~/.zshrc" ;;
  esac
fi

if [[ $UD -eq 1 ]]; then
  echo "==> Universal Dependencies corpus"
  UD_ARGS=(--pando-index "$BIN/pando-index")
  [[ -n "$UD_DIR" ]] && UD_ARGS+=(--data-dir "$UD_DIR")
  python3 scripts/build_ud_corpus.py "${UD_ARGS[@]}"
fi

echo
echo "pando is ready: $BIN/pando"
if [[ $UD -eq 1 ]]; then
  echo "  $BIN/pando ${UD_DIR:-~/ud-data}/pando_idx '[upos=\"VERB\"]' --total"
else
  echo "  sample corpus: $BIN/pando-index test/data/sample.conllu /tmp/sample_idx"
  echo "                 $BIN/pando /tmp/sample_idx '[upos=\"VERB\"]' --total"
  echo "  all of UD:     ./install.sh --ud   (or: $BIN/pando-build-ud)"
fi
