"""CLI backend: talk to the ``pando`` binary (no C++ / libpando required)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Any, Iterator, Optional, TextIO

from pando_cql.parse import PandoCqlError, normalize_run_payload


def find_pando_binary(explicit: Optional[str] = None, *, check_runs: bool = True) -> str:
    """
    Resolve path to the ``pando`` executable.

    Order: ``pando_bin=`` argument, then ``PANDO_BIN``, then ``PATH``.
    An explicit argument or env value is used alone (no silent fallback).
    Raises ``PandoCqlError`` with a clear message if missing or not runnable.
    """
    if explicit:
        path = os.path.abspath(os.path.expanduser(explicit))
        _validate_pando_binary(path, source="pando_bin= argument", check_runs=check_runs)
        return path
    env = os.environ.get("PANDO_BIN")
    if env:
        path = os.path.abspath(os.path.expanduser(env))
        _validate_pando_binary(
            path, source="PANDO_BIN environment variable", check_runs=check_runs
        )
        return path
    which = shutil.which("pando")
    if which:
        path = os.path.abspath(which)
        _validate_pando_binary(path, source="PATH", check_runs=check_runs)
        return path
    raise PandoCqlError(
        "pando binary not found.\n"
        "  Tried: pando_bin= argument, PANDO_BIN, and PATH.\n"
        "  Fix: build pando, then either add it to PATH or set e.g.\n"
        "       export PANDO_BIN=/path/to/build/pando\n"
        "       Corpus.open(..., pando_bin='/path/to/build/pando')"
    )


def _validate_pando_binary(path: str, *, source: str, check_runs: bool) -> None:
    if not os.path.exists(path):
        raise PandoCqlError(f"pando binary does not exist ({source}): {path}")
    if os.path.isdir(path):
        raise PandoCqlError(f"pando binary path is a directory ({source}): {path}")
    if not os.path.isfile(path):
        raise PandoCqlError(f"pando binary is not a regular file ({source}): {path}")
    if not os.access(path, os.X_OK):
        raise PandoCqlError(
            f"pando binary is not executable ({source}): {path}\n"
            f"  Fix: chmod +x {path}"
        )
    if not check_runs:
        return
    try:
        proc = subprocess.run(
            [path, "--version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except OSError as e:
        raise PandoCqlError(
            f"could not execute pando binary ({source}): {path}\n  {e}"
        ) from e
    except subprocess.TimeoutExpired as e:
        raise PandoCqlError(
            f"pando --version timed out ({source}): {path}"
        ) from e
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise PandoCqlError(
            f"pando --version failed ({source}): {path}"
            + (f"\n  {err}" if err else f"\n  exit code {proc.returncode}")
        )


def validate_corpus_dir(corpus_dir: str) -> str:
    """
    Ensure ``corpus_dir`` looks like a pando index. Returns the absolute path.

    Raises ``PandoCqlError`` if missing or not a corpus.
    """
    if not corpus_dir or not str(corpus_dir).strip():
        raise PandoCqlError(
            "corpus directory not specified.\n"
            "  Pass a path to Corpus.open(...), or set PANDO_CORPUS for the example script."
        )
    path = os.path.abspath(os.path.expanduser(corpus_dir))
    if not os.path.exists(path):
        raise PandoCqlError(f"corpus path does not exist: {path}")
    if not os.path.isdir(path):
        raise PandoCqlError(
            f"corpus path is not a directory: {path}\n"
            "  Expected a pando index folder (contains corpus.info), not a .conllu file."
        )
    info = os.path.join(path, "corpus.info")
    if not os.path.isfile(info):
        # Helpful hint for TEITOK-style ./pando subdir
        nested = os.path.join(path, "pando", "corpus.info")
        hint = ""
        if os.path.isfile(nested):
            hint = (
                f"\n  Hint: found an index at {os.path.join(path, 'pando')} "
                "— pass that path instead."
            )
        raise PandoCqlError(
            f"not a pando corpus index (missing corpus.info): {path}\n"
            "  Build an index with pando-index, or point at the directory that "
            "contains corpus.info."
            + hint
        )
    if os.path.getsize(info) == 0:
        raise PandoCqlError(
            f"corpus.info is empty (corrupt or incomplete index?): {info}"
        )
    return path


class CliSession:
    """
    Persistent ``pando --api --json <corpus>`` process; one CQL program per line.
    Named queries persist across ``run()`` calls (same as the REPL).
    """

    def __init__(
        self,
        corpus_dir: str,
        *,
        pando_bin: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
        check_pando: bool = True,
    ) -> None:
        self.corpus_dir = validate_corpus_dir(corpus_dir)
        self.pando_bin = find_pando_binary(pando_bin, check_runs=check_pando)
        self.extra_args = list(extra_args or [])
        self._proc: Optional[subprocess.Popen[str]] = None
        self._pending = ""

    def start(self) -> None:
        if self._proc is not None:
            return
        cmd = [self.pando_bin, "--api", "--json", *self.extra_args, self.corpus_dir]
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except OSError as e:
            raise PandoCqlError(
                f"failed to start pando ({self.pando_bin}) on corpus "
                f"{self.corpus_dir}:\n  {e}"
            ) from e
        if self._proc.stdin is None or self._proc.stdout is None:
            raise PandoCqlError("failed to open pipes to pando")
        self._pending = ""

    def close(self) -> None:
        if self._proc is None:
            return
        try:
            if self._proc.stdin and not self._proc.stdin.closed:
                self._proc.stdin.write("quit\n")
                self._proc.stdin.flush()
        except BrokenPipeError:
            pass
        try:
            self._proc.terminate()
            self._proc.wait(timeout=5)
        except Exception:
            try:
                self._proc.kill()
            except Exception:
                pass
        self._proc = None
        self._pending = ""

    def __enter__(self) -> "CliSession":
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def run_raw(self, cql: str) -> dict[str, Any]:
        """Send one CQL program; return the next JSON object from stdout."""
        self.start()
        assert self._proc is not None and self._proc.stdin is not None
        assert self._proc.stdout is not None
        line = cql.strip()
        if not line:
            raise PandoCqlError("empty CQL")
        if "\n" in line:
            line = " ".join(p.strip() for p in line.splitlines() if p.strip())
        try:
            self._proc.stdin.write(line + "\n")
            self._proc.stdin.flush()
        except BrokenPipeError as e:
            err = self._proc.stderr.read() if self._proc.stderr else ""
            raise PandoCqlError(f"pando pipe broken: {err}") from e
        return self._read_json_object(self._proc.stdout)

    def run(self, cql: str) -> Any:
        payload = self.run_raw(cql)
        return normalize_run_payload(payload)

    def _read_json_object(self, stdout: TextIO) -> dict[str, Any]:
        """The next top-level JSON object on stdout. pando ends one at the start of a
        line (`}`, or a one-line `{…}`): parsing is only tried there, so a large object
        is parsed about once, not after every line."""
        dec = json.JSONDecoder()
        buf = self._pending
        self._pending = ""
        assert self._proc is not None
        try_now = bool(buf.strip())
        while True:
            if try_now:
                start = len(buf) - len(buf.lstrip())
                try:
                    obj, end = dec.raw_decode(buf, start)
                except json.JSONDecodeError:
                    pass
                else:
                    self._pending = buf[end:]
                    if not isinstance(obj, dict):
                        raise PandoCqlError("pando JSON root must be an object")
                    return obj
            chunk = stdout.readline()
            if chunk == "":
                if self._proc.poll() is not None:
                    err = self._proc.stderr.read() if self._proc.stderr else ""
                    raise PandoCqlError(
                        f"pando exited before completing JSON "
                        f"(code {self._proc.returncode}): {err}"
                    )
                continue
            buf += chunk
            line = chunk.rstrip()
            try_now = line.startswith("}") or (line.startswith("{") and line.endswith("}"))


def run_oneshot(
    corpus_dir: str,
    cql: str,
    *,
    pando_bin: Optional[str] = None,
    extra_args: Optional[list[str]] = None,
    raw: bool = False,
) -> Any:
    """Run a full CQL program in one process (no session across calls).

    ``raw``: return the last JSON object as pando sent it (else normalised)."""
    corpus_dir = validate_corpus_dir(corpus_dir)
    bin_path = find_pando_binary(pando_bin)
    cmd = [bin_path, "--api", "--json", *(extra_args or []), corpus_dir, cql]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0 and not proc.stdout.strip():
        raise PandoCqlError(proc.stderr.strip() or f"pando exited {proc.returncode}")
    payloads = list(_iter_json_objects(proc.stdout))
    if not payloads:
        raise PandoCqlError(proc.stderr.strip() or "pando produced no JSON")
    return payloads[-1] if raw else normalize_run_payload(payloads[-1])


def _iter_json_objects(text: str) -> Iterator[dict[str, Any]]:
    dec = json.JSONDecoder()
    i = 0
    n = len(text)
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        obj, end = dec.raw_decode(text, i)
        if not isinstance(obj, dict):
            raise PandoCqlError("pando JSON root must be an object")
        yield obj
        i = end
