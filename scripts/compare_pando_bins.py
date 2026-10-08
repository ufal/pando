#!/usr/bin/env python3
"""
Compare two `pando` binaries on the same corpus and query list (stdout; stderr optional).

Typical use (from repo root):

  ./scripts/compare_pando_bins.py \\
    --corpus /tmp/pando-sample-rich \\
    --bin-a ./build/pando \\
    --bin-b /usr/local/bin/pando

Stand-off overlay directories (user-generated SO annotations merged at query time):

  ./scripts/compare_pando_bins.py --corpus /tmp/pando-sample-rich \\
    --overlay /tmp/pando-sample-rich-overlays/mwe \\
    --bin-a ./build/pando --bin-b /usr/local/bin/pando

  # The overlay **index directory** is often a layer subfolder (here `mwe`), not the parent.
  # With `layer_id` defaulting to the last path segment, token-group `mwe` becomes
  # `<overlay-mwe-mwe>` (see dev/USER-OVERLAY-ANNOTATIONS.md).

Repeat `--overlay` for multiple layers. See `dev/USER-OVERLAY-ANNOTATIONS.md`.

Optional extra probe (only if you have a suitable CQL for merged attrs):

  export PANDO_OVERLAY_CQL='[overlay-mylayer-myattr="x"]'
  ./scripts/compare_pando_bins.py ...

Exit code: 0 if all pairs match, 1 if any differ or a binary/corpus is missing.

**Overlays:** each `--overlay` path must exist and look like a Pando overlay/mini-index
(`overlay.info` and/or `corpus.info`). Placeholder paths such as `/path/to/your/overlay_index`
cause **both** binaries to fail the same way; the script aborts up front instead of reporting
a false “all match”.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any


def validate_overlay_dir(path: str) -> None:
    """Require a real directory with at least one manifest file."""
    p = os.path.realpath(os.path.expanduser(path))
    if not os.path.exists(p):
        raise ValueError(f"overlay directory does not exist: {path}")
    if not os.path.isdir(p):
        raise ValueError(f"overlay path is not a directory: {path}")
    has_overlay_info = os.path.isfile(os.path.join(p, "overlay.info"))
    has_corpus_info = os.path.isfile(os.path.join(p, "corpus.info"))
    if not has_overlay_info and not has_corpus_info:
        hint = ""
        try:
            for name in sorted(os.listdir(p)):
                sub = os.path.join(p, name)
                if not os.path.isdir(sub):
                    continue
                if os.path.isfile(os.path.join(sub, "overlay.info")) or os.path.isfile(
                    os.path.join(sub, "corpus.info")
                ):
                    hint = (
                        f"\n  Hint: the index may be one level down — try e.g.\n"
                        f"    --overlay {sub}"
                    )
                    break
        except OSError:
            pass
        raise ValueError(
            f"overlay directory has neither overlay.info nor corpus.info: {path}\n"
            "  Build an overlay with: pando-index --format jsonl --overlay-index --index-dir MAIN ...\n"
            "  See dev/USER-OVERLAY-ANNOTATIONS.md"
            + hint
        )


def smoke_open_with_overlays(
    bin_path: str, corpus: str, overlays: list[str],
) -> tuple[bool, str]:
    """Return (ok, stderr_snippet) from a minimal query; catches bad overlay merge."""
    cmd = [bin_path]
    for d in overlays:
        cmd += ["--overlay", d]
    cmd += ["--count-only", corpus, '[upos="NOUN"]']
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode == 0:
        return True, ""
    return False, (p.stderr or p.stdout or "").strip()[:500]


# (case_id, extra_pando_flags_before_corpus, query_string)
# extra flags are inserted after any --overlay args and before corpus_dir.
DEFAULT_SUITE: list[tuple[str, list[str], str]] = [
    ("version", ["--version"], ""),
    ("count_noun", ["--count-only"], '[upos="NOUN"]'),
    ("count_regex", ["--count-only"], "[form=/./]"),
    ("json_det", ["--json", "--limit", "2"], '[upos="DET"]'),
    (
        "tabulate_err_group",
        [],
        "n:<err>; tabulate spellout(n,lemma), n.type, tcnt(n)",
    ),
]


def normalize_json_text(s: str) -> str:
    """Parse JSON and re-dump with sorted keys for stable comparison."""
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return s

    def norm(x: Any) -> Any:
        if isinstance(x, dict):
            return {k: norm(x[k]) for k in sorted(x.keys())}
        if isinstance(x, list):
            return [norm(i) for i in x]
        return x

    return json.dumps(norm(obj), ensure_ascii=False, sort_keys=True, indent=2)


def run_one(
    bin_path: str,
    corpus: str,
    query: str,
    extra: list[str],
    overlays: list[str],
) -> tuple[int, str, str]:
    if extra == ["--version"]:
        cmd = [bin_path, "--version"]
    else:
        cmd = [bin_path]
        for d in overlays:
            cmd += ["--overlay", d]
        cmd += extra + [corpus, query]
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def compare_outputs(
    case_id: str,
    a_out: str,
    b_out: str,
    *,
    is_json: bool,
) -> bool:
    if is_json:
        try:
            a = normalize_json_text(a_out)
            b = normalize_json_text(b_out)
        except Exception:
            a, b = a_out, b_out
    else:
        a, b = a_out, b_out
    if a == b:
        return True
    print(f"\n=== MISMATCH: {case_id} ===", file=sys.stderr)
    import difflib

    diff = difflib.unified_diff(
        b.splitlines(keepends=True),
        a.splitlines(keepends=True),
        fromfile="bin_b",
        tofile="bin_a",
    )
    sys.stderr.writelines(diff)
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare two pando binaries on a query suite.")
    ap.add_argument(
        "--corpus",
        default=os.environ.get("PANDO_COMPARE_CORPUS", "/tmp/pando-sample-rich"),
        help="Indexed corpus directory (default: /tmp/pando-sample-rich)",
    )
    ap.add_argument(
        "--bin-a",
        default=os.environ.get("PANDO_COMPARE_BIN_A", "./build/pando"),
        help="First binary (default: ./build/pando)",
    )
    ap.add_argument(
        "--bin-b",
        default=os.environ.get("PANDO_COMPARE_BIN_B", "/usr/local/bin/pando"),
        help="Second binary (default: /usr/local/bin/pando)",
    )
    ap.add_argument(
        "--overlay",
        action="append",
        default=[],
        help="Merge stand-off overlay index (repeatable). Same as `pando --overlay DIR`.",
    )
    ap.add_argument(
        "--skip-tabulate-err",
        action="store_true",
        help="Omit the n:<err> tabulate case (if corpus has no err group).",
    )
    ap.add_argument(
        "--fail-on-stderr-diff",
        action="store_true",
        help="Also require stderr to match (default: compare stdout only).",
    )
    args = ap.parse_args()

    corpus = args.corpus
    bin_a = os.path.expanduser(args.bin_a)
    bin_b = os.path.expanduser(args.bin_b)
    overlays = list(args.overlay)

    if not os.path.exists(bin_a) or not os.access(bin_a, os.X_OK):
        print(f"Error: bin-a not found or not executable: {bin_a}", file=sys.stderr)
        return 1
    if not os.path.exists(bin_b) or not os.access(bin_b, os.X_OK):
        print(f"Error: bin-b not found or not executable: {bin_b}", file=sys.stderr)
        return 1
    if not os.path.isdir(corpus) or not os.path.isfile(os.path.join(corpus, "corpus.info")):
        print(
            f"Error: corpus not found or missing corpus.info: {corpus}\n"
            "Run this on a machine where the index exists, or set --corpus.",
            file=sys.stderr,
        )
        return 1

    for od in overlays:
        try:
            validate_overlay_dir(od)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    if overlays:
        ok, err_snip = smoke_open_with_overlays(bin_a, corpus, overlays)
        if not ok:
            print(
                "Error: corpus failed to open with the given --overlay path(s) "
                f"(smoke test with {bin_a}).\n{err_snip}",
                file=sys.stderr,
            )
            return 1

    suite = list(DEFAULT_SUITE)
    if args.skip_tabulate_err:
        suite = [s for s in suite if s[0] != "tabulate_err_group"]

    extra_overlay = os.environ.get("PANDO_OVERLAY_CQL", "").strip()
    if extra_overlay:
        suite.append(("overlay_env_cql", ["--count-only"], extra_overlay))

    # Sample-rich overlay: layer dir basename `mwe` → merged token-group `<overlay-mwe-mwe>`.
    if overlays:
        for od in overlays:
            if os.path.basename(os.path.realpath(os.path.expanduser(od))) == "mwe":
                suite.insert(
                    4,
                    ("overlay_mwe_anchor", ["--count-only"], "<overlay-mwe-mwe>"),
                )
                break

    all_ok = True
    print(f"Corpus: {corpus}", file=sys.stderr)
    print(f"bin-a:  {bin_a}", file=sys.stderr)
    print(f"bin-b:  {bin_b}", file=sys.stderr)
    if overlays:
        print(f"Overlays: {overlays}", file=sys.stderr)
    print(file=sys.stderr)

    for case_id, extra, query in suite:
        if case_id == "version":
            rc_a, out_a, err_a = run_one(bin_a, corpus, query, extra, overlays)
            rc_b, out_b, err_b = run_one(bin_b, corpus, query, extra, overlays)
            if rc_a != rc_b:
                print(f"{case_id}: return code differs (a={rc_a}, b={rc_b})", file=sys.stderr)
                all_ok = False
                continue
            ok = compare_outputs(case_id, out_a, out_b, is_json=False)
            if not ok:
                all_ok = False
            if args.fail_on_stderr_diff and err_a != err_b:
                print(f"stderr mismatch: {case_id}", file=sys.stderr)
                all_ok = False
            print(f"{case_id}: {'OK' if ok else 'DIFF'}", file=sys.stderr)
            continue

        is_json = "--json" in extra
        rc_a, out_a, err_a = run_one(bin_a, corpus, query, extra, overlays)
        rc_b, out_b, err_b = run_one(bin_b, corpus, query, extra, overlays)

        if rc_a != rc_b:
            print(
                f"{case_id}: return code differs (a={rc_a}, b={rc_b})",
                file=sys.stderr,
            )
            print("--- stderr a ---", file=sys.stderr)
            print(err_a, file=sys.stderr, end="")
            print("--- stderr b ---", file=sys.stderr)
            print(err_b, file=sys.stderr, end="")
            all_ok = False
            continue

        if rc_a != 0:
            # Same failure on both — compare message (identical error = match).
            # After overlay smoke-test, this should be rare (e.g. unsupported query).
            if out_a != out_b or err_a != err_b:
                compare_outputs(case_id + "_stdout", out_a, out_b, is_json=False)
                all_ok = False
                print(f"{case_id}: both failed but output differed", file=sys.stderr)
            else:
                print(
                    f"{case_id}: both failed identically (rc={rc_a}) — OK "
                    "(same error text; fix corpus/query if unexpected)",
                    file=sys.stderr,
                )
            continue

        ok = compare_outputs(case_id, out_a, out_b, is_json=is_json)
        if not ok:
            all_ok = False
        if args.fail_on_stderr_diff and err_a != err_b:
            print(f"{case_id}: stderr differs", file=sys.stderr)
            all_ok = False
        print(f"{case_id}: {'OK' if ok else 'DIFF'}", file=sys.stderr)

    if all_ok:
        print("All compared cases match.", file=sys.stderr)
        return 0
    print("One or more mismatches.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
