#!/usr/bin/env python3
"""
Download Universal Dependencies treebanks, prepend # newregion text and # text_* lines
(filename → text_id, text_langcode, text_treebank; optional JSON overrides), then run pando-index.
Tokens then resolve text_lang (etc.) from the open text region.

Example (everything has a default):
  ./scripts/build_ud_corpus.py
      → downloads the latest UD release to ~/ud-data and indexes it into
        ~/ud-data/pando_idx with the pando-index next to this repository
        (build/pando-index, else the one on PATH).

  ./scripts/build_ud_corpus.py --skip-download --data-dir ~/ud/ud-treebanks-v2.17 \\
    --output-index ./idx

Defaults: --data-dir $PANDO_UD_DATA or ~/ud-data; --output-index <data-dir>/pando_idx;
--pando-index $PANDO_INDEX, <repo>/build/pando-index, or pando-index on PATH.
An archive already in --data-dir is not downloaded again.

By default (no --ud-archive-url): fetches https://universaldependencies.org/download.html,
takes the latest *published* release line ("Version X.Y treebanks are available at …"),
resolves the treebank archive on LINDAT via the REST API (typically ud-treebanks-vX.Y.tgz).
Override with --ud-archive-url for a direct download link.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import tarfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any


UD_DOWNLOAD_PAGE = "https://universaldependencies.org/download.html"
LINDAT_API = "https://lindat.mff.cuni.cz/repository/server/api"

# First "Version X.Y treebanks are available at <a href="hdl…">" on the UD download page
RE_UD_LATEST_RELEASE = re.compile(
    r"Version\s+(\d+\.\d+)\s+treebanks\s+are\s+available\s+at\s+"
    r'<a\s+href="(https?://hdl\.handle\.net/[^"]+)"',
    re.IGNORECASE,
)


def load_manifest(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not str(k).startswith("_")}


def parse_ud_filename(name: str) -> tuple[str, str] | None:
    """
    Parse UD CoNLL-U basename: {lcode}_{tcode}-ud-{split}.conllu
    Returns (lang_code, treebank_code) lowercase, or None.
    """
    if not name.endswith(".conllu"):
        return None
    base = name[: -len(".conllu")]
    if "-ud-" not in base:
        return None
    head, _split = base.rsplit("-ud-", 1)
    if "_" not in head:
        return None
    lang, treebank = head.rsplit("_", 1)
    if not lang or not treebank:
        return None
    return lang.lower(), treebank.lower()


def ud_treebank_folder_for(path: Path, ud_root: Path) -> str | None:
    rel = path.relative_to(ud_root)
    for part in rel.parts:
        if part.startswith("UD_"):
            return part
    return None


def file_header_lines(text_attrs: dict[str, str]) -> list[str]:
    """
    Lines to prepend so pando-index opens a `text` region before # newdoc / sentences.
    Each token then resolves text_lang, text_langcode, text_id, etc. via that region.
    """
    out = ["# newregion text\n"]
    for k, v in sorted(text_attrs.items()):
        if not k.startswith("text_"):
            continue
        out.append(f"# {k} = {v}\n")
    return out


def looks_like_pando_text_header(lines: list[str]) -> bool:
    """True if file already starts with # newregion text (avoid duplicate injection)."""
    for line in lines[:120]:
        s = line.strip()
        if not s:
            continue
        if not s.startswith("#"):
            return False
        if re.match(r"^#\s*newregion\s+text\s*$", s):
            return True
    return False


def sentence_blocks(lines: list[str]) -> list[tuple[int, int]]:
    n = len(lines)
    blocks: list[tuple[int, int]] = []
    i = 0
    while i < n:
        while i < n and lines[i].strip() == "":
            i += 1
        if i >= n:
            break
        start = i
        while i < n and lines[i].strip() != "":
            i += 1
        blocks.append((start, i))
    return blocks


def is_token_line(line: str) -> bool:
    s = line.strip("\r\n")
    if not s or s.lstrip().startswith("#"):
        return False
    return "\t" in s


def inject_into_block(
    block_lines: list[str], text_attrs: dict[str, str], skip_existing: bool
) -> list[str]:
    if not block_lines:
        return block_lines

    comment_idx: list[int] = []
    first_token = None
    for i, line in enumerate(block_lines):
        if line.lstrip().startswith("#"):
            comment_idx.append(i)
        elif is_token_line(line):
            first_token = i
            break
    if first_token is None:
        return block_lines

    existing_keys: set[str] = set()
    for i in comment_idx:
        m = re.match(r"^#\s*(\S+)\s*=", block_lines[i])
        if m:
            existing_keys.add(m.group(1))

    to_add: list[str] = []
    for key, val in sorted(text_attrs.items()):
        if skip_existing and key in existing_keys:
            continue
        if key in existing_keys:
            continue
        to_add.append(f"# {key} = {val}\n")

    if not to_add:
        return block_lines

    insert_at = comment_idx[0] if comment_idx else 0
    for i in comment_idx:
        if "sent_id" in block_lines[i] and "=" in block_lines[i]:
            insert_at = i + 1
            break

    return block_lines[:insert_at] + to_add + block_lines[insert_at:]


def attrs_for_file(
    path: Path,
    ud_root: Path,
    manifest: dict[str, Any],
    *,
    force: bool,
) -> dict[str, str] | None:
    """Build text_* keys for one .conllu file."""
    folder = ud_treebank_folder_for(path, ud_root)
    base = path.name

    overrides = manifest.get("overrides_by_folder") or manifest.get("folder_overrides") or {}
    basename_overrides = manifest.get("overrides_by_basename") or {}

    if base in basename_overrides:
        raw = dict(basename_overrides[base])
    elif folder and folder in overrides:
        raw = dict(overrides[folder])
    else:
        parsed = parse_ud_filename(base)
        if not parsed:
            return None
        lang, tb = parsed
        raw = {
            "text_langcode": lang,
            "text_treebank": tb,
        }

    out: dict[str, str] = {}
    for k, v in raw.items():
        if not k.startswith("text_"):
            raise ValueError(
                f"Manifest keys must start with text_ (got {k!r}) in {path}"
            )
        out[k] = str(v)
    if "text_id" not in out:
        out["text_id"] = base
    return out


def inject_tree(
    ud_root: Path,
    manifest: dict[str, Any],
    *,
    skip_unknown: bool,
    force: bool,
    dry_run: bool,
) -> tuple[int, int, int]:
    files = sorted(ud_root.rglob("*.conllu"))
    if not files:
        raise FileNotFoundError(f"No .conllu under {ud_root}")

    n_files = 0
    n_blocks = 0
    n_file_headers = 0
    for f in files:
        try:
            text_attrs = attrs_for_file(f, ud_root, manifest, force=force)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            raise
        if text_attrs is None:
            if skip_unknown:
                print(f"Skip (unparsed name): {f}", file=sys.stderr)
                continue
            raise ValueError(
                f"Cannot infer metadata for {f.name}; add overrides in manifest "
                "or use --skip-unknown-filename"
            )

        raw = f.read_bytes()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1")
        lines = text.splitlines(keepends=True)

        prepended = False
        if not looks_like_pando_text_header(lines):
            lines = file_header_lines(text_attrs) + lines
            prepended = True
            n_file_headers += 1

        blocks = sentence_blocks(lines)
        new_lines: list[str] = []
        prev = 0
        cb = 0
        for start, end in blocks:
            new_lines.extend(lines[prev:start])
            block = lines[start:end]
            # File-level text_* lives in the header; per-sentence inject only for extra keys.
            inj = inject_into_block(block, {}, skip_existing=not force)
            if inj != block:
                cb += 1
            new_lines.extend(inj)
            prev = end
        new_lines.extend(lines[prev:])

        n_files += 1
        n_blocks += cb
        if not dry_run and (prepended or cb > 0):
            f.write_text("".join(new_lines), encoding="utf-8", newline="")

    return n_files, n_blocks, n_file_headers


def find_ud_subdirs(root: Path) -> list[Path]:
    return sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith("UD_"))


def discover_ud_root(data_dir: Path) -> Path:
    """Find a directory that contains UD_* folders (extracted archive)."""
    direct = find_ud_subdirs(data_dir)
    if direct:
        return data_dir
    for child in sorted(data_dir.iterdir()):
        if not child.is_dir():
            continue
        if find_ud_subdirs(child):
            return child
    raise FileNotFoundError(
        f"No UD_* treebank folders under {data_dir}. "
        "Unpack the UD archive here or pass --ud-root."
    )


def _http_get_json(url: str, timeout: int = 120) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "pando-build_ud_corpus/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _format_size(n: int) -> str:
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024**3):.2f} GiB"
    if n >= 1024 * 1024:
        return f"{n / (1024**2):.2f} MiB"
    if n >= 1024:
        return f"{n / 1024:.1f} KiB"
    return f"{n} B"


def http_download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading:\n  {url}\n  -> {dest}", flush=True)
    req = urllib.request.Request(url, headers={"User-Agent": "pando-build_ud_corpus/1.0"})
    with urllib.request.urlopen(req, timeout=3600) as r:
        cl = r.headers.get("Content-Length")
        total: int | None
        try:
            total = int(cl) if cl else None
        except (TypeError, ValueError):
            total = None
        tmp = dest.with_suffix(dest.suffix + ".part")
        block = 256 * 1024
        downloaded = 0
        last_draw = 0.0
        bar_w = 36

        def draw() -> None:
            nonlocal last_draw
            now = time.monotonic()
            at_end = bool(total and downloaded >= total)
            if now - last_draw < 0.12 and not at_end:
                return
            last_draw = now
            if total and total > 0:
                frac = min(1.0, downloaded / total)
                filled = int(frac * bar_w)
                bar = "#" * filled + "-" * (bar_w - filled)
                pct = 100.0 * frac
                line = (
                    f"\r[{bar}] {pct:5.1f}%  "
                    f"{_format_size(downloaded)} / {_format_size(total)}"
                )
            else:
                line = f"\r  downloaded {_format_size(downloaded)} …"
            sys.stderr.write(line)
            sys.stderr.flush()

        try:
            with open(tmp, "wb") as out:
                while True:
                    chunk = r.read(block)
                    if not chunk:
                        break
                    out.write(chunk)
                    downloaded += len(chunk)
                    draw()
        finally:
            sys.stderr.write("\n")
            sys.stderr.flush()

        tmp.replace(dest)
    print(f"Saved {_format_size(dest.stat().st_size)} ({dest.stat().st_size} bytes).", flush=True)


def fetch_ud_download_page_html(page_url: str | None = None) -> str:
    u = page_url or UD_DOWNLOAD_PAGE
    req = urllib.request.Request(u, headers={"User-Agent": "pando-build_ud_corpus/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_latest_published_ud_release(html: str) -> tuple[str, str]:
    """
    Parse UD download page for the latest *published* release.
    Returns (version_str, handle) e.g. ('2.17', '11234/1-6036').
    """
    m = RE_UD_LATEST_RELEASE.search(html)
    if not m:
        raise ValueError(
            "Could not find 'Version X.Y treebanks are available at <a href=…' "
            "on the UD download page; page format may have changed."
        )
    version = m.group(1)
    href = m.group(2).strip()
    # http://hdl.handle.net/11234/1-6036 → 11234/1-6036
    if "hdl.handle.net/" in href:
        handle = href.split("hdl.handle.net/", 1)[-1].strip().rstrip("/")
    else:
        raise ValueError(f"Unexpected handle URL: {href!r}")
    return version, handle


def normalize_handle(handle: str) -> str:
    h = handle.strip().rstrip("/")
    if "hdl.handle.net/" in h:
        h = h.split("hdl.handle.net/", 1)[-1]
    return h


def resolve_lindat_treebank_archive_url(handle: str) -> tuple[str, str]:
    """
    Use LINDAT DSpace REST to find the direct download URL for the UD treebank
    bundle (ud-treebanks-v*.tgz), not documentation or tools.
    Returns (content_url, filename).
    """
    h = normalize_handle(handle)
    q = urllib.parse.urlencode({"query": f"handle:{h}", "configuration": "default"})
    search_url = f"{LINDAT_API}/discover/search/objects?{q}"
    data = _http_get_json(search_url)
    objects = (
        data.get("_embedded", {})
        .get("searchResult", {})
        .get("_embedded", {})
        .get("objects", [])
    )
    if not objects:
        raise ValueError(f"LINDAT search returned no item for handle {h!r}")
    item = objects[0].get("_embedded", {}).get("indexableObject", {})
    uuid = item.get("uuid") or item.get("id")
    if not uuid:
        raise ValueError("LINDAT search result missing item uuid")

    bundles_url = f"{LINDAT_API}/core/items/{uuid}/bundles"
    bundles_data = _http_get_json(bundles_url)
    bundle_list = bundles_data.get("_embedded", {}).get("bundles", [])
    original = None
    for b in bundle_list:
        if b.get("name") == "ORIGINAL":
            original = b
            break
    if not original:
        raise ValueError("No ORIGINAL bundle on LINDAT item")

    bs_href = original["_links"]["bitstreams"]["href"]
    bs_data = _http_get_json(bs_href)
    bitstreams = bs_data.get("_embedded", {}).get("bitstreams", [])
    candidates: list[tuple[str, str]] = []
    for b in bitstreams:
        name = (b.get("name") or "").lower()
        if name.startswith("ud-treebanks-") and (
            name.endswith(".tgz") or name.endswith(".tar.gz") or name.endswith(".zip")
        ):
            href = b.get("_links", {}).get("content", {}).get("href")
            if href:
                candidates.append((href, b["name"]))

    if not candidates:
        names = [b.get("name") for b in bitstreams]
        raise ValueError(
            "No ud-treebanks-*.tgz / .zip in ORIGINAL bundle. "
            f"Available files: {names}"
        )
    # Prefer .tgz (current UD releases use this)
    for href, fname in candidates:
        if fname.lower().endswith(".tgz"):
            return href, fname
    return candidates[0]


def resolve_default_ud_archive_url(
    download_page_url: str | None = None,
) -> tuple[str, str, str]:
    """Latest release from UD website + LINDAT. Returns (download_url, filename, version)."""
    html = fetch_ud_download_page_html(download_page_url)
    version, handle = parse_latest_published_ud_release(html)
    url, fname = resolve_lindat_treebank_archive_url(handle)
    return url, fname, version


def extract_archive(archive_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    name = archive_path.name.lower()
    print(f"Extracting {archive_path} -> {dest_dir}")
    if name.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(dest_dir)
    elif name.endswith(".tgz") or name.endswith(".tar.gz"):
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(dest_dir)
    else:
        raise ValueError(
            f"Unsupported archive format: {archive_path.name} "
            "(expected .zip, .tgz, or .tar.gz)"
        )


def run_pando_index(pando_index: Path, input_dir: Path, output_index: Path) -> None:
    # (pando-index writes the derived files — bitmaps, dependency pairs, %c/%d
    # folding — itself; `pando-index --upgrade` is only for older indexes)
    output_index.parent.mkdir(parents=True, exist_ok=True)
    cmd = [str(pando_index), str(input_dir), str(output_index)]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def default_pando_index() -> Path | None:
    """$PANDO_INDEX, this repository's build/pando-index, or pando-index on PATH."""
    env = os.environ.get("PANDO_INDEX")
    if env:
        return Path(env).expanduser()
    here = Path(__file__).resolve().parent
    for cand in (here / "pando-index",                 # installed: bin/pando-build-ud
                 here.parent / "build" / "pando-index"):   # in the repository: scripts/
        if cand.is_file():
            return cand
    found = shutil.which("pando-index")
    return Path(found) if found else None


def download_once(url: str, dest: Path) -> None:
    """Download unless the archive is already there (a finished earlier run)."""
    if dest.is_file() and dest.stat().st_size > 0:
        print(f"Using the archive already downloaded: {dest}")
        return
    http_download(url, dest)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-dir",
        default=Path(os.environ.get("PANDO_UD_DATA", "~/ud-data")).expanduser(),
        type=Path,
        help="Root for downloaded archive, extract, and working tree (in-place inject); "
        "default $PANDO_UD_DATA or ~/ud-data",
    )
    p.add_argument(
        "--pando-index",
        default=None,
        type=Path,
        help="pando-index executable (default: $PANDO_INDEX, the one next to this script, "
        "<repo>/build/pando-index, or pando-index on PATH)",
    )
    p.add_argument(
        "--output-index",
        default=None,
        type=Path,
        help="Output directory for pando-index (default: <data-dir>/pando_idx)",
    )
    p.add_argument(
        "--ud-archive-url",
        default=None,
        help="Direct HTTP(S) URL to ud-treebanks archive (.tgz / .zip). "
        "If omitted, use latest release from the UD download page + LINDAT API.",
    )
    p.add_argument(
        "--ud-handle",
        default=None,
        metavar="PREFIX/SUFFIX",
        help="LINDAT handle (e.g. 11234/1-6036) to resolve via API; skips UD download page. "
        "Ignored if --ud-archive-url is set.",
    )
    p.add_argument(
        "--ud-download-page",
        default=UD_DOWNLOAD_PAGE,
        help="Override UD HTML page used to discover the latest release (default: official download page)",
    )
    p.add_argument(
        "--ud-zip-url",
        default=None,
        dest="ud_legacy_archive_url",
        metavar="URL",
        help="Deprecated alias for --ud-archive-url",
    )
    p.add_argument(
        "--ud-root",
        type=Path,
        default=None,
        help="Folder containing UD_* treebanks (default: auto-discover under --data-dir)",
    )
    p.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional JSON: overrides_by_folder, overrides_by_basename, etc.",
    )
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Do not download; use existing tree under --data-dir",
    )
    p.add_argument(
        "--inject-only",
        action="store_true",
        help="Only inject metadata; do not run pando-index",
    )
    p.add_argument(
        "--index-only",
        action="store_true",
        help="Only run pando-index (no download/inject)",
    )
    p.add_argument(
        "--skip-unknown-filename",
        action="store_true",
        help="Skip .conllu files whose basename does not match UD pattern",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-add text_* lines even if key already present",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without writing files or indexing",
    )
    args = p.parse_args()
    data_dir = args.data_dir.expanduser().resolve()
    if args.output_index is None and not args.inject_only:
        args.output_index = data_dir / "pando_idx"
    output_index = args.output_index.expanduser().resolve() if args.output_index else None
    manifest = load_manifest(args.manifest)

    pando_index = args.pando_index.expanduser() if args.pando_index else default_pando_index()
    if not args.inject_only and not args.dry_run:
        if pando_index is None or not pando_index.is_file():
            print("pando-index not found: build pando first (./install.sh, or cmake --build build), "
                  "or pass --pando-index PATH", file=sys.stderr)
            return 1
        pando_index = pando_index.resolve()
    print(f"Data dir:    {data_dir}\nIndex:       {output_index}\npando-index: {pando_index}")

    ud_root: Path | None = args.ud_root.resolve() if args.ud_root else None

    if args.index_only and args.inject_only:
        print("Cannot use both --index-only and --inject-only.", file=sys.stderr)
        return 1

    if not args.index_only:
        if not args.skip_download:
            data_dir.mkdir(parents=True, exist_ok=True)
            explicit_url = args.ud_archive_url or args.ud_legacy_archive_url
            if explicit_url and args.ud_handle:
                p.error("Use only one of --ud-archive-url and --ud-handle")
            if args.dry_run:
                if explicit_url:
                    print(f"[dry-run] would download archive from {explicit_url!r}")
                elif args.ud_handle:
                    print(
                        f"[dry-run] would resolve LINDAT handle {args.ud_handle!r} and download"
                    )
                else:
                    print(
                        "[dry-run] would fetch latest UD release from "
                        f"{args.ud_download_page!r} and download treebanks via LINDAT API"
                    )
                print(f"[dry-run] would extract under {data_dir}")
            else:
                if explicit_url:
                    fname = explicit_url.rstrip("/").split("/")[-1].split("?")[0]
                    if not fname or fname == explicit_url:
                        fname = "ud-treebanks-archive.bin"
                    dest_arc = data_dir / fname
                    download_once(explicit_url, dest_arc)
                    extract_archive(dest_arc, data_dir)
                elif args.ud_handle:
                    dl_url, fname = resolve_lindat_treebank_archive_url(args.ud_handle)
                    print(
                        f"Resolved handle {normalize_handle(args.ud_handle)!r} "
                        f"-> {fname!r}"
                    )
                    dest_arc = data_dir / fname
                    download_once(dl_url, dest_arc)
                    extract_archive(dest_arc, data_dir)
                else:
                    dl_url, fname, version = resolve_default_ud_archive_url(
                        args.ud_download_page
                    )
                    print(
                        f"Latest published UD release on download page: v{version} "
                        f"(treebank archive: {fname!r})"
                    )
                    dest_arc = data_dir / fname
                    download_once(dl_url, dest_arc)
                    extract_archive(dest_arc, data_dir)

        try:
            ud_root = ud_root or discover_ud_root(data_dir)
        except FileNotFoundError:
            if not args.dry_run:
                raise
            print(f"[dry-run] would look for UD_* treebank folders under {data_dir} (after the download)")
            ud_root = data_dir
        print(f"UD root: {ud_root}")

        if args.dry_run:
            print("[dry-run] skipping inject (no file writes)")
        else:
            nf, nb, nh = inject_tree(
                ud_root,
                manifest,
                skip_unknown=args.skip_unknown_filename,
                force=args.force,
                dry_run=False,
            )
            print(
                f"Injected metadata: {nf} file(s), {nh} file header(s) (# newregion text + text_*), "
                f"{nb} sentence block(s) updated."
            )

        if args.inject_only:
            return 0
    else:
        ud_root = ud_root or discover_ud_root(data_dir)
        print(f"UD root: {ud_root}")

    if args.dry_run:
        print(f"[dry-run] would run pando-index {ud_root} {output_index}")
        return 0

    assert output_index is not None
    run_pando_index(pando_index, ud_root, output_index)
    print(f"Done. Try:\n  pando {output_index} '[upos=\"VERB\"]' --total")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
