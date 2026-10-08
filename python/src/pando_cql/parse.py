"""Normalize pando --json / --api count output into flat row dicts for pandas."""

from __future__ import annotations

from typing import Any, Mapping, MutableMapping, Sequence


class PandoCqlError(Exception):
    """Raised when pando JSON cannot be interpreted as expected."""


def _as_mapping(obj: Any) -> Mapping[str, Any]:
    if not isinstance(obj, Mapping):
        raise PandoCqlError(f"expected JSON object, got {type(obj).__name__}")
    return obj


def extract_result(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the ``result`` object from a pando JSON response."""
    if payload.get("ok") is False:
        err = payload.get("error", "pando returned ok=false")
        raise PandoCqlError(str(err))
    result = payload.get("result")
    if result is None:
        raise PandoCqlError("pando JSON missing 'result'")
    return _as_mapping(result)


def flatten_count_result(
    result: Mapping[str, Any],
    *,
    freq_key: str = "freq",
    include_pct: bool = False,
) -> list[dict[str, Any]]:
    """
    Flatten a count/group ``result`` into one dict per leaf group.

    Handles:
      - one field: ``rows: [{key, count, pct}, ...]`` + ``fields: [name]``
      - multi-field: ``hierarchy`` tree with ``field`` / ``value`` / ``children`` / ``count``

    Intermediate hierarchy nodes are rollups for CLI; only **leaves** become rows.
    The metric is exposed as ``freq_key`` (default ``freq``); JSON ``count`` is unchanged upstream.
    """
    fields = result.get("fields")
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
        raise PandoCqlError("count result missing fields array")
    field_names = [str(f) for f in fields]
    if not field_names:
        raise PandoCqlError("count result has empty fields")

    if "hierarchy" in result:
        hierarchy = result.get("hierarchy")
        if hierarchy is None:
            return []
        if not isinstance(hierarchy, list):
            raise PandoCqlError("hierarchy must be a list")
        rows: list[dict[str, Any]] = []
        _walk_hierarchy(
            hierarchy, field_names, {}, rows, freq_key=freq_key, include_pct=include_pct
        )
        return rows

    raw_rows = result.get("rows")
    if raw_rows is None:
        return []
    if not isinstance(raw_rows, list):
        raise PandoCqlError("rows must be a list")
    if len(field_names) != 1:
        raise PandoCqlError("flat rows expected with exactly one field name")
    col = field_names[0]
    out: list[dict[str, Any]] = []
    for item in raw_rows:
        m = _as_mapping(item)
        row: dict[str, Any] = {col: m.get("key"), freq_key: m.get("count")}
        if include_pct and "pct" in m:
            row["pct"] = m["pct"]
        out.append(row)
    return out


def _walk_hierarchy(
    nodes: Sequence[Any],
    field_names: Sequence[str],
    path: MutableMapping[str, Any],
    out: list[dict[str, Any]],
    *,
    freq_key: str,
    include_pct: bool,
) -> None:
    for node in nodes:
        m = _as_mapping(node)
        field = m.get("field")
        value = m.get("value")
        if field is None:
            raise PandoCqlError("hierarchy node missing field")
        fname = str(field)
        path[fname] = value
        children = m.get("children")
        if children:
            if not isinstance(children, list):
                raise PandoCqlError("children must be a list")
            _walk_hierarchy(
                children, field_names, path, out, freq_key=freq_key, include_pct=include_pct
            )
        else:
            row = {name: path.get(name) for name in field_names}
            for k, v in path.items():
                row.setdefault(k, v)
            row[freq_key] = m.get("count")
            if include_pct and "pct" in m:
                row["pct"] = m["pct"]
            out.append(row)
        path.pop(fname, None)


def is_count_payload(payload: Mapping[str, Any]) -> bool:
    op = payload.get("operation") or payload.get("last_command")
    if op in ("count", "group"):
        return True
    if op == "tabulate":   # rows of fields, no counts: see Corpus.table / Table
        return False
    result = payload.get("result")
    if isinstance(result, Mapping) and "fields" in result:
        return "hierarchy" in result or "rows" in result
    return False


def normalize_run_payload(payload: Mapping[str, Any], *, freq_key: str = "freq") -> Any:
    """
    If payload is a count/group result, return flattened rows; otherwise return ``result``
    (or the whole payload if there is no result wrapper).
    """
    if is_count_payload(payload):
        result = extract_result(payload)
        return flatten_count_result(result, freq_key=freq_key)
    if "result" in payload:
        return payload["result"]
    return dict(payload)
