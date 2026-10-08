"""Hits as a table: one row per hit, one column per tabulate field."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

from pando_cql.parse import PandoCqlError


def _is_position_field(name: str) -> bool:
    return name == "cpos" or name.endswith(".cpos")


class Table(list):
    """
    Rows of ``tabulate`` (a list of dicts, field → value), plus

    * ``fields``: the column names, in query order;
    * ``total``: the hits of the query (more than the rows when sampled or limited);
    * ``column(name)``, ``frame()`` (a pandas DataFrame; pandas is only needed for this).

    Corpus positions (``cpos`` fields) are ints, other values strings as in the corpus.
    """

    def __init__(self, rows: Iterable[Mapping[str, Any]] = (), *, fields: Optional[list[str]] = None,
                 total: Optional[int] = None) -> None:
        super().__init__(rows)
        self.fields = list(fields or [])
        self.total = total

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "Table":
        if payload.get("ok") is False:
            err = payload.get("error")
            if isinstance(err, Mapping):
                err = err.get("message", err)
            raise PandoCqlError(f"pando: {err}")
        result = payload.get("result")
        if not isinstance(result, Mapping) or "fields" not in result or "rows" not in result:
            raise PandoCqlError("not a tabulate result")
        fields = [str(f) for f in result["fields"]]
        convert = [int if _is_position_field(f) else str for f in fields]
        rows = (
            {f: conv(v) for f, conv, v in zip(fields, convert, row)}
            for row in result["rows"]
        )
        total = result.get("total_matches")
        return cls(rows, fields=fields, total=int(total) if total is not None else None)

    def column(self, name: str) -> list[Any]:
        return [row[name] for row in self]

    def frame(self):
        import pandas as pd

        return pd.DataFrame(list(self), columns=self.fields)

    def __repr__(self) -> str:
        return f"<Table {len(self)} rows × {len(self.fields)} fields {self.fields}, total {self.total}>"
