import json
from pathlib import Path

from pando_cql.parse import flatten_count_result, normalize_run_payload

FIXTURES = Path(__file__).parent / "fixtures"


def test_flatten_one_field():
    payload = json.loads((FIXTURES / "count_one_field.json").read_text())
    rows = normalize_run_payload(payload)
    assert rows == [
        {"a.upos": "NOUN", "freq": 277},
        {"a.upos": "DET", "freq": 188},
        {"a.upos": "VERB", "freq": 134},
    ]


def test_flatten_hierarchy_leaves_only():
    payload = json.loads((FIXTURES / "count_hierarchy.json").read_text())
    rows = flatten_count_result(payload["result"])
    assert len(rows) == 4
    assert {"form": "book", "upos": "NOUN", "freq": 7} in rows
    assert {"form": "book", "upos": "VERB", "freq": 3} in rows
    assert {"form": "books", "upos": "NOUN", "freq": 3} in rows
    assert {"form": "books", "upos": "VERB", "freq": 1} in rows
    # Intermediate rollup (form=book, count=10) must not appear as a row
    assert all(r["freq"] != 10 for r in rows)


def test_include_pct():
    payload = json.loads((FIXTURES / "count_one_field.json").read_text())
    rows = flatten_count_result(payload["result"], include_pct=True)
    assert rows[0]["pct"] == 24.0
