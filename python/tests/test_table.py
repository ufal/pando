"""Corpus.table / Table: unit tests on a tabulate payload, and against a built pando
on the repository's sample corpus (skipped when there is no pando binary)."""

import os
import shutil
from pathlib import Path

import pytest

from pando_cql import Corpus, PandoCqlError, Table
from pando_cql.parse import normalize_run_payload

REPO = Path(__file__).resolve().parents[2]
SAMPLE = REPO / "test" / "data" / "sample_idx_tmp_tcnt"
PANDO = os.environ.get("PANDO_BIN") or (str(REPO / "build" / "pando") if (REPO / "build" / "pando").exists()
                                         else shutil.which("pando"))
needs_pando = pytest.mark.skipif(not PANDO or not SAMPLE.exists(), reason="no pando binary / sample corpus")
QUERY = 'h:[upos="VERB"] > d:[deprel="obj"]'

PAYLOAD = {
    "ok": True, "operation": "tabulate",
    "result": {"fields": ["h.cpos", "d.cpos", "h.lemma"], "total_matches": 60,
               "rows": [["24", "27", "open"], ["34", "36", "water"]]},
}


def test_table_from_payload_converts_positions():
    t = Table.from_payload(PAYLOAD)
    assert t.fields == ["h.cpos", "d.cpos", "h.lemma"]
    assert t.total == 60
    assert t[0] == {"h.cpos": 24, "d.cpos": 27, "h.lemma": "open"}
    assert t.column("d.cpos") == [27, 36]


def test_table_from_error_payload_raises():
    with pytest.raises(PandoCqlError, match="unknown token attribute"):
        Table.from_payload({"ok": False, "error": "unknown token attribute 'x'"})


def test_run_leaves_tabulate_results_alone():
    # a tabulate result has fields + rows like a count, but no counts to flatten
    assert normalize_run_payload(PAYLOAD) == PAYLOAD["result"]


def test_table_frame():
    pd = pytest.importorskip("pandas")
    df = Table.from_payload(PAYLOAD).frame()
    assert list(df.columns) == ["h.cpos", "d.cpos", "h.lemma"]
    assert df["h.cpos"].dtype.kind == "i"


@needs_pando
def test_set_answers_in_a_session():
    with Corpus(str(SAMPLE), pando_bin=PANDO) as c:
        assert c.run("set limit 2") == {"ok": True, "operation": "set", "name": "limit", "value": "2"}
        with pytest.raises(PandoCqlError):
            c.table(QUERY, "h.nosuch")
        assert len(c.table(QUERY, "h.cpos", limit=3)) == 3      # the session survived the error


@needs_pando
def test_table_all_rows_and_positions():
    with Corpus(str(SAMPLE), pando_bin=PANDO) as c:
        t = c.table(QUERY, ["h.cpos", "d.cpos", "h.text_lang"])
        assert len(t) == t.total == 60
        assert all(isinstance(r["h.cpos"], int) and r["h.cpos"] != r["d.cpos"] for r in t)
        hits = c.run(QUERY)            # the same positions as the hits of the query
        assert (hits["hits"][0]["tokens"][0]["pos"], hits["hits"][0]["tokens"][1]["pos"]) \
            == (t[0]["h.cpos"], t[0]["d.cpos"])


@needs_pando
def test_sample_is_reproducible_in_session_and_one_shot():
    with Corpus(str(SAMPLE), pando_bin=PANDO) as c:
        a = c.table(QUERY, "h.cpos", sample=7, seed=5).column("h.cpos")
        b = c.table(QUERY, "h.cpos", sample=7, seed=5).column("h.cpos")
        full = c.table(QUERY, "h.cpos")               # the sample setting does not stick
    one = Corpus(str(SAMPLE), pando_bin=PANDO, session=False).table(QUERY, "h.cpos", sample=7, seed=5)
    assert a == b == one.column("h.cpos") and len(a) == 7
    assert len(full) == 60 and set(a) <= set(full.column("h.cpos"))


@needs_pando
def test_table_after_an_earlier_statement():
    with Corpus(str(SAMPLE), pando_bin=PANDO) as c:
        c.run(QUERY)
        t = c.table('x:[upos="DET"] :: h.s_sent_id = x.s_sent_id', "x.form, x.s_sent_id")
        sents = set(c.table(QUERY, "h.s_sent_id").column("h.s_sent_id"))
        assert len(t) > 0 and set(t.column("x.s_sent_id")) <= sents
