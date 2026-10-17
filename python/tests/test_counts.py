"""Counts are complete: pando's display limits (20 values under each group) are switched off, in a
session and in one-shot runs; all_groups=False keeps them. Against the repository's sample corpus
(skipped when there is no pando / pando-index binary)."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from pando_cql import Corpus

REPO = Path(__file__).resolve().parents[2]
SAMPLE_CONLLU = REPO / "test" / "data" / "sample.conllu"


def _binary(env, name):
    local = REPO / "build" / name
    return os.environ.get(env) or (str(local) if local.exists() else shutil.which(name))


PANDO = _binary("PANDO_BIN", "pando")
PANDO_INDEX = _binary("PANDO_INDEX_BIN", "pando-index")
pytestmark = pytest.mark.skipif(not PANDO or not PANDO_INDEX or not SAMPLE_CONLLU.exists(),
                                reason="no pando / pando-index binary, or no test/data/sample.conllu")
COUNT = "[]; count by upos, lemma"


@pytest.fixture(scope="module")
def sample(tmp_path_factory):
    out = tmp_path_factory.mktemp("sample") / "idx"
    subprocess.run([PANDO_INDEX, str(SAMPLE_CONLLU), str(out)], check=True, capture_output=True)
    return str(out)


def _most_under_one_group(rows):
    per = {}
    for r in rows:
        per[r["upos"]] = per.get(r["upos"], 0) + 1
    return max(per.values())


@pytest.mark.parametrize("session", [True, False])
def test_counts_are_complete(sample, session):
    with Corpus.open(sample, pando_bin=PANDO, session=session) as corp:
        rows = corp.run(COUNT)
        assert _most_under_one_group(rows) > 20          # more lemmas under a part of speech than pando shows
        assert sum(r["freq"] for r in rows) == sum(r["freq"] for r in corp.run("[]; count by upos"))
        assert len(corp.run(COUNT)) == len(rows)          # still complete on the next query


@pytest.mark.parametrize("session", [True, False])
def test_all_groups_false_keeps_pandos_limits(sample, session):
    with Corpus.open(sample, pando_bin=PANDO, session=session, all_groups=False) as corp:
        assert _most_under_one_group(corp.run(COUNT)) == 20
