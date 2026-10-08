import os

import pytest

from pando_cql import PandoCqlError, find_pando_binary, validate_corpus_dir


def test_validate_corpus_missing(tmp_path):
    with pytest.raises(PandoCqlError, match="does not exist"):
        validate_corpus_dir(str(tmp_path / "nope"))


def test_validate_corpus_not_index(tmp_path):
    d = tmp_path / "empty"
    d.mkdir()
    with pytest.raises(PandoCqlError, match="corpus.info"):
        validate_corpus_dir(str(d))


def test_validate_corpus_hint_nested_pando(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    nested = root / "pando"
    nested.mkdir()
    (nested / "corpus.info").write_text("ok\n")
    with pytest.raises(PandoCqlError, match="Hint: found an index"):
        validate_corpus_dir(str(root))


def test_validate_corpus_ok(tmp_path):
    d = tmp_path / "idx"
    d.mkdir()
    (d / "corpus.info").write_text("name=test\n")
    assert validate_corpus_dir(str(d)) == str(d.resolve())


def test_find_pando_missing(monkeypatch):
    monkeypatch.delenv("PANDO_BIN", raising=False)
    monkeypatch.setattr("pando_cql.cli_backend.shutil.which", lambda _: None)
    with pytest.raises(PandoCqlError, match="pando binary not found"):
        find_pando_binary(None, check_runs=False)


def test_find_pando_explicit_missing(tmp_path):
    with pytest.raises(PandoCqlError, match="does not exist"):
        find_pando_binary(str(tmp_path / "no-pando"), check_runs=False)
