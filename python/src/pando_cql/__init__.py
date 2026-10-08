"""Python client for pando CQL (CLI backend): counts and hit tables for pandas."""

from pando_cql.cli_backend import find_pando_binary, validate_corpus_dir
from pando_cql.corpus import Corpus
from pando_cql.parse import PandoCqlError, flatten_count_result, normalize_run_payload
from pando_cql.table import Table

__all__ = [
    "Corpus",
    "Table",
    "PandoCqlError",
    "flatten_count_result",
    "normalize_run_payload",
    "find_pando_binary",
    "validate_corpus_dir",
]

__version__ = "0.2.0"
