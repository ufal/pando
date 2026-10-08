"""High-level Corpus API for pando_cql."""

from __future__ import annotations

from typing import Any, Optional, Sequence, Union

from pando_cql.cli_backend import (
    CliSession,
    find_pando_binary,
    run_oneshot,
    validate_corpus_dir,
)
from pando_cql.parse import PandoCqlError
from pando_cql.table import Table

# `tabulate Last 0 N`: every hit
_ALL_ROWS = 2**62


class Corpus:
    """
    Open a pando corpus index directory and run CQL via the ``pando`` CLI.

    Count/group results are returned as a ``list[dict]`` suitable for
    ``pandas.DataFrame`` (hierarchy flattened in Python). Other operations
    return the JSON ``result`` object.

    ``Corpus.open`` validates that the corpus looks like a pando index and that
    a usable ``pando`` binary can be found (argument, ``PANDO_BIN``, or PATH).

    Example::

        from pando_cql import Corpus
        import pandas as pd

        corp = Corpus.open("/path/to/index")
        corp.run('Matches = [lemma="book"]')
        df = pd.DataFrame(corp.run("count Matches by form, upos"))
        df = df.rename(columns={"form": "word", "upos": "pos"}).query("freq >= 2")
    """

    def __init__(
        self,
        corpus_dir: str,
        *,
        pando_bin: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
        session: bool = True,
        check_pando: bool = True,
    ) -> None:
        self.corpus_dir = validate_corpus_dir(corpus_dir)
        self.pando_bin = find_pando_binary(pando_bin, check_runs=check_pando)
        self.extra_args = list(extra_args or [])
        self._session: Optional[CliSession] = None
        if session:
            # Binary already validated above; skip a second --version probe.
            self._session = CliSession(
                self.corpus_dir,
                pando_bin=self.pando_bin,
                extra_args=self.extra_args,
                check_pando=False,
            )

    @classmethod
    def open(
        cls,
        corpus_dir: str,
        *,
        pando_bin: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
        session: bool = True,
        check_pando: bool = True,
    ) -> "Corpus":
        return cls(
            corpus_dir,
            pando_bin=pando_bin,
            extra_args=extra_args,
            session=session,
            check_pando=check_pando,
        )

    def run(self, cql: str) -> Any:
        """Run a CQL program. Count/group → ``list[dict]``; else JSON ``result``."""
        if self._session is not None:
            return self._session.run(cql)
        return run_oneshot(
            self.corpus_dir,
            cql,
            pando_bin=self.pando_bin,
            extra_args=self.extra_args,
        )

    def table(
        self,
        query: str,
        fields: Union[str, Sequence[str]],
        *,
        sample: Optional[int] = None,
        seed: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Table:
        """
        Run ``query`` and tabulate its hits: one row per hit, one column per field.

        ``fields`` as in CQL ``tabulate``: ``"h.lemma, d.cpos, h.text_langcode"`` or a
        list. Token fields use the query's labels (``h.lemma``), region attributes the
        structure prefix (``h.text_langcode``), ``h.cpos`` gives the corpus position.
        ``sample``: a random sample of that many hits (``seed`` for the same sample
        every time); ``limit``: at most that many rows (default: all, or the sample).

            t = corpus.table('h:[upos="VERB"] > d:[deprel="obj"]',
                             "h.text_langcode, h.cpos, d.cpos", sample=10000, seed=1)
            t.frame()   # pandas DataFrame
        """
        cols = fields if isinstance(fields, str) else ", ".join(fields)
        n = limit if limit is not None else (sample if sample else _ALL_ROWS)
        program = f"{query.strip().rstrip(';').strip()}; tabulate Last 0 {n} {cols}"
        if self._session is not None:
            # settings persist in the session: set both every time (0 = off / random)
            for name, value in (("sample", sample or 0), ("seed", seed or 0)):
                answer = self._session.run_raw(f"set {name} {value}")
                if answer.get("ok") is False:
                    raise PandoCqlError(f"pando: set {name}: {answer.get('error')}")
            return Table.from_payload(self._session.run_raw(program))
        args = list(self.extra_args)
        if sample:
            args += ["--sample", str(sample)]
        if seed:
            args += ["--seed", str(seed)]
        return Table.from_payload(
            run_oneshot(self.corpus_dir, program, pando_bin=self.pando_bin, extra_args=args, raw=True)
        )

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self) -> "Corpus":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
