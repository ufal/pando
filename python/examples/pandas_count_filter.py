#!/usr/bin/env python3
"""Minimal demo: pando count → pandas filter/sort (no CQL named tables)."""

import os
import sys

import pandas as pd
from pando_cql import Corpus

corpus = sys.argv[1] if len(sys.argv) > 1 else os.environ["PANDO_CORPUS"]

with Corpus.open(corpus) as corp:
    corp.run('Matches = [lemma="book"]')
    rows = corp.run("count Matches by form, upos")

df = (
    pd.DataFrame(rows)
    .query("freq >= 2")
    .sort_values("freq", ascending=False)
)
print(df[["form", "upos", "freq"]].to_string(index=False))
