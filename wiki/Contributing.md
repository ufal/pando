# Contributing

## Layout

- **`src/`** — C++ core: `corpus/`, `index/`, `query/`, `api/`, `cli/`
- **`docs/`** — Long-form docs (`PANDO-CQL.md`, `SAMPLE-CORPUS.md`, `PANDO.md`)
- **`wiki/`** — This wiki (overview + topic pages; links into `docs/`)
- **`dev/`** — Design notes, benchmarks, roadmaps (not shipped)

## Design notes

See `README.md` in the repo root for pointers to `dev/ROADMAP-TODO.md`, `dev/PANDO-INDEX-INTEGRATION.md`, `dev/QUERY-COMPAT.md`, etc.

## Wiki

- Prefer updating **topic wikis** for short, stable explanations and **../docs/PANDO-CQL.md** for full language tutorial.
- Preview Markdown locally: `python scripts/serve-wiki-preview.py`

## Build

See [Installation](Installation.md).
