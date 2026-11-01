# The result-format contract

pando's answers, as [wiki/Result-Format.md](../wiki/Result-Format.md) describes them, fixed and tested:

| file | |
|---|---|
| `corpus.jsonl` | a small self-authored corpus (two texts, English and Dutch, aligned by `tuid`; dependencies; `np` regions) |
| `requests.json` | one request per result kind, route and error, with the HTTP status it gets |
| `answers/NAME.json` | pando's complete answer to each request (`request`, `status`, `answer`) |
| `contract_test.py` | replays the requests and compares each answer's shape with the stored one (`ctest -R contract`) |

```
contract/contract_test.py --server build/pando-server --pando-index build/pando-index          # check
contract/contract_test.py --server build/pando-server --pando-index build/pando-index --write  # store anew
```

A difference means the result format changed. Within a `format_version`, only additions are
allowed: store them with `--write`, review the diff, and commit. Anything else raises the version
(`ServerApi::kFormatVersion`).

Consumers (FQS, flexicorp, KonText) test their readers on `answers/*.json`: every kind must come
out right and non-empty, and an unknown shape must be reported, never shown as an empty result.
