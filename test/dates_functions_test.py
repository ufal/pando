#!/usr/bin/env python3
"""Date functions (core/dates) agree on every route: one text per date value, and for year(), century(),
decade(), month(), day(), earliest(), latest() and precision() of match.text_date:

  * tabulate gives the value the date determines (EDTF; the old reading for other values; nothing else);
  * count by groups the texts the same way;
  * a :: filter on the function selects the same texts (functions with a number; match. as in count by);
  * freq by (its own subcorpus path) counts the same;
  * the answers say which values were read the old way, are no date, or do not give what was asked.

  test/dates_functions_test.py --server build/pando-server --pando-index build/pando-index
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)
    return cond


# value → expected (year, century, decade, month, day, earliest, latest, precision); "" = none
VALUES = {
    "1945": ("1945", "20", "1940", "", "", "1945", "1945", "year"),
    "1945-12-02": ("1945", "20", "1940", "12", "2", "1945", "1945", "day"),
    "1945-12-02T13:02:45Z": ("1945", "20", "1940", "12", "2", "1945", "1945", "day"),
    "1945/12/02": ("1945", "20", "1940", "12", "2", "1945", "1945", "day"),       # not EDTF: the old reading
    "0945": ("945", "10", "940", "", "", "945", "945", "year"),
    "1945?": ("1945", "20", "1940", "", "", "1945", "1945", "year"),
    "1550s": ("1550", "16", "1550", "", "", "1550", "1550", "year"),              # old reading
    "1712/1734": ("", "18", "", "", "", "1712", "1734", "interval"),
    "1712-1734": ("1712", "18", "1710", "", "", "1712", "1712", "year"),          # old reading: the start
    "-0044": ("-44", "-1", "-50", "", "", "-44", "-44", "year"),
    "196X": ("", "20", "1960", "", "", "1960", "1969", "decade"),
    "16XX": ("", "", "", "", "", "1600", "1699", "century"),
    "1985-04-XX": ("1985", "20", "1980", "4", "", "1985", "1985", "month"),
    "1985-21": ("1985", "20", "1980", "", "", "1985", "1985", "season"),
    "../1734": ("", "", "", "", "", "", "1734", "interval"),
    "02-12-1945": ("", "", "", "", "", "", "", ""),
    "[1945]": ("", "", "", "", "", "", "", ""),
    "XVI": ("", "", "", "", "", "", "", ""),
}
FUNCS = ["year", "century", "decade", "month", "day", "earliest", "latest", "precision"]


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando_dates_")
    try:
        lines = [{"type": "header", "version": 2, "positional": ["form"]}]
        texts = list(VALUES)
        for i, v in enumerate(texts):
            lines.append({"type": "token", "tok_id": "1", "form": "w"})
            lines.append({"type": "region", "struct": "text", "start_pos": i, "end_pos": i,
                          "attrs": {"id": f"t{i}", "date": v}})
        src = os.path.join(tmp, "dates.jsonl")
        with open(src, "w") as f:
            for o in lines:
                f.write(json.dumps(o) + "\n")
        idx = os.path.join(tmp, "idx")
        subprocess.run([a.pando_index, "--format", "jsonl", src, idx], check=True, capture_output=True)
        port = free_port()
        proc = subprocess.Popen([a.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        base = f"http://127.0.0.1:{port}"

        def run(cql):
            req = urllib.request.Request(base + "/run", data=json.dumps({"cql": cql, "limit": 1000, "group_limit": 1000}).encode(),
                                         method="POST", headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                return {"ok": False, "status": e.code, "error": json.loads(e.read() or b"{}").get("error"), "cql": cql}

        try:
            for _ in range(200):
                try:
                    urllib.request.urlopen(base + "/health", timeout=2)
                    break
                except (urllib.error.URLError, ConnectionError):
                    time.sleep(0.05)
            # tabulate: per text, every function
            fields = ", ".join(["match.text_id"] + [f"{fn}(match.text_date)" for fn in FUNCS])
            tab = run(f"<text> []; tabulate {fields}")
            got = {row[0]: row[1:] for row in tab["result"]["rows"]}
            for i, v in enumerate(texts):
                want = list(VALUES[v])
                check(got.get(f"t{i}") == want, f"tabulate {v!r}: {got.get(f't{i}')} != {want}")
            for k, fn in enumerate(FUNCS):
                expect = {}
                for v in texts:
                    expect[VALUES[v][k]] = expect.get(VALUES[v][k], 0) + 1
                # count by
                rows = run(f"<text> []; count by {fn}(match.text_date)")["result"]["rows"]
                counts = {r["key"]: r["count"] for r in rows}
                check(counts == expect, f"count by {fn}: {counts} != {expect}")
                # freq by (the subcorpus path)
                rows = run(f"<text> []; freq by {fn}(match.text_date)")["result"]["rows"]
                counts = {r["key"]: r["count"] for r in rows}
                check(counts == expect, f"freq by {fn}: {counts} != {expect}")
                # :: filter, for each value with a number
                if fn == "precision":
                    continue
                for val, n in expect.items():
                    if val == "":
                        continue
                    r = run(f"<text> [] :: {fn}(match.text_date) = {val}; size")
                    check(r.get("result") == n, f":: {fn}(match.text_date) = {val}: {r.get('result')} != {n}")
            # warnings: what the values are, said once per answer (query/date_notes)
            w = run("<text> []; count by year(match.text_date)")["result"].get("warnings", [])
            check(any("3 of 18 values are not EDTF" in x for x in w), f"warnings: the old reading: {w}")
            check(any("3 of 18 values are no date" in x for x in w), f"warnings: no date: {w}")
            check(any(x.startswith("year(text_date): 4 of 18 values do not give a year") for x in w), f"warnings: no year: {w}")
            req = urllib.request.Request(base + "/query", data=json.dumps({"query": "<text> [] :: year(text_date) = 1945", "limit": 5}).encode(),
                                         method="POST", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as r:
                q = json.loads(r.read())
            check(len(q["result"]["hits"]) == 5 and any("not EDTF" in x for x in q["result"].get("warnings", [])),
                  f"/query: hits and warnings: {len(q['result']['hits'])}, {q['result'].get('warnings')}")
            r = run("<text> []; count by match.text_id")
            check("warnings" not in r["result"], f"no date function, no warnings: {r['result'].get('warnings')}")
            # a filter on a range: could be from (overlap) and certainly from (within) 1700–1750
            r = run("<text> [] :: earliest(match.text_date) <= 1750 & latest(match.text_date) >= 1700; size")
            # 1712/1734 and 1712-1734; ../1734 has no earliest year (open start), so it does not pass
            check(r.get("result") == 2, f"overlaps 1700–1750: {r.get('result')} != 2")
            r = run("<text> [] :: earliest(match.text_date) >= 1700 & latest(match.text_date) <= 1750; size")
            check(r.get("result") == 2, f"within 1700–1750: {r.get('result')} != 2")
        finally:
            proc.terminate()
            proc.wait()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("FAILED: %d" % len(FAILS) if FAILS else "all date functions agree")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
