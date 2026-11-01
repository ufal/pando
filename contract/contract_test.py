#!/usr/bin/env python3
"""The result-format contract: pando-server's answer to every request in requests.json, on
corpus.jsonl, must have the shape stored in answers/NAME.json, and the status it names.

    contract/contract_test.py --server build/pando-server --pando-index build/pando-index [--write [--only NAMES]]

A shape is the keys of every object and the type of every value: str, number (integers and
decimals alike), bool, null, object, list. A list's shape is the union of its elements'; null and
an empty list match anything (values that depend on timing, or on there being any). Every
answer must be an object with "format_version" equal to requests.json's.

Within a format version, changes are additive only (wiki/Result-Format.md). A key that is gone
or changed type is a broken contract; a new key also fails here, so that it enters the contract
on purpose: --write stores the answers anew (review the diff, then commit). The stored answers
are complete, so consumers (FQS, flexicorp, KonText) can test their readers on them.
Exit status 0 = the contract holds, 1 = a difference, 2 = a run error.
"""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


def shape(x):
    if isinstance(x, dict):
        return {k: shape(v) for k, v in x.items()}
    if isinstance(x, list):
        merged = None
        for e in x:
            merged = shape(e) if merged is None else merge(merged, shape(e))
        return [merged] if merged is not None else []
    if x is None:
        return "null"
    if isinstance(x, bool):
        return "bool"
    if isinstance(x, (int, float)):
        return "number"
    return "str"


def merge(a, b):
    if a == b or b == "null" or b == []:
        return a
    if a == "null" or a == []:
        return b
    if isinstance(a, dict) and isinstance(b, dict):
        return {k: merge(a[k], b[k]) if k in a and k in b else a.get(k, b.get(k)) for k in {**a, **b}}
    if isinstance(a, list) and isinstance(b, list):
        return [merge(a[0], b[0])]
    names = lambda s: set(s.split("|")) if isinstance(s, str) else {"object" if isinstance(s, dict) else "list"}
    return "|".join(sorted(names(a) | names(b)))


def differences(stored, now, path="$"):
    """What in `now` breaks or extends the stored shape."""
    if stored == "null" or now == "null" or stored == [] or now == []:
        return []
    if isinstance(stored, dict) and isinstance(now, dict):
        out = [f"{path}.{k}: missing" for k in stored if k not in now]
        out += [f"{path}.{k}: new key" for k in now if k not in stored]
        for k in stored:
            if k in now:
                out += differences(stored[k], now[k], f"{path}.{k}")
        return out
    if isinstance(stored, list) and isinstance(now, list):
        return differences(stored[0], now[0], f"{path}[]")
    kind = lambda s: "object" if isinstance(s, dict) else "list" if isinstance(s, list) else s
    if isinstance(stored, str) and isinstance(now, str) and set(now.split("|")) <= set(stored.split("|")):
        return []
    return [f"{path}: {kind(stored)} became {kind(now)}"]


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def call(base, method, path, body):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def fill(x, saved):
    """{var} in strings → the value saved under var."""
    if isinstance(x, str):
        return re.sub(r"\{(\w+)\}", lambda m: str(saved.get(m.group(1), m.group(0))), x)
    if isinstance(x, dict):
        return {k: fill(v, saved) for k, v in x.items()}
    if isinstance(x, list):
        return [fill(v, saved) for v in x]
    return x


def dig(x, dotted):
    for part in dotted.split("."):
        x = x.get(part) if isinstance(x, dict) else None
    return x


def scrub(x, tmp):
    """The answer as stored: the temporary folder's path replaced by <index>."""
    if isinstance(x, str):
        return x.replace(tmp, "<index>")
    if isinstance(x, dict):
        return {k: scrub(v, tmp) for k, v in x.items()}
    if isinstance(x, list):
        return [scrub(v, tmp) for v in x]
    return x


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--write", action="store_true", help="store the answers as the contract")
    ap.add_argument("--only", default="", help="with --write: store only these answers (comma-separated names)")
    a = ap.parse_args()
    spec = json.load(open(os.path.join(HERE, "requests.json"), encoding="utf-8"))
    version = spec["format_version"]
    tmp = tempfile.mkdtemp(prefix="pando_contract_")
    idx = os.path.join(tmp, "index")
    p = subprocess.run([a.pando_index, "--format", "jsonl", os.path.join(HERE, "corpus.jsonl"), idx],
                       capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout[-2000:], p.stderr[-2000:])
        return 2
    port = free_port()
    log = open(os.path.join(tmp, "server.log"), "w")
    proc = subprocess.Popen([a.server, idx, str(port)], stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    fails, saved, names = [], {}, []
    try:
        for _ in range(200):
            try:
                if call(base, "GET", "/health", None)[0] == 200:
                    break
            except (urllib.error.URLError, ConnectionError):
                pass
            time.sleep(0.05)
        else:
            print("pando-server did not start:", open(os.path.join(tmp, "server.log")).read()[-2000:])
            return 2
        for r in spec["requests"]:
            name = r["name"]
            names.append(name)
            path, body = fill(r["path"], saved), fill(r.get("body"), saved)
            status, raw = call(base, r["method"], path, body)
            try:
                answer = json.loads(raw)
            except ValueError:
                fails.append(f"{name}: not JSON: {raw[:200]!r}")
                continue
            for var, where in r.get("save", {}).items():
                saved[var] = dig(answer, where)
            problems = []
            if status != r.get("status", 200):
                problems.append(f"HTTP {status}, expected {r.get('status', 200)}")
            if not isinstance(answer, dict) or answer.get("format_version") != version:
                problems.append(f"format_version {answer.get('format_version') if isinstance(answer, dict) else '-'}, expected {version}")
            stored_file = os.path.join(HERE, "answers", f"{name}.json")
            only = {n for n in a.only.split(",") if n}
            if a.write and only and name not in only:
                pass
            elif a.write:
                os.makedirs(os.path.dirname(stored_file), exist_ok=True)
                with open(stored_file, "w", encoding="utf-8") as f:
                    json.dump({"request": {"method": r["method"], "path": r["path"], "body": r.get("body")},
                               "status": status, "answer": scrub(answer, tmp)}, f, ensure_ascii=False, indent=1)
                    f.write("\n")
            elif not os.path.exists(stored_file):
                problems.append("no stored answer (run with --write)")
            else:
                stored = json.load(open(stored_file, encoding="utf-8"))["answer"]
                problems += differences(shape(stored), shape(answer))
            fails += [f"{name}: {x}" for x in problems]
            print(("FAIL " if problems else "ok   ") + name + ("" if not problems else ": " + "; ".join(problems[:6])))
        if a.write and not a.only:
            for f in os.listdir(os.path.join(HERE, "answers")):
                if f.endswith(".json") and f[:-5] not in names:
                    os.remove(os.path.join(HERE, "answers", f))
                    print(f"removed answers/{f} (no longer requested)")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"{len(names) - len({f.split(':')[0] for f in fails})} of {len(names)} answers as the contract says"
          + (" (written)" if a.write else ""))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
