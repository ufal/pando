#!/usr/bin/env python3
"""P6.4: the server's result cache must not change any answer.

On test/data/sample.conllu, two pando-servers, one with `--cache-mb 0` (no
cache) and one with the default cache, get the same requests, each twice (the
second one is answered from the cache): /query pages and totals (also async and
limit 0), /run programs (count / freq / coll / dcoll / tabulate / size / sort,
`set` options, several statements), and session flows (a stored query, sort,
pages, commands, a second sort, persistent names). Every answer must be the same
(timings masked), and the cached server must report cache hits in /health.

  test/result_cache_test.py --server build/pando-server --pando-index build/pando-index \\
      --conllu test/data/sample.conllu
"""
import argparse, json, os, re, subprocess, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import Server  # noqa: E402

FAILS = []
VOLATILE = re.compile(r"^(elapsed_ms|elapsed|time_ms|query_ms|session_id|id|job|progress|counted|estimate|idle_s)$")


def check(c, msg):
    if not c:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)


def mask(o):
    if isinstance(o, dict):
        return {k: ("*" if VOLATILE.match(k) else mask(v)) for k, v in o.items()}
    if isinstance(o, list):
        return [mask(x) for x in o]
    return o


QUERIES = [
    {"query": '[upos="NOUN"]', "limit": 5},
    {"query": '[upos="NOUN"]', "limit": 5, "offset": 10, "total": True},
    {"query": '[upos="NOUN"]', "limit": 0, "total": True},
    {"query": 'a:[upos="ADJ"] b:[upos="NOUN"]', "limit": 3, "total": True, "context": 2},
    {"query": 'a:[upos="ADJ"] b:[upos="NOUN"]', "limit": 3, "total": True, "context": 7, "attrs": "lemma"},
    {"query": '[lemma="book"]', "limit": 2, "sentence": True},
    {"query": '[upos="VERB"] > [upos="NOUN"]', "limit": 4, "total": True, "max_total": 10},
    {"query": '[upos="NOUN"]', "limit": 3, "sample": 5, "seed": 7},
    {"query": '[upos="NOUN"]', "limit": 3, "shuffle": True, "seed": 3},
    {"query": '[upos="NOUN"', "limit": 3},
]
PROGRAMS = [
    '[upos="NOUN"]; count by lemma',
    '[upos="NOUN"]; count by lemma, text_lang',
    '[upos="ADJ"]; freq by lemma',
    '[upos="ADJ"] [upos="NOUN"]; coll by lemma',
    'set left 2; set right 1; [upos="ADJ"] [upos="NOUN"]; coll by lemma',
    '[upos="VERB"]; dcoll obj by lemma',
    'a:[upos="VERB"] > b:[upos="NOUN"]; dcoll on b head by lemma',
    '[upos="NOUN"]; tabulate 2 5 match.form, match.lemma',
    'x = [upos="NOUN"]; y = [upos="VERB"]; count x by lemma',
    'x = [upos="NOUN"]; y = [upos="VERB"]; count y by lemma',
    'x = [upos="NOUN"]; sort x by lemma; tabulate x 0 6 match.lemma',
    'x = [upos="NOUN"]; sort x by lemma; sort x by upos; tabulate x 0 6 match.lemma',
    'x = [upos="NOUN"]; size x',
    '[upos="NOUN"]',
    'x = [upos="NOUN"]; x',
    'eng:[lemma="library"]; nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma',
    'eng:[lemma="park"]; nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma',
    '[upos="NOUN"]; count by nosuch',
]


def session_flow(srv):
    out = []
    sid = srv.session()
    def post(path, body):
        st, r = srv.post(path, dict(body, session_id=sid))
        out.append((st, mask(r)))
    post("/query", {"name": "B", "query": '[upos="NOUN"]', "limit": 2, "total": True})
    post("/run", {"cql": "count B by lemma", "group_limit": 5})
    post("/run", {"cql": "freq B by upos"})
    post("/run", {"cql": "sort B by lemma", "limit": 3})
    for off in (0, 4, 30, 400):
        post("/query", {"from": "B", "offset": off, "limit": 3})
    post("/run", {"cql": "tabulate B 5 4 match.lemma"})
    post("/run", {"cql": "coll B by lemma"})
    post("/run", {"cql": "sort B by form; tabulate B 0 4 match.form"})
    post("/run", {"cql": 'eng:[lemma="library"]; nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma'})
    post("/run", {"cql": 'eng:[lemma="park"]; count by lemma'})
    post("/run", {"cql": 'nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma'})
    srv.post("/session/close", {"session_id": sid})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    opts = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([opts.pando_index, opts.conllu, idx], check=True, capture_output=True)
        plain = Server(opts.server, idx, "--cache-mb", "0")
        cached = Server(opts.server, idx)
        try:
            for rnd in (1, 2):
                for body in QUERIES:
                    a, b = plain.post("/query", body), cached.post("/query", body)
                    check(a[0] == b[0] and mask(a[1]) == mask(b[1]),
                          f"/query round {rnd} {body}: {str(a)[:200]} != {str(b)[:200]}")
                for cql in PROGRAMS:
                    a, b = plain.post("/run", {"cql": cql}), cached.post("/run", {"cql": cql})
                    check(a[0] == b[0] and mask(a[1]) == mask(b[1]),
                          f"/run round {rnd} {cql}: {str(a)[:200]} != {str(b)[:200]}")
                fa, fb = session_flow(plain), session_flow(cached)
                for i, (x, y) in enumerate(zip(fa, fb)):
                    check(x == y, f"session round {rnd} step {i}: {str(x)[:200]} != {str(y)[:200]}")
            st, h = cached.get("/health")
            c = (h.get("server") or h).get("cache", {})
            check(c.get("hits", 0) > 20 and c.get("entries", 0) > 0, f"cache hits / entries: {c}")
            st, h = plain.get("/health")
            c = (h.get("server") or h).get("cache", {})
            check(c.get("entries", 1) == 0 and c.get("max_bytes", 1) == 0, f"--cache-mb 0 keeps nothing: {c}")
        finally:
            plain.close()
            cached.close()
    if FAILS:
        print(f"{len(FAILS)} failure(s)")
        sys.exit(1)
    print("PASS result_cache")


if __name__ == "__main__":
    main()
