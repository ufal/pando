#!/usr/bin/env python3
"""pando-server client sessions (P6.1): stored hit sets reused across requests.

Builds an index from a CoNLL-U file (or uses --corpus), starts pando-server and
checks, for every query in the battery, that a session gives exactly what the
session-less routes give:

  * /query with "session_id" + "name" stores the set; /query "from" pages it
    (lazy: the query again; deep pages: materialised) == /query pages;
  * `sort Q by …` in a session == the inline `q; sort by …` program, and every
    later /query "from" page of the sorted set == the inline program's page;
    two sort steps are applied in order;
  * `count Q by …` (sink) == the inline `q; count by …`, also after sorting;
    `Q = q; count Q by a` then `count Q by b` counts b (no stale buckets);
  * `size Q`, "total" on a stored set, limit 0 (total only);
  * a /run query without a name (a page) is Last: `count by` on it counts all hits;
  * with --session-memory-bytes 1 every cache is dropped after each request:
    pages, sorts and counts are re-derived and still equal;
  * --session-max-hits: 413 "too_large" for a set that would need more (`raw`), small sets work,
    a sort of a big set works without keeping its hits;
  * async deposits ("total": "async") pick up the background total;
  * TTL expiry, --max-sessions (LRU idle session makes room), client-chosen ids,
    bad ids / names, unknown session / set (404 with unknown_session / unknown_hitset),
    /session/close, /sessions, /health "sessions";
  * concurrent clients on separate sessions (and on one session) get the answers
    of a single-threaded run.

  test/server_sessions_test.py --server build/pando-server \\
      --pando-index build/pando-index --conllu test/data/sample.conllu
  test/server_sessions_test.py --server build/pando-server --corpus /data/ud_demo --big
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)
    return cond


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Server:
    def __init__(self, exe, corpus, *args, env=None):
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        e = dict(os.environ)
        e.update(env or {})
        self.proc = subprocess.Popen([exe, corpus, str(self.port), "8", *args],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=e)
        for _ in range(200):
            try:
                if self.get("/health")[0] == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("pando-server did not start")

    def close(self):
        self.proc.terminate()
        self.proc.wait(timeout=10)

    def req(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method,
                                   headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=300) as f:
                return f.status, json.loads(f.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def post(self, path, body):
        return self.req("POST", path, body)

    def get(self, path):
        return self.req("GET", path)

    def session(self, **kw):
        st, r = self.post("/session", kw)
        assert st == 200, (st, r)
        return r["session_id"]


def hits_key(res):
    """What must be identical between two pages: positions and the KWIC."""
    r = res.get("result", {})
    return [(h["match_start"], h["match_end"], h["context"]["left"], h["context"]["match"],
             h["context"]["right"]) for h in r.get("hits", [])]


def page_total(res):
    p = res.get("result", {}).get("page", {})
    return p.get("total"), p.get("total_exact")


def rows_key(res):
    r = res.get("result", {})
    if "rows" in r:
        return [(x["key"], x["count"]) for x in r["rows"]], r.get("total_matches")
    return json.dumps(r, sort_keys=True), None


QUERIES_SMALL = [
    '[upos="NOUN"]',
    '[upos="ADJ"] [upos="NOUN"]',
    '[upos="DET"] []{0,2} [upos="NOUN"]',
    '[upos="VERB"] > [upos="NOUN"]',
    'a:[upos="ADJ"] b:[upos="NOUN"]',
    '[lemma="the"]',
    '[upos="NOUN"] within s',
    '[lemma="zzzznotaword"]',
]
QUERIES_BIG = [
    '[upos="ADJ"] [upos="NOUN"]',
    '[upos="VERB"] > [upos="NOUN"]',
    '[lemma="dog"]',
    '[upos="DET"] []{0,3} [upos="NOUN"]',
    '[upos="NUM"] [upos="NOUN"] within s',
]


def check_battery(srv, queries, label, offsets, count_field="lemma", sort2="upos"):
    """Every session answer == the session-less answer, for every query."""
    for q in queries:
        sid = srv.session()
        # deposit (page + exact total), then pages from the lazy set
        st, dep = srv.post("/query", {"session_id": sid, "name": "Q", "query": q, "limit": 10, "total": True})
        if not check(st == 200 and dep.get("session_id") == sid and dep.get("hitset") == "Q",
                     f"{label} deposit {q}: {st} {str(dep)[:200]}"):
            continue
        st, plain = srv.post("/query", {"query": q, "limit": 10, "total": True})
        check(hits_key(dep) == hits_key(plain) and page_total(dep) == page_total(plain),
              f"{label} deposit page == /query page: {q}")
        total = page_total(plain)[0]
        for off in offsets:
            _, want = srv.post("/query", {"query": q, "offset": off, "limit": 7, "total": True})
            st, got = srv.post("/query", {"session_id": sid, "from": "Q", "offset": off, "limit": 7, "total": True})
            check(st == 200 and hits_key(got) == hits_key(want) and page_total(got) == page_total(want),
                  f"{label} lazy page {off} of {q}: {st} {page_total(got)} vs {page_total(want)}")
        # limit 0 and size
        st, got = srv.post("/query", {"session_id": sid, "from": "Q", "limit": 0, "total": True})
        check(st == 200 and page_total(got)[0] == total and not hits_key(got), f"{label} limit 0 from {q}")
        st, got = srv.post("/run", {"session_id": sid, "cql": "size Q"})
        check(st == 200 and got.get("result") == total, f"{label} size Q {q}: {got.get('result')} vs {total}")
        # count (sink) == inline
        _, want_c = srv.post("/run", {"cql": f"{q}; count by {count_field}", "group_limit": 1000})
        st, got_c = srv.post("/run", {"session_id": sid, "cql": f"count Q by {count_field}", "group_limit": 1000})
        check(st == 200 and rows_key(got_c) == rows_key(want_c), f"{label} count Q by {count_field}: {q}")
        # sort == inline sort program, then pages of the sorted set
        _, want_s = srv.post("/run", {"cql": f"{q}; sort by {count_field}", "limit": 10})
        st, got_s = srv.post("/run", {"session_id": sid, "cql": f"sort Q by {count_field}", "limit": 10})
        if st == 413:
            check(got_s.get("too_large"), f"{label} 413 without too_large: {q}")
            continue
        check(st == 200 and hits_key(got_s) == hits_key(want_s), f"{label} sort Q by {count_field}: {q}")
        for off in offsets:
            _, want = srv.post("/run", {"cql": f"{q}; sort by {count_field}", "offset": off, "limit": 7})
            st, got = srv.post("/query", {"session_id": sid, "from": "Q", "offset": off, "limit": 7, "total": True})
            check(st == 200 and hits_key(got) == hits_key(want) and page_total(got)[0] == total,
                  f"{label} sorted page {off} of {q}: {st} {page_total(got)}")
        # count after sorting (order-free) and a second sort step
        st, got_c2 = srv.post("/run", {"session_id": sid, "cql": f"count Q by {count_field}", "group_limit": 1000})
        check(rows_key(got_c2) == rows_key(want_c), f"{label} count after sort: {q}")
        _, want_s2 = srv.post("/run", {"cql": f"{q}; sort by {count_field}; sort by {sort2}", "offset": 3, "limit": 9})
        srv.post("/run", {"session_id": sid, "cql": f"sort Q by {sort2}", "limit": 1})
        st, got = srv.post("/query", {"session_id": sid, "from": "Q", "offset": 3, "limit": 9})
        check(st == 200 and hits_key(got) == hits_key(want_s2), f"{label} two sort steps: {q}")
        st, info = srv.get(f"/session?session_id={sid}")
        sets = {s["name"]: s for s in info.get("sets", [])}
        check(st == 200 and sets.get("Q", {}).get("sorts") == [[count_field], [sort2]]
              and "Last" in sets["Q"].get("aliases", []), f"{label} /session sorts / aliases: {q}")
        srv.post("/session/close", {"session_id": sid})


def check_semantics(srv, label):
    q = '[upos="ADJ"] [upos="NOUN"]'
    sid = srv.session()
    # the old aggregated-set bug: a second count by another field
    srv.post("/run", {"session_id": sid, "cql": f"Q = {q}; count Q by lemma", "group_limit": 50})
    _, want = srv.post("/run", {"cql": f"{q}; count by upos", "group_limit": 50})
    st, got = srv.post("/run", {"session_id": sid, "cql": "count Q by upos", "group_limit": 50})
    check(st == 200 and rows_key(got) == rows_key(want), f"{label} count by upos after count by lemma")
    # an unnamed /run query is a page: count on Last must count every hit
    srv.post("/run", {"session_id": sid, "cql": q, "limit": 2})
    _, want = srv.post("/run", {"cql": f"{q}; count by lemma", "group_limit": 1000})
    st, got = srv.post("/run", {"session_id": sid, "cql": "count by lemma", "group_limit": 1000})
    check(st == 200 and rows_key(got) == rows_key(want), f"{label} count on a paged Last")
    st, got = srv.post("/run", {"session_id": sid, "cql": "size Last"})
    _, plain = srv.post("/query", {"query": q, "limit": 1, "total": True})
    check(got.get("result") == page_total(plain)[0], f"{label} size of a paged Last")
    # show named lists the sets
    st, got = srv.post("/run", {"session_id": sid, "cql": "show named"})
    names = sorted(x["name"] for x in got.get("result", []))
    check(st == 200 and names == ["Last", "Q"], f"{label} show named: {names}")
    # drop
    srv.post("/run", {"session_id": sid, "cql": "drop Q"})
    st, got = srv.post("/query", {"session_id": sid, "from": "Q"})
    check(st == 404 and got.get("unknown_hitset"), f"{label} from a dropped set: {st}")
    # name / from without a session, bad names, unknown sessions
    st, _ = srv.post("/query", {"query": q, "name": "X"})
    check(st == 400, f"{label} name without session_id: {st}")
    st, _ = srv.post("/query", {"session_id": sid, "query": q, "name": "1bad-name"})
    check(st == 400, f"{label} bad set name: {st}")
    for path, body in (("/query", {"session_id": "nope", "query": q}),
                       ("/run", {"session_id": "nope", "cql": q})):
        st, got = srv.post(path, body)
        check(st == 404 and got.get("unknown_session") and got.get("session_id") == "nope",
              f"{label} unknown session {path}: {st}")
    st, got = srv.get("/session?session_id=nope")
    check(st == 404 and got.get("unknown_session"), f"{label} GET /session unknown: {st}")
    # client-chosen ids: idempotent create
    st, a = srv.post("/session", {"session_id": "kontext-user_1.a:b"})
    st2, b = srv.post("/session", {"session_id": "kontext-user_1.a:b"})
    check(st == 200 and a.get("created") is True and st2 == 200 and b.get("created") is False,
          f"{label} client-chosen id: {a} {b}")
    st, _ = srv.post("/session", {"session_id": "bad id!"})
    check(st == 400, f"{label} bad session id: {st}")
    # close
    st, got = srv.post("/session/close", {"session_id": sid})
    check(st == 200 and got.get("closed") is True, f"{label} close")
    st, got = srv.post("/query", {"session_id": sid, "from": "Last"})
    check(st == 404 and got.get("unknown_session"), f"{label} closed session: {st}")
    st, got = srv.get("/sessions")
    check(st == 200 and any(s["session_id"] == "kontext-user_1.a:b" for s in got.get("sessions", [])),
          f"{label} /sessions")
    st, h = srv.get("/health")
    check(st == 200 and "sessions" in h.get("features", []) and "open" in h.get("sessions", {}),
          f"{label} /health sessions")


def check_async(srv, label):
    q = '[upos="NOUN"]'
    sid = srv.session()
    st, dep = srv.post("/query", {"session_id": sid, "name": "N", "query": q, "limit": 3, "total": "async"})
    check(st == 200 and "job_id" in dep.get("result", {}), f"{label} async deposit")
    _, plain = srv.post("/query", {"query": q, "limit": 1, "total": True})
    want = page_total(plain)[0]
    got_total = None
    for _ in range(200):
        st, got = srv.post("/query", {"session_id": sid, "from": "N", "offset": 3, "limit": 3, "total": "async"})
        job = got.get("result", {}).get("job", {})
        if job.get("state") == "finished" or got.get("result", {}).get("page", {}).get("total_exact"):
            got_total = page_total(got)
            break
        time.sleep(0.05)
    check(got_total == (want, True), f"{label} async total via from: {got_total} vs {want}")
    st, info = srv.get(f"/session?session_id={sid}")
    n = {s["name"]: s for s in info.get("sets", [])}.get("N", {})
    check(n.get("total") == want and n.get("total_exact") is True, f"{label} /session total after async: {n}")


def check_limits(exe, corpus, label, big_q):
    # max hits: a big set cannot be kept (raw, a second sort), a small one can; one
    # sort of a set that is not kept needs no hits (P7.10: per sort key)
    srv = Server(exe, corpus, "--session-max-hits", "5")
    try:
        sid = srv.session()
        srv.post("/query", {"session_id": sid, "name": "B", "query": big_q, "limit": 1, "total": True})
        st, got = srv.post("/run", {"session_id": sid, "cql": "sort B by lemma"})
        check(st == 200 and got.get("result", {}).get("page", {}).get("total", 0) > 5,
              f"{label} sort of a too-large set (per key, no hits kept): {st} {str(got)[:200]}")
        st, got = srv.post("/run", {"session_id": sid, "cql": "raw B"})
        check(st == 413 and got.get("too_large") and got.get("max_hits") == 5, f"{label} max hits 413: {st} {str(got)[:200]}")
        st, got = srv.post("/run", {"session_id": sid, "cql": "count B by lemma"})
        check(st == 200, f"{label} count of a too-large set still works (sink): {st}")
        st, got = srv.post("/query", {"session_id": sid, "from": "B", "offset": 20000, "limit": 2})
        check(st == 200, f"{label} deep page of a too-large set pages by query: {st}")
        srv.post("/query", {"session_id": sid, "name": "S", "query": '[lemma="zzzznotaword"]', "limit": 1, "total": True})
        st, got = srv.post("/run", {"session_id": sid, "cql": "sort S by lemma"})
        check(st == 200, f"{label} small set sorts under max hits: {st}")
    finally:
        srv.close()
    # ttl
    srv = Server(exe, corpus, "--session-ttl", "1")
    try:
        sid = srv.session()
        st, _ = srv.post("/run", {"session_id": sid, "cql": "show named"})
        check(st == 200, f"{label} ttl: fresh session works")
        time.sleep(2.3)
        st, got = srv.post("/run", {"session_id": sid, "cql": "show named"})
        check(st == 404 and got.get("unknown_session"), f"{label} ttl: expired session 404: {st}")
    finally:
        srv.close()
    # max sessions: the least recently used idle one makes room
    srv = Server(exe, corpus, "--max-sessions", "2")
    try:
        a = srv.session()
        time.sleep(0.02)
        b = srv.session()
        time.sleep(0.02)
        srv.post("/run", {"session_id": a, "cql": "show named"})   # a is now the more recent
        c = srv.session()
        st_a, _ = srv.get(f"/session?session_id={a}")
        st_b, _ = srv.get(f"/session?session_id={b}")
        st_c, _ = srv.get(f"/session?session_id={c}")
        check((st_a, st_b, st_c) == (200, 404, 200), f"{label} max sessions LRU: {(st_a, st_b, st_c)}")
    finally:
        srv.close()


def check_concurrent(srv, queries, label, offsets):
    """Several clients at once, each on its own session; one extra thread per
    query hammering a shared session. Answers == a single-threaded run."""
    def script(sid, q):
        out = []
        out.append(hits_key(srv.post("/query", {"session_id": sid, "name": "Q", "query": q, "limit": 5, "total": True})[1]))
        out.append(rows_key(srv.post("/run", {"session_id": sid, "cql": "count Q by lemma", "group_limit": 100})[1]))
        st, r = srv.post("/run", {"session_id": sid, "cql": "sort Q by lemma", "limit": 5})
        out.append(st if st != 200 else hits_key(r))
        for off in offsets:
            st, r = srv.post("/query", {"session_id": sid, "from": "Q", "offset": off, "limit": 5, "total": True})
            out.append((st, hits_key(r), page_total(r)))
        return out

    want = {q: script(srv.session(), q) for q in queries}
    got, errs = {}, []

    def worker(q, k):
        try:
            got[(q, k)] = script(srv.session(), q)
        except Exception as e:   # noqa: BLE001
            errs.append(repr(e))

    shared = srv.session()

    def shared_worker():
        try:
            for q in queries:
                srv.post("/query", {"session_id": shared, "name": "S", "query": q, "limit": 3, "total": True})
                srv.post("/run", {"session_id": shared, "cql": "sort S by lemma", "limit": 3})
                srv.post("/query", {"session_id": shared, "from": "S", "offset": 2, "limit": 3})
        except Exception as e:   # noqa: BLE001
            errs.append(repr(e))

    ts = [threading.Thread(target=worker, args=(q, k)) for q in queries for k in range(2)]
    ts += [threading.Thread(target=shared_worker) for _ in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    check(not errs, f"{label} concurrent errors: {errs[:3]}")
    for (q, k), v in got.items():
        check(v == want[q], f"{label} concurrent answers differ: {q} #{k}")


def check_warm(exe, corpus, label):
    """--warm hot reads the hot index files in the background; POST /warm all
    extends it to every file; the answers are the same as without."""
    def wait_done(srv):
        w = {}
        for _ in range(400):
            st, r = srv.get("/warm")
            w = r.get("warm", {})
            if w.get("state") != "running":
                break
            time.sleep(0.05)
        return w
    plain = Server(exe, corpus)
    srv = Server(exe, corpus, "--warm", "hot", "--warm-streams", "3", "--pool-threads", "2",
                 "--query-threads", "auto")
    try:
        # P4.1d: the process pool and query_threads auto in /health
        st, h = srv.get("/health")
        pool = h.get("pool", {})
        check(pool.get("threads") == 2 and pool.get("usable_cpus", 0) >= 1 and h.get("query_threads") == 2,
              f"{label}: /health pool: {pool}, query_threads {h.get('query_threads')}")
        st, h = plain.get("/health")
        check(h.get("query_threads") == 1 and h.get("pool", {}).get("threads", 0) >= 1,
              f"{label}: default pool / query_threads: {h.get('pool')} {h.get('query_threads')}")
        w = wait_done(srv)
        check(w.get("state") == "done" and w.get("level") == "hot" and w.get("files", 0) > 5
              and w.get("bytes_done") == w.get("bytes"), f"{label}: --warm hot: {w}")
        hot_files = w.get("files", 0)
        st, h = srv.get("/health")
        check(h.get("warm", {}).get("state") == "done", f"{label}: /health warm: {h.get('warm')}")
        st, r = srv.post("/warm", {"level": "all"})
        check(st == 200 and r.get("warm", {}).get("level") == "all", f"{label}: POST /warm all: {st} {r}")
        w = wait_done(srv)
        check(w.get("state") == "done" and w.get("files", 0) > hot_files and w.get("bytes_done") == w.get("bytes"),
              f"{label}: warm all: {w}")
        st, r = srv.post("/warm", {"level": "lukewarm"})
        check(st == 400, f"{label}: bad level: {st} {r}")
        # residency: after warming everything, (nearly) all of it is in the page cache
        st, r = srv.get("/warm?residency=all")
        res = r.get("residency", {})
        check(st == 200 and res.get("level") == "all" and res.get("bytes", 0) > 0
              and res.get("resident_bytes", -1) <= res.get("bytes", 0) and res.get("resident", 0) > 0.5,
              f"{label}: residency all: {st} {res}")
        st, r = srv.get("/warm?residency=hot")
        check(st == 200 and 0 < r.get("residency", {}).get("bytes", 0) < res.get("bytes", 0),
              f"{label}: residency hot: {st} {r.get('residency')}")
        st, r = srv.get("/warm?residency=tepid")
        check(st == 400, f"{label}: bad residency level: {st} {r}")
        st, r = srv.get("/warm")
        check("residency" not in r, f"{label}: residency only on request: {r}")
        st, r = plain.get("/warm")
        check(st == 200 and r.get("warm", {}).get("state") == "idle", f"{label}: no warm: {r}")
        for q in ('[upos="NOUN"]', '[upos="VERB"] > [deprel="nsubj"]', '[upos="DET"] [upos="NOUN"]'):
            body = {"query": q, "limit": 5, "total": True}
            a, b = plain.post("/query", body), srv.post("/query", body)
            check(a[0] == b[0] == 200 and a[1].get("result", {}).get("hits") == b[1].get("result", {}).get("hits")
                  and a[1]["result"]["page"]["total"] == b[1]["result"]["page"]["total"],
                  f"{label}: {q} differs after warming")
    finally:
        srv.close()
        plain.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index")
    ap.add_argument("--conllu")
    ap.add_argument("--corpus", help="an existing index (instead of --conllu)")
    ap.add_argument("--big", action="store_true", help="ud_demo-sized battery")
    opts = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        corpus = opts.corpus
        if not corpus:
            corpus = os.path.join(tmp, "idx")
            subprocess.run([opts.pando_index, opts.conllu, corpus], check=True, capture_output=True)
        queries = QUERIES_BIG if opts.big else QUERIES_SMALL
        offsets = [0, 7, 30, 12000] if opts.big else [0, 3, 11, 40]
        big_q = '[upos="NOUN"]'

        srv = Server(opts.server, corpus, "--query-threads", "2", env={"PANDO_PARTITION_MIN": "50"})
        try:
            check_battery(srv, queries, "default", offsets)
            check_semantics(srv, "default")
            check_async(srv, "default")
            check_concurrent(srv, queries[:4], "default", offsets[:3])
        finally:
            srv.close()
        # every cache dropped after each request: sets are re-derived (sorted again)
        srv = Server(opts.server, corpus, "--session-memory-bytes", "1")
        try:
            check_battery(srv, queries, "evicting", offsets)
        finally:
            srv.close()
        check_limits(opts.server, corpus, "limits", big_q)
        check_warm(opts.server, corpus, "warm")

    print(f"{'OK' if not FAILS else 'FAILED'}: {len(FAILS)} failures")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
