#!/usr/bin/env python3
"""pando-server background totals (P6.2): /query "total": "async", /status, /cancel.

Builds an index from a CoNLL-U file, starts pando-server on a free port with
--debug-total-delay (every total is revealed gradually, so the jobs are
observable on a small corpus) and --abandon-after 1, and checks:

  * an async /query returns the first page at once with a queued / running job;
  * /status reports a non-decreasing count and finishes with the exact total
    (checked against `pando --count-only`);
  * the finished total is cached: the next /query (async or sync) has it at once;
  * a page that holds every hit finishes the job immediately;
  * a synchronous total is recorded as a finished job;
  * /cancel stops a job; a job nobody polls is abandoned;
  * unknown job ids give 404;
  * /version, /health, /info report the build (== `pando-server --version`),
    the features and the index's bitmaps / builder;
  * the fields kontext-pando reads (result.job_id, /status result.*) and
    limit 0 (total only, no hits).

  test/server_jobs_test.py --server build/pando-server --pando build/pando \\
      --pando-index build/pando-index --conllu test/data/sample.conllu
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
        print("FAIL:", msg)
    return cond


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Server:
    def __init__(self, port):
        self.base = f"http://127.0.0.1:{port}"

    def post(self, path, body):
        req = urllib.request.Request(self.base + path, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    def get(self, path):
        try:
            with urllib.request.urlopen(self.base + path, timeout=30) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def query(self, q, **kw):
        body = {"query": q, "limit": 1}
        body.update(kw)
        t0 = time.monotonic()
        res = self.post("/query", body)
        return res, time.monotonic() - t0


def count_only(pando, corpus, q):
    p = subprocess.run([pando, corpus, q, "--count-only"], capture_output=True, text=True, check=True)
    return int(p.stdout.strip().splitlines()[-1])


def poll(srv, job_id, timeout=15, interval=0.25):
    seen = []
    t_end = time.monotonic() + timeout
    while time.monotonic() < t_end:
        code, st = srv.get(f"/status?job={job_id}")
        if code != 200:
            return code, seen
        job = st["job"]
        seen.append(job)
        if job["state"] not in ("queued", "running"):
            return 200, seen
        time.sleep(interval)
    return 0, seen


def check_query_threads(opts, corpus, tmp):
    """P4.1: --query-threads splits sync totals, background totals and /run count by
    into position ranges (PANDO_PARTITION_MIN makes the sample corpus split too);
    every number must equal the single-threaded CLI's."""
    port = free_port()
    log = open(os.path.join(tmp, "server_mt.log"), "w")
    env = dict(os.environ, PANDO_PARTITION_MIN="50")
    proc = subprocess.Popen([opts.server, corpus, str(port), "4", "--query-threads", "4"],
                            stdout=log, stderr=subprocess.STDOUT, env=env)
    srv = Server(port)
    try:
        for _ in range(100):
            try:
                if srv.get("/health")[0] == 200:
                    break
            except (urllib.error.URLError, ConnectionError):
                pass
            time.sleep(0.05)
        code, h = srv.get("/health")
        check(code == 200 and h.get("query_threads") == 4, f"/health query_threads: {h.get('query_threads')}")
        for q in ('[upos="NOUN"] [upos="PUNCT"]', '[upos="VERB"] > [upos="NOUN"]',
                  '[upos="DET"] []{0,2} [upos="NOUN"]', '[upos="VERB"] >> [upos="NOUN"]',
                  '[upos="NOUN"] !> [upos="DET"]', '[upos="ADJ"] [] [upos="NOUN"] within s'):
            want = count_only(opts.pando, corpus, q)
            res, _ = srv.query(q, total=True, limit=5)
            page = res["result"]["page"]
            check(page["total"] == want and page["total_exact"], f"--query-threads sync {q}: {page} vs {want}")
            res, _ = srv.query(q + " ", total="async", limit=1)   # a new result set
            code, seen = poll(srv, res["result"]["job"]["id"])
            last = seen[-1] if seen else {}
            check(code == 200 and last.get("state") == "finished" and last.get("total") == want,
                  f"--query-threads async {q}: {last} vs {want}")
        cli = subprocess.run([opts.pando, corpus, 'a:[upos="ADJ"] b:[upos="NOUN"]; count by b.lemma',
                              "--json"], capture_output=True, text=True, check=True).stdout
        res = srv.post("/run", {"cql": 'a:[upos="ADJ"] b:[upos="NOUN"]; count by b.lemma', "group_limit": 1000})
        want_rows = {r["key"]: r["count"] for r in json.loads(cli)["result"]["rows"]}
        got_rows = {r["key"]: r["count"] for r in res["result"]["rows"]}
        check(got_rows == want_rows and got_rows, f"--query-threads /run count by: {len(got_rows)} vs {len(want_rows)} rows")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    opts = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="pando_srvjobs_")
    corpus = os.path.join(tmp, "idx")
    p = subprocess.run([opts.pando_index, opts.conllu, corpus], capture_output=True, text=True)
    if p.returncode != 0:
        print(p.stdout, p.stderr, file=sys.stderr)
        return 2
    port = free_port()
    log = open(os.path.join(tmp, "server.log"), "w")
    proc = subprocess.Popen([opts.server, corpus, str(port), "4", "--debug-total-delay", "2500",
                             "--abandon-after", "1", "--total-workers", "2"],
                            stdout=log, stderr=subprocess.STDOUT)
    srv = Server(port)
    try:
        for _ in range(100):
            try:
                if srv.get("/health")[0] == 200:
                    break
            except (urllib.error.URLError, ConnectionError):
                pass
            time.sleep(0.05)

        # 0. versioning: /version, /health and /info say which pando build answers
        #    and what the index has (P3 bitmaps built by pando-index --upgrade)
        ver = subprocess.run([opts.server, "--version"], capture_output=True, text=True).stdout.strip()
        code, v = srv.get("/version")
        check(code == 200 and v.get("build") and v.get("version") and "async_total" in v.get("features", []),
              f"/version: {v}")
        check(ver == "pando-server " + v.get("build_string", ""), f"--version {ver!r} vs /version {v.get('build_string')!r}")
        code, h = srv.get("/health")
        check(code == 200 and h.get("build") == v.get("build"), f"/health: {h}")
        code, info = srv.get("/info")
        r = info.get("result", {})
        check(r.get("server", {}).get("build") == v.get("build") and r.get("pando", {}).get("build") == v.get("build"),
              f"/info server / pando: {r.get('server')} {r.get('pando')}")
        idx = r.get("index", {})
        check(idx.get("indexed_with") and idx.get("upgraded_with"), f"/info index built / upgraded with: {idx}")
        check(any(b["attr"] == "upos" and b["status"] == "ok" for b in idx.get("bitmaps", [])),
              f"/info index bitmaps: {idx.get('bitmaps')}")
        check(any(b["struct"] == "s" and b["status"] == "ok" for b in idx.get("structure_bitmaps", [])),
              f"/info structure bitmaps: {idx.get('structure_bitmaps')}")

        # 1. async: page now, job in the background
        q = '[upos="NOUN"]'
        want = count_only(opts.pando, corpus, q)
        res, dt = srv.query(q, total="async")
        page, job = res["result"]["page"], res["result"]["job"]
        check(dt < 1.0, f"async /query took {dt:.2f}s (the page must not wait for the count)")
        check(page["returned"] == 1, f"page returned {page['returned']}")
        check(job["state"] in ("queued", "running"), f"job state {job['state']}")
        check(not page["total_exact"], "async page total must not be exact yet")
        # 2. poll: counted non-decreasing, finishes with the exact total
        code, seen = poll(srv, job["id"])
        check(code == 200 and seen and seen[-1]["state"] == "finished", f"job did not finish: {seen[-1:]}")
        counts = [s["counted"] for s in seen]
        check(counts == sorted(counts), f"counted decreased: {counts}")
        check(any(0 < s["counted"] < want for s in seen), f"no partial count seen: {counts}")
        check(all(s["progress"] is None or 0 <= s["progress"] <= 1 for s in seen), "progress out of [0,1]")
        check(seen[-1]["total"] == want and seen[-1]["total_exact"], f"total {seen[-1]['total']} != {want}")
        # 3. cached: async and sync
        res, dt = srv.query(q, total="async", limit=5)
        check(res["result"]["page"]["total"] == want and res["result"]["page"]["total_exact"],
              f"cached async total: {res['result']['page']}")
        check(res["result"]["job"]["state"] == "finished", "cached job not finished")
        res, dt = srv.query(q, total=True, offset=3)
        check(res["result"]["page"]["total"] == want and dt < 1.0, f"cached sync total: {res['result']['page']} {dt:.2f}s")
        # 4. page holds every hit → finished at once
        q_small = '[lemma="house"]'
        n_small = count_only(opts.pando, corpus, q_small)
        res, _ = srv.query(q_small, total="async", limit=n_small + 5)
        check(res["result"]["job"]["state"] == "finished" and res["result"]["page"]["total"] == n_small,
              f"small result not finished at once: {res['result']['job']} ({n_small})")
        # 5. synchronous total recorded as a finished job
        q_sync = '[upos="VERB"]'
        res, _ = srv.query(q_sync, total=True)
        jid = res["result"]["job"]["id"]
        code, st = srv.get(f"/status?job={jid}")
        check(code == 200 and st["job"]["state"] == "finished"
              and st["job"]["total"] == count_only(opts.pando, corpus, q_sync), f"sync job: {st}")
        # 6. cancel
        res, _ = srv.query('[upos="ADJ"]', total="async")
        jid = res["result"]["job"]["id"]
        c = srv.post(f"/cancel?job={jid}", {})
        check(c.get("cancelled") is True, f"cancel answered {c}")
        code, seen = poll(srv, jid)
        check(seen and seen[-1]["state"] == "cancelled", f"after cancel: {seen[-1:]}")
        # a new request for a cancelled query starts it again
        res, _ = srv.query('[upos="ADJ"]', total="async")
        check(res["result"]["job"]["state"] in ("queued", "running"), f"restart: {res['result']['job']}")
        srv.post(f"/cancel?job={jid}", {})
        # 7. abandoned: nobody polls → cancelled after --abandon-after
        res, _ = srv.query('[upos="DET"]', total="async")
        jid = res["result"]["job"]["id"]
        time.sleep(2.2)
        code, st = srv.get(f"/status?job={jid}")
        check(code == 200 and st["job"]["state"] == "cancelled", f"abandoned job: {st}")
        # 8. the contract kontext-pando's pando_backend.py reads:
        #    result.job_id (string); /status → result.{finished,total,progress};
        #    limit 0 + "async" → no hits, just the job (must not materialise every hit)
        res, dt = srv.query('[upos="PUNCT"]', total="async", limit=0)
        r = res["result"]
        check(isinstance(r.get("job_id"), str) and r["job_id"] == r["job"]["id"], f"job_id: {r.get('job_id')}")
        check(r["page"]["returned"] == 0 and not r["hits"] and dt < 1.0, f"limit 0: {r['page']} {dt:.2f}s")
        code, st = srv.get(f"/status?job={r['job_id']}")
        check(code == 200 and {"finished", "total", "progress"} <= set(st.get("result", {})),
              f"/status result fields: {st}")
        res, dt = srv.query('[upos="PRON"]', total=True, limit=0)
        check(res["result"]["page"]["total"] == count_only(opts.pando, corpus, '[upos="PRON"]')
              and res["result"]["page"]["total_exact"] and not res["result"]["hits"],
              f"limit 0 sync: {res['result']['page']}")
        # 9. unknown id; different max_total = different result set
        code, _ = srv.get("/status?job=0000000000000000")
        check(code == 404, f"unknown job gave {code}")
        res, _ = srv.query(q, total="async", max_total=3)
        check(res["result"]["job"]["id"] != job["id"], "max_total must be part of the result identity")
        code, jobs = srv.get("/jobs")
        check(code == 200 and len(jobs["jobs"]) >= 5, f"/jobs: {code} {len(jobs.get('jobs', []))}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            FAILS.append("server did not stop on SIGTERM within 10 s")
        log.close()
        if FAILS:
            print(open(os.path.join(tmp, "server.log")).read()[-2000:])
    try:
        check_query_threads(opts, corpus, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"server_jobs_test: {'FAILED (' + str(len(FAILS)) + ')' if FAILS else 'all passed'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
