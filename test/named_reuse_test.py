#!/usr/bin/env python3
"""`A = q` again with the same query reuses the session's set (pando-server /run, with and
without a client session): same answers, `"reused": [...]` names them; a changed query, a
sorted set and the last statement (its page is the answer) run again.

  test/named_reuse_test.py --pando-index build/pando-index --server build/pando-server \\
      --conllu test/data/sample.conllu
"""
import argparse, json, os, socket, subprocess, sys, tempfile, time, urllib.request


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser()
    for a in ("--pando-index", "--server", "--conllu"):
        ap.add_argument(a, required=True)
    o = ap.parse_args()
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([o.pando_index, o.conllu, idx], check=True, capture_output=True)
        port = free_port()
        srv = subprocess.Popen([o.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def post(path, body):
            for _ in range(50):
                try:
                    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=json.dumps(body).encode())
                    return json.loads(urllib.request.urlopen(req, timeout=30).read())
                except (ConnectionError, urllib.error.URLError):
                    time.sleep(0.1)
            raise RuntimeError("server did not start")
        try:
            sid = post("/session", {})["session_id"]
            for label, extra in (("shared session", {}), ("client session", {"session_id": sid})):
                run = lambda cql: post("/run", dict(extra, cql=cql))
                P = 'A = [upos="NOUN"]; B = [upos="VERB"]; freq A, B by text_lang'
                a, b = run(P), run(P)
                if a.get("reused") and label == "client session":
                    fails.append(f"{label}: first run of a new session reused {a.get('reused')}")
                if b.get("reused") != ["A", "B"] or a["result"]["rows"] != b["result"]["rows"]:
                    fails.append(f"{label}: second run reused {b.get('reused')}, same rows {a['result']['rows'] == b['result']['rows']}")
                c = run('A = [upos="ADJ"]; B = [upos="VERB"]; freq A, B by text_lang')
                if c.get("reused") != ["B"] or c["result"]["totals_per_query"]["A"] == a["result"]["totals_per_query"]["A"]:
                    fails.append(f"{label}: changed A: reused {c.get('reused')}")
                run('A = [upos="ADJ"]; sort A by form; B = [upos="VERB"]; size B')
                e = run('A = [upos="ADJ"]; B = [upos="VERB"]; freq A, B by text_lang')
                if e.get("reused") != ["B"]:
                    fails.append(f"{label}: sorted A reused: {e.get('reused')}")
                f = run('A = [upos="ADJ"]; B = [upos="VERB"];')
                if f.get("reused") != ["A"] or not f["result"].get("hits"):
                    fails.append(f"{label}: last statement: reused {f.get('reused')}")
        finally:
            srv.terminate()
            srv.wait()
    if fails:
        print("\n".join("FAIL " + x for x in fails))
        sys.exit(1)
    print("PASS named_reuse")


if __name__ == "__main__":
    main()
