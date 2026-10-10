#!/usr/bin/env python3
"""`dcoll … by attr, attr2` tallies attr2 per collocate (CLI text and JSON, pando-server
/run), an unknown attr2 is an error; JSON requests decode escapes (\\uXXXX, \\n).

  test/dcoll_breakdown_test.py --pando build/pando --pando-index build/pando-index \\
      --server build/pando-server --conllu test/data/sample.conllu
"""
import argparse, json, os, socket, subprocess, sys, tempfile, time, urllib.request

Q = '[upos="NOUN"]; dcoll children by lemma, deprel'


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser()
    for a in ("--pando", "--pando-index", "--server", "--conllu"):
        ap.add_argument(a, required=True)
    o = ap.parse_args()
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([o.pando_index, o.conllu, idx], check=True, capture_output=True)

        # CLI text: a column named after attr2, values most frequent first
        out = subprocess.run([o.pando, idx, Q], capture_output=True, text=True).stdout.splitlines()
        if not out or out[0].split("\t")[-1] != "deprel":
            fails.append(f"text header {out[:1]}")
        rows = {l.split("\t")[0]: l.split("\t")[-1] for l in out[1:]}
        if rows.get("de") != "case:27 det:12":
            fails.append(f"text de: {rows.get('de')!r}")

        # CLI JSON: breakdown_attribute + per collocate; counts add up to obs
        j = json.loads(subprocess.run([o.pando, "--json", idx, Q], capture_output=True, text=True).stdout)
        r = j["result"]
        if r.get("breakdown_attribute") != "deprel":
            fails.append(f"json attribute {r.get('breakdown_attribute')!r}")
        for c in r["collocates"]:
            if sum(c["breakdown"].values()) != c["obs"]:
                fails.append(f"json {c['word']}: breakdown {c['breakdown']} != obs {c['obs']}")
                break

        # without attr2: no breakdown at all
        j = json.loads(subprocess.run([o.pando, "--json", idx, '[upos="NOUN"]; dcoll children by lemma'],
                                      capture_output=True, text=True).stdout)
        if "breakdown_attribute" in j["result"] or "breakdown" in j["result"]["collocates"][0]:
            fails.append("breakdown without attr2")

        # unknown attr2
        p = subprocess.run([o.pando, idx, '[upos="NOUN"]; dcoll head by lemma, nosuch'], capture_output=True, text=True)
        if "unknown attribute nosuch" not in p.stderr:
            fails.append(f"unknown attr2: {p.stderr.strip()!r}")

        # pando-server /run: the same breakdown; \\u and \\n in the request are decoded
        port = free_port()
        srv = subprocess.Popen([o.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            def run(raw_json):
                for _ in range(50):
                    try:
                        req = urllib.request.Request(f"http://127.0.0.1:{port}/run", data=raw_json.encode())
                        return json.loads(urllib.request.urlopen(req, timeout=30).read())
                    except (ConnectionError, urllib.error.URLError):
                        time.sleep(0.1)
                raise RuntimeError("server did not start")
            r = run(json.dumps({"cql": Q}))["result"]
            de = next((c for c in r["collocates"] if c["word"] == "de"), None)
            if not de or de.get("breakdown") != {"case": 27, "det": 12}:
                fails.append(f"server de: {de}")
            plain = run(json.dumps({"cql": '[lemma="library"]; size'}, ensure_ascii=False))["result"]
            escaped = run('{"cql": "[lemma=\\"l\\u0069brary\\"];\\nsize"}')["result"]
            if plain != escaped or not plain:
                fails.append(f"escapes: {plain} vs {escaped}")
        finally:
            srv.terminate()
            srv.wait()
    if fails:
        print("\n".join("FAIL " + f for f in fails))
        sys.exit(1)
    print("PASS dcoll_breakdown")


if __name__ == "__main__":
    main()
