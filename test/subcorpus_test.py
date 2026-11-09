#!/usr/bin/env python3
"""EF-12 subcorpora: pando-server against brute force on a generated (self-authored) corpus.

- POST /subcorpus: tokens / ranges of region-based, token-based and set-expression definitions
  equal the positions computed here from the definitions' own hits;
- /query with "subcorpus": the hits are exactly the unrestricted hits whose tokens all lie in
  the subcorpus (regions as hits: any part inside), for single tokens, sequences, repetition,
  dependency relations, `within s`, region anchors; same answer for both encodings
  (PANDO_SUBCORPUS_ENCODING) and with the fast paths off (PANDO_FASTPATH=off);
- /run: `S = (…) | (…)`, `use S`, `within S`, `use A; … within B`, `use;`: totals and N
  (corpus_size, ipm), freq per group inside, coll (window collocates inside, frequencies in
  the subcorpus), keyness against the rest of the subcorpus;
- errors: 400 with a message; the CLI's --subcorpus gives the server's answer.
"""
import argparse, json, os, random, socket, subprocess, sys, tempfile, time, urllib.error, urllib.request

GENRES = ["fiction", "news", "science"]
VOCAB = [("the", "DET"), ("a", "DET"), ("cat", "NOUN"), ("dog", "NOUN"), ("house", "NOUN"), ("idea", "NOUN"),
         ("runs", "VERB"), ("sees", "VERB"), ("likes", "VERB"), ("old", "ADJ"), ("green", "ADJ"),
         ("quickly", "ADV"), ("and", "CCONJ"), ("in", "ADP"), (".", "PUNCT")]


def make_corpus(path, seed=11):
    rng = random.Random(seed)
    with open(path, "w", encoding="utf-8") as f:
        for t in range(24):
            f.write("# newregion text\n")
            f.write(f"# text_id = t{t}\n# text_genre = {GENRES[t % 3]}\n# text_year = {1990 + (t * 7) % 6}\n")
            for s in range(rng.randint(2, 6)):
                f.write(f"# sent_id = t{t}-s{s}\n")
                n = rng.randint(3, 11)
                for i in range(1, n + 1):
                    form, upos = rng.choice(VOCAB)
                    head = 0 if i == 1 else rng.randint(1, i - 1)
                    f.write(f"{i}\t{form}\t{form}\t{upos}\t_\t_\t{head}\t{'root' if head == 0 else 'dep'}\t_\t_\n")
                f.write("\n")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Server:
    def __init__(self, binary, idx, env=None):
        self.port = free_port()
        e = dict(os.environ)
        e.update(env or {})
        self.proc = subprocess.Popen([binary, idx, str(self.port)], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, env=e)
        for _ in range(100):
            try:
                self.get("/health")
                return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def get(self, path):
        return json.loads(urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=60).read())

    def post(self, path, body):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=json.dumps(body).encode())
        try:
            r = urllib.request.urlopen(req, timeout=120)
            return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def stop(self):
        self.proc.terminate()
        self.proc.wait()


failures = []


def check(cond, what):
    if not cond:
        failures.append(what)
        print("FAIL", what)


def hit_positions(h):
    return sorted({t["pos"] for t in h.get("tokens", [])}) or list(range(h["match_start"], h["match_end"] + 1))


def all_hits(srv, query, sub=None):
    body = {"query": query, "limit": 100000, "total": True, "context": 0}
    if sub:
        body["subcorpus"] = sub
    st, a = srv.post("/query", body)
    assert st == 200, (query, a)
    return a, a["result"]["hits"]


def positions_of(srv, definition):
    """The positions a definition stands for, from its query's hits (each hit's span)."""
    _, hits = all_hits(srv, definition)
    out = set()
    for h in hits:
        out.update(range(h["match_start"], h["match_end"] + 1))
    return out


def runs(s):
    n, prev = 0, -2
    for p in sorted(s):
        if p != prev + 1:
            n += 1
        prev = p
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando", required=True)
    o = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando-subcorpus-")
    conllu = os.path.join(tmp, "c.conllu")
    idx = os.path.join(tmp, "idx")
    make_corpus(conllu)
    subprocess.run([o.pando_index, conllu, idx], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    servers = {"auto": Server(o.server, idx),
               "ranges": Server(o.server, idx, {"PANDO_SUBCORPUS_ENCODING": "ranges"}),
               "roaring": Server(o.server, idx, {"PANDO_SUBCORPUS_ENCODING": "roaring"}),
               "nofast": Server(o.server, idx, {"PANDO_FASTPATH": "off"})}
    srv = servers["auto"]
    try:
        FIC = 't:<text genre="fiction">'
        Y92 = 't:<text year="1992">'
        NOUN = '[upos="NOUN"]'
        S = {"fic": positions_of(srv, FIC), "y92": positions_of(srv, Y92), "noun": positions_of(srv, NOUN)}
        defs = {
            FIC: S["fic"],
            NOUN: S["noun"],
            f"({FIC}) | ({Y92})": S["fic"] | S["y92"],
            f"({FIC}) & ({Y92})": S["fic"] & S["y92"],
            f"({FIC}) - ({Y92})": S["fic"] - S["y92"],
            f"({FIC}) & ({NOUN})": S["fic"] & S["noun"],
            f"({FIC}) | ({Y92}) & ({NOUN})": S["fic"] | (S["y92"] & S["noun"]),   # & binds tighter
            f"(({FIC}) | ({Y92})) - ({NOUN})": (S["fic"] | S["y92"]) - S["noun"],
        }
        check(len(S["fic"]) > 50 and len(S["noun"]) > 50, "test corpus has fiction and nouns")

        # 1. /subcorpus
        for d, ref in defs.items():
            for name, sv in servers.items():
                st, a = sv.post("/subcorpus", {"definition": d})
                check(st == 200 and a["tokens"] == len(ref) and a["ranges"] == runs(ref),
                      f"/subcorpus {d} [{name}]: {a} vs tokens {len(ref)} ranges {runs(ref)}")
                if name in ("ranges", "roaring"):
                    check(a.get("encoding") == name, f"/subcorpus encoding forced {name}: {a.get('encoding')}")
        st, a = srv.post("/subcorpus", {"definition": NOUN})
        check(a["encoding"] in ("ranges", "roaring") and "index_id" in a, "/subcorpus answer fields")

        # 2. /query: hits = unrestricted hits all inside (regions: overlapping)
        queries = [NOUN, '[upos="DET"] [upos="NOUN"]', '[upos="ADJ"] []{1,2} [upos="NOUN"]',
                   '[upos="NOUN"] < [upos="VERB"]', '[upos="DET"] [upos="NOUN"] within s',
                   '<s> [upos="DET"]', 's:<s>', '[upos="NOUN"]', '"cat"', '[lemma="cat" | lemma="dog"] [upos!="PUNCT"]']
        for d, ref in defs.items():
            for q in queries:
                _, full = all_hits(srv, q)
                region_only = q == "s:<s>"
                want = []
                for h in full:
                    ps = list(range(h["match_start"], h["match_end"] + 1)) if region_only else hit_positions(h)
                    inside = any(p in ref for p in ps) if region_only else all(p in ref for p in ps)
                    if inside:
                        want.append((h["match_start"], h["match_end"], tuple(hit_positions(h))))
                for name, sv in servers.items():
                    for mode in ("use", "within"):
                        a, got = all_hits(sv, q, {"definition": d, "mode": mode, "name": "X"})
                        g = [(h["match_start"], h["match_end"], tuple(hit_positions(h))) for h in got]
                        check(g == want and a["result"]["page"]["total"] == len(want),
                              f"/query {q} in {d} [{name}, {mode}]: {len(g)} hits, total "
                              f"{a['result']['page']['total']}, want {len(want)}")
                        sc = a.get("subcorpus", {})
                        check(sc.get("tokens") == len(ref) and sc.get("mode") == mode and sc.get("name") == "X",
                              f"/query subcorpus member {sc}")
        # a page and a background total under a subcorpus: the same total
        st, a = srv.post("/query", {"query": NOUN, "limit": 1, "total": "async", "subcorpus": FIC})
        tot = a["result"]["page"]["total"]
        for _ in range(50):
            if a["result"].get("job", {}).get("finished", True):
                break
            time.sleep(0.1)
            st, a = srv.post("/query", {"query": NOUN, "limit": 1, "total": "async", "subcorpus": FIC})
        check(a["result"]["page"]["total"] == len(S["fic"] & S["noun"]),
              f"async total under a subcorpus: {a['result']['page']['total']} vs {len(S['fic'] & S['noun'])}")
        # the result cache keeps subcorpora apart
        _, a1 = srv.post("/query", {"query": NOUN, "limit": 1, "total": True, "subcorpus": FIC})
        _, a2 = srv.post("/query", {"query": NOUN, "limit": 1, "total": True, "subcorpus": Y92})
        _, a3 = srv.post("/query", {"query": NOUN, "limit": 1, "total": True})
        check(a1["result"]["page"]["total"] == len(S["fic"] & S["noun"]) and
              a2["result"]["page"]["total"] == len(S["y92"] & S["noun"]) and
              a3["result"]["page"]["total"] == len(S["noun"]) and "subcorpus" not in a3,
              "cache keys: one query, three subcorpora")

        # 3. /run statistics
        st, a = srv.post("/session", {})
        sid = a["session_id"]

        def run(cql, **kw):
            body = {"cql": cql, "session_id": sid, "limit": 5, "group_limit": 0}
            body.update(kw)
            st, a = srv.post("/run", body)
            check(st == 200 and a.get("ok"), f"/run {cql}: {st} {a}")
            return a

        a = run(f"FIC = {FIC}; Y92 = {Y92}; EITHER = (FIC) | (Y92)")
        check(a["operation"] == "subcorpus" and a["tokens"] == len(S["fic"] | S["y92"]), f"define: {a}")
        fn = len(S["fic"] & S["noun"])
        # within: a filter, N of the corpus
        a = run(f"X = {NOUN} within FIC; freq X by upos")
        check(a["result"]["total_matches"] == fn and a["subcorpus"]["mode"] == "within", f"within total: {a}")
        whole = a["result"]["corpus_size"]
        check(whole > len(S["fic"]), "within: corpus_size is the whole corpus")
        check(abs(a["result"]["rows"][0]["ipm"] - 1e6 * fn / whole) < 0.01, "within: ipm over the corpus")
        a = run(f"X = {NOUN} not within FIC; size X")
        check(a["result"] == len(S["noun"] - S["fic"]), f"not within: {a['result']}")
        # use: the subcorpus is the corpus
        a = run(f"use FIC; X = {NOUN}; freq X by upos")
        check(a["result"]["corpus_size"] == len(S["fic"]) and a["subcorpus"]["mode"] == "use"
              and a["subcorpus"]["name"] == "FIC", f"use: N {a}")
        check(abs(a["result"]["rows"][0]["ipm"] - 1e6 * fn / len(S["fic"])) < 0.01, "use: ipm over the subcorpus")
        # the session keeps the use; within inside it: N of A, B a filter
        a = run(f"X = {NOUN} within Y92; freq X by upos")
        check(a["result"]["total_matches"] == len(S["fic"] & S["y92"] & S["noun"])
              and a["result"]["corpus_size"] == len(S["fic"]), f"use A within B: {a}")
        # freq by a region attribute: per-group sizes inside the subcorpus
        a = run(f"use EITHER; X = {NOUN}; freq X by text_genre")
        either = S["fic"] | S["y92"]
        rows = {r["key"]: r for r in a["result"]["rows"]}
        check(set(rows) <= set(GENRES) and rows and all("subcorpus_size" in r for r in rows.values()),
              f"freq by text_genre rows: {a['result']['rows']}")
        genre_pos = {g: positions_of(srv, f't:<text genre="{g}">') for g in GENRES}
        for g, r in rows.items():
            check(r["subcorpus_size"] == len(genre_pos[g] & either), f"group size {g}: {r} vs {len(genre_pos[g] & either)}")
            check(r["count"] == len(genre_pos[g] & either & S["noun"]), f"group count {g}")
        # coll: window collocates inside, the collocate's frequency in the subcorpus
        _, toks = all_hits(srv, "[]")
        lemma_at = {h["match_start"]: h["tokens"][0]["lemma"] for h in toks}
        a = run(f"use FIC; X = {NOUN}; coll X by lemma", )
        obs = {}
        for p in sorted(S["fic"] & S["noun"]):
            for q in list(range(p - 5, p)) + list(range(p + 1, p + 6)):
                if q in S["fic"] and q in lemma_at:
                    obs[lemma_at[q]] = obs.get(lemma_at[q], 0) + 1
        fic_freq = {}
        for p in S["fic"]:
            fic_freq[lemma_at[p]] = fic_freq.get(lemma_at[p], 0) + 1
        for c in a["result"]["collocates"]:
            check(c["obs"] == obs.get(c["word"]) and c["freq"] == fic_freq.get(c["word"]),
                  f"coll {c} vs obs {obs.get(c['word'])} freq {fic_freq.get(c['word'])}")
        check(len(a["result"]["collocates"]) > 0, "coll under use has collocates")
        # keyness: the rest of the subcorpus
        a = run(f"use FIC; X = [upos=\"VERB\"]; keyness X by lemma")
        verbs = len(S["fic"] & positions_of(srv, '[upos="VERB"]'))
        check(a["result"]["focus_size"] == verbs and a["result"]["ref_size"] == len(S["fic"]) - verbs,
              f"keyness sizes: {a['result']['focus_size']} {a['result']['ref_size']}")
        # dcoll (corpus baseline): the collocate's frequency in the subcorpus
        a = run(f"use FIC; X = {NOUN}; dcoll X by lemma", )
        check(a.get("ok") and a["result"]["baseline"] == "relation", "dcoll under use")
        # use; resets
        a = run(f"use; X = {NOUN}; size X")
        check(a["result"] == len(S["noun"]) and "subcorpus" not in a, f"use; resets: {a}")
        # per-request subcorpus on /run (mode use), names of the session
        a = run(f"X = {NOUN}; freq X by upos", subcorpus={"definition": "FIC", "name": "fiction"})
        check(a["result"]["corpus_size"] == len(S["fic"]) and a["subcorpus"]["name"] == "fiction", f"/run subcorpus: {a}")
        a = run(f"X = {NOUN}; freq X by upos", subcorpus={"definition": "(FIC) - (Y92)", "mode": "within"})
        check(a["result"]["total_matches"] == len((S["fic"] - S["y92"]) & S["noun"])
              and a["result"]["corpus_size"] == whole, f"/run subcorpus within: {a}")

        # 4. errors
        for body, path in [({"definition": "[upos=\"NOUN\""}, "/subcorpus"), ({"definition": "(A) &"}, "/subcorpus"),
                           ({"definition": "NOSUCH"}, "/subcorpus"),
                           ({"query": NOUN, "subcorpus": {"definition": FIC, "mode": "inside"}}, "/query"),
                           ({"query": NOUN, "subcorpus": {"definition": "[]; count by lemma"}}, "/query"),
                           ({"cql": "use NOSUCH"}, "/run")]:
            st, a = srv.post(path, body)
            check(st == 400 and not a.get("ok") and a.get("error"), f"error {path} {body}: {st} {a}")

        # 5. the CLI
        out = subprocess.run([o.pando, idx, "--subcorpus", FIC, "--subcorpus-name", "f", f"X = {NOUN}; size X"],
                             capture_output=True, text=True).stdout
        a = json.loads(out)
        check(a["result"] == fn and a["subcorpus"]["name"] == "f", f"CLI --subcorpus: {out}")
        out = subprocess.run([o.pando, idx, f"use ({FIC}); X = {NOUN}; size X"], capture_output=True, text=True).stdout
        check(json.loads(out)["result"] == fn, f"CLI use: {out}")
    finally:
        for sv in servers.values():
            sv.stop()
    if failures:
        print(f"{len(failures)} failure(s)")
        return 1
    print("subcorpus: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
