#!/usr/bin/env python3
"""Persistent names: `:: eng.s_tuid = nld.s_tuid` with `eng` bound by an earlier statement.

On test/data/aligned.conllu (English / Dutch, sentence tuid u1..u3 — one Dutch sentence
has the multivalue `u3|u9` — a `_` sentence per language, token tuids u1.2 / u3.2 on
library / bibliotheek) checks, in the CLI (every fast-path mode) and in pando-server /run
(one request; a later request of the same session; after a /query with a name):

  * sentence alignment (region attribute, multivalue components, `_` never aligns);
  * token alignment (positional attribute);
  * the earlier statement's hits are used in full, not only its first page;
  * named sets (`DutchWords = nld:[…] :: …; count DutchWords by nld.form`), either
    operand order, `freq`, and a chain (a third statement aligned with the second);
  * an unknown name still errors.

  test/persistent_names_test.py --pando build/pando --pando-index build/pando-index \\
      --server build/pando-server --conllu test/data/aligned.conllu
"""
import argparse, os, subprocess, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import Server  # noqa: E402

FAILS = []


def check(c, msg):
    if not c:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)


def rows_of(text):
    """`count by` / `freq by` text output → {key: count}"""
    out = {}
    for line in text.splitlines()[1:]:
        if line.startswith("Total:") or not line.strip():
            continue
        parts = line.split("\t")
        out[parts[0]] = int(parts[1])
    return out


SENT = {".": 2, "bibliotheek": 2, "de": 2, "gaan": 1, "open": 1, "oud": 1, "zijn": 1}
CASES = [
    ('eng:[lemma="library"] :: match.text_lang="English"; '
     'nld:[] :: match.text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma', SENT),
    ('eng:[lemma="library"]; nld:[] :: text_lang="Dutch" & nld.s_tuid = eng.s_tuid; count by nld.lemma', SENT),
    ('eng:[lemma="library"] :: text_lang="English"; nld:[] :: text_lang="Dutch" & eng.tuid = nld.tuid; '
     'count by nld.lemma', {"bibliotheek": 2}),
    ('eng:[lemma="library"]; DutchWords = nld:[upos="NOUN"] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; '
     'count DutchWords by nld.form', {"bibliotheek": 2}),
    ('eng:[lemma="park"]; nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma',
     {"ik": 1, "houden": 1, "van": 1, "het": 1, "park": 1, ".": 1}),
    # chain: English aligned with the Dutch nouns of the aligned sentences
    ('eng:[lemma="library"]; nld:[upos="NOUN"] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; '
     'back:[upos="VERB"|upos="AUX"] :: text_lang="English" & nld.s_tuid = back.s_tuid; count by back.lemma',
     {"be": 1, "open": 1}),
    # `_` never aligns: the English `_` sentence has library too, the Dutch `_` one bibliotheek
    ('eng:[lemma="library" & s_tuid="_"]; nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; '
     'count by nld.lemma', {}),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server")
    ap.add_argument("--conllu", required=True)
    opts = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([opts.pando_index, opts.conllu, idx], check=True, capture_output=True)

        def cli(q, *args, mode="on"):
            env = dict(os.environ, PANDO_FASTPATH=mode)
            p = subprocess.run([opts.pando, idx, q, *args], capture_output=True, text=True, env=env)
            return p.returncode, p.stdout, p.stderr

        for q, want in CASES:
            for mode in ("on", "nomerge", "off"):
                rc, out, err = cli(q, mode=mode)
                got = rows_of(out)
                check(rc == 0 and got == want, f"[{mode}] {q}\n   got {got} {err.strip()[:200]}\n  want {want}")
        # the earlier statement's hits in full: a first page of 1 hit still aligns both sentences
        rc, out, err = cli(CASES[0][0], "--limit", "1")
        check(rows_of(out) == SENT, f"--limit 1 prelude: {rows_of(out)}")
        # freq
        rc, out, err = cli(CASES[0][0].replace("count by", "freq by"))
        check(rc == 0 and rows_of(out) == SENT, f"freq: {out[:200]} {err[:200]}")
        # concordance of the aligned statement
        rc, out, err = cli('eng:[lemma="library"]; nld:[lemma="bibliotheek"] :: eng.s_tuid = nld.s_tuid',
                           "--dump-matches")
        check(rc == 0 and out.startswith("total=2 "), f"aligned concordance: {out[:100]} {err[:200]}")
        # still an error: a name nobody binds
        rc, out, err = cli('nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma')
        check(rc != 0 or "unknown name 'eng'" in err + out, f"unknown name: rc={rc} {err[:200]}")

        if opts.server:
            srv = Server(opts.server, idx)
            try:
                def run(cql, **kw):
                    body = {"cql": cql}
                    body.update(kw)
                    return srv.post("/run", body)

                def rows(r):
                    return {x["key"]: x["count"] for x in r.get("result", {}).get("rows", [])}

                for q, want in CASES:
                    st, r = run(q)
                    check(st == 200 and rows(r) == want, f"/run {q}: {st} {rows(r)} {str(r)[:200]}")
                sid = srv.session()
                run('eng:[lemma="library"] :: text_lang="English"', session_id=sid)
                st, r = run('nld:[] :: text_lang="Dutch" & eng.s_tuid = nld.s_tuid; count by nld.lemma',
                            session_id=sid)
                check(st == 200 and rows(r) == SENT, f"/run, earlier request: {st} {rows(r)}")
                srv.post("/query", {"query": 'p:[lemma="park"]', "session_id": sid, "name": "P", "limit": 1})
                st, r = run('nld:[] :: text_lang="Dutch" & p.s_tuid = nld.s_tuid; count by nld.lemma',
                            session_id=sid)
                check(st == 200 and rows(r) == CASES[4][1], f"/run after /query: {st} {rows(r)}")
                sid2 = srv.session()
                st, r = run('nld:[] :: text_lang="Dutch" & nobody.s_tuid = nld.s_tuid; count by nld.lemma',
                            session_id=sid2)
                check(st >= 400, f"/run unknown name: {st}")
            finally:
                srv.close()
    print(f"{'OK' if not FAILS else 'FAILED'}: {len(FAILS)} failures")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
