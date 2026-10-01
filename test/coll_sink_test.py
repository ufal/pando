#!/usr/bin/env python3
"""P7.8: `coll` / `dcoll` counted in the executor (hit sink) and `tabulate` run for its
page only must give what counting over materialised hits gives; P7.10: `sort` per
sort key (pages re-run, no hits kept) must give the materialised sort's order.

On test/data/sample.conllu:

  * CLI, text and --json: each `query; coll|dcoll …` with the fast paths on (the
    sink) and off (every hit materialised, then counted);
  * pando-server /run: the same, one server per mode;
  * tabulate: `query; tabulate o n fields` (only the first o+n hits) against the
    named set `x = query; tabulate x o n fields` (all hits), CLI and /run;
  * sort: `query; sort by f[; tabulate o n …]` (CLI, per key) against the named set
    (materialised and sorted), and /run `x = query; sort x by f` + tabulate / /query
    pages against `x = query; raw x; sort x by f` (kept, then sorted).

  test/coll_sink_test.py --pando build/pando --pando-index build/pando-index \\
      --server build/pando-server --conllu test/data/sample.conllu
"""
import argparse, json, os, subprocess, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import Server  # noqa: E402

FAILS = []


def check(c, msg):
    if not c:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)


COLL = [
    '[upos="ADJ"] [upos="NOUN"]; coll by lemma',
    'a:[upos="ADJ"] b:[upos="NOUN"]; coll on b by lemma',
    'a:[upos="DET"] []{0,2} b:[upos="NOUN"]; coll on a by form',
    '[upos="ADJ"]? [upos="NOUN"]; coll by lemma',
    '[upos="PROPN"|upos="NOUN"]+; coll by lemma',
    'a:[upos="VERB"] b:[]; coll on nosuch by lemma',
    'a:[upos="VERB"] > b:[upos="NOUN"]; coll on b by lemma',
    '[upos="NOUN"] within s; coll by form',
    '[upos="VERB"]; dcoll obj by lemma',
    '[upos="NOUN"]; dcoll by lemma',
    '[upos="NOUN"]; dcoll head, amod by form',
    '[upos="NOUN"]; dcoll descendants by lemma',
    'a:[upos="VERB"] > b:[upos="NOUN"]; dcoll on b head by lemma',
    '[upos="VERB"]; dcoll on nosuch obj, nsubj by lemma',
]
SORT = [
    ('[upos="NOUN"]', 'lemma', '3 6 match.lemma, match.form'),
    ('[upos="NOUN"]', 'form', '0 300 match.form'),
    ('a:[upos="ADJ"] b:[upos="NOUN"]', 'b.lemma, a.lemma', '0 9 a.lemma, b.lemma'),
    ('a:[upos="VERB"] > b:[upos="NOUN"]', 'b.form', '2 7 a.form, b.form'),
    ('[upos="NOUN"]', 'text_lang', '0 12 match.form, text_lang'),
    ('[upos="DET"] []{0,2} [upos="NOUN"]', 'lemma', '1 8 match.lemma'),
    ('[upos="ADJ"]+ [upos="NOUN"]', 'lemma', '0 5 match.lemma'),
    ('<s> [upos="DET"]', 'lemma', '0 5 match.lemma'),         # anchored: materialised
    ('[upos="NOUN"]', 'feats/Number', '0 4 match.form'),      # keys the index does not cover
    ('[upos="NOUN"]', 'lemma, form, upos', '0 4 match.form'),
]
TAB = [
    ('[upos="NOUN"]', '3 5 match.form, match.lemma'),
    ('a:[upos="ADJ"] b:[upos="NOUN"]', '0 4 a.lemma, b.lemma'),
    ('a:[upos="VERB"] > b:[upos="NOUN"]', '2 3 a.form, b.form'),
    ('[upos="NOUN"]', '100000 3 match.form'),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    ap.add_argument("--conllu", required=True)
    opts = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([opts.pando_index, opts.conllu, idx], check=True, capture_output=True)

        def cli(q, mode, *args):
            env = dict(os.environ, PANDO_FASTPATH=mode)
            p = subprocess.run([opts.pando, *args, idx, q], capture_output=True, text=True, env=env)
            return p.returncode, p.stdout

        for q in COLL:
            for args in ((), ("--json",)):
                on, off = cli(q, "on", *args), cli(q, "off", *args)
                check(on == off, f"CLI {' '.join(args)} {q}: sink {on[1][:200]!r} != materialised {off[1][:200]!r}")
                check(on[0] == 0 and (on[1].count("\n") > 1 or "nosuch" in q),
                      f"CLI {q}: no result: {on[1][:200]!r}")

        def tab_rows(text):
            return json.loads(text)["result"]["rows"]

        for q, t in TAB:
            page = cli(f"{q}; tabulate {t}", "on", "--json")
            full = cli(f"x = {q}; tabulate x {t}", "on", "--json")
            check(page[0] == 0 and full[0] == 0, f"CLI tabulate {q}: failed")
            if page[0] == 0 and full[0] == 0:
                p, f = json.loads(page[1])["result"], json.loads(full[1])["result"]
                check(p == f, f"CLI tabulate {q} {t}: page {p} != all hits {f}")

        for q, f, t in SORT:
            for args in (("--api",), ("--api", "--offset", "4", "--limit", "5")):
                lazy = cli(f"{q}; sort by {f}", "on", *args)
                kept = cli(f"x = {q}; sort x by {f}", "on", *args)
                check(lazy[0] == 0 and lazy == kept, f"CLI {args} {q}; sort by {f}: {lazy[1][:300]!r} != {kept[1][:300]!r}")
            lazy = cli(f"{q}; sort by {f}; tabulate {t}", "on", "--api")
            kept = cli(f"x = {q}; sort x by {f}; tabulate x {t}", "on", "--api")
            check(lazy[0] == 0 and lazy == kept, f"CLI {q}; sort by {f}; tabulate: {lazy[1][:300]!r} != {kept[1][:300]!r}")

        servers = {m: Server(opts.server, idx, env={"PANDO_FASTPATH": m}) for m in ("on", "off")}
        try:
            for q in COLL:
                r = {m: s.req("POST", "/run", {"cql": q}) for m, s in servers.items()}
                check(r["on"] == r["off"], f"/run {q}: sink {str(r['on'])[:200]} != materialised {str(r['off'])[:200]}")
                check(r["on"][0] == 200 and r["on"][1].get("ok"), f"/run {q}: {str(r['on'])[:200]}")
            s = servers["on"]
            for q, t in TAB:
                sp, p = s.req("POST", "/run", {"cql": f"{q}; tabulate {t}"})
                # a kept set (`raw` materialises it) is paged from its hits
                sf, f = s.req("POST", "/run", {"cql": f"x = {q}; raw x; tabulate x {t}"})
                ok = sp == 200 and sf == 200 and p.get("ok") and f.get("ok")
                check(ok, f"/run tabulate {q}: {p} / {f}")
                if ok:
                    check(p["result"]["rows"] == f["result"]["rows"]
                          and p["result"]["total_matches"] == f["result"]["total_matches"],
                          f"/run tabulate {q} {t}: page {p['result']} != all hits {f['result']}")
            for q, f, t in SORT:
                lz = s.req("POST", "/run", {"cql": f"x = {q}; sort x by {f}; tabulate x {t}"})
                kp = s.req("POST", "/run", {"cql": f"x = {q}; raw x; sort x by {f}; tabulate x {t}"})
                check(lz[0] == 200 and lz == kp, f"/run sort {q} by {f}: {str(lz)[:300]} != {str(kp)[:300]}")
                pages = {}
                for kind, cql in (("lazy", f"sort S by {f}"), ("kept", f"raw S; sort S by {f}")):
                    sid = s.session()
                    s.post("/query", {"session_id": sid, "name": "S", "query": q, "limit": 1, "total": True})
                    first = s.post("/run", {"session_id": sid, "cql": cql, "offset": 1, "limit": 3})
                    rest = [s.post("/query", {"session_id": sid, "from": "S", "offset": o, "limit": 4})
                            for o in (0, 5, 30, 1000)]
                    pages[kind] = json.dumps([first] + rest, sort_keys=True).replace(sid, "SID")
                check(pages["lazy"] == pages["kept"], f"/query pages of sorted {q} by {f} differ: "
                      f"{pages['lazy'][:300]} != {pages['kept'][:300]}")
        finally:
            for srv in servers.values():
                srv.close()
    if FAILS:
        print(f"{len(FAILS)} failure(s)")
        sys.exit(1)
    print("PASS coll_sink")


if __name__ == "__main__":
    main()
