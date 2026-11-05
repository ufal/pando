#!/usr/bin/env python3
"""`dcoll` scores compare with totals in the same space as the observed count (src/api/dcoll_totals.h):
the collocate's frequency as that kind of relative, out of all such pairs (`baseline: relation`, the default),
or its corpus frequency out of the corpus size (`set dcoll-baseline corpus`, pando before 0.4.5).
References straight from the CoNLL-U; the totals pando-index stores (<attr>.dcoll) and those counted without them,
CLI JSON and pando-server /run must agree.

  test/dcoll_totals_test.py --pando build/pando --pando-index build/pando-index \\
      --server build/pando-server --conllu test/data/sample.conllu
"""
import argparse, json, math, os, socket, subprocess, sys, tempfile, time, urllib.request
from collections import Counter


def read_conllu(path):
    """sentences as lists of (lemma, deprel, head index or None), ids 0-based"""
    sents = []
    for block in open(path, encoding="utf-8").read().split("\n\n"):
        toks = []
        for line in block.splitlines():
            if not line or line.startswith("#"):
                continue
            f = line.split("\t")
            if "-" in f[0] or "." in f[0]:
                continue
            toks.append((f[2], f[7], None if f[6] in ("0", "_") else int(f[6]) - 1))
        if toks:
            sents.append(toks)
    return sents


def references(sents):
    """per kind of pair: Counter of the collocate's lemma, as the relative"""
    ref = {"children": Counter(), "head": Counter(), "descendants": Counter(), "amod": Counter(),
           "corpus": Counter()}
    for toks in sents:
        for i, (lemma, rel, h) in enumerate(toks):
            ref["corpus"][lemma] += 1
            if h is None:
                continue
            ref["children"][lemma] += 1
            if h != i:
                ref["head"][toks[h][0]] += 1
            if rel == "amod":
                ref["amod"][lemma] += 1
            depth, p = 0, h
            while p is not None and p != i and depth < 4096:
                depth += 1
                p = toks[p][2]
            ref["descendants"][lemma] += depth
    return ref


def logdice(obs, f_node, f_coll):
    return 14 + math.log2(2 * obs / (f_node + f_coll)) if obs else 0.0


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser()
    for a in ("--pando", "--pando-index", "--server", "--conllu"):
        ap.add_argument(a, required=True)
    o = ap.parse_args()
    ref = references(read_conllu(o.conllu))
    fails = []

    def check(label, r, kind, baseline):
        if r.get("baseline") != baseline:
            fails.append(f"{label}: baseline {r.get('baseline')!r}")
            return
        f_node = None
        if baseline == "relation":
            want_n = sum(ref[kind].values())
            if r.get("relation_total") != want_n:
                fails.append(f"{label}: relation_total {r.get('relation_total')} != {want_n}")
        elif "relation_total" in r:
            fails.append(f"{label}: relation_total in a corpus-baseline answer")
        if not r["collocates"]:
            fails.append(f"{label}: no collocates")
        for c in r["collocates"]:
            w = c["word"]
            if c["freq"] != ref["corpus"][w]:
                fails.append(f"{label} {w}: freq {c['freq']} != corpus {ref['corpus'][w]}")
            f_coll = ref[kind][w] if baseline == "relation" else ref["corpus"][w]
            if baseline == "relation" and c.get("relation_freq") != f_coll:
                fails.append(f"{label} {w}: relation_freq {c.get('relation_freq')} != {f_coll}")
            if baseline == "corpus" and "relation_freq" in c:
                fails.append(f"{label} {w}: relation_freq in a corpus-baseline answer")
            # the node total is not in the answer: it is the logDice of the top collocate solved for f_node
            if f_node is None:
                f_node = 2 * c["obs"] / 2 ** (c["logdice"] - 14) - f_coll
            want = logdice(c["obs"], f_node, f_coll)
            if abs(c["logdice"] - want) > 0.01:
                fails.append(f"{label} {w}: logdice {c['logdice']} != {want:.3f}")

    cases = [("children", "dcoll children by lemma"), ("amod", "dcoll amod by lemma"),
             ("head", "dcoll head by lemma"), ("descendants", "dcoll descendants by lemma")]
    with tempfile.TemporaryDirectory() as tmp:
        idx = os.path.join(tmp, "idx")
        subprocess.run([o.pando_index, o.conllu, idx], check=True, capture_output=True)

        def cli(prog, *flags):
            out = subprocess.run([o.pando, "--json", *flags, idx, prog], capture_output=True, text=True)
            return json.loads(out.stdout)

        # pando-index stored the totals (<attr>.dcoll); without the files dcoll counts them, with the same answers
        stored = sorted(f for f in os.listdir(idx) if f.endswith(".dcoll"))
        if "lemma.dcoll" not in stored:
            fails.append(f"pando-index wrote no lemma.dcoll ({stored})")
        for source in ("stored", "counted"):
            if source == "counted":
                for f in stored:
                    os.remove(os.path.join(idx, f))
            for kind, cmd in cases:
                node = '[upos="NOUN"]' if kind != "head" else '[upos="ADJ"]'
                check(f"cli {source} {cmd}", cli(f"set min-freq 1; {node}; {cmd}")["result"], kind, "relation")
        # pando-index --upgrade writes them again
        subprocess.run([o.pando_index, "--upgrade", idx], check=True, capture_output=True)
        if not os.path.exists(os.path.join(idx, "lemma.dcoll")):
            fails.append("pando-index --upgrade wrote no lemma.dcoll")
        check("cli corpus", cli('set min-freq 1; set dcoll-baseline corpus; [upos="NOUN"]; dcoll amod by lemma')
              ["result"], "amod", "corpus")
        check("cli --dcoll-baseline corpus",
              cli('set min-freq 1; [upos="NOUN"]; dcoll amod by lemma', "--dcoll-baseline", "corpus")["result"],
              "amod", "corpus")
        bad = subprocess.run([o.pando, idx, "set dcoll-baseline nonsense"], capture_output=True, text=True)
        if "relation or corpus" not in bad.stderr:
            fails.append(f"cli: a wrong baseline is not refused ({bad.stderr!r})")

        port = free_port()
        srv = subprocess.Popen([o.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            url = f"http://127.0.0.1:{port}"
            for _ in range(100):
                try:
                    urllib.request.urlopen(url + "/health", timeout=1)
                    break
                except Exception:
                    time.sleep(0.1)

            def run(prog):
                req = urllib.request.Request(url + "/run", data=json.dumps({"cql": prog}).encode(),
                                             headers={"Content-Type": "application/json"})
                return json.loads(urllib.request.urlopen(req, timeout=30).read())

            for kind, cmd in cases:
                node = '[upos="NOUN"]' if kind != "head" else '[upos="ADJ"]'
                check(f"server {cmd}", run(f"set min-freq 1; {node}; {cmd}")["result"], kind, "relation")
            check("server corpus", run('set min-freq 1; set dcoll-baseline corpus; [upos="NOUN"]; dcoll amod by lemma')
                  ["result"], "amod", "corpus")
            bad = run("set dcoll-baseline nonsense")
            if bad.get("ok") is not False:
                fails.append(f"server: a wrong baseline is not refused ({bad})")
        finally:
            srv.terminate()
            srv.wait()

    for f in fails:
        print("FAIL", f)
    print("dcoll_totals:", "FAILED" if fails else "ok")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
