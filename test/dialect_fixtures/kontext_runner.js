// Run the dialect fixtures against a KonText instance (Manatee backend) from the
// browser: open any page of the KonText site, paste this file into the console
// (or an automation tool), then
//
//   const out = await runFixtures("ud_ewt_manatee", QUERIES);   // [[id, cql], ...]
//   copy(out.join("\n"));    // -> test/dialect_fixtures/expected/manatee-<corpus>.jsonl
//
// Uses KonText's own concordance endpoint (/view?…&format=json), i.e. exactly what
// KonText shows: total = concsize, hits = (toknum, toknum + kwiclen - 1).
// Records have the same fields as fixtures.py (sha256 of the sorted distinct
// "match matchend\n" pairs, first 50 pairs as `head`); no `window`.

async function kontextHits(corpname, cql, pagesize = 10000, maxHits = 500000) {
  const base = `/view?corpname=${encodeURIComponent(corpname)}&q=${encodeURIComponent("q" + cql)}` +
               `&format=json&attrs=word&viewmode=kwic&pagesize=${pagesize}`;
  const first = await (await fetch(base + "&fromp=1", { credentials: "include" })).json();
  const errs = (first.messages || []).filter(m => m[0] === "error");
  if (errs.length || first.concsize === undefined) {
    return { error: errs.map(m => m[1]).join("; ") || "no concsize in response" };
  }
  const total = first.concsize;
  if (total > maxHits) return { total, pairs: null };
  const pairs = [];
  const take = j => { for (const l of j.Lines || []) pairs.push([l.toknum, l.toknum + l.kwiclen - 1]); };
  take(first);
  for (let p = 2; (p - 1) * pagesize < total; p++) {
    take(await (await fetch(base + "&fromp=" + p, { credentials: "include" })).json());
  }
  return { total, pairs, finished: first.finished };
}

async function sha256hex(s) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return Array.from(new Uint8Array(d)).map(b => b.toString(16).padStart(2, "0")).join("");
}

async function runFixtures(corpname, queries, opts = {}) {
  const out = [JSON.stringify({ meta: { engine: "manatee", via: "kontext " + location.origin,
                                        corpus: corpname, window: null, head: 50,
                                        created: new Date().toISOString() } })];
  for (const [id, q] of queries) {
    const t0 = performance.now();
    let rec;
    try {
      const r = await kontextHits(corpname, q, opts.pagesize, opts.maxHits);
      if (r.error) {
        rec = { id, query: q, engine: "manatee", total: null, unique: null, sha256: null,
                head: null, window: null, error: r.error };
      } else if (!r.pairs) {
        rec = { id, query: q, engine: "manatee", total: r.total, unique: null, sha256: null,
                head: null, window: null, error: null };
      } else {
        const key = p => p[0] * 4294967296 + p[1];
        const uniq = Array.from(new Map(r.pairs.map(p => [key(p), p])).values())
                          .sort((a, b) => a[0] - b[0] || a[1] - b[1]);
        rec = { id, query: q, engine: "manatee", total: r.total, unique: uniq.length,
                sha256: await sha256hex(uniq.map(p => `${p[0]} ${p[1]}\n`).join("")),
                head: uniq.slice(0, 50), window: null, error: null,
                listed: r.pairs.length, finished: r.finished };
      }
    } catch (e) {
      rec = { id, query: q, engine: "manatee", total: null, unique: null, sha256: null,
              head: null, window: null, error: String(e) };
    }
    rec.seconds = Math.round(performance.now() - t0) / 1000;
    out.push(JSON.stringify(rec));
  }
  return out;
}
