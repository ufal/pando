#!/usr/bin/env python3
"""Geolocation functions: lat(), lon(), geodist() in `::` filters and as fields, geocell() as a
count / tabulate field; location values in the accepted formats (lat,lon; lat lon; geo: URI; WKT
POINT(lon lat)); values that are not locations give no value. CLI and pando-server /run.

  test/geo_functions_test.py --pando build/pando --pando-index build/pando-index --server build/pando-server
"""
import argparse, json, os, socket, subprocess, sys, tempfile, time, urllib.request

FAILS = []
def check(cond, msg):
    if not cond:
        FAILS.append(msg)
        print("FAIL:", msg, flush=True)

TEXTS = [("prague", "50.087,14.421"), ("vienna", "geo:48.2082,16.3738;u=10"), ("berlin", "POINT(13.405 52.52)"),
         ("london", "51.5074 -0.1278"), ("nowhere", "somewhere"), ("bad", "95,10")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pando", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--server", required=True)
    a = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="pando-geo-")
    vrt, idx = os.path.join(tmp, "c.vrt"), os.path.join(tmp, "idx")
    with open(vrt, "w") as out:
        out.write("<!-- positional-attributes: word -->\n")
        for tid, g in TEXTS:
            out.write(f'<text id="{tid}" geo="{g}">\n<s>\nx\ny\n</s>\n</text>\n')
    subprocess.run([a.pando_index, "--format", "vertical", vrt, idx], check=True, capture_output=True)

    def cli(q):
        r = subprocess.run([a.pando, idx, q], capture_output=True, text=True)
        return (r.stdout + r.stderr).rstrip("\n").splitlines()

    rows = [l.split("\t") for l in cli('a:[word="x"]; tabulate Last 0 10 a.text_id, lat(a.text_geo), lon(a.text_geo), '
                                       'geodist(a.text_geo, 50.087, 14.421), geocell(a.text_geo, 0.5)')[1:]]
    got = {r[0]: r[1:] for r in rows}
    check(got.get("prague") == ["50.087", "14.421", "0", "50.25,14.25"], f"prague: {got.get('prague')}")
    check(got.get("vienna", [""])[0] == "48.2082" and abs(float(got["vienna"][2]) - 252.6) < 0.5, f"geo: URI, distance: {got.get('vienna')}")
    check(got.get("berlin", [""])[:2] == ["52.52", "13.405"], f"WKT is lon lat: {got.get('berlin')}")
    check(got.get("london", [""])[1] == "-0.1278", f"space separated, negative: {got.get('london')}")
    check(got.get("nowhere") == ["", "", "", ""] and got.get("bad") == ["", "", "", ""], f"no location: {got.get('nowhere')} {got.get('bad')}")

    cells = {l.split("\t")[0]: l.split("\t")[1] for l in cli('[word="x"]; count by geocell(text_geo, 5)')[1:] if "\t" in l}
    check(cells.get("52.5,12.5") == "2", f"Prague and Berlin share a 5° cell: {cells}")

    def ids(q):
        return sorted(l for l in cli(q + "; tabulate Last 0 10 a.text_id")[1:])
    check(ids('a:[word="x"] :: geodist(a.text_geo, 50.087, 14.421) < 300') == ["berlin", "prague", "vienna"], "geodist filter")
    check(ids('a:[word="x"] :: lat(a.text_geo) > 50.5 & lon(a.text_geo) < 13.5') == ["berlin", "london"], "decimal comparisons")
    check(ids('a:[word="x"] :: lon(text_geo) < 0') == ["london"], "region attribute directly")
    check(ids('a:[word="x"] :: geodist(a.text_geo, a.text_geo) = 0') == ["berlin", "london", "prague", "vienna"], "two locations")
    check(any("not a number" in l for l in cli('a:[word="x"] :: geocell(a.text_geo, 1) = 3')), "geocell in a filter: error")
    check(any("needed, not a number" in l for l in cli('a:[word="x"] :: geodist(a.text_geo, 50.087) < 3')), "geodist arguments: error")

    # pando-server: the same through /run (the route flexicorp and FQS use)
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    p = subprocess.Popen([a.server, idx, str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        base = f"http://127.0.0.1:{port}"
        for _ in range(100):
            try:
                urllib.request.urlopen(base + "/health", timeout=2); break
            except Exception:
                time.sleep(0.05)
        req = urllib.request.Request(base + "/run", data=json.dumps({"query": '[word="x"] :: geodist(text_geo, 50.087, 14.421) < 300; count by geocell(text_geo, 5)'}).encode(),
                                     headers={"Content-Type": "application/json"})
        res = json.loads(urllib.request.urlopen(req, timeout=30).read())
        txt = json.dumps(res)
        check('"52.5,12.5"' in txt and '"47.5,17.5"' in txt and '"52.5,-2.5"' not in txt, f"server: {txt[:300]}")
    finally:
        p.kill(); p.wait()

    print("geo_functions:", "FAIL" if FAILS else "ok")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
