#!/usr/bin/env python3
"""The embedded-server C ABI (src/api/server_capi.h) loaded at run time, the way
FQS (Rust libloading) or PHP FFI would: dlopen libpando, check the ABI version,
open a corpus, send requests, poll a background total, close.

  cmake -DPANDO_BUILD_SHARED=ON ... && ninja libpando
  test/server_capi_ctypes.py --lib build-shared/libpando.so --corpus <index dir>
"""

import argparse
import ctypes
import json
import sys
import time


def load(path):
    lib = ctypes.CDLL(path)
    lib.pando_server_abi_version.restype = ctypes.c_int
    lib.pando_server_build_string.restype = ctypes.c_char_p
    lib.pando_server_build_json.restype = ctypes.c_char_p
    lib.pando_server_open.restype = ctypes.c_void_p
    lib.pando_server_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.pando_server_request.restype = ctypes.c_void_p          # char*, freed with pando_server_free
    lib.pando_server_request.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
                                         ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_int)]
    lib.pando_server_busy.restype = ctypes.c_size_t
    lib.pando_server_busy.argtypes = [ctypes.c_void_p]
    lib.pando_server_close.argtypes = [ctypes.c_void_p]
    lib.pando_server_free.argtypes = [ctypes.c_void_p]
    return lib


def request(lib, h, method, path, body=None, qs=None):
    status = ctypes.c_int(0)
    b = json.dumps(body).encode() if body is not None else None
    p = lib.pando_server_request(h, method.encode(), path.encode(), qs.encode() if qs else None,
                                 b, len(b) if b else 0, ctypes.byref(status))
    text = ctypes.string_at(p).decode()
    lib.pando_server_free(p)
    return status.value, json.loads(text)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lib", required=True)
    ap.add_argument("--corpus", required=True)
    a = ap.parse_args()
    lib = load(a.lib)
    fails = []

    def check(c, msg):
        if not c:
            fails.append(msg)
            print("FAIL:", msg)

    check(lib.pando_server_abi_version() == 1, "abi version")
    info = json.loads(lib.pando_server_build_json())
    print("loaded", lib.pando_server_build_string().decode(), "abi", info["abi_version"])

    err = ctypes.c_void_p()
    check(not lib.pando_server_open(b"/nonexistent", None, ctypes.byref(err)) and err.value, "open failure")
    if err.value:
        lib.pando_server_free(err)

    h = lib.pando_server_open(a.corpus.encode(), json.dumps({"embedded_in": "ctypes-test"}).encode(), None)
    check(h, "open")
    st, health = request(lib, h, "GET", "/health")
    check(st == 200 and health.get("embedded_in") == "ctypes-test", f"/health {health}")
    t0 = time.monotonic()
    st, r = request(lib, h, "POST", "/query", {"query": '[upos="NOUN"]', "limit": 3, "total": "async"})
    page_ms = (time.monotonic() - t0) * 1000
    check(st == 200 and r["result"]["page"]["returned"] == 3, f"/query {st}")
    job = r["result"]["job_id"]
    for _ in range(200):
        st, s = request(lib, h, "GET", "/status", qs=f"job={job}")
        if s["result"]["finished"]:
            break
        time.sleep(0.02)
    check(s["result"]["finished"] and s["result"]["total"] > 0, f"/status {s}")
    print(f"page {page_ms:.1f} ms, total {s['result']['total']}")
    st, r = request(lib, h, "POST", "/run", {"cql": '[upos="NOUN"]; count by lemma;', "group_limit": 3})
    check(st == 200 and r.get("ok"), f"/run {st}")
    st, r = request(lib, h, "GET", "/nope")
    check(st == 404 and not r["ok"], f"unknown route {st}")
    check(lib.pando_server_busy(h) == 0, "busy after the job finished")
    lib.pando_server_close(h)
    print("server_capi_ctypes:", "FAILED" if fails else "all passed")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
