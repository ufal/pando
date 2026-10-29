#!/usr/bin/env python3
"""pando-server and its port: a port another server listens on is an error.

httplib's default socket options set SO_REUSEPORT, so a second pando-server on a busy
port used to start without complaint while the old one kept answering every request
(an old build's parse errors looked like the new build's). Checks:

  * a second server on the port of a running one exits non-zero with "Failed to bind",
    and the first one still answers;
  * a server restarted on the port its predecessor just served from comes up
    (SO_REUSEADDR: the closed connections' TIME_WAIT does not block it).

  test/server_port_test.py --server build/pando-server --pando-index build/pando-index \\
      --conllu test/data/sample.conllu
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server_sessions_test import check, FAILS, free_port  # noqa: E402


def start(exe, corpus, port):
    return subprocess.Popen([exe, corpus, str(port), "2"], stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE, text=True)


def healthy(port, tries=200):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as f:
                if f.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--pando-index", required=True)
    ap.add_argument("--conllu", required=True)
    opts = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        corpus = os.path.join(tmp, "idx")
        subprocess.run([opts.pando_index, opts.conllu, corpus], check=True, capture_output=True)
        port = free_port()

        first = start(opts.server, corpus, port)
        try:
            if not check(healthy(port), "the first server did not start"):
                return 1
            second = start(opts.server, corpus, port)
            try:
                rc = second.wait(timeout=30)
                err = second.stderr.read()
            except subprocess.TimeoutExpired:
                stop(second)
                rc, err = None, ""
            check(rc not in (None, 0), f"a second server on a busy port kept running (exit {rc})")
            check("Failed to bind" in err, f"a second server on a busy port: no bind error ({err.strip()!r})")
            check(healthy(port, 20), "the first server stopped answering")
        finally:
            stop(first)

        # restarted on the same port right away (connections of the first one in TIME_WAIT)
        again = start(opts.server, corpus, port)
        try:
            check(healthy(port), "a restart on the port just freed did not come up")
        finally:
            stop(again)

    if FAILS:
        print(f"{len(FAILS)} failure(s)")
        return 1
    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
