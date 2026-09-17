#!/usr/bin/env python3
"""Zpoint tests: REST client against a local mock (http), crypto, mux.
Property of the @ily_bio research channel (@iliyahsatam).
Run: python3 tests/test_rest.py
"""
import http.server
import json
import sys
import threading

sys.path.insert(0, "/root/projects/zpoint")
from core.rtdb import RTDB  # noqa: E402

store = {}


class Mock(http.server.BaseHTTPRequestHandler):
    def _c(self, code=200, body=b"{}"):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        n = int(self.headers.get("Content-Length", 0))
        v = json.loads(self.rfile.read(n))
        store[self.path.split("?")[0]] = v
        self._c(200, json.dumps(v).encode())

    def do_GET(self):
        v = store.get(self.path.split("?")[0])
        b = json.dumps(v).encode() if v is not None else b"null"
        self._c(200, b)

    def do_DELETE(self):
        store.pop(self.path.split("?")[0], None)
        self._c(200, b'"1"')

    def log_message(self, *a):
        pass


class HTTPRTDB(RTDB):
    """http:// variant for the local mock (SSL check bypassed)."""

    def __init__(self, url, auth=""):
        # bypass the https check but keep every hardening attribute
        self.base = url.rstrip("/")
        self.auth = auth
        self._put_base = None
        self._base_lock = threading.Lock()


def main():
    srv = http.server.HTTPServer(("127.0.0.1", 18599), Mock)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    db = HTTPRTDB("http://127.0.0.1:18599", "tok")
    assert db.put("z/t/1", {"i": 1, "b": "AA=="}) is True
    assert db.get("z/t/1") == {"i": 1, "b": "AA=="}
    assert db.delete("z/t/1") is True
    assert db.get("z/t/1") is None
    print("REST PUT/GET/DELETE OK")

    # multiple keys
    for i in range(5):
        db.put(f"z/bulk/{i}", {"i": i})
    for i in range(5):
        assert db.get(f"z/bulk/{i}")["i"] == i
    print("bulk write/read OK")

    srv.shutdown()
    print("ALL REST TESTS PASSED ✅")


if __name__ == "__main__":
    main()
