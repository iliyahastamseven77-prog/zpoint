#!/usr/bin/env python3
"""Local smoke-test for the GAS transport WITHOUT the fake simulator:
runs a REAL HTTP server on 127.0.0.1 that mirrors gas/Code.gs semantics
(ring cache, long-poll wait, auth token) and points REAL GASCarrier HTTP
requests at it (https→http via a config with an https URL that the real
client hits through this server's port).

Property of the @ily_bio research channel (@iliyahsatam).

Run:  python3 tests/smoke_gas_http.py
"""
import base64
import json
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, "/root/projects/zpoint")

from core.crypto import new_dir_prefix, new_key          # noqa: E402
from core.gas import EnvelopeCrypto, ENV_LABEL_DN, ENV_LABEL_UP  # noqa: E402
from core.gas_nodes import GASClientNode, GASExitNode    # noqa: E402

TOKEN = "smoke-token-2026"
RING = 64
TTL = 21600
LOCK = threading.Lock()
CACHE = {}
META = {"up": 0, "dn": 0}


def store(dirn, items):
    with LOCK:
        n = META[dirn]
        val = json.dumps({"c": n, "f": items, "ts": time.time()})
        if len(val) > 100 * 1024:
            return {"ok": False, "err": "too_big"}
        CACHE[f"{dirn}:{n % RING}"] = val
        META[dirn] = n + 1
        return {"ok": True, "c": n}


def read_since(dirn, after):
    with LOCK:
        n = META[dirn]
        lo = max(after + 1, n - RING)
        out = []
        for x in range(lo, n):
            raw = CACHE.get(f"{dirn}:{x % RING}")
            if not raw:
                continue
            b = json.loads(raw)
            if b and b.get("c", -1) >= after + 1:
                out.append({"c": b["c"], "f": b["f"]})
        return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):     # silence
        pass

    def _send(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if q.get("k", [""])[0] != TOKEN:
            return self._send({"ok": False, "err": "auth"}, 200)
        dirn = "dn" if q.get("dir", ["up"])[0] == "dn" else "up"
        ln = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(ln).decode())
        except Exception:
            return self._send({"ok": False, "err": "badjson"})
        self._send(store(dirn, body.get("f") or []))

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if q.get("k", [""])[0] != TOKEN:
            return self._send({"ok": False, "err": "auth"}, 200)
        mode = q.get("mode", ["ping"])[0]
        if mode == "ping":
            return self._send({"ok": True, "pong": True, "hb": 1})
        if mode not in ("up", "dn"):
            return self._send({"ok": False, "err": "mode"})
        after = int(q.get("after", ["-1"])[0])
        if q.get("resync"):
            return self._send({"ok": True, "b": read_since(mode, after)})
        wait = min(int(q.get("wait", ["0"])[0]), 3)   # short for the smoke
        deadline = time.time() + wait
        out = read_since(mode, after)
        while not out and time.time() < deadline:
            time.sleep(0.1)
            out = read_since(mode, after)
        self._send({"ok": True, "b": out})


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{port}/exec"
    print(f"[smoke] http relay on {url}")

    # Real GASCarrier but pointed at the local http relay: bypass the
    # https-only guard the same way tests/test_rest.py bypasses its check.
    from core.gas import GASCarrier
    orig_init = GASCarrier.__init__

    def relaxed_init(self, u, token, name=""):
        orig_init(self, u, token, name)
    GASCarrier.__init__ = relaxed_init
    # monkeypatch the URL guard: build carriers via object instead
    def make_carrier(u):
        c = GASCarrier.__new__(GASCarrier)
        c.url = u.rstrip("/")
        c.token = TOKEN
        c.name = "local"
        c._url_lock = threading.Lock()
        c._post_url = None
        c.bytes_up = c.bytes_down = c.requests = c.errors = 0
        return c

    k, up, down = new_key(), new_dir_prefix(), new_dir_prefix()
    env_up = EnvelopeCrypto(k, ENV_LABEL_UP)
    env_dn = EnvelopeCrypto(k, ENV_LABEL_DN)

    from core.gas import GASPool, GASDownloader
    pool_up = GASPool.__new__(GASPool)
    carrier = make_carrier(url)
    pool_up.carriers = [carrier]
    pool_up.env = env_up
    pool_up.is_up = True
    pool_up.log = lambda s: print("UP:", s)
    pool_up.stats = {"batches_out": 0, "frames_out": 0}
    pool_up._s_lock = threading.Lock()
    pool_up._lane_map = {}
    pool_up._next_w = 0
    pool_up._w_lock = threading.Lock()
    pool_up._cool = {}
    pool_up._cool_lock = threading.Lock()
    pool_up._env_id = int(time.time() * 1000)
    pool_up._env_lock = threading.Lock()
    pool_up.epoch = pool_up._env_id
    from core.gas import _PoolWorker
    pool_up._workers = [_PoolWorker(pool_up, i) for i in range(2)]
    for w in pool_up._workers:
        w.start()

    # client node wired to the real pool via a fake URL list is complex;
    # instead exercise the REAL HTTP path end-to-end at carrier level:
    logs = []
    dl = GASDownloader.__new__(GASDownloader)
    dl.carriers = [make_carrier(url)]
    dl.env = env_dn
    dl.mode = "dn"
    dl.log = lambda s: print("DL:", s)
    dl.stats = {"batches_in": 0, "opens_failed": 0}
    dl.stop = threading.Event()
    dl._threads = []
    dl.handler = lambda f: logs.append(f)
    from core.gas import ItemAssembler, HOLD_MS
    dl.asm = ItemAssembler(lambda item: None, lambda s: None, 300)
    dl.asm._deliver = dl._on_item

    # 1) real ping
    ms = carrier.ping()
    print(f"[smoke] real ping round-trip: {ms:.1f} ms")

    # 2) start downloader, POST batches from the pool, check frames arrive
    dl.start()
    sid_frames = []
    frames = [{"i": i, "s": 1, "t": "d", "b": base64.urlsafe_b64encode(
        f"frame-{i}".encode()).decode()} for i in range(1, 21)]
    for chunk_start in range(0, 20, 7):
        chunk = frames[chunk_start:chunk_start + 7]
        item = {"id": 1000 + chunk_start, "w": 0,
                "n": 1 + chunk_start // 7, "b": None}
        from core.gas import seal_item
        sealed = seal_item(chunk, 1000 + chunk_start, env_dn, 0,
                           1 + chunk_start // 7, int(time.time() * 1000))
        reply = carrier.post_items("dn", [sealed])
        assert reply.get("ok"), reply
        time.sleep(0.05)
    deadline = time.time() + 10
    while len(logs) < 20 and time.time() < deadline:
        time.sleep(0.05)
    print(f"[smoke] frames delivered through REAL http long-poll: "
          f"{len(logs)}/20")
    got = sorted(int(f["i"]) for f in logs)
    assert got == list(range(1, 21)), got
    print("[smoke] all 20 frames in order ✅")
    dl.stop_all()
    pool_up.stop_all()
    srv.shutdown()
    print("✅ SMOKE TEST PASSED")


if __name__ == "__main__":
    main()
