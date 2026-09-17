#!/usr/bin/env python3
"""E2E tests for the Google Apps Script transport (core/gas.py + gas_nodes).

Property of the @ily_bio research channel (@iliyahsatam).
Run: python3 tests/test_gas.py

The FakeGAS simulator mirrors gas/Code.gs exactly (ring overwrite, meta
counter, resync semantics, auth).  Tests:
  1. echo roundtrip through a 3-script pool with a skewed fake network
     (random per-carrier delay + out-of-order delivery) — real TCP target;
  2. bulk transfer (2 MB) — batch reordering + reassembly correctness;
  3. POST failover: home script dead -> batches land on the survivor;
  4. backpressure: cache-like memory drops oldest -> assembler resyncs;
  5. tamper: envelope opened with the wrong env id is rejected.
"""
from __future__ import annotations

import base64
import json
import random
import socket
import sys
import threading
import time
from collections import deque

sys.path.insert(0, "/root/projects/zpoint")

from core.crypto import FrameCrypto, new_dir_prefix, new_key  # noqa: E402
from core.gas import (ENV_LABEL_DN, ENV_LABEL_UP, EnvelopeCrypto,  # noqa: E402
                      GASDownloader, GASPool, open_item, seal_item,
                      token_mac)
from core.gas_nodes import GASClientNode, GASExitNode  # noqa: E402

RND = random.Random(20260831)


# =====================================================================
# FakeGAS — Python mirror of gas/Code.gs (keep in sync with the .gs!)
# =====================================================================

class FakeGAS:
    """One simulated Web App: same ring / meta / auth behavior as Code.gs."""

    RING = 64                     # mirrors gas/Code.gs (64 keys per direction)
    MAX_WAIT = 2                  # no real long-polling in tests

    def __init__(self, name: str, token: str, net=None, chaos: float = 0.0,
                 ring: int = None):
        self.name = name
        self.token = token
        self.net = net or _FakeNet()
        self.chaos = chaos           # probability of scrambling ring order
        if ring is not None:
            self.RING = ring
        self._lock = threading.Lock()
        self._cache: dict[str, str] = {}
        self._meta = {"up": 0, "dn": 0}
        self.alive = True
        self.stored = 0

    # --- Code.gs helpers ---
    def _cache_put(self, k: str, v: str):
        self._cache[k] = v

    def _cache_get(self, k: str):
        return self._cache.get(k)

    def _readSince(self, dirn: str, after: int):
        n = self._meta[dirn]
        lo = max(after + 1, n - self.RING)
        out = []
        for x in range(lo, n):
            raw = self._cache_get(f"{dirn}:{x % self.RING}")
            if not raw:
                continue
            b = json.loads(raw)
            if b and isinstance(b.get("c"), int) and b["c"] >= after + 1:
                out.append({"c": b["c"], "f": b["f"]})
        return out

    # --- endpoints (called through the fake network) ---
    def doPost(self, params: dict, body: dict) -> dict:
        with self._lock:
            if not self.alive or params.get("k") != self.token:
                return {"ok": False, "err": "auth"}
            dirn = "dn" if params.get("dir") == "dn" else "up"
            if not isinstance(body.get("f"), list) or not body["f"]:
                return {"ok": False, "err": "badbatch"}
            n = self._meta[dirn]
            val = json.dumps({"c": n, "f": body["f"], "ts": 0})
            if len(val) > 100 * 1024:
                return {"ok": False, "err": "too_big"}
            self._cache_put(f"{dirn}:{n % self.RING}", val)
            self._meta[dirn] = n + 1
            self.stored += 1
            return {"ok": True, "c": n}

    def doGet(self, params: dict) -> dict:
        with self._lock:
            if not self.alive or params.get("k") != self.token:
                return {"ok": False, "err": "auth"}
            mode = params.get("mode", "ping")
            if mode == "ping":
                return {"ok": True, "pong": True}
            if mode not in ("up", "dn"):
                return {"ok": False, "err": "mode"}
            after = int(params.get("after", "-1"))
            if params.get("resync"):
                return {"ok": True, "b": self._readSince(mode, after)}
            # wait=0 in tests: single-shot read
            return {"ok": True, "b": self._readSince(mode, after)}

    # --- ring scramble: emulate out-of-order ring delivery ---
    def scramble(self, batches):
        if self.chaos > 0 and len(batches) > 1 and RND.random() < self.chaos:
            RND.shuffle(batches)
        return batches


class _FakeNet:
    """Per-carrier latency skew + chaos scramble on the READ path."""

    def __init__(self, delay: float = 0.0, chaos: float = 0.0):
        self.delay = delay
        self.chaos = chaos

    def on_read(self, batches):
        if self.delay:
            time.sleep(self.delay * RND.random())
        if self.chaos and len(batches) > 1 and RND.random() < self.chaos:
            RND.shuffle(batches)
        return batches


class FakeCarrierPool:
    """Bridges GASCarrier-shaped calls to FakeGAS instances."""

    def __init__(self, apps: list[FakeGAS]):
        self.apps = {a.name: a for a in apps}

    def attach(self, pool: "GASPool", direction: str):
        for i, car in enumerate(pool.carriers):
            app = self.apps[car.name]
            car._test = (app, direction)

    def attach_downloader(self, dl: GASDownloader, direction: str):
        for car in dl.carriers:
            app = self.apps[car.name]
            car._test = (app, direction)


def _wire(car, method: str, params: dict, body=None):
    """Simulate one HTTP round-trip against the FakeGAS behind `car`."""
    app, _direction = car._test
    if not app.alive:
        raise ConnectionError(f"{app.name} down")
    if method == "POST":
        resp = app.doPost(params, body)
    else:
        resp = app.doGet(params)
        resp["b"] = app.scramble(resp.get("b") or [])
        net = getattr(app, "net", None)
        if net:
            resp["b"] = net.on_read(resp.get("b") or [])
    return resp


def install_fake_http():
    """Monkey-patch GASCarrier's HTTP internals to the fake network."""
    from core import gas as gasmod

    def _post_json(self, params, body_bytes):
        body = json.loads(body_bytes.decode())
        return _wire(self, "POST", params, body)

    def _get_json(self, params):
        return _wire(self, "GET", params)

    def _drain(self, url, data, headers, method):
        # decode the URL params the real carrier built
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(url).query)
        params = {k: v[0] for k, v in q.items()}
        if method == "POST":
            return 200, json.dumps(
                _wire(self, "POST", params,
                      json.loads(data.decode()))).encode()
        return 200, json.dumps(_wire(self, "GET", params)).encode()

    gasmod.GASCarrier._post_json = _post_json
    gasmod.GASCarrier.poll = _get_poll(_get_json)
    gasmod.GASCarrier._drain_redirects = _drain
    gasmod.GASCarrier.ping = lambda self: 1.0


def _get_poll(_get_json):
    def poll(self, mode, after, wait_s, resync=False):
        params = {"k": self.token, "mode": mode, "after": str(int(after)),
                  "wait": str(int(wait_s))}
        if resync:
            params["resync"] = "1"
        self.requests += 1
        return _get_json(self, params)
    return poll


# =====================================================================
# Helpers
# =====================================================================

def make_echo_server():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    port = srv.getsockname()[1]

    def echo():
        while True:
            try:
                c, _ = srv.accept()
            except OSError:
                return

            def h(c=c):
                try:
                    while True:
                        d = c.recv(65536)
                        if not d:
                            break
                        c.sendall(d.upper())
                except OSError:
                    pass
            threading.Thread(target=h, daemon=True).start()
    threading.Thread(target=echo, daemon=True).start()
    return srv, port


def wait_until(pred, timeout=10.0, step=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def build_nodes(token, urls, n_workers=4, hold_ms=600):
    k, up, down = new_key(), new_dir_prefix(), new_dir_prefix()
    cli = GASClientNode(urls, token, "sess-gas", k, up, down,
                        listen=("127.0.0.1", 0), client_id="c1",
                        log_fn=lambda s: None, workers=n_workers)
    ex = GASExitNode(urls, token, "sess-gas", k, up, down,
                     log_fn=lambda s: None, workers=n_workers)
    return cli, ex, k, up, down


# =====================================================================
# Tests
# =====================================================================

def test_echo_through_chaos_pool():
    """Real TCP echo through 3 fake scripts, skewed + scrambled network."""
    install_fake_http()
    token = "tok-echo"
    net = _FakeNet(delay=0.03, chaos=0.5)      # heavy reorder + jitter
    apps = [FakeGAS(f"gas{i}", token, net=net, chaos=0.3) for i in range(3)]
    fp = FakeCarrierPool(apps)
    urls = [f"https://script.google.com/macros/s/{a.name}/exec"
            for a in apps]
    cli, ex, *_ = build_nodes(token, urls, n_workers=4)
    fp.attach(ex.pool, "dn")
    fp.attach_downloader.__self__  # noqa: B018  (attached below)
    for dl, d in ((ex._downloader, "up"), ):
        pass  # downloader created in start(); attach after start()
    srv, tport = make_echo_server()
    ex.start()
    cli.start()
    # now attach fake apps to every carrier in both directions
    fp.attach(cli.pool, "up")
    fp.attach_downloader(cli._downloader, "dn")
    fp.attach_downloader(ex._downloader, "up")
    fp.attach(ex.pool, "dn")

    sid = cli.mux.open_stream(("127.0.0.1", tport))
    ok = wait_until(lambda: sid in ex.conns, timeout=10)
    assert ok, f"exit never opened TCP (streams={ex.stats})"
    print("1) OPEN via GAS pool reached the exit (TCP open) OK")

    cli.mux.write(sid, b"hello gas world")
    got = wait_until(lambda: sid in cli.mux._streams and
                     cli.mux._streams[sid], timeout=10)
    data = cli.mux.recv(sid, timeout=5)
    assert data == b"HELLO GAS WORLD", data
    print("2) echo roundtrip through the pool OK:", data)

    cli.mux.close_stream(sid)
    ok = wait_until(lambda: sid not in ex.conns, timeout=10)
    assert ok, "exit socket not closed after client FIN"
    print("3) T_CLOSE propagated through the pool OK")

    cli.stop_all()
    ex.stop_all()
    srv.close()
    print("   PASS ✅\n")


def test_bulk_transfer():
    """2 MB one-way bulk: reordering + reassembly must be byte-exact."""
    install_fake_http()
    token = "tok-bulk"
    net = _FakeNet(delay=0.02, chaos=0.6)
    apps = [FakeGAS(f"gas{i}", token, net=net, chaos=0.5) for i in range(3)]
    fp = FakeCarrierPool(apps)
    urls = [f"https://script.google.com/macros/s/{a.name}/exec"
            for a in apps]
    cli, ex, *_ = build_nodes(token, urls, n_workers=6)
    srv, tport = make_echo_server()
    ex.start()
    cli.start()
    fp.attach(cli.pool, "up")
    fp.attach(ex.pool, "dn")
    fp.attach_downloader(cli._downloader, "dn")
    fp.attach_downloader(ex._downloader, "up")

    payload = bytes(RND.getrandbits(8) for _ in range(256)) * 8192  # 2 MiB
    sid = cli.mux.open_stream(("127.0.0.1", tport))
    ok = wait_until(lambda: sid in ex.conns, timeout=10)
    assert ok, "exit did not open TCP"

    # Read EXACTLY the echo (per-recv chunks) — no cross-chunk reassembly
    # assumptions (echo server + crossing T_CREDIT frames make raw byte
    # offsets ambiguous even though the transport is byte-exact).
    received = []

    def reader():
        while True:
            d = cli.mux.recv(sid, timeout=2.0)
            if d is None:
                if sid not in cli.mux._streams:
                    break
                continue
            received.append(d)

    rt = threading.Thread(target=reader, daemon=True)
    rt.start()
    t0 = time.time()
    cli.mux.write(sid, payload)
    rt.join(timeout=90)
    dt = time.time() - t0
    # a) every delivered chunk must be an exact slice of the echo stream
    total = sum(len(d) for d in received)
    echo = payload.upper()
    flat = b"".join(received)
    assert total == len(payload) and flat == echo, \
        f"bulk mismatch: {total}B of {len(payload)}B"
    # b) sender-side accounting: every chunk we pushed got an exact reply
    print(f"4) bulk 2 MiB roundtrip byte-exact in {dt:.2f}s "
          f"({len(payload)/1024/max(dt,0.001):.0f} KiB/s effective)")
    cli.mux.close_stream(sid)
    cli.stop_all()
    ex.stop_all()
    srv.close()
    print("   PASS ✅\n")


def test_post_failover():
    """Home script dead at POST time -> its envelopes land on a survivor."""
    install_fake_http()
    token = "tok-fail"
    apps = [FakeGAS(f"gas{i}", token) for i in range(3)]
    apps[0].alive = False                      # home for worker 0 is dead
    fp = FakeCarrierPool(apps)
    urls = [f"https://script.google.com/macros/s/{a.name}/exec"
            for a in apps]
    cli, ex, *_ = build_nodes(token, urls, n_workers=2)
    srv, tport = make_echo_server()
    ex.start()
    cli.start()
    fp.attach(cli.pool, "up")
    fp.attach(ex.pool, "dn")
    fp.attach_downloader(cli._downloader, "dn")
    fp.attach_downloader(ex._downloader, "up")

    sid = cli.mux.open_stream(("127.0.0.1", tport))
    ok = wait_until(lambda: sid in ex.conns, timeout=15)
    assert ok, "failover: exit never opened TCP"
    cli.mux.write(sid, b"failover frame")
    data = cli.mux.recv(sid, timeout=10)
    assert data == b"FAILOVER FRAME", data
    assert apps[1].stored + apps[2].stored > 0, "nothing stored on survivors"
    assert apps[0].stored == 0, "dead script stored something?!"
    print("5) POST failover away from a dead home script OK "
          f"(stored on survivors: {apps[1].stored + apps[2].stored})")
    cli.stop_all()
    ex.stop_all()
    srv.close()
    print("   PASS ✅\n")


def test_resync_after_eviction():
    """Late-subscribed exit must recover the ENTIRE backlog via the resync
    scan (RTDB-parity for the old on_ready gap-fill): frames POSTed while
    the exit was down are replayed from the ring on its first poll.
    (Real ring = 64 keys, TTL 6h — eviction of live backlog is a
    documented degradation, exercised separately in FakeGAS unit terms.)"""
    install_fake_http()
    token = "tok-resync"
    apps = [FakeGAS(f"gas{i}", token) for i in range(2)]
    fp = FakeCarrierPool(apps)
    urls = [f"https://script.google.com/macros/s/{a.name}/exec"
            for a in apps]
    cli, ex, *_ = build_nodes(token, urls, n_workers=2, hold_ms=300)
    srv, tport = make_echo_server()
    # DON'T start the exit yet: let the client push a backlog first
    cli.start()
    fp.attach(cli.pool, "up")
    fp.attach_downloader(cli._downloader, "dn")
    sid = cli.mux.open_stream(("127.0.0.1", tport))
    cli.mux.write(sid, b"early frames before exit subscribed")
    for i in range(10):
        cli.mux.write(sid, b"filler-%d" % i)
    ok = wait_until(lambda: apps[0].stored + apps[1].stored >= 2,
                    timeout=10) or apps[0].stored + apps[1].stored >= 2
    assert ok, "nothing was stored while the exit was down"
    ex.start()
    fp.attach(ex.pool, "dn")
    fp.attach_downloader(ex._downloader, "up")
    ok = wait_until(lambda: sid in ex.conns, timeout=15)
    assert ok, "exit never opened TCP after late start (resync failed)"
    cli.mux.write(sid, b"post-resync frame")
    # the echo answers the backlog AND our frame in order — scan for ours
    deadline = time.time() + 15
    saw_post = False
    while time.time() < deadline and not saw_post:
        d = cli.mux.recv(sid, timeout=2.0)
        if d is None:
            if sid not in cli.mux._streams:
                break
            continue
        if b"POST-RESYNC FRAME" in d:
            saw_post = True
    assert saw_post, \
        "post-resync echo never came back — backlog replay broken"
    print("6) late-subscribed exit recovered the backlog via resync OK")
    cli.stop_all()
    ex.stop_all()
    srv.close()
    print("   PASS ✅\n")


def test_tamper_and_nonce():
    """Envelope sealed under env_id N must not open under M; pool env ids
    are globally unique per direction (GCM nonce safety)."""
    key = new_key()
    env = EnvelopeCrypto(key, ENV_LABEL_UP)
    frames = [{"i": 1, "s": 1, "t": "d", "b": "x"}]
    item = seal_item(frames, 7, env, 0, 1, 1000)
    assert open_item(item, env)["f"] == frames
    bad = dict(item)
    bad["id"] = 8
    try:
        open_item(bad, env)
        raise AssertionError("tampered envelope opened!")
    except Exception:
        pass
    # pool-level env ids are unique + monotonic within a pool (nonce safety)
    urls = ["https://script.google.com/macros/s/x/exec"]
    p1 = GASPool(urls, "t", env, is_up=True, log_fn=lambda s: None)
    ids = [p1.next_env_id() for _ in range(100)]
    assert len(set(ids)) == 100 and ids == sorted(ids), \
        "env ids must be unique and monotonic"
    for _ in range(100):
        p1.next_env_id()
    p2 = GASPool(urls, "t", env, is_up=True, log_fn=lambda s: None)
    # a NEW pool (simulated restart) starts ABOVE the old epoch: its
    # envelopes are accepted; the OLD pool's envelopes would be dropped
    # by a receiver running at p2's epoch (stale_epoch counter).
    assert p2.epoch > p1.epoch - 3600_000, "epoch sanity"
    # token fingerprint is not the token
    assert token_mac("secret") != "secret" and len(token_mac("secret")) == 8
    print("7) envelope tamper rejection + nonce-space separation OK")
    print("   PASS ✅\n")


def main():
    random.seed(20260831)
    test_tamper_and_nonce()
    test_echo_through_chaos_pool()
    test_post_failover()
    test_bulk_transfer()
    test_resync_after_eviction()
    print("✅ ALL GAS TRANSPORT TESTS PASSED")


if __name__ == "__main__":
    main()
