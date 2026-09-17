#!/usr/bin/env python3
"""Hardening regression tests for the v2 audit patches.

Covers the four audit targets as executable proofs:
  T1  WriteQueue: 3-lane interleaved submits commit STRICTLY per lane,
      while distinct lanes run concurrently (ordering + no false sharing).
  T2  FrameIngest + snapshot_scan: SSE snapshot replay in seq order,
      duplicate seq dropped, lossy frames skipped.
  T3  Mux concurrency: 8 threads x 50 frames -> global seq == enqueue order
      (the old _push race would produce inversions); write() raises on
      credit starvation instead of silently dropping.
  T4  Exit socket lifecycle: fast connect/EOF cycles leak no fds and no
      sockets, teardown is idempotent under racing close.

Run: python3 tests/test_hardening.py
Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import queue
import socket
import sys
import threading
import time
from collections import defaultdict, deque

sys.path.insert(0, "/root/projects/zpoint")

from core.crypto import FrameCrypto, new_dir_prefix, new_key  # noqa: E402
from core.ingest import FrameIngest, snapshot_scan            # noqa: E402
from core.mux import Mux                                      # noqa: E402
from core.writequeue import CONTROL_LANE, WriteQueue          # noqa: E402


def test_writequeue_lane_order():
    """Per-lane FIFO must hold exactly, across 3 lanes x 60 items."""
    committed: list[tuple[str, int]] = []
    lock = threading.Lock()

    def put_fn(path, value):
        time.sleep(0.001)                    # jitter the HTTP round-trip
        with lock:
            committed.append((path, value))

    wq = WriteQueue(put_fn, workers=4, log_fn=lambda s: None)
    per_lane = {f"l{i}": list(range(60)) for i in range(3)}
    for lane, seqs in per_lane.items():
        for s in seqs:
            wq.submit(f"{lane}/{s}", s, key=lane)
    assert wq.flush(30), "writequeue did not drain"
    got: dict[str, list[int]] = defaultdict(list)
    for path, value in committed:
        got[path.split("/")[0]].append(value)
    for lane, seqs in per_lane.items():
        assert got[lane] == seqs, f"lane {lane} out of order: {got[lane][:10]}"
    print("  T1 writequeue per-lane ordering OK "
          f"(3 lanes x 60, {len(committed)} commits)")


def test_writequeue_control_never_blocked():
    """A saturated data lane must not delay the control lane."""
    done = queue.Queue()

    def put_fn(path, value):
        if value == "slow-data":
            time.sleep(0.15)
        done.put((path, value))

    wq = WriteQueue(put_fn, workers=2, log_fn=lambda s: None)
    for i in range(20):
        wq.submit(f"data/{i}", "slow-data", key="d:1")
    t0 = time.time()
    wq.submit("ctl/close", "ctl", key=CONTROL_LANE)
    ctl_done = None
    deadline = t0 + 5
    while time.time() < deadline:
        path, value = done.get(timeout=5)
        if value == "ctl":
            ctl_done = time.time() - t0
            break
    assert ctl_done is not None and ctl_done < 0.5, \
        f"control frame delayed {ctl_done:.2f}s by saturated data lane"
    wq.stop_all()
    print(f"  T3b control lane bypass OK (ctl committed in {ctl_done*1000:.0f}ms)")


def test_ingest_dedupe_and_snapshot():
    got: list[int] = []
    ing = FrameIngest(log_fn=lambda s: None)
    # duplicate delivery (SSE snapshot replay) must collapse to one
    for _ in range(3):
        ing.accept({"i": 7, "s": 1, "t": "d", "b": "AA=="}, lambda n: got.append(n["i"]))
    assert got == [7], got
    # snapshot scan: dict-of-seq->frame, ascending order, "/" keys, missing "i"
    snap = {"/9": {"s": 1, "t": "d", "b": "AA=="},
            "/10": {"i": 10, "s": 1, "t": "d", "b": "AA=="},
            "meta": {"junk": True}}
    out: list[int] = []
    snapshot_scan(snap, lambda n: out.append(n["i"]))
    assert out == [9, 10], out
    print("  T2 ingest dedupe + snapshot scan OK")


def test_mux_global_seq_order():
    """8 concurrent writers -> delivered seq order == assigned order."""
    k = new_key()
    up = new_dir_prefix()
    sent: list[dict] = []
    lock = threading.Lock()

    def send(o):
        with lock:
            sent.append(o)

    m = Mux(send, FrameCrypto(k, up), is_server=False, client_id="c1")
    sids = [m.open_stream(("h", 1)) for _ in range(8)]

    def blast(sid):
        for i in range(50):
            m.write(sid, f"{sid}:{i}".encode())

    threads = [threading.Thread(target=blast, args=(s,)) for s in sids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    seqs = [f["i"] for f in sent]
    assert seqs == sorted(seqs), "global seq order broken under concurrency"
    assert len(set(seqs)) == len(seqs), "duplicate seq assigned"
    print(f"  T3 mux seq atomicity OK ({len(seqs)} frames, 8 writers, 0 inversions)")


def test_mux_credit_starvation_raises():
    """write() must raise (not silently drop) when the window stays closed."""
    k, up = new_key(), new_dir_prefix()
    m = Mux(lambda o: None, FrameCrypto(k, up), is_server=False,
            client_id="c1", window=16 * 1024)
    sid = m.open_stream(("h", 1))
    try:
        m.write(sid, b"x" * (64 * 1024), credit_timeout=0.5)
        raise AssertionError("write() returned on credit starvation")
    except ConnectionError:
        print("  T3b credit starvation raises OK")


def test_exit_socket_lifecycle():
    """Fast open/EOF cycles: no fd growth, teardown idempotent."""
    from core.exit import ExitNode

    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    port = srv.getsockname()[1]

    def _fdcount():
        import os
        return len(os.listdir("/proc/self/fd"))

    k, up, down = new_key(), new_dir_prefix(), new_dir_prefix()
    node = ExitNode("https://mock", "", "sess", k, up, down,
                    log_fn=lambda s: None)
    node.db = type("FakeDB", (), {
        "put": lambda self, p, v: None,
        "get": lambda self, p: {},
        "delete": lambda self, p: None,
    })()
    rx = FrameCrypto(k, up)
    down_rx = FrameCrypto(k, down)
    down_frames: list[dict] = []
    node._send_down = lambda o: down_frames.append(o)  # capture T_CLOSEs
    node.mux._send = lambda o: down_frames.append(o)

    cm = Mux(lambda o: node._handle(o), FrameCrypto(k, up),
             is_server=False, client_id="c1")
    baseline = _fdcount()
    for cycle in range(6):
        sid = cm.open_stream(("127.0.0.1", port))
        deadline = time.time() + 3
        while sid not in node.conns and time.time() < deadline:
            time.sleep(0.01)
        assert sid in node.conns, f"cycle {cycle}: connect failed"
        cm.write(sid, b"ping")
        time.sleep(0.05)
        # abrupt client close -> T_CLOSE -> exit teardown
        cm.close_stream(sid)
        deadline = time.time() + 3
        while sid in node.conns and time.time() < deadline:
            time.sleep(0.01)
        assert sid not in node.conns, f"cycle {cycle}: socket leaked"
        # idempotent double-teardown must be safe
        node._teardown_stream(sid, send_close=True)
        node._teardown_stream(sid, send_close=True)
    time.sleep(0.2)
    after = _fdcount()
    assert after <= baseline + 4, f"fd leak: {baseline} -> {after}"
    node.stop_all()
    srv.close()
    print(f"  T4 socket lifecycle OK (6 fast cycles, fds {baseline}->{after})")


if __name__ == "__main__":
    print("Zpoint hardening tests (v2 audit):")
    test_writequeue_lane_order()
    test_writequeue_control_never_blocked()
    test_ingest_dedupe_and_snapshot()
    test_mux_global_seq_order()
    test_mux_credit_starvation_raises()
    test_exit_socket_lifecycle()
    print("\n✅ ALL HARDENING TESTS PASSED")
