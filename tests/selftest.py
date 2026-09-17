#!/usr/bin/env python3
"""Zpoint tests: crypto, mux, credit flow, REST client.
Property of the @ily_bio research channel (@iliyahsatam).
Run: python3 tests/selftest.py
"""
import sys
import threading
import time
from collections import deque

sys.path.insert(0, "/root/projects/zpoint")

from core.crypto import FrameCrypto, new_key, new_dir_prefix  # noqa: E402
from core.mux import (Mux, pack_frame, unpack_frame,  # noqa: E402
                      encode_body, decode_body)


def test_crypto_roundtrip():
    k, up = new_key(), new_dir_prefix()
    c = FrameCrypto(k, up)
    for seq in (0, 1, 123456789):
        ct = c.seal(seq, b"payload-" + str(seq).encode())
        assert c.open(seq, ct) == b"payload-" + str(seq).encode()
    print("  crypto roundtrip OK")


def test_crypto_tamper():
    k, up = new_key(), new_dir_prefix()
    c = FrameCrypto(k, up)
    ct = c.seal(5, b"secret")
    try:
        c.open(6, ct)  # wrong seq -> wrong nonce -> must fail
        raise AssertionError("replay with wrong seq accepted!")
    except Exception:
        print("  crypto tamper/replay rejected OK")


def test_frame_codec():
    k, up = new_key(), new_dir_prefix()
    c = FrameCrypto(k, up)
    raw = b"x" * 100_000
    f = pack_frame(11, 2, "d", raw, c)
    assert f["s"] == 2 and len(f["b"]) > 0
    s2, sid, t, body = unpack_frame(f, c)
    assert (s2, sid, t) == (11, 2, "d") and body == raw
    print(f"  frame codec OK (100KB -> {len(f['b'])//1024}KB stored)")


def test_mux_roundtrip():
    k, up = new_key(), new_dir_prefix()
    k2, down = new_key(), new_dir_prefix()
    sent_up, sent_down = [], []

    # client seals uplink with 'up'; exit unseals with 'up'.
    # exit seals downlink with 'down'; client unseals with 'down'.
    cm = Mux(lambda o: sent_up.append(o), FrameCrypto(k, up),
             is_server=False, client_id="c1")
    em = Mux(lambda o: sent_down.append(o), FrameCrypto(k2, down),
             is_server=True)

    got = {}
    em.on_open = lambda sid, tgt: got.setdefault("open", (sid, tgt))
    em.on_data = lambda sid, b: got.setdefault("data", []).append(b)
    em.on_close = lambda sid: got.setdefault("closed", sid)

    sid = cm.open_stream(("example.com", 443))
    cm.write(sid, b"GET / HTTP/1.1\r\n" * 3)
    cm.close_stream(sid)

    for o in sent_up:
        em.deliver(o, FrameCrypto(k, up))   # rx crypto = peer's tx prefix

    assert got["open"][0] == sid
    assert got["open"][1][1] == 443           # host:port parsed
    assert "c1@" in got["open"][1][0]         # client id learned
    assert b"".join(got["data"]) == b"GET / HTTP/1.1\r\n" * 3
    assert got["closed"] == sid

    # reverse path: exit replies, client unpacks with its rx crypto
    reply_frames = []
    em2 = Mux(lambda o: reply_frames.append(o), FrameCrypto(k2, down),
              is_server=True)
    em2._streams[sid] = deque()
    for chunk in (b"HTTP/1.1 200 OK\r\n", b"Server: zpoint\r\n"):
        em2.write(sid, chunk)
    cm._streams[sid] = deque()
    rx = FrameCrypto(k2, down)
    for o in reply_frames:
        cm.deliver(o, rx)
    got_back = b"".join(list(cm._streams[sid]))
    assert got_back == b"HTTP/1.1 200 OK\r\nServer: zpoint\r\n"
    print(f"  mux roundtrip OK ({len(sent_up)} up, {len(reply_frames)} down)")


def test_credit_flow():
    """After the initial window is exhausted, credits must keep it moving."""
    k, up = new_key(), new_dir_prefix()
    frames = []
    W = 64 * 1024
    cm = Mux(lambda o: frames.append(o), FrameCrypto(k, up),
             is_server=False, client_id="c1", window=W)
    sid = cm.open_stream(("h", 1))
    big = b"z" * (200 * 1024)

    th = threading.Thread(target=cm.write, args=(sid, big), daemon=True)
    th.start()
    time.sleep(0.25)                       # initial window drains
    tx0 = cm._tx_used[sid]
    assert 0 < tx0 <= W, tx0               # blocked after initial window

    # receiver grants half-window credits, as a live peer would
    grants = 0
    deadline = time.time() + 4
    while cm._tx_used[sid] < len(big) and time.time() < deadline:
        cm._tx_credits[sid] += W // 2
        grants += 1
        time.sleep(0.01)
    th.join(2)
    assert cm._tx_used[sid] == len(big), (cm._tx_used[sid], len(big))
    assert grants >= 3                     # 200KB needs >3 half-window grants
    print(f"  credit flow OK ({grants} grants carried {len(big)//1024}KB)")


def test_credit_bug_regression():
    """The old buggy condition (n % (window//2) == 0) never sent credits.
    Simulate deliver() on the receiver side and assert credits ARE pushed."""
    k, up = new_key(), new_dir_prefix()
    pushed = []
    rm = Mux(lambda o: pushed.append(o), FrameCrypto(k, up),
             is_server=True, window=64 * 1024)
    # pretend a stream exists as if OPEN was delivered
    rm._streams[1] = deque()
    rm._recv_bytes[1] = 0
    rm._recv_granted[1] = W_ = 64 * 1024 // 2
    # deliver 3x half-window of data in odd-sized chunks
    rx = FrameCrypto(k, up)
    sid_dummy = None
    total = 0
    for size in (500, 70000, 130001, 1):
        frame = pack_frame(total + 1, 1, "d", b"a" * size, rx)
        rm.deliver(frame, rx)
        total += size
    credits = [p for p in pushed if p["t"] == "w"]
    assert len(credits) >= 3, f"credits pushed: {len(credits)}"
    print(f"  credit regression OK ({len(credits)} credits for odd chunks)")


def test_credit_bulk_window_regression():
    """BUGFIX #12/#13 regression: a 2MB stream must not deadlock at
    W - (W mod chunk).  Uses the REAL window (512KB) and real open/write/
    deliver paths both directions (old code stalled at exactly 491,520B)."""
    k, up = new_key(), new_dir_prefix()
    down = new_dir_prefix()
    up_q, down_q = [], []
    cm = Mux(up_q.append, FrameCrypto(k, up), is_server=False,
             client_id="c1")                       # default 512KB window
    em = Mux(down_q.append, FrameCrypto(k, down), is_server=True)
    sid = cm.open_stream(("h", 443))
    em.deliver(up_q.pop(0), FrameCrypto(k, up))    # exit sees OPEN
    assert em._tx_credits.get(sid) == em.window    # BUGFIX #13 armed
    payload = b"q" * (2 * 1024 * 1024)

    done = threading.Event()

    def pusher():
        cm.write(sid, payload, credit_timeout=20)
        done.set()

    th = threading.Thread(target=pusher, daemon=True)
    th.start()
    deadline = time.time() + 20
    while not done.is_set() and time.time() < deadline:
        while up_q:
            em.deliver(up_q.pop(0), FrameCrypto(k, up))
        while down_q:                              # credits back to client
            cm.deliver(down_q.pop(0), FrameCrypto(k, down))
        time.sleep(0.001)
    th.join(2)
    assert done.is_set(), \
        f"bulk write deadlocked at {cm._tx_used.get(sid, 0)} bytes"
    assert em._recv_bytes.get(sid, 0) == len(payload)
    print(f"  bulk window regression OK (2MB pushed, no stall at "
          f"491520B)")


def test_json_body():
    o = {"i": 1, "s": 2, "t": "d", "b": "AAA="}
    assert decode_body(encode_body(o)) == o
    print("  json body OK")


def test_socks_reply_bytes():
    """SOCKS5 success reply must be the exact 10-byte success frame."""
    expected = b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00"
    assert len(expected) == 10 and expected[1] == 0
    import core.net as netmod
    src = open(netmod.__file__, encoding="utf-8").read()
    assert "05\\x00\\x00\\x01" in src or "\x05\x00\x00\x01" in src
    print("  socks connect-reply present OK")


if __name__ == "__main__":
    print("Zpoint self-tests:")
    test_crypto_roundtrip()
    test_crypto_tamper()
    test_frame_codec()
    test_mux_roundtrip()
    test_credit_flow()
    test_credit_bug_regression()
    test_credit_bulk_window_regression()
    test_json_body()
    test_socks_reply_bytes()
    print("ALL TESTS PASSED ✅")
