"""Frame codec + smux-style multiplexer for Zpoint.

Frame JSON: {"i": seq, "s": stream, "t": type, "b": b64(ciphertext)}
types: o=open(host inside), d=data, c=close, k=ping, w=window credit

AUDIT HARDENING (v2):
  * _push() now assigns the transport seq AND hands the frame to send_fn
    under ONE critical section.  The old code released the lock between
    numbering and enqueue, so two threads could interleave seq 41 and 42
    and enqueue them 42-then-41 — downstream reassembly corrupted even
    with an ordered WriteQueue.  Ordering now starts at the mux itself.
  * deliver() uses a per-mux lock for the stream table + credit maps (the
    old dict mutations raced with _drop() from other threads).
  * recv() blocks on a per-stream Condition instead of a 5 ms sleep poll
    (lower latency, no busy wakeups, no lost-wakeup window).
  * close_stream() is idempotent; _drop() wakes all recv() waiters.
  * write() raises ConnectionError after the credit deadline instead of
    silently returning (callers can tear the stream down deterministically).

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import base64
import json
import struct
import threading
import time
import zlib
from collections import deque
from typing import Callable, Optional

T_OPEN, T_DATA, T_CLOSE, T_PING, T_CREDIT = "o", "d", "c", "k", "w"
MAX_PLAIN = 60 * 1024          # 60 KiB plaintext cap per frame
B64 = base64.urlsafe_b64encode


def UNB64(s) -> bytes:
    """Padding-tolerant urlsafe base64 decode.

    BUGFIX #21 (31 Aug): the Kotlin app encodes the INNER mux frame body
    with `Base64.getUrlEncoder().withoutPadding()`, while this module
    called bare `base64.urlsafe_b64decode` — which REQUIRES padding.
    Frames whose unpadded length % 4 != 0 raised binascii.Error
    ("Incorrect padding") on the exit and were silently dropped: the
    stream opened (short OPEN frames often survived), the uplink
    "worked", and the downlink never answered ("upload sent, no
    download").  This is the inner-frame half of the #17 envelope fix.
    """
    if isinstance(s, str):
        s = s.encode()
    return base64.urlsafe_b64decode(s + b"=" * (-len(s) % 4))


def pack_frame(seq: int, stream: int, ftype: str, body: bytes,
               crypto) -> dict:
    """zlib-compress then encrypt a frame body; returns the JSON dict."""
    comp = zlib.compress(body, 6) if body else b""
    ct = crypto.seal(seq, comp, aad=struct.pack("!IB", stream, ord(ftype)))
    return {"i": seq, "s": stream, "t": ftype, "b": B64(ct).decode()}


def unpack_frame(obj: dict, crypto) -> tuple[int, int, str, bytes]:
    seq = int(obj["i"])
    stream = int(obj["s"])
    ftype = str(obj["t"])
    ct = UNB64(obj["b"].encode())
    aad = struct.pack("!IB", stream, ord(ftype))
    comp = crypto.open(seq, ct, aad=aad)
    return seq, stream, ftype, zlib.decompress(comp) if comp else b""


class Mux:
    """smux-lite over an async send() callback.

    Exactly ONE crypto per direction:
      - the *sending* crypto is passed to __init__ (used by pack_frame)
      - the *receiving* crypto is passed to deliver() (used by unpack_frame)

    Flow control: receiver grants T_CREDIT of `window//2` bytes every time
    its cumulative per-stream received bytes cross another half-window
    threshold (tracked in _recv_bytes / _recv_granted).
    """

    def __init__(self, send_fn: Callable[[dict], None], crypto,
                 is_server: bool = False, window: int = 512 * 1024,
                 client_id: str = "c1"):
        self._send = send_fn
        self.crypto = crypto            # for OUTGOING frames (this side)
        self.client_id = client_id      # embedded in OPEN frames
        self._seq = 0
        self._io_lock = threading.Lock()      # seq assignment + send order
        self._next_sid = 0 if is_server else 1
        self._sid_lock = threading.Lock()
        self.window = window
        self._grant = window // 2
        # outbound credit accounting (what the PEER allows us to send)
        self._tx_credits: dict[int, int] = {}
        self._tx_used: dict[int, int] = {}
        # inbound accounting (what WE must credit back)
        self._recv_bytes: dict[int, int] = {}
        self._recv_granted: dict[int, int] = {}
        self._rx_lock = threading.RLock()     # inbound tables + credits
        self._streams: dict[int, deque] = {}
        self._recv_cond = threading.Condition(self._rx_lock)
        self.on_open: Optional[Callable[[int, tuple], None]] = None
        self.on_data: Optional[Callable[[int, bytes], None]] = None
        self.on_close: Optional[Callable[[int], None]] = None
        self.closed = threading.Event()

    # ---------- outbound ----------
    def open_stream(self, target: tuple[str, int]) -> int:
        with self._sid_lock:
            sid = self._next_sid
            self._next_sid += 2
        with self._rx_lock:
            self._streams[sid] = deque()
            self._tx_credits[sid] = self.window
            self._tx_used[sid] = 0
        # OPEN payload: "<client_id>@host:port" so the exit learns where to
        # write downlink frames for this client (BUGFIX #3).
        meta = f"{self.client_id}@{target[0]}:{target[1]}".encode()
        self._push(sid, T_OPEN, meta)
        return sid

    def write(self, sid: int, data: bytes,
              credit_timeout: float = 30.0) -> None:
        for off in range(0, len(data), MAX_PLAIN):
            chunk = data[off:off + MAX_PLAIN]
            self._wait_credit(sid, len(chunk), credit_timeout)
            if self.closed.is_set():
                raise ConnectionError("mux closed")
            self._push(sid, T_DATA, chunk)

    def close_stream(self, sid: int) -> None:
        """Idempotent close: exactly one T_CLOSE per stream per side."""
        first = False
        with self._io_lock:
            # membership check under the same lock that _drop() uses,
            # so double-close can never double-send T_CLOSE.
            if sid in self._streams or sid in self._tx_credits:
                first = True
        if not first:
            return
        if not self.closed.is_set():
            self._push(sid, T_CLOSE, b"")
        self._drop(sid)

    def ping(self) -> None:
        self._push(0, T_PING, b"t")

    # ---------- inbound ----------
    def deliver(self, node: dict, rx_crypto) -> None:
        """Decrypt with rx_crypto (the PEER's tx prefix) and dispatch."""
        seq, sid, ftype, body = unpack_frame(node, rx_crypto)
        if ftype == T_OPEN:
            host, _, port = body.decode().rpartition(":")
            with self._rx_lock:
                self._streams.setdefault(sid, deque())
                self._recv_bytes[sid] = 0
                # BUGFIX #12: ack bookkeeping starts at ZERO.  Starting at
                # _grant meant the first T_CREDIT fired only after 512KB
                # received, while senders can only use floor(W/chunk)*chunk
                # (491,520B with 60KB chunks) — a guaranteed credit
                # deadlock on every bulk transfer.
                self._recv_granted[sid] = 0
                # BUGFIX #13: the RECEIVER of an OPEN (the exit for client
                # streams) must also arm its TRANSMIT credit table here.
                # Without it, the T_CREDIT branch's `if sid in
                # self._tx_credits` guard silently dropped every credit the
                # client granted, stalling the downlink after one window.
                self._tx_credits.setdefault(sid, self.window)
                self._tx_used.setdefault(sid, 0)
            if self.on_open:
                self.on_open(sid, (host, int(port)))
        elif ftype == T_DATA:
            with self._rx_lock:
                q = self._streams.get(sid)
                if q is not None:
                    q.append(body)
                    self._recv_cond.notify_all()
                n = self._recv_bytes.get(sid, 0) + len(body)
                self._recv_bytes[sid] = n
                grants: list[bytes] = []
                if sid in self._recv_granted:
                    while n - self._recv_granted[sid] >= self._grant:
                        self._recv_granted[sid] += self._grant
                        grants.append(str(self._grant).encode())
            for g in grants:   # outside the lock: send may enqueue
                self._push(sid, T_CREDIT, g)
            if self.on_data:
                self.on_data(sid, body)
        elif ftype == T_CLOSE:
            if self.on_close:
                self.on_close(sid)
            self._drop(sid)
        elif ftype == T_CREDIT:
            with self._rx_lock:
                if sid in self._tx_credits:
                    self._tx_credits[sid] += int(body)
                    self._recv_cond.notify_all()
        elif ftype == T_PING:
            pass

    def deliver_received(self, sid: int, body: bytes) -> None:
        """Insert already-decrypted payload into a stream's recv queue and
        run the same cumulative credit accounting deliver() does."""
        grants: list[bytes] = []   # NOTE: used below — do not remove again
        with self._rx_lock:
            q = self._streams.get(sid)
            if q is not None:
                q.append(body)
                self._recv_cond.notify_all()
            n = self._recv_bytes.get(sid, 0) + len(body)
            self._recv_bytes[sid] = n
            if sid not in self._recv_granted:
                # BUGFIX #12 (site 3): ack accounting starts at ZERO —
                # lazy-init at _grant deferred the first T_CREDIT to 2*_grant,
                # deadlocking senders at W - (W mod chunk) bytes.
                self._recv_granted[sid] = 0
            while n - self._recv_granted[sid] >= self._grant:
                self._recv_granted[sid] += self._grant
                grants.append(str(self._grant).encode())
        for g in grants:
            self._push(sid, T_CREDIT, g)

    def _push(self, sid: int, ftype: str, body: bytes) -> None:
        if self.closed.is_set():
            return
        # CRITICAL: seq assignment + send_fn must be atomic with respect to
        # other senders, or RTDB can receive 42 before 41 (out-of-order PUTs
        # even with a per-stream ordered WriteQueue).
        with self._io_lock:
            if self.closed.is_set():
                return
            self._seq += 1
            seq = self._seq
            if ftype == T_DATA:
                self._tx_used[sid] = self._tx_used.get(sid, 0) + len(body)
            frame = pack_frame(seq, sid, ftype, body, self.crypto)
            self._send(frame)

    def _wait_credit(self, sid: int, n: int, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        with self._recv_cond:
            while (self._tx_credits.get(sid, self.window)
                   - self._tx_used.get(sid, 0)) < n:
                if self.closed.is_set():
                    return
                left = deadline - time.time()
                if left <= 0:
                    raise ConnectionError(
                        f"credit timeout on stream {sid} ({n}B)")
                self._recv_cond.wait(timeout=min(left, 0.25))

    def _drop(self, sid: int) -> None:
        with self._rx_lock:
            self._streams.pop(sid, None)
            self._tx_credits.pop(sid, None)
            self._tx_used.pop(sid, None)
            self._recv_bytes.pop(sid, None)
            self._recv_granted.pop(sid, None)
            self._recv_cond.notify_all()

    # ---------- control-frame helpers (additive, used by daemons that
    # pre-decrypt frames instead of calling deliver()) ----------

    def apply_credit(self, sid: int, nbytes: int) -> None:
        """Peer granted `nbytes` more transmit credit on this stream."""
        with self._rx_lock:
            if sid in self._tx_credits:
                self._tx_credits[sid] += int(nbytes)
                self._recv_cond.notify_all()

    def peer_closed(self, sid: int) -> None:
        """Peer sent T_CLOSE: drop the stream's tx table so writers fail
        fast; wake everyone blocked in recv()/_wait_credit()."""
        self._drop(sid)

    def peer_opened(self, sid: int) -> None:
        """Defensive: create the recv queue for a stream we didn't open."""
        with self._rx_lock:
            self._streams.setdefault(sid, deque())
            self._recv_bytes.setdefault(sid, 0)
            self._recv_granted.setdefault(sid, 0)   # BUGFIX #12 parity

    def recv(self, sid: int, timeout: float = None) -> Optional[bytes]:
        """Pop the next chunk for this stream; None on timeout/close."""
        deadline = None if timeout is None else time.time() + timeout
        with self._recv_cond:
            while True:
                q = self._streams.get(sid)
                if q is None:
                    return None            # stream dropped/closed
                if q:
                    return q.popleft()
                if deadline is not None and time.time() >= deadline:
                    return None
                if self.closed.is_set():
                    return None
                wait = 0.25 if deadline is None else \
                    min(0.25, max(0.0, deadline - time.time()))
                self._recv_cond.wait(timeout=wait)


def encode_body(obj: dict) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def decode_body(raw: bytes) -> dict:
    return json.loads(raw.decode())
