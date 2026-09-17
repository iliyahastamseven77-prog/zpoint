"""Zpoint client daemon (local side).

Listens on SOCKS5, muxes app streams over Firebase RTDB to the exit node,
subscribes (SSE) to the downlink for responses.

AUDIT HARDENING (v2):
  * downlink SSE frames are consumed by a SINGLE ingest thread via a bounded
    queue (the old design processed each frame inline in the SSE reader and
    deleted synchronously — slow deletes stalled the live reader; and the
    initial snapshot event was misread as one frame, losing everything that
    arrived while offline).  Gap-fill runs on_ready() before live events.
  * WriteQueue lanes: data frames enqueue on a per-stream lane (ordered
    commits), control frames on the control lane — closure/credit frames are
    never stuck behind saturated data lanes.
  * socket lifecycle: to_mux owns the socket; the drain loop exits on
    T_CLOSE *or* EOF *or* stop, closes exactly once, and the stream teardown
    is idempotent (no double T_CLOSE, no fd leak, no dangling threads).

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import queue
import socket as _s
import threading

from .crypto import FrameCrypto
from .ingest import FrameIngest, snapshot_scan
from .mux import (Mux, T_CLOSE, T_CREDIT, T_DATA, T_OPEN, unpack_frame)
from .net import socks5_serve
from .rtdb import RTDB
from .writequeue import CONTROL_LANE, WriteQueue

DEFAULT_SOCKS_PORT = 1086


class ClientNode:
    def __init__(self, db_url: str, auth: str, session_id: str, key: bytes,
                 up_prefix: bytes, down_prefix: bytes,
                 listen: tuple[str, int] = ("127.0.0.1", DEFAULT_SOCKS_PORT),
                 client_id: str = "c1", log_fn=print,
                 state_dir: str = ""):
        self.db = RTDB(db_url, auth)
        self.session = session_id
        self.listen = listen
        self.client_id = client_id
        self.up_path = f"z/{session_id}/u"
        self.down_path = f"z/{session_id}/d/{client_id}"
        self.crypto = FrameCrypto(key, up_prefix)
        self.crypto_down = FrameCrypto(key, down_prefix)
        self.mux = Mux(self._send_up, self.crypto, is_server=False,
                       client_id=client_id)
        self.socks_thread = None
        self.stop = threading.Event()
        self.log = log_fn
        self.stats = {"frames_in": 0, "frames_out": 0, "bytes_in": 0,
                      "bytes_out": 0, "streams": 0}
        # monthly bandwidth meter (persists across restarts)
        from .bandwidth import BandwidthMeter
        self.meter = BandwidthMeter(state_dir or "/root/projects/zpoint/configs")
        # v2: lane-ordered async uplink writes
        self.wq = WriteQueue(self._raw_put, workers=4, log_fn=log_fn)
        # v2: bounded ingest pipeline for the downlink (dedupe by seq)
        self.ingest = FrameIngest(log_fn=log_fn)
        self._rx_q: queue.Queue = queue.Queue(maxsize=1024)
        self._ingest_thread: threading.Thread | None = None

    # ---------- outbound (client -> exit) ----------
    def _raw_put(self, path: str, value) -> None:
        self.db.put(path, value)
        self.stats["frames_out"] += 1

    def _send_up(self, obj: dict) -> None:
        # lane per mux stream -> per-stream ordering is absolute;
        # non-data frames ride the control lane (never blocked by data).
        lane = CONTROL_LANE if obj.get("t") != "d" else f"u:{obj.get('s')}"
        self.wq.submit(f"{self.up_path}/{obj['i']}", obj, key=lane)

    # ---------- inbound (exit -> client) ----------
    def _on_event(self, name: str, data: dict) -> None:
        """SSE callback — only ENQUEUES; heavy work runs on the ingest
        thread so a slow delete never stalls the live reader."""
        payload = (data or {}).get("data")
        if isinstance(payload, dict) and payload and \
                all(str(k).lstrip("/").isdigit() for k in payload):
            # synthetic snapshot put: {"/<seq>": frame, ...}
            try:
                self._rx_q.put(("snap", payload), timeout=5.0)
                return
            except queue.Full:
                self.log("[client] rx queue full — dropping snapshot batch")
                return
        node = payload
        if not isinstance(node, dict) or "b" not in node:
            return
        try:
            self._rx_q.put(("one", node), timeout=5.0)
        except queue.Full:
            self.log("[client] rx queue full — backpressuring SSE")

    def _on_ready(self) -> None:
        """Fires after each (re)connect BEFORE live events flow: drain the
        whole downlink subtree via a one-shot REST GET — deterministic
        gap-fill for frames PUT while we were disconnected."""
        try:
            snap = self.db.get(self.down_path) or {}
            frames = [v for v in snap.values()
                      if isinstance(v, dict) and "b" in v]
            if frames:
                self.log(f"[client] gap-fill: {len(frames)} frame(s) "
                         f"recovered after reconnect")
            for v in sorted(frames, key=lambda f: int(f.get("i", 0))):
                try:
                    self._rx_q.put(("one", v), timeout=5.0)
                except queue.Full:
                    self.log("[client] rx queue full during gap-fill")
                    break
        except Exception as e:
            self.log(f"[client] gap-fill scan failed: {e}")

    def _ingest_loop(self) -> None:
        while not self.stop.is_set():
            try:
                kind, payload = self._rx_q.get(timeout=0.5)
            except queue.Empty:
                continue
            if kind == "snap":
                snapshot_scan(payload, self._consume)
            else:
                self.ingest.accept(payload, self._consume)

    def _consume(self, node: dict) -> None:
        self.stats["frames_in"] += 1
        seq = node.get("i")
        try:
            # downlink frames are sealed with the DOWN prefix — peer is exit
            _, sid, ftype, body = unpack_frame(node, self.crypto_down)
            if ftype == T_DATA:
                self.stats["bytes_in"] += len(body)
                # recv queue + window credit bookkeeping (self-heals unknown
                # streams by re-creating the queue: a data frame racing the
                # OPEN snapshot still lands in the right deque)
                self.mux.deliver_received(sid, body)
            elif ftype == T_CREDIT:
                # peer granted transmit credit — WITHOUT this the client's
                # mux.write() deadlocks after 512KB per stream (audit
                # finding: control frames were silently dropped here).
                self.mux.apply_credit(sid, int(body.decode() or 0))
            elif ftype == T_CLOSE:
                self.mux.peer_closed(sid)
            elif ftype == T_OPEN:
                self.mux.peer_opened(sid)   # defensive; client never opens
            self.db.delete(f"{self.down_path}/{seq}")
        except Exception as e:
            self.log(f"[client] bad frame: {e}")

    def _on_lost(self, reason: str) -> None:
        self.log(f"[client] SSE lost: {reason} — reconnecting")

    # ---------- SOCKS ----------
    def _socks_handler(self, client_sock, target):
        self.stats["streams"] += 1
        sid = self.mux.open_stream(target)
        self.log(f"[client] stream {sid} -> {target[0]}:{target[1]}")

        def to_mux():
            # owns the socket's rx side; tears the stream down on EOF
            try:
                while not self.stop.is_set():
                    data = client_sock.recv(65536)
                    if not data:
                        break
                    self.stats["bytes_out"] += len(data)
                    self.meter.add(tx=len(data))
                    self.mux.write(sid, data)
            except (ConnectionError, OSError):
                pass
            finally:
                self.mux.close_stream(sid)

        t = threading.Thread(target=to_mux, daemon=True)
        t.start()
        # drain downlink into the socket (single owner of the tx side)
        while not self.stop.is_set():
            data = self.mux.recv(sid, timeout=0.5)
            if data is None:
                if sid not in self.mux._streams:
                    break            # peer closed (T_CLOSE) — BUGFIX #10
                continue
            try:
                client_sock.sendall(data)
                self.meter.add(rx=len(data))
            except OSError:
                break
        # teardown exactly once — close_stream is idempotent, and the
        # socket (owned by this thread + to_mux) is closed here.
        self.mux.close_stream(sid)
        try:
            client_sock.shutdown(_s.SHUT_RDWR)
        except OSError:
            pass
        try:
            client_sock.close()
        except Exception:
            pass

    # ---------- lifecycle ----------
    def start(self) -> None:
        lst = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
        lst.setsockopt(_s.SOL_SOCKET, _s.SO_REUSEADDR, 1)
        lst.bind(self.listen)
        lst.listen(64)
        self.log(f"[client] SOCKS5 on {self.listen[0]}:{self.listen[1]} "
                 f"session={self.session}")
        self.socks_thread = socks5_serve(lst, self._socks_handler)
        self._ingest_thread = threading.Thread(
            target=self._ingest_loop, daemon=True, name="zp-cli-ingest")
        self._ingest_thread.start()
        threading.Thread(target=self._downlink_loop, daemon=True,
                         name="zp-cli-sse").start()

    def _downlink_loop(self) -> None:
        # subscribe to this client's downlink subtree; on_ready() runs the
        # gap-fill scan on every (re)connect.
        self.db.stream(self.down_path, self._on_event, self._on_lost,
                       self.stop, on_ready=self._on_ready)

    def stop_all(self) -> None:
        self.stop.set()
        self.mux.closed.set()
        self.wq.flush(timeout=5.0)
        self.wq.stop_all()
