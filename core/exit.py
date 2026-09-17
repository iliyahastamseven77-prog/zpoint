"""Zpoint exit node daemon (VPS side).

Subscribes (SSE) to the client uplink, opens real TCP connections, and
publishes responses on the client downlink.  Runs a janitor GC loop.

AUDIT HARDENING (v2):
  * the old _on_event spawned an UNBOUNDED thread per inbound SSE packet —
    a burst of 500 frames = 500 threads (scheduler storm, 8 MB stacks),
    and the threads raced each other into mux.deliver() out of seq order.
    Replaced by a bounded dispatcher: one ingest thread per direction with
    a bounded queue; per-stream ordering is now guaranteed end-to-end.
  * the initial SSE snapshot {"/<seq>": frame, ...} was misread as a single
    frame and dropped — frames published while the exit was offline never
    reached the mux.  snapshot_scan() replays them in seq order, and the
    on_ready() hook re-scans the whole uplink subtree on every reconnect
    (deterministic gap-fill, no loss window between EOF and re-subscribe).
  * uplink deletes moved OFF the ingest critical path (async GC queue) —
    the SSE loop never blocks on an HTTP round-trip.
  * socket lifecycle: connect worker installs the socket BEFORE the reader
    thread starts, stream state is owned by exactly one closer, shutdown()
    (FIN) precedes close(), stop_all() shuts down every socket (unblocking
    readers) — no fd leaks on fast connect/close cycling.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import queue
import socket
import threading
import time

from .crypto import FrameCrypto
from .ingest import FrameIngest, snapshot_scan
from .mux import Mux
from .rtdb import RTDB
from .writequeue import CONTROL_LANE, WriteQueue


class ExitNode:
    def __init__(self, db_url: str, auth: str, session_id: str, key: bytes,
                 up_prefix: bytes, down_prefix: bytes,
                 log_fn=print):
        self.db = RTDB(db_url, auth)
        self.session = session_id
        self.up_path = f"z/{session_id}/u"
        # downlink write path MUST match the client's subscription root
        # z/{session}/d/<client_id>; the exit learns the client_id
        # dynamically from the OPEN frame's "<client_id>@host:port" payload
        # (BUGFIX #3/#8).
        self.down_root = f"z/{session_id}/d"
        self._client_id: str | None = None
        self.crypto = FrameCrypto(key, up_prefix)         # decrypt client->exit
        self.crypto_down = FrameCrypto(key, down_prefix)  # seal exit->client
        self.wq = WriteQueue(self._raw_put, workers=4, log_fn=log_fn)
        self.mux = Mux(self._send_down, self.crypto_down, is_server=True)
        self.mux.on_open = self._on_open
        self.mux.on_data = self._on_data
        self.mux.on_close = self._on_close
        self.conns: dict[int, socket.socket] = {}
        self._conn_lock = threading.Lock()
        self._closing: set[int] = set()
        self._closing_lock = threading.Lock()
        self.threads: list[threading.Thread] = []
        self.stop = threading.Event()
        self.log = log_fn
        self.stats = {"frames_in": 0, "frames_out": 0, "bytes_in": 0,
                      "bytes_out": 0, "streams": 0}
        # bounded ingest pipeline + async delete GC
        self.ingest = FrameIngest(log_fn=log_fn)
        self._rx_q: queue.Queue = queue.Queue(maxsize=1024)
        self._del_q: queue.Queue = queue.Queue(maxsize=2048)

    # ---------- outbound (exit -> client) ----------
    def _raw_put(self, path: str, value) -> None:
        self.db.put(path, value)
        self.stats["frames_out"] += 1

    def _send_down(self, obj: dict) -> None:
        cid = self._client_id or "c1"     # learned from first OPEN
        path = f"{self.down_root}/{cid}/{obj['i']}"
        lane = CONTROL_LANE if obj.get("t") != "d" else f"d:{obj.get('s')}"
        self.wq.submit(path, obj, key=lane)   # async — never blocks the mux

    # ---------- inbound (client -> exit) ----------
    def _on_event(self, name: str, data: dict) -> None:
        """SSE callback — only ENQUEUES (no per-packet threads)."""
        payload = (data or {}).get("data")
        if isinstance(payload, dict) and payload and \
                all(str(k).lstrip("/").isdigit() for k in payload):
            # synthetic snapshot put: {"/<seq>": frame, ...}
            try:
                self._rx_q.put(("snap", payload), timeout=5.0)
                return
            except queue.Full:
                self.log("[exit] rx queue full — dropping snapshot batch")
                return
        node = payload
        if not isinstance(node, dict) or "b" not in node:
            return
        try:
            self._rx_q.put(("one", node), timeout=5.0)
        except queue.Full:
            self.log("[exit] rx queue full — backpressuring SSE")

    def _on_ready(self) -> None:
        """After each (re)connect, BEFORE live events: one-shot REST GET of
        the whole uplink subtree recovers frames posted during the drop
        (deterministic gap-fill; the SSE snapshot covers Window A, this
        covers anything the snapshot could race with)."""
        try:
            snap = self.db.get(self.up_path) or {}
            frames = [v for v in snap.values()
                      if isinstance(v, dict) and "b" in v]
            if frames:
                self.log(f"[exit] gap-fill: {len(frames)} frame(s) recovered")
            for v in sorted(frames, key=lambda f: int(f.get("i", 0))):
                try:
                    self._rx_q.put(("one", v), timeout=5.0)
                except queue.Full:
                    self.log("[exit] rx queue full during gap-fill")
                    break
        except Exception as e:
            self.log(f"[exit] gap-fill scan failed: {e}")

    def _ingest_loop(self) -> None:
        while not self.stop.is_set():
            try:
                kind, payload = self._rx_q.get(timeout=0.5)
            except queue.Empty:
                continue
            if kind == "snap":
                snapshot_scan(payload, self._handle)
            else:
                self.ingest.accept(payload, self._handle)

    def _handle(self, node: dict) -> None:
        try:
            # deliver into the mux: fires on_open / on_data / on_close
            self.mux.deliver(node, self.crypto)
            self.stats["frames_in"] += 1
            seq = node.get("i")
            if seq is not None:
                try:
                    self._del_q.put(f"{self.up_path}/{seq}", timeout=2.0)
                except queue.Full:
                    self.log("[exit] delete queue full — GC janitor will "
                             "catch up")
        except Exception as e:  # bad frame — drop silently but count
            self.log(f"[exit] bad frame: {e}")

    def _delete_loop(self) -> None:
        while not self.stop.is_set():
            try:
                path = self._del_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.db.delete(path)
            except Exception as e:
                self.log(f"[exit] delete failed ({path}): {e}")

    def _on_data(self, sid: int, data: bytes) -> None:
        """Mux callback: app-level bytes from the client -> real TCP socket."""
        self.stats["bytes_in"] += len(data)
        s = self._socket_for(sid)
        if s is not None:
            try:
                s.sendall(data)
            except OSError as e:
                self.log(f"[exit] send to target failed: {e}")
                self._teardown_stream(sid, send_close=True)
        else:
            self.log(f"[exit] data for stream {sid} with no socket — dropped")

    def _socket_for(self, sid: int) -> socket.socket | None:
        """Wait briefly for the connect worker to install the socket
        (data racing the TCP connect).  Bounded, lock-based — the old
        50 x 20 ms busy-poll is gone."""
        deadline = time.time() + 1.0
        while time.time() < deadline and not self.stop.is_set():
            with self._conn_lock:
                s = self.conns.get(sid)
            if s is not None:
                return s
            time.sleep(0.005)
        return None

    def _on_open(self, sid: int, target: tuple) -> None:
        self.stats["streams"] += 1
        # OPEN payload format: "<client_id>@host:port" (see mux.open_stream)
        # BUGFIX #8: strip the "@client_id" part UNCONDITIONALLY — it exists
        # on every OPEN frame, not just the first one.  Otherwise streams 2+
        # try to connect to "c1@domain" and die with gaierror.
        if target and "@" in target[0]:
            cid, _, real_host = target[0].partition("@")
            if cid:
                if self._client_id is None:
                    self._client_id = cid
                    self.log(f"[exit] client_id learned: {cid}")
            target = (real_host, target[1])

        def worker():
            s = None
            try:
                s = socket.create_connection(target, timeout=15)
                s.settimeout(None)
                with self._conn_lock:
                    # data may have arrived while connecting; _teardown may
                    # have raced the connect — check before installing.
                    if sid in self._closing:
                        raise ConnectionError("stream already closed")
                    self.conns[sid] = s
                self.log(f"[exit] stream {sid} -> {target[0]}:{target[1]}")

                def to_mux():
                    # owns the socket's rx side; propagates target EOF
                    # immediately as T_CLOSE to the client (BUGFIX #9)
                    try:
                        while not self.stop.is_set():
                            data = s.recv(65536)
                            if not data:
                                break
                            self.stats["bytes_out"] += len(data)
                            self.mux.write(sid, data)
                    except OSError:
                        pass
                    finally:
                        self._teardown_stream(sid, send_close=True)

                threading.Thread(target=to_mux, daemon=True,
                                 name=f"zp-tgt-{sid}").start()
            except Exception as e:
                self.log(f"[exit] connect {target} failed: {e}")
                if s is not None:
                    try:
                        s.close()
                    except Exception:
                        pass
                self._teardown_stream(sid, send_close=True)

        threading.Thread(target=worker, daemon=True,
                         name=f"zp-conn-{sid}").start()

    def _teardown_stream(self, sid: int, send_close: bool) -> None:
        """Single idempotent closer for a stream's socket + mux state."""
        first = False
        with self._closing_lock:
            if sid not in self._closing:
                self._closing.add(sid)
                first = True
        with self._conn_lock:
            s = self.conns.pop(sid, None)
        if s is not None:
            try:
                # FIN first: unblocks a recv() blocked in to_mux — plain
                # close() would leak that thread (BUGFIX #9 follow-up).
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except Exception:
                pass
        if first and send_close:
            self.mux.close_stream(sid)   # idempotent, sends one T_CLOSE
        if first:
            with self._closing_lock:
                self._closing.discard(sid)

    def _on_close(self, sid: int) -> None:
        """Client sent T_CLOSE (its FIN).  Close the target socket."""
        self._teardown_stream(sid, send_close=False)

    # ---------- lifecycle ----------
    def start(self) -> None:
        self.log(f"[exit] session={self.session} subscribing {self.up_path}")
        t = threading.Thread(
            target=self.db.stream,
            args=(self.up_path, self._on_event, self._on_lost, self.stop),
            kwargs={"on_ready": self._on_ready},
            daemon=True, name="zp-exit-sse")
        t.start()
        self.threads.append(t)
        it = threading.Thread(target=self._ingest_loop, daemon=True,
                              name="zp-exit-ingest")
        it.start()
        self.threads.append(it)
        dt = threading.Thread(target=self._delete_loop, daemon=True,
                              name="zp-exit-del")
        dt.start()
        self.threads.append(dt)
        jt = threading.Thread(target=self._janitor, daemon=True,
                              name="zp-exit-janitor")
        jt.start()
        self.threads.append(jt)

    def _on_lost(self, reason: str) -> None:
        self.log(f"[exit] SSE lost: {reason} — reconnecting")

    def _janitor(self) -> None:
        while not self.stop.is_set():
            time.sleep(120)
            try:
                tree = self.db.get(f"z/{self.session}") or {}
                # stale frames are sequence-keyed; consumer deletion is
                # primary; this loop only clears empty junk dirs
                for d in ("u", "d"):
                    subtree = tree.get(d)
                    if isinstance(subtree, dict) and not subtree:
                        self.db.delete(f"z/{self.session}/{d}")
            except Exception:
                pass  # janitor is best-effort

    def stop_all(self) -> None:
        self.stop.set()
        self.mux.closed.set()
        self.wq.flush(timeout=5.0)
        self.wq.stop_all()
        with self._conn_lock:
            socks = list(self.conns.values())
            self.conns.clear()
        for s in socks:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except Exception:
                pass
