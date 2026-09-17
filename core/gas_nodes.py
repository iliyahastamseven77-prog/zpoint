"""Zpoint daemon nodes over the Google Apps Script carrier.

Property of the @ily_bio research channel (@iliyahsatam).

Same behavior as core/client.py / core/exit.py, but frames travel in
sealed batches over a pool of Apps Script Web Apps (core/gas.py) instead
of Firebase RTDB PUT/SSE/DELETE.

Ingest threading mirrors the v2 audit rules:
  - ONE ingest thread per direction (no per-packet threads);
  - socket installed before reader thread starts; shutdown() before close();
  - teardown idempotent via _closing set; T_CLOSE exactly once;
  - no async delete-GC loop — CacheService TTL (6h) is the GC.
"""
from __future__ import annotations

import queue
import socket
import threading
import time
from typing import Optional

from .crypto import FrameCrypto
from .gas import (ENV_LABEL_DN, ENV_LABEL_UP, EnvelopeCrypto, GASDownloader,
                  GASPool, load_gas_config)
from .ingest import FrameIngest
from .mux import (Mux, T_CLOSE, T_CREDIT, T_DATA, T_OPEN, unpack_frame)
from .net import socks5_serve


class GASClientNode:
    """SOCKS5 -> mux -> sealed batches over the GAS script pool."""

    def __init__(self, urls: list[str], token: str, session_id: str,
                 key: bytes, up_prefix: bytes, down_prefix: bytes,
                 listen: tuple[str, int] = ("127.0.0.1", 1086),
                 client_id: str = "c1", log_fn=print, workers: int = 4):
        self.session = session_id
        self.listen = listen
        self.client_id = client_id
        self.stop = threading.Event()
        self.log = log_fn
        self.crypto = FrameCrypto(key, up_prefix)         # client->exit
        self.crypto_down = FrameCrypto(key, down_prefix)  # exit->client
        self.env_up = EnvelopeCrypto(key, ENV_LABEL_UP)
        self.env_dn = EnvelopeCrypto(key, ENV_LABEL_DN)
        self.mux = Mux(self._send_up, self.crypto, is_server=False,
                       client_id=client_id)
        self.pool = GASPool(urls, token, self.env_up, is_up=True,
                            log_fn=log_fn, workers=workers)
        self.stats = dict(self.pool.stats)
        self.stats.update({"frames_in": 0, "bytes_in": 0, "bytes_out": 0,
                           "streams": 0})
        # downlink: one long-poll thread per script -> assembler -> ingest
        self.ingest = FrameIngest(log_fn=log_fn)
        self._rx_q: queue.Queue = queue.Queue(maxsize=2048)
        self._ingest_thread: Optional[threading.Thread] = None
        self._downloader: Optional[GASDownloader] = None
        self.meter = None   # optional BandwidthMeter, set by zpointd

    # ---------- outbound ----------
    def _send_up(self, obj: dict) -> None:
        # ALL frames of one stream (o/d/c/w) share ONE lane -> ONE pool
        # worker -> worker_seq order == mux seq order, and OPEN is
        # guaranteed to precede that stream's DATA (deterministic, unlike
        # a separate control lane which races DATA onto another worker).
        lane = f"u:{obj.get('s', 0)}"
        self.pool.submit(f"z/{self.session}/u", obj, key=lane)
        self.stats["frames_out"] = self.pool.stats["frames_out"]

    # ---------- inbound ----------
    def _consume(self, node: dict) -> None:
        self.stats["frames_in"] += 1
        try:
            _, sid, ftype, body = unpack_frame(node, self.crypto_down)
            if ftype == T_DATA:
                self.stats["bytes_in"] += len(body)
                if self.meter:
                    self.meter.add(rx=len(body))
                self.mux.deliver_received(sid, body)
            elif ftype == T_CREDIT:
                self.mux.apply_credit(sid, int(body.decode() or 0))
            elif ftype == T_CLOSE:
                self.mux.peer_closed(sid)
            elif ftype == T_OPEN:
                self.mux.peer_opened(sid)   # defensive; client never opens
        except Exception as e:
            self.log(f"[gas-client] bad frame: {e}")

    def _ingest_loop(self) -> None:
        while not self.stop.is_set():
            try:
                node = self._rx_q.get(timeout=0.5)
            except queue.Empty:
                continue
            self.ingest.accept(node, self._consume)

    def _on_batch_frame(self, node: dict) -> None:
        """GASDownloader callback (poll threads) -> enqueue only."""
        try:
            self._rx_q.put(node, timeout=5.0)
        except queue.Full:
            self.log("[gas-client] rx queue full — frame dropped")

    # ---------- SOCKS (same contract as core/client.py) ----------
    def _socks_handler(self, client_sock, target):
        self.stats["streams"] += 1
        sid = self.mux.open_stream(target)
        self.log(f"[gas-client] stream {sid} -> {target[0]}:{target[1]}")

        def to_mux():
            try:
                while not self.stop.is_set():
                    data = client_sock.recv(65536)
                    if not data:
                        break
                    self.stats["bytes_out"] += len(data)
                    if self.meter:
                        self.meter.add(tx=len(data))
                    self.mux.write(sid, data)
            except (ConnectionError, OSError):
                pass
            finally:
                self.mux.close_stream(sid)

        threading.Thread(target=to_mux, daemon=True).start()
        while not self.stop.is_set():
            data = self.mux.recv(sid, timeout=0.5)
            if data is None:
                if sid not in self.mux._streams:
                    break
                continue
            try:
                client_sock.sendall(data)
                if self.meter:
                    self.meter.add(rx=len(data))
            except OSError:
                break
        self.mux.close_stream(sid)
        try:
            client_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            client_sock.close()
        except Exception:
            pass

    # ---------- lifecycle ----------
    def start(self) -> None:
        lst = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lst.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lst.bind(self.listen)
        lst.listen(64)
        self.log(f"[gas-client] SOCKS5 on {self.listen[0]}:{self.listen[1]} "
                 f"session={self.session} pool={len(self.pool.carriers)}")
        socks5_serve(lst, self._socks_handler)
        self._ingest_thread = threading.Thread(target=self._ingest_loop,
                                               daemon=True,
                                               name="zp-gas-cli-ingest")
        self._ingest_thread.start()
        self._downloader = GASDownloader(
            [c.url for c in self.pool.carriers],
            self.pool.carriers[0].token, self.env_dn, "dn",
            self._on_batch_frame, self.log)
        self._downloader.start()

    def stop_all(self) -> None:
        self.stop.set()
        self.mux.closed.set()
        if self._downloader:
            self._downloader.stop_all()
        self.pool.flush(timeout=3.0)
        self.pool.stop_all()


class GASExitNode:
    """Long-polls the uplink, opens real TCP, answers on the downlink."""

    def __init__(self, urls: list[str], token: str, session_id: str,
                 key: bytes, up_prefix: bytes, down_prefix: bytes,
                 log_fn=print, workers: int = 4):
        self.session = session_id
        self.stop = threading.Event()
        self.log = log_fn
        self._client_id: Optional[str] = None
        self.crypto = FrameCrypto(key, up_prefix)         # decrypt client->exit
        self.crypto_down = FrameCrypto(key, down_prefix)  # seal exit->client
        self.env_up = EnvelopeCrypto(key, ENV_LABEL_UP)
        self.env_dn = EnvelopeCrypto(key, ENV_LABEL_DN)
        self.pool = GASPool(urls, token, self.env_dn, is_up=False,
                            log_fn=log_fn, workers=workers)
        self.mux = Mux(self._send_down, self.crypto_down, is_server=True)
        self.mux.on_open = self._on_open
        self.mux.on_data = self._on_data
        self.mux.on_close = self._on_close
        self.stats = dict(self.pool.stats)
        self.stats.update({"frames_in": 0, "bytes_in": 0, "bytes_out": 0,
                           "streams": 0})
        self.ingest = FrameIngest(log_fn=log_fn)
        self._rx_q: queue.Queue = queue.Queue(maxsize=2048)
        self.conns: dict[int, socket.socket] = {}
        self._conn_lock = threading.Lock()
        self._closing: set[int] = set()
        self._closing_lock = threading.Lock()
        self._downloader: Optional[GASDownloader] = None
        # Janitor (BUGFIX #19): kills TCP sockets whose OPEN was delivered
        # but whose traffic died — the classic "app vanished mid-handshake
        # / phone went offline" leak (real-world evidence: orphan OPENs for
        # Telegram DC IPs with no follow-up).  120s covers the slowest
        # legitimate TLS handshake + typical keep-alive gaps.  Idle times
        # live in a side table (socket.socket uses __slots__ — no room for
        # custom attributes).
        self._tcp_idle_s = 120.0
        self._last_rx: dict[int, float] = {}
        self._janitor: Optional[threading.Thread] = None

    # ---------- outbound ----------
    def _send_down(self, obj: dict) -> None:
        cid = self._client_id or "c1"      # learned from first OPEN
        # same lane rule as the client: per-stream lanes keep OPEN-before-DATA
        lane = f"d:{obj.get('s', 0)}"
        self.pool.submit(f"z/{self.session}/d/{cid}", obj, key=lane)
        self.stats["frames_out"] = self.pool.stats["frames_out"]

    # ---------- inbound ----------
    def _consume(self, node: dict) -> None:
        try:
            self.mux.deliver(node, self.crypto)
            self.stats["frames_in"] += 1
        except Exception as e:
            self.log(f"[gas-exit] bad frame: {e}")

    def _ingest_loop(self) -> None:
        while not self.stop.is_set():
            try:
                node = self._rx_q.get(timeout=0.5)
            except queue.Empty:
                continue
            self.ingest.accept(node, self._consume)

    def _on_batch_frame(self, node: dict) -> None:
        try:
            self._rx_q.put(node, timeout=5.0)
        except queue.Full:
            self.log("[gas-exit] rx queue full — frame dropped")

    # ---------- TCP bridge (same as core/exit.py, v2-audited) ----------
    def _on_data(self, sid: int, data: bytes) -> None:
        self.stats["bytes_in"] += len(data)
        s = self._socket_for(sid)
        if s is not None:
            with self._conn_lock:
                self._last_rx[sid] = time.time()   # janitor bookkeeping
            try:
                s.sendall(data)
            except OSError as e:
                self.log(f"[gas-exit] send to target failed: {e}")
                self._teardown_stream(sid, send_close=True)
        else:
            self.log(f"[gas-exit] data for stream {sid} with no socket — dropped")

    def _socket_for(self, sid: int) -> Optional[socket.socket]:
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
        # OPEN payload "<client_id>@host:port" — strip the @ part
        # UNCONDITIONALLY (BUGFIX #8).
        if target and "@" in target[0]:
            cid, _, real_host = target[0].partition("@")
            if cid:
                if self._client_id is None:
                    self._client_id = cid
                    self.log(f"[gas-exit] client_id learned: {cid}")
            target = (real_host, target[1])

        def worker():
            s = None
            try:
                s = socket.create_connection(target, timeout=15)
                s.settimeout(None)
                with self._conn_lock:
                    self._last_rx[sid] = time.time()  # janitor bookkeeping
                    if sid in self._closing:
                        raise ConnectionError("stream already closed")
                    self.conns[sid] = s
                self.log(f"[gas-exit] stream {sid} -> {target[0]}:{target[1]}")

                def to_mux():
                    try:
                        while not self.stop.is_set():
                            data = s.recv(65536)
                            if not data:
                                break
                            self.stats["bytes_out"] += len(data)
                            with self._conn_lock:
                                self._last_rx[sid] = time.time()
                            self.mux.write(sid, data)
                    except OSError:
                        pass
                    finally:
                        self._teardown_stream(sid, send_close=True)

                threading.Thread(target=to_mux, daemon=True,
                                 name=f"zp-gas-tgt-{sid}").start()
            except Exception as e:
                self.log(f"[gas-exit] connect {target} failed: {e}")
                if s is not None:
                    try:
                        s.close()
                    except Exception:
                        pass
                self._teardown_stream(sid, send_close=True)

        threading.Thread(target=worker, daemon=True,
                         name=f"zp-gas-conn-{sid}").start()

    def _teardown_stream(self, sid: int, send_close: bool) -> None:
        first = False
        with self._closing_lock:
            if sid not in self._closing:
                self._closing.add(sid)
                first = True
        with self._conn_lock:
            s = self.conns.pop(sid, None)
        if s is not None:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except Exception:
                pass
        if first and send_close:
            self.mux.close_stream(sid)
        if first:
            with self._closing_lock:
                self._closing.discard(sid)

    def _on_close(self, sid: int) -> None:
        self._teardown_stream(sid, send_close=False)

    # ---------- lifecycle ----------
    def start(self) -> None:
        self.log(f"[gas-exit] session={self.session} pool="
                 f"{len(self.pool.carriers)} — long-polling uplink")
        self._downloader = GASDownloader(
            [c.url for c in self.pool.carriers],
            self.pool.carriers[0].token, self.env_up, "up",
            self._on_batch_frame, self.log)
        self._downloader.start()
        threading.Thread(target=self._ingest_loop, daemon=True,
                         name="zp-gas-exit-ingest").start()
        self._janitor = threading.Thread(target=self._janitor_loop,
                                         daemon=True, name="zp-gas-janitor")
        self._janitor.start()

    def _janitor_loop(self) -> None:
        """BUGFIX #19: reap sockets whose stream died before its OPEN
        reached a socket entry (client cancelled mid-handshake, phone
        switched networks) or whose peer vanished without a T_CLOSE.
        without this, every aborted OPEN leaks one socket (and its
        exit-side credits) until process death."""
        while not self.stop.is_set():
            time.sleep(15.0)
            if self.stop.is_set():
                break
            now = time.time()
            doomed: list[int] = []
            with self._conn_lock:
                for sid in list(self.conns.keys()):
                    if now - self._last_rx.get(sid, now) > self._tcp_idle_s:
                        doomed.append(sid)
            for sid in doomed:
                self.log(f"[gas-exit] janitor: reaping idle stream {sid}")
                self._teardown_stream(sid, send_close=True)

    def stop_all(self) -> None:
        self.stop.set()
        self.mux.closed.set()
        if self._downloader:
            self._downloader.stop_all()
        self.pool.flush(timeout=3.0)
        self.pool.stop_all()
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


def node_from_config(cfg: dict, role: str, log_fn=print,
                     listen=("127.0.0.1", 1086)):
    """Build GASClientNode/GASExitNode from a transport=gas config dict."""
    urls, token = load_gas_config(cfg)
    import base64 as _b64
    b64d = lambda s: _b64.urlsafe_b64decode(s.encode())
    key = b64d(cfg["key"])
    up, down = b64d(cfg["up_prefix"]), b64d(cfg["down_prefix"])
    session = cfg["session"]
    if role == "client":
        return GASClientNode(urls, token, session, key, up, down,
                             listen=listen,
                             client_id=cfg.get("client_id", "c1"),
                             log_fn=log_fn)
    return GASExitNode(urls, token, session, key, up, down, log_fn=log_fn)
