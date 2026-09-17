"""Low-level SOCKS5 listener (client side) + TCP bridge helpers (exit side).

SOCKS5: no-auth CONNECT only — enough for browsers/proxified apps.

AUDIT HARDENING (v2): the accept loop no longer dies on EBADF from a
listener closed during shutdown; _socks_session closes the socket on every
rejection path; pump() preserves the old select() behaviour for exit-side
bridges and closes both sockets exactly once via finally.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import select
import socket
import struct
import threading
from typing import Optional


def _read_exact(c: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = c.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socks peer closed")
        buf += chunk
    return buf


def socks5_serve(listener: socket.socket, handler) -> threading.Thread:
    """handler(client_sock, target) runs in a thread per connection."""

    def loop():
        while True:
            try:
                c, _ = listener.accept()
            except OSError:
                return          # listener closed (shutdown) — clean exit
            threading.Thread(target=_socks_session, args=(c, handler),
                             daemon=True).start()

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t


def _socks_session(c: socket.socket, handler) -> None:
    try:
        c.settimeout(30)
        # greeting: VER NMETHODS METHODS
        hdr = _read_exact(c, 2)
        if hdr[0] != 5:
            c.close(); return
        _read_exact(c, hdr[1])          # methods (we answer no-auth)
        c.sendall(b"\x05\x00")
        # request: VER CMD RSV ATYP ADDR PORT
        req = _read_exact(c, 4)
        if req[1] != 1:                  # CONNECT only
            c.sendall(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00"); c.close(); return
        atyp = req[3]
        if atyp == 1:
            host = socket.inet_ntoa(_read_exact(c, 4))
        elif atyp == 3:
            ln = _read_exact(c, 1)[0]
            host = _read_exact(c, ln).decode("idna")
        elif atyp == 4:
            host = socket.inet_ntop(socket.AF_INET6, _read_exact(c, 16))
        else:
            c.close(); return
        port = struct.unpack("!H", _read_exact(c, 2))[0]
        # BUGFIX #2: reply success BEFORE handing the socket to the handler,
        # otherwise apps wait for the CONNECT reply and time out.
        # VER=5 REP=0 RSV=0 ATYP=1 BND.ADDR=0.0.0.0 BND.PORT=0
        try:
            c.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        except OSError:
            c.close(); return
        c.settimeout(None)
        handler(c, (host, port))
    except Exception:
        try: c.close()
        except Exception: pass


def pump(a: socket.socket, b: socket.socket,
         on_done: Optional[callable] = None) -> None:
    """Bidirectional pump between two sockets until either side closes."""
    try:
        a.setblocking(False); b.setblocking(False)
        socks = [a, b]
        while True:
            r, _, x = select.select(socks, [], socks, 5.0)
            if x:
                break
            if not r:
                continue
            for s in r:
                try:
                    data = s.recv(65536)
                except (BlockingIOError, InterruptedError):
                    continue
                if not data:
                    raise ConnectionError("eof")
                dst = b if s is a else a
                sent = 0
                while sent < len(data):
                    try:
                        sent += dst.send(data[sent:])
                    except (BlockingIOError, InterruptedError):
                        select.select([], [dst], [], 1.0)
    except OSError:
        pass
    finally:
        # FIN before close: half-close cleanly, unblock the remote reader
        for s in (a, b):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except Exception:
                pass
        if on_done:
            on_done()
