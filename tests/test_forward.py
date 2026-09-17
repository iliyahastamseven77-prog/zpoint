#!/usr/bin/env python3
"""E2E forward tests for ExitNode: real TCP bridge through the mux.
Property of the @ily_bio research channel (@iliyahsatam).
Run: python3 tests/test_forward.py
"""
import socket
import sys
import threading
import time
from collections import deque

sys.path.insert(0, "/root/projects/zpoint")

from core.crypto import FrameCrypto, new_key, new_dir_prefix  # noqa: E402
from core.mux import Mux  # noqa: E402
from core.exit import ExitNode  # noqa: E402


class FakeDB:
    """In-memory RTDB: routes PUTs like Firebase would; SSE = callback."""

    def __init__(self):
        self.up = {}
        self.down = {}
        self.on_down = None      # called with each downlink frame
        self.on_up = None        # called with each uplink frame

    def put(self, p, v):
        if "/u/" in p:
            self.up[p] = v
            if self.on_up:
                self.on_up(v)
        else:
            self.down[p] = v
            if self.on_down:
                self.on_down(v)

    def get(self, p):
        return self.up.get(p)

    def delete(self, p):
        self.up.pop(p, None)
        self.down.pop(p, None)


def make_echo_server():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    open_conns = []

    def echo():
        while True:
            c, _ = srv.accept()
            open_conns.append(c)

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
    return srv, port, open_conns


def main():
    srv, tport, open_conns = make_echo_server()
    k, up, down = new_key(), new_dir_prefix(), new_dir_prefix()

    node = ExitNode("https://mock", "", "sess1", k, up, down,
                    log_fn=lambda s: None)
    node.db = FakeDB()

    # wire: client uplink frames -> exit._handle (as SSE would)
    rx_down = FrameCrypto(k, down)

    def client_send(obj):
        threading.Thread(target=node._handle, args=(obj,),
                         daemon=True).start()

    cm = Mux(client_send, FrameCrypto(k, up), is_server=False,
             client_id="c1")
    received = deque()
    cm.on_data = lambda sid, b: received.append(b)

    # exit downlink frames -> client mux (as SSE would)
    node.db.on_down = lambda v: cm.deliver(v, rx_down)

    # ---- test 1: roundtrip ----
    sid1 = cm.open_stream(("127.0.0.1", tport))
    deadline = time.time() + 2
    while sid1 not in node.conns and time.time() < deadline:
        time.sleep(0.02)
    assert sid1 in node.conns, "exit did not open TCP (gaierror-style bug?)"
    print("1) OPEN stream 1 -> real TCP OK")

    cm.write(sid1, b"hello exit server")
    deadline = time.time() + 3
    while sum(len(b) for b in received) < 17 and time.time() < deadline:
        time.sleep(0.02)
    got1 = b"".join(received); received.clear()
    assert got1 == b"HELLO EXIT SERVER", got1
    print("2) roundtrip up+down OK:", got1)

    # ---- test 2: SECOND stream must also connect (the '@' bug) ----
    sid2 = cm.open_stream(("127.0.0.1", tport))
    deadline = time.time() + 2
    while sid2 not in node.conns and time.time() < deadline:
        time.sleep(0.02)
    assert sid2 in node.conns, \
        f"stream 2 failed to connect — '@' not stripped: {node.conns}"
    cm.write(sid2, b"second stream")
    deadline = time.time() + 3
    while sum(len(b) for b in received) < 13 and time.time() < deadline:
        time.sleep(0.02)
    got2 = b"".join(received); received.clear()
    assert got2 == b"SECOND STREAM", got2
    print("3) OPEN stream 2 (the '@' bug) OK:", got2)

    # ---- test 3: target EOF closes the mux stream (T_CLOSE to client) ----
    # Close the EXIT-SIDE socket (what to_mux reads from) via a proper
    # shutdown so recv() unblocks with EOF and the finally-block fires.
    target_sock = node.conns.get(sid1)
    target_sock.shutdown(socket.SHUT_RDWR)
    target_sock.close()
    # T_CLOSE goes through the async WriteQueue — poll until propagated
    deadline = time.time() + 4
    while sid1 in cm._streams and time.time() < deadline:
        time.sleep(0.02)
    assert sid1 not in cm._streams, "T_CLOSE not propagated to client"
    print("4) target EOF -> T_CLOSE reached client OK")

    cm.close_stream(sid2)
    time.sleep(0.3)
    assert sid2 not in node.conns, "exit socket not closed on client FIN"
    print("5) client FIN -> exit socket closed OK")

    node.stop_all()
    srv.close()
    print("\n✅ ALL FORWARD TESTS PASSED")


if __name__ == "__main__":
    main()
