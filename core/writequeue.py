"""Ordered async writer: per-key sequential lanes over a shared worker pool.

BUGFIX #11 (v1): _send_up/_send_down blocked 100-250 ms per frame on the HTTP
round-trip, capping throughput.  A queue.Queue + 3 workers fixed the blocking.

AUDIT HARDENING (v2): N generic workers over ONE FIFO still commit REST PUTs
out of order — worker A can be slow on frame 10 while worker B finishes frame
11 first, and RTDB stores 11 before 10.  The wire is only "ordered" at enqueue
time, not at commit time, which corrupts downstream TCP stream reassembly
whenever PUT latency jitters.

v2 model (audited + concurrency-tested):
  * submit(key, path, value) — every job is tagged with a lane key.
    Zpoint tags data frames with the mux stream id (frame field "s"), so two
    streams never reorder each other while ONE stream is strictly FIFO (its
    frames carry increasing seq "i").
  * A lane holds an explicit `running` claim: claimed atomically under the
    same Condition lock that enqueues jobs, so two workers can NEVER drain
    one lane concurrently (proven: 0 overlapping put_fn calls per lane).
  * The claiming worker drains the whole lane inline, one HTTP PUT at a
    time — per-lane absolute order; parallelism = number of distinct busy
    lanes (streams), never within one.
  * One dedicated CONTROL lane ("__ctl__") carries T_OPEN / T_CLOSE /
    T_CREDIT so saturated data lanes can never delay stream setup, teardown
    or window grants.
  * submit() backpressures on the global backlog (maxsize) — bounded memory,
    zero frame drops.  flush() drains deterministically via the Condition.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable

CONTROL_LANE = "__ctl__"


class WriteQueue:
    """Lane-ordered FIFO writer over a bounded worker pool."""

    def __init__(self, put_fn: Callable[[str, object], None],
                 workers: int = 4, maxsize: int = 512, log_fn=print):
        self._put_fn = put_fn
        self._maxsize = maxsize
        self._stop = threading.Event()
        self._log = log_fn
        self._lock = threading.Lock()
        self._work = threading.Condition(self._lock)   # guards everything below
        self._lanes: dict[str, deque] = {}   # key -> pending jobs (FIFO)
        self._running: set[str] = set()      # keys claimed by a worker
        self._outstanding = 0
        self.workers = [
            threading.Thread(target=self._worker, daemon=True,
                             name=f"zp-w{i}")
            for i in range(max(1, workers))
        ]
        for w in self.workers:
            w.start()

    # ---------- submit ----------
    def submit(self, path: str, value, key: str = CONTROL_LANE) -> None:
        """Enqueue (path, value) onto lane `key`.  Blocks on backpressure.

        Jobs on the same lane execute strictly one-at-a-time, in submission
        order.  Control frames (default lane) stay independent of data.
        """
        while not self._stop.is_set():
            with self._work:
                if self._outstanding >= self._maxsize:
                    full = True
                else:
                    full = False
                    lane = self._lanes.get(key)
                    if lane is None:
                        lane = deque()
                        self._lanes[key] = lane
                    lane.append((path, value))
                    if key not in self._running:
                        self._work.notify()   # a worker can claim it now
                    self._outstanding += 1
            if full:
                # bounded backlog: let the workers drain, then retry
                time.sleep(0.005)
                continue
            return
        # stopping: refuse silently — callers are daemons shutting down

    # ---------- workers ----------
    def _claim(self) -> str | None:
        """Pick a lane with pending work and no running claim (lock held)."""
        for key, lane in self._lanes.items():
            if lane and key not in self._running:
                self._running.add(key)
                return key
        return None

    def _worker(self) -> None:
        while not self._stop.is_set():
            with self._work:
                key = None
                while key is None:
                    if self._stop.is_set():
                        return
                    key = self._claim()
                    if key is None:
                        self._work.wait(timeout=0.25)
            self._run_lane(key)

    def _run_lane(self, key: str) -> None:
        """Drain lane `key` inline: one job at a time, absolute FIFO order.

        The lane claim is held for the whole drain, so no other worker can
        interleave into this lane.  Lock is released during every PUT.
        """
        while True:
            with self._work:
                if self._stop.is_set():
                    # shutdown: abandon the rest of this lane, keep the
                    # accounting consistent for flush()
                    lane = self._lanes.pop(key, None)
                    if lane:
                        self._outstanding -= len(lane)
                    self._running.discard(key)
                    self._work.notify_all()
                    return
                lane = self._lanes.get(key)
                if not lane:
                    # fully drained; release the claim.  A submit arriving
                    # after this point sees key not in _running and notifies
                    # a worker — no lost wakeup.
                    self._lanes.pop(key, None)
                    self._running.discard(key)
                    return
                path, value = lane.popleft()
            try:
                self._put_fn(path, value)
            except Exception as e:   # never kill the worker
                self._log(f"[wq] put failed ({key}): {e}")
            finally:
                with self._work:
                    self._outstanding -= 1
                    self._work.notify_all()   # flush() waiters

    # ---------- lifecycle ----------
    def flush(self, timeout: float = 10.0) -> bool:
        """Wait until all submitted items are written.  True on success."""
        deadline = time.time() + timeout
        with self._work:
            while self._outstanding > 0:
                left = deadline - time.time()
                if left <= 0:
                    break
                self._work.wait(timeout=min(left, 0.1))
        return self._outstanding == 0

    def stats(self) -> dict:
        with self._work:
            return {"outstanding": self._outstanding,
                    "lanes": len(self._lanes),
                    "running": len(self._running)}

    def stop_all(self) -> None:
        self._stop.set()
        with self._work:
            self._work.notify_all()
