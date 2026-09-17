"""Google Apps Script transport for Zpoint (strict google.com-only networks).

Property of the @ily_bio research channel (@iliyahsatam).

Replaces the Firebase RTDB carrier when only script.google.com is reachable.
Core modules (mux, crypto, writequeue, net, ingest) are UNCHANGED; only the
carrier swaps:

  RTDB (old):  PUT z/{s}/u/{seq} .. SSE push .. DELETE (consumer GC)
  GAS  (new):  POST sealed batches to a pool of Web Apps .. GET long-poll ..
               CacheService TTL 6h (server-side GC, zero extra requests)

WIRE MODEL
  mux frames (i/s/t/b, AES-256-GCM end-to-end) are grouped into BATCHES by
  pool workers.  A batch is sealed into ONE opaque envelope:

      env_ct = AESGCM(nonce = env_label(4B) + env_id(8B BE), aad="zpoint-batch-v1")
      item   = {"id": env_id, "w": worker_id, "n": worker_seq, "b": b64(env_ct)}

  POST /exec?k=<token>&dir=up|dn   body {"v":1,"f":[item,...]}
    -> the script stores the item list in its ring (its OWN ring id `c`),
  GET  /exec?k=<token>&mode=up|dn&after=<ringC>&wait=<s>
    -> {"ok":true,"b":[{"c":ringC,"f":[item,...]},...]}

ORDERING (the jitter buffer)
  * Every mux stream's frames ride ONE lane -> ONE pool worker (WriteQueue
    lane contract).  A worker POSTs batches strictly serially, so its
    worker_seq n is gapless WHEN DELIVERED IN ORDER.
  * Steady state: a worker pins to its HOME script (carriers[w % n]) and
    that script's poller delivers its items in ring order = n order.
  * Failover (home down -> batch shipped to another script) can reorder
    arrival.  The receiver keeps a SMALL per-worker reorder buffer: items
    are delivered in worker_seq order; a gap is held up to HOLD_MS (a
    genuine loss — pool exhausted — is flushed after the hold).  This is
    the jitter buffer; it needs only (w, n) — no global clock.

CRYPTO SAFETY
  env_id comes from a POOL-LEVEL monotonic counter (never per-worker), so
  every envelope of a direction uses a distinct nonce.  Envelope labels are
  distinct per direction AND distinct from frame nonce prefixes, so frame
  nonces (prefix + seq) and envelope nonces can never collide.

QUOTA NOTES (developers.google.com/apps-script/guides/services/quotas):
  UrlFetchApp is never used (20k/day URL Fetch quota irrelevant);
  doGet/doPost web invocations have no published daily cap; long-polls are
  capped at 240s vs the 6-min execution limit; the pool spreads in-flight
  executions under the 30-per-user cap and isolates rate-limit hits.
"""
from __future__ import annotations

import base64
import hashlib
import http.client  # noqa: F401  (parity with core/rtdb.py imports)
import json
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Optional

from .crypto import FrameCrypto

_UA = "Zpoint/0.2 (research; @ily_bio)"
_SSL_CTX = ssl.create_default_context()
_REDIRECTS = (301, 302, 307, 308)
_MAX_FOLLOW = 5

FLUSH_MS = 120            # batch accumulation window per pool worker
MAX_WORKERS = 8           # pool worker cap (lanes map round-robin onto these)
BATCH_WIRE_CAP = 96 * 1024   # <= CacheService 100KB/key limit (JSON on the wire)
BATCH_CAP = 64            # hard frames-per-batch ceiling (server allows 128)

POLL_WAIT_S = 50          # server-side long-poll hold (script caps at 240)
POLL_TIMEOUT_S = POLL_WAIT_S + 15   # client-side socket timeout
POLL_ERR_SLEEP = 1.5      # pause after a failed poll iteration
HOLD_MS = 1000            # per-worker reorder hold before flushing a gap


# =====================================================================
# Low-level HTTP carrier (stdlib only, mirrors core/rtdb.py conventions)
# =====================================================================

class GASCarrier:
    """One deployed Web App: POST a batch, long-poll items, ping."""

    def __init__(self, url: str, token: str, name: str = ""):
        u = urllib.parse.urlparse(url)
        if u.scheme != "https" or not u.hostname:
            raise ValueError(f"gas url must be https: {url!r}")
        if "script.google.com" not in u.hostname and \
                u.hostname not in ("google.com", "www.google.com"):
            raise ValueError(f"gas url host not allowed: {u.hostname}")
        self.url = url.rstrip("/")
        self.token = token
        self.name = name or u.hostname
        self._url_lock = threading.Lock()
        self._post_url: Optional[str] = None   # redirect-resolved POST target
        self.bytes_up = 0
        self.bytes_down = 0
        self.requests = 0
        self.errors = 0

    # ---------------- helpers ----------------
    def _params(self, extra: dict) -> dict:
        p = {"k": self.token}
        p.update(extra)
        return p

    @staticmethod
    def _full(url: str, path: str, params: dict) -> str:
        sep = "&" if "?" in url else "?"
        q = urllib.parse.urlencode(params)
        return f"{url}/{path}{sep}{q}" if path else f"{url}{sep}{q}"

    def _drain_redirects(self, url: str, data: Optional[bytes],
                         headers: dict, method: str):
        """Follow redirects EXPLICITLY (302/307 to googleusercontent) like
        core/rtdb.py does — urllib's auto-follow re-issues POST bodies
        unreliably across hosts."""
        req = urllib.request.Request(url, data=data, method=method,
                                     headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=POLL_TIMEOUT_S,
                                        context=_SSL_CTX) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            if e.code not in _REDIRECTS:
                raise
            final = url
            for _ in range(_MAX_FOLLOW):
                loc = e.headers.get("Location", "")
                final = urllib.parse.urljoin(final, loc)
                req = urllib.request.Request(final, data=data, method=method,
                                             headers=headers)
                try:
                    with urllib.request.urlopen(req, timeout=POLL_TIMEOUT_S,
                                                context=_SSL_CTX) as r:
                        return r.status, r.read()
                except urllib.error.HTTPError as e2:
                    if e2.code not in _REDIRECTS:
                        raise
                    e = e2
            raise RuntimeError(f"too many redirects on {url}")

    def _post_json(self, params: dict, body: bytes) -> dict:
        headers = {"User-Agent": _UA, "Content-Type": "application/json"}
        with self._url_lock:
            base = self._post_url
        url = self._full(base, "exec", params) if base else \
            self._full(self.url, "exec", params)
        status, raw = self._drain_redirects(url, body, headers, "POST")
        if base is None and status == 200:
            marker = "/exec"
            if marker in url:
                with self._url_lock:
                    self._post_url = url.split(marker, 1)[0]
        if status != 200:
            raise RuntimeError(f"POST status {status}")
        return json.loads(raw.decode())

    # ---------------- public API ----------------
    def post_items(self, direction: str, items: list[dict]) -> dict:
        """Ship sealed items; server wraps them in its own ring batch."""
        data = json.dumps({"v": 1, "f": items}, separators=(",", ":")).encode()
        self.bytes_up += len(data)
        self.requests += 1
        try:
            return self._post_json(self._params({"dir": direction}), data)
        except Exception:
            self.errors += 1
            raise

    def poll(self, mode: str, after: int, wait_s: int,
             resync: bool = False) -> dict:
        """Long-poll: {"ok":true,"b":[{"c":ringC,"f":[item,...]},...]}."""
        params = self._params({"mode": mode, "after": str(int(after)),
                               "wait": str(int(wait_s))})
        if resync:
            params["resync"] = "1"
        url = self._full(self.url, "exec", params)
        self.requests += 1
        try:
            status, raw = self._drain_redirects(url, None,
                                                {"User-Agent": _UA}, "GET")
            if status != 200:
                raise RuntimeError(f"poll status {status}")
            self.bytes_down += len(raw)
            return json.loads(raw.decode())
        except Exception:
            self.errors += 1
            raise

    def ping(self) -> float:
        t0 = time.time()
        status, raw = self._drain_redirects(
            self._full(self.url, "exec", self._params({"mode": "ping"})),
            None, {"User-Agent": _UA}, "GET")
        if status != 200:
            raise RuntimeError(f"ping status {status}")
        if not json.loads(raw.decode()).get("pong"):
            raise RuntimeError("ping bad payload")
        return (time.time() - t0) * 1000.0


# =====================================================================
# Batch envelope crypto
# =====================================================================

class EnvelopeCrypto:
    """AES-256-GCM over batch envelopes, reusing FrameCrypto's nonce scheme
    with a DEDICATED 4B label so envelope nonces never collide with frame
    nonces (dir prefixes) or with the other direction."""

    def __init__(self, key: bytes, label: bytes):
        if len(label) != 4:
            raise ValueError("envelope label must be 4 bytes")
        self._fc = FrameCrypto(key, label)

    def seal(self, env_id: int, obj) -> bytes:
        raw = json.dumps(obj, separators=(",", ":")).encode()
        return self._fc.seal(env_id, raw, aad=b"zpoint-batch-v1")

    def open(self, env_id: int, ct: bytes):
        raw = self._fc.open(env_id, ct, aad=b"zpoint-batch-v1")
        return json.loads(raw.decode())


def token_mac(token: str) -> str:
    """Non-reversible log fingerprint of the shared token (no raw secrets
    in logs — Tor PT spec guidance)."""
    return hashlib.sha256(token.encode()).hexdigest()[:8]


# =====================================================================
# Receiver-side jitter buffer: per-worker (w, n) reorder + id dedupe
# =====================================================================

class ItemAssembler:
    """Delivers sealed items in per-worker seq order, exactly once.

    - dedupe by envelope id (failover double-store, ring replays);
    - per-worker hold buffer: n == expected delivers immediately, gaps are
      held up to HOLD_MS then flushed in n order (loss is logged, not
      silently skipped).
    """

    def __init__(self, deliver: Callable[[dict], None],
                 log_fn: Callable[[str], None] = print,
                 hold_ms: float = HOLD_MS):
        self._deliver = deliver
        self._log = log_fn
        self._hold_ms = hold_ms
        # BUGFIX #15 (hardening): RLock instead of Lock.  entry() runs on
        # poll threads whose downstream handler can call back into the
        # assembler (ingest congestion, node teardown), and sweep() can
        # fire from the sweeper thread while a delivery callback is still
        # inside this object — a plain Lock self-deadlocks on that
        # re-entrancy; RLock makes same-thread re-entry safe.
        self._lock = threading.RLock()
        self._seen_ids: set[int] = set()
        self._expected: dict[int, int] = {}        # wid -> next n
        self._hold: dict[int, dict[int, tuple]] = {}  # wid -> {n: (ts,item)}
        self._epoch_floor = 0          # ratchet: drops PRE-START replays only
        self._epochs: dict[int, int] = {}          # wid -> highest epoch seen
        self.stats = {"items_in": 0, "dupes": 0, "held_flushes": 0,
                      "stale_epoch": 0}

    def entry(self, item: dict) -> None:
        try:
            eid = int(item["id"])
            wid = int(item.get("w", 0))
            n = int(item.get("n", 0))
        except (KeyError, TypeError, ValueError):
            return
        with self._lock:
            exp = self._expected.get(wid, 1)
            h = self._hold.get(wid)
            if n < exp or (h and n in h):
                # stale replay OR an item already sitting in the hold
                # buffer (double-STORE race: two poll threads read the
                # same envelope from two ring keys before the ring-id
                # filter catches it) — seen_ids would absorb the replay
                # but NOT the parked hold copy, and marking it seen here
                # would make the REAL entry a duplicate later.
                self.stats["dupes"] += 1
                return
            if eid in self._seen_ids:
                self.stats["dupes"] += 1
                return
            ready: list[dict] = []
            if n == exp:
                self._seen_ids.add(eid)
                ready.append(item)
                self._expected[wid] = n + 1
            else:
                # future item: park in the hold buffer WITHOUT marking
                # seen — the id is registered only on actual delivery.
                self._hold.setdefault(wid, {})[n] = (time.time(), item)
            if ready and self._expected.get(wid) is not None:
                # drain whatever became contiguous behind the head
                h = self._hold.get(wid)
                if h:
                    nxt = self._expected[wid]
                    while nxt in h:
                        item2 = h.pop(nxt)[1]
                        ready.append(item2)
                        self._seen_ids.add(int(item2["id"]))
                        nxt += 1
                    self._expected[wid] = nxt
        # deliver outside the lock; items already in per-worker order
        for it in ready:
            self._emit(it)

    def _emit(self, item: dict) -> None:
        self._deliver(item)

    def sweep(self) -> None:
        """Flush per-worker holds whose head gap exceeded HOLD_MS.

        BUGFIX #15 (hardening): the old head-gap check tested
        ``exp not in h: continue`` — but exp is by definition the MISSING
        seq, so it could never be in h and the whole sweep was a no-op.
        Out-of-order streams stalled forever (a lost worker_seq from
        failover/cache churn parked every later item permanently).

        Correct semantics: hold UNTIL the deadline, then flush the
        contiguous run starting at the LOWEST AVAILABLE key.  Frames
        reordered ahead of their turn are re-parked for the next sweep;
        frames behind the hole are delivered in n order.  The 1s hole is
        a total loss for that stream anyway (mux has no seq replay
        guard), so continuing to wait cannot help — the pre-fix hold
        already spent that 1s doing nothing but starving everyone.
        """
        now = time.time()
        to_flush: list[dict] = []
        to_repark: list[dict] = []
        with self._lock:
            for wid, h in list(self._hold.items()):
                if not h:
                    self._hold.pop(wid, None)
                    continue
                # age of the OLDEST parked item of this worker
                oldest_ts = min(ts for ts, _ in h.values())
                if (now - oldest_ts) * 1000.0 < self._hold_ms:
                    continue                       # still inside the hold
                lowest = min(h)                    # earliest available n
                self._expected[wid] = lowest
                nxt = lowest
                while nxt in h:
                    entry_ts, item = h.pop(nxt)
                    if (now - entry_ts) * 1000.0 < self._hold_ms:
                        # arrived early (its own hold not yet expired) —
                        # keep it parked for the next sweep instead of
                        # delivering it out of turn
                        to_repark.append(item)
                        nxt += 1
                        continue
                    to_flush.append(item)
                    nxt += 1
                # next expectation is ALWAYS the first undelivered n —
                # even when the hold drained completely (else the stream
                # could never resume after the sweep) and even when a
                # fresh item was re-parked at nxt (it will flush on its
                # own clock in a later sweep).
                self._expected[wid] = nxt
                self.stats["held_flushes"] += 1
                self._log(f"[gas] worker {wid}: hold expired — "
                          f"flushing from n={lowest} "
                          f"({len(to_flush)} item(s); loss possible at the gap)")
        for it in to_repark:
            with self._lock:
                self._hold.setdefault(int(it.get("w", 0)), {})[
                    int(it.get("n", 0))] = (now, it)
        for it in to_flush:
            self._emit(it)

    def reset_worker(self, wid: int, n: int) -> None:
        """Generation reset (BUGFIX #18): the sender restarted and began
        numbering from n (usually 1) again while we still expected the
        OLD counter — without this reset every later envelope of the new
        generation looks 'stale' (n < expected) and the whole worker lane
        dies silently.  seen_ids is kept: a replayed old-generation
        envelope carries an OLDER epoch and is gated out before entry.
        """
        with self._lock:
            self._hold.pop(wid, None)
            self._expected[wid] = n

    def snapshot(self) -> dict:
        with self._lock:
            return {"items": self.stats["items_in"],
                    "dupes": self.stats["dupes"],
                    "held": sum(len(h) for h in self._hold.values()),
                    "held_flushes": self.stats["held_flushes"],
                    "workers": len(self._expected)}


# =====================================================================
# Send side: pool workers, batching per lane, home-pinned weighted RR
# =====================================================================

class _PoolWorker(threading.Thread):
    """Accumulates frames for FLUSH_MS, seals ONE envelope, POSTs it to its
    HOME script first; on failure fails over to the remaining scripts."""

    def __init__(self, pool: "GASPool", wid: int):
        super().__init__(daemon=True, name=f"zp-gas-w{wid}")
        self.pool = pool
        self.wid = wid
        self._q: list[tuple[str, dict]] = []      # (lane, frame)
        self._cv = threading.Condition()
        # NOTE: must NOT be named `_stop` — that shadows Thread._stop() and
        # breaks join() ('bool' object is not callable).
        self._stop_flag = False
        self.wseq = 0                              # per-worker order tag

    def submit(self, lane: str, frame: dict) -> None:
        with self._cv:
            self._q.append((lane, frame))
            self._cv.notify()

    def stop(self) -> None:
        with self._cv:
            self._stop_flag = True
            self._cv.notify()

    def run(self) -> None:
        while True:
            with self._cv:
                if not self._q and not self._stop_flag:
                    self._cv.wait(timeout=FLUSH_MS / 1000.0)
                batch, self._q = self._q, []
                stopping = self._stop_flag
            if batch:
                self._ship(batch)
            if stopping and not batch:
                return

    def _ship(self, items: list[tuple[str, dict]]) -> None:
        frames = [f for _, f in items]
        if not frames:
            return
        # BUGFIX #16 (hardening): the 64-frame BATCH_CAP is unreachable in
        # practice (a worker can hold at most ceil(FLUSH_MS)/mux-rate
        # frames per wake-up), but if it ever trips we REQUEUE the excess
        # instead of truncating it away — silent data loss is never a
        # valid batch policy.  Requeued frames keep their original lane
        # and go out in the next batch, in order.
        if len(frames) > BATCH_CAP:
            excess = items[BATCH_CAP:]
            frames = frames[:BATCH_CAP]
            with self._cv:
                self._q[:0] = excess
                self._cv.notify()
            self.pool.log(f"[gas] batch over {BATCH_CAP} frames — "
                          f"requeued {len(excess)} for the next batch")
        env_id = self.pool.next_env_id()
        self.wseq += 1
        item = seal_item(frames, env_id, self.pool.env, self.wid, self.wseq,
                         self.pool.epoch)
        wire = {"v": 1, "f": [item]}
        # BUGFIX #16 (hardening): wire-cap trim no longer silently DROPS
        # the newest frames (frames.pop() into nowhere).  Frames that
        # cannot fit inside the 96KB envelope are pushed back onto the
        # worker queue (front, lane order preserved) and ship with the
        # NEXT envelope — zero loss, envelope just stays <= BATCH_WIRE_CAP.
        overflow: list[tuple[str, dict]] = []
        while len(json.dumps(wire).encode()) > BATCH_WIRE_CAP and len(frames) > 1:
            frames.pop()
            overflow.insert(0, items[len(frames)])
            item = seal_item(frames, env_id, self.pool.env,
                             self.wid, self.wseq, self.pool.epoch)
            wire = {"v": 1, "f": [item]}
        if overflow:
            with self._cv:
                self._q[:0] = overflow
                self._cv.notify()
            self.pool.log(f"[gas] wire-cap: envelope {env_id} carries "
                          f"{len(frames)}/{len(items)} frame(s); "
                          f"{len(overflow)} requeued for the next batch")
        direction = "up" if self.pool.is_up else "dn"
        # HOME first (per-stream steady-state order), then failover
        order: list[GASCarrier] = []
        home = self.pool.carriers[self.wid % len(self.pool.carriers)]
        order.append(home)
        order += [c for c in self.pool.carriers if c is not home]
        for attempt, car in enumerate(order):
            try:
                reply = car.post_items(direction, wire["f"])
                if reply.get("ok"):
                    self.pool.mark_ok(car)
                    with self.pool._s_lock:
                        self.pool.stats["batches_out"] += 1
                        self.pool.stats["frames_out"] += len(frames)
                    return
                self.pool.mark_fail(car, f"reply {reply.get('err')}")
            except Exception as e:
                self.pool.mark_fail(car, f"{type(e).__name__}: {e}")
            time.sleep(min(0.5, 0.15 * (attempt + 1)))
        self.pool.log(f"[gas] envelope {env_id} UNDELIVERED "
                      f"({len(frames)} frames) — pool exhausted")


class GASPool:
    """Lane -> worker sharding + home-pinned failover over the script pool.

    Mirrors WriteQueue's lane contract: submit(path, frame, key=lane) with
    lane = f"u:{stream}" for data, CONTROL_LANE for control frames.  All
    frames of one lane map to ONE worker -> per-stream order + gapless
    worker_seq on the wire.
    """

    def __init__(self, urls: list[str], token: str, env: EnvelopeCrypto,
                 is_up: bool, log_fn: Callable[[str], None] = print,
                 workers: int = 4):
        if not urls:
            raise ValueError("gas pool needs at least one script URL")
        self.carriers = [GASCarrier(u, token, name=f"gas{i}")
                         for i, u in enumerate(dict.fromkeys(urls))]
        self.env = env
        self.is_up = is_up
        self.log = log_fn
        self.stats = {"batches_out": 0, "frames_out": 0}
        self._s_lock = threading.Lock()
        self._lane_map: dict[str, int] = {}
        self._next_w = 0
        self._w_lock = threading.Lock()
        self._cool: dict[int, float] = {}       # carrier idx -> cooldown until
        self._cool_lock = threading.Lock()
        # env ids seed from wall-clock ms and increase monotonically within
        # the process — never collide with envelopes stored by an earlier
        # run of this node (CacheService holds <=6h), never reuse a nonce.
        self._env_id = int(time.time() * 1000) & 0x7FFF_FFFF_FFFF_FFFF
        self._env_lock = threading.Lock()
        # sender epoch (start time, ms): receivers drop envelopes from any
        # OLDER epoch, so ring replays of a previous run are inert.
        self.epoch = self._env_id
        self._workers = [
            _PoolWorker(self, i)
            for i in range(max(1, min(workers, MAX_WORKERS)))
        ]
        for w in self._workers:
            w.start()

    # ---- envelope nonce counter: POOL-LEVEL (GCM safety) ----
    def next_env_id(self) -> int:
        with self._env_lock:
            self._env_id += 1
            return self._env_id
    # ---- lane sharding ----
    def _worker_for(self, lane: str) -> _PoolWorker:
        with self._w_lock:
            w = self._lane_map.get(lane)
            if w is None:
                w = self._next_w % len(self._workers)
                self._lane_map[lane] = w
                self._next_w += 1
            return self._workers[w]

    def submit(self, path: str, frame: dict, key: str = "") -> None:
        """WriteQueue-compatible signature; path is informational."""
        self._worker_for(key).submit(key, frame)

    # ---- cooldown bookkeeping (advisory: workers prefer healthy homes) ----
    def mark_ok(self, car: GASCarrier) -> None:
        for i, c in enumerate(self.carriers):
            if c is car:
                with self._cool_lock:
                    self._cool.pop(i, None)

    def mark_fail(self, car: GASCarrier, why: str) -> None:
        for i, c in enumerate(self.carriers):
            if c is car:
                with self._cool_lock:
                    prev = self._cool.get(i, 0.0)
                    backoff = 2.0 if prev <= time.time() else \
                        min(30.0, (prev - time.time()) * 2)
                    self._cool[i] = time.time() + backoff
                self.log(f"[gas] {car.name} cooling down "
                         f"({backoff:.1f}s): {why}")

    def cool_until(self, idx: int) -> float:
        with self._cool_lock:
            return self._cool.get(idx, 0.0)

    def flush(self, timeout: float = 5.0) -> bool:
        deadline = time.time() + timeout
        for w in self._workers:
            while w._q and time.time() < deadline:
                time.sleep(0.02)
        return not any(w._q for w in self._workers)

    def stop_all(self) -> None:
        for w in self._workers:
            w.stop()
        deadline = time.time() + 3.0
        for w in self._workers:
            w.join(timeout=max(0.1, deadline - time.time()))


# =====================================================================
# Receive side: one long-poll thread per script + jitter-buffer sweep
# =====================================================================

class GASDownloader:
    """Long-polls every pool script for its inbound direction and feeds
    items through an ItemAssembler -> handler (single-threaded consumer
    via the node's ingest queue)."""

    def __init__(self, urls: list[str], token: str, env: EnvelopeCrypto,
                 mode: str, handler: Callable[[dict], None],
                 log_fn: Callable[[str], None] = print,
                 hold_ms: float = HOLD_MS):
        assert mode in ("up", "dn")
        self.carriers = [GASCarrier(u, token, name=f"gas{i}")
                         for i, u in enumerate(dict.fromkeys(urls))]
        self.env = env
        self.mode = mode
        self.handler = handler
        self.log = log_fn
        self.asm = ItemAssembler(self._on_item, log_fn, hold_ms)
        self.stats = {"batches_in": 0, "opens_failed": 0}
        self.stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for car in self.carriers:
            t = threading.Thread(target=self._loop, args=(car,),
                                 daemon=True,
                                 name=f"zp-gas-poll-{car.name}")
            t.start()
            self._threads.append(t)
        threading.Thread(target=self._sweep_loop, daemon=True,
                         name="zp-gas-sweep").start()

    def stop_all(self) -> None:
        self.stop.set()

    def _gate(self, item: dict) -> bool:
        """Open the envelope and run the epoch gate BEFORE the (w, n)
        reorder buffer sees the item (BUGFIX #18, part 2): a sender that
        restarted re-numbers its envelopes from n=1 — entry() would
        stale-drop them all as 'n < expected' long before _on_item could
        ever run, so the generation reset must happen HERE.
        Returns True when the item may enter the assembler."""
        try:
            payload = open_item(item, self.env)
        except Exception as e:
            self.stats["opens_failed"] += 1
            self.log(f"[gas] envelope {item.get('id')} open failed: {e}")
            return False
        try:
            wid = int(item.get("w", 0))
            ep = int(payload.get("e", 0))
        except (TypeError, ValueError):
            return False
        with self.asm._lock:
            prev = self.asm._epochs.get(wid, 0)
            # BUGFIX #28: the epoch gate used a GLOBAL ratcheting floor
            # (max epoch seen minus one, across ALL workers). With one
            # session shared by several clients (the owner's phone plus any
            # test client), whichever sender had the highest epoch at the
            # moment of the first read ratcheted the global floor above
            # every other client's epoch — their hellos/frames were then
            # silently dropped as "stale" and the owner saw exactly the
            # 'stuck on Connecting' symptom. The gate exists only to kill
            # REPLAYS of a worker's OWN previous generation, so the floor
            # is now per-worker: ep < prev[wid] drops; ep > prev[wid]
            # rebases that lane; a NEW worker id always starts clean.
            if ep < prev:
                # replay from BEFORE our receiver started (ring TTL) — drop
                self.asm.stats["stale_epoch"] += 1
                return False
            if ep > prev:
                self.asm._epochs[wid] = ep
                if prev != 0:
                    # NEW sender generation: it re-numbers from n=1 —
                    # rebase the reorder buffer so the lane resumes.
                    self.asm.reset_worker(wid, int(item.get("n", 0)))
        item["_zp_payload"] = payload    # travels WITH the item (no shared state)
        # epoch stamp for the ingest dedupe key (BUGFIX #20): restarts
        # re-number transport seqs, so dedupe must be generation-aware
        for f in payload.get("f") or []:
            if isinstance(f, dict):
                f["_zp_ep"] = ep
        return True

    def _on_item(self, item: dict) -> None:
        """ItemAssembler deliver callback: hand the (already opened by
        _gate) frames over.  Must not raise."""
        payload = item.pop("_zp_payload", None)
        if payload is None:
            try:
                payload = open_item(item, self.env)
            except Exception:
                return
        for f in (payload.get("f") or []):
            self.handler(f)

    def _sweep_loop(self) -> None:
        while not self.stop.is_set():
            time.sleep(HOLD_MS / 1000.0)
            try:
                self.asm.sweep()
            except Exception:      # never kill the sweeper
                pass

    def _loop(self, car: GASCarrier) -> None:
        after = -1
        first = True
        while not self.stop.is_set():
            try:
                if first:
                    # initial sync: drain the whole readable ring at once
                    reply = car.poll(self.mode, after, 0, resync=True)
                    first = False
                else:
                    reply = car.poll(self.mode, after, POLL_WAIT_S)
                if not reply.get("ok"):
                    raise RuntimeError(f"reply {reply.get('err')}")
                batches = reply.get("b") or []
                if batches:
                    after = max(after,
                                max(int(b.get("c", after)) for b in batches))
                    self.stats["batches_in"] += len(batches)
                    for b in batches:
                        for item in (b.get("f") or []):
                            if isinstance(item, dict) and self._gate(item):
                                self.asm.entry(item)
            except Exception as e:
                if not self.stop.is_set():
                    self.log(f"[gas] {car.name} poll error: "
                             f"{type(e).__name__}: {e}")
                time.sleep(POLL_ERR_SLEEP)


# =====================================================================
# Kit/config helpers
# =====================================================================

def new_gas_config(kind: str, urls: list[str], token: str,
                   session_id: str, key: bytes, up_prefix: bytes,
                   down_prefix: bytes, client_id: str = "c1") -> dict:
    """Config dict for the GAS carrier (v2 schema on the v1 base)."""
    b64 = lambda b: base64.urlsafe_b64encode(b).decode()
    return {
        "kind": kind,
        "transport": "gas",
        "gas_urls": list(dict.fromkeys(urls)),
        "gas_token": token,
        "session": session_id,
        "key": b64(key),
        "up_prefix": b64(up_prefix),
        "down_prefix": b64(down_prefix),
        "client_id": client_id if kind == "client" else "*",
    }


def load_gas_config(cfg: dict) -> tuple[list[str], str]:
    """Extract (urls, token) from a config dict; raises on missing parts."""
    urls = cfg.get("gas_urls") or []
    token = cfg.get("gas_token") or ""
    if cfg.get("transport") != "gas" or not urls or not token:
        raise ValueError("config is not a valid gas-transport config "
                         "(transport/gas_urls/gas_token)")
    return list(urls), token


# Envelope labels: exactly 4 bytes (FrameCrypto contract), distinct from
# each other, so envelope and frame nonce spaces never overlap.
ENV_LABEL_UP = b"\x00\x00gU"
ENV_LABEL_DN = b"\x00\x00gD"


# =====================================================================
# Wire items (sealed batch envelopes)
# =====================================================================

def b64pad(s: str) -> str:
    """Restore stripped '=' padding on a urlsafe-base64 string.

    BUGFIX #17 (31 Aug): the Kotlin client sends URL_SAFE|NO_WRAP|
    NO_PADDING base64.  base64.urlsafe_b64decode() REQUIRES correct
    padding (length % 4 != 1) — 3 of every 4 unpadded strings are a
    length that raises binascii.Error, so a Python exit silently
    dropped ~75% of a phone's envelopes ("ping OK, zero traffic").
    """
    return s + "=" * (-len(s) % 4)


def seal_item(frames: list, env_id: int, env: EnvelopeCrypto,
              wid: int, wseq: int, epoch: int) -> dict:
    """frames -> one sealed wire item {"id","w","n","b"}.

    `epoch` (sender start-time, ms) rides INSIDE the sealed payload: a
    receiver drops everything from a smaller epoch, so ring replays of a
    previous sender run (CacheService TTL, replacing RTDB's DELETE GC)
    can never be re-delivered as fresh frames.
    """
    ct = env.seal(env_id, {"v": 1, "e": int(epoch), "f": frames})
    # PADDED urlsafe b64 (decoder-compat); Kotlin side pads-tolerant too.
    return {"id": env_id, "w": wid, "n": wseq,
            "b": base64.urlsafe_b64encode(ct).decode()}


def open_item(item: dict, env: EnvelopeCrypto) -> dict:
    """Wire item -> sealed payload {"v","e","f"}.

    BUGFIX #17: tolerant of UNPADDED b64 (Kotlin NO_PADDING twin) and of
    legacy multi-item bodies {"v":1,"f":[item,...]} POSTed by the old
    APK (one envelope per ring batch).
    """
    raw = item.get("b", "")
    if isinstance(raw, dict):
        raw = raw.get("b", "")
    ct = base64.urlsafe_b64decode(b64pad(raw).encode())
    payload = env.open(int(item["id"]), ct)
    if isinstance(payload, dict) and isinstance(payload.get("f"), list) \
            and payload.get("f") and isinstance(payload["f"][0], dict) \
            and set(payload["f"][0].keys()) >= {"id", "b"}:
        # legacy shape: the sealed payload IS a POST body {"v","f":[item,...]}
        inner = payload["f"][0]
        return env.open(int(inner["id"]),
                        base64.urlsafe_b64decode(b64pad(inner["b"]).encode()))
    return payload
