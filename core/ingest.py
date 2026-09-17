"""Deterministic ingest pipeline for SSE-delivered frames.

AUDIT TARGET #2 + #3: two loss windows existed around SSE reconnects.

  Window A (initial snapshot): RTDB delivers the ENTIRE subtree as ONE
  synthetic put event when the stream opens.  The old _on_event treated it
  like a single frame (data["data"] is a dict of seq->frame) and dropped
  every frame in it except one — frames posted while the node was offline
  were lost forever.  The client's TCP peer then hangs (its data never
  arrives) until app-level timeouts.

  Window B (reconnect gap): frames PUT between SSE EOF and the new
  subscription's snapshot must be re-scanned deterministically.  The
  snapshot actually covers this (it contains everything remaining at
  connect time) — as long as Window A is handled and the scan is done
  BEFORE live events are processed.  core.rtdb.stream(on_ready=...) gives
  exactly that hook.

This module provides:
  * FrameIngest — dedupe by transport seq (RTDB SSE can replay the
    snapshot), strict per-path ordering is preserved by the caller's
    single-threaded ingest loop, bounded pending set.
  * snapshot_scan() — iterate a snapshot dict {seq_str: frame} and hand
    every frame to the handler in ascending seq order.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import threading
from typing import Callable, Iterable, Optional


class FrameIngest:
    """Single-threaded frame front-door with seq dedupe.

    The owning daemon calls accept(frame_dict, handler) from its SSE
    callback context (one thread per direction), so no internal locking
    race exists on _pending; the lock still guards snapshot()/metrics.
    """

    def __init__(self, log_fn=print, max_pending: int = 4096):
        self._pending: set = set()
        self._lock = threading.Lock()
        self._log = log_fn
        self._max_pending = max_pending
        self.duplicates = 0
        self.delivered = 0

    def accept(self, node: dict, handler: Callable[[dict], None]) -> None:
        """Deliver node to handler exactly once per (epoch, transport seq).

        BUGFIX #20 (31 Aug): the key used to be the bare transport seq —
        but a client RESTART re-numbers its frames from seq=1 with a NEW
        envelope epoch, so every frame of the new generation looked like
        a replay and was silently dropped ("ping OK, zero traffic" after
        any app restart).  GAS frames carry their epoch (_zp_ep, stamped
        by GASDownloader._gate); the dedupe key is (epoch, seq).  Frames
        without an epoch (RTDB path) key as (0, seq) — unchanged.
        """
        if not isinstance(node, dict) or "b" not in node or "i" not in node:
            return
        try:
            seq = int(node["i"])
        except (TypeError, ValueError):
            return
        gen = node.get("_zp_ep") or 0
        try:
            gen = int(gen)
        except (TypeError, ValueError):
            gen = 0
        key = (gen, seq)
        with self._lock:
            if key in self._pending:
                self.duplicates += 1
                return
            if len(self._pending) >= self._max_pending:
                # pathological backlog: keep going, drop the dedupe guard
                # for the OLDEST half — handler-side deletes make re-delivery
                # impossible for live data, so this is a safety valve only.
                for s in sorted(self._pending)[: self._max_pending // 2]:
                    self._pending.discard(s)
            self._pending.add(key)
        try:
            handler(node)
            self.delivered += 1
        except Exception as e:      # bad frame: allow a future retry
            with self._lock:
                self._pending.discard(key)
            self._log(f"[ingest] handler failed seq={seq}: {e}")

    def forget(self, seq: int) -> None:
        with self._lock:
            self._pending.discard(seq)

    def snapshot(self) -> set:
        with self._lock:
            return set(self._pending)


def snapshot_scan(snapshot: dict, handler: Callable[[dict], None],
                  key_type=int) -> None:
    """Deliver every frame in an SSE snapshot dict in ascending seq order.

    RTDB synthetic put data looks like {"/<seq>": frame, ...} (paths may be
    prefixed with "/").  Numeric seq keys only — meta nodes are skipped.
    """
    if not isinstance(snapshot, dict):
        return
    items = []
    for k, v in snapshot.items():
        if not isinstance(v, dict) or "b" not in v:
            continue
        try:
            seq = key_type(str(k).lstrip("/"))
        except (TypeError, ValueError):
            continue
        v = dict(v)
        v.setdefault("i", seq)   # trust the key if the frame lacks "i"
        items.append((seq, v))
    items.sort(key=lambda kv: kv[0])
    for _, frame in items:
        handler(frame)
