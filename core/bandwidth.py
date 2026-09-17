"""Monthly bandwidth accounting for Zpoint clients.

Persisted JSON at <state_dir>/bandwidth.json:
    {"month": "2026-08", "tx": <bytes>, "rx": <bytes>}

Month rollover resets counters automatically.  Thread-safe.

AUDIT HARDENING (v2): add() is called from every stream thread at up to
frame rate; the old code re-wrote the JSON file on EVERY call (fsync-heavy
hot path).  v2 batches persistence: the file is written at most once per
interval (default 5 s) plus on rollover — counters stay exact in memory,
disk is a crash-tolerant approximation.  close()/flush() persists on demand.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import json
import os
import threading
import time


class BandwidthMeter:
    FREE_TIER_BYTES = 10 * 1024 * 1024 * 1024   # 10 GiB / month

    def __init__(self, state_dir: str, persist_interval: float = 5.0):
        self.path = os.path.join(state_dir, "bandwidth.json")
        self._lock = threading.Lock()
        self._tx = 0
        self._rx = 0
        self._month = self._current_month()
        self._dirty = False
        self._persist_interval = persist_interval
        self._last_save = 0.0
        self._load()

    # ---------- persistence ----------
    @staticmethod
    def _current_month() -> str:
        return time.strftime("%Y-%m")

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                d = json.load(f)
            if d.get("month") == self._month:
                self._tx = int(d.get("tx", 0))
                self._rx = int(d.get("rx", 0))
        except (OSError, ValueError):
            pass

    def _save(self) -> None:
        d = {"month": self._month, "tx": self._tx, "rx": self._rx}
        tmp = self.path + ".tmp"
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f)
        os.replace(tmp, self.path)
        self._dirty = False

    # ---------- api ----------
    def add(self, tx: int = 0, rx: int = 0) -> None:
        now = time.time()
        with self._lock:
            m = self._current_month()
            if m != self._month:            # month rollover
                self._month = m
                self._tx = 0
                self._rx = 0
                self._dirty = True
            self._tx += tx
            self._rx += rx
            self._dirty = True
            persist = (now - self._last_save) >= self._persist_interval
            if persist:
                self._last_save = now
            try:
                if persist:
                    self._save()
            except OSError:
                pass

    def flush(self) -> None:
        """Force-persist current counters (shutdown hooks)."""
        with self._lock:
            try:
                self._save()
            except OSError:
                pass

    def snapshot(self) -> dict:
        with self._lock:
            m = self._current_month()
            if m != self._month:
                self._month = m
                self._tx = 0
                self._rx = 0
                self._dirty = True
            used = self._tx + self._rx
            cap = self.FREE_TIER_BYTES
            return {
                "month": self._month,
                "tx_bytes": self._tx,
                "rx_bytes": self._rx,
                "total_bytes": used,
                "cap_bytes": cap,
                "remaining_bytes": max(0, cap - used),
                "used_pct": round(100.0 * used / cap, 2) if cap else 0.0,
            }

    def human(self) -> str:
        s = self.snapshot()

        def h(n: float) -> str:
            for unit in ("B", "KB", "MB", "GB"):
                if n < 1024 or unit == "GB":
                    return f"{n:,.1f} {unit}"
                n /= 1024
            return f"{n:,.1f} GB"

        return (f"{s['month']} · up {h(s['tx_bytes'])} · down {h(s['rx_bytes'])} "
                f"· total {h(s['total_bytes'])} / 10 GiB "
                f"({s['used_pct']:.1f}%) · left {h(s['remaining_bytes'])}")
