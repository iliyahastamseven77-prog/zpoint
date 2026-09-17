#!/usr/bin/env python3
"""Hardening regression tests (v2 audit follow-up).

Property of the @ily_bio research channel (@iliyahsatam).
Run: python3 tests/test_hardening2.py

Covers the three core/gas.py hardening fixes:
  #15 ItemAssembler.sweep stalled gap flush  (exp-not-in-h dead check)
  #15 ItemAssembler RLock re-entrancy safety
  #16 _PoolWorker._ship silent frame drop on BATCH_CAP / BATCH_WIRE_CAP
      overflow (frames must be requeued, never dropped)
"""
from __future__ import annotations

import base64
import json
import sys
import threading
import time

sys.path.insert(0, "/root/projects/zpoint")

from core import gas as gasmod  # noqa: E402
from core.crypto import new_key  # noqa: E402
from core.gas import (BATCH_CAP, BATCH_WIRE_CAP, ENV_LABEL_UP,  # noqa: E402
                      EnvelopeCrypto, ItemAssembler, _PoolWorker)


# =====================================================================
# helpers
# =====================================================================

def mkitem(env: EnvelopeCrypto, eid: int, wid: int, n: int, epoch=1000,
           body=b"x") -> dict:
    """A sealed wire item carrying one dummy mux frame."""
    frame = {"i": 0, "s": 1, "t": "d", "b": base64.urlsafe_b64encode(
        body).decode()}
    return gasmod.seal_item([frame], eid, env, wid, n, epoch)


def make_assembler(delivered: list, hold_ms=1000) -> ItemAssembler:
    return ItemAssembler(delivered.append, log_fn=lambda s: None,
                         hold_ms=hold_ms)


# =====================================================================
# #15a: sweep must flush when the gap head NEVER arrives
# =====================================================================

def test_sweep_flushes_missing_gap():
    env = EnvelopeCrypto(new_key(), ENV_LABEL_UP)
    delivered: list = []
    asm = make_assembler(delivered, hold_ms=300)

    # n=2..5 arrive while n=1 (the expected head) is LOST forever.
    # Old bug: sweep checked `exp not in h` -> exp==1 never in h -> the
    # whole sweep was a no-op -> items stuck until process death.
    for n in (2, 3, 4, 5):
        asm.entry(mkitem(env, 100 + n, 7, n))
    snap = asm.snapshot()
    assert snap["held"] == 4, f"items should be parked: {snap}"
    assert not delivered, "nothing may deliver while inside the hold"

    time.sleep(0.35)               # let the hold expire
    asm.sweep()

    assert len(delivered) == 4, f"sweep stalled: delivered={len(delivered)}"
    ns = [int(it["n"]) for it in delivered]
    assert ns == [2, 3, 4, 5], f"flush must run in n order: {ns}"
    assert asm.snapshot()["held"] == 0
    assert asm.stats["held_flushes"] >= 1
    print("1) sweep flushes when the head gap never arrives OK "
          f"(delivered n={ns})")


# =====================================================================
# #15b: items inside their hold window are NOT flushed early
# =====================================================================

def test_sweep_respects_hold_window():
    env = EnvelopeCrypto(new_key(), ENV_LABEL_UP)
    delivered: list = []
    asm = make_assembler(delivered, hold_ms=2000)

    asm.entry(mkitem(env, 200, 3, 2))     # gap at 1 -> parked
    asm.sweep()                           # immediately: hold not expired
    assert not delivered, "hold window must be respected"
    assert asm.snapshot()["held"] == 1

    # a second sweep right away still delivers nothing
    asm.sweep()
    assert not delivered
    print("2) sweep respects the hold window (no early flush) OK")


# =====================================================================
# #15c: partially-expired park -> expired items flush, fresh ones re-park
# =====================================================================

def test_sweep_reparks_fresh_items():
    env = EnvelopeCrypto(new_key(), ENV_LABEL_UP)
    delivered: list = []
    asm = make_assembler(delivered, hold_ms=400)

    # mixed park: n=2 parked NOW (fresh), n=3..4 parked 500ms ago (expired)
    now = time.time()
    old = now - 0.5
    with asm._lock:
        h = asm._hold.setdefault(9, {})
        h[2] = (now, mkitem(env, 302, 9, 2))
        h[3] = (old, mkitem(env, 303, 9, 3))
        h[4] = (old, mkitem(env, 304, 9, 4))

    asm.sweep()
    ns = [int(it["n"]) for it in delivered]
    assert ns == [3, 4], f"only expired items flush, in order: {ns}"
    snap = asm.snapshot()
    assert snap["held"] == 1, f"fresh n=2 must be re-parked: {snap}"

    # and after ITS hold expires, it flushes too
    time.sleep(0.45)
    asm.sweep()
    ns = [int(it["n"]) for it in delivered]
    assert ns == [3, 4, 2], f"re-parked item flushes on its own clock: {ns}"
    print("3) mixed fresh/expired hold: expired flush, fresh re-parks OK")


# =====================================================================
# #15d: entry() still works after sweeps (expected pointer restored)
# =====================================================================

def test_entry_resumes_after_sweep_flush():
    env = EnvelopeCrypto(new_key(), ENV_LABEL_UP)
    delivered: list = []
    asm = make_assembler(delivered, hold_ms=200)

    asm.entry(mkitem(env, 400, 5, 2))     # gap at 1, parked
    time.sleep(0.25)
    asm.sweep()                            # flushes n=2, expected -> 3
    assert len(delivered) == 1

    # stream recovers: the LOST n=1 never comes; n=3 must now deliver
    # normally against the restored expectation
    asm.entry(mkitem(env, 403, 5, 3))
    assert len(delivered) == 2 and int(delivered[1]["n"]) == 3
    asm.entry(mkitem(env, 404, 5, 4))
    assert len(delivered) == 3
    print("4) entry() resumes cleanly after a sweep flush OK")


# =====================================================================
# #15e: RLock — re-entrant callback must not self-deadlock
# =====================================================================

def test_rlock_reentrancy():
    env = EnvelopeCrypto(new_key(), ENV_LABEL_UP)
    delivered: list = []
    reentered = {"ok": False}

    def deliver(item):
        delivered.append(item)
        # downstream callback re-enters the assembler (what e.g. a
        # teardown/congestion path could do); with a plain Lock this
        # self-deadlocks here.
        snap = asm.snapshot()
        reentered["ok"] = snap["items"] >= 0

    asm = ItemAssembler(deliver, log_fn=lambda s: None, hold_ms=1000)
    asm.entry(mkitem(env, 500, 1, 1))     # in-order -> immediate deliver
    assert reentered["ok"] and len(delivered) == 1

    # and sweep() under the same re-entrancy (deliver -> sweep -> lock)
    with asm._lock:
        asm.sweep()
    print("5) RLock: re-entrant entry/sweep callbacks do not deadlock OK")


# =====================================================================
# #16a: wire-cap overflow must REQUEUE, never drop
# =====================================================================

class _Pool:
    def __init__(self):
        self.env = EnvelopeCrypto(new_key(), ENV_LABEL_UP)
        self.epoch = 1000
        self.is_up = True
        self.carriers = []
        self.logged = []

    def log(self, s):
        self.logged.append(s)

    def next_env_id(self):
        return 900

    def mark_ok(self, car):
        pass

    def mark_fail(self, car, why):
        pass


def test_wirecap_requeue():
    """Force the wire-cap trim with a tiny cap and verify the invariant:
    shipped + requeued == total (zero loss, order preserved)."""
    pool = _Pool()
    w = _PoolWorker(pool, 0)

    # one BIG frame per item so the 4KB cap can never fit two
    big = "A" * 4096
    items = []
    for i in range(3):
        frame = {"i": 0, "s": 1, "t": "d",
                 "b": base64.urlsafe_b64encode(
                     (big + str(i)).encode()).decode()}
        items.append(("u:1", frame))

    class _Dead:
        def __init__(self):
            self.got = None

        def post_items(self, direction, wire_items):
            self.got = wire_items
            return {"ok": True}

    dead = _Dead()
    pool.carriers = [dead]
    # monkey-patch the cap so the trim path actually trips
    real_cap = gasmod.BATCH_WIRE_CAP
    gasmod.BATCH_WIRE_CAP = 4096   # < one big frame -> every trim pops
    try:
        w._ship(list(items))
    finally:
        gasmod.BATCH_WIRE_CAP = real_cap

    requeued = [f for _, f in w._q]
    shipped = dead.got or []
    assert len(requeued) + len(shipped) == len(items), \
        f"zero loss violated: shipped={len(shipped)} requeued={len(requeued)}"
    assert len(shipped) == 1, \
        f"envelope floors at one frame: shipped={len(shipped)}"
    # requeued tail must be the LAST frames, original order preserved
    assert requeued == [f for _, f in items[len(items) - len(requeued):]], \
        "requeue must preserve lane order"
    print(f"6) wire-cap overflow: {len(shipped)} shipped + "
          f"{len(requeued)} requeued = zero loss OK")


# =====================================================================
# #16b: BATCH_CAP overflow must REQUEUE the excess tail
# =====================================================================

def test_batchcap_requeue():
    pool = _Pool()

    class _Dead:
        def post_items(self, *a, **k):
            raise RuntimeError("net down (intentional)")

    pool.carriers = [_Dead()]
    w = _PoolWorker(pool, 0)
    items = [(f"u:1", {"i": 0, "s": 1, "t": "d",
                       "b": base64.urlsafe_b64encode(f"f{i}".encode()).decode()})
             for i in range(BATCH_CAP + 7)]
    w._ship(items)
    got = [f["b"] for _, f in w._q]
    want = [f["b"] for _, f in items[BATCH_CAP:]]
    assert got == want, f"excess {BATCH_CAP}.. must be requeued in order"
    print(f"7) BATCH_CAP overflow requeues the {len(want)}-frame tail OK")


# =====================================================================
# main
# =====================================================================

def main():
    test_sweep_flushes_missing_gap()
    test_sweep_respects_hold_window()
    test_sweep_reparks_fresh_items()
    test_entry_resumes_after_sweep_flush()
    test_rlock_reentrancy()
    test_wirecap_requeue()
    test_batchcap_requeue()
    print("✅ ALL HARDENING REGRESSION TESTS PASSED")


if __name__ == "__main__":
    main()
