"""Firebase RTDB REST client: PUT/GET/DELETE + SSE streaming.

Uses only stdlib (urllib + http.client) — no SDK, no third-party deps.

SSE contract (docs/database/rest/retrieve-data#section-rest-streaming):
  GET <url>.json?auth=<cred>  with  Accept: text/event-stream
  events: put/patch/keep-alive/cancel/auth_revoked
  MUST follow 307 redirects.

AUDIT HARDENING (v2):
  * redirects are followed EXPLICITLY (loop, urljoin) for PUT/GET/DELETE —
    the old code re-called itself on the ORIGINAL path: for PUT that re-issued
    the request to a URL that redirects again (works by accident), and a
    relative Location or a 301 with a changed verb silently broke it.
  * put() caches the redirect-resolved base once; put_via() performs
    constant-resolution PUTs on the cached base for the write-queue hot path
    and self-heals (re-resolves) on any later redirect.
  * stream() now: follows 307 BEFORE parsing SSE (the old code raised
    "SSE status 307" whenever RTDB relocated the reader), fires on_ready()
    after the subscription is live and BEFORE any event (the deterministic
    gap-fill hook, see core/ingest.py), and closes the TLS socket on every
    exit path via finally (the old code leaked one fd per disconnect).

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import http.client
import json
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Optional

_UA = "Zpoint/0.1 (research; @ily_bio)"
_SSL_CTX = ssl.create_default_context()
_REDIRECTS = (301, 302, 307, 308)
_MAX_FOLLOW = 5


class RTDB:
    def __init__(self, db_url: str, auth: str = ""):
        if not db_url.startswith("https://"):
            raise ValueError("db_url must be https")
        self.base = db_url.rstrip("/")
        self.auth = auth
        self._put_base: Optional[str] = None    # redirect-resolved origin
        self._base_lock = threading.Lock()

    # ---------------- paths ----------------
    def _url(self, path: str, extra: str = "") -> str:
        p = f"{self.base}/{path.strip('/')}.json"
        params = []
        if self.auth:
            params.append(f"auth={urllib.parse.quote(self.auth)}")
        if extra:
            params.append(extra)
        return p + ("?" + "&".join(params) if params else "")

    def _url_on(self, base: str, path: str) -> str:
        p = f"{base.rstrip('/')}/{path.strip('/')}.json"
        if self.auth:
            p += "?auth=" + urllib.parse.quote(self.auth)
        return p

    def _mark_base(self, final_url: str, path: str) -> None:
        """Remember the redirect-resolved origin for future hot-path PUTs."""
        marker = f"/{path.strip('/')}.json"
        if marker in final_url:
            resolved = final_url.split(marker, 1)[0]
            with self._base_lock:
                self._put_base = resolved

    # ---------------- one-shot REST ----------------
    def put(self, path: str, value) -> bool:
        data = json.dumps(value).encode()
        with self._base_lock:
            base = self._put_base
        url = self._url_on(base, path) if base else self._url(path)
        req = urllib.request.Request(
            url, data=data, method="PUT",
            headers={"User-Agent": _UA, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as r:
                return r.status == 200
        except urllib.error.HTTPError as e:
            if e.code not in _REDIRECTS:
                raise
            final = url
            for _ in range(_MAX_FOLLOW):
                loc = e.headers.get("Location", "")
                final = urllib.parse.urljoin(final, loc)
                req = urllib.request.Request(
                    final, data=data, method="PUT",
                    headers={"User-Agent": _UA,
                             "Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req, timeout=30,
                                                context=_SSL_CTX) as r:
                        self._mark_base(final, path)
                        return r.status == 200
                except urllib.error.HTTPError as e2:
                    if e2.code not in _REDIRECTS:
                        raise
                    e = e2
            raise RuntimeError(f"too many redirects PUTting {path}")

    def put_via(self, path: str, value) -> bool:
        """Hot-path PUT (write queues).  Alias of put(): resolution is
        already cached after the first redirect, so per-call cost is one
        request.  Kept as a named hook so daemons can swap implementations."""
        return self.put(path, value)

    def get(self, path: str, extra: str = ""):
        req = urllib.request.Request(self._url(path, extra),
                                     headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as r:
            return json.loads(r.read().decode() or "null")

    def delete(self, path: str) -> bool:
        url = self._url(path)
        req = urllib.request.Request(url, method="DELETE",
                                     headers={"User-Agent": _UA})
        try:
            with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as r:
                return r.status == 200
        except urllib.error.HTTPError as e:
            if e.code not in _REDIRECTS:
                raise
            final = url
            for _ in range(_MAX_FOLLOW):
                loc = e.headers.get("Location", "")
                final = urllib.parse.urljoin(final, loc)
                req = urllib.request.Request(final, method="DELETE",
                                             headers={"User-Agent": _UA})
                try:
                    with urllib.request.urlopen(req, timeout=30,
                                                context=_SSL_CTX) as r:
                        return r.status == 200
                except urllib.error.HTTPError as e2:
                    if e2.code not in _REDIRECTS:
                        raise
                    e = e2
            raise RuntimeError(f"too many redirects DELETEing {path}")

    # ---------------- SSE ----------------
    def stream(self, path: str, on_event: Callable[[str, dict], None],
               on_lost: Optional[Callable[[str], None]] = None,
               stop: Optional[threading.Event] = None,
               idle_timeout: float = 210.0,
               on_ready: Optional[Callable[[], None]] = None) -> None:
        """Long-lived SSE reader; auto-reconnects with backoff.

        on_event(name, data_dict_or_None) for put/patch; keep-alive ignored.
        on_lost(reason) fires before each reconnect attempt.
        on_ready() fires once per (re)connect AFTER the subscription is live
        and BEFORE any event is delivered — the deterministic gap-fill hook
        (see core/ingest.py).
        """
        backoff = 0.5
        while not (stop and stop.is_set()):
            try:
                self._stream_once(path, on_event, stop, idle_timeout, on_ready)
                reason = "stream ended"
            except Exception as e:  # noqa: BLE001 — reconnect on anything
                reason = f"{type(e).__name__}: {e}"
            if stop and stop.is_set():
                return
            if on_lost:
                on_lost(reason)
            time.sleep(backoff + (time.time() % 0.25))
            backoff = min(backoff * 2, 10.0)

    def _stream_once(self, path: str, on_event, stop, idle_timeout,
                     on_ready=None) -> None:
        url = self._url(path)
        # Follow 307 BEFORE trying to parse SSE — RTDB relocates SSE readers
        # (region hosts) and the old code died with "SSE status 307".
        parsed = urllib.parse.urlparse(url)
        try:
            conn = http.client.HTTPSConnection(
                parsed.hostname, parsed.port or 443, timeout=10,
                context=_SSL_CTX)
            try:
                conn.request("GET", parsed.path + "?" + parsed.query,
                             headers={"User-Agent": _UA})
                resp = conn.getresponse()
                if resp.status in _REDIRECTS:
                    loc = resp.getheader("Location", "")
                    resp.read()
                    url = urllib.parse.urljoin(url, loc)
                    parsed = urllib.parse.urlparse(url)
                elif resp.status != 200:
                    raise RuntimeError(f"SSE status {resp.status}")
                else:
                    resp.read()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            # --- live subscription on the resolved host ---
            conn = http.client.HTTPSConnection(
                parsed.hostname, parsed.port or 443, timeout=10,
                context=_SSL_CTX)
            try:
                conn.request("GET", parsed.path + "?" + parsed.query,
                             headers={"User-Agent": _UA,
                                      "Accept": "text/event-stream"})
                resp = conn.getresponse()
                if resp.status != 200:
                    raise RuntimeError(f"SSE status {resp.status}")
                event, data = None, None
                last = time.time()
                if on_ready:
                    # gap-fill runs here, before live events flow, so the
                    # ordering scan -> live has no lost window.
                    on_ready()
                while not (stop and stop.is_set()):
                    line = resp.fp.readline()
                    if not line:
                        raise RuntimeError("SSE EOF")
                    now = time.time()
                    if now - last > idle_timeout:
                        raise RuntimeError("SSE idle timeout")
                    last = now
                    s = line.decode("utf-8", "replace").rstrip("\r\n")
                    if s.startswith("event:"):
                        event = s[6:].strip()
                    elif s.startswith("data:"):
                        data = s[5:].strip()
                    elif s == "" and event is not None:
                        payload = None
                        if data and data not in ("null", ""):
                            try:
                                payload = json.loads(data)
                            except json.JSONDecodeError:
                                payload = {"raw": data}
                        if event in ("put", "patch"):
                            on_event(event, payload or {})
                        elif event == "auth_revoked":
                            raise RuntimeError("auth_revoked")
                        elif event == "cancel":
                            raise RuntimeError("cancel: permission/policy")
                        # keep-alive: nothing
                        event, data = None, None
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        finally:
            pass  # conn closed in inner finally blocks; kept for symmetry
