# Zpoint Architecture

Research transport: Firebase RTDB (REST + SSE) carrier **plus** a
Google Apps Script Web-App pool carrier for networks where only
script.google.com is reachable (see docs/GAS-DEPLOY.md and core/gas.py).
Private project of the @ily_bio channel (@iliyahsatam).

## 1. Topology

```
 apps -> [SOCKS5 :1086] Zpoint-Client --REST PUT--> Firebase RTDB
                                                      |  (SSE push)
 apps <- [SOCKS5 frames] Zpoint-Client <--SSE-------- Firebase RTDB
                                                      ^
 Zpoint-Exit (VPS): SSE subscribe + REST PUT back, opens real TCP targets
```

Both sides speak the same frame protocol; the direction prefixes separate
uplink (client→exit) and downlink (exit→client).

## 2. Why SSE beats object-mailbox polling

Drive-based carriers pay, per round-trip: upload → visibility wait → list
poll → download → cleanup. With default 1s polling the floor is ~1.5-3s RTT.
Zpoint replaces *discovery* with push:

- each side holds ONE long-lived SSE subscription on its inbound path
  (`Accept: text/event-stream`);
- Firebase fires `put` the moment the peer writes — no polling, no 404
  pressure, no generated-ID rendezvous (all documented anti-patterns in
  Skirk's transport research);
- `keep-alive` frames double as liveness; idle timeout triggers reconnect
  (challenge: SSE reset / NAT timeout).

Target RTT: carrier RTT (client→Firebase→exit ≈ one TLS RTT each side, so
~100-250ms) + frame handling ≈ **300-800ms**, the project goal.

## 3. Wire protocol (v1)

Transport frames are JSON objects under
`z/{session}/{dir}/{seq}` where `dir ∈ {u (client→exit), d (exit→client)}`:

```json
{"i": 12, "s": 3, "t": "d", "b": "<urlsafe-b64 ciphertext>"}
```
- `i` — global monotonic sequence per direction (gap detection)
- `s` — mux stream id (smux-style: client odd from 1, exit even from 0)
- `t` — frame type: `d` data, `o` stream open (first frame of a stream),
  `c` stream close, `k` keepalive/ping, `w` window credit
- `b` — AES-256-GCM ciphertext over the *packed* payload (see §4)

Frame size cap: 60 KiB plaintext per frame (before b64 overhead ~80 KiB
stored) — keeps each REST write small and under typical WAF/URL limits.

## 4. Packing & encryption (challenges: bandwidth, quota, E2E)

Pipeline per frame: `payload -> zstd/lzma-less: zlib level 6 -> AES-256-GCM`
then urlsafe-base64 for JSON safety. zlib wins on repetitive TCP payloads
(HTTP headers etc.) and typically pays for the base64 +33% overhead on text;
binary payload trades ~4% overhead — acceptable at 10GB/month free tier.

Keys: the server config generation creates `session_key` = 32 random bytes
(XChaCha would need libsodium; stdlib `cryptography` gives us AES-GCM).
Nonce = 96-bit: 32-bit random prefix per direction + 64-bit counter
(sequence). Key never leaves the two configs; Firebase sees only
ciphertext + metadata (`i`,`s`,`t` — needed for routing).

## 5. Multiplexing (challenge: head-of-line)

smux-shaped: one SSE subscription carries all streams; stream ids reuse the
smux convention; `o` frames carry the target `host:port` (inside ciphertext)
so the exit opens TCP lazily; `w` frames implement per-stream receive-window
credits so one stalled bulk stream cannot starve interactive ones.
Client→exit uses odd ids, exit→client even — both directions may overlap.

## 6. Garbage collection (challenge: 1GB database cap)

- After a frame is consumed (delivered to the local app / TCP socket), the
  consumer deletes that node (`DELETE .../z/{session}/{dir}/{seq}.json`).
- The exit additionally runs a janitor loop (default 120s) deleting anything
  older than `max_age` (default 600s) in its prefixes — covers crashed peers.
- Steady state per direction ≈ frames in flight (a few KB); the 1GB cap is
  never approached in normal use. GC deletes are batched and never on the
  hot write path (async queue, low concurrency).

## 7. Quotas (free tier: 10GB/mo egress, 100 SIM connections)

- 100 concurrent SSE connections is per-DB; one client + one exit = 2.
  Multiple clients supported: each appends its client-id to the down path
  (`z/{session}/d/{client}/{run}`) so all subscribe to disjoint subtrees.
- 10GB/month egress: zlib + 60KiB frames + deletion keeps metadata low;
  the bench tool reports bytes/frame and projected GB/month at observed rate.

## 8. Reconnect & session continuity (challenge: SSE resets)

- SSE read loop treats: connection error, 3-min silence (no keep-alive), or
  `cancel` → backoff reconnect (0.5s×2^n capped 10s, jitter).
- Sequence tracking: consumer keeps `next_seq`; on reconnect it requests
  `?startAt` is not available — instead it replays from `next_seq` by
  reading the remaining nodes with one GET (list under seq window) before
  re-subscribing — no loss, no duplicates (idempotent delete after process).
- TCP sockets on the exit are owned by the mux session; a brief SSE blip
  does NOT kill established TCP conns (frames buffered in RTDB meanwhile).

## 9. Geo-blocking note

Firebase RTDB instances can be created in regions whose endpoints are not
served to Iranian IPs (403). Out of scope for the daemon: the operator picks
a region where their Google account works (e.g. `europe-west1.firebasedatabase.app`).
No sanctions circumvention is implemented or intended.

## 10. Security model

- E2E: AES-256-GCM; Firebase sees ciphertext only. Key distribution =
  out-of-band (the `.zpoint` profile file, generated server-side).
- The `.zpoint` profile contains: DB URL, session id, session key (b64),
  client-id. Treat as a password.
- No client IPs are logged (Tor PT spec's anonymity guidance).
- Replays: 96-bit nonce with per-direction counter defeats reuse; `i` gaps
  beyond a window trigger resync, not blind acceptance.

## 11. Limits / honest notes

- SSE through hostile middleboxes: if googleapis fronting is required
  (SNI pinning etc.), that lives OUTSIDE this daemon — Zpoint assumes
  firebaseio/firebasedatabase.app endpoints are reachable. Route-layer
  fronting can be layered later (e.g. via an upstream proxy env var, the
  same way Skirk does `--upstream-proxy`).
- 100-connection cap: fine for research (1 exit + N clients); not a
  multi-tenant product.
