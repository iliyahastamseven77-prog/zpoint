# Zpoint

**End-to-end encrypted transport that rides on infrastructure nobody blocks — because it *is* the cloud.**

Zpoint turns ordinary TCP traffic (browser, SSH, anything SOCKS-compatible) into small AES-256-GCM-encrypted frames and relays them through a cloud carrier to an **exit node** that opens the real connections. It is a fully self-hosted, end-to-end encrypted proxy whose entire wire layer lives on services that are almost never blocked: **Google Apps Script** or **Firebase Realtime Database**.

```
 your apps ──SOCKS5──▶ Zpoint Client ──sealed batches──▶ [carrier] ──▶ Zpoint Exit ──TCP──▶ destination
                       (127.0.0.1:1086)                 (ciphertext        (VPS)        (real DNS/TCP)
                             ▲                            only)
                             └────────sealed batches────────┘
```

## Two interchangeable carriers

| Carrier | Transport | Good for |
|---|---|---|
| **GAS pool** (`transport: "gas"`) | Google Apps Script Web Apps + CacheService ring, long-poll | Networks where only `google.com` / `script.google.com` is reachable |
| **Firebase RTDB** (default) | REST PUT + Server-Sent Events push | Networks where `*.firebasedatabase.app` is reachable |

In both cases the relay infrastructure stores **only ciphertext**. Keys never leave your machines; the carrier cannot read (AES-256-GCM), cannot forge (AEAD + AAD), and cannot replay old traffic (per-direction nonces + startup-epoch ratchet).

## Feature highlights

- **Zero dependencies** — pure Python 3.10+ standard library; nothing to `pip install`
- **AES-256-GCM frames** with per-direction nonce prefixes; batch envelopes sealed separately
- **smux-style multiplexing** (OPEN / DATA / CREDIT / CLOSE) with a 512 KB credit window per stream
- **GAS pool**: 3–10 Web Apps, batching (120 ms flush / ≤96 KB wire), jitter-buffer reordering (1 s hold), automatic failover with exponential cooldown
- **Epoch ratchet**: ring replays from previous runs are cryptographically inert
- **Interactive CLI** (`zpoint`): kit generation, pool bench, bandwidth meter, service management
- **Desktop app** (tkinter) and **Android app** (Kotlin / Jetpack Compose) — both in v4
- **Extensive test suite**: crypto/mux round-trips, REST mocks, 7 GAS E2E scenarios incl. a 2 MiB bulk transfer, two hardening regression suites

## Repository layout

```
core/           Python package (stdlib only):
                  crypto.py      AES-256-GCM frames (nonce = dir-prefix + seq)
                  mux.py         smux-style multiplexing (OPEN/DATA/CREDIT/CLOSE)
                  gas.py         GAS carrier: pool workers, batching, envelope
                                 crypto, jitter-buffer assembler (ItemAssembler)
                  gas_nodes.py   GASClientNode / GASExitNode daemons
                  rtdb.py        Firebase RTDB REST + SSE layer
                  client.py      Firebase client node
                  exit.py        exit node daemon (opens real TCP connections)
                  writequeue.py  ingest.py  net.py  bandwidth.py  config.py
gas/Code.gs     Google Apps Script relay (deploy on script.google.com)
bin/zpoint      interactive management menu (bash TUI)
bin/zpointd     daemon runner (serve client | serve exit)
tools/          install.sh, pack.sh, verify-fixes.sh, gen_icon.py
tests/          selftest + E2E + regression suites
docs/           ARCHITECTURE.md, GAS-DEPLOY.md, GAS-DEPLOY-EN (README), APPS.md
desktop/        zpoint_gui.py — tkinter desktop app
android/        Kotlin/Compose client (APK-buildable in Android Studio)
packaging/      systemd unit for always-on exit nodes
```

---

# Zero to Hero — from zero to a working tunnel

Everything below assumes a POSIX shell (Linux VPS for the exit node; Linux, macOS or Windows+WSL for the client). Python **3.10+** with the standard library is the only dependency.

## Step 0 — Get the code

```bash
git clone https://github.com/iliyahastamseven77-prog/zpoint.git && cd zpoint
python3 --version          # must be >= 3.10
```

## Step 1 — Deploy the Google Apps Script relay pool (GAS carrier)

This is the heart of the `transport: "gas"` mode. Deploy a small pool of identical **Web Apps** — three to ten scripts is the sweet spot.

1. Sign in at **https://script.google.com** with any Google account.
2. **New project** → delete the placeholder → paste the full contents of [`gas/Code.gs`](gas/Code.gs). Name it e.g. `zpoint-relay-1`.
3. **Project Settings ⚙ → Script Properties → Add script property:**
   - Name: `ZP_TOKEN`
   - Value: a long random secret, e.g. `openssl rand -hex 24`
   - Use the **SAME token on every script of the pool** and in Step 3.
4. **Deploy → New deployment → Select type: Web app**
   - Execute as: **Me**
   - Who has access: **Anyone**
5. Copy the **Web app URL** — it ends in `/exec`.
6. Repeat on other accounts/projects to grow the pool; collect all `/exec` URLs.

> **Why this scales:** `doGet`/`doPost` web invocations have no published daily cap; `UrlFetchApp` is never used. Effective limits per script: 6 min/execution (our long-poll stops itself at `EXEC_DEADLINE_MS = 255s`), 30 simultaneous executions per user, 100 KB per cache key (batches are sealed ≤ 96 KB), 1000 cache entries (we use ~130). The script is a dumb 6-hour ring buffer of ciphertext — it never sees plaintext.

> **Gotchas**
> - After any edit to `Code.gs` you must **Deploy → New deployment** — saving is not deploying.
> - `/exec` redirects to `script.googleusercontent.com`; both hostnames must be reachable. `core/gas.py` follows the redirect chain explicitly.
> - The script URL host must be `script.google.com` — the client validates.

(Firebase v1 carrier needs no external service; skip this step and choose transport `1` in Step 3 if `*.firebasedatabase.app` is reachable.)

## Step 2 — Install (both machines)

```bash
sudo bash tools/install.sh
```

`tools/install.sh` resolves its own location (`ZP_ROOT`), makes the binaries executable, symlinks `bin/zpoint` to `/usr/local/bin/zpoint`, and runs a crypto+mux round-trip self-test.

## Step 3 — Generate a config kit

On the client machine (the kit generator holds the secrets):

```bash
zpoint
```

Menu path: **option `1`** — *Create new server/client kit*.

- **Transport** — `2` = Google Apps Script pool, `1` = Firebase RTDB.
  - GAS: paste the pool URLs one per line (empty line to finish), then the shared `ZP_TOKEN`.
  - Firebase: paste the RTDB URL and optionally the DB secret.
- **Client id** — Enter for the default `c1`.

The menu writes **two chmod-600 files** into `configs/`:

```
server-<timestamp>.server.zpoint   → stays ONLY on the VPS (exit node)
client-<timestamp>.client.zpoint   → goes ONLY to the client device
```

Both files share the session id, the 32-byte AES key and the per-direction nonce prefixes (`up_prefix` / `down_prefix`). Everything else on the wire is derived from these. **Never copy the server file to the client or vice versa.**

## Step 4 — Run the daemons

**VPS (Exit Node):**

```bash
scp configs/server-XXXX.server.zpoint vps:~/
ssh vps
zpointd serve exit ~/server-XXXX.server.zpoint
```

**Local machine (Client Node):**

```bash
zpointd serve client configs/client-XXXX.client.zpoint --listen 127.0.0.1:1086
```

The client exposes a **SOCKS5 proxy on 127.0.0.1:1086**. Point any application at it:

```bash
curl --socks5-hostname 127.0.0.1:1086 https://ifconfig.me   # shows the exit's IP
```

**Optional niceties**

- `zpoint` menu → `4g` — ping-bench the whole GAS pool (per-script RTT)
- `zpoint` menu → `5` — monthly bandwidth meter (10 GiB free-tier frame)
- systemd unit: `packaging/systemd/zpoint-exit.service`
- Desktop app: `python3 desktop/zpoint_gui.py` — import a `*.client.zpoint`, connect/disconnect, live metrics
- Android: open `android/` in Android Studio and build

## How a request travels (GAS carrier)

```
1. app → SOCKS5 CONNECT → client mux stream  (OPEN "c1@host:port")
2. mux frames (AES-GCM, ≤60KB plaintext) → lane of that stream → ONE pool
   worker → batched 120ms → sealed envelope (AES-256-GCM, ≤96KB wire)
3. worker POSTs to its HOME script; on failure fails over to the rest
   (exponential cooldown). Script stores {ring-slot, TTL 6h}.
4. exit long-polls each script (≤240s) → jitter buffer reorders per-worker
   sequences (dedupe by envelope id, hold gaps ≤1s, then flush) → ingest →
   mux → real TCP to destination
5. responses take the same road back with the DOWN direction crypto
6. GC = CacheService TTL + epoch ratchet (old-run ring replays are inert);
   every consumer advance is free — no delete traffic, unlike RTDB v1
```

## Tests

```bash
python3 tests/selftest.py          # core: crypto, mux, credit, socks bytes
python3 tests/test_rest.py         # RTDB REST layer (mock HTTP)
python3 tests/test_forward.py      # E2E TCP bridge, real echo server
python3 tests/test_gas.py          # 7 GAS E2E scenarios incl. 2MiB bulk
python3 tests/smoke_gas_http.py    # real-HTTP loop on localhost
python3 tests/test_hardening.py    # v2 audit: lane ordering, gap-fill, …
python3 tests/test_hardening2.py   # v2.1: sweep flush, RLock, requeue caps
```

Every suite prints `ALL ... PASSED` on success.

## Limits & quotas (cheat sheet)

| Thing | Value |
|---|---|
| Frame plaintext cap | 60 KB (≈80 KB on the wire, base64+JSON) |
| Batch wire cap | 96 KB (CacheService key limit 100 KB) |
| Batch frame cap | 64 frames — overflow is **requeued**, never dropped |
| Credit window | 512 KB per stream, grants of 256 KB |
| GAS long-poll | client asks 50 s / server caps 240 s / hard stop 255 s |
| GAS ring | 64 keys × 2 directions, TTL 6 h |
| FLUSH / HOLD | 120 ms batching / 1000 ms reorder hold |
| Firebase free tier | 10 GiB/mo egress, 100 concurrent SSE |
| UDP | not in v1 (TCP only) |

## Security model

- **End-to-end AES-256-GCM**: frames and batch envelopes are sealed on your machines; carriers (Google/Firebase) store opaque ciphertext with AAD binding stream and type.
- **Nonce discipline**: per-direction 4-byte prefixes for frames, dedicated 4-byte labels for envelopes, pool-level monotonic envelope ids — no two encryptions ever share a nonce; the startup-epoch ratchet neutralises ring replays from previous runs.
- **Shared token (`ZP_TOKEN`)** is anti-scan only — it gates who can store into your script's ring. Tokens never appear in logs (only an 8-hex MAC).
- **Deliberate scope limits**: no pattern obfuscation against abuse-detection and no geo-block circumvention features. This is transport research.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Bench fails for one script | That deployment wasn't published — re-deploy (New deployment). |
| Client can't POST, "url host not allowed" | URL must be `https://script.google.com/...` (the `/exec` one, not `/dev`). |
| Frames flow then stall after ~1 window | You mixed kits — regenerate both config files from one run of menu option 1. |
| `script.googleusercontent.com` unreachable | The firewall must allow it too — same Google service, different host. |
| Exit sees nothing after Google maintenance | Pollers auto-resync from the ring on the next poll; check `ZP_TOKEN` matches on all scripts. |
| Want a fresh transport | Menu option 1 again; new session id + key; deploy nothing new if reusing the pool. |

---

# فارسی — زپوینت چیست؟

**زپوینت یک تونلِ رمزگذاری‌شده‌ی سرتاسری (E2E) است که روی زیرساختی سوار می‌شود که عملاً هرگز مسدود نمی‌شود — چون خودِ گوگل است.**

زپوینت ترافیک TCP شما (مرورگر، SSH، هر چیزی که با SOCKS5 کار کند) را به فریم‌های کوچک رمزگذاری‌شده با **AES-256-GCM** تبدیل می‌کند و آن‌ها را از طریق یک «حامل ابری» به یک **نود خروجی** (روی سرور شما) می‌رساند؛ نود خروجی اتصال واقعی را باز می‌کند. نتیجه: یک پراکسی کاملاً خودمیزبان که کل لایه‌ی انتقالش روی سرویس‌هایی زندگی می‌کند که معمولاً در دسترس‌اند: **Google Apps Script** یا **Firebase Realtime Database**.

## دو حامل قابل‌تعویض

| حامل | انتقال | مناسب برای |
|---|---|---|
| **استخر GAS** | وب‌اپ‌های Google Apps Script + رینگ CacheService با نظرسنجی بلند | شبکه‌هایی که فقط `google.com` باز است |
| **Firebase RTDB** (پیش‌فرض) | REST PUT + پوشِ Server-Sent Events | شبکه‌هایی که `*.firebasedatabase.app` باز است |

در هر دو حالت، زیرساخت حامل **فقط متن رمز** می‌بیند. کلیدها هرگز از دستگاه‌های شما خارج نمی‌شوند؛ حامل نمی‌تواند بخواند (AES-GCM)، جعل کند (AEAD) یا ترافیک قدیمی را بازپخش کند (نانس‌های تک‌جهته + چرخ‌دنده‌ی اپاک).

## ویژگی‌های کلیدی

- **صفر وابستگی** — فقط کتابخانه‌ی استاندارد پایتون ۳.۱۰+؛ هیچ `pip install` لازم نیست
- رمزگذاری **AES-256-GCM** با نانسن‌های تفکیک‌شده در هر جهت
- **مالتی‌پلکسینگ سبک smux** با پنجره‌ی اعتبار ۵۱۲ کیلوبایت بر استریم
- **استخر GAS**: دسته‌بندی، بافر بازچینی، شکست خودکار به اسکریپت‌های دیگر با کول‌داون نمایی
- **منوی تعاملی `zpoint`**: ساخت کیت، بنچ استخر، متر پهنای‌باند، مدیریت سرویس
- **اپ دسکتاپ** (tkinter) و **اپ اندروید** (Kotlin/Compose) — هر دو در نسخه‌ی ۴
- **سوییت تست کامل**: راندتریپ رمز/ماکس، E2E با سرور اکو، ۷ سناریوی گاز شامل انتقال ۲ مگابایتی

## نصب سریع (خلاصه)

```bash
git clone https://github.com/iliyahastamseven77-prog/zpoint.git && cd zpoint
sudo bash tools/install.sh          # نصب روی هر دو دستگاه (سرور و کلاینت)
zpoint                              # گزینه ۱: ساخت کیت کانفیگ
```

راهنمای کامل فارسی گام‌به‌گام (از ساخت اسکریپت‌های گوگل تا اجرای دیمن‌ها) در [`docs/GAS-DEPLOY.md`](docs/GAS-DEPLOY.md) و [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) آمده است.

## مدل امنیتی (خلاصه)

- رمزگذاری سرتاسری: فریم‌ها و پاکت‌های دسته روی دستگاه‌های شما مُهر می‌شوند؛ گوگل/فایربیس فقط متن رمز می‌بینند
- نظم نانسن: هیچ دو رمزگذاری هرگز نانس مشترک ندارند؛ رپلی رینگ‌های اجرای قبلی با اپاک بی‌اثر می‌شود
- `ZP_TOKEN` صرفاً ضد اسکن است و هرگز در لاگ نمی‌آید
- **محدوده‌ی عمدی**: بدون مبهم‌سازی الگو در برابر تشخیص سوءاستفاده و بدون دور زدن جئوبلاک — این یک پروژه‌ی تحقیقاتی ترنسپورت است

## لایسنس

MIT — آزاد برای استفاده، تغییر و بازنشر با ذکر کپی‌رایت.

## اعتبار

معماری، پروتکل، هسته، حامل‌ها و اپ‌ها: **@ily_bio / @iliyahsatam**

---

## License

MIT © 2026 iliyahastamseven77-prog (@ily_bio / @iliyahsatam). See [LICENSE](LICENSE).