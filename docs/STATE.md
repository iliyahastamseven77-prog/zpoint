## فاز v4.2 ✅ (۱ سپتامبر) — UI اوبسیدین؛ بهینه‌سازی شبکه REVERT شد

شبکه: پاس بهینه‌سازی (keep-alive، hop1-only، poll همپوشان، RING 256، NODELAY)
اندازه‌گیری شد → median کمی بهتر ولی فقط 1/8 موفق (throttle آپس‌اسکریپت:
حامل نرخ‌محدود است نه تأخیرمحدود). کل wire به v4.1 برگشت (md5 تأیید لوکال+VPS)؛
فقط فیکس #21 (padding فریم داخلی) نگه داشته شد. کف مستند ~7-9s.
UI: Theme.kt + ui/Glass.kt + ZpointApp.kt + Sheets.kt کامل بازنویسی
(obsidian glass، hero ring، Bezier graph، sheets). APK debug 15MB /
release 10.6MB امضاشده. کل سوییت تست پاس.

## فیکس #21 ✅ (۳۱ آگوست شب) — آپلود بله، دانلود خیر

core/mux.py::UNB64 پدینگ‌محور شد + پارتی Kotlin (Gas.kt::b64d، Crypto.kt::b64decode).
علت: Kotlin فریم داخلی را بدون پدینگ می‌فرستد، پایتون پدینگ می‌خواست → DATA/CLOSE
drop، فقط OPEN عبور می‌کرد. روی VPS: padding errors 41/60s → 0.

## فاز v4.1 — فیکس «پینگ OK، صفر ترافیک» ✅ (۳۱ آگوست ۲۰۲۶)

- باگ #17: b64 بدون پدینگ Kotlin ↔ پایتون → ۷۵٪ پاکت‌ها drop | b64pad + open_item
- باگ #18: epoch-gate بعد از assembler بود؛ reset نسل هرگز fire نمی‌شد | _gate قبل از entry + reset_worker
- باگ #20: dedupe با seq برهنه → نسل جدید پس از ری‌استارت کامل drop | کلید (epoch, seq) + مهر _zp_ep
- باگ #19: نشت سوکت OPEN-بی‌داده | janitor ۱۲۰ ثانیه‌ای در GASExitNode
- Code.gs: handshake ساده برای تست ویندوز (auth باقی ماند)
- سرویس zpoint-exit (systemd) فعال روی کیت 20260831-103423 — تست زنده موفق
- کل سوییت: selftest/rest/gas/forward/hardening/hardening2 — همه پاس

# Zpoint — وضعیت پروژه و گام‌های بعدی

> به‌روزرسانی: ۳۱ آگوست ۲۰۲۶ · مالکیت: @ily_bio / @iliyahsatam

## فاز v2.1 — هاردنینگ نهایی GAS + README جامع ✅ (۳۱ آگوست ۲۰۲۶)

- باگ #15: sweep گپِ هولد را هرگز فلاش نمی‌کرد (چک `exp not in h` همیشه
  برقرار) → فلاش از کمترین کلید موجود بعد از HOLD، re-park آیتم تازه،
  expected=اولین n تحویل‌نشده. RLock برای ضد-reenrency.
- باگ #16: `_ship` روی سرریز BATCH_CAP/WIRE_CAP فریم را دور می‌ریخت →
  requeue به `self._q` (صفر اتلاف، ترتیب لاین حفظ).
- `gas/Code.gs`: گارد `EXEC_DEADLINE_MS=255s` در doGet — خروج تمیز قبل از
  سقف اجرای گوگل، ادامه در اجرای تازه.
- `tools/install.sh`: مسیر داینامیک ZP_ROOT (بدون هاردکد).
- `README.md` بازنویسی کامل: راهنمای صفر تا قهرمان + اعتبار سازنده.
- تست: `tests/test_hardening2.py` (۷ سناریو) پاس؛ کل سویت پاس.

## فاز v3 — ترنسپورت Google Apps Script ✅ (۳۱ آگوست ۲۰۲۶)

- `gas/Code.gs` + `core/gas.py` + `core/gas_nodes.py` — استخر Web App،
  batching، long-poll، jitter-buffer بازچینی، epoch ضدهست‌پوشانی؛
  docs/GAS-DEPLOY.md راهنمای استقرار.
- باگ‌های هسته که آزمون bulk بیرون کشید و فیکس شد:
  #12 دور ریختن فریم‌های w/c/o در `_consume` کلاینت (استال بالای یک پنجره)
  #13 شروع شمارش ack از _grant در سه سایت (استال تضمینی در 491,520B)
  #14 مسلح‌نشدن _tx_credits روی OPEN در exit (استال downlink)
- رگرسیون: `test_credit_bulk_window_regression` در selftest + ۷ سناریوی
  `test_gas.py` + `smoke_gas_http.py` — همه پاس.

## ساخته‌شده در فازهای قبل ✅

- هسته crypto (AES-256-GCM، nonce ضدreplay) — self-test پاس
- mux smux-شکل (SYN/DATA/CLOSE/CREDIT، split 60KB) — roundtrip تست پاس
- RTDB REST (PUT/GET/DELETE) + SSE reader با reconnect نمایی — با Firebase واقعی (hacker-news) تست `put` موفق
- SOCKS5 no-auth CONNECT + pump دوطرفه
- Exit daemon (subscribe uplink، اتصال TCP واقعی، janitor 120s)
- Client daemon (SOCKS→mux، subscribe downlink per-client، حذف فریم مصرف‌شده)
- منوی تعاملی `zpoint` (kit generation، show/delete، bench، سرویس‌ها، foreground)
- لینک سراسری `/usr/local/bin/zpoint` ✅
- selftest کامل: ALL TESTS PASSED

## چیزهایی که برای تولید نهایی باید انجام شود ⚠️

1. **سیم‌کشی واقعی client↔exit روی یک RTDB واقعی** — نیازمند پروژه فایربیس واقعی شما.
2. ~~decrypt downlink~~ ✅ رفع شد (crypto تفکیک per-direction)
3. **gap-fill پس از reconnect** — بعد از SSE drop، next_seq را با GET بردار (هنوز not implemented).
4. ~~credit deadlock~~ ✅ رفع شد (accounting تجمعی _recv_granted)
5. **bench-live دوطرفه** — بعد از اتصال واقعی.
6. **بسته‌بندی systemd hardening** — ProtectSystem, PrivateTmp, NoNewPrivileges.

## ۵ باگ گزارش‌شده (۳۰ آگوست) — همه رفع شد ✅

1. deliver_down → mux.deliver(node, rx_crypto) با crypto تفکیک‌شده
2. SOCKS5 CONNECT reply بایت‌های b"\x05\x00\x00\x01..." قبل از handler
3. مسیر downlink یکسان: z/{session}/d/{client_id} — exit از OPEN payload
   "cid@host:port" client_id را یاد می‌گیرد
4. credit تجمعی با _recv_granted — regression test اضافه شد
5. exit mux با crypto_down (نه up) — regression: InvalidTag رفع

## محدودیت‌های مستند (عمدی)

- بدون مبهم‌سازی الگو و بدون دور زدن geo-block (خارج از محدوده تعریف‌شده)
- فرض: endpoint فایربیس برای کاربر مستقیماً قابل دسترس است (region مناسب)


## فیکس ۷ (۳۰ آگوست، نهایی) — فوروارد داده

مشکل: _handle فقط decrypt/delete می‌کرد؛ mux callbacks هرگز fire نمی‌شدند →
داده به مقصد نمی‌رفت. فیکس: mux.deliver در _handle + _on_data با sendall +
wait-race. تست E2E با echo server: client→exit→TCP→exit→client پاس شد
(HELLO→HELLO) و FIN سوکت exit را هم بست. install.sh حالا ZP_ROOT را از مسیر
خودش resolve می‌کند + بررسی which zpoint بعد از نصب.


## فیکس‌های ۸-۱۱ (۳۰ آگوست) — پاس به تست E2E کامل

- #۸ @ بی‌قید (استریم ۲+ gaierror) ✅
- #۹ EOF→T_CLOSE فوری (to_mux finally) ✅
- #۱۰ نشت ترد تخلیه کلاینت ✅
- #۱۱ WriteQueue سه‌تردی (غیربلاک‌کننده PUT) ✅
- اکتشاف جانبی: _on_close باید shutdown() بزند نه close() (وگرنه ترد to_mux
  در recv قفل می‌ماند) — فیکس شد.
- tests/test_forward.py جدید: ۵ سناریو E2E با echo server واقعی — همه پاس.
