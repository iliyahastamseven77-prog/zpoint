# Zpoint GAS Relay — Deployment Guide (استقرار)


این راهنما ترنسپورت Apps Script پروژه را در شبکه‌ای که فقط دامنه‌های
اصلی گوگل (google.com / script.google.com) باز است مستقر می‌کند.

## ۰. منطق معماری در یک نگاه

```
apps → SOCKS5 → GASClientNode ──POST batch──▶ script.google.com (استخر N اسکریپت)
                     ▲                                │ CacheService ring (TTL 6h)
                     └──────GET long-poll (dn)────────┘
GASExitNode ──GET long-poll (up)──▶ همان استخر ──TCP واقعی به مقصد
             └──POST batch (dn)──▶
```

- هر فریم mux داخل یک **پوشهٔ AES-256-GCM جدا** سفر می‌کند؛ اسکریپت فقط
  ciphertext می‌بیند (نمی‌خواند، نمی‌تواند دستکاری کند — GCM).
- **Batching**: پول‌ورکرها تا FLUSH_MS=120ms فریم جمع می‌کنند و یک POST
  می‌زنند؛ سقف 96KB روی سیم (سقف کش 100KB/key).
- **Load balancing**: هر استریم mux به یک worker می‌چسبد (ترتیب مطلق)،
  worker به اسکریپت «خانه» (w mod n) پست می‌کند و در خطا به بقیه
  failover می‌کند؛ هر اسکریپت یک thread گیرندهٔ long-poll دارد.
- **Reordering**: گیرنده با (worker id, worker_seq) بازچینی می‌کند؛ گپ تا
  HOLD_MS=1000ms نگه داشته می‌شود؛ dedupe با env id؛ epoch داخل پوشه،
  بازپخشِ رینگِ ران قبلی را خنثی می‌کند.

## ۱. ساخت اسکریپت‌های استخر (per script)

1. در اکانت گوگلِ دلخواه: script.google.com → New project.
2. محتوای `gas/Code.gs` را Paste کنید.
3. Project Settings ⚙ → Script Properties → افزودن:
   - `ZP_TOKEN` = یک راز تصادفی (مثلاً 32 کاراکتر). **داخل همهٔ
     اسکریپت‌های استخر یکسان** و همان در کانفیگ کلاینت/exit می‌آید.
4. Deploy → New deployment → type: **Web app**
   - Execute as: **Me**
   - Who has access: **Anyone**
5. URL `/exec` را کپی کنید (شکل:
   `https://script.google.com/macros/s/AKfycb…/exec`).
6. برای ظرفیت بیشتر، مراحل ۱-۵ را روی اکانت/پروژهٔ دیگر تکرار کنید.
   پیشنهاد: ۳ تا ۱۰ اسکریپت.

### کوتاها (چرا این طراحی مقیاس می‌کند)
- doGet/doPost کوتای روزانهٔ منتشرشده ندارد؛ UrlFetchApp استفاده
  نمی‌شود → سقف 20k/day «URL Fetch» اصلاً وارد بازی نیست.
- سقف‌های مؤثر: 6 min/execution (long-poll ما 240s)، 30 اجرای همزمان
  per-user (هر اسکریپت ~۲ درخواست در لحظه)، 1000 entry و 100KB/key در
  Cache (ما ~130 کلید و ≤96KB).
- مصرف واقعی هر کلاینت ≈ ۲ long-poll + چند POST در دقیقه per script.

## ۲. ساخت کیت (کانفیگ‌ها)

```bash
zpoint            # گزینه ۱ → Transport: 2
                  # URLs استخر را خط‌به‌خط، خط خالی برای پایان
                  # ZP_TOKEN را وارد کنید
```
خروجی: `configs/server-*.server.zpoint` و `client-*.client.zpoint`
(شامل `transport:"gas"`, `gas_urls`, `gas_token`, key/prefixes).

## ۳. اجرا

```bash
# VPS (exit) — فایل server را فقط آنجا ببرید
zpointd serve exit   configs/server-XXXX.server.zpoint

# کلاینت محلی — فقط فایل client
zpointd serve client configs/client-XXXX.client.zpoint --listen 127.0.0.1:1086
```

بنچ پول (ping تک‌تک اسکریپت‌ها): منو → گزینهٔ `4g`.

## ۴. نکات عملیاتی

- **نشر deployment**: بعد از هر تغییر Code.gs باید New deployment
  بزنید (یا /dev و پایان URL را عوض کنید)؛ copy-paste کافی نیست.
- **ریدایرکت**: `/exec` به `script.googleusercontent.com` ریدایرکت
  می‌شود؛ این میزبان هم باید در سفید‌لیست فایروال باشد (بخشی از همان
  سرویس Google است) — `core/gas.py` ریدایرکت‌ها را صریح دنبال می‌کند.
- **حدود ظرفیت**: رینگ ۶۴ کلیدی هر جهت + TTL 6h = حافظهٔ پوششی
  backlog؛ اگر گیرنده‌ای بیش از آن عقب بماند، فریم‌های قدیمی
  overwrite می‌شوند (دقیقاً مثل GC ناپدیدشدن گره در RTDB).
- **چرخش توکن**: ZP_TOKEN را در همهٔ اسکریپت‌ها + دو کانفیگ همزمان
  عوض کنید؛ توکن فقط ضد اسکن است، رمز E2E جدا و در کانفیگ است.
- لاگ‌ها هرگز توکن خام را چاپ نمی‌کنند (`token_mac` = 8 hex از SHA256).

## ۵. نقشهٔ تست‌ها

```bash
python3 tests/selftest.py          # هسته + رگرسیون بن‌بست 2MB (فیکس 12/13/14)
python3 tests/test_forward.py      # E2E TCP-bridge هسته
python3 tests/test_gas.py          # ۷ سناریو ترنسپورت GAS (شبیه‌ساز + آشوب)
python3 tests/smoke_gas_http.py    # حلقهٔ HTTP واقعی روی لوکال‌هاست
```
