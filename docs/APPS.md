# Zpoint Apps — Desktop & Android clients (فاز v4)


هر دو اپ از موتور `transport=gas` استفاده می‌کنند (استخر Web Appهای
Apps Script). کانفیگ = همان JSON کیت با `transport:"gas"`.

## ۱. Desktop — `desktop/zpoint_gui.py` (tkinter، صفر وابستگی)

### اجرا
```bash
python3 desktop/zpoint_gui.py          # یا بعد از نصب: zpoint:// کلیک کنید
python3 desktop/zpoint_gui.py --install-scheme   # ثبت zpoint:// در OS
```
نیازمندی: Python 3.10+ با tkinter (روی دبیان: `apt install python3-tk`).

### امکانات
- Hero connect button (pulse در handshake، حلقهٔ سبز در حالت متصل)
- متریکس زنده: ping، nodes up/total، سرعت DL/UL (گراف بِزیه ۶۰ثانیه‌ای)
- مدیریت پروفایل: import از فایل / JSON / base64 / `zpoint://import?cfg=…`
- Ping All (health-check موازی همهٔ اسکریپت‌های استخر)
- System proxy toggle (Win registry / macOS networksetup / gsettings)
- Autostart (autostart desktop / LaunchAgent / HKCU Run)
- کنسول لاگ رنگی، یکپارچه با موتور (EngineManager در همان پروسه،
  قطعِ قطعی با join — بدون thread زامبی؛ تست‌شده)
- گزینهٔ پورت SOCKS (پیش‌فرض 1086)

## ۲. Android — `android/` (Kotlin + Jetpack Compose + Material3)

### ساخت
```bash
cd android
./gradlew assembleDebug      # خروجی: app/build/outputs/apk/debug/app-debug.apk
./gradlew assembleRelease    # app-release-unsigned.apk (با keystore خودت ساین کن)
```
نیازمندی: JDK 17، Android SDK 34، AGP 8.1.4، Kotlin 1.9.22،
Compose BOM 2024.02.01 (compiler ext 1.5.8).

### امکانات
- صفحهٔ اتصال Material3 دارک: هیرو-باتن با pulse/glow، چیپ‌های
  PING/NODES/STREAMS، گراف سرعت زندهٔ DL/UL
- Profile manager: import (paste JSON/base64/deep-link)، ping test،
  حذف با ✕، کپی لینک `zpoint://` برای اشتراک (پایهٔ QR)
- Deep-link یک‌کلیکی: `zpoint://import?cfg=<b64url(json)>`
- Foreground service + notification زنده (پورت SOCKS روی loopback)
- EngineBus: تک‌نمونهٔ موتور بین سرویس و UI؛ stop قطعی
- TunnelService آمادهٔ اتصال به VpnService/tun2socks (TUN در
  ConfigManager با کلید `tun_mode` فعال می‌شود؛ consent flow پیاده است)

### معماری
```
ui/ZpointApp.kt (Compose)  →  ZpointViewModel  →  EngineBus  →  GasClientNode
                                                    └ TunnelService (foreground)
engine/Gas.kt      — حامل GAS (carrier/pool/downloader/assembler)
engine/GasClientNode.kt — SOCKS5 → mux → استخر GAS (port of gas_nodes.py)
engine/{Mux,Frames,Crypto,Socks5,WriteQueue}.kt — port هستهٔ v2
```

### راهنمای TUN (تکمیل فاز بعد)
`TunnelService` اسکلت VpnService را دارد؛ برای تونل کامل:
1. `VpnService.Builder` با route 0.0.0.0/0 و DNS داخلی
2. tun2socks (binary Go: `eycorsican/go-tun2socks` یا `xjasonlyu/tun2socks`)
   به SOCKS لوکال‌هاست وصل می‌شود
3. Split-tunneling: `Builder.addAllowedApplication(pkg)` /
   `addDisallowedApplication(pkg)`

## ۳. نکات مشترک
- هیچ‌کدام از اپ‌ها progress bar حجم/سهمیه ندارند (درخواست صریح کاربر) —
  فقط سرعت زنده/بینگ/وضعیت.
- رمز E2E هرگز از دستگاه خارج نمی‌شود؛ توکن `ZP_TOKEN` فقط ضد اسکن است.
- تست کانفیگ در دسکتاپ: منو `zpoint` → گزینهٔ ۱ → Transport: 2.
