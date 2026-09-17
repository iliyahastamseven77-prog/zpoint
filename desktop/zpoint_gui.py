#!/usr/bin/env python3
"""Zpoint Desktop — modern dark-mode GUI client for the GAS transport.

Property of the @ily_bio research channel (@iliyahsatam).

ARCHITECTURE
  zpoint_gui.py  (this file)   — tkinter UI + EngineManager bridge
  core/gas_nodes.py            — GASClientNode (SOCKS5 + mux + GAS pool)
  core/gas.py                  — HTTP carrier, batching, pool, assembler

The GUI runs the ENGINE IN-PROCESS on a worker thread (same daemon code
as `zpointd serve client`), so there is no subprocess to babysit and no
zombie risk: stop_all() is deterministic and joined with a timeout.

No third-party dependencies — Python stdlib only (tkinter).
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import threading
import time
import tkinter as tk
import urllib.parse
from tkinter import filedialog, messagebox, simpledialog, ttk

ZP_ROOT = "/root/projects/zpoint"
if ZP_ROOT not in sys.path:
    sys.path.insert(0, ZP_ROOT)

from core.config import load as cfg_load, save as cfg_save          # noqa: E402
from core.gas import GASCarrier, load_gas_config, token_mac         # noqa: E402
from core.gas_nodes import node_from_config                         # noqa: E402

CFG_DIR = os.path.join(ZP_ROOT, "configs")
os.makedirs(CFG_DIR, exist_ok=True)

# ---------------------------------------------------------------------
# Design tokens (dark-first, emerald accent — Warp/Clash-Verge vibe)
# ---------------------------------------------------------------------
C_BG = "#0b0f14"        # app background
C_BG2 = "#111823"       # card background
C_BG3 = "#1a2432"       # nested surfaces
C_FG = "#e6edf3"        # primary text
C_MUT = "#7d8ca0"       # muted text
C_ACC = "#2dd4a7"       # emerald accent (connected)
C_WARN = "#f5b83d"      # amber (connecting)
C_ERR = "#f2545b"       # red (error / disconnected)
C_LINE = "#223043"      # hairlines
import tkinter.font as _tkfont
FONTS: dict[str, object] = {}


def init_fonts(root) -> None:
    """Pick the first available family per class; build Font objects.
    (Tk tuples accept exactly ONE family — multi-family tuples raise.)"""
    if FONTS:
        return
    fams = set(_tkfont.families(root))
    ui = next((f for f in ("SF Pro Display", "Segoe UI", "Roboto",
                           "Helvetica Neue", "Helvetica", "DejaVu Sans")
               if f in fams), "Helvetica")
    mono = next((f for f in ("JetBrains Mono", "Menlo", "Consolas",
                             "DejaVu Sans Mono", "Courier New", "Courier")
                 if f in fams), "Courier")
    FONTS["ui"] = _tkfont.Font(family=ui, size=11)
    FONTS["ui_bold"] = _tkfont.Font(family=ui, size=11, weight="bold")
    FONTS["ui_big"] = _tkfont.Font(family=ui, size=15, weight="bold")
    FONTS["mono"] = _tkfont.Font(family=mono, size=9)

STATE_IDLE = "disconnected"
STATE_CONNECTING = "connecting"
STATE_ON = "connected"


# =====================================================================
# EngineManager — owns the daemon lifecycle + live metrics
# =====================================================================
class EngineManager:
    """Runs GASClientNode in-process; exposes thread-safe live stats."""

    def __init__(self, log_fn):
        self.log = log_fn
        self.node = None
        self.cfg = None
        self.state = STATE_IDLE
        self._lock = threading.Lock()
        self._last_bytes = (0, 0, time.time())      # (in, out, ts)
        self.rate_in = 0.0                           # bytes/s
        self.rate_out = 0.0
        self.pool_health: dict[str, str] = {}        # url -> "ok 23ms" | "down"

    # ---- lifecycle ----
    def connect(self, cfg_path: str, port: int, use_system_proxy: bool):
        with self._lock:
            if self.node is not None:
                return False, "already connected"
            try:
                cfg = cfg_load(cfg_path)
                urls, token = load_gas_config(cfg)
            except Exception as e:
                return False, f"bad config: {e}"
            cfg = dict(cfg)
            cfg["client_id"] = cfg.get("client_id", "c1")
            self.cfg = cfg
            self.state = STATE_CONNECTING
            self.log(f"[ui] engine start: pool={len(urls)} "
                     f"token={token_mac(token)} port={port}")
            try:
                node = node_from_config(
                    cfg, "client", log_fn=self.log,
                    listen=("127.0.0.1", port))
                node.meter = None                 # quota meter not needed (GAS)
                node.start()
                self.state = STATE_ON
            except Exception as e:
                self.state = STATE_IDLE
                return False, f"engine failed: {e}"
            self.node = node
            if use_system_proxy:
                self._set_system_proxy(True, port)
            return True, ""

    def disconnect(self):
        node, cfg = self.node, self.cfg
        with self._lock:
            self.node = None
            self.state = STATE_IDLE
        if node is None:
            return
        if cfg is not None:
            self._set_system_proxy(False, 0)
        try:
            node.stop_all()
        except Exception:
            pass
        # join poll/sweep threads so NOTHING outlives a disconnect
        # (no zombie background threads — explicit deliverable).
        deadline = time.time() + 4
        for t in list(threading.enumerate()):
            if t is threading.current_thread() or not t.is_alive():
                continue
            if t.name.startswith(("zp-gas-poll", "zp-gas-sweep",
                                  "zp-gas-w", "zp-cli-ingest",
                                  "zp-gas-cli-ingest")):
                t.join(timeout=max(0.05, deadline - time.time()))
        self.log("[ui] engine stopped")

    # ---- metrics ----
    def snapshot(self) -> dict:
        node = self.node
        out = {"state": self.state, "pool_total": 0, "pool_up": 0,
               "ping_ms": None, "rate_in": self.rate_in,
               "rate_out": self.rate_out, "frames_in": 0, "frames_out": 0,
               "streams": 0, "asm": None}
        if node is None:
            return out
        urls = [c.url for c in node.pool.carriers]
        out["pool_total"] = len(urls)
        for c in node.pool.carriers:
            ok = c.errors == 0 or c.requests > c.errors * 4
            out["pool_up"] += 1 if ok else 0
        st = node.stats
        out["frames_in"] = st["frames_in"]
        out["frames_out"] = st["frames_out"]
        out["streams"] = st["streams"]
        now = time.time()
        li, lo, t0 = self._last_bytes
        bi = node._downloader.stats.get("frames_in", 0)  # frame counters
        # true byte accounting: carrier counters carry wire bytes
        rx = sum(c.bytes_down for c in node._downloader.carriers)
        tx = sum(c.bytes_up for c in node.pool.carriers)
        dt = max(now - t0, 1e-6)
        self.rate_in = (rx - li) / dt
        self.rate_out = (tx - lo) / dt
        self._last_bytes = (rx, tx, now)
        out["rate_in"], out["rate_out"] = self.rate_in, self.rate_out
        asm = node._downloader.asm.snapshot() if node._downloader else None
        out["asm"] = asm
        return out

    def ping_pool(self) -> dict[str, str]:
        """One-shot health check: ping every script of the pool."""
        node = self.node
        if node is None:
            # ping from a detached carrier set using the saved config
            try:
                urls, token = load_gas_config(self.cfg or {})
            except Exception:
                return {}
            carriers = [GASCarrier(u, token, name=f"gas{i}")
                        for i, u in enumerate(urls)]
        else:
            carriers = list(node._downloader.carriers)
        res: dict[str, str] = {}
        threads = []

        def probe(car: GASCarrier):
            try:
                ms = car.ping()
                res[car.url] = f"{ms:.0f} ms"
            except Exception as e:
                res[car.url] = f"down ({type(e).__name__})"

        for c in carriers:
            t = threading.Thread(target=probe, args=(c,), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(8)
        self.pool_health = dict(res)
        return res

    # ---- system proxy (best effort per OS) ----
    def _set_system_proxy(self, on: bool, port: int):
        host = "127.0.0.1"
        try:
            if sys.platform.startswith("win"):
                import winreg  # type: ignore
                key = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion"
                    r"\Internet Settings", 0, winreg.KEY_SET_VALUE)
                winreg.SetValueEx(key, "ProxyEnable", 0, winreg.REG_DWORD,
                                  1 if on else 0)
                if on:
                    winreg.SetValueEx(key, "ProxyServer", 0, winreg.REG_SZ,
                                      f"socks={host}:{port}")
                self.log(f"[ui] system proxy {'on' if on else 'off'} (winreg)")
            elif sys.platform == "darwin":
                svc = subprocess.run(
                    ["networksetup", "-listallnetworkservices"],
                    capture_output=True, text=True, timeout=5
                ).stdout.splitlines()[1:]
                for s in svc:
                    args = (["networksetup", "-setsocksfirewallproxy", s,
                             host, str(port)] if on else
                            ["networksetup", "-setsocksfirewallproxystate",
                             s, "off"])
                    subprocess.run(args, capture_output=True, timeout=5)
                self.log(f"[ui] system proxy {'on' if on else 'off'} (macOS)")
            else:
                for scheme in ("http", "https", "socks"):
                    val = f"socks://{host}:{port}" if on else "''"
                    subprocess.run(["gsettings", "set",
                                    "org.gnome.system.proxy", scheme, val],
                                   capture_output=True, timeout=5)
                subprocess.run(["gsettings", "set", "org.gnome.system.proxy",
                                "mode",
                                "manual" if on else "none"],
                               capture_output=True, timeout=5)
                self.log(f"[ui] system proxy {'on' if on else 'off'} (gsettings)")
        except Exception as e:
            self.log(f"[ui] system proxy not set: {e}")


# =====================================================================
# Widgets
# =====================================================================

class HeroButton(tk.Canvas):
    """Central connect button: pulsing ring while connecting, glowing
    emerald ring while connected.  Pure canvas — no images."""

    DIAMETER = 190

    def __init__(self, master, on_click):
        super().__init__(master, width=self.DIAMETER, height=self.DIAMETER,
                         bg=C_BG, highlightthickness=0)
        self.on_click = on_click
        self.state = STATE_IDLE
        self._t = 0
        self._anim = None
        self.bind("<Button-1>", lambda e: on_click())
        self._draw()

    def set_state(self, state: str):
        self.state = state
        if self._anim:
            self.after_cancel(self._anim)
            self._anim = None
        if state == STATE_CONNECTING:
            self._pulse()
        else:
            self._draw()

    def _pulse(self):
        self._t = (self._t + 0.08) % (2 * 3.14159)
        self._draw()
        self._anim = self.after(33, self._pulse)

    def _draw(self):
        self.delete("all")
        w = self.DIAMETER
        cx = cy = w / 2
        r = 66
        if self.state == STATE_ON:
            ring, core, glow = C_ACC, C_ACC, "#0e3f33"
            # static glow ring
            self.create_oval(cx-r-18, cy-r-18, cx+r+18, cy+r+18,
                             outline="", fill=glow)
            self.create_oval(cx-r-9, cy-r-9, cx+r+9, cy+r+9,
                             outline=C_ACC, width=2)
        elif self.state == STATE_CONNECTING:
            core, ring = C_BG3, C_WARN
            # expanding fading pulse
            ph = (self._t % (2 * 3.14159)) / (2 * 3.14159)
            pr = r + 8 + 26 * ph
            fade = max(0, 1 - ph)
            col = "#%02x%02x%02x" % (int(245*fade+17*(1-fade)),
                                     int(184*fade+24*(1-fade)),
                                     int(61*fade+20*(1-fade)))
            self.create_oval(cx-pr, cy-pr, cx+pr, cy+pr, outline=col, width=2)
        else:
            ring, core = C_LINE, C_BG3
        self.create_oval(cx-r, cy-r, cx+r, cy+r, fill=core,
                         outline=ring, width=3)
        label = {"disconnected": "CONNECT",
                 "connecting": "…",
                 "connected": "STOP"}[self.state]
        colr = {"disconnected": C_FG, "connecting": C_WARN,
                "connected": "#04120d"}[self.state]
        self.create_text(cx, cy, text=label, fill=colr,
                         font=FONTS["ui_big"])


class SpeedGraph(tk.Canvas):
    """Rolling DL/UL throughput graph, smooth bezier curves."""

    def __init__(self, master, seconds=60):
        super().__init__(master, height=120, bg=C_BG2,
                         highlightthickness=1,
                         highlightbackground=C_LINE)
        self.seconds = seconds
        self.dl: list[float] = [0.0] * seconds
        self.ul: list[float] = [0.0] * seconds
        self._max = 1.0
        self.bind("<Configure>", lambda e: self.redraw())

    def push(self, dl: float, ul: float):
        self.dl.pop(0); self.dl.append(dl)
        self.ul.pop(0); self.ul.append(ul)
        self._max = max(1024.0, max(self.dl), max(self.ul))
        self.redraw()

    def redraw(self):
        self.delete("all")
        w = max(self.winfo_width(), 10)
        h = max(int(self["height"]), 10)
        # grid
        for gy in range(1, 4):
            y = h * gy / 4
            self.create_line(0, y, w, y, fill=C_LINE, dash=(2, 4))
        for series, color in ((self.dl, C_ACC), (self.ul, "#5aa7ff")):
            pts = []
            n = len(series)
            for i, v in enumerate(series):
                x = w * i / (n - 1)
                y = h - 6 - (h - 16) * (v / self._max)
                pts += [x, y]
            if len(pts) >= 4:
                self.create_line(*pts, fill=color, width=2, smooth=True)
        self.create_text(8, 8, anchor="nw", text="↓ DL", fill=C_ACC,
                         font=FONTS["ui"])
        self.create_text(52, 8, anchor="nw", text="↑ UL", fill="#5aa7ff",
                         font=FONTS["ui"])
        self.create_text(w - 8, 8, anchor="ne", text=human_rate(self._max),
                         fill=C_MUT, font=FONTS["ui"])


def human_rate(bps: float) -> str:
    for unit in ("B/s", "KB/s", "MB/s"):
        if bps < 1024 or unit == "MB/s":
            return f"{bps:.0f} {unit}" if unit == "B/s" else f"{bps:.1f} {unit}"
        bps /= 1024
    return f"{bps:.1f} MB/s"


# =====================================================================
# Main application
# =====================================================================

class ZpointApp:
    def __init__(self, root: tk.Tk):
        init_fonts(root)
        self.root = root
        root.title("Zpoint")
        root.geometry("460x780")
        root.configure(bg=C_BG)
        root.minsize(420, 700)
        self.engine = EngineManager(self.log)
        self.configs = self._scan_configs()
        self.current_cfg = tk.StringVar(value=self.configs[0]
                                       if self.configs else "")
        self.port = tk.IntVar(value=1086)
        self.sysproxy = tk.BooleanVar(value=False)
        self.autostart = tk.BooleanVar(value=self._autostart_enabled())
        self.pings: dict[str, str] = {}
        self._build_ui()
        self._tick()
        self._handle_deeplink_argv()

    # ---------------- UI ----------------
    def _build_ui(self):
        pad = dict(padx=16, pady=4)

        # header: profile card
        head = tk.Frame(self.root, bg=C_BG2, highlightthickness=1,
                        highlightbackground=C_LINE)
        head.pack(fill="x", **pad)
        tk.Label(head, text="ACTIVE PROFILE", bg=C_BG2, fg=C_MUT,
                 font=FONTS["ui"]).pack(anchor="w", padx=14, pady=(10, 0))
        row = tk.Frame(head, bg=C_BG2)
        row.pack(fill="x", padx=10, pady=(0, 10))
        self.profile_cb = ttk.Combobox(row, textvariable=self.current_cfg,
                                       values=self.configs, state="readonly",
                                       font=FONTS["mono"])
        self.profile_cb.pack(side="left", fill="x", expand=True, padx=6,
                             pady=6)
        self.profile_cb.bind("<<ComboboxSelected>>",
                             lambda e: self._refresh_profile())
        tk.Button(row, text="Import", command=self.import_dialog,
                  bg=C_BG3, fg=C_FG, relief="flat",
                  activebackground=C_LINE, activeforeground=C_FG
                  ).pack(side="left", padx=4)
        tk.Button(row, text="Ping All", command=self._ping_all,
                  bg=C_BG3, fg=C_FG, relief="flat",
                  activebackground=C_LINE, activeforeground=C_FG
                  ).pack(side="left", padx=4)

        # pool health strip
        self.pool_lbl = tk.Label(self.root, text="pool — import a config",
                                 bg=C_BG, fg=C_MUT, font=FONTS["ui"])
        self.pool_lbl.pack(anchor="w", **pad)

        # hero
        self.hero = HeroButton(self.root, self._toggle)
        self.hero.pack(pady=14)

        # status + metrics
        self.state_lbl = tk.Label(self.root, text="Disconnected", bg=C_BG,
                                  fg=C_ERR, font=FONTS["ui_big"])
        self.state_lbl.pack()
        self.ping_lbl = tk.Label(self.root, text="ping —  ·  nodes —",
                                 bg=C_BG, fg=C_MUT, font=FONTS["ui"])
        self.ping_lbl.pack()
        self.speed_lbl = tk.Label(self.root, text="↓ 0 B/s   ↑ 0 B/s",
                                  bg=C_BG, fg=C_FG, font=FONTS["mono"])
        self.speed_lbl.pack(pady=(2, 6))

        # graph card
        self.graph = SpeedGraph(self.root)
        self.graph.pack(fill="x", **pad)

        # settings row
        srow = tk.Frame(self.root, bg=C_BG)
        srow.pack(fill="x", **pad)
        tk.Label(srow, text="SOCKS port", bg=C_BG, fg=C_MUT,
                 font=FONTS["ui"]).pack(side="left")
        tk.Spinbox(srow, from_=1024, to=65535, textvariable=self.port,
                   width=7, bg=C_BG3, fg=C_FG, buttonbackground=C_BG3,
                   relief="flat", insertbackground=C_FG).pack(side="left",
                                                              padx=8)
        tk.Checkbutton(srow, text="System proxy", variable=self.sysproxy,
                       command=self._apply_sysproxy_live, bg=C_BG, fg=C_FG,
                       activebackground=C_BG, selectcolor=C_BG3,
                       highlightthickness=0).pack(side="left", padx=6)
        tk.Checkbutton(srow, text="Autostart", variable=self.autostart,
                       command=self._apply_autostart, bg=C_BG, fg=C_FG,
                       activebackground=C_BG, selectcolor=C_BG3,
                       highlightthickness=0).pack(side="left", padx=6)

        # log console (collapsible)
        self.log_open = True
        lbar = tk.Frame(self.root, bg=C_BG)
        lbar.pack(fill="x", **pad)
        tk.Button(lbar, text="▾ Log console", command=self._toggle_log,
                  bg=C_BG, fg=C_MUT, relief="flat", anchor="w",
                  font=FONTS["ui"]).pack(fill="x")
        self.log_box = tk.Text(self.root, height=10, bg=C_BG2, fg=C_FG,
                               insertbackground=C_FG, relief="flat",
                               font=FONTS["mono"], state="disabled",
                               wrap="none")
        self.log_box.pack(fill="both", expand=True, **pad)
        for tag, col in (("info", C_FG), ("ok", C_ACC), ("warn", C_WARN),
                         ("err", C_ERR), ("dim", C_MUT)):
            self.log_box.tag_configure(tag, foreground=col)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---------------- actions ----------------
    def _toggle(self):
        if self.engine.node is not None:
            self.hero.set_state(STATE_IDLE)
            self.state_lbl.config(text="Disconnecting…", fg=C_WARN)
            threading.Thread(target=self._do_disconnect,
                             daemon=True).start()
            return
        cfg = self.current_cfg.get()
        if not cfg:
            messagebox.showinfo("Zpoint", "Import a config first.")
            return
        self.hero.set_state(STATE_CONNECTING)
        self.state_lbl.config(text="Connecting…", fg=C_WARN)
        threading.Thread(target=self._do_connect, args=(cfg,),
                         daemon=True).start()

    def _do_connect(self, cfg):
        ok, err = self.engine.connect(cfg, int(self.port.get()),
                                      bool(self.sysproxy.get()))
        if ok:
            self.hero.set_state(STATE_ON)
            self.state_lbl.config(text="Connected", fg=C_ACC)
            self.log(f"[ui] SOCKS5 on 127.0.0.1:{self.port.get()}", "ok")
            self.root.after(300, self._ping_all)     # initial health check
        else:
            self.hero.set_state(STATE_IDLE)
            self.state_lbl.config(text="Disconnected", fg=C_ERR)
            self.log(f"[ui] connect failed: {err}", "err")

    def _do_disconnect(self):
        self.engine.disconnect()
        self.hero.set_state(STATE_IDLE)
        self.state_lbl.config(text="Disconnected", fg=C_ERR)

    def _ping_all(self):
        def work():
            res = self.engine.ping_pool()
            for url, r in res.items():
                tag = "ok" if "down" not in r else "err"
                self.log(f"[ping] {url[:48]}…  {r}", tag)
        threading.Thread(target=work, daemon=True).start()

    def import_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("Import config")
        win.configure(bg=C_BG, geometry="440x360")
        tk.Label(win, text="Import .zpoint profile", bg=C_BG, fg=C_FG,
                 font=FONTS["ui_big"]).pack(anchor="w", padx=14,
                                                   pady=(14, 4))
        tk.Label(win, text="Paste config JSON, base64(payload), or a\n"
                           "zpoint://import?cfg=… deep link:",
                 bg=C_BG, fg=C_MUT, font=FONTS["ui"], justify="left"
                 ).pack(anchor="w", padx=14)
        box = tk.Text(win, height=7, bg=C_BG2, fg=C_FG, relief="flat",
                      font=FONTS["mono"], insertbackground=C_FG)
        box.pack(fill="x", padx=14, pady=8)
        name = tk.StringVar(value="")

        def do_file():
            p = filedialog.askopenfilename(
                initialdir=CFG_DIR, filetypes=[("Zpoint", "*.zpoint"),
                                               ("All", "*.*")])
            if p:
                box.delete("1.0", "end")
                box.insert("1.0", open(p, encoding="utf-8").read())
                name.set(os.path.basename(p))

        def do_save():
            raw = box.get("1.0", "end").strip()
            cfg = parse_payload(raw)
            if isinstance(cfg, str):
                messagebox.showerror("Zpoint", cfg, parent=win)
                return
            fn = simpledialog.askstring(
                "Zpoint", "Profile name:",
                initialvalue=name.get() or
                f"gas-{time.strftime('%Y%m%d-%H%M%S')}", parent=win)
            if not fn:
                return
            path = os.path.join(CFG_DIR, f"{fn}.client.zpoint")
            cfg_save(path, cfg)
            self.configs = self._scan_configs()
            self.profile_cb["values"] = self.configs
            self.current_cfg.set(path)
            self._refresh_profile()
            self.log(f"[ui] config saved: {os.path.basename(path)}", "ok")
            win.destroy()

        br = tk.Frame(win, bg=C_BG)
        br.pack(fill="x", padx=14)
        tk.Button(br, text="From file…", command=do_file, bg=C_BG3, fg=C_FG,
                  relief="flat").pack(side="left")
        tk.Button(br, text="Save profile", command=do_save, bg=C_ACC,
                  fg="#04120d", relief="flat").pack(side="right")
        tk.Label(win, text="Tip: install the zpoint:// handler once:\n"
                           "  zpoint_gui.py --install-scheme",
                 bg=C_BG, fg=C_MUT, font=FONTS["ui"], justify="left"
                 ).pack(anchor="w", padx=14, pady=8)

    def _refresh_profile(self):
        p = self.current_cfg.get()
        if not p:
            return
        try:
            urls, token = load_gas_config(cfg_load(p))
            self.pool_lbl.config(text=f"pool {len(urls)} script(s)  ·  "
                                      f"token {token_mac(token)}")
        except Exception as e:
            self.pool_lbl.config(text=f"profile unreadable: {e}")

    def _apply_sysproxy_live(self):
        on = bool(self.sysproxy.get())
        if self.engine.node is not None:
            self.engine._set_system_proxy(on, int(self.port.get()))

    def _apply_autostart(self):
        self._set_autostart(bool(self.autostart.get()))

    def _toggle_log(self):
        self.log_open = not self.log_open
        self.log_box.pack_forget() if not self.log_open else \
            self.log_box.pack(fill="both", expand=True, **pad_kwargs())

    # ---------------- loop / logs ----------------
    def _tick(self):
        snap = self.engine.snapshot()
        if snap["pool_total"]:
            self.ping_lbl.config(
                text=f"nodes {snap['pool_up']}/{snap['pool_total']}  ·  "
                     f"streams {snap['streams']}  ·  "
                     f"frames {snap['frames_out']}↑/{snap['frames_in']}↓")
        self.speed_lbl.config(text=f"↓ {human_rate(snap['rate_in'])}   "
                                   f"↑ {human_rate(snap['rate_out'])}")
        if snap["state"] == STATE_ON:
            self.graph.push(snap["rate_in"], snap["rate_out"])
        self.root.after(700, self._tick)

    def log(self, msg: str, tag: str = "info"):
        def append():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", f"{time.strftime('%H:%M:%S')} {msg}\n",
                                tag)
            self.log_box.see("end")
            # sanitize: hard cap console length
            if int(self.log_box.index("end-1c").split(".")[0]) > 2000:
                self.log_box.delete("1.0", "500.0")
            self.log_box.configure(state="disabled")
        try:
            self.root.after(0, append)
        except RuntimeError:
            pass

    # ---------------- config plumbing ----------------
    @staticmethod
    def _scan_configs() -> list[str]:
        return sorted(
            os.path.join(CFG_DIR, f) for f in os.listdir(CFG_DIR)
            if f.endswith(".client.zpoint"))

    def _autostart_enabled(self) -> bool:
        try:
            if sys.platform.startswith("linux"):
                p = os.path.expanduser(
                    "~/.config/autostart/zpoint-gui.desktop")
                return os.path.exists(p)
            if sys.platform == "darwin":
                return os.path.exists(os.path.expanduser(
                    "~/Library/LaunchAgents/ir.zpoint.gui.plist"))
            if sys.platform.startswith("win"):
                import winreg  # type: ignore
                k = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion\Run")
                try:
                    winreg.QueryValueEx(k, "ZpointGUI")
                    return True
                except FileNotFoundError:
                    return False
        except Exception:
            return False
        return False

    def _set_autostart(self, on: bool):
        try:
            if sys.platform.startswith("linux"):
                ad = os.path.expanduser("~/.config/autostart")
                os.makedirs(ad, exist_ok=True)
                p = os.path.join(ad, "zpoint-gui.desktop")
                if on:
                    with open(p, "w") as f:
                        f.write(
                            "[Desktop Entry]\nType=Application\n"
                            f"Exec={sys.executable} {os.path.abspath(__file__)}"
                            "\nName=Zpoint\nX-GNOME-Autostart-enabled=true\n")
                elif os.path.exists(p):
                    os.remove(p)
            elif sys.platform == "darwin":
                la = os.path.expanduser("~/Library/LaunchAgents")
                os.makedirs(la, exist_ok=True)
                p = os.path.join(la, "ir.zpoint.gui.plist")
                if on:
                    with open(p, "w") as f:
                        f.write(f"""<?xml version="1.0"?>
<plist version="1.0"><dict>
<key>Label</key><string>ir.zpoint.gui</string>
<key>ProgramArguments</key><array>
<string>{sys.executable}</string>
<string>{os.path.abspath(__file__)}</string>
</array><key>RunAtLoad</key><true/>
</dict></plist>""")
                elif os.path.exists(p):
                    os.remove(p)
            elif sys.platform.startswith("win"):
                import winreg  # type: ignore
                k = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion\Run", 0,
                    winreg.KEY_SET_VALUE)
                if on:
                    winreg.SetValueEx(k, "ZpointGUI", 0, winreg.REG_SZ,
                                      f'"{sys.executable}" '
                                      f'"{os.path.abspath(__file__)}"')
                else:
                    try:
                        winreg.DeleteValue(k, "ZpointGUI")
                    except FileNotFoundError:
                        pass
            self.log(f"[ui] autostart {'enabled' if on else 'disabled'}",
                     "ok")
        except Exception as e:
            self.log(f"[ui] autostart failed: {e}", "err")

    def _handle_deeplink_argv(self):
        """zpoint_gui.py 'zpoint://import?cfg=<b64|json>'"""
        for arg in sys.argv[1:]:
            if arg.startswith("zpoint://"):
                cfg = parse_payload(arg)
                if isinstance(cfg, str):
                    self.log(f"[ui] deep link rejected: {cfg}", "err")
                    continue
                fn = f"deeplink-{int(time.time())}.client.zpoint"
                path = os.path.join(CFG_DIR, fn)
                cfg_save(path, cfg)
                self.configs = self._scan_configs()
                self.profile_cb["values"] = self.configs
                self.current_cfg.set(path)
                self._refresh_profile()
                self.log(f"[ui] deep-link config imported: {fn}", "ok")

    def _on_close(self):
        self.engine.disconnect()
        self.root.destroy()


def pad_kwargs():
    return dict(padx=16, pady=4)


# =====================================================================
# Config payload parsing: raw JSON | base64(JSON) | zpoint:// deep link
# =====================================================================

def parse_payload(raw: str):
    """Returns dict cfg or an error string.  Accepts:
      - {'transport':'gas', ...} JSON
      - base64(json) (urlsafe or standard, with/without padding)
      - zpoint://import?cfg=<same payload, url-quoted>
    """
    raw = (raw or "").strip()
    if not raw:
        return "empty payload"
    if raw.startswith("zpoint://"):
        u = urllib.parse.urlparse(raw)
        q = urllib.parse.parse_qs(u.query)
        inner = (q.get("cfg") or [""])[0]
        if not inner:
            return "deep link missing cfg"
        return parse_payload(urllib.parse.unquote(inner))
    # try direct JSON
    try:
        cfg = json.loads(raw)
        if isinstance(cfg, dict) and cfg.get("transport") == "gas":
            load_gas_config(cfg)          # validates fields
            return cfg
        return "not a gas-transport config"
    except json.JSONDecodeError:
        pass
    # try base64
    pad = "=" * (-len(raw) % 4)
    for dec in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            return parse_payload(dec(raw + pad).decode("utf-8"))
        except Exception:
            continue
    return "unrecognized payload (JSON / base64 / zpoint://)"


# =====================================================================
# zpoint:// scheme installer (Linux/Windows/macOS, best effort)
# =====================================================================

def install_scheme():
    me = os.path.abspath(__file__)
    if sys.platform.startswith("linux"):
        app = os.path.expanduser("~/.local/share/applications/zpoint.desktop")
        os.makedirs(os.path.dirname(app), exist_ok=True)
        with open(app, "w") as f:
            f.write(f"""[Desktop Entry]
Type=Application
Name=Zpoint
Exec={sys.executable} {me} %u
Terminal=false
MimeType=x-scheme-handler/zpoint;
NoDisplay=true
""")
        subprocess.run(["update-desktop-database",
                        os.path.dirname(app)], capture_output=True)
        subprocess.run(["xdg-mime", "default", "zpoint.desktop",
                        "x-scheme-handler/zpoint"], capture_output=True)
        print("zpoint:// handler installed (Linux)")
    elif sys.platform == "darwin":
        print("macOS: register via Info.plist of an app bundle "
              "(CFBundleURLTypes zpoint) — see docs.")
    elif sys.platform.startswith("win"):
        import winreg  # type: ignore
        key_path = r"Software\Classes\zpoint"
        with winreg.CreateKey(winreg.HKEY_CURRENT_USER, key_path) as k:
            winreg.SetValueEx(k, "", 0, winreg.REG_SZ, "URL:Zpoint Protocol")
            winreg.SetValueEx(k, "URL Protocol", 0, winreg.REG_SZ, "")
        with winreg.CreateKey(winreg.HKEY_CURRENT_USER,
                              key_path + r"\shell\open\command") as k:
            winreg.SetValueEx(k, "", 0, winreg.REG_SZ,
                              f'"{sys.executable}" "{me}" "%1"')
        print("zpoint:// handler installed (Windows)")


def main():
    if "--install-scheme" in sys.argv:
        install_scheme()
        return
    root = tk.Tk()
    init_fonts(root)
    try:
        root.call("tk", "scaling", 1.3)
    except Exception:
        pass
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass
    style.configure("TCombobox", fieldbackground=C_BG3, background=C_BG3,
                    foreground=C_FG, arrowcolor=C_FG, bordercolor=C_LINE)
    style.configure("TSpinbox", fieldbackground=C_BG3, background=C_BG3,
                    foreground=C_FG, arrowcolor=C_FG)
    ZpointApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
