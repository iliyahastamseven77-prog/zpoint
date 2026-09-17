#!/usr/bin/env python3
"""Generate Zpoint launcher icons: emerald Z-bolt on obsidian.

The mark: a bold "Z" whose diagonal stroke is drawn as a lightning bolt —
"Z" (Zpoint) + speed/energy. Flat, high-contrast, works at 48dp.
All mipmap densities + adaptive-icon foreground + round.

Property of the @ily_bio research channel (@iliyahsatam).
"""
import os

RES = "/root/projects/zpoint/android/app/src/main/res"

BG = "#0B0E14"        # obsidian
GRAD_TOP = "#1E293B"  # slate sheen
EMERALD = "#10B981"
EMERALD_DARK = "#059669"
WHITE = "#F1F5F9"

ADAPTIVE_SIZES = {  # foreground drawable is 108dp canvas, icon 72dp safe
    "mipmap-mdpi": 108, "mipmap-hdpi": 162, "mipmap-xhdpi": 216,
    "mipmap-xxhdpi": 324, "mipmap-xxxhdpi": 432,
}
LEGACY_SIZES = {  # plain 48dp icons
    "mipmap-mdpi": 48, "mipmap-hdpi": 72, "mipmap-xhdpi": 96,
    "mipmap-xxhdpi": 144, "mipmap-xxxhdpi": 192,
}


def z_bolt_svg(size: int, padding: float, bg: str, with_bg: bool) -> str:
    """SVG of the Z-bolt mark. Viewbox 100x100."""
    s = size
    p = padding  # percent padding around the 100x100 mark
    bg_rect = f'<rect width="100" height="100" rx="22" fill="{bg}"/>' if with_bg else ""
    # Z: top bar, bolt diagonal, bottom bar
    # bolt diagonal: from top-right area to bottom-left area as a polygon
    topbar = f'M{20+p} {18+p} H{80-p} V{34+p} H{20+p} Z'
    botbar = f'M{20+p} {82-p} H{80-p} V{66-p} H{20+p} Z'
    # diagonal bolt: zigzag polygon between the bars
    bolt = (f'M{74-p} {30+p} L{38+p} {56+p} H{54+p} L{26+p} {70+p} '
            f'L{62+p} {44+p} H{46+p} L{74-p} {30+p} Z')
    grad = f'''
    <defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="{EMERALD}"/>
      <stop offset="1" stop-color="{EMERALD_DARK}"/>
    </linearGradient></defs>'''
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{s}" height="{s}"
 viewBox="0 0 100 100">{grad}{bg_rect}
 <g fill="url(#g)"><path d="{topbar}"/><path d="{botbar}"/><path d="{bolt}"/></g>
 <g fill="{WHITE}" opacity="0.10"><path d="{topbar}"/></g>
</svg>'''


def write(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


for dpi, size in LEGACY_SIZES.items():
    write(f"{RES}/{dpi}/ic_launcher.png.svg", "")
for dpi, size in LEGACY_SIZES.items():
    svg = z_bolt_svg(size, padding=6, bg=BG, with_bg=True)
    write(f"{RES}/{dpi}/ic_launcher.xml.svg", svg)
for dpi, size in ADAPTIVE_SIZES.items():
    # adaptive foreground: mark on TRANSPARENT bg (system provides bg color)
    svg = z_bolt_svg(size, padding=14, bg="none", with_bg=False)
    write(f"{RES}/{dpi}/ic_launcher_foreground.xml.svg", svg)

# adaptive icon definition (vector-safe: solid bg color + drawable)
write(f"{RES}/mipmap-anydpi-v26/ic_launcher.xml", f'''<?xml version="1.0" encoding="utf-8"?>
<adaptive-icon xmlns:android="http://schemas.android.com/apk/res/android">
    <background android:drawable="@color/ic_launcher_background"/>
    <foreground android:drawable="@drawable/ic_launcher_foreground"/>
</adaptive-icon>''')
write(f"{RES}/values/ic_launcher_background.xml",
      f'<?xml version="1.0" encoding="utf-8"?>\n<resources>\n'
      f'    <color name="ic_launcher_background">{BG}</color>\n</resources>')

print("icon svg sources written; render with rsvg/cairosvg next")
