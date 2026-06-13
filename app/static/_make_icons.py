"""One-off generator for the PWA app icons. Run: python app/static/_make_icons.py
Produces icon-192.png, icon-512.png, apple-touch-icon.png in this folder.
The generated PNGs are committed; Pillow is NOT a runtime dependency.
"""
import os
from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(__file__)
TOP = (30, 136, 229)      # --primary  #1e88e5
BOT = (13, 71, 161)       # --primary-darker #0d47a1


def _font(size):
    for path in (
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
    ):
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def make(size, name, maskable=False):
    img = Image.new("RGB", (size, size), BOT)
    d = ImageDraw.Draw(img)
    # vertical gradient
    for y in range(size):
        t = y / max(1, size - 1)
        d.line(
            [(0, y), (size, y)],
            fill=tuple(int(TOP[i] + (BOT[i] - TOP[i]) * t) for i in range(3)),
        )
    txt = "MP"
    f = _font(int(size * (0.42 if maskable else 0.5)))
    bbox = d.textbbox((0, 0), txt, font=f)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    d.text(
        ((size - w) / 2 - bbox[0], (size - h) / 2 - bbox[1]),
        txt, font=f, fill=(255, 255, 255),
    )
    img.save(os.path.join(HERE, name))
    print("wrote", name)


if __name__ == "__main__":
    make(192, "icon-192.png", maskable=True)
    make(512, "icon-512.png", maskable=True)
    make(180, "apple-touch-icon.png")
