"""Render JSON terminal frames produced by ``demo.py --record-file`` into a GIF.

This generator is used only for maintainers. It requires Pillow, which is not
a runtime dependency of NimbusKV itself.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/consola.ttf",
        "C:/Windows/Fonts/Consolas.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/System/Library/Fonts/Menlo.ttc",
    ]
    for path in candidates:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


def line_color(line: str) -> str:
    stripped = line.strip()
    if stripped.startswith("$ "):
        return (90, 220, 120)
    if "killing leader" in stripped:
        return (255, 120, 120)
    if "failover time" in stripped or "survived" in stripped or "leader elected" in stripped:
        return (120, 220, 255)
    if "waiting" in stripped or "starting" in stripped:
        return (160, 180, 255)
    return (235, 240, 245)


def render_frame(
    text: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    width: int,
    line_height: int,
    padding: int,
    height: int | None = None,
) -> Image.Image:
    lines = strip_ansi(text).splitlines()
    if height is None:
        height = padding * 2 + len(lines) * line_height
    image = Image.new("RGB", (width, height), (12, 17, 28))
    draw = ImageDraw.Draw(image)
    y = padding
    for line in lines:
        draw.text((padding, y), line, font=font, fill=line_color(line))
        y += line_height
    # Draw a small block cursor after the last line.
    if lines:
        x = padding + draw.textlength(lines[-1], font=font)
        draw.rectangle((x + 2, y - line_height + 4, x + 10, y - 2), fill=(90, 220, 120))
    return image


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("frames", help="JSON file written by demo.py --record-file")
    parser.add_argument("output", help="output GIF path")
    parser.add_argument("--font-size", type=int, default=20)
    parser.add_argument("--duration", type=int, default=700, help="ms per frame")
    parser.add_argument("--last-duration", type=int, default=1800)
    args = parser.parse_args(argv)

    frames = json.loads(Path(args.frames).read_text(encoding="utf-8"))
    font = load_font(args.font_size)
    padding = 24
    line_height = args.font_size + 12
    max_len = max(
        (
            len(line)
            for frame in frames
            for line in strip_ansi(frame).splitlines()
        ),
        default=80,
    )
    max_lines = max((len(strip_ansi(frame).splitlines()) for frame in frames), default=1)
    height = padding * 2 + max_lines * line_height
    char_width = args.font_size * 0.62
    width = max(760, min(1080, int(max_len * char_width) + padding * 2))

    images = [
        render_frame(frame, font, width, line_height, padding, height=height)
        for frame in frames
    ]
    durations = [args.duration] * (len(images) - 1) + [args.last_duration]
    images[0].save(
        args.output,
        save_all=True,
        append_images=images[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"wrote {args.output} ({len(images)} frames, {width}x{images[0].height})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
