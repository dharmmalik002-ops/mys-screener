"""One standard picture of a chart, so that only the pattern varies.

Every chart — a newsletter reference or an Indian stock today — is drawn from
its own daily bars in exactly this style: the last `WINDOW` sessions ending ON
the chosen day, candles over a 50-day average, volume underneath, the price axis
scaled to the window's own range. Scaling to the range is what lets "the last
contraction was 3%" and "it was 6%" read as the same idea: the picture records
the shape of the base, not its price level or its absolute percentages.

Nothing after `end` is read. `test_lookalike.py` pins that by appending bars and
checking the picture does not change.
"""

from __future__ import annotations

import numpy as np
from PIL import Image, ImageDraw

WINDOW = 120
SMA_LEN = 50
SIZE = 224
# Drawn at 2x and downsampled: at 224 px a 120-bar window is under 2 px per
# bar, and drawing that directly aliases wicks away.
_SCALE = 2
_PRICE_FRACTION = 0.78
_GAP = 0.02

_BG = (255, 255, 255)
_UP = (150, 150, 150)
_DOWN = (20, 20, 20)
_SMA = (90, 120, 200)
_VOL = (170, 170, 170)


# Extra window lengths for the multi-scale similarity layer: a short look at
# the base itself and a long one at the trend it sits in. The model's style
# classifier still reads 120 only.
SCALES = (60, 120, 250)


def min_bars_needed(window: int = WINDOW) -> int:
    return window + SMA_LEN - 1


def _sma(close: np.ndarray, end: int, window: int = WINDOW) -> np.ndarray:
    start = end - window + 1
    out = np.empty(window)
    csum = np.cumsum(np.insert(close[: end + 1].astype(float), 0, 0.0))
    for k, i in enumerate(range(start, end + 1)):
        out[k] = (csum[i + 1] - csum[i + 1 - SMA_LEN]) / SMA_LEN
    return out


def window_arrays(o, h, l, c, v, end: int, window: int = WINDOW) -> dict[str, np.ndarray] | None:
    """The `WINDOW` sessions ending at `end` (inclusive), plus the 50-day
    average over the same span. None when there is not enough history, or the
    window holds bad prices — never a padded or partial picture."""
    if end < min_bars_needed(window) - 1 or end >= len(c):
        return None
    s = slice(end - window + 1, end + 1)
    arrs = {
        "o": np.asarray(o[s], dtype=float),
        "h": np.asarray(h[s], dtype=float),
        "l": np.asarray(l[s], dtype=float),
        "c": np.asarray(c[s], dtype=float),
        "v": np.asarray(v[s], dtype=float),
    }
    if not all(np.isfinite(a).all() for k, a in arrs.items() if k != "v"):
        return None
    if (arrs["l"] <= 0).any():
        return None
    arrs["v"] = np.nan_to_num(arrs["v"], nan=0.0)
    arrs["sma"] = _sma(np.asarray(c, dtype=float), end, window)
    return arrs


def normalised(arrs: dict[str, np.ndarray]) -> dict[str, list[float]]:
    """The window in 0..1 price units and 0..1 volume units — what the picture
    shows, in numbers. Shipped to the page so it can redraw the same chart
    without the raw prices (a reference's prices are not ours to republish)."""
    lo = float(min(arrs["l"].min(), arrs["sma"].min()))
    hi = float(max(arrs["h"].max(), arrs["sma"].max()))
    span = hi - lo or 1.0
    vmax = float(arrs["v"].max()) or 1.0

    def p(a):
        return [round((float(x) - lo) / span, 4) for x in a]

    return {
        "o": p(arrs["o"]),
        "h": p(arrs["h"]),
        "l": p(arrs["l"]),
        "c": p(arrs["c"]),
        "sma": p(arrs["sma"]),
        "v": [round(float(x) / vmax, 4) for x in arrs["v"]],
    }


def draw(arrs: dict[str, np.ndarray]) -> Image.Image:
    n = len(arrs["c"])
    W = H = SIZE * _SCALE
    img = Image.new("RGB", (W, H), _BG)
    g = ImageDraw.Draw(img)
    norm = normalised(arrs)

    price_h = H * _PRICE_FRACTION
    vol_top = H * (_PRICE_FRACTION + _GAP)
    vol_h = H - vol_top
    step = W / n
    body = max(1.0, step * 0.6)

    def y(value: float) -> float:
        # 1 px margin so the extreme wick is not clipped by the frame
        return 1 + (1 - value) * (price_h - 2)

    for i in range(n):
        x = (i + 0.5) * step
        top, bot = y(norm["h"][i]), y(norm["l"][i])
        up = norm["c"][i] >= norm["o"][i]
        colour = _UP if up else _DOWN
        g.line([(x, top), (x, bot)], fill=colour, width=1)
        y0, y1 = sorted((y(norm["o"][i]), y(norm["c"][i])))
        g.rectangle([x - body / 2, y0, x + body / 2, max(y1, y0 + 1)], fill=colour)
        vh = norm["v"][i] * vol_h
        g.rectangle([x - body / 2, H - vh, x + body / 2, H], fill=_VOL)

    pts = [((i + 0.5) * step, y(norm["sma"][i])) for i in range(n)]
    g.line(pts, fill=_SMA, width=2)
    return img.resize((SIZE, SIZE), Image.LANCZOS)


def picture(o, h, l, c, v, end: int, window: int = WINDOW) -> tuple[Image.Image, dict[str, list[float]]] | None:
    arrs = window_arrays(o, h, l, c, v, end, window)
    if arrs is None:
        return None
    return draw(arrs), normalised(arrs)


AFTER_SESSIONS = 40


def extended(o, h, l, c, v, end: int, dates=None, after: int = AFTER_SESSIONS) -> dict | None:
    """The picture's window plus up to `after` sessions that followed, for
    showing a setup next to how it played out. Normalised on the setup window's
    own range — exactly as the model saw it — so the sessions after can run
    above 1 or below 0. Carries the real price range and the dates so a page
    can label it. Display only: nothing here is ever fed back to the model."""
    arrs = window_arrays(o, h, l, c, v, end)
    if arrs is None:
        return None
    lo = float(min(arrs["l"].min(), arrs["sma"].min()))
    hi = float(max(arrs["h"].max(), arrs["sma"].max()))
    span = hi - lo or 1.0
    vmax = float(arrs["v"].max()) or 1.0
    last = min(len(c) - 1, end + after)
    s = slice(end - WINDOW + 1, last + 1)
    cc = np.asarray(c, dtype=float)
    sma = [float(cc[max(0, i - SMA_LEN + 1): i + 1].mean()) for i in range(end - WINDOW + 1, last + 1)]

    def p(a):
        return [round((float(x) - lo) / span, 3) for x in a]

    out = {
        "o": p(np.asarray(o, dtype=float)[s]),
        "h": p(np.asarray(h, dtype=float)[s]),
        "l": p(np.asarray(l, dtype=float)[s]),
        "c": p(cc[s]),
        "sma": p(sma),
        "v": [round(float(x) / vmax, 3) for x in np.nan_to_num(np.asarray(v, dtype=float)[s])],
        "setup_index": WINDOW - 1,
        "lo": round(lo, 4),
        "hi": round(hi, 4),
    }
    if dates is not None:
        out["dates"] = {
            "start": dates[end - WINDOW + 1].isoformat(),
            "setup": dates[end].isoformat(),
            "end": dates[last].isoformat(),
        }
    return out
