"""Measured shape of a chart — the precise check behind "these look the same".

The image fingerprint says two charts look alike at a glance. These features
say whether they agree on the details a trader reads: how far below its high,
how deep the base, the size of each pullback in turn, whether the range is
tightening, whether volume has dried up, where price sits against its 50-day
average, and the trend over six months and a year. All are percentages or
ratios, so a ₹50 stock and a ₹5,000 one compare directly. Everything reads
bars up to `end` only.

Used to re-rank the closest matches (`similarity.py`), never to choose picks:
picking still rests on the validated style classifier and the trader's rules.
"""

from __future__ import annotations

import numpy as np

NAMES = (
    "below_high_pct",
    "base_depth_pct",
    "pullback_1_pct",
    "pullback_2_pct",
    "pullback_3_pct",
    "tightness",
    "volume_ratio",
    "vs_sma50_pct",
    "sma50_slope_pct",
    "return_120_pct",
    "return_250_pct",
)
ZIGZAG_PCT = 4.0
MIN_BARS = 170


def _pullbacks(c: np.ndarray, threshold: float = ZIGZAG_PCT) -> list[float]:
    """Depths of the swings down, most recent first: a peak-to-trough fall of
    at least `threshold`% before the next rise of the same size."""
    peak = trough = c[0]
    falling = False
    out: list[float] = []
    for x in c[1:]:
        if not falling:
            if x > peak:
                peak = x
            elif (peak - x) / peak * 100 >= threshold:
                falling, trough = True, x
        else:
            if x < trough:
                trough = x
            elif (x - trough) / trough * 100 >= threshold:
                out.append((peak - trough) / peak * 100)
                falling, peak = False, x
    if falling:
        out.append((peak - trough) / peak * 100)
    return out[::-1]


def features(o, h, l, c, v, end: int) -> np.ndarray | None:
    """The shape vector at index `end`; nan where history is too short for a
    feature. None without ~170 sessions."""
    if end < MIN_BARS - 1 or end >= len(c):
        return None
    c = np.asarray(c[: end + 1], dtype=float)
    h = np.asarray(h[: end + 1], dtype=float)
    l = np.asarray(l[: end + 1], dtype=float)
    v = np.nan_to_num(np.asarray(v[: end + 1], dtype=float))
    price = c[-1]
    if not np.isfinite(price) or price <= 0:
        return None
    hi120 = h[-120:].max()
    hi60, lo60 = h[-60:].max(), l[-60:].min()
    pulls = _pullbacks(c[-120:])
    pulls = (pulls + [np.nan] * 3)[:3]

    def rng(a, b):
        return (h[a:b].max() - l[a:b].min()) / c[b - 1]

    n = len(c)
    recent = rng(n - 10, n)
    base = np.mean([rng(s, s + 10) for s in range(n - 70, n - 10, 10)])
    vol50 = v[-50:].mean()
    sma50 = c[-50:].mean()
    sma50_then = c[-70:-20].mean()
    return np.array([
        (1 - price / hi120) * 100,
        (hi60 - lo60) / hi60 * 100,
        *pulls,
        recent / base if base > 0 else np.nan,
        v[-10:].mean() / vol50 if vol50 > 0 else np.nan,
        (price / sma50 - 1) * 100,
        (sma50 / sma50_then - 1) * 100,
        (price / c[-120] - 1) * 100,
        (price / c[-250] - 1) * 100 if n >= 250 else np.nan,
    ], dtype=float)


def robust_scale(F: np.ndarray) -> np.ndarray:
    """Per-feature spread (MAD x 1.4826) across a population, so a 3-point gap
    in distance-from-high and a 0.2 gap in the volume ratio count alike."""
    med = np.nanmedian(F, axis=0)
    mad = np.nanmedian(np.abs(F - med), axis=0) * 1.4826
    return np.where(np.isfinite(mad) & (mad > 0), mad, 1.0)


def distance(A: np.ndarray, B: np.ndarray, scale: np.ndarray) -> np.ndarray:
    """Mean scaled absolute difference between every row of A and every row of
    B, over the features both have, each capped at 3 so one wild feature cannot
    decide a match. Shape (len(A), len(B))."""
    D = np.abs(A[:, None, :] - B[None, :, :]) / scale[None, None, :]
    D = np.minimum(D, 3.0)
    valid = np.isfinite(D)
    return np.where(valid, D, 0.0).sum(axis=2) / np.maximum(valid.sum(axis=2), 1)
