"""How alike two charts are: three layers on top of the image fingerprint.

1. **Chart-trained layer** (`projection.py`) — the fingerprint passed through
   a small network trained on Indian charts to keep the same pattern together
   even when shifted a few sessions or stretched in time. Used only if it beat
   the raw fingerprint on stocks it never saw; otherwise the raw one is used.
2. **More than one time frame** — the 60-, 120- and 250-session pictures are
   compared separately and blended (`SCALE_WEIGHTS`), so a match must fit the
   recent base and the larger trend, not just the middle window.
3. **Precise shape check** (`shape.py`) — among the closest `RERANK_POOL` by
   look, matches are re-ordered half by look and half by how well their
   measured shape agrees: distance from the high, base depth, each pullback,
   tightness, volume, position against the 50-day, trend.

The blend is rank-based within each query's pool, so neither a cosine nor a
feature distance needs a hand-tuned exchange rate. The displayed "% alike" is
the blended look (layers 1-2); layer 3 only changes the order.
"""

from __future__ import annotations

import numpy as np

from . import projection, shape

SCALE_WEIGHTS = {60: 0.25, 120: 0.5, 250: 0.25}
RERANK_POOL = 40
SHAPE_WEIGHT = 0.5


def _unit(X: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(X, axis=1, keepdims=True)
    return np.where(n > 0, X / np.where(n > 0, n, 1), 0.0)


def blended_cosine(Q: dict[int, np.ndarray], R: dict[int, np.ndarray], head) -> np.ndarray:
    """(len(Q), len(R)) similarity blended across the scales both sides have.
    A row of zeros (or nan) at a scale means "no picture at this scale" and that
    scale's weight is redistributed for the pair."""
    num = 0.0
    den = 0.0
    for scale, w in SCALE_WEIGHTS.items():
        if scale not in Q or scale not in R:
            continue
        q, r = np.nan_to_num(Q[scale]), np.nan_to_num(R[scale])
        hq = (np.abs(q).sum(axis=1) > 0).astype(float)
        hr = (np.abs(r).sum(axis=1) > 0).astype(float)
        eq = _unit(projection.embed(head, q)) * hq[:, None]
        er = _unit(projection.embed(head, r)) * hr[:, None]
        both = hq[:, None] * hr[None, :]
        num = num + w * (eq @ er.T)
        den = den + w * both
    return np.where(np.asarray(den) > 0, num / np.where(np.asarray(den) > 0, den, 1), 0.0)


def ranked(cos_row: np.ndarray, shape_row: np.ndarray | None, k: int, exclude: int | None = None) -> np.ndarray:
    """Indices of the best `k` matches for one query: the `RERANK_POOL` closest
    by look, re-ordered half by look and half by shape agreement."""
    row = cos_row.copy()
    if exclude is not None:
        row[exclude] = -np.inf
    pool = np.argsort(-row)[:RERANK_POOL]
    pool = pool[np.isfinite(row[pool])]
    if shape_row is None or not len(pool):
        return pool[:k]
    # Ties share a rank (count of strictly better candidates): two matches that
    # look exactly as alike must be separated by shape, not by list position.
    look = row[pool]
    look_rank = (look[None, :] > look[:, None]).sum(axis=1) / max(1, len(pool) - 1)
    d = shape_row[pool]
    d = np.where(np.isfinite(d), d, np.nanmax(d) if np.isfinite(d).any() else 0)
    shape_rank = (d[None, :] < d[:, None]).sum(axis=1) / max(1, len(pool) - 1)
    score = (1 - SHAPE_WEIGHT) * look_rank + SHAPE_WEIGHT * shape_rank
    return pool[np.argsort(score, kind="stable")][:k]


def shape_distances(FQ: np.ndarray, FR: np.ndarray, scale: np.ndarray, chunk: int = 256) -> np.ndarray:
    out = np.empty((len(FQ), len(FR)))
    for s in range(0, len(FQ), chunk):
        out[s : s + chunk] = shape.distance(FQ[s : s + chunk], FR, scale)
    return out


def alike(row: np.ndarray, j: int) -> float:
    """What the page shows as "% alike": the share of the other candidates this
    match is closer than (0-1). The trained layer spreads raw similarities out,
    so its cosine — around 0.5 for an excellent match — reads as alarmingly low
    while meaning the opposite; a rank is what a reader can use."""
    finite = row[np.isfinite(row)]
    if len(finite) <= 1:
        return 1.0
    return float((finite < row[j]).sum() / (len(finite) - 1))
