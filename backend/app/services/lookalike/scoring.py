"""Score the Indian market against the library AS OF any date.

One function serves both the daily scan (as of the latest session) and the
backfill of past picks (as of each past Friday), so the two can never drift
apart. Everything for a date reads bars up to that date only: the picture, the
rule checks, the turnover filter and the staleness test all stop at `as_of`.
The library itself was built from references dated 2022 and earlier, so a pick
made for any date after that is a genuine out-of-sample test of it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np

from . import embed, model, projection, render, rules, shape, similarity

MIN_TURNOVER_CRORE = 2.0
STALE_DAYS = 6
INDEX_SYMBOLS = {"NIFTY", "NIFTY500", "BANKNIFTY"}
INDIA_INDEX = "NIFTY500"
NEAREST = 3


@dataclass
class Library:
    styles: dict[str, dict]               # style -> build summary
    refs: dict[str, list[dict]]           # style -> reference rows, in fingerprint order
    clf: dict[str, model.Logistic]
    X_ref: dict[str, np.ndarray]
    cal_logits: dict[str, np.ndarray]
    sources: dict[str, str] = field(default_factory=dict)  # ref key -> source list it came from
    # similarity layers: fingerprints per scale, measured shapes, trained head
    X_ref_scales: dict[str, dict[int, np.ndarray]] = field(default_factory=dict)
    F_ref: dict[str, np.ndarray] = field(default_factory=dict)
    head: object = None


def load_library(data_dir: Path) -> Library:
    from .pipeline import library_dir
    from .references import sources_root

    lib_dir = library_dir(data_dir)
    meta = json.loads((lib_dir / "library.json").read_text())
    arrays = np.load(lib_dir / "library.npz")
    styles = meta["styles"]
    refs = {s: [r for r in meta["references"] if r["style"] == s] for s in styles}
    sources: dict[str, str] = {}
    root = sources_root(data_dir)
    if root.exists():
        for path in root.glob("*/*.txt"):
            for line in path.read_text().splitlines():
                if "," in line:
                    t, d = line.strip().split(",", 1)
                    sources.setdefault(f"{t}@{d}", path.stem)
    return Library(
        styles=styles,
        refs=refs,
        clf={s: model.Logistic(arrays[f"{s}__w"], float(arrays[f"{s}__b"][0]), arrays[f"{s}__mean"]) for s in styles},
        X_ref={s: arrays[f"{s}__X_ref"] for s in styles},
        cal_logits={s: arrays[f"{s}__cal_logits"] for s in styles},
        sources=sources,
        X_ref_scales={
            s: {render.WINDOW: arrays[f"{s}__X_ref"], **{sc: arrays[f"{s}__X_ref{sc}"] for sc in render.SCALES if f"{s}__X_ref{sc}" in arrays}}
            for s in styles
        },
        F_ref={s: arrays[f"{s}__F_ref"] for s in styles if f"{s}__F_ref" in arrays},
        head=projection.load(data_dir),
    )


def load_universe(data_dir: Path):
    """Every Indian history held in memory once (~0.5 GB), plus the Nifty 500
    for the relative-strength rule. Worth it when scoring many dates."""
    from app.services.bot.history import iter_bars, read_bars

    bars = [b for b in iter_bars(data_dir, min_bars=render.min_bars_needed()) if b.symbol not in INDEX_SYMBOLS]
    nifty = read_bars(data_dir, INDIA_INDEX)
    index = (list(nifty.dates), nifty.close) if nifty is not None else None
    return bars, index


@dataclass
class Scored:
    as_of: date
    symbols: list[str]
    sessions: list[date]
    closes: list[float]
    turnover: list[float]
    windows: list[dict]
    flags: list[dict | None]
    metrics: list[dict | None]
    X: np.ndarray
    # per style
    logits: dict[str, np.ndarray]
    percentile: dict[str, np.ndarray]
    sims: dict[str, np.ndarray]          # blended look across scales — the displayed "% alike"
    Xs: dict[int, np.ndarray] = field(default_factory=dict)   # fingerprints per scale (zero rows = no picture)
    F: np.ndarray | None = None          # measured shapes
    shape_d: dict[str, np.ndarray] = field(default_factory=dict)  # shape distance to each style's references
    head: object = None

    def nearest(self, style: str, i: int, k: int) -> np.ndarray:
        """The closest references to stock `i`: all three similarity layers."""
        d = self.shape_d.get(style)
        return similarity.ranked(self.sims[style][i], None if d is None else d[i], k)

    def alike(self, style: str, i: int, j: int) -> float:
        return similarity.alike(self.sims[style][i], j)

    def peers(self, k: int) -> list[np.ndarray]:
        """For every stock, the other stocks whose charts look most like it."""
        cos = similarity.blended_cosine(self.Xs, self.Xs, self.head)
        d = None
        if self.F is not None:
            d = similarity.shape_distances(self.F, self.F, shape.robust_scale(self.F))
        return [similarity.ranked(cos[i], None if d is None else d[i], k, exclude=i) for i in range(len(cos))]


def _turnover_crore(close: np.ndarray, volume: np.ndarray, end: int) -> float:
    s = slice(max(0, end - 19), end + 1)
    return float(np.median(close[s] * volume[s])) / 1e7


def score(universe, index, library: Library, as_of: date | None = None) -> Scored | None:
    """Score every stock on the last session on or before `as_of` (None = each
    stock's latest bar). A stock whose last bar is more than STALE_DAYS older
    than the newest bar in the market was suspended or delisted by then."""
    rows = []
    images = []
    for bars in universe:
        if as_of is None:
            end = len(bars) - 1
        else:
            end = int(np.searchsorted(bars.dates, as_of, side="right")) - 1
        if end < render.min_bars_needed() - 1:
            continue
        turnover = _turnover_crore(bars.close, bars.volume, end)
        if turnover < MIN_TURNOVER_CRORE:
            continue
        pic = render.picture(bars.open, bars.high, bars.low, bars.close, bars.volume, end)
        if pic is None:
            continue
        ret = None
        if index is not None and end >= 126:
            ret = rules.index_return(index[0], index[1], bars.dates[end], bars.dates[end - 126])
        m = rules.metrics(bars.open, bars.high, bars.low, bars.close, bars.volume, end, ret)
        extra = {}
        for sc in render.SCALES:
            if sc == render.WINDOW:
                continue
            ep = render.picture(bars.open, bars.high, bars.low, bars.close, bars.volume, end, sc)
            extra[sc] = ep[0] if ep else None
        f = shape.features(bars.open, bars.high, bars.low, bars.close, bars.volume, end)
        rows.append((bars.symbol, bars.dates[end], float(bars.close[end]), turnover, pic[1], m, extra, f))
        images.append(pic[0])
    if not rows:
        return None
    latest = max(r[1] for r in rows)
    keep = [i for i, r in enumerate(rows) if (latest - r[1]).days <= STALE_DAYS]
    rows = [rows[i] for i in keep]
    images = [images[i] for i in keep]

    X = embed.fingerprints(images)
    Xs = {render.WINDOW: X}
    for sc in render.SCALES:
        if sc == render.WINDOW:
            continue
        imgs = [r[6][sc] for r in rows]
        have = [i for i, im in enumerate(imgs) if im is not None]
        Xsc = np.zeros_like(X)
        if have:
            Xsc[have] = embed.fingerprints([imgs[i] for i in have])
        Xs[sc] = Xsc
    F = np.array([r[7] if r[7] is not None else np.full(len(shape.NAMES), np.nan) for r in rows])
    F_scale = shape.robust_scale(F)
    logits, pct, sims, shape_d = {}, {}, {}, {}
    for style in library.styles:
        lg = library.clf[style].logit(X)
        logits[style] = lg
        pct[style] = model.percentile_against(lg, library.cal_logits[style])
        sims[style] = similarity.blended_cosine(Xs, library.X_ref_scales.get(style, {render.WINDOW: library.X_ref[style]}), library.head)
        if style in library.F_ref:
            shape_d[style] = similarity.shape_distances(F, library.F_ref[style], F_scale)
    return Scored(
        as_of=latest,
        symbols=[r[0] for r in rows],
        sessions=[r[1] for r in rows],
        closes=[r[2] for r in rows],
        turnover=[r[3] for r in rows],
        windows=[r[4] for r in rows],
        metrics=[r[5] for r in rows],
        flags=[None if r[5] is None else rules.flags_from(r[5]) for r in rows],
        X=X,
        logits=logits,
        percentile=pct,
        sims=sims,
        Xs=Xs,
        F=F,
        shape_d=shape_d,
        head=library.head,
    )


def tradingview_india(symbol: str) -> str:
    # TradingView spells NSE symbols with underscores where NSE uses & or -.
    return "https://www.tradingview.com/chart/?symbol=NSE%3A" + symbol.replace("&", "_").replace("-", "_")


def tradingview_us(ticker: str) -> str:
    return "https://www.tradingview.com/chart/?symbol=" + ticker.replace(".", "-")
