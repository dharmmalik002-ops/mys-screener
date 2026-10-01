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

from . import embed, model, render, rules

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
    sims: dict[str, np.ndarray]


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
        rows.append((bars.symbol, bars.dates[end], float(bars.close[end]), turnover, pic[1], m))
        images.append(pic[0])
    if not rows:
        return None
    latest = max(r[1] for r in rows)
    keep = [i for i, r in enumerate(rows) if (latest - r[1]).days <= STALE_DAYS]
    rows = [rows[i] for i in keep]
    images = [images[i] for i in keep]

    X = embed.fingerprints(images)
    logits, pct, sims = {}, {}, {}
    for style in library.styles:
        lg = library.clf[style].logit(X)
        logits[style] = lg
        pct[style] = model.percentile_against(lg, library.cal_logits[style])
        sims[style] = X @ library.X_ref[style].T
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
    )


def tradingview_india(symbol: str) -> str:
    # TradingView spells NSE symbols with underscores where NSE uses & or -.
    return "https://www.tradingview.com/chart/?symbol=NSE%3A" + symbol.replace("&", "_").replace("-", "_")


def tradingview_us(ticker: str) -> str:
    return "https://www.tradingview.com/chart/?symbol=" + ticker.replace(".", "-")
