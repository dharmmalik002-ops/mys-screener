"""Bars read off a reference chart's own image, for stocks Yahoo cannot supply.

Old newsletters chart many companies that have since been delisted, acquired
or renamed — and some symbols now belong to a different company. For those,
the bars were read off the newsletter's chart (price axis by OCR, one OHLC bar
per session, dated on the US trading calendar ending on the chart's date) by
the Zanger import (`scripts/import_zanger_newsletters.py`), checked against
Yahoo wherever Yahoo has the stock: a chart whose prices disagree with Yahoo's
is a reused symbol, and its own bars are used instead.

Stored under `data/lookalike/chart_bars/<TICKER>@<date>.json` (gitignored, the
newsletter's own data) in the us_bars shape, with `verdicts.json` naming which
source each reference uses. A chart-read reference has no sessions after its
date, so it teaches what the style looks like but never counts towards how
often the style worked.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import numpy as np

from . import render, us_bars

VERDICTS = "verdicts.json"
YAHOO, CHART, NONE = "yahoo", "chart", "none"


def folder(data_dir: Path) -> Path:
    return data_dir / "lookalike" / "chart_bars"


def verdicts(data_dir: Path) -> dict[str, str]:
    """reference key (TICKER@YYYY-MM-DD) -> which prices to use. A reference
    with no verdict uses Yahoo, as every reference did before this existed."""
    path = folder(data_dir) / VERDICTS
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def read(data_dir: Path, key: str) -> us_bars.Series | None:
    """The chart's bars. A chart showing fewer sessions than the picture's
    50-day average needs (`render.min_bars_needed`), but at least the 120 the
    picture shows, is padded at the front with its first bar repeated: the
    padding only feeds the early part of the average line, never a bar the
    picture draws. Shorter charts are returned as they are (and skipped)."""
    path = folder(data_dir) / f"{key}.json"
    series = us_bars._read(path) if path.exists() else None
    if series is None:
        return None
    need = render.min_bars_needed()
    n = len(series.dates)
    if render.WINDOW <= n < need:
        k = need - n
        first = series.dates[0]
        dates = [first - timedelta(days=7 * (k - i) / 5 + 1) for i in range(k)] + list(series.dates)

        def pad(a, fill):
            return np.concatenate([np.full(k, fill, dtype=float), a])

        c0 = float(series.c[0])
        series = us_bars.Series(series.ticker, dates, pad(series.o, c0), pad(series.h, c0), pad(series.l, c0), pad(series.c, c0), pad(series.v, 0.0))
    return series
