"""Breadth over the market-cap universe — the series the Markets page plots.

Why this exists
---------------
The page used to read `free_historical_breadth.json`, which covers **Nifty 500
only**, is gitignored, and is written only by a full snapshot refresh that the
deployed Space never runs. In production the file is simply absent, so the
Markets page fell all the way back to the XP universe's single %-above-20-EMA
line — one series, a different universe, and nothing structural.

This module defines a third source that fixes both halves of that: every NSE
name above a market-cap floor (₹1,000 cr by default), carrying the three
averages a swing trader actually reads — the 21-EMA, the 50-DMA and the
200-DMA. The file is small (a handful of numbers per session), committed, and
refreshed by the daily bhavcopy job, so production has it.

Shape of `breadth_mcap_history.json`::

    {
      "generated_at": "2026-08-16T...",
      "universe": "NSE stocks over Rs 1,000 cr",
      "market_cap_floor_crore": 1000.0,
      "symbols": 1525,
      "days": [
        {"date": "2024-01-02", "total": 1480,
         "above_ema21_pct": 55.1, "above_sma50_pct": 60.2, "above_sma200_pct": 71.0},
        ...
      ]
    }

Every percentage is optional and `None` means "not computable", never zero: a
200-DMA does not exist until a stock has 200 bars, and a 0 there would read as
"no stock is above its 200-DMA" — a maximally bearish number invented out of
missing data. Same rule the rest of the market frame enforces.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

HISTORY_FILENAME = "breadth_mcap_history.json"

# ₹1,000 cr. Below this the universe fills up with names whose "breadth" is
# really just illiquidity, and the free universe's own floor (₹800 cr) already
# sits just under it, so almost nothing is lost to missing coverage.
DEFAULT_MCAP_FLOOR_CRORE = 1000.0

# Percentage keys carried per session, in plot order.
#
# Both a 20-EMA and a 21-EMA, which look redundant and are not quite: the rest
# of the app counts its fast average off the snapshot's `ema20` field, so a
# 20-EMA here is the one series that can be compared like-for-like with the
# posture tile. The 21-EMA is the swing-trading convention (one trading month)
# and is what the chart leads with. They typically sit 1-3 points apart.
METRIC_KEYS = (
    "above_ema20_pct",
    "above_ema21_pct",
    "above_sma50_pct",
    "above_sma200_pct",
)

# A session computed off a small slice of the universe is not a reading of the
# universe. Partial bhavcopies and half-downloaded yfinance chunks both show up
# this way, and served raw they draw spikes that never happened.
MIN_COVERAGE_FRACTION = 0.55


def universe_label(floor_crore: float = DEFAULT_MCAP_FLOOR_CRORE) -> str:
    """Human label for the universe, e.g. "NSE stocks over Rs 1,000 cr"."""
    return f"NSE stocks over Rs {floor_crore:,.0f} cr"


def universe_symbols(
    universe: Iterable[Mapping[str, Any]],
    floor_crore: float = DEFAULT_MCAP_FLOOR_CRORE,
) -> set[str]:
    """Uppercased symbols from `free_universe.json` at or above the floor."""
    out: set[str] = set()
    for row in universe or []:
        if not isinstance(row, Mapping):
            continue
        try:
            cap = float(row.get("market_cap_crore") or 0.0)
        except (TypeError, ValueError):
            continue
        if cap < floor_crore:
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            out.add(symbol)
    return out


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None  # NaN check without importing math


def rows_by_date(doc: Mapping[str, Any] | None) -> dict[str, dict]:
    """Usable sessions keyed by ISO date.

    A row survives only if at least one of the three percentages is present —
    an all-null row carries nothing and would just be a gap in every series.
    """
    rows: dict[str, dict] = {}
    for row in (doc or {}).get("days") or []:
        if not isinstance(row, Mapping):
            continue
        iso = str(row.get("date") or "")
        if not iso:
            continue
        if not any(_to_float(row.get(key)) is not None for key in METRIC_KEYS):
            continue
        rows[iso] = dict(row)
    return rows


def merge_days(existing: Iterable[Mapping[str, Any]], fresh: Iterable[Mapping[str, Any]]) -> list[dict]:
    """Fresh rows win per date; everything else is preserved, oldest first.

    The daily job only recomputes a trailing window, and the committed file
    reaches back years — so a merge, never a replace. Rewriting a date that is
    already stored is deliberate: it is how a session first written off a
    partial feed gets corrected on the next run.
    """
    merged: dict[str, dict] = {}
    for row in existing or []:
        if isinstance(row, Mapping) and row.get("date"):
            merged[str(row["date"])] = dict(row)
    for row in fresh or []:
        if isinstance(row, Mapping) and row.get("date"):
            merged[str(row["date"])] = dict(row)
    return [merged[iso] for iso in sorted(merged)]


def build_day_row(
    date_iso: str,
    counts: Mapping[str, tuple[int, int]],
    *,
    total: int,
) -> dict:
    """One session's row.

    `counts` maps each key in `METRIC_KEYS` to `(above, eligible)`. A metric
    with no eligible stocks is null, not 0: a 200-DMA does not exist until a
    stock has 200 bars, and a 0 there would read as "no stock is above its
    200-DMA" — the most bearish number there is, invented out of a missing
    average. A key absent from `counts` is null for the same reason.
    """

    def pct(key: str) -> float | None:
        above, eligible = counts.get(key, (0, 0))
        return round(above / eligible * 100.0, 2) if eligible else None

    row: dict = {"date": date_iso, "total": total}
    row.update({key: pct(key) for key in METRIC_KEYS})
    return row
