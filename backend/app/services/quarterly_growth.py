"""Growth and margin figures per company, computed from BSE's own quarterly filings.

The AI scanner's fundamental filters ("sales growing 20%+", "profit up 30% YoY",
"operating margin above 15%") read from here. Every number derives from the
committed `quarterly_summary.json` (`bse_quarterly`), which works on the Space —
Screener.in, the other fundamentals source, refuses it (gotcha 137).

Conventions, chosen so a filter never passes on a number that is not real:

* YoY compares a quarter with the SAME calendar quarter a year earlier, found by
  period label rather than list position, so a missing filing never pairs Jun
  with Mar.
* Growth on a zero or negative base is None, not a huge percentage: a loss
  turning into a profit is a turnaround, and "profit up 4,000%" would sail
  through any growth filter. A None fails a min/max filter.
* A company whose newest filed quarter ended more than `STALE_AFTER_DAYS` ago
  has no current figures — last year's growth is not this year's.
* Figures are STANDALONE (as BSE files them) and EPS is as filed, so a bonus or
  split reads as an EPS drop. Net profit growth is the safer reading of
  "earnings growth".
"""

from __future__ import annotations

import calendar
import threading
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from app.services import bse_quarterly

STALE_AFTER_DAYS = 240

_MONTHS = {name: index for index, name in enumerate(calendar.month_abbr) if name}


@dataclass(frozen=True)
class QuarterlyGrowth:
    latest_period: str
    latest_period_end: date
    latest_sales_crore: float | None = None
    latest_net_profit_crore: float | None = None
    sales_growth_yoy_pct: float | None = None
    profit_growth_yoy_pct: float | None = None
    eps_growth_yoy_pct: float | None = None
    sales_growth_qoq_pct: float | None = None
    profit_growth_qoq_pct: float | None = None
    sales_growth_ttm_pct: float | None = None
    profit_growth_ttm_pct: float | None = None
    operating_margin_pct: float | None = None
    net_margin_pct: float | None = None
    operating_margin_change_yoy_pp: float | None = None
    # YoY growth of the newest quarters, newest first (up to 4). A None entry
    # means that quarter's growth could not be measured.
    sales_growth_yoy_series: tuple[float | None, ...] = field(default_factory=tuple)
    profit_growth_yoy_series: tuple[float | None, ...] = field(default_factory=tuple)


def period_end(label: str) -> date | None:
    """'Jun 2026' -> 2026-06-30."""
    try:
        month_name, year_text = str(label).split(" ")
        month = _MONTHS[month_name[:3].title()]
        year = int(year_text)
    except (ValueError, KeyError):
        return None
    return date(year, month, calendar.monthrange(year, month)[1])


def _shift(label: str, months: int) -> str | None:
    end = period_end(label)
    if end is None:
        return None
    index = end.year * 12 + (end.month - 1) + months
    year, month = divmod(index, 12)
    return f"{calendar.month_abbr[month + 1]} {year}"


def growth_pct(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None or prior <= 0:
        return None
    return round((current / prior - 1) * 100, 2)


def _margin(numerator: float | None, sales: float | None) -> float | None:
    if numerator is None or sales is None or sales <= 0:
        return None
    return round(numerator / sales * 100, 2)


def _sum(rows: list[dict[str, Any] | None], key: str) -> float | None:
    values = [row.get(key) if row else None for row in rows]
    if any(value is None for value in values):
        return None
    return float(sum(values))


def compute(rows: list[dict[str, Any]], *, today: date | None = None) -> QuarterlyGrowth | None:
    """Newest-first quarterly rows (the `bse_quarterly.results_for` shape) ->
    growth figures, or None when there is no current quarter."""
    by_period: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        label = row.get("period")
        if label and period_end(label) is not None:
            by_period.setdefault(label, row)
    if not by_period:
        return None
    ordered = sorted(by_period, key=lambda label: period_end(label), reverse=True)
    latest_label = ordered[0]
    latest_end = period_end(latest_label)
    if latest_end is None:
        return None
    if ((today or date.today()) - latest_end).days > STALE_AFTER_DAYS:
        return None

    def row_at(label: str | None) -> dict[str, Any] | None:
        return by_period.get(label) if label else None

    def get(label: str | None, key: str) -> float | None:
        row = row_at(label)
        return row.get(key) if row else None

    latest = by_period[latest_label]
    year_ago = _shift(latest_label, -12)
    quarter_ago = _shift(latest_label, -3)

    sales_series: list[float | None] = []
    profit_series: list[float | None] = []
    for back in range(4):
        label = _shift(latest_label, -3 * back)
        if label not in by_period:
            break
        prior = _shift(label, -12)
        sales_series.append(growth_pct(get(label, "sales_crore"), get(prior, "sales_crore")))
        profit_series.append(growth_pct(get(label, "net_profit_crore"), get(prior, "net_profit_crore")))

    recent4 = [row_at(_shift(latest_label, -3 * back)) for back in range(4)]
    prior4 = [row_at(_shift(latest_label, -3 * back - 12)) for back in range(4)]

    operating_margin = latest.get("operating_margin_pct")
    if operating_margin is None:
        operating_margin = _margin(latest.get("operating_profit_crore"), latest.get("sales_crore"))
    year_ago_margin = get(year_ago, "operating_margin_pct")
    if year_ago_margin is None:
        year_ago_margin = _margin(get(year_ago, "operating_profit_crore"), get(year_ago, "sales_crore"))

    return QuarterlyGrowth(
        latest_period=latest_label,
        latest_period_end=latest_end,
        latest_sales_crore=latest.get("sales_crore"),
        latest_net_profit_crore=latest.get("net_profit_crore"),
        sales_growth_yoy_pct=growth_pct(latest.get("sales_crore"), get(year_ago, "sales_crore")),
        profit_growth_yoy_pct=growth_pct(latest.get("net_profit_crore"), get(year_ago, "net_profit_crore")),
        eps_growth_yoy_pct=growth_pct(latest.get("eps"), get(year_ago, "eps")),
        sales_growth_qoq_pct=growth_pct(latest.get("sales_crore"), get(quarter_ago, "sales_crore")),
        profit_growth_qoq_pct=growth_pct(latest.get("net_profit_crore"), get(quarter_ago, "net_profit_crore")),
        sales_growth_ttm_pct=growth_pct(_sum(recent4, "sales_crore"), _sum(prior4, "sales_crore")),
        profit_growth_ttm_pct=growth_pct(_sum(recent4, "net_profit_crore"), _sum(prior4, "net_profit_crore")),
        operating_margin_pct=operating_margin,
        net_margin_pct=_margin(latest.get("net_profit_crore"), latest.get("sales_crore")),
        operating_margin_change_yoy_pp=(
            round(operating_margin - year_ago_margin, 2)
            if operating_margin is not None and year_ago_margin is not None
            else None
        ),
        sales_growth_yoy_series=tuple(sales_series),
        profit_growth_yoy_series=tuple(profit_series),
    )


_lock = threading.Lock()
_cache: tuple[str | None, date, dict[str, QuarterlyGrowth | None]] | None = None


def for_symbol(symbol: str) -> QuarterlyGrowth | None:
    """Cached per artifact build and per day (staleness depends on today)."""
    global _cache
    stamp = bse_quarterly.artifact_date()
    today = date.today()
    with _lock:
        if _cache is None or _cache[0] != stamp or _cache[1] != today:
            _cache = (stamp, today, {})
        table = _cache[2]
        key = symbol.upper()
        if key not in table:
            table[key] = compute(bse_quarterly.results_for(key), today=today)
        return table[key]


def artifact_date() -> str | None:
    return bse_quarterly.artifact_date()


def reset_cache() -> None:
    global _cache
    with _lock:
        _cache = None
