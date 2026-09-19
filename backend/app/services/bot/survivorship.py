"""Measure how much the backtest is flattered by only knowing today's winners.

The universe is `free_universe.json` — companies listed, liquid and above the
market-cap floor *now*. Every company that went bankrupt, was delisted, or
simply shrank out of the floor between 2007 and today is missing. So a trade
simulated in 2009 was taken in a company we already know survived the next
seventeen years, and no one trading in 2009 had that information.

This inflates historical results, and it inflates them *unevenly*: 40% of
today's universe existed in 2008 versus 95% in 2026, so the further back a
period sits the harder it has been filtered. That gradient is measurable even
without point-in-time constituent lists, which the free sources do not publish:

  - `coverage_pct` per era says how much of today's universe existed then, and
    therefore how strong the filter was.
  - `avg_r` per era says how well the strategies did.

If performance rises as coverage falls, the extra return is the filter rather
than an edge. The number is reported rather than corrected, because correcting
it would need the delisted names themselves. What the system does instead is
structural: the walk-forward split puts the held-out period in recent history,
where coverage is highest, so every cell in the playbook was confirmed on the
least-biased data available. The decay of every `recovery` cell — strong
in-sample on 2009/2012/2020, negative out-of-sample on recent data — is this
effect caught in the act, and is why `recovery` ends up as stand-down.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path

import numpy as np

from .engine import Trade
from .history import available_symbols, read_bars

# Era boundaries chosen on market history (post-GFC, taper era, pre-COVID,
# post-COVID) rather than to produce a tidy gradient.
ERAS: tuple[tuple[str, date, date], ...] = (
    ("2007-2012", date(2007, 1, 1), date(2012, 12, 31)),
    ("2013-2017", date(2013, 1, 1), date(2017, 12, 31)),
    ("2018-2021", date(2018, 1, 1), date(2021, 12, 31)),
    ("2022-today", date(2022, 1, 1), date(2099, 12, 31)),
)


@dataclass
class EraStats:
    era: str
    start: str
    end: str
    symbols_existing: int
    coverage_pct: float       # % of today's universe that existed then
    trades: int
    avg_r: float
    win_rate: float
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


def coverage_by_era(data_dir: Path) -> dict[str, tuple[int, float]]:
    """How many of today's symbols had already listed at each era's start."""
    symbols = available_symbols(data_dir)
    total = len(symbols) or 1
    firsts: list[date] = []
    for symbol in symbols:
        bars = read_bars(data_dir, symbol)
        if bars is not None and bars.first_date:
            firsts.append(bars.first_date)

    out: dict[str, tuple[int, float]] = {}
    for name, start, _end in ERAS:
        existing = sum(1 for d in firsts if d <= start)
        out[name] = (existing, round(100.0 * existing / total, 1))
    return out


def analyse(data_dir: Path, trades: list[Trade]) -> dict:
    """Era-by-era performance against era-by-era universe coverage."""
    coverage = coverage_by_era(data_dir)
    resolved = [t for t in trades if t.resolved]

    rows: list[EraStats] = []
    for name, start, end in ERAS:
        window = [t for t in resolved if start <= t.entry_day <= end]
        existing, pct = coverage.get(name, (0, 0.0))
        if window:
            r = np.array([t.r_multiple for t in window])
            avg_r = round(float(r.mean()), 3)
            win = round(100.0 * float((r > 0).mean()), 1)
        else:
            avg_r, win = 0.0, 0.0
        rows.append(
            EraStats(
                era=name,
                start=start.isoformat(),
                end=end.isoformat() if end.year < 2099 else "today",
                symbols_existing=existing,
                coverage_pct=pct,
                trades=len(window),
                avg_r=avg_r,
                win_rate=win,
                note=(
                    f"{100.0 - pct:.0f}% of today's universe had not yet listed, so this era is "
                    "the most heavily filtered by survival." if pct < 60 else
                    "Coverage is high here; survivorship distortion is smallest."
                ),
            )
        )

    scored = [r for r in rows if r.trades >= 100]
    gradient = None
    if len(scored) >= 3:
        cov = np.array([r.coverage_pct for r in scored])
        perf = np.array([r.avg_r for r in scored])
        if cov.std() > 0 and perf.std() > 0:
            gradient = round(float(np.corrcoef(cov, perf)[0, 1]), 2)

    if gradient is None:
        verdict = "Not enough eras with a usable sample to measure the gradient."
    elif gradient < -0.5:
        verdict = (
            f"Performance falls as universe coverage rises (correlation {gradient}). That is the "
            "survivorship signature: the older, more heavily filtered eras look best. Treat any "
            "pre-2018 result as an upper bound, and rely on the held-out period instead."
        )
    elif gradient > 0.5:
        verdict = (
            f"Performance rises with coverage (correlation {gradient}) — the opposite of the "
            "survivorship signature, so the recent, better-covered era is carrying the result "
            "on its own merits."
        )
    else:
        verdict = (
            f"No clear relationship between coverage and performance (correlation {gradient}). "
            "Survivorship is still present but is not obviously driving the headline numbers."
        )

    return {
        "eras": [r.to_dict() for r in rows],
        "coverage_performance_correlation": gradient,
        "verdict": verdict,
        "limitation": (
            "Delisted and bankrupt companies are absent from the universe entirely. This cannot "
            "be corrected without point-in-time constituent lists, which the free data sources "
            "do not publish. The mitigation is structural, not statistical: the playbook is built "
            "only from the held-out recent period, where coverage is highest."
        ),
    }
