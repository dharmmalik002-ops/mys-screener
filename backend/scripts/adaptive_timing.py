#!/usr/bin/env python3
"""Can the rule that EARNS evolve, or does it only work because it was frozen?

    python3 scripts/adaptive_timing.py

Every learning test in this project so far has been aimed at stock selection —
the half with no edge — or at the exit, or at which cells to stand down. The
component that actually beats a professional is regime timing, and its one
parameter, `INVESTED_REGIMES`, was chosen **once**: five candidate sets
declared in advance, scored on the first half of history, the held-out half run
once. It has not adapted since and nothing here has ever asked whether it
should.

That is the sharpest remaining form of "the strategy should evolve with market
conditions". Not a new strategy — the same rule, re-deriving its own invested
set each year from the tape it has already seen:

  * At each 1 January the learner sees index sessions and regime labels
    **strictly before** that date, and nothing after.
  * It measures each regime's realised mean daily return over that history and
    invests in the regimes that paid.
  * That set trades the following year and is scored unchanged, then the
    learner refits. Equity chains across years, so a bad year is carried.

Three baselines, all on the identical span: the **frozen** a-priori set that
ships, **buy and hold**, and the **hindsight** set derived from the whole
period at once (unreachable, included to size the prize).

Declared before running: the learner's rule is "invest in regimes whose mean
daily return in the training window was positive", with a minimum of three
years of history before the first decision. `MIN_REGIME_SESSIONS` stops a
regime seen for a fortnight from earning a place. No threshold is tuned after
seeing results — the alternative variants below are a robustness grid, not a
menu.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import timing as tm  # noqa: E402
from app.services.bot.backtest import build_context  # noqa: E402
from app.services.bot.benchmark import INDEX_KEY  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.regime import REGIMES  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("adaptive-timing")

MIN_TRAIN_YEARS = 3
MIN_REGIME_SESSIONS = 60
THRESHOLDS = (0.0, 0.0002, -0.0002)   # robustness grid, not a menu


def learn_invested_set(
    sessions: list[date],
    closes: dict[date, float],
    regime_by_day: dict[date, str],
    boundary: date,
    threshold: float,
) -> frozenset[str]:
    """Regimes that paid, using only sessions strictly before `boundary`."""
    returns: dict[str, list[float]] = defaultdict(list)
    prior = [d for d in sessions if d < boundary and d in closes]
    for i in range(1, len(prior)):
        a, b = prior[i - 1], prior[i]
        regime = regime_by_day.get(a)
        if not regime or closes[a] <= 0:
            continue
        returns[regime].append(closes[b] / closes[a] - 1.0)

    chosen = {
        regime for regime, values in returns.items()
        if len(values) >= MIN_REGIME_SESSIONS
        and (sum(values) / len(values)) > threshold
    }
    return frozenset(chosen)


def chain(
    sessions: list[date],
    closes: dict[date, float],
    regime_by_day: dict[date, str],
    years: list[int],
    pick,
) -> tuple[float, float, list[tuple[int, float, frozenset[str]]]]:
    """Run year by year, compounding. `pick(boundary)` returns the invested set."""
    equity, peak, drawdown = 1.0, 1.0, 0.0
    rows = []
    for year in years:
        window = [d for d in sessions if d.year == year]
        if len(window) < 60:
            continue
        chosen = pick(date(year, 1, 1))
        if not chosen:
            rows.append((year, 0.0, chosen))       # stood down all year
            continue
        run = tm.simulate(window, closes, regime_by_day, invested_regimes=chosen)
        if run is None:
            continue
        growth = run.equity_curve[-1]["equity"] / run.equity_curve[0]["equity"]
        equity *= growth
        peak = max(peak, equity)
        drawdown = min(drawdown, equity / peak - 1.0)
        rows.append((year, (growth - 1.0) * 100.0, chosen))
    span = max((years[-1] - years[0] + 1), 1)
    cagr = (equity ** (1.0 / span) - 1.0) * 100.0
    return cagr, drawdown * 100.0, rows


def continuous(
    sessions: list[date],
    closes: dict[date, float],
    regime_by_day: dict[date, str],
    years: list[int],
    pick,
) -> "tm.TimingResult | None":
    """One unbroken run whose invested SET changes at each year boundary.

    `chain()` restarts the simulation every January, which silently forces a
    flat-and-re-enter at each boundary and charges a switch that the rule never
    asked for. That artifact hits both arms, so the comparison survives it, but
    the absolute numbers are not the ones the system ships.

    This is the honest version: relabel each session as INVESTED or OUT using
    the set that was learned for *that* year, then run the whole span once, so
    a position held across New Year stays held and costs are charged only on
    real switches.
    """
    chosen_by_year = {y: pick(date(y, 1, 1)) for y in years}
    relabelled: dict[date, str] = {}
    window: list[date] = []
    for day in sessions:
        if day.year not in chosen_by_year:
            continue
        regime = regime_by_day.get(day)
        if regime is None:
            continue
        window.append(day)
        relabelled[day] = "INVESTED" if regime in chosen_by_year[day.year] else "OUT"
    if len(window) < 250:
        return None
    return tm.simulate(window, closes, relabelled, invested_regimes=frozenset({"INVESTED"}))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-symbols", type=int, default=0)
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    symbols = available_symbols(data_dir)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]

    context = build_context(data_dir, symbols)
    regime_by_day = {d: row.regime for d, row in context.regime_by_day.items()}
    bars = read_bars(data_dir, INDEX_KEY)
    closes = {d: float(c) for d, c in zip(bars.dates, bars.close)}
    sessions = [d for d in bars.dates if d in regime_by_day]

    first = sessions[0].year + MIN_TRAIN_YEARS
    years = sorted({d.year for d in sessions if d.year >= first})
    logger.info("scoring %d years: %s..%s", len(years), years[0], years[-1])

    hindsight = learn_invested_set(sessions, closes, regime_by_day, sessions[-1], 0.0)

    results = {}
    for threshold in THRESHOLDS:
        cagr, dd, rows = chain(
            sessions, closes, regime_by_day, years,
            lambda b, t=threshold: learn_invested_set(sessions, closes, regime_by_day, b, t),
        )
        results[f"adaptive (>{threshold:+.4f})"] = (cagr, dd, rows)

    frozen_cagr, frozen_dd, frozen_rows = chain(
        sessions, closes, regime_by_day, years, lambda b: tm.INVESTED_REGIMES
    )
    hind_cagr, hind_dd, _ = chain(
        sessions, closes, regime_by_day, years, lambda b: hindsight
    )
    hold_cagr, hold_dd, _ = chain(
        sessions, closes, regime_by_day, years, lambda b: frozenset(REGIMES)
    )

    print("\n=== WHAT THE LEARNER CHOSE, YEAR BY YEAR ===")
    primary = results["adaptive (>+0.0000)"][2]
    frozen_by_year = {y: r for y, r, _ in frozen_rows}
    print(f"{'year':>6} {'adaptive %':>11} {'frozen %':>10}   invested set it learned")
    for year, ret, chosen in primary:
        names = ",".join(sorted(chosen)) if chosen else "(stood down)"
        print(f"{year:>6} {ret:>+11.2f} {frozen_by_year.get(year, 0.0):>+10.2f}   {names}")

    print("\n=== COMPOUNDED OVER THE WHOLE SPAN ===")
    for label, (cagr, dd, _) in results.items():
        print(f"  {label:<24} CAGR {cagr:>+7.2f}%   maxDD {dd:>7.2f}%")
    print(f"  {'frozen (ships)':<24} CAGR {frozen_cagr:>+7.2f}%   maxDD {frozen_dd:>7.2f}%")
    print(f"  {'hindsight set':<24} CAGR {hind_cagr:>+7.2f}%   maxDD {hind_dd:>7.2f}%")
    print(f"  {'buy and hold':<24} CAGR {hold_cagr:>+7.2f}%   maxDD {hold_dd:>7.2f}%")

    # --- the artifact-free version, and the control that matters ----------
    print("\n=== CONTINUOUS RUN (no yearly reset) — THE HONEST ONE ===")
    print("  The chained table above samples equity once a YEAR, so it cannot see an")
    print("  intra-year drawdown at all and understates the real one roughly threefold.")
    print("  Read these rows, not those.")
    adaptive_run = continuous(
        sessions, closes, regime_by_day, years,
        lambda b: learn_invested_set(sessions, closes, regime_by_day, b, 0.0),
    )
    frozen_run = continuous(
        sessions, closes, regime_by_day, years, lambda b: tm.INVESTED_REGIMES
    )
    hind_run = continuous(sessions, closes, regime_by_day, years, lambda b: hindsight)
    hold_run = continuous(
        sessions, closes, regime_by_day, years, lambda b: frozenset(REGIMES)
    )
    for label, run in (("adaptive", adaptive_run), ("frozen (ships)", frozen_run),
                       ("hindsight", hind_run), ("buy and hold", hold_run)):
        if run is None:
            continue
        ratio = run.cagr_pct / abs(run.max_drawdown_pct) if run.max_drawdown_pct else 0.0
        print(f"  {label:<18} CAGR {run.cagr_pct:>+7.2f}%   maxDD {run.max_drawdown_pct:>7.2f}%"
              f"   ret/DD {ratio:>5.2f}   exposure {run.exposure_pct:>5.1f}%"
              f"   switches {run.switches:>3}")

    # Does it survive losing its best year? Two years carry most of the gap,
    # and a result that evaporates without them is one lucky window.
    print("\n=== LEAVE-ONE-YEAR-OUT (compounded advantage, adaptive - frozen) ===")
    frozen_by_y = {y: r for y, r, _ in frozen_rows}
    adaptive_by_y = {y: r for y, r, _ in results["adaptive (>+0.0000)"][2]}
    common = sorted(set(frozen_by_y) & set(adaptive_by_y))
    worst_case = None
    for dropped in common:
        a = f = 1.0
        for y in common:
            if y == dropped:
                continue
            a *= 1.0 + adaptive_by_y[y] / 100.0
            f *= 1.0 + frozen_by_y[y] / 100.0
        n = len(common) - 1
        gap = (a ** (1.0 / n) - 1.0) * 100.0 - (f ** (1.0 / n) - 1.0) * 100.0
        if worst_case is None or gap < worst_case[1]:
            worst_case = (dropped, gap)
    print(f"  worst case: dropping {worst_case[0]} leaves {worst_case[1]:+.2f}pp a year")
    print(f"  the advantage {'survives' if worst_case[1] > 0 else 'DOES NOT survive'} "
          f"losing its best single year")

    best = results["adaptive (>+0.0000)"][0]
    print(f"\nfrozen INVESTED_REGIMES : {sorted(tm.INVESTED_REGIMES)}")
    print(f"hindsight set           : {sorted(hindsight)}")
    print(f"adaptive - frozen       : {best - frozen_cagr:+.2f}pp CAGR")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
