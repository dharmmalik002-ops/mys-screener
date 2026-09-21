#!/usr/bin/env python3
"""Does the bot get better at exiting by watching its own closed trades?

    python3 scripts/adaptive_exit_walkforward.py

Every learning test in this project so far has asked the same question in a
different place: *does past performance of X predict future performance of X?*
Strategy x regime cells, book structures, exposure, individual symbols — all
null. But all four ask about **stock selection**, and selection is the axis
this system has already proved is empty (-2.60%/yr walk-forward).

This asks a different question on a different axis. The exit is the single
highest-leverage parameter in the system: choosing it moved the result from
-0.005R to +0.28R per trade, which is larger than any selection effect ever
measured here. And market state — the axis the exit choice would key off — is
the one axis that demonstrably carries information, because the regime timing
rule beats the professional benchmark.

So: **if the bot re-picks its exit rule each year from the trades it has
already closed, does it beat the exit rule that was frozen in once?**

Protocol, fixed before running:

  1. The candidate rules are the five declared in `sweep_exit_models.py`. No
     new rules, no grid search, no parameters invented after seeing results.
  2. At each year boundary the chooser sees ONLY trades that had already
     CLOSED before that boundary. Not trades still open, not trades entered
     later. This is the information a live bot would actually have.
  3. The choice is applied to trades entered during the following year and
     scored unchanged.
  4. Three baselines are reported beside it: the frozen incumbent, every fixed
     candidate, and the in-hindsight best rule per year (unreachable, included
     to show the size of the prize).

Two chooser variants run: one global, one per regime. The per-regime variant
is the "strategy evolves with market conditions" claim stated precisely enough
to be false.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import statistics
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot.backtest import BacktestConfig, build_context, run_strategies_multi  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402
from app.services.bot.exit_learning import (  # noqa: E402
    MIN_PRIOR_TRADES,
    choose_exit,
    prior_returns,
)
# data_dir is BACKEND_ROOT/"data"; the store lives in data/deep_history

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("adaptive-exit")

# The five rules from the original sweep, unchanged. `trend_loose_90d` is the
# incumbent that protocol selected and that ships in ExitModel's defaults.
CANDIDATES: dict[str, ExitModel] = {
    "swing_2.5R_15d": ExitModel(target_r=2.5, max_hold_sessions=15, trail_after_r=1.5,
                                trail_atr_mult=2.5, breakeven_after_r=1.0),
    "position_3R_40d": ExitModel(target_r=3.0, max_hold_sessions=40, trail_after_r=2.0,
                                 trail_atr_mult=3.0, breakeven_after_r=1.5),
    "trend_notarget_60d": ExitModel(target_r=None, max_hold_sessions=60, trail_after_r=1.0,
                                    trail_atr_mult=3.0, breakeven_after_r=None),
    "trend_loose_90d": ExitModel(target_r=None, max_hold_sessions=90, trail_after_r=1.5,
                                 trail_atr_mult=4.0, breakeven_after_r=None),
    "quick_2R_25d": ExitModel(target_r=2.0, max_hold_sessions=25, trail_after_r=None,
                              trail_atr_mult=2.5, breakeven_after_r=None),
}
INCUMBENT = "trend_loose_90d"

# MIN_PRIOR_TRADES / MIN_PRIOR_REGIME and the chooser itself live in
# app/services/bot/exit_learning.py, where the tests can reach them — the
# information discipline is the part of this that has to be right.
MIN_YEAR_TRADES = 100      # a year too thin to score is skipped


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def _bootstrap_ci(values: list[float], n: int = 2000, seed: int = 7) -> tuple[float, float]:
    if len(values) < 20:
        return (float("nan"), float("nan"))
    rng = random.Random(seed)
    size = len(values)
    means = []
    for _ in range(n):
        means.append(sum(values[rng.randrange(size)] for _ in range(size)) / size)
    means.sort()
    return (means[int(0.025 * n)], means[int(0.975 * n)])


def _payoff(values: list[float]) -> tuple[float, float]:
    """Win rate and payoff ratio (avg win / abs avg loss)."""
    wins = [v for v in values if v > 0]
    losses = [v for v in values if v <= 0]
    if not wins or not losses:
        return (float("nan"), float("nan"))
    return (100.0 * len(wins) / len(values), _avg(wins) / abs(_avg(losses)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-symbols", type=int, default=0)
    ap.add_argument("--trailing-years", type=int, default=0,
                    help="0 = expanding window (all history before the boundary)")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    symbols = available_symbols(data_dir)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    logger.info("universe: %d symbols", len(symbols))

    context = build_context(data_dir, symbols)
    config = BacktestConfig()

    logger.info("replaying once, settling under %d exit rules ...", len(CANDIDATES))
    by_rule = run_strategies_multi(data_dir, context, config, CANDIDATES, symbols)
    for name, trades in by_rule.items():
        logger.info("  %-20s %7d trades", name, len(trades))

    # Index every rule's trades by the year they were ENTERED, and keep a flat
    # list keyed by exit date for the chooser's information set.
    entered: dict[str, dict[int, list]] = {n: defaultdict(list) for n in CANDIDATES}
    closed: dict[str, list] = {n: [] for n in CANDIDATES}
    for name, trades in by_rule.items():
        for t in trades:
            entered[name][t.entry_day.year].append(t)
            if t.exit_day is not None:
                closed[name].append(t)
        closed[name].sort(key=lambda t: t.exit_day)

    years = sorted({y for n in CANDIDATES for y in entered[n]})
    years = [y for y in years if len(entered[INCUMBENT].get(y, [])) >= MIN_YEAR_TRADES]
    logger.info("scorable years: %s .. %s (%d)", years[0], years[-1], len(years))

    def prior(name: str, boundary: date, regime: str | None = None) -> list[float]:
        return prior_returns(
            closed[name], boundary, regime=regime, trailing_years=args.trailing_years
        )

    rows = []
    adaptive_r: list[float] = []
    regime_adaptive_r: list[float] = []
    incumbent_r: list[float] = []
    oracle_r: list[float] = []
    picks: list[str] = []
    regime_picks: list[str] = []

    for year in years:
        boundary = date(year, 1, 1)

        # --- global chooser -------------------------------------------------
        if not any(len(prior(n, boundary)) >= MIN_PRIOR_TRADES for n in CANDIDATES):
            continue
        pick = choose_exit(
            closed, boundary, fallback=INCUMBENT, trailing_years=args.trailing_years
        )
        picks.append(pick)

        year_ad = [t.r_multiple for t in entered[pick].get(year, [])]
        year_in = [t.r_multiple for t in entered[INCUMBENT].get(year, [])]
        per_rule = {n: _avg([t.r_multiple for t in entered[n].get(year, [])]) for n in CANDIDATES}
        best_now = max(per_rule, key=per_rule.get)

        adaptive_r.extend(year_ad)
        incumbent_r.extend(year_in)
        oracle_r.extend([t.r_multiple for t in entered[best_now].get(year, [])])

        # --- per-regime chooser --------------------------------------------
        reg_pick: dict[str, str] = {}
        for reg in {t.regime for t in entered[INCUMBENT].get(year, []) if t.regime}:
            reg_pick[reg] = choose_exit(
                closed, boundary, fallback=INCUMBENT, regime=reg,
                trailing_years=args.trailing_years,
            )
        chosen_desc = ",".join(f"{r}:{reg_pick[r]}" for r in sorted(reg_pick))
        regime_picks.append(chosen_desc)
        for reg, rule in reg_pick.items():
            regime_adaptive_r.extend(
                t.r_multiple for t in entered[rule].get(year, []) if t.regime == reg
            )

        rows.append({
            "year": year,
            "pick": pick,
            "adaptive_avg_r": _avg(year_ad),
            "incumbent_avg_r": _avg(year_in),
            "best_possible": best_now,
            "best_possible_avg_r": per_rule[best_now],
            "n": len(year_in),
            "regime_picks": reg_pick,
        })

    print("\n=== YEAR BY YEAR ===")
    print(f"{'year':>6} {'chosen from prior':>20} {'adaptive':>9} {'incumbent':>10} "
          f"{'hindsight best':>20} {'its R':>7} {'n':>6}")
    for r in rows:
        print(f"{r['year']:>6} {r['pick']:>20} {r['adaptive_avg_r']:>+9.3f} "
              f"{r['incumbent_avg_r']:>+10.3f} {r['best_possible']:>20} "
              f"{r['best_possible_avg_r']:>+7.3f} {r['n']:>6}")

    beat = sum(1 for r in rows if r["adaptive_avg_r"] > r["incumbent_avg_r"])
    print(f"\nadaptive beat the frozen rule in {beat} of {len(rows)} years")

    print("\n=== POOLED, ACROSS EVERY SCORED YEAR ===")
    for label, vals in (
        ("adaptive (global)", adaptive_r),
        ("adaptive (per regime)", regime_adaptive_r),
        ("frozen incumbent", incumbent_r),
        ("hindsight best/yr", oracle_r),
    ):
        lo, hi = _bootstrap_ci(vals)
        wr, po = _payoff(vals)
        print(f"  {label:<24} n={len(vals):>6}  avgR {_avg(vals):>+.4f}  "
              f"CI [{lo:+.4f}, {hi:+.4f}]  win {wr:.1f}%  payoff {po:.2f}")

    print("\n=== FIXED RULES OVER THE SAME YEARS ===")
    span = {r["year"] for r in rows}
    for n in CANDIDATES:
        vals = [t.r_multiple for y in span for t in entered[n].get(y, [])]
        wr, po = _payoff(vals)
        print(f"  {n:<22} n={len(vals):>6}  avgR {_avg(vals):>+.4f}  win {wr:.1f}%  payoff {po:.2f}")

    diff = _avg(adaptive_r) - _avg(incumbent_r)
    rdiff = _avg(regime_adaptive_r) - _avg(incumbent_r)
    print(f"\nadaptive - frozen        : {diff:+.4f}R per trade")
    print(f"per-regime - frozen      : {rdiff:+.4f}R per trade")
    print(f"distinct choices made    : {sorted(set(picks))}")

    if args.out:
        Path(args.out).write_text(json.dumps({"rows": rows}, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
