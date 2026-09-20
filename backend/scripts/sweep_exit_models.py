#!/usr/bin/env python3
"""Compare exit rules on the same signals, choosing on in-sample data only.

    python3 scripts/sweep_exit_models.py --limit-symbols 400

The exit rule is the one genuinely free parameter in this system, and the
temptation is to try rules until the equity curve looks good. That is how a
backtest becomes a story. So this script is explicit about the protocol:

  1. The candidate rules are written down first, and each is defensible on its
     own terms before any of them is run (a swing rule, a position-trading
     rule, a pure trend-following rule, and the user's own 3/5/10 rule).
  2. Every candidate is scored on the EARLY portion of history only.
  3. The winner is chosen on that portion, and its performance on the held-out
     later portion is reported beside it, unchanged.

A rule that wins in-sample and collapses out-of-sample has told you something
useful and must not be quietly swapped for the runner-up — swapping is how the
held-out period stops being held out.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import attribution as attr  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies_multi  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402
from app.services.bot.regime import REGIMES  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("exit-sweep")

# Declared before running. Each is a rule a real trader would recognise, not a
# point on a grid search — a grid search over exit parameters would find the
# best-fitting noise in the sample and call it a strategy.
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

    # --- Second experiment: the round-trip leak ---------------------------
    # The trade review found 21% of trades went over 1R in profit and finished
    # negative, averaging -0.68R. These three lock a floor in once the move is
    # real, at thresholds high enough to sit outside ordinary noise.
    #
    # This is a SECOND look at exits, run after seeing the first set's results,
    # and that costs something: the more rules tried, the likelier one wins by
    # chance. So a lock rule replaces the incumbent only if it beats it on the
    # HELD-OUT period as well as in-sample — a stricter bar than the first
    # sweep applied, and the honest price of a second look.
    "lock_0.5R_after_2R": ExitModel(target_r=None, max_hold_sessions=90, trail_after_r=1.5,
                                    trail_atr_mult=4.0, lock_trigger_r=2.0, lock_floor_r=0.5),
    "lock_1R_after_2.5R": ExitModel(target_r=None, max_hold_sessions=90, trail_after_r=1.5,
                                    trail_atr_mult=4.0, lock_trigger_r=2.5, lock_floor_r=1.0),
    "lock_1.5R_after_3R": ExitModel(target_r=None, max_hold_sessions=90, trail_after_r=1.5,
                                    trail_atr_mult=4.0, lock_trigger_r=3.0, lock_floor_r=1.5),
}

# The rule currently in force. A challenger must beat it out-of-sample, not
# merely in-sample, before `ExitModel`'s defaults are touched.
INCUMBENT = "trend_loose_90d"


def score(trades, boundary):
    """(in-sample, out-of-sample) summary for one exit rule."""
    resolved = [t for t in trades if t.resolved]
    early = [t for t in resolved if t.entry_day < boundary]
    late = [t for t in resolved if t.entry_day >= boundary]
    return (
        attr.summarise_cell("ALL", "ALL", early),
        attr.summarise_cell("ALL", "ALL", late),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-symbols", type=int, default=400)
    parser.add_argument("--train-fraction", type=float, default=0.6)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    every = available_symbols(data_dir)
    context_keys = {"NIFTY", "NIFTY500", "INDIAVIX", "MIDCAP", "BANKNIFTY", "MIDCAP100"}
    symbols = None
    if args.limit_symbols:
        ctx = [s for s in every if s in context_keys]
        eq = [s for s in every if s not in context_keys][: args.limit_symbols]
        symbols = ctx + eq

    config = BacktestConfig(train_fraction=args.train_fraction)
    logger.info("building regime context…")
    context = build_context(data_dir, symbols)

    t0 = time.time()
    logger.info("replaying %d exit rules over the same signals…", len(CANDIDATES))
    by_model = run_strategies_multi(data_dir, context, config, CANDIDATES, symbols)
    logger.info("replay done in %.1f min", (time.time() - t0) / 60)

    # One boundary for every rule, from the rule with the most trades, so the
    # comparison is not shifted by a rule that happens to trade less.
    reference = max(by_model.values(), key=len)
    boundary = attr.split_date([t for t in reference if t.resolved], args.train_fraction)

    results = []
    for name, trades in by_model.items():
        ins, outs = score(trades, boundary)
        results.append({"model": name, "in_sample": ins.to_dict(), "out_sample": outs.to_dict()})

    results.sort(key=lambda r: -r["in_sample"]["avg_r"])
    winner = results[0]

    print("\n=== EXIT RULE SWEEP (selected on in-sample only) ===")
    print(f"{'rule':22s} | {'IN-SAMPLE':>28s} | {'OUT-OF-SAMPLE':>28s}")
    print(f"{'':22s} | {'n':>7s} {'win%':>6s} {'avgR':>7s} {'PF':>5s} | {'n':>7s} {'win%':>6s} {'avgR':>7s} {'PF':>5s}")
    for r in results:
        i, o = r["in_sample"], r["out_sample"]
        print(
            f"{r['model']:22s} | {i['trades']:7d} {i['win_rate']:6.1f} {i['avg_r']:+7.3f} {i['profit_factor']:5.2f} "
            f"| {o['trades']:7d} {o['win_rate']:6.1f} {o['avg_r']:+7.3f} {o['profit_factor']:5.2f}"
        )
    print(f"\nbest in-sample: {winner['model']}")
    print(f"  its held-out result: {winner['out_sample']['avg_r']:+.3f}R on {winner['out_sample']['trades']} trades")
    print(f"  validation split:    {boundary}")

    incumbent = next((r for r in results if r["model"] == INCUMBENT), None)
    if incumbent is not None:
        print(f"\nincumbent ({INCUMBENT}): in-sample {incumbent['in_sample']['avg_r']:+.3f}R, "
              f"held-out {incumbent['out_sample']['avg_r']:+.3f}R")
        beats_both = (
            winner["model"] != INCUMBENT
            and winner["in_sample"]["avg_r"] > incumbent["in_sample"]["avg_r"]
            and winner["out_sample"]["avg_r"] > incumbent["out_sample"]["avg_r"]
        )
        if winner["model"] == INCUMBENT:
            print("VERDICT: incumbent still best. No change.")
        elif beats_both:
            print(f"VERDICT: {winner['model']} beats the incumbent in BOTH periods — a real challenger.")
        else:
            print(f"VERDICT: {winner['model']} leads in-sample but does NOT beat the incumbent "
                  "out-of-sample. Keeping the incumbent; this is what a second look costs.")

    payload = {
        "protocol": "candidates declared in advance; selected on in-sample; out-of-sample reported unchanged",
        "validation_split": boundary.isoformat() if boundary else None,
        "symbols": len(symbols) if symbols else len(every),
        "selected": winner["model"],
        "results": results,
    }
    out_path = Path(args.out) if args.out else data_dir / "bot_exit_sweep.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("wrote %s", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
