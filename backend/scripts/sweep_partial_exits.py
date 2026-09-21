#!/usr/bin/env python3
"""What does taking money off a winner actually cost?

    python3 scripts/sweep_partial_exits.py

Scaling out raises the win rate almost by definition: a trade that banks a
third at 1.5R and is then stopped at breakeven finishes positive rather than
at zero, so it moves from the loss column to the win column. The question is
what it costs, and the answer is not obvious — the fraction sold stops
compounding, and this book's entire result lives in a handful of positions
that run past 50R.

Both effects are measured here rather than assumed, and the second sale is
charged its own STT, stamp duty and brokerage.

Candidates are declared before running. No grid search.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import rules as R  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies_multi  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("partial")


def model(at_r, frac):
    return ExitModel(
        target_r=None, max_hold_sessions=R.EXIT_MAX_HOLD_SESSIONS,
        trail_after_r=R.EXIT_TRAIL_AFTER_R, trail_atr_mult=R.EXIT_TRAIL_ATR_MULT,
        scale_out_at_r=at_r, scale_out_fraction=frac, breakeven_after_scale=True,
    )


CANDIDATES = {
    "none":              model(None, 0.0),
    "30pct_at_1R":       model(1.0, 0.30),
    "30pct_at_1.5R":     model(1.5, 0.30),
    "30pct_at_2R":       model(2.0, 0.30),
    "50pct_at_2R":       model(2.0, 0.50),
    "30pct_at_3R":       model(3.0, 0.30),
    "50pct_at_1.5R":     model(1.5, 0.50),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-symbols", type=int, default=0)
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    symbols = available_symbols(data_dir)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    context = build_context(data_dir, symbols)
    out = run_strategies_multi(data_dir, context, BacktestConfig(), CANDIDATES, symbols)

    print(f"\n{'rule':16} {'n':>6} {'avgR':>8} {'win%':>7} {'payoff':>7} "
          f"{'maxR':>7} {'>10R':>6}   (rules-filtered subset)")
    for name in CANDIDATES:
        rows = []
        for t in out[name]:
            d = t.to_dict()
            if R.accepts(d):
                rows.append(d)
        if not rows:
            continue
        r = np.array([x["r_multiple"] for x in rows])
        w, l = r[r > 0], r[r <= 0]
        payoff = w.mean() / abs(l.mean()) if len(w) and len(l) else float("nan")
        print(f"{name:16} {len(r):6} {r.mean():+8.3f} {100*(r>0).mean():6.1f}% "
              f"{payoff:7.2f} {r.max():7.1f} {int((r>10).sum()):6}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
