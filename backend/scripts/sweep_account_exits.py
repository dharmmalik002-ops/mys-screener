#!/usr/bin/env python3
"""Choose the exit horizon by what the ACCOUNT earns, not by per-trade R.

    python3 scripts/sweep_account_exits.py --limit-symbols 500

`sweep_exit_models.py` optimised the average R of a signal, which was the right
question for a narrow book where capacity was never the binding constraint. In
a sixty-position book it is the wrong question: the book is limited by capital,
not by ideas, so a trade that runs ninety sessions blocks roughly four shorter
trades behind it. A rule with a lower per-trade R and twice the turnover can
compound faster, and per-trade R cannot see that.

Same protocol as every other choice here: candidates declared in advance,
scored on the pre-split window alone, and the held-out window run once against
whichever the pre-split data picked. The selection statistic is the account's
Sharpe, so a rule cannot win simply by holding more risk for longer.

**READ THIS BEFORE TRUSTING THE OUTPUT.** This script holds the playbook fixed
while varying the exit, and that is not sound. The playbook is produced by
walk-forward validation *on the trades*, and changing the exit rule changes
every trade — so a candidate is scored against a set of cells that were
validated under a different rule. The error is not small: `trail_only_wide`
scored +15.09% here and returned **5.20%** on a full rebuild that re-derived
the playbook under its own trades, because the cells that validate with a
90-session ceiling are not the cells that validate without one.

Treat the ranking as a screen for candidates worth rebuilding properly, never
as a result. Any rule that wins here must be confirmed by a full
`run_bot_backtest.py` before it goes anywhere near `ExitModel`'s defaults.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import portfolio as pf  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies_multi  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402
from app.services.bot.learning import build_rows  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("account-exits")

CONTEXT_KEYS = {"NIFTY", "NIFTY500", "INDIAVIX", "MIDCAP", "BANKNIFTY", "MIDCAP100"}

# The incumbent plus shorter horizons. Trail and stop geometry are held
# constant so the only thing varying is how long capital may stay committed.
CANDIDATES: dict[str, ExitModel] = {
    "hold_90_incumbent": ExitModel(target_r=None, max_hold_sessions=90, trail_after_r=1.5, trail_atr_mult=4.0),
    "hold_60": ExitModel(target_r=None, max_hold_sessions=60, trail_after_r=1.5, trail_atr_mult=4.0),
    "hold_40": ExitModel(target_r=None, max_hold_sessions=40, trail_after_r=1.5, trail_atr_mult=4.0),
    "hold_25": ExitModel(target_r=None, max_hold_sessions=25, trail_after_r=1.5, trail_atr_mult=4.0),
    # A tighter trail shortens holds indirectly rather than by decree; included
    # so the comparison is not purely about the ceiling.
    "hold_90_tight_trail": ExitModel(target_r=None, max_hold_sessions=90, trail_after_r=1.0, trail_atr_mult=2.5),

    # --- Longer than the incumbent ---------------------------------------
    # The first sweep only ever looked *down* from 90 sessions, which left the
    # obvious question unasked. The account's measured weakness is strong
    # rising markets — it made 16.6% in a year the index made 26.0% — and the
    # mechanism is visible: a fund rides a trend indefinitely while a time stop
    # sells out of one. If that is the cause, letting the trail alone decide
    # when to leave should close part of the gap. If the gap is instead the
    # stops themselves, these will change nothing.
    "hold_120": ExitModel(target_r=None, max_hold_sessions=120, trail_after_r=1.5, trail_atr_mult=4.0),
    "hold_180": ExitModel(target_r=None, max_hold_sessions=180, trail_after_r=1.5, trail_atr_mult=4.0),
    "hold_250": ExitModel(target_r=None, max_hold_sessions=250, trail_after_r=1.5, trail_atr_mult=4.0),
    # No time stop worth the name: the trail is the only way out.
    "trail_only": ExitModel(target_r=None, max_hold_sessions=500, trail_after_r=1.5, trail_atr_mult=4.0),
    # Trail-only with a wider leash, so a trend has room to breathe.
    "trail_only_wide": ExitModel(target_r=None, max_hold_sessions=500, trail_after_r=2.0, trail_atr_mult=6.0),
}


def account(rows, cells, expectancy, start, end, label):
    result = pf.simulate(
        rows, [], pf.PortfolioConfig(), start=start, end=end, label=label,
        playbook_cells=cells, cell_expectancy=expectancy,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-symbols", type=int, default=0)
    parser.add_argument("--split", default="2022-11-28")
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    split = date.fromisoformat(args.split)

    symbols = None
    if args.limit_symbols:
        every = available_symbols(data_dir)
        ctx = [s for s in every if s in CONTEXT_KEYS]
        symbols = ctx + [s for s in every if s not in CONTEXT_KEYS][: args.limit_symbols]

    artifact = json.loads((data_dir / "bot_backtest.json").read_text(encoding="utf-8"))
    cells, expectancy = set(), {}
    for book in artifact.get("playbooks") or []:
        for entry in book.get("entries") or []:
            key = (entry["strategy"], book["regime"])
            cells.add(key)
            expectancy[key] = float(entry.get("out_sample_r") or 0.0)
    if not cells:
        logger.error("no playbook cells in the artifact — run the backtest first")
        return 1

    logger.info("building context…")
    context = build_context(data_dir, symbols)
    t0 = time.time()
    logger.info("settling %d exit rules over one replay…", len(CANDIDATES))
    by_model = run_strategies_multi(data_dir, context, BacktestConfig(), CANDIDATES, symbols)
    logger.info("replay done in %.1f min", (time.time() - t0) / 60)

    print("\n=== STEP 1: score each rule on PRE-SPLIT data (account Sharpe decides) ===")
    print(f"{'rule':22s} {'trades':>8s} {'avg hold':>9s} {'CAGR':>9s} {'maxDD':>8s} {'Sharpe':>8s}")
    scored = []
    rows_by_model = {}
    for name, trades in by_model.items():
        resolved = [t for t in trades if t.resolved]
        rows = build_rows(resolved, context.regimes)
        rows_by_model[name] = rows
        pre = account(rows, cells, expectancy, None, split, name)
        if not pre:
            continue
        holds = [t.sessions_held for t in resolved] or [0]
        print(f"{name:22s} {pre.trades_taken:8,d} {sum(holds)/len(holds):9.1f} "
              f"{pre.cagr_pct:8.2f}% {pre.max_drawdown_pct:7.1f}% {pre.sharpe:8.2f}")
        scored.append((pre.sharpe, name))

    if not scored:
        logger.error("no rule produced a scoreable pre-split account")
        return 1
    best = max(scored)
    print(f"\nCHOSEN on pre-split: {best[1]} (Sharpe {best[0]:.2f})")

    print("\n=== STEP 2: score the held-out window ONCE ===")
    for name in (best[1], "hold_90_incumbent"):
        out = account(rows_by_model[name], cells, expectancy, split, None, name)
        if not out:
            continue
        tag = " <- chosen" if name == best[1] else " (incumbent, for contrast)"
        print(f"  {name:22s} CAGR {out.cagr_pct:+7.2f}%  maxDD {out.max_drawdown_pct:6.1f}%  "
              f"Sharpe {out.sharpe:5.2f}  n={out.trades_taken:,}{tag}")
    print("\n  reference: Nifty +6.06% | fund median 11.36% at -27.53%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
