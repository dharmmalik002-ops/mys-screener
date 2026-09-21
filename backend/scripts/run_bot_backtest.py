#!/usr/bin/env python3
"""Run the bot backtest and write `data/bot_backtest.json`.

    python3 scripts/run_bot_backtest.py                    # full history, full universe
    python3 scripts/run_bot_backtest.py --limit-symbols 150 # smoke test
    python3 scripts/run_bot_backtest.py --start 2010-01-01

Needs `data/deep_history/` — build it first with `scripts/build_deep_history.py`.
The artifact is committed so a cold Space serves the Bot tab immediately; the
deep history behind it is not, because it is ~1 GB and rebuildable.
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

from app.services.bot.backtest import BacktestConfig, build_artifact  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot-backtest")


def parse_day(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=None, help="ISO date to begin taking signals")
    parser.add_argument("--end", default=None)
    parser.add_argument("--limit-symbols", type=int, default=0)
    # Defaults deliberately None: the real defaults live in ExitModel, which is
    # where the sweep's decision is recorded. Duplicating the numbers here is
    # how this script silently ran the rejected rule once already.
    parser.add_argument("--target-r", type=float, default=None,
                        help="profit target in R (omit for the selected rule: no target)")
    parser.add_argument("--max-hold", type=int, default=None)
    parser.add_argument("--slippage-bps", type=float, default=None)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    out_path = Path(args.out) if args.out else data_dir / "bot_backtest.json"

    symbols = None
    if args.limit_symbols:
        # Context series first so the benchmark and VIX are always present, then
        # equities; otherwise a truncated list can omit the benchmark entirely.
        every = available_symbols(data_dir)
        context = [s for s in every if s in {"NIFTY", "NIFTY500", "INDIAVIX", "MIDCAP", "BANKNIFTY", "MIDCAP100"}]
        equities = [s for s in every if s not in context][: args.limit_symbols]
        symbols = context + equities
        logger.info("limited run: %d symbols", len(symbols))

    defaults = ExitModel()
    exits = ExitModel(
        target_r=args.target_r if args.target_r is not None else defaults.target_r,
        max_hold_sessions=args.max_hold if args.max_hold is not None else defaults.max_hold_sessions,
        trail_after_r=defaults.trail_after_r,
        trail_atr_mult=defaults.trail_atr_mult,
        breakeven_after_r=defaults.breakeven_after_r,
    )
    logger.info("exit rule: %s", exits)
    config = BacktestConfig(
        start=parse_day(args.start),
        end=parse_day(args.end),
        exits=exits,
        limit_symbols=args.limit_symbols,
    )
    if args.slippage_bps is not None:
        from app.services.bot.costs import CostModel

        config.costs = CostModel(slippage_bps=args.slippage_bps)

    t0 = time.time()
    artifact = build_artifact(data_dir, config, symbols)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")

    coverage = artifact["coverage"]
    logger.info(
        "wrote %s (%.1f KB) in %.1f min — %s trades over %d sessions",
        out_path, out_path.stat().st_size / 1024, (time.time() - t0) / 60,
        f"{coverage['trades_resolved']:,}", coverage["sessions"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
