#!/usr/bin/env python3
"""Rebuild the playbook every year from prior data only, then trade the next year.

    python3 scripts/rolling_walkforward.py

Every result elsewhere in this project rests on one split: train before
2022-11, test after. That is a legitimate evaluation and a fragile one — the
test window is 3.8 years and contains a single mid-cap bull in which the
account lagged the index by 19 points, so one year dominates the answer.

This is the stronger version. For each year Y, the playbook is rebuilt using
only trades that had *closed* before January of Y, by exactly the process the
live system uses (`attribution.validate`, keeping cells that survive their own
walk-forward). The account then trades year Y with that playbook and nothing
else. Repeating from 2012 gives fourteen independent out-of-sample years
instead of one window, and no year's result can have informed the playbook
that traded it.

It is slower and it is the number to trust.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import get_settings  # noqa: E402
from app.services.bot import attribution as attr  # noqa: E402
from app.services.bot import ledger as lg  # noqa: E402
from app.services.bot import portfolio as pf  # noqa: E402
from app.services.bot.benchmark import INDEX_KEY  # noqa: E402
from app.services.bot.engine import Trade  # noqa: E402
from app.services.bot.history import read_bars  # noqa: E402
from app.services.bot.regime import REGIMES  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rolling-wf")

TRADEABLE = {"confirmed", "confirmed_weak"}
# Below this the playbook is built on too little history to mean anything.
MIN_PRIOR_TRADES = 3000


def as_trade(row: dict) -> Trade:
    """A ledger row as the dataclass `attribution` expects."""
    return Trade(
        strategy=row["strategy"], symbol=row["symbol"],
        signal_day=date.fromisoformat(row["entry_day"]),
        entry_day=date.fromisoformat(row["entry_day"]),
        exit_day=date.fromisoformat(row["exit_day"]),
        entry=row["entry"] or 0.0, stop=row["stop"] or 0.0,
        exit_price=row["exit_price"], exit_reason=row["exit_reason"],
        sessions_held=row["sessions_held"] or 0,
        r_multiple=row["r_multiple"], gross_pct=0.0, net_pct=row["net_pct"] or 0.0,
        mae_r=row["mae_r"] or 0.0, mfe_r=row["mfe_r"] or 0.0,
        risk_pct=row["risk_pct"] or 0.0,
        atr_pct_at_entry=row["atr_pct_at_entry"] or 0.0,
        regime=row["regime"], volatility_band=row["volatility_band"] or "",
    )


def playbook_as_of(rows: list[dict], cutoff: date) -> tuple[set, dict]:
    """Cells that validated using only trades closed before `cutoff`."""
    prior = [r for r in rows if date.fromisoformat(r["exit_day"]) < cutoff]
    if len(prior) < MIN_PRIOR_TRADES:
        return set(), {}
    validated = attr.validate([as_trade(r) for r in prior], REGIMES, train_fraction=0.6)
    cells, expectancy = set(), {}
    for cell in validated:
        if cell.verdict in TRADEABLE and cell.out_sample and cell.out_sample.avg_r > 0:
            key = (cell.strategy, cell.regime)
            cells.add(key)
            expectancy[key] = cell.out_sample.avg_r
    return cells, expectancy


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-year", type=int, default=2012)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    with lg.connect(get_settings().app_state_dir) as conn:
        rows = [
            dict(r) for r in conn.execute(
                "SELECT * FROM trades WHERE source='backtest' AND exit_day IS NOT NULL"
            )
        ]
    logger.info("ledger holds %s resolved trades", f"{len(rows):,}")

    index = read_bars(data_dir, INDEX_KEY)
    closes = {d: float(c) for d, c in zip(index.dates, index.close)} if index else {}

    def index_return(d0: date, d1: date) -> float | None:
        days = sorted(d for d in closes if d0 <= d <= d1)
        if len(days) < 2 or closes[days[0]] <= 0:
            return None
        return (closes[days[-1]] / closes[days[0]] - 1.0) * 100.0

    last_year = max(date.fromisoformat(r["exit_day"]) for r in rows).year
    results = []
    for year in range(args.from_year, last_year + 1):
        start, end = date(year, 1, 1), date(year, 12, 31)
        cells, expectancy = playbook_as_of(rows, start)
        if not cells:
            logger.info("%d: no validated playbook from prior data — skipped", year)
            continue

        run = pf.simulate(
            rows, [], pf.PortfolioConfig(), start=start, end=end, label=str(year),
            playbook_cells=cells, cell_expectancy=expectancy,
        )
        if run is None:
            continue
        benchmark = index_return(start, end)
        # Short windows are reported as total return, not annualised.
        bot_return = run.total_return_pct
        results.append(
            {
                "year": year,
                "cells": len(cells),
                "trades": run.trades_taken,
                "bot_return_pct": round(bot_return, 2),
                "index_return_pct": round(benchmark, 2) if benchmark is not None else None,
                "excess_pct": round(bot_return - benchmark, 2) if benchmark is not None else None,
                "max_drawdown_pct": run.max_drawdown_pct,
            }
        )
        logger.info(
            "%d: %d cells, %d trades, bot %+.2f%% vs index %s",
            year, len(cells), run.trades_taken, bot_return,
            f"{benchmark:+.2f}%" if benchmark is not None else "n/a",
        )

    if not results:
        logger.error("no year produced a result")
        return 1

    bot = np.array([r["bot_return_pct"] for r in results], dtype=np.float64)
    idx = np.array([r["index_return_pct"] for r in results if r["index_return_pct"] is not None])
    excess = np.array([r["excess_pct"] for r in results if r["excess_pct"] is not None])
    # Geometric, because annual returns compound.
    bot_cagr = (np.prod(1 + bot / 100.0) ** (1 / len(bot)) - 1) * 100
    idx_cagr = (np.prod(1 + idx / 100.0) ** (1 / len(idx)) - 1) * 100 if len(idx) else None

    print("\n=== ROLLING WALK-FORWARD: playbook rebuilt each year from prior data only ===")
    print(f"{'year':>6s} {'cells':>6s} {'trades':>7s} {'bot':>9s} {'index':>9s} {'excess':>9s} {'maxDD':>8s}")
    for r in results:
        idx_s = f"{r['index_return_pct']:+.2f}%" if r["index_return_pct"] is not None else "n/a"
        ex_s = f"{r['excess_pct']:+.2f}" if r["excess_pct"] is not None else "n/a"
        print(f"{r['year']:6d} {r['cells']:6d} {r['trades']:7,d} {r['bot_return_pct']:+8.2f}% "
              f"{idx_s:>9s} {ex_s:>9s} {r['max_drawdown_pct']:7.1f}%")
    print()
    print(f"  years evaluated:        {len(results)}")
    print(f"  bot compound return:    {bot_cagr:+.2f}% a year")
    if idx_cagr is not None:
        print(f"  index compound return:  {idx_cagr:+.2f}% a year")
        print(f"  compound excess:        {bot_cagr - idx_cagr:+.2f} points a year")
        print(f"  years beating the index: {int((excess > 0).sum())} of {len(excess)}")
    print(f"  median annual drawdown: {np.median([r['max_drawdown_pct'] for r in results]):.1f}%")

    payload = {"years": results, "bot_cagr": round(bot_cagr, 2),
               "index_cagr": round(idx_cagr, 2) if idx_cagr is not None else None,
               "years_beating_index": int((excess > 0).sum()) if len(excess) else 0,
               "years_evaluated": len(results)}
    out = Path(args.out) if args.out else data_dir / "bot_rolling_walkforward.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("wrote %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
