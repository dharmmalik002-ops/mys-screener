#!/usr/bin/env python3
"""Seed the trade ledger from the backtest and review every trade in it.

    python3 scripts/build_bot_ledger.py
    python3 scripts/build_bot_ledger.py --limit-symbols 300   # quick check

Writes `bot_ledger.db` into `APP_STATE_DIR`, beside the trade journal. The
database is deliberately NOT committed: it is binary, the Space's pre-receive
hook rejects binaries outright (CLAUDE.md gotcha 21), and the aggregates the UI
needs already ship inside `bot_backtest.json`. What the ledger adds locally is
the ability to ask questions the aggregates did not anticipate — one symbol,
one month, one verdict — and a place for live trades to accumulate under the
same schema as the simulated ones.

Re-running is safe: trades are keyed on (source, strategy, symbol, entry_day)
and re-inserted with INSERT OR IGNORE, so a second run adds only what is new
and never duplicates the statistical base. Live rows are never touched.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import get_settings  # noqa: E402
from app.services.bot import evolution as evo  # noqa: E402
from app.services.bot import ledger as lg  # noqa: E402
from app.services.bot import review as rv  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402
from app.services.bot.learning import build_rows  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot-ledger")

CONTEXT_KEYS = {"NIFTY", "NIFTY500", "INDIAVIX", "MIDCAP", "BANKNIFTY", "MIDCAP100"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-symbols", type=int, default=0)
    parser.add_argument("--state-dir", default=None, help="override APP_STATE_DIR")
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    state_dir = Path(args.state_dir) if args.state_dir else get_settings().app_state_dir
    logger.info("ledger will be written to %s", lg.ledger_path(state_dir))

    symbols = None
    if args.limit_symbols:
        every = available_symbols(data_dir)
        context = [s for s in every if s in CONTEXT_KEYS]
        symbols = context + [s for s in every if s not in CONTEXT_KEYS][: args.limit_symbols]

    t0 = time.time()
    config = BacktestConfig()
    context = build_context(data_dir, symbols)
    trades = run_strategies(data_dir, context, config, symbols)
    resolved = [t for t in trades if t.resolved]
    logger.info("replayed %s resolved trades in %.1f min", f"{len(resolved):,}", (time.time() - t0) / 60)

    rows = build_rows(resolved, context.regimes)
    ledger_trades = [
        lg.LedgerTrade(
            source="backtest",
            strategy=r["strategy"], symbol=r["symbol"],
            signal_day=r["signal_day"], entry_day=r["entry_day"], exit_day=r["exit_day"],
            entry=r["entry"], stop=r["stop"], exit_price=r["exit_price"],
            exit_reason=r["exit_reason"], sessions_held=r["sessions_held"],
            r_multiple=r["r_multiple"], net_pct=r["net_pct"],
            mae_r=r["mae_r"], mfe_r=r["mfe_r"], risk_pct=r["risk_pct"],
            atr_pct_at_entry=r["atr_pct_at_entry"],
            regime=r["regime"], volatility_band=r["volatility_band"],
            breadth_above_200dma=r["breadth_above_200dma"],
            pct_from_52w_high=r["pct_from_52w_high"],
            vix_percentile=r["vix_percentile"],
            macro_headwinds=r["macro_headwinds"],
            ret_63_at_entry=r.get("ret_63_at_entry"),
            ret_252_at_entry=r.get("ret_252_at_entry"),
            dist_52w_high_at_entry=r.get("dist_52w_high_at_entry"),
            rel_volume_at_entry=r.get("rel_volume_at_entry"),
            turnover_crore_at_entry=r.get("turnover_crore_at_entry"),
            above_200dma_pct_at_entry=r.get("above_200dma_pct_at_entry"),
            expected_r=r["expected_r"], thesis=r["thesis"],
        )
        for r in rows
    ]

    with lg.connect(state_dir) as conn:
        added = lg.record_trades(conn, ledger_trades)
        logger.info("inserted %s new trades (%s already present)", f"{added:,}", f"{len(ledger_trades) - added:,}")

        pending = lg.unreviewed(conn)
        logger.info("reviewing %s trades…", f"{len(pending):,}")
        for row in pending:
            verdict = rv.review_trade(row)
            lg.record_review(
                conn, verdict.trade_id, verdict.verdict, verdict.tags,
                verdict.stop_quality, verdict.exit_quality,
                verdict.r_left_on_table, verdict.note,
            )

        # Record the current standing of every cell so the lifecycle table has
        # a starting point; `replay_evolution` in the artifact covers history.
        as_of = max((r["exit_day"] for r in rows if r["exit_day"]), default=None)
        if as_of:
            verdicts = evo.score_all(rows, date.fromisoformat(as_of))
            lg.record_cell_status(conn, as_of, [v.to_dict() for v in verdicts])
            logger.info("recorded status for %d cells as at %s", len(verdicts), as_of)

        summary = lg.counts(conn)

    logger.info(
        "ledger holds %s trades (%s), %s reviewed, %s -> %s",
        f"{summary['total']:,}", summary["by_source"], f"{summary['reviewed']:,}",
        summary["first_entry"], summary["last_entry"],
    )
    logger.info("done in %.1f min", (time.time() - t0) / 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
