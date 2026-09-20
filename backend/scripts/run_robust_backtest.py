#!/usr/bin/env python3
"""The rules run through a mark-to-market account, with the yearly table.

    python3 scripts/run_robust_backtest.py

Two things here matter more than the numbers they produce.

**Equity is marked to market every session.** `portfolio.simulate` books a
trade's whole profit on its exit day, which is harmless at a 90-session
ceiling and ruinous at the 500-session trail these rules need: a position
opened in 2023 and closed in 2024 puts every rupee into 2024. The first
version of this report was read as "the bot loses money in 2009, 2019 and
2023" when in fact those years' gains were simply being booked late. Several
apparently strong years were the previous year's profit arriving. The fix
inverted which years look weak, so any conclusion about *when* this struggles
has to come from a marked book.

**Drawdown gets worse under this accounting, and that is correct.** A
realised-only curve cannot see a position hand back 40% of its gain, because
it never sees the gain until the trade is closed.

Size classification uses `free_universe.json` market caps on the SEBI
convention (top 100 large, next 150 mid, the rest small), so the report can
say plainly what the book actually traded rather than inferring it from
turnover.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import adaptive_sizing as ad  # noqa: E402
from app.services.bot import diagnose as dg  # noqa: E402
from app.services.bot import indicators as ind  # noqa: E402
from app.services.bot import memory as mem  # noqa: E402
from app.services.bot import mtm_account as mtm  # noqa: E402
from app.services.bot import rules as R  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.benchmark import INDEX_KEY  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.portfolio import PortfolioConfig  # noqa: E402

# Chosen on return-per-drawdown, after the position-cap bug was fixed. The
# earlier 4% cap bound on 100% of trades, so the risk budget and the adaptive
# multiplier had no effect on size at all — 12% lets them actually express
# themselves.
BOOK = PortfolioConfig(
    risk_per_trade_pct=0.25, watch_risk_pct=0.25, max_concurrent=60,
    max_portfolio_risk_pct=60.0, max_deployed_pct=100.0, max_position_pct=8.0,
)


def size_bands(data_dir: Path) -> dict[str, str]:
    """SEBI convention: top 100 large, next 150 mid, everything else small."""
    rows = json.loads((data_dir / "free_universe.json").read_text())
    ranked = sorted(
        (r for r in rows if r.get("market_cap_crore")),
        key=lambda r: float(r["market_cap_crore"]), reverse=True,
    )
    band = {}
    for i, row in enumerate(ranked):
        band[str(row["symbol"])] = "large" if i < 100 else ("mid" if i < 250 else "small")
    return band


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-symbols", type=int, default=0)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    symbols = available_symbols(data_dir)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]

    context = build_context(data_dir, symbols)
    exits = ExitModel(
        target_r=R.EXIT_TARGET_R, max_hold_sessions=R.EXIT_MAX_HOLD_SESSIONS,
        trail_after_r=R.EXIT_TRAIL_AFTER_R, trail_atr_mult=R.EXIT_TRAIL_ATR_MULT,
    )
    trades = run_strategies(data_dir, context, BacktestConfig(exits=exits), symbols)
    rows = []
    for t in trades:
        row = asdict(t)
        for key in ("signal_day", "entry_day", "exit_day"):
            if row.get(key) is not None:
                row[key] = str(row[key])
        rows.append(row)

    # The stop-width cap is re-derived from the trailing year of signals, so
    # it breathes with volatility instead of starving the book after a crash.
    kept = R.accepted_with_rolling_risk(rows)
    print(f"signals {len(rows):,}  accepted {len(kept):,} ({100*len(kept)/len(rows):.1f}%)")

    # Bet more when the market is paying. Built only from that morning's tape:
    # index trend, breadth, regime — never from the bot's own recent P&L.
    bars = read_bars(data_dir, INDEX_KEY)
    closes = np.asarray(bars.close, dtype=float)
    sma200 = ind.sma(closes, 200)
    index_above = {
        d: (bool(closes[i] > sma200[i]) if not np.isnan(sma200[i]) else None)
        for i, d in enumerate(bars.dates)
    }
    schedule = ad.build_schedule(
        {d: r.regime for d, r in context.regime_by_day.items()},
        {r.day: r.pct_above_200dma for r in context.breadth},
        index_above,
    )
    result = mtm.simulate(kept, data_dir, BOOK, label="rules",
                          risk_scale_by_day=schedule)
    if result is None:
        print("no account")
        return 1
    print(f"CAGR {result.cagr_pct:+.2f}%  maxDD {result.max_drawdown_pct:.2f}%  "
          f"Sharpe {result.sharpe:.2f}  trades {result.trades_taken}  "
          f"win {result.win_rate}%  payoff {result.payoff}")

    band = size_bands(data_dir)
    counts: dict[int, int] = {}
    caps: dict[str, int] = {"small": 0, "mid": 0, "large": 0, "unknown": 0}
    for t in kept:
        y = int(str(t["entry_day"])[:4])
        counts[y] = counts.get(y, 0) + 1
        caps[band.get(str(t["symbol"]), "unknown")] += 1
    total = sum(caps.values()) or 1
    print("size mix: " + "  ".join(f"{k} {100*v/total:.1f}%" for k, v in caps.items()))

    # Benchmark: the Smallcap 250, because the book is ~80% small cap.
    index_yearly: dict[int, float] = {}
    try:
        import yfinance as yf
        from datetime import date as _d
        h = yf.Ticker("NIFTYSMLCAP250.NS").history(period="max")
        px = {x.date(): float(c) for x, c in zip(h.index, h["Close"])}
        ds = sorted(px)
        for y in range(2009, 2027):
            inside = [x for x in ds if x.year == y]
            if len(inside) < 150:
                continue
            prior = [x for x in ds if x < _d(y, 1, 1)]
            start = px[prior[-1]] if prior else px[inside[0]]
            index_yearly[y] = (px[inside[-1]] / start - 1.0) * 100.0
    except Exception as exc:                       # offline: report without it
        print(f"(no index comparison: {exc})")

    rows_d = dg.diagnose(rows, kept, result.yearly, index_yearly)
    summary = dg.summarise(rows_d)

    print("\nyear   return    index    alpha  trades  verdict")
    for d in rows_d:
        if d.year not in result.yearly:
            continue
        print(f"{d.year}  {d.bot_return:+7.1f}%  "
              f"{('%+.1f%%' % d.index_return) if d.index_return is not None else '   n/a':>7}  "
              f"{('%+.1fpp' % d.alpha) if d.alpha is not None else '  n/a':>8}  "
              f"{d.accepted:5}  {d.verdict}")

    print(f"\nbehind the index in {summary['years_behind']} of {summary['years_total']} years")
    print(f"verdicts: {summary['verdict_counts']}")
    for w in summary["worst"]:
        print(f"  worst {w['year']}: {w['note']}")

    state_dir = Path(__import__("os").environ.get("APP_STATE_DIR", str(data_dir)))
    mem.record(state_dir, [d.to_dict() for d in rows_d], {
        "cagr": result.cagr_pct, "max_drawdown": result.max_drawdown_pct,
        "sharpe": result.sharpe, "win_rate": result.win_rate, "payoff": result.payoff,
    })
    print(f"\nmemory: {mem.recall(state_dir).note}")

    if args.out:
        Path(args.out).write_text(json.dumps({
            "yearly": result.yearly, "trades_per_year": counts, "size_mix": caps,
            "diagnosis": [d.to_dict() for d in rows_d], "summary": summary,
            "cagr": result.cagr_pct, "max_drawdown": result.max_drawdown_pct,
            "sharpe": result.sharpe, "win_rate": result.win_rate,
            "payoff": result.payoff, "trades": result.trades_taken,
        }, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
