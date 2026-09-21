#!/usr/bin/env python3
"""The two components that survived, run together and measured against funds.

    python3 scripts/combined_product.py

Everything in this project has been measured one component at a time, and only
two survived scrutiny:

  * **Regime timing** — when to be in the market at all. Beats the median
    Indian equity fund on return AND drawdown.
  * **The defensive breaker** — which cells to stand down. Cuts tail drawdown
    beyond a matched random control, at flat return.

They have never been run together, and there is a specific structural gap
between them: the timing rule counts `recovery` as investable, while the
playbook clears **no cells** in recovery, so the stock book sits in cash
through a period the other half of the system calls investable. A blend is the
obvious way to hold both, and it has never been measured.

Declared before running:

  * Two sleeves, **50/50, no rebalancing**. Each compounds on its own and the
    total is their sum. No rebalancing, because periodic rebalancing between
    two positively-performing sleeves manufactures a return that depends
    entirely on the rebalancing interval, and picking the interval that looks
    best is how this project produced eight false positives.
  * 70/30 and 30/70 are a ROBUSTNESS GRID, not a menu to choose from.
  * The comparison window is matched to the fund window EXACTLY. Comparing a
    3.8-year CAGR against a 3-year fund return was false positive #2 here, so
    the blend is also cut to precisely 3 years and that is the cut used
    against the funds.
  * The measure that settles it is `funds_dominating` — how many real funds
    beat the blend on return **and** drawdown at once. A sleeve can win either
    one alone by losing the other.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import benchmark as bm  # noqa: E402
from app.services.bot import portfolio as pf  # noqa: E402
from app.services.bot import timing as tm  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.circuit_breaker import suspended_mask  # noqa: E402
from app.services.bot.combined import blend, curve_to_series, stats  # noqa: E402
from app.services.bot.benchmark import INDEX_KEY  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.portfolio import PortfolioResult  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("combined")

PRIMARY_WEIGHT = 0.50
GRID = (0.50, 0.70, 0.30)
TRADING_DAYS = 252


def as_result(label: str, days: list[date], values: np.ndarray, trades: int) -> PortfolioResult:
    s = stats(days, values)
    return PortfolioResult(
        label=label, start=days[0].isoformat(), end=days[-1].isoformat(),
        years=s["years"], starting_equity=1.0, ending_equity=float(values[-1]),
        cagr_pct=s["cagr_pct"], max_drawdown_pct=s["max_drawdown_pct"], sharpe=s["sharpe"],
        total_return_pct=round((float(values[-1]) - 1.0) * 100.0, 2),
        trades_taken=trades, signals_declined=0, win_rate=0.0, avg_r=0.0, payoff=0.0,
        exposure_pct=0.0, equity_curve=[],
    )


def line(name: str, s: dict) -> str:
    return (f"  {name:<26} CAGR {s['cagr_pct']:>+7.2f}%  maxDD {s['max_drawdown_pct']:>7.2f}%  "
            f"Sharpe {s['sharpe']:>5.2f}  ret/DD {str(s['return_per_drawdown']):>5}")


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
    trades = run_strategies(data_dir, context, BacktestConfig(), symbols)
    rows = [asdict(t) for t in trades]
    for row in rows:
        for key in ("signal_day", "entry_day", "exit_day"):
            if row.get(key) is not None:
                row[key] = str(row[key])
    logger.info("trades: %d", len(rows))

    artifact = json.loads((data_dir / "bot_backtest.json").read_text())
    split = date.fromisoformat(artifact["coverage"]["validation_split"])
    cells, expectancy = set(), {}
    for book in artifact.get("playbooks") or []:
        for entry in book.get("entries") or []:
            key = (str(entry["strategy"]), str(book["regime"]))
            cells.add(key)
            expectancy[key] = float(entry.get("out_sample_r") or 0.0)

    adjustment = pf.derive_atr_adjustment(rows, split)
    mask = suspended_mask(rows)
    kept = [r for r, off in zip(rows, mask) if not off]
    logger.info("breaker suspended %d of %d signals", sum(mask), len(rows))

    # The timing rule holds the broad index, not the Nifty 50 — this is the
    # same INDEX_KEY the benchmark and the shipped timing study use, and
    # using a different one here would compare two different rules.
    bars = read_bars(data_dir, INDEX_KEY)
    closes = dict(zip(bars.dates, bars.close))
    regime_by_day = {d: row.regime for d, row in context.regime_by_day.items()}

    results: dict[str, dict] = {}

    for window_label, start in (("held-out", split),
                                ("exact 3y (fund-matched)", None)):
        sessions = [d for d in context.sessions if d >= split]
        if start is None:
            last = sessions[-1]
            sessions = [d for d in sessions if d >= last - timedelta(days=3 * 365 + 1)]
        lo = sessions[0]

        book = pf.simulate(
            kept, [], start=lo, label="book_breaker", playbook_cells=cells,
            cell_expectancy=expectancy, atr_adjustment=adjustment,
        )
        # The same blend with the learning switched off, which is the only way
        # to say what the learned component is worth in the finished product
        # rather than on its own bench.
        book_raw = pf.simulate(
            rows, [], start=lo, label="book_no_breaker", playbook_cells=cells,
            cell_expectancy=expectancy, atr_adjustment=adjustment,
        )
        timed = tm.simulate(sessions, closes, regime_by_day)
        if book is None or timed is None:
            logger.warning("%s: a sleeve produced nothing", window_label)
            continue

        print(f"\n=== {window_label.upper()}  ({lo} -> {sessions[-1]}) ===")
        book_days, book_val = curve_to_series(book.equity_curve)
        timed_days, timed_val = curve_to_series(timed.equity_curve)
        print(line("stock book + breaker", stats(book_days, book_val)))
        print(line("regime timing", stats(timed_days, timed_val)))

        results[window_label] = {
            "book": stats(book_days, book_val),
            "timing": stats(timed_days, timed_val),
            "blends": {},
        }

        for weight in GRID:
            blended = blend(book.equity_curve, timed.equity_curve, weight)
            if blended is None:
                continue
            days, values = blended
            s = stats(days, values)
            tag = "  <-- PRIMARY" if weight == PRIMARY_WEIGHT else ""
            print(line(f"blend {int(weight*100)}/{int((1-weight)*100)}", s) + tag)
            results[window_label]["blends"][f"{int(weight*100)}/{int((1-weight)*100)}"] = s

            if weight == PRIMARY_WEIGHT and book_raw is not None:
                off = blend(book_raw.equity_curve, timed.equity_curve, weight)
                if off:
                    s_off = stats(*off)
                    print(line("  same blend, breaker OFF", s_off))
                    print(f"    learning contributes:  CAGR "
                          f"{s['cagr_pct'] - s_off['cagr_pct']:+.2f}pp   "
                          f"maxDD {s['max_drawdown_pct'] - s_off['max_drawdown_pct']:+.2f}pp   "
                          f"Sharpe {s['sharpe'] - s_off['sharpe']:+.2f}")
                    results[window_label]["blend_breaker_off"] = s_off

            if window_label.startswith("exact 3y") and weight == PRIMARY_WEIGHT:
                # Every configuration gets the same fund comparison, not just
                # the blend. Reporting it for one weighting only would let the
                # choice of weighting be made after seeing the fund result,
                # which is the same error as choosing an exit rule from a grid.
                for name, (d, v) in {
                    "timing sleeve alone": (timed_days, timed_val),
                    "stock book alone": (book_days, book_val),
                }.items():
                    other = bm.compare(as_result(name, d, v, book.trades_taken), data_dir)
                    if other:
                        st = stats(d, v)
                        print(f"\n  --- {name} vs the same 691 funds ---")
                        print(f"    CAGR {st['cagr_pct']:+.2f}% vs fund median "
                              f"{other.fund_median_cagr:+.2f}%   "
                              f"maxDD {st['max_drawdown_pct']:.2f}% vs "
                              f"{other.fund_median_drawdown}%")
                        print(f"    percentile {other.percentile}   "
                              f"funds beating it on BOTH: {other.funds_dominating}"
                              f"  ({other.funds_dominating_pct}%)")
                        results[window_label].setdefault("alternatives", {})[name] = {
                            "stats": st, "benchmark": asdict(other),
                        }

                comparison = bm.compare(
                    as_result("blend", days, values, book.trades_taken), data_dir
                )
                if comparison:
                    c = asdict(comparison)
                    print("\n  --- against real Indian equity funds, same 3-year window ---")
                    print(f"    funds counted          : {c['funds_counted']}")
                    print(f"    fund median CAGR       : {c['fund_median_cagr']:+.2f}%"
                          f"   blend {s['cagr_pct']:+.2f}%")
                    print(f"    fund median drawdown   : {c['fund_median_drawdown']}%"
                          f"   blend {s['max_drawdown_pct']:.2f}%")
                    print(f"    fund median Sharpe     : {c['fund_median_sharpe']}"
                          f"   blend {s['sharpe']}")
                    print(f"    percentile among funds : {c['percentile']}")
                    print(f"    funds beating it on BOTH return and drawdown: "
                          f"{c['funds_dominating']}  ({c['funds_dominating_pct']}%)")
                    results[window_label]["benchmark"] = c

    if args.out:
        Path(args.out).write_text(json.dumps(results, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
