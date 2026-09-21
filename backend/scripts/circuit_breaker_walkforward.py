#!/usr/bin/env python3
"""Does learning help the bot STOP, even when it cannot help it choose?

    python3 scripts/circuit_breaker_walkforward.py

Five learning tests in this project have all asked the offensive question:
can the bot learn to pick something better? Cells, books, exposure, symbols,
exits — every answer was no, and the exit test proved the ceiling is negative.

But that is only half of what a trader's journal is for. Most of a
professional's database exists to tell them what to **stop doing**. That is
the defensive half, `calibration.py` implements it — it can demote a cell and
is structurally forbidden from promoting one — and it has never been measured.

The rule here is the simplest honest form of it. A strategy x regime cell is
suspended for as long as its own recently closed trades average below a
threshold, and reinstated the moment they recover. It uses only trades that
had CLOSED before the day in question, so it is the information a live bot
actually holds.

Declared before running:

  * The primary rule is `w25_below_0` — suspend while the last 25 closed
    trades in the cell average below zero. It is the natural statement of
    "stop doing what has stopped working", not a point chosen from a grid.
  * The other five settings are a ROBUSTNESS GRID, not a menu. The question
    they answer is whether the direction holds, not which one scores best.
    Picking the winner from six and reporting it would be exactly the error
    that produced nine false positives earlier in this project.
  * Every setting is scored against a MATCHED RANDOM CONTROL: the same number
    of signals suspended, chosen at random, over 40 seeded draws. This is the
    control that matters and it is not optional. A breaker cuts the trade
    count, and a smaller book has a smaller drawdown for reasons that have
    nothing to do with learning. Unless the real breaker beats the random one,
    the result is exposure reduction wearing a lab coat.
  * The primary metrics are **max drawdown and Sharpe**, because the
    hypothesis is about risk. CAGR is reported beside them and is not the
    test. A breaker that cuts drawdown while costing return has still done
    its job; one that lifts CAGR while leaving drawdown alone has not.

Both windows are reported: the held-out period the bot actually ships on, and
the full history, which is thin on the former (~220 trades) and so cannot
carry the conclusion alone.
"""

from __future__ import annotations

import argparse
import json
import random
import logging
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import portfolio as pf  # noqa: E402
from app.services.bot.circuit_breaker import suspended_mask  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.history import available_symbols  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("breaker")

PRIMARY = "w25_below_0"
GRID: dict[str, tuple[int, float]] = {
    "w25_below_0":     (25, 0.00),
    "w25_below_-0.10": (25, -0.10),
    "w25_below_-0.25": (25, -0.25),
    "w50_below_0":     (50, 0.00),
    "w50_below_-0.10": (50, -0.10),
    "w50_below_-0.25": (50, -0.25),
}


def matched_random(
    rows: list[dict], n_suspended: int, account, draws: int = 40,
    seed: int = 20260920, start=None,
) -> dict[str, tuple[float, float]]:
    """Suspend `n_suspended` signals at random, `draws` times, and report the spread.

    The comparison the whole experiment turns on. If the learned breaker sits
    inside this distribution it has discovered that trading less reduces
    drawdown, which is arithmetic, not intelligence.
    """
    rng = random.Random(seed)
    cagr, dd, sharpe = [], [], []
    index = list(range(len(rows)))
    for _ in range(draws):
        drop = set(rng.sample(index, n_suspended))
        kept = [r for i, r in enumerate(rows) if i not in drop]
        run = account(kept, "random", start)
        if run is None:
            continue
        cagr.append(run.cagr_pct)
        dd.append(run.max_drawdown_pct)
        sharpe.append(run.sharpe)
    if not cagr:
        return {}
    def band(v):
        v = sorted(v)
        return (sum(v) / len(v), v[int(0.95 * (len(v) - 1))])
    return {"cagr": band(cagr), "maxdd": band(dd), "sharpe": band(sharpe)}


def describe(result) -> str:
    if result is None:
        return "  (no trades)"
    return (f"  CAGR {result.cagr_pct:>+7.2f}%  maxDD {result.max_drawdown_pct:>7.2f}%  "
            f"Sharpe {result.sharpe:>5.2f}  trades {result.trades_taken:>5}  "
            f"win {result.win_rate:>4.1f}%  payoff {result.payoff:>4.2f}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-symbols", type=int, default=0)
    ap.add_argument("--draws", type=int, default=40)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    symbols = available_symbols(data_dir)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    logger.info("universe: %d symbols", len(symbols))

    context = build_context(data_dir, symbols)
    config = BacktestConfig()
    trades = run_strategies(data_dir, context, config, symbols)
    rows = [asdict(t) for t in trades]
    for row in rows:
        for key in ("signal_day", "entry_day", "exit_day"):
            if row.get(key) is not None:
                row[key] = str(row[key])
    logger.info("trades: %d", len(rows))

    artifact = json.loads((data_dir / "bot_backtest.json").read_text())
    split = artifact["coverage"]["validation_split"]
    validation_split = date.fromisoformat(split) if split else None
    cells: set[tuple[str, str]] = set()
    expectancy: dict[tuple[str, str], float] = {}
    for book in artifact.get("playbooks") or []:
        for entry in book.get("entries") or []:
            key = (str(entry["strategy"]), str(book["regime"]))
            cells.add(key)
            expectancy[key] = float(entry.get("out_sample_r") or 0.0)
    logger.info("playbook cells: %d   split: %s", len(cells), validation_split)
    if not cells or validation_split is None:
        logger.error("no playbook in the artifact — run scripts/run_bot_backtest.py first")
        return 1

    adjustment = pf.derive_atr_adjustment(rows, validation_split)

    def account(subset: list[dict], label: str, start: date | None):
        return pf.simulate(
            subset, [], start=start, label=label,
            playbook_cells=cells, cell_expectancy=expectancy,
            atr_adjustment=adjustment,
        )

    windows = [("held-out (as shipped)", validation_split), ("full history", None)]
    report: dict[str, dict] = {}

    for window_label, start in windows:
        print(f"\n=== {window_label.upper()} ===")
        base = account(rows, "baseline", start)
        print(f"baseline (no breaker)")
        print(describe(base))
        report.setdefault(window_label, {})["baseline"] = asdict(base) if base else None

        for name, (w, threshold) in GRID.items():
            mask = suspended_mask(rows, w, threshold)
            kept = [r for r, off in zip(rows, mask) if not off]
            run = account(kept, name, start)
            tag = "  <-- PRIMARY" if name == PRIMARY else ""
            blocked = sum(mask)
            print(f"{name}  (suspended {blocked:,} of {len(rows):,} signals){tag}")
            print(describe(run))
            if run and base:
                print(f"    vs baseline:  CAGR {run.cagr_pct - base.cagr_pct:+.2f}pp   "
                      f"maxDD {run.max_drawdown_pct - base.max_drawdown_pct:+.2f}pp   "
                      f"Sharpe {run.sharpe - base.sharpe:+.2f}")
            report[window_label][name] = asdict(run) if run else None

            if run and base:
                control = matched_random(rows, blocked, account, draws=args.draws, start=start)
                if control:
                    mean_dd, p95_dd = control["maxdd"]
                    mean_sh, p95_sh = control["sharpe"]
                    mean_cg, p95_cg = control["cagr"]
                    print(f"    matched random ({blocked:,} dropped, 40 draws): "
                          f"CAGR {mean_cg:+.2f}% (95th {p95_cg:+.2f})  "
                          f"maxDD {mean_dd:.2f}% (95th {p95_dd:.2f})  "
                          f"Sharpe {mean_sh:+.2f} (95th {p95_sh:+.2f})")
                    verdict = ("BEATS random" if run.max_drawdown_pct > p95_dd
                               else "inside random noise")
                    print(f"    drawdown vs random control: {verdict}")
                    report[window_label][name + "__random_control"] = control

    print("\nReminder: maxDD is negative, so a LESS negative maxDD is an improvement.")
    print("The primary rule is w25_below_0. The grid tests direction, not selection.")

    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
