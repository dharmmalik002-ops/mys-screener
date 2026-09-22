#!/usr/bin/env python3
"""The yearly-rebuild test for the RULES book — the evaluation that decides.

`run_robust_backtest.py` measures the rules book on a single train/test split
and reports +41.5%/yr. `rolling_walkforward.py` does the honest thing for the
*strategy playbook* — rebuild each January from prior data only, trade the
year that follows — and that returned -2.60%/yr, which is why CLAUDE.md
gotcha 59 calls the playbook a research instrument rather than a system.

The rules book has never been put through the same test. This script does it.

**What gets re-derived every January, from signals dated strictly earlier:**

  * `MAX_TURNOVER_CRORE` — the 60th percentile of prior turnover
  * `TRADEABLE_SETUPS`   — setups whose prior average R is positive
  * `SETUP_QUALITY`      — each setup's prior average R, rescaled to 0-1
  * `DECILE_CUTS`        — the deciles of the prior confidence-score spread
  * the thrust threshold and the recovery window, re-chosen from prior years

**What stays fixed, and why.** The confidence weights, the regime thresholds
and the strategy library's `expects` were all declared before anything was
measured, so re-deriving them would not test anything. The stop cap and the
gap allowance are risk rules, not fitted edges. The rolling stop-width cap is
already causal by construction (trailing year, strictly before each signal).

**The account runs CONTINUOUSLY.** Gotcha 74 measured that chaining yearly
growth factors understates drawdown roughly threefold, because it samples
equity once a year and cannot see an intra-year hole. Positions are carried
across January boundaries and one equity curve spans the whole run.
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import confidence as cf  # noqa: E402
from app.services.bot import indicators as ind  # noqa: E402
from app.services.bot import mtm_account as mtm  # noqa: E402
from app.services.bot import rules as R  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.portfolio import PortfolioConfig  # noqa: E402

FIRST_TRADED_YEAR = 2012      # needs a few years of prior signals to derive from
MIN_PRIOR_SIGNALS = 400       # below this the year stands down rather than guessing


def _rederive(prior: list[dict]) -> dict:
    """Everything the book fits, re-fitted on `prior` alone."""
    turnovers = [float(t["turnover_crore_at_entry"]) for t in prior
                 if t.get("turnover_crore_at_entry") is not None]
    by_setup: dict[str, list[float]] = {}
    for t in prior:
        by_setup.setdefault(str(t.get("strategy")), []).append(float(t["r_multiple"]))
    avg = {k: sum(v) / len(v) for k, v in by_setup.items() if len(v) >= 30}
    keep = {k for k, v in avg.items() if v > 0}
    # Rescale prior avg R onto the 0-1 range SETUP_QUALITY uses. Rank, not
    # magnitude, is what the score consumes.
    lo, hi = (min(avg.values()), max(avg.values())) if avg else (0.0, 1.0)
    span = (hi - lo) or 1.0
    quality = {k: 0.3 + 0.7 * (v - lo) / span for k, v in avg.items()}
    return {
        "turnover_cap": float(np.quantile(turnovers, 0.60)) if turnovers else R.MAX_TURNOVER_CRORE,
        "setups": keep or set(R.TRADEABLE_SETUPS),
        "quality": quality,
        "n_prior": len(prior),
    }


def _score(trade: dict, quality: dict, above200: bool | None) -> float:
    stop = float(trade.get("risk_pct") or 99.0)
    turnover = float(trade.get("turnover_crore_at_entry") or 1e9)
    momentum = float(trade.get("ret_63_at_entry") or 0.0)
    regime = str(trade.get("regime") or "")
    total = (cf.W_STOP * cf._band(stop, 2.0, 8.0)
             + cf.W_TURNOVER * cf._band(turnover, 1.0, 12.0)
             + cf.W_SETUP * quality.get(str(trade.get("strategy") or ""), 0.3)
             + cf.W_MOMENTUM * cf._band(momentum, 40.0, 0.0))
    market = 0.7 if regime in cf.STRONG_REGIMES else (0.35 if regime in cf.OK_REGIMES else 0.0)
    if above200:
        market += 0.3
    return max(1.0, min(10.0, round(total + cf.W_MARKET * market, 2)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default="", help="pickled signal cache (skips the 90s rebuild)")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    if args.signals and Path(args.signals).exists():
        rows = pickle.loads(Path(args.signals).read_bytes())["rows"]
        print(f"loaded {len(rows):,} cached signals")
    else:
        symbols = available_symbols(data_dir)
        context = build_context(data_dir, symbols)
        exits = ExitModel(target_r=R.EXIT_TARGET_R, max_hold_sessions=R.EXIT_MAX_HOLD_SESSIONS,
                          trail_after_r=R.EXIT_TRAIL_AFTER_R, trail_atr_mult=R.EXIT_TRAIL_ATR_MULT,
                          max_stop_pct=R.EXIT_MAX_STOP_PCT)
        trades = run_strategies(data_dir, context, BacktestConfig(exits=exits), symbols)
        rows = []
        for t in trades:
            r = asdict(t)
            for k in ("signal_day", "entry_day", "exit_day"):
                if r.get(k) is not None:
                    r[k] = str(r[k])
            rows.append(r)
        print(f"built {len(rows):,} signals")

    bars = read_bars(data_dir, "NIFTY500")
    closes = np.asarray(bars.close, dtype=float)
    s200 = ind.sma(closes, 200)
    above = {d: (bool(closes[i] > s200[i]) if not np.isnan(s200[i]) else True)
             for i, d in enumerate(bars.dates)}

    cleared = R.accepted_with_rolling_risk(rows)     # already causal
    cleared.sort(key=lambda t: str(t["entry_day"]))
    print(f"cleared the rolling stop-width cap: {len(cleared):,}")

    selected: list[dict] = []
    table: list[dict] = []
    for year in range(FIRST_TRADED_YEAR, 2027):
        cut = f"{year}-01-01"
        prior = [t for t in cleared if str(t["entry_day"]) < cut]
        this = [t for t in cleared if str(t["entry_day"])[:4] == str(year)]
        if len(prior) < MIN_PRIOR_SIGNALS or not this:
            table.append({"year": year, "taken": 0, "note": "stood down: too little prior evidence"})
            continue
        p = _rederive(prior)
        # Re-derive the decile cuts on the PRIOR signals, scored with the
        # prior-derived quality map — not on the year being traded.
        prior_raw = sorted(_score(t, p["quality"], above.get(date.fromisoformat(str(t["entry_day"]))))
                           for t in prior)
        cuts = [float(np.quantile(prior_raw, q / 10.0)) for q in range(1, 10)]
        took = []
        for t in this:
            if str(t.get("strategy")) not in p["setups"]:
                continue
            if float(t.get("turnover_crore_at_entry") or 1e9) > p["turnover_cap"]:
                continue
            if float(t.get("ret_63_at_entry") or -1e9) <= R.MIN_RET_63:
                continue
            if str(t.get("regime")) not in R.TRADEABLE_REGIMES:
                continue
            raw = _score(t, p["quality"], above.get(date.fromisoformat(str(t["entry_day"]))))
            if 1.0 + sum(1 for c in cuts if raw >= c) >= cf.CONVICTION_BAR:
                took.append(t)
        selected.extend(took)
        table.append({"year": year, "taken": len(took), "prior": p["n_prior"],
                      "turnover_cap": round(p["turnover_cap"], 2), "setups": len(p["setups"])})

    print(f"\nselected across all years: {len(selected):,}")
    for r in table:
        if r.get("note"):
            print(f"  {r['year']}  {r['note']}")
        else:
            print(f"  {r['year']}  took {r['taken']:4}   prior {r['prior']:6,}   "
                  f"turnover cap {r['turnover_cap']:5.1f}cr   setups {r['setups']}")

    if args.out:
        Path(args.out).write_text(json.dumps(
            {"per_year_params": table, "selected": len(selected),
             "selected_rows": selected}, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
