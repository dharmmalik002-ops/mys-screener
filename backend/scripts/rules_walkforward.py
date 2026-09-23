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
from app.services.bot import group_strength as gs  # noqa: E402
from app.services.bot import indicators as ind  # noqa: E402
from app.services.bot import mtm_account as mtm  # noqa: E402
from app.services.bot import rules as R  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.portfolio import PortfolioConfig  # noqa: E402

FIRST_TRADED_YEAR = 2012      # needs a few years of prior signals to derive from
MIN_PRIOR_SIGNALS = 400       # below this the year stands down rather than guessing


def _rederive(prior: list[dict], cut: str | None = None) -> dict:
    """Everything the book fits, re-fitted on `prior` alone.

    Outcomes come only from trades CLOSED before `cut`. A trade entered before
    the cut but still open at it has an R nobody knew yet — with 500-session
    holds that was most of the recent evidence, and reading it was look-ahead
    (gotcha 115). Entry-time fields (turnover) may use every prior entry.
    """
    turnovers = [float(t["turnover_crore_at_entry"]) for t in prior
                 if t.get("turnover_crore_at_entry") is not None]
    by_setup: dict[str, list[float]] = {}
    for t in prior:
        if cut is not None and not (t.get("exit_day") and str(t["exit_day"]) < cut
                                    and t.get("exit_reason") != "open"):
            continue
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


def _score(trade: dict, quality: dict, above200: bool | None,
           group_rank: float | None = None) -> float:
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
    total += cf.W_GROUP * (float(group_rank) if group_rank is not None else 0.0)
    return max(1.0, round(total + cf.W_MARKET * market, 2))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default="", help="pickled signal cache (skips the 90s rebuild)")
    ap.add_argument("--out", default="")
    ap.add_argument("--if-stale", action="store_true",
                    help="do nothing unless bot_live_params.json is from an earlier year")
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    if args.if_stale:
        live_path = data_dir / "bot_live_params.json"
        if live_path.exists() and int(json.loads(live_path.read_text()).get("year", 0)) >= date.today().year:
            print("live params are current; the yearly rebuild runs each January")
            return 0
    from app.services.bot import results_calendar as rc
    print(f"results calendar: {rc.load(data_dir):,} symbols")
    if args.signals and Path(args.signals).exists():
        rows = pickle.loads(Path(args.signals).read_bytes())["rows"]
        print(f"loaded {len(rows):,} cached signals")
    else:
        symbols = available_symbols(data_dir)
        context = build_context(data_dir, symbols)
        exits = R.exit_model()
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

    # Group strength is causal per day already, so there is nothing to
    # re-derive: the rank on a signal day reads closes up to that day only.
    ranks = gs.build_ranks(data_dir)
    grank = lambda t: ranks.rank(t["symbol"], str(t["signal_day"]))
    # The rebuild chooses among CANDIDATE_SETUPS; `_rederive` admits one only
    # once its closed record before that January is positive.
    cleared = R.accepted_with_rolling_risk(rows, R.CANDIDATE_SETUPS)     # already causal
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
        p = _rederive(prior, cut)
        # Re-derive the decile cuts on the PRIOR signals, scored with the
        # prior-derived quality map — not on the year being traded.
        prior_raw = sorted(_score(t, p["quality"], above.get(date.fromisoformat(str(t["signal_day"]))),
                                  grank(t))
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
            raw = _score(t, p["quality"], above.get(date.fromisoformat(str(t["signal_day"]))),
                         grank(t))
            if 1.0 + sum(1 for c in cuts if raw >= c) >= cf.CONVICTION_BAR:
                took.append(t)
        selected.extend(took)
        table.append({"year": year, "taken": len(took), "prior": p["n_prior"],
                      "turnover_cap": round(p["turnover_cap"], 2), "setups": len(p["setups"])})
        live = {"year": year, "setups": sorted(p["setups"]), "quality": p["quality"],
                "turnover_cap": p["turnover_cap"], "decile_cuts": cuts}

    print(f"\nselected across all years: {len(selected):,}")
    for r in table:
        if r.get("note"):
            print(f"  {r['year']}  {r['note']}")
        else:
            print(f"  {r['year']}  took {r['taken']:4}   prior {r['prior']:6,}   "
                  f"turnover cap {r['turnover_cap']:5.1f}cr   setups {r['setups']}")

    # The live book trades THIS year's re-derived rules — the same thing the
    # rebuild measured, rather than the single split's fixed constants.
    (data_dir / "bot_live_params.json").write_text(json.dumps(live, indent=2, default=str))
    print(f"live params ({live['year']}): {len(live['setups'])} setups")

    artifact = _account(selected, rows, cleared, data_dir, table)
    (data_dir / "bot_rules_walkforward.json").write_text(json.dumps(artifact, indent=2))
    print(f"walk-forward {artifact['cagr']:+.2f}% vs index {artifact['index_cagr']:+.2f}%, "
          f"beat {artifact['years_beaten']}/{artifact['years']}, maxDD {artifact['max_drawdown']}%")
    if args.out:
        Path(args.out).write_text(json.dumps(
            {"per_year_params": table, "selected": len(selected),
             "selected_rows": selected}, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


BOOK = PortfolioConfig(risk_per_trade_pct=0.50, watch_risk_pct=0.50, max_concurrent=40,
                       max_portfolio_risk_pct=60.0, max_deployed_pct=100.0, max_position_pct=35.0)
TINY = PortfolioConfig(risk_per_trade_pct=0.0001, watch_risk_pct=0.0001, max_concurrent=1,
                       max_portfolio_risk_pct=0.01, max_deployed_pct=0.01, max_position_pct=0.001)
RANDOM_CONTROLS = 10


def _yahoo(symbol: str) -> dict:
    try:
        import yfinance as yf
        h = yf.Ticker(symbol).history(period="max")
        return {x.date(): float(c) for x, c in zip(h.index, h["Close"])}
    except Exception as exc:  # noqa: BLE001
        print(f"(no {symbol}: {exc})")
        return {}


def _account(selected, rows, cleared, data_dir, table) -> dict:
    """Run the continuous account on the rebuild's picks, plus its controls."""
    import random
    import statistics as st
    from app.services.bot import sleeve as sl
    from app.services.bot.backtest import build_context as _bc
    idx = read_bars(data_dir, "NIFTY500")
    index_close = {d: float(c) for d, c in zip(idx.dates, idx.close)}
    context = _bc(data_dir, available_symbols(data_dir))
    regimes = {d: r.regime for d, r in context.regime_by_day.items()}
    gold = sl.clean_series(_yahoo("GOLDBEES.NS"))
    small = sl.clean_series(_yahoo("NIFTYSMLCAP250.NS"))
    sleeve_on, book_on = sl.risk_on_days(index_close, regimes)
    level = sl.build_level(index_close, gold, small, sleeve_on) if gold else dict(index_close)
    book = sl.book_regime(level, set(index_close), book_on)

    def run(sel, cfg=BOOK):
        return mtm.simulate(sel, data_dir, cfg, label="wf", park_idle_in=level,
                            park_only_on=set(level), regime_by_day=book,
                            healthy_regimes=frozenset({"bull_strong"}), derisk_losers_only=False,
                            pyramid=True, pyramid_scale=0.30, max_equity_loss_pct=1.5)

    def yearly_index():
        out, ds = {}, sorted(small)
        for y in range(FIRST_TRADED_YEAR, 2027):
            ins = [d for d in ds if d.year == y]; pr = [d for d in ds if d.year < y]
            if len(ins) >= 150 and pr:
                out[y] = (small[ins[-1]] / small[pr[-1]] - 1) * 100
        return out
    ix = yearly_index()

    def geom(r):
        ys = {y: v for y, v in r.yearly.items() if y in ix}
        g = 1.0
        for v in ys.values():
            g *= 1 + v / 100
        return 100 * (g ** (1 / len(ys)) - 1), sum(1 for y in ys if ys[y] >= ix[y]), ys

    full = run(selected)
    g, beat, ys = geom(full)
    gs_, _, _ = geom(run(selected, TINY))
    top = sorted(selected, key=lambda t: -float(t["r_multiple"]))[:50]
    drop = [t for t in selected if t not in top]
    gd, _, _ = geom(run(drop))
    # Matched random control: the same NUMBER of picks each year, drawn at
    # random from what the rules cleared that year.
    by_year: dict = {}
    for t in cleared:
        by_year.setdefault(str(t["entry_day"])[:4], []).append(t)
    need: dict = {}
    for t in selected:
        need[str(t["entry_day"])[:4]] = need.get(str(t["entry_day"])[:4], 0) + 1
    ctrl = []
    for seed in range(RANDOM_CONTROLS):
        rnd = random.Random(seed)
        pick = [t for y, n in need.items() for t in rnd.sample(by_year.get(y, []), min(n, len(by_year.get(y, []))))]
        ctrl.append(geom(run(pick))[0])
    gi = 1.0
    for y in ys:
        gi *= 1 + ix[y] / 100
    byy: dict = {}
    for t in selected:
        byy.setdefault(int(str(t["entry_day"])[:4]), []).append(float(t["r_multiple"]))
    ctrl.sort()
    return {
        "evaluation": "yearly rebuild — every fitted parameter re-derived each January from trades "
                      "CLOSED before it, account run continuously, every state acted on the next session",
        "first_year": min(ys), "last_year": max(ys), "years": len(ys),
        "cagr": round(g, 2), "index_cagr": round(100 * (gi ** (1 / len(ys)) - 1), 2),
        "max_drawdown": full.max_drawdown_pct, "sharpe": full.sharpe,
        "win_rate": full.win_rate, "payoff": full.payoff, "trades": full.trades_taken,
        "years_beaten": beat, "worst_trade_equity_pct": full.worst_trade_equity_pct,
        "sleeve_only_cagr": round(gs_, 2), "stock_picking_worth_pp": round(g - gs_, 2),
        "signal_avg_r": round(st.mean([r for v in byy.values() for r in v]), 3),
        "signal_years_negative": sum(1 for v in byy.values() if st.mean(v) < 0),
        "yearly": {str(y): round(v, 2) for y, v in ys.items()},
        "index_yearly": {str(y): round(v, 2) for y, v in ix.items()},
        "signal_avg_r_by_year": {str(y): round(st.mean(v), 3) for y, v in byy.items()},
        "trades_by_year": {str(y): len(v) for y, v in byy.items()},
        "robustness": {
            "drop_top_50_trades_cagr": round(gd, 2),
            "matched_random_control_cagr": round(st.mean(ctrl), 2),
            "matched_random_control_95th": round(ctrl[int(0.95 * (len(ctrl) - 1))], 2),
            "beats_matched_control": g > ctrl[int(0.95 * (len(ctrl) - 1))],
        },
        "per_year_params": table,
    }


if __name__ == "__main__":
    raise SystemExit(main())
