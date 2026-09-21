#!/usr/bin/env python3
"""Can the bot be scaled up to the funds' risk level and out-earn them?

    python3 scripts/risk_frontier.py

The finished product earns +7.69% against the median fund's +11.36%, and the
obvious objection is that it is not being compared fairly: it runs at -7.28%
drawdown against the funds' -27.53%, roughly a quarter of their risk. Comparing
raw returns across different risk levels is the exact error `funds_dominating`
exists to avoid, and it cuts both ways. So: turn the risk up to theirs and see
what the return does.

**`max_deployed_pct` stays at 100 throughout. No margin, ever.** This is cash
delivery trading; a frontier built on borrowed money would be measuring a
different instrument. The only lever is position size.

The answer is no, and the reason is structural rather than a tuning failure.
The account is already **~97% deployed** at the shipped setting, so a larger
risk budget buys no additional capital — it simply concentrates the same
capital into fewer, bigger positions. That destroys the thing the 60-position
book exists for: R outcomes here are violently right-skewed, about 12% of
trades carry the whole result, and a concentrated book samples that tail badly.
Return falls while drawdown rises.

One point on the grid (0.15) reads better than the shipped 0.10 on both axes.
It is **deliberately not adopted.** The book structure was chosen on pre-split
data by Sharpe; re-picking it now on the held-out window is precisely the
error this project has caught eight times. The non-monotonic shape of the grid
(better at 0.15 and 0.20, worse at 0.30-0.60, better again at 0.80) is what
noise looks like, and treating one bump in it as a discovery is how a backtest
becomes a story.

What this establishes is a limit, not a setting: **the bot's advantage is on
the risk axis and cannot be converted into return.** Its low drawdown is not
idle capital waiting to be put to work.

Caveat the cost model cannot charge for: larger positions attract market impact
beyond the turnover-scaled slippage in `costs.py`, so the higher-risk rows here
are, if anything, optimistic — which strengthens the conclusion rather than
weakening it.
"""
import json, sys
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
BACKEND_ROOT = Path("/Users/dharmender/Desktop/Stock Scanner c/backend")
sys.path.insert(0, str(BACKEND_ROOT))
from app.services.bot import portfolio as pf, timing as tm
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies
from app.services.bot.circuit_breaker import suspended_mask
from app.services.bot.combined import blend, curve_to_series, stats
from app.services.bot.benchmark import INDEX_KEY
from app.services.bot.history import available_symbols, read_bars

data_dir = BACKEND_ROOT / "data"
symbols = available_symbols(data_dir)
context = build_context(data_dir, symbols)
trades = run_strategies(data_dir, context, BacktestConfig(), symbols)
rows = [asdict(t) for t in trades]
for r in rows:
    for k in ("signal_day", "entry_day", "exit_day"):
        if r.get(k) is not None: r[k] = str(r[k])

art = json.loads((data_dir / "bot_backtest.json").read_text())
split = date.fromisoformat(art["coverage"]["validation_split"])
cells, exp = set(), {}
for b in art.get("playbooks") or []:
    for e in b.get("entries") or []:
        k = (str(e["strategy"]), str(b["regime"])); cells.add(k); exp[k] = float(e.get("out_sample_r") or 0.0)
adj = pf.derive_atr_adjustment(rows, split)
kept = [r for r, off in zip(rows, suspended_mask(rows)) if not off]

# The timing rule holds the broad index, not the Nifty 50 — this is the
# same INDEX_KEY the benchmark and the shipped timing study use, and
# using a different one here would compare two different rules.
bars = read_bars(data_dir, INDEX_KEY)
closes = dict(zip(bars.dates, bars.close))
regime_by_day = {d: row.regime for d, row in context.regime_by_day.items()}

sessions_all = [d for d in context.sessions if d >= split]
last = sessions_all[-1]
sessions = [d for d in sessions_all if d >= last - timedelta(days=3*365+1)]
lo = sessions[0]
timed = tm.simulate(sessions, closes, regime_by_day)

# Declared in advance. max_deployed_pct stays 100 — no margin, ever.
GRID = [0.10, 0.15, 0.20, 0.30, 0.40, 0.60, 0.80]
print(f"{'risk/trade':>10} {'bookCAGR':>9} {'bookDD':>8} {'deployed':>9} | "
      f"{'blend50':>8} {'blendDD':>8} {'blendShp':>8}")
out = {}
for rpt in GRID:
    cfg = pf.PortfolioConfig(risk_per_trade_pct=rpt, watch_risk_pct=rpt*0.6,
                             max_portfolio_risk_pct=rpt*60, max_deployed_pct=100.0)
    book = pf.simulate(kept, [], cfg, start=lo, label=f"r{rpt}", playbook_cells=cells,
                       cell_expectancy=exp, atr_adjustment=adj)
    if book is None: continue
    bd, bv = curve_to_series(book.equity_curve)
    bs = stats(bd, bv)
    bl = blend(book.equity_curve, timed.equity_curve, 0.5)
    ls = stats(*bl) if bl else {}
    print(f"{rpt:>10.2f} {bs['cagr_pct']:>+9.2f} {bs['max_drawdown_pct']:>8.2f} "
          f"{book.exposure_pct:>9.1f} | {ls.get('cagr_pct',0):>+8.2f} "
          f"{ls.get('max_drawdown_pct',0):>8.2f} {ls.get('sharpe',0):>8.2f}")
    out[rpt] = {"book": bs, "blend": ls, "exposure": book.exposure_pct}
print("\ntiming sleeve alone:", stats(*curve_to_series(timed.equity_curve)))
print("fund median: CAGR +11.36%, maxDD -27.53%, Sharpe 0.40")
Path("/private/tmp/claude-501/-Users-dharmender-Desktop-Stock-Scanner-c/b819157b-9c2e-430b-ae71-a7f7c06ac14f/scratchpad/frontier.json").write_text(json.dumps(out, indent=2, default=str))
