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
from datetime import date
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import adaptive_sizing as ad  # noqa: E402
from app.services.bot import confidence as cf  # noqa: E402
from app.services.bot import group_strength as gs  # noqa: E402
from app.services.bot import diagnose as dg  # noqa: E402
from app.services.bot import indicators as ind  # noqa: E402
from app.services.bot import memory as mem  # noqa: E402
from app.services.bot import mtm_account as mtm  # noqa: E402
from app.services.bot import sleeve as sl  # noqa: E402
from app.services.bot import rules as R  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.benchmark import INDEX_KEY  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.portfolio import PortfolioConfig  # noqa: E402

# Chosen on years-beaten and CAGR jointly, after the position-cap bug was
# fixed. The earlier 4% cap bound on 100% of trades, so the risk budget had no
# effect on position size at all; 8% lets it express itself. Selecting instead
# on return-per-drawdown picked a book that beat the index in only 8 years of
# 18, which is the wrong thing to optimise here.
# Sized for a confidence-filtered book: far fewer trades, so each one gets
# more money. 0.25%/8%/60 slots was right when the book took 1,536 trades;
# taking 541 of the best-scored ones supports 0.5%/12%/40.
# The brief's hard risk rule: a single trade may cost at most 1% of total
# equity. See `mtm_account.GAP_ALLOWANCE_PCT` for why the stop alone does not
# enforce it. Measured cost: the worst single-trade hit to equity falls from
# -1.65% to -0.91%, and the account's own drawdown from -40.92% to -31.81%.
MAX_EQUITY_LOSS_PCT = 1.5

BOOK = PortfolioConfig(
    risk_per_trade_pct=0.50, watch_risk_pct=0.50, max_concurrent=40,
    max_portfolio_risk_pct=60.0, max_deployed_pct=100.0, max_position_pct=35.0,
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
    exits = R.exit_model()
    # Results dates feed `results_follow_through` (gotcha 116); an absent file
    # leaves it silent rather than guessing.
    from app.services.bot import results_calendar as rc
    print(f"results calendar: {rc.load(data_dir):,} symbols")
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

    # Score every surviving signal 1-10 and take only high conviction. The
    # score was built on the training half and holds out of sample: >=8 is
    # +0.088R in training and +0.069R held out, against -0.068 / -0.035 for
    # the full set — the first selection filter in this project to show a
    # positive edge in BOTH halves.
    _idx_bars = read_bars(data_dir, "NIFTY500")
    _ic = np.asarray(_idx_bars.close, dtype=float)
    _is200 = ind.sma(_ic, 200)
    _above = {dd: (bool(_ic[i] > _is200[i]) if not np.isnan(_is200[i]) else True)
              for i, dd in enumerate(_idx_bars.dates)}
    # Industry-group strength on the signal day (gotcha 113).
    _ranks = gs.build_ranks(data_dir)
    for t in kept:
        # The DECILE rating, not the raw sum. On the raw scale only 12 signals
        # in 18 years ever reached 9, so "take the 9s and 10s" is an empty
        # book; on deciles it is the top fifth of what the rules cleared.
        t["conf"] = cf.rated(t, _above.get(date.fromisoformat(str(t["signal_day"]))),
                             _ranks.rank(t["symbol"], str(t["signal_day"])))
    scored = len(kept)
    kept = [t for t in kept if t["conf"] >= cf.CONVICTION_BAR]
    print(f"signals {len(rows):,}  cleared rules {scored:,}  "
          f"conviction {cf.CONVICTION_BAR:.0f}-10 {len(kept):,} "
          f"({100*len(kept)/max(scored,1):.1f}%)")

    # Benchmark prices (Smallcap 250) for the comparison table.
    index_yearly_prices: dict = {}
    try:
        import yfinance as yf
        _h = yf.Ticker("NIFTYSMLCAP250.NS").history(period="max")
        index_yearly_prices = {x.date(): float(c) for x, c in zip(_h.index, _h["Close"])}
    except Exception as exc:
        print(f"(no index series: {exc})")

    # Idle capital parks in the BROAD index, not the benchmark. The book is
    # ~80% small cap, so parking the remainder in smallcap too doubles down on
    # the same exposure; the Nifty 500 is the better sleeve on every measure
    # that matters here — 15 years beating the benchmark against 13, a
    # shallower drawdown (-27.98% vs -29.89%) and a higher Sharpe (1.32 vs
    # 1.28), for 0.3pp of CAGR. It also ships in the local store, so the
    # account does not depend on an external fetch.
    # The sleeve and the book's de-risk come from sleeve.py — the SAME module
    # the paper book and the yearly rebuild use, so the three cannot drift.
    # Broad index (small caps inside a crash-recovery window) while the
    # regime is healthy or the index is above its 200-DMA, gold otherwise;
    # the stock book sells into an unhealthy regime. Every state is read at a
    # close and acts on the NEXT session (gotcha 115).
    _park_bars = read_bars(data_dir, "NIFTY500")
    index_close = (
        {d: float(c) for d, c in zip(_park_bars.dates, _park_bars.close)}
        if _park_bars is not None else {}
    )
    gold: dict = {}
    try:
        import yfinance as yf
        _g = yf.Ticker("GOLDBEES.NS").history(period="max")
        gold = sl.clean_series({x.date(): float(c) for x, c in zip(_g.index, _g["Close"])})
    except Exception as exc:
        print(f"(no gold series, sleeve holds the index throughout: {exc})")
    regimes = {d: r.regime for d, r in context.regime_by_day.items()}
    small = sl.clean_series(index_yearly_prices) if index_yearly_prices else None
    # Market-type sleeve (gotcha 117): each regime holds the asset that paid
    # best under it in the years before, re-derived every January.
    liquid = sl.fetch_liquid_fund()
    print(f"liquid fund: {len(liquid):,} NAVs" if liquid else "WARNING: no liquid-fund series — corrections fall back to gold/index")
    park_prices, _book_regime = sl.build_sleeve(index_close, gold, small, regimes, cash_close=liquid)
    park_days = set(park_prices)
    _derisk = bool(gold) and __import__("os").environ.get("DERISK", "1") == "1"
    sleeve_on, book_on = sl.risk_on_days(index_close, regimes,
                                         small_close=small if sl.SLEEVE_MODE == "regime_map" else None)
    print(f"sleeve mode {sl.SLEEVE_MODE}; book invested on {len(book_on):,} sessions")

    # Adaptive sizing is built and OFF. Under correct accounting it costs
    # 1.4pp of CAGR and 0.11 of Sharpe (see adaptive_sizing's docstring); the
    # gain it appeared to give was the position-cap bug, not the rule. The
    # schedule is still built so it stays testable.
    _s200 = ind.sma(np.asarray(_park_bars.close, dtype=float), 200)
    schedule = ad.build_schedule(
        regimes, {r.day: r.pct_above_200dma for r in context.breadth},
        {d: (bool(c > _s200[i]) if not np.isnan(_s200[i]) else None)
         for i, (d, c) in enumerate(zip(_park_bars.dates, _park_bars.close))},
    )
    _ = schedule

    result = mtm.simulate(
        kept, data_dir, BOOK, label="rules",
        park_idle_in=park_prices or None,
        park_only_on=park_days or None,
        regime_by_day=_book_regime if _derisk else None,
        healthy_regimes=frozenset({"bull_strong"}) if _derisk else None,
        derisk_losers_only=False,
        pyramid=True, pyramid_scale=0.30,
        # No single trade may cost more than 1% of total equity. A stop does
        # not deliver that on its own — it is a resting order and a gap jumps
        # it — so the position is sized against an assumed adverse move of
        # GAP_ALLOWANCE_PCT as well as against the stop itself.
        max_equity_loss_pct=MAX_EQUITY_LOSS_PCT,
    )
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
        px = index_yearly_prices
        _d = date
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

    # The trade-shape numbers a trader actually asks for — over the positions
    # the account FILLED, not over every signal that cleared. Computed over
    # signals this block once reported a -62.4% worst trade that the account
    # never took, while the worst position it actually held lost 41.6%.
    taken = result.fills
    wins = [t for t in taken if float(t["r"]) > 0]
    losses = [t for t in taken if float(t["r"]) <= 0]
    def avg(xs):
        return sum(xs) / len(xs) if xs else float("nan")
    print("\n--- trade shape (positions actually filled) ---")
    print(f"average stop            {avg([float(t['risk_pct']) for t in taken]):6.2f}%   "
          f"widest {max(float(t['risk_pct']) for t in taken):.2f}%  "
          f"(hard ceiling {R.HARD_MAX_RISK_PCT:.0f}%)")
    print(f"average gain, winners   {avg([float(t['net_pct']) for t in wins]):+6.2f}%   "
          f"hold {avg([float(t['sessions_held']) for t in wins if t['sessions_held'] is not None]):5.1f} sessions")
    print(f"average loss, losers    {avg([float(t['net_pct']) for t in losses]):+6.2f}%   "
          f"hold {avg([float(t['sessions_held']) for t in losses if t['sessions_held'] is not None]):5.1f} sessions")
    print(f"best trade              {max(float(t['net_pct']) for t in taken):+7.1f}%  "
          f"({max(float(t['r']) for t in taken):+.1f}R)")
    print(f"worst trade             {min(float(t['net_pct']) for t in taken):+7.1f}%  "
          f"({min(float(t['r']) for t in taken):+.1f}R)")
    # The number that answers "how much can one trade cost me". A trade can
    # lose far more than its stop when the market GAPS through it overnight —
    # TEXRAIL's stop sat at -6.7% and the stock opened -41.7% the next
    # morning. No stop prevents that; only position size does.
    print(f"worst hit to equity     {result.worst_trade_equity_pct:+7.2f}%  "
          f"(limit {MAX_EQUITY_LOSS_PCT:.1f}%)")
    print(f"stops above the {R.HARD_MAX_RISK_PCT:.0f}% ceiling: "
          f"{sum(1 for t in taken if float(t['risk_pct']) > R.HARD_MAX_RISK_PCT)}    "
          f"trades costing more than {MAX_EQUITY_LOSS_PCT:.1f}% of equity: "
          f"{sum(1 for t in taken if float(t['equity_pct']) < -MAX_EQUITY_LOSS_PCT)}")

    # Per-strategy and per-year shape, over the positions the account actually
    # FILLED rather than over every signal that cleared. Those are different
    # books: 2,720 signals clear and ~1,200 get a slot, so shape computed over
    # signals describes a portfolio that was never run.
    def _shape(rows):
        w = [r for r in rows if r["r"] > 0]
        l = [r for r in rows if r["r"] <= 0]
        hw = [r["sessions_held"] for r in w if r["sessions_held"] is not None]
        hl = [r["sessions_held"] for r in l if r["sessions_held"] is not None]
        m = lambda xs: (sum(xs) / len(xs)) if xs else float("nan")
        return (len(rows), 100.0 * len(w) / max(len(rows), 1),
                m([r["risk_pct"] for r in rows]), m([r["net_pct"] for r in w]),
                m([r["net_pct"] for r in l]),
                max([r["net_pct"] for r in rows], default=float("nan")),
                min([r["net_pct"] for r in rows], default=float("nan")),
                m(hw), m(hl), m([r["r"] for r in rows]),
                min([r["equity_pct"] for r in rows], default=float("nan")))

    _hdr = (f"{'':21}{'n':>5}{'win%':>6}{'stop':>7}{'avg gain':>10}{'avg loss':>9}"
            f"{'max gain':>10}{'max loss':>9}{'holdW':>7}{'holdL':>6}{'avgR':>7}{'worstEq':>9}")

    def _row(label, rows):
        n, win, stop, gain, loss, mx, mn, hw, hl, r, eq = _shape(rows)
        print(f"{label:21}{n:5}{win:6.1f}{stop:6.2f}%{gain:+9.1f}%{loss:+8.1f}%"
              f"{mx:+9.1f}%{mn:+8.1f}%{hw:7.0f}{hl:6.0f}{r:+7.2f}{eq:+8.2f}%")

    fills = result.fills
    for title, keyfn in (("BY STRATEGY", lambda r: r["strategy"] or "?"),
                         ("BY YEAR", lambda r: r["entry_day"][:4])):
        groups: dict = {}
        for f in fills:
            groups.setdefault(keyfn(f), []).append(f)
        order = (sorted(groups, key=lambda g: -len(groups[g]))
                 if title == "BY STRATEGY" else sorted(groups))
        print(f"\n--- {title} (positions actually filled: {len(fills)}) ---")
        print(_hdr)
        for g in order:
            _row(str(g), groups[g])
        if title == "BY STRATEGY":
            _row("ALL", fills)

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
        "worst_trade_equity_pct": result.worst_trade_equity_pct,
    }, config={
        "max_stop_pct": R.EXIT_MAX_STOP_PCT, "trail_atr_mult": R.EXIT_TRAIL_ATR_MULT,
        "max_hold": R.EXIT_MAX_HOLD_SESSIONS, "risk_per_trade": BOOK.risk_per_trade_pct,
        "max_position_pct": BOOK.max_position_pct, "derisk": _derisk, "pyramid": True,
    })
    print(f"\nmemory: {mem.recall(state_dir).note}")
    # The record is consulted for an action, not just printed. It can only
    # stand something down, never promote it, and it refuses on thin evidence.
    rec = mem.recommend(state_dir)
    print(f"memory says: {rec.action} — {rec.reason}")

    if args.out:
        Path(args.out).write_text(json.dumps({
            "yearly": result.yearly, "trades_per_year": counts, "size_mix": caps,
            "diagnosis": [d.to_dict() for d in rows_d], "summary": summary,
            "cagr": result.cagr_pct, "max_drawdown": result.max_drawdown_pct,
            "sharpe": result.sharpe, "win_rate": result.win_rate,
            "payoff": result.payoff, "trades": result.trades_taken,
            "fills": result.fills,
        }, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
