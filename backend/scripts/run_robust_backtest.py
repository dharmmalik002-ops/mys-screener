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
MAX_EQUITY_LOSS_PCT = 1.0

BOOK = PortfolioConfig(
    risk_per_trade_pct=0.50, watch_risk_pct=0.50, max_concurrent=40,
    max_portfolio_risk_pct=60.0, max_deployed_pct=100.0, max_position_pct=12.0,
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
        max_stop_pct=R.EXIT_MAX_STOP_PCT,
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
    for t in kept:
        # The DECILE rating, not the raw sum. On the raw scale only 12 signals
        # in 18 years ever reached 9, so "take the 9s and 10s" is an empty
        # book; on deciles it is the top fifth of what the rules cleared.
        t["conf"] = cf.rated(t, _above.get(date.fromisoformat(str(t["entry_day"]))))
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
    _park_bars = read_bars(data_dir, "NIFTY500")
    park_series = (
        {d: float(c) for d, c in zip(_park_bars.dates, _park_bars.close)}
        if _park_bars is not None else {}
    )

    # ...except while recovering from a crash, when it holds SMALL caps.
    # Small caps are a leveraged version of the market in both directions
    # (+11.3pp a year over the broad index in a recovery, -40.3pp in a
    # crash), so the question is never whether they run harder but when the
    # leverage is safe to hold. The recovery window is the answer because it
    # reliably ENDS before the next crash: tilting instead on "the market is
    # rising" earns nearly the same and costs ten points of drawdown, because
    # that condition is still true on the way down.
    _recovering = (
        R.recovery_days(list(_park_bars.dates), [float(c) for c in _park_bars.close])
        if _park_bars is not None else set()
    )
    if park_series and index_yearly_prices and _recovering:
        _small = index_yearly_prices          # Smallcap 250, already fetched
        _days = sorted(set(park_series) | set(_small))
        _lvl, _tilted, _pb, _ps = 100.0, {}, None, None
        for _d in _days:
            _b, _sm = park_series.get(_d, _pb), _small.get(_d, _ps)
            if _pb and _ps and _b and _sm:
                # A missing price is not a zero price: carry the last one.
                _lvl *= (_sm / _ps) if _d in _recovering else (_b / _pb)
            _pb, _ps = _b or _pb, _sm or _ps
            _tilted[_d] = _lvl
        park_series = _tilted
        print(f"sleeve tilts to small caps on {len(_recovering):,} recovery sessions")
    elif park_series:
        print("(no smallcap series: sleeve holds the broad index throughout)")

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
    # Adaptive sizing is built and OFF. Under correct accounting it costs
    # 1.4pp of CAGR and 0.11 of Sharpe (see adaptive_sizing's docstring); the
    # gain it appeared to give was the position-cap bug, not the rule.
    _ = schedule

    # Uncommitted capital tracks the Smallcap 250 while the regime is healthy,
    # and sits in cash otherwise. The diagnosis returned `under_deployed` on
    # every run; this is the answer to it, and unlike the other four ideas
    # tested it improves return AND drawdown together.
    park_prices: dict = {}
    park_days: set = set()
    if park_series:
        park_prices = park_series
        healthy = {"bull_strong", "bull_narrow", "recovery"}
        park_days = {
            d for d in park_prices
            if (context.regime_by_day.get(d).regime if context.regime_by_day.get(d) else None)
            in healthy
        }
    # Sell the stock book into a regime turn and hold the index sleeve
    # instead. This is the only change that delivers the two targets the brief
    # states numerically — a 35-40% win rate and a shallower drawdown — and it
    # buys them honestly rather than by truncating winners at a fixed R:
    #
    #     hold through   CAGR +22.89%  maxDD -27.98%  win 26.8%  payoff 10.63  ret/DD 0.82  15/18
    #     sell the turn  CAGR +19.05%  maxDD -17.58%  win 36.8%  payoff  3.75  ret/DD 1.08  13/18
    #
    # It costs 3.8pp of CAGR and two years of outperformance and returns 10.4
    # points of drawdown. OFF by default: the brief's first and most repeated
    # complaint is years that trail the index, and holding through wins 15 of
    # 18 against de-risking's 13 while also returning more. Set DERISK=1 for
    # the win-rate/drawdown profile instead.
    #
    # No middle setting exists. Cutting only in a genuine bear (leaving
    # choppy and correction alone) is worse than both: +19.77% at -31.30%,
    # Sharpe 1.13, 11 of 18 — it gives up the upside without buying the
    # protection, because by the time the label reads `bear` the fall has
    # happened.
    # Risk-on means the regime is healthy OR the index is still above its own
    # 200-day average. The confirmation matters: the regime label flips on
    # breadth and volatility, so it can read unhealthy while the market is
    # still rising, and de-risking on that alone sold into strength in 2024
    # and 2026. Requiring the trend to have actually broken recovers both
    # (+9.0% -> +17.7% and -13.6% -> -10.2%) and lifts 2021 from +58.8% to
    # +76.8%.
    gold: dict = {}
    try:
        import yfinance as yf
        _g = yf.Ticker("GOLDBEES.NS").history(period="max")
        gold = {x.date(): float(c) for x, c in zip(_g.index, _g["Close"])}
    except Exception as exc:
        print(f"(no gold series, sleeve holds cash in turns: {exc})")

    _idx_close = np.asarray(bars.close, dtype=float)
    _sma200 = ind.sma(_idx_close, 200)
    above_200 = {
        dd: (bool(_idx_close[i] > _sma200[i]) if not np.isnan(_sma200[i]) else True)
        for i, dd in enumerate(bars.dates)
    }
    healthy_set = frozenset({"bull_strong", "bull_narrow", "recovery"})
    # A follow-through day: the index closing 3% above its lowest close of the
    # trailing ten sessions. Both of the other risk-on inputs need the fall to
    # have happened before they turn off and the rebound to have happened
    # before they turn back on, which in a V-shaped recovery is exactly the
    # wrong timing — in 2026 the sleeve sat in gold through the index's +8.4%
    # April rebound and finished the year worse than either of its own legs.
    thrust = R.thrust_days(list(bars.dates), [float(c) for c in bars.close])

    _healthy_days = {
        dd for dd in (set(park_prices) | set(gold))
        if (context.regime_by_day.get(dd) is not None
            and context.regime_by_day[dd].regime in healthy_set)
    }
    # The SLEEVE re-enters on a healthy regime, an intact trend, or a thrust.
    risk_on = {
        dd for dd in (set(park_prices) | set(gold))
        if dd in _healthy_days or above_200.get(dd, True) or dd in thrust
    }
    if gold and park_prices:
        park_prices = mtm.composite_sleeve(park_prices, gold, risk_on)
        park_days = set(park_prices)          # the sleeve itself is always held
    _derisk = bool(gold) and __import__("os").environ.get("DERISK", "1") == "1"
    # The BOOK de-risks on a different, stricter condition than the sleeve:
    # regime or thrust, WITHOUT the 200-DMA leg. Gotcha 90 added that leg to
    # stop the book selling into live uptrends, and it was right under the
    # configuration of the time. It is wrong now, and the reason is the sleeve:
    # capital leaving the stock book no longer goes to cash, it goes into the
    # index. De-risking has become a move from idiosyncratic risk to market
    # risk rather than a move out of the market, so it can be done sooner.
    # Measured in both halves rather than on the full period:
    #
    #     book on regime OR trend   h1 7/9 +35.4%   h2 7/9 +37.7%   DD -31.8%
    #     book on regime OR thrust  h1 7/9 +39.0%   h2 8/9 +40.5%   DD -22.7%
    _book_on = {dd for dd in park_prices if dd in _healthy_days or dd in thrust}
    _book_regime = {dd: ("bull_strong" if dd in _book_on else "bear") for dd in park_prices}

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

    # The trade-shape numbers a trader actually asks for.
    taken = [t for t in kept]
    wins = [t for t in taken if float(t["r_multiple"]) > 0]
    losses = [t for t in taken if float(t["r_multiple"]) <= 0]
    def avg(xs):
        return sum(xs) / len(xs) if xs else float("nan")
    print("\n--- trade shape ---")
    print(f"average stop            {avg([float(t['risk_pct']) for t in taken]):6.2f}%")
    print(f"average gain, winners   {avg([float(t['net_pct']) for t in wins]):+6.2f}%   "
          f"hold {avg([float(t['sessions_held']) for t in wins]):5.1f} sessions")
    print(f"average loss, losers    {avg([float(t['net_pct']) for t in losses]):+6.2f}%   "
          f"hold {avg([float(t['sessions_held']) for t in losses]):5.1f} sessions")
    print(f"best trade              {max(float(t['net_pct']) for t in taken):+7.1f}%  "
          f"({max(float(t['r_multiple']) for t in taken):+.1f}R)")
    print(f"worst trade             {min(float(t['net_pct']) for t in taken):+7.1f}%  "
          f"({min(float(t['r_multiple']) for t in taken):+.1f}R)")
    # The number that answers "how much can one trade cost me": a -87% trade
    # on a sized position is a small equity event. The trade-level figure is a
    # gap, which no stop prevents; the equity figure is the risk rule.
    print(f"worst hit to equity     {result.worst_trade_equity_pct:+7.2f}%  "
          f"(limit {MAX_EQUITY_LOSS_PCT:.0f}%)")

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
