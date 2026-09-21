#!/usr/bin/env python3
"""Let the bot propose and validate its own configuration changes.

    python3 scripts/autotune_bot.py

Each candidate is scored on a training window and on a held-out window, and
adopted only if it beats the incumbent on BOTH. "Adopt nothing" is the
expected result; config selection on one window correlates -0.70 with
held-out return here, so a tuner that always finds a winner is fitting noise.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import autotune as at  # noqa: E402
from app.services.bot import indicators as ind  # noqa: E402
from app.services.bot import memory as mem  # noqa: E402
from app.services.bot import mtm_account as mtm  # noqa: E402
from app.services.bot import rules as R  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies_multi  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402
from app.services.bot.portfolio import PortfolioConfig  # noqa: E402

SPLIT = date(2018, 1, 1)


def main() -> int:
    data_dir = BACKEND_ROOT / "data"
    symbols = available_symbols(data_dir)
    context = build_context(data_dir, symbols)

    baseline = {
        "trail_atr_mult": R.EXIT_TRAIL_ATR_MULT,
        "max_stop_pct": R.EXIT_MAX_STOP_PCT,
        "max_hold_sessions": float(R.EXIT_MAX_HOLD_SESSIONS),
        "pyramid_scale": 0.30,
    }
    configs = {"baseline": baseline}
    for cand in at.DEFAULT_CANDIDATES:
        configs[cand.name] = {**baseline, **cand.overrides}

    exits = {
        name: ExitModel(
            target_r=None, max_hold_sessions=int(c["max_hold_sessions"]),
            trail_after_r=R.EXIT_TRAIL_AFTER_R, trail_atr_mult=c["trail_atr_mult"],
            max_stop_pct=c["max_stop_pct"],
        )
        for name, c in configs.items()
    }
    print(f"replaying once under {len(exits)} configurations ...")
    by_cfg = run_strategies_multi(data_dir, context, BacktestConfig(), exits, symbols)

    bars = read_bars(data_dir, "NIFTY500")
    n5 = {d: float(c) for d, c in zip(bars.dates, bars.close)}
    closes = np.asarray(bars.close, dtype=float)
    sma200 = ind.sma(closes, 200)
    above = {d: (bool(closes[i] > sma200[i]) if not np.isnan(sma200[i]) else True)
             for i, d in enumerate(bars.dates)}
    gold: dict = {}
    try:
        import yfinance as yf
        h = yf.Ticker("GOLDBEES.NS").history(period="max")
        gold = {x.date(): float(c) for x, c in zip(h.index, h["Close"])}
    except Exception as exc:
        print(f"(no gold: {exc})")
    healthy = frozenset({"bull_strong", "bull_narrow", "recovery"})
    risk_on = {d for d in (set(n5) | set(gold))
               if (context.regime_by_day.get(d) is not None
                   and context.regime_by_day[d].regime in healthy) or above.get(d, True)}
    sleeve = mtm.composite_sleeve(n5, gold, risk_on) if gold else n5
    book_regime = {d: ("bull_strong" if d in risk_on else "bear") for d in sleeve}
    book = PortfolioConfig(risk_per_trade_pct=0.25, watch_risk_pct=0.25, max_concurrent=60,
                           max_portfolio_risk_pct=60.0, max_deployed_pct=100.0,
                           max_position_pct=8.0)

    cache: dict[tuple[str, bool], float] = {}

    def run(name: str, holdout: bool) -> float:
        key = (name, holdout)
        if key in cache:
            return cache[key]
        rows = [t.to_dict() for t in by_cfg[name]]
        kept = R.accepted_with_rolling_risk(rows)
        kept = [t for t in kept
                if (date.fromisoformat(str(t["entry_day"])) >= SPLIT) == holdout]
        res = mtm.simulate(kept, data_dir, book, label=name, park_idle_in=sleeve,
                           park_only_on=set(sleeve), regime_by_day=book_regime,
                           healthy_regimes=frozenset({"bull_strong"}),
                           derisk_losers_only=False, pyramid=True,
                           pyramid_scale=configs[name]["pyramid_scale"])
        cache[key] = res.cagr_pct if res else -99.0
        return cache[key]

    name_of = {id(c): n for n, c in configs.items()}

    def score(overrides, holdout: bool) -> float:
        for n, c in configs.items():
            if all(abs(c[k] - overrides[k]) < 1e-9 for k in c):
                return run(n, holdout)
        return -99.0

    trials = at.evaluate(at.DEFAULT_CANDIDATES, score, baseline)
    print(f"\n{'candidate':16} {'train':>8} {'held-out':>9}  verdict")
    base_t, base_h = run("baseline", False), run("baseline", True)
    print(f"{'baseline':16} {base_t:+8.2f} {base_h:+9.2f}")
    for t in trials:
        print(f"{t.name:16} {t.train_score:+8.2f} {t.holdout_score:+9.2f}  {t.note}")

    chosen = at.adopt(trials)
    print(f"\nADOPT: {chosen.name if chosen else 'nothing — no candidate held up on both windows'}")
    out = data_dir / "bot_autotune.json"
    out.write_text(json.dumps({
        "baseline": {"train": base_t, "holdout": base_h},
        "trials": [t.to_dict() for t in trials],
        "adopted": chosen.name if chosen else None,
    }, indent=2))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
