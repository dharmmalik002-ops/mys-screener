"""The paper book — what the bot actually owns, carried across days.

Everything before this module was a backtest: one process, all the bars in
memory, the whole history replayed at once. A bot that trades has to survive
being switched off. It must know at 9:15 tomorrow what it bought last March,
what stop that position is on now, and how many sessions it has been held.

**This is the piece that has to exist before a broker connection, not after.**
An execution layer on top of a book that forgets its positions places orders
it should not place. The ledger is therefore deliberately dull: plain JSON in
`APP_STATE_DIR` beside the trade journal, one file, human-readable, so a
disagreement with the broker can be settled by reading it.

Three properties are load-bearing and each has a test.

**Idempotent per session.** `advance` refuses a session it has already
processed. A cron that fires twice, a retried workflow, or a manual re-run
must not open the same position twice — and the failure would be invisible,
because the second copy looks exactly like a legitimate pyramid entry.

**Causal.** A signal generated on the close of day D is entered at the OPEN of
day D+1, which is what the backtest charges and what a person could actually
do. `advance` takes today's candidates and today's bars and enters at today's
open only for candidates dated strictly earlier.

**The same arithmetic as the backtest.** Position size, stop placement, the
trail and the time ceiling are read from `rules.py` and `mtm_account.py`
rather than restated here. A paper book that sizes differently from the study
is not paper-trading the study; it is testing something else and calling it a
validation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import rules as R

BOOK_FILE = "bot_paper_book.json"

# Matches the shipped book in `scripts/run_robust_backtest.py`. Restating the
# numbers here would let the two drift silently, which is why the runner's
# config is the source and these are asserted equal in the tests.
MAX_CONCURRENT = 40
MAX_EQUITY_LOSS_PCT = 1.5
MAX_POSITION_PCT = 35.0
GAP_ALLOWANCE_STOP_MULT = 10.0
GAP_ALLOWANCE_PCT = 15.0


@dataclass
class Position:
    symbol: str
    strategy: str
    entry_day: str
    entry_price: float
    shares: float
    stop_price: float
    initial_stop_pct: float
    high_water: float          # highest close seen, for the trail
    atr_at_entry: float
    conf: float
    sessions_held: int = 0
    # Closing-basis trail (SEASONED_RULES) — tracked apart from the hard
    # intraday stop, exactly as the engine does it.
    trail_level: float | None = None
    # An exit decided on a close is executed at the NEXT open, so it has to
    # survive the night in the ledger.
    pending_exit: str | None = None

    def risk_amount(self) -> float:
        """What this trade risked AT ENTRY — the denominator of its R.

        Deliberately computed from `initial_stop_pct` and not from the current
        `stop_price`. Once the trail lifts the stop above entry, the live
        distance `entry - stop` goes NEGATIVE, and dividing by it flips the
        sign of every R: a trade that made 50% reported -0.68R. That bug hid
        behind the disarmed trail and only surfaced when the trail was fixed.
        """
        return self.shares * self.entry_price * self.initial_stop_pct / 100.0


@dataclass
class ClosedTrade:
    symbol: str
    strategy: str
    entry_day: str
    exit_day: str
    entry_price: float
    exit_price: float
    shares: float
    reason: str                # stop | trail | ceiling | derisk
    sessions_held: int
    net_pct: float
    r_multiple: float
    equity_pct: float          # what it cost the account, against equity at entry


@dataclass
class PaperBook:
    started: str
    starting_equity: float
    cash: float
    last_session: str | None = None
    sessions: int = 0
    # Units of the idle-capital sleeve (see `sleeve.py`). Without this the
    # book holds cash while the study holds the index, and the first paper
    # replay proved how large that gap is: +5.87% over 2.7 years against a
    # study returning ~40% a year.
    sleeve_units: float = 0.0
    sleeve_level: float = 0.0
    positions: list[Position] = field(default_factory=list)
    closed: list[ClosedTrade] = field(default_factory=list)
    equity_curve: list[dict] = field(default_factory=list)
    # Signals seen but not taken, so the record shows what was declined and
    # why — a book that only logs its fills cannot be audited against the
    # study, which declines 99% of what it sees.
    declined: list[dict] = field(default_factory=list)

    # ---- persistence -----------------------------------------------------
    @classmethod
    def load(cls, state_dir: Path) -> "PaperBook | None":
        path = Path(state_dir) / BOOK_FILE
        if not path.exists():
            return None
        raw = json.loads(path.read_text())
        book = cls(
            started=raw["started"], starting_equity=float(raw["starting_equity"]),
            cash=float(raw["cash"]), last_session=raw.get("last_session"),
            sessions=int(raw.get("sessions", 0)),
            sleeve_units=float(raw.get("sleeve_units", 0.0)),
            sleeve_level=float(raw.get("sleeve_level", 0.0)),
            equity_curve=list(raw.get("equity_curve", [])),
            declined=list(raw.get("declined", [])),
        )
        book.positions = [Position(**p) for p in raw.get("positions", [])]
        book.closed = [ClosedTrade(**t) for t in raw.get("closed", [])]
        return book

    def save(self, state_dir: Path) -> Path:
        path = Path(state_dir) / BOOK_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "started": self.started, "starting_equity": self.starting_equity,
            "cash": round(self.cash, 2), "last_session": self.last_session,
            "sessions": self.sessions,
            "sleeve_units": round(self.sleeve_units, 6),
            "sleeve_level": round(self.sleeve_level, 6),
            "positions": [asdict(p) for p in self.positions],
            "closed": [asdict(t) for t in self.closed],
            "equity_curve": self.equity_curve[-2000:],
            "declined": self.declined[-500:],
        }
        # Written whole rather than appended: a half-written ledger is worse
        # than a stale one, and at this size the cost is nothing.
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(path)
        return path

    # ---- valuation -------------------------------------------------------
    def equity(self, prices: Mapping[str, float], sleeve_level: float | None = None) -> float:
        held = 0.0
        for p in self.positions:
            # A missing price is not a zero price (gotcha 83, four times over).
            px = prices.get(p.symbol)
            held += p.shares * (px if px else p.entry_price)
        level = sleeve_level if sleeve_level else self.sleeve_level
        return self.cash + held + self.sleeve_units * (level or 0.0)


def position_size(equity: float, price: float, stop_pct: float) -> float:
    """Shares to buy, under every rule the study is measured with.

    The binding constraint is almost always the equity rule rather than the
    position cap: at a 6% stop the gap allowance is 60%, so a position may be
    at most 1.5/60 = 2.5% of equity (gotcha 107).
    """
    if price <= 0 or stop_pct <= 0:
        return 0.0
    allowance = max(GAP_ALLOWANCE_PCT, GAP_ALLOWANCE_STOP_MULT * stop_pct)
    ceiling = min(
        equity * MAX_POSITION_PCT / 100.0,
        equity * MAX_EQUITY_LOSS_PCT / allowance,
        equity * MAX_EQUITY_LOSS_PCT / stop_pct,
    )
    return max(0.0, ceiling / price)


def _exit_price(bar: Mapping[str, float], stop: float) -> tuple[float, str] | None:
    """Did this bar take the stop out, and at what price?

    A gap opens below the stop and fills at the OPEN, not at the stop — that
    is the whole of gotcha 106 and it is why the worst trade in the record
    lost 41.7% against a 6.7% stop.
    """
    o, low = float(bar["open"]), float(bar["low"])
    if o <= stop:
        return o, "gap"
    if low <= stop:
        return stop, "stop"
    return None


def advance(
    book: PaperBook,
    session: date,
    bars: Mapping[str, Mapping[str, float]],
    candidates: Iterable[Mapping[str, Any]],
    *,
    sleeve_level: float | None = None,
    force: bool = False,
) -> dict:
    """Run one session. Returns what happened, for the log.

    `bars` is today's OHLC per symbol. `candidates` are signals whose
    `entry_day` is today — generated on a previous close, entered at today's
    open.
    """
    day = session.isoformat()
    if book.last_session and day <= book.last_session and not force:
        # The guard that matters. Re-running a day must change nothing.
        return {"session": day, "skipped": "already processed", "entered": 0, "exited": 0}

    closes = {s: float(b["close"]) for s, b in bars.items() if b.get("close")}

    # --- 0. liquidate the sleeve, BEFORE anything can spend the cash --------
    # Money only conserves if this happens first. Restoring cash from parked
    # units at the end of the session, after trades had already spent it, made
    # a flat index print money and read +1345% in 2009 (gotcha 83).
    if sleeve_level and sleeve_level > 0:
        if book.sleeve_units:
            book.cash += book.sleeve_units * sleeve_level
            book.sleeve_units = 0.0
        book.sleeve_level = sleeve_level
    exited: list[str] = []
    entered: list[str] = []

    # --- 1. age and exit ---------------------------------------------------
    # Mirrors engine.simulate_symbol bar for bar. The paper book cannot call
    # the engine (it advances one session at a time and must survive
    # restarts), so it restates the rules — and reads which rules are live
    # from R.SEASONED_RULES, so adopting a rule changes both paths at once.
    close_trail = bool(R.SEASONED_RULES.get("trail_on_close"))
    climax_mult = R.SEASONED_RULES.get("climax_sma50_mult")
    still: list[Position] = []
    for p in book.positions:
        bar = bars.get(p.symbol)
        if not bar:
            still.append(p)            # no print today: carry untouched
            continue
        p.sessions_held += 1
        reason = None
        px = None
        if p.pending_exit:
            # Decided on yesterday's close, executed at today's open.
            px, reason = float(bar["open"]), p.pending_exit
        else:
            hit = _exit_price(bar, p.stop_price)
            if hit:
                px, kind = hit
                reason = "gap" if kind == "gap" else "stop"
            elif p.sessions_held >= R.EXIT_MAX_HOLD_SESSIONS:
                px, reason = float(bar["close"]), "ceiling"
        if reason:
            equity_at_exit = book.equity(closes)
            gross = p.shares * px
            book.cash += gross
            risk = p.risk_amount() or 1.0
            book.closed.append(ClosedTrade(
                symbol=p.symbol, strategy=p.strategy, entry_day=p.entry_day, exit_day=day,
                entry_price=p.entry_price, exit_price=px, shares=p.shares, reason=reason,
                sessions_held=p.sessions_held,
                net_pct=round(100.0 * (px / p.entry_price - 1.0), 3),
                r_multiple=round(p.shares * (px - p.entry_price) / risk, 3),
                equity_pct=round(100.0 * p.shares * (px - p.entry_price)
                                 / max(equity_at_exit, 1.0), 3),
            ))
            exited.append(p.symbol)
            continue

        # --- decisions on the CLOSE, acted on at the next open ------------
        close = float(bar["close"])
        p.high_water = max(p.high_water, close)
        sma50 = bar.get("sma50")
        if climax_mult and sma50 and close >= float(climax_mult) * float(sma50):
            p.pending_exit = "climax"
            still.append(p)
            continue
        if close_trail and p.trail_level is not None and close < p.trail_level:
            p.pending_exit = "trail"
            still.append(p)
            continue
        # --- trail, once the trade is a winner by 1R -----------------------
        # Current ATR, not ATR at entry: the engine trails off each bar's own
        # ATR, and a frozen entry value drifts further from it the longer a
        # winner runs — which is exactly the trade that matters.
        atr_now = float(bar.get("atr") or p.atr_at_entry or 0.0)
        initial_risk = p.entry_price * p.initial_stop_pct / 100.0
        gain_r = (close - p.entry_price) / max(initial_risk, 1e-9)
        if gain_r >= R.EXIT_TRAIL_AFTER_R and atr_now > 0:
            level = close - R.EXIT_TRAIL_ATR_MULT * atr_now
            if close_trail:
                p.trail_level = level if p.trail_level is None else max(p.trail_level, level)
            else:
                p.stop_price = max(p.stop_price, level)   # a stop never moves down
        still.append(p)
    book.positions = still

    # --- 2. enter ----------------------------------------------------------
    equity = book.equity(closes)
    held = {p.symbol for p in book.positions}
    for c in candidates:
        if len(book.positions) >= MAX_CONCURRENT:
            book.declined.append({"session": day, "symbol": c.get("symbol"), "why": "no slot"})
            continue
        sym = str(c.get("symbol"))
        if sym in held:
            book.declined.append({"session": day, "symbol": sym, "why": "already held"})
            continue
        bar = bars.get(sym)
        if not bar or not bar.get("open"):
            book.declined.append({"session": day, "symbol": sym, "why": "no price"})
            continue
        entry = float(bar["open"])
        stop_pct = min(float(c.get("risk_pct") or 99.0), R.EXIT_MAX_STOP_PCT)
        if stop_pct > R.HARD_MAX_RISK_PCT:
            book.declined.append({"session": day, "symbol": sym, "why": "stop too wide"})
            continue
        shares = position_size(equity, entry, stop_pct)
        cost = shares * entry
        if shares <= 0 or cost > book.cash:
            book.declined.append({"session": day, "symbol": sym, "why": "no cash"})
            continue
        book.cash -= cost
        book.positions.append(Position(
            symbol=sym, strategy=str(c.get("strategy") or ""), entry_day=day,
            entry_price=entry, shares=shares,
            stop_price=entry * (1.0 - stop_pct / 100.0), initial_stop_pct=stop_pct,
            high_water=float(bar.get("close") or entry),
            # The signal carries ATR as a PERCENT of price (`atr_pct_at_entry`),
            # not in rupees. Reading a non-existent `atr` field left this at
            # 0.0, which silently disarmed the trail: `gain_r >= 1 and atr > 0`
            # was never true, so every winner ran all the way back to its
            # ORIGINAL stop. 169 paper trades closed 132 stop / 30 gap / 7
            # ceiling — not one trail exit — and the win rate read 4.1%
            # against the study's 32%. A field that silently defaults to zero
            # turns a rule off without failing.
            atr_at_entry=entry * float(c.get("atr_pct_at_entry") or 0.0) / 100.0,
            conf=float(c.get("conf") or 0.0),
        ))
        held.add(sym)
        entered.append(sym)

    # --- 3. re-establish the sleeve with whatever is left ------------------
    if book.sleeve_level and book.sleeve_level > 0 and book.cash > 0:
        book.sleeve_units = book.cash / book.sleeve_level
        book.cash = 0.0

    # --- 4. record ---------------------------------------------------------
    book.last_session = day
    book.sessions += 1
    eq = book.equity(closes)
    book.equity_curve.append({
        "day": day, "equity": round(eq, 2), "open": len(book.positions),
        "cash": round(book.cash, 2), "sleeve": round(book.sleeve_units * book.sleeve_level, 2),
    })
    return {
        "session": day, "entered": len(entered), "exited": len(exited),
        "open_positions": len(book.positions), "equity": round(eq, 2),
        "entered_symbols": entered, "exited_symbols": exited,
    }


def summary(book: PaperBook) -> dict:
    """What the account has actually done, for the UI and the daily log."""
    wins = [t for t in book.closed if t.r_multiple > 0]
    losses = [t for t in book.closed if t.r_multiple <= 0]
    eq = book.equity_curve[-1]["equity"] if book.equity_curve else book.starting_equity
    peak, dd = book.starting_equity, 0.0
    for point in book.equity_curve:
        peak = max(peak, point["equity"])
        dd = min(dd, point["equity"] / peak - 1.0)
    m = lambda xs: (sum(xs) / len(xs)) if xs else None
    return {
        "started": book.started, "last_session": book.last_session,
        "sessions": book.sessions,
        "starting_equity": book.starting_equity, "equity": round(eq, 2),
        "return_pct": round(100.0 * (eq / book.starting_equity - 1.0), 2),
        "max_drawdown_pct": round(100.0 * dd, 2),
        "open_positions": len(book.positions), "closed_trades": len(book.closed),
        "win_rate": round(100.0 * len(wins) / len(book.closed), 1) if book.closed else None,
        "avg_win_pct": m([t.net_pct for t in wins]),
        "avg_loss_pct": m([t.net_pct for t in losses]),
        "worst_trade_equity_pct": min([t.equity_pct for t in book.closed], default=None),
        "declined_recent": len(book.declined),
    }
