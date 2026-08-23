"""The user's own mutual-fund holdings: storage and valuation.

Positions are stored as **transactions**, not as a units-and-average-cost
snapshot. That costs a little more typing but it is the only way to get an
honest return on a position built through a SIP: a "cost vs value" percentage
on twenty instalments spread over three years is not a return, it just
flatters or punishes depending on when the money went in. With dated
cashflows we can report XIRR, which is the real answer.

Every valuation number here is computed from the same AMFI NAV series the
charts use, so a position's gain always agrees with the fund's chart.
"""

from __future__ import annotations

import json
import threading
import uuid
from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some deployments
    psycopg = None

from . import metrics

MAX_POSITIONS = 200
MAX_TRANSACTIONS_PER_POSITION = 600


class MutualFundPortfolioStore:
    """Durable storage for the fund portfolio blob, mirroring the journal store.

    The payload is a single opaque JSON document owned by this module, so it is
    persisted as-is rather than modelled as SQL columns.
    """

    _ROW_ID = "default"

    def __init__(self, database_url: str | None, *, connect_timeout_seconds: int = 10) -> None:
        self._database_url = str(database_url or "").strip() or None
        self._connect_timeout_seconds = max(1, int(connect_timeout_seconds or 10))
        self._schema_ready = False
        self._schema_lock = threading.Lock()

    def is_enabled(self) -> bool:
        return bool(self._database_url) and psycopg is not None

    def _connect(self):
        if not self._database_url:
            raise RuntimeError("DATABASE_URL is not configured")
        if psycopg is None:
            raise RuntimeError("psycopg is not installed")
        return psycopg.connect(
            self._database_url,
            autocommit=True,
            connect_timeout=self._connect_timeout_seconds,
        )

    def _ensure_schema(self, cursor) -> None:
        if not self.is_enabled() or self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS mf_portfolio_state (
                    id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    server_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            self._schema_ready = True

    def load_data(self) -> dict | None:
        if not self.is_enabled():
            return None
        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute("SELECT payload FROM mf_portfolio_state WHERE id = %s", (self._ROW_ID,))
            row = cursor.fetchone()
        if row is None:
            return None
        payload = row[0]
        return payload if isinstance(payload, dict) else json.loads(str(payload))

    def save_data(self, payload: dict) -> dict:
        if not self.is_enabled():
            return payload
        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute(
                """
                INSERT INTO mf_portfolio_state (id, payload, server_updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (id)
                DO UPDATE SET payload = EXCLUDED.payload, server_updated_at = NOW()
                """,
                (self._ROW_ID, json.dumps(payload)),
            )
        return payload


# ------------------------------------------------------------------ validation

def _clean_date(value: Any) -> str | None:
    text = str(value or "").strip()[:10]
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _clean_float(value: Any, *, minimum: float = 0.0) -> float | None:
    """`minimum=-inf` allows negatives, which realised P&L needs."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed if parsed >= minimum else None


def _clean_sip_plan(value: Any) -> dict[str, Any] | None:
    """The SIP still running on a fund: amount, frequency, next instalment.

    Forward-looking, and kept separate from the transaction history on purpose.
    Transactions are what happened; this is the standing instruction. Recording
    it means the portfolio can show what is committed each month and when the
    next debit lands, neither of which is derivable from past instalments alone.
    """
    if not isinstance(value, dict):
        return None
    amount = _clean_float(value.get("amount"), minimum=1.0)
    next_date = _clean_date(value.get("next_date"))
    if amount is None or next_date is None:
        return None
    frequency = str(value.get("frequency") or "monthly").strip().lower()
    if frequency not in SIP_FREQUENCIES:
        frequency = "monthly"
    per_year = {"weekly": 52, "fortnightly": 26, "monthly": 12, "quarterly": 4}[frequency]
    return {
        "amount": amount,
        "frequency": frequency,
        "next_date": next_date,
        "active": bool(value.get("active", True)),
        # Convenience for the UI, computed once here so every surface agrees.
        "annual_commitment": round(amount * per_year, 2),
        "monthly_equivalent": round(amount * per_year / 12, 2),
    }


def upcoming_instalments(plan: dict[str, Any] | None, *, count: int = 6) -> list[dict[str, Any]]:
    """The next few dates a SIP will debit, from its next_date onward."""
    if not plan or not plan.get("active"):
        return []
    start = _clean_date(plan.get("next_date"))
    amount = _clean_float(plan.get("amount"), minimum=0.01)
    if start is None or amount is None:
        return []
    frequency = plan.get("frequency", "monthly")
    cursor = date.fromisoformat(start)
    # Anchor on the *original* day, not the last emitted one. Stepping from the
    # emitted date makes a 31st mandate drift permanently to the 30th after it
    # passes through a 30-day month, and then to the 28th after February.
    anchor_day = cursor.day
    out: list[dict[str, Any]] = []
    for index in range(max(1, min(count, 24))):
        out.append({"date": cursor.isoformat(), "amount": amount})
        if frequency == "weekly":
            cursor += timedelta(days=7)
        elif frequency == "fortnightly":
            cursor += timedelta(days=14)
        else:
            step = 1 if frequency == "monthly" else 3
            month_index = cursor.month - 1 + step
            year = cursor.year + month_index // 12
            month = month_index % 12 + 1
            next_month_start = date(year + (month // 12), (month % 12) + 1, 1)
            days_in_month = (next_month_start - date(year, month, 1)).days
            cursor = date(year, month, min(anchor_day, days_in_month))
    return out


def _clean_realised_override(value: Any) -> dict[str, float] | None:
    if not isinstance(value, dict):
        return None
    pnl = _clean_float(value.get("realised_pnl"), minimum=float("-inf"))
    cost = _clean_float(value.get("cost_of_units_sold"), minimum=0.0)
    proceeds = _clean_float(value.get("realised_proceeds"), minimum=0.0)
    if pnl is None and cost is None and proceeds is None:
        return None
    return {
        "realised_pnl": pnl or 0.0,
        "cost_of_units_sold": cost or 0.0,
        "realised_proceeds": proceeds or 0.0,
    }


def normalise_payload(payload: dict | None) -> dict:
    """Coerce whatever the client sent into the canonical shape.

    PUT semantics: the incoming document is authoritative and a removed
    position is a deletion, not an absence to be merged back in — the same
    rule the watchlists store learned the hard way.
    """
    source = payload if isinstance(payload, dict) else {}
    raw_positions = source.get("positions")
    positions: list[dict[str, Any]] = []

    for entry in (raw_positions if isinstance(raw_positions, list) else [])[:MAX_POSITIONS]:
        if not isinstance(entry, dict):
            continue
        scheme_code = str(entry.get("scheme_code") or "").strip()
        if not scheme_code:
            continue

        transactions: list[dict[str, Any]] = []
        raw_transactions = entry.get("transactions")
        for item in (raw_transactions if isinstance(raw_transactions, list) else [])[:MAX_TRANSACTIONS_PER_POSITION]:
            if not isinstance(item, dict):
                continue
            when = _clean_date(item.get("date"))
            if when is None:
                continue
            kind = str(item.get("type") or "buy").strip().lower()
            if kind not in ("buy", "sell"):
                kind = "buy"
            amount = _clean_float(item.get("amount"), minimum=0.0)
            units = _clean_float(item.get("units"), minimum=0.0)
            nav = _clean_float(item.get("nav"), minimum=0.0)
            # A transaction needs either a rupee amount or a unit count; the
            # other side is filled from NAV history at valuation time.
            if not amount and not units:
                continue
            transaction = {
                "id": str(item.get("id") or uuid.uuid4()),
                "date": when,
                "type": kind,
                "amount": amount,
                "units": units,
                "nav": nav,
            }
            lot_cost_nav = _clean_float(item.get("lot_cost_nav"), minimum=0.0)
            if lot_cost_nav:
                transaction["lot_cost_nav"] = lot_cost_nav
            transactions.append(transaction)

        transactions.sort(key=lambda item: item["date"])
        positions.append({
            "id": str(entry.get("id") or uuid.uuid4()),
            "scheme_code": scheme_code,
            "notes": str(entry.get("notes") or "").strip()[:500] or None,
            # Set by a statement import: exact units and cost, no dated
            # cashflows, so XIRR is withheld rather than invented.
            "cost_basis_only": bool(entry.get("cost_basis_only")),
            # Realised figures straight from a broker statement. When present
            # these are authoritative and are not re-derived: the broker's own
            # lot matching (and any load or STT it netted) is the real answer,
            # and weighted-average would blend a closed lot's cost into the
            # open lot and make both disagree with the statement.
            "realised_override": _clean_realised_override(entry.get("realised_override")),
            # The standing SIP instruction, if there is one.
            "sip_plan": _clean_sip_plan(entry.get("sip_plan")),
            "transactions": transactions,
        })

    return {
        "updated_at": str(source.get("updated_at") or datetime.now().astimezone().isoformat()),
        "positions": positions,
    }


SIP_FREQUENCIES = ("weekly", "fortnightly", "monthly", "quarterly")


def expand_sip(
    *,
    start_date: str,
    end_date: str,
    amount: float,
    frequency: str = "monthly",
    day_of_month: int | None = None,
    weekday: int | None = None,
) -> list[dict[str, Any]]:
    """Turn a recurring SIP description into individual transactions.

    Saves entering 150-odd rows by hand for a multi-year weekly SIP, and gives
    XIRR the dated cashflows it needs — a weekly SIP valued as one lump sum at
    average cost is not a return.

    * ``weekly`` / ``fortnightly`` step in fixed 7- or 14-day intervals from the
      first occurrence of ``weekday`` (defaulting to the start date's own
      weekday), which is how a real weekly SIP mandate behaves.
    * ``monthly`` / ``quarterly`` land on ``day_of_month``, clamped to the last
      day of short months so a 31st SIP does not silently skip February.
    """
    first = _clean_date(start_date)
    last = _clean_date(end_date)
    installment = _clean_float(amount, minimum=0.01)
    if first is None or last is None or installment is None:
        return []
    begin = date.fromisoformat(first)
    finish = date.fromisoformat(last)
    if finish < begin:
        return []

    normalised = str(frequency or "monthly").strip().lower()
    if normalised not in SIP_FREQUENCIES:
        normalised = "monthly"

    def transaction(when: date) -> dict[str, Any]:
        return {
            "id": str(uuid.uuid4()),
            "date": when.isoformat(),
            "type": "buy",
            "amount": installment,
            "units": None,
            "nav": None,
        }

    out: list[dict[str, Any]] = []

    if normalised in ("weekly", "fortnightly"):
        step = 7 if normalised == "weekly" else 14
        cursor = begin
        if weekday is not None:
            target = min(max(int(weekday), 0), 6)
            # Advance to the first occurrence of the requested weekday.
            cursor += timedelta(days=(target - begin.weekday()) % 7)
        while cursor <= finish and len(out) < MAX_TRANSACTIONS_PER_POSITION:
            out.append(transaction(cursor))
            cursor += timedelta(days=step)
        return out

    step_months = 1 if normalised == "monthly" else 3
    target_day = min(max(int(day_of_month or begin.day), 1), 31)
    year, month = begin.year, begin.month
    while len(out) < MAX_TRANSACTIONS_PER_POSITION:
        month_start = date(year, month, 1)
        next_month = date(year + (month // 12), (month % 12) + 1, 1)
        days_in_month = (next_month - month_start).days
        when = date(year, month, min(target_day, days_in_month))
        if when > finish:
            break
        if when >= begin:
            out.append(transaction(when))
        advanced = month - 1 + step_months
        year, month = year + advanced // 12, advanced % 12 + 1
    return out


def opening_position(*, units: float, as_of: str) -> list[dict[str, Any]]:
    """A starting holding: units already owned as of a date.

    For someone who has been investing for years and does not want to type
    every past instalment. The units are priced at that date's NAV, so the
    position enters at its then market value — which makes returns and XIRR
    measure performance *since that date*, not since the original purchases.
    The UI says so, because the distinction changes what the XIRR means.
    """
    held = _clean_float(units, minimum=1e-6)
    when = _clean_date(as_of)
    if held is None or when is None:
        return []
    return [{
        "id": str(uuid.uuid4()),
        "date": when,
        "type": "buy",
        "amount": None,
        "units": held,
        "nav": None,
    }]


# ------------------------------------------------------------------- valuation

def nav_on_or_before(dates: list[str], navs: list[float], target: str) -> tuple[str, float] | None:
    """The applicable NAV for a transaction date.

    A purchase dated on a market holiday is allotted at the next available
    NAV in reality, but the difference is a day and using the prior close
    keeps the function total. A tolerance stops a typo'd 2019 date on a
    2023-launched fund from silently valuing at the launch NAV.
    """
    position = bisect_right(dates, target) - 1
    if position < 0:
        # Transaction predates the series — use the earliest NAV we have, but
        # only if it is close enough to be plausibly the same event.
        if dates and (date.fromisoformat(dates[0]) - date.fromisoformat(target)).days <= 10:
            return dates[0], navs[0]
        return None
    if (date.fromisoformat(target) - date.fromisoformat(dates[position])).days > 30:
        return None
    return dates[position], navs[position]


def value_position(
    position: dict[str, Any],
    *,
    nav_series: dict[str, Any] | None,
) -> dict[str, Any]:
    """Units, cost, value and P&L for one held fund.

    Realised and unrealised P&L are tracked separately, the way a broker
    statement reports them, because they answer different questions: what the
    open position has done, versus what selling actually banked.

    Cost accounting is weighted-average, which is what Indian AMCs and CAS
    statements use. On a sale the *cost* of the sold units leaves `invested`
    and the difference between proceeds and that cost becomes realised P&L —
    an earlier version subtracted the sale *proceeds* from invested, which
    drove `invested` negative on a fully-exited position and made its P&L
    nonsense.
    """
    transactions = position.get("transactions") or []
    result: dict[str, Any] = {
        "id": position.get("id"),
        "scheme_code": position.get("scheme_code"),
        "notes": position.get("notes"),
        "transaction_count": len(transactions),
        "units": None,
        "invested": None,
        "current_value": None,
        "unrealised_pnl": None,
        "unrealised_pct": None,
        "realised_pnl": None,
        "realised_proceeds": None,
        "cost_of_units_sold": None,
        "gain": None,
        "gain_pct": None,
        "xirr": None,
        "avg_cost_nav": None,
        "latest_nav": None,
        "latest_nav_date": None,
        "first_transaction_date": transactions[0]["date"] if transactions else None,
        "sip_plan": position.get("sip_plan"),
        "upcoming_instalments": upcoming_instalments(position.get("sip_plan")),
        "unpriced_transactions": 0,
        "is_closed": False,
        # True when the position came from a statement import: units and cost
        # are exact but there are no dated cashflows, so XIRR cannot be had.
        "cost_basis_only": bool(position.get("cost_basis_only")),
    }
    if not transactions:
        # A fund can be tracked for its standing SIP before any instalment has
        # been recorded; that is a valid position, not an empty one.
        return result

    if not nav_series or not nav_series.get("dates"):
        invested = sum(t["amount"] or 0 for t in transactions if t["type"] == "buy")
        result["invested"] = round(invested, 2) if invested else None
        return result

    dates: list[str] = nav_series["dates"]
    navs: list[float] = nav_series["navs"]
    latest_nav = navs[-1]

    units = 0.0
    invested = 0.0
    realised_pnl = 0.0
    realised_proceeds = 0.0
    cost_of_sold = 0.0
    cashflows: list[tuple[date, float]] = []
    unpriced = 0

    # With an override in play the sell legs exist only to remove units; their
    # cost and proceeds come from the statement, not from our averaging.
    override = position.get("realised_override") or None

    for transaction in transactions:
        priced = nav_on_or_before(dates, navs, transaction["date"])
        nav = transaction["nav"] or (priced[1] if priced else None)
        if nav is None or nav <= 0:
            unpriced += 1
            continue

        amount = transaction["amount"]
        unit_count = transaction["units"]
        if unit_count is None and amount is not None:
            unit_count = amount / nav
        elif amount is None and unit_count is not None:
            amount = unit_count * nav
        if unit_count is None or amount is None:
            unpriced += 1
            continue

        when = date.fromisoformat(transaction["date"])
        if transaction["type"] == "buy":
            units += unit_count
            invested += amount
            cashflows.append((when, -amount))
        else:
            # Never let a mistyped redemption drive units negative.
            sold = min(unit_count, units)
            if sold <= 0:
                continue
            proceeds = sold * nav
            lot_cost_nav = transaction.get("lot_cost_nav")
            if lot_cost_nav:
                # A statement-imported closed lot. Remove exactly what its own
                # buy leg put in, so a same-scheme open lot keeps the cost basis
                # the statement reports for it instead of being averaged into.
                cost = sold * lot_cost_nav
            else:
                average_cost = invested / units if units > 0 else nav
                cost = sold * average_cost
            units -= sold
            invested -= cost
            cost_of_sold += cost
            realised_proceeds += proceeds
            realised_pnl += proceeds - cost
            cashflows.append((when, proceeds))

    current_value = units * latest_nav
    if units > 0:
        cashflows.append((date.fromisoformat(dates[-1]), current_value))

    if override is not None:
        realised_pnl = override["realised_pnl"]
        cost_of_sold = override["cost_of_units_sold"]
        realised_proceeds = override["realised_proceeds"]

    unrealised = current_value - invested if units > 0 else 0.0
    total_pnl = unrealised + realised_pnl
    # Return on all the capital that was ever put to work in this fund.
    deployed = invested + cost_of_sold

    result.update({
        "units": round(units, 4),
        "invested": round(invested, 2) if invested else (0.0 if units == 0 else None),
        "current_value": round(current_value, 2),
        "unrealised_pnl": round(unrealised, 2) if units > 0 else 0.0,
        "unrealised_pct": round(unrealised / invested * 100.0, 2) if invested > 0 else None,
        "realised_pnl": round(realised_pnl, 2) if cost_of_sold else None,
        "realised_proceeds": round(realised_proceeds, 2) if realised_proceeds else None,
        "cost_of_units_sold": round(cost_of_sold, 2) if cost_of_sold else None,
        "gain": round(total_pnl, 2),
        "gain_pct": round(total_pnl / deployed * 100.0, 2) if deployed > 0 else None,
        "avg_cost_nav": round(invested / units, 4) if units > 0 and invested > 0 else None,
        "latest_nav": latest_nav,
        "latest_nav_date": dates[-1],
        "unpriced_transactions": unpriced,
        "is_closed": units <= 1e-6 and cost_of_sold > 0,
    })

    if not result["cost_basis_only"]:
        computed_xirr = metrics.xirr(cashflows)
        # An XIRR outside a sane band means the cashflow set is degenerate (a
        # same-week buy and valuation, say) — report nothing rather than "830%".
        if computed_xirr is not None and -95.0 < computed_xirr < 300.0:
            first = min(when for when, _ in cashflows)
            if (date.fromisoformat(dates[-1]) - first).days >= 90:
                result["xirr"] = round(computed_xirr, 2)
    return result
