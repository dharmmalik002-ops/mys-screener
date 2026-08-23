"""Turn a broker mutual-fund P&L statement into portfolio positions.

Written for the Zerodha/Console-style "P&L Statement for Mutual Funds" export,
which carries, per scheme: units sold with their buy and sell value, and units
still held with their cost and last NAV.

This lives in the service layer rather than in a script because the import has
to be reachable from the running app. A statement parsed on a developer's
laptop writes to that laptop's `APP_STATE_DIR` and never reaches the deployed
Space — which is exactly how a full portfolio import went missing.

**What a statement does and does not give.** Units and cost basis are exact,
so invested, current value and P&L are exact. There are no transaction dates,
so XIRR cannot be computed; positions are flagged `cost_basis_only` and the UI
shows a dash rather than a fabricated number.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

HEADER_MARKER = "Symbol"

# A statement NAV further than this from the resolved scheme's own NAV means the
# wrong scheme matched. Real drift is a few days of movement; a plan-variant
# mismatch shows up as tens of percent.
NAV_DRIFT_TOLERANCE_PCT = 8.0

_FILLERS = re.compile(
    r"\b(direct|regular|plan|growth|idcw|payout|reinvest|reinvestment|option|fund|scheme|the)\b",
    re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class StatementError(ValueError):
    """The file is not a statement we can read."""


def normalise(name: str | None) -> str:
    text = _FILLERS.sub(" ", str(name or "").lower().replace("&", " and "))
    return " ".join(_NON_ALNUM.sub(" ", text).split())


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_statement(data: bytes) -> tuple[list[dict], dict]:
    """Scheme rows plus the statement's own summary, for cross-checking."""
    try:
        from python_calamine import CalamineWorkbook
    except ImportError as exc:  # pragma: no cover
        raise StatementError("python-calamine is not installed on the server") from exc

    import io

    try:
        workbook = CalamineWorkbook.from_filelike(io.BytesIO(data))
    except Exception as exc:
        raise StatementError(f"could not open the workbook ({type(exc).__name__})") from exc

    sheet = next(
        (name for name in workbook.sheet_names if "mutual" in name.lower()),
        workbook.sheet_names[0] if workbook.sheet_names else None,
    )
    if sheet is None:
        raise StatementError("the workbook has no sheets")
    grid = workbook.get_sheet_by_name(sheet).to_python()

    header_index = next(
        (i for i, row in enumerate(grid) if row and str(row[0]).strip() == HEADER_MARKER),
        None,
    )
    if header_index is None:
        raise StatementError(
            f"no '{HEADER_MARKER}' header row found — this does not look like a "
            "mutual fund P&L statement"
        )

    header = [str(cell).strip() for cell in grid[header_index]]

    summary: dict[str, float] = {}
    for row in grid[:header_index]:
        if not row or not str(row[0]).strip():
            continue
        label = str(row[0]).strip().lower()
        if label in ("realized p&l", "unrealized p&l", "charges", "other credit & debit"):
            summary[label] = _number(row[1] if len(row) > 1 else 0)

    rows: list[dict] = []
    for raw in grid[header_index + 1:]:
        if not raw or not str(raw[0]).strip():
            continue
        record = dict(zip(header, raw))
        rows.append({
            "symbol": str(record.get("Symbol") or "").strip(),
            "isin": str(record.get("ISIN") or "").strip(),
            "sold_units": _number(record.get("Quantity")),
            "buy_value": _number(record.get("Buy Value")),
            "sell_value": _number(record.get("Sell Value")),
            "realised": _number(record.get("Realized P&L")),
            "last_nav": _number(record.get("Previous Closing Price")),
            "open_units": _number(record.get("Open Quantity")),
            "open_cost": _number(record.get("Open Value")),
            "unrealised": _number(record.get("Unrealized P&L")),
        })
    if not rows:
        raise StatementError("the statement has a header but no scheme rows")
    return rows, summary


def resolve_row(
    row: dict,
    *,
    universe_by_isin: dict[str, dict],
    universe_by_name: dict[str, dict],
    amfi_by_isin: dict[str, dict],
) -> tuple[str | None, dict | None, str]:
    """Statement row -> (scheme code, universe row if any, how it matched).

    ISIN through the AMFI index wins, because it identifies the exact plan and
    option. Name matching alone is dangerous: a statement's holding is often an
    IDCW or Payout plan, and matching that by name lands on the Direct/Growth
    sibling whose NAV can differ by 40% — a wrong valuation shown as fact.
    """
    amfi = amfi_by_isin.get(row["isin"])
    if amfi is not None:
        return str(amfi.get("schemeCode")), universe_by_isin.get(row["isin"]), "isin"

    fund = universe_by_isin.get(row["isin"])
    if fund is not None:
        return str(fund["scheme_code"]), fund, "isin"

    key = normalise(row["symbol"])
    fund = universe_by_name.get(key)
    if fund is None:
        for candidate_key, candidate in universe_by_name.items():
            if candidate_key and (candidate_key in key or key in candidate_key):
                fund = candidate
                break
    if fund is None:
        return None, None, "unmatched"
    return str(fund["scheme_code"]), fund, "name"


def build_positions(
    rows: list[dict],
    *,
    universe: dict,
    amfi_by_isin: dict[str, dict],
    as_of: str,
    source_label: str = "statement",
) -> dict[str, Any]:
    """Positions, plus a per-row report of what happened and why."""
    by_isin: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    by_code: dict[str, dict] = {}
    for fund in universe.get("funds") or []:
        code = str(fund.get("scheme_code"))
        by_code[code] = fund
        isin = str(fund.get("isin") or "").strip()
        if isin:
            by_isin.setdefault(isin, fund)
        key = normalise(fund.get("name"))
        if key:
            by_name.setdefault(key, fund)

    merged: dict[str, dict] = {}
    skipped: list[dict[str, str]] = []
    matched: list[dict[str, Any]] = []

    for row in rows:
        code, fund, how = resolve_row(
            row,
            universe_by_isin=by_isin,
            universe_by_name=by_name,
            amfi_by_isin=amfi_by_isin,
        )
        if code is None:
            skipped.append({"symbol": row["symbol"], "reason": "no ISIN or name match"})
            continue

        universe_row = fund or by_code.get(code)
        reference_nav = (universe_row or {}).get("nav_latest") or (universe_row or {}).get("nav")
        if how == "name" and row["last_nav"] and reference_nav:
            drift = abs(reference_nav - row["last_nav"]) / row["last_nav"] * 100.0
            if drift > NAV_DRIFT_TOLERANCE_PCT:
                skipped.append({
                    "symbol": row["symbol"],
                    "reason": (
                        f"name match rejected: statement NAV {row['last_nav']:.2f} vs "
                        f"{reference_nav:.2f} ({drift:.0f}% apart) — likely a different plan"
                    ),
                })
                continue

        # Never merge two statement rows into one position: different ISINs are
        # different schemes with different NAVs, and blending their cost bases
        # corrupts both.
        entry = merged.setdefault(code, {
            "open_units": 0.0, "open_cost": 0.0, "sold_units": 0.0, "buy_value": 0.0,
            "sell_value": 0.0, "realised": 0.0, "unrealised": 0.0,
            "how": how,
            "name": (universe_row or {}).get("name") or row["symbol"].title(),
        })
        for key in ("open_units", "open_cost", "sold_units", "buy_value",
                    "sell_value", "realised", "unrealised"):
            entry[key] += row[key]

    positions: list[dict] = []
    totals = {"open_cost": 0.0, "unrealised": 0.0, "realised": 0.0}

    for code, entry in merged.items():
        transactions: list[dict] = []

        if entry["open_units"] > 0 and entry["open_cost"] > 0:
            # Price the lot at its own average cost so invested matches the
            # statement exactly rather than being re-derived from a NAV lookup.
            transactions.append({
                "id": str(uuid.uuid4()),
                "date": as_of,
                "type": "buy",
                "amount": None,
                "units": round(entry["open_units"], 4),
                "nav": round(entry["open_cost"] / entry["open_units"], 6),
            })
            totals["open_cost"] += entry["open_cost"]
            totals["unrealised"] += entry["unrealised"]

        if entry["sold_units"] > 0 and entry["buy_value"] > 0:
            buy_nav = entry["buy_value"] / entry["sold_units"]
            transactions.append({
                "id": str(uuid.uuid4()),
                "date": as_of,
                "type": "buy",
                "amount": None,
                "units": round(entry["sold_units"], 4),
                "nav": round(buy_nav, 6),
            })
            transactions.append({
                "id": str(uuid.uuid4()),
                "date": as_of,
                "type": "sell",
                "amount": None,
                "units": round(entry["sold_units"], 4),
                "nav": round(entry["sell_value"] / entry["sold_units"], 6),
                # Retire this lot at its own buy price so a same-scheme open lot
                # keeps the cost basis the statement reports for it.
                "lot_cost_nav": round(buy_nav, 6),
            })
            totals["realised"] += entry["realised"]

        if not transactions:
            continue

        position: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "scheme_code": code,
            "cost_basis_only": True,
            "notes": f"Imported from {source_label}",
            "transactions": transactions,
        }
        if entry["sold_units"] > 0 and entry["buy_value"] > 0:
            # The broker's realised figure is authoritative: it reflects its own
            # lot matching and anything it netted off.
            position["realised_override"] = {
                "realised_pnl": round(entry["realised"], 2),
                "cost_of_units_sold": round(entry["buy_value"], 2),
                "realised_proceeds": round(entry["sell_value"], 2),
            }
        positions.append(position)
        matched.append({
            "scheme_code": code,
            "name": entry["name"],
            "matched_by": entry["how"],
            "units": round(entry["open_units"], 4) or None,
            "cost": round(entry["open_cost"], 2) or None,
            "realised": round(entry["realised"], 2) or None,
        })

    return {
        "positions": positions,
        "matched": matched,
        "skipped": skipped,
        "totals": {key: round(value, 2) for key, value in totals.items()},
    }


def reconcile(totals: dict[str, float], summary: dict[str, float]) -> list[dict[str, Any]]:
    """Compare what we built against the statement's own summary block."""
    checks = []
    for label, key in (("unrealized p&l", "unrealised"), ("realized p&l", "realised")):
        stated = summary.get(label)
        if stated is None:
            continue
        ours = totals.get(key, 0.0)
        checks.append({
            "label": label.replace("p&l", "P&L").title(),
            "statement": round(stated, 2),
            "imported": round(ours, 2),
            "agrees": abs(ours - stated) < 1.0,
        })
    return checks
