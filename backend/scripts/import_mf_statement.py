#!/usr/bin/env python3
"""Load a broker mutual-fund P&L statement into the portfolio.

Built for the Zerodha/Console-style "P&L Statement for Mutual Funds" export,
which carries, per scheme: units sold with their buy and sell value, and units
still held with their cost and last NAV.

    cd backend
    python scripts/import_mf_statement.py ~/Downloads/pnl-XXXX.xlsx
    python scripts/import_mf_statement.py <file> --dry-run     # parse only
    python scripts/import_mf_statement.py <file> --replace     # discard existing

**What the statement does and does not give.** Units and cost basis are exact,
so invested, current value and P&L are exact. There are no transaction dates,
so XIRR cannot be computed — positions are flagged `cost_basis_only` and the
UI shows a dash rather than a fabricated number. Add the actual SIP dates for
a fund and it starts reporting XIRR for that fund.

Schemes are matched to the universe on ISIN first, then on a normalised name,
because a statement's ISIN is often the plan variant the universe does not
happen to store. Two statement rows for the same fund (a held lot and a sold
lot under different ISINs) merge into one position.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import get_settings
from app.services.mutual_funds import nav_source, paths

# A statement NAV more than this far from the resolved scheme's own NAV means
# the wrong scheme was matched. Real drift is a few days of movement; a plan
# variant mismatch shows up as tens of percent.
NAV_DRIFT_TOLERANCE_PCT = 8.0

HEADER_MARKER = "Symbol"
_FILLERS = re.compile(
    r"\b(direct|regular|plan|growth|idcw|payout|reinvest|reinvestment|option|fund|scheme|the)\b",
    re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalise(name: str | None) -> str:
    text = _FILLERS.sub(" ", str(name or "").lower())
    return " ".join(_NON_ALNUM.sub(" ", text).split())


def read_statement(path: Path) -> tuple[list[dict], dict]:
    """Rows plus the statement's own summary, for cross-checking our maths."""
    try:
        from python_calamine import CalamineWorkbook
    except ImportError:  # pragma: no cover
        raise SystemExit("python-calamine is required: pip install python-calamine")

    workbook = CalamineWorkbook.from_path(str(path))
    sheet = next(
        (name for name in workbook.sheet_names if "mutual" in name.lower()),
        workbook.sheet_names[0],
    )
    grid = workbook.get_sheet_by_name(sheet).to_python()

    header_index = next(
        (i for i, row in enumerate(grid) if row and str(row[0]).strip() == HEADER_MARKER),
        None,
    )
    if header_index is None:
        raise SystemExit(f"could not find a '{HEADER_MARKER}' header row in {path.name}")

    header = [str(cell).strip() for cell in grid[header_index]]

    def number(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    summary: dict[str, float] = {}
    for row in grid[:header_index]:
        if not row or not str(row[0]).strip():
            continue
        label = str(row[0]).strip().lower()
        if label in ("realized p&l", "unrealized p&l", "charges", "other credit & debit"):
            summary[label] = number(row[1] if len(row) > 1 else 0)

    rows: list[dict] = []
    for raw in grid[header_index + 1:]:
        if not raw or not str(raw[0]).strip():
            continue
        record = dict(zip(header, raw))
        rows.append({
            "symbol": str(record.get("Symbol") or "").strip(),
            "isin": str(record.get("ISIN") or "").strip(),
            "sold_units": number(record.get("Quantity")),
            "buy_value": number(record.get("Buy Value")),
            "sell_value": number(record.get("Sell Value")),
            "realised": number(record.get("Realized P&L")),
            "last_nav": number(record.get("Previous Closing Price")),
            "open_units": number(record.get("Open Quantity")),
            "open_cost": number(record.get("Open Value")),
            "unrealised": number(record.get("Unrealized P&L")),
        })
    return rows, summary


def build_index(universe: dict) -> tuple[dict, dict]:
    by_isin: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for fund in universe.get("funds") or []:
        isin = str(fund.get("isin") or "").strip()
        if isin:
            by_isin.setdefault(isin, fund)
        key = normalise(fund.get("name"))
        if key:
            by_name.setdefault(key, fund)
    return by_isin, by_name


def resolve(
    row: dict,
    by_isin: dict,
    by_name: dict,
    amfi_by_isin: dict,
) -> tuple[str | None, dict | None, str]:
    """Statement row -> (scheme code, universe row if any, how it was matched).

    ISIN via the AMFI index comes first and wins, because it identifies the
    exact plan and option. Name matching is the fallback and is dangerous on
    its own: several holdings here are IDCW or Payout variants, and matching
    those by name lands on the Direct/Growth sibling, whose NAV differs by up
    to 40% — a wrong valuation presented as a real one.
    """
    amfi = amfi_by_isin.get(row["isin"])
    if amfi is not None:
        code = str(amfi.get("schemeCode"))
        return code, by_isin.get(row["isin"]) or None, "isin"

    fund = by_isin.get(row["isin"])
    if fund is not None:
        return str(fund["scheme_code"]), fund, "isin"

    key = normalise(row["symbol"])
    fund = by_name.get(key)
    if fund is None:
        for candidate_key, candidate in by_name.items():
            if candidate_key and (candidate_key in key or key in candidate_key):
                fund = candidate
                break
    if fund is None:
        return None, None, "unmatched"
    return str(fund["scheme_code"]), fund, "name"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("statement", type=Path)
    parser.add_argument("--as-of", default=None, help="date to stamp the imported lots (default: universe as_of)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true", help="discard existing positions instead of merging")
    args = parser.parse_args()

    if not args.statement.exists():
        raise SystemExit(f"no such file: {args.statement}")

    universe = json.loads(paths.UNIVERSE_PATH.read_text())
    by_isin, by_name = build_index(universe)
    as_of = args.as_of or universe.get("as_of")
    if not as_of:
        raise SystemExit("universe has no as_of date; pass --as-of YYYY-MM-DD")

    rows, summary = read_statement(args.statement)
    print(f"read {len(rows)} scheme rows from {args.statement.name}")

    print("resolving ISINs against the AMFI scheme index…")
    try:
        amfi_by_isin = nav_source.build_isin_index()
    except nav_source.NavUnavailable as exc:
        print(f"  AMFI index unavailable ({exc}); falling back to name matching")
        amfi_by_isin = {}
    else:
        print(f"  {len(amfi_by_isin)} ISINs indexed")

    by_code = {str(f["scheme_code"]): f for f in (universe.get("funds") or [])}

    # Merge by scheme code: a fund can appear twice (a held lot and a sold lot
    # filed under different plan ISINs).
    merged: dict[str, dict] = {}
    unmatched: list[tuple[str, str]] = []
    for row in rows:
        code, fund, how = resolve(row, by_isin, by_name, amfi_by_isin)
        if code is None:
            unmatched.append((row["symbol"], "no ISIN or name match"))
            continue

        # Cross-check the resolved scheme's NAV against the statement's own
        # closing price. This is what catches a plan-variant mismatch.
        universe_row = fund or by_code.get(code)
        reference_nav = (universe_row or {}).get("nav_latest") or (universe_row or {}).get("nav")
        if how == "name" and row["last_nav"] and reference_nav:
            drift = abs(reference_nav - row["last_nav"]) / row["last_nav"] * 100.0
            if drift > NAV_DRIFT_TOLERANCE_PCT:
                unmatched.append((
                    row["symbol"],
                    f"name match rejected: NAV {row['last_nav']:.2f} vs {reference_nav:.2f} "
                    f"({drift:.0f}% apart) — likely a different plan variant",
                ))
                continue

        # Never merge two statement rows into one position: different ISINs are
        # different schemes with different NAVs, and blending their cost bases
        # corrupts both.
        entry = merged.setdefault(code, {
            "fund": universe_row, "how": how, "open_units": 0.0, "open_cost": 0.0,
            "sold_units": 0.0, "buy_value": 0.0, "sell_value": 0.0,
            "realised": 0.0, "unrealised": 0.0, "sources": [],
            "statement_nav": row["last_nav"] or None,
            "name": (universe_row or {}).get("name") or row["symbol"].title(),
        })
        for key in ("open_units", "open_cost", "sold_units", "buy_value",
                    "sell_value", "realised", "unrealised"):
            entry[key] += row[key]
        entry["sources"].append(row["symbol"])

    positions: list[dict] = []
    total_open_cost = total_unrealised = total_realised = 0.0

    for code, entry in merged.items():
        transactions: list[dict] = []

        if entry["open_units"] > 0 and entry["open_cost"] > 0:
            # Price the lot at its own average cost, so invested matches the
            # statement exactly rather than being re-derived from a NAV lookup.
            transactions.append({
                "id": str(uuid.uuid4()),
                "date": as_of,
                "type": "buy",
                "amount": None,
                "units": round(entry["open_units"], 4),
                "nav": round(entry["open_cost"] / entry["open_units"], 6),
            })
            total_open_cost += entry["open_cost"]
            total_unrealised += entry["unrealised"]

        if entry["sold_units"] > 0 and entry["buy_value"] > 0:
            # A round trip: bought at the statement's average buy price, sold
            # at its average sell price. Reproduces realised P&L exactly.
            transactions.append({
                "id": str(uuid.uuid4()),
                "date": as_of,
                "type": "buy",
                "amount": None,
                "units": round(entry["sold_units"], 4),
                "nav": round(entry["buy_value"] / entry["sold_units"], 6),
            })
            transactions.append({
                "id": str(uuid.uuid4()),
                "date": as_of,
                "type": "sell",
                "amount": None,
                "units": round(entry["sold_units"], 4),
                "nav": round(entry["sell_value"] / entry["sold_units"], 6),
                # Retire this lot at its own buy price so a same-scheme open
                # lot keeps the cost basis the statement reports for it.
                "lot_cost_nav": round(entry["buy_value"] / entry["sold_units"], 6),
            })
            total_realised += entry["realised"]

        if not transactions:
            continue

        position: dict = {
            "id": str(uuid.uuid4()),
            "scheme_code": code,
            "cost_basis_only": True,
            "notes": f"Imported from {args.statement.name}",
            "transactions": transactions,
        }
        if entry["sold_units"] > 0 and entry["buy_value"] > 0:
            # The broker's realised figure is authoritative — it reflects its
            # own lot matching and anything it netted off.
            position["realised_override"] = {
                "realised_pnl": round(entry["realised"], 2),
                "cost_of_units_sold": round(entry["buy_value"], 2),
                "realised_proceeds": round(entry["sell_value"], 2),
            }
        positions.append(position)
        name = entry["name"]
        held = f"{entry['open_units']:.3f} units @ cost {entry['open_cost']:,.0f}" if entry["open_units"] else "—"
        sold = f"realised {entry['realised']:+,.0f}" if entry["sold_units"] else "—"
        print(f"  [{entry['how']:5s}] {name[:40]:40s} held: {held:38s} sold: {sold}")

    if unmatched:
        print(f"\n  {len(unmatched)} row(s) skipped:")
        for name, why in unmatched:
            print(f"    - {name[:44]}\n        {why}")

    # Cross-check against the statement's own summary before writing anything.
    print("\ncross-check against the statement summary:")
    for label, ours in (("unrealized p&l", total_unrealised), ("realized p&l", total_realised)):
        theirs = summary.get(label)
        if theirs is None:
            continue
        ok = abs(ours - theirs) < 1.0
        print(f"  {'OK ' if ok else 'MISMATCH'} {label}: statement {theirs:,.2f} vs imported {ours:,.2f}")
    print(f"  open cost basis: {total_open_cost:,.2f}")

    if args.dry_run:
        print("\ndry run — nothing written")
        return 0

    settings = get_settings()
    target = Path(settings.app_state_dir) / "data" / "mf_portfolio.json"
    existing: list[dict] = []
    if not args.replace:
        try:
            payload = json.loads(target.read_text())
            existing = payload.get("positions") or []
        except (OSError, ValueError):
            existing = []
        imported_codes = {p["scheme_code"] for p in positions}
        kept = [p for p in existing if str(p.get("scheme_code")) not in imported_codes]
        if len(kept) != len(existing):
            print(f"\nreplacing {len(existing) - len(kept)} existing position(s) that the statement also covers")
        existing = kept

    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps({"updated_at": None, "positions": existing + positions}, indent=2))
    temp.replace(target)
    print(f"\nwrote {len(positions)} position(s) ({len(existing)} kept) -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
