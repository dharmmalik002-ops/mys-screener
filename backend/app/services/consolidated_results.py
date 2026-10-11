"""Consolidated quarterly results, from NSE's Integrated Filing XBRL.

BSE's structured results (`bse_quarterly`) are standalone only. Groups with
large subsidiaries read very differently consolidated — TCS's standalone sales
are ~83% of its consolidated — and most results trackers lead with the
consolidated figures, so the Results page offers both.

Source: since January 2025 every listed company files its results as a SEBI
"Integrated Filing (Financials)". NSE lists them at
``/api/integrated-filing-results`` (paged; each row carries ``consolidated``,
``qe_Date``, ``creation_Date`` and an ``xbrl`` link to a plain XBRL 2.1
instance on nsearchives — the ``ixbrl`` field is an HTML rendering, never read).
The instance uses SEBI's ``in-capmkt`` taxonomy; element names below are
checked against SEBI's own 2025-01-31 taxonomy schema, not guessed.

Consolidated history therefore starts with the quarters filed in 2025, so a
consolidated YoY needs the quarter a year earlier to have been an integrated
filing too: true from the December 2025 quarter onward. Where a consolidated
figure cannot be compared, it is None, never filled from standalone.

`scripts/update_consolidated_results.py` builds the committed
`data/consolidated_results.json`; this module parses instances and serves rows
in the same shape as `bse_quarterly.results_for`.
"""

from __future__ import annotations

import calendar
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.services import bse_quarterly

ARTIFACT_PATH = bse_quarterly.ARTIFACT_PATH.with_name("consolidated_results.json")
QUARTERS_KEPT = 12
RUPEES_PER_CRORE = 1e7

_XBRLI = "{http://www.xbrl.org/2003/instance}"

# First present wins. Names from SEBI's in-capmkt taxonomy (2025-01-31).
REVENUE = ("RevenueFromOperations", "InterestEarned")
EXPENSES = ("Expenses",)
DEPRECIATION = ("DepreciationDepletionAndAmortisationExpense",)
FINANCE = ("FinanceCosts",)
PROFIT_BEFORE_TAX = ("ProfitBeforeTax", "ProfitLossBeforeTax")
# Owners of the parent: what EPS and PE are measured on. A consolidated
# profit including minority interests would overstate a group with partly
# owned subsidiaries.
NET_PROFIT = ("ProfitOrLossAttributableToOwnersOfParent", "ProfitLossForPeriod")
EPS = (
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "BasicEarningsLossPerShareFromContinuingOperations",
)
NATURE = "NatureOfReportStandaloneConsolidated"


def _local(tag: str) -> tuple[str | None, str]:
    if tag.startswith("{"):
        uri, _, name = tag[1:].partition("}")
        return uri, name
    return None, tag


def _parse_day(text: str | None) -> date | None:
    try:
        return date.fromisoformat(str(text).strip()[:10])
    except (TypeError, ValueError):
        return None


def period_label(end: date) -> str:
    return f"{calendar.month_abbr[end.month]} {end.year}"


def parse_instance(xml_bytes: bytes, expect_end: date | None = None) -> dict[str, Any] | None:
    """One Integrated Filing XBRL instance -> a quarter row (money in crore),
    or None when it is not a consolidated quarter with a revenue line.

    The quarter is the context with no dimensions whose period runs ~3 months
    and ends latest (or exactly on ``expect_end``) — filings also carry the
    year-to-date, the comparative quarters and segment contexts, and reading
    the wrong one silently doubles or shifts every figure.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None
    if root.tag != f"{_XBRLI}xbrl":
        return None

    quarters: dict[str, tuple[date, date]] = {}
    for ctx in root.findall(f"{_XBRLI}context"):
        ctx_id = ctx.attrib.get("id")
        entity = ctx.find(f"{_XBRLI}entity")
        has_dims = ctx.find(f"{_XBRLI}scenario") is not None or (
            entity is not None and entity.find(f"{_XBRLI}segment") is not None
        )
        period = ctx.find(f"{_XBRLI}period")
        if not ctx_id or has_dims or period is None:
            continue
        start = _parse_day(getattr(period.find(f"{_XBRLI}startDate"), "text", None))
        end = _parse_day(getattr(period.find(f"{_XBRLI}endDate"), "text", None))
        if start and end and 60 <= (end - start).days <= 100:
            quarters[ctx_id] = (start, end)
    if not quarters:
        return None
    if expect_end is not None and any(end == expect_end for _s, end in quarters.values()):
        chosen = {cid for cid, (_s, end) in quarters.items() if end == expect_end}
    else:
        latest = max(end for _s, end in quarters.values())
        chosen = {cid for cid, (_s, end) in quarters.items() if end == latest}
    quarter_end = quarters[next(iter(chosen))][1]

    values: dict[str, float] = {}
    nature = None
    for el in root:
        if not isinstance(el.tag, str):
            continue
        _uri, name = _local(el.tag)
        text = (el.text or "").strip()
        if name == NATURE and text:
            nature = text
            continue
        if el.attrib.get("contextRef") not in chosen or name in values:
            continue
        try:
            values[name] = float(text)
        except ValueError:
            continue
    if nature and nature.strip().lower() != "consolidated":
        return None

    def first(keys: tuple[str, ...]) -> float | None:
        for key in keys:
            if key in values:
                return values[key]
        return None

    def crore(value: float | None) -> float | None:
        return None if value is None else round(value / RUPEES_PER_CRORE, 2)

    sales = first(REVENUE)
    if sales is None:
        return None
    expenses = first(EXPENSES)
    operating = None
    if expenses is not None:
        # The same definition as the standalone rows: expenses before
        # depreciation and interest.
        operating = sales - (abs(expenses) - abs(first(DEPRECIATION) or 0.0) - abs(first(FINANCE) or 0.0))
    eps = first(EPS)
    return {
        "period": period_label(quarter_end),
        "sales_crore": crore(sales),
        "operating_profit_crore": crore(operating),
        "operating_margin_pct": round(operating / sales * 100, 2) if operating is not None and sales else None,
        "profit_before_tax_crore": crore(first(PROFIT_BEFORE_TAX)),
        "net_profit_crore": crore(first(NET_PROFIT)),
        "eps": round(eps, 2) if eps is not None else None,
    }


def listing_row(row: dict[str, Any]) -> dict[str, Any] | None:
    """One `integrated-filing-results` row -> what the job needs, or None.
    Field names as NSE serves them (verified live 2026-10-01 by the
    india-xbrl-filings project)."""
    symbol = str(row.get("symbol") or row.get("sm_symbol") or "").strip().upper()
    consolidated = str(row.get("consolidated") or row.get("consol") or "").strip().lower()
    xbrl = str(row.get("xbrl") or "").strip()
    if not symbol or consolidated not in {"consolidated", "cons", "y", "yes", "true"}:
        return None
    if not xbrl.startswith("http") or xbrl.rstrip("/").rsplit("/", 1)[-1] in {"", "-"}:
        return None
    end = _parse_nse_date(row.get("qe_Date") or row.get("toDate") or row.get("to"))
    filed = _parse_nse_stamp(row.get("creation_Date") or row.get("broadCastDate") or row.get("filingDate"))
    if end is None or filed is None:
        return None
    return {"symbol": symbol, "period_end": end.isoformat(), "filed": filed.isoformat(timespec="minutes"), "xbrl": xbrl}


def _parse_nse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_nse_stamp(value: Any) -> datetime | None:
    text = str(value or "").replace("IST", "").strip()
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


# ── Serving ─────────────────────────────────────────────────────────────────

def load() -> dict[str, Any]:
    return bse_quarterly.read_cached(ARTIFACT_PATH)


def results_for(symbol: str) -> list[dict[str, Any]]:
    """Newest-first consolidated rows, [] when the company files none."""
    entry = (load().get("symbols") or {}).get(symbol.upper()) or {}
    rows = [dict(row) for row in entry.get("quarters") or [] if row.get("period")]
    return sorted(rows, key=lambda row: bse_quarterly._period_key(row["period"]), reverse=True)


def filed_at(symbol: str, period: str) -> datetime | None:
    for row in (load().get("symbols") or {}).get(symbol.upper(), {}).get("quarters") or []:
        if row.get("period") == period and row.get("filed"):
            try:
                return datetime.fromisoformat(row["filed"])
            except ValueError:
                return None
    return None


def path() -> Path:
    return ARTIFACT_PATH
