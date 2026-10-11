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
REVENUE = ("RevenueFromOperations", "InterestEarned", "TotalInterestEarned")
EXPENSES = ("Expenses",)
DEPRECIATION = ("DepreciationDepletionAndAmortisationExpense",)
FINANCE = ("FinanceCosts",)
PROFIT_BEFORE_TAX = ("ProfitBeforeTax", "ProfitLossBeforeTax")
# Owners of the parent: what EPS and PE are measured on. A consolidated
# profit including minority interests would overstate a group with partly
# owned subsidiaries.
NET_PROFIT = ("ProfitOrLossAttributableToOwnersOfParent", "ProfitLossForPeriod", "NetProfitLossForThePeriod",
              "ProfitLossForThePeriod")
EPS = (
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "BasicEarningsLossPerShareFromContinuingOperations",
    "BasicEarningsPerShareBeforeExtraordinaryItems",
    "BasicEarningsLossPerShareBeforeExtraordinaryItems",
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


# Banks do not use Ind AS and file a different schedule. These names are what
# the bank results format calls its lines; a filing that has none of them is
# skipped and its local names are logged by the job so they can be added.
BANK_OPERATING = (
    "OperatingProfitBeforeProvisionsAndContingencies",
    "OperatingProfitBeforeProvisionAndContingencies",
    "OperatingProfitBeforeProvisions",
)


def _contexts(root: ET.Element) -> dict[str, tuple[date, date]]:
    """id -> (start, end) for every undimensioned ~3-month context."""
    out: dict[str, tuple[date, date]] = {}
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
            out[ctx_id] = (start, end)
    return out


def _row(values: dict[str, float], quarter_end: date) -> dict[str, Any] | None:
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
    operating = first(BANK_OPERATING)
    expenses = first(EXPENSES)
    if operating is None and expenses is not None:
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


def parse_filing(xml_bytes: bytes) -> dict[str, Any] | None:
    """Every quarter an Integrated Filing XBRL instance carries.

    A results filing reports the quarter, the quarter before it and the same
    quarter a year earlier side by side (SEBI's results format), so one
    filing is enough for a card's QoQ and YoY — even for a company that listed
    this year or whose earlier filing was missed. Each column is an
    undimensioned context running ~3 months; year-to-date and segment
    contexts are never read as a quarter.

    Returns {"nature": "Consolidated" | "Standalone" | None,
             "periods": {quarter end: row}, "quarter_end": the filing's own
             quarter (its latest column), "names": local names seen},
    or None for a document that is not an XBRL instance.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None
    if root.tag != f"{_XBRLI}xbrl":
        return None
    contexts = _contexts(root)
    by_context: dict[str, dict[str, float]] = {cid: {} for cid in contexts}
    nature = None
    names: set[str] = set()
    for el in root:
        if not isinstance(el.tag, str):
            continue
        _uri, name = _local(el.tag)
        text = (el.text or "").strip()
        if name == NATURE and text:
            nature = text
            continue
        names.add(name)
        bucket = by_context.get(el.attrib.get("contextRef") or "")
        if bucket is None or name in bucket:
            continue
        try:
            bucket[name] = float(text)
        except ValueError:
            continue
    periods: dict[date, dict[str, Any]] = {}
    for cid, (_start, end) in sorted(contexts.items(), key=lambda kv: kv[1][1], reverse=True):
        if end in periods:
            continue
        row = _row(by_context[cid], end)
        if row is not None:
            periods[end] = row
    quarter_end = max((end for _s, end in contexts.values()), default=None)
    return {"nature": nature, "periods": periods, "quarter_end": quarter_end, "names": sorted(names)}


def parse_instance(xml_bytes: bytes, expect_end: date | None = None, basis: str = "consolidated") -> dict[str, Any] | None:
    """The filing's own quarter (``expect_end``, else the latest column) as a
    row in crore, or None when the filing is not of ``basis`` or has no
    revenue line. Kept for callers that want one row; the job reads every
    column through `parse_filing`."""
    parsed = parse_filing(xml_bytes)
    if parsed is None:
        return None
    if parsed["nature"] and parsed["nature"].strip().lower() != basis:
        return None
    # The filing's own quarter is its latest column, whether or not that
    # column parsed — never fall back to a comparative.
    return parsed["periods"].get(expect_end or parsed["quarter_end"])


def listing_row(row: dict[str, Any]) -> dict[str, Any] | None:
    """One `integrated-filing-results` row -> what the job needs, or None.
    Field names as NSE serves them (verified live 2026-10-01 by the
    india-xbrl-filings project)."""
    symbol = str(row.get("symbol") or row.get("sm_symbol") or "").strip().upper()
    nature = str(row.get("consolidated") or row.get("consol") or "").strip().lower()
    xbrl = str(row.get("xbrl") or "").strip()
    if nature in {"consolidated", "cons", "y", "yes", "true"}:
        basis = "consolidated"
    elif nature in {"non-consolidated", "nonconsolidated", "standalone", "n", "no", "false"}:
        basis = "standalone"
    else:
        return None
    if not symbol:
        return None
    if not xbrl.startswith("http") or xbrl.rstrip("/").rsplit("/", 1)[-1] in {"", "-"}:
        return None
    end = _parse_nse_date(row.get("qe_Date") or row.get("toDate") or row.get("to"))
    filed = _parse_nse_stamp(row.get("creation_Date") or row.get("broadCastDate") or row.get("filingDate"))
    if end is None or filed is None:
        return None
    return {"symbol": symbol, "basis": basis, "period_end": end.isoformat(),
            "filed": filed.isoformat(timespec="minutes"), "xbrl": xbrl}


ROW_KEYS = {"consolidated": "quarters", "standalone": "standalone"}


def merge_filing(rows: list[dict[str, Any]], periods: dict[date, dict[str, Any]], own_end: date | None,
                 filed: str) -> list[dict[str, Any]]:
    """Lay one filing's columns over a company's stored rows.

    The filing's own quarter is stored as ``filed`` and replaces anything but
    a later revision of itself. Its comparative columns (the previous quarter
    and the year-ago quarter) are stored as ``via`` and only fill a quarter no
    filing of its own has supplied — a comparative never overrides the
    quarter's own report, but a newer comparative (a restatement) replaces
    an older one."""
    by_period = {row["period"]: row for row in rows if row.get("period")}
    for end, fresh in periods.items():
        row = dict(fresh)
        current = by_period.get(row["period"])
        if end == own_end:
            if current and str(current.get("filed") or "") > filed:
                continue
            row["filed"] = filed
        else:
            if current and (current.get("filed") or str(current.get("via") or "") > filed):
                continue
            row["via"] = filed
        by_period[row["period"]] = row
    ordered = sorted(by_period.values(), key=lambda r: bse_quarterly._period_key(r["period"]), reverse=True)
    return ordered[:QUARTERS_KEPT]


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


def results_for(symbol: str, basis: str = "consolidated") -> list[dict[str, Any]]:
    """Newest-first rows of one basis from NSE's filings, [] when none."""
    entry = (load().get("symbols") or {}).get(symbol.upper()) or {}
    rows = [dict(row) for row in entry.get(ROW_KEYS[basis]) or [] if row.get("period")]
    return sorted(rows, key=lambda row: bse_quarterly._period_key(row["period"]), reverse=True)


def filed_at(symbol: str, period: str) -> datetime | None:
    """When NSE received the quarter's own filing (either basis), or None."""
    entry = (load().get("symbols") or {}).get(symbol.upper(), {})
    for row in (entry.get("quarters") or []) + (entry.get("standalone") or []):
        if row.get("period") == period and row.get("filed"):
            try:
                return datetime.fromisoformat(row["filed"])
            except ValueError:
                return None
    return None


def path() -> Path:
    return ARTIFACT_PATH
