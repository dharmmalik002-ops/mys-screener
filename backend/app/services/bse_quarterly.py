"""Quarterly results from BSE's own filings — the fallback when Screener fails.

Screener.in refuses the HF Space outright ("Could not load Screener page" for
every symbol), so the chart's Earnings widget, the peer list's "Last Q" and
every consumer of `quarterly_results` came back empty in production. BSE is
the regulator of record and is not geo-blocked from datacenter IPs.

`scripts/build_quarterly_summary.py` crawls the last `QUARTERS_KEPT` quarters
of STANDALONE results per company into the committed
`data/quarterly_summary.json`; this module parses BSE's line items into the
`QuarterlyResultItem` shape and serves them. BSE reports in Rs million, so
every money figure is divided by 10 into crore.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

ARTIFACT_PATH = Path(__file__).resolve().parents[2] / "data" / "quarterly_summary.json"
QUARTERS_KEPT = 12
MILLION_PER_CRORE = 10.0

_MONTHS = {
    "jan": "Jan", "feb": "Feb", "mar": "Mar", "apr": "Apr", "may": "May", "jun": "Jun",
    "jul": "Jul", "aug": "Aug", "sep": "Sep", "oct": "Oct", "nov": "Nov", "dec": "Dec",
}

# First match wins. Banks and NBFCs file a different schedule ("Interest
# Earned", "Basic EPS before Extraordinary items"), so every field carries its
# alternatives.
_SALES_KEYS = (
    "net sales/revenue from operations",
    "revenue from operations",
    "interest earned/net income from sales/services",
    "interest earned",
    "net sales",
    "income from operations",
    "total revenue",
)
_EPS_KEYS = (
    "basic eps for continuing operation",
    "basic eps before extraordinary items",
    "basic eps",
    "basic for discontinued & continuing operation",
    "basic eps after extraordinary items",
)


def bse_period_label(label: str) -> str | None:
    """'Jun-26' -> 'Jun 2026', the "Mon YYYY" form every consumer parses."""
    try:
        month, year = label.strip().split("-")
        return f"{_MONTHS[month[:3].lower()]} {2000 + int(year[-2:])}"
    except (ValueError, KeyError):
        return None


def _number(value: Any) -> float | None:
    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
    return number


def _find(rows: dict[str, float | None], keys: tuple[str, ...], *, nonzero: bool = False) -> float | None:
    for key in keys:
        for desc, value in rows.items():
            if desc.startswith(key) and value is not None and (value != 0 or not nonzero):
                return value
    return None


def parse_quarter(table: list[dict[str, Any]], period: str, document_url: str | None = None) -> dict[str, Any] | None:
    """One quarter's `Corp_detailedResult_Transpose_ng` rows -> a
    `QuarterlyResultItem` dict, money in crore. None when no revenue line."""
    rows: dict[str, float | None] = {}
    for item in table or []:
        desc = str(item.get("fld_desc") or "").strip().lower()
        if desc and desc not in rows:
            rows[desc] = _number(item.get("Value"))

    def crore(value: float | None) -> float | None:
        return None if value is None else round(value / MILLION_PER_CRORE, 2)

    sales = _find(rows, _SALES_KEYS)
    if sales is None:
        return None
    expenditure = rows.get("expenditure")
    depreciation = _find(rows, ("depreciation",))
    finance = _find(rows, ("finance costs", "finance cost"))
    expenses = None
    if expenditure is not None:
        # Screener's "Expenses" excludes depreciation and interest, so
        # operating profit is comparable with the rows it used to serve.
        expenses = abs(expenditure) - abs(depreciation or 0.0) - abs(finance or 0.0)
    operating_profit = sales - expenses if expenses is not None else None
    pbt = _find(rows, ("profit (+)/ loss (-) from ordinary activities before tax", "profit before tax"))
    net_profit = rows.get("net profit")
    if net_profit is None:
        net_profit = _find(rows, ("net profit (+)/ loss (-) from ordinary activities after tax", "net profit"))
    eps = _find(rows, _EPS_KEYS, nonzero=True)

    return {
        "period": period,
        "sales_crore": crore(sales),
        "expenses_crore": crore(expenses),
        "operating_profit_crore": crore(operating_profit),
        "operating_margin_pct": round(operating_profit / sales * 100, 2) if operating_profit is not None and sales else None,
        "profit_before_tax_crore": crore(pbt),
        "net_profit_crore": crore(net_profit),
        "eps": round(eps, 2) if eps is not None else None,
        "result_document_url": document_url,
    }


def quarter_codes(listing: dict[str, Any]) -> list[tuple[str, str, str | None]]:
    """`CorprateResultbeta` rows -> [(period, quarter_code, document_url)],
    newest first. Half-year and full-year entries are skipped."""
    out: list[tuple[str, str, str | None]] = []
    for row in listing.get("Table") or []:
        for key in ("Q4", "Q3", "Q2", "Q1"):
            raw = row.get(key)
            if not raw:
                continue
            parts = str(raw).split(";")
            if len(parts) < 3:
                continue
            period = bse_period_label(parts[0])
            if period is None:
                continue
            out.append((period, parts[2], parts[3] if len(parts) > 3 and parts[3] else None))
    # The listing is fiscal-year rows newest first; order within by period.
    def sort_key(entry: tuple[str, str, str | None]) -> tuple[int, int]:
        month, year = entry[0].split(" ")
        return int(year), list(_MONTHS.values()).index(month)
    seen: dict[str, tuple[str, str, str | None]] = {}
    for entry in out:
        seen.setdefault(entry[0], entry)
    return sorted(seen.values(), key=sort_key, reverse=True)


_lock = threading.Lock()
_cache: tuple[float, dict[str, Any]] | None = None


def _load() -> dict[str, Any]:
    global _cache
    try:
        mtime = ARTIFACT_PATH.stat().st_mtime
    except OSError:
        return {}
    with _lock:
        if _cache is None or _cache[0] != mtime:
            try:
                _cache = (mtime, json.loads(ARTIFACT_PATH.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                _cache = (mtime, {})
        return _cache[1]


RESULT_URL = "https://www.bseindia.com/corporates/results.aspx?Code={code}&qtr={qtr}&RType="


def compact_row(row: dict[str, Any]) -> dict[str, Any]:
    """Stored form: the result link is rebuilt from the quarter code at serve
    time — the full URLs were half the file."""
    out = {k: v for k, v in row.items() if k != "result_document_url"}
    url = row.get("result_document_url") or ""
    if "qtr=" in url and "qtr" not in out:
        out["qtr"] = url.split("qtr=")[1].split("&")[0]
    return out


def results_for(symbol: str) -> list[dict[str, Any]]:
    """Newest-first quarterly rows for `symbol`, or [] when not in the file."""
    entry = (_load().get("symbols") or {}).get(symbol.upper())
    if not entry:
        return []
    code = entry.get("bse")
    rows = []
    for stored in entry.get("quarters") or []:
        row = {k: v for k, v in stored.items() if k != "qtr"}
        qtr = stored.get("qtr")
        row["result_document_url"] = RESULT_URL.format(code=code, qtr=qtr) if code and qtr else None
        rows.append(row)
    return rows


def artifact_date() -> str | None:
    return _load().get("generated_at")
