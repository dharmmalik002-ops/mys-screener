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
    if operating_profit is None:
        # Banks file a different schedule with no "Expenditure" line; their
        # operating profit is the pre-provision line every bank reports.
        operating_profit = _find(rows, ("operating profit before provision",))
    pbt = _find(rows, ("profit (+)/ loss (-) from ordinary activities before tax", "profit before tax"))
    net_profit = rows.get("net profit")
    if net_profit is None:
        net_profit = _find(rows, ("net profit (+)/ loss (-) from ordinary activities after tax", "net profit"))
    # Prefer a non-zero line (some filers leave the continuing-ops line 0.00
    # beside the real one), but a company whose EPS rounds to 0.00 has 0.00.
    eps = _find(rows, _EPS_KEYS, nonzero=True)
    if eps is None:
        eps = _find(rows, _EPS_KEYS)

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


# The evening results feed (scripts/update_results_feed.py): quarters filed
# after the 4:23 PM bhavcopy run, and the date each quarter was declared. A
# separate file because the bhavcopy job rewrites the summary, and two jobs
# committing one file collide on push (see services/results_feed.py).
FEED_PATH = ARTIFACT_PATH.with_name("results_feed.json")

_lock = threading.Lock()
_caches: dict[Path, tuple[float, dict[str, Any]]] = {}


def read_cached(path: Path) -> dict[str, Any]:
    """A JSON object file, parsed once per mtime ({} when missing or bad)."""
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {}
    with _lock:
        cached = _caches.get(path)
        if cached is None or cached[0] != mtime:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                payload = {}
            cached = (mtime, payload if isinstance(payload, dict) else {})
            _caches[path] = cached
        return cached[1]


def _load() -> dict[str, Any]:
    return read_cached(ARTIFACT_PATH)


def load_feed() -> dict[str, Any]:
    return read_cached(FEED_PATH)


RESULT_URL = "https://www.bseindia.com/corporates/results.aspx?Code={code}&qtr={qtr}&RType="


def compact_row(row: dict[str, Any]) -> dict[str, Any]:
    """Stored form: the result link is rebuilt from the quarter code at serve
    time — the full URLs were half the file."""
    out = {k: v for k, v in row.items() if k != "result_document_url"}
    url = row.get("result_document_url") or ""
    if "qtr=" in url and "qtr" not in out:
        out["qtr"] = url.split("qtr=")[1].split("&")[0]
    return out


def _period_key(label: str) -> tuple[int, int]:
    try:
        month, year = str(label).split(" ")
        return int(year), list(_MONTHS.values()).index(month)
    except ValueError:
        return 0, 0


def stored_quarters(symbol: str) -> tuple[str | None, list[dict[str, Any]]]:
    """(BSE code, compact rows newest first): the summary's quarters with the
    evening feed's laid over them, a period in both taken from the feed."""
    key = symbol.upper()
    entry = (_load().get("symbols") or {}).get(key) or {}
    extra = (load_feed().get("quarters") or {}).get(key) or {}
    by_period: dict[str, dict[str, Any]] = {}
    for stored in list(extra.get("quarters") or []) + list(entry.get("quarters") or []):
        period = stored.get("period")
        if period and period not in by_period:
            by_period[period] = stored
    rows = sorted(by_period.values(), key=lambda row: _period_key(row["period"]), reverse=True)
    return entry.get("bse") or extra.get("bse"), rows[:QUARTERS_KEPT]


def results_for(symbol: str) -> list[dict[str, Any]]:
    """Newest-first quarterly rows for `symbol`, or [] when not in either file."""
    code, stored_rows = stored_quarters(symbol)
    rows = []
    for stored in stored_rows:
        row = {k: v for k, v in stored.items() if k not in ("qtr", "v")}
        qtr = stored.get("qtr")
        row["result_document_url"] = RESULT_URL.format(code=code, qtr=qtr) if code and qtr else None
        rows.append(row)
    return rows


def symbols() -> set[str]:
    return set(_load().get("symbols") or {}) | set(load_feed().get("quarters") or {})


def artifact_date() -> str | None:
    """The newer of the summary's and the evening feed's build times — what
    caches over `results_for` key on."""
    stamps = [s for s in (_load().get("generated_at"), load_feed().get("generated_at")) if s]
    return max(stamps) if stamps else None
