"""Turn a raw holdings list into something the rest of the app can use.

Three jobs, all of which exist because the disclosed holding *name* is the
only key a fund portfolio gives us:

1. **Link to the app's own equity data.** Matching a holding to a symbol in
   `free_universe.json` means a holding row can open the same chart, RS rating
   and scanner history the Screener page already shows. That link is the whole
   reason to build this inside the scanner instead of reading a fund website.
2. **Classify by market cap.** Groww's `market_cap` field is null in practice,
   but we already know every Indian stock's market cap, so the SEBI
   large/mid/small split (top 100 / next 150 / rest by full market cap) can be
   derived rather than trusted.
3. **Separate what is not a domestic equity position.** Overseas holdings,
   index futures, debt paper, T-bills, REITs and cash all show up in the same
   list. Bucketing them keeps "equity allocation" honest — a fund holding 8%
   in Nifty futures is not 8% in cash.
"""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path
from typing import Any

_BACKEND_ROOT = Path(__file__).resolve().parents[3]
_UNIVERSE_PATH = _BACKEND_ROOT / "data" / "free_universe.json"

# SEBI's market-cap definition, by rank on full market capitalisation.
LARGE_CAP_MAX_RANK = 100
MID_CAP_MAX_RANK = 250

_FILLER_WORDS = re.compile(
    r"\b(limited|ltd|the|company|co|corporation|corp|india|indian|of|inc|plc)\b"
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")

# "Reliance Industries Limited July 2026 Future", "Lupin Ltd JUL-2026" — a
# derivative position on an underlying we do know.
_FUTURES_SUFFIX = re.compile(
    r"\s*(?:"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\s\-/]*\d{2,4}"
    r"|\d{1,2}[\s\-/](?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\s\-/]*\d{2,4}"
    r")\s*(?:future|fut|option|call|put)?\s*$",
    re.IGNORECASE,
)
_FUTURES_MARKER = re.compile(r"\b(future|fut\.?|option|call|put)\b", re.IGNORECASE)
_FOREIGN_MARKER = re.compile(r"forgn\.?\s*eq|foreign\s*eq|\(usa\)|\(us\)|\badr\b|\bgdr\b", re.IGNORECASE)
_TICKER_IN_PARENS = re.compile(r"\s*\([A-Z]{1,6}\)\s*$")

# Companies that renamed after the holdings feed started using the old name,
# or whose disclosed name never matches the exchange name. Kept deliberately
# short — a long alias table is a sign the matcher needs fixing, not padding.
_ALIASES: dict[str, str] = {
    "zomato": "eternal",
    "mahindra cie automotive": "cie automotive",
    "l t": "larsen toubro",
    "l t finance holdings": "l t finance",
    "bajaj finserv": "bajaj finserv",
    "jio financial services": "jio financial services",
    "hdfc bank": "hdfc bank",
}

# `nature_name` / `instrument_name` values seen in the feed, mapped to the
# asset buckets the UI shows.
_ASSET_BUCKETS: dict[str, str] = {
    "EQUITY": "equity",
    "MF": "mutual_fund",
    "DEBT": "debt",
    "CASH": "cash",
    "GOLD": "commodity",
    "SILVER": "commodity",
    "REIT": "reit",
    "INVIT": "reit",
    "DERIVATIVES": "derivatives",
}

_universe_lock = threading.Lock()
_universe_cache: dict[str, Any] | None = None


def _normalise_name(raw: str | None) -> str:
    text = str(raw or "").lower()
    text = _TICKER_IN_PARENS.sub("", text)
    text = _FUTURES_SUFFIX.sub("", text)
    text = _FUTURES_MARKER.sub(" ", text)
    text = _FOREIGN_MARKER.sub(" ", text)
    text = _FILLER_WORDS.sub(" ", text)
    text = _NON_ALNUM.sub(" ", text)
    collapsed = " ".join(text.split())
    return _ALIASES.get(collapsed, collapsed)


def _load_universe() -> dict[str, Any]:
    """Name -> {symbol, market_cap_crore, cap_rank, cap_class, sector}.

    Cached process-wide: the file is ~700 KB and every holdings render would
    otherwise re-read and re-rank it.
    """
    global _universe_cache
    with _universe_lock:
        if _universe_cache is not None:
            return _universe_cache
        lookup: dict[str, Any] = {}
        try:
            rows = json.loads(_UNIVERSE_PATH.read_text())
        except (OSError, ValueError):
            _universe_cache = lookup
            return lookup

        ranked = sorted(
            (row for row in rows if isinstance(row.get("market_cap_crore"), (int, float))),
            key=lambda row: row["market_cap_crore"],
            reverse=True,
        )
        for rank, row in enumerate(ranked, start=1):
            if rank <= LARGE_CAP_MAX_RANK:
                cap_class = "large"
            elif rank <= MID_CAP_MAX_RANK:
                cap_class = "mid"
            else:
                cap_class = "small"
            entry = {
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "market_cap_crore": row.get("market_cap_crore"),
                "cap_rank": rank,
                "cap_class": cap_class,
                "sector": row.get("sector"),
                "sub_sector": row.get("sub_sector"),
            }
            key = _normalise_name(row.get("name"))
            if key and key not in lookup:
                lookup[key] = entry
            # Also index the bare symbol — some AMCs disclose "INFY" style.
            symbol_key = _normalise_name(row.get("symbol"))
            if symbol_key and symbol_key not in lookup:
                lookup[symbol_key] = entry
        _universe_cache = lookup
        return lookup


def _bucket_for(holding: dict[str, Any], *, is_foreign: bool, is_derivative: bool) -> str:
    if is_derivative:
        return "derivatives"
    nature = str(holding.get("nature_name") or "").strip().upper()
    instrument = str(holding.get("instrument_name") or "").strip().lower()
    if nature == "EQUITY":
        return "international_equity" if is_foreign else "equity"
    if nature in _ASSET_BUCKETS:
        return _ASSET_BUCKETS[nature]
    if "treasury" in instrument or "bill" in instrument or "bond" in instrument or "debenture" in instrument:
        return "debt"
    if "cash" in instrument or "repo" in instrument or "trep" in instrument or "net receivable" in instrument:
        return "cash"
    if "mutual fund" in instrument or "etf" in instrument:
        return "mutual_fund"
    if "reit" in instrument or "invit" in instrument:
        return "reit"
    return "other"


def enrich_holdings(raw_holdings: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Normalise a fund's disclosed portfolio.

    Returns holdings enriched with symbol/cap-class where we could match them,
    plus the aggregate breakdowns the detail view renders. Weights are the
    AMC's own `corpus_per` — never recomputed from market values, because the
    disclosed list is sometimes truncated and would not sum to 100.
    """
    holdings_in = raw_holdings or []
    universe = _load_universe()

    rows: list[dict[str, Any]] = []
    portfolio_date: str | None = None

    for holding in holdings_in:
        name = str(holding.get("company_name") or "").strip()
        if not name:
            continue
        if portfolio_date is None and holding.get("portfolio_date"):
            portfolio_date = str(holding["portfolio_date"])[:10]

        is_foreign = bool(_FOREIGN_MARKER.search(name))
        is_derivative = bool(_FUTURES_MARKER.search(name) or _FUTURES_SUFFIX.search(name))
        key = _normalise_name(name)
        # Never claim a domestic symbol for an overseas line — "Alphabet Inc
        # Forgn. Eq" must not resolve to an Indian ticker by loose matching.
        matched = None if is_foreign else universe.get(key)

        try:
            weight = float(holding.get("corpus_per"))
        except (TypeError, ValueError):
            weight = None
        try:
            market_value = float(holding.get("market_value"))
        except (TypeError, ValueError):
            market_value = None

        rows.append({
            "name": name,
            "weight_pct": weight,
            "market_value_crore": market_value,
            "sector": holding.get("sector_name") or None,
            "instrument": holding.get("instrument_name") or None,
            "rating": holding.get("rating") or None,
            "asset_class": _bucket_for(holding, is_foreign=is_foreign, is_derivative=is_derivative),
            "is_derivative": is_derivative,
            "is_foreign": is_foreign,
            # Populated only on a confident match; the UI turns these into a
            # link into the Screener/chart for that stock.
            "symbol": (matched or {}).get("symbol"),
            "cap_class": (matched or {}).get("cap_class"),
            "cap_rank": (matched or {}).get("cap_rank"),
            "market_cap_crore": (matched or {}).get("market_cap_crore"),
            "app_sector": (matched or {}).get("sector"),
        })

    rows.sort(key=lambda row: (row["weight_pct"] is None, -(row["weight_pct"] or 0)))

    def weight_sum(predicate) -> float:
        return round(sum(row["weight_pct"] or 0 for row in rows if predicate(row)), 2)

    asset_allocation: dict[str, float] = {}
    for row in rows:
        bucket = row["asset_class"]
        asset_allocation[bucket] = round(asset_allocation.get(bucket, 0.0) + (row["weight_pct"] or 0), 2)

    sector_allocation: dict[str, float] = {}
    for row in rows:
        if row["asset_class"] not in ("equity", "international_equity"):
            continue
        sector = row["sector"] or "Unspecified"
        sector_allocation[sector] = round(sector_allocation.get(sector, 0.0) + (row["weight_pct"] or 0), 2)

    domestic_equity = [row for row in rows if row["asset_class"] == "equity"]
    matched_equity = [row for row in domestic_equity if row["cap_class"]]
    cap_allocation: dict[str, float] = {}
    for row in matched_equity:
        cap_allocation[row["cap_class"]] = round(
            cap_allocation.get(row["cap_class"], 0.0) + (row["weight_pct"] or 0), 2
        )

    equity_weight = weight_sum(lambda row: row["asset_class"] == "equity")
    matched_weight = round(sum(row["weight_pct"] or 0 for row in matched_equity), 2)

    return {
        "portfolio_date": portfolio_date,
        "holdings": rows,
        "holdings_count": len(rows),
        "equity_holdings_count": len(domestic_equity),
        "top10_weight_pct": round(sum(row["weight_pct"] or 0 for row in rows[:10]), 2) if rows else None,
        "top5_weight_pct": round(sum(row["weight_pct"] or 0 for row in rows[:5]), 2) if rows else None,
        "asset_allocation": dict(sorted(asset_allocation.items(), key=lambda kv: -kv[1])),
        "sector_allocation": dict(sorted(sector_allocation.items(), key=lambda kv: -kv[1])),
        "cap_allocation": cap_allocation,
        # How much of the domestic equity book we could classify. Shown in the
        # UI so a 60%-matched breakdown is not read as the whole portfolio.
        "cap_coverage_pct": round(matched_weight / equity_weight * 100.0, 1) if equity_weight > 0 else None,
        "sector_count": len(sector_allocation),
    }
