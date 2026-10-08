"""Build data/company_profiles.json — a description and key ratios per company.

Yahoo serves company info to residential IPs and refuses most of it from the
Space's datacenter range (the same split as gotcha 14), so the live
fundamentals call came back with no business description for every stock.
This file is built where Yahoo answers and committed, and the fundamentals
endpoint reads it first.

Incremental: a profile younger than --max-age-days is kept, and the file is
never rewritten smaller than it was.

    python3 scripts/build_company_profiles.py            # fill gaps / stale rows
    python3 scripts/build_company_profiles.py --refresh  # re-fetch everything
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "company_profiles.json"

# Yahoo field -> our field. Ratios Yahoo gives as fractions are scaled to % below.
NUMERIC_FIELDS = {
    "trailingPE": "pe",
    "forwardPE": "forward_pe",
    "priceToBook": "price_to_book",
    "bookValue": "book_value",
    "trailingEps": "eps_ttm",
    "dividendYield": "dividend_yield_pct",
    "returnOnEquity": "roe_pct",
    "returnOnAssets": "roa_pct",
    "debtToEquity": "debt_to_equity",
    "currentRatio": "current_ratio",
    "profitMargins": "net_margin_pct",
    "operatingMargins": "operating_margin_pct",
    "grossMargins": "gross_margin_pct",
    "revenueGrowth": "revenue_growth_pct",
    "earningsGrowth": "earnings_growth_pct",
    "beta": "beta",
    "enterpriseToEbitda": "ev_to_ebitda",
    "totalCash": "total_cash",
    "totalDebt": "total_debt",
    "totalRevenue": "revenue_ttm",
    "sharesOutstanding": "shares_outstanding",
    "heldPercentInsiders": "promoter_holding_pct",
    "heldPercentInstitutions": "institution_holding_pct",
    "fullTimeEmployees": "employees",
}
FRACTION_FIELDS = {
    "returnOnEquity", "returnOnAssets", "profitMargins", "operatingMargins", "grossMargins",
    "revenueGrowth", "earningsGrowth", "heldPercentInsiders", "heldPercentInstitutions",
}
CRORE = 1e7
RUPEE_FIELDS = {"totalCash", "totalDebt", "totalRevenue"}


def universe() -> list[tuple[str, str]]:
    seen: dict[str, str] = {}
    for row in json.loads((DATA / "free_universe.json").read_text()):
        sym = str(row.get("symbol") or "").upper()
        if sym:
            seen[sym] = row.get("ticker") or f"{sym}.NS"
    snaps = DATA / "free_snapshots.json"
    if snaps.exists():
        for row in json.loads(snaps.read_text()):
            sym = str(row.get("symbol") or "").upper()
            if sym and sym not in seen:
                seen[sym] = row.get("instrument_key") or (f"{sym}.BO" if row.get("exchange") == "BSE" else f"{sym}.NS")
    return sorted(seen.items())


def fetch(symbol: str, ticker: str) -> dict | None:
    for attempt in range(3):
        try:
            info = yf.Ticker(ticker).get_info() or {}
            break
        except Exception:
            time.sleep(2 + attempt * 3)
    else:
        return None
    summary = (info.get("longBusinessSummary") or "").strip()
    if not summary and not info.get("trailingPE") and not info.get("bookValue"):
        return None
    profile: dict = {
        "about": summary or None,
        "website": info.get("website"),
        "industry": info.get("industry"),
        "city": info.get("city"),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    for src, dst in NUMERIC_FIELDS.items():
        value = info.get(src)
        if not isinstance(value, (int, float)) or value != value:
            continue
        if src in FRACTION_FIELDS:
            value = value * 100
        elif src in RUPEE_FIELDS:
            value = value / CRORE  # Rs crore
        elif src == "dividendYield" and value < 1:
            value = value * 100  # older yfinance returns a fraction
        profile[dst] = round(float(value), 4)
    return profile


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--max-age-days", type=int, default=30)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    existing: dict = json.loads(OUT.read_text()) if OUT.exists() else {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.max_age_days)

    def stale(sym: str) -> bool:
        row = existing.get(sym)
        if args.refresh or not row:
            return True
        try:
            return datetime.fromisoformat(row["fetched_at"]) < cutoff
        except Exception:
            return True

    todo = [(s, t) for s, t in universe() if stale(s)]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(existing)} profiles on file, fetching {len(todo)}", flush=True)

    profiles = dict(existing)
    done = got = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch, s, t): s for s, t in todo}
        for future in as_completed(futures):
            sym = futures[future]
            done += 1
            profile = future.result()
            if profile:
                profiles[sym] = profile
                got += 1
            if done % 100 == 0:
                print(f"  {done}/{len(todo)} fetched, {got} usable", flush=True)
                save(profiles, existing)
    save(profiles, existing)
    print(f"done: {got} usable of {len(todo)}; {len(profiles)} profiles", flush=True)
    return 0


def save(profiles: dict, existing: dict) -> None:
    if len(profiles) < len(existing):
        print("refusing to shrink the profile file", file=sys.stderr)
        return
    ordered = {k: profiles[k] for k in sorted(profiles)}
    OUT.write_text(json.dumps(ordered, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
