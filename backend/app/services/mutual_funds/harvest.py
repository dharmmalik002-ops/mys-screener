"""Reshape one fund's raw source payload into the two blobs we store.

Split in two on purpose:

* ``universe_row`` — the ~40 fields the screener table needs, for every fund.
  This is what ships in git and what lives in memory, so it stays small and
  flat (one row ≈ 800 bytes × 1,600 funds ≈ 1.3 MB).
* ``detail_blob`` — holdings, expense history, manager tenure, pros/cons.
  Fetched lazily when a fund is opened and cached on disk.

Nothing here computes a return. All performance numbers are derived from AMFI
NAV in `metrics.py`; the third-party return fields are carried through only as
`source_*` cross-checks, never rendered as the primary number.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from . import benchmarks
from .holdings_enrich import enrich_holdings


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None


def _to_int(value: Any) -> int | None:
    parsed = _to_float(value)
    return int(parsed) if parsed is not None else None


def _clean_str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _parse_launch_date(raw: Any) -> str | None:
    """Groww gives '24-May-2013'; store ISO so the frontend can sort on it."""
    text = _clean_str(raw)
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _nav_date(raw: Any) -> str | None:
    return _parse_launch_date(raw)


def _strip_amc(name: str | None, fund_house: str | None) -> str | None:
    """Drop the fund house's name from the front of a scheme name.

    Derived from `fund_house` rather than a hardcoded AMC list, so a new AMC
    needs no code change.
    """
    scheme = (name or "").strip()
    house = (fund_house or "").strip()
    if not scheme or not house:
        return scheme or None
    # "Bank of India Mutual Fund" -> "bank of india"
    house_words = re.sub(r"\b(mutual\s+fund|mutual|fund|amc|asset\s+management)\b", " ", house, flags=re.I)
    house_words = " ".join(house_words.split()).lower()
    if not house_words:
        return scheme
    lowered = scheme.lower()
    if lowered.startswith(house_words):
        trimmed = scheme[len(house_words):].strip(" -–—")
        # Never strip away the whole name.
        return trimmed or scheme
    return scheme


def _dominant_sector(raw_holdings: Any) -> str | None:
    """Largest equity sector in the disclosed portfolio, by weight.

    Only used to benchmark a themed fund whose name does not name its theme
    (a "Special Opportunities" or "Business Cycle" fund, say). Debt, cash and
    derivative lines are excluded so a heavily hedged fund is not classified
    as an "Unspecified" sector play.
    """
    weights: dict[str, float] = {}
    for holding in (raw_holdings or []):
        if not isinstance(holding, dict):
            continue
        if str(holding.get("nature_name") or "").strip().upper() != "EQUITY":
            continue
        sector = _clean_str(holding.get("sector_name"))
        if not sector or sector.lower() == "unspecified":
            continue
        weight = _to_float(holding.get("corpus_per")) or 0.0
        weights[sector] = weights.get(sector, 0.0) + weight
    if not weights:
        return None
    best, share = max(weights.items(), key=lambda kv: kv[1])
    # Below a third of the book it is a tilt, not a theme.
    return best if share >= 33.0 else None


def universe_row(data: dict[str, Any]) -> dict[str, Any]:
    """Screener-table fields for one fund."""
    return_stats = (data.get("return_stats") or [{}])
    stats = return_stats[0] if isinstance(return_stats, list) and return_stats else {}
    if not isinstance(stats, dict):
        stats = {}

    sub_category = _clean_str(data.get("sub_category"))
    category = _clean_str(data.get("category"))
    fund_name = _clean_str(data.get("fund_name")) or _clean_str(data.get("scheme_name"))

    # A themed fund is benchmarked to its own sector, which needs the name and
    # — when the name is uninformative — the largest sector in the portfolio.
    #
    # The AMC name has to come off first. "Bank of India Manufacturing &
    # Infrastructure Fund" is an infrastructure fund, but a naive keyword match
    # sees "bank" in the fund house's own name and benchmarks it to Nifty Bank.
    benchmark = benchmarks.resolve(
        sub_category,
        category=category,
        name=_strip_amc(fund_name, _clean_str(data.get("fund_house"))),
        dominant_sector=_dominant_sector(data.get("holdings")),
    )

    return {
        "scheme_code": str(data.get("scheme_code") or "").strip(),
        "isin": _clean_str(data.get("isin")),
        "slug": _clean_str(data.get("search_id")),
        "name": fund_name,
        "scheme_name": _clean_str(data.get("scheme_name")),
        "amc": _clean_str(data.get("fund_house")),
        "amc_code": _clean_str(data.get("amc")),
        "logo_url": _clean_str(data.get("logo_url")),
        "category": category,
        "sub_category": sub_category,
        "plan": _clean_str(data.get("plan_type")),
        "option": _clean_str(data.get("scheme_type")),

        # Official SEBI benchmark as named in the scheme document, kept
        # verbatim so the UI can show what the fund is *actually* measured
        # against even when we chart a stand-in.
        "official_benchmark": _clean_str(data.get("benchmark_name")) or _clean_str(data.get("benchmark")),
        "benchmark_key": benchmark.key,
        "benchmark_label": benchmark.label,
        "benchmark_is_reference_only": benchmark.is_reference_only,

        "nav": _to_float(data.get("nav")),
        "nav_date": _nav_date(data.get("nav_date")),
        "aum_crore": _to_float(data.get("aum")),
        "expense_ratio": _to_float(data.get("expense_ratio")),
        "portfolio_turnover": _to_float(data.get("portfolio_turnover")),
        "launch_date": _parse_launch_date(data.get("launch_date")),
        "fund_manager": _clean_str(data.get("fund_manager")),
        "exit_load": _clean_str(data.get("exit_load")),
        "min_lumpsum": _to_float(data.get("min_investment_amount")),
        "min_sip": _to_float(data.get("min_sip_investment")),
        "lock_in_years": _to_int((data.get("lock_in") or {}).get("years")) if isinstance(data.get("lock_in"), dict) else None,
        "risk_label": _clean_str(stats.get("risk")),
        "source_rating": _to_int(data.get("groww_rating")),

        # Cross-check only — our own NAV-derived figures are authoritative.
        "source_return_1y": _to_float(stats.get("return1y")),
        "source_return_3y": _to_float(stats.get("return3y")),
        "source_return_5y": _to_float(stats.get("return5y")),
        "source_rank_1y": _to_int(stats.get("rank1yr")),
        "source_rank_3y": _to_int(stats.get("rank3yr")),
        "source_sharpe": _to_float(stats.get("sharpe_ratio")),
        "source_beta": _to_float(stats.get("beta")),
        "source_alpha": _to_float(stats.get("alpha")),
        "source_std_dev": _to_float(stats.get("standard_deviation")),
    }


def detail_blob(data: dict[str, Any]) -> dict[str, Any]:
    """Everything the fund-detail view shows beyond the screener row."""
    enriched = enrich_holdings(data.get("holdings"))

    managers = []
    for manager in (data.get("fund_manager_details") or []):
        if not isinstance(manager, dict):
            continue
        managers.append({
            "name": _clean_str(manager.get("person_name")),
            "since": (_clean_str(manager.get("date_from")) or "")[:10] or None,
            "education": _clean_str(manager.get("education")),
            "experience": _clean_str(manager.get("experience")),
        })

    expense_history = []
    for entry in (data.get("historic_fund_expense") or []):
        if not isinstance(entry, dict):
            continue
        expense_history.append({
            "date": (_clean_str(entry.get("as_on_date")) or "")[:10] or None,
            "expense_ratio": _to_float(entry.get("expense_ratio")),
            "turnover": _to_float(entry.get("turn_over_ratio")),
        })
    expense_history.sort(key=lambda row: row["date"] or "")

    pros: list[str] = []
    cons: list[str] = []
    for item in (data.get("analysis") or []):
        if not isinstance(item, dict):
            continue
        text = _clean_str(item.get("analysis_desc"))
        if not text:
            continue
        if str(item.get("analysis_type") or "").upper() == "PROS":
            pros.append(text)
        else:
            cons.append(text)

    peers = []
    for peer in (data.get("peerComparison") or []):
        if not isinstance(peer, dict):
            continue
        peers.append({
            "name": _clean_str(peer.get("scheme_name")) or _clean_str(peer.get("fund_name")),
            "slug": _clean_str(peer.get("search_id")),
            "scheme_code": _clean_str(peer.get("scheme_code")),
        })

    amc_info = data.get("amc_info") if isinstance(data.get("amc_info"), dict) else {}
    category_info = data.get("category_info") if isinstance(data.get("category_info"), dict) else {}

    return {
        "scheme_code": str(data.get("scheme_code") or "").strip(),
        "objective": _clean_str(data.get("description")),
        "category_definition": _clean_str(category_info.get("definition")),
        "amc": {
            "name": _clean_str(amc_info.get("name")),
            "total_aum_crore": _to_float(amc_info.get("aum")),
            "website": _clean_str(data.get("sid_url")),
        },
        "managers": managers,
        "expense_history": expense_history[-36:],
        "pros": pros,
        "cons": cons,
        "peers": peers[:12],
        "stamp_duty": _clean_str(data.get("stamp_duty")),
        "scheme_document": _clean_str(data.get("scheme_info_link")) or _clean_str(data.get("brochure_link")),
        **enriched,
    }
