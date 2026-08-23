"""Daily NAV history, from AMFI via the mfapi.in mirror.

AMFI publishes only *today's* NAV at a stable URL (`NAVAll.txt`); its
historical endpoint is a form-posted report, not something to depend on.
mfapi.in mirrors the full AMFI history per scheme code as plain JSON, which
is what every return, drawdown and rank in `metrics.py` is computed from.

This is the authoritative leg of the pipeline. Unlike `groww_source`, if this
breaks the screener has no numbers — so NAV series are cached on disk and the
service will serve a stale series with its `as_of` date rather than nothing.
"""

from __future__ import annotations

import threading
from datetime import date, datetime
from typing import Any

import requests

LIST_URL = "https://api.mfapi.in/mf"
SCHEME_URL_TEMPLATE = "https://api.mfapi.in/mf/{scheme_code}"

_session_lock = threading.Lock()
_session: requests.Session | None = None


class NavUnavailable(RuntimeError):
    pass


def _get_session() -> requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            session = requests.Session()
            session.headers.update({"Accept": "application/json"})
            _session = session
        return _session


def _parse_nav_date(raw: str) -> date | None:
    try:
        return datetime.strptime(raw.strip(), "%d-%m-%Y").date()
    except (ValueError, AttributeError):
        return None


def fetch_nav_history(scheme_code: str | int, *, timeout: int = 30) -> dict[str, Any]:
    """Full NAV history for one scheme, oldest first.

    Returned shape:
        {"scheme_code", "scheme_name", "fund_house", "scheme_category",
         "dates": ["YYYY-MM-DD", ...], "navs": [float, ...]}

    Dates and NAVs are kept as two parallel arrays rather than a list of
    objects: a 3,400-point series is ~40% smaller on disk and on the wire that
    way, and every consumer wants them column-wise anyway.
    """
    url = SCHEME_URL_TEMPLATE.format(scheme_code=str(scheme_code).strip())
    try:
        response = _get_session().get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise NavUnavailable(f"{type(exc).__name__} fetching NAV for {scheme_code}") from exc

    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise NavUnavailable(f"no NAV rows for {scheme_code}")

    meta = payload.get("meta") or {}
    # mfapi returns newest-first; flip to chronological and drop the "0.00000"
    # placeholder rows AMFI emits for non-business days.
    pairs: list[tuple[date, float]] = []
    for row in reversed(rows):
        parsed = _parse_nav_date(str(row.get("date", "")))
        if parsed is None:
            continue
        try:
            nav = float(row.get("nav"))
        except (TypeError, ValueError):
            continue
        if nav <= 0:
            continue
        pairs.append((parsed, nav))

    if not pairs:
        raise NavUnavailable(f"no usable NAV points for {scheme_code}")

    # AMFI occasionally repeats a date across two feed rows; keep the last.
    deduped: dict[date, float] = {}
    for parsed, nav in pairs:
        deduped[parsed] = nav
    ordered = sorted(deduped.items())

    return {
        "scheme_code": str(meta.get("scheme_code") or scheme_code),
        "scheme_name": meta.get("scheme_name"),
        "fund_house": meta.get("fund_house"),
        "scheme_category": meta.get("scheme_category"),
        "dates": [d.isoformat() for d, _ in ordered],
        "navs": [nav for _, nav in ordered],
    }


def build_isin_index(*, timeout: int = 90) -> dict[str, dict[str, Any]]:
    """ISIN -> AMFI scheme, across every plan and option.

    A broker statement identifies a holding by ISIN, and that ISIN is often an
    IDCW or Payout variant. The screener universe is Direct/Growth only, so
    matching a statement row by *name* quietly lands on the Growth sibling —
    which has a different NAV and therefore a wrong valuation. This index is
    the authoritative way to get from a statement row to the exact scheme.
    """
    index: dict[str, dict[str, Any]] = {}
    for scheme in fetch_scheme_index(timeout=timeout):
        if not isinstance(scheme, dict):
            continue
        for key in ("isinGrowth", "isinDivReinvestment"):
            isin = str(scheme.get(key) or "").strip()
            if isin and isin.lower() not in ("none", "null", "-"):
                index.setdefault(isin, scheme)
    return index


def fetch_scheme_index(*, timeout: int = 60) -> list[dict[str, Any]]:
    """Every AMFI scheme code + name. Used to resolve benchmark index funds
    and to sanity-check the crawled universe against the official list."""
    try:
        response = _get_session().get(LIST_URL, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise NavUnavailable(f"{type(exc).__name__} fetching scheme index") from exc
    if not isinstance(payload, list):
        raise NavUnavailable("scheme index was not a list")
    return payload
