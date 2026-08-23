"""Fund reference data (holdings, benchmark, category, TER, risk stats).

There is no free API for Indian mutual-fund *holdings*. The only official
route is each AMC's monthly SEBI portfolio disclosure, published as ~40
mutually incompatible XLSX layouts. This module takes the pragmatic route
instead: Groww renders the whole payload into the server-side props of its
public fund pages, and those pages are crawlable.

**Why the HTML page and not the JSON API.** Groww's `robots.txt` disallows
`/v1/api/*` for every user agent, so we do not touch it. It *allows*
`/mf-sitemap.xml` and `/mutual-funds/<slug>`, and the Next.js
`__NEXT_DATA__` blob on those pages carries the identical payload. So we
read the allowed surface and parse what is already in the page.

This is a third-party source, so treat every field as best-effort: the
screener's own maths (returns, ranks, drawdown) is computed from AMFI NAV in
`metrics.py` and never depends on anything here. If Groww changes its markup
this module degrades to "no holdings" rather than taking the page down.
"""

from __future__ import annotations

import json
import re
import threading
import time
from typing import Any

import requests

SITEMAP_URL = "https://groww.in/mf-sitemap.xml"
FUND_URL_TEMPLATE = "https://groww.in/mutual-funds/{slug}"

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    re.DOTALL,
)
_SLUG_RE = re.compile(r"<loc>https://groww\.in/mutual-funds/([a-z0-9\-]+)</loc>")

# The sitemap mixes scheme pages with AMC and category landing pages. These are
# the landing pages, and they are the only thing worth excluding by slug shape.
#
# An earlier version instead *included* by slug shape, keeping only
# `*-direct-growth`. That silently dropped 49 real funds whose slug reads
# `-direct-plan-growth` (Quant Small Cap Fund among them) — a whole AMC's
# range missing from the screener with nothing in the logs to show it. Slug
# conventions are the source's business and change without notice, so the
# authoritative Direct/Growth test is now the payload's own `plan_type` and
# `scheme_type`, applied after the fetch. Be permissive here; a landing page
# has no `mfServerSideData` and is skipped for free.
_LANDING_PAGE_RE = re.compile(
    r"(?:-mutual-funds|-amc|-category|-invalid)$|^mutual-funds$|invalid"
)


class GrowwUnavailable(RuntimeError):
    """Raised when the source cannot be read — never fatal to a request."""


_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

_session_lock = threading.Lock()
_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """One pooled session — a 2,000-page crawl over fresh TLS handshakes
    spends most of its wall clock in the handshake."""
    global _session
    with _session_lock:
        if _session is None:
            session = requests.Session()
            session.headers.update(_HEADERS)
            _session = session
        return _session


def _fetch(url: str, *, timeout: int = 30) -> str:
    try:
        response = _get_session().get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise GrowwUnavailable(f"{type(exc).__name__} fetching {url}") from exc
    return response.text


def list_scheme_slugs() -> list[str]:
    """Every candidate scheme slug in the sitemap, minus the landing pages.

    Deliberately permissive — Regular plans and IDCW variants come through
    here and are filtered after the fetch on the payload's declared plan and
    option, which is the only field that actually means what it says.
    """
    xml = _fetch(SITEMAP_URL, timeout=45)
    keep: list[str] = []
    seen: set[str] = set()
    for slug in _SLUG_RE.findall(xml):
        if slug in seen or _LANDING_PAGE_RE.search(slug):
            continue
        seen.add(slug)
        keep.append(slug)
    if not keep:
        raise GrowwUnavailable("sitemap returned no scheme slugs")
    return keep


def is_direct_growth(row: dict[str, Any]) -> bool:
    """The authoritative Direct/Growth test, from the payload rather than the slug."""
    plan = str(row.get("plan") or "").strip().lower()
    option = str(row.get("option") or "").strip().lower()
    return plan == "direct" and option == "growth"


def fetch_scheme(slug: str, *, timeout: int = 30) -> dict[str, Any]:
    """Server-side props for one fund page.

    Raises GrowwUnavailable on any transport or shape problem so callers can
    skip the fund instead of aborting a 2,000-fund crawl.
    """
    html = _fetch(FUND_URL_TEMPLATE.format(slug=slug), timeout=timeout)
    match = _NEXT_DATA_RE.search(html)
    if match is None:
        raise GrowwUnavailable(f"no __NEXT_DATA__ block on page for {slug}")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise GrowwUnavailable(f"unparseable __NEXT_DATA__ for {slug}") from exc
    data = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("mfServerSideData")
    )
    if not isinstance(data, dict) or not data.get("scheme_code"):
        raise GrowwUnavailable(f"no mfServerSideData for {slug}")
    return data


def fetch_scheme_with_retry(
    slug: str,
    *,
    attempts: int = 3,
    backoff_seconds: float = 2.0,
    timeout: int = 30,
) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            return fetch_scheme(slug, timeout=timeout)
        except GrowwUnavailable as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(backoff_seconds * (attempt + 1))
    raise GrowwUnavailable(str(last))
