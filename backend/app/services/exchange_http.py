"""HTTP sessions that BSE's and NSE's JSON APIs will answer.

Both APIs sit behind Akamai, which refuses a client by its TLS / HTTP-2
fingerprint rather than by its IP or headers. From 2026-09-23 every call
from the GitHub runners to ``api.bseindia.com`` came back 403 "Access Denied"
with ``requests`` — the earnings calendar, the result filings, the quarterly
results and the IPO list all stopped updating — while the same calls from
``curl_cffi`` impersonating Chrome, on the same runner, returned 200.

``curl_cffi`` ships with yfinance, so it is present wherever this app runs;
when it is not, the plain ``requests`` session is returned and behaves as
before. The returned session has the ``requests`` call shape (``get``,
``params=``, ``timeout=``, ``status_code``, ``json()``, ``raise_for_status()``)
and is NOT thread-safe — give each thread its own.
"""
from __future__ import annotations

from typing import Any, Mapping

import requests

IMPERSONATE = "chrome"


def browser_session(headers: Mapping[str, str] | None = None) -> Any:
    """A session presenting a real browser's TLS fingerprint when possible."""
    try:
        from curl_cffi import requests as cffi_requests

        session = cffi_requests.Session(impersonate=IMPERSONATE)
        # The impersonated browser sends its own matching User-Agent; a
        # different one on top would contradict the fingerprint.
        extra = {k: v for k, v in (headers or {}).items() if k.lower() != "user-agent"}
    except Exception:  # noqa: BLE001 - curl_cffi missing or unusable
        session = requests.Session()
        extra = dict(headers or {})
    session.headers.update(extra)
    return session
