#!/usr/bin/env python3
"""Daily data-health check for the live stock-scanner backend.

Personal-use monitoring: instead of noticing stale numbers by opening the app,
this runs on a schedule in GitHub Actions and *fails the workflow* when the live
backend is behind the data we actually have committed — GitHub then emails the
repo owner. Hitting the endpoints also wakes the (sleeping) HF Space, which
triggers the backend's own traffic self-heal, so this both verifies and nudges.

Failure modes it catches:
  * backend unreachable
  * live bhavcopy date behind the latest committed patch  (delivery/apply lag)
  * live XP-breadth date behind the latest committed XP history
  * XP date out of sync with the bhavcopy date on the backend

Soft signals (reported, not failed — they're intermittent upstream issues):
  * degraded feed source (YFINANCE-only instead of the full YF+BSE merge)
  * committed data itself older than the latest expected trading session
    (generator likely didn't run today; skipped on weekends to avoid noise)

Zero third-party deps (urllib only) so it runs on a bare CI Python.
"""
from __future__ import annotations

import json
import ssl
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))
BACKEND = "https://dharmmalik-stock-scanner-backend.hf.space"
REPO_ROOT = Path(__file__).resolve().parents[1]  # .../backend


def _get_json(url: str, timeout: int = 25) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "data-health-check"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        # Some hosts (e.g. a bare macOS Python) lack a CA bundle, surfacing the
        # SSL failure wrapped in URLError. This is a read-only health check
        # against our own public endpoint, so an unverified retry is acceptable.
        if "CERTIFICATE" not in str(exc).upper() and "SSL" not in str(exc).upper():
            raise
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode("utf-8"))


def _committed_bhavcopy_date() -> str:
    try:
        doc = json.loads((REPO_ROOT / "data" / "bhavcopy_patch.json").read_text(encoding="utf-8"))
        return str(doc.get("date") or "")
    except Exception:
        return ""


def _committed_xp_date() -> str:
    try:
        doc = json.loads((REPO_ROOT / "data" / "xp_breadth_history.json").read_text(encoding="utf-8"))
        latest = doc.get("latest") if isinstance(doc, dict) else None
        if isinstance(latest, dict) and latest.get("date"):
            return str(latest["date"])
        days = doc.get("days") if isinstance(doc, dict) else None
        if isinstance(days, list) and days:
            return str(days[-1].get("date") or "")
    except Exception:
        pass
    return ""


def _latest_expected_trading_date() -> date:
    now = datetime.now(IST)
    eod_ready = now.hour * 60 + now.minute >= 16 * 60 + 45  # EOD published/applied by ~4:45 PM IST
    cand = now.date() if (now.weekday() < 5 and eod_ready) else now.date() - timedelta(days=1)
    while cand.weekday() >= 5:  # roll Sat/Sun back to Friday
        cand -= timedelta(days=1)
    return cand


def main() -> int:
    problems: list[str] = []
    notes: list[str] = []

    committed_bhav = _committed_bhavcopy_date()
    committed_xp = _committed_xp_date()

    # Live backend.
    try:
        status = _get_json(f"{BACKEND}/api/bhavcopy/status")
        dashboard = _get_json(f"{BACKEND}/api/dashboard?market=india")
    except Exception as exc:
        print(f"FAIL: backend unreachable — {exc}")
        return 1

    live_bhav = str(status.get("date") or "")
    live_source = str(status.get("source") or "")
    xp = dashboard.get("xp_breadth") or {}
    live_xp = str(xp.get("date") or "")

    print("── data-health-check ─────────────────────────────")
    print(f"expected latest trading session : {_latest_expected_trading_date().isoformat()}")
    print(f"committed bhavcopy / xp          : {committed_bhav or '?'} / {committed_xp or '?'}")
    print(f"live bhavcopy / xp               : {live_bhav or '?'} / {live_xp or '?'}   source={live_source or '?'}")

    # Hard failures — the backend is behind data we already have committed.
    if committed_bhav and live_bhav and live_bhav < committed_bhav:
        problems.append(f"live bhavcopy {live_bhav} is BEHIND committed {committed_bhav} (backend didn't apply the latest patch)")
    if committed_xp and live_xp and live_xp < committed_xp:
        problems.append(f"live XP breadth {live_xp} is BEHIND committed {committed_xp} (XP self-heal didn't run)")
    if live_bhav and live_xp and live_xp < live_bhav:
        problems.append(f"live XP breadth {live_xp} is out of sync with live bhavcopy {live_bhav}")

    # Soft signals — reported so the email is informative, but not a hard fail.
    expected = _latest_expected_trading_date().isoformat()
    if committed_bhav and committed_bhav < expected:
        notes.append(f"committed bhavcopy {committed_bhav} < expected {expected} — daily generator may not have run (or it's a holiday)")
    if live_source and live_source.upper() not in ("YF+BSE", "BSE"):
        notes.append(f"feed source is degraded ({live_source}) — BSE primary likely failed; coverage/XP may hold")

    for n in notes:
        print(f"NOTE: {n}")
    if problems:
        for p in problems:
            print(f"FAIL: {p}")
        return 1
    print("OK: live backend is current with the committed data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
