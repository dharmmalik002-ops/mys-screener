"""Which recent NSE listings were IPOs, as decided by the exchanges' own files.

NSE's DATE OF LISTING marks when a company started trading on NSE: an IPO,
but also an old BSE company admitted to NSE or an SME moving to the mainboard.
`scripts/generate_bhavcopy_patch.py` settles each one by ISIN against the last
BSE/NSE EOD file before its listing date and commits the verdict to
`data/ipo_listings.json`; this module only reads it.

A symbol missing from the file (or with no verdict) returns None, and the IPO
scanner falls back to its batch-date rule for it.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import NamedTuple

ARTIFACT_PATH = Path(__file__).resolve().parents[2] / "data" / "ipo_listings.json"


class ListingVerdict(NamedTuple):
    ipo: bool
    listing_date: date | None


_cache: tuple[float, dict[str, ListingVerdict], dict[str, str]] | None = None


def _load() -> tuple[dict[str, ListingVerdict], dict[str, str]]:
    global _cache
    try:
        mtime = ARTIFACT_PATH.stat().st_mtime
    except OSError:
        return {}, {}
    if _cache is not None and _cache[0] == mtime:
        return _cache[1], _cache[2]
    try:
        payload = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
        listings = payload.get("listings") if isinstance(payload, dict) else None
        raw_aliases = payload.get("aliases") if isinstance(payload, dict) else None
    except Exception:
        listings = None
        raw_aliases = None
    aliases = {
        str(old).strip().upper(): str(new).strip().upper()
        for old, new in (raw_aliases if isinstance(raw_aliases, dict) else {}).items()
    }
    parsed: dict[str, ListingVerdict] = {}
    for symbol, entry in (listings or {}).items():
        if not isinstance(entry, dict) or not isinstance(entry.get("ipo"), bool):
            continue
        try:
            listed = date.fromisoformat(str(entry.get("listing_date")))
        except ValueError:
            listed = None
        parsed[str(symbol).strip().upper()] = ListingVerdict(entry["ipo"], listed)
    _cache = (mtime, parsed, aliases)
    return parsed, aliases


def verdicts() -> dict[str, ListingVerdict]:
    return _load()[0]


def renamed_symbols() -> dict[str, str]:
    """Old symbol -> the symbol NSE lists the same company under now."""
    return _load()[1]
