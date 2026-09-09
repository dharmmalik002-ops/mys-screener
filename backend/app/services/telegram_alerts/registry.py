"""The digest-eligible scanner catalog.

Deliberately an EXPLICIT ordered tuple rather than something derived from
``definitions.SCANS``. Two reasons:

* ``SCANS`` order is an implementation detail -- ``definitions.py`` filters the
  list at import time via ``WEAKNESS_SCAN_IDS``, so its indices can shift
  without anyone intending it.
* Inline-keyboard ``callback_data`` encodes a scanner as its INDEX in this
  tuple (Telegram caps callback_data at 64 bytes). An index is only safe if the
  ordering is a reviewable, diffable fact -- hence ``EPOCH`` below, and the
  pinned regression test in tests/test_telegram_keyboard.py.

Ids not offered in v1:
  ``ipo``          -- bypasses the snapshot staleness filter by design, which
                      fights the digest's data-freshness contract.
  ``custom-scan``  -- needs a request body.
  the POST-only runners (``momentum-burst``, ``demand-zone``, ``pull-backs``,
  ``near-pivot``, ``consolidating``, ``gap-up-openers``, ``improving-rs``,
  ``returns``) -- not reachable through ``get_scan_results``. Adding them later
  needs only an optional per-id runner here; keyboard and prefs stay unchanged.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class DigestScan:
    scan_id: str
    label: str
    page: int


# Paged by how you would actually think about the menu, not by slicing.
PAGES: tuple[str, ...] = ("Setups", "More setups", "Highs", "Extras")

DIGEST_SCANS: tuple[DigestScan, ...] = (
    # --- page 0: the setups you trade ---
    DigestScan("vcp", "VCP", 0),
    DigestScan("tight-closes", "3 Tight Closes", 0),
    DigestScan("contraction", "Contraction", 0),
    DigestScan("power-base", "Power Base", 0),
    DigestScan("minervini-1m", "Minervini 1M", 0),
    DigestScan("minervini-5m", "Minervini 5M", 0),
    DigestScan("ema-expansion", "Expansion", 0),
    DigestScan("positive-earnings", "Positive Earnings", 0),
    DigestScan("episodic-pivot", "Episodic Pivot", 0),
    DigestScan("rs-line-leads", "RS Line Leads", 0),
    DigestScan("high-tight-flag", "High Tight Flag", 0),
    DigestScan("darvas-box", "Darvas Box", 0),
    DigestScan("pivot-breakout", "Pivot Breakouts", 0),
    DigestScan("clean-pullback", "Clean Pullbacks", 0),
    DigestScan("breakout-ath", "ATH Breakouts", 0),
    DigestScan("breakout-52w", "52W Breakouts", 0),
    # --- page 1 ---
    DigestScan("breakout-range", "Range Breakouts", 1),
    DigestScan("relative-strength", "Relative Strengths", 1),
    DigestScan("volume-price", "Volume + Price", 1),
    DigestScan("strong-nifty", "Strong vs Bench", 1),
    DigestScan("strong-sector", "Strong vs Sector", 1),
    # --- page 2: plain high scans ---
    DigestScan("high-52w", "52-Week High", 2),
    DigestScan("near-52w-high", "Near 52W High", 2),
    DigestScan("all-time-high", "All-Time High", 2),
    DigestScan("near-ath", "Near ATH", 2),
    DigestScan("six-month-high", "6-Month High", 2),
    DigestScan("month-high", "Month High", 2),
    DigestScan("week-high", "Week High", 2),
    DigestScan("day-high", "Day High", 2),
    DigestScan("near-day-high", "Near Day High", 2),
    DigestScan("prev-day-high-break", "Prev Day High Break", 2),
    # --- page 3: ids synthesized by dashboard_service, not present in SCANS ---
    DigestScan("bread-butter", "Bread & Butter", 3),
    DigestScan("volume", "Volume", 3),
    DigestScan("fresh-stage2", "Fresh Stage 2", 3),
)

DIGEST_SCAN_IDS: tuple[str, ...] = tuple(scan.scan_id for scan in DIGEST_SCANS)
_BY_ID: dict[str, DigestScan] = {scan.scan_id: scan for scan in DIGEST_SCANS}

# Changes automatically whenever the tuple changes, which is what lets a stale
# inline keyboard from before a deploy be detected instead of silently toggling
# the wrong scanner. Telegram keeps old messages tappable forever.
EPOCH: str = hashlib.blake2s(
    "|".join(DIGEST_SCAN_IDS).encode("utf-8"), digest_size=3
).hexdigest()

DEFAULT_ENABLED: tuple[str, ...] = (
    "vcp",
    "contraction",
    "minervini-5m",
    "ema-expansion",
    "episodic-pivot",
)

PAGE_COUNT: int = len(PAGES)


def label(scan_id: str) -> str:
    scan = _BY_ID.get(scan_id)
    return scan.label if scan else scan_id


def is_known(scan_id: str) -> bool:
    return scan_id in _BY_ID


def index_of(scan_id: str) -> int | None:
    scan = _BY_ID.get(scan_id)
    return DIGEST_SCANS.index(scan) if scan else None


def scan_at(index: int) -> DigestScan | None:
    if 0 <= index < len(DIGEST_SCANS):
        return DIGEST_SCANS[index]
    return None


def page_scans(page: int) -> tuple[DigestScan, ...]:
    return tuple(scan for scan in DIGEST_SCANS if scan.page == page)


def page_title(page: int) -> str:
    return PAGES[page] if 0 <= page < len(PAGES) else "Scanners"


def resolve(query: str) -> list[DigestScan]:
    """Resolve a user-typed scanner for ``/run``: exact id, then a
    case-insensitive prefix/substring match on id or label."""
    text = (query or "").strip().lower()
    if not text:
        return []
    if text in _BY_ID:
        return [_BY_ID[text]]
    normalized = text.replace(" ", "-")
    if normalized in _BY_ID:
        return [_BY_ID[normalized]]
    starts = [s for s in DIGEST_SCANS if s.scan_id.startswith(normalized) or s.label.lower().startswith(text)]
    if starts:
        return starts[:8]
    return [s for s in DIGEST_SCANS if text in s.label.lower() or normalized in s.scan_id][:8]
