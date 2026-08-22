"""Filesystem layout for the mutual-fund subsystem.

Two tiers, mirroring how the rest of `backend/data` is treated:

* `mf_universe.json` is **tracked in git** — it is the screener table itself
  (one row per fund) and must be present the moment a cold HF Space boots,
  otherwise the Funds page renders empty until a crawl finishes.
* `mf_details/` and `mf_nav/` are **gitignored** — they are per-fund blobs
  fetched lazily on first open and cached on disk, the same treatment
  `chart_cache/` already gets.

`PORTFOLIO_PATH` here is only a fallback. The user's own holdings are written
under `settings.app_state_dir`, which is where the trade journal keeps its
state too: a writable per-machine location that survives a redeploy without
being committed to the repo.
"""

from __future__ import annotations

from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = BACKEND_ROOT / "data"

UNIVERSE_PATH = DATA_DIR / "mf_universe.json"
PORTFOLIO_PATH = DATA_DIR / "mf_portfolio.json"
DETAILS_DIR = DATA_DIR / "mf_details"
NAV_DIR = DATA_DIR / "mf_nav"

# Holdings are a monthly disclosure, so a week-old detail blob is still the
# current disclosure.
DETAIL_TTL_SECONDS = 7 * 24 * 60 * 60

# NAV moves daily, but this TTL governs *serving*, not correctness: the nightly
# builder refreshes every series, and 36 hours keeps the cache warm across a
# missed or late build. Setting it to 12h instead meant that the first person to
# open a fund the morning after a build paid a live mfapi round trip on the
# request path.
NAV_TTL_SECONDS = 36 * 60 * 60


def ensure_dirs() -> None:
    for directory in (DATA_DIR, DETAILS_DIR, NAV_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def detail_path(scheme_code: str) -> Path:
    return DETAILS_DIR / f"{scheme_code}.json"


def nav_path(scheme_code: str) -> Path:
    return NAV_DIR / f"{scheme_code}.json"
