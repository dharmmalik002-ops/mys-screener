"""
Generate volume_history.json — the rolling "volume push" list that powers the
Volume screener.

Run by the daily-bhavcopy GitHub Action after the EOD bhavcopy patch. It reads
the NSE-keyed universe, batch-downloads ~1.4 years of daily Volume via yfinance,
and for every symbol finds its MOST RECENT day, within the last
``PERSIST_SESSIONS`` (~1 trading month), on which it printed a new volume high
over any of the nested windows:

    21 sessions  ≈ 1 month   (tier 1, "Monthly")
    63 sessions  ≈ 3 months  (tier 2, "Quarterly")
    126 sessions ≈ 6 months  (tier 3, "Half-yearly")
    252 sessions ≈ 1 year    (tier 4, "Yearly")

Because the windows are nested, clearing a longer window implies clearing every
shorter one, so a single "tier" = the LONGEST window the volume cleared that day.

Output is compact — only the symbols that pushed a new high in the retention
window, each as ``[offset, tier, volume, prior_peak]``:

    offset      sessions ago the push happened (0 = latest EOD session)
    tier        1..4 (longest window cleared)
    volume      the pushing session's volume
    prior_peak  the prior window peak it beat (for the tier's window)

    {
      "date": "YYYY-MM-DD", "updated_at": "...", "windows": [21,63,126,252],
      "persist_sessions": 21,
      "symbols": {"RELIANCE": [0, 2, 41027699, 30957881], ...}
    }

The backend serves this as a single recency-sorted list (newest push on top),
retaining each stock for ~1 month so post-push behavior can be tracked.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
UNIVERSE_PATH = DATA_DIR / "free_universe.json"
OUTPUT_PATH = DATA_DIR / "volume_history.json"

WINDOWS = [21, 63, 126, 252]   # tier 1..4 (Monthly / Quarterly / Half-yearly / Yearly)
PERSIST_SESSIONS = 21          # keep a pusher listed for ~1 trading month
MIN_TIER = 2                   # only record Quarterly+ pushes (drop noisy Monthly-only)
SURGE_MULT = 1.5               # push must be ≥ this × the prior 1-month avg volume
CHUNK = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("volume_history")


def _load_tickers() -> list[str]:
    if not UNIVERSE_PATH.exists():
        logger.error("universe not found: %s", UNIVERSE_PATH)
        return []
    try:
        universe = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("failed to read universe: %s", exc)
        return []
    if not isinstance(universe, list):
        return []
    return [
        str(item.get("ticker", ""))
        for item in universe
        if str(item.get("ticker", "")).endswith(".NS")
    ]


def _recent_volume_event(vols: list[int]) -> list[int] | None:
    """Return [offset, tier, volume, prior_peak] for the most recent new-high
    push of at least MIN_TIER (Quarterly+) within the last PERSIST_SESSIONS, or
    None if there wasn't one. Monthly-only pushes (tier 1) are skipped so the
    screener surfaces only the more significant quarterly / half-yearly / yearly
    volume highs.

    A day qualifies for tier ``ti`` (window N) only when the FULL N-session
    prior window is available AND the day's volume STRICTLY exceeds that
    window's peak. A qualifying day must also be a genuine surge — at least
    SURGE_MULT × the prior 1-month average volume — so flat or slowly-drifting
    volume never registers as a "push". Nested windows ⇒ stop escalating at the
    first window the day fails."""
    base = WINDOWS[0]
    vols = [v for v in vols if v > 0]
    n_total = len(vols)
    # Need at least a full 1-month prior window for the latest session.
    if n_total < base + 2:
        return None

    for k in range(PERSIST_SESSIONS):
        idx = n_total - 1 - k
        if idx < base:
            break  # not enough prior history for the 1-month baseline/window
        vol_k = vols[idx]
        if vol_k <= 0:
            continue
        # Magnitude gate: must be a real surge over the recent 1-month baseline.
        baseline = sum(vols[idx - base : idx]) / base
        if baseline <= 0 or vol_k < SURGE_MULT * baseline:
            continue
        tier = 0
        peak = 0
        for ti, window in enumerate(WINDOWS, start=1):
            start = idx - window
            if start < 0:
                break  # not enough history for this (or any longer) window
            prior = vols[start:idx]
            if len(prior) < window:
                break
            prior_peak = max(prior)
            if prior_peak > 0 and vol_k > prior_peak:  # STRICT new high
                tier = ti
                peak = prior_peak
            else:
                break  # failed this window ⇒ can't clear any longer one
        if tier >= MIN_TIER:
            # First (smallest k) qualifying day is the most recent Quarterly+ push.
            return [k, tier, int(vol_k), int(peak)]
    return None


def main() -> int:
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:
        logger.error("yfinance/pandas not installed; cannot build volume history")
        return 1

    tickers = _load_tickers()
    if not tickers:
        logger.error("no .NS tickers in universe; aborting")
        return 1
    logger.info("Scanning volume pushes for %s tickers", len(tickers))

    end = datetime.now(IST).date() + timedelta(days=1)
    start = end - timedelta(days=430)  # ~1.4y calendar → ≥ 273 trading sessions
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    symbols: dict[str, list[int]] = {}

    def _event_with_date(series) -> list | None:
        """Run push detection and append the push DATE (YYYY-MM-DD) so the chart
        can mark the exact day. We zero-filter with dates kept in lock-step with
        the volumes (mirroring _recent_volume_event's internal v>0 filter) so the
        returned offset maps to the correct date."""
        pairs: list[tuple] = []
        for ts, value in zip(series.index, series.values):
            if value is None or pd.isna(value):
                continue
            try:
                vol = int(float(value))
            except (TypeError, ValueError):
                continue
            if vol > 0:
                pairs.append((ts, vol))
        if not pairs:
            return None
        vols = [v for _, v in pairs]
        event = _recent_volume_event(vols)
        if not event:
            return None
        offset = event[0]
        if 0 <= offset < len(pairs):
            push_ts = pairs[-1 - offset][0]
            try:
                event = event + [push_ts.strftime("%Y-%m-%d")]
            except Exception:
                pass
        return event

    for offset in range(0, len(tickers), CHUNK):
        chunk = tickers[offset : offset + CHUNK]
        try:
            df = yf.download(
                chunk,
                start=start_str,
                end=end_str,
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            logger.warning("batch %s failed: %s", offset // CHUNK, exc)
            continue
        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            try:
                vol_df = df["Volume"]
            except Exception:
                continue
            for ticker in chunk:
                if ticker not in vol_df.columns:
                    continue
                event = _event_with_date(vol_df[ticker].dropna())
                if event:
                    symbols[ticker.replace(".NS", "")] = event
        else:
            if "Volume" in df.columns:
                event = _event_with_date(df["Volume"].dropna())
                if event:
                    symbols[chunk[0].replace(".NS", "")] = event

        logger.info("processed %s/%s tickers (%s pushes so far)", min(offset + CHUNK, len(tickers)), len(tickers), len(symbols))

    if not symbols:
        logger.warning("no volume pushes found in retention window; writing empty set")

    payload = {
        "date": datetime.now(IST).date().isoformat(),
        "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "windows": WINDOWS,
        "persist_sessions": PERSIST_SESSIONS,
        "symbols": symbols,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    logger.info("wrote %s with %s volume-push symbols", OUTPUT_PATH, len(symbols))
    return 0


if __name__ == "__main__":
    sys.exit(main())
