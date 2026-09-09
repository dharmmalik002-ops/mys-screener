"""Every history_source label must be registered, or its volumes are zeroed.

CLAUDE.md gotcha 3 spells this out. `bhavcopy_ipo_seed` was added to bootstrap
freshly listed stocks -- and it sets avg_volume_20d/30d/50d from the listing-day
volume, which is the correct average daily volume for a one-session stock -- but
it was never registered, so _normalize_snapshot_volume_baselines zeroed exactly
those fields on load. avg_rupee_volume_30d_crore (average DAILY turnover) then
came back 0, and any liquidity threshold removed the stock.

Run: `cd backend && pytest tests/test_ipo_seed_volume_baselines.py`
"""

from __future__ import annotations

import unittest

from app.providers.free import RELIABLE_HISTORY_SOURCES, FreeMarketDataProvider


def _row(source: str) -> dict:
    return {
        "history_source": source,
        "avg_volume_20d": 400_000,
        "avg_volume_30d": 400_000,
        "avg_volume_50d": 400_000,
        "recent_volumes": [400_000],
    }


class IpoSeedVolumeBaselineTests(unittest.TestCase):
    def setUp(self):
        self.provider = FreeMarketDataProvider()

    def test_ipo_seed_is_registered(self):
        self.assertIn("bhavcopy_ipo_seed", RELIABLE_HISTORY_SOURCES)

    def test_seed_baselines_survive_normalisation(self):
        out = self.provider._normalize_snapshot_volume_baselines(_row("bhavcopy_ipo_seed"))
        self.assertEqual(out["avg_volume_20d"], 400_000)
        self.assertEqual(out["avg_volume_30d"], 400_000)
        self.assertEqual(out["recent_volumes"], [400_000])

    def test_seeded_listing_reports_average_daily_turnover(self):
        # 400k shares at Rs 250 is Rs 10 crore of average daily turnover, so a
        # Rs 1 crore threshold must keep it. Before the fix this was 0.
        snap = self.provider._normalize_snapshot_volume_baselines(_row("bhavcopy_ipo_seed"))
        turnover_crore = (snap["avg_volume_30d"] * 250.0) / 10_000_000
        self.assertGreaterEqual(turnover_crore, 1.0)

    def test_an_unregistered_source_is_still_zeroed(self):
        # The guard itself must keep working for genuinely untrusted sources.
        out = self.provider._normalize_snapshot_volume_baselines(_row("some_future_source"))
        self.assertEqual(out["avg_volume_20d"], 0)
        self.assertEqual(out["recent_volumes"], [])

    # Labels that are deliberately NOT trusted. "quote" is a live-quote row
    # with no history behind it, so it has today's volume but no 20/30/50-day
    # average -- zeroing its baselines is the correct behaviour, not a bug.
    DELIBERATELY_UNTRUSTED = {"quote"}

    def test_every_source_written_by_the_provider_is_registered_or_listed(self):
        # Catches the next time a label is introduced without a decision being
        # made about it -- which is how bhavcopy_ipo_seed silently zeroed the
        # volume baselines of every recent listing.
        import re
        from pathlib import Path
        src = Path(__file__).resolve().parents[1] / "app" / "providers" / "free.py"
        written = set(re.findall(r'"history_source":\s*"([a-z_]+)"', src.read_text()))
        undecided = written - set(RELIABLE_HISTORY_SOURCES) - self.DELIBERATELY_UNTRUSTED
        self.assertEqual(
            undecided, set(),
            f"history_source label(s) written but neither trusted nor listed as "
            f"untrusted: {sorted(undecided)} -- see CLAUDE.md gotcha 3",
        )


if __name__ == "__main__":
    unittest.main()
