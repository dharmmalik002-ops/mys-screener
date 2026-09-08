"""Guards that the app emits ONE sector vocabulary.

Yahoo returns GICS-style sector names; NSE/BSE metadata returns NSE macro-sector
names, and most of the universe is labelled from the latter. When the alias map
was only half-complete, 99 stocks (6% of the live universe) kept GICS labels and
produced seven duplicate sectors -- Energy next to Oil Gas & Consumable Fuels,
Utilities next to Power, Real Estate next to Realty -- which split each real
sector's breadth and returns and left the market map full of unlabelled slivers.

Run: `cd backend && pytest tests/test_sector_taxonomy.py`
"""

from __future__ import annotations

import unittest

from app.core.sector_taxonomy import NSE_MACRO_SECTORS as CANONICAL_SECTORS
from app.core.sector_taxonomy import SECTOR_ALIASES, normalize_sector
from app.models.market import StockSnapshot
from app.providers.free import YAHOO_SECTOR_ALIASES, FreeMarketDataProvider

# The NSE macro sectors the universe actually uses. Every alias has to land on
# one of these; anything else means we have invented a sector.
NSE_MACRO_SECTORS = {
    "Automobile and Auto Components",
    "Capital Goods",
    "Chemicals",
    "Construction",
    "Construction Materials",
    "Consumer Durables",
    "Consumer Services",
    "Diversified",
    "Fast Moving Consumer Goods",
    "Financial Services",
    "Forest Materials",
    "Healthcare",
    "Information Technology",
    "Media Entertainment & Publication",
    "Metals & Mining",
    "Oil Gas & Consumable Fuels",
    "Power",
    "Realty",
    "Services",
    "Telecommunication",
    "Textiles",
    "Unclassified",
}

# Everything Yahoo can hand us. Each of these previously reached the UI verbatim
# or landed on another GICS name.
GICS_SECTOR_NAMES = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Materials",
    "Real Estate",
    "Technology",
    "Utilities",
]


def _snapshot(sector: str) -> StockSnapshot:
    return StockSnapshot(
        symbol="X", name="X", exchange="NSE", sector=sector,
        market_cap_crore=1.0, last_price=1.0, change_pct=0.0, volume=1,
        avg_volume_20d=1, day_high=1.0, day_low=1.0, ath=1.0, high_52w=1.0,
        range_high_20d=1.0, benchmark_return_20d=0.0, sector_return_20d=0.0,
        pivot_high=1.0, darvas_high=1.0, darvas_low=1.0,
        pullback_depth_pct=0.0, trend_strength=0.0,
    )


class SectorTaxonomyTests(unittest.TestCase):
    def test_module_and_test_agree_on_the_canonical_set(self):
        # If someone adds an NSE sector, both lists have to learn about it.
        self.assertEqual(set(CANONICAL_SECTORS), NSE_MACRO_SECTORS)

    def test_snapshot_normalises_regardless_of_assembly_path(self):
        # The real fix: patching free.py's seven sector sites left 40 stocks on
        # GICS labels in production. The model validator is the choke point.
        for name in GICS_SECTOR_NAMES:
            with self.subTest(sector=name):
                self.assertIn(_snapshot(name).sector, NSE_MACRO_SECTORS)

    def test_snapshot_tolerates_blank_and_odd_spacing(self):
        self.assertEqual(_snapshot("").sector, "Unclassified")
        self.assertEqual(_snapshot("  Capital   Goods ").sector, "Capital Goods")

    def test_case_insensitive(self):
        self.assertEqual(normalize_sector("real estate"), "Realty")
        self.assertEqual(normalize_sector("INDUSTRIALS"), "Capital Goods")

    def test_unknown_label_is_kept_not_swallowed(self):
        # A brand-new NSE sector should surface as itself rather than vanish
        # into Unclassified, which would hide a real classification change.
        self.assertEqual(normalize_sector("Space Logistics"), "Space Logistics")

    def test_provider_and_module_share_one_map(self):
        self.assertIs(YAHOO_SECTOR_ALIASES, SECTOR_ALIASES)
    def test_every_gics_name_normalises_to_an_nse_macro_sector(self):
        for name in GICS_SECTOR_NAMES:
            with self.subTest(sector=name):
                resolved = FreeMarketDataProvider._normalize_sector_label(name)
                self.assertIn(
                    resolved,
                    NSE_MACRO_SECTORS,
                    f"{name!r} normalised to {resolved!r}, which is not an NSE macro sector",
                )

    def test_no_alias_lands_on_another_gics_only_name(self):
        # The specific bug: "Basic Materials" -> "Materials" swapped one GICS
        # label for another, so the duplicate sector survived normalisation.
        gics_only = {n for n in GICS_SECTOR_NAMES if n not in NSE_MACRO_SECTORS}
        for source, target in YAHOO_SECTOR_ALIASES.items():
            with self.subTest(alias=f"{source} -> {target}"):
                self.assertNotIn(
                    target,
                    gics_only,
                    f"alias {source!r} -> {target!r} still points at a GICS-only name",
                )

    def test_nse_labels_pass_through_untouched(self):
        for name in sorted(NSE_MACRO_SECTORS):
            with self.subTest(sector=name):
                self.assertEqual(FreeMarketDataProvider._normalize_sector_label(name), name)

    def test_normalisation_is_idempotent(self):
        for name in GICS_SECTOR_NAMES:
            once = FreeMarketDataProvider._normalize_sector_label(name)
            twice = FreeMarketDataProvider._normalize_sector_label(once)
            with self.subTest(sector=name):
                self.assertEqual(once, twice)

    def test_the_seven_duplicates_seen_in_production_are_gone(self):
        # Exactly the labels measured on the live /api/groups payload.
        observed = {
            "Industrials": "Capital Goods",
            "Materials": "Metals & Mining",
            "Consumer Discretionary": "Consumer Durables",
            "Utilities": "Power",
            "Real Estate": "Realty",
            "Consumer Staples": "Fast Moving Consumer Goods",
            "Energy": "Oil Gas & Consumable Fuels",
        }
        for source, expected in observed.items():
            with self.subTest(sector=source):
                self.assertEqual(FreeMarketDataProvider._normalize_sector_label(source), expected)


if __name__ == "__main__":
    unittest.main()
