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


class SectorTaxonomyTests(unittest.TestCase):
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
