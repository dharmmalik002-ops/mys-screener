"""A freshly listed stock must not keep a market cap of 0 forever.

The IPO seed row cannot know shares outstanding -- the bhavcopy has no share
count -- so it writes market_cap_crore 0.0 and relies on the metadata pass to
fill it. That pass already fetched the value (_fetch_company_profile derives
issuedSize x last price) and then discarded it: the apply block copied
listing_date, sector and sub_sector only. Measured on the committed cache: 183
bhavcopy_ipo_seed rows at 0, none of them present in free_universe.json.

Run: `cd backend && pytest tests/test_market_cap_backfill.py`
"""

from __future__ import annotations

import inspect
import unittest

from app.providers.free import FreeMarketDataProvider


class MarketCapBackfillTests(unittest.TestCase):
    """Asserted against the source of the apply block.

    _fetch_market_cap_universe does network I/O over the whole universe, so it is
    not callable in a unit test; these check the contract of the block that was
    wrong rather than mocking the entire fetch path.
    """

    def setUp(self):
        self.src = inspect.getsource(FreeMarketDataProvider._fetch_market_cap_universe)

    def test_apply_block_reads_market_cap_from_metadata(self):
        self.assertIn('metadata.get("market_cap_crore")', self.src)

    def test_apply_block_writes_market_cap_onto_the_row(self):
        self.assertRegex(self.src, r'row\["market_cap_crore"\]\s*=')

    def test_it_only_fills_when_the_row_has_nothing_usable(self):
        # An established row must keep the cap it has; the bhavcopy patch
        # rescales that one by the day's price move, and overwriting it here
        # would fight that.
        self.assertRegex(self.src, r'existing_mcap\s*<=\s*0')

    def test_it_ignores_a_zero_or_missing_fetched_value(self):
        self.assertRegex(self.src, r'fetched_mcap\s+and\s+fetched_mcap\s*>\s*0')

    def test_the_seed_row_still_starts_from_zero_meaning_unknown(self):
        seed = inspect.getsource(FreeMarketDataProvider._build_ipo_seed_row)
        self.assertIn('"market_cap_crore": 0.0', seed)

    def test_the_profile_fetcher_derives_cap_from_issued_size(self):
        profile = inspect.getsource(FreeMarketDataProvider._company_profile_from_quote_payload)
        self.assertIn("issuedSize", profile)
        self.assertIn("market_cap_crore", profile)


if __name__ == "__main__":
    unittest.main()
