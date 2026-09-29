"""The IPO scanner must list every mainboard IPO, and only IPOs.

Three defects kept listings out or let non-IPOs in:

* The patch generator read only NSE series EQ, so every IPO NSE lists in the
  trade-for-trade BE series (MILKYMIST, OMFREIGHT, JSIPL, SONA ...) never got
  a snapshot row at all.
* The batch-date rule — a date carrying 10+ NSE listings is a bulk admission
  of old BSE companies — also hid the genuine IPOs that listed that day
  (MOLBIO, DHOOTTRANS on 2026-08-17), and could not see old companies admitted
  on quiet days (MODIS, ALGOQUANT). The exchanges' verdict by ISIN now wins.
* IPO seed rows were appended after the pass that consumes the patch's
  indicator block, so a listing a month old still showed one close, ADR 0,
  no trend and no turnover — and the universe gate removed it for missing data.

Run: `cd backend && pytest tests/test_ipo_listing_completeness.py`
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from app.models.market import StockSnapshot
from app.providers.free import FreeMarketDataProvider
from app.scanners import definitions
from app.services import ipo_listings
from app.services.ipo_listings import ListingVerdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import generate_bhavcopy_patch as generator  # noqa: E402


_PROVIDER = FreeMarketDataProvider()


def _snapshot(symbol: str, listed: date | None) -> StockSnapshot:
    row = _PROVIDER._build_ipo_seed_row(
        symbol, {"listing_date": ""}, {"o": 100, "h": 101, "l": 99, "c": 100, "v": 100_000, "p": 99},
        date.today(),
    )
    return StockSnapshot(**{**row, "listing_date": listed})


class ExchangeVerdictTests(unittest.TestCase):
    def setUp(self):
        self.today = date.today()
        self.batch_day = self.today - timedelta(days=40)

    def _run(self, snapshots, verdicts):
        with mock.patch.object(ipo_listings, "verdicts", return_value=verdicts):
            return {m.symbol for m in definitions.run_scan(definitions.SCAN_BY_ID["ipo"], snapshots)}

    def _batch(self):
        return [_snapshot(f"OLD{i}", self.batch_day) for i in range(definitions.IPO_BATCH_LISTING_MIN)]

    def test_a_real_ipo_on_a_bulk_admission_day_is_kept(self):
        snaps = self._batch() + [_snapshot("MOLBIO", self.batch_day)]
        hits = self._run(snaps, {"MOLBIO": ListingVerdict(True, self.batch_day)})
        self.assertEqual(hits, {"MOLBIO"})

    def test_an_old_company_admitted_on_a_quiet_day_is_dropped(self):
        quiet = self.today - timedelta(days=90)
        hits = self._run([_snapshot("MODIS", quiet)], {"MODIS": ListingVerdict(False, quiet)})
        self.assertEqual(hits, set())

    def test_unjudged_symbols_keep_the_batch_rule(self):
        quiet = self.today - timedelta(days=90)
        snaps = self._batch() + [_snapshot("NEWCO", quiet)]
        self.assertEqual(self._run(snaps, {}), {"NEWCO"})

    def test_the_verdict_supplies_a_missing_listing_date(self):
        listed = self.today - timedelta(days=20)
        hits = self._run([_snapshot("GROWW", None)], {"GROWW": ListingVerdict(True, listed)})
        self.assertEqual(hits, {"GROWW"})

    def test_the_committed_file_carries_verdicts(self):
        # The Space cannot fetch exchange files at request time; the verdicts
        # have to ship. An empty file would silently revert to the batch rule.
        loaded = ipo_listings.verdicts()
        self.assertGreater(sum(1 for v in loaded.values() if v.ipo), 50)
        self.assertGreater(sum(1 for v in loaded.values() if not v.ipo), 50)


class GeneratorListingTests(unittest.TestCase):
    CSV = (
        "SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE\n"
        "EQCO,Eq Co Limited,EQ,{d},10,1,INE000A01011,10\n"
        "BECO,Be Co Limited,BE,{d},10,1,INE000B01011,10\n"
        "BZCO,Bz Co Limited,BZ,{d},10,1,INE000C01011,10\n"
        "EQCO-RE,Eq Co Limited-RE,BE,{d},10,1,INE000A20011,10\n"
        "PPCO,Pp Co Limited,EQ,{d},10,1,IN9000D01011,10\n"
    )

    def test_trade_for_trade_listings_are_discovered_and_rights_lines_are_not(self):
        listed = date(2026, 9, 1)
        response = mock.Mock(text=self.CSV.format(d=listed.strftime("%d-%b-%Y").upper()))
        response.raise_for_status = mock.Mock()
        with mock.patch.object(generator.requests, "get", return_value=response):
            found = generator._fetch_recent_nse_listings(date(2026, 9, 29))
        self.assertEqual(set(found), {"EQCO", "BECO"})
        self.assertEqual(found["BECO"]["series"], "BE")

    def test_classification_is_by_isin_against_the_prior_session(self):
        listings = {
            "NEWIPO": {"listing_date": "2026-09-16", "isin": "INE111A01011", "name": "N", "series": "EQ"},
            "OLDCO": {"listing_date": "2026-09-16", "isin": "INE222A01011", "name": "O", "series": "EQ"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ipo_listings.json"
            with mock.patch.object(generator, "IPO_LISTINGS_PATH", path), \
                 mock.patch.object(generator, "_exchange_isins_on", return_value={"INE222A01011"}) as fetch:
                result = generator._classify_recent_listings(listings, date(2026, 9, 29))
                # 2026-09-16 is a Wednesday: the file read is Tuesday's.
                fetch.assert_called_once_with(date(2026, 9, 15))
                self.assertTrue(result["NEWIPO"]["ipo"])
                self.assertFalse(result["OLDCO"]["ipo"])
                # A second run re-uses the stored verdicts and fetches nothing.
                fetch.reset_mock()
                generator._classify_recent_listings(listings, date(2026, 9, 30))
                fetch.assert_not_called()

    def test_an_unreadable_prior_session_leaves_the_listing_unjudged(self):
        listings = {"X": {"listing_date": "2026-09-16", "isin": "INE333A01011", "name": "X", "series": "EQ"}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ipo_listings.json"
            with mock.patch.object(generator, "IPO_LISTINGS_PATH", path), \
                 mock.patch.object(generator, "_exchange_isins_on", return_value=None):
                result = generator._classify_recent_listings(listings, date(2026, 9, 29))
        self.assertNotIn("ipo", result["X"])


class SeedRowIndicatorTests(unittest.TestCase):
    def test_a_seeded_listing_receives_the_patch_indicator_block(self):
        patch_date = date.today()
        closes = [100.0, 104.0, 102.0, 108.0, 110.0, 112.0]
        block = {
            "d": patch_date.isoformat(),
            "rc": closes, "rh": [c * 1.03 for c in closes], "rl": [c * 0.97 for c in closes],
            "rv": [500_000] * len(closes), "adr": 6.1, "av20": 500_000, "av30": 500_000,
            "av50": 500_000, "b5": 104.0, "b20": 100.0,
        }
        provider = FreeMarketDataProvider()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            provider.backend_root = root
            provider.snapshot_cache_path = root / "data" / "free_snapshots.json"
            existing = provider._build_ipo_seed_row(
                "OLDROW", {"listing_date": "2020-01-01"}, {"o": 50, "h": 51, "l": 49, "c": 50, "v": 1000, "p": 50},
                patch_date - timedelta(days=1),
            )
            provider.snapshot_cache_path.write_text(json.dumps([existing]), encoding="utf-8")
            (root / "data" / "bhavcopy_patch.json").write_text(json.dumps({
                "date": patch_date.isoformat(),
                "source": "YFINANCE",
                "symbols": {
                    "OLDROW": {"o": 50, "h": 51, "l": 49, "c": 50.5, "v": 1200, "p": 50},
                    "NEWIPO": {"o": 111, "h": 113, "l": 109, "c": 112, "v": 500_000, "p": 110, "i": block},
                },
                "new_listings": {"NEWIPO": {"listing_date": (patch_date - timedelta(days=8)).isoformat(), "name": "New"}},
            }), encoding="utf-8")
            result = provider.apply_committed_bhavcopy_patch(force=True)
            rows = {r["symbol"]: r for r in json.loads(provider.snapshot_cache_path.read_text())}
        self.assertEqual(result["status"], "ok")
        seeded = rows["NEWIPO"]
        self.assertEqual(seeded["recent_closes"], closes)
        self.assertEqual(seeded["adr_pct_20"], 6.1)
        self.assertEqual(seeded["avg_volume_30d"], 500_000)

    def test_a_short_block_is_accepted_only_for_a_fresh_listing(self):
        provider = FreeMarketDataProvider()
        today = date.today()
        fresh = {"listing_date": (today - timedelta(days=3)).isoformat()}
        established = {"listing_date": "2015-01-01"}
        self.assertEqual(provider._min_indicator_bars(fresh, today), 2)
        self.assertEqual(provider._min_indicator_bars(established, today), 5)
        self.assertEqual(provider._min_indicator_bars({}, today), 5)

    def test_a_seed_row_carries_sector_and_market_cap_from_the_listing(self):
        provider = FreeMarketDataProvider()
        row = provider._build_ipo_seed_row(
            "AUGMONT",
            {"listing_date": "2026-08-31", "sector": "Metals & Mining", "sub_sector": "Precious Metals", "shares_crore": 9.137354},
            {"o": 960, "h": 970, "l": 950, "c": 964.75, "v": 100_000, "p": 950},
            date(2026, 9, 28),
        )
        self.assertEqual((row["sector"], row["sub_sector"]), ("Metals & Mining", "Precious Metals"))
        self.assertAlmostEqual(row["market_cap_crore"], 9.137354 * 964.75, places=1)

    def test_an_existing_row_gains_a_missing_listing_date(self):
        # Without it a sub-floor row fails the universe filter and never
        # reaches the IPO scan (TAALTECH on the Space).
        provider = FreeMarketDataProvider()
        row = {"symbol": "TAALTECH", "listing_date": None, "last_price": 1069.1, "sector": "IT", "market_cap_crore": 333.0}
        provider._fill_seed_row_reference(row, {"listing_date": "2026-04-20"})
        self.assertEqual(row["listing_date"], "2026-04-20")
        self.assertEqual(row["market_cap_crore"], 333.0)

    def test_a_nan_price_never_becomes_a_seed_row(self):
        # One NaN row fails the schema check for the whole snapshot file.
        provider = FreeMarketDataProvider()
        nan = float("nan")
        self.assertIsNone(provider._build_ipo_seed_row(
            "SONA", {"listing_date": "2026-09-24"}, {"o": nan, "h": nan, "l": nan, "c": nan, "v": 0, "p": 100}, date.today(),
        ))


if __name__ == "__main__":
    unittest.main()
