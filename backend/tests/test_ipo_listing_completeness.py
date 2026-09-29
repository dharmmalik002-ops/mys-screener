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

    def test_a_renamed_symbol_is_listed_once(self):
        listed = self.today - timedelta(days=100)
        snaps = [_snapshot("AMIRCHAND", listed), _snapshot("AEROPLANE", listed)]
        with mock.patch.object(ipo_listings, "renamed_symbols", return_value={"AMIRCHAND": "AEROPLANE"}):
            hits = self._run(snaps, {"AEROPLANE": ListingVerdict(True, listed)})
        self.assertEqual(hits, {"AEROPLANE"})

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

    def _classify(self, listings, *, scrips=None, bse_ipos=None, prior=None, master=None, previous=None, stored=None, when=date(2026, 9, 29)):
        """Run the classifier with every exchange call stubbed."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ipo_listings.json"
            if stored is not None:
                path.write_text(json.dumps(stored), encoding="utf-8")
            elif previous is not None:
                path.write_text(json.dumps({"listings": previous}), encoding="utf-8")
            with mock.patch.object(generator, "IPO_LISTINGS_PATH", path), \
                 mock.patch.object(generator, "DATA_DIR", Path(tmp)), \
                 mock.patch.object(generator, "_bse_scrips", return_value=scrips or {}), \
                 mock.patch.object(generator, "_bse_mainboard_ipos", return_value=bse_ipos), \
                 mock.patch.object(generator, "_exchange_isins_on", return_value=prior) as fetch, \
                 mock.patch.object(generator, "_attach_listing_reference", return_value=0), \
                 mock.patch.object(generator, "_NSE_MASTER_ISINS", set(master or ())):
                result = generator._classify_recent_listings(listings, when)
                saved = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
        return result, fetch, saved

    def test_bse_ipo_list_decides_anything_that_trades_on_bse(self):
        # A demerger (TMCV) is a new ISIN that never traded — the history
        # test calls it an IPO; BSE's list of public issues does not.
        listings = {
            "REALIPO": {"listing_date": "2026-09-16", "isin": "INE111A01011", "name": "R", "series": "EQ"},
            "DEMERGED": {"listing_date": "2026-09-16", "isin": "INE222A01011", "name": "D", "series": "EQ"},
        }
        scrips = {
            "INE111A01011": {"code": "544900", "symbol": "REALIPO", "group": "B"},
            "INE222A01011": {"code": "544901", "symbol": "DEMERGED", "group": "B"},
        }
        bse_ipos = {"544900": {"name": "R", "symbol": "REALIPO", "listing_date": "2026-09-16", "issue_price": 100}}
        result, fetch, _ = self._classify(listings, scrips=scrips, bse_ipos=bse_ipos, prior=(set(), True))
        self.assertTrue(result["REALIPO"]["ipo"])
        self.assertFalse(result["DEMERGED"]["ipo"])
        self.assertEqual(result["DEMERGED"]["basis"], "bse_ipo_list")
        fetch.assert_not_called()  # no history needed when BSE has ruled

    def test_an_ipo_listed_on_bse_alone_is_added(self):
        # National Stock Exchange of India cannot list on NSE.
        scrips = {
            "INE721I01024": {"code": "544937", "symbol": "NSE", "group": "A"},
            "INE999Z01011": {"code": "544950", "symbol": "SMEONE", "group": "M"},
        }
        bse_ipos = {
            "544937": {"name": "National Stock Exchange of India Limited", "symbol": "NSE", "listing_date": "2026-09-24", "issue_price": 1500},
            "544950": {"name": "Sme", "symbol": "SMEONE", "listing_date": "2026-09-24", "issue_price": 10},
        }
        listings = {"OTHER": {"listing_date": "2026-09-16", "isin": "INE111A01011", "name": "O", "series": "EQ"}}
        result, _, _ = self._classify(listings, scrips=scrips, bse_ipos=bse_ipos, prior=(set(), True))
        self.assertEqual(result["NSE"]["exchange"], "BSE")
        self.assertTrue(result["NSE"]["ipo"])
        self.assertEqual(result["NSE"]["listing_date"], "2026-09-24")
        self.assertNotIn("SMEONE", result)  # SME board groups are not mainboard

    def test_a_bse_ipo_that_is_also_on_nse_is_not_added_twice(self):
        scrips = {"INE111A01011": {"code": "544900", "symbol": "DUALBSE", "group": "B"}}
        bse_ipos = {"544900": {"name": "Dual", "symbol": "DUALBSE", "listing_date": "2026-09-16", "issue_price": 1}}
        result, _, _ = self._classify({}, scrips=scrips, bse_ipos=bse_ipos, master={"INE111A01011"})
        self.assertEqual(result, {})  # empty NSE master => stored verdicts kept, nothing invented
        listings = {"DUAL": {"listing_date": "2026-09-16", "isin": "INE111A01011", "name": "Dual", "series": "EQ"}}
        result, _, _ = self._classify(listings, scrips=scrips, bse_ipos=bse_ipos, master={"INE111A01011"})
        self.assertEqual(set(result), {"DUAL"})

    def test_a_night_the_bse_api_refuses_keeps_the_stored_list(self):
        # 2026-09-29: the runner read BSE's EOD files but no API call; every
        # verdict reverted to the history test, the demergers came back and
        # National Stock Exchange (BSE-only) dropped out.
        scrips = {
            "INE721I01024": {"code": "544937", "symbol": "NSE", "group": "A"},
            "INE1TAE01010": {"code": "544569", "symbol": "TMCV", "group": "A"},
        }
        stored = {
            "bse_ipos_as_of": "2026-09-28",
            "bse_ipos": {"544937": {"name": "National Stock Exchange of India Limited", "symbol": "NSE", "listing_date": "2026-09-24", "issue_price": 1500}},
            "listings": {},
        }
        listings = {"TMCV": {"listing_date": "2025-11-12", "isin": "INE1TAE01010", "name": "Tata Motors", "series": "EQ"}}
        result, fetch, saved = self._classify(listings, scrips=scrips, bse_ipos=None, prior=(set(), True), stored=stored)
        self.assertFalse(result["TMCV"]["ipo"])       # demerger stays out
        self.assertTrue(result["NSE"]["ipo"])         # BSE-only IPO stays in
        fetch.assert_not_called()
        self.assertEqual(saved["bse_ipos_as_of"], "2026-09-28")  # not advanced by a failed read
        self.assertIn("544937", saved["bse_ipos"])

    def test_a_listing_newer_than_the_stored_list_waits_for_history(self):
        scrips = {"INE0M5301040": {"code": "544942", "symbol": "VARMORA", "group": "B"}}
        stored = {"bse_ipos_as_of": "2026-09-28", "bse_ipos": {}, "listings": {}}
        listings = {"VARMORA": {"listing_date": "2026-09-29", "isin": "INE0M5301040", "name": "V", "series": "EQ"}}
        result, _, _ = self._classify(listings, scrips=scrips, bse_ipos=None, prior=(set(), True), stored=stored)
        self.assertEqual(result["VARMORA"]["basis"], "isin_history")
        self.assertTrue(result["VARMORA"]["ipo"])

    def test_nse_only_history_is_matched_on_the_issuer_prefix(self):
        # LEMERITE traded on NSE Emerge as ...01017 and listed as ...01025.
        listings = {"LEMERITE": {"listing_date": "2025-12-12", "isin": "INE0G1L01025", "name": "L", "series": "EQ"}}
        result, fetch, _ = self._classify(listings, bse_ipos={}, prior=({"INE0G1L01017"}, True))
        fetch.assert_called_once_with(date(2025, 12, 11))
        self.assertFalse(result["LEMERITE"]["ipo"])

    def test_never_traded_needs_nse_read_before_it_counts(self):
        listings = {"NSEONLY": {"listing_date": "2026-09-16", "isin": "INE333A01011", "name": "X", "series": "EQ"}}
        result, _, _ = self._classify(listings, bse_ipos={}, prior=({"INE999A01011"}, False))
        self.assertNotIn("ipo", result["NSEONLY"])  # BSE alone cannot see NSE Emerge
        result, _, _ = self._classify(listings, bse_ipos={}, prior=({"INE999A01011"}, True))
        self.assertTrue(result["NSEONLY"]["ipo"])

    def test_checked_evidence_is_reused(self):
        listings = {"NSEONLY": {"listing_date": "2026-09-16", "isin": "INE333A01011", "name": "X", "series": "EQ"}}
        first, _, _ = self._classify(listings, bse_ipos={}, prior=(set(), True))
        _, fetch, _ = self._classify(listings, bse_ipos={}, prior=(set(), True), previous=first)
        fetch.assert_not_called()

    def test_an_unreadable_bse_list_falls_back_to_history(self):
        listings = {"X": {"listing_date": "2026-09-16", "isin": "INE333A01011", "name": "X", "series": "EQ"}}
        scrips = {"INE333A01011": {"code": "544900", "symbol": "X", "group": "B"}}
        result, _, _ = self._classify(listings, scrips=scrips, bse_ipos=None, prior=(set(), True))
        self.assertEqual(result["X"]["basis"], "isin_history")
        self.assertTrue(result["X"]["ipo"])

    def test_an_unreadable_prior_session_leaves_the_listing_unjudged(self):
        listings = {"X": {"listing_date": "2026-09-16", "isin": "INE333A01011", "name": "X", "series": "EQ"}}
        result, _, _ = self._classify(listings, bse_ipos={}, prior=None)
        self.assertNotIn("ipo", result["X"])

    def test_renamed_symbols_are_recorded(self):
        listings = {"AEROPLANE": {"listing_date": "2026-04-02", "isin": "INE05TO01019", "name": "Amir Chand", "series": "EQ"}}
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "free_universe.json").write_text(json.dumps([{"symbol": "AMIRCHAND", "isin": "INE05TO01019"}]))
            with mock.patch.object(generator, "DATA_DIR", Path(tmp)):
                aliases = generator._renamed_symbols({"AEROPLANE": {"isin": "INE05TO01019"}})
        self.assertEqual(aliases, {"AMIRCHAND": "AEROPLANE"})


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

    def test_a_bse_only_ipo_is_seeded_on_its_bse_ticker(self):
        row = FreeMarketDataProvider()._build_ipo_seed_row(
            "NSE", {"listing_date": "2026-09-24", "exchange": "BSE"},
            {"o": 1786, "h": 1787, "l": 1761, "c": 1762.7, "v": 4_687_406, "p": 1792.65}, date(2026, 9, 28),
        )
        self.assertEqual((row["exchange"], row["ticker"], row["instrument_key"]), ("BSE", "NSE.BO", "NSE.BO"))

    def test_a_nan_price_never_becomes_a_seed_row(self):
        # One NaN row fails the schema check for the whole snapshot file.
        provider = FreeMarketDataProvider()
        nan = float("nan")
        self.assertIsNone(provider._build_ipo_seed_row(
            "SONA", {"listing_date": "2026-09-24"}, {"o": nan, "h": nan, "l": nan, "c": nan, "v": 0, "p": 100}, date.today(),
        ))


if __name__ == "__main__":
    unittest.main()
