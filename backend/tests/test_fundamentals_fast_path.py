"""Fundamentals must answer at once, like charts.

A full build costs ~20 s on the Space (a Screener refusal, Yahoo, the AI
enrichment) and every payload there carries a "could not be refreshed"
warning, which the freshness rule rejects — so every open rebuilt from scratch
and took ~21 s, repeat opens included. Pinned here:
  * nothing cached -> a quick local payload marked ``partial``, the full build
    in the background;
  * anything cached, however stale -> served as-is, refreshed behind it;
  * the committed company profile fills the description and ratios;
  * a Screener failure backs off instead of being paid for on every build.
"""

from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from app.models.market import CompanyFundamentals, QuarterlyResultItem
from app.providers.free import FUNDAMENTALS_CACHE_VERSION, FreeMarketDataProvider


class FundamentalsFastPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / "data").mkdir()
        self.provider = FreeMarketDataProvider()
        self.provider.backend_root = root
        self.provider.fundamentals_cache_path = root / "fundamentals.json"
        self.provider.snapshot_cache_path = root / "free_snapshots.json"
        self.provider._fundamentals_memory_cache = {}
        self.provider._company_profiles_cache = None
        (root / "data" / "company_profiles.json").write_text(
            json.dumps(
                {
                    "ABC": {
                        "about": "ABC makes widgets.",
                        "website": "https://abc.example",
                        "pe": 21.5,
                        "roe_pct": 18.0,
                        "fetched_at": "2026-10-08T00:00:00+00:00",
                    }
                }
            ),
            encoding="utf-8",
        )
        quarters = [QuarterlyResultItem(period="Jun 2026", sales_crore=100.0, net_profit_crore=10.0, result_document_url=None)]
        patcher = mock.patch.object(FreeMarketDataProvider, "_bse_quarterly_results", staticmethod(lambda symbol: quarters))
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_nothing_cached_answers_quickly_and_builds_behind(self) -> None:
        async def go():
            with mock.patch.object(self.provider, "_build_company_fundamentals") as build:
                build.return_value = CompanyFundamentals(symbol="ABC", name="ABC Ltd")
                payload = await self.provider.get_fundamentals("ABC")
                self.assertTrue(payload.partial)
                self.assertEqual(payload.about, "ABC makes widgets.")
                self.assertEqual(payload.valuation.pe_ratio, 21.5)
                self.assertEqual(payload.quarterly_results[0].period, "Jun 2026")
                await asyncio.gather(*self.provider._fundamentals_refresh_tasks.values())
                await asyncio.sleep(0)
                self.assertEqual(build.call_count, 1)
            full = await self.provider.get_fundamentals("ABC")
            self.assertFalse(full.partial)
            self.assertEqual(full.about, "ABC makes widgets.")

        self._run(go())

    def test_a_stale_payload_is_served_not_rebuilt_inline(self) -> None:
        stale = CompanyFundamentals(
            symbol="ABC",
            name="ABC Ltd",
            data_warnings=["Quarterly tables could not be refreshed from the company page right now."],
        ).model_dump(mode="json")
        stale["cache_version"] = FUNDAMENTALS_CACHE_VERSION
        self.provider.fundamentals_cache_path.write_text(json.dumps({"ABC": stale}), encoding="utf-8")

        async def go():
            with mock.patch.object(self.provider, "_rebuild_fundamentals") as rebuild:
                rebuild.return_value = CompanyFundamentals(symbol="ABC", name="ABC Ltd")
                payload = await self.provider.get_fundamentals("ABC")
                self.assertEqual(payload.name, "ABC Ltd")
                self.assertFalse(payload.partial)
                # Served immediately; one refresh, and the cooldown stops a second.
                await self.provider.get_fundamentals("ABC")
                await asyncio.gather(*self.provider._fundamentals_refresh_tasks.values())
                self.assertEqual(rebuild.call_count, 1)

        self._run(go())

    def test_a_screener_failure_backs_off(self) -> None:
        with mock.patch.object(self.provider, "_fetch_screener_company_page", side_effect=RuntimeError("refused")) as fetch, \
             mock.patch.object(self.provider, "_fetch_yfinance_fundamentals", return_value={}), \
             mock.patch.object(self.provider, "_enrich_updates_from_linked_sources", return_value=([], [], [])), \
             mock.patch.object(type(self.provider.ai_service), "available", new_callable=mock.PropertyMock, return_value=False):
            self.provider._build_company_fundamentals("ABC", None)
            self.provider._build_company_fundamentals("ABC", None)
        self.assertEqual(fetch.call_count, 1)

    def test_peer_metrics_read_local_data(self) -> None:
        rows = [
            {"period": "Jun 2026", "sales_crore": 120.0, "net_profit_crore": 12.0, "operating_margin_pct": 20.0},
            {"period": "Mar 2026", "sales_crore": 110.0, "net_profit_crore": 11.0},
            {"period": "Dec 2025", "sales_crore": 105.0, "net_profit_crore": 10.0},
            {"period": "Sep 2025", "sales_crore": 102.0, "net_profit_crore": 9.0},
            {"period": "Jun 2025", "sales_crore": 100.0, "net_profit_crore": 8.0},
        ]
        with mock.patch("app.services.bse_quarterly.results_for", return_value=rows):
            [row] = self.provider.peer_metrics(["abc"])
        self.assertEqual(row["symbol"], "ABC")
        self.assertEqual(row["pe"], 21.5)
        self.assertEqual(row["sales_yoy_pct"], 20.0)
        self.assertEqual(row["profit_yoy_pct"], 50.0)
        self.assertEqual(row["ttm_net_profit_crore"], 42.0)


if __name__ == "__main__":
    unittest.main()
