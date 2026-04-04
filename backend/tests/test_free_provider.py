from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import ChartBar, IndexQuoteItem, StockSnapshot
from app.providers.free import CHART_CACHE_VERSION, LIVE_SNAPSHOT_MAX_AGE_SECONDS, FreeMarketDataProvider, UNIVERSE_CACHE_VERSION
from app.providers.us_free import USFreeMarketDataProvider


class FreeProviderRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.provider = FreeMarketDataProvider()
        self.provider.backend_root = Path(self.temp_dir.name)
        self.provider.universe_cache_path = self.provider.backend_root / "free_universe.json"
        self.provider.snapshot_cache_path = self.provider.backend_root / "free_snapshots.json"
        self.provider.company_metadata_path = self.provider.backend_root / "free_company_metadata.json"
        self.provider.fundamentals_cache_path = self.provider.backend_root / "free_fundamentals.json"
        self.provider.historical_breadth_cache_path = self.provider.backend_root / "free_historical_breadth.json"
        self.provider.chart_cache_dir = self.provider.backend_root / "chart_cache"
        self.provider.chart_cache_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _instrument(self, symbol: str = "TEST") -> dict[str, object]:
        return {
            "symbol": symbol,
            "name": f"{symbol} Industries",
            "exchange": "NSE",
            "listing_date": "2020-01-02",
            "sector": "Industrials",
            "sub_sector": "Capital Goods",
            "market_cap_crore": 5000.0,
            "ticker": f"{symbol}.NS",
        }

    def _benchmark_close(self, index: pd.DatetimeIndex) -> pd.Series:
        return pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)

    def _session_timestamp(self, *, day_offset: int = 0) -> int:
        session_date = self.provider._current_or_previous_trading_day_ist() + timedelta(days=day_offset)
        return int(datetime(session_date.year, session_date.month, session_date.day, 10, tzinfo=timezone.utc).timestamp())

    def _history(
        self,
        *,
        periods: int = 520,
        start_close: float = 100.0,
        step: float = 0.8,
        split_index: int | None = None,
        split_ratio: float = 2.0,
    ) -> pd.DataFrame:
        index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=periods)
        rows: list[dict[str, float]] = []
        for idx in range(periods):
            if split_index is not None and idx < split_index:
                close = start_close + (idx * step * split_ratio)
            elif split_index is not None:
                close = start_close + (idx * step)
            else:
                close = start_close + (idx * step)
            rows.append(
                {
                    "Open": close - 1.2,
                    "High": close + 2.0,
                    "Low": close - 2.5,
                    "Close": close,
                    "Volume": 100_000 + (idx * 1000),
                    "Stock Splits": split_ratio if split_index is not None and idx == split_index else 0.0,
                }
            )
        return pd.DataFrame(rows, index=index)

    def _snapshot_row(
        self,
        *,
        symbol: str = "TEST",
        session_date: str | None = None,
        history: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        source_history = history if history is not None else self._history()
        row = self.provider._history_to_snapshot(
            self._instrument(symbol),
            source_history,
            self._benchmark_close(source_history.index),
        )
        assert row is not None
        row["sector"] = "Industrials"
        row["sub_sector"] = "Capital Goods"
        if session_date is not None:
            row["history_session_date"] = session_date
        return row

    def _seed_snapshot_cache(self, rows: list[dict[str, object]]) -> None:
        self.provider.snapshot_cache_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    def test_stale_live_quotes_do_not_rewrite_cache_or_count_as_live_refresh(self) -> None:
        today = self.provider._current_or_previous_trading_day_ist().isoformat()
        row = self._snapshot_row(session_date=today)
        self._seed_snapshot_cache([row])
        before_mtime = self.provider.snapshot_cache_path.stat().st_mtime_ns
        stale_quote_time = self._session_timestamp(day_offset=-1)

        with patch.object(self.provider, "_fetch_nse_live_prices", return_value={}), patch.object(
            self.provider,
            "_fetch_quote_batch",
            return_value={
                "TEST.NS": {
                    "regularMarketPrice": 111.0,
                    "regularMarketPreviousClose": 109.0,
                    "regularMarketDayHigh": 112.0,
                    "regularMarketDayLow": 108.5,
                    "regularMarketOpen": 109.5,
                    "regularMarketVolume": 150_000,
                    "regularMarketTime": stale_quote_time,
                }
            },
        ), patch.object(self.provider, "_snapshot_schema_ok", return_value=True), patch.object(
            self.provider, "_load_or_refresh_universe", return_value=[self._instrument("TEST")]
        ):
            time.sleep(0.02)
            rows = self.provider._load_or_refresh_snapshots(1000.0, True)

        after_mtime = self.provider.snapshot_cache_path.stat().st_mtime_ns
        self.assertEqual(before_mtime, after_mtime)
        self.assertEqual(rows[0]["last_price"], row["last_price"])
        self.assertEqual(self.provider.get_last_refresh_metadata()["applied_quote_count"], 0)
        self.assertIsNone(self.provider.get_last_refresh_metadata()["quote_source"])

    def test_fresh_same_day_quotes_update_snapshot_rows(self) -> None:
        today = self.provider._current_or_previous_trading_day_ist().isoformat()
        row = self._snapshot_row(session_date=today)
        self._seed_snapshot_cache([row])
        current_time = self._session_timestamp()

        with patch.object(
            self.provider,
            "_fetch_nse_live_prices",
            return_value={
                "TEST": {
                    "regularMarketPrice": 126.0,
                    "regularMarketPreviousClose": 120.0,
                    "regularMarketDayHigh": 127.5,
                    "regularMarketDayLow": 119.8,
                    "regularMarketOpen": 121.0,
                    "regularMarketVolume": 250_000,
                    "regularMarketTime": current_time,
                }
            },
        ), patch.object(self.provider, "_fetch_quote_batch", return_value={}), patch.object(
            self.provider, "_snapshot_schema_ok", return_value=True
        ), patch.object(self.provider, "_load_or_refresh_universe", return_value=[self._instrument("TEST")]):
            rows = self.provider._load_or_refresh_snapshots(1000.0, True)

        refreshed = rows[0]
        self.assertEqual(refreshed["last_price"], row["last_price"])
        self.assertEqual(refreshed["previous_close"], row["previous_close"])
        self.assertEqual(refreshed["change_pct"], row["change_pct"])
        self.assertEqual(refreshed["volume"], row["volume"])
        self.assertEqual(self.provider.get_last_refresh_metadata()["applied_quote_count"], 0)
        self.assertIsNone(self.provider.get_last_refresh_metadata()["quote_source"])

    def test_history_snapshot_uses_latest_history_date_for_session_date(self) -> None:
        history = self._history(periods=120)

        row = self.provider._history_to_snapshot(
            self._instrument("TEST"),
            history,
            self._benchmark_close(history.index),
        )

        assert row is not None
        expected_session_date = history.index[-1].date().isoformat()
        self.assertEqual(row["history_as_of_date"], expected_session_date)
        self.assertEqual(row["history_session_date"], expected_session_date)

    def test_quote_only_snapshot_uses_quote_trade_date_for_session_date(self) -> None:
        quote_time = datetime(2026, 3, 25, 9, 45, tzinfo=timezone.utc)

        row = self.provider._quote_only_snapshot_row(
            self._instrument("TEST"),
            {
                "regularMarketPrice": 126.0,
                "regularMarketPreviousClose": 120.0,
                "regularMarketDayHigh": 127.5,
                "regularMarketDayLow": 119.8,
                "regularMarketOpen": 121.0,
                "regularMarketVolume": 250_000,
                "regularMarketTime": int(quote_time.timestamp()),
            },
        )

        assert row is not None
        expected_session_date = quote_time.astimezone(timezone(timedelta(hours=5, minutes=30))).date().isoformat()
        self.assertEqual(row["history_as_of_date"], expected_session_date)
        self.assertEqual(row["history_session_date"], expected_session_date)

    def test_live_quote_fetch_uses_direct_nse_fallback_and_skips_yahoo_for_bse_tail(self) -> None:
        current_time = self._session_timestamp()
        rows = [
            {
                "symbol": "TEST",
                "exchange": "NSE",
                "instrument_key": "TEST.NS",
            },
            {
                "symbol": "BSETEST",
                "exchange": "BSE",
                "instrument_key": "BSETEST.BO",
            },
        ]

        with patch.object(self.provider, "_fetch_nse_live_prices", return_value={}), patch.object(
            self.provider,
            "_fetch_nse_quote_equity_live",
            return_value={
                "TEST": {
                    "regularMarketPrice": 126.0,
                    "regularMarketPreviousClose": 120.0,
                    "regularMarketDayHigh": 127.5,
                    "regularMarketDayLow": 119.8,
                    "regularMarketOpen": 121.0,
                    "regularMarketVolume": 250_000,
                    "regularMarketTime": current_time,
                }
            },
        ) as direct_nse_quotes, patch.object(
            self.provider,
            "_fetch_quote_batch",
            side_effect=AssertionError("yahoo fallback should not run for BSE-only remainder during India live hours"),
        ), patch.object(self.provider, "_is_market_open_ist", return_value=True):
            live_quotes, quote_sources = self.provider._fetch_live_quotes_for_rows(rows)

        direct_nse_quotes.assert_called_once_with(["TEST"])
        self.assertEqual(live_quotes["TEST"]["regularMarketPrice"], 126.0)
        self.assertEqual(quote_sources["TEST"], "nse-direct")
        self.assertNotIn("BSETEST", live_quotes)

    def test_get_index_quotes_prefers_cached_chart_bars_without_network_history_download(self) -> None:
        self.provider._write_chart_cache(
            "^CNX500",
            "1D",
            [
                ChartBar(time=1712000000, open=20800.0, high=20810.0, low=20790.0, close=20800.0, volume=0),
                ChartBar(time=1712086400, open=20900.0, high=20910.0, low=20890.0, close=20900.0, volume=0),
            ],
        )

        async def run() -> list[IndexQuoteItem]:
            with patch.object(self.provider, "_fetch_quote_batch", side_effect=AssertionError("unexpected live quote fetch")), patch.object(
                self.provider,
                "_fetch_nse_index_quote",
                side_effect=AssertionError("unexpected NSE quote fetch"),
            ), patch.object(
                self.provider,
                "_fetch_index_quote_from_history",
                side_effect=AssertionError("unexpected history download"),
            ):
                return await self.provider.get_index_quotes(["^CNX500"])

        items = asyncio.run(run())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].symbol, "^CNX500")
        self.assertEqual(items[0].price, 20900.0)
        self.assertEqual(items[0].change_pct, 0.48)

    def test_bulk_live_refresh_skips_chart_cache_patch_io(self) -> None:
        current_time = self._session_timestamp()
        row = self._snapshot_row(session_date=self.provider._current_or_previous_trading_day_ist().isoformat())
        quote = {
            "regularMarketPrice": 126.0,
            "regularMarketPreviousClose": 120.0,
            "regularMarketDayHigh": 127.5,
            "regularMarketDayLow": 119.8,
            "regularMarketOpen": 121.0,
            "regularMarketVolume": 250_000,
            "regularMarketTime": current_time,
        }

        with patch.object(
            self.provider,
            "_fetch_live_quotes_for_rows",
            return_value=({"TEST": quote}, {"TEST": "nse"}),
        ), patch.object(
            self.provider,
            "_live_quote_for_symbol",
            return_value=None,
        ), patch.object(
            self.provider,
            "_apply_sector_benchmarks",
            side_effect=lambda rows: rows,
        ), patch.object(
            self.provider,
            "_apply_rs_rating",
            side_effect=lambda rows: rows,
        ), patch.object(self.provider, "_patch_daily_chart_cache_with_quote") as patch_chart_cache:
            refreshed_rows, metadata = self.provider._refresh_snapshot_rows_live([row])

        self.assertEqual(metadata["applied_quote_count"], 1)
        self.assertEqual(refreshed_rows[0]["last_price"], 126.0)
        patch_chart_cache.assert_not_called()

    def test_parse_nse_last_update_time_returns_exchange_timestamp(self) -> None:
        parsed = self.provider._parse_nse_last_update_time("30-Mar-2026 15:29:00")

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.astimezone(timezone.utc).year, 2026)
        self.assertEqual(parsed.astimezone(timezone.utc).month, 3)
        self.assertEqual(parsed.astimezone(timezone.utc).day, 30)

    def test_quote_without_trade_timestamp_does_not_patch_daily_chart_cache(self) -> None:
        history = self._history(periods=40)
        self.provider._seed_daily_chart_cache("TEST", history)
        before = self.provider._read_chart_cache("TEST", "1D", 520)

        self.provider._patch_daily_chart_cache_with_quote(
            "TEST",
            {
                "regularMarketPrice": 150.0,
                "regularMarketPreviousClose": 148.0,
                "regularMarketDayHigh": 151.0,
                "regularMarketDayLow": 147.5,
                "regularMarketOpen": 149.0,
                "regularMarketVolume": 200_000,
            },
        )

        after = self.provider._read_chart_cache("TEST", "1D", 520)
        self.assertEqual(before, after)

    def test_bad_live_quote_falls_back_to_snapshot_session_bar_in_cache(self) -> None:
        history = self._history(periods=40, start_close=180.0, step=1.0)
        self.provider._seed_daily_chart_cache("TEST", history)
        snapshot_row = self._snapshot_row(symbol="TEST", session_date=self.provider._current_or_previous_trading_day_ist().isoformat(), history=history)
        snapshot_row["last_price"] = 236.7
        snapshot_row["previous_close"] = 197.28
        snapshot_row["day_high"] = 236.7
        snapshot_row["day_low"] = 205.2
        snapshot_row["volume"] = 9291600
        self._seed_snapshot_cache([snapshot_row])

        self.provider._patch_daily_chart_cache_with_quote(
            "TEST",
            {
                "regularMarketPrice": 11530.15,
                "regularMarketPreviousClose": 1972.8,
                "regularMarketDayHigh": 11572.75,
                "regularMarketDayLow": 2052.0,
                "regularMarketOpen": 2065.0,
                "regularMarketVolume": 0,
                "regularMarketTime": self._session_timestamp(),
            },
        )

        patched = self.provider._read_chart_cache("TEST", "1D", 520)
        self.assertTrue(patched)
        self.assertEqual(patched[-1].close, 236.7)
        self.assertEqual(patched[-1].high, 236.7)
        self.assertEqual(patched[-1].low, 205.2)
        self.assertEqual(patched[-1].volume, 9291600)

    def test_apply_live_quote_to_daily_history_falls_back_to_snapshot_bar(self) -> None:
        history = self.provider._split_adjusted_history(self._history(periods=60, start_close=100.0, step=0.4))
        snapshot_row = self._snapshot_row(symbol="TEST", session_date=self.provider._current_or_previous_trading_day_ist().isoformat(), history=history)
        snapshot_row["last_price"] = 236.7
        snapshot_row["previous_close"] = 197.28
        snapshot_row["day_high"] = 236.7
        snapshot_row["day_low"] = 205.2
        snapshot_row["volume"] = 9291600
        self._seed_snapshot_cache([snapshot_row])
        quote = {
            "regularMarketPrice": 900.0,
            "regularMarketPreviousClose": 905.0,
            "regularMarketDayHigh": 910.0,
            "regularMarketDayLow": 890.0,
            "regularMarketOpen": 902.0,
            "regularMarketVolume": 250_000,
            "regularMarketTime": self._session_timestamp(),
        }

        with patch.object(self.provider, "_is_market_open_ist", return_value=True), patch.object(
            self.provider, "_live_quote_for_symbol", return_value=quote
        ):
            patched = self.provider._apply_live_quote_to_daily_history("TEST", "TEST.NS", history)

        latest = patched.iloc[-1]
        self.assertEqual(round(float(latest["Close"]), 2), 236.7)
        self.assertEqual(round(float(latest["High"]), 2), 236.7)
        self.assertEqual(round(float(latest["Low"]), 2), 205.2)
        self.assertEqual(int(float(latest["Volume"])), 9291600)

    def test_read_chart_cache_rejects_poisoned_daily_bar_for_current_session(self) -> None:
        history = self._history(periods=60, start_close=180.0, step=1.0)
        snapshot_row = self._snapshot_row(symbol="TEST", session_date=self.provider._current_or_previous_trading_day_ist().isoformat(), history=history)
        snapshot_row["last_price"] = 2367.3
        snapshot_row["previous_close"] = 1972.8
        snapshot_row["day_high"] = 2367.3
        snapshot_row["day_low"] = 2052.0
        snapshot_row["volume"] = 9291600
        self._seed_snapshot_cache([snapshot_row])

        trade_date = self.provider._current_or_previous_trading_day_ist()
        poisoned_bar = ChartBar(
            time=int(datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc).timestamp()),
            open=2065.0,
            high=11572.75,
            low=2052.0,
            close=2359.3,
            volume=0,
        )
        self.provider._write_chart_cache("TEST", "1D", [poisoned_bar])

        cached = self.provider._read_chart_cache("TEST", "1D", 520)

        self.assertEqual(cached, [])

    def test_snapshot_cache_mtime_advances_effective_session_date_for_chart_validation(self) -> None:
        history = self._history(periods=60, start_close=180.0, step=1.0)
        session_date = self.provider._current_or_previous_trading_day_ist()
        snapshot_row = self._snapshot_row(symbol="TEST", session_date=(session_date - timedelta(days=1)).isoformat(), history=history)
        snapshot_row["last_price"] = 2367.3
        snapshot_row["previous_close"] = 1972.8
        snapshot_row["day_high"] = 2367.3
        snapshot_row["day_low"] = 2052.0
        snapshot_row["volume"] = 9291600
        self._seed_snapshot_cache([snapshot_row])
        fresh_write_time = datetime(session_date.year, session_date.month, session_date.day, 10, tzinfo=timezone.utc).timestamp()
        os.utime(self.provider.snapshot_cache_path, (fresh_write_time, fresh_write_time))

        poisoned_bar = ChartBar(
            time=int(datetime.combine(session_date, datetime.min.time(), tzinfo=timezone.utc).timestamp()),
            open=2065.0,
            high=11572.75,
            low=2052.0,
            close=2359.3,
            volume=0,
        )
        self.provider._write_chart_cache("TEST", "1D", [poisoned_bar])

        self.assertEqual(self.provider._snapshot_session_date("TEST"), session_date)
        self.assertEqual(self.provider._read_chart_cache("TEST", "1D", 520), [])

    def test_quote_only_snapshot_rows_zero_out_relative_volume_baselines(self) -> None:
        row = self.provider._quote_only_snapshot_row(
            self._instrument("TEST"),
            {
                "regularMarketPrice": 125.0,
                "regularMarketPreviousClose": 120.0,
                "regularMarketDayHigh": 126.0,
                "regularMarketDayLow": 119.5,
                "regularMarketOpen": 121.0,
                "regularMarketVolume": 585877,
                "regularMarketTime": self._session_timestamp(),
            },
        )

        assert row is not None
        self.assertEqual(row["history_source"], "quote")
        self.assertEqual(row["avg_volume_20d"], 0)
        self.assertEqual(row["avg_volume_30d"], 0)
        self.assertEqual(row["avg_volume_50d"], 0)

        materialized = self.provider._materialize_snapshot_rows([row])[0]
        self.assertEqual(materialized.avg_volume_20d, 0)
        self.assertEqual(materialized.relative_volume, 0.0)

    def test_apply_live_quote_to_daily_history_ignores_out_of_scale_quote(self) -> None:
        history = self.provider._split_adjusted_history(self._history(periods=60, start_close=100.0, step=0.4))
        quote = {
            "regularMarketPrice": 900.0,
            "regularMarketPreviousClose": 905.0,
            "regularMarketDayHigh": 910.0,
            "regularMarketDayLow": 890.0,
            "regularMarketOpen": 902.0,
            "regularMarketVolume": 250_000,
            "regularMarketTime": self._session_timestamp(),
        }

        with patch.object(self.provider, "_is_market_open_ist", return_value=True), patch.object(
            self.provider, "_live_quote_for_symbol", return_value=quote
        ):
            patched = self.provider._apply_live_quote_to_daily_history("TEST", "TEST.NS", history)

        pd.testing.assert_frame_equal(patched, history)

    def test_history_to_snapshot_computes_adr_pct_20(self) -> None:
        history = self._history(periods=80, start_close=100.0, step=1.1)
        row = self.provider._history_to_snapshot(self._instrument("TEST"), history, self._benchmark_close(history.index))

        assert row is not None
        recent = history.tail(20)
        expected_adr = float((recent["High"] - recent["Low"]).mean())
        expected_reference = float(recent["Close"].mean())
        expected_pct = round((expected_adr / expected_reference) * 100, 2)
        self.assertEqual(row["adr_pct_20"], expected_pct)

    def test_metadata_needs_refresh_when_circuit_update_is_stale(self) -> None:
        current_session = self.provider._current_or_previous_trading_day_ist().isoformat()
        stale_session = (self.provider._current_or_previous_trading_day_ist() - timedelta(days=1)).isoformat()

        fresh_metadata = {
            "sector": "Industrials",
            "sub_sector": "Capital Goods",
            "listing_date": "2020-01-02",
            "circuit_band_label": "5%",
            "upper_circuit_limit": 105.0,
            "lower_circuit_limit": 95.0,
            "circuit_updated_on": current_session,
        }
        stale_metadata = {
            **fresh_metadata,
            "circuit_updated_on": stale_session,
        }

        self.assertFalse(self.provider._metadata_needs_refresh(fresh_metadata))
        self.assertTrue(self.provider._metadata_needs_refresh(stale_metadata))

    def test_load_company_metadata_skips_network_refresh_on_read_path(self) -> None:
        universe = [self._instrument("TEST")]
        self.provider.company_metadata_path.write_text(
            json.dumps(
                {
                    "TEST": {
                        "market_cap_crore": 5000.0,
                        "sector": "Industrials",
                        "sub_sector": "Capital Goods",
                        "listing_date": "2020-01-02",
                        "circuit_band_label": None,
                        "upper_circuit_limit": None,
                        "lower_circuit_limit": None,
                        "circuit_updated_on": None,
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch.object(self.provider, "_fetch_company_metadata", side_effect=AssertionError("network should not be used")):
            metadata = self.provider._load_or_refresh_company_metadata(universe, False, False)

        self.assertIn("TEST", metadata)
        self.assertEqual(metadata["TEST"]["sector"], "Industrials")
        self.assertIsNone(metadata["TEST"]["circuit_band_label"])

    def test_get_snapshots_serves_cached_rows_without_background_live_refresh(self) -> None:
        today = self.provider._current_or_previous_trading_day_ist().isoformat()
        row = self._snapshot_row(session_date=today)
        self._seed_snapshot_cache([row])

        with patch.object(self.provider, "_snapshot_schema_ok", return_value=True), patch.object(
            self.provider,
            "_load_or_refresh_snapshots",
            side_effect=AssertionError("request path should not block on live refresh"),
        ), patch.object(
            self.provider,
            "_schedule_background_snapshot_refresh",
        ) as schedule_refresh, patch.object(
            self.provider,
            "_is_market_open_ist",
            return_value=True,
        ), patch.object(
            self.provider,
            "_snapshot_age_seconds",
            return_value=LIVE_SNAPSHOT_MAX_AGE_SECONDS + 1,
        ):
            snapshots = asyncio.run(self.provider.get_snapshots(1000.0))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].symbol, "TEST")
        schedule_refresh.assert_not_called()

    def test_get_snapshots_serves_stale_closed_session_cache_without_queueing_refresh(self) -> None:
        current_session = self.provider._current_or_previous_trading_day_ist()
        stale_session = self.provider._previous_trading_day(current_session).isoformat()
        row = self._snapshot_row(session_date=stale_session)
        self._seed_snapshot_cache([row])

        with patch.object(self.provider, "_snapshot_schema_ok", return_value=True), patch.object(
            self.provider,
            "_load_or_refresh_snapshots",
            side_effect=AssertionError("request path should not block on close-session rebuild"),
        ), patch.object(
            self.provider,
            "_schedule_background_snapshot_refresh",
        ) as schedule_refresh, patch.object(
            self.provider,
            "_is_market_open_ist",
            return_value=False,
        ), patch.object(
            self.provider,
            "_market_close_refresh_due",
            return_value=True,
        ), patch.object(
            self.provider,
            "_strict_closed_session_refresh_due",
            return_value=True,
        ):
            snapshots = asyncio.run(self.provider.get_snapshots(1000.0))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].last_price, row["last_price"])
        schedule_refresh.assert_not_called()

    def test_force_refresh_rebuilds_closed_session_history_on_weekend(self) -> None:
        friday = datetime(2026, 4, 3, tzinfo=timezone.utc).date()
        thursday = friday - timedelta(days=1)

        with patch.object(self.provider, "_current_or_previous_trading_day_ist", return_value=friday), patch.object(
            self.provider,
            "_is_trading_day_ist",
            return_value=False,
        ):
            self.assertTrue(
                self.provider._should_rebuild_snapshot_history(
                    [{"history_session_date": thursday.isoformat()}],
                    True,
                )
            )

    def test_india_good_friday_is_not_treated_as_trading_day(self) -> None:
        holiday = date(2026, 4, 3)

        with patch.object(self.provider, "_current_ist_date", return_value=holiday):
            self.assertFalse(self.provider._is_trading_day_ist())
            self.assertEqual(self.provider._current_or_previous_trading_day_ist(), date(2026, 4, 2))

    def test_cached_holiday_snapshot_dates_are_rolled_back_to_previous_session(self) -> None:
        row = self._snapshot_row(session_date="2026-04-03")
        row["history_as_of_date"] = "2026-04-03"
        self._seed_snapshot_cache([row])

        with patch.object(self.provider, "_snapshot_schema_ok", return_value=True):
            rows = self.provider._load_valid_cached_snapshot_rows()

        self.assertEqual(rows[0]["history_session_date"], "2026-04-02")
        self.assertEqual(rows[0]["history_as_of_date"], "2026-04-02")
        cached_rows = json.loads(self.provider.snapshot_cache_path.read_text(encoding="utf-8"))
        self.assertEqual(cached_rows[0]["history_session_date"], "2026-04-02")

    def test_read_chart_cache_rejects_india_holiday_last_bar(self) -> None:
        holiday_bar = ChartBar(
            time=int(datetime(2026, 4, 3, tzinfo=timezone.utc).timestamp()),
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=12345,
        )

        self.provider._write_chart_cache("TEST", "1D", [holiday_bar])

        self.assertEqual(self.provider._read_chart_cache("TEST", "1D", 10), [])

    def test_snapshot_scale_lookup_does_not_advance_session_from_holiday_file_mtime(self) -> None:
        row = self._snapshot_row(session_date="2026-04-02")
        self._seed_snapshot_cache([row])
        holiday_mtime = datetime(2026, 4, 3, 10, tzinfo=timezone.utc).timestamp()
        os.utime(self.provider.snapshot_cache_path, (holiday_mtime, holiday_mtime))

        with patch.object(self.provider, "_snapshot_schema_ok", return_value=True):
            self.assertEqual(self.provider._snapshot_session_date("TEST"), date(2026, 4, 2))

    def test_concurrent_get_snapshots_share_single_request_task(self) -> None:
        snapshot = self.provider._materialize_snapshot_rows([self._snapshot_row()])[0]
        calls = 0

        async def fake_load(_: float, __: float | None = None) -> list:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return [snapshot]

        async def run_test() -> tuple[list, list]:
            with patch.object(self.provider, "_load_cached_snapshot_rows", return_value=[]), patch.object(
                self.provider,
                "_load_snapshots_with_fallback",
                side_effect=fake_load,
            ):
                return await asyncio.gather(
                    self.provider.get_snapshots(1000.0),
                    self.provider.get_snapshots(1000.0),
                )

        first, second = asyncio.run(run_test())

        self.assertEqual(calls, 1)
        self.assertEqual(first[0].symbol, "TEST")
        self.assertEqual(second[0].symbol, "TEST")

    def test_historical_breadth_aggregation_uses_per_metric_denominators(self) -> None:
        snapshots = [self._instrument("AAA"), self._instrument("BBB")]
        index = pd.to_datetime(["2026-03-30"])
        breadth_dfs = {
            "AAA.NS": pd.DataFrame(
                {
                    "above_ma20": [1.0],
                    "above_ma50": [float("nan")],
                    "above_sma200": [float("nan")],
                    "new_high_52w": [float("nan")],
                    "new_low_52w": [float("nan")],
                },
                index=index,
            ),
            "BBB.NS": pd.DataFrame(
                {
                    "above_ma20": [0.0],
                    "above_ma50": [1.0],
                    "above_sma200": [1.0],
                    "new_high_52w": [0.0],
                    "new_low_52w": [1.0],
                },
                index=index,
            ),
        }

        self.provider._aggregate_and_save_historical_breadth(snapshots, breadth_dfs)

        payload = json.loads(self.provider.historical_breadth_cache_path.read_text(encoding="utf-8"))
        history_row = payload["universes"][0]["history"][0]
        self.assertEqual(history_row["above_ma20_pct"], 50.0)
        self.assertEqual(history_row["above_ma50_pct"], 100.0)
        self.assertEqual(history_row["above_sma200_pct"], 100.0)
        self.assertEqual(history_row["new_high_52w_pct"], 0.0)
        self.assertEqual(history_row["new_low_52w_pct"], 100.0)

    def test_recently_touched_daily_chart_cache_without_current_session_bar_is_patched_from_snapshot(self) -> None:
        history = self._history(periods=40, start_close=180.0, step=1.0)
        session_date = self.provider._current_or_previous_trading_day_ist()
        snapshot_row = self._snapshot_row(symbol="TEST", session_date=session_date.isoformat(), history=history)
        snapshot_row["last_price"] = 236.7
        snapshot_row["previous_close"] = 197.28
        snapshot_row["day_high"] = 236.7
        snapshot_row["day_low"] = 205.2
        snapshot_row["volume"] = 9291600
        self._seed_snapshot_cache([snapshot_row])

        stale_bars = [
            ChartBar(
                time=int(datetime.combine(session_date - timedelta(days=2), datetime.min.time(), tzinfo=timezone.utc).timestamp()),
                open=180.0,
                high=183.0,
                low=178.0,
                close=181.0,
                volume=120000,
            ),
            ChartBar(
                time=int(datetime.combine(session_date - timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp()),
                open=181.0,
                high=184.0,
                low=179.0,
                close=182.0,
                volume=123456,
            ),
        ]
        self.provider._write_chart_cache("TEST", "1D", stale_bars)
        now_timestamp = time.time()
        os.utime(self.provider._chart_cache_path("TEST", "1D"), (now_timestamp, now_timestamp))

        with patch.object(self.provider, "_is_market_open_ist", return_value=True), patch.object(
            self.provider, "_live_quote_for_symbol", return_value=None
        ):
            bars = asyncio.run(self.provider.get_chart("TEST", "1D", bars=2))

        self.assertEqual(len(bars), 2)
        self.assertEqual(self.provider._chart_bar_trade_date(bars[-1]), session_date)
        self.assertEqual(bars[-1].close, 236.7)
        self.assertEqual(bars[-1].high, 236.7)
        self.assertEqual(bars[-1].low, 205.2)
        self.assertEqual(bars[-1].volume, 9291600)


class USFreeProviderRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.provider = USFreeMarketDataProvider()
        self.provider.backend_root = Path(self.temp_dir.name)
        self.provider.universe_cache_path = self.provider.backend_root / "free_universe_us.json"
        self.provider.snapshot_cache_path = self.provider.backend_root / "free_snapshots_us.json"
        self.provider.company_metadata_path = self.provider.backend_root / "free_company_metadata_us.json"
        self.provider.fundamentals_cache_path = self.provider.backend_root / "free_fundamentals_us.json"
        self.provider.historical_breadth_cache_path = self.provider.backend_root / "free_historical_breadth_us.json"
        self.provider.chart_cache_dir = self.provider.backend_root / "chart_cache_us"
        self.provider.chart_cache_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_preferred_refresh_strategy_uses_historical_after_post_close_grace(self) -> None:
        row = self._snapshot_row()

        with patch.object(self.provider, "_load_valid_cached_snapshot_rows", return_value=[row]), patch.object(
            self.provider,
            "_is_market_open_ist",
            return_value=False,
        ), patch.object(
            self.provider,
            "_market_close_refresh_due",
            return_value=True,
        ):
            self.assertEqual(self.provider.preferred_refresh_strategy(), "historical")

    def test_us_good_friday_is_not_treated_as_trading_day(self) -> None:
        holiday = date(2026, 4, 3)

        with patch.object(self.provider, "_current_ist_date", return_value=holiday):
            self.assertFalse(self.provider._is_trading_day_ist())
            self.assertEqual(self.provider._current_or_previous_trading_day_ist(), date(2026, 4, 2))

    def test_read_chart_cache_rejects_us_holiday_last_bar(self) -> None:
        holiday_bar = ChartBar(
            time=int(datetime(2026, 4, 3, tzinfo=timezone.utc).timestamp()),
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=12345,
        )

        self.provider._write_chart_cache("TEST", "1D", [holiday_bar])

        self.assertEqual(self.provider._read_chart_cache("TEST", "1D", 10), [])

    def _instrument(self, symbol: str = "TEST") -> dict[str, object]:
        return {
            "symbol": symbol,
            "name": f"{symbol} Inc",
            "exchange": "NASDAQ",
            "listing_date": "2020-01-02",
            "sector": "Technology",
            "sub_sector": "Software",
            "market_cap_crore": 5000.0,
            "ticker": symbol,
        }

    def _benchmark_close(self, index: pd.DatetimeIndex) -> pd.Series:
        return pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)

    def _session_timestamp(self, *, day_offset: int = 0) -> int:
        session_date = self.provider._current_or_previous_trading_day_ist() + timedelta(days=day_offset)
        return int(datetime(session_date.year, session_date.month, session_date.day, 10, tzinfo=timezone.utc).timestamp())

    def _history(
        self,
        *,
        periods: int = 260,
        start_close: float = 100.0,
        step: float = 0.8,
        split_index: int | None = None,
        split_ratio: float = 2.0,
    ) -> pd.DataFrame:
        index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=periods)
        rows: list[dict[str, float]] = []
        for idx in range(periods):
            if split_index is not None and idx < split_index:
                close = (start_close + (idx * step)) * split_ratio
                adjusted_close = close / split_ratio
            else:
                close = start_close + (idx * step)
                adjusted_close = close
            rows.append(
                {
                    "Open": close - 1.2,
                    "High": close + 2.0,
                    "Low": close - 2.5,
                    "Close": close,
                    "Adj Close": adjusted_close,
                    "Volume": 100_000 + (idx * 1000),
                    "Stock Splits": split_ratio if split_index is not None and idx == split_index else 0.0,
                }
            )
        return pd.DataFrame(rows, index=index)

    def _snapshot_row(
        self,
        *,
        symbol: str = "TEST",
        session_date: str | None = None,
        history: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        source_history = history if history is not None else self._history()
        row = self.provider._history_to_snapshot(
            self._instrument(symbol),
            source_history,
            self._benchmark_close(source_history.index),
        )
        assert row is not None
        row["sector"] = "Technology"
        row["sub_sector"] = "Software"
        if session_date is not None:
            row["history_session_date"] = session_date
        return row

    def _seed_snapshot_cache(self, rows: list[dict[str, object]]) -> None:
        self.provider.snapshot_cache_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    def test_history_to_snapshot_derives_us_listing_date_from_first_bar(self) -> None:
        index = pd.bdate_range(end=datetime(2026, 3, 30, tzinfo=timezone.utc), periods=70)
        history = pd.DataFrame(
            [
                {
                    "Open": 100 + idx - 1,
                    "High": 100 + idx + 2,
                    "Low": 100 + idx - 2,
                    "Close": 100 + idx,
                    "Adj Close": 100 + idx,
                    "Volume": 100_000 + (idx * 500),
                    "Stock Splits": 0.0,
                }
                for idx in range(len(index))
            ],
            index=index,
        )
        benchmark = pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)

        snapshot = self.provider._history_to_snapshot(
            {
                "symbol": "NEWUS",
                "name": "New US Listing",
                "exchange": "NASDAQ",
                "listing_date": None,
                "sector": "Technology",
                "sub_sector": "Software",
                "market_cap_crore": 10_000.0,
                "ticker": "NEWUS",
            },
            history,
            benchmark,
        )

        assert snapshot is not None
        self.assertEqual(snapshot["listing_date"], index[0].date().isoformat())

    def test_backfill_listing_dates_derives_short_history_us_rows(self) -> None:
        rows, changed = self.provider._backfill_listing_dates(
            [
                {
                    "symbol": "NEWUS",
                    "listing_date": None,
                    "history_source": "history",
                    "history_bars": 70,
                    "history_as_of_date": "2026-03-30",
                },
                {
                    "symbol": "OLDUS",
                    "listing_date": None,
                    "history_source": "history",
                    "history_bars": 320,
                    "history_as_of_date": "2026-03-30",
                },
            ]
        )

        self.assertTrue(changed)
        self.assertIsNotNone(rows[0]["listing_date"])
        self.assertIsNone(rows[1]["listing_date"])

    def test_prior_session_cache_rebuilds_history_without_live_patch(self) -> None:
        yesterday = (self.provider._current_or_previous_trading_day_ist() - timedelta(days=1)).isoformat()
        cached_row = self._snapshot_row(session_date=yesterday)
        rebuilt_row = self._snapshot_row(session_date=self.provider._current_or_previous_trading_day_ist().isoformat())
        rebuilt_row["last_price"] = 101.0
        rebuilt_row["previous_close"] = 100.0
        self._seed_snapshot_cache([cached_row])
        current_time = self._session_timestamp()

        with patch.object(self.provider, "_load_or_refresh_universe", return_value=[self._instrument("TEST")]), patch.object(
            self.provider,
            "_build_snapshot_cache",
            return_value=[rebuilt_row],
        ) as build_snapshot_cache, patch.object(
            self.provider,
            "_is_trading_day_ist",
            return_value=True,
        ), patch.object(
            self.provider,
            "_fetch_nse_live_prices",
            return_value={
                "TEST": {
                    "regularMarketPrice": 110.0,
                    "regularMarketPreviousClose": 100.0,
                    "regularMarketDayHigh": 111.0,
                    "regularMarketDayLow": 99.5,
                    "regularMarketOpen": 100.5,
                    "regularMarketVolume": 320_000,
                    "regularMarketTime": current_time,
                }
            },
        ), patch.object(self.provider, "_fetch_quote_batch", return_value={}), patch.object(
            self.provider, "_snapshot_schema_ok", return_value=True
        ):
            rows = self.provider._load_or_refresh_snapshots(1000.0, True)

        self.assertEqual(build_snapshot_cache.call_count, 1)
        self.assertEqual(rows[0]["last_price"], 101.0)
        metadata = self.provider.get_last_refresh_metadata()
        self.assertTrue(metadata["historical_rebuild"])
        self.assertEqual(metadata["applied_quote_count"], 0)

    def test_split_adjusted_snapshot_metrics_stay_on_post_split_scale(self) -> None:
        history = self._history(periods=90, start_close=95.0, step=0.6, split_index=45, split_ratio=2.0)
        snapshot = self.provider._history_to_snapshot(
            self._instrument("SPLIT"),
            history,
            self._benchmark_close(history.index),
        )
        assert snapshot is not None

        self.assertLess(snapshot["high_52w"], 160.0)
        self.assertLess(snapshot["ema20"], 150.0)
        self.assertLess(snapshot["previous_close"], 150.0)

    def test_daily_and_weekly_charts_use_split_adjusted_history(self) -> None:
        history = self._history(periods=90, start_close=95.0, step=0.6, split_index=45, split_ratio=2.0)
        raw_first_close = float(history.iloc[0]["Close"])
        latest_close = float(history.iloc[-1]["Close"])

        with patch.object(self.provider, "_resolve_ticker", return_value="SPLIT"), patch.object(
            self.provider, "_download_history_frame", return_value=history
        ), patch.object(
            self.provider, "_is_market_open_ist", return_value=False
        ):
            daily_bars = self.provider._fetch_chart_bars("SPLIT", "1D", bars=90)
            weekly_bars = self.provider._fetch_chart_bars("SPLIT", "1W", bars=30)

        self.assertLess(daily_bars[0].close, raw_first_close)
        self.assertLess(weekly_bars[0].close, raw_first_close)
        self.assertAlmostEqual(daily_bars[-1].close, latest_close, places=2)

    def test_short_history_snapshot_only_populates_available_moving_averages(self) -> None:
        history = self._history(periods=35, start_close=120.0, step=0.9)
        snapshot = self.provider._history_to_snapshot(
            self._instrument("SHORT"),
            history,
            self._benchmark_close(history.index),
        )
        assert snapshot is not None

        self.assertIsNotNone(snapshot["ema20"])
        self.assertIsNotNone(snapshot["sma20"])
        self.assertIsNone(snapshot["ema50"])
        self.assertIsNone(snapshot["sma50"])
        self.assertIsNone(snapshot["ema200"])
        self.assertIsNone(snapshot["sma150"])
        self.assertIsNone(snapshot["sma200"])
        self.assertIsNone(snapshot["weekly_ema20"])

    def test_quote_only_snapshot_keeps_moving_averages_blank(self) -> None:
        quote = {
            "regularMarketPrice": 142.0,
            "regularMarketPreviousClose": 139.0,
            "regularMarketDayHigh": 144.0,
            "regularMarketDayLow": 138.5,
            "regularMarketOpen": 140.0,
            "regularMarketVolume": 250_000,
            "regularMarketTime": self._session_timestamp(),
        }

        row = self.provider._quote_only_snapshot_row(self._instrument("QUOTE"), quote)
        assert row is not None

        self.assertIsNone(row["ema20"])
        self.assertIsNone(row["ema50"])
        self.assertIsNone(row["ema200"])
        self.assertIsNone(row["sma20"])
        self.assertIsNone(row["sma50"])
        self.assertIsNone(row["sma150"])
        self.assertIsNone(row["sma200"])
        self.assertIsNone(row["weekly_ema20"])

    def test_build_snapshot_cache_falls_back_to_quote_only_rows_when_batch_download_is_sparse(self) -> None:
        universe = [self._instrument("AAA"), self._instrument("BBB")]
        benchmark_index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=260)
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100 + index for index in range(len(benchmark_index))],
                "High": [101 + index for index in range(len(benchmark_index))],
                "Low": [99 + index for index in range(len(benchmark_index))],
                "Close": [100 + index for index in range(len(benchmark_index))],
                "Adj Close": [100 + index for index in range(len(benchmark_index))],
                "Volume": [1_000_000 for _ in range(len(benchmark_index))],
                "Stock Splits": [0.0 for _ in range(len(benchmark_index))],
            },
            index=benchmark_index,
        )

        with patch("app.providers.free.yf.download", side_effect=[benchmark_frame, pd.DataFrame()]), patch.object(
            self.provider,
            "_fetch_quote_batch",
            return_value={
                "AAA": {
                    "regularMarketPrice": 110.0,
                    "regularMarketPreviousClose": 108.0,
                    "regularMarketDayHigh": 111.0,
                    "regularMarketDayLow": 107.5,
                    "regularMarketOpen": 108.5,
                    "regularMarketVolume": 120_000,
                    "regularMarketTime": int(datetime.now(timezone.utc).timestamp()),
                },
                "BBB": {
                    "regularMarketPrice": 84.0,
                    "regularMarketPreviousClose": 82.5,
                    "regularMarketDayHigh": 85.0,
                    "regularMarketDayLow": 82.0,
                    "regularMarketOpen": 82.8,
                    "regularMarketVolume": 95_000,
                    "regularMarketTime": int(datetime.now(timezone.utc).timestamp()),
                },
            },
        ):
            snapshots = self.provider._build_snapshot_cache(universe)

        self.assertEqual(len(snapshots), 2)
        self.assertTrue(all(snapshot.get("chart_grid_points") for snapshot in snapshots))

    def test_recover_missing_snapshot_rows_builds_real_history_rows_before_quote_only_fallback(self) -> None:
        universe = [self._instrument(f"T{index:02d}") for index in range(10)]
        benchmark_index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=520)
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100 + index for index in range(len(benchmark_index))],
                "High": [101 + index for index in range(len(benchmark_index))],
                "Low": [99 + index for index in range(len(benchmark_index))],
                "Close": [100 + index for index in range(len(benchmark_index))],
                "Adj Close": [100 + index for index in range(len(benchmark_index))],
                "Volume": [1_000_000 for _ in range(len(benchmark_index))],
                "Stock Splits": [0.0 for _ in range(len(benchmark_index))],
            },
            index=benchmark_index,
        )
        histories = {
            str(item["ticker"]): self._history(periods=520, start_close=100.0 + offset, step=0.4 + (offset * 0.02))
            for offset, item in enumerate(universe)
        }
        combined_history_payload = pd.concat(histories, axis=1)

        with patch(
            "app.providers.us_free.yf.download",
            side_effect=[benchmark_frame, combined_history_payload],
        ) as download_history, patch.object(
            self.provider,
            "_fetch_quote_batch",
            return_value={},
        ) as fetch_quote_batch:
            snapshots = self.provider._build_snapshot_cache(universe)

        self.assertEqual(download_history.call_count, 2)
        self.assertEqual(fetch_quote_batch.call_count, 0)
        self.assertEqual(len(snapshots), len(universe))
        self.assertTrue(all(snapshot.get("baseline_close_252d") is not None for snapshot in snapshots))
        self.assertTrue(all(snapshot.get("rs_eligible") for snapshot in snapshots))

    def test_recover_snapshot_row_uses_cached_daily_bars_before_network_download(self) -> None:
        instrument = self._instrument("CACHE")
        history = self._history(periods=520, start_close=120.0, step=0.55)
        adjusted_history = self.provider._split_adjusted_history(history)
        self.provider._seed_daily_chart_cache("CACHE", adjusted_history)

        with patch.object(self.provider, "_download_history_frame") as download_history_frame:
            snapshot, breadth_df = self.provider._recover_snapshot_row(
                instrument,
                self._benchmark_close(history.index),
            )

        self.assertIsNotNone(snapshot)
        self.assertIsNotNone(breadth_df)
        self.assertEqual(download_history_frame.call_count, 0)
        assert snapshot is not None
        self.assertEqual(snapshot["history_source"], "chart_cache")
        self.assertTrue(snapshot["rs_eligible"])
        self.assertIsNotNone(snapshot["baseline_close_252d"])

    def test_quote_only_builder_uses_cached_daily_bars_before_quote_fallback(self) -> None:
        instrument = self._instrument("CACHEQ")
        history = self._history(periods=520, start_close=90.0, step=0.45)
        adjusted_history = self.provider._split_adjusted_history(history)
        self.provider._seed_daily_chart_cache("CACHEQ", adjusted_history)

        with patch.object(self.provider, "_fetch_quote_batch", return_value={}) as fetch_quote_batch:
            snapshots = self.provider._build_quote_only_snapshots(
                [instrument],
                self._benchmark_close(history.index),
            )

        self.assertEqual(fetch_quote_batch.call_count, 0)
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0]["history_source"], "chart_cache")
        self.assertTrue(snapshots[0]["rs_eligible"])
        self.assertIsNotNone(snapshots[0]["baseline_close_252d"])

    def test_build_snapshot_cache_prefers_fresh_cached_bars_before_batch_download(self) -> None:
        instrument = self._instrument("CACHEFAST")
        history = self._history(periods=520, start_close=110.0, step=0.35)
        adjusted_history = self.provider._split_adjusted_history(history)
        self.provider._seed_daily_chart_cache("CACHEFAST", adjusted_history)
        benchmark_index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=520)
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100 + index for index in range(len(benchmark_index))],
                "High": [101 + index for index in range(len(benchmark_index))],
                "Low": [99 + index for index in range(len(benchmark_index))],
                "Close": [100 + index for index in range(len(benchmark_index))],
                "Adj Close": [100 + index for index in range(len(benchmark_index))],
                "Volume": [1_000_000 for _ in range(len(benchmark_index))],
                "Stock Splits": [0.0 for _ in range(len(benchmark_index))],
            },
            index=benchmark_index,
        )

        with patch("app.providers.us_free.yf.download", return_value=benchmark_frame) as download_history:
            snapshots = self.provider._build_snapshot_cache([instrument])

        self.assertEqual(download_history.call_count, 1)
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0]["history_source"], "chart_cache")
        self.assertTrue(snapshots[0]["rs_eligible"])

    def test_build_snapshot_cache_tries_network_for_old_listings_with_short_cached_history(self) -> None:
        instrument = self._instrument("OLDSHORT")
        short_history = self._history(periods=120, start_close=70.0, step=0.25)
        full_history = self._history(periods=520, start_close=70.0, step=0.25)
        self.provider._seed_daily_chart_cache("OLDSHORT", self.provider._split_adjusted_history(short_history))
        benchmark_index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=520)
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100 + index for index in range(len(benchmark_index))],
                "High": [101 + index for index in range(len(benchmark_index))],
                "Low": [99 + index for index in range(len(benchmark_index))],
                "Close": [100 + index for index in range(len(benchmark_index))],
                "Adj Close": [100 + index for index in range(len(benchmark_index))],
                "Volume": [1_000_000 for _ in range(len(benchmark_index))],
                "Stock Splits": [0.0 for _ in range(len(benchmark_index))],
            },
            index=benchmark_index,
        )
        rebuilt_snapshot = self.provider._history_to_snapshot(
            instrument,
            full_history,
            self._benchmark_close(full_history.index),
        )
        assert rebuilt_snapshot is not None

        with patch(
            "app.providers.us_free.yf.download",
            side_effect=[benchmark_frame, full_history],
        ) as download_history:
            snapshots = self.provider._build_snapshot_cache([instrument])

        self.assertEqual(download_history.call_count, 2)
        self.assertEqual(len(snapshots), 1)
        self.assertNotEqual(snapshots[0]["history_source"], "chart_cache")
        self.assertTrue(snapshots[0]["rs_eligible"])

    def test_build_snapshot_cache_does_not_reuse_poisoned_cached_snapshot_rows(self) -> None:
        instrument = self._instrument("FRESH")
        stale_row = self._snapshot_row(symbol="FRESH")
        stale_row["stock_return_5d"] = -9.91
        stale_row["stock_return_20d"] = -14.4
        stale_row["stock_return_60d"] = -15.47
        stale_row["stock_return_126d"] = -22.46
        stale_row["stock_return_12m"] = 20.82
        self._seed_snapshot_cache([stale_row])

        history = self._history(periods=520, start_close=130.0, step=0.42)
        adjusted_history = self.provider._split_adjusted_history(history)
        self.provider._seed_daily_chart_cache("FRESH", adjusted_history)
        benchmark_index = pd.bdate_range(end=datetime(2026, 3, 27, tzinfo=timezone.utc), periods=520)
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100 + index for index in range(len(benchmark_index))],
                "High": [101 + index for index in range(len(benchmark_index))],
                "Low": [99 + index for index in range(len(benchmark_index))],
                "Close": [100 + index for index in range(len(benchmark_index))],
                "Adj Close": [100 + index for index in range(len(benchmark_index))],
                "Volume": [1_000_000 for _ in range(len(benchmark_index))],
                "Stock Splits": [0.0 for _ in range(len(benchmark_index))],
            },
            index=benchmark_index,
        )

        with patch("app.providers.free.yf.download", return_value=benchmark_frame):
            snapshots = self.provider._build_snapshot_cache([instrument])

        self.assertEqual(len(snapshots), 1)
        self.assertNotEqual(snapshots[0]["stock_return_5d"], -9.91)
        self.assertEqual(snapshots[0]["history_source"], "chart_cache")

    def test_intraday_quote_batch_uses_minute_bars_for_live_prices(self) -> None:
        intraday_index = pd.DatetimeIndex(
            [
                datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 27, 7, 1, tzinfo=timezone.utc),
            ]
        )
        intraday_frame = pd.DataFrame(
            [
                {"Open": 99.0, "High": 101.0, "Low": 98.5, "Close": 100.0, "Volume": 1_000},
                {"Open": 101.0, "High": 106.0, "Low": 101.0, "Close": 104.0, "Volume": 1_500},
                {"Open": 104.0, "High": 107.0, "Low": 103.0, "Close": 106.0, "Volume": 2_000},
            ],
            index=intraday_index,
        )

        with patch.object(self.provider, "_fetch_yahoo_live_quotes", return_value={}), patch(
            "app.providers.us_free.yf.download",
            return_value=intraday_frame,
        ):
            quotes = self.provider._fetch_quote_batch(["TEST"])

        quote = quotes["TEST"]
        self.assertEqual(quote["regularMarketPrice"], 106.0)
        self.assertEqual(quote["regularMarketPreviousClose"], 100.0)
        self.assertEqual(quote["regularMarketDayHigh"], 107.0)
        self.assertEqual(quote["regularMarketDayLow"], 101.0)
        self.assertEqual(quote["regularMarketVolume"], 3500)

    def test_intraday_quote_batch_handles_single_ticker_multiindex_downloads(self) -> None:
        intraday_index = pd.DatetimeIndex(
            [
                datetime(2026, 3, 30, 15, 59, tzinfo=timezone.utc),
                datetime(2026, 3, 31, 13, 30, tzinfo=timezone.utc),
                datetime(2026, 3, 31, 13, 31, tzinfo=timezone.utc),
            ]
        )
        intraday_frame = pd.DataFrame(
            {
                ("Adj Close", "AAPL"): [100.0, 104.0, 106.0],
                ("Close", "AAPL"): [100.0, 104.0, 106.0],
                ("High", "AAPL"): [101.0, 106.0, 107.0],
                ("Low", "AAPL"): [98.5, 101.0, 103.0],
                ("Open", "AAPL"): [99.0, 101.0, 104.0],
                ("Volume", "AAPL"): [1_000, 1_500, 2_000],
            },
            index=intraday_index,
        )
        intraday_frame.columns = pd.MultiIndex.from_tuples(intraday_frame.columns, names=["Price", "Ticker"])

        with patch.object(self.provider, "_fetch_yahoo_live_quotes", return_value={}), patch(
            "app.providers.us_free.yf.download",
            return_value=intraday_frame,
        ):
            quotes = self.provider._fetch_quote_batch(["AAPL"])

        quote = quotes["AAPL"]
        self.assertEqual(quote["regularMarketPrice"], 106.0)
        self.assertEqual(quote["regularMarketPreviousClose"], 100.0)
        self.assertEqual(quote["regularMarketDayHigh"], 107.0)
        self.assertEqual(quote["regularMarketDayLow"], 101.0)
        self.assertEqual(quote["regularMarketVolume"], 3500)

    def test_daily_quote_batch_handles_single_ticker_multiindex_downloads(self) -> None:
        daily_index = pd.DatetimeIndex(
            [
                datetime(2026, 3, 30, tzinfo=timezone.utc),
                datetime(2026, 3, 31, tzinfo=timezone.utc),
            ]
        )
        daily_frame = pd.DataFrame(
            {
                ("Adj Close", "^GSPC"): [6343.72, 6430.40],
                ("Close", "^GSPC"): [6343.72, 6430.40],
                ("High", "^GSPC"): [6405.0, 6435.0],
                ("Low", "^GSPC"): [6328.0, 6388.0],
                ("Open", "^GSPC"): [6403.37, 6395.88],
                ("Volume", "^GSPC"): [5_458_640_000, 811_816_660],
            },
            index=daily_index,
        )
        daily_frame.columns = pd.MultiIndex.from_tuples(daily_frame.columns, names=["Price", "Ticker"])

        with patch("app.providers.us_free.yf.download", return_value=daily_frame):
            quotes = self.provider._fetch_daily_quote_batch(["^GSPC"])

        quote = quotes["^GSPC"]
        self.assertEqual(quote["regularMarketPrice"], 6430.40)
        self.assertEqual(quote["regularMarketPreviousClose"], 6343.72)
        self.assertEqual(quote["regularMarketDayHigh"], 6435.0)
        self.assertEqual(quote["regularMarketDayLow"], 6388.0)
        self.assertEqual(quote["regularMarketOpen"], 6395.88)

    def test_adjusted_close_normalizes_legacy_history_without_split_event(self) -> None:
        history = self._history(periods=70, start_close=100.0, step=1.0)
        history["Adj Close"] = history["Close"]
        history.iloc[:35, history.columns.get_loc("Adj Close")] = history.iloc[:35]["Close"] * 0.5

        adjusted = self.provider._split_adjusted_history(history)

        self.assertAlmostEqual(float(adjusted.iloc[0]["Close"]), float(history.iloc[0]["Close"]) * 0.5, places=4)
        self.assertAlmostEqual(float(adjusted.iloc[-1]["Close"]), float(history.iloc[-1]["Close"]), places=4)

    def test_load_or_refresh_snapshots_keeps_cached_rs_metrics_during_market_hours(self) -> None:
        today = self.provider._current_or_previous_trading_day_ist().isoformat()
        leader = self._snapshot_row(symbol="LEADER", session_date=today)
        laggard = self._snapshot_row(symbol="LAG", session_date=today, history=self._history(start_close=60.0, step=0.2))
        self._seed_snapshot_cache([leader, laggard])
        current_time = self._session_timestamp()

        with patch.object(
            self.provider,
            "_fetch_nse_live_prices",
            return_value={
                "LEADER": {
                    "regularMarketPrice": float(leader["previous_close"]) + 25.0,
                    "regularMarketPreviousClose": float(leader["previous_close"]),
                    "regularMarketDayHigh": float(leader["previous_close"]) + 25.5,
                    "regularMarketDayLow": float(leader["previous_close"]) - 1.0,
                    "regularMarketOpen": float(leader["previous_close"]) + 2.0,
                    "regularMarketVolume": 320_000,
                    "regularMarketTime": current_time,
                }
            },
        ), patch.object(self.provider, "_fetch_quote_batch", return_value={}), patch.object(
            self.provider, "_snapshot_schema_ok", return_value=True
        ), patch.object(
            self.provider, "_load_or_refresh_universe", return_value=[self._instrument("LEADER"), self._instrument("LAG")]
        ), patch.object(
            self.provider,
            "_live_quote_for_symbol",
            return_value={
                "regularMarketPrice": 25050.0,
                "regularMarketPreviousClose": 24900.0,
                "regularMarketTime": current_time,
            },
        ):
            rows = self.provider._load_or_refresh_snapshots(1000.0, True)

        refreshed_leader = next(row for row in rows if row["symbol"] == "LEADER")
        self.assertEqual(float(refreshed_leader["stock_return_20d"]), float(leader["stock_return_20d"]))
        self.assertEqual(float(refreshed_leader["rs_weighted_score"]), float(leader["rs_weighted_score"]))
        self.assertEqual(self.provider.get_last_refresh_metadata()["applied_quote_count"], 0)

    def test_weighted_rs_score_uses_cumulative_lookbacks(self) -> None:
        history = self._history(periods=520, start_close=100.0, step=0.5)
        closes = history["Close"]

        score = self.provider._weighted_rs_score(closes, 0)
        expected = (
            (self.provider._return_pct_as_of(closes, 63, 0) * 0.4)
            + (self.provider._return_pct_as_of(closes, 126, 0) * 0.2)
            + (self.provider._return_pct_as_of(closes, 189, 0) * 0.2)
            + (self.provider._return_pct_as_of(closes, 252, 0) * 0.2)
        )

        self.assertAlmostEqual(score, expected, places=4)

    def test_partial_history_returns_fall_back_to_available_bars(self) -> None:
        history = self._history(periods=80, start_close=100.0, step=1.0)
        snapshot = self.provider._history_to_snapshot(
            self._instrument("SHORT"),
            history,
            self._benchmark_close(history.index),
        )

        assert snapshot is not None
        adjusted_close = self.provider._split_adjusted_history(history)["Close"].dropna()
        full_period_return = ((float(adjusted_close.iloc[-1]) / float(adjusted_close.iloc[0])) - 1) * 100

        self.assertAlmostEqual(snapshot["stock_return_126d"], full_period_return, places=2)
        self.assertAlmostEqual(snapshot["stock_return_12m"], full_period_return, places=2)
        self.assertAlmostEqual(snapshot["stock_return_504d"], full_period_return, places=2)

    def test_weighted_rs_score_adapts_to_short_history(self) -> None:
        history = self._history(periods=80, start_close=100.0, step=1.0)
        closes = self.provider._split_adjusted_history(history)["Close"]

        lookbacks = self.provider._adaptive_rs_lookbacks(closes, 0)
        self.assertEqual(lookbacks, [(20, 0.4), (40, 0.2), (59, 0.2), (79, 0.2)])

        score = self.provider._weighted_rs_score(closes, 0)
        expected = (
            (self.provider._return_pct_as_of(closes, 20, 0, allow_partial=False) * 0.4)
            + (self.provider._return_pct_as_of(closes, 40, 0, allow_partial=False) * 0.2)
            + (self.provider._return_pct_as_of(closes, 59, 0, allow_partial=False) * 0.2)
            + (self.provider._return_pct_as_of(closes, 79, 0, allow_partial=False) * 0.2)
        )

        self.assertAlmostEqual(score, expected, places=4)

        snapshot = self.provider._history_to_snapshot(
            self._instrument("ADAPT"),
            history,
            self._benchmark_close(history.index),
        )
        assert snapshot is not None
        rated_snapshot = self.provider._apply_rs_rating([snapshot])[0]
        self.assertTrue(rated_snapshot["rs_eligible"])
        self.assertGreater(rated_snapshot["rs_rating"], 0)
        self.assertIsNotNone(rated_snapshot["rs_baseline_close_q1"])
        self.assertIsNotNone(rated_snapshot["rs_baseline_close_q4"])

    def test_month_and_three_month_returns_use_trading_month_and_quarter_lookbacks(self) -> None:
        history = self._history(periods=520, start_close=100.0, step=0.3)
        snapshot = self.provider._history_to_snapshot(
            self._instrument("RETURNS"),
            history,
            self._benchmark_close(history.index),
        )

        assert snapshot is not None
        adjusted_close = self.provider._split_adjusted_history(history)["Close"]
        self.assertAlmostEqual(snapshot["stock_return_20d"], self.provider._return_pct(adjusted_close, 21), places=2)
        self.assertAlmostEqual(snapshot["stock_return_60d"], self.provider._return_pct(adjusted_close, 63), places=2)
        self.assertAlmostEqual(snapshot["benchmark_return_20d"], self.provider._return_pct(self._benchmark_close(history.index), 21), places=2)
        self.assertAlmostEqual(snapshot["benchmark_return_60d"], self.provider._return_pct(self._benchmark_close(history.index), 63), places=2)

    def test_legacy_chart_cache_payload_is_invalidated_and_rewritten(self) -> None:
        path = self.provider._chart_cache_path("TEST", "1D")
        path.write_text(
            json.dumps(
                [
                    {"time": 1, "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "volume": 1000},
                ],
                indent=2,
            ),
            encoding="utf-8",
        )
        new_bars = [
            self.provider._history_to_chart_bars(
                pd.DataFrame(
                    [{"Open": 20.0, "High": 22.0, "Low": 19.0, "Close": 21.0, "Volume": 2000}],
                    index=pd.DatetimeIndex([datetime(2026, 3, 27, tzinfo=timezone.utc)]),
                )
            )[0]
        ]

        with patch.object(self.provider, "_fetch_chart_bars", return_value=new_bars) as fetch_chart_bars:
            bars = asyncio.run(self.provider.get_chart("TEST", "1D", bars=1))

        self.assertEqual(fetch_chart_bars.call_count, 1)
        self.assertEqual(bars[-1].close, 21.0)
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["cache_version"], CHART_CACHE_VERSION)

    def test_poisoned_chart_cache_is_rejected_and_rebuilt(self) -> None:
        history = self._history(periods=40, start_close=100.0, step=0.5)
        snapshot_row = self._snapshot_row(history=history, session_date=self.provider._current_or_previous_trading_day_ist().isoformat())
        self._seed_snapshot_cache([snapshot_row])
        self.provider._write_chart_cache(
            "TEST",
            "1D",
            [
                ChartBar(
                    time=int(datetime(2026, 3, 27, tzinfo=timezone.utc).timestamp()),
                    open=880.0,
                    high=905.0,
                    low=870.0,
                    close=900.0,
                    volume=12345,
                )
            ],
        )
        rebuilt_bars = [
            ChartBar(
                time=int(datetime(2026, 3, 27, tzinfo=timezone.utc).timestamp()),
                open=118.0,
                high=121.0,
                low=117.0,
                close=120.0,
                volume=54321,
            )
        ]

        with patch.object(self.provider, "_fetch_chart_bars", return_value=rebuilt_bars) as fetch_chart_bars:
            bars = asyncio.run(self.provider.get_chart("TEST", "1D", bars=1))

        self.assertEqual(fetch_chart_bars.call_count, 1)
        self.assertEqual(bars[-1].close, 120.0)
        payload = json.loads(self.provider._chart_cache_path("TEST", "1D").read_text(encoding="utf-8"))
        self.assertEqual(payload["symbol"], "TEST")
        self.assertEqual(payload["timeframe"], "1D")
        self.assertEqual(payload["ticker"], "TEST")
        self.assertEqual(payload["bars"][-1]["close"], 120.0)

    def test_weekly_chart_prefers_cached_daily_history_before_network_download(self) -> None:
        history = self._history(periods=260, start_close=100.0, step=0.5)
        adjusted_history = self.provider._split_adjusted_history(history)
        self.provider._seed_daily_chart_cache("TEST", adjusted_history)

        with patch.object(self.provider, "_download_history_frame", side_effect=AssertionError("network should not be used")):
            bars = self.provider._fetch_chart_bars("TEST", "1W", 40)

        self.assertTrue(bars)
        self.assertLessEqual(len(bars), 40)
        self.assertAlmostEqual(bars[-1].close, round(float(adjusted_history["Close"].iloc[-1]), 2), places=2)

    def test_refresh_snapshots_falls_back_to_cached_snapshot_on_refresh_error(self) -> None:
        row = self._snapshot_row(session_date=self.provider._current_ist_date().isoformat())
        self._seed_snapshot_cache([row])

        with patch.object(self.provider, "_load_or_refresh_snapshots", side_effect=RuntimeError("boom")), patch.object(
            self.provider,
            "_snapshot_schema_ok",
            return_value=True,
        ), patch.object(
            self.provider, "_load_or_refresh_universe", return_value=[self._instrument("TEST")]
        ), patch.object(
            self.provider,
            "_load_or_refresh_company_metadata",
            return_value={"TEST": {"sector": "Industrials", "sub_sector": "Capital Goods", "listing_date": "2020-01-02"}},
        ):
            snapshots = asyncio.run(self.provider.refresh_snapshots(1000.0))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].symbol, "TEST")
        self.assertEqual(self.provider.get_last_refresh_metadata()["applied_quote_count"], 0)

    def test_refresh_live_snapshots_updates_cached_rows_without_full_rebuild(self) -> None:
        row = self._snapshot_row(session_date=self.provider._current_ist_date().isoformat())
        updated_row = dict(row)
        updated_row["last_price"] = 126.0
        updated_row["previous_close"] = 120.0
        updated_row["change_pct"] = 5.0
        self._seed_snapshot_cache([row])

        with patch.object(
            self.provider,
            "_is_market_open_ist",
            return_value=True,
        ), patch.object(
            self.provider,
            "_load_valid_cached_snapshot_rows",
            return_value=[row],
        ), patch.object(
            self.provider,
            "_refresh_snapshot_rows_live",
            return_value=(
                [updated_row],
                {"applied_quote_count": 1, "historical_rebuild": False, "quote_source": "yahoo"},
            ),
        ):
            snapshots = asyncio.run(self.provider.refresh_live_snapshots(1000.0))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].last_price, 126.0)
        cached_rows = json.loads(self.provider.snapshot_cache_path.read_text(encoding="utf-8"))
        self.assertEqual(cached_rows[0]["last_price"], 126.0)
        self.assertEqual(self.provider.get_last_refresh_metadata()["applied_quote_count"], 1)

    def test_refresh_live_snapshots_falls_back_to_full_refresh_when_cache_missing(self) -> None:
        snapshot = StockSnapshot.model_validate(self.provider._with_snapshot_fallbacks(self._snapshot_row()))

        with patch.object(
            self.provider,
            "_is_market_open_ist",
            return_value=True,
        ), patch.object(self.provider, "refresh_snapshots", return_value=[snapshot]) as refresh_snapshots:
            snapshots = asyncio.run(self.provider.refresh_live_snapshots(1000.0))

        refresh_snapshots.assert_called_once_with(1000.0)
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].symbol, "TEST")

    def test_universe_builder_merges_cached_metadata_with_us_listed_rows(self) -> None:
        self.provider.company_metadata_path.write_text(
            json.dumps(
                {
                    "ABB": {
                        "sector": "Industrials",
                        "sub_sector": "Electrical Equipment",
                        "listing_date": "1995-02-08",
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch.object(
            self.provider,
            "_fetch_us_listed_universe",
            return_value=[
                {
                    "symbol": "ABB",
                    "name": "ABB Ltd",
                    "exchange": "NYSE",
                    "ticker": "ABB",
                    "market_cap_crore": 130_517.55,
                    "last_price": 52.4,
                    "previous_close": 51.9,
                    "last_session_volume": 200_000,
                    "listing_date": "1995-02-08",
                },
                {
                    "symbol": "MSFT",
                    "name": "Microsoft Corp",
                    "exchange": "NASDAQ",
                    "ticker": "MSFT",
                    "market_cap_crore": 910.0,
                    "last_price": 430.0,
                    "previous_close": 425.0,
                    "last_session_volume": 150_000,
                    "listing_date": "1986-03-13",
                    "sector": "Technology",
                    "sub_sector": "Software",
                },
            ],
        ):
            rows = self.provider._fetch_market_cap_universe(800.0)

        symbols = {row["symbol"] for row in rows}
        self.assertEqual(symbols, {"ABB", "MSFT"})

        abb = next(row for row in rows if row["symbol"] == "ABB")
        self.assertEqual(abb["exchange"], "NYSE")
        self.assertEqual(abb["ticker"], "ABB")
        self.assertEqual(abb["sector"], "Industrials")
        self.assertEqual(abb["sub_sector"], "Electrical Equipment")
        self.assertEqual(abb["listing_date"], "1995-02-08")

        msft = next(row for row in rows if row["symbol"] == "MSFT")
        self.assertEqual(msft["exchange"], "NASDAQ")
        self.assertEqual(msft["ticker"], "MSFT")
        self.assertEqual(msft["market_cap_crore"], 910.0)
        self.assertEqual(msft["sector"], "Technology")

    def test_universe_cache_refreshes_on_new_session_and_reuses_current_session(self) -> None:
        current_session = self.provider._current_or_previous_trading_day_ist().isoformat()
        previous_session = (self.provider._current_or_previous_trading_day_ist() - timedelta(days=1)).isoformat()
        cached_row = {
            "symbol": "CACHED",
            "name": "Cached Inc",
            "exchange": "NASDAQ",
            "market_cap_crore": 950.0,
            "ticker": "CACHED",
            "sector": "Technology",
            "sub_sector": "Software",
            "listing_date": "2020-01-02",
            "last_price": 125.0,
            "previous_close": 124.0,
            "last_session_volume": 120000,
            "universe_version": UNIVERSE_CACHE_VERSION,
            "universe_session_date": current_session,
            "universe_market_cap_floor_crore": 800.0,
        }
        self.provider.universe_cache_path.write_text(json.dumps([cached_row], indent=2), encoding="utf-8")

        with patch.object(self.provider, "_fetch_market_cap_universe") as fetch_universe:
            rows = self.provider._load_or_refresh_universe(800.0, False)
        self.assertEqual(rows[0]["symbol"], "CACHED")
        self.assertEqual(fetch_universe.call_count, 0)

        stale_row = dict(cached_row)
        stale_row["universe_session_date"] = previous_session
        self.provider.universe_cache_path.write_text(json.dumps([stale_row], indent=2), encoding="utf-8")

        with patch.object(self.provider, "_fetch_market_cap_universe", return_value=[cached_row]) as fetch_universe:
            rows = self.provider._load_or_refresh_universe(800.0, False)
        self.assertEqual(rows[0]["symbol"], "CACHED")
        self.assertEqual(fetch_universe.call_count, 1)


if __name__ == "__main__":
    unittest.main()
