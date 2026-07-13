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
from app.providers.free import (
    CHART_CACHE_VERSION,
    LIVE_SNAPSHOT_MAX_AGE_SECONDS,
    SNAPSHOT_CACHE_VERSION,
    FreeMarketDataProvider,
    UNIVERSE_CACHE_VERSION,
)


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
        self.provider.eod_bars_dir = self.provider.backend_root / "eod_bars"
        self.provider._recent_eod_bars_cache = None

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

    def test_history_snapshot_includes_recent_closes(self) -> None:
        row = self._snapshot_row()

        self.assertIn("recent_closes", row)
        self.assertGreaterEqual(len(row["recent_closes"]), 2)
        snapshot = StockSnapshot.model_validate(row)
        self.assertEqual(len(snapshot.recent_closes), len(row["recent_closes"]))

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

    def test_load_valid_cached_snapshot_rows_bootstraps_from_seed_snapshots(self) -> None:
        seeded_row = json.loads((BACKEND_ROOT / "data" / "free_snapshots_seed_0.json").read_text(encoding="utf-8"))[0]
        seeded_row = dict(seeded_row)
        seeded_row["snapshot_cache_version"] = SNAPSHOT_CACHE_VERSION + 2
        seed_path = self.provider.snapshot_cache_path.parent / "free_snapshots_seed_0.json"
        seed_path.write_text(json.dumps([seeded_row], indent=2), encoding="utf-8")

        rows = self.provider._load_valid_cached_snapshot_rows()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], seeded_row["symbol"])
        self.assertEqual(rows[0]["snapshot_cache_version"], SNAPSHOT_CACHE_VERSION)
        self.assertTrue(self.provider.snapshot_cache_path.exists())

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

    def test_get_snapshots_refreshes_stale_closed_session_cache_inline(self) -> None:
        current_session = self.provider._current_or_previous_trading_day_ist()
        stale_session = self.provider._previous_trading_day(current_session).isoformat()
        row = self._snapshot_row(session_date=stale_session)
        refreshed_row = self._snapshot_row(session_date=current_session.isoformat())
        refreshed_row["last_price"] = row["last_price"] + 10
        self._seed_snapshot_cache([row])

        with patch.object(self.provider, "_snapshot_schema_ok", return_value=True), patch.object(
            self.provider,
            "_load_or_refresh_snapshots",
            return_value=[refreshed_row],
        ) as load_or_refresh, patch.object(
            self.provider,
            "_write_snapshot_rows",
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
        self.assertEqual(snapshots[0].last_price, refreshed_row["last_price"])
        load_or_refresh.assert_called_once_with(1000.0, True)
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

    def test_eod_only_prefers_historical_refresh_when_latest_close_is_stale(self) -> None:
        provider = FreeMarketDataProvider(eod_only_mode=True)

        with patch.object(provider, "_load_valid_cached_snapshot_rows", return_value=[self._snapshot_row()]), patch.object(
            provider,
            "_market_close_refresh_due",
            return_value=True,
        ):
            self.assertEqual(provider.preferred_refresh_strategy(), "historical")

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

    def test_eod_only_intraday_chart_requests_reuse_daily_cache(self) -> None:
        provider = FreeMarketDataProvider(eod_only_mode=True)
        provider.backend_root = Path(self.temp_dir.name)
        provider.chart_cache_dir = provider.backend_root / "chart_cache_eod"
        provider.chart_cache_dir.mkdir(parents=True, exist_ok=True)
        daily_bars = [
            ChartBar(
                time=int(datetime(2026, 4, 2, tzinfo=timezone.utc).timestamp()),
                open=100.0,
                high=103.0,
                low=99.0,
                close=102.0,
                volume=120000,
            ),
            ChartBar(
                time=int(datetime(2026, 4, 3, tzinfo=timezone.utc).timestamp()),
                open=102.0,
                high=104.0,
                low=101.0,
                close=103.0,
                volume=125000,
            ),
        ]

        with patch.object(provider, "_read_chart_cache", return_value=daily_bars) as read_chart_cache, patch.object(
            provider,
            "_chart_cache_covers_snapshot_session",
            return_value=True,
        ), patch.object(
            provider,
            "_is_chart_cache_fresh",
            return_value=True,
        ), patch.object(
            provider,
            "_refresh_cached_daily_chart_from_quote",
            side_effect=AssertionError("EOD mode should not quote-patch daily charts"),
        ), patch.object(
            provider,
            "_fetch_chart_bars",
            side_effect=AssertionError("EOD mode should not fetch network intraday charts when daily cache is fresh"),
        ):
            bars = asyncio.run(provider.get_chart("TEST", "15m", bars=2))

        self.assertEqual([bar.close for bar in bars], [102.0, 103.0])
        self.assertEqual(read_chart_cache.call_args.args[1], "1D")

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

    def _write_eod_session(self, session_date: date, symbols: dict[str, list]) -> None:
        self.provider.eod_bars_dir.mkdir(parents=True, exist_ok=True)
        payload = {"date": session_date.isoformat(), "symbols": symbols}
        (self.provider.eod_bars_dir / f"{session_date.isoformat()}.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        self.provider._recent_eod_bars_cache = None

    def _daily_bar(self, session_date: date, close: float) -> ChartBar:
        ts = int(datetime(session_date.year, session_date.month, session_date.day, tzinfo=timezone.utc).timestamp())
        return ChartBar(time=ts, open=close - 1, high=close + 1, low=close - 2, close=close, volume=1000)

    def test_eod_store_fills_interior_and_trailing_gaps_yahoo_dropped(self) -> None:
        # Yahoo returned Mon, Tue, Thu — dropping Wed (interior) and never
        # supplying Fri (trailing). The authoritative EOD store carries all five.
        mon, tue, wed, thu, fri = (date(2026, 7, 6), date(2026, 7, 7), date(2026, 7, 8), date(2026, 7, 9), date(2026, 7, 10))
        yahoo_bars = [self._daily_bar(mon, 100.0), self._daily_bar(tue, 101.0), self._daily_bar(thu, 103.0)]
        self._write_eod_session(wed, {"TEST": [101.5, 102.5, 101.0, 102.0, 5000]})
        self._write_eod_session(fri, {"TEST": [103.0, 104.5, 102.8, 104.0, 6000]})

        filled = self.provider._with_daily_eod_gaps_filled("TEST", yahoo_bars)
        got = [self.provider._chart_bar_trade_date(b) for b in filled]

        self.assertEqual(got, [mon, tue, wed, thu, fri])  # gaps filled, sorted
        wed_bar = next(b for b in filled if self.provider._chart_bar_trade_date(b) == wed)
        self.assertEqual((wed_bar.open, wed_bar.high, wed_bar.low, wed_bar.close, wed_bar.volume), (101.5, 102.5, 101.0, 102.0, 5000))
        fri_bar = filled[-1]
        self.assertEqual(fri_bar.close, 104.0)

    def test_eod_store_leaves_existing_bars_untouched_and_skips_scale_mismatch(self) -> None:
        mon, tue, wed = (date(2026, 7, 6), date(2026, 7, 7), date(2026, 7, 8))
        yahoo_bars = [self._daily_bar(mon, 100.0), self._daily_bar(tue, 101.0), self._daily_bar(wed, 102.0)]
        # A stale BSE paise-scale print (100× off) must be rejected, not injected;
        # and an already-present date must never be overwritten.
        self._write_eod_session(tue, {"TEST": [999.0, 999.0, 999.0, 999.0, 1]})  # existing date → ignored
        self._write_eod_session(date(2026, 7, 9), {"TEST": [1.01, 1.02, 1.00, 1.01, 7000]})  # 100× low → skip

        filled = self.provider._with_daily_eod_gaps_filled("TEST", yahoo_bars)

        self.assertEqual(len(filled), 3)  # no bad Thu bar added
        tue_bar = next(b for b in filled if self.provider._chart_bar_trade_date(b) == tue)
        self.assertEqual(tue_bar.close, 101.0)  # untouched

    def test_eod_store_absent_is_noop(self) -> None:
        bars = [self._daily_bar(date(2026, 7, 6), 100.0)]
        self.assertEqual(self.provider._with_daily_eod_gaps_filled("TEST", bars), bars)


if __name__ == "__main__":
    unittest.main()
