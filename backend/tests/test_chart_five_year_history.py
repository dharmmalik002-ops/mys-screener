"""Charts carry five years of daily bars, and keep them.

Pinned here:
  * the daily chart limit is the provider's five-year depth;
  * nothing that rewrites the daily cache cuts it back (the daily patches used
    to keep 520 bars, a snapshot rebuild seeds only three years);
  * a cache written before the change (~520 bars) is downloaded once at full
    depth, a young listing marked complete is never downloaded again, and a
    failed download serves the old cache and backs off;
  * "Load Full History" goes past the cache instead of returning it;
  * the per-open work stays cheap: the earnings merge is reused and the built
    responses held in memory are bounded.
"""

from __future__ import annotations

import asyncio
import json
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from app.core.config import Settings
from app.models.market import ChartBar
from app.providers.free import CHART_DAILY_HISTORY_BARS, FreeMarketDataProvider
from app.services import dashboard_service
from app.services.dashboard_service import DashboardService


def _provider(root: Path) -> FreeMarketDataProvider:
    provider = FreeMarketDataProvider()
    provider.chart_cache_dir = root / "chart_cache"
    provider.chart_cache_dir.mkdir()
    provider.snapshot_cache_path = root / "free_snapshots.json"
    provider.eod_bars_dir = root / "eod_bars"
    provider._instrument_key_cache = None
    provider._chart_symbol_scale_cache = None
    provider._recent_eod_bars_cache = None
    return provider


def _trading_days(provider: FreeMarketDataProvider, count: int, *, end: date | None = None) -> list[date]:
    day = end or (date.today() - timedelta(days=3))
    days: list[date] = []
    while len(days) < count:
        if provider._is_trading_day_ist(day):
            days.append(day)
        day -= timedelta(days=1)
    return list(reversed(days))


def _bars(days: list[date], *, scale: float = 1.0) -> list[ChartBar]:
    bars = []
    for index, day in enumerate(days):
        close = round((100.0 + index * 0.05) * scale, 2)
        bars.append(
            ChartBar(
                time=int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp()),
                open=close,
                high=round(close * 1.01, 2),
                low=round(close * 0.99, 2),
                close=close,
                volume=1_000 + index,
            )
        )
    return bars


def _frame(bars: list[ChartBar]) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                "Open": bar.open,
                "High": bar.high,
                "Low": bar.low,
                "Close": bar.close,
                "Adj Close": bar.close,
                "Volume": bar.volume,
                "Stock Splits": 0.0,
            }
            for bar in bars
        ],
        index=pd.DatetimeIndex([pd.Timestamp(datetime.fromtimestamp(bar.time, tz=timezone.utc)) for bar in bars]),
    )


def _stored(provider: FreeMarketDataProvider, symbol: str) -> dict:
    return json.loads(provider._chart_cache_path(symbol, "1D").read_text(encoding="utf-8"))


class FiveYearDepthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.provider = _provider(Path(self.temp_dir.name))
        self.full_days = _trading_days(self.provider, CHART_DAILY_HISTORY_BARS + 40)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_the_daily_chart_asks_for_five_years(self) -> None:
        self.assertGreaterEqual(CHART_DAILY_HISTORY_BARS, 5 * 248)
        # Above 1300 the fetch switches to Yahoo's "max" period.
        self.assertLessEqual(CHART_DAILY_HISTORY_BARS, 1300)
        self.assertEqual(DashboardService._chart_bar_limit("1D"), CHART_DAILY_HISTORY_BARS)

    def test_a_daily_patch_keeps_five_years(self) -> None:
        days = self.full_days[:-1]
        self.provider._write_chart_cache("AAA", "1D", _bars(days[-CHART_DAILY_HISTORY_BARS:]), history_complete=True)
        session_bar = _bars(self.full_days)[-1]
        with mock.patch.object(self.provider, "_snapshot_session_bar", return_value=session_bar), mock.patch.object(
            self.provider, "_bar_matches_snapshot_session", return_value=True
        ):
            self.provider._patch_daily_chart_cache_with_snapshot("AAA")

        stored = _stored(self.provider, "AAA")
        self.assertEqual(len(stored["bars"]), CHART_DAILY_HISTORY_BARS)
        self.assertEqual(stored["bars"][-1]["time"], session_bar.time)
        self.assertTrue(stored["history_complete"])

    def test_a_cache_stores_at_most_five_years(self) -> None:
        self.provider._write_chart_cache("AAA", "1D", _bars(_trading_days(self.provider, 2000)))
        stored = _stored(self.provider, "AAA")
        self.assertEqual(len(stored["bars"]), CHART_DAILY_HISTORY_BARS)
        self.assertTrue(stored["history_complete"])

    def test_a_two_year_cache_is_downloaded_once_at_five_years(self) -> None:
        self.provider._write_chart_cache("AAA", "1D", _bars(self.full_days[-520:]))
        self.assertFalse(_stored(self.provider, "AAA")["history_complete"])
        download = mock.Mock(return_value=_frame(_bars(self.full_days)))
        with mock.patch.object(self.provider, "_download_history_frame", download):
            bars, complete = self.provider._fetch_chart_bars_with_depth("AAA", "1D", CHART_DAILY_HISTORY_BARS)

        self.assertEqual(len(bars), CHART_DAILY_HISTORY_BARS)
        self.assertTrue(complete)
        self.assertEqual(download.call_count, 1)
        self.assertEqual(download.call_args.kwargs["period"], "5y")

    def test_a_young_listing_marked_complete_is_not_downloaded_again(self) -> None:
        self.provider._write_chart_cache("NEWCO", "1D", _bars(self.full_days[-300:]), history_complete=True)
        download = mock.Mock(side_effect=AssertionError("must not download"))
        with mock.patch.object(self.provider, "_download_history_frame", download):
            bars, complete = self.provider._fetch_chart_bars_with_depth("NEWCO", "1D", CHART_DAILY_HISTORY_BARS)

        self.assertEqual(len(bars), 300)
        self.assertTrue(complete)
        download.assert_not_called()

    def test_a_failed_download_serves_the_old_cache_and_backs_off(self) -> None:
        self.provider._write_chart_cache("AAA", "1D", _bars(self.full_days[-520:]))
        download = mock.Mock(side_effect=RuntimeError("Yahoo refused"))
        with mock.patch.object(self.provider, "_download_history_frame", download):
            bars, complete = self.provider._fetch_chart_bars_with_depth("AAA", "1D", CHART_DAILY_HISTORY_BARS)
            again, _ = self.provider._fetch_chart_bars_with_depth("AAA", "1D", CHART_DAILY_HISTORY_BARS)

        self.assertEqual(len(bars), 520)
        self.assertFalse(complete)
        self.assertEqual(len(again), 520)
        # The second open did not wait on Yahoo again.
        self.assertEqual(download.call_count, 1)

    def test_load_full_history_goes_past_the_cache(self) -> None:
        self.provider._write_chart_cache("AAA", "1D", _bars(self.full_days[-CHART_DAILY_HISTORY_BARS:]), history_complete=True)
        long_days = _trading_days(self.provider, 2500)
        download = mock.Mock(return_value=_frame(_bars(long_days)))
        with mock.patch.object(self.provider, "_download_history_frame", download):
            bars = self.provider._fetch_chart_bars("AAA", "1D", 5000)

        self.assertEqual(download.call_args.kwargs["period"], "max")
        self.assertEqual(len(bars), 2500)

    def test_a_snapshot_rebuild_keeps_the_older_bars(self) -> None:
        cached = _bars(self.full_days[-CHART_DAILY_HISTORY_BARS - 1 : -1])
        self.provider._write_chart_cache("AAA", "1D", cached, history_complete=True)
        # The rebuild downloads three years, ending one session later.
        all_bars = _bars(self.full_days[-CHART_DAILY_HISTORY_BARS - 1 :])
        self.provider._seed_daily_chart_cache("AAA", _frame(all_bars[-740:]))

        stored = _stored(self.provider, "AAA")
        self.assertEqual(len(stored["bars"]), CHART_DAILY_HISTORY_BARS)
        self.assertEqual(stored["bars"][0]["time"], all_bars[-CHART_DAILY_HISTORY_BARS].time)
        self.assertEqual(stored["bars"][-1]["time"], all_bars[-1].time)
        self.assertTrue(stored["history_complete"])

    def test_a_rebuild_across_a_split_is_not_glued_onto_old_prices(self) -> None:
        cached = _bars(self.full_days[-CHART_DAILY_HISTORY_BARS:])
        self.provider._write_chart_cache("AAA", "1D", cached, history_complete=True)
        # After a 1:2 split every adjusted price is half the cached one.
        self.provider._seed_daily_chart_cache("AAA", _frame(_bars(self.full_days[-CHART_DAILY_HISTORY_BARS:], scale=0.5)[-740:]))

        stored = _stored(self.provider, "AAA")
        self.assertEqual(len(stored["bars"]), 740)
        # Short and not complete: the next stale open downloads five years again.
        self.assertFalse(stored["history_complete"])


class _ChartStub:
    def __init__(self) -> None:
        self.snapshots: list = []
        self.updated_at = datetime(2026, 10, 9, tzinfo=timezone.utc)

    async def get_snapshots(self, market_cap_min_crore: float):
        return self.snapshots

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
        return [ChartBar(time=1_700_000_000, open=1.0, high=1.0, low=1.0, close=1.0, volume=1)]

    def get_snapshot_updated_at(self) -> datetime:
        return self.updated_at


class ChartOpenCostTests(unittest.TestCase):
    def test_the_earnings_merge_is_reused_until_the_snapshot_changes(self) -> None:
        provider = _ChartStub()
        provider.snapshots = [SimpleNamespace(symbol="AAA"), SimpleNamespace(symbol="BBB")]
        service = DashboardService(provider=provider, settings=Settings())
        with mock.patch.object(service, "_with_earnings_metrics", side_effect=lambda rows: list(rows)) as merge:
            first = asyncio.run(service._snapshots())
            second = asyncio.run(service._snapshots())
            self.assertEqual(merge.call_count, 1)
            self.assertEqual([s.symbol for s in first], [s.symbol for s in second])
            # Each caller gets its own list.
            self.assertIsNot(first, second)
            first.reverse()
            self.assertEqual([s.symbol for s in asyncio.run(service._snapshots())], ["AAA", "BBB"])

            provider.snapshots = [SimpleNamespace(symbol="CCC")]
            self.assertEqual([s.symbol for s in asyncio.run(service._snapshots())], ["CCC"])
            self.assertEqual(merge.call_count, 2)

    def test_built_chart_responses_held_in_memory_are_bounded(self) -> None:
        service = DashboardService(provider=_ChartStub(), settings=Settings())

        async def open_charts() -> None:
            for symbol in ("^A", "^B", "^C", "^D", "^E"):
                await service.get_chart(symbol, "1D")

        with mock.patch.object(dashboard_service, "CHART_RESPONSE_CACHE_MAX", 3):
            asyncio.run(open_charts())

        self.assertEqual([key[0] for key in service._chart_response_cache], ["^C", "^D", "^E"])


if __name__ == "__main__":
    unittest.main()
