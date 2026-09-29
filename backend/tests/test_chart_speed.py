"""Chart opens must not pay for work unrelated to the chart.

Two regressions pinned here:
  * `_resolve_ticker` parsed the whole ~40 MB snapshot file on every call, and
    every chart build calls it twice — ~0.5 s of CPU per chart.
  * After a deploy the Space's chart_cache/ is empty; the universe warm fills
    it in the background so a user's first click is not a Yahoo download.
"""

from __future__ import annotations

import asyncio
import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

from app.providers.free import FreeMarketDataProvider
from app.services import maintenance


class TickerLookupIsCachedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        self.provider = FreeMarketDataProvider()
        self.provider.snapshot_cache_path = Path(self.temp_dir.name) / "free_snapshots.json"
        self.provider._instrument_key_cache = None

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, rows: list[dict], mtime: float) -> None:
        self.provider.snapshot_cache_path.write_text(json.dumps(rows), encoding="utf-8")
        os.utime(self.provider.snapshot_cache_path, (mtime, mtime))

    def test_snapshot_file_is_parsed_once_per_version(self) -> None:
        self._write([{"symbol": "ABC", "instrument_key": "ABC.BO"}, {"symbol": "XYZ"}], 1_000_000.0)
        with mock.patch("app.providers.free.json.loads", wraps=json.loads) as loads:
            self.assertEqual(self.provider._resolve_ticker("ABC"), "ABC.BO")
            self.assertEqual(self.provider._resolve_ticker("XYZ"), "XYZ.NS")
            self.assertEqual(self.provider._resolve_ticker("ABC"), "ABC.BO")
        self.assertEqual(loads.call_count, 1)

    def test_a_rewritten_snapshot_file_is_picked_up(self) -> None:
        self._write([{"symbol": "ABC", "instrument_key": "ABC.BO"}], 1_000_000.0)
        self.assertEqual(self.provider._resolve_ticker("ABC"), "ABC.BO")
        self._write([{"symbol": "ABC", "instrument_key": "ABC.NS"}], 1_000_100.0)
        self.assertEqual(self.provider._resolve_ticker("ABC"), "ABC.NS")

    def test_missing_file_falls_back_to_nse(self) -> None:
        self.assertEqual(self.provider._resolve_ticker("ABC"), "ABC.NS")
        self.assertEqual(self.provider._resolve_ticker("^NSEI"), "^NSEI")


class _FakeProvider:
    def __init__(self, warm: set[str], failing: set[str] = frozenset()) -> None:
        self.warm = set(warm)
        self.failing = set(failing)
        self.fetched: list[str] = []

    def _chart_cache_path(self, symbol: str, timeframe: str) -> Path:
        return Path("/nonexistent") / symbol if symbol not in self.warm else Path(__file__)

    def _is_chart_cache_fresh(self, symbol: str, timeframe: str) -> bool:
        return symbol in self.warm

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
        self.fetched.append(symbol)
        if symbol in self.failing:
            raise RuntimeError("no data")
        self.warm.add(symbol)
        return [object()]


def _service(provider: _FakeProvider, symbols: list[str]) -> SimpleNamespace:
    snapshots = [SimpleNamespace(symbol=s, avg_rupee_volume_30d_crore=float(len(symbols) - i)) for i, s in enumerate(symbols)]

    async def _snapshots():
        return snapshots

    return SimpleNamespace(provider=provider, _snapshots=_snapshots, _chart_bar_limit=lambda timeframe: 600)


class UniverseChartWarmTests(unittest.TestCase):
    def _run(self, provider: _FakeProvider, symbols: list[str]) -> dict:
        with mock.patch("app.scanners.definitions.scan_catalog_with_counts", return_value=([], {})):
            return asyncio.run(
                maintenance.warm_universe_chart_cache("india", _service(provider, symbols), pause_seconds=0)
            )

    def test_every_cold_symbol_is_fetched_and_warm_ones_are_skipped(self) -> None:
        provider = _FakeProvider(warm={"^NSEI", "^BSESN", "^NSEBANK", "AAA"})
        result = self._run(provider, ["AAA", "BBB", "CCC"])
        self.assertEqual(provider.fetched, ["BBB", "CCC"])
        self.assertEqual(result["warmed"], 2)
        self.assertEqual(result["skipped"], 4)

    def test_a_second_run_over_a_warm_disk_fetches_nothing(self) -> None:
        provider = _FakeProvider(warm={"^NSEI", "^BSESN", "^NSEBANK"})
        self._run(provider, ["AAA", "BBB"])
        provider.fetched.clear()
        self._run(provider, ["AAA", "BBB"])
        self.assertEqual(provider.fetched, [])

    def test_one_failing_symbol_does_not_stop_the_run(self) -> None:
        provider = _FakeProvider(warm={"^NSEI", "^BSESN", "^NSEBANK"}, failing={"BBB"})
        result = self._run(provider, ["AAA", "BBB", "CCC"])
        self.assertEqual(provider.fetched, ["AAA", "BBB", "CCC"])
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["warmed"], 2)


if __name__ == "__main__":
    unittest.main()
