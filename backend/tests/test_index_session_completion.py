"""An Indian index series that is a session short is completed from the official
close — and only when that is safe. The failure this pins: on 2026-10-08 every
index chart ended on the 7th (Yahoo leaves the newest index bar null after the
close), so the home briefing quoted Nifty 50 at 22,603 / -0.76% when the market
had closed at 22,231.8 / -1.64%, and the Markets page showed Smallcap 250 and
Midcap 150 a session old."""
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from app.models.market import ChartBar
from app.providers.free import FreeMarketDataProvider, IST

OCT7 = int(datetime(2026, 10, 7, tzinfo=timezone.utc).timestamp())


def bar(close: float, ts: int = OCT7) -> ChartBar:
    return ChartBar(time=ts, open=close, high=close, low=close, close=close, volume=0)


def nse_stamp(day: int, hour: int, minute: int) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=IST)


NSE_ROW = {"last": 22231.8, "previousClose": 22603.05, "open": 22599.05, "high": 22599.05, "low": 22180.3}


class IndexSessionCompletionTests(unittest.TestCase):
    def setUp(self):
        self.provider = FreeMarketDataProvider(eod_only_mode=True)

    def _complete(self, symbol, bars, stamp, row, quote=None):
        with patch.object(self.provider, "_nse_index_rows", return_value=(stamp, {"NIFTY 50": row} if row else {})), \
             patch.object(self.provider, "_fetch_quote_batch", return_value=quote or {}):
            return self.provider._with_completed_index_session(symbol, bars)

    def test_a_finished_session_is_appended_with_official_values(self):
        out = self._complete("^NSEI", [bar(22603.05)], nse_stamp(8, 15, 30), NSE_ROW)
        self.assertEqual(len(out), 2)
        self.assertEqual((out[-1].close, out[-1].low), (22231.8, 22180.3))
        self.assertAlmostEqual((out[-1].close / out[-2].close - 1) * 100, -1.64, places=2)

    def test_a_mid_session_print_is_never_appended(self):
        out = self._complete("^NSEI", [bar(22603.05)], nse_stamp(8, 11, 0), NSE_ROW)
        self.assertEqual(len(out), 1)

    def test_a_series_that_already_has_the_session_is_left_alone(self):
        oct8 = int(datetime(2026, 10, 8, tzinfo=timezone.utc).timestamp())
        out = self._complete("^NSEI", [bar(22603.05), bar(22231.8, oct8)], nse_stamp(8, 15, 30), NSE_ROW)
        self.assertEqual(len(out), 2)

    def test_a_discontinuous_series_is_not_joined(self):
        # our last close is 2 months old: the quote belongs to a different stretch
        out = self._complete("^NSEI", [bar(24583.8)], nse_stamp(8, 15, 30), NSE_ROW)
        self.assertEqual(len(out), 1)

    def test_falls_back_to_the_quote_when_nse_cannot_be_reached(self):
        close_stamp = int(nse_stamp(8, 15, 31).timestamp())
        quote = {"^NSEI": {"regularMarketPrice": 22231.8, "regularMarketPreviousClose": 22603.05,
                           "regularMarketTime": close_stamp, "regularMarketOpen": 22599.05,
                           "regularMarketDayHigh": 22599.05, "regularMarketDayLow": 22180.3}}
        out = self._complete("^NSEI", [bar(22603.05)], None, None, quote)
        self.assertEqual(out[-1].close, 22231.8)

    def test_stocks_and_foreign_indices_are_untouched(self):
        for symbol in ("RELIANCE", "^GSPC"):
            out = self._complete(symbol, [bar(22603.05)], nse_stamp(8, 15, 30), NSE_ROW)
            self.assertEqual(len(out), 1, symbol)

    def test_range_names_the_front_end_sends_are_completed_too(self):
        """Home asks for "3Y" and Markets for "1Y"; both are daily series on the backend."""
        import asyncio

        async def run(timeframe):
            with patch.object(self.provider, "_get_chart_uncompleted", return_value=[bar(22603.05)]) as inner, \
                 patch.object(self.provider, "_nse_index_rows", return_value=(nse_stamp(8, 15, 30), {"NIFTY 50": NSE_ROW})):
                async def fake(*_a, **_k):
                    return [bar(22603.05)]
                inner.side_effect = fake
                return await self.provider.get_chart("^NSEI", timeframe, 500)

        for timeframe in ("1D", "1Y", "3Y"):
            self.assertEqual(asyncio.run(run(timeframe))[-1].close, 22231.8, timeframe)
        for timeframe in ("1W", "15m", "1h"):
            self.assertEqual(asyncio.run(run(timeframe))[-1].close, 22603.05, timeframe)

    def test_yahoo_smallcap_100_is_not_called_the_250(self):
        from app.providers.free import INDEX_SYMBOL_TO_NSE_NAME
        self.assertEqual(INDEX_SYMBOL_TO_NSE_NAME["^CNXSC"], "NIFTY SMALLCAP 100")
        self.assertEqual(INDEX_SYMBOL_TO_NSE_NAME["NIFTYSMLCAP250.NS"], "NIFTY SMALLCAP 250")


if __name__ == "__main__":
    unittest.main()
