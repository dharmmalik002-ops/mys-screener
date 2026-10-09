"""An Indian index series that is a session short is completed from the official
close — and only when that is safe. The failure this pins: on 2026-10-08 every
index chart ended on the 7th (Yahoo leaves the newest index bar null after the
close), so the home briefing quoted Nifty 50 at 22,603 / -0.76% when the market
had closed at 22,231.8 / -1.64%, and the Markets page showed Smallcap 250 and
Midcap 150 a session old."""
import unittest
from datetime import date, datetime, timezone, timedelta
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

    def _complete(self, symbol, bars, stamp, row, quote=None, archive=None, today=date(2026, 10, 8)):
        """``archive`` maps a session date to that day's closing-file rows (None = not published)."""
        archive = archive or {}
        with patch.object(self.provider, "_nse_index_rows", return_value=(stamp, {"NIFTY 50": row} if row else {})), \
             patch.object(self.provider, "_nse_index_close_rows", side_effect=lambda d: archive.get(d)), \
             patch.object(self.provider, "_today_ist", return_value=today), \
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

    # --- the official per-session closing file: fills a gap of any length ---------------

    @staticmethod
    def archive_row(open_v, high, low, close, points):
        return {"nifty 50": {"Open Index Value": str(open_v), "High Index Value": str(high), "Low Index Value": str(low),
                             "Closing Index Value": str(close), "Points Change": str(points)}}

    def test_a_multi_session_gap_is_filled_from_the_archive(self):
        """Yahoo was days behind: 7th is our last bar, the 8th and 9th are both archived."""
        archive = {
            date(2026, 10, 8): self.archive_row(22599.05, 22599.05, 22179.9, 22231.8, -371.25),
            date(2026, 10, 9): self.archive_row(22250.0, 22500.0, 22200.0, 22457.15, 225.35),
        }
        out = self._complete("^NSEI", [bar(22603.05)], None, None, archive=archive, today=date(2026, 10, 9))
        self.assertEqual([b.close for b in out], [22603.05, 22231.8, 22457.15])
        self.assertEqual(out[1].low, 22179.9)  # official low, not a wickless synthetic candle

    def test_the_archive_works_while_the_next_session_is_open(self):
        """The failure that survived the first fix: at 10am on the 9th NSE's live row is the
        9th's, so its previous close (the 8th) no longer joined a chart that ended on the 7th."""
        archive = {date(2026, 10, 8): self.archive_row(22599.05, 22599.05, 22179.9, 22231.8, -371.25)}
        live_row = {"last": 22457.15, "previousClose": 22231.8, "open": 22250.0, "high": 22500.0, "low": 22200.0}
        out = self._complete("^NSEI", [bar(22603.05)], nse_stamp(9, 10, 0), live_row, archive=archive, today=date(2026, 10, 9))
        self.assertEqual([b.close for b in out], [22603.05, 22231.8])  # the open session is not appended

    def test_weekends_and_holidays_are_skipped(self):
        fri = int(datetime(2026, 10, 9, tzinfo=timezone.utc).timestamp())
        archive = {date(2026, 10, 12): self.archive_row(22450.0, 22500.0, 22400.0, 22480.0, 22.85)}
        out = self._complete("^NSEI", [bar(22457.15, fri)], None, None, archive=archive, today=date(2026, 10, 12))
        self.assertEqual([b.close for b in out], [22457.15, 22480.0])  # Sat/Sun have no file

    def test_a_session_missing_from_the_middle_is_inserted(self):
        """Yahoo serving the 9th while still lacking the 8th must not leave a hole in the candles."""
        oct9 = int(datetime(2026, 10, 9, tzinfo=timezone.utc).timestamp())
        archive = {date(2026, 10, 8): self.archive_row(22599.05, 22599.05, 22179.9, 22231.8, -371.25)}
        out = self._complete("^NSEI", [bar(22603.05), bar(22457.15, oct9)], None, None, archive=archive, today=date(2026, 10, 9))
        self.assertEqual([b.close for b in out], [22603.05, 22231.8, 22457.15])

    def test_a_discontinuous_archive_file_is_not_joined(self):
        archive = {date(2026, 10, 8): self.archive_row(1, 1, 1, 1000.0, -5.0)}
        out = self._complete("^NSEI", [bar(22603.05)], None, None, archive=archive)
        self.assertEqual(len(out), 1)

    def test_a_malformed_archive_row_adds_nothing(self):
        archive = {date(2026, 10, 8): {"nifty 50": {"Open Index Value": "n/a"}}}
        out = self._complete("^NSEI", [bar(22603.05)], None, None, archive=archive)
        self.assertEqual(len(out), 1)

    def test_range_names_the_front_end_sends_are_completed_too(self):
        """Home asks for "3Y" and Markets for "1Y"; both are daily series on the backend."""
        import asyncio

        async def run(timeframe):
            with patch.object(self.provider, "_get_chart_uncompleted", return_value=[bar(22603.05)]) as inner, \
                 patch.object(self.provider, "_nse_index_close_rows", return_value=None), \
                 patch.object(self.provider, "_today_ist", return_value=date(2026, 10, 8)), \
                 patch.object(self.provider, "_nse_index_rows", return_value=(nse_stamp(8, 15, 30), {"NIFTY 50": NSE_ROW})):
                async def fake(*_a, **_k):
                    return [bar(22603.05)]
                inner.side_effect = fake
                return await self.provider.get_chart("^NSEI", timeframe, 500)

        for timeframe in ("1D", "1Y", "3Y"):
            self.assertEqual(asyncio.run(run(timeframe))[-1].close, 22231.8, timeframe)
        for timeframe in ("15m", "1h"):
            self.assertEqual(asyncio.run(run(timeframe))[-1].close, 22603.05, timeframe)
        # weekly: this week's candle is folded from the completed daily series
        self.assertEqual(asyncio.run(run("1W"))[-1].close, 22231.8)

    def test_yahoo_smallcap_100_is_not_called_the_250(self):
        from app.providers.free import INDEX_SYMBOL_TO_NSE_NAME
        self.assertEqual(INDEX_SYMBOL_TO_NSE_NAME["^CNXSC"], "NIFTY SMALLCAP 100")
        self.assertEqual(INDEX_SYMBOL_TO_NSE_NAME["NIFTYSMLCAP250.NS"], "NIFTY SMALLCAP 250")


if __name__ == "__main__":
    unittest.main()
