"""Tests for the session-aligned market frame.

The phantom-weekend-row test is the highest-value one here: the shipped
breadth file contains 110 Sunday rows carrying plausible-looking values, and
every downstream slope, divergence lookback and chart depends on them never
reaching the frame.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from app.services import market_frame as mf


def ts(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp())


def bar(iso: str, close: float, *, volume: float = 1000.0, high=None, low=None) -> dict:
    return {
        "time": ts(iso),
        "open": close,
        "high": high if high is not None else close,
        "low": low if low is not None else close,
        "close": close,
        "volume": volume,
    }


def breadth_row(iso: str, above50: float, above200: float = 60.0, **extra) -> dict:
    row = {
        "date": iso,
        "above_ma20_pct": extra.get("above_ma20_pct", above50),
        "above_ma50_pct": above50,
        "above_sma200_pct": above200,
        "new_high_52w_pct": extra.get("new_high_52w_pct", 0.0),
        "new_low_52w_pct": extra.get("new_low_52w_pct", 0.0),
    }
    return row


def breadth_doc(rows, universe=mf.DEFAULT_UNIVERSE, generated_at="2026-08-12T10:00:00Z") -> dict:
    return {"generated_at": generated_at, "universes": [{"universe": universe, "history": rows}]}


def xp_doc(rows, generated_at="2026-08-11T19:00:00+05:30") -> dict:
    return {"generated_at": generated_at, "days": rows}


class BuildFrameTests(unittest.TestCase):
    def test_sunday_rows_are_dropped(self):
        """A weekend breadth row absent from the index calendar must not survive.

        This is the shipped-data bug: Fri 61.90 / Sun 68.21 / Mon 64.08, where
        the Sunday value is computed over a handful of symbols.
        """
        bars = [bar("2026-08-07", 100.0), bar("2026-08-10", 101.0)]
        rows = [
            breadth_row("2026-08-07", 61.90),
            breadth_row("2026-08-09", 68.21),  # Sunday — never traded
            breadth_row("2026-08-10", 64.08),
        ]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]))
        self.assertEqual([r.date for r in frame], ["2026-08-07", "2026-08-10"])
        self.assertNotIn("2026-08-09", [r.date for r in frame])

    def test_all_zero_warmup_rows_are_dropped(self):
        bars = [bar("2026-08-07", 100.0), bar("2026-08-10", 101.0)]
        rows = [
            breadth_row("2026-08-07", 0.0, 0.0),  # warmup: writer fillna(0)
            breadth_row("2026-08-10", 64.0, 60.0),
        ]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]))
        self.assertEqual([r.date for r in frame], ["2026-08-10"])

    def test_missing_xp_is_none_not_zero(self):
        """XP lags price by a session; the gap must not read as a real zero."""
        bars = [bar("2026-08-10", 100.0), bar("2026-08-11", 101.0)]
        rows = [breadth_row("2026-08-10", 60.0), breadth_row("2026-08-11", 61.0)]
        xp = xp_doc([{"date": "2026-08-10", "xp_score": 14.6, "regime": "Progressive Exposure"}])
        frame = mf.build_frame(bars, breadth_doc(rows), xp)
        self.assertEqual(frame[0].xp_score, 14.6)
        self.assertEqual(frame[0].xp_regime, "Progressive Exposure")
        self.assertIsNone(frame[1].xp_score)
        self.assertIsNone(frame[1].xp_regime)

    def test_warmup_xp_rows_are_excluded(self):
        bars = [bar("2026-08-10", 100.0)]
        rows = [breadth_row("2026-08-10", 60.0)]
        xp = xp_doc([{"date": "2026-08-10", "xp_score": 1.5, "regime": "Avoid Longs", "warmup": True}])
        frame = mf.build_frame(bars, breadth_doc(rows), xp)
        self.assertIsNone(frame[0].xp_score)

    def test_participation_blend(self):
        self.assertEqual(mf.participation_of(60.0, 50.0), 57.0)  # .7*60 + .3*50

    def test_frame_is_session_ordered_without_duplicates(self):
        bars = [bar("2026-08-10", 100.0), bar("2026-08-07", 99.0), bar("2026-08-11", 101.0)]
        rows = [breadth_row(d, 60.0) for d in ("2026-08-11", "2026-08-07", "2026-08-10")]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]))
        dates = [r.date for r in frame]
        self.assertEqual(dates, sorted(dates))
        self.assertEqual(len(dates), len(set(dates)))

    def test_ten_sessions_ago_skips_calendar_gaps(self):
        """rows[i-10] must be ten *sessions* back, not ten calendar rows back."""
        sessions = [
            "2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05",
            "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19",
            "2026-06-22",
        ]  # a two-week holiday between the 5th and the 15th
        bars = [bar(d, 100.0 + i) for i, d in enumerate(sessions)]
        rows = [breadth_row(d, 60.0) for d in sessions]
        # weekend rows the writer would have manufactured across the gap
        rows += [breadth_row(d, 99.0) for d in ("2026-06-07", "2026-06-14", "2026-06-21")]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]))
        self.assertEqual(len(frame), len(sessions))
        self.assertEqual(frame[-1].date, "2026-06-22")
        self.assertEqual(frame[len(frame) - 1 - 10].date, "2026-06-01")

    def test_non_positive_close_bars_are_dropped(self):
        bars = [bar("2026-08-10", 0.0), bar("2026-08-11", 101.0)]
        rows = [breadth_row("2026-08-10", 60.0), breadth_row("2026-08-11", 61.0)]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]))
        self.assertEqual([r.date for r in frame], ["2026-08-11"])

    def test_zero_volume_is_preserved_not_dropped(self):
        """The newest index bar legitimately has volume 0; the row must survive."""
        bars = [bar("2026-08-11", 100.0, volume=0.0)]
        rows = [breadth_row("2026-08-11", 60.0)]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]))
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame[0].volume, 0.0)

    def test_unknown_universe_yields_empty_frame(self):
        bars = [bar("2026-08-11", 100.0)]
        rows = [breadth_row("2026-08-11", 60.0)]
        frame = mf.build_frame(bars, breadth_doc(rows), xp_doc([]), universe="Nonexistent")
        self.assertEqual(frame, [])

    def test_empty_inputs_are_safe(self):
        self.assertEqual(mf.build_frame([], {}, {}), [])
        self.assertEqual(mf.build_frame(None or [], {"universes": []}, {"days": []}), [])


class DegradedSourceTests(unittest.TestCase):
    """The production failure: breadth absent must not empty the frame.

    `free_historical_breadth.json` is gitignored and only written during a full
    snapshot refresh, which the deployed Space never runs — so it is simply not
    there. An inner join on it zeroed participation, the XP regime AND the
    distribution days, even though the index bars and the XP file were both
    present and current.
    """

    def test_frame_survives_missing_breadth(self):
        bars = [bar("2026-08-11", 100.0), bar("2026-08-12", 101.0)]
        xp = xp_doc([
            {"date": "2026-08-11", "xp_score": 12.8, "regime": "Progressive Exposure", "ma20_pct": 47.5},
            {"date": "2026-08-12", "xp_score": 13.1, "regime": "Progressive Exposure", "ma20_pct": 49.3},
        ])
        frame = mf.build_frame(bars, {"universes": []}, xp)
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame[-1].xp_regime, "Progressive Exposure")
        self.assertEqual(frame[-1].participation, 49.3)
        self.assertEqual(frame[-1].participation_source, "xp-universe")
        self.assertIsNone(frame[-1].above_ma50_pct)  # unknown, not zero

    def test_breadth_is_preferred_over_xp_when_both_present(self):
        bars = [bar("2026-08-12", 101.0)]
        rows = [breadth_row("2026-08-12", 60.0, 50.0)]
        xp = xp_doc([{"date": "2026-08-12", "xp_score": 13.1, "regime": "X", "ma20_pct": 49.3}])
        frame = mf.build_frame(bars, breadth_doc(rows), xp)
        self.assertEqual(frame[0].participation, 57.0)  # 0.7*60 + 0.3*50
        self.assertEqual(frame[0].participation_source, "nifty500-breadth")

    def test_frame_survives_missing_xp(self):
        bars = [bar("2026-08-12", 101.0)]
        rows = [breadth_row("2026-08-12", 60.0, 50.0)]
        frame = mf.build_frame(bars, breadth_doc(rows), {"days": []})
        self.assertEqual(len(frame), 1)
        self.assertIsNone(frame[0].xp_score)
        self.assertEqual(frame[0].participation_source, "nifty500-breadth")

    def test_bare_price_bar_with_no_joined_data_is_skipped(self):
        frame = mf.build_frame([bar("2026-08-12", 101.0)], {"universes": []}, {"days": []})
        self.assertEqual(frame, [])

    def test_participation_is_none_when_neither_source_has_it(self):
        bars = [bar("2026-08-12", 101.0)]
        xp = xp_doc([{"date": "2026-08-12", "xp_score": 13.1, "regime": "X"}])  # no ma20_pct
        frame = mf.build_frame(bars, {"universes": []}, xp)
        self.assertEqual(len(frame), 1)
        self.assertIsNone(frame[0].participation)
        self.assertIsNone(frame[0].participation_source)

    def test_sources_reports_which_participation_source_is_live(self):
        bars = [bar("2026-08-12", 101.0)]
        xp = xp_doc([{"date": "2026-08-12", "xp_score": 13.1, "regime": "X", "ma20_pct": 49.3}])
        src = mf.frame_sources(bars, {"universes": []}, xp)
        self.assertEqual(src["participation_source"], "xp-universe")
        self.assertEqual(src["breadth_sessions"], 0)
        self.assertEqual(src["aligned_sessions"], 1)


class FrameSourcesTests(unittest.TestCase):
    def test_counts_only_in_span_non_session_rows(self):
        """Breadth reaches further back than the index cache.

        Older rows are outside the calendar's span, not phantoms, and counting
        them would overstate the problem by an order of magnitude.
        """
        bars = [bar("2026-08-07", 100.0), bar("2026-08-10", 101.0)]
        rows = [
            breadth_row("2023-01-02", 55.0),  # long before the index cache starts
            breadth_row("2026-08-07", 61.0),
            breadth_row("2026-08-09", 68.0),  # in-span phantom Sunday
            breadth_row("2026-08-10", 64.0),
        ]
        src = mf.frame_sources(bars, breadth_doc(rows), xp_doc([]))
        self.assertEqual(src["rows_dropped_non_session"], 1)
        self.assertEqual(src["aligned_sessions"], 2)

    def test_staleness_warning_when_xp_lags(self):
        bars = [bar("2026-08-10", 100.0), bar("2026-08-11", 101.0)]
        rows = [breadth_row("2026-08-10", 60.0), breadth_row("2026-08-11", 61.0)]
        xp = xp_doc([{"date": "2026-08-10", "xp_score": 14.0, "regime": "Progressive Exposure"}])
        src = mf.frame_sources(bars, breadth_doc(rows), xp)
        self.assertIsNotNone(src["staleness_warning"])
        self.assertEqual(src["xp_last_session"], "2026-08-10")
        self.assertEqual(src["index_last_session"], "2026-08-11")

    def test_no_warning_when_current(self):
        bars = [bar("2026-08-11", 101.0)]
        rows = [breadth_row("2026-08-11", 61.0)]
        xp = xp_doc([{"date": "2026-08-11", "xp_score": 14.0, "regime": "Progressive Exposure"}])
        src = mf.frame_sources(bars, breadth_doc(rows), xp)
        self.assertIsNone(src["staleness_warning"])

    def test_empty_inputs_are_safe(self):
        src = mf.frame_sources([], {}, {})
        self.assertEqual(src["aligned_sessions"], 0)
        self.assertIsNone(src["index_last_session"])


if __name__ == "__main__":
    unittest.main()
