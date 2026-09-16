"""Tests for per-stock Weinstein stage analysis.

The point of most of these is restraint: the classifier must refuse to place a
stage it does not have the history for, and must not drift from the sector
version that shares its calibration.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.services import stock_stages
from app.services.mutual_funds import sector_stages


def _bars(rate: float, n: int = 600, start_price: float = 100.0) -> list[dict]:
    """Daily bars compounding at `rate` per session, weekends skipped."""
    day = date(2024, 1, 1)
    out: list[dict] = []
    while len(out) < n:
        if day.weekday() < 5:
            price = start_price * (rate ** len(out))
            stamp = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
            out.append({"time": stamp, "close": price, "high": price * 1.01, "low": price * 0.99})
        day += timedelta(days=1)
    return out


def test_a_sustained_advance_reads_as_stage_2():
    result = stock_stages.stage_for_bars(_bars(1.0012))
    assert result["available"] is True
    assert result["stage"] == 2
    assert result["stage_label"] == "Advancing"


def test_a_sustained_decline_reads_as_stage_4():
    result = stock_stages.stage_for_bars(_bars(0.9988))
    assert result["available"] is True
    assert result["stage"] == 4


def test_a_short_history_refuses_to_guess():
    """A newly listed stock has no cycle to place. Returning Stage 1 because
    the average happens to be flat would be confidently wrong."""
    result = stock_stages.stage_for_bars(_bars(1.001, n=60))
    assert result["available"] is False
    assert "history" in result["reason"]
    assert "stage" not in result


def test_unusable_bars_are_dropped_not_defaulted():
    bars = _bars(1.0012)
    for bar in bars[:5]:
        bar["close"] = 0          # a zero close is missing data, not a price
    result = stock_stages.stage_for_bars(bars)
    assert result["available"] is True
    # The five bad bars are gone, not carried through as zeros.
    assert result["price"] > 0


def test_bars_may_be_objects_as_well_as_dicts():
    """The provider hands back pydantic Candles; the tests use dicts. Both have
    to work or the endpoint passes its tests and fails in production."""
    class Candle:
        def __init__(self, row):
            self.time, self.close, self.high, self.low = row["time"], row["close"], row["high"], row["low"]

    result = stock_stages.stage_for_bars([Candle(r) for r in _bars(1.0012)])
    assert result["available"] is True
    assert result["stage"] == 2


def test_labels_come_from_the_one_shared_definition():
    """Two copies of the stage vocabulary is exactly the bug the app already
    fixed once for sectors."""
    for stage in (1, 2, 3, 4):
        assert stock_stages.STAGE_ACTION_NOTE[stage]
        assert sector_stages.STAGE_LABELS[stage]
    result = stock_stages.stage_for_bars(_bars(1.0012))
    assert result["stage_label"] == sector_stages.STAGE_LABELS[result["stage"]]
    assert result["blurb"] == sector_stages.STAGE_BLURBS[result["stage"]]


def test_the_note_never_tells_the_reader_what_to_do():
    """Same constraint as the fund review (CLAUDE.md gotcha 12): describe the
    condition, do not issue the trade."""
    banned = ("you should buy", "you should sell", "buy now", "sell now", "we recommend")
    for note in stock_stages.STAGE_ACTION_NOTE.values():
        lowered = note.lower()
        for phrase in banned:
            assert phrase not in lowered, note


def test_minimum_bar_count_covers_the_classifier_requirement():
    """MIN_DAILY_BARS has to imply MIN_WEEKS of weekly closes, or the endpoint
    accepts input the classifier then rejects."""
    assert stock_stages.MIN_DAILY_BARS / 5 >= sector_stages.MIN_WEEKS
