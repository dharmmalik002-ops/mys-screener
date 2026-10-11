"""AI scanner: the model translates, the scanner picks.

Pins the three things a user relies on without seeing them:
* quarterly growth is measured against the right quarter and never invented
  (loss bases, stale filings);
* the chart-pattern detectors find the shape they are named after and reject
  its look-alikes (a V is not a cup, one touch is not resistance);
* whatever the model answers, only real filters run, and everything else is
  reported back as unsupported instead of vanishing.
"""

from __future__ import annotations

import asyncio
import math
import unittest
from datetime import date, datetime, timezone

from app.models.market import AiScanRequest, StockSnapshot
from app.scanners import chart_patterns as cp
from app.services import ai_scanner, close_history, quarterly_growth


def _q(period: str, sales: float | None, profit: float | None, eps: float | None = None, margin: float | None = None) -> dict:
    return {"period": period, "sales_crore": sales, "net_profit_crore": profit, "eps": eps, "operating_margin_pct": margin}


ROWS = [
    _q("Jun 2026", 150.0, 30.0, 3.0, 22.0),
    _q("Mar 2026", 140.0, 25.0, 2.5, 20.0),
    _q("Dec 2025", 130.0, 22.0, 2.2, 19.0),
    _q("Sep 2025", 120.0, 20.0, 2.0, 18.0),
    _q("Jun 2025", 100.0, 20.0, 2.0, 17.0),
    _q("Mar 2025", 110.0, 20.0, 2.0, 18.0),
    _q("Dec 2024", 100.0, 18.0, 1.8, 18.0),
    _q("Sep 2024", 100.0, 15.0, 1.5, 18.0),
]
TODAY = date(2026, 10, 10)


class QuarterlyGrowthTests(unittest.TestCase):
    def test_yoy_compares_the_same_quarter_a_year_earlier(self) -> None:
        growth = quarterly_growth.compute(ROWS, today=TODAY)
        self.assertEqual(growth.latest_period, "Jun 2026")
        self.assertAlmostEqual(growth.sales_growth_yoy_pct, 50.0)
        self.assertAlmostEqual(growth.profit_growth_yoy_pct, 50.0)
        self.assertAlmostEqual(growth.sales_growth_qoq_pct, round((150 / 140 - 1) * 100, 2))
        self.assertAlmostEqual(growth.operating_margin_change_yoy_pp, 5.0)
        # 540 over the four quarters vs 410 the year before.
        self.assertAlmostEqual(growth.sales_growth_ttm_pct, round((540 / 410 - 1) * 100, 2))
        self.assertEqual(len(growth.sales_growth_yoy_series), 4)

    def test_a_missing_filing_never_pairs_the_wrong_quarters(self) -> None:
        rows = [row for row in ROWS if row["period"] != "Jun 2025"]
        growth = quarterly_growth.compute(rows, today=TODAY)
        self.assertIsNone(growth.sales_growth_yoy_pct)
        self.assertIsNone(growth.sales_growth_ttm_pct)

    def test_growth_on_a_loss_is_not_a_number(self) -> None:
        rows = [dict(row) for row in ROWS]
        rows[4]["net_profit_crore"] = -5.0  # Jun 2025 was a loss
        growth = quarterly_growth.compute(rows, today=TODAY)
        self.assertIsNone(growth.profit_growth_yoy_pct)

    def test_a_stale_filing_has_no_current_figures(self) -> None:
        self.assertIsNone(quarterly_growth.compute(ROWS, today=date(2027, 6, 1)))


def _cup_handle_series() -> list[float]:
    advance = [70 + 30 * i / 59 for i in range(60)]
    cup = [100 - 25 * math.sin(math.pi * t / 60) for t in range(1, 61)]
    handle = [99, 97, 95, 94, 93, 93.5, 94, 95, 95.5, 96]
    return advance + cup + handle


def _v_series() -> list[float]:
    advance = [70 + 30 * i / 59 for i in range(60)]
    down = [100 - 25 * t / 30 for t in range(1, 31)]
    up = [75 + 25 * t / 30 for t in range(1, 31)]
    handle = [99, 97, 95, 94, 93, 93.5, 94, 95, 95.5, 96]
    return advance + down + up + handle


class ChartPatternTests(unittest.TestCase):
    def test_cup_and_handle_is_found(self) -> None:
        found = cp.detect_cup_handle(_cup_handle_series())
        self.assertIsNotNone(found)
        self.assertGreaterEqual(found.cup_depth_pct, 20)
        self.assertLessEqual(found.handle_depth_pct, 10)
        self.assertGreaterEqual(found.pct_below_pivot, 0)

    def test_a_v_is_not_a_cup(self) -> None:
        self.assertIsNone(cp.detect_cup_handle(_v_series()))

    def test_extended_past_the_pivot_is_not_forming(self) -> None:
        self.assertIsNone(cp.detect_cup_handle(_cup_handle_series() + [104.0, 110.0]))

    def test_resistance_needs_two_turns(self) -> None:
        once = [80 + i * 0.2 for i in range(60)] + [100, 95, 92, 90] + [90 + i * 0.5 for i in range(16)]
        self.assertIsNone(cp.detect_resistance(once))
        twice = (
            [80 + i * 0.3 for i in range(60)]
            + [100, 96, 92, 90, 89, 90, 93, 96, 99, 99.8, 96, 93, 91, 90, 92, 94, 96, 97, 97.5, 98]
        )
        found = cp.detect_resistance(twice)
        self.assertIsNotNone(found)
        self.assertGreaterEqual(found.touches, 2)
        self.assertLessEqual(found.pct_below, 5)

    def test_through_resistance_is_not_under_it(self) -> None:
        series = [80 + i * 0.3 for i in range(60)] + [100, 96, 92, 90, 93, 96, 99.8, 96, 92, 90, 94, 98, 103]
        self.assertIsNone(cp.detect_resistance(series))

    def test_double_bottom(self) -> None:
        decline = [120 - i * 0.6 for i in range(40)]  # 120 -> ~96
        left = [95, 93, 91, 90, 91, 93]
        middle = [96, 99, 102, 104, 103, 101, 99, 97, 95, 94]
        right = [92, 91, 90.5, 91.5, 93, 95, 98, 100, 101, 102]
        w = left + [93.5, 94] + middle + [93, 92.5] + right  # lows 90 and 90.5, 18 sessions apart
        found = cp.detect_double_bottom(decline + w)
        self.assertIsNotNone(found)
        self.assertGreaterEqual(found.middle_high, 104)

    def test_flat_base(self) -> None:
        advance = [60 + i for i in range(40)]
        base = [100 + (3 if i % 4 < 2 else -3) for i in range(40)]
        found = cp.detect_flat_base(advance + base + [102.0])
        self.assertIsNotNone(found)
        self.assertLessEqual(found.range_pct, 15)

    def test_inside_day_nr7_pocket_pivot(self) -> None:
        self.assertTrue(cp.is_inside_day([10, 12, 11.5], [8, 9, 9.5]))
        self.assertFalse(cp.is_inside_day([10, 12, 12.5], [8, 9, 9.5]))
        highs = [12, 12, 12, 12, 12, 12, 10.5]
        lows = [10, 10, 10, 10, 10, 10, 10]
        self.assertTrue(cp.is_nr7(highs, lows))
        # Same-day arrays, as the snapshot carries them. Down days: 1, 3, 5, 7, 10.
        closes = [10, 9.8, 10, 9.7, 10, 9.9, 10.1, 9.9, 10, 10.2, 10.1, 10.6]
        volumes = [100, 300, 100, 250, 100, 200, 100, 220, 100, 100, 150, 400]
        self.assertTrue(cp.is_pocket_pivot(closes, volumes))
        self.assertFalse(cp.is_pocket_pivot(closes, volumes[:-1] + [280]))


def _snapshot(symbol: str, *, price: float = 96.0, high_52w: float = 100.0) -> StockSnapshot:
    return StockSnapshot.model_validate({
        "symbol": symbol, "name": symbol, "exchange": "NSE", "sector": "Industrials",
        "sub_sector": "Capital Goods", "market_cap_crore": 2000.0, "last_price": price,
        "change_pct": 0.5, "volume": 100000, "avg_volume_20d": 100000,
        "day_high": price * 1.01, "day_low": price * 0.99, "ath": 200.0, "high_52w": high_52w, "low_52w": 60.0,
        "range_high_20d": 110.0, "benchmark_return_20d": 1.0, "sector_return_20d": 1.0,
        "pivot_high": 110.0, "darvas_high": 110.0, "darvas_low": 95.0,
        "pullback_depth_pct": 2.0, "trend_strength": 0.8,
        "recent_closes": [price] * 20,
        "history_session_date": date(2026, 9, 1),
    })


class SanitizeTests(unittest.TestCase):
    def test_only_real_filters_run_and_the_rest_is_reported(self) -> None:
        parsed = ai_scanner.sanitize({
            "filters": {
                "max_pct_from_52w_high": "20%",
                "min_sales_growth_yoy_pct": 20,
                "max_pe_ratio": 25,           # not a filter we have
                "min_rs_rating": 150,         # out of range
                "above_ema50": False,         # off means not set
            },
            "patterns": ["cup-handle", "head-and-shoulders"],
            "unsupported": [{"text": "good management", "reason": "not measurable"}],
            "summary": "test",
        })
        request: AiScanRequest = parsed["request"]
        self.assertEqual(request.max_pct_from_52w_high, 20.0)
        self.assertEqual(request.min_sales_growth_yoy_pct, 20.0)
        self.assertIsNone(request.min_rs_rating)
        self.assertFalse(request.above_ema50)
        self.assertEqual(request.patterns, ["cup-handle"])
        texts = " | ".join(item["text"] for item in parsed["unsupported"])
        for expected in ("good management", "max pe ratio", "head and shoulders", "RS rating"):
            self.assertIn(expected, texts)

    def test_criteria_read_as_plain_language(self) -> None:
        request = AiScanRequest(max_pct_from_52w_high=20, min_sales_growth_yoy_pct=20, growth_quarters=3)
        texts = [c["text"] for c in ai_scanner.describe(request)]
        self.assertIn("Below 52W high ≤ 20%", texts)
        self.assertIn("Sales growth YoY ≥ 20%", texts)
        self.assertIn("Growth held 3 quarters in a row", texts)

    def test_parse_calls_the_model_once_per_query(self) -> None:
        calls: list[str] = []

        async def fake(prompt: str) -> dict:
            calls.append(prompt)
            return {"filters": {"max_pct_from_52w_high": 20}, "patterns": ["vcp"], "summary": "s"}

        ai_scanner._parse_cache.clear()
        first = asyncio.run(ai_scanner.parse_with(fake, "VCP within 20% of the high"))
        second = asyncio.run(ai_scanner.parse_with(fake, "  vcp within 20%   of the high "))
        self.assertEqual(len(calls), 1)
        self.assertEqual(first, second)
        self.assertEqual(first["request"]["patterns"], ["vcp"])
        self.assertIn("vcp", calls[0])  # the pattern list reaches the model

    def test_an_answer_without_filters_is_an_error(self) -> None:
        async def fake(prompt: str) -> dict:
            return {"raw": "I think you should buy..."}

        ai_scanner._parse_cache.clear()
        with self.assertRaises(ValueError):
            asyncio.run(ai_scanner.parse_with(fake, "anything"))


class RunTests(unittest.TestCase):
    def setUp(self) -> None:
        close_history.reset_cache()
        quarterly_growth.reset_cache()
        stamp = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())
        close_history._cache = {
            "CUP": {"last_time": stamp, "closes": _cup_handle_series()},
            "VEE": {"last_time": stamp, "closes": _v_series()},
        }
        self._orig = quarterly_growth.bse_quarterly.results_for
        rows = {"CUP": ROWS, "VEE": [dict(r, sales_crore=100.0) for r in ROWS]}
        quarterly_growth.bse_quarterly.results_for = lambda symbol: rows.get(symbol, [])
        self._orig_date = quarterly_growth.date
        class _Today(date):
            @classmethod
            def today(cls):
                return TODAY

        quarterly_growth.date = _Today

    def tearDown(self) -> None:
        close_history.reset_cache()
        quarterly_growth.reset_cache()
        quarterly_growth.bse_quarterly.results_for = self._orig
        quarterly_growth.date = self._orig_date

    def test_pattern_and_fundamental_filters_combine(self) -> None:
        snapshots = [_snapshot("CUP"), _snapshot("VEE"), _snapshot("FAR", price=60.0)]
        request = AiScanRequest(max_pct_from_52w_high=20, min_sales_growth_yoy_pct=20, patterns=["cup-handle"])
        result = ai_scanner.run(request, snapshots)
        self.assertEqual([row["symbol"] for row in result["items"]], ["CUP"])
        row = result["items"][0]
        self.assertEqual(row["matched_patterns"], ["Cup and Handle"])
        self.assertAlmostEqual(row["metrics"]["sales_yoy"], 50.0)
        self.assertAlmostEqual(row["metrics"]["from_52w_high"], 4.0)
        self.assertEqual({c["id"] for c in result["columns"]}, {"from_52w_high", "sales_yoy"})

    def test_a_stock_without_filings_fails_a_growth_filter(self) -> None:
        result = ai_scanner.run(AiScanRequest(min_sales_growth_yoy_pct=0), [_snapshot("FAR")])
        self.assertEqual(result["items"], [])

    def test_all_patterns_must_match_when_asked(self) -> None:
        snapshots = [_snapshot("CUP")]
        any_of = ai_scanner.run(AiScanRequest(patterns=["cup-handle", "double-bottom"]), snapshots)
        all_of = ai_scanner.run(AiScanRequest(patterns=["cup-handle", "double-bottom"], pattern_match="all"), snapshots)
        self.assertEqual(len(any_of["items"]), 1)
        self.assertEqual(all_of["items"], [])


if __name__ == "__main__":
    unittest.main()
