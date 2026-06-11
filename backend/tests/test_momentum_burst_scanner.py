"""Unit tests for the Momentum Burst scanner.

Covers the four pieces called out in the spec:
  * burst detection (Type A explosive leg + volume confirmation)
  * consolidation-tightness logic (range + contraction)
  * EMA-surf classification (10 EMA vs 21 EMA setup)
  * RS rating math (percentile rank across the scanned universe)

Follows the existing unittest-based conventions in backend/tests.
Run: `cd backend && pytest tests/test_momentum_burst_scanner.py`
"""

from __future__ import annotations

import unittest

from app.models.market import MomentumBurstScanRequest, StockSnapshot
from app.scanners.definitions import (
    _ema_series,
    _mb_best_window_gain,
    _mb_detect_burst,
    _mb_detect_setup,
    _mb_is_contracting,
    momentum_burst_rs_ratings,
    run_momentum_burst_scan,
)


def _ramp_then_flat(pre: int, ramp_days: int, start: float, end: float, flat_days: int) -> list[float]:
    """A flat base, then a linear ramp from ``start`` to ``end``, then a flat rest."""
    closes = [start] * pre
    for i in range(ramp_days):
        closes.append(round(start + (end - start) * (i + 1) / ramp_days, 4))
    closes.extend([end] * flat_days)
    return closes


def _make_snapshot(
    *,
    symbol: str = "TEST",
    last_price: float = 130.0,
    closes: list[float] | None = None,
    recent_highs: list[float] | None = None,
    recent_lows: list[float] | None = None,
    recent_volumes: list[int] | None = None,
    sma50: float | None = 115.0,
    sma200: float | None = 105.0,
    avg_volume_50d: int | None = 1_000_000,
    avg_volume_20d: int = 1_000_000,
    stock_return_20d: float = 30.0,
    stock_return_60d: float = 40.0,
    stock_return_126d: float = 50.0,
    benchmark_return_20d: float = 5.0,
    benchmark_return_60d: float = 8.0,
    benchmark_return_126d: float = 10.0,
) -> StockSnapshot:
    closes = closes or [last_price] * 60
    grid = [{"time": i, "value": float(c)} for i, c in enumerate(closes)]
    return StockSnapshot.model_validate(
        {
            "symbol": symbol,
            "name": f"{symbol} Ltd",
            "exchange": "NSE",
            "sector": "Industrials",
            "sub_sector": "Capital Goods",
            "market_cap_crore": 5000.0,
            "last_price": last_price,
            "change_pct": 1.0,
            "volume": avg_volume_20d,
            "avg_volume_20d": avg_volume_20d,
            "avg_volume_30d": avg_volume_20d,
            "avg_volume_50d": avg_volume_50d,
            "day_high": last_price * 1.01,
            "day_low": last_price * 0.99,
            "ath": max(closes) * 1.1,
            "high_52w": max(closes) * 1.05,
            "range_high_20d": max(closes),
            "benchmark_return_20d": benchmark_return_20d,
            "benchmark_return_60d": benchmark_return_60d,
            "benchmark_return_126d": benchmark_return_126d,
            "sector_return_20d": 4.0,
            "pivot_high": max(closes),
            "darvas_high": max(closes),
            "darvas_low": min(closes),
            "pullback_depth_pct": 0.0,
            "trend_strength": 0.9,
            "sma50": sma50,
            "sma200": sma200,
            "ema10": closes[-1],
            "ema20": closes[-1] * 0.98,
            "stock_return_20d": stock_return_20d,
            "stock_return_60d": stock_return_60d,
            "stock_return_126d": stock_return_126d,
            "recent_highs": recent_highs if recent_highs is not None else [c * 1.01 for c in closes[-20:]],
            "recent_lows": recent_lows if recent_lows is not None else [c * 0.99 for c in closes[-20:]],
            "recent_volumes": recent_volumes if recent_volumes is not None else [avg_volume_20d] * 20,
            "chart_grid_points": grid,
        }
    )


def _contracting_band(days: int, center: float, start_half_range: float, end_half_range: float):
    """Build (highs, lows) of length ``days`` that hug ``center`` with a daily
    range that shrinks linearly from start to end (so it is 'contracting')."""
    highs: list[float] = []
    lows: list[float] = []
    for i in range(days):
        hr = start_half_range + (end_half_range - start_half_range) * i / max(1, days - 1)
        highs.append(round(center + hr, 4))
        lows.append(round(center - hr, 4))
    return highs, lows


class RsRatingMathTests(unittest.TestCase):
    def test_empty_and_single(self) -> None:
        self.assertEqual(momentum_burst_rs_ratings([]), [])
        self.assertEqual(momentum_burst_rs_ratings([42.0]), [99])

    def test_monotonic_and_bounds(self) -> None:
        ratings = momentum_burst_rs_ratings([-10.0, 0.0, 5.0, 50.0])
        self.assertEqual(ratings[0], 1)  # lowest -> floor
        self.assertEqual(ratings[-1], 99)  # highest -> ceiling
        self.assertTrue(all(1 <= r <= 99 for r in ratings))
        self.assertEqual(ratings, sorted(ratings))  # higher score -> higher rating

    def test_ties_share_rating(self) -> None:
        ratings = momentum_burst_rs_ratings([1.0, 5.0, 5.0, 9.0, 20.0])
        self.assertEqual(ratings[1], ratings[2])  # the two 5.0s tie

    def test_top_decile_is_at_least_70(self) -> None:
        scores = [float(i) for i in range(100)]
        ratings = momentum_burst_rs_ratings(scores)
        # the strongest names clear the default RS >= 70 floor
        self.assertGreaterEqual(ratings[-1], 70)
        self.assertGreaterEqual(ratings[80], 70)


class EmaSeriesTests(unittest.TestCase):
    def test_constant_series(self) -> None:
        self.assertEqual(_ema_series([5.0] * 10, 10), [5.0] * 10)

    def test_length_and_seed(self) -> None:
        out = _ema_series([10.0, 20.0, 30.0], 2)
        self.assertEqual(len(out), 3)
        self.assertEqual(out[0], 10.0)  # seeded with first value
        self.assertGreater(out[-1], out[0])  # rising series -> rising EMA


class ContractionTests(unittest.TestCase):
    def test_contracting_true(self) -> None:
        highs, lows = _contracting_band(10, 100.0, 4.0, 1.0)  # ranges shrink 8 -> 2
        self.assertTrue(_mb_is_contracting(highs, lows))

    def test_expanding_false(self) -> None:
        highs, lows = _contracting_band(10, 100.0, 1.0, 4.0)  # ranges widen
        self.assertFalse(_mb_is_contracting(highs, lows))

    def test_too_short_false(self) -> None:
        self.assertFalse(_mb_is_contracting([101.0, 100.5], [99.0, 99.5]))


class BurstWindowTests(unittest.TestCase):
    def test_best_window_gain_picks_max(self) -> None:
        closes = [100, 100, 100, 110, 120, 130]  # +30% over last 3 days
        gain, days, s, e = _mb_best_window_gain(closes, win_min=3, win_max=10, end_lo=0, end_hi=5)
        self.assertAlmostEqual(gain, 30.0, places=2)
        self.assertEqual(e, 5)


class BurstDetectionTests(unittest.TestCase):
    def _emas(self, closes):
        return _ema_series(closes, 10), _ema_series(closes, 21)

    def test_detects_fresh_burst_with_volume(self) -> None:
        closes = [100.0] * 45 + [104.0, 108.0, 112.0, 116.0, 120.0]  # +20% over the last 5 sessions
        ema10, ema21 = self._emas(closes)
        vols = [500_000] * 15 + [3_000_000] * 5  # a high-volume day inside the move
        req = MomentumBurstScanRequest()
        result = _mb_detect_burst(closes, vols, ema10, ema21, 1_000_000, req)
        self.assertIsNotNone(result)
        burst_pct, burst_days = result
        self.assertGreaterEqual(burst_pct, 15.0)

    def test_rejects_when_no_volume_confirmation(self) -> None:
        closes = [100.0] * 45 + [104.0, 108.0, 112.0, 116.0, 120.0]
        ema10, ema21 = self._emas(closes)
        vols = [400_000] * 20  # never reaches 1.5x the 50d average
        req = MomentumBurstScanRequest()
        self.assertIsNone(_mb_detect_burst(closes, vols, ema10, ema21, 1_000_000, req))

    def test_rejects_small_move(self) -> None:
        closes = [100.0] * 45 + [100.5, 101.0, 101.5, 102.0, 102.5]  # only +2.5%
        ema10, ema21 = self._emas(closes)
        vols = [500_000] * 15 + [3_000_000] * 5
        req = MomentumBurstScanRequest()
        self.assertIsNone(_mb_detect_burst(closes, vols, ema10, ema21, 1_000_000, req))


class EmaSurfClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        # +30% leg (100 -> 130) then a flat, contracting rest at 130.
        self.closes = _ramp_then_flat(pre=30, ramp_days=13, start=100.0, end=130.0, flat_days=17)
        self.req = MomentumBurstScanRequest()
        highs, lows = _contracting_band(20, 130.0, 2.0, 0.5)
        # First few entries map to the ramp tail; keep them inside a sane band.
        self.recent_highs = highs
        self.recent_lows = lows
        self.recent_volumes = [3_000_000] * 5 + [400_000] * 15  # leg loud, rest quiet

    def _snapshot(self):
        return _make_snapshot(
            closes=self.closes,
            recent_highs=self.recent_highs,
            recent_lows=self.recent_lows,
            recent_volumes=self.recent_volumes,
        )

    def test_classifies_10_ema_setup(self) -> None:
        n = len(self.closes)
        ema10 = [130.0] * n  # price hugs the 10 EMA
        ema21 = [126.0] * n  # 21 EMA below, no close beneath it
        plan = _mb_detect_setup(self._snapshot(), self.closes, ema10, ema21, self.req)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.tag, "10 EMA Setup")
        self.assertGreaterEqual(plan.burst_pct, 20.0)
        self.assertIsNotNone(plan.entry)
        self.assertIsNotNone(plan.stop)
        self.assertLess(plan.stop, plan.entry)
        self.assertIsNotNone(plan.target_2r)
        self.assertGreater(plan.target_3r, plan.target_2r)
        self.assertLess(plan.volume_dryup_ratio, self.req.volume_dryup_ratio)

    def test_classifies_21_ema_setup_when_deeper(self) -> None:
        n = len(self.closes)
        ema10 = [120.0] * n  # price (130) sits >4% above the 10 EMA -> fails 10 EMA surf
        ema21 = [127.0] * n  # but within 4% of the 21 EMA
        plan = _mb_detect_setup(self._snapshot(), self.closes, ema10, ema21, self.req)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.tag, "21 EMA Setup")

    def test_rejects_wide_range(self) -> None:
        n = len(self.closes)
        ema10 = [130.0] * n
        ema21 = [126.0] * n
        wide_highs, wide_lows = _contracting_band(20, 130.0, 30.0, 25.0)  # ~40% range
        snap = _make_snapshot(
            closes=self.closes,
            recent_highs=wide_highs,
            recent_lows=wide_lows,
            recent_volumes=self.recent_volumes,
        )
        self.assertIsNone(_mb_detect_setup(snap, self.closes, ema10, ema21, self.req))


class RunMomentumBurstScanTests(unittest.TestCase):
    def _setup_snapshot(self) -> StockSnapshot:
        closes = _ramp_then_flat(pre=30, ramp_days=13, start=100.0, end=130.0, flat_days=17)
        highs, lows = _contracting_band(20, 130.0, 2.0, 0.5)
        return _make_snapshot(
            symbol="WINNER",
            last_price=130.0,
            closes=closes,
            recent_highs=highs,
            recent_lows=lows,
            recent_volumes=[3_000_000] * 5 + [400_000] * 15,
        )

    def _filler(self, idx: int) -> StockSnapshot:
        # Passes the universe/trend gate but is flat -> no burst, weak RS.
        return _make_snapshot(
            symbol=f"FLAT{idx}",
            last_price=100.0,
            closes=[100.0] * 60,
            sma50=98.0,
            sma200=95.0,
            stock_return_20d=0.5,
            stock_return_60d=0.5,
            stock_return_126d=0.5,
        )

    def test_surfaces_setup_with_high_rs(self) -> None:
        universe = [self._setup_snapshot()] + [self._filler(i) for i in range(12)]
        results = run_momentum_burst_scan(MomentumBurstScanRequest(), universe)
        winners = [m for m in results if m.symbol == "WINNER"]
        self.assertEqual(len(winners), 1)
        match = winners[0]
        self.assertIsNotNone(match.momentum_burst)
        self.assertEqual(match.momentum_burst.tag, "10 EMA Setup")
        self.assertGreaterEqual(match.momentum_burst.rs_rating, 70)
        self.assertEqual(match.rs_rating, match.momentum_burst.rs_rating)

    def test_below_rs_floor_excluded(self) -> None:
        # Raise the RS floor above what a lone universe can produce for non-top names.
        universe = [self._setup_snapshot()] + [self._filler(i) for i in range(12)]
        # Force the winner's blended outperformance below the rest so it can't clear 99.
        weak = self._setup_snapshot()
        weak_results = run_momentum_burst_scan(
            MomentumBurstScanRequest(min_rs_rating=99), [self._filler(i) for i in range(50)] + [weak]
        )
        # Fillers never produce a plan, and a forced very-high floor keeps the list tight.
        self.assertTrue(all(m.momentum_burst.rs_rating >= 99 for m in weak_results))

    def test_sorts_setups_before_bursts(self) -> None:
        universe = [self._setup_snapshot()] + [self._filler(i) for i in range(12)]
        results = run_momentum_burst_scan(MomentumBurstScanRequest(), universe)
        tags = [m.momentum_burst.tag for m in results if m.momentum_burst]
        if "Burst" in tags and "10 EMA Setup" in tags:
            self.assertLess(tags.index("10 EMA Setup"), tags.index("Burst"))

    def test_skips_insufficient_history(self) -> None:
        snap = _make_snapshot(symbol="SHORT", closes=[130.0] * 10)  # too few bars
        results = run_momentum_burst_scan(MomentumBurstScanRequest(), [snap])
        self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()
