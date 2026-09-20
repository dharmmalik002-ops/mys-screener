"""Tests for the bot's backtest engine.

These pin the properties that decide whether the study is research or fiction.
Every one of them corresponds to a way a backtest lies, and each is written so
that the *comfortable* mistake fails the test:

  - indicators that peek at future bars
  - fills at the signal bar's close instead of the next open
  - stops that fill at their level when price gapped straight through
  - a bar that hits both stop and target resolved in the trade's favour
  - costs quietly omitted
  - a "held-out" period that shares weeks with the training period
  - sixty cells tested at p<0.05 with no multiple-testing correction
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

import numpy as np

from app.services.bot import attribution as attr
from app.services.bot import indicators as ind
from app.services.bot import regime as rg
from app.services.bot.costs import DEFAULT_COSTS, CostModel
from app.services.bot.engine import ExitModel, Trade, simulate_symbol
from app.services.bot.features import build_features
from app.services.bot.history import Bars
from app.services.bot.strategies import StrategySpec


def make_bars(closes, highs=None, lows=None, opens=None, volume=1_000_000.0, symbol="TEST") -> Bars:
    n = len(closes)
    closes = np.asarray(closes, dtype=np.float64)
    return Bars(
        symbol=symbol,
        dates=np.array([date(2020, 1, 1) + timedelta(days=i) for i in range(n)], dtype=object),
        open=np.asarray(opens if opens is not None else closes, dtype=np.float64),
        high=np.asarray(highs if highs is not None else closes * 1.01, dtype=np.float64),
        low=np.asarray(lows if lows is not None else closes * 0.99, dtype=np.float64),
        close=closes,
        volume=np.full(n, volume, dtype=np.float64),
    )


class IndicatorCausalityTests(unittest.TestCase):
    """Truncating the input must not change any earlier value."""

    def setUp(self) -> None:
        rng = np.random.default_rng(7)
        self.series = np.cumsum(rng.normal(size=400)) + 500.0

    def test_no_lookahead(self) -> None:
        cases = {
            "sma": lambda v: ind.sma(v, 50),
            "ema": lambda v: ind.ema(v, 21),
            "rsi": lambda v: ind.rsi(v, 14),
            "rolling_max": lambda v: ind.rolling_max(v, 20),
            "slope": lambda v: ind.slope_pct_per_bar(v, 20),
            "percentile": lambda v: ind.rolling_percentile_rank(v, 60),
            "drawdown": ind.drawdown_pct,
        }
        cut = 250
        for name, fn in cases.items():
            with self.subTest(indicator=name):
                full = fn(self.series)[:cut]
                truncated = fn(self.series[:cut])
                both = np.isfinite(full) & np.isfinite(truncated)
                np.testing.assert_allclose(full[both], truncated[both], rtol=1e-9, atol=1e-9)
                # The warm-up pattern must match too, or one version is
                # inventing an opinion the other does not have.
                np.testing.assert_array_equal(np.isfinite(full), np.isfinite(truncated))

    def test_warmup_is_nan_not_zero(self) -> None:
        """A zero warm-up reads as a real value and fires comparisons."""
        out = ind.sma(self.series, 50)
        self.assertTrue(np.isnan(out[:49]).all())
        self.assertTrue(np.isfinite(out[49]))


class EntryAndExitTests(unittest.TestCase):
    """The four modelling choices that keep the fills honest."""

    def _spec(self, signal_index: int, n: int, stop_atr_mult: float = 2.0) -> tuple[StrategySpec, np.ndarray]:
        signals = np.zeros(n, dtype=bool)
        signals[signal_index] = True
        spec = StrategySpec(
            "t", "Test", "breakout", "fixture", ("bull_strong",),
            lambda _f: signals, stop_atr_mult=stop_atr_mult,
        )
        return spec, signals

    def _features(self, bars: Bars):
        features = build_features(bars)
        self.assertIsNotNone(features, "fixture must be long enough to build features")
        return features

    def test_fills_at_next_open_not_signal_close(self) -> None:
        """Filling at the signal bar's close books a price nobody could get."""
        n = 300
        closes = np.full(n, 100.0)
        opens = np.full(n, 100.0)
        signal_at = 250
        # The bar after the signal opens well away from the signal close. If the
        # engine fills at the signal close the entry is 100, not 108.
        opens[signal_at + 1] = 108.0
        closes[signal_at + 1] = 108.0
        bars = make_bars(closes, opens=opens)
        spec, signals = self._spec(signal_at, n)

        trades = simulate_symbol(spec, self._features(bars), signals, ExitModel(), DEFAULT_COSTS)
        self.assertEqual(len(trades), 1)
        # Entry is the next open plus slippage against us, never the signal close.
        self.assertGreater(trades[0].entry, 107.9)
        self.assertEqual(trades[0].entry_day, bars.dates[signal_at + 1])

    def test_gap_through_stop_fills_at_the_open(self) -> None:
        """A stop is not a guarantee. Gaps fill where the market opens."""
        n = 300
        closes = np.full(n, 100.0)
        opens = np.full(n, 100.0)
        highs = closes * 1.005
        lows = closes * 0.995
        signal_at = 250
        # Two bars later the stock opens 20% down — far below any plausible stop.
        crash = signal_at + 2
        opens[crash] = 80.0
        closes[crash] = 79.0
        highs[crash] = 80.5
        lows[crash] = 78.0
        bars = make_bars(closes, highs=highs, lows=lows, opens=opens)
        spec, signals = self._spec(signal_at, n)

        trades = simulate_symbol(spec, self._features(bars), signals, ExitModel(), DEFAULT_COSTS)
        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade.exit_reason, "gap_stop")
        # The loss must be far worse than the nominal stop distance. If this
        # assertion ever relaxes, tail risk has been modelled away.
        self.assertLess(trade.r_multiple, -2.0)
        self.assertLess(trade.exit_price, trade.stop)

    def test_same_bar_stop_and_target_resolves_as_a_loss(self) -> None:
        """Daily bars cannot order the two. Ambiguity goes against the trade."""
        n = 300
        closes = np.full(n, 100.0)
        opens = np.full(n, 100.0)
        highs = closes * 1.005
        lows = closes * 0.995
        signal_at = 250
        both = signal_at + 1
        # One enormous bar that spans the stop below and the target above.
        opens[both] = 100.0
        highs[both] = 140.0
        lows[both] = 60.0
        closes[both] = 135.0
        bars = make_bars(closes, highs=highs, lows=lows, opens=opens)
        spec, signals = self._spec(signal_at, n)

        exits = ExitModel(target_r=2.0, max_hold_sessions=15, trail_after_r=None, breakeven_after_r=None)
        trades = simulate_symbol(spec, self._features(bars), signals, exits, DEFAULT_COSTS)
        self.assertEqual(len(trades), 1)
        self.assertIn(trades[0].exit_reason, {"stop", "gap_stop"})
        self.assertLess(trades[0].r_multiple, 0.0)

    def test_costs_reduce_the_result(self) -> None:
        """A zero-cost run must beat a costed one on the identical signal."""
        n = 300
        closes = np.linspace(100.0, 160.0, n)
        bars = make_bars(closes)
        spec, signals = self._spec(250, n)
        features = self._features(bars)

        free = CostModel(slippage_bps=0.0, dp_charge=0.0)
        costed = simulate_symbol(spec, features, signals, ExitModel(), DEFAULT_COSTS)
        uncosted = simulate_symbol(spec, features, signals, ExitModel(), free)
        self.assertTrue(costed and uncosted)
        self.assertLess(costed[0].r_multiple, uncosted[0].r_multiple)

    def test_overlapping_signals_do_not_stack(self) -> None:
        """The same setup firing daily is one decision, not thirty."""
        n = 300
        closes = np.linspace(100.0, 200.0, n)
        bars = make_bars(closes)
        signals = np.zeros(n, dtype=bool)
        signals[240:260] = True     # fires twenty sessions running
        spec = StrategySpec("t", "Test", "breakout", "f", ("bull_strong",), lambda _f: signals)

        trades = simulate_symbol(spec, self._features(bars), signals, ExitModel(), DEFAULT_COSTS)
        # A 90-session hold means the first trade is still open for all of them.
        self.assertLessEqual(len(trades), 2)


class CostModelTests(unittest.TestCase):
    def test_round_trip_is_material(self) -> None:
        """If this ever reads near zero the schedule has been gutted."""
        drag = DEFAULT_COSTS.round_trip_pct(500.0, 400.0) + 2 * DEFAULT_COSTS.slippage_bps / 100.0
        self.assertGreater(drag, 0.35)
        self.assertLess(drag, 1.0)

    def test_slippage_always_hurts(self) -> None:
        self.assertGreater(DEFAULT_COSTS.fill_price(100.0, "buy"), 100.0)
        self.assertLess(DEFAULT_COSTS.fill_price(100.0, "sell"), 100.0)


class AttributionTests(unittest.TestCase):
    def _trade(self, day: date, r: float, strategy: str = "s", regime: str = "bull_strong") -> Trade:
        return Trade(
            strategy=strategy, symbol="X", signal_day=day, entry_day=day, exit_day=day,
            entry=100.0, stop=98.0, exit_price=100.0 + r, exit_reason="target",
            sessions_held=5, r_multiple=r, gross_pct=r, net_pct=r,
            mae_r=-0.2, mfe_r=max(r, 0.1), risk_pct=2.0, atr_pct_at_entry=2.0, regime=regime,
        )

    def test_split_is_chronological(self) -> None:
        """A random split leaks the same week into both halves."""
        trades = [self._trade(date(2020, 1, 1) + timedelta(days=i), 0.1) for i in range(100)]
        boundary = attr.split_date(trades, 0.6)
        early = [t for t in trades if t.entry_day < boundary]
        late = [t for t in trades if t.entry_day >= boundary]
        self.assertTrue(early and late)
        self.assertLess(max(t.entry_day for t in early), min(t.entry_day for t in late))

    def test_thin_cells_are_not_reportable(self) -> None:
        few = [self._trade(date(2020, 1, 1), 2.0) for _ in range(attr.MIN_SAMPLE - 1)]
        cell = attr.summarise_cell("s", "bull_strong", few)
        self.assertFalse(cell.reportable)
        self.assertFalse(cell.significant)

    def test_fdr_is_stricter_than_raw_p_values(self) -> None:
        """The realistic family: mostly null, one real effect, two marginals.

        Naive testing at p<0.05 calls all three significant, and the two
        marginals are exactly the false discoveries that make a backtest look
        like it found several edges. BH keeps the real one and drops them.

        (A family where 59 of 60 cells sit at p=0.04 is *not* this case —
        there BH accepts them all, and correctly so: that pattern is itself
        strong evidence against the global null.)
        """
        p_values = [0.001, 0.045, 0.045] + list(np.linspace(0.10, 0.99, 57))
        cells = []
        for i, p_value in enumerate(p_values):
            cell = attr.CellStats(f"s{i}", "bull_strong", 50, 40.0, 0.1, 0.1, 5.0,
                                  1.1, 1.0, -1.0, 10.0, 0.5)
            cell.reportable = True
            cell.p_value = p_value
            cells.append(cell)

        naive = sum(1 for c in cells if c.p_value < 0.05)
        attr.apply_fdr(cells, alpha=0.10)
        survivors = [c for c in cells if c.significant]

        self.assertEqual(naive, 3, "fixture should have three cells under 0.05")
        self.assertLess(len(survivors), naive, "BH must be stricter than raw p<0.05")
        self.assertTrue(cells[0].significant, "the real effect should survive")

    def test_fdr_accepts_a_family_that_is_broadly_real(self) -> None:
        """The mirror case, so the correction is not mistaken for a blanket filter."""
        cells = []
        for i in range(60):
            cell = attr.CellStats(f"s{i}", "bull_strong", 50, 40.0, 0.1, 0.1, 5.0,
                                  1.1, 1.0, -1.0, 10.0, 0.5)
            cell.reportable = True
            cell.p_value = 0.0001 if i == 0 else 0.04
            cells.append(cell)
        attr.apply_fdr(cells, alpha=0.10)
        self.assertTrue(all(c.significant for c in cells))

    def test_decay_is_reported_not_averaged(self) -> None:
        """Strong then dead must read 'decayed', never a flattering mean."""
        early = [self._trade(date(2020, 1, 1) + timedelta(days=i), 1.0) for i in range(60)]
        late = [self._trade(date(2023, 1, 1) + timedelta(days=i), -1.0) for i in range(40)]
        results = attr.validate(early + late, rg.REGIMES, train_fraction=0.6)
        verdicts = {c.verdict for c in results}
        self.assertIn("decayed", verdicts)


class RegimeTests(unittest.TestCase):
    def test_run_smoothing_is_backward_only(self) -> None:
        """Absorbing a short run forward would read the future."""
        labels = ["bear"] * 20 + ["choppy"] * 2 + ["bear"] * 20
        smoothed = rg._smooth_runs(labels, rg.MIN_REGIME_RUN)
        self.assertEqual(smoothed[20:22], ["bear", "bear"])
        self.assertEqual(len(smoothed), len(labels))

    def test_leading_short_run_is_left_alone(self) -> None:
        """There is no prior regime to absorb into at the very start."""
        labels = ["choppy"] * 2 + ["bull_strong"] * 30
        smoothed = rg._smooth_runs(labels, rg.MIN_REGIME_RUN)
        self.assertEqual(smoothed[0], "choppy")

    def test_deep_drawdown_below_trend_is_bear(self) -> None:
        self.assertEqual(
            rg._classify_one(above_200=False, above_50=False, slope_200=-0.1,
                             drawdown=-25.0, breadth_200=20.0, crosses_50=0, breadth_known=True),
            "bear",
        )

    def test_unknown_breadth_never_reads_as_strong(self) -> None:
        """Without a breadth count, 'broad participation' is unmeasured."""
        self.assertEqual(
            rg._classify_one(above_200=True, above_50=True, slope_200=0.1,
                             drawdown=-1.0, breadth_200=0.0, crosses_50=0, breadth_known=False),
            "bull_narrow",
        )


if __name__ == "__main__":
    unittest.main()


class SecondCohortTests(unittest.TestCase):
    """The five strategies that were measured and left unregistered.

    They are individually sound — +0.15R to +0.36R, all significant — and
    enabling them still cost 4.3 points of median CAGR, because the book is
    capital-constrained and they crowded better candidates out of it. These
    tests keep them working so the measurement stays reproducible, and pin
    that they are structurally distinct rather than relabelled duplicates.
    """

    def _features(self, n: int = 400, benchmark: bool = True):
        rng = np.random.default_rng(3)
        closes = np.cumsum(rng.normal(0.3, 2.0, n)) + 300.0
        closes = np.maximum(closes, 10.0)
        bars = make_bars(closes, highs=closes * 1.02, lows=closes * 0.98, opens=closes)
        index = np.cumsum(rng.normal(0.1, 1.0, n)) + 1000.0 if benchmark else None
        return build_features(bars, index)

    def test_all_second_cohort_strategies_run_without_error(self) -> None:
        from app.services.bot.strategies import SECOND_COHORT

        features = self._features()
        self.assertIsNotNone(features)
        for name, generate in SECOND_COHORT:
            with self.subTest(strategy=name):
                signals = generate(features)
                self.assertEqual(len(signals), len(features.bars))
                self.assertEqual(signals.dtype, bool)

    def test_relative_strength_setups_go_quiet_without_a_benchmark(self) -> None:
        """No benchmark must mean no opinion, not a fabricated ratio."""
        from app.services.bot.strategies import SECOND_COHORT

        features = self._features(benchmark=False)
        self.assertIsNotNone(features)
        generate = dict(SECOND_COHORT)["rs_leader_pullback"]
        signals = generate(features)
        # RS is nan throughout, so the leading gate can never pass.
        self.assertEqual(int(signals.sum()), 0)

    def test_the_library_is_not_all_one_trade(self) -> None:
        """Two setups firing on identical bars are one setup with two names.

        Checked across the registered library *and* the unregistered cohort,
        because the claim being tested is that these describe different market
        events — which is true or false regardless of whether a given setup is
        currently switched on.
        """
        from app.services.bot.strategies import SECOND_COHORT, STRATEGIES

        features = self._features()
        fired = {s.id: s.generate(features) for s in STRATEGIES}
        fired.update({name: generate(features) for name, generate in SECOND_COHORT})
        active = {k: v for k, v in fired.items() if v.any()}
        self.assertGreaterEqual(len(active), 3, "fixture should trigger several setups")

        # No two setups may fire on an identical set of bars.
        seen: dict[bytes, str] = {}
        for name, signals in active.items():
            key = signals.tobytes()
            self.assertNotIn(
                key, seen,
                f"{name} fires identically to {seen.get(key)} — it is not a distinct setup",
            )
            seen[key] = name
