from __future__ import annotations

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.mutual_funds import benchmarks, metrics, portfolio
from app.services.mutual_funds.holdings_enrich import enrich_holdings


def nav_series(days: int, *, start: date = date(2016, 1, 4), daily_growth: float = 0.0004, base: float = 100.0):
    """Business-day NAV series compounding at a fixed daily rate.

    Weekends are skipped so the series has the same shape as a real AMFI feed,
    which is what the as-of-date tolerance logic has to cope with.
    """
    dates: list[str] = []
    navs: list[float] = []
    nav = base
    day = start
    while len(dates) < days:
        if day.weekday() < 5:
            dates.append(day.isoformat())
            navs.append(nav)
            nav *= 1.0 + daily_growth
        day += timedelta(days=1)
    return dates, navs


class ReturnWindowTests(unittest.TestCase):
    def test_windows_under_a_year_are_absolute_and_over_are_annualised(self):
        dates, navs = nav_series(1500)
        result = metrics.point_to_point_returns(dates, navs)

        # 1M is an absolute move; a ~0.04%/day compounding series gains ~1.3%
        # over a month. Annualising it would report ~17%, which is the bug this
        # asserts against.
        self.assertLess(result["1m"], 3.0)
        # 3Y is annualised, so it should land near the ~10.5% implied CAGR
        # rather than the ~35% cumulative.
        self.assertGreater(result["3y"], 8.0)
        self.assertLess(result["3y"], 13.0)

    def test_window_longer_than_history_is_none_not_zero(self):
        dates, navs = nav_series(300)  # ~14 months
        result = metrics.point_to_point_returns(dates, navs)
        self.assertIsNotNone(result["1y"])
        self.assertIsNone(result["3y"])
        self.assertIsNone(result["10y"])

    def test_anchor_falls_back_to_previous_session_across_a_weekend(self):
        dates, navs = nav_series(400)
        # Every window must resolve despite the series having no weekend rows.
        for label in ("1w", "1m", "3m", "6m", "1y"):
            self.assertIsNotNone(metrics.point_to_point_returns(dates, navs)[label], label)

    def test_stale_series_does_not_report_a_return(self):
        """A fund that stopped reporting must not answer with an old number."""
        dates, navs = nav_series(500)
        # Drop the last 18 months of a 2-year window: the 1y anchor is now
        # further back than the tolerance allows.
        truncated_dates = dates[:80]
        truncated_navs = navs[:80]
        self.assertIsNone(metrics.point_to_point_returns(truncated_dates, truncated_navs)["1y"])


class RiskTests(unittest.TestCase):
    def test_monotonic_series_has_no_drawdown(self):
        _, navs = nav_series(600)
        self.assertAlmostEqual(metrics.max_drawdown_pct(navs), 0.0, places=6)
        self.assertAlmostEqual(metrics.current_drawdown_pct(navs), 0.0, places=6)

    def test_drawdown_measures_peak_to_trough_not_start_to_end(self):
        navs = [100.0, 120.0, 60.0, 130.0]
        # The fund ends up 30%, but it halved from its peak along the way.
        self.assertAlmostEqual(metrics.max_drawdown_pct(navs), -50.0, places=6)
        self.assertAlmostEqual(metrics.current_drawdown_pct(navs), 0.0, places=6)

    def test_volatility_needs_enough_observations(self):
        self.assertIsNone(metrics.volatility_pct([100.0, 101.0, 102.0]))

    def test_beta_of_a_series_against_itself_is_one(self):
        _, navs = nav_series(400, daily_growth=0.0003)
        # Perturb so the series is not perfectly smooth.
        wobbled = [nav * (1.02 if index % 3 else 0.99) for index, nav in enumerate(navs)]
        stats = metrics.beta_alpha(wobbled, wobbled, fund_cagr_pct=12.0, bench_cagr_pct=12.0)
        self.assertAlmostEqual(stats["beta"], 1.0, places=6)
        self.assertAlmostEqual(stats["correlation"], 1.0, places=6)
        # Same series both sides: no active risk, so no excess return.
        self.assertAlmostEqual(stats["alpha"], 0.0, places=6)

    def test_beta_scales_with_amplitude(self):
        base = [100.0]
        for step in range(300):
            base.append(base[-1] * (1.01 if step % 2 == 0 else 0.995))
        levered = [100.0]
        for step in range(300):
            levered.append(levered[-1] * (1.02 if step % 2 == 0 else 0.99))
        stats = metrics.beta_alpha(levered, base)
        self.assertGreater(stats["beta"], 1.7)
        self.assertLess(stats["beta"], 2.3)


class RollingReturnTests(unittest.TestCase):
    def test_steady_grower_never_lost_money_over_three_years(self):
        dates, navs = nav_series(2000)
        rolling = metrics.rolling_returns(dates, navs, window_years=3, step_days=14)
        self.assertGreater(rolling["count"], 20)
        self.assertEqual(rolling["pct_negative"], 0.0)
        # Median of a constant-growth series is its CAGR.
        self.assertGreater(rolling["median"], 8.0)
        self.assertLess(rolling["median"], 13.0)

    def test_too_short_a_history_yields_no_samples(self):
        dates, navs = nav_series(200)
        rolling = metrics.rolling_returns(dates, navs, window_years=5)
        self.assertEqual(rolling["count"], 0)
        self.assertIsNone(rolling["median"])


class RankingTests(unittest.TestCase):
    def make_rows(self):
        return [
            {"scheme_code": "a", "sub_category": "Small Cap", "return_3y": 25.0},
            {"scheme_code": "b", "sub_category": "Small Cap", "return_3y": 15.0},
            {"scheme_code": "c", "sub_category": "Small Cap", "return_3y": 20.0},
            {"scheme_code": "d", "sub_category": "Small Cap", "return_3y": None},
            {"scheme_code": "e", "sub_category": "Large Cap", "return_3y": 12.0},
        ]

    def rank(self, rows):
        metrics.assign_ranks(
            rows,
            value_key="return_3y",
            group_key="sub_category",
            rank_key="rank_3y",
            count_key="rank_count_3y",
            percentile_key="percentile_3y",
            quartile_key="quartile_3y",
        )

    def test_rank_is_within_category_and_best_is_first(self):
        rows = self.make_rows()
        self.rank(rows)
        by_code = {row["scheme_code"]: row for row in rows}
        self.assertEqual(by_code["a"]["rank_3y"], 1)
        self.assertEqual(by_code["c"]["rank_3y"], 2)
        self.assertEqual(by_code["b"]["rank_3y"], 3)
        # A different category ranks on its own, not against small caps.
        self.assertEqual(by_code["e"]["rank_3y"], 1)
        self.assertEqual(by_code["e"]["rank_count_3y"], 1)

    def test_funds_without_a_record_are_excluded_from_the_denominator(self):
        rows = self.make_rows()
        self.rank(rows)
        by_code = {row["scheme_code"]: row for row in rows}
        # Four small caps, but only three have a 3-year number: "1 of 3".
        self.assertEqual(by_code["a"]["rank_count_3y"], 3)
        self.assertIsNone(by_code["d"]["rank_3y"])

    def test_rank_never_exceeds_its_own_count(self):
        rows = self.make_rows()
        self.rank(rows)
        for row in rows:
            if row.get("rank_3y") is not None:
                self.assertLessEqual(row["rank_3y"], row["rank_count_3y"])

    def test_percentile_puts_the_best_fund_at_100(self):
        rows = self.make_rows()
        self.rank(rows)
        by_code = {row["scheme_code"]: row for row in rows}
        self.assertEqual(by_code["a"]["percentile_3y"], 100.0)
        self.assertEqual(by_code["b"]["percentile_3y"], 0.0)
        # A single-member category has no meaningful percentile.
        self.assertIsNone(by_code["e"]["percentile_3y"])


class XirrTests(unittest.TestCase):
    def test_single_buy_and_valuation_recovers_the_growth_rate(self):
        flows = [(date(2023, 1, 1), -100000.0), (date(2026, 1, 1), 200000.0)]
        # Doubling over 3 years is ~26% a year.
        self.assertAlmostEqual(metrics.xirr(flows), 25.99, places=1)

    def test_sip_xirr_sits_between_the_two_naive_answers(self):
        """The whole reason XIRR exists, pinned as a test.

        A 24-instalment SIP valued shortly after the last one has each rupee
        invested for a different length of time — on average about 1.06 years
        here, not the 2 years the calendar span suggests. So the honest
        annualised return must land between the two numbers a naive
        calculation would give:

          * cost-over-value (16.67%) treats a 2-year span as if it were one
            period, ignoring that the money was not all there for it;
          * annualising over the full span (8.01%) assumes every rupee was
            invested on day one, which understates it just as badly.
        """
        flows = [(date(2024, 1, 1) + timedelta(days=30 * month), -10000.0) for month in range(24)]
        flows.append((date(2026, 1, 1), 280000.0))
        result = metrics.xirr(flows)
        self.assertIsNotNone(result)

        cumulative = (280000.0 / 240000.0 - 1) * 100.0
        over_full_span = ((280000.0 / 240000.0) ** (365 / 731) - 1) * 100.0
        self.assertGreater(result, over_full_span)
        self.assertLess(result, cumulative)

    def test_all_outflows_has_no_solution(self):
        self.assertIsNone(metrics.xirr([(date(2024, 1, 1), -100.0), (date(2025, 1, 1), -100.0)]))


class SipExpansionTests(unittest.TestCase):
    def test_instalment_count_and_bounds(self):
        out = portfolio.expand_sip(start_date="2024-01-15", end_date="2024-12-15", amount=5000)
        self.assertEqual(len(out), 12)
        self.assertEqual(out[0]["date"], "2024-01-15")
        self.assertEqual(out[-1]["date"], "2024-12-15")
        self.assertTrue(all(item["amount"] == 5000 for item in out))

    def test_day_31_clamps_to_the_end_of_short_months(self):
        out = portfolio.expand_sip(
            start_date="2024-01-31", end_date="2024-04-30", amount=1000, day_of_month=31
        )
        self.assertEqual(
            [item["date"] for item in out],
            ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30"],
        )

    def test_reversed_range_yields_nothing(self):
        self.assertEqual(portfolio.expand_sip(start_date="2025-01-01", end_date="2024-01-01", amount=100), [])


class PositionValuationTests(unittest.TestCase):
    def test_units_derived_from_nav_on_the_purchase_date(self):
        dates, navs = nav_series(600, base=50.0, daily_growth=0.0005)
        position = {
            "id": "p1",
            "scheme_code": "123",
            "transactions": [{"id": "t1", "date": dates[0], "type": "buy", "amount": 50000.0, "units": None, "nav": None}],
        }
        valued = portfolio.value_position(position, nav_series={"dates": dates, "navs": navs})
        self.assertAlmostEqual(valued["units"], 50000.0 / navs[0], places=3)
        self.assertAlmostEqual(valued["current_value"], valued["units"] * navs[-1], places=1)
        self.assertGreater(valued["gain"], 0)

    def test_transaction_outside_nav_history_is_flagged_not_guessed(self):
        dates, navs = nav_series(300)
        position = {
            "id": "p1",
            "scheme_code": "123",
            # Years before the series starts — a typo, not a real purchase.
            "transactions": [{"id": "t1", "date": "2001-01-02", "type": "buy", "amount": 1000.0, "units": None, "nav": None}],
        }
        valued = portfolio.value_position(position, nav_series={"dates": dates, "navs": navs})
        self.assertEqual(valued["unpriced_transactions"], 1)
        self.assertEqual(valued["units"], 0.0)

    def test_redemption_cannot_drive_units_negative(self):
        dates, navs = nav_series(400)
        position = {
            "id": "p1",
            "scheme_code": "123",
            "transactions": [
                {"id": "t1", "date": dates[0], "type": "buy", "amount": 10000.0, "units": None, "nav": None},
                {"id": "t2", "date": dates[-1], "type": "sell", "amount": None, "units": 1e9, "nav": None},
            ],
        }
        valued = portfolio.value_position(position, nav_series={"dates": dates, "navs": navs})
        self.assertGreaterEqual(valued["units"], 0.0)

    def test_no_nav_series_reports_cost_only(self):
        position = {
            "id": "p1",
            "scheme_code": "123",
            "transactions": [{"id": "t1", "date": "2024-01-01", "type": "buy", "amount": 7000.0, "units": None, "nav": None}],
        }
        valued = portfolio.value_position(position, nav_series=None)
        self.assertEqual(valued["invested"], 7000.0)
        self.assertIsNone(valued["current_value"])


class PayloadNormalisationTests(unittest.TestCase):
    def test_removed_position_stays_removed(self):
        """PUT semantics: absence is a delete, not something to merge back."""
        payload = normalised = portfolio.normalise_payload({
            "positions": [
                {"scheme_code": "1", "transactions": []},
                {"scheme_code": "2", "transactions": []},
            ]
        })
        self.assertEqual(len(normalised["positions"]), 2)
        trimmed = portfolio.normalise_payload({"positions": [payload["positions"][0]]})
        self.assertEqual([p["scheme_code"] for p in trimmed["positions"]], ["1"])

    def test_junk_transactions_are_dropped(self):
        normalised = portfolio.normalise_payload({
            "positions": [{
                "scheme_code": "1",
                "transactions": [
                    {"date": "not-a-date", "amount": 100},
                    {"date": "2024-05-01"},                        # no amount and no units
                    {"date": "2024-05-02", "amount": 500},         # keep
                ],
            }]
        })
        self.assertEqual(len(normalised["positions"][0]["transactions"]), 1)

    def test_position_without_a_scheme_code_is_dropped(self):
        normalised = portfolio.normalise_payload({"positions": [{"transactions": []}]})
        self.assertEqual(normalised["positions"], [])


class BenchmarkResolutionTests(unittest.TestCase):
    def test_each_cap_category_maps_to_its_own_index(self):
        self.assertEqual(benchmarks.resolve("Small Cap").key, "niftysmallcap250")
        self.assertEqual(benchmarks.resolve("Mid Cap").key, "niftymidcap150")
        self.assertEqual(benchmarks.resolve("Large Cap").key, "nifty100")
        self.assertEqual(benchmarks.resolve("Flexi Cap").key, "nifty500")

    def test_small_cap_uses_a_total_return_index_fund_not_a_price_index(self):
        """Yahoo's Nifty Smallcap price series has no usable history, so this
        category must resolve to an index fund's NAV."""
        bench = benchmarks.resolve("Small Cap")
        self.assertEqual(bench.source, "mf")
        self.assertTrue(bench.total_return)
        self.assertIsNotNone(bench.scheme_code)

    def test_unknown_category_substring_still_finds_the_right_index(self):
        self.assertEqual(benchmarks.resolve("Smallcap 250 Index").key, "niftysmallcap250")
        self.assertEqual(benchmarks.resolve("Large & Midcap").key, "niftylargemidcap250")

    def test_hybrid_benchmark_is_marked_reference_only(self):
        """Hybrid schemes track CRISIL blended indices we cannot source, so the
        Nifty 50 line must be labelled a reference rather than the benchmark."""
        for name in ("Aggressive Hybrid", "Balanced Advantage", "Equity Savings", "Arbitrage"):
            self.assertTrue(benchmarks.resolve(name).is_reference_only, name)

    def test_unrecognised_category_falls_back_without_raising(self):
        self.assertEqual(benchmarks.resolve(None).key, benchmarks.DEFAULT_BENCHMARK.key)
        self.assertEqual(benchmarks.resolve("Brand New SEBI Category").key, benchmarks.DEFAULT_BENCHMARK.key)


class HoldingsEnrichmentTests(unittest.TestCase):
    def holding(self, name, weight, **kwargs):
        base = {
            "company_name": name,
            "corpus_per": weight,
            "market_value": weight * 10,
            "nature_name": "EQUITY",
            "sector_name": "Financial",
            "instrument_name": "Equity",
            "portfolio_date": "2026-06-29T18:30:00.000Z",
        }
        base.update(kwargs)
        return base

    def test_weights_come_from_the_amc_not_recomputed(self):
        enriched = enrich_holdings([self.holding("HDFC Bank Ltd", 8.0), self.holding("ITC Ltd", 6.0)])
        self.assertEqual(enriched["holdings"][0]["weight_pct"], 8.0)
        self.assertEqual(enriched["top10_weight_pct"], 14.0)
        self.assertEqual(enriched["portfolio_date"], "2026-06-29")

    def test_overseas_holdings_never_claim_a_domestic_symbol(self):
        enriched = enrich_holdings([self.holding("Alphabet Inc Forgn. Eq (GOOGL)", 3.0)])
        row = enriched["holdings"][0]
        self.assertTrue(row["is_foreign"])
        self.assertIsNone(row["symbol"])
        self.assertEqual(row["asset_class"], "international_equity")

    def test_futures_positions_are_bucketed_as_derivatives(self):
        enriched = enrich_holdings([self.holding("JSW Steel Limited July 2026 Future", 2.0)])
        row = enriched["holdings"][0]
        self.assertTrue(row["is_derivative"])
        self.assertEqual(row["asset_class"], "derivatives")

    def test_cash_and_debt_are_split_out_of_equity(self):
        enriched = enrich_holdings([
            self.holding("HDFC Bank Ltd", 90.0),
            self.holding("TREPS", 8.0, nature_name="CASH", instrument_name="TREPS"),
            self.holding("7.1% GOI 2029", 2.0, nature_name="DEBT", instrument_name="Government Bond"),
        ])
        self.assertEqual(enriched["asset_allocation"]["cash"], 8.0)
        self.assertEqual(enriched["asset_allocation"]["debt"], 2.0)
        self.assertEqual(enriched["asset_allocation"]["equity"], 90.0)

    def test_empty_holdings_degrade_quietly(self):
        enriched = enrich_holdings(None)
        self.assertEqual(enriched["holdings"], [])
        self.assertEqual(enriched["holdings_count"], 0)
        self.assertIsNone(enriched["portfolio_date"])


if __name__ == "__main__":
    unittest.main()
