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


class SipFrequencyTests(unittest.TestCase):
    def test_weekly_lands_on_the_same_weekday(self):
        out = portfolio.expand_sip(
            start_date="2024-01-03", end_date="2024-03-06", amount=2000, frequency="weekly"
        )
        weekdays = {date.fromisoformat(item["date"]).weekday() for item in out}
        self.assertEqual(len(weekdays), 1)
        self.assertEqual(len(out), 10)

    def test_weekly_can_be_pinned_to_a_chosen_weekday(self):
        out = portfolio.expand_sip(
            start_date="2024-01-01", end_date="2024-02-01", amount=500,
            frequency="weekly", weekday=4,  # Friday
        )
        self.assertTrue(all(date.fromisoformat(i["date"]).weekday() == 4 for i in out))
        self.assertEqual(out[0]["date"], "2024-01-05")

    def test_fortnightly_steps_fourteen_days(self):
        out = portfolio.expand_sip(
            start_date="2024-01-03", end_date="2024-03-01", amount=1000, frequency="fortnightly"
        )
        gaps = {
            (date.fromisoformat(b["date"]) - date.fromisoformat(a["date"])).days
            for a, b in zip(out, out[1:])
        }
        self.assertEqual(gaps, {14})

    def test_quarterly_steps_three_months(self):
        out = portfolio.expand_sip(
            start_date="2024-01-15", end_date="2025-01-15", amount=5000, frequency="quarterly"
        )
        self.assertEqual([i["date"] for i in out],
                         ["2024-01-15", "2024-04-15", "2024-07-15", "2024-10-15", "2025-01-15"])

    def test_unknown_frequency_falls_back_to_monthly(self):
        out = portfolio.expand_sip(
            start_date="2024-01-10", end_date="2024-06-10", amount=100, frequency="hourly"
        )
        self.assertEqual(len(out), 6)

    def test_a_multi_year_weekly_sip_stays_inside_the_cap(self):
        out = portfolio.expand_sip(
            start_date="2010-01-01", end_date="2026-01-01", amount=100, frequency="weekly"
        )
        self.assertLessEqual(len(out), portfolio.MAX_TRANSACTIONS_PER_POSITION)


class OpeningPositionTests(unittest.TestCase):
    def test_units_are_recorded_without_an_amount(self):
        out = portfolio.opening_position(units=1234.567, as_of="2024-06-03")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["units"], 1234.567)
        # Amount is left for the NAV lookup to fill; hardcoding one here would
        # invent a cost basis the user never gave.
        self.assertIsNone(out[0]["amount"])

    def test_rejects_nonsense(self):
        self.assertEqual(portfolio.opening_position(units=0, as_of="2024-06-03"), [])
        self.assertEqual(portfolio.opening_position(units=-5, as_of="2024-06-03"), [])
        self.assertEqual(portfolio.opening_position(units=10, as_of="not-a-date"), [])

    def test_an_opening_holding_plus_a_sip_values_together(self):
        dates, navs = nav_series(700, base=40.0, daily_growth=0.0004)
        opening = portfolio.opening_position(units=1000.0, as_of=dates[0])
        sip = portfolio.expand_sip(
            start_date=dates[50], end_date=dates[-1], amount=2000, frequency="weekly"
        )
        position = {"id": "p", "scheme_code": "1", "transactions": opening + sip}
        valued = portfolio.value_position(position, nav_series={"dates": dates, "navs": navs})
        self.assertGreater(valued["units"], 1000.0)
        self.assertGreater(valued["invested"], 1000.0 * navs[0])
        self.assertIsNotNone(valued["xirr"])
        self.assertEqual(valued["unpriced_transactions"], 0)


class ThemeBenchmarkTests(unittest.TestCase):
    def test_named_themes_route_to_their_own_sector(self):
        cases = {
            "Technology Fund": "niftyit",
            "Pharma Fund": "niftypharma",
            "Banking and Financial Services Fund": "niftybank",
            "Infrastructure Fund": "niftyinfra",
            "FMCG Fund": "niftyfmcg",
            "PSU Fund": "niftypse",
            "Realty Fund": "niftyrealty",
        }
        for name, expected in cases.items():
            self.assertEqual(benchmarks.resolve("Thematic", name=name).key, expected, name)

    def test_a_strategy_theme_keeps_the_broad_benchmark(self):
        """A momentum or quant fund is not a sector bet — guessing one would put
        a wrong benchmark on screen."""
        for name in ("Momentum Fund", "Quantamental Fund", "Business Cycle Fund", "ESG Fund"):
            self.assertEqual(benchmarks.resolve("Thematic", name=name).key, "nifty500", name)

    def test_holdings_break_the_tie_when_the_name_says_nothing(self):
        bench = benchmarks.resolve("Thematic", name="Special Opportunities Fund",
                                   dominant_sector="Technology")
        self.assertEqual(bench.key, "niftyit")

    def test_non_themed_categories_ignore_the_name(self):
        """A small-cap fund with 'Technology' in its name is still a small-cap fund."""
        self.assertEqual(
            benchmarks.resolve("Small Cap", name="Technology Small Cap Fund").key,
            "niftysmallcap250",
        )

    def test_sector_benchmarks_are_flagged_as_price_indices(self):
        bench = benchmarks.resolve("Sectoral", name="Pharma Fund")
        self.assertFalse(bench.total_return)
        self.assertFalse(bench.is_reference_only)


class StripAmcTests(unittest.TestCase):
    def test_amc_name_is_removed_before_theme_matching(self):
        """The bug this exists for: 'Bank of India Manufacturing & Infrastructure
        Fund' is an infrastructure fund, not a banking one."""
        from app.services.mutual_funds.harvest import _strip_amc
        stripped = _strip_amc(
            "Bank of India Manufacturing & Infrastructure Fund", "Bank of India Mutual Fund"
        )
        self.assertEqual(stripped, "Manufacturing & Infrastructure Fund")
        self.assertEqual(benchmarks.resolve("Thematic", name=stripped).key, "niftyinfra")

    def test_a_genuine_banking_fund_still_resolves_to_bank(self):
        from app.services.mutual_funds.harvest import _strip_amc
        stripped = _strip_amc("SBI Banking & Financial Services Fund", "SBI Mutual Fund")
        self.assertEqual(benchmarks.resolve("Sectoral", name=stripped).key, "niftybank")

    def test_never_strips_the_whole_name(self):
        from app.services.mutual_funds.harvest import _strip_amc
        self.assertEqual(_strip_amc("Quant", "Quant Mutual Fund"), "Quant")

    def test_missing_inputs_are_survivable(self):
        from app.services.mutual_funds.harvest import _strip_amc
        self.assertIsNone(_strip_amc(None, "SBI Mutual Fund"))
        self.assertEqual(_strip_amc("Some Fund", None), "Some Fund")


class FundReviewTests(unittest.TestCase):
    def peers(self):
        return [
            {"scheme_code": "a", "sub_category": "Small Cap", "return_1y": 20.0, "return_3y": 25.0,
             "expense_ratio": 0.5, "max_drawdown": -20.0, "sharpe": 1.2, "percentile_1y": 95.0,
             "percentile_3y": 95.0, "percentile_5y": 90.0},
            {"scheme_code": "b", "sub_category": "Small Cap", "return_1y": 10.0, "return_3y": 15.0,
             "expense_ratio": 1.0, "max_drawdown": -35.0, "sharpe": 0.6, "percentile_1y": 50.0,
             "percentile_3y": 50.0, "percentile_5y": 50.0},
            {"scheme_code": "c", "sub_category": "Small Cap", "return_1y": 2.0, "return_3y": 8.0,
             "expense_ratio": 1.8, "max_drawdown": -48.0, "sharpe": 0.2, "percentile_1y": 5.0,
             "percentile_3y": 5.0, "percentile_5y": 60.0},
        ]

    def test_leader_and_laggard_get_different_standings(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        best = fund_review.build_review(rows[0], rows)
        worst = fund_review.build_review(rows[2], rows)
        self.assertGreater(best["measured_standing"], worst["measured_standing"])
        self.assertGreater(best["strength_count"], 0)
        self.assertGreater(worst["concern_count"], 0)

    def test_slipping_trajectory_is_detected(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        # c went from the 60th percentile over 5y to the 5th over 1y.
        trajectory = fund_review.build_review(rows[2], rows)["rank_trajectory"]
        self.assertEqual(trajectory["direction"], "slipping")
        self.assertLess(trajectory["change"], 0)

    def test_small_percentile_drift_is_not_called_a_decline(self):
        from app.services.mutual_funds import fund_review
        steady = {"scheme_code": "s", "sub_category": "X", "percentile_5y": 55.0,
                  "percentile_3y": 52.0, "percentile_1y": 48.0}
        self.assertEqual(fund_review.rank_trajectory(steady)["direction"], "steady")

    def test_peers_ahead_requires_better_on_all_three_axes(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        ahead = fund_review.peers_ahead(rows[2], rows)
        # Both a and b beat c on return, cost and drawdown together.
        self.assertEqual({p["scheme_code"] for p in ahead}, {"a", "b"})
        # The leader has nobody ahead of it.
        self.assertEqual(fund_review.peers_ahead(rows[0], rows), [])

    def test_a_higher_return_alone_does_not_qualify_a_peer(self):
        from app.services.mutual_funds import fund_review
        mine = {"scheme_code": "m", "sub_category": "X", "return_3y": 10.0,
                "expense_ratio": 0.4, "max_drawdown": -15.0}
        # Better return, but pricier and a deeper fall — must not be listed.
        pricey = {"scheme_code": "p", "sub_category": "X", "return_3y": 30.0,
                  "expense_ratio": 2.0, "max_drawdown": -50.0}
        self.assertEqual(fund_review.peers_ahead(mine, [mine, pricey]), [])

    def test_signals_never_instruct_the_reader(self):
        """The review reports measured facts. Recommending an action is out of
        scope — it would be personalised investment advice."""
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        banned = ("switch", "buy ", "sell", "exit", "you should", "we recommend",
                  "invest in", "redeem", "avoid")
        for row in rows:
            for signal in fund_review.build_review(row, rows)["signals"]:
                lowered = signal["text"].lower()
                for word in banned:
                    self.assertNotIn(word, lowered, f"{word!r} in {signal['text']!r}")

    def test_percentile_puts_best_at_the_top_for_lower_is_better_columns(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        card = {item["key"]: item for item in fund_review.build_scorecard(rows[0], rows)}
        # Cheapest fund should score highest on expense ratio.
        self.assertEqual(card["expense_ratio"]["standing"], "strong")

    def test_a_fund_with_no_peers_still_returns_a_review(self):
        from app.services.mutual_funds import fund_review
        solo = {"scheme_code": "z", "sub_category": "International", "return_3y": 9.0}
        review = fund_review.build_review(solo, [solo])
        self.assertEqual(review["peer_count"], 1)
        self.assertIsNotNone(review["signals"])


class PeerComparisonTests(unittest.TestCase):
    """The portfolio-level 'which funds did better than mine' comparison."""

    def peers(self):
        return [
            {"scheme_code": "lead", "sub_category": "Small Cap", "name": "Leader Fund",
             "return_3y": 26.0, "expense_ratio": 0.6, "max_drawdown": -24.0, "sharpe": 1.1},
            {"scheme_code": "mid", "sub_category": "Small Cap", "name": "Middle Fund",
             "return_3y": 18.0, "expense_ratio": 0.9, "max_drawdown": -33.0, "sharpe": 0.7},
            {"scheme_code": "lag", "sub_category": "Small Cap", "name": "Laggard Fund",
             "return_3y": 12.0, "expense_ratio": 1.6, "max_drawdown": -48.0, "sharpe": 0.3},
        ]

    def test_limit_is_honoured(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        self.assertEqual(len(fund_review.peers_ahead(rows[2], rows, limit=1)), 1)
        self.assertEqual(len(fund_review.peers_ahead(rows[2], rows, limit=10)), 2)

    def test_results_are_ordered_by_how_far_ahead(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        ahead = fund_review.peers_ahead(rows[2], rows, limit=10)
        gaps = [item["return_gap"] for item in ahead]
        self.assertEqual(gaps, sorted(gaps, reverse=True))
        self.assertEqual(ahead[0]["scheme_code"], "lead")

    def test_the_fund_never_appears_in_its_own_comparison(self):
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        for row in rows:
            codes = {item["scheme_code"] for item in fund_review.peers_ahead(row, rows, limit=10)}
            self.assertNotIn(row["scheme_code"], codes)

    def test_a_fund_with_no_three_year_record_yields_no_comparison(self):
        """Better to show nothing than to compare against a number we lack."""
        from app.services.mutual_funds import fund_review
        rows = self.peers()
        young = {"scheme_code": "new", "sub_category": "Small Cap", "return_3y": None}
        self.assertEqual(fund_review.peers_ahead(young, rows + [young], limit=10), [])

    def test_deeper_drawdown_disqualifies_a_higher_returning_peer(self):
        from app.services.mutual_funds import fund_review
        mine = {"scheme_code": "m", "sub_category": "X", "return_3y": 15.0,
                "expense_ratio": 0.8, "max_drawdown": -25.0}
        risky = {"scheme_code": "r", "sub_category": "X", "return_3y": 40.0,
                 "expense_ratio": 0.7, "max_drawdown": -60.0}
        self.assertEqual(fund_review.peers_ahead(mine, [mine, risky], limit=10), [])
