"""Chart look-alikes: the properties the pipeline's honesty rests on.

No torch here — the fingerprint model is exercised by the build script. These
pin what is easy to break silently: reading a reference's date, drawing a
picture that cannot see the future, grading outcomes conservatively, grouping
the test by ticker, and never publishing reference names by default.
"""

from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
import tempfile

import numpy as np

from app.services.lookalike import model, outcome, references, refine, render, rules


def _bars(n: int, seed: int = 0):
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.02, n)))
    o = c * (1 + rng.normal(0, 0.005, n))
    h = np.maximum(o, c) * 1.01
    l = np.minimum(o, c) * 0.99
    v = rng.uniform(1e5, 1e6, n)
    return o, h, l, c, v


class ReferenceParsingTests(unittest.TestCase):
    def test_newsletter_filename_is_month_day_year(self):
        self.assertEqual(references.parse_filename("NVDA-08-12-26.gif"), ("NVDA", date(2026, 8, 12)))

    def test_share_class_tickers_keep_their_suffix(self):
        self.assertEqual(references.parse_filename("BF-B-08-12-26.gif"), ("BF-B", date(2026, 8, 12)))
        self.assertEqual(references.parse_filename("BRK.B-1-5-2024.png"), ("BRK.B", date(2024, 1, 5)))

    def test_nineties_two_digit_years(self):
        self.assertEqual(references.parse_filename("QCOM-12-29-99.gif"), ("QCOM", date(1999, 12, 29)))

    def test_impossible_dates_and_non_charts_do_not_parse(self):
        self.assertIsNone(references.parse_filename("X-13-40-26.gif"))
        self.assertIsNone(references.parse_filename("logo.png"))

    def test_html_page_yields_each_chart_once(self):
        html = '<img src="//x/charts/AVGO-08-12-26.gif"><img src="charts/AVGO-08-12-26.gif"><img src="images/logo.png">'
        refs = references.from_html_text(html, "page")
        self.assertEqual([(r.ticker, r.day) for r in refs], [("AVGO", date(2026, 8, 12))])

    def test_folder_reports_images_it_could_not_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "NVDA-08-12-26.gif").write_bytes(b"")
            (root / "scan0001.gif").write_bytes(b"")
            (root / "list.txt").write_text("PLTR,2025-03-04\n")
            refs, unparsed = references.collect([root])
        self.assertEqual({r.key for r in refs}, {"NVDA@2026-08-12", "PLTR@2025-03-04"})
        self.assertEqual(unparsed, ["scan0001.gif"])


class RenderTests(unittest.TestCase):
    def test_picture_cannot_see_bars_after_its_end(self):
        o, h, l, c, v = _bars(400)
        end = 250
        first, _ = render.picture(o, h, l, c, v, end)
        o2, h2, l2, c2, v2 = (np.concatenate([a[: end + 1], a[end + 1 :] * 3]) for a in (o, h, l, c, v))
        second, _ = render.picture(o2, h2, l2, c2, v2, end)
        self.assertTrue(np.array_equal(np.asarray(first), np.asarray(second)))

    def test_price_level_does_not_change_the_picture(self):
        """A stock at Rs 50 and one at Rs 5,000 with the same shape draw the same."""
        o, h, l, c, v = _bars(300, seed=3)
        a, _ = render.picture(o, h, l, c, v, 299)
        b, _ = render.picture(o * 100, h * 100, l * 100, c * 100, v * 7, 299)
        self.assertTrue(np.array_equal(np.asarray(a), np.asarray(b)))

    def test_short_history_gives_no_picture_rather_than_a_padded_one(self):
        o, h, l, c, v = _bars(render.min_bars_needed() - 1)
        self.assertIsNone(render.picture(o, h, l, c, v, len(c) - 1))

    def test_window_is_normalised_so_prices_never_ship(self):
        o, h, l, c, v = _bars(300, seed=5)
        _, window = render.picture(o * 1000, h * 1000, l * 1000, c * 1000, v, 299)
        for key in ("o", "h", "l", "c", "sma", "v"):
            self.assertEqual(len(window[key]), render.WINDOW)
            self.assertTrue(all(0.0 <= x <= 1.0 for x in window[key]), key)


class OutcomeTests(unittest.TestCase):
    def _flat(self, n=60):
        return np.full(n, 100.0), np.full(n, 100.5), np.full(n, 99.5)

    def test_entry_is_the_next_open_not_the_chart_day(self):
        o, h, l = self._flat()
        h[10] = 1000.0  # the chart day itself must not count
        self.assertNotEqual(outcome.grade(o, h, l, 10).label, outcome.WORKED)

    def test_target_first_is_worked(self):
        o, h, l = self._flat()
        h[15] = 121.0
        g = outcome.grade(o, h, l, 10)
        self.assertEqual((g.label, g.days_to_result), (outcome.WORKED, 5))

    def test_a_session_touching_both_levels_is_a_loss(self):
        o, h, l = self._flat()
        h[12], l[12] = 125.0, 90.0
        self.assertEqual(outcome.grade(o, h, l, 10).label, outcome.FAILED)

    def test_unfinished_window_is_pending_not_failed(self):
        o, h, l = self._flat(30)
        self.assertEqual(outcome.grade(o, h, l, 10).label, outcome.PENDING)

    def test_finished_window_without_a_move_is_failed(self):
        o, h, l = self._flat(10 + outcome.HORIZON + 5)
        self.assertEqual(outcome.grade(o, h, l, 10).label, outcome.FAILED)


class ModelTests(unittest.TestCase):
    def test_folds_never_split_a_ticker(self):
        groups = ["A", "A", "B", "C", "C", "C", "D", "E", "F"]
        folds = model.grouped_folds(groups)
        for g in set(groups):
            self.assertEqual(len({f for f, x in zip(folds, groups) if x == g}), 1)

    def test_auc_basics(self):
        self.assertEqual(model.auc(np.array([3.0, 4.0, 1.0, 2.0]), np.array([1, 1, 0, 0])), 1.0)
        self.assertEqual(model.auc(np.array([1.0, 1.0]), np.array([1, 0])), 0.5)
        self.assertIsNone(model.auc(np.array([1.0]), np.array([1])))

    def test_a_separable_library_is_detected_out_of_sample(self):
        rng = np.random.default_rng(1)
        direction = rng.normal(size=32)
        pos = rng.normal(size=(60, 32)) + 1.5 * direction / np.linalg.norm(direction) * 4
        neg = rng.normal(size=(120, 32))
        X = np.vstack([pos, neg]); y = np.r_[np.ones(60), np.zeros(120)]
        groups = [f"T{i % 20}" for i in range(180)]
        ev = model.evaluate(X, y, groups, [date(2026, 1, 1)] * 180, ["pending"] * 60)
        self.assertGreater(ev.setup_vs_random_auc, 0.9)

    def test_noise_is_not_called_signal(self):
        rng = np.random.default_rng(2)
        X = rng.normal(size=(180, 32)); y = np.r_[np.ones(60), np.zeros(120)]
        groups = [f"T{i % 20}" for i in range(180)]
        ev = model.evaluate(X, y, groups, [date(2026, 1, 1)] * 180, ["pending"] * 60)
        self.assertLess(abs(ev.setup_vs_random_auc - 0.5), 0.15)

    def test_small_libraries_carry_their_warnings(self):
        rng = np.random.default_rng(3)
        X = rng.normal(size=(30, 8)); y = np.r_[np.ones(10), np.zeros(20)]
        ev = model.evaluate(X, y, [f"T{i % 10}" for i in range(30)], [date(2026, 1, 1)] * 30, ["worked"] * 10)
        text = " ".join(ev.warnings)
        self.assertIn("reference charts", text)
        self.assertIn("one year", text)

    def test_library_spanning_years_is_tested_chronologically(self):
        rng = np.random.default_rng(4)
        X = rng.normal(size=(40, 8)); y = np.r_[np.ones(20), np.zeros(20)]
        days = [date(2018 + i % 6, 1, 1) for i in range(40)]
        ev = model.evaluate(X, y, [f"T{i}" for i in range(40)], days, ["pending"] * 20)
        self.assertEqual(ev.method, "chronological")


class StyleSourceTests(unittest.TestCase):
    def test_each_folder_is_its_own_style_and_only_names_are_saved(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            refs = [references.Reference("NVDA", date(2026, 8, 12), "x")]
            path = references.save_source(data, "Zanger", "newsletter 1", refs)
            references.save_source(data, "minervini", "ideas", [references.Reference("AAPL", date(2021, 11, 17), "y")])
            self.assertEqual(path.read_text(), "NVDA,2026-08-12\n")
            found, _ = references.collect_sources(data)
        self.assertEqual({(r.style, r.ticker) for r in found}, {("zanger", "NVDA"), ("minervini", "AAPL")})


class LearningCurveTests(unittest.TestCase):
    def test_no_curve_without_a_time_split(self):
        rng = np.random.default_rng(5)
        X = rng.normal(size=(300, 8)); y = np.r_[np.ones(100), np.zeros(200)]
        self.assertEqual(model.learning_curve(X, y, [date(2026, 1, 1)] * 300), [])

    def test_curve_rises_when_there_is_something_to_learn(self):
        rng = np.random.default_rng(6)
        d = 64
        signal = rng.normal(size=d); signal /= np.linalg.norm(signal)
        n_pos, n_neg = 400, 1200
        X = np.vstack([rng.normal(size=(n_pos, d)) + 1.2 * signal, rng.normal(size=(n_neg, d))])
        y = np.r_[np.ones(n_pos), np.zeros(n_neg)]
        days = [date(2010 + (i % 12), 1 + i % 12, 1) for i in range(n_pos + n_neg)]
        curve = model.learning_curve(X, y, days)
        self.assertGreaterEqual(len(curve), 3)
        self.assertEqual(curve[-1]["charts"], max(p["charts"] for p in curve))
        self.assertGreater(curve[-1]["auc"], curve[0]["auc"])
        # the best any classifier can do here is Phi(1.2 / sqrt 2) ~ 0.80
        self.assertGreater(curve[-1]["auc"], 0.75)


class RefineTests(unittest.TestCase):
    def _base_then_breakout(self, breakout_at=100, n=140):
        c = np.full(n, 50.0); h = c + 0.5; v = np.full(n, 1000.0)
        c[breakout_at:] = 55.0; h[breakout_at:] = 55.5; v[breakout_at] = 3000.0
        return h, c, v

    def test_a_late_reading_is_pulled_back_to_the_breakout(self):
        h, c, v = self._base_then_breakout()
        r = refine.refine(h, c, v, approx=110, precision="month")
        self.assertEqual((r.breakout, r.end, r.refined), (100, 99, True))

    def test_the_window_respects_the_reading_precision(self):
        h, c, v = self._base_then_breakout()
        r = refine.refine(h, c, v, approx=110, precision="day")  # +-3 sessions cannot reach 100
        self.assertEqual((r.end, r.refined), (110, False))

    def test_a_breakout_needs_volume(self):
        h, c, v = self._base_then_breakout()
        v[100] = 1000.0
        self.assertFalse(refine.refine(h, c, v, approx=100, precision="week").refined)


class RulesTests(unittest.TestCase):
    def test_a_clean_uptrend_passes_the_template(self):
        n = 400
        c = np.linspace(20, 100, n); h = c * 1.01; l = c * 0.99; o = c; v = np.full(n, 1e6)
        flags = rules.check(o, h, l, c, v, n - 1, index_return_6m=0.05)
        self.assertEqual(rules.template_score(flags), 8)

    def test_a_downtrend_fails_it(self):
        n = 400
        c = np.linspace(100, 20, n); h = c * 1.01; l = c * 0.99
        flags = rules.check(c, h, l, c, np.full(n, 1e6), n - 1, index_return_6m=0.0)
        self.assertLessEqual(rules.template_score(flags), 1)

    def test_unknown_benchmark_fails_relative_strength_rather_than_guessing(self):
        n = 400
        c = np.linspace(20, 100, n)
        flags = rules.check(c, c * 1.01, c * 0.99, c, np.full(n, 1e6), n - 1, index_return_6m=None)
        self.assertFalse(flags["beats_index"])

    def test_rules_cannot_see_the_future(self):
        n = 400
        c = np.linspace(20, 100, n); h = c * 1.01; l = c * 0.99; v = np.full(n, 1e6)
        a = rules.check(c, h, l, c, v, 350, 0.0)
        c2 = c.copy(); c2[351:] = 1.0
        b = rules.check(c2, h, l, c2, v, 350, 0.0)
        self.assertEqual(a, b)

    def test_needs_a_year_of_history(self):
        c = np.linspace(20, 100, 200)
        self.assertIsNone(rules.check(c, c, c, c, c, 199, 0.0))


class PublicFileTests(unittest.TestCase):
    def test_reference_names_are_hidden_by_default(self):
        import inspect

        from app.services.lookalike import pipeline

        default = inspect.signature(pipeline.scan_india).parameters["show_reference_names"].default
        self.assertIs(default, False)


if __name__ == "__main__":
    unittest.main()
