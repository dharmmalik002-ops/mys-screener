"""Tests for the bot's learning layer.

The failure modes here are quieter than the engine's and more dangerous,
because a bad lesson looks exactly like a good one. Each test below pins one
of them:

  - a lesson drawn from a fact that only exists after the trade closed
  - a cell scored on trades that had not happened yet at the `as_of` date
  - a re-seed that duplicates the statistical base
  - a "discovery" that is one measurement counted twice
  - an effect certified from in-sample evidence alone
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from app.services.bot import conditions as cond
from app.services.bot import evolution as evo
from app.services.bot import ledger as lg
from app.services.bot import quality as ql
from app.services.bot import review as rv


def trade_row(
    trade_id: int = 1,
    r: float = 1.0,
    mae: float = -0.2,
    mfe: float = 1.5,
    exit_reason: str = "trail",
    **extra,
) -> dict:
    row = {
        "id": trade_id,
        "r_multiple": r,
        "mae_r": mae,
        "mfe_r": mfe,
        "exit_reason": exit_reason,
        "atr_pct_at_entry": 3.0,
        "volatility_band": "normal",
        "breadth_above_200dma": 55.0,
        "macro_headwinds": 1,
        "risk_pct": 6.0,
        "entry_day": "2020-01-01",
        "regime": "bull_strong",
        "strategy": "s",
    }
    row.update(extra)
    return row


class ReviewVerdictTests(unittest.TestCase):
    def test_round_trip_loss_is_separated_from_a_clean_loss(self) -> None:
        """Losing after being 2R up is a different mistake from being wrong fast."""
        round_trip = rv.review_trade(trade_row(r=-1.0, mae=-1.0, mfe=2.4, exit_reason="stop"))
        clean = rv.review_trade(trade_row(r=-1.0, mae=-1.0, mfe=0.1, exit_reason="stop"))
        self.assertEqual(round_trip.verdict, "round_trip_loss")
        self.assertEqual(clean.verdict, "clean_loss")
        self.assertIn("profit_handed_back", round_trip.tags)

    def test_win_after_near_stop_is_not_called_clean(self) -> None:
        """Outcome is not decision quality — this is the whole point of the module."""
        lucky = rv.review_trade(trade_row(r=2.0, mae=-0.9, mfe=2.4))
        clean = rv.review_trade(trade_row(r=2.0, mae=-0.1, mfe=2.4))
        self.assertEqual(lucky.verdict, "lucky_win")
        self.assertEqual(clean.verdict, "clean_win")

    def test_gap_loss_is_its_own_category(self) -> None:
        gapped = rv.review_trade(trade_row(r=-2.5, mae=-2.5, mfe=0.0, exit_reason="gap_stop"))
        self.assertEqual(gapped.verdict, "gap_loss")
        self.assertIn("stop_gapped", gapped.tags)


class LessonHonestyTests(unittest.TestCase):
    """The circularity guard. This is the test that matters most here."""

    def _population(self) -> list[dict]:
        rows: list[dict] = []
        # 60 winners and 60 losers, so both tag families are well populated.
        for i in range(60):
            rows.append(trade_row(trade_id=i, r=2.0, mae=-0.9, mfe=2.5))
        for i in range(60, 120):
            rows.append(trade_row(trade_id=i, r=-1.0, mae=-1.0, mfe=0.05, exit_reason="stop"))
        return [
            {**row, **rv.review_trade(row).to_dict()}
            for row in rows
        ]

    def test_outcome_tags_never_become_lessons(self) -> None:
        summary = rv.summarise_reviews(self._population())
        lesson_tags = {l["tag"] for l in summary["lessons"]}
        for tag in lesson_tags:
            self.assertIn(
                tag, rv.ENTRY_TAGS,
                f"'{tag}' is derived from the outcome and cannot inform an entry decision",
            )
        # `survived_near_stop` is the specific trap: it excludes stopped-out
        # trades by construction, so it always looks spectacular.
        self.assertNotIn("survived_near_stop", lesson_tags)
        self.assertNotIn("stop_hit", lesson_tags)

    def test_outcome_tags_are_still_reported_separately(self) -> None:
        """Quarantined, not hidden — the verdict breakdown is built from them."""
        summary = rv.summarise_reviews(self._population())
        outcome = {t["tag"] for t in summary["outcome_tags"]}
        self.assertTrue(outcome, "outcome tags should still be surfaced")
        self.assertTrue(all(t not in rv.ENTRY_TAGS for t in outcome))


class EvolutionCausalityTests(unittest.TestCase):
    def _cell(self, n: int, r: float, start: date) -> list[dict]:
        return [
            {
                "strategy": "s", "regime": "bull_strong", "r_multiple": r,
                "exit_day": (start + timedelta(days=i * 7)).isoformat(),
                "entry_day": (start + timedelta(days=i * 7)).isoformat(),
            }
            for i in range(n)
        ]

    def test_scoring_ignores_trades_that_had_not_closed_yet(self) -> None:
        """An `as_of` score must not see the future. This is the core contract."""
        early = self._cell(40, 1.0, date(2020, 1, 1))
        later = self._cell(40, -1.0, date(2024, 1, 1))
        as_of = date(2021, 1, 1)
        verdict = evo.score_cell("s", "bull_strong", early + later, as_of)
        self.assertIsNotNone(verdict)
        # Only the winners had closed by then, so the score must be positive
        # despite the losses sitting in the input list.
        self.assertGreater(verdict.avg_r, 0.5)
        self.assertEqual(verdict.trades, len(early))

    def test_a_cell_that_stops_paying_is_retired_not_averaged(self) -> None:
        old_wins = self._cell(60, 1.2, date(2018, 1, 1))
        recent_losses = self._cell(40, -0.9, date(2025, 1, 1))
        verdict = evo.score_cell("s", "bull_strong", old_wins + recent_losses, date(2026, 1, 1))
        self.assertIsNotNone(verdict)
        self.assertEqual(verdict.status, "retired")
        # Lifetime is still positive; that is exactly why averaging would hide it.
        self.assertGreater(verdict.avg_r, 0)
        self.assertLess(verdict.recent_avg_r, 0)

    def test_positive_but_stale_is_a_candidate_not_a_confirmation(self) -> None:
        stale = self._cell(60, 1.0, date(2015, 1, 1))
        verdict = evo.score_cell("s", "bull_strong", stale, date(2026, 1, 1))
        self.assertIsNotNone(verdict)
        self.assertEqual(verdict.status, "candidate")
        self.assertIsNone(verdict.recent_avg_r)

    def test_thin_cells_get_no_status_at_all(self) -> None:
        self.assertIsNone(
            evo.score_cell("s", "bull_strong", self._cell(5, 1.0, date(2020, 1, 1)), date(2021, 1, 1))
        )

    def test_replay_records_a_change_of_mind(self) -> None:
        trades = self._cell(60, 1.2, date(2018, 1, 1)) + self._cell(60, -1.0, date(2023, 1, 1))
        snapshots = evo.replay_evolution(trades, cadence_days=91, warmup_trades=40)
        self.assertTrue(snapshots)
        changes = evo.summarise_changes(snapshots)
        self.assertTrue(changes, "a cell that reverses must show up as a status change")


class ConditionStudyTests(unittest.TestCase):
    def _rows(self, atr_to_r: dict[float, float], n: int = 300) -> list[dict]:
        rows: list[dict] = []
        trade_id = 0
        for atr, r in atr_to_r.items():
            for i in range(n):
                rows.append(
                    trade_row(
                        trade_id=trade_id, r=r, atr_pct_at_entry=atr,
                        entry_day=(date(2015, 1, 1) + timedelta(days=i * 3)).isoformat(),
                    )
                )
                trade_id += 1
        return rows

    def test_a_non_monotone_effect_is_rejected(self) -> None:
        """A U-shape is what a cherry-picked cutoff is made of."""
        rows = self._rows({1.0: 1.0, 2.5: -0.5, 3.5: 1.0, 5.0: -0.5, 7.0: 1.0})
        study = cond.study_condition(
            rows, condition="atr_pct_at_entry", label="x", question="q",
            bucket_of=lambda r: cond._band(r["atr_pct_at_entry"], cond.ATR_EDGES, cond.ATR_NAMES),
            order=cond.ATR_NAMES, split=date(2018, 1, 1),
        )
        self.assertIsNotNone(study)
        self.assertFalse(study.monotone)
        self.assertIn("cherry-pick", study.verdict)

    def test_mechanically_linked_conditions_are_flagged(self) -> None:
        """Stop width is ATR rescaled — agreement is arithmetic, not evidence."""
        rows = self._rows({1.0: 1.0, 2.5: 0.6, 3.5: 0.3, 5.0: 0.1, 7.0: -0.2})
        for row in rows:
            row["risk_pct"] = row["atr_pct_at_entry"] * 2.0
        studies = cond.study_all(rows, date(2018, 1, 1))
        by_id = {s.condition: s for s in studies}
        self.assertIn("risk_pct", by_id)
        self.assertEqual(by_id["risk_pct"].duplicates, "atr_pct_at_entry")
        self.assertIn("same effect", by_id["risk_pct"].verdict)
        self.assertIsNone(by_id["atr_pct_at_entry"].duplicates)


class QualityModelTests(unittest.TestCase):
    def _artifact(self, verdict: str, monotone: bool = True) -> dict:
        return {
            "learning": {
                "review_summary": {"book_avg_r": 0.24},
                "condition_studies": [
                    {
                        "condition": "atr_pct_at_entry",
                        "monotone": monotone,
                        "verdict": verdict,
                        "buckets": [
                            {"label": "under 2%", "avg_r": 0.48},
                            {"label": "over 6%", "avg_r": 0.09},
                        ],
                    }
                ],
            }
        }

    def test_uncertified_effects_do_not_move_the_ranking(self) -> None:
        """In-sample-only evidence must not reorder candidates."""
        model = ql.load_quality_model(self._artifact("Monotone in-sample but reverses out-of-sample."))
        self.assertFalse(model.active)
        self.assertEqual(model.adjustment(1.0), (0.0, ""))

    def test_certified_effect_adjusts_by_the_measured_margin(self) -> None:
        model = ql.load_quality_model(
            self._artifact("Monotone in-sample and the direction holds out-of-sample.")
        )
        self.assertTrue(model.active)
        adjustment, bucket = model.adjustment(1.2)
        self.assertEqual(bucket, "under 2%")
        self.assertAlmostEqual(adjustment, 0.48 - 0.24, places=3)

    def test_missing_study_degrades_instead_of_raising(self) -> None:
        model = ql.load_quality_model({})
        self.assertFalse(model.active)
        parts = ql.score(model, 0.31, 3.0)
        self.assertEqual(parts["edge_score_r"], 0.31)


class LedgerTests(unittest.TestCase):
    def _trade(self, symbol: str = "AAA", entry_day: str = "2020-01-02") -> lg.LedgerTrade:
        return lg.LedgerTrade(
            source="backtest", strategy="s", symbol=symbol,
            signal_day="2020-01-01", entry_day=entry_day, exit_day="2020-02-01",
            entry=100.0, stop=94.0, exit_price=112.0, exit_reason="trail",
            sessions_held=20, r_multiple=2.0, net_pct=12.0, mae_r=-0.2, mfe_r=2.4,
            risk_pct=6.0, atr_pct_at_entry=3.0, regime="bull_strong",
            volatility_band="calm", breadth_above_200dma=60.0, pct_from_52w_high=-2.0,
            vix_percentile=25.0, macro_headwinds=0, expected_r=0.35, thesis="fixture",
        )

    def test_reseeding_does_not_duplicate_the_statistical_base(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            with lg.connect(state) as conn:
                first = lg.record_trades(conn, [self._trade()])
            with lg.connect(state) as conn:
                second = lg.record_trades(conn, [self._trade()])
                total = lg.counts(conn)["total"]
            self.assertEqual(first, 1)
            self.assertEqual(second, 0)
            self.assertEqual(total, 1)

    def test_reviews_attach_and_survive_a_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            with lg.connect(state) as conn:
                lg.record_trades(conn, [self._trade()])
                pending = lg.unreviewed(conn)
                self.assertEqual(len(pending), 1)
                # `unreviewed` must hand back something the review engine can
                # read with .get() — sqlite3.Row cannot, and silently broke this.
                verdict = rv.review_trade(pending[0])
                lg.record_review(
                    conn, verdict.trade_id, verdict.verdict, verdict.tags,
                    verdict.stop_quality, verdict.exit_quality,
                    verdict.r_left_on_table, verdict.note,
                )
            with lg.connect(state) as conn:
                self.assertEqual(lg.unreviewed(conn), [])
                rows = lg.query_trades(conn, limit=10)
                self.assertEqual(rows[0]["verdict"], "clean_win")
                self.assertIsInstance(rows[0]["tags"], list)

    def test_status_transitions_report_only_real_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            base = {
                "strategy": "s", "regime": "bull_strong", "trades": 50,
                "win_rate": 30.0, "avg_r": 0.3, "payoff": 3.0,
                "recent_avg_r": 0.2, "recent_trades": 25, "note": "",
            }
            with lg.connect(state) as conn:
                lg.record_cell_status(conn, "2024-01-01", [{**base, "status": "confirmed"}])
                lg.record_cell_status(conn, "2024-04-01", [{**base, "status": "confirmed"}])
                lg.record_cell_status(conn, "2024-07-01", [{**base, "status": "retired"}])
                transitions = lg.status_transitions(conn)
            self.assertEqual(len(transitions), 1)
            self.assertEqual(transitions[0]["from_status"], "confirmed")
            self.assertEqual(transitions[0]["to_status"], "retired")


if __name__ == "__main__":
    unittest.main()
