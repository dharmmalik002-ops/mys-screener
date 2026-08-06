from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import StockSnapshot
from app.providers.free import FreeMarketDataProvider
from app.services import industry_groups
from app.services.industry_groups import build_industry_groups_response, write_industry_group_files


class IndustryGroupsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = FreeMarketDataProvider()
        self.snapshot_updated_at = datetime(2026, 4, 2, 10, 30, tzinfo=timezone.utc)
        # Isolate rank history: temp dir instead of the real data dir, no store.
        self._history_dir_ctx = TemporaryDirectory()
        self._orig_history_dir = industry_groups.RANK_HISTORY_DIR
        self._orig_rank_store = industry_groups._rank_store
        industry_groups.RANK_HISTORY_DIR = Path(self._history_dir_ctx.name)
        industry_groups._rank_store = False

    def tearDown(self) -> None:
        industry_groups.RANK_HISTORY_DIR = self._orig_history_dir
        industry_groups._rank_store = self._orig_rank_store
        self._history_dir_ctx.cleanup()

    def _seed_history(self, days_ago: int, payload: list[dict]) -> None:
        day = (self.snapshot_updated_at - timedelta(days=days_ago)).astimezone(timezone.utc)
        out = Path(self._history_dir_ctx.name) / f"ranks_{day.strftime('%Y%m%d')}.json"
        out.write_text(json.dumps(payload), encoding="utf-8")

    def _build_snapshot(
        self,
        *,
        symbol: str,
        sector: str,
        sub_sector: str,
        market_cap_crore: float,
        start_close: float,
        step: float,
    ) -> StockSnapshot:
        index = pd.bdate_range(end=self.snapshot_updated_at, periods=520)
        history = pd.DataFrame(
            [
                {
                    "Open": start_close + (idx * step) - 1,
                    "High": start_close + (idx * step) + 2,
                    "Low": start_close + (idx * step) - 2,
                    "Close": start_close + (idx * step),
                    "Adj Close": start_close + (idx * step),
                    "Volume": 500_000 + (idx * 2500),
                    "Stock Splits": 0.0,
                }
                for idx in range(len(index))
            ],
            index=index,
        )
        benchmark = pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)
        row = self.provider._history_to_snapshot(
            {
                "symbol": symbol,
                "name": f"{symbol} Limited",
                "exchange": "NSE",
                "listing_date": "2020-01-02",
                "sector": sector,
                "sub_sector": sub_sector,
                "market_cap_crore": market_cap_crore,
                "ticker": f"{symbol}.NS",
            },
            history,
            benchmark,
        )
        assert row is not None
        row["market_cap_crore"] = market_cap_crore
        row["sector"] = sector
        row["sub_sector"] = sub_sector
        return StockSnapshot.model_validate(row)

    def test_build_industry_groups_response_maps_india_groups_and_writes_json_files(self) -> None:
        snapshots = [
            self._build_snapshot(
                symbol="PHARMA1",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=12_000.0,
                start_close=100.0,
                step=0.9,
            ),
            self._build_snapshot(
                symbol="PHARMA2",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=8_000.0,
                start_close=80.0,
                step=0.8,
            ),
            self._build_snapshot(
                symbol="PHARMA3",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=7_500.0,
                start_close=75.0,
                step=0.75,
            ),
            self._build_snapshot(
                symbol="PHARMA4",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=7_000.0,
                start_close=70.0,
                step=0.7,
            ),
            self._build_snapshot(
                symbol="PHARMA5",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=6_800.0,
                start_close=68.0,
                step=0.68,
            ),
            self._build_snapshot(
                symbol="AUTO1",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=9_500.0,
                start_close=60.0,
                step=0.7,
            ),
            self._build_snapshot(
                symbol="AUTO2",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=6_500.0,
                start_close=55.0,
                step=0.65,
            ),
            self._build_snapshot(
                symbol="AUTO3",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=6_000.0,
                start_close=50.0,
                step=0.6,
            ),
            self._build_snapshot(
                symbol="AUTO4",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=5_500.0,
                start_close=45.0,
                step=0.55,
            ),
            self._build_snapshot(
                symbol="AUTO5",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=5_300.0,
                start_close=43.0,
                step=0.53,
            ),
        ]
        previous_snapshots = [
            self._build_snapshot(
                symbol="PHARMA1",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=12_000.0,
                start_close=100.0,
                step=0.7,
            ),
            self._build_snapshot(
                symbol="PHARMA2",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=8_000.0,
                start_close=80.0,
                step=0.6,
            ),
            self._build_snapshot(
                symbol="PHARMA3",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=7_500.0,
                start_close=75.0,
                step=0.55,
            ),
            self._build_snapshot(
                symbol="PHARMA4",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=7_000.0,
                start_close=70.0,
                step=0.5,
            ),
            self._build_snapshot(
                symbol="PHARMA5",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=6_800.0,
                start_close=68.0,
                step=0.48,
            ),
            self._build_snapshot(
                symbol="AUTO1",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=9_500.0,
                start_close=60.0,
                step=0.4,
            ),
            self._build_snapshot(
                symbol="AUTO2",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=6_500.0,
                start_close=55.0,
                step=0.35,
            ),
            self._build_snapshot(
                symbol="AUTO3",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=6_000.0,
                start_close=50.0,
                step=0.3,
            ),
            self._build_snapshot(
                symbol="AUTO4",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=5_500.0,
                start_close=45.0,
                step=0.25,
            ),
            self._build_snapshot(
                symbol="AUTO5",
                sector="Automobile and Auto Components",
                sub_sector="Auto Components & Equipments",
                market_cap_crore=5_300.0,
                start_close=43.0,
                step=0.23,
            ),
        ]

        response = build_industry_groups_response(
            snapshots,
            snapshots,
            previous_snapshots,
            previous_snapshots,
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )

        self.assertEqual(response.total_groups, 2)
        self.assertEqual({group.group_name for group in response.groups}, {"Pharma Formulations", "Auto Ancillaries - Powertrain"})
        self.assertTrue(all(group.stock_count >= 5 for group in response.groups))
        self.assertEqual({item.final_group_name for item in response.stocks}, {"Pharma Formulations", "Auto Ancillaries - Powertrain"})
        self.assertTrue(all(group.rank >= 1 for group in response.groups))
        self.assertTrue(any(group.score_change_1w is not None for group in response.groups))

        with TemporaryDirectory() as temp_dir:
          temp_path = Path(temp_dir)
          groups_path = temp_path / "groups.json"
          ranks_path = temp_path / "group-ranks.json"
          stocks_path = temp_path / "stocks-to-groups.json"
          write_industry_group_files(
              response,
              groups_path=groups_path,
              ranks_path=ranks_path,
              stocks_path=stocks_path,
          )

          self.assertTrue(groups_path.exists())
          self.assertTrue(ranks_path.exists())
          self.assertTrue(stocks_path.exists())
          self.assertIn("Pharma Formulations", groups_path.read_text(encoding="utf-8"))
          self.assertIn("Auto Ancillaries - Powertrain", stocks_path.read_text(encoding="utf-8"))

    def test_small_industry_groups_merge_into_similar_parent_sector_group(self) -> None:
        snapshots = [
            self._build_snapshot(
                symbol=f"PHARMA{index}",
                sector="Healthcare",
                sub_sector="Pharmaceuticals",
                market_cap_crore=10_000.0 - index,
                start_close=100.0 + index,
                step=0.6,
            )
            for index in range(1, 4)
        ]
        snapshots.append(
            self._build_snapshot(
                symbol="HOSP1",
                sector="Healthcare",
                sub_sector="Hospital",
                market_cap_crore=9_000.0,
                start_close=90.0,
                step=0.55,
            )
        )

        response = build_industry_groups_response(
            snapshots,
            snapshots,
            [],
            [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )

        self.assertEqual(response.total_groups, 1)
        self.assertEqual(response.groups[0].group_name, "Healthcare (Parent bucket)")
        self.assertEqual(response.groups[0].stock_count, 4)
        self.assertEqual({item.final_group_name for item in response.stocks}, {"Healthcare (Parent bucket)"})

    def _tune(
        self,
        snapshot: StockSnapshot,
        *,
        return_1w: float,
        return_1m: float,
        return_3m: float,
        return_6m: float,
        rs: int,
        rvol: float,
    ) -> StockSnapshot:
        snapshot.stock_return_5d = return_1w
        snapshot.stock_return_20d = return_1m
        snapshot.stock_return_60d = return_3m
        snapshot.stock_return_126d = return_6m
        snapshot.rs_rating = rs
        snapshot.rs_eligible = True
        snapshot.volume = int(snapshot.avg_volume_20d * rvol)
        snapshot.change_pct = max(return_1w / 2, 0.5)
        snapshot.sma50 = snapshot.last_price * 0.9
        snapshot.sma200 = snapshot.last_price * 0.75
        return snapshot

    def _spiker_vs_steady_snapshots(self) -> list[StockSnapshot]:
        """Pharma = one-week spiker with no medium-term trend; auto = steady
        1m/3m/6m leader without this week's pop."""
        spiker_pharma = [
            self._tune(
                self._build_snapshot(
                    symbol=f"FASTPHARMA{index}",
                    sector="Healthcare",
                    sub_sector="Pharmaceuticals",
                    market_cap_crore=8_000.0 + index,
                    start_close=100.0 + index,
                    step=0.25,
                ),
                return_1w=7.0 + index * 0.2,
                return_1m=3.0,
                return_3m=2.0,
                return_6m=5.0,
                rs=88,
                rvol=1.8,
            )
            for index in range(5)
        ]
        steady_auto = [
            self._tune(
                self._build_snapshot(
                    symbol=f"STEADYAUTO{index}",
                    sector="Automobile and Auto Components",
                    sub_sector="Auto Components & Equipments",
                    market_cap_crore=9_000.0 + index,
                    start_close=90.0 + index,
                    step=0.55,
                ),
                return_1w=1.0,
                return_1m=6.0,
                return_3m=25.0,
                return_6m=60.0,
                rs=82,
                rvol=1.0,
            )
            for index in range(5)
        ]
        return [*spiker_pharma, *steady_auto]

    def test_steady_medium_term_strength_outranks_one_week_spike(self) -> None:
        response = build_industry_groups_response(
            self._spiker_vs_steady_snapshots(),
            [],
            [],
            [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )

        by_name = {group.group_name: group for group in response.groups}
        steady = by_name["Auto Ancillaries - Powertrain"]
        spiker = by_name["Pharma Formulations"]

        # The 1-3 month leader holds the top rank...
        self.assertEqual(response.groups[0].group_name, "Auto Ancillaries - Powertrain")
        self.assertGreater(steady.score, spiker.score)
        # ...while the fresh spike is still visible via momentum + emerging flag.
        assert spiker.momentum_score is not None and steady.momentum_score is not None
        self.assertGreater(spiker.momentum_score, steady.momentum_score)
        self.assertTrue(spiker.emerging_flag)
        self.assertFalse(steady.emerging_flag)

    def test_cold_start_smoothed_equals_raw(self) -> None:
        response = build_industry_groups_response(
            self._spiker_vs_steady_snapshots(),
            [],
            [],
            [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )
        for group in response.groups:
            self.assertEqual(group.score, group.raw_score)

    def test_score_smoothing_is_idempotent_and_seeds_from_history(self) -> None:
        snapshots = self._spiker_vs_steady_snapshots()
        cold = build_industry_groups_response(
            snapshots, [], [], [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )
        raw_by_gid = {g.group_id: g.raw_score for g in cold.groups}

        # Wipe today's saved snapshot, then seed three prior sessions with known raw scores.
        for stale in Path(self._history_dir_ctx.name).glob("ranks_*.json"):
            stale.unlink()
        prior_raws = {gid: [50.0, 60.0, 70.0] for gid in raw_by_gid}
        for offset, day_idx in ((3, 0), (2, 1), (1, 2)):
            self._seed_history(
                offset,
                [
                    {"groupId": gid, "rank": 1, "score": 0.0, "rawScore": series[day_idx]}
                    for gid, series in prior_raws.items()
                ],
            )

        first = build_industry_groups_response(
            snapshots, [], [], [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )
        alpha = 2.0 / (industry_groups.SCORE_EMA_SPAN + 1)
        for group in first.groups:
            series = prior_raws[group.group_id] + [raw_by_gid[group.group_id]]
            expected = series[0]
            for value in series[1:]:
                expected = value * alpha + expected * (1 - alpha)
            self.assertAlmostEqual(group.score, round(expected, 2), places=2)
            self.assertEqual(group.raw_score, raw_by_gid[group.group_id])

        # Same-day rebuild (today's snapshot now saved on disk) must not double-smooth.
        second = build_industry_groups_response(
            snapshots, [], [], [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )
        self.assertEqual(
            [(g.group_id, g.rank, g.score) for g in first.groups],
            [(g.group_id, g.rank, g.score) for g in second.groups],
        )

    def test_rank_change_lookback_is_date_based(self) -> None:
        snapshots = self._spiker_vs_steady_snapshots()
        cold = build_industry_groups_response(
            snapshots, [], [], [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )
        gids = [g.group_id for g in cold.groups]
        for stale in Path(self._history_dir_ctx.name).glob("ranks_*.json"):
            stale.unlink()

        # Sessions 7d ago (exact 1w hit) and 28d ago (within 1m tolerance of 30d);
        # nothing near the 91d target for 3m.
        seeded_7d = {gid: idx + 5 for idx, gid in enumerate(gids)}
        seeded_28d = {gid: idx + 9 for idx, gid in enumerate(gids)}
        self._seed_history(7, [{"groupId": gid, "rank": rank, "score": 50.0} for gid, rank in seeded_7d.items()])
        self._seed_history(28, [{"groupId": gid, "rank": rank, "score": 50.0} for gid, rank in seeded_28d.items()])

        response = build_industry_groups_response(
            snapshots, [], [], [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )
        for group in response.groups:
            self.assertEqual(group.rank_change_1w, seeded_7d[group.group_id] - group.rank)
            self.assertEqual(group.rank_change_1m, seeded_28d[group.group_id] - group.rank)
            self.assertIsNone(group.rank_change_3m)

    def test_rank_history_series_exposes_real_per_group_trend(self) -> None:
        """The daily snapshots were always written but never served; this is
        what lets the UI draw a real rank trend instead of a mock curve."""
        self._seed_history(3, [{"groupId": "alpha", "rank": 9, "score": 40.0},
                               {"groupId": "beta", "rank": 2, "score": 88.0}])
        self._seed_history(2, [{"groupId": "alpha", "rank": 6, "score": 55.5},
                               {"groupId": "beta", "rank": 3, "score": 80.0}])
        self._seed_history(1, [{"groupId": "alpha", "rank": 4, "score": 61.0}])

        payload = industry_groups.build_rank_history_series(limit=30)

        self.assertEqual(len(payload["sessions"]), 3)
        self.assertEqual(payload["as_of_date"], payload["sessions"][-1])

        alpha = payload["groups"]["alpha"]
        self.assertEqual([p["rank"] for p in alpha], [9, 6, 4], "series must be oldest-first")
        self.assertEqual([p["score"] for p in alpha], [40.0, 55.5, 61.0])
        # A group absent from the latest session keeps only the days it appeared in.
        self.assertEqual([p["rank"] for p in payload["groups"]["beta"]], [2, 3])

    def test_rank_history_series_limit_keeps_most_recent_and_skips_bad_rows(self) -> None:
        for days_ago in (5, 4, 3, 2, 1):
            self._seed_history(days_ago, [{"groupId": "alpha", "rank": days_ago, "score": 10.0}])
        # Malformed rows must not break or pollute the series.
        self._seed_history(6, [{"groupId": "alpha", "rank": 0},
                               {"groupId": "", "rank": 3},
                               {"rank": 4}])

        payload = industry_groups.build_rank_history_series(limit=2)
        self.assertEqual(len(payload["sessions"]), 2, "limit keeps only the newest sessions")
        self.assertEqual([p["rank"] for p in payload["groups"]["alpha"]], [2, 1])

        full = industry_groups.build_rank_history_series(limit=90)
        self.assertNotIn("", full["groups"])
        self.assertEqual([p["rank"] for p in full["groups"]["alpha"]], [5, 4, 3, 2, 1],
                         "rank<=0 and id-less rows are dropped")


if __name__ == "__main__":
    unittest.main()
