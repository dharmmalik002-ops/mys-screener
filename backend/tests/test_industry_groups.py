from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import StockSnapshot
from app.providers.free import FreeMarketDataProvider
from app.services.industry_groups import build_industry_groups_response, write_industry_group_files


class IndustryGroupsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = FreeMarketDataProvider()
        self.snapshot_updated_at = datetime(2026, 4, 2, 10, 30, tzinfo=timezone.utc)

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

    def test_fast_recent_group_momentum_outranks_slow_long_term_strength(self) -> None:
        def tune(
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

        fast_pharma = [
            tune(
                self._build_snapshot(
                    symbol=f"FASTPHARMA{index}",
                    sector="Healthcare",
                    sub_sector="Pharmaceuticals",
                    market_cap_crore=8_000.0 + index,
                    start_close=100.0 + index,
                    step=0.25,
                ),
                return_1w=7.0 + index * 0.2,
                return_1m=15.0 + index,
                return_3m=18.0,
                return_6m=20.0,
                rs=88,
                rvol=1.8,
            )
            for index in range(5)
        ]
        stale_auto = [
            tune(
                self._build_snapshot(
                    symbol=f"STALEAUTO{index}",
                    sector="Automobile and Auto Components",
                    sub_sector="Auto Components & Equipments",
                    market_cap_crore=9_000.0 + index,
                    start_close=90.0 + index,
                    step=0.55,
                ),
                return_1w=-1.0,
                return_1m=2.0,
                return_3m=25.0,
                return_6m=80.0,
                rs=82,
                rvol=0.9,
            )
            for index in range(5)
        ]

        response = build_industry_groups_response(
            [*fast_pharma, *stale_auto],
            [],
            [],
            [],
            generated_at=self.snapshot_updated_at,
            benchmark_label="NIFTY 500",
            market_key="india",
        )

        self.assertEqual(response.groups[0].group_name, "Pharma Formulations")
        self.assertGreater(response.groups[0].return_1w, response.groups[1].return_1w)
        self.assertGreater(response.groups[0].score, response.groups[1].score)


if __name__ == "__main__":
    unittest.main()
