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
        self.assertEqual({group.group_name for group in response.groups}, {"Pharma", "Auto Ancillaries"})
        self.assertEqual({item.final_group_name for item in response.stocks}, {"Pharma", "Auto Ancillaries"})
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
          self.assertIn("Pharma", groups_path.read_text(encoding="utf-8"))
          self.assertIn("Auto Ancillaries", stocks_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()