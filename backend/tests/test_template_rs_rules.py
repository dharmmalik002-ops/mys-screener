"""Rules that were stated but not enforced.

* Minervini's Trend Template has eight criteria; the eighth — an RS ranking of
  at least 70 — was only a score bonus, so laggards passed the template.
* RS Line Leads claims a "fresh RS high"; with the earlier readings unknown (0)
  every rating was a new high.

Run: `cd backend && pytest tests/test_template_rs_rules.py`
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.scanners import definitions as d


def _template(**overrides):
    base = dict(
        last_price=150.0, sma50=140.0, sma150=128.0, sma200=115.0,
        sma200_1m_ago=112.0, sma200_5m_ago=100.0,
        pct_from_52w_low=60.0, pct_from_52w_high=4.0,
        stock_return_20d=6.0, rs_eligible=True, rs_rating=85,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class MinerviniTemplateRsTests(unittest.TestCase):
    def test_a_leader_passes_both_templates(self):
        self.assertIsNotNone(d._minervini_1m(_template()))
        self.assertIsNotNone(d._minervini_5m(_template()))

    def test_rs_below_70_fails_the_template(self):
        for fn in (d._minervini_1m, d._minervini_5m):
            self.assertIsNone(fn(_template(rs_rating=69)))
            self.assertIsNotNone(fn(_template(rs_rating=70)))

    def test_no_rs_history_fails_the_template(self):
        self.assertIsNone(d._minervini_1m(_template(rs_eligible=False)))


def _rs_leader(**overrides):
    base = dict(
        avg_volume_20d=100_000, last_price=200.0, rs_eligible=True, rs_rating=90,
        rs_rating_1d_ago=88, rs_rating_1w_ago=86, rs_rating_1m_ago=78,
        pct_from_52w_high=8.0, sma50=190.0, sma200=170.0,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class RsLineLeadsTests(unittest.TestCase):
    def test_a_fresh_rs_high_below_the_pivot_matches(self):
        self.assertIsNotNone(d._rs_line_leads(_rs_leader()))

    def test_unknown_earlier_readings_are_not_a_fresh_high(self):
        self.assertIsNone(d._rs_line_leads(_rs_leader(rs_rating_1m_ago=0)))
        self.assertIsNone(d._rs_line_leads(_rs_leader(rs_rating_1w_ago=0)))


if __name__ == "__main__":
    unittest.main()
