"""BSE quarterly results stand in for Screener, which refuses the HF Space.

Run: `cd backend && pytest tests/test_bse_quarterly.py`
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from app.services import bse_quarterly as bq


def _row(desc, value):
    return {"fld_desc": desc, "Value": value}


# RELIANCE, quarter ending 30-Jun-26, as BSE's transposed endpoint returns it (Rs million).
RELIANCE_Q = [
    _row("Type", "Un-Audited"),
    _row("Net Sales/Revenue From Operations", "1660130.0000"),
    _row("Other Income", "41080.0000"),
    _row("Expenditure", "-1525210.0000"),
    _row("Depreciation and amortisation expense", "-41960.0000"),
    _row("Finance Costs", "-18380.0000"),
    _row("Profit (+)/ Loss (-) from Ordinary Activities before Tax", "176000.0000"),
    _row("Net Profit (+)/ Loss (-) from Ordinary Activities after Tax", "132720.0000"),
    _row("Net Profit", "132720.0000"),
    _row("Basic EPS for continuing operation", "9.8100"),
    _row("Basic for discontinued & continuing operation", "0.0000"),
]

# A bank files a different schedule.
BANK_Q = [
    _row("Interest Earned/Net Income from sales/services", "793627.8000"),
    _row("Net Profit", "190597.2000"),
    _row("Basic EPS before Extraordinary items", "12.3800"),
]


class ParseQuarterTests(unittest.TestCase):
    def test_million_to_crore_and_screener_style_operating_profit(self):
        q = bq.parse_quarter(RELIANCE_Q, "Jun 2026", "https://example/doc")
        self.assertEqual(q["sales_crore"], 166013.0)
        self.assertEqual(q["net_profit_crore"], 13272.0)
        self.assertEqual(q["profit_before_tax_crore"], 17600.0)
        # Expenses exclude depreciation and interest, as Screener's did.
        self.assertEqual(q["expenses_crore"], round((1525210 - 41960 - 18380) / 10, 2))
        self.assertAlmostEqual(q["operating_margin_pct"], 11.77, places=1)
        self.assertEqual(q["eps"], 9.81)  # the continuing-ops line, not the 0.00 one
        self.assertEqual(q["result_document_url"], "https://example/doc")

    def test_a_bank_schedule_parses(self):
        q = bq.parse_quarter(BANK_Q, "Jun 2026")
        self.assertEqual(q["sales_crore"], 79362.78)
        self.assertEqual(q["eps"], 12.38)
        self.assertIsNone(q["operating_margin_pct"])  # no expenditure line: unknown, not zero

    def test_no_revenue_line_means_no_row(self):
        self.assertIsNone(bq.parse_quarter([_row("Net Profit", "10")], "Jun 2026"))


class ListingTests(unittest.TestCase):
    def test_quarters_newest_first_without_half_or_full_year(self):
        listing = {"Table": [
            {"Q1": "Jun-26;JQ;130.00;u130", "Q2": None, "Q3": None, "Q4": None, "HY": None, "YR": None},
            {"Q1": "Jun-25;JQ;126.00;u126", "Q2": "Sep-25;SQ;127.00;u127", "Q3": "Dec-25;DQ;128.00;u128",
             "Q4": "Mar-26;MQ;129.00;u129", "HY": "Sep-25;SH;127.20;x", "YR": "Mar-26;MC;129.50;x"},
        ]}
        codes = bq.quarter_codes(listing)
        self.assertEqual([c[0] for c in codes], ["Jun 2026", "Mar 2026", "Dec 2025", "Sep 2025", "Jun 2025"])
        self.assertEqual(codes[0][1], "130.00")

    def test_period_label(self):
        self.assertEqual(bq.bse_period_label("Mar-26"), "Mar 2026")
        self.assertIsNone(bq.bse_period_label("garbage"))


class ArtifactTests(unittest.TestCase):
    def test_results_for_reads_the_committed_file(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "quarterly_summary.json"
            path.write_text(json.dumps({"generated_at": "2026-09-29", "symbols": {
                "ABC": {"bse": "1", "quarters": [{"period": "Jun 2026", "sales_crore": 10.0}]}}}))
            with mock.patch.object(bq, "ARTIFACT_PATH", path), mock.patch.object(bq, "_cache", None):
                self.assertEqual(bq.results_for("abc")[0]["sales_crore"], 10.0)
                self.assertEqual(bq.results_for("MISSING"), [])

    def test_the_stored_quarter_code_rebuilds_the_result_link(self):
        # Full URLs were half the file; only the quarter code is stored.
        stored = bq.compact_row({"period": "Jun 2026", "sales_crore": 1.0,
                                 "result_document_url": "https://x/results.aspx?Code=500325&qtr=130.00&RType="})
        self.assertEqual(stored["qtr"], "130.00")
        self.assertNotIn("result_document_url", stored)
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "quarterly_summary.json"
            path.write_text(json.dumps({"symbols": {"RELIANCE": {"bse": "500325", "quarters": [stored]}}}))
            with mock.patch.object(bq, "ARTIFACT_PATH", path), mock.patch.object(bq, "_cache", None):
                row = bq.results_for("RELIANCE")[0]
        self.assertIn("Code=500325&qtr=130.00", row["result_document_url"])
        self.assertNotIn("qtr", row)


if __name__ == "__main__":
    unittest.main()


class EarningsEndpointFallbackTests(unittest.TestCase):
    """With no Screener data, /api/earnings answers from the BSE file instead of
    waiting out Screener's 25 s timeout and returning an empty table."""

    def test_bse_rows_are_served_with_growth(self):
        import asyncio
        from types import SimpleNamespace
        from app.services import dashboard_service as ds

        service = ds.DashboardService.__new__(ds.DashboardService)

        async def snapshots():
            return []

        async def no_fundamentals(*_args, **_kwargs):
            return None

        service._snapshots = snapshots  # type: ignore[method-assign]
        service._read_earnings_cache = lambda _s: None  # type: ignore[method-assign]
        service.provider = SimpleNamespace(
            get_fundamentals_cached=no_fundamentals,
            _fetch_screener_company_page=lambda _s: (_ for _ in ()).throw(AssertionError("Screener must not be tried")),
            _parse_screener_company_page=lambda *_a: {},
        )
        rows = [
            {"period": p, "sales_crore": s, "net_profit_crore": 1.0, "eps": 1.0}
            for p, s in [("Jun 2026", 125.0), ("Mar 2026", 110.0), ("Dec 2025", 105.0), ("Sep 2025", 102.0), ("Jun 2025", 100.0)]
        ]
        with mock.patch.object(ds.bse_quarterly, "results_for", return_value=rows), \
                mock.patch.object(ds.bse_quarterly, "artifact_date", return_value="2026-09-29"):
            payload = asyncio.run(service.get_earnings_summary("ABC"))
        self.assertEqual(payload["source"], "bse")
        latest = payload["quarterly_results"][0]
        self.assertEqual(latest["period"], "Jun 2026")
        self.assertEqual(latest["sales_yoy_pct"], 25.0)
