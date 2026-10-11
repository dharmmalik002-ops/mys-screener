"""Consolidated results from NSE Integrated Filing XBRL (services/consolidated_results.py,
scripts/update_consolidated_results.py) and their place on the Results page.

Run: `cd backend && pytest tests/test_consolidated_results.py`
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services import bse_quarterly as bq
from app.services import consolidated_results as cr
from app.services import results_feed as rf

IST = timezone(timedelta(hours=5, minutes=30))


def _ctx(cid, start, end, segment=False):
    seg = ('<xbrli:segment><xbrldi:explicitMember dimension="in-capmkt:SegmentsAxis">'
           'in-capmkt:ItServicesMember</xbrldi:explicitMember></xbrli:segment>') if segment else ""
    return (f'<xbrli:context id="{cid}"><xbrli:entity><xbrli:identifier scheme="http://www.sebi.gov.in/xbrl">'
            f'INE467B01029</xbrli:identifier>{seg}</xbrli:entity><xbrli:period><xbrli:startDate>{start}'
            f'</xbrli:startDate><xbrli:endDate>{end}</xbrli:endDate></xbrli:period></xbrli:context>')


def _fact(name, ctx, value, unit="INR"):
    return f'<in-capmkt:{name} contextRef="{ctx}" unitRef="{unit}" decimals="-5">{value}</in-capmkt:{name}>'


def instance(nature="Consolidated", revenue=True) -> bytes:
    """Shaped like a real Integrated Filing: the quarter, the year to date, the
    previous quarter, a segment context for the same quarter, an instant."""
    facts = [
        _fact("NatureOfReportStandaloneConsolidated", "OneD", nature, unit=None).replace(' unitRef="None"', "").replace(' decimals="-5"', ""),
        # Quarter Jul-Sep 2026: Rs 73,188 cr revenue.
        _fact("RevenueFromOperations", "OneD", "731880000000") if revenue else "",
        _fact("Expenses", "OneD", "560000000000"),
        _fact("DepreciationDepletionAndAmortisationExpense", "OneD", "15000000000"),
        _fact("FinanceCosts", "OneD", "2000000000"),
        _fact("ProfitBeforeTax", "OneD", "185000000000"),
        _fact("ProfitLossForPeriod", "OneD", "140000000000"),
        _fact("ProfitOrLossAttributableToOwnersOfParent", "OneD", "138840000000"),
        _fact("BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations", "OneD", "38.35", unit="INRPerShare"),
        # Year to date: must never be read as the quarter.
        _fact("RevenueFromOperations", "FourD", "1454630000000"),
        # The previous quarter, as a comparative column.
        _fact("RevenueFromOperations", "TwoD", "722750000000"),
        # A segment of the same quarter: dimensional, never the total.
        _fact("RevenueFromOperations", "OneD_IT", "999999999999"),
        _fact("PaidUpEquityShareCapital", "N1D", "3620000000"),
    ]
    body = "".join([
        _ctx("OneD", "2026-07-01", "2026-09-30"),
        _ctx("FourD", "2026-04-01", "2026-09-30"),
        _ctx("TwoD", "2026-04-01", "2026-06-30"),
        _ctx("OneD_IT", "2026-07-01", "2026-09-30", segment=True),
        '<xbrli:context id="N1D"><xbrli:entity><xbrli:identifier scheme="http://www.sebi.gov.in/xbrl">'
        'INE467B01029</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-09-30</xbrli:instant>'
        '</xbrli:period></xbrli:context>',
        *facts,
    ])
    return ('<?xml version="1.0" encoding="UTF-8"?><xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" '
            'xmlns:xbrldi="http://xbrl.org/2006/xbrldi" xmlns:in-capmkt="http://www.sebi.gov.in/xbrl/2025-01-31">'
            f'{body}</xbrli:xbrl>').encode()


# ── parsing ─────────────────────────────────────────────────────────────────

def test_reads_the_quarter_not_the_year_to_date_or_a_segment():
    row = cr.parse_instance(instance())
    assert row["period"] == "Sep 2026"
    assert row["sales_crore"] == 73188.0
    # Revenue less expenses before depreciation and interest: 73,188 - (56,000 - 1,500 - 200).
    assert row["operating_profit_crore"] == pytest.approx(73188 - (56000 - 1500 - 200))
    assert row["profit_before_tax_crore"] == 18500.0
    # Owners of the parent, not the total including minority interests.
    assert row["net_profit_crore"] == 13884.0
    assert row["eps"] == 38.35


def test_an_expected_quarter_end_picks_that_column():
    row = cr.parse_instance(instance(), expect_end=date(2026, 6, 30))
    assert (row["period"], row["sales_crore"]) == ("Jun 2026", 72275.0)


def test_a_standalone_filing_or_one_without_revenue_is_not_a_consolidated_row():
    assert cr.parse_instance(instance(nature="Standalone")) is None
    assert cr.parse_instance(instance(revenue=False)) is None
    assert cr.parse_instance(b"<html>not xbrl</html>") is None
    assert cr.parse_instance(b"not xml") is None


def test_listing_rows_keep_consolidated_filings_with_an_instance():
    row = {"symbol": "tcs", "consolidated": "Consolidated", "qe_Date": "30-Sep-2026",
           "creation_Date": "09-Oct-2026 17:42:10", "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/X.xml",
           "ixbrl": "https://nsearchives.nseindia.com/corporate/ixbrl/X.html"}
    assert cr.listing_row(row) == {"symbol": "TCS", "period_end": "2026-09-30", "filed": "2026-10-09T17:42",
                                   "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/X.xml"}
    assert cr.listing_row({**row, "consolidated": "Non-Consolidated"}) is None
    # NSE's null sentinel, bare or joined into a URL.
    assert cr.listing_row({**row, "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/-"}) is None
    assert cr.listing_row({**row, "xbrl": "-"}) is None


# ── on the page ─────────────────────────────────────────────────────────────

def _q(period, sales, profit, eps=1.0):
    return {"period": period, "sales_crore": sales, "operating_profit_crore": sales / 5,
            "net_profit_crore": profit, "eps": eps}


@pytest.fixture
def files(tmp_path, monkeypatch):
    summary = tmp_path / "quarterly_summary.json"
    summary.write_text(json.dumps({"generated_at": "2026-10-10T11:00:00+00:00", "symbols": {
        "TCS": {"bse": "532540", "quarters": [{**_q("Sep 2026", 600.0, 140.0), "qtr": "131.00"}, _q("Jun 2026", 590.0, 136.0),
                                              _q("Sep 2025", 540.0, 120.0)]},
    }}))
    feed = tmp_path / "results_feed.json"
    feed.write_text(json.dumps({"declared": {"TCS": {"Sep 2026": "2026-10-09T15:40"}}}))
    cons = tmp_path / "consolidated_results.json"
    cons.write_text(json.dumps({"generated_at": "2026-10-10T14:00:00+00:00", "symbols": {
        "TCS": {"quarters": [{**_q("Sep 2026", 731.88, 138.84), "filed": "2026-10-09T17:42"},
                             {**_q("Jun 2026", 722.75, 133.49), "filed": "2026-07-10T18:00"},
                             {**_q("Sep 2025", 657.99, 120.75), "filed": "2025-10-09T18:00"}]},
        # Files consolidated on NSE only: dated from that filing.
        "NSEONLY": {"quarters": [{**_q("Sep 2026", 50.0, 5.0), "filed": "2026-10-10T19:05"}]},
    }}))
    monkeypatch.setattr(bq, "ARTIFACT_PATH", summary)
    monkeypatch.setattr(bq, "FEED_PATH", feed)
    monkeypatch.setattr(cr, "ARTIFACT_PATH", cons)
    monkeypatch.setattr(rf, "CALENDAR_PATH", tmp_path / "results_calendar.json")
    return tmp_path


def _snap(symbol, cap):
    return SimpleNamespace(symbol=symbol, name=symbol, sector="IT", sub_sector="Software", market_cap_crore=cap)


def test_each_entry_carries_both_bases_compared_within_their_own_basis(files):
    page = rf.build_quarterly([_snap("TCS", 800000.0), _snap("NSEONLY", 900.0)], date(2026, 10, 1), date(2026, 10, 11))
    by = {item["symbol"]: item for item in page["items"]}
    tcs = by["TCS"]
    assert tcs["declared_at"] == "2026-10-09T15:40"  # the BSE announcement, not NSE's later filing
    assert tcs["standalone"]["metrics"]["sales"]["value"] == 600.0
    cons = tcs["consolidated"]["metrics"]["sales"]
    assert (cons["value"], cons["prev"], cons["year_ago"]) == (731.88, 722.75, 657.99)
    assert cons["yoy_pct"] == pytest.approx((731.88 / 657.99 - 1) * 100, abs=0.01)
    # No four consolidated quarters yet: no consolidated PE rather than a guess.
    assert tcs["consolidated"]["pe"] is None

    only = by["NSEONLY"]
    assert only["standalone"] is None
    assert only["declared_at"] == "2026-10-10T19:05"
    # One consolidated quarter: nothing to compare it with.
    assert only["consolidated"]["metrics"]["sales"]["yoy_pct"] is None


# ── the job, against a fake NSE ─────────────────────────────────────────────

@pytest.fixture
def job(files, monkeypatch):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import update_consolidated_results as module

    root = files / "root"
    (root / "data").mkdir(parents=True)
    (root / "data" / "free_universe.json").write_text(json.dumps([{"symbol": "TCS"}, {"symbol": "INFY"}]))
    (files / "consolidated_results.json").write_text(json.dumps({"symbols": {}}))
    monkeypatch.setattr(module, "BACKEND_ROOT", root)
    monkeypatch.setattr(module, "_now", lambda: datetime(2026, 10, 10, 20, 0, tzinfo=IST))
    monkeypatch.setattr(sys, "argv", ["update_consolidated_results.py", "--workers", "2"])
    return module


def _listing_row(symbol, end, filed, nature="Consolidated"):
    return {"symbol": symbol, "consolidated": nature, "qe_Date": end, "creation_Date": filed,
            "xbrl": f"https://nsearchives.nseindia.com/corporate/xbrl/{symbol}_{end}.xml"}


def test_the_job_queues_downloads_and_stores_consolidated_quarters(files, job, monkeypatch):
    listed = [_listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10"),
              _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:40:00", nature="Non-Consolidated"),
              _listing_row("NOTOURS", "30-Sep-2026", "09-Oct-2026 17:00:00")]
    calls: list[str] = []
    monkeypatch.setattr(job, "fetch_listing", lambda start, end: (calls.append(f"{start}"), list(listed))[1])

    def fake_get(url, params=None, *, expect_json):
        assert not expect_json and "TCS" in url
        return instance()

    monkeypatch.setattr(job, "_get", fake_get)
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert calls == ["2025-01-01"]  # first run: from the start of integrated filing
    assert doc["symbols"]["TCS"]["quarters"][0]["sales_crore"] == 73188.0
    assert doc["symbols"]["TCS"]["quarters"][0]["filed"] == "2026-10-09T17:42"
    assert doc["queue"] == [] and doc["listed_through"] == "2026-10-10"

    # Next run: overlap of one day; the same filing is not fetched again.
    monkeypatch.setattr(job, "_get", lambda *a, **k: pytest.fail("already stored"))
    assert job.main() == 0
    assert calls[-1] == "2026-10-09"


def test_an_unreachable_instance_stays_queued_and_a_failed_listing_does_not_advance(files, job, monkeypatch):
    monkeypatch.setattr(job, "fetch_listing", lambda start, end: [_listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10")])
    monkeypatch.setattr(job, "_get", lambda *a, **k: None)
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert [q["symbol"] for q in doc["queue"]] == ["TCS"] and doc["queue"][0]["attempts"] == 1

    monkeypatch.setattr(job, "fetch_listing", lambda start, end: None)
    monkeypatch.setattr(job, "_get", lambda *a, **k: instance())
    assert job.main() == 1
    doc = json.loads((files / "consolidated_results.json").read_text())
    # The queued filing was still taken; the listing date did not move.
    assert doc["queue"] == [] and doc["symbols"]["TCS"]["quarters"][0]["period"] == "Sep 2026"
    assert doc["listed_through"] == "2026-10-10"
