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


def instance(nature="Consolidated", revenue=True, owners_profit="138840000000") -> bytes:
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
        _fact("ProfitOrLossAttributableToOwnersOfParent", "OneD", owners_profit),
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


def test_an_owners_profit_tagged_zero_does_not_hide_the_period_profit():
    # One filing in eight leaves the attributable line 0 beside a real
    # profit (ANGELONE Mar 2025 read 0.0 on a Rs 236 cr pre-tax profit).
    row = cr.parse_instance(instance(owners_profit="0"))
    assert row["net_profit_crore"] == 14000.0



def _insurer(facts: list[tuple[str, str]]) -> bytes:
    body = "".join([_ctx("OneD", "2026-04-01", "2026-06-30"),
                    _fact("NatureOfReportStandaloneConsolidated", "OneD", "Standalone", unit=None)
                    .replace(' unitRef="None"', "").replace(' decimals="-5"', ""),
                    *[_fact(name, "OneD", value) for name, value in facts]])
    return ('<?xml version="1.0" encoding="UTF-8"?><xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" '
            'xmlns:xbrldi="http://xbrl.org/2006/xbrldi" xmlns:in-capmkt="http://www.sebi.gov.in/xbrl/2025-01-31">'
            f'{body}</xbrli:xbrl>').encode()


def test_a_life_insurer_reads_net_premium_and_profit_after_tax():
    # Element names as LICI / SBILIFE / HDFCLIFE file them.
    row = cr.parse_instance(_insurer([
        ("NetPremiumIncome", "1200000000000"), ("Expenses", "900000000000"),
        ("ProfitLossBeforeTax", "110000000000"), ("ProfitLossAfterTaxAndExtraordinaryItems", "100000000000"),
        ("BasicAndDilutedEPSAfterExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized", "15.81")]),
        basis="standalone")
    assert row["sales_crore"] == 120000.0 and row["net_profit_crore"] == 10000.0 and row["eps"] == 15.81
    assert row["profit_before_tax_crore"] == 11000.0
    assert row["operating_profit_crore"] is None  # no such line; never derived from policyholder expenses


def test_a_general_insurer_reads_premium_earned_and_its_operating_profit():
    # Element names as GICRE / NIACL / GODIGIT file them.
    row = cr.parse_instance(_insurer([
        ("PremiumEarned", "90000000000"), ("NetPremiumWritten", "95000000000"),
        ("OperatingProfitOrLoss", "8000000000"), ("ProfitOrLossBeforeTax", "12000000000"),
        ("ProfitLossAfterTax", "9000000000"),
        ("BasicAndDilutedEPSAfterExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized", "5.13")]),
        basis="standalone")
    assert (row["sales_crore"], row["operating_profit_crore"], row["net_profit_crore"], row["eps"]) == (9000.0, 800.0, 900.0, 5.13)


def test_a_stored_zero_profit_beside_a_real_pre_tax_profit_reads_as_unknown(files):
    doc = json.loads((files / "consolidated_results.json").read_text())
    doc["symbols"]["ANGELONE"] = {"quarters": [
        {"period": "Mar 2025", "sales_crore": 1056.01, "profit_before_tax_crore": 235.73, "net_profit_crore": 0.0, "eps": 19.33},
        {"period": "Dec 2024", "sales_crore": 1000.0, "profit_before_tax_crore": 0.2, "net_profit_crore": 0.0, "eps": 0.0}]}
    (files / "consolidated_results.json").write_text(json.dumps(doc))
    rows = cr.results_for("ANGELONE")
    assert rows[0]["net_profit_crore"] is None
    assert rows[1]["net_profit_crore"] == 0.0  # a genuine break-even quarter stays 0


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
    assert cr.listing_row(row) == {"symbol": "TCS", "basis": "consolidated", "period_end": "2026-09-30",
                                   "filed": "2026-10-09T17:42",
                                   "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/X.xml"}
    assert cr.listing_row({**row, "consolidated": "Non-Consolidated"})["basis"] == "standalone"
    assert cr.listing_row({**row, "consolidated": ""}) is None
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


def _fake_windows(job, monkeypatch, listed_by_day=None, short=(), dead=()):
    """list_window replaced: rows whose filing day falls in the window,
    `short` windows come back incomplete, `dead` ones unanswered."""
    calls: list[tuple[str, str]] = []

    def fake(start, stop):
        calls.append((start.isoformat(), stop.isoformat()))
        if start.isoformat() in dead:
            return None
        rows = [row for day, row in (listed_by_day or []) if start <= day <= stop]
        return rows, start.isoformat() not in short

    monkeypatch.setattr(job, "list_window", fake)
    return calls


def test_the_job_stores_every_column_of_both_bases(files, job, monkeypatch):
    listed = [(date(2026, 10, 9), _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10")),
              (date(2026, 10, 9), _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:40:00", nature="Non-Consolidated")),
              (date(2026, 10, 9), _listing_row("NOTOURS", "30-Sep-2026", "09-Oct-2026 17:00:00"))]
    calls = _fake_windows(job, monkeypatch, listed)
    fetched: list[str] = []

    def fake_fetch(url):
        fetched.append(url)
        assert "TCS" in url
        return instance()  # a consolidated instance for both filings

    monkeypatch.setattr(job, "_fetch_archive", fake_fetch)
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert calls[0] == ("2025-01-01", "2025-01-15")  # first run: from the start of integrated filing
    assert all((date.fromisoformat(b) - date.fromisoformat(a)).days < job.WINDOW_DAYS for a, b in calls)
    rows = doc["symbols"]["TCS"]["quarters"]
    # The filing's own quarter and its previous-quarter column.
    assert [(r["period"], r["sales_crore"]) for r in rows] == [("Sep 2026", 73188.0), ("Jun 2026", 72275.0)]
    assert rows[0]["filed"] == "2026-10-09T17:42" and rows[1]["via"] == "2026-10-09T17:42"
    # The standalone filing's XBRL said Consolidated: not stored as standalone,
    # remembered, and not fetched again.
    assert "standalone" not in doc["symbols"]["TCS"]
    assert doc["queue"] == [] and doc["listed_through"] == "2026-10-10" and doc["listing_gaps"] == []
    assert len(fetched) == 2

    calls.clear()
    assert job.main() == 0
    assert calls == [("2026-10-09", "2026-10-10")]
    assert len(fetched) == 2  # neither filing downloaded again


def test_nothing_listed_is_thrown_away_when_a_window_gets_no_answer(files, job, monkeypatch):
    (files / "consolidated_results.json").write_text(json.dumps({
        "symbols": {}, "listed_through": "2026-09-20", "harvest_version": job.HARVEST_VERSION}))
    listed = [(date(2026, 10, 9), _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10"))]
    # 2026-09-19 .. 2026-10-03 does not answer; the window after it does.
    _fake_windows(job, monkeypatch, listed, dead={"2026-09-19"})
    monkeypatch.setattr(job, "_fetch_archive", lambda url: instance())
    assert job.main() == 1  # red, so it is seen
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert doc["symbols"]["TCS"]["quarters"][0]["period"] == "Sep 2026"  # the answered window was used
    assert doc["listing_gaps"] == [["2026-09-19", "2026-10-03"]]
    assert doc["listed_through"] == "2026-10-10"

    calls = _fake_windows(job, monkeypatch, [])
    assert job.main() == 0
    assert ("2026-09-19", "2026-10-03") in calls
    assert json.loads((files / "consolidated_results.json").read_text())["listing_gaps"] == []


def test_listing_stops_at_its_share_of_the_budget_and_keeps_the_rest(files, job, monkeypatch):
    (files / "consolidated_results.json").write_text(json.dumps({
        "symbols": {}, "listed_through": "2026-10-09", "harvest_version": job.HARVEST_VERSION,
        "listing_gaps": [["2026-01-01", "2026-03-31"]]}))
    clock = {"t": 0.0}
    monkeypatch.setattr(job.time, "monotonic", lambda: clock["t"])

    def slow(start, stop):
        clock["t"] += 200.0  # each window takes 200 s; listing may use 45% of 1200 s
        return [], True

    monkeypatch.setattr(job, "list_window", slow)
    monkeypatch.setattr(sys, "argv", ["update_consolidated_results.py", "--max-seconds", "1200"])
    assert job.main() == 0
    gaps = json.loads((files / "consolidated_results.json").read_text())["listing_gaps"]
    # The new window and two gap windows (newest first) were listed; the rest wait.
    assert gaps and gaps[0][0] < "2026-03-01" and len(gaps) == len(job.windows(date(2026, 1, 1), date(2026, 3, 31))) - 2


def test_an_old_file_reads_its_history_again_over_several_runs(files, job, monkeypatch):
    (files / "consolidated_results.json").write_text(json.dumps({"listed_through": "2026-10-09", "symbols": {
        "TCS": {"quarters": [{"period": "Sep 2026", "sales_crore": 73188.0, "filed": "2026-10-09T17:42"}]}}}))
    listed = [(date(2026, 10, 9), _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10"))]
    calls = _fake_windows(job, monkeypatch, listed, short={"2025-01-01"})
    fetched: list[str] = []
    monkeypatch.setattr(job, "_fetch_archive", lambda url: (fetched.append(url), instance())[1])
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    # The history was listed (new window first, then newest gap first) and the
    # stored TCS filing fetched again for its comparative columns.
    assert calls[0] == ("2026-10-08", "2026-10-10") and calls[1][0] > calls[-1][0]
    assert len(fetched) == 1 and doc["symbols"]["TCS"]["quarters"][1]["period"] == "Jun 2026"
    # One window came back short: still harvesting until it is listed in full.
    assert doc["harvesting"] is True and doc["listing_gaps"] == [["2025-01-01", "2025-01-15"]]
    assert doc["harvest_version"] != job.HARVEST_VERSION

    calls = _fake_windows(job, monkeypatch, [])
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert ("2025-01-01", "2025-01-15") in calls
    assert doc["harvesting"] is False and doc["harvest_version"] == job.HARVEST_VERSION


def test_a_new_reader_restarts_a_re_read_already_under_way(files, job, monkeypatch):
    (files / "consolidated_results.json").write_text(json.dumps({
        "symbols": {}, "listed_through": "2026-10-09", "harvest_version": job.HARVEST_VERSION - 1,
        "harvesting": True, "listing_gaps": [["2025-01-01", "2025-01-15"]],
        "skipped_filings": {"X|2025-06-30|consolidated": "2025-08-01T10:00"}}))
    calls = _fake_windows(job, monkeypatch, [], short={"2025-01-01"})
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    # The whole history was listed again, not just the one gap left over.
    assert len(calls) > 30 and doc["harvesting"] is True and doc["harvest_target"] == job.HARVEST_VERSION
    assert doc["skipped_filings"] == {}
    # The next run carries on rather than starting over a second time.
    calls = _fake_windows(job, monkeypatch, [])
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert ("2025-01-01", "2025-01-15") in calls and len(calls) <= 3
    assert doc["harvesting"] is False and doc["harvest_version"] == job.HARVEST_VERSION


def test_a_filing_never_overrides_a_quarter_s_own_report():
    own = {"period": "Jun 2026", "sales_crore": 100.0, "filed": "2026-08-10T18:00"}
    later = {date(2026, 9, 30): {"period": "Sep 2026", "sales_crore": 120.0},
             date(2026, 6, 30): {"period": "Jun 2026", "sales_crore": 99.0}}  # comparative column
    rows = cr.merge_filing([own], later, date(2026, 9, 30), "2026-10-09T18:00")
    by = {r["period"]: r for r in rows}
    assert by["Jun 2026"]["sales_crore"] == 100.0 and by["Jun 2026"]["filed"] == "2026-08-10T18:00"
    assert by["Sep 2026"]["filed"] == "2026-10-09T18:00"

    # A comparative fills a gap, and a newer comparative (a restatement) replaces it.
    rows = cr.merge_filing([], {date(2025, 9, 30): {"period": "Sep 2025", "sales_crore": 90.0}},
                           date(2026, 9, 30), "2026-10-09T18:00")
    assert rows[0]["via"] == "2026-10-09T18:00"
    rows = cr.merge_filing(rows, {date(2025, 9, 30): {"period": "Sep 2025", "sales_crore": 91.0}},
                           date(2026, 12, 31), "2027-01-20T18:00")
    assert rows[0]["sales_crore"] == 91.0
    # A revision of the own quarter replaces the earlier one; an older one does not.
    rows = cr.merge_filing([own], {date(2026, 6, 30): {"period": "Jun 2026", "sales_crore": 101.0}},
                           date(2026, 6, 30), "2026-08-01T10:00")
    assert rows[0]["sales_crore"] == 100.0


def test_every_quarter_column_of_a_filing_is_read():
    parsed = cr.parse_filing(instance())
    assert parsed["nature"] == "Consolidated"
    assert parsed["quarter_end"] == date(2026, 9, 30)
    assert {end: row["sales_crore"] for end, row in parsed["periods"].items()} == {
        date(2026, 9, 30): 73188.0, date(2026, 6, 30): 72275.0}


def test_archive_downloads_are_paced_http11_and_back_off(job, monkeypatch):
    """nsearchives refuses HTTP/2 and rate-limits close requests: plain
    `requests`, a gap per worker, and an escalating backoff on failure."""
    import requests

    sleeps: list[float] = []
    clock = {"t": 1000.0}
    monkeypatch.setattr(job.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(job.time, "sleep", lambda s: (sleeps.append(s), clock.__setitem__("t", clock["t"] + s)))
    replies = iter([SimpleNamespace(status_code=403, content=b"denied"), SimpleNamespace(status_code=200, content=instance())])
    monkeypatch.setattr(requests.Session, "get", lambda self, url, timeout: next(replies))
    if hasattr(job._local, "archive"):
        del job._local.archive
    body = job._fetch_archive("https://nsearchives.nseindia.com/corporate/xbrl/X.xml")
    assert body == instance()
    assert isinstance(job._local.archive, requests.Session)
    # A 403 backs off at least ARCHIVE_BACKOFF before the retry.
    assert any(s >= job.ARCHIVE_BACKOFF for s in sleeps)
    assert job._statuses.get("403", 0) >= 1


def test_an_empty_page_short_of_the_total_is_retried_not_taken_as_the_end(job, monkeypatch):
    """A rate-limited page comes back as an empty list, which reads exactly
    like the end of the listing; the first live run lost two quarters so."""
    monkeypatch.setattr(job.time, "sleep", lambda s: None)
    pages = {1: [{"n": 1}, {"n": 2}], 2: [], 3: [{"n": 3}]}
    seen = {2: 0}

    def fake_get(url, params=None, *, expect_json):
        page = int(params["page"])
        if page == 2 and seen[2] == 0:  # throttled once
            seen[2] += 1
            return {"data": [], "totalCount": 3}
        return {"data": pages.get(page if page != 2 else 3, []), "totalCount": 3}

    monkeypatch.setattr(job, "_get", fake_get)
    rows, complete = job.list_window(date(2026, 7, 1), date(2026, 9, 28))
    assert complete and [r["n"] for r in rows] == [1, 2, 3]

    # Still short after the retries: kept as incomplete, never silently done.
    monkeypatch.setattr(job, "_get", lambda url, params=None, *, expect_json:
                        {"data": [{"n": 1}] if params["page"] == "1" else [], "totalCount": 5})
    rows, complete = job.list_window(date(2026, 7, 1), date(2026, 9, 28))
    assert not complete and len(rows) == 1




def test_an_unreachable_instance_stays_queued_with_its_attempts_counted(files, job, monkeypatch):
    listed = [(date(2026, 10, 9), _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10"))]
    _fake_windows(job, monkeypatch, listed)
    monkeypatch.setattr(job, "_fetch_archive", lambda url: None)
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert [(q["symbol"], q["attempts"]) for q in doc["queue"]] == [("TCS", 1)]

    _fake_windows(job, monkeypatch, [])
    monkeypatch.setattr(job, "_fetch_archive", lambda url: instance())
    assert job.main() == 0
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert doc["queue"] == [] and doc["symbols"]["TCS"]["quarters"][0]["period"] == "Sep 2026"


def test_a_filing_that_keeps_failing_is_tried_last_never_dropped(files, job, monkeypatch):
    listed = [(date(2026, 10, 9), _listing_row("TCS", "30-Sep-2026", "09-Oct-2026 17:42:10")),
              (date(2026, 10, 9), _listing_row("INFY", "30-Sep-2026", "08-Oct-2026 17:00:00"))]
    _fake_windows(job, monkeypatch, listed)
    tried: list[str] = []
    monkeypatch.setattr(job, "_fetch_archive", lambda url: tried.append(url))
    for _ in range(job.MAX_ATTEMPTS + 1):
        assert job.main() == 0
        _fake_windows(job, monkeypatch, [])
    doc = json.loads((files / "consolidated_results.json").read_text())
    assert sorted((q["symbol"], q["attempts"]) for q in doc["queue"]) == [("INFY", 6), ("TCS", 6)]

    # A newer filing goes ahead of ones that have failed MAX_ATTEMPTS times.
    _fake_windows(job, monkeypatch, [(date(2026, 10, 10), _listing_row("TCS", "30-Jun-2026", "07-Oct-2026 10:00:00"))])
    tried.clear()
    monkeypatch.setattr(sys, "argv", ["update_consolidated_results.py", "--workers", "1"])
    assert job.main() == 0
    assert "30-Jun-2026" in tried[0]
