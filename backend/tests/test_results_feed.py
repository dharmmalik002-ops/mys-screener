"""The Results page: which quarter a filing announces, when it was declared,
and what the page shows (services/results_feed.py, scripts/update_results_feed.py).

Run: `cd backend && pytest tests/test_results_feed.py`
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


def _q(period, sales, op, profit, eps, qtr):
    return {"period": period, "sales_crore": sales, "operating_profit_crore": op,
            "net_profit_crore": profit, "eps": eps, "qtr": qtr}


# Eight quarters: enough for QoQ, YoY and two trailing years (PE, PEG).
ABC_SUMMARY = [
    _q("Jun 2026", 110.0, 22.0, 11.0, 1.1, "130.00"),
    _q("Mar 2026", 105.0, 21.0, 10.5, 1.05, "129.00"),
    _q("Dec 2025", 100.0, 20.0, 10.0, 1.0, "128.00"),
    _q("Sep 2025", 100.0, 20.0, 10.0, 1.0, "127.00"),
    _q("Jun 2025", 90.0, 18.0, 8.0, 0.8, "126.00"),
    _q("Mar 2025", 90.0, 18.0, 8.0, 0.8, "125.00"),
    _q("Dec 2024", 90.0, 18.0, 8.0, 0.8, "124.00"),
    _q("Sep 2024", 90.0, 18.0, 8.0, 0.8, "123.00"),
]
ABC_SEP_2026 = _q("Sep 2026", 120.0, 24.0, 12.0, 1.2, "131.00")


@pytest.fixture
def files(tmp_path, monkeypatch):
    summary = tmp_path / "quarterly_summary.json"
    feed = tmp_path / "results_feed.json"
    calendar = tmp_path / "results_calendar.json"
    summary.write_text(json.dumps({"generated_at": "2026-10-10T11:00:00+00:00", "symbols": {
        "ABC": {"bse": "500001", "quarters": ABC_SUMMARY},
        "XYZ": {"bse": "500002", "quarters": [_q("Sep 2026", 50.0, 5.0, 2.0, 0.2, "131.00"),
                                              _q("Jun 2026", 40.0, 4.0, 1.0, 0.1, "130.00")]},
    }}))
    # ABC's June quarter was declared 2026-07-20 at 15:30 (930 minutes).
    calendar.write_text(json.dumps({"ABC": [["2026-04-25", 600], ["2026-07-20", 930]]}))
    feed.write_text(json.dumps({"generated_at": "2026-10-10T14:00:00+00:00", "declared": {}, "pending": {}, "quarters": {}}))
    monkeypatch.setattr(bq, "ARTIFACT_PATH", summary)
    monkeypatch.setattr(bq, "FEED_PATH", feed)
    monkeypatch.setattr(rf, "CALENDAR_PATH", calendar)
    monkeypatch.setattr(rf, "UPCOMING_PATH", tmp_path / "earnings_calendar.json")
    monkeypatch.setattr(cr, "ARTIFACT_PATH", tmp_path / "consolidated_results.json")
    return SimpleNamespace(summary=summary, feed=feed, calendar=calendar, dir=tmp_path)


def _snap(symbol, cap, name=None):
    return SimpleNamespace(symbol=symbol, name=name or symbol, sector="Tech", sub_sector="Software", market_cap_crore=cap)


# ── which quarter a filing announces ────────────────────────────────────────

@pytest.mark.parametrize("filed,period", [
    ("2026-10-10", "Sep 2026"),
    ("2026-10-05", "Sep 2026"),
    # Four days after the quarter ended nobody has closed their books: a
    # results filing then is a late filer's previous quarter.
    ("2026-10-04", "Jun 2026"),
    ("2026-08-14", "Jun 2026"),
    ("2026-05-28", "Mar 2026"),
    ("2026-01-03", "Sep 2025"),
    ("2026-01-20", "Dec 2025"),
])
def test_target_period(filed, period):
    assert rf.target_period(date.fromisoformat(filed)) == period


def test_a_filing_dates_its_quarter_to_the_minute(files):
    feed: dict = {}
    filed = datetime(2026, 10, 9, 17, 42)
    assert rf.apply_filing(feed, "ABC", filed, {"Sep 2026", "Jun 2026"}) == "declared"
    assert feed["declared"]["ABC"]["Sep 2026"] == "2026-10-09T17:42"
    # The limited-review report and press release that follow are not the
    # announcement: the earlier time stands.
    assert rf.apply_filing(feed, "ABC", datetime(2026, 10, 10, 9, 5), {"Sep 2026"}) == "ignored"
    assert feed["declared"]["ABC"]["Sep 2026"] == "2026-10-09T17:42"


def test_figures_not_out_yet_wait_and_keep_the_filing_time(files):
    feed: dict = {}
    filed = datetime(2026, 10, 9, 18, 10)
    # BSE has the filing but not the structured numbers: pending.
    assert rf.apply_filing(feed, "XYZ", filed, {"Jun 2026"}) == "pending"
    assert feed["pending"]["XYZ"]["Sep 2026"] == "2026-10-09T18:10"
    assert "Sep 2026" not in feed["declared"]["XYZ"]
    # The next run retries with the recorded filing time and dates the
    # quarter from it, not from when the figures turned up.
    assert rf.apply_filing(feed, "XYZ", filed, {"Sep 2026", "Jun 2026"}) == "declared"
    assert feed["declared"]["XYZ"]["Sep 2026"] == "2026-10-09T18:10"
    assert "XYZ" not in feed["pending"]


def test_the_calendars_earlier_filing_wins(files):
    # A filing in the evening job's window that the results calendar already
    # dates earlier (the first run reaches back past it) is not the announcement.
    feed: dict = {}
    later = datetime(2026, 8, 30, 11, 0)
    assert rf.apply_filing(feed, "ABC", later, {"Jun 2026"}) == "ignored"
    assert feed["declared"]["ABC"]["Jun 2026"] == "2026-07-20T15:30"


def test_prune_drops_what_the_summary_caught_up_with(files):
    feed = {
        "quarters": {"ABC": {"bse": "500001", "quarters": [ABC_SEP_2026, ABC_SUMMARY[0]]}},
        "pending": {"XYZ": {"Sep 2026": "2026-09-20T10:00"}, "ABC": {"Sep 2026": "2026-10-09T10:00"}},
        "declared": {"OLD": {"Jun 2023": "2023-07-20T10:00"}, "ABC": {"Sep 2026": "2026-10-09T17:42"}},
    }
    summary = json.loads(files.summary.read_text())["symbols"]
    rf.prune(feed, summary, date(2026, 10, 10))
    assert feed["quarters"]["ABC"]["quarters"] == [ABC_SEP_2026]
    assert feed["pending"] == {"ABC": {"Sep 2026": "2026-10-09T10:00"}}
    assert feed["declared"] == {"ABC": {"Sep 2026": "2026-10-09T17:42"}}


# ── the page ────────────────────────────────────────────────────────────────

def test_the_page_lists_results_declared_in_the_range(files):
    files.feed.write_text(json.dumps({
        "generated_at": "2026-10-10T14:00:00+00:00",
        "filings_checked_at": "2026-10-10T14:00:00+00:00",
        "declared": {"ABC": {"Sep 2026": "2026-10-09T17:42"}},
        "pending": {"XYZ": {"Sep 2026": "2026-10-10T18:10"}},
        "quarters": {"ABC": {"bse": "500001", "quarters": [ABC_SEP_2026]}},
    }))
    snaps = [_snap("ABC", 360.0, "Abc Ltd"), _snap("XYZ", 50.0)]
    page = rf.build_quarterly(snaps, date(2026, 10, 1), date(2026, 10, 11))

    # XYZ's filing is still marked pending, but its September figures are in
    # the data: it is listed, dated by the filing's own time, not awaiting.
    assert [(item["symbol"], item["declared_at"]) for item in page["items"]] == [
        ("XYZ", "2026-10-10T18:10"), ("ABC", "2026-10-09T17:42")]
    assert page["awaiting_figures"] == []
    item = page["items"][1]
    assert item["name"] == "Abc Ltd"
    assert item["declared_at"] == "2026-10-09T17:42"
    assert (item["period"], item["prev_period"], item["year_ago_period"]) == ("Sep 2026", "Jun 2026", "Sep 2025")
    sales = item["standalone"]["metrics"]["sales"]
    assert (sales["value"], sales["prev"], sales["year_ago"]) == (120.0, 110.0, 100.0)
    assert sales["yoy_pct"] == 20.0
    assert sales["qoq_pct"] == pytest.approx(9.09)
    # PE: market cap over the latest four quarters' profit (12 + 11 + 10.5 + 10).
    assert item["standalone"]["pe"] == pytest.approx(360.0 / 43.5, abs=0.01)
    # PEG: PE over that profit's growth on the four quarters before (34).
    assert item["standalone"]["peg"] == pytest.approx(round(360.0 / 43.5, 2) / round((43.5 / 34 - 1) * 100, 2), abs=0.01)
    assert "Code=500001&qtr=131.00" in item["standalone"]["result_url"]
    assert item["consolidated"] is None  # files no consolidated results
    assert item["cap_bucket"] == "large"

    # The June quarter is dated from the calendar, so older seasons work too.
    july = rf.build_quarterly(snaps, date(2026, 7, 1), date(2026, 7, 31))
    assert [(i["symbol"], i["period"], i["declared_at"]) for i in july["items"]] == [("ABC", "Jun 2026", "2026-07-20T15:30")]



def test_a_company_bse_has_no_figures_for_gets_its_card_from_nse(files):
    # CANHLIFE, 9 Oct 2026: BSE publishes no line items for an insurer, so its
    # filing stays pending there; NSE's filing has the figures.
    files.feed.write_text(json.dumps({"declared": {}, "quarters": {},
                                      "pending": {"INS": {"Sep 2026": "2026-10-09T16:45"}}}))
    (files.dir / "consolidated_results.json").write_text(json.dumps({"symbols": {"INS": {"standalone": [
        {"period": "Sep 2026", "sales_crore": 2553.16, "net_profit_crore": 43.05, "eps": 0.45, "filed": "2026-10-09T16:48"}]}}}))
    page = rf.build_quarterly([_snap("INS", 13065.0)], date(2026, 10, 1), date(2026, 10, 11))
    # Dated by the BSE filing (16:45), not NSE's later one (16:48).
    assert [(i["symbol"], i["declared_at"]) for i in page["items"]] == [("INS", "2026-10-09T16:45")]
    assert page["items"][0]["standalone"]["metrics"]["net_profit"]["value"] == 43.05
    assert page["awaiting_figures"] == []


def test_growth_on_a_loss_base_is_measured_against_its_size(files):
    # As results trackers print it: from a -3 loss to a +2 profit is +166.67%,
    # from -1 to +2 is +300%; a deeper loss reads negative.
    rows = [_q("Sep 2026", 10.0, 1.0, 2.0, 0.2, "1"), _q("Jun 2026", 10.0, 1.0, -1.0, -0.1, "2"),
            _q("Sep 2025", 10.0, 1.0, -3.0, -0.3, "3")]
    block = rf._metric_block({r["period"]: r for r in rows}, "Sep 2026", "net_profit_crore")
    assert (block["yoy_pct"], block["qoq_pct"]) == (166.67, 300.0)
    assert rf.change_pct(-5.0, -2.0) == -150.0
    assert rf.change_pct(1.0, 0.0) is None
    # No trailing year of profit, no PE.
    assert rf.valuation(rows, 100.0) == (None, None)


def test_cap_buckets_follow_sebi_ranks():
    snaps = [_snap(f"S{i}", 10_000 - i) for i in range(600)]
    buckets = rf.cap_buckets(snaps)
    assert buckets["S0"] == buckets["S99"] == "large"
    assert buckets["S100"] == buckets["S249"] == "mid"
    assert buckets["S250"] == buckets["S499"] == "small"
    assert buckets["S500"] == "micro"


def test_upcoming_lists_board_meetings_ahead(files):
    (files.dir / "earnings_calendar.json").write_text(json.dumps({
        "updated_at": "2026-10-11T04:00:00+00:00",
        "upcoming": {"ABC": "2026-10-20", "XYZ": "2026-10-08", "GONE": "2026-10-15"},
    }))
    page = rf.build_upcoming([_snap("ABC", 360.0), _snap("XYZ", 50.0)], date(2026, 10, 11))
    assert [(i["symbol"], i["date"], i["standalone"]["last_period"]) for i in page["items"]] == [("ABC", "2026-10-20", "Jun 2026")]
    assert page["items"][0]["consolidated"] is None


def test_season_start():
    assert rf.season_start(date(2026, 10, 11)) == date(2026, 10, 1)
    assert rf.season_start(date(2026, 11, 14)) == date(2026, 10, 1)
    assert rf.season_start(date(2026, 2, 3)) == date(2026, 1, 1)


def test_a_run_is_owed_every_half_hour_through_the_day():
    checked = "2026-10-10T05:35:00+00:00"  # 11:05 IST
    assert rf.run_due(checked, datetime(2026, 10, 10, 11, 15, tzinfo=IST)) is False  # a duplicate trigger
    assert rf.run_due(checked, datetime(2026, 10, 10, 11, 35, tzinfo=IST)) is True
    assert rf.run_due(None, datetime(2026, 10, 10, 11, 0, tzinfo=IST)) is True
    assert rf.in_run_hours(datetime(2026, 10, 10, 8, 5, tzinfo=IST))
    assert rf.in_run_hours(datetime(2026, 10, 10, 23, 35, tzinfo=IST))
    assert not rf.in_run_hours(datetime(2026, 10, 10, 3, 5, tzinfo=IST))


# ── the evening script, against a fake BSE ──────────────────────────────────

@pytest.fixture
def job(files, monkeypatch):
    """The evening script, run on 2026-10-10 20:00 IST against `files`."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import update_results_feed as module

    root = files.dir / "root"
    (root / "data").mkdir(parents=True)
    (root / "data" / "free_universe.json").write_text(json.dumps([
        {"symbol": "ABC", "bse_code": "500001"}, {"symbol": "XYZ", "bse_code": "500002"}]))
    monkeypatch.setattr(module, "BACKEND_ROOT", root)
    monkeypatch.setattr(module, "_now", lambda: datetime(2026, 10, 10, 20, 0, tzinfo=IST))
    monkeypatch.setattr(sys, "argv", ["update_results_feed.py", "--workers", "2"])
    return module


def test_the_evening_run_fetches_dates_and_retries(files, job, monkeypatch):
    xyz_ready = {"value": False}
    filings = [("500001", datetime(2026, 10, 9, 17, 42)), ("500001", datetime(2026, 10, 10, 9, 0)),
               ("500002", datetime(2026, 10, 10, 18, 10)), ("999999", datetime(2026, 10, 10, 10, 0))]
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: list(filings))

    def fake_get(url, params):
        if "CorprateResultbeta" in url:
            if params["scripcode"] == "500001" or xyz_ready["value"]:
                return {"Table": [{"Q2": "Sep-26;X;131.00;", "Q1": "Jun-26;X;130.00;"}]}
            return {"Table": [{"Q1": "Jun-26;X;130.00;"}]}
        # Only ABC's September quarter is missing from both files.
        assert "Corp_detailedResult" in url and (params["Scrip_cd"], params["Qtr"]) == ("500001", "131.00")
        return {"table1": [{"fld_desc": "Net Sales/Revenue From Operations", "Value": "1200"},
                           {"fld_desc": "Net Profit", "Value": "120"},
                           {"fld_desc": "Basic EPS", "Value": "1.2"}]}

    monkeypatch.setattr(job, "_get", fake_get)
    assert job.main() == 0

    feed = json.loads(files.feed.read_text())
    assert feed["declared"]["ABC"] == {"Sep 2026": "2026-10-09T17:42"}
    assert feed["quarters"]["ABC"]["quarters"][0]["period"] == "Sep 2026"
    assert feed["quarters"]["ABC"]["quarters"][0]["sales_crore"] == 120.0
    # XYZ filed and BSE has no structured figures yet: it waits.
    assert feed["pending"]["XYZ"] == {"Sep 2026": "2026-10-10T18:10"}
    assert feed["filings_through"] == "2026-10-10" and feed["filings_checked_at"]

    # Next slot: XYZ's numbers are structured now. Its September row is
    # already in the summary, so nothing is fetched — only dated, from the
    # original filing time.
    xyz_ready["value"] = True
    filings.clear()
    assert job.main() == 0
    feed = json.loads(files.feed.read_text())
    assert feed["declared"]["XYZ"] == {"Sep 2026": "2026-10-10T18:10"}
    assert "XYZ" not in feed["pending"]
    assert feed["declared"]["ABC"] == {"Sep 2026": "2026-10-09T17:42"}


def test_a_failed_filings_read_does_not_advance_the_window(files, job, monkeypatch):
    files.feed.write_text(json.dumps({"filings_through": "2026-10-08", "declared": {}, "pending": {}, "quarters": {}}))
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: None)
    assert job.main() == 1
    assert json.loads(files.feed.read_text())["filings_through"] == "2026-10-08"


def test_if_due_skips_a_run_that_just_happened(files, job, monkeypatch):
    # The Space's trigger and GitHub's cron can both fire: a run 10 minutes
    # after the last one is a duplicate.
    files.feed.write_text(json.dumps({"filings_checked_at": "2026-10-10T14:20:00+00:00"}))  # 19:50 IST
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: pytest.fail("should not read BSE"))
    monkeypatch.setattr(sys, "argv", ["update_results_feed.py", "--if-due"])
    assert job.main() == 0


def test_the_season_that_just_closed_is_dated_from_bse_in_stretches(files, job, monkeypatch):
    # A run read today's filings ten minutes ago, but filings before Sep 29 were never read:
    # each run reads one earlier stretch until the June quarter's season
    # (from Jul 1) is covered, then stops.
    files.feed.write_text(json.dumps({"filings_checked_at": "2026-10-10T14:20:00+00:00", "filings_through": "2026-10-10",
                                      "filings_from": "2026-09-29", "declared": {}, "pending": {}, "quarters": {}}))
    reads: list[tuple[date, date]] = []

    def fake_filings(start, end):
        reads.append((start, end))
        return [("500002", datetime(2026, 8, 12, 16, 5))] if start <= date(2026, 8, 12) <= end else []

    monkeypatch.setattr(job, "fetch_filings", fake_filings)
    monkeypatch.setattr(job, "_get", lambda url, params: {"Table": [{"Q1": "Jun-26;X;130.00;"}]})
    monkeypatch.setattr(sys, "argv", ["update_results_feed.py", "--if-due", "--workers", "2"])
    for _ in range(6):
        assert job.main() == 0
    assert reads[0] == (date(2026, 9, 1), date(2026, 9, 28))
    assert reads[-1][0] == date(2026, 7, 1) and len(reads) == 4  # then nothing left to read
    feed = json.loads(files.feed.read_text())
    assert feed["filings_from"] == "2026-07-01"
    assert feed["declared"]["XYZ"] == {"Jun 2026": "2026-08-12T16:05"}
    # A backfill alone does not count as the evening's read.
    assert feed["filings_checked_at"] == "2026-10-10T14:20:00+00:00"


def test_a_schedule_the_parser_cannot_read_is_recorded(files, job, monkeypatch):
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: [("500001", datetime(2026, 10, 9, 17, 42))])

    def fake_get(url, params):
        if "CorprateResultbeta" in url:
            return {"Table": [{"Q2": "Sep-26;X;131.00;", "Q1": "Jun-26;X;130.00;"}]}
        return {"table1": [{"fld_desc": "Interest Income", "Value": "900"}, {"fld_desc": "Net Profit", "Value": "120"}]}

    monkeypatch.setattr(job, "_get", fake_get)
    assert job.main() == 0
    feed = json.loads(files.feed.read_text())
    assert feed["unparsed_samples"] == [{"symbol": "ABC", "names": ["Interest Income", "Net Profit"]}]



def test_a_run_that_finds_nothing_new_writes_nothing(files, job, monkeypatch):
    # Half-hourly runs: one that finds no new filing must not rewrite the
    # file, or `main` gets a commit every half hour that only moves a stamp.
    files.feed.write_text(json.dumps({"generated_at": "2026-10-10T14:00:00+00:00",
                                      "filings_checked_at": "2026-10-10T14:00:00+00:00",
                                      "filings_through": "2026-10-10", "filings_from": "2026-07-01",
                                      "source": "BSE Result filings (declared time) + standalone results (Rs million / 10 = crore)",
                                      "declared": {}, "pending": {}, "quarters": {}}))
    before = files.feed.read_text()
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: [])
    assert job.main() == 0
    assert files.feed.read_text() == before



def test_a_result_is_read_from_its_pdf_while_bse_has_not_structured_it(files, job, monkeypatch):
    # HCLTECH, 12 Oct 2026: the PDF was filed at 17:12; BSE's structured
    # figures and NSE's XBRL came hours later. The card shows the PDF's
    # figures once its comparatives match the stored quarters.
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: [("500001", datetime(2026, 10, 10, 17, 12), "abc-result.pdf")])
    monkeypatch.setattr(job, "_get", lambda url, params: {"Table": [{"Q1": "Jun-26;X;130.00;"}]})  # Sep not structured yet
    monkeypatch.setattr(job, "_download_pdf", lambda name: b"%PDF-1.4" if name == "abc-result.pdf" else None)
    from app.services import result_pdf
    statement = ("Revenue from operations 120.00 110.00 100.00\n"
                 "Net profit for the period 12.00 11.00 10.00\n"
                 "Basic 1.20 1.10 1.00\n")
    monkeypatch.setattr(result_pdf, "extract_text", lambda pdf: statement)
    assert job.main() == 0
    feed = json.loads(files.feed.read_text())
    assert feed["pending"]["ABC"] == {"Sep 2026": "2026-10-10T17:12"}
    row = feed["pdf_quarters"]["ABC"]["standalone"]
    assert (row["sales_crore"], row["net_profit_crore"], row["eps"]) == (120.0, 12.0, 1.2)

    page = rf.build_quarterly([_snap("ABC", 360.0)], date(2026, 10, 1), date(2026, 10, 11))
    item = page["items"][0]
    assert (item["symbol"], item["period"], item["declared_at"]) == ("ABC", "Sep 2026", "2026-10-10T17:12")
    sales = item["standalone"]["metrics"]["sales"]
    assert (sales["value"], sales["prev"], sales["year_ago"], sales["yoy_pct"]) == (120.0, 110.0, 100.0, 20.0)

    # BSE structures the quarter: its figures replace the PDF's, which are dropped.
    monkeypatch.setattr(job, "_get", lambda url, params: (
        {"Table": [{"Q2": "Sep-26;X;131.00;", "Q1": "Jun-26;X;130.00;"}]} if "CorprateResultbeta" in url else
        {"table1": [{"fld_desc": "Net Sales/Revenue From Operations", "Value": "1200"},
                    {"fld_desc": "Net Profit", "Value": "120"}, {"fld_desc": "Basic EPS", "Value": "1.2"}]}))
    assert job.main() == 0
    feed = json.loads(files.feed.read_text())
    assert "ABC" not in (feed.get("pdf_quarters") or {}) and "ABC" not in feed["pending"]


def test_a_pdf_whose_comparatives_do_not_match_is_not_used(files, job, monkeypatch):
    monkeypatch.setattr(job, "fetch_filings", lambda start, end, *category: [("500001", datetime(2026, 10, 10, 17, 12), "abc-result.pdf")])
    monkeypatch.setattr(job, "_get", lambda url, params: {"Table": [{"Q1": "Jun-26;X;130.00;"}]})
    monkeypatch.setattr(job, "_download_pdf", lambda name: b"%PDF-1.4")
    from app.services import result_pdf
    monkeypatch.setattr(result_pdf, "extract_text", lambda pdf: "Revenue from operations 130.00 115.00 99.00\n")
    assert job.main() == 0
    feed = json.loads(files.feed.read_text())
    assert not (feed.get("pdf_quarters") or {}).get("ABC")
    assert rf.build_quarterly([_snap("ABC", 360.0)], date(2026, 10, 1), date(2026, 10, 11))["items"] == []


def test_nse_standalone_columns_fill_quarters_bse_lacks(files):
    # A recent listing: BSE holds only its quarters since listing; NSE's
    # filing carries the year-ago and previous quarters as columns.
    (files.dir / "consolidated_results.json").write_text(json.dumps({"symbols": {"XYZ": {"standalone": [
        {"period": "Sep 2026", "sales_crore": 999.0, "net_profit_crore": 9.0, "filed": "2026-10-10T18:10"},
        {"period": "Sep 2025", "sales_crore": 40.0, "net_profit_crore": 1.5, "via": "2026-10-10T18:10"},
    ]}}}))
    rows = {r["period"]: r for r in rf.standalone_rows("XYZ")}
    assert rows["Sep 2026"]["sales_crore"] == 50.0  # BSE's own figure is kept
    assert rows["Sep 2025"]["sales_crore"] == 40.0  # the gap is filled
    block = rf.basis_block(rf.standalone_rows("XYZ"), "Sep 2026", 100.0)
    assert block["metrics"]["sales"]["yoy_pct"] == 25.0
