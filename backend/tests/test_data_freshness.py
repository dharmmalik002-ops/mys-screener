"""The header's data-freshness badge.

It exists to make quiet staleness loud, so the tests pin the two ways it could
lie: calling a stale feed fine, and calling an ordinary holiday stale.
"""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from app.services import data_freshness as df

IST = timezone(timedelta(hours=5, minutes=30))


def _ist(y, m, d, h=20):
    return datetime(y, m, d, h, 0, tzinfo=IST)


def test_expected_session_waits_for_the_evening_and_skips_weekends():
    # Friday 9 Oct 2026, before the bhavcopy is due: Thursday is the newest.
    assert df.expected_session(_ist(2026, 10, 9, 15)) == date(2026, 10, 8)
    # Friday evening: Friday itself.
    assert df.expected_session(_ist(2026, 10, 9, 20)) == date(2026, 10, 9)
    # Saturday and Sunday both read Friday.
    assert df.expected_session(_ist(2026, 10, 10, 12)) == date(2026, 10, 9)
    assert df.expected_session(_ist(2026, 10, 11, 23)) == date(2026, 10, 9)
    # Monday morning: still Friday.
    assert df.expected_session(_ist(2026, 10, 12, 9)) == date(2026, 10, 9)


def test_sessions_between_counts_weekdays_only():
    assert df.sessions_between(date(2026, 10, 9), date(2026, 10, 9)) == 0
    assert df.sessions_between(date(2026, 10, 9), date(2026, 10, 12)) == 1  # Fri -> Mon
    assert df.sessions_between(date(2026, 10, 2), date(2026, 10, 9)) == 5
    # A feed dated after the expected session is not "behind".
    assert df.sessions_between(date(2026, 10, 12), date(2026, 10, 9)) == 0


def test_one_session_behind_is_ok_so_a_holiday_never_reads_stale():
    assert df.feed_status(1, grace=1) == "ok"
    assert df.feed_status(2, grace=1) == "late"
    assert df.feed_status(3, grace=1) == "late"
    assert df.feed_status(4, grace=1) == "stale"
    assert df.feed_status(None, grace=1) == "unknown"


def _write(dir_: Path, name: str, doc) -> None:
    (dir_ / name).write_text(json.dumps(doc), encoding="utf-8")


def test_report_reads_each_feed_and_takes_the_worst(tmp_path):
    _write(tmp_path, "bhavcopy_patch.json", {"date": "2026-10-09"})
    _write(tmp_path, "nse_breadth_history.json", {"days": [{"date": "2026-10-08"}, {"date": "2026-10-09"}]})
    _write(tmp_path, "xp_breadth_history.json", {"latest": {"date": "2026-10-09"}})
    _write(tmp_path, "breakout_stats.json", {"as_of_session": "2026-10-08"})
    _write(tmp_path, "bot_signals.json", {"as_of": "2026-10-08"})
    # Picks last published a week earlier: five sessions behind.
    _write(tmp_path, "lookalike_picks.json", {"calendar": {"2026-09-30": {}, "2026-10-02": {}}})
    _write(tmp_path, "mf_universe.json", {"as_of": "2026-10-09"})

    report = df.build_report(tmp_path, now=_ist(2026, 10, 10, 12))
    by_key = {f["key"]: f for f in report["feeds"]}

    assert report["expected_session"] == "2026-10-09"
    assert by_key["prices"]["status"] == "ok"
    assert by_key["breakouts"]["sessions_behind"] == 1
    assert by_key["breakouts"]["status"] == "ok"
    assert by_key["lookalikes"]["as_of"] == "2026-10-02"
    assert by_key["lookalikes"]["sessions_behind"] == 5
    assert by_key["lookalikes"]["status"] == "stale"
    assert report["status"] == "stale"


def test_a_missing_or_broken_file_is_reported_not_dropped(tmp_path):
    _write(tmp_path, "bhavcopy_patch.json", {"no_date_here": True})
    (tmp_path / "xp_breadth_history.json").write_text("{not json", encoding="utf-8")

    report = df.build_report(tmp_path, now=_ist(2026, 10, 9))
    by_key = {f["key"]: f for f in report["feeds"]}

    assert len(report["feeds"]) == len(df.FEEDS)
    assert by_key["prices"]["status"] == "unknown"
    assert by_key["prices"]["error"] == "no session date in the file"
    assert by_key["xp"]["status"] == "unknown"
    assert by_key["xp"]["error"].startswith("unreadable")
    assert by_key["funds"]["error"] == "file not found"
    assert report["status"] == "unknown"


def test_every_feed_reads_a_date_from_the_committed_files():
    """If a pipeline renames the field a feed reads, the badge would silently
    say "unknown" forever. The shipped files must all yield a date."""
    data_dir = Path(__file__).resolve().parents[1] / "data"
    report = df.build_report(data_dir)
    missing = [f["key"] for f in report["feeds"] if f["as_of"] is None]
    assert missing == []
