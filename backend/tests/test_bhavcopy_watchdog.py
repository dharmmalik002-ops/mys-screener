"""The Space starts daily-bhavcopy.yml itself every 15 minutes from 16:12 IST,
because GitHub's own schedule for it runs hours late or not at all. The
throttle keeps those dispatches from piling runs on top of each other."""

from datetime import datetime, timedelta

from app import main


def _at(hour: int, minute: int, day: int = 12) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=main.IST)


def setup_function() -> None:
    main._bhavcopy_dispatches.clear()


def test_first_dispatch_of_the_day_is_allowed() -> None:
    assert main._bhavcopy_dispatch_allowed(_at(16, 12))


def test_a_run_that_can_still_be_going_holds_the_next_dispatch() -> None:
    main._bhavcopy_dispatches.append(_at(16, 12))
    assert not main._bhavcopy_dispatch_allowed(_at(16, 27))
    assert main._bhavcopy_dispatch_allowed(_at(16, 12) + timedelta(minutes=main.BHAVCOPY_DISPATCH_GAP_MINUTES))


def test_yesterdays_dispatches_do_not_count() -> None:
    main._bhavcopy_dispatches.extend(_at(16, 12 + i, day=9) for i in range(main.BHAVCOPY_MAX_DISPATCHES_PER_DAY))
    assert main._bhavcopy_dispatch_allowed(_at(16, 12))


def test_daily_cap() -> None:
    start = _at(16, 0)
    main._bhavcopy_dispatches.extend(
        start + timedelta(minutes=30 * i) for i in range(main.BHAVCOPY_MAX_DISPATCHES_PER_DAY)
    )
    assert not main._bhavcopy_dispatch_allowed(_at(23, 50))


def test_missing_price_patch_needs_a_run() -> None:
    assert main._bhavcopy_run_needed("2026-10-09", "2026-10-09", _at(16, 12))


def test_nse_breadth_behind_after_the_prices_needs_a_run() -> None:
    # 2026-10-12: prices in at 16:43 IST, NSE's file not yet published, and
    # the whole site dated itself by NSE's last row (the 9th) all evening.
    assert main._bhavcopy_run_needed("2026-10-12", "2026-10-09", _at(17, 12))


def test_nse_breadth_behind_before_its_publish_hour_waits() -> None:
    assert not main._bhavcopy_run_needed("2026-10-12", "2026-10-09", _at(16, 57))


def test_everything_current_needs_nothing() -> None:
    assert not main._bhavcopy_run_needed("2026-10-12", "2026-10-12", _at(19, 12))


def test_nse_last_date_reads_the_committed_file(tmp_path) -> None:
    (tmp_path / "nse_breadth_history.json").write_text('{"days":[{"date":"2026-10-08"},{"date":"2026-10-09"}]}')
    assert main._nse_breadth_last_date(tmp_path) == "2026-10-09"
    assert main._nse_breadth_last_date(tmp_path / "missing") == ""
