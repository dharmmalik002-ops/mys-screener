"""Watchlist trigger/stop alerts.

Pins what a crossing is (a close through the level, either way for a trigger,
downward for a stop), that nothing is sent twice, and that an unconfigured or
failing Telegram never marks anything as sent.
"""

import asyncio
import json

from app.models.market import WatchlistItem, WatchlistNote, WatchlistsStateResponse
from app.services import watchlist_alerts as wa


def _state(notes: dict[str, WatchlistNote], name="Breakouts", extra=None):
    lists = [WatchlistItem(id="a", name=name, color="#000", symbols=list(notes), notes=notes)]
    if extra:
        lists.append(extra)
    return WatchlistsStateResponse(market="india", watchlists=lists)


def _closes(**rows):
    return {sym: ("2026-10-09", prev, last) for sym, (prev, last) in rows.items()}


class _Client:
    available = True

    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    async def send_message(self, chat_id, text, **_):
        if self.fail:
            raise RuntimeError("telegram down")
        self.sent.append((chat_id, text))


def test_trigger_fires_in_the_direction_price_crossed():
    state = _state({
        "UPX": WatchlistNote(trigger=100),
        "DOWNX": WatchlistNote(trigger=100),
        "QUIET": WatchlistNote(trigger=100),
    })
    found = wa.find_crossings(state, _closes(UPX=(98, 101), DOWNX=(103, 99), QUIET=(102, 104)))
    by = {c.symbol: c for c in found}
    assert set(by) == {"UPX", "DOWNX"}
    assert by["UPX"].direction == "up"
    assert by["DOWNX"].direction == "down"


def test_a_close_exactly_at_the_trigger_counts():
    found = wa.find_crossings(_state({"X": WatchlistNote(trigger=100)}), _closes(X=(99, 100)))
    assert [c.kind for c in found] == ["trigger"]


def test_stop_fires_only_on_a_close_below_it():
    state = _state({"A": WatchlistNote(stop=90), "B": WatchlistNote(stop=90)})
    found = wa.find_crossings(state, _closes(A=(92, 89), B=(88, 91)))
    assert [(c.symbol, c.kind) for c in found] == [("A", "stop")]


def test_a_stock_already_past_its_level_does_not_fire_again_each_day():
    state = _state({"X": WatchlistNote(trigger=100)})
    assert wa.find_crossings(state, _closes(X=(105, 107))) == []


def test_the_same_level_on_two_lists_is_one_event():
    extra = WatchlistItem(id="b", name="Other", color="#000", symbols=["X"], notes={"X": WatchlistNote(trigger=100)})
    found = wa.find_crossings(_state({"X": WatchlistNote(trigger=100)}, extra=extra), _closes(X=(99, 101)))
    assert len(found) == 1


def test_symbols_without_levels_or_closes_are_ignored():
    state = _state({"NOLEVEL": WatchlistNote(why="just watching"), "NOCLOSE": WatchlistNote(trigger=10)})
    assert wa.find_crossings(state, _closes(NOLEVEL=(1, 2))) == []


def test_sent_once_then_remembered(tmp_path):
    state = _state({"X": WatchlistNote(trigger=100, stop=90)})
    client = _Client()
    first = asyncio.run(wa.run_watchlist_alerts(state, tmp_path, closes=_closes(X=(99, 101)), client=client, chat_id=7))
    second = asyncio.run(wa.run_watchlist_alerts(state, tmp_path, closes=_closes(X=(99, 101)), client=client, chat_id=7))
    assert first["status"] == "sent" and first["new"] == 1
    assert second["status"] == "no_crossings" and second["new"] == 0
    assert len(client.sent) == 1
    chat_id, text = client.sent[0]
    assert chat_id == 7
    assert "X" in text and "above your trigger 100" in text


def test_unconfigured_sends_nothing_and_marks_nothing(tmp_path, monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ALERT_CHAT_ID", raising=False)
    state = _state({"X": WatchlistNote(trigger=100)})
    summary = asyncio.run(wa.run_watchlist_alerts(state, tmp_path, closes=_closes(X=(99, 101))))
    assert summary["status"] == "not_configured"
    assert summary["configured"] is False
    stored = json.loads((tmp_path / wa.STATE_FILENAME).read_text())
    assert stored["sent_keys"] == []
    # Configured later, the same crossing still gets sent.
    client = _Client()
    later = asyncio.run(wa.run_watchlist_alerts(state, tmp_path, closes=_closes(X=(99, 101)), client=client, chat_id=1))
    assert later["status"] == "sent"


def test_a_failed_send_is_retried_next_run(tmp_path):
    state = _state({"X": WatchlistNote(stop=90)})
    failed = asyncio.run(wa.run_watchlist_alerts(state, tmp_path, closes=_closes(X=(92, 89)), client=_Client(fail=True), chat_id=1))
    assert failed["status"] == "error"
    ok = _Client()
    retried = asyncio.run(wa.run_watchlist_alerts(state, tmp_path, closes=_closes(X=(92, 89)), client=ok, chat_id=1))
    assert retried["status"] == "sent"
    assert "below your stop 90" in ok.sent[0][1]


def test_message_escapes_list_names():
    crossing = wa.Crossing("X", "trigger", 100.0, "up", 99.0, 101.0, "2026-10-09", "<Breakouts & co>")
    text = wa.format_message([crossing])
    assert "&lt;Breakouts &amp; co&gt;" in text


def test_close_history_reader_takes_the_last_two_closes(tmp_path):
    path = tmp_path / "close_history.json"
    path.write_text(json.dumps({"symbols": {
        "abc": {"last_time": 1791504000, "closes": [10, 11, 12.5]},
        "short": {"last_time": 1791504000, "closes": [10]},
        "bad": {"last_time": None, "closes": [1, 2]},
    }}))
    closes = wa.load_last_two_closes(path)
    assert closes == {"ABC": ("2026-10-09", 11.0, 12.5)}
