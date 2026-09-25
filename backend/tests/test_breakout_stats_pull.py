"""The Space gets the nightly breakout stats only by pulling them.

breakout-stats.yml pushes with GITHUB_TOKEN, which never triggers deploy.yml,
so `pull_latest_breakout_stats` is the whole delivery path. These pin the two
ways it could quietly break the Markets page: overwriting a good file with an
empty one, and replacing newer stats with older ones.
"""

import json

import pytest

import app.main as main


class _Resp:
    def __init__(self, payload, status=200):
        self.status_code = status
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


def _stats(stamp, *, weeks=True, signals=100):
    return {
        "generated_at": stamp,
        "as_of_session": stamp[:10],
        "weeks": [{"week": "2026-W39"}] if weeks else [],
        "signals": signals,
    }


@pytest.fixture
def pull(monkeypatch, tmp_path):
    path = tmp_path / "breakout_stats.json"

    def _run(remote, local=None):
        if local is not None:
            path.write_text(json.dumps(local), encoding="utf-8")
        monkeypatch.setattr(main, "_last_breakout_stats_pull_monotonic", 0.0)
        monkeypatch.setattr("requests.get", lambda *a, **k: _Resp(remote))
        return main.pull_latest_breakout_stats(path=path)

    return _run, path


def test_newer_remote_replaces_local(pull):
    run, path = pull
    assert run(_stats("2026-09-25T15:00:00"), local=_stats("2026-08-10T05:40:00")) is True
    assert json.loads(path.read_text())["generated_at"] == "2026-09-25T15:00:00"


def test_older_or_equal_remote_is_ignored(pull):
    run, path = pull
    assert run(_stats("2026-08-10T05:40:00"), local=_stats("2026-09-25T15:00:00")) is False
    assert json.loads(path.read_text())["generated_at"] == "2026-09-25T15:00:00"


@pytest.mark.parametrize("remote", [_stats("2026-09-26T15:00:00", weeks=False), _stats("2026-09-26T15:00:00", signals=0)])
def test_empty_remote_never_overwrites(pull, remote):
    run, path = pull
    assert run(remote, local=_stats("2026-09-25T15:00:00")) is False
    assert json.loads(path.read_text())["generated_at"] == "2026-09-25T15:00:00"


def test_pull_is_throttled(pull, monkeypatch):
    run, path = pull
    assert run(_stats("2026-09-25T15:00:00")) is True
    # A second call inside the hour does not even ask the network.
    monkeypatch.setattr("requests.get", lambda *a, **k: pytest.fail("pulled twice inside the throttle"))
    assert main.pull_latest_breakout_stats(path=path) is False
