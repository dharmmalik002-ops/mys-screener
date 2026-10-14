"""The Space pulls the nightly fund artifacts itself; a worse copy never wins."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.mutual_funds import committed_pull, paths  # noqa: E402


class _Resp:
    def __init__(self, doc, status=200):
        self.status_code = status
        self.content = json.dumps(doc).encode()
        self.headers = {"ETag": '"x"'}

    def json(self):
        return json.loads(self.content)


class _Http:
    def __init__(self, resp):
        self.resp = resp

    def get(self, *_a, **_k):
        return self.resp


def _sectors(*lasts: str) -> dict:
    return {"sectors": {f"s{i}": {"daily": {"dates": ["2026-07-16", d]}} for i, d in enumerate(lasts)}}


def _setup(tmp_path, monkeypatch, local: dict) -> Path:
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path)
    committed_pull._etags.clear()
    path = tmp_path / "sector_indices.json"
    path.write_text(json.dumps(local))
    return path


def test_a_newer_sector_file_replaces_a_stale_one(tmp_path, monkeypatch):
    path = _setup(tmp_path, monkeypatch, _sectors("2026-10-13", "2026-07-17"))
    remote = _sectors("2026-10-13", "2026-10-13")
    assert committed_pull.pull("sector_indices.json", http=_Http(_Resp(remote))) is True
    assert json.loads(path.read_text()) == remote


def test_an_older_or_smaller_file_never_replaces_the_local_one(tmp_path, monkeypatch):
    local = _sectors("2026-10-13", "2026-10-13")
    path = _setup(tmp_path, monkeypatch, local)
    older = _sectors("2026-07-17", "2026-10-13")
    assert committed_pull.pull("sector_indices.json", http=_Http(_Resp(older))) is False
    fewer = _sectors("2026-10-14")
    assert committed_pull.pull("sector_indices.json", http=_Http(_Resp(fewer))) is False
    assert committed_pull.pull("sector_indices.json", http=_Http(_Resp({}, status=404))) is False
    assert json.loads(path.read_text()) == local


def test_a_thin_fund_universe_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "DATA_DIR", tmp_path)
    thin = {"as_of": "2026-10-14", "funds": [{}] * 10}
    assert committed_pull.pull("mf_universe.json", http=_Http(_Resp(thin))) is False
    assert not (tmp_path / "mf_universe.json").exists()


def test_background_pull_is_off_outside_the_space(monkeypatch):
    monkeypatch.delenv("SPACE_ID", raising=False)
    monkeypatch.delenv("MF_ARTIFACT_PULL", raising=False)
    started = []
    monkeypatch.setattr(committed_pull.threading, "Thread", lambda *a, **k: started.append(1))
    committed_pull.pull_in_background("sector_indices.json")
    assert started == []
