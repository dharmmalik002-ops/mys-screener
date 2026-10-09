"""Catalysts tab: filtering, the once-per-item AI contract, and the daily cache."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.catalyst_routes import build_catalyst_router
from app.services import catalysts as C

IST = C.IST


def _filing(news_id: str, title: str, days_ago: int = 1, attachment: str = "", category: str = "Company Update") -> dict:
    dt = datetime.now(IST) - timedelta(days=days_ago)
    row = {
        "NEWSID": news_id, "NEWSSUB": title, "NEWS_DT": dt.strftime("%Y-%m-%dT%H:%M:%S.000"),
        "ATTACHMENTNAME": attachment, "MORE": "", "CATEGORYNAME": category, "SUBCATNAME": "",
    }
    return C.bse_row_to_item(row, "500325")


class FakeAI:
    def __init__(self, keep: set[str] | None = None, fail: bool = False):
        self.keep = keep or set()
        self.fail = fail
        self.prompts: list[str] = []

    def __call__(self, prompt: str) -> dict:
        self.prompts.append(prompt)
        if self.fail:
            raise RuntimeError("quota")
        ids = [line.split("id=")[1].split(" |")[0] for line in prompt.splitlines() if line.startswith("- id=")]
        reply = {
            "items": [
                {"id": i, "keep": i in self.keep, "polarity": "positive", "category": "Order win", "impact": "high",
                 "horizon": "medium", "headline": f"H {i}", "what_happened": "w", "effect_on_company": "e", "analyst_view": "a"}
                for i in ids
            ],
            "concall": [],
            "overall": {"stance": "supportive", "summary": "s", "reasons_to_own": ["r"], "reasons_to_avoid": [], "watch_next": []},
        }
        if "<transcript>" in prompt:
            reply["concall"] = [{"headline": "Guides 20% growth", "polarity": "positive", "category": "Guidance",
                                 "impact": "high", "horizon": "medium", "what_happened": "w", "effect_on_company": "e", "analyst_view": "a"}]
        return reply


def _service(tmp_path, filings, news=None, ai=None, transcript_text="x" * 600):
    calls = {"transcript": 0}

    def fetch_transcript(name):
        calls["transcript"] += 1
        return transcript_text

    svc = C.CatalystService(
        tmp_path,
        fetch_filings=lambda code, days: list(filings),
        fetch_headlines=lambda name, sym, days: list(news or []),
        fetch_transcript=fetch_transcript,
        generate=ai,
    )
    return svc, calls


def test_routine_filings_are_dropped_and_material_ones_kept():
    assert C.is_routine("Closure of Trading Window")
    assert C.is_routine("Certificate under Regulation 74(5) of SEBI (DP) Regulations, 2018")
    assert C.is_routine("Copy of Newspaper Publication")
    assert not C.is_routine("Receipt of Order worth Rs 450 crore from NHAI")
    assert not C.is_routine("Outcome of Board Meeting - Approval of Acquisition of XYZ")
    assert not C.is_routine("Imposition of penalty by GST authority")


def test_generic_and_unrelated_news_never_reach_the_ai():
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    xml = f"""<rss><channel>
      <item><title>Reliance Industries wins 10 GW solar order - ET</title><link>https://a/1</link><pubDate>{now}</pubDate><source>ET</source></item>
      <item><title>Stocks to watch: Reliance Industries, TCS, Infosys - Mint</title><link>https://a/2</link><pubDate>{now}</pubDate><source>Mint</source></item>
      <item><title>Sensex today: market live updates - Mint</title><link>https://a/3</link><pubDate>{now}</pubDate><source>Mint</source></item>
      <item><title>Adani Green bags order - BS</title><link>https://a/4</link><pubDate>{now}</pubDate><source>BS</source></item>
      <item><title>Reliance Industries wins 10 GW solar order - ET</title><link>https://a/5</link><pubDate>{now}</pubDate><source>ET</source></item>
    </channel></rss>""".encode()
    items = C.parse_news_rss(xml, "Reliance Industries Limited", "RELIANCE", 30)
    assert [i["title"] for i in items] == ["Reliance Industries wins 10 GW solar order"]
    assert items[0]["source"] == "ET"


def test_items_are_judged_once_and_a_quiet_day_costs_no_ai_call(tmp_path, monkeypatch):
    order = _filing("1", "Receipt of order worth Rs 900 crore", days_ago=2)
    noise = _filing("2", "Intimation of change in address of registered office", days_ago=1)
    window = _filing("3", "Closure of Trading Window", days_ago=1)
    ai = FakeAI(keep={"bse:1"})
    svc, _ = _service(tmp_path, [order, noise, window], ai=ai)

    record = svc.refresh("RELIANCE")
    assert [c["id"] for c in record["catalysts"]] == ["bse:1"]
    assert record["catalysts"][0]["ai"] is True
    assert "bse:3" not in ai.prompts[0]  # routine filings never sent
    assert record["seen"] == {"bse:1": "kept", "bse:2": "dropped"}
    assert record["overall"]["stance"] == "supportive"

    # Same sources tomorrow: nothing new, so no AI call, and the record moves to the new day.
    tomorrow = datetime.now(IST).date() + timedelta(days=1)
    monkeypatch.setattr(C, "today_ist", lambda: tomorrow.isoformat())
    assert svc.needs_refresh("RELIANCE")
    svc.refresh("RELIANCE")
    assert len(ai.prompts) == 1
    assert not svc.needs_refresh("RELIANCE")


def test_only_new_items_go_to_the_ai_and_the_newest_is_first(tmp_path):
    first = _filing("1", "Receipt of order worth Rs 900 crore", days_ago=5)
    filings = [first]
    ai = FakeAI(keep={"bse:1", "bse:9"})
    svc, _ = _service(tmp_path, filings, ai=ai)
    svc.refresh("RELIANCE")
    filings.insert(0, _filing("9", "Imposition of penalty by SEBI", days_ago=0))
    record = svc.refresh("RELIANCE")
    assert "id=bse:1" not in ai.prompts[1] and "id=bse:9" in ai.prompts[1]
    assert "H bse:1" in ai.prompts[1]  # known catalysts inform the overall view
    assert [c["id"] for c in record["catalysts"]] == ["bse:9", "bse:1"]


def test_ai_failure_falls_back_to_rules_and_retries_later(tmp_path):
    order = _filing("1", "Receipt of order worth Rs 900 crore", days_ago=1)
    ai = FakeAI(keep={"bse:1"}, fail=True)
    svc, _ = _service(tmp_path, [order], ai=ai)
    record = svc.refresh("RELIANCE")
    assert record["pending_ai"] and record["ai_error"]
    assert record["catalysts"][0]["ai"] is False
    assert record["catalysts"][0]["polarity"] == "positive"
    assert "bse:1" not in record["seen"]

    ai.fail = False
    record = svc.refresh("RELIANCE")
    assert record["catalysts"][0]["ai"] is True and not record["pending_ai"]


def test_only_the_transcript_is_read_and_its_points_replace_the_older_calls(tmp_path):
    audio = _filing("5", "Audio recording of Earnings Call", days_ago=3)
    t1 = _filing("6", "Transcript of Earnings Call for Q4 FY26", days_ago=100, attachment="t1.pdf")
    filings = [audio, t1]
    ai = FakeAI()
    svc, calls = _service(tmp_path, filings, ai=ai)
    record = svc.refresh("RELIANCE")
    assert calls["transcript"] == 1
    assert [c["id"] for c in record["catalysts"]] == ["bse:6#0"]
    assert record["catalysts"][0]["source_type"] == "concall"
    assert record["concall"]["read"] is True

    svc.refresh("RELIANCE")
    assert calls["transcript"] == 1  # the same call is never read twice

    filings.insert(0, _filing("7", "Transcript of Earnings Call for Q1 FY27", days_ago=2, attachment="t2.pdf"))
    record = svc.refresh("RELIANCE")
    assert [c["id"] for c in record["catalysts"]] == ["bse:7#0"]


def test_record_persists_across_instances(tmp_path):
    ai = FakeAI(keep={"bse:1"})
    svc, _ = _service(tmp_path, [_filing("1", "Receipt of order worth Rs 900 crore")], ai=ai)
    svc.refresh("RELIANCE")
    again, _ = _service(tmp_path, [], ai=ai)
    assert again.load("RELIANCE")["catalysts"][0]["id"] == "bse:1"
    assert json.loads((tmp_path / "catalysts" / "RELIANCE.json").read_text())["version"] == C.VERSION


def test_route_returns_at_once_and_reports_the_build(tmp_path):
    svc, _ = _service(tmp_path, [], ai=FakeAI())
    started = []
    svc.refresh_async = lambda symbol, force=False: started.append((symbol, force)) or True  # type: ignore[assignment]
    app = FastAPI()
    app.include_router(build_catalyst_router(svc))
    client = TestClient(app)
    body = client.get("/api/catalysts/reliance").json()
    assert started == [("RELIANCE", False)]
    assert body["symbol"] == "RELIANCE" and body["status"] == "empty"
    assert client.get("/api/catalysts/..%2Fetc").status_code in (400, 404)
