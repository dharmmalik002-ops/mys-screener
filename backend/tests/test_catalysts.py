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


def _news(n: str, title: str, days_ago: int = 1) -> dict:
    dt = datetime.now(IST) - timedelta(days=days_ago)
    return {"id": f"news:{n}", "date": dt.isoformat(), "source": "The Economic Times", "source_type": "news",
            "title": title, "details": "", "link": f"https://news.google.com/{n}"}


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


def test_generic_and_unrelated_news_never_reach_the_ai():
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    xml = f"""<rss><channel>
      <item><title>Reliance Industries wins 10 GW solar order - The Economic Times</title><link>https://a/1</link><pubDate>{now}</pubDate><source url="https://economictimes.indiatimes.com">The Economic Times</source></item>
      <item><title>Stocks to watch: Reliance Industries, TCS, Infosys - Mint</title><link>https://a/2</link><pubDate>{now}</pubDate><source url="https://www.livemint.com">Mint</source></item>
      <item><title>Sensex today: market live updates - Mint</title><link>https://a/3</link><pubDate>{now}</pubDate><source url="https://www.livemint.com">Mint</source></item>
      <item><title>Adani Green bags order - Business Standard</title><link>https://a/4</link><pubDate>{now}</pubDate><source url="https://www.business-standard.com">Business Standard</source></item>
      <item><title>Reliance Industries wins 10 GW solar order - The Economic Times</title><link>https://a/5</link><pubDate>{now}</pubDate><source url="https://economictimes.indiatimes.com">The Economic Times</source></item>
      <item><title>Reliance Industries shares: 5 reasons to buy - MarketsMojo</title><link>https://a/6</link><pubDate>{now}</pubDate><source url="https://www.marketsmojo.com">MarketsMojo</source></item>
      <item><title>Reliance Industries signs pact with Google - Reuters</title><link>https://a/7</link><pubDate>{now}</pubDate><source url="https://www.reuters.com">Reuters</source></item>
    </channel></rss>""".encode()
    items = C.parse_news_rss(xml, "Reliance Industries Limited", "RELIANCE", 30)
    assert sorted(i["title"] for i in items) == ["Reliance Industries signs pact with Google", "Reliance Industries wins 10 GW solar order"]
    assert {i["source"] for i in items} == {"The Economic Times", "Reuters"}


def test_only_reputed_outlets_count():
    assert C.reputed_outlet("https://economictimes.indiatimes.com", "The Economic Times") == "The Economic Times"
    assert C.reputed_outlet("https://m.economictimes.indiatimes.com", "") == "The Economic Times"
    assert C.reputed_outlet("https://www.marketsmojo.com", "MarketsMojo") is None
    assert C.reputed_outlet("https://economictimes.indiatimes.com.evil.example", "x") is None
    assert C.reputed_outlet("", "Reuters") == "Reuters"
    assert C.reputed_outlet("", "Trendlyne") is None
    # The second search goes straight to the reputed sites.
    assert "site:livemint.com" in C.news_queries("Reliance Industries Limited", 60)[1]


def test_bse_filings_are_never_catalysts(tmp_path):
    order = _filing("1", "Receipt of order worth Rs 900 crore", days_ago=2)
    ai = FakeAI(keep={"bse:1", "news:a"})
    svc, _ = _service(tmp_path, [order], news=[_news("a", "Reliance Industries wins 10 GW solar order")], ai=ai)
    record = svc.refresh("RELIANCE")
    assert "bse:1" not in ai.prompts[0]
    assert [c["id"] for c in record["catalysts"]] == ["news:a"]


def test_items_are_judged_once_and_a_quiet_day_costs_no_ai_call(tmp_path, monkeypatch):
    order = _news("1", "Reliance wins Rs 900 crore order", days_ago=2)
    noise = _news("2", "Reliance shifts registered office", days_ago=1)
    ai = FakeAI(keep={"news:1"})
    svc, _ = _service(tmp_path, [], news=[order, noise], ai=ai)

    record = svc.refresh("RELIANCE")
    assert [c["id"] for c in record["catalysts"]] == ["news:1"]
    assert record["catalysts"][0]["ai"] is True
    assert record["seen"] == {"news:1": "kept", "news:2": "dropped"}
    assert record["overall"]["stance"] == "supportive"

    # Same sources tomorrow: nothing new, so no AI call, and the record moves to the new day.
    tomorrow = datetime.now(IST).date() + timedelta(days=1)
    monkeypatch.setattr(C, "today_ist", lambda: tomorrow.isoformat())
    assert svc.needs_refresh("RELIANCE")
    svc.refresh("RELIANCE")
    assert len(ai.prompts) == 1
    assert not svc.needs_refresh("RELIANCE")


def test_only_new_items_go_to_the_ai_and_the_newest_is_first(tmp_path):
    news = [_news("1", "Reliance wins Rs 900 crore order", days_ago=5)]
    ai = FakeAI(keep={"news:1", "news:9"})
    svc, _ = _service(tmp_path, [], news=news, ai=ai)
    svc.refresh("RELIANCE")
    news.insert(0, _news("9", "SEBI fines Reliance Rs 25 crore", days_ago=0))
    record = svc.refresh("RELIANCE")
    assert "id=news:1" not in ai.prompts[1] and "id=news:9" in ai.prompts[1]
    assert "H news:1" in ai.prompts[1]  # known catalysts inform the overall view
    assert [c["id"] for c in record["catalysts"]] == ["news:9", "news:1"]


def test_ai_failure_falls_back_to_rules_and_retries_later(tmp_path):
    order = _news("1", "Reliance wins Rs 900 crore order", days_ago=1)
    ai = FakeAI(keep={"news:1"}, fail=True)
    svc, _ = _service(tmp_path, [], news=[order], ai=ai)
    record = svc.refresh("RELIANCE")
    assert record["pending_ai"] and record["ai_error"]
    assert record["catalysts"][0]["ai"] is False
    assert record["catalysts"][0]["polarity"] == "positive"
    assert "news:1" not in record["seen"]

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
    ai = FakeAI(keep={"news:1"})
    svc, _ = _service(tmp_path, [], news=[_news("1", "Reliance wins Rs 900 crore order")], ai=ai)
    svc.refresh("RELIANCE")
    again, _ = _service(tmp_path, [], ai=ai)
    assert again.load("RELIANCE")["catalysts"][0]["id"] == "news:1"
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


def test_both_news_searches_merge_and_one_failure_is_tolerated(monkeypatch):
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

    def feed(title, url, outlet):
        return f"""<rss><channel><item><title>{title} - {outlet}</title><link>https://a/{abs(hash(title))}</link>
        <pubDate>{now}</pubDate><source url="{url}">{outlet}</source></item></channel></rss>""".encode()

    class Resp:
        def __init__(self, body):
            self.content = body

        def raise_for_status(self):
            pass

    calls = []

    def fake_get(url, **kw):
        calls.append(url)
        if len(calls) == 1:
            return Resp(feed("Reliance Industries wins solar order", "https://www.livemint.com", "Mint"))
        return Resp(feed("Reliance Industries faces GST demand", "https://www.reuters.com", "Reuters"))

    monkeypatch.setattr(C.requests, "get", fake_get)
    items = C.fetch_news("Reliance Industries Limited", "RELIANCE", 60)
    assert len(calls) == 2 and "site%3A" in calls[1]
    assert {i["source"] for i in items} == {"Mint", "Reuters"}

    def half_down(url, **kw):
        if "site%3A" in url:
            raise C.requests.ConnectionError("blocked")
        return Resp(feed("Reliance Industries wins solar order", "https://www.livemint.com", "Mint"))

    monkeypatch.setattr(C.requests, "get", half_down)
    assert [i["source"] for i in C.fetch_news("Reliance Industries Limited", "RELIANCE", 60)] == ["Mint"]
