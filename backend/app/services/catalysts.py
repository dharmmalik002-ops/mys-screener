"""Catalysts: what could move this company's business, from filings, the latest concall and news.

A stock needs a reason to re-rate. This module collects the candidates for one
company and has the AI read them the way an experienced analyst would:

* **BSE corporate announcements** for the scrip (the regulator of record —
  orders, acquisitions, capacity, rating actions, management changes,
  penalties, pledges). Routine compliance filings (trading window, Reg 74(5),
  lost share certificates, newspaper ads, ESOP allotments…) are dropped before
  the AI ever sees them.
* **The latest earnings-call transcript** filed on BSE: its PDF text is read and
  the AI pulls out guidance, capex, order book, margin outlook and the risks
  management admitted to.
* **Google News** for the company name, with market wraps, "stocks to watch"
  lists and price-only stories dropped by rule first.

The AI then keeps only items that change the business outlook, explains each in
plain words, says how it affects revenue / margins / earnings relative to the
company's size, marks it a tailwind or a headwind, and writes the overall read:
reasons to own, reasons to avoid, and what to watch next.

Daily, and cheap: the result is cached per symbol for the IST day in
``APP_STATE_DIR/catalysts/``. Every item is analysed once and remembered by id,
so the next day's refresh only sends the AI what is new — a quiet day costs no
AI call at all. Builds run in a background thread; the route returns the last
known result immediately with ``refreshing: true`` and the page polls.
A scheduler job refreshes every watchlist stock each evening.

Without an AI key (or when the call fails) the page still shows the filtered
filings and news, classified by keyword and marked ``ai: false``; they are sent
to the AI on the next refresh that can reach it.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote_plus

import requests

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
VERSION = 1
BSE_ANN_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
BSE_ATTACH_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/{name}"
BSE_ATTACH_HIS_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/{name}"
BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}
NEWS_URL = "https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"

FILING_LOOKBACK_DAYS = 120
NEWS_LOOKBACK_DAYS = 45
CONCALL_LOOKBACK_DAYS = 200
KEEP_DAYS = 365
MAX_CATALYSTS = 60
MAX_NEW_PER_CALL = 40
TRANSCRIPT_MAX_CHARS = 60_000
TRANSCRIPT_MAX_PAGES = 45
WATCHLIST_REFRESH_MAX = 80
FORCE_MIN_MINUTES = 15

POLARITIES = ("positive", "negative", "mixed")
IMPACTS = ("high", "medium", "low")
HORIZONS = ("near", "medium", "long")
STANCES = ("supportive", "mixed", "cautionary", "quiet")

# Filings that never move a business. Matched against subject + sub-category.
ROUTINE_PATTERNS = [
    r"trading window", r"74\s*\(5\)", r"reg(ulation)?\.?\s*74", r"loss of share certificate", r"duplicate share",
    r"issue of duplicate", r"newspaper", r"news paper", r"advertisement", r"publication of",
    r"compliance certificate", r"reg(ulation)?\.?\s*40\s*\(\s*9", r"reg(ulation)?\.?\s*7\s*\(3\)",
    r"investor complaint", r"statement of investor", r"shareholding pattern", r"esop", r"esos", r"employee stock option",
    r"allotment of .*(esop|options|under .*scheme)", r"book closure", r"record date", r"change in (the )?registrar",
    r"\brta\b", r"certificate under", r"scrutini[sz]er", r"voting results", r"proceedings of", r"notice of (the )?agm",
    r"annual report", r"business responsibility", r"\bbrsr\b", r"secretarial", r"related party transaction",
    r"intimation of (the )?(analyst|investor) meet", r"schedule of (analyst|investor)", r"(analyst|investor)s? meet",
    r"audio recording", r"link of (the )?(audio|recording)", r"recording of", r"loss of certificate",
    r"closure of trading", r"structured digital database", r"\bsdd\b", r"cyber security incident.*nil",
    r"disclosure under regulation 30.*(newspaper|advert)", r"postal ballot", r"e-voting", r"\bagm\b", r"\begm\b",
    r"change in (company )?secretary", r"compliance officer", r"name change of rta", r"reconciliation of share capital",
    r"intimation (of|regarding) (board meeting|date of board)", r"board meeting (intimation|on)",
    r"clarification sought", r"price movement", r"increase in volume",
    r"spurt in volume", r"general updates?$", r"\bxbrl\b", r"format of", r"credit of shares", r"demat",
]
_ROUTINE_RE = re.compile("|".join(ROUTINE_PATTERNS), re.I)
# Kept even if a routine word appears (e.g. "outcome of board meeting - acquisition").
MATERIAL_HINT_RE = re.compile(
    r"\border|contract|award|letter of (intent|award)|\bloa\b|acqui|merger|amalgamat|demerg|capacity|expansion|"
    r"commission(ed|ing)|\blaunch|approval|usfda|warning letter|import alert|form 483|downgrade|upgrade|fund rais|"
    r"\bqip\b|preferential|rights issue|buy\s?back|bonus|split|resign|penalt|show cause|demand|fraud|insolven|default|"
    r"pledge|joint venture|\bjv\b|stake|divest|guidance|fire\b|accident|shutdown|strike",
    re.I,
)
TRANSCRIPT_RE = re.compile(r"transcript|earnings call|conference call|con-?call", re.I)
# Only a transcript itself is read; call intimations and audio links mention the call too.
TRANSCRIPT_DOC_RE = re.compile(r"transcript", re.I)

# News that is about the market, not the company.
GENERIC_NEWS_RE = re.compile(
    r"stocks? to (watch|buy)|stocks in (news|focus)|buzzing stocks|hot stocks|top (gainers|losers)|"
    r"market (live|today|wrap|update|outlook)|sensex|nifty (today|50 today|ends|closes)|closing bell|opening bell|"
    r"share price (today|live)|stock price (today|live)|live updates?|stock market today|trade setup|"
    r"technical (pick|view)|stock picks?|stocks? recommendations?|(buy|sell) or hold|target price|multibagger|"
    r"penny stock|top \d+ stocks|\d+ stocks (to|that)|week ahead|dalal street|mutual funds? (bought|sold)|"
    r"(fii|dii)s? (bought|sold)|block deal|bulk deal|52-week (high|low) list",
    re.I,
)
NAME_SUFFIX_RE = re.compile(r"\b(limited|ltd\.?|ltd|pvt|private|(the )?company|co\.|corporation|corp\.?|inc\.?|india|\(india\)|&)\s*$", re.I)

POSITIVE_RE = re.compile(
    r"order|contract|award|\bloa\b|letter of award|acqui|capacity|expansion|commission|launch|approval|wins?|bagged|"
    r"secures?|partnership|mou|upgrade|buy\s?back|bonus|record|robust|surge|jumps?|soars?|growth|raises? guidance",
    re.I,
)
NEGATIVE_RE = re.compile(
    r"resign|penalt|fine[ds]?\b|show cause|demand notice|tax demand|search|raid|fire|accident|shutdown|strike|downgrade|"
    r"default|pledge|warning letter|import alert|form 483|fraud|insolven|nclt|litigation|sebi order|cancel|delay|"
    r"loss|declin|falls?|plunges?|slump|probe|ban|recall|lower(s|ed)? guidance|cuts? guidance",
    re.I,
)
CATEGORY_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("Earnings call", TRANSCRIPT_RE),
    ("Order win", re.compile(r"order|contract|award|\bloa\b|letter of (intent|award)|tender", re.I)),
    ("M&A / restructuring", re.compile(r"acqui|merger|amalgamat|demerg|stake|divest|joint venture|\bjv\b", re.I)),
    ("Capacity / capex", re.compile(r"capacity|expansion|commission|plant|capex|greenfield|brownfield", re.I)),
    ("Regulatory", re.compile(r"usfda|\bfda\b|warning letter|import alert|form 483|approval|licen|sebi|rbi|cci", re.I)),
    ("Legal / tax", re.compile(r"penalt|fine|show cause|demand|tax|gst|court|arbitration|litigation|nclt|search|raid", re.I)),
    ("Management", re.compile(r"resign|appoint|ceo|cfo|managing director|chairman|auditor", re.I)),
    ("Fund raising", re.compile(r"fund rais|qip|preferential|rights issue|ncd|debenture|warrant", re.I)),
    ("Credit rating", re.compile(r"rating|crisil|icra|care ratings|india ratings", re.I)),
    ("Promoter action", re.compile(r"pledge|encumbr|promoter|offer for sale|\bofs\b", re.I)),
    ("Results", re.compile(r"result|quarter|q[1-4]|profit|revenue|earnings", re.I)),
    ("Product / partnership", re.compile(r"launch|partnership|mou|agreement|patent|product", re.I)),
]


# ───────────────────────────── universe ──────────────────────────────

_universe_cache: dict[str, Any] = {"mtime": None, "by_symbol": {}}


def _universe_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "free_universe.json"


def company_info(symbol: str) -> dict[str, Any] | None:
    path = _universe_path()
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    if _universe_cache["mtime"] != mtime:
        try:
            rows = json.loads(path.read_text())
        except (OSError, ValueError):
            rows = []
        _universe_cache["by_symbol"] = {str(r.get("symbol") or "").upper(): r for r in rows if isinstance(r, dict)}
        _universe_cache["mtime"] = mtime
    return _universe_cache["by_symbol"].get(symbol.upper())


def short_name(name: str) -> str:
    """'Larsen & Toubro Limited' -> 'Larsen & Toubro'. Used for the news query and the title match."""
    out = (name or "").strip()
    for _ in range(3):
        trimmed = NAME_SUFFIX_RE.sub("", out).strip(" .,-")
        if trimmed == out or not trimmed:
            break
        out = trimmed
    return out


# ───────────────────────────── sources ──────────────────────────────


def _parse_bse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.split(".")[0])
    except ValueError:
        return None
    return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt


def is_routine(subject: str) -> bool:
    """A compliance filing that cannot move the business. A material word overrides."""
    text = subject or ""
    if TRANSCRIPT_RE.search(text):
        return False
    if not _ROUTINE_RE.search(text):
        return False
    # "Outcome of board meeting - approval of acquisition" stays.
    stripped = _ROUTINE_RE.sub(" ", text)
    return not MATERIAL_HINT_RE.search(stripped)


def bse_row_to_item(row: dict[str, Any], bse_code: str) -> dict[str, Any] | None:
    news_id = str(row.get("NEWSID") or "").strip()
    subject = unescape(str(row.get("NEWSSUB") or row.get("HEADLINE") or "")).strip()
    if not news_id or not subject:
        return None
    dt = _parse_bse_dt(row.get("NEWS_DT") or row.get("DissemDT") or row.get("News_submission_dt"))
    if dt is None:
        return None
    attachment = str(row.get("ATTACHMENTNAME") or "").strip()
    link = BSE_ATTACH_URL.format(name=attachment) if attachment else (
        f"https://www.bseindia.com/stock-share-price/x/y/{bse_code}/corp-announcements/"
    )
    more = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", str(row.get("MORE") or row.get("HEADLINE") or "")))).strip()
    subcat = str(row.get("SUBCATNAME") or "").strip()
    category = str(row.get("CATEGORYNAME") or "").strip()
    return {
        "id": f"bse:{news_id}",
        "date": dt.isoformat(),
        "source": "BSE filing",
        "source_type": "filing",
        "title": subject,
        "details": more[:1200],
        "bse_category": " / ".join(x for x in (category, subcat) if x),
        "link": link,
        "attachment": attachment or None,
    }


def fetch_bse_announcements(bse_code: str, days: int, session: requests.Session | None = None) -> list[dict[str, Any]]:
    """All of one scrip's announcements over `days`, newest first. Raises on network failure."""
    sess = session or requests.Session()
    sess.headers.update(BSE_HEADERS)
    today = datetime.now(IST).date()
    start = today - timedelta(days=days)

    def query(frm: date, to: date) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        total_pages: int | None = None
        for page in range(1, 20):
            if total_pages is not None and page > total_pages:
                break
            params = {
                "pageno": str(page), "strCat": "-1", "strPrevDate": frm.strftime("%Y%m%d"), "strScrip": bse_code,
                "strSearch": "P", "strToDate": to.strftime("%Y%m%d"), "strType": "C", "subcategory": "-1",
            }
            resp = sess.get(BSE_ANN_URL, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json() if resp.content else {}
            table = (payload or {}).get("Table") or []
            if not table:
                break
            rows.extend(table)
            if total_pages is None:
                try:
                    total_pages = int(table[0].get("TotalPageCnt") or 1)
                except (TypeError, ValueError):
                    total_pages = 1
        return rows

    rows = query(start, today)
    if not rows:
        # The API has returned {} for long ranges before (earnings_metrics); walk it in fortnights.
        end = today
        while end >= start:
            frm = max(start, end - timedelta(days=13))
            rows.extend(query(frm, end))
            end = frm - timedelta(days=1)
    items: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = bse_row_to_item(row, bse_code)
        if item:
            items[item["id"]] = item
    return sorted(items.values(), key=lambda i: i["date"], reverse=True)


def news_matches_company(title: str, name: str, symbol: str) -> bool:
    low = title.lower()
    short = short_name(name).lower()
    if short and short in low:
        return True
    if symbol and re.search(rf"\b{re.escape(symbol.lower())}\b", low):
        return True
    # "Larsen & Toubro" also appears as "L&T"; accept the first two words of a long name.
    words = [w for w in re.split(r"\s+", short) if len(w) > 2]
    return len(words) >= 2 and " ".join(words[:2]) in low


def parse_news_rss(xml_bytes: bytes, name: str, symbol: str, days: int) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out: dict[str, dict[str, Any]] = {}
    for item in root.iter("item"):
        raw_title = unescape((item.findtext("title") or "").strip())
        link = (item.findtext("link") or "").strip()
        source_el = item.find("source")
        publisher = (source_el.text or "").strip() if source_el is not None and source_el.text else ""
        title = raw_title
        if publisher and title.endswith(f" - {publisher}"):
            title = title[: -len(publisher) - 3].strip()
        if not title or not link:
            continue
        try:
            published = parsedate_to_datetime(item.findtext("pubDate") or "")
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        if published < cutoff:
            continue
        if GENERIC_NEWS_RE.search(title) or not news_matches_company(title, name, symbol):
            continue
        key = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
        news_id = "news:" + hashlib.sha1(key.encode()).hexdigest()[:16]
        if news_id in out:
            continue
        desc = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", item.findtext("description") or ""))).strip()
        out[news_id] = {
            "id": news_id,
            "date": published.astimezone(IST).isoformat(),
            "source": publisher or "News",
            "source_type": "news",
            "title": title,
            "details": desc[:400] if desc and desc.lower() != title.lower() else "",
            "link": link,
        }
    return sorted(out.values(), key=lambda i: i["date"], reverse=True)


def fetch_news(name: str, symbol: str, days: int) -> list[dict[str, Any]]:
    query = f'"{short_name(name)}" when:{days}d'
    resp = requests.get(NEWS_URL.format(q=quote_plus(query)), headers={"User-Agent": BSE_HEADERS["User-Agent"]}, timeout=15)
    resp.raise_for_status()
    return parse_news_rss(resp.content, name, symbol, days)[:40]


def fetch_transcript_text(attachment: str, session: requests.Session | None = None) -> str | None:
    """The PDF's text, whitespace-collapsed and capped. None when it cannot be read."""
    if not attachment:
        return None
    sess = session or requests.Session()
    sess.headers.update(BSE_HEADERS)
    data = None
    for template in (BSE_ATTACH_URL, BSE_ATTACH_HIS_URL):
        try:
            resp = sess.get(template.format(name=attachment), timeout=40)
            if resp.ok and resp.content[:4] == b"%PDF":
                data = resp.content
                break
        except requests.RequestException as exc:
            logger.info("transcript %s not fetched: %s", attachment, exc)
    if not data:
        return None
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(data))
        parts: list[str] = []
        total = 0
        for page in reader.pages[:TRANSCRIPT_MAX_PAGES]:
            text = page.extract_text() or ""
            parts.append(text)
            total += len(text)
            if total > TRANSCRIPT_MAX_CHARS:
                break
    except Exception as exc:  # a scanned or malformed PDF
        logger.info("transcript %s unreadable: %s", attachment, exc)
        return None
    text = re.sub(r"[ \t]+", " ", "\n".join(parts))
    text = re.sub(r"\n\s*\n+", "\n", text).strip()
    return text[:TRANSCRIPT_MAX_CHARS] if len(text) > 500 else None


# ───────────────────────────── classification ──────────────────────────────


def classify_by_rule(item: dict[str, Any]) -> dict[str, Any]:
    """The no-AI fallback: keyword polarity and category, clearly marked as such."""
    text = f"{item.get('title', '')} {item.get('details', '')}"
    pos = len(POSITIVE_RE.findall(text))
    neg = len(NEGATIVE_RE.findall(text))
    polarity = "positive" if pos > neg else "negative" if neg > pos else "mixed"
    category = next((name for name, rx in CATEGORY_RULES if rx.search(text)), "Company update")
    return {
        **_public_fields(item),
        "polarity": polarity,
        "category": category,
        "impact": "low",
        "horizon": "medium",
        "headline": item.get("title", ""),
        "what_happened": item.get("details") or "",
        "effect_on_company": "",
        "analyst_view": "",
        "ai": False,
    }


def _public_fields(item: dict[str, Any]) -> dict[str, Any]:
    return {k: item.get(k) for k in ("id", "date", "source", "source_type", "title", "link")}


def _pick(value: Any, allowed: tuple[str, ...], default: str) -> str:
    text = str(value or "").strip().lower()
    return text if text in allowed else default


def _clip(value: Any, limit: int) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def build_prompt(
    company: dict[str, Any],
    new_items: list[dict[str, Any]],
    transcript: dict[str, Any] | None,
    known: list[dict[str, Any]],
) -> str:
    ctx = [f"Company: {company.get('name')} (NSE: {company.get('symbol')})"]
    for label, key in (("Sector", "sector"), ("Industry", "sub_sector")):
        if company.get(key):
            ctx.append(f"{label}: {company[key]}")
    if company.get("market_cap_crore"):
        ctx.append(f"Market cap: Rs {company['market_cap_crore']:,.0f} crore")
    if company.get("ttm_sales_crore"):
        ctx.append(f"Revenue, last four quarters (standalone): Rs {company['ttm_sales_crore']:,.0f} crore")
    if company.get("ttm_profit_crore") is not None:
        ctx.append(f"Net profit, last four quarters (standalone): Rs {company['ttm_profit_crore']:,.0f} crore")
    if company.get("latest_quarter"):
        ctx.append(f"Latest filed quarter: {company['latest_quarter']}")

    lines = [
        "You are an experienced Indian equity analyst and stock picker. A stock re-rates when something changes its "
        "business: new orders, capacity, products, approvals, acquisitions, guidance, margins, management, financing, "
        "regulatory or legal events. Your job is to find those CATALYSTS for this company and explain them simply.",
        "",
        "\n".join(ctx),
        "",
        "Rules:",
        "- keep=true ONLY for items that change the company's revenue, margins, earnings, balance sheet, risk or "
        "management. keep=false for generic or routine items: share-price moves with no business reason, market "
        "wraps, 'stocks to watch' lists, broker target-price notes, compliance filings, AGM/record-date notices, "
        "meeting intimations with no content, and duplicates (if a news story repeats a filing, keep the filing).",
        "- Include NEGATIVE catalysts as readily as positive ones: penalties, tax demands, regulatory action, plant "
        "shutdowns, order cancellations, resignations of key people or auditors, pledges, downgrades, weak guidance.",
        "- Size every catalyst against the company: an order worth 2% of annual revenue is low impact, 25% is high. "
        "Use the revenue and market cap above. Never invent numbers that are not in the text; say 'size not "
        "disclosed' when it is not.",
        "- Write for someone who is not a finance expert: short sentences, no jargon (explain any term you must use).",
        "- analyst_view: one or two sentences on what a seasoned stock picker takes from it — why it strengthens the "
        "case for owning the stock, or why it is a reason for caution. No price targets.",
        "- Dates: use the item's own date.",
        "",
    ]
    if new_items:
        lines.append("NEW ITEMS (judge each one, by id):")
        for item in new_items:
            lines.append(
                f"- id={item['id']} | {item['date'][:10]} | {item['source']} | {item['title']}"
                + (f" | category: {item['bse_category']}" if item.get("bse_category") else "")
                + (f" | details: {item['details']}" if item.get("details") else "")
            )
        lines.append("")
    if transcript:
        lines += [
            f"LATEST EARNINGS-CALL TRANSCRIPT (filed {transcript['date'][:10]}): extract the 3-8 points from management's "
            "remarks and the Q&A that matter most for the outlook: revenue/margin guidance, order book, capacity and "
            "capex, new products or markets, demand commentary, and any risks or weak spots management admitted.",
            "<transcript>",
            transcript["text"],
            "</transcript>",
            "",
        ]
    if known:
        lines.append("CATALYSTS ALREADY ON RECORD (use them for the overall view; do not repeat them in items):")
        for c in known[:30]:
            lines.append(f"- {c['date'][:10]} | {c['polarity']} | {c['impact']} | {c['headline']}")
        lines.append("")
    lines += [
        "Reply as JSON:",
        "{",
        '  "items": [{"id": "<id from the list>", "keep": true|false, "polarity": "positive"|"negative"|"mixed",',
        '             "category": "Order win|Capacity / capex|M&A / restructuring|Results|Guidance|Regulatory|Legal / tax|'
        'Management|Fund raising|Credit rating|Promoter action|Product / partnership|Other",',
        '             "impact": "high"|"medium"|"low", "horizon": "near"|"medium"|"long",',
        '             "headline": "<= 12 plain words", "what_happened": "1-2 simple sentences",',
        '             "effect_on_company": "1-2 sentences: how it changes revenue, margins, earnings or risk, sized vs the company",',
        '             "analyst_view": "1-2 sentences"}],',
        '  "concall": [{"headline": ..., "polarity": ..., "category": ..., "impact": ..., "horizon": ...,',
        '               "what_happened": ..., "effect_on_company": ..., "analyst_view": ...}],',
        '  "overall": {"stance": "supportive"|"mixed"|"cautionary"|"quiet", "summary": "2 plain sentences",',
        '              "reasons_to_own": ["short point", ...], "reasons_to_avoid": ["short point", ...],',
        '              "watch_next": ["what would confirm or break the story", ...]}',
        "}",
        "Every id in NEW ITEMS must appear once in items. Leave concall empty when no transcript is given. "
        "'quiet' means there is no meaningful catalyst either way.",
    ]
    return "\n".join(lines)


def parse_ai_reply(
    raw: dict[str, Any],
    new_items: list[dict[str, Any]],
    transcript: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], set[str], list[dict[str, Any]], dict[str, Any] | None]:
    """(kept catalysts, ids judged, concall catalysts, overall) from the model's JSON."""
    by_id = {i["id"]: i for i in new_items}
    kept: list[dict[str, Any]] = []
    judged: set[str] = set()
    for entry in raw.get("items") or []:
        if not isinstance(entry, dict):
            continue
        item = by_id.get(str(entry.get("id") or ""))
        if item is None or item["id"] in judged:
            continue
        judged.add(item["id"])
        if not entry.get("keep"):
            continue
        kept.append(_catalyst_from(entry, _public_fields(item)))
    concall: list[dict[str, Any]] = []
    if transcript:
        for n, entry in enumerate(raw.get("concall") or []):
            if not isinstance(entry, dict) or not entry.get("headline"):
                continue
            base = {
                "id": f"{transcript['id']}#{n}",
                "date": transcript["date"],
                "source": f"Earnings call · {transcript['title'][:60]}",
                "source_type": "concall",
                "title": transcript["title"],
                "link": transcript["link"],
            }
            concall.append(_catalyst_from(entry, base))
    overall = raw.get("overall") if isinstance(raw.get("overall"), dict) else None
    if overall:
        overall = {
            "stance": _pick(overall.get("stance"), STANCES, "mixed"),
            "summary": _clip(overall.get("summary"), 500),
            "reasons_to_own": [_clip(x, 220) for x in (overall.get("reasons_to_own") or []) if str(x).strip()][:6],
            "reasons_to_avoid": [_clip(x, 220) for x in (overall.get("reasons_to_avoid") or []) if str(x).strip()][:6],
            "watch_next": [_clip(x, 220) for x in (overall.get("watch_next") or []) if str(x).strip()][:5],
        }
    return kept, judged, concall, overall


def _catalyst_from(entry: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    return {
        **base,
        "polarity": _pick(entry.get("polarity"), POLARITIES, "mixed"),
        "category": _clip(entry.get("category"), 40) or "Other",
        "impact": _pick(entry.get("impact"), IMPACTS, "low"),
        "horizon": _pick(entry.get("horizon"), HORIZONS, "medium"),
        "headline": _clip(entry.get("headline"), 140) or base.get("title", ""),
        "what_happened": _clip(entry.get("what_happened"), 600),
        "effect_on_company": _clip(entry.get("effect_on_company"), 600),
        "analyst_view": _clip(entry.get("analyst_view"), 500),
        "ai": True,
    }


# ───────────────────────────── store + builder ──────────────────────────────


def today_ist() -> str:
    return datetime.now(IST).date().isoformat()


def _ttm(symbol: str) -> dict[str, Any]:
    try:
        from app.services import bse_quarterly

        rows = bse_quarterly.results_for(symbol)
    except Exception:
        return {}
    months = {m: i for i, m in enumerate(("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1)}

    def key(row: dict[str, Any]) -> int:
        m = re.match(r"([A-Za-z]{3})\w*\s+(\d{4})", str(row.get("period") or ""))
        return int(m.group(2)) * 12 + months.get(m.group(1).lower(), 0) if m else 0

    rows = sorted((r for r in rows if key(r)), key=key)
    if not rows:
        return {}
    last4 = rows[-4:]
    out: dict[str, Any] = {"latest_quarter": rows[-1].get("period")}
    if len(last4) == 4 and key(last4[-1]) - key(last4[0]) == 9:
        sales = [r.get("sales_crore") for r in last4]
        profit = [r.get("net_profit_crore") for r in last4]
        if all(isinstance(x, (int, float)) for x in sales):
            out["ttm_sales_crore"] = float(sum(sales))
        if all(isinstance(x, (int, float)) for x in profit):
            out["ttm_profit_crore"] = float(sum(profit))
    return out


class CatalystService:
    """Per-symbol catalyst records in ``state_dir/catalysts``, rebuilt at most once per IST day."""

    def __init__(
        self,
        state_dir: Path | None,
        llm_api_key: str | None = None,
        gemini_api_key: str | None = None,
        *,
        fetch_filings: Callable[[str, int], list[dict[str, Any]]] | None = None,
        fetch_headlines: Callable[[str, str, int], list[dict[str, Any]]] | None = None,
        fetch_transcript: Callable[[str], str | None] | None = None,
        generate: Callable[[str], dict[str, Any]] | None = None,
    ) -> None:
        self.dir = (state_dir / "catalysts") if state_dir else None
        self.llm_api_key = llm_api_key
        self.gemini_api_key = gemini_api_key
        self._fetch_filings = fetch_filings or (lambda code, days: fetch_bse_announcements(code, days))
        self._fetch_headlines = fetch_headlines or fetch_news
        self._fetch_transcript = fetch_transcript or fetch_transcript_text
        self._generate = generate
        self._client = None
        self._locks: dict[str, threading.Lock] = {}
        self._building: set[str] = set()
        self._guard = threading.Lock()
        self._memory: dict[str, dict[str, Any]] = {}

    @property
    def ai_available(self) -> bool:
        return self._generate is not None or bool(self.llm_api_key or self.gemini_api_key)

    # -- persistence --
    def _path(self, symbol: str) -> Path | None:
        if not self.dir:
            return None
        safe = re.sub(r"[^A-Z0-9_&-]", "_", symbol.upper())
        return self.dir / f"{safe}.json"

    def load(self, symbol: str) -> dict[str, Any] | None:
        symbol = symbol.upper()
        if symbol in self._memory:
            return self._memory[symbol]
        path = self._path(symbol)
        if path and path.exists():
            try:
                record = json.loads(path.read_text())
                if record.get("version") == VERSION:
                    self._memory[symbol] = record
                    return record
            except (OSError, ValueError):
                pass
        return None

    def _save(self, record: dict[str, Any]) -> None:
        self._memory[record["symbol"]] = record
        path = self._path(record["symbol"])
        if not path:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(record, ensure_ascii=False))
            tmp.replace(path)
        except OSError as exc:
            logger.warning("catalysts for %s not saved: %s", record["symbol"], exc)

    # -- AI --
    def _ai(self, prompt: str) -> dict[str, Any]:
        if self._generate is not None:
            return self._generate(prompt)
        from app.services.llm import MODELS, GenConfig, LLMClient

        if self._client is None:
            self._client = LLMClient(api_key=self.llm_api_key, gemini_api_key=self.gemini_api_key)
        last: Exception | None = None
        for model in MODELS:
            try:
                resp = self._client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=GenConfig(temperature=0.2, max_output_tokens=8000, response_mime_type="application/json"),
                )
                return json.loads(resp.text or "{}")
            except Exception as exc:
                last = exc
                logger.info("catalyst AI call with %s failed: %s", model, exc)
        raise RuntimeError(f"AI analysis failed: {last}")

    # -- public --
    def status(self, symbol: str) -> dict[str, Any]:
        """What the route returns: the stored record (possibly stale) plus build state."""
        symbol = symbol.upper()
        record = self.load(symbol)
        with self._guard:
            building = symbol in self._building
        if record is None:
            return {"symbol": symbol, "status": "building" if building else "empty", "refreshing": building,
                    "catalysts": [], "overall": None, "ai_available": self.ai_available}
        return {**record, "status": "ready", "refreshing": building,
                "stale": record.get("refreshed_on") != today_ist(), "ai_available": self.ai_available}

    def needs_refresh(self, symbol: str) -> bool:
        record = self.load(symbol)
        if record is None or record.get("refreshed_on") != today_ist():
            return True
        # Items analysed without the AI get another go once the AI is reachable, at most hourly.
        if self.ai_available and record.get("pending_ai"):
            last = record.get("refreshed_at") or ""
            try:
                return datetime.now(timezone.utc) - datetime.fromisoformat(last) > timedelta(hours=1)
            except ValueError:
                return True
        return False

    def refresh_async(self, symbol: str, force: bool = False) -> bool:
        """Start a background build unless one is running or today's is current. True if started.

        A forced refresh is still throttled to one per `FORCE_MIN_MINUTES`, so a
        repeatedly pressed button cannot run up AI calls.
        """
        symbol = symbol.upper()
        if force:
            last = (self.load(symbol) or {}).get("refreshed_at")
            try:
                if last and datetime.now(timezone.utc) - datetime.fromisoformat(last) < timedelta(minutes=FORCE_MIN_MINUTES):
                    force = False
            except ValueError:
                pass
        if not force and not self.needs_refresh(symbol):
            return False
        with self._guard:
            if symbol in self._building:
                return False
            self._building.add(symbol)

        def run() -> None:
            try:
                self.refresh(symbol)
            except Exception:
                logger.exception("catalyst refresh for %s failed", symbol)
            finally:
                with self._guard:
                    self._building.discard(symbol)

        threading.Thread(target=run, name=f"catalysts-{symbol}", daemon=True).start()
        return True

    def refresh(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        with self._guard:
            lock = self._locks.setdefault(symbol, threading.Lock())
        with lock:
            return self._refresh_locked(symbol)

    def _refresh_locked(self, symbol: str) -> dict[str, Any]:
        info = company_info(symbol) or {"symbol": symbol, "name": symbol}
        company = {**info, "symbol": symbol, **_ttm(symbol)}
        name = str(info.get("name") or symbol)
        bse_code = str(info.get("bse_code") or "").strip()
        previous = self.load(symbol) or {}
        seen: dict[str, str] = dict(previous.get("seen") or {})
        catalysts: dict[str, dict[str, Any]] = {c["id"]: c for c in previous.get("catalysts") or []}
        sources: dict[str, Any] = {}

        filings: list[dict[str, Any]] = []
        if bse_code:
            try:
                filings = self._fetch_filings(bse_code, max(FILING_LOOKBACK_DAYS, CONCALL_LOOKBACK_DAYS))
                sources["filings"] = {"ok": True, "count": len(filings)}
            except Exception as exc:
                logger.info("BSE announcements for %s failed: %s", symbol, exc)
                sources["filings"] = {"ok": False, "error": str(exc)[:160]}
        else:
            sources["filings"] = {"ok": False, "error": "no BSE code on record"}
        try:
            news = self._fetch_headlines(name, symbol, NEWS_LOOKBACK_DAYS)
            sources["news"] = {"ok": True, "count": len(news)}
        except Exception as exc:
            logger.info("news for %s failed: %s", symbol, exc)
            news = []
            sources["news"] = {"ok": False, "error": str(exc)[:160]}

        filing_cutoff = (datetime.now(IST) - timedelta(days=FILING_LOOKBACK_DAYS)).isoformat()
        transcripts = [f for f in filings if f.get("attachment") and TRANSCRIPT_DOC_RE.search(f"{f['title']} {f.get('bse_category', '')}")]
        latest_transcript = transcripts[0] if transcripts else None
        candidates = [
            f for f in filings
            if f is not latest_transcript and f["date"] >= filing_cutoff and not TRANSCRIPT_RE.search(f["title"])
            and not is_routine(f"{f['title']} {f.get('bse_category', '')}")
        ] + news
        new_items = [c for c in candidates if c["id"] not in seen][:MAX_NEW_PER_CALL]

        transcript = None
        concall_meta = previous.get("concall")
        already_read = bool(concall_meta and latest_transcript and concall_meta.get("id") == latest_transcript["id"] and concall_meta.get("read"))
        if latest_transcript and not already_read:
            text = None
            try:
                text = self._fetch_transcript(latest_transcript.get("attachment") or "")
            except Exception as exc:
                logger.info("transcript for %s failed: %s", symbol, exc)
            concall_meta = {k: latest_transcript[k] for k in ("id", "date", "title", "link")}
            concall_meta["read"] = bool(text)
            if text:
                transcript = {**concall_meta, "text": text}
        sources["concall"] = {"ok": bool(concall_meta), "date": (concall_meta or {}).get("date"),
                              "read": bool((concall_meta or {}).get("read"))}

        overall = previous.get("overall")
        pending_ai = False
        ai_error = None
        if new_items or transcript:
            known = sorted((c for c in catalysts.values() if c.get("ai")), key=lambda c: c["date"], reverse=True)
            if self.ai_available:
                try:
                    raw = self._ai(build_prompt(company, new_items, transcript, known))
                    kept, judged, concall, new_overall = parse_ai_reply(raw, new_items, transcript)
                    for item in new_items:
                        if item["id"] in judged:
                            seen[item["id"]] = "kept" if any(k["id"] == item["id"] for k in kept) else "dropped"
                            catalysts.pop(item["id"], None)
                    for c in kept + concall:
                        catalysts[c["id"]] = c
                    if transcript:
                        # The previous call's points are replaced by the newer call's.
                        for cid in [k for k, v in catalysts.items() if v.get("source_type") == "concall" and not k.startswith(transcript["id"] + "#")]:
                            catalysts.pop(cid)
                    if new_overall:
                        overall = new_overall
                    unjudged = [i for i in new_items if i["id"] not in judged]
                    for item in unjudged:
                        catalysts.setdefault(item["id"], classify_by_rule(item))
                    pending_ai = bool(unjudged)
                except Exception as exc:
                    ai_error = str(exc)[:200]
                    logger.warning("catalyst AI for %s failed: %s", symbol, exc)
            if not self.ai_available or ai_error:
                pending_ai = True
                for item in new_items:
                    catalysts.setdefault(item["id"], classify_by_rule(item))
                if transcript:
                    concall_meta = {**concall_meta, "read": False}  # not analysed: read it again next time
        live_ids = {c["id"] for c in candidates}
        # A keyword-classified item that has left the sources' window can no
        # longer be sent to the AI; drop it rather than retry it forever.
        catalysts = {k: v for k, v in catalysts.items() if v.get("ai") or k in live_ids}
        # Any leftover keyword-classified items still need the AI.
        pending_ai = pending_ai or any(not c.get("ai") for c in catalysts.values())

        cutoff = (datetime.now(IST) - timedelta(days=KEEP_DAYS)).isoformat()
        ordered = sorted((c for c in catalysts.values() if c["date"] >= cutoff), key=lambda c: c["date"], reverse=True)
        if overall is None and not ordered:
            overall = {"stance": "quiet", "summary": "No company-specific catalyst in recent filings, calls or news.",
                       "reasons_to_own": [], "reasons_to_avoid": [], "watch_next": []}
        record = {
            "version": VERSION,
            "symbol": symbol,
            "name": name,
            "refreshed_on": today_ist(),
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
            "catalysts": ordered[:MAX_CATALYSTS],
            "overall": overall,
            "concall": concall_meta,
            "sources": sources,
            "pending_ai": pending_ai,
            "ai_error": ai_error,
            # Remember judgements only while their items can still come back from the sources.
            "seen": {k: v for k, v in seen.items() if k in live_ids or k in catalysts},
        }
        self._save(record)
        return record

    def refresh_many(self, symbols: list[str], pause_seconds: float = 2.0) -> int:
        done = 0
        for symbol in symbols[:WATCHLIST_REFRESH_MAX]:
            if not self.needs_refresh(symbol):
                continue
            try:
                self.refresh(symbol)
                done += 1
            except Exception:
                logger.exception("catalyst refresh for %s failed", symbol)
            time.sleep(pause_seconds)
        return done
