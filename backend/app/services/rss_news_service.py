"""RSS News Service for India market/company feeds."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import ssl
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ─────────────────────── Feed catalogue ──────────────────────────────────────

@dataclass
class FeedConfig:
    id: str
    name: str
    url: str
    category: str
    ttl: int          # seconds
    color: str = "#888888"

_INDIA_FEEDS: list[FeedConfig] = [
    # ── Tier 1: 60s ──────────────────────────────────────────────────────────
    FeedConfig("livemint-markets",   "Livemint Markets",  "https://www.livemint.com/rss/markets",                                                                                               "Markets",   60,  "#EC4327"),
    FeedConfig("et-markets",         "ET Markets",         "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",                                                               "Markets",   60,  "#1A73E8"),
    # ── Tier 2: 120s ─────────────────────────────────────────────────────────
    FeedConfig("livemint-companies", "Livemint Companies", "https://www.livemint.com/rss/companies",                                                                                            "Corporate", 120, "#EC4327"),
    FeedConfig("et-stocks",          "ET Stocks",          "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",                                                          "Stocks",    120, "#1A73E8"),
    FeedConfig("et-corporate",       "ET Corporate",       "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms",                                                               "Corporate", 120, "#1A73E8"),
    FeedConfig("bbc-world",          "BBC World",          "http://feeds.bbci.co.uk/news/world/rss.xml",                                                                                        "World",     120, "#BB1919"),
    # ── Tier 3: 300s ─────────────────────────────────────────────────────────
    FeedConfig("livemint-news",      "Livemint News",      "https://www.livemint.com/rss/news",                                                                                                 "Headlines", 300, "#EC4327"),
    FeedConfig("livemint-money",     "Livemint Money",     "https://www.livemint.com/rss/money",                                                                                                "Money",     300, "#EC4327"),
    FeedConfig("etcfo-top",          "ET CFO",             "https://cfo.economictimes.indiatimes.com/rss/topstories",                                                                           "CFO",       300, "#0F766E"),
    FeedConfig("gn-bloomberg",       "Bloomberg",          "https://news.google.com/rss/search?q=when:1d+source:%22Bloomberg%22&hl=en-US&gl=US&ceid=US:en",                                    "Markets",   300, "#F59E0B"),
    FeedConfig("gn-reuters",         "Reuters",            "https://news.google.com/rss/search?q=when:1d+source:%22Reuters%22+India+stock+market&hl=en-IN&gl=IN&ceid=IN:en",                   "Global",    300, "#F97316"),
    FeedConfig("zerohedge",          "ZeroHedge",          "https://feeds.feedburner.com/zerohedge/feed",                                                                                       "Markets",   300, "#D97706"),
    FeedConfig("gn-india-defense",   "India Defense",      "https://news.google.com/rss/search?q=India+defense+military+DRDO+HAL+border&hl=en-IN&gl=IN&ceid=IN:en",                           "Defense",   300, "#E53935"),
    FeedConfig("gn-geopolitics",     "Geopolitics",        "https://news.google.com/rss/search?q=geopolitics+diplomacy+sanctions+ceasefire&hl=en-US&gl=US&ceid=US:en",                        "Geopolitics",300,"#E53935"),
    FeedConfig("gn-ai",              "AI & Technology",    "https://news.google.com/rss/search?q=Artificial+Intelligence+OpenAI+ChatGPT+AI+Nvidia&hl=en-US&gl=US&ceid=US:en",                 "AI",        300, "#10B981"),
]

# ─────────────────────── Company maps ────────────────────────────────────────

# India: symbol → list of name aliases (from Newsdesk's companyMap.js)
_INDIA_COMPANY_MAP: dict[str, list[str]] = {
    "RELIANCE": ["Reliance", "Reliance Industries", "RIL", "Mukesh Ambani"],
    "TCS": ["TCS", "Tata Consultancy", "Tata Consultancy Services"],
    "HDFCBANK": ["HDFC Bank"],
    "INFY": ["Infosys", "INFY"],
    "ICICIBANK": ["ICICI Bank", "ICICI"],
    "HINDUNILVR": ["Hindustan Unilever", "HUL"],
    "ITC": ["ITC Ltd", "ITC Limited"],
    "SBIN": ["SBI", "State Bank", "State Bank of India"],
    "BHARTIARTL": ["Bharti Airtel", "Airtel"],
    "KOTAKBANK": ["Kotak Mahindra", "Kotak Bank"],
    "LT": ["Larsen & Toubro", "L&T"],
    "HCLTECH": ["HCL Tech", "HCL Technologies"],
    "AXISBANK": ["Axis Bank"],
    "ASIANPAINT": ["Asian Paints"],
    "MARUTI": ["Maruti Suzuki", "Maruti"],
    "SUNPHARMA": ["Sun Pharma", "Sun Pharmaceutical"],
    "TITAN": ["Titan", "Titan Company"],
    "BAJFINANCE": ["Bajaj Finance"],
    "BAJFINSV": ["Bajaj Finserv"],
    "WIPRO": ["Wipro"],
    "DMART": ["D-Mart", "Avenue Supermarts", "DMart"],
    "NESTLEIND": ["Nestle India", "Nestle"],
    "ULTRACEMCO": ["UltraTech Cement", "UltraTech"],
    "TATAMOTORS": ["Tata Motors"],
    "TATASTEEL": ["Tata Steel"],
    "NTPC": ["NTPC Ltd"],
    "POWERGRID": ["Power Grid", "Power Grid Corporation"],
    "ONGC": ["ONGC", "Oil and Natural Gas"],
    "M&M": ["Mahindra & Mahindra", "M&M", "Mahindra"],
    "JSWSTEEL": ["JSW Steel"],
    "ADANIENT": ["Adani Enterprises", "Adani"],
    "ADANIPORTS": ["Adani Ports"],
    "ADANIPOWER": ["Adani Power"],
    "ADANIGREEN": ["Adani Green Energy", "Adani Green"],
    "COALINDIA": ["Coal India"],
    "BPCL": ["BPCL", "Bharat Petroleum"],
    "IOC": ["Indian Oil", "IOC", "Indian Oil Corporation"],
    "GRASIM": ["Grasim Industries"],
    "TECHM": ["Tech Mahindra"],
    "INDUSINDBK": ["IndusInd Bank"],
    "CIPLA": ["Cipla"],
    "DRREDDY": ["Dr Reddy", "Dr. Reddy's", "Dr Reddys"],
    "EICHERMOT": ["Eicher Motors", "Royal Enfield"],
    "DIVISLAB": ["Divi's Lab", "Divis Laboratories"],
    "APOLLOHOSP": ["Apollo Hospitals", "Apollo"],
    "HEROMOTOCO": ["Hero MotoCorp", "Hero Moto"],
    "BAJAJ-AUTO": ["Bajaj Auto"],
    "BRITANNIA": ["Britannia Industries"],
    "TATACONSUM": ["Tata Consumer", "Tata Consumer Products"],
    "SBILIFE": ["SBI Life", "SBI Life Insurance"],
    "HDFCLIFE": ["HDFC Life"],
    "VEDL": ["Vedanta", "Vedanta Ltd"],
    "HINDALCO": ["Hindalco", "Hindalco Industries"],
    "TRENT": ["Trent Limited", "Zudio"],
    "ETERNAL": ["Eternal", "Zomato"],
    "ZOMATO": ["Zomato"],
    "PAYTM": ["Paytm", "One97"],
    "NYKAA": ["Nykaa", "FSN E-Commerce"],
    "HAL": ["HAL", "Hindustan Aeronautics"],
    "BEL": ["BEL", "Bharat Electronics"],
    "DLF": ["DLF Ltd"],
    "TATAPOWER": ["Tata Power"],
    "IRCON": ["IRCON", "Ircon International"],
    "RVNL": ["RVNL", "Rail Vikas Nigam"],
    "DABUR": ["Dabur"],
    "MARICO": ["Marico"],
    "COLPAL": ["Colgate-Palmolive", "Colgate"],
    "JUBLFOOD": ["Jubilant FoodWorks", "Jubilant"],
    "BANKBARODA": ["Bank of Baroda"],
    "PNB": ["Punjab National Bank", "PNB"],
    "LUPIN": ["Lupin"],
    "BIOCON": ["Biocon"],
    "AUROPHARMA": ["Aurobindo Pharma"],
    "TORNTPHARM": ["Torrent Pharma"],
    "LTIM": ["LTIMindtree", "LTI Mindtree"],
    "PERSISTENT": ["Persistent Systems"],
    "COFORGE": ["Coforge"],
    "MPHASIS": ["Mphasis"],
    "TVSMOTOR": ["TVS Motor"],
    "ASHOKLEY": ["Ashok Leyland"],
    "MOTHERSON": ["Motherson", "Samvardhana Motherson"],
    "NHPC": ["NHPC Ltd"],
    "NMDC": ["NMDC Ltd"],
    "KALYANJWLR": ["Kalyan Jewellers"],
}

# ─────────────────────── Article dataclass ───────────────────────────────────

@dataclass
class RssArticle:
    id: str
    title: str
    description: str
    link: str
    pub_date: str          # ISO 8601
    image: str | None
    category: str
    companies: list[str]   # matched symbols
    source_id: str
    source_name: str
    source_color: str

# ─────────────────────── Per-market service ──────────────────────────────────

class RssNewsService:
    """Fetch, parse, cache and categorise RSS feeds for one market."""

    def __init__(self, market: str) -> None:
        self.market = "india"
        self._feeds: list[FeedConfig] = _INDIA_FEEDS
        self._company_map: dict[str, list[str]] = _INDIA_COMPANY_MAP
        # cache: feed_id → (timestamp, list[RssArticle])
        self._cache: dict[str, tuple[float, list[RssArticle]]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_all_news(self, limit: int = 200) -> list[dict]:
        """Return all recent articles, newest first."""
        articles = await self._fetch_all()
        articles.sort(key=lambda a: a.pub_date, reverse=True)
        return [self._to_dict(a) for a in articles[:limit]]

    async def get_company_news(self, symbol: str, limit: int = 30) -> list[dict]:
        """Return articles mentioning a specific symbol."""
        symbol = symbol.upper()
        articles = await self._fetch_all()
        matched = [a for a in articles if symbol in a.companies]
        matched.sort(key=lambda a: a.pub_date, reverse=True)
        return [self._to_dict(a) for a in matched[:limit]]

    # ── Feed fetching ─────────────────────────────────────────────────────────

    async def _fetch_all(self) -> list[RssArticle]:
        # One shared client + semaphore for all feeds so we don't spawn 16
        # separate connection pools simultaneously (kills HF-Space networking).
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(18.0),
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=8),
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Newsdesk/2.0; +https://github.com/MrChartist)",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
        ) as client:
            sem = asyncio.Semaphore(6)  # max 6 concurrent feed fetches
            tasks = [self._fetch_feed_async(feed, client, sem) for feed in self._feeds]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        articles: list[RssArticle] = []
        seen_links: set[str] = set()
        for result in results:
            if isinstance(result, Exception):
                continue
            for article in result:
                if article.link not in seen_links:
                    seen_links.add(article.link)
                    articles.append(article)
        return articles

    async def _fetch_feed_async(self, feed: FeedConfig, client: httpx.AsyncClient | None = None, sem: asyncio.Semaphore | None = None) -> list[RssArticle]:
        now = time.time()
        cached_ts, cached_items = self._cache.get(feed.id, (0, []))
        if cached_items and now - cached_ts < feed.ttl:
            return cached_items

        async def _do_fetch(c: httpx.AsyncClient) -> bytes:
            resp = await c.get(feed.url)
            resp.raise_for_status()
            return resp.content[:800_000]

        try:
            if client is not None:
                # Use shared client (headers already set on client)
                if sem is not None:
                    async with sem:
                        xml_bytes = await _do_fetch(client)
                else:
                    xml_bytes = await _do_fetch(client)
            else:
                # Standalone call (e.g. company news for a single feed)
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(15.0),
                    follow_redirects=True,
                    verify=False,
                    headers={
                        "User-Agent": "Mozilla/5.0 (compatible; Newsdesk/2.0)",
                        "Accept": "application/rss+xml, application/xml, text/xml, */*",
                    },
                ) as solo_client:
                    xml_bytes = await _do_fetch(solo_client)
        except Exception as exc:
            logger.warning("Feed %s fetch error (%s): %s", feed.id, type(exc).__name__, exc)
            return cached_items  # serve stale on error

        items = self._parse_feed(xml_bytes, feed)
        self._cache[feed.id] = (now, items)
        return items

    # ── XML parsing ───────────────────────────────────────────────────────────

    _NS = {
        "media": "http://search.yahoo.com/mrss/",
        "dc":    "http://purl.org/dc/elements/1.1/",
    }

    def _parse_feed(self, xml_bytes: bytes, feed: FeedConfig) -> list[RssArticle]:
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            # Some feeds emit invalid XML — try with replacement
            try:
                root = ET.fromstring(xml_bytes.decode("utf-8", errors="replace").encode("utf-8"))
            except Exception:
                return []

        items: list[RssArticle] = []
        channel = root.find("channel")
        if channel is None:
            # Atom feeds put entries at root level
            entries = root.findall("{http://www.w3.org/2005/Atom}entry")
            for entry in entries[:40]:
                article = self._parse_atom_entry(entry, feed)
                if article:
                    items.append(article)
            return items

        for item in channel.findall("item")[:40]:
            article = self._parse_rss_item(item, feed)
            if article:
                items.append(article)
        return items

    def _parse_rss_item(self, item: ET.Element, feed: FeedConfig) -> RssArticle | None:
        _t = item.find("title"); _t = item.find("dc:title", self._NS) if _t is None else _t
        title = self._text(_t) or ""
        link  = self._text(item.find("link")) or self._text(item.find("guid")) or ""
        desc  = self._text(item.find("description")) or ""
        _p = item.find("pubDate"); _p = item.find("dc:date", self._NS) if _p is None else _p
        pub   = self._text(_p) or ""

        if not title.strip() or not link.strip():
            return None

        title = self._clean_title(unescape(title))
        desc  = self._strip_html(unescape(desc))[:300]
        link  = link.strip()
        image = self._extract_image(item)
        pub_iso = self._parse_date(pub)

        category = self._extract_category(title, link, feed.category)
        companies = self._match_companies(title + " " + desc)
        art_id = hashlib.md5(link.encode()).hexdigest()[:16]

        return RssArticle(
            id=art_id, title=title, description=desc, link=link,
            pub_date=pub_iso, image=image, category=category,
            companies=companies, source_id=feed.id,
            source_name=feed.name, source_color=feed.color,
        )

    def _parse_atom_entry(self, entry: ET.Element, feed: FeedConfig) -> RssArticle | None:
        ns = "http://www.w3.org/2005/Atom"
        title = entry.findtext(f"{{{ns}}}title") or ""
        link_el = entry.find(f"{{{ns}}}link")
        link = (link_el.get("href") or "") if link_el is not None else ""
        desc = entry.findtext(f"{{{ns}}}summary") or entry.findtext(f"{{{ns}}}content") or ""
        pub  = entry.findtext(f"{{{ns}}}published") or entry.findtext(f"{{{ns}}}updated") or ""
        if not title.strip() or not link.strip():
            return None
        title = self._clean_title(unescape(title))
        desc  = self._strip_html(unescape(desc))[:300]
        pub_iso = self._parse_date(pub)
        category = self._extract_category(title, link, feed.category)
        companies = self._match_companies(title + " " + desc)
        art_id = hashlib.md5(link.encode()).hexdigest()[:16]
        return RssArticle(
            id=art_id, title=title, description=desc, link=link,
            pub_date=pub_iso, image=None, category=category,
            companies=companies, source_id=feed.id,
            source_name=feed.name, source_color=feed.color,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _text(el: ET.Element | None) -> str | None:
        if el is None:
            return None
        if el.text:
            return el.text.strip()
        # CDATA is exposed as text by ElementTree
        return None

    def _extract_image(self, item: ET.Element) -> str | None:
        for ns_url, tag in [
            ("http://search.yahoo.com/mrss/", "content"),
            ("http://search.yahoo.com/mrss/", "thumbnail"),
        ]:
            el = item.find(f"{{{ns_url}}}{tag}")
            if el is not None:
                url = el.get("url") or el.get("href")
                if url:
                    return url
        enc = item.find("enclosure")
        if enc is not None:
            url = enc.get("url", "")
            if url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                return url
        return None

    @staticmethod
    def _parse_date(date_str: str) -> str:
        if not date_str:
            return datetime.now(timezone.utc).isoformat()
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
        # ISO 8601 fallback
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str[:25], fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                continue
        return datetime.now(timezone.utc).isoformat()

    _HTML_TAG_RE = re.compile(r"<[^>]+>")
    _SPACES_RE   = re.compile(r"\s{2,}")

    @classmethod
    def _strip_html(cls, text: str) -> str:
        text = cls._HTML_TAG_RE.sub(" ", text)
        return cls._SPACES_RE.sub(" ", text).strip()

    _GOOGLE_NEWS_SUFFIX = re.compile(r"\s*-\s*[A-Z][A-Za-z\s&'.,]+$")
    _BIZTOC_HASHTAG     = re.compile(r"#\w+\s*$")

    @classmethod
    def _clean_title(cls, title: str) -> str:
        title = cls._BIZTOC_HASHTAG.sub("", title).strip()
        title = cls._GOOGLE_NEWS_SUFFIX.sub("", title).strip()
        return title

    # ── Category detection ────────────────────────────────────────────────────

    _GEO = {
        "MiddleEast":  re.compile(r"\b(iran|israel|lebanon|hezbollah|hamas|gaza|syria|iraq|yemen|houthi|saudi|uae|tehran|netanyahu)\b", re.I),
        "Defense":     re.compile(r"\b(military|defense|defence|missile|drone\s?strike|nato|pentagon|army|navy|nuclear|drdo|hal|rafale)\b", re.I),
        "Geopolitics": re.compile(r"\b(geopolit|sanctions|ceasefire|diplomacy|negotiations|embargo|treaty|g7|g20|brics|invasion|occupation)\b", re.I),
        "Crypto":      re.compile(r"\b(bitcoin|btc|ethereum|crypto|blockchain|defi|nft|binance|coinbase)\b", re.I),
        "IPO":         re.compile(r"\b(ipo|initial public offering|listing|grey\s?market|gmp)\b", re.I),
        "AI":          re.compile(r"\b(artificial intelligence|openai|chatgpt|llm|generative ai|nvidia|anthropic|gemini|claude)\b", re.I),
    }
    _URL_CATS: list[tuple[str, str]] = [
        ("market/stock-market", "Stocks"),
        ("market/ipo",          "IPO"),
        ("market/commodit",     "Commodities"),
        ("market/cryptocurrency","Crypto"),
        ("market/forex",        "Forex"),
        ("/companies",          "Corporate"),
        ("/money",              "Money"),
        ("/economy",            "Economy"),
        ("/defense",            "Defense"),
        ("/defence",            "Defense"),
        ("/politics",           "Geopolitics"),
    ]

    def _extract_category(self, title: str, link: str, feed_cat: str) -> str:
        for cat, pattern in self._GEO.items():
            if pattern.search(title):
                return cat
        link_lower = link.lower()
        for fragment, cat in self._URL_CATS:
            if fragment in link_lower:
                return cat
        return feed_cat or "General"

    # ── Company matching ─────────────────────────────────────────────────────

    def _match_companies(self, text: str) -> list[str]:
        matched: set[str] = set()
        for symbol, aliases in self._company_map.items():
            for alias in aliases:
                # Word-boundary regex per alias
                escaped = re.escape(alias)
                if re.search(rf"\b{escaped}\b", text, re.I):
                    matched.add(symbol)
                    break
        return sorted(matched)

    # ── Serialise ────────────────────────────────────────────────────────────

    @staticmethod
    def _to_dict(a: RssArticle) -> dict:
        return {
            "id":           a.id,
            "title":        a.title,
            "description":  a.description,
            "link":         a.link,
            "pub_date":     a.pub_date,
            "image":        a.image,
            "category":     a.category,
            "companies":    a.companies,
            "source": {
                "id":    a.source_id,
                "name":  a.source_name,
                "color": a.source_color,
            },
        }


# ── Singleton registry (one per market) ─────────────────────────────────────

_services: dict[str, RssNewsService] = {}

def get_rss_service(market: str) -> RssNewsService:
    key = market.lower()
    if key not in _services:
        _services[key] = RssNewsService(key)
    return _services[key]
