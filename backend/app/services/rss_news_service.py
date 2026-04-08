"""
RSS News Service — Python port of Newsdesk (https://github.com/MrChartist/Newsdesk)

Fetches, parses, caches and categorises articles from 30+ premium RSS feeds.
Includes company→symbol matching for NSE/BSE (India) and US stocks.
No external dependencies beyond the Python stdlib + xml.etree (both included).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any

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
    FeedConfig("cnbc-economy",       "CNBC Economy",       "https://www.cnbc.com/id/20910255/device/rss/rss.html",                                                                             "Economy",   300, "#005994"),
    FeedConfig("etcfo-top",          "ET CFO",             "https://cfo.economictimes.indiatimes.com/rss/topstories",                                                                           "CFO",       300, "#0F766E"),
    FeedConfig("gn-bloomberg",       "Bloomberg",          "https://news.google.com/rss/search?q=when:1d+source:%22Bloomberg%22&hl=en-US&gl=US&ceid=US:en",                                    "Markets",   300, "#F59E0B"),
    FeedConfig("gn-reuters",         "Reuters",            "https://news.google.com/rss/search?q=when:1d+source:%22Reuters%22+India+stock+market&hl=en-IN&gl=IN&ceid=IN:en",                   "Global",    300, "#F97316"),
    FeedConfig("zerohedge",          "ZeroHedge",          "https://feeds.feedburner.com/zerohedge/feed",                                                                                       "Markets",   300, "#D97706"),
    FeedConfig("gn-india-defense",   "India Defense",      "https://news.google.com/rss/search?q=India+defense+military+DRDO+HAL+border&hl=en-IN&gl=IN&ceid=IN:en",                           "Defense",   300, "#E53935"),
    FeedConfig("gn-geopolitics",     "Geopolitics",        "https://news.google.com/rss/search?q=geopolitics+diplomacy+sanctions+ceasefire&hl=en-US&gl=US&ceid=US:en",                        "Geopolitics",300,"#E53935"),
    FeedConfig("gn-ai",              "AI & Technology",    "https://news.google.com/rss/search?q=Artificial+Intelligence+OpenAI+ChatGPT+AI+Nvidia&hl=en-US&gl=US&ceid=US:en",                 "AI",        300, "#10B981"),
]

_US_FEEDS: list[FeedConfig] = [
    FeedConfig("cnbc-business",      "CNBC Business",      "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",                                               "Business",  120, "#005994"),
    FeedConfig("cnbc-world",         "CNBC World",         "https://www.cnbc.com/id/100727362/device/rss/rss.html",                                                                            "World",     300, "#005994"),
    FeedConfig("cnbc-economy",       "CNBC Economy",       "https://www.cnbc.com/id/20910255/device/rss/rss.html",                                                                             "Economy",   300, "#005994"),
    FeedConfig("seeking-alpha",      "Seeking Alpha",      "https://seekingalpha.com/market_currents.xml",                                                                                      "Stocks",    300, "#EA580C"),
    FeedConfig("gn-bloomberg",       "Bloomberg",          "https://news.google.com/rss/search?q=when:1d+source:%22Bloomberg%22&hl=en-US&gl=US&ceid=US:en",                                    "Markets",   300, "#F59E0B"),
    FeedConfig("gn-reuters-us",      "Reuters US",         "https://news.google.com/rss/search?q=when:1d+source:%22Reuters%22+US+stock+market&hl=en-US&gl=US&ceid=US:en",                     "Global",    300, "#F97316"),
    FeedConfig("gn-wsj",             "Wall Street Journal","https://news.google.com/rss/search?q=when:1d+source:%22The+Wall+Street+Journal%22&hl=en-US&gl=US&ceid=US:en",                     "Business",  300, "#06B6D4"),
    FeedConfig("gn-ft",              "Financial Times",    "https://news.google.com/rss/search?q=when:1d+source:%22Financial+Times%22&hl=en-US&gl=US&ceid=US:en",                             "Economy",   300, "#F472B6"),
    FeedConfig("zerohedge",          "ZeroHedge",          "https://feeds.feedburner.com/zerohedge/feed",                                                                                       "Markets",   300, "#D97706"),
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

# US: symbol → list of name aliases
_US_COMPANY_MAP: dict[str, list[str]] = {
    "AAPL": ["Apple", "Apple Inc"],
    "MSFT": ["Microsoft"],
    "NVDA": ["Nvidia", "NVDA"],
    "GOOGL": ["Google", "Alphabet"],
    "AMZN": ["Amazon"],
    "META": ["Meta", "Facebook"],
    "TSLA": ["Tesla"],
    "NFLX": ["Netflix"],
    "AMD": ["AMD", "Advanced Micro Devices"],
    "INTC": ["Intel"],
    "QCOM": ["Qualcomm"],
    "AVGO": ["Broadcom"],
    "TSM": ["TSMC", "Taiwan Semiconductor"],
    "JPM": ["JPMorgan", "JP Morgan"],
    "BAC": ["Bank of America"],
    "GS": ["Goldman Sachs"],
    "MS": ["Morgan Stanley"],
    "V": ["Visa"],
    "MA": ["Mastercard"],
    "WMT": ["Walmart"],
    "XOM": ["ExxonMobil", "Exxon"],
    "CVX": ["Chevron"],
    "JNJ": ["Johnson & Johnson"],
    "PFE": ["Pfizer"],
    "ABBV": ["AbbVie"],
    "UNH": ["UnitedHealth"],
    "LLY": ["Eli Lilly"],
    "BRK-B": ["Berkshire Hathaway", "Warren Buffett"],
    "COST": ["Costco"],
    "HD": ["Home Depot"],
    "DIS": ["Disney", "Walt Disney"],
    "PYPL": ["PayPal"],
    "CRM": ["Salesforce"],
    "ORCL": ["Oracle"],
    "IBM": ["IBM"],
    "UBER": ["Uber"],
    "ABNB": ["Airbnb"],
    "COIN": ["Coinbase"],
    "PLTR": ["Palantir"],
    "ARM": ["Arm Holdings"],
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
        self.market = market.lower()
        self._feeds: list[FeedConfig] = _INDIA_FEEDS if self.market == "india" else _US_FEEDS
        self._company_map: dict[str, list[str]] = _INDIA_COMPANY_MAP if self.market == "india" else _US_COMPANY_MAP
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
        tasks = [asyncio.to_thread(self._fetch_feed, feed) for feed in self._feeds]
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

    def _fetch_feed(self, feed: FeedConfig) -> list[RssArticle]:
        now = time.time()
        cached_ts, cached_items = self._cache.get(feed.id, (0, []))
        if cached_items and now - cached_ts < feed.ttl:
            return cached_items

        try:
            req = urllib.request.Request(
                feed.url,
                headers={
                    "User-Agent": "Newsdesk/2.0 (MrChartist Stock Scanner)",
                    "Accept": "application/rss+xml, application/xml, text/xml",
                },
            )
            with urllib.request.urlopen(req, timeout=12) as resp:
                xml_bytes = resp.read(800_000)
        except Exception as exc:
            logger.debug("Feed %s fetch error: %s", feed.id, exc)
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
        title = self._text(item.find("title") or item.find("dc:title", self._NS)) or ""
        link  = self._text(item.find("link")) or self._text(item.find("guid")) or ""
        desc  = self._text(item.find("description")) or ""
        pub   = self._text(item.find("pubDate")) or self._text(item.find("dc:date", self._NS)) or ""

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
