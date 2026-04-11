import re
from typing import Any, Literal
from urllib.parse import urlparse
from app.models.market import DetailedNews

# Core categories
NewsClassification = Literal[
    "editorial_news", "company_release", "exchange_filing",
    "regulatory_filing", "transcript", "investor_presentation",
    "duplicate", "low_quality", "rumor"
]

# ── HARD DENY LIST ────────────────────────────────────────────────────────────
# Any item whose domain matches (ends with) one of these CANNOT appear in Latest News.
# These are PR wire services, exchange portals, and official company IR domains.
OFFICIAL_DOMAIN_DENYLIST: frozenset[str] = frozenset({
    # PR wire services
    "prnewswire.com", "businesswire.com", "globenewswire.com",
    "accesswire.com", "newsvoir.com", "prnewswire.in",
    # Exchange / regulatory portals
    "sec.gov", "nseindia.com", "bseindia.com", "sebi.gov.in",
    "edgar-online.com", "efts.sec.gov", "irdirect.net",
    "filing.com", "cmlviz.com",
    # Generic IR / newsroom subdomains that could match any company
    # (handled separately via subdomain pattern check below)
})

# Subdomain prefixes that indicate official IR / newsroom pages
_IR_SUBDOMAIN_RE = re.compile(
    r"^(?:investor|investors|ir|newsroom|media|press|corporate|annualreport|reports)\.",
    re.IGNORECASE,
)

# ── ALLOW LIST (editorial sources) ───────────────────────────────────────────
# Domain must END WITH one of these to be classified as editorial_news.
EDITORIAL_DOMAIN_ALLOWLIST: frozenset[str] = frozenset({
    # US / Global
    "reuters.com", "bloomberg.com", "wsj.com", "ft.com", "cnbc.com",
    "marketwatch.com", "barrons.com", "finance.yahoo.com", "yahoo.com",
    "seekingalpha.com", "thestreet.com", "investopedia.com",
    "motleyfool.com", "benzinga.com", "zacks.com", "fool.com",
    "ap.org", "apnews.com", "nytimes.com", "washingtonpost.com",
    "fortune.com", "forbes.com", "businessinsider.com",
    "techcrunch.com", "venturebeat.com",
    # India
    "economictimes.indiatimes.com", "moneycontrol.com", "livemint.com",
    "business-standard.com", "ndtvprofit.com", "thehindubusinessline.com",
    "financialexpress.com", "thehindu.com", "cnbctv18.com",
    "zeebiz.com", "bloombergquint.com", "capitalmarket.com",
    "investing.com",
})

# ── OFFICIAL HEADLINE PATTERNS ────────────────────────────────────────────────
# Titles matching these patterns are company_release UNLESS from an editorial domain.
_OFFICIAL_HEADLINE_PATTERNS: tuple[re.Pattern, ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^\s*(?:the\s+)?company\s+(?:announces|reports|declares|receives|to\s+consider|files)",
        r"^\s*board\s+(?:of\s+directors\s+)?(?:approves|declares|announces)",
        r"^\s*press\s+release\b",
        r"^\s*sebi\s+disclosure",
        r"^\s*exchange\s+filing",
        r"^\s*notice\s+of\s+(?:annual|extraordinary|special)\s+general\s+meeting",
        r"^\s*outcome\s+of\s+(?:board|agm|egm)\s+meeting",
        r"^\s*(?:audited|unaudited)\s+(?:financial|standalone|consolidated)\s+results",
        r"^\s*(?:dividend|bonus\s+issue|rights\s+issue|stock\s+split)\s+(?:declared|announced|approved)",
        r"^\s*regulation\s+\d+\s+of\s+sebi",
        r"^\s*disclosure\s+under\s+(?:sebi|lodr|sast)",
    ]
)

# ── IMPACT KEYWORDS for relevance scoring ────────────────────────────────────
_HIGH_IMPACT_WORDS: frozenset[str] = frozenset({
    "revenue", "sales", "profit", "margin", "ebitda", "earnings", "eps",
    "guidance", "outlook", "order", "contract", "deal", "approval",
    "capacity", "capex", "expansion", "acquisition", "merger",
    "commissioning", "launch", "pricing", "market share", "bookings",
    "pipeline", "operating leverage", "roce", "roe", "free cash flow",
    "subscriber", "user growth", "store", "branch",
})


def _domain_in_denylist(domain: str) -> bool:
    """Return True if domain ends with any entry in the official denylist."""
    if not domain:
        return False
    for entry in OFFICIAL_DOMAIN_DENYLIST:
        if domain == entry or domain.endswith("." + entry):
            return True
    return False


def _domain_in_allowlist(domain: str) -> bool:
    """Return True if domain ends with any entry in the editorial allowlist."""
    if not domain:
        return False
    for entry in EDITORIAL_DOMAIN_ALLOWLIST:
        if domain == entry or domain.endswith("." + entry):
            return True
    return False


class NewsPipeline:
    """Hard-rule news processing pipeline: classify → dedupe → split → score."""

    # ── Public helpers ────────────────────────────────────────────────────────

    @staticmethod
    def get_domain(url: str | None) -> str:
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().strip()
            if domain.startswith("www."):
                domain = domain[4:]
            return domain
        except Exception:
            return ""

    @staticmethod
    def is_editorial_source(domain: str) -> bool:
        """Hard allow-list check: is this domain a credible editorial outlet?"""
        return _domain_in_allowlist(domain)

    @staticmethod
    def is_official_source(domain: str) -> bool:
        """Hard deny-list check: is this domain a PR/IR/filing source?"""
        return _domain_in_denylist(domain) or bool(_IR_SUBDOMAIN_RE.match(domain))

    @staticmethod
    def classify_item(item: dict[str, Any]) -> NewsClassification:
        """Classify a raw news item. Hard rules take priority over soft signals.

        Priority order:
          1. Exchange filing domains → exchange_filing
          2. PR wire / filing / IR deny-list domains → company_release
          3. IR subdomain pattern → company_release
          4. Official headline patterns (when not from editorial domain) → company_release
          5. Editorial allow-list domain → editorial_news
          6. Explicit source_type hint from upstream AI → respect it
          7. Unknown sources → low_quality (never bleeds into Latest News)
        """
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "")
        domain = NewsPipeline.get_domain(url)
        source = str(item.get("source") or "").strip().lower()
        source_type_hint = str(item.get("source_type") or "").lower()

        # ── Step 1: Exchange filing portals (hardest rule) ────────────────────
        if domain and ("nseindia.com" in domain or "bseindia.com" in domain or "sec.gov" in domain
                       or "sebi.gov.in" in domain):
            return "exchange_filing"
        if "exchange filing" in source_type_hint or "regulatory" in source_type_hint:
            return "exchange_filing"
        if "sebi" in source or "exchange" in source and "filing" in source:
            return "exchange_filing"

        # ── Step 2: PR wire / official domain hard deny-list ─────────────────
        if domain and _domain_in_denylist(domain):
            # Distinguish PR releases from formal exchange filings
            if "sec.gov" in domain or "bseindia.com" in domain or "nseindia.com" in domain:
                return "exchange_filing"
            return "company_release"

        # ── Step 3: IR subdomain pattern (investor.X, ir.X, newsroom.X, …) ───
        if domain and _IR_SUBDOMAIN_RE.match(domain):
            return "company_release"
        # Generic domain-keyword heuristic (e.g. "acmecorp-investor-relations.com")
        if domain and ("investor-relations" in domain or ".ir." in domain):
            return "company_release"

        # ── Step 4: Transcript / presentation labels ──────────────────────────
        if "transcript" in source_type_hint:
            return "transcript"
        if "investor presentation" in source_type_hint or "presentation" in source_type_hint:
            return "investor_presentation"

        # ── Step 5: Official headline patterns ───────────────────────────────
        if title:
            for pattern in _OFFICIAL_HEADLINE_PATTERNS:
                if pattern.search(title):
                    # Exception: editorial outlet independently reporting on an event
                    if _domain_in_allowlist(domain):
                        return "editorial_news"
                    return "company_release"

        # ── Step 6: Editorial allow-list ─────────────────────────────────────
        if domain and _domain_in_allowlist(domain):
            return "editorial_news"

        # ── Step 7: source_type hint from upstream AI ────────────────────────
        if "editorial" in source_type_hint:
            return "editorial_news"
        if "company release" in source_type_hint or "press release" in source_type_hint:
            return "company_release"

        # ── Step 8: Default — unknown sources are low_quality, not editorial ──
        # This is the critical safety rule: unknown domains NEVER appear in Latest News.
        return "low_quality"

    @staticmethod
    def is_editorial(classification: NewsClassification) -> bool:
        return classification == "editorial_news"

    @staticmethod
    def score_news_relevance(item: dict[str, Any], company_context: dict[str, Any] | None = None) -> float:
        """Compute a 0–1 relevance score for a news item.

        Factors:
        - Presence of high-impact financial keywords in title/summary
        - Source credibility (editorial > low_quality)
        - Explicit relevance_score hint from upstream
        """
        title = str(item.get("title") or "").lower()
        summary = str(item.get("summary") or "").lower()
        text = f"{title} {summary}"

        # Keyword hit score (0–0.5)
        hits = sum(1 for kw in _HIGH_IMPACT_WORDS if kw in text)
        keyword_score = min(hits / max(len(_HIGH_IMPACT_WORDS) * 0.15, 1), 0.5)

        # Source credibility (0–0.3)
        domain = NewsPipeline.get_domain(item.get("url"))
        if _domain_in_allowlist(domain):
            source_score = 0.3
        elif _domain_in_denylist(domain):
            source_score = 0.0
        else:
            source_score = 0.1

        # Upstream hint (0–0.2)
        upstream = float(item.get("relevance_score") or 0.5)
        upstream_score = min(upstream * 0.2, 0.2)

        return round(min(keyword_score + source_score + upstream_score, 1.0), 3)

    @staticmethod
    def dedupe_news(items: list[DetailedNews]) -> list[DetailedNews]:
        """Deduplicate by near-identical title overlap (>85 % word Jaccard similarity).

        Always keeps the item with the highest relevance_score when duplicates exist.
        O(n·m) but acceptable for typical news feed sizes (< 50 items per call).
        """
        sorted_items = sorted(
            items,
            key=lambda x: (x.relevance_score, x.published_date or ""),
            reverse=True,
        )
        unique: list[DetailedNews] = []
        seen_word_sets: list[frozenset[str]] = []

        for item in sorted_items:
            norm = re.sub(r"[^\w\s]", "", item.title.lower()).strip()
            words = frozenset(norm.split())
            if not words:
                continue
            is_dup = False
            for existing_words in seen_word_sets:
                union = words | existing_words
                if not union:
                    continue
                jaccard = len(words & existing_words) / len(union)
                if jaccard >= 0.85:
                    is_dup = True
                    break
            if not is_dup:
                seen_word_sets.append(words)
                unique.append(item)

        return unique

    @staticmethod
    def process_and_split(
        raw_items: list[dict[str, Any]],
    ) -> tuple[list[DetailedNews], list[DetailedNews]]:
        """Classify, normalise, deduplicate, and split into:
        - editorial list (Latest News tab)
        - official list (Official Updates tab)

        Items classified as low_quality, duplicate, or rumor go to official tab
        so they never pollute Latest News.
        """
        editorial: list[DetailedNews] = []
        official: list[DetailedNews] = []

        for raw in raw_items:
            cls = NewsPipeline.classify_item(raw)
            domain = NewsPipeline.get_domain(raw.get("url"))
            rel_score = NewsPipeline.score_news_relevance(raw)

            news_item = DetailedNews(
                title=str(raw.get("title") or ""),
                summary=str(raw.get("summary") or raw.get("snippet") or ""),
                impact_category=str(raw.get("impact_category") or "market"),
                sentiment=str(raw.get("sentiment") or "neutral"),  # type: ignore[arg-type]
                source=str(raw.get("source") or "Unknown"),
                domain=domain or None,
                classification=cls,
                is_editorial=NewsPipeline.is_editorial(cls),
                url=raw.get("url"),
                published_date=raw.get("published_date") or raw.get("published_at"),
                relevance_score=rel_score,
                impact_area=raw.get("impact_area"),
                why_it_matters=raw.get("why_it_matters"),
                detailed_points=raw.get("detailed_points") or [],
                impact_tags=raw.get("impact_tags") or [],
                connection_to_guidance=raw.get("connection_to_guidance"),
            )

            if news_item.is_editorial:
                editorial.append(news_item)
            else:
                official.append(news_item)

        return NewsPipeline.dedupe_news(editorial), NewsPipeline.dedupe_news(official)
