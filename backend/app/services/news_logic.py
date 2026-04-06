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

# Hard rules for official source domains
OFFICIAL_DOMAINS = {
    "prnewswire.com", "businesswire.com", "globenewswire.com",
    "accesswire.com", "newsvoir.com", "sec.gov", "nseindia.com",
    "bseindia.com", "edgar-online.com", "filing.com", "investor-relations"
}

# Hard rules for high-quality editorial sources
EDITORIAL_DOMAINS = {
    "reuters.com", "bloomberg.com", "wsj.com", "ft.com", "cnbc.com",
    "marketwatch.com", "barrons.com", "finance.yahoo.com", "seekingalpha.com",
    "economictimes.indiatimes.com", "moneycontrol.com", "livemint.com",
    "business-standard.com", "ndtvprofit.com", "thehindubusinessline.com",
    "financialexpress.com", "investing.com"
}

# Subdomain patterns for IR
IR_SUBDOMAIN_PATTERNS = [r"^investor\.", r"^ir\.", r"^newsroom\.", r"^media\."]

# Headline patterns for official updates
OFFICIAL_HEADLINE_PATTERNS = [
    r"^company announces", r"^company reports", r"^company declares",
    r"^company to consider", r"^company receives order", r"^board approves",
    r"^press release", r"^sebi disclosure", r"^exchange filing"
]

class NewsPipeline:
    """Robust news processing pipeline for classification and deduplication."""

    @staticmethod
    def get_domain(url: str | None) -> str:
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]
            return domain
        except Exception:
            return ""

    @staticmethod
    def classify_item(item: dict[str, Any]) -> NewsClassification:
        title = str(item.get("title", "")).lower()
        url = str(item.get("url", "") or "")
        domain = NewsPipeline.get_domain(url)
        source = str(item.get("source", "")).lower()

        # 1. Check for Exchange Filings (Highest Priority)
        if "nseindia.com" in domain or "bseindia.com" in domain or "sec.gov" in domain:
            return "exchange_filing"
        if "filing" in source or "sebi" in source:
            return "exchange_filing"

        # 2. Check for PR Networks
        if any(d in domain for d in OFFICIAL_DOMAINS if "news" in d or "wire" in d):
            return "company_release"

        # 3. Check for IR Subdomains and Media Rooms
        if any(re.search(p, domain) for p in IR_SUBDOMAIN_PATTERNS):
            return "company_release"
        if "investor" in domain or "ir." in domain:
            return "company_release"

        # 4. Headline Pattern matching
        if any(re.search(p, title) for p in OFFICIAL_HEADLINE_PATTERNS):
            # Exception: If it's a known editorial source reporting on a release, we still consider it news
            if domain in EDITORIAL_DOMAINS:
                return "editorial_news"
            return "company_release"

        # 5. Editorial Source preference
        if domain in EDITORIAL_DOMAINS:
            return "editorial_news"

        # Default fallback
        return "editorial_news"

    @staticmethod
    def is_editorial(classification: NewsClassification) -> bool:
        return classification == "editorial_news"

    @staticmethod
    def dedupe_news(items: list[DetailedNews]) -> list[DetailedNews]:
        """Simple title-based deduplication."""
        unique_items = []
        seen_titles = set()
        
        # Sort by relevance and then date if possible
        sorted_items = sorted(items, key=lambda x: (x.relevance_score, x.published_date or ""), reverse=True)

        for item in sorted_items:
            # Normalize title for better matching (remove punctuation, lower case)
            norm_title = re.sub(r'[^\w\s]', '', item.title.lower()).strip()
            if norm_title in seen_titles:
                continue
            
            # Check for near matches (90% word overlap)
            is_near_dup = False
            words = set(norm_title.split())
            if not words:
                continue
                
            for existing in seen_titles:
                existing_words = set(existing.split())
                if not existing_words:
                    continue
                overlap = len(words.intersection(existing_words)) / max(len(words), len(existing_words))
                if overlap > 0.85:
                    is_near_dup = True
                    break
            
            if is_near_dup:
                continue
                
            seen_titles.add(norm_title)
            unique_items.append(item)
            
        return unique_items

    @staticmethod
    def process_and_split(raw_items: list[dict[str, Any]]) -> tuple[list[DetailedNews], list[DetailedNews]]:
        """Classify, normalize, and split into Editorial vs Official tabs."""
        editorial = []
        official = []
        
        for raw in raw_items:
            cls = NewsPipeline.classify_item(raw)
            domain = NewsPipeline.get_domain(raw.get("url"))
            
            # Normalize to DetailedNews model
            news_item = DetailedNews(
                title=raw.get("title", ""),
                summary=raw.get("summary", "") or raw.get("snippet", ""),
                impact_category=raw.get("impact_category", "market"),
                sentiment=raw.get("sentiment", "neutral"),
                source=raw.get("source", "Unknown"),
                domain=domain,
                classification=cls,
                is_editorial=NewsPipeline.is_editorial(cls),
                url=raw.get("url"),
                published_date=raw.get("published_date") or raw.get("published_at"),
                relevance_score=raw.get("relevance_score", 0.5),
                impact_tags=raw.get("impact_tags", [])
            )
            
            if news_item.is_editorial:
                editorial.append(news_item)
            else:
                official.append(news_item)
                
        # Dedupe within each list
        return NewsPipeline.dedupe_news(editorial), NewsPipeline.dedupe_news(official)
