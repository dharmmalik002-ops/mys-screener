import re
from typing import Literal

# Classification categories
SourceType = Literal["Editorial News", "Company Release", "Exchange Filing", "Transcript", "Analyst Report"]

# Known PR distribution networks and official subdomains
OFFICIAL_PATTERNS = [
    r"prnewswire\.com",
    r"businesswire\.com",
    r"globenewswire\.com",
    r"accesswire\.com",
    r"newsfilecorp\.com",
    r"marketwired\.com",
    r"investor\.+",
    r"ir\.+",
    r"company-announcements",
    r"press-release",
    r"regulatory-news",
    r"exchange-filing",
    r"sebi-filing",
    r"sec\.gov",
    r"bseindia\.com",
    r"nseindia\.com",
]

# Known editorial/journalism financial outlets
EDITORIAL_PATTERNS = [
    r"reuters\.com",
    r"bloomberg\.com",
    r"wsj\.com",
    r"ft\.com",
    r"cnbc\.com",
    r"barrons\.com",
    r"marketwatch\.com",
    r"livemint\.com",
    r"economictimes\.indiatimes\.com",
    r"moneycontrol\.com",
    r"thehindubusinessline\.com",
    r"business-standard\.com",
    r"investing\.com",
    r"financialexpress\.com",
    r"reuters\.co\.in",
]

class NewsProcessor:
    """Helper to classify and process news items based on source and content."""

    @staticmethod
    def classify_source(source_name: str, url: str | None = None) -> tuple[SourceType, bool]:
        """
        Classifies a news item into Editorial vs Official based on source name and URL.
        Returns (SourceType, is_editorial).
        """
        source_name_lower = source_name.lower()
        url_lower = (url or "").lower()

        # Check for Exchange Filings first (Highest priority)
        if any(p in source_name_lower or p in url_lower for p in ["bse", "nse", "sec.gov", "filing"]):
            return "Exchange Filing", False

        # Check for PR Networks / Company Releases
        if any(re.search(p, source_name_lower) or re.search(p, url_lower) for p in OFFICIAL_PATTERNS):
            return "Company Release", False

        # Check for known Journalism outlets
        if any(re.search(p, source_name_lower) or re.search(p, url_lower) for p in EDITORIAL_PATTERNS):
            return "Editorial News", True

        # Fallback logic: If it contains 'press release' or 'newswire', it's official
        if "press release" in source_name_lower or "newswire" in source_name_lower:
            return "Company Release", False

        # Default to Editorial News if no strong match for official
        return "Editorial News", True

    @staticmethod
    def identify_impact_area(title: str, summary: str) -> str | None:
        """Identifies the primary business area impacted by the news."""
        content = (title + " " + summary).lower()
        
        if any(x in content for x in ["profit", "eps", "earnings", "results", "pat", "ebitda"]):
            return "EPS / Profit"
        if any(x in content for x in ["revenue", "sales", "income", "top-line"]):
            return "Revenue"
        if any(x in content for x in ["margin", "opm", "profitability"]):
            return "Margins"
        if any(x in content for x in ["capex", "investment", "expansion", "capacity"]):
            return "Capex"
        if any(x in content for x in ["guidance", "outlook", "forecast"]):
            return "Guidance"
        if any(x in content for x in ["order book", "contracts", "tender", "win"]):
            return "Order Book"
        if any(x in content for x in ["regulatory", "sebi", "sec", "notice", "fine", "advisory"]):
            return "Regulatory"
        
        return "General Market"

    @staticmethod
    def is_duplicate(title1: str, title2: str, threshold: float = 0.85) -> bool:
        """Very basic deduplication logic (can be expanded with fuzzy matching)."""
        # Simple implementation: direct comparison or shared word count
        words1 = set(re.findall(r'\w+', title1.lower()))
        words2 = set(re.findall(r'\w+', title2.lower()))
        
        if not words1 or not words2:
            return False
            
        intersection = words1.intersection(words2)
        score = len(intersection) / max(len(words1), len(words2))
        return score >= threshold
