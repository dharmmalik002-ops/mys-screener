"""AI-powered company analysis using Google Gemini."""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from google import genai

from app.models.market import (
    AISummary,
    BalanceSheetItem,
    BusinessSegment,
    BusinessTrigger,
    CashFlowItem,
    CompanyFundamentals,
    CompetitivePosition,
    DetailedNews,
    FinancialRatios,
    InsiderTransaction,
    ManagementGuidance,
    RiskAnalysis,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
AI_CACHE_VERSION = 4


def _equity_market_labels(fundamentals: CompanyFundamentals) -> tuple[str, str, str]:
    exchange = (fundamentals.exchange or "").strip().upper()
    if exchange in {"NYSE", "NASDAQ", "AMEX", "ARCA"}:
        return ("US stock", "US-listed company", "US equities")
    return ("Indian stock", "Indian listed company", "Indian equities")


def _build_analysis_prompt(fundamentals: CompanyFundamentals) -> str:
    """Build a structured prompt for Gemini from existing fundamentals data."""
    stock_label, _, equities_label = _equity_market_labels(fundamentals)
    q_results = ""
    if fundamentals.quarterly_results:
        q_results = "\n".join(
            f"  {q.period}: Sales={q.sales_crore}Cr, NetProfit={q.net_profit_crore}Cr, OPM={q.operating_margin_pct}%"
            for q in fundamentals.quarterly_results[:6]
        )

    pl_data = ""
    if fundamentals.profit_loss:
        pl_data = "\n".join(
            f"  {p.period}: Sales={p.sales_crore}Cr, NetProfit={p.net_profit_crore}Cr, OPM={p.operating_margin_pct}%"
            for p in fundamentals.profit_loss[:6]
        )

    valuation_info = ""
    if fundamentals.valuation:
        v = fundamentals.valuation
        valuation_info = (
            f"MarketCap={v.market_cap_crore}Cr, PE={v.pe_ratio}, PEG={v.peg_ratio}, "
            f"OPM={v.operating_margin_pct}%, NetMargin={v.net_margin_pct}%, "
            f"ROE={v.roe_pct}%, ROCE={v.roce_pct}%, DivYield={v.dividend_yield_pct}%"
        )

    growth_info = ""
    if fundamentals.growth:
        g = fundamentals.growth
        growth_info = (
            f"SalesQoQ={g.sales_qoq_pct}%, SalesYoY={g.sales_yoy_pct}%, "
            f"ProfitQoQ={g.profit_qoq_pct}%, ProfitYoY={g.profit_yoy_pct}%, "
            f"OPM_latest={g.operating_margin_latest_pct}%, OPM_prev={g.operating_margin_previous_pct}%"
        )

    news_data = ""
    if fundamentals.recent_updates:
        news_data = "\n".join(
            f"  - [{u.published_at or 'recent'}] {u.title}" + (f": {u.summary}" if u.summary else "")
            for u in fundamentals.recent_updates[:10]
        )

    guidance_data = ""
    if fundamentals.management_guidance:
        guidance_data = "\n".join(
            f"  - [{item.guidance_date or 'recent'}] {item.guidance_source or item.fiscal_year}: " + "; ".join(item.key_guidance_points[:3])
            for item in fundamentals.management_guidance[:6]
            if item.key_guidance_points
        )

    detailed_news_data = ""
    if fundamentals.detailed_news:
        detailed_news_data = "\n".join(
            f"  - [{item.published_date}] {item.source}: {item.title}: {item.summary}"
            for item in fundamentals.detailed_news[:6]
        )

    shareholding = ""
    if fundamentals.shareholding_pattern:
        shareholding = "\n".join(
            f"  {s.period}: Promoter={s.promoter_pct}%, FII={s.fii_pct}%, DII={s.dii_pct}%, Public={s.public_pct}%"
            for s in fundamentals.shareholding_pattern[:4]
        )

    drivers = ""
    if fundamentals.growth_drivers:
        drivers = "\n".join(f"  - {d.title}: {d.detail}" for d in fundamentals.growth_drivers)

    return f"""Analyze this {stock_label} and return ONLY valid JSON (no markdown, no code fences).

COMPANY: {fundamentals.name} ({fundamentals.symbol})
SECTOR: {fundamentals.sector or 'Unknown'} / {fundamentals.sub_sector or 'Unknown'}
EXCHANGE: {fundamentals.exchange or 'Unknown'}

ABOUT: {fundamentals.about or 'Not available'}

QUARTERLY RESULTS (recent):
{q_results or 'Not available'}

ANNUAL PROFIT & LOSS:
{pl_data or 'Not available'}

VALUATION: {valuation_info or 'Not available'}

GROWTH METRICS: {growth_info or 'Not available'}

SHAREHOLDING:
{shareholding or 'Not available'}

GROWTH DRIVERS:
{drivers or 'Not available'}

MANAGEMENT GUIDANCE / CONCALL NOTES:
{guidance_data or 'Not available'}

DETAILED SOURCE NEWS:
{detailed_news_data or 'Not available'}

RECENT NEWS:
{news_data or 'Not available'}

Based on all the above data, provide a comprehensive analysis in this EXACT JSON structure:

{{
  "management_guidance": [
    {{
      "fiscal_year": "FY25",
      "revenue_growth_guidance_pct": null,
      "ebitda_guidance_pct": null,
      "capex_guidance_crore": null,
      "key_guidance_points": ["point1", "point2"]
    }}
  ],
  "strategy_and_outlook": "2-3 sentence management strategy and outlook based on recent data",
  "competitive_position": {{
    "market_position": "e.g. Market leader in X segment",
    "competitive_advantages": ["advantage1", "advantage2"],
    "market_share_estimate": null,
    "key_competitors": ["competitor1", "competitor2"]
  }},
  "business_segments": [
    {{
      "name": "segment name",
      "revenue_crore": null,
      "revenue_pct": null,
      "growth_pct": null,
      "period": "FY24"
    }}
  ],
  "risks_and_opportunities": [
    {{
      "risk_category": "operational|financial|regulatory|market|competitive",
      "description": "description",
      "severity": "high|medium|low",
      "mitigation_strategy": "strategy or null"
    }}
  ],
  "detailed_news": [
    {{
      "title": "news title",
      "summary": "detailed 2-3 sentence summary of impact",
      "impact_category": "earnings|strategic|regulatory|market|operational",
      "sentiment": "positive|negative|neutral",
      "source": "source name",
      "published_date": "YYYY-MM-DD",
      "detailed_points": ["point1", "point2"],
      "relevance_score": 0.8
    }}
  ],
  "ai_news_summary": {{
    "summary": "2-3 sentence overall market narrative for this stock",
    "key_points": ["key insight 1", "key insight 2", "key insight 3"],
    "sentiment": "positive|negative|neutral"
  }},
  "business_triggers": [
    {{
      "title": "trigger title",
      "description": "what could drive the stock",
      "impact": "positive|negative|neutral",
      "date": "YYYY-MM-DD or null",
      "source": "analysis",
      "likelihood_to_impact": 0.7
    }}
  ],
  "insider_transactions": [
    {{
      "person_name": "name",
      "position": "Promoter|Director|KMP",
      "transaction_type": "buy|sell",
      "quantity": 10000,
      "price_per_share": 100.0,
      "total_value_crore": 0.1,
      "date": "YYYY-MM-DD",
      "pct_of_holding_change": null
    }}
  ],
  "latest_earnings_key_metrics": {{
    "Revenue Growth QoQ": "15%",
    "Net Profit Growth YoY": "20%",
    "Operating Margin": "18%",
    "EPS TTM": "₹25"
  }},
  "upcoming_events": [
    {{
      "date": "YYYY-MM-DD",
      "event": "event description",
      "impact": "expected impact"
    }}
  ]
}}

RULES:
- Use ONLY data provided above. Do not hallucinate numbers not present in the input.
- For insider_transactions: only include if mentioned in news. If no insider data, return empty array.
- For management_guidance: prefer the latest dated earnings call / concall / management commentary. Do not rely on stale or undated boilerplate. Use null for unknown numeric fields.
- When transcript-derived management guidance or source-derived detailed news is already present in the input, preserve and sharpen that latest commentary instead of replacing it with generic text.
- For latest_earnings_key_metrics: compute from the quarterly results provided.
- For upcoming_events: infer likely next quarter results date, AGM, etc.
- For detailed_news: include only recent, credible, company-specific items that are likely to impact revenue, profit, margin, sales, demand, capacity, pricing, or valuation.
- Ignore routine compliance items such as generic Regulation 30 / SEBI (LODR) filings, transcripts, audio links, newspaper publications, and similar boilerplate unless the filing clearly contains financially material information. If such a filing is material, summarize only the financially relevant part.
- For strategy_and_outlook and growth-oriented commentary, prioritize the latest management guidance about revenue, profit, margin, sales, demand, capex, order book, and business outlook.
- Keep all text concise and actionable for traders following {equities_label}.
- Return ONLY the JSON object, no other text."""


def _build_company_question_prompt(fundamentals: CompanyFundamentals, question: str) -> str:
    _, company_label, equities_label = _equity_market_labels(fundamentals)
    recent_updates = "\n".join(
        f"- {item.title}" + (f": {item.summary}" if item.summary else "")
        for item in fundamentals.recent_updates[:6]
    ) or "Not available"
    growth_drivers = "\n".join(
        f"- {item.title}: {item.detail}"
        for item in fundamentals.growth_drivers[:5]
    ) or "Not available"
    quarterly = "\n".join(
        f"- {item.period}: Sales={item.sales_crore}Cr, NetProfit={item.net_profit_crore}Cr, OPM={item.operating_margin_pct}%"
        for item in fundamentals.quarterly_results[:4]
    ) or "Not available"
    risks = "\n".join(
        f"- {item.risk_category}: {item.description}"
        for item in fundamentals.risks_and_opportunities[:4]
    ) or "Not available"

    return f"""You are answering a user question about a {company_label}.
Use ONLY the supplied company data. If the data is insufficient, say so clearly.
Do not mention unavailable sources or make up facts.

COMPANY: {fundamentals.name} ({fundamentals.symbol})
SECTOR: {fundamentals.sector or 'Unknown'} / {fundamentals.sub_sector or 'Unknown'}
ABOUT: {fundamentals.business_summary or fundamentals.about or 'Not available'}
STRATEGY: {fundamentals.strategy_and_outlook or 'Not available'}
AI NEWS SUMMARY: {fundamentals.ai_news_summary.summary if fundamentals.ai_news_summary else 'Not available'}

RECENT QUARTERS:
{quarterly}

GROWTH DRIVERS:
{growth_drivers}

RECENT DEVELOPMENTS:
{recent_updates}

RISKS:
{risks}

VALUATION:
PE={fundamentals.valuation.pe_ratio if fundamentals.valuation else 'NA'}, PEG={fundamentals.valuation.peg_ratio if fundamentals.valuation else 'NA'}, ROE={fundamentals.valuation.roe_pct if fundamentals.valuation else 'NA'}, ROCE={fundamentals.valuation.roce_pct if fundamentals.valuation else 'NA'}

QUESTION: {question}

Answer in 2-4 concise paragraphs with direct reasoning. Keep it practical for an investor following {equities_label}."""


class AIAnalysisService:
    """Generates AI-powered company analysis using Google Gemini."""

    # Models to try in order — if primary model quota is exhausted, fall back
    _MODELS = ["gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]
    _MAX_RETRIES = 3
    _RETRY_BASE_DELAY = 5  # seconds

    def __init__(self, api_key: str | None, cache_dir: Path | None = None):
        self._api_key = api_key
        self._client: genai.Client | None = None
        self._cache_dir = cache_dir or Path(__file__).resolve().parents[2] / "data"
        self._cache_path = self._cache_dir / "ai_analysis_cache.json"
        self._memory_cache: dict[str, dict[str, Any]] = {}
        self._disabled_until: datetime | None = None

        if api_key:
            try:
                self._client = genai.Client(api_key=api_key)
                logger.info("Gemini AI client initialized (models: %s)", ", ".join(self._MODELS))
            except Exception as exc:
                logger.warning("Failed to initialize Gemini client: %s", exc)
                self._client = None

    @property
    def available(self) -> bool:
        if self._client is None:
            return False
        if self._disabled_until and datetime.now(timezone.utc) < self._disabled_until:
            return False
        return True

    def _mark_quota_exhausted(self, cooldown_minutes: int = 30) -> None:
        self._disabled_until = datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)
        logger.warning("Gemini AI temporarily disabled until %s after quota exhaustion", self._disabled_until.isoformat())

    def analyze_company(self, fundamentals: CompanyFundamentals) -> dict[str, Any]:
        """Run AI analysis on company fundamentals. Returns dict of AI-generated fields."""
        if not self.available:
            return {}

        symbol = fundamentals.symbol.upper()

        # Check memory cache
        cached = self._memory_cache.get(symbol)
        if cached and self._is_cache_fresh(cached):
            return cached.get("data", {})

        # Check disk cache
        disk_cache = self._load_disk_cache()
        cached_entry = disk_cache.get(symbol)
        if isinstance(cached_entry, dict) and self._is_cache_fresh(cached_entry):
            self._memory_cache[symbol] = cached_entry
            return cached_entry.get("data", {})

        # Generate fresh analysis
        try:
            result = self._generate_analysis(fundamentals)
            cache_entry = {
                "data": result,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "cache_version": AI_CACHE_VERSION,
            }
            self._memory_cache[symbol] = cache_entry
            disk_cache[symbol] = cache_entry
            self._save_disk_cache(disk_cache)
            return result
        except Exception as exc:
            logger.error("AI analysis failed for %s: %s", symbol, exc)
            return {}

    def _generate_analysis(self, fundamentals: CompanyFundamentals) -> dict[str, Any]:
        """Call Gemini API with retry and model fallback."""
        prompt = _build_analysis_prompt(fundamentals)

        last_exc: Exception | None = None
        for model_name in self._MODELS:
            for attempt in range(self._MAX_RETRIES):
                try:
                    response = self._client.models.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=genai.types.GenerateContentConfig(
                            temperature=0.3,
                            max_output_tokens=4096,
                        ),
                    )
                    text = response.text.strip()
                    if text.startswith("```"):
                        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()
                    if text.startswith("json"):
                        text = text[4:].strip()

                    result = json.loads(text)
                    logger.info("AI analysis succeeded with model %s (attempt %d)", model_name, attempt + 1)
                    return result
                except Exception as exc:
                    last_exc = exc
                    err_str = str(exc).lower()
                    if "resource_exhausted" in err_str or "429" in err_str:
                        if attempt < self._MAX_RETRIES - 1:
                            delay = self._RETRY_BASE_DELAY * (attempt + 1)
                            logger.warning(
                                "Rate limited on %s (attempt %d), retrying in %ds...",
                                model_name, attempt + 1, delay,
                            )
                            time.sleep(delay)
                            continue
                        else:
                            logger.warning("Quota exhausted on %s, trying next model...", model_name)
                            break  # try next model
                    else:
                        logger.error("AI generation error on %s: %s", model_name, exc)
                        raise  # non-rate-limit errors should not retry

        self._mark_quota_exhausted()
        raise RuntimeError(f"All Gemini models exhausted after retries: {last_exc}")

    def _is_cache_fresh(self, entry: dict[str, Any]) -> bool:
        if int(entry.get("cache_version", 0)) != AI_CACHE_VERSION:
            return False
        generated_at = entry.get("generated_at")
        if not generated_at:
            return False
        try:
            gen_time = datetime.fromisoformat(generated_at)
            age_hours = (datetime.now(timezone.utc) - gen_time).total_seconds() / 3600
            return age_hours < 24
        except Exception:
            return False

    def _load_disk_cache(self) -> dict[str, Any]:
        if not self._cache_path.exists():
            return {}
        try:
            return json.loads(self._cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_disk_cache(self, cache: dict[str, Any]) -> None:
        try:
            self._cache_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to save AI cache: %s", exc)

    def clear_cache(self, symbol: str | None = None) -> None:
        """Clear AI analysis cache for a symbol or all symbols."""
        if symbol:
            self._memory_cache.pop(symbol.upper(), None)
            disk_cache = self._load_disk_cache()
            disk_cache.pop(symbol.upper(), None)
            self._save_disk_cache(disk_cache)
        else:
            self._memory_cache.clear()
            self._save_disk_cache({})

    def bulk_analyze(
        self,
        fundamentals_list: list[CompanyFundamentals],
        delay_seconds: float = 2.0,
    ) -> dict[str, dict[str, Any]]:
        """Analyze multiple companies with rate limiting for the scheduler."""
        results: dict[str, dict[str, Any]] = {}
        for idx, fund in enumerate(fundamentals_list):
            try:
                result = self.analyze_company(fund)
                results[fund.symbol] = result
                logger.info("AI analysis %d/%d: %s done", idx + 1, len(fundamentals_list), fund.symbol)
            except Exception as exc:
                logger.error("AI analysis failed for %s: %s", fund.symbol, exc)
            if idx < len(fundamentals_list) - 1:
                time.sleep(delay_seconds)
        return results

    def generate_money_flow_report(self, sector_data: str, week_key: str) -> dict[str, Any]:
        """Generate a weekly money flow / sector rotation report using Gemini."""
        if not self.available:
            return {}

        prompt = f"""You are an expert Indian equity market analyst. Today is {datetime.now(IST).strftime('%d %B %Y')}.
Based on the sector performance data below, generate a detailed weekly money flow analysis.

SECTOR PERFORMANCE DATA:
{sector_data}

Return ONLY valid JSON (no markdown, no code fences) with this EXACT structure:
{{
  "inflows": [
    {{"name": "Sector Name", "sentiment": "bullish", "reason": "Detailed reason why money is flowing in", "magnitude": "strong"}}
  ],
  "outflows": [
    {{"name": "Sector Name", "sentiment": "bearish", "reason": "Detailed reason why money is flowing out", "magnitude": "moderate"}}
  ],
  "sector_performance": [
    {{"name": "Sector Name", "sentiment": "bullish|bearish|neutral", "reason": "Key performance driver", "magnitude": "strong|moderate|mild"}}
  ],
  "short_term_headwinds": [
    {{"name": "Sector Name", "sentiment": "bearish", "reason": "Why this sector faces near-term pressure (1-4 weeks)", "magnitude": "strong|moderate|mild"}}
  ],
  "short_term_tailwinds": [
    {{"name": "Sector Name", "sentiment": "bullish", "reason": "Why this sector has near-term upside catalysts (1-4 weeks)", "magnitude": "strong|moderate|mild"}}
  ],
  "long_term_tailwinds": [
    {{"name": "Sector Name", "sentiment": "bullish", "reason": "Structural/multi-year growth driver", "magnitude": "strong|moderate|mild"}}
  ],
  "macro_summary": "3-4 sentences summarising the overall market money-flow picture, dominant themes, and key risks for Indian equities this week."
}}

Rules:
- Include 3-6 items per list; rank by conviction.
- Use concrete reasoning (global cues, macro data, policy, earnings trends, FII/DII flows, commodities).
- sentiment must be exactly: bullish | bearish | neutral
- magnitude must be exactly: strong | moderate | mild
- Return ONLY the JSON object."""

        last_exc: Exception | None = None
        for model_name in self._MODELS:
            for attempt in range(self._MAX_RETRIES):
                try:
                    response = self._client.models.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=genai.types.GenerateContentConfig(temperature=0.4, max_output_tokens=4096),
                    )
                    text = response.text.strip()
                    if text.startswith("```"):
                        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    if text.endswith("```"):
                        text = text[:-3].strip()
                    if text.startswith("json"):
                        text = text[4:].strip()
                    result = json.loads(text)
                    logger.info("Money flow report generated with model %s week %s", model_name, week_key)
                    return result
                except Exception as exc:
                    last_exc = exc
                    err_str = str(exc).lower()
                    if "resource_exhausted" in err_str or "429" in err_str:
                        if attempt < self._MAX_RETRIES - 1:
                            time.sleep(self._RETRY_BASE_DELAY * (attempt + 1))
                            continue
                        break
                    raise
        self._mark_quota_exhausted()
        raise RuntimeError(f"All models exhausted for money flow report: {last_exc}")

    def answer_company_question(self, fundamentals: CompanyFundamentals, question: str) -> str:
        """Answer a freeform company question using Gemini and existing fundamentals context."""
        cleaned_question = question.strip()
        if not cleaned_question:
            raise ValueError("Question cannot be empty")
        if not self.available:
            raise RuntimeError("Gemini API key not configured")

        prompt = _build_company_question_prompt(fundamentals, cleaned_question)
        last_exc: Exception | None = None
        for model_name in self._MODELS:
            for attempt in range(self._MAX_RETRIES):
                try:
                    response = self._client.models.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=genai.types.GenerateContentConfig(
                            temperature=0.25,
                            max_output_tokens=1400,
                        ),
                    )
                    text = response.text.strip()
                    logger.info(
                        "Company Q&A succeeded with model %s for %s",
                        model_name,
                        fundamentals.symbol,
                    )
                    return text
                except Exception as exc:
                    last_exc = exc
                    err_str = str(exc).lower()
                    if "resource_exhausted" in err_str or "429" in err_str:
                        if attempt < self._MAX_RETRIES - 1:
                            time.sleep(self._RETRY_BASE_DELAY * (attempt + 1))
                            continue
                        break
                    raise
        self._mark_quota_exhausted()
        raise RuntimeError(f"All Gemini models exhausted for company Q&A: {last_exc}")


def parse_ai_management_guidance(raw: list[dict[str, Any]]) -> list[ManagementGuidance]:
    result = []
    for item in (raw or []):
        try:
            result.append(ManagementGuidance(
                fiscal_year=str(item.get("fiscal_year", "Unknown")),
                revenue_growth_guidance_pct=item.get("revenue_growth_guidance_pct"),
                ebitda_guidance_pct=item.get("ebitda_guidance_pct"),
                eps_guidance=item.get("eps_guidance"),
                capex_guidance_crore=item.get("capex_guidance_crore"),
                guidance_date=item.get("guidance_date"),
                guidance_source=item.get("guidance_source"),
                key_guidance_points=item.get("key_guidance_points") or [],
            ))
        except Exception:
            continue
    return result


def parse_ai_competitive_position(raw: dict[str, Any] | None) -> CompetitivePosition | None:
    if not raw:
        return None
    try:
        return CompetitivePosition(
            market_position=raw.get("market_position"),
            competitive_advantages=raw.get("competitive_advantages") or [],
            market_share_estimate=raw.get("market_share_estimate"),
            key_competitors=raw.get("key_competitors") or [],
        )
    except Exception:
        return None


def parse_ai_business_segments(raw: list[dict[str, Any]]) -> list[BusinessSegment]:
    result = []
    for item in (raw or []):
        try:
            result.append(BusinessSegment(
                name=str(item.get("name", "Unknown")),
                revenue_crore=item.get("revenue_crore"),
                revenue_pct=item.get("revenue_pct"),
                growth_pct=item.get("growth_pct"),
                period=str(item.get("period", "")),
            ))
        except Exception:
            continue
    return result


def parse_ai_risks(raw: list[dict[str, Any]]) -> list[RiskAnalysis]:
    result = []
    for item in (raw or []):
        try:
            result.append(RiskAnalysis(
                risk_category=str(item.get("risk_category", "market")),
                description=str(item.get("description", "")),
                severity=str(item.get("severity", "medium")),
                mitigation_strategy=item.get("mitigation_strategy"),
            ))
        except Exception:
            continue
    return result


def parse_ai_detailed_news(raw: list[dict[str, Any]]) -> list[DetailedNews]:
    result = []
    for item in (raw or []):
        try:
            result.append(DetailedNews(
                title=str(item.get("title", "")),
                summary=str(item.get("summary", "")),
                impact_category=str(item.get("impact_category", "market")),
                sentiment=str(item.get("sentiment", "neutral")),
                source=str(item.get("source", "AI Analysis")),
                published_date=str(item.get("published_date", "")),
                detailed_points=item.get("detailed_points") or [],
                relevance_score=float(item.get("relevance_score", 0.5)),
            ))
        except Exception:
            continue
    return result


def parse_ai_summary(raw: dict[str, Any] | None) -> AISummary | None:
    if not raw:
        return None
    try:
        return AISummary(
            summary=str(raw.get("summary", "")),
            key_points=raw.get("key_points") or [],
            sentiment=raw.get("sentiment", "neutral"),
        )
    except Exception:
        return None


def parse_ai_business_triggers(raw: list[dict[str, Any]]) -> list[BusinessTrigger]:
    result = []
    for item in (raw or []):
        try:
            result.append(BusinessTrigger(
                title=str(item.get("title", "")),
                description=str(item.get("description", "")),
                impact=item.get("impact", "neutral"),
                date=item.get("date"),
                source=item.get("source"),
                likelihood_to_impact=float(item.get("likelihood_to_impact", 0.5)),
            ))
        except Exception:
            continue
    return result


def parse_ai_insider_transactions(raw: list[dict[str, Any]]) -> list[InsiderTransaction]:
    result = []
    for item in (raw or []):
        try:
            result.append(InsiderTransaction(
                person_name=str(item.get("person_name", "")),
                position=str(item.get("position", "")),
                transaction_type=item.get("transaction_type", "buy"),
                quantity=int(item.get("quantity", 0)),
                price_per_share=float(item.get("price_per_share", 0)),
                total_value_crore=float(item.get("total_value_crore", 0)),
                date=str(item.get("date", "")),
                pct_of_holding_change=item.get("pct_of_holding_change"),
                remarks=item.get("remarks"),
            ))
        except Exception:
            continue
    return result


def _merge_management_guidance(
    existing: list[ManagementGuidance],
    generated: list[ManagementGuidance],
) -> list[ManagementGuidance]:
    merged = sorted(
        [*existing, *generated],
        key=lambda item: item.guidance_date or "",
        reverse=True,
    )
    results: list[ManagementGuidance] = []
    seen: set[str] = set()
    for item in merged:
        points = "|".join(point.strip().lower() for point in item.key_guidance_points[:2])
        key = f"{(item.guidance_date or '').lower()}::{(item.guidance_source or '').lower()}::{points}"
        if key in seen:
            continue
        seen.add(key)
        results.append(item)
    return results[:6]


def _merge_detailed_news(
    existing: list[DetailedNews],
    generated: list[DetailedNews],
) -> list[DetailedNews]:
    merged = sorted(
        [*existing, *generated],
        key=lambda item: item.published_date or "",
        reverse=True,
    )
    results: list[DetailedNews] = []
    seen: set[str] = set()
    for item in merged:
        key = f"{item.published_date.lower()}::{item.title.strip().lower()}::{item.source.strip().lower()}"
        if key in seen:
            continue
        seen.add(key)
        results.append(item)
    return results[:8]


def enrich_fundamentals_with_ai(
    fundamentals: CompanyFundamentals,
    ai_data: dict[str, Any],
) -> CompanyFundamentals:
    """Merge AI-generated analysis data into CompanyFundamentals."""
    if not ai_data:
        return fundamentals

    updates: dict[str, Any] = {}

    if ai_data.get("management_guidance"):
        updates["management_guidance"] = _merge_management_guidance(
            fundamentals.management_guidance,
            parse_ai_management_guidance(ai_data["management_guidance"]),
        )

    if ai_data.get("strategy_and_outlook"):
        updates["strategy_and_outlook"] = str(ai_data["strategy_and_outlook"])

    if ai_data.get("competitive_position"):
        updates["competitive_position"] = parse_ai_competitive_position(ai_data["competitive_position"])

    if ai_data.get("business_segments"):
        updates["business_segments"] = parse_ai_business_segments(ai_data["business_segments"])

    if ai_data.get("risks_and_opportunities"):
        updates["risks_and_opportunities"] = parse_ai_risks(ai_data["risks_and_opportunities"])

    if ai_data.get("detailed_news"):
        updates["detailed_news"] = _merge_detailed_news(
            fundamentals.detailed_news,
            parse_ai_detailed_news(ai_data["detailed_news"]),
        )

    if ai_data.get("ai_news_summary"):
        updates["ai_news_summary"] = parse_ai_summary(ai_data["ai_news_summary"])
        updates["last_news_update"] = datetime.now(timezone.utc)

    if ai_data.get("business_triggers"):
        updates["business_triggers"] = parse_ai_business_triggers(ai_data["business_triggers"])

    if ai_data.get("insider_transactions"):
        updates["insider_transactions"] = parse_ai_insider_transactions(ai_data["insider_transactions"])

    if ai_data.get("latest_earnings_key_metrics"):
        updates["latest_earnings_key_metrics"] = ai_data["latest_earnings_key_metrics"]

    if ai_data.get("upcoming_events"):
        updates["upcoming_events"] = ai_data["upcoming_events"]

    if updates:
        return fundamentals.model_copy(update=updates)
    return fundamentals
