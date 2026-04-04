from datetime import date, datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class StockSnapshot(BaseModel):
    symbol: str
    name: str
    exchange: Literal["NSE", "BSE", "NYSE", "NASDAQ"]
    listing_date: date | None = None
    sector: str
    sub_sector: str = "Unclassified"
    circuit_band_label: str | None = None
    upper_circuit_limit: float | None = None
    lower_circuit_limit: float | None = None
    market_cap_crore: float
    last_price: float
    previous_close: float | None = None
    change_pct: float
    volume: int
    avg_volume_20d: int
    avg_volume_30d: int | None = None
    day_high: float
    day_low: float
    avg_volume_50d: int | None = None
    previous_day_high: float | None = None
    previous_day_low: float | None = None
    week_high: float | None = None
    week_low: float | None = None
    week_high_prev: float | None = None
    week_low_prev: float | None = None
    month_high: float | None = None
    month_low: float | None = None
    month_high_prev: float | None = None
    month_low_prev: float | None = None
    ath: float
    ath_prev: float | None = None
    high_52w: float
    high_52w_prev: float | None = None
    low_52w: float | None = None
    low_52w_prev: float | None = None
    atl: float | None = None
    multi_year_high: float | None = None
    high_3y: float | None = None
    high_6m: float | None = None
    high_6m_prev: float | None = None
    low_6m: float | None = None
    low_6m_prev: float | None = None
    high_3m: float | None = None
    range_high_20d: float
    range_high_prev_20d: float | None = None
    sma20: float | None = None
    sma50: float | None = None
    sma150: float | None = None
    sma200: float | None = None
    sma200_1m_ago: float | None = None
    sma200_5m_ago: float | None = None
    ema10: float | None = None
    ema20: float | None = None
    ema50: float | None = None
    ema200: float | None = None
    weekly_ema20: float | None = None
    benchmark_return_1d: float = 0.0
    benchmark_return_5d: float = 0.0
    benchmark_return_20d: float
    benchmark_return_60d: float = 0.0
    benchmark_return_126d: float = 0.0
    benchmark_return_252d: float = 0.0
    sector_return_20d: float
    stock_return_5d: float = 0.0
    stock_return_20d: float = 0.0
    stock_return_40d: float = 0.0
    stock_return_60d: float = 0.0
    stock_return_126d: float = 0.0
    stock_return_189d: float = 0.0
    stock_return_12m: float = 0.0
    stock_return_504d: float = 0.0
    stock_return_12m_1d_ago: float = 0.0
    stock_return_12m_1w_ago: float = 0.0
    stock_return_12m_1m_ago: float = 0.0
    rsi_14: float = 50.0  # Added for Market Health Dashboard
    rs_line_today: float = 0.0
    rs_line_1m: float = 0.0
    gap_pct: float = 0.0
    rs_rating: int = 0
    rs_rating_1d_ago: int = 0
    rs_rating_1w_ago: int = 0
    rs_rating_1m_ago: int = 0
    rs_weighted_score: float = 0.0
    rs_eligible: bool = False
    pivot_high: float
    darvas_high: float
    darvas_low: float
    atr14: float = 0.0
    adr_pct_20: float = 0.0
    pullback_depth_pct: float
    trend_strength: float
    long_base_avg_range_pct: float | None = None
    long_base_span_pct: float | None = None
    long_base_window_days: int | None = None
    recent_highs: list[float] = Field(default_factory=list)
    recent_lows: list[float] = Field(default_factory=list)
    recent_volumes: list[int] = Field(default_factory=list)
    chart_grid_points: list["ChartLinePoint"] = Field(default_factory=list)
    instrument_key: str | None = None
    
    # Fundamental cached fields
    eps_growth_yoy: float | None = None
    revenue_growth_yoy: float | None = None
    operating_margin: float | None = None
    profit_margin: float | None = None
    roe: float | None = None
    roa: float | None = None
    trailing_eps: float | None = None
    forward_eps: float | None = None
    peg_ratio: float | None = None
    pe_ratio: float | None = None
    price_to_book: float | None = None
    debt_to_equity: float | None = None

    @property
    def relative_volume(self) -> float:
        if self.avg_volume_20d <= 0:
            return 0.0
        return round(self.volume / self.avg_volume_20d, 2)

    @property
    def avg_rupee_volume_30d_crore(self) -> float:
        avg_volume = self.avg_volume_30d or self.avg_volume_20d
        return round((avg_volume * self.last_price) / 10_000_000, 2)

    @property
    def avg_rupee_turnover_20d_crore(self) -> float:
        return round((self.avg_volume_20d * self.last_price) / 10_000_000, 2)

    @property
    def nifty_outperformance(self) -> float:
        return round(self.stock_return_20d - self.benchmark_return_20d, 2)

    @staticmethod
    def _relative_strength_pct(stock_return_pct: float, benchmark_return_pct: float) -> float:
        stock_factor = 1 + (stock_return_pct / 100)
        benchmark_factor = 1 + (benchmark_return_pct / 100)
        if stock_factor <= 0 or benchmark_factor <= 0:
            return 0.0
        return ((stock_factor / benchmark_factor) - 1) * 100

    @property
    def rs_today(self) -> float:
        return round(self._relative_strength_pct(self.change_pct, self.benchmark_return_1d), 2)

    @property
    def rs_1m(self) -> float:
        return round(self._relative_strength_pct(self.stock_return_20d, self.benchmark_return_20d), 2)

    @property
    def three_month_rs(self) -> float:
        return round(self.stock_return_60d - self.benchmark_return_60d, 2)

    @property
    def sector_outperformance(self) -> float:
        return round(self.stock_return_20d - self.sector_return_20d, 2)

    @property
    def previous_day_high_level(self) -> float:
        return round(self.previous_day_high or self.day_high * 0.985, 2)

    @property
    def previous_day_low_level(self) -> float:
        return round(self.previous_day_low or self.day_low * 1.015, 2)

    @property
    def week_high_level(self) -> float:
        return round(self.week_high or max(self.day_high, self.range_high_20d), 2)

    @property
    def week_low_level(self) -> float:
        return round(self.week_low or self.day_low, 2)

    @property
    def previous_week_high_level(self) -> float:
        return round(self.week_high_prev or self.week_high_level, 2)

    @property
    def previous_week_low_level(self) -> float:
        return round(self.week_low_prev or self.week_low_level, 2)

    @property
    def month_high_level(self) -> float:
        return round(self.month_high or max(self.range_high_20d, self.day_high * 0.998), 2)

    @property
    def month_low_level(self) -> float:
        return round(self.month_low or self.day_low, 2)

    @property
    def previous_month_high_level(self) -> float:
        return round(self.month_high_prev or self.month_high_level, 2)

    @property
    def previous_month_low_level(self) -> float:
        return round(self.month_low_prev or self.month_low_level, 2)

    @property
    def high_6m_level(self) -> float:
        return round(self.high_6m or self.high_52w * 0.992, 2)

    @property
    def high_3m_level(self) -> float:
        return round(self.high_3m or self.high_6m_level, 2)

    @property
    def low_6m_level(self) -> float:
        return round(self.low_6m or self.day_low, 2)

    @property
    def previous_high_6m_level(self) -> float:
        return round(self.high_6m_prev or self.high_6m_level, 2)

    @property
    def previous_low_6m_level(self) -> float:
        return round(self.low_6m_prev or self.low_6m_level, 2)

    @property
    def low_52w_level(self) -> float:
        return round(self.low_52w or self.day_low, 2)

    @property
    def previous_high_52w_level(self) -> float:
        return round(self.high_52w_prev or self.high_52w, 2)

    @property
    def previous_low_52w_level(self) -> float:
        return round(self.low_52w_prev or self.low_52w_level, 2)

    @property
    def ath_breakout_level(self) -> float:
        return round(self.ath_prev or self.ath, 2)

    @property
    def range_breakout_level(self) -> float:
        return round(self.range_high_prev_20d or self.range_high_20d, 2)

    @property
    def atl_level(self) -> float:
        return round(self.atl or self.day_low, 2)

    @property
    def ema_stack_bullish(self) -> bool:
        if self.ema20 is None or self.ema50 is None or self.ema200 is None:
            return False
        return self.last_price >= self.ema20 >= self.ema50 >= self.ema200

    @property
    def ema_stack_bearish(self) -> bool:
        if self.ema20 is None or self.ema50 is None or self.ema200 is None:
            return False
        return self.last_price <= self.ema20 <= self.ema50 <= self.ema200

    @property
    def rs_composite(self) -> float:
        return round(
            (self.stock_return_20d * 0.4)
            + (self.stock_return_60d * 0.3)
            + (self.nifty_outperformance * 0.2)
            + (self.sector_outperformance * 0.1),
            2,
        )

    @property
    def pct_from_52w_high(self) -> float:
        if not self.high_52w:
            return 0.0
        return round(((self.high_52w - self.last_price) / self.high_52w) * 100, 2)

    @property
    def pct_from_ath(self) -> float:
        if not self.ath:
            return 0.0
        return round(((self.ath - self.last_price) / self.ath) * 100, 2)

    @property
    def pct_from_52w_low(self) -> float:
        low = self.low_52w_level
        if not low:
            return 0.0
        return round(((self.last_price - low) / low) * 100, 2)

    @property
    def day_range_pct(self) -> float:
        if not self.last_price:
            return 0.0
        return round(((self.day_high - self.day_low) / self.last_price) * 100, 2)

    def ma_value(self, key: str) -> float | None:
        mapping = {
            "ema10": self.ema10 or self.ema20,
            "ema20": self.ema20,
            "ema50": self.ema50,
            "ema200": self.ema200,
        }
        value = mapping[key]
        if value is None or value <= 0:
            return None
        return float(value)


class ChartBar(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: int


class ChartLinePoint(BaseModel):
    time: int
    value: float


class ChartLineMarker(BaseModel):
    time: int
    value: float
    label: str
    color: str


class QuarterlyResultItem(BaseModel):
    period: str
    sales_crore: float | None = None
    expenses_crore: float | None = None
    operating_profit_crore: float | None = None
    operating_margin_pct: float | None = None
    profit_before_tax_crore: float | None = None
    net_profit_crore: float | None = None
    eps: float | None = None
    result_document_url: str | None = None


class ProfitLossItem(BaseModel):
    period: str
    sales_crore: float | None = None
    operating_profit_crore: float | None = None
    operating_margin_pct: float | None = None
    net_profit_crore: float | None = None
    eps: float | None = None
    dividend_payout_pct: float | None = None


class GrowthSnapshot(BaseModel):
    latest_period: str | None = None
    sales_qoq_pct: float | None = None
    sales_yoy_pct: float | None = None
    profit_qoq_pct: float | None = None
    profit_yoy_pct: float | None = None
    operating_margin_latest_pct: float | None = None
    operating_margin_previous_pct: float | None = None
    net_margin_latest_pct: float | None = None
    net_margin_previous_pct: float | None = None


class ValuationSnapshot(BaseModel):
    market_cap_crore: float | None = None
    pe_ratio: float | None = None
    peg_ratio: float | None = None
    operating_margin_pct: float | None = None
    net_margin_pct: float | None = None
    roce_pct: float | None = None
    roe_pct: float | None = None
    dividend_yield_pct: float | None = None


class ShareholdingPatternItem(BaseModel):
    period: str
    promoter_pct: float | None = None
    fii_pct: float | None = None
    dii_pct: float | None = None
    public_pct: float | None = None
    shareholder_count: int | None = None


class ShareholdingDelta(BaseModel):
    latest_period: str | None = None
    previous_period: str | None = None
    promoter_change_pct: float | None = None
    fii_change_pct: float | None = None
    dii_change_pct: float | None = None
    public_change_pct: float | None = None


class GrowthDriver(BaseModel):
    title: str
    detail: str
    tone: Literal["positive", "neutral", "watch"] = "neutral"


class CompanyUpdateItem(BaseModel):
    title: str
    source: str
    published_at: str | None = None
    summary: str | None = None
    link: str | None = None
    kind: Literal["results", "concall", "news", "holding", "filing"] = "news"


class BusinessTrigger(BaseModel):
    title: str
    description: str
    impact: Literal["positive", "negative", "neutral"] = "neutral"
    date: str | None = None
    source: str | None = None
    likelihood_to_impact: float = Field(default=0.8, ge=0.0, le=1.0)


class InsiderTransaction(BaseModel):
    person_name: str
    position: str
    transaction_type: Literal["buy", "sell"] = "buy"
    quantity: int
    price_per_share: float
    total_value_crore: float
    date: str
    pct_of_holding_change: float | None = None
    remarks: str | None = None


class AISummary(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    summary: str
    key_points: list[str] = Field(default_factory=list)
    sentiment: Literal["positive", "negative", "neutral"] = "neutral"


class BalanceSheetItem(BaseModel):
    """Balance sheet metrics for a period"""
    period: str
    total_assets_crore: float | None = None
    current_assets_crore: float | None = None
    total_liabilities_crore: float | None = None
    current_liabilities_crore: float | None = None
    shareholders_equity_crore: float | None = None
    debt_crore: float | None = None
    cash_and_equivalents_crore: float | None = None
    inventory_crore: float | None = None
    receivables_crore: float | None = None


class CashFlowItem(BaseModel):
    """Cash flow metrics for a period"""
    period: str
    operating_cash_flow_crore: float | None = None
    investing_cash_flow_crore: float | None = None
    financing_cash_flow_crore: float | None = None
    free_cash_flow_crore: float | None = None
    capital_expenditure_crore: float | None = None
    dividends_paid_crore: float | None = None


class FinancialRatios(BaseModel):
    """Key financial ratios"""
    period: str
    roe_pct: float | None = None  # Return on Equity
    roa_pct: float | None = None  # Return on Assets
    roce_pct: float | None = None  # Return on Capital Employed
    current_ratio: float | None = None
    quick_ratio: float | None = None
    debt_to_equity_ratio: float | None = None
    debt_to_assets_ratio: float | None = None
    interest_coverage: float | None = None
    asset_turnover: float | None = None


class ManagementGuidance(BaseModel):
    """Forward-looking guidance from management"""
    fiscal_year: str
    revenue_growth_guidance_pct: float | None = None
    ebitda_guidance_pct: float | None = None
    eps_guidance: float | None = None
    capex_guidance_crore: float | None = None
    guidance_date: str | None = None
    guidance_source: str | None = None
    key_guidance_points: list[str] = Field(default_factory=list)


class CompetitivePosition(BaseModel):
    """Company's competitive standing"""
    market_position: str | None = None  # Market leader, strong player, etc.
    competitive_advantages: list[str] = Field(default_factory=list)
    market_share_estimate: float | None = None  # Percentage
    key_competitors: list[str] = Field(default_factory=list)


class BusinessSegment(BaseModel):
    """Revenue breakdown by business segment"""
    name: str
    revenue_crore: float | None = None
    revenue_pct: float | None = None
    growth_pct: float | None = None
    period: str


class DetailedNews(BaseModel):
    """Detailed news article with full summary"""
    title: str
    summary: str  # Full detailed summary, not just 1-2 sentences
    impact_category: str  # earnings, strategic, regulatory, market, etc.
    sentiment: str  # positive, negative, neutral
    source: str
    published_date: str
    detailed_points: list[str] = Field(default_factory=list)
    relevance_score: float  # 0-1, how relevant to stock price


class RiskAnalysis(BaseModel):
    """Key risks and opportunities"""
    risk_category: str  # operational, financial, regulatory, market, etc.
    description: str
    severity: str  # high, medium, low
    mitigation_strategy: str | None = None


class CompanyFundamentals(BaseModel):
    symbol: str
    name: str
    exchange: str | None = None
    sector: str | None = None
    sub_sector: str | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Business Overview
    about: str | None = None
    business_summary: str | None = None  # Detailed business description
    company_website: str | None = None
    headquarters: str | None = None
    
    # Financial Data
    quarterly_results: list[QuarterlyResultItem] = Field(default_factory=list)
    profit_loss: list[ProfitLossItem] = Field(default_factory=list)
    balance_sheet: list[BalanceSheetItem] = Field(default_factory=list)
    cash_flow: list[CashFlowItem] = Field(default_factory=list)
    financial_ratios: list[FinancialRatios] = Field(default_factory=list)
    
    # Valuation & Growth
    growth: GrowthSnapshot | None = None
    valuation: ValuationSnapshot | None = None
    growth_drivers: list[GrowthDriver] = Field(default_factory=list)
    
    # Management & Strategy
    management_team: list[dict[str, str]] = Field(default_factory=list)  # [{name, position, background}]
    management_guidance: list[ManagementGuidance] = Field(default_factory=list)
    strategy_and_outlook: str | None = None  # Management commentary on future plans
    
    # Competitive Analysis
    competitive_position: CompetitivePosition | None = None
    business_segments: list[BusinessSegment] = Field(default_factory=list)
    geographic_presence: list[str] = Field(default_factory=list)
    
    # Risk Analysis
    risks_and_opportunities: list[RiskAnalysis] = Field(default_factory=list)
    
    # Updates & News
    recent_updates: list[CompanyUpdateItem] = Field(default_factory=list)
    detailed_news: list[DetailedNews] = Field(default_factory=list)
    shareholding_pattern: list[ShareholdingPatternItem] = Field(default_factory=list)
    shareholding_delta: ShareholdingDelta | None = None
    
    # AI-generated insights
    ai_news_summary: AISummary | None = None
    business_triggers: list[BusinessTrigger] = Field(default_factory=list)
    insider_transactions: list[InsiderTransaction] = Field(default_factory=list)
    last_news_update: datetime | None = None
    
    # Recent Performance & Catalysts
    latest_earnings_key_metrics: dict[str, float | str] = Field(default_factory=dict)
    upcoming_events: list[dict[str, str]] = Field(default_factory=list)  # [{date, event, impact}]
    
    data_warnings: list[str] = Field(default_factory=list)


class ScanDescriptor(BaseModel):
    id: str
    name: str
    category: str
    description: str
    hit_count: int = 0


class ScanMatch(BaseModel):
    scan_id: str
    symbol: str
    name: str
    exchange: str
    listing_date: date | None = None
    sector: str
    sub_sector: str | None = None
    market_cap_crore: float
    last_price: float
    change_pct: float
    relative_volume: float
    avg_rupee_volume_30d_crore: float | None = None
    score: float
    pattern: str | None = None
    rs_rating: int | None = None
    rs_rating_1d_ago: int | None = None
    rs_rating_1w_ago: int | None = None
    rs_rating_1m_ago: int | None = None
    rs_today: float | None = None
    rs_1m: float | None = None
    nifty_outperformance: float | None = None
    sector_outperformance: float | None = None
    three_month_rs: float | None = None
    stock_return_20d: float | None = None
    stock_return_60d: float | None = None
    stock_return_12m: float | None = None
    gap_pct: float | None = None
    reasons: list[str] = Field(default_factory=list)


class AlertItem(BaseModel):
    id: str
    symbol: str
    scan_name: str
    message: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DashboardSection(BaseModel):
    title: str
    items: list[ScanMatch]


class DashboardResponse(BaseModel):
    app_name: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    market_status: str
    data_mode: Literal["demo", "upstox", "free"]
    market_cap_min_crore: float
    universe_count: int
    scanners: list[ScanDescriptor]
    popular_scan_ids: list[str]
    top_gainers: list[ScanMatch]
    top_losers: list[ScanMatch]
    top_volume_spikes: list[ScanMatch]
    recent_alerts: list[AlertItem]


class IndexQuoteItem(BaseModel):
    symbol: str
    price: float
    change_pct: float
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IndexQuotesResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    items: list[IndexQuoteItem]


class ScanResultsResponse(BaseModel):
    scan: ScanDescriptor
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    market_cap_min_crore: float
    total_hits: int
    items: list[ScanMatch]
    sector_summaries: list["ScanSectorSummary"] = Field(default_factory=list)


class AiScanResponse(BaseModel):
    results: ScanResultsResponse
    parsed_request: "CustomScanRequest"


class ScanSectorSummary(BaseModel):
    sector: str
    current_hits: int
    prior_week_hits: int = 0
    prior_month_hits: int = 0
    sector_return_1w: float = 0.0
    sector_return_1m: float = 0.0


class NearPivotScanRequest(BaseModel):
    min_rs_rating: int = Field(default=70, ge=1, le=99)
    max_pct_from_52w_high: float = Field(default=20.0, ge=0.0)
    max_consolidation_range_pct: float = Field(default=8.0, ge=0.1)
    min_consolidation_days: int = Field(default=4, ge=2, le=20)
    min_liquidity_crore: float | None = Field(default=None, ge=0.0)
    limit: int = Field(default=1500, ge=1, le=5000)


PullBackMaMode = Literal["either", "ema10", "ema20"]


class PullBackScanRequest(BaseModel):
    enable_rs_rating: bool = True
    min_rs_rating: int = Field(default=70, ge=1, le=99)
    enable_first_leg_up: bool = True
    min_first_leg_up_pct: float = Field(default=20.0, ge=0.0, le=200.0)
    enable_consolidation_range: bool = True
    max_consolidation_range_pct: float = Field(default=8.0, ge=0.1, le=30.0)
    enable_consolidation_days: bool = True
    min_consolidation_days: int = Field(default=4, ge=2, le=20)
    enable_volume_contraction: bool = True
    max_recent_volume_vs_avg20: float = Field(default=1.0, ge=0.1, le=3.0)
    enable_ma_support: bool = True
    pullback_ma: PullBackMaMode = "ema20"
    max_ma_distance_pct: float = Field(default=2.0, ge=0.1, le=20.0)
    min_liquidity_crore: float | None = Field(default=None, ge=0.0)
    limit: int = Field(default=1500, ge=1, le=5000)


class ReturnsScanRequest(BaseModel):
    timeframe: Literal["1D", "1W", "1M", "3M"] = "1M"
    min_return_pct: float | None = None
    max_return_pct: float | None = None
    above_21_ema: bool = False
    above_50_ema: bool = False
    above_200_sma: bool = False
    # Optional: Consolidation after first leg up
    enable_first_leg_up: bool = False
    min_first_leg_up_pct: float = Field(default=15.0, ge=0.0, le=500.0)
    enable_consolidation_filter: bool = False
    max_drawdown_after_leg_up: float = Field(default=8.0, ge=0.1, le=50.0)
    max_consolidation_range_pct: float = Field(default=8.0, ge=0.1, le=30.0)
    min_consolidation_days: int = Field(default=4, ge=2, le=20)
    # Optional: Volume dry up against 50-day MA
    enable_volume_contraction: bool = False
    max_volume_vs_50d_avg: float = Field(default=0.85, ge=0.1, le=1.0)
    # Optional: Single day price move filter
    enable_price_move_filter: bool = False
    min_price_move_pct: float = Field(default=1.0, ge=0.1, le=50.0)
    max_price_move_pct: float = Field(default=10.0, ge=0.1, le=100.0)
    min_liquidity_crore: float | None = Field(default=None, ge=0.0)
    limit: int = Field(default=1500, ge=1, le=5000)


class ConsolidatingScanRequest(BaseModel):
    enable_run_up_consolidation: bool = True
    enable_near_multi_year_breakout: bool = True
    min_liquidity_crore: float | None = Field(default=None, ge=0.0)
    limit: int = Field(default=1500, ge=1, le=5000)


class StockOverview(BaseModel):
    symbol: str
    name: str
    exchange: str
    sector: str
    sub_sector: str
    circuit_band_label: str | None = None
    upper_circuit_limit: float | None = None
    lower_circuit_limit: float | None = None
    market_cap_crore: float
    last_price: float
    change_pct: float
    relative_volume: float
    avg_rupee_volume_30d_crore: float
    rs_rating: int | None = None
    rs_rating_1d_ago: int | None = None
    rs_rating_1w_ago: int | None = None
    rs_rating_1m_ago: int | None = None
    nifty_outperformance: float
    sector_outperformance: float
    three_month_rs: float
    stock_return_5d: float
    stock_return_20d: float
    stock_return_60d: float
    stock_return_126d: float
    stock_return_12m: float
    adr_pct_20: float
    pct_from_52w_high: float
    pct_from_ath: float
    pct_from_52w_low: float
    gap_pct: float


CustomScanPattern = Literal[
    "any",
    "consolidating",
    "breakout-ath",
    "breakout-52w",
    "breakout-range",
    "volume-price",
    "strong-nifty",
    "strong-sector",
    "clean-pullback",
    "darvas-box",
    "pivot-breakout",
    "relative-strength",
]

CustomSortBy = Literal[
    "pattern",
    "price",
    "change_pct",
    "listing_date",
    "relative_volume",
    "relative_strength",
    "rs_rating",
    "three_month_rs",
    "stock_return_20d",
    "stock_return_60d",
    "stock_return_12m",
    "market_cap",
    "avg_rupee_volume",
]
SortOrder = Literal["asc", "desc"]
PriceVsMaMode = Literal["any", "above", "below"]
MaKey = Literal["ema10", "ema20", "ema50", "ema200"]
ReturnPeriod = Literal["1D", "1W", "1M", "3M", "6M", "1Y"]
NearHighPeriod = Literal["1M", "3M", "6M", "52W", "ATH"]


class CustomScanRequest(BaseModel):
    min_price: float | None = None
    max_price: float | None = None
    listing_date_from: date | None = None
    listing_date_to: date | None = None
    min_change_pct: float | None = None
    max_change_pct: float | None = None
    min_relative_volume: float | None = None
    min_nifty_outperformance: float | None = None
    min_sector_outperformance: float | None = None
    min_rs_rating: int | None = Field(default=None, ge=1, le=99)
    max_rs_rating: int | None = Field(default=None, ge=1, le=99)
    min_stock_return_20d: float | None = None
    min_stock_return_60d: float | None = None
    min_market_cap_crore: float | None = None
    max_market_cap_crore: float | None = None
    min_trend_strength: float | None = None
    max_pullback_depth_pct: float | None = None
    min_avg_rupee_volume_30d_crore: float | None = None
    min_avg_rupee_turnover_20d_crore: float | None = None
    min_pct_from_52w_low: float | None = None
    max_pct_from_52w_low: float | None = None
    min_pct_from_52w_high: float | None = None
    max_pct_from_52w_high: float | None = None
    min_pct_from_ath: float | None = None
    max_pct_from_ath: float | None = None
    min_gap_pct: float | None = None
    max_gap_pct: float | None = None
    min_day_range_pct: float | None = None
    max_day_range_pct: float | None = None
    min_three_month_rs: float | None = None
    near_high_period: NearHighPeriod | None = None
    near_high_max_distance_pct: float | None = None
    price_vs_ma_mode: PriceVsMaMode = "any"
    
    # Fundamental filters
    min_eps_growth_yoy: float | None = None
    min_revenue_growth_yoy: float | None = None
    min_operating_margin: float | None = None
    min_profit_margin: float | None = None
    min_roe: float | None = None
    max_peg_ratio: float | None = None
    min_pe_ratio: float | None = None
    max_pe_ratio: float | None = None
    
    # Technical Guru & Shakeout filters
    minervini_trend_template: bool = False
    kullamagi_setup: bool = False
    shakeout_21ema: bool = False
    shakeout_50ema: bool = False
    max_consolidation_range_pct: float | None = None

    price_vs_ma_key: MaKey = "ema20"
    require_bullish_ma_order: bool = False
    require_bearish_ma_order: bool = False
    price_to_ma_key: MaKey = "ema10"
    min_price_to_ma_ratio: float | None = None
    max_price_to_ma_ratio: float | None = None
    return_period: ReturnPeriod = "1Y"
    min_return_pct: float | None = None
    max_return_pct: float | None = None
    above_ema20: bool = False
    above_ema50: bool = False
    above_ema200: bool = False
    pattern: CustomScanPattern = "any"
    sort_by: CustomSortBy = "pattern"
    sort_order: SortOrder = "desc"
    limit: int = Field(default=1500, ge=1, le=5000)


class ChartResponse(BaseModel):
    symbol: str
    timeframe: str
    bars: list[ChartBar]
    summary: StockOverview | None = None
    rs_line: list[ChartLinePoint] = Field(default_factory=list)
    rs_line_markers: list[ChartLineMarker] = Field(default_factory=list)


class FundamentalsResponse(CompanyFundamentals):
    pass


SectorSortBy = Literal["1D", "1W", "1M", "3M", "6M", "1Y", "2Y"]
SectorGroupKind = Literal["sector", "index"]
ChartGridTimeframe = Literal["3M", "6M", "1Y", "2Y"]
ImprovingRsWindow = Literal["1D", "1W", "1M"]


class SectorCompanyItem(BaseModel):
    symbol: str
    name: str
    exchange: str
    sector: str
    sub_sector: str
    market_cap_crore: float
    last_price: float
    return_1d: float
    return_1w: float
    return_1m: float
    return_3m: float
    return_6m: float
    return_1y: float
    return_2y: float
    rs_rating: int | None = None


class SectorGroup(BaseModel):
    sub_sector: str
    company_count: int
    companies: list[SectorCompanyItem]


class SectorCard(BaseModel):
    group_kind: SectorGroupKind = "sector"
    sector: str
    company_count: int
    sub_sector_count: int
    last_price: float | None = None
    return_1d: float
    return_1w: float
    return_1m: float
    return_3m: float
    return_6m: float
    return_1y: float
    return_2y: float
    sparkline: list[ChartLinePoint] = Field(default_factory=list)
    sub_sectors: list[SectorGroup]


class SectorTabResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_sectors: int
    sort_by: SectorSortBy
    sort_order: SortOrder
    sectors: list[SectorCard]


class ChartGridCard(BaseModel):
    symbol: str
    name: str
    exchange: str
    sector: str
    sub_sector: str
    market_cap_crore: float
    last_price: float
    change_pct: float
    return_1d: float
    return_1w: float
    return_1m: float
    return_3m: float
    return_6m: float
    return_1y: float
    return_2y: float
    rs_rating: int | None = None
    weight_pct: float | None = None
    sparkline: list[ChartLinePoint] = Field(default_factory=list)


class ChartGridResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    name: str
    group_kind: SectorGroupKind
    timeframe: ChartGridTimeframe
    total_items: int
    cards: list[ChartGridCard]


class ChartGridSeriesItem(BaseModel):
    symbol: str
    bars: list[ChartBar] = Field(default_factory=list)


class ChartGridSeriesResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    timeframe: ChartGridTimeframe
    total_items: int
    items: list[ChartGridSeriesItem]


class IndustryGroupFilters(BaseModel):
    min_market_cap_cr: float
    min_avg_daily_value_cr: float


class IndustryGroupTopStock(BaseModel):
    symbol: str
    company_name: str
    rs_rating: int | None = None
    return_1m: float
    return_3m: float
    return_6m: float
    relative_return_3m: float
    relative_return_6m: float


class IndustryGroupMasterItem(BaseModel):
    group_id: str
    group_name: str
    parent_sector: str
    description: str
    stock_count: int
    symbols: list[str]


class IndustryGroupStockItem(BaseModel):
    symbol: str
    company_name: str
    exchange: str
    market_cap_cr: float
    avg_traded_value_50d_cr: float
    sector: str
    raw_industry: str
    final_group_id: str
    final_group_name: str
    last_price: float
    change_pct: float
    return_1m: float
    return_3m: float
    return_6m: float
    return_1y: float
    rs_rating: int | None = None


class IndustryGroupRankItem(BaseModel):
    rank: int
    rank_label: str
    rank_change_1w: int | None = None
    score_change_1w: float | None = None
    strength_bucket: str
    trend_label: str
    group_id: str
    group_name: str
    parent_sector: str
    description: str
    stock_count: int
    score: float
    return_1m: float
    return_3m: float
    return_6m: float
    relative_return_1m: float
    relative_return_3m: float
    relative_return_6m: float
    median_return_1m: float
    median_return_3m: float
    median_return_6m: float
    pct_above_50dma: float
    pct_above_200dma: float
    pct_outperform_benchmark_3m: float
    pct_outperform_benchmark_6m: float
    breadth_score: float
    trend_health_score: float
    leaders: list[str]
    laggards: list[str]
    top_constituents: list[IndustryGroupTopStock]
    symbols: list[str]


class IndustryGroupsResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    as_of_date: str
    benchmark: str
    filters: IndustryGroupFilters
    total_groups: int
    groups: list[IndustryGroupRankItem]
    master: list[IndustryGroupMasterItem]
    stocks: list[IndustryGroupStockItem]


class ImprovingRsItem(BaseModel):
    symbol: str
    name: str
    exchange: str
    sector: str
    sub_sector: str
    market_cap_crore: float
    last_price: float
    change_pct: float
    rs_rating: int | None = None
    rs_rating_1d_ago: int | None = None
    rs_rating_1w_ago: int | None = None
    rs_rating_1m_ago: int | None = None
    improvement_1d: int | None = None
    improvement_1w: int | None = None
    improvement_1m: int | None = None


class ImprovingRsResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    window: ImprovingRsWindow
    total_hits: int
    items: list[ImprovingRsItem]


class UniverseBreadth(BaseModel):
    universe: str
    total: int
    advances: int
    declines: int
    unchanged: int
    above_ma20_pct: float
    above_ma50_pct: float
    above_sma200_pct: float
    ma20_above_ma50_pct: float
    ma50_above_ma200_pct: float
    new_high_52w_pct: float
    new_low_52w_pct: float
    rsi_14_overbought_pct: float
    rsi_14_oversold_pct: float


class HistoricalBreadthDataPoint(BaseModel):
    date: str
    above_ma20_pct: float
    above_ma50_pct: float
    above_sma200_pct: float
    new_high_52w_pct: float
    new_low_52w_pct: float


class HistoricalUniverseBreadth(BaseModel):
    universe: str
    history: list[HistoricalBreadthDataPoint]


class HistoricalBreadthResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    universes: list[HistoricalUniverseBreadth]


class MarketHealthResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    universes: list[UniverseBreadth]


class MarketMacroItem(BaseModel):
    symbol: str
    label: str
    price: float | None = None
    change_pct: float | None = None
    trailing_pe: float | None = None
    currency: str = "INR"


class MarketOverviewResponse(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    items: list[MarketMacroItem]


class IndexPePoint(BaseModel):
    date: str
    pe: float


class IndexPeHistoryResponse(BaseModel):
    symbol: str
    label: str
    points: list[IndexPePoint]
    avg_5y: float | None = None
    current_pe: float | None = None
    forward_pe: float | None = None
    source: str = "nse"


# ── Money Flow ──────────────────────────────────────────────────────────────

class MoneyFlowSector(BaseModel):
    name: str
    sentiment: str  # "bullish" | "bearish" | "neutral"
    reason: str
    magnitude: str  # "strong" | "moderate" | "mild"


class MoneyFlowReport(BaseModel):
    week_key: str  # "YYYY-WNN" e.g. "2026-W13"
    week_start: str  # ISO date string
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    inflows: list[MoneyFlowSector]
    outflows: list[MoneyFlowSector]
    sector_performance: list[MoneyFlowSector]
    short_term_headwinds: list[MoneyFlowSector]
    short_term_tailwinds: list[MoneyFlowSector]
    long_term_tailwinds: list[MoneyFlowSector]
    macro_summary: str
    ai_model: str = "gemini"


class MoneyFlowHistoryResponse(BaseModel):
    reports: list[MoneyFlowReport]
    latest_week_key: str | None = None


class MoneyFlowStockIdea(BaseModel):
    symbol: str
    name: str
    exchange: str
    sector: str
    sub_sector: str
    recommendation_type: Literal["consolidation", "value"]
    last_price: float
    change_pct: float
    market_cap_crore: float
    rs_rating: int | None = None
    relative_volume: float | None = None
    stock_return_20d: float | None = None
    stock_return_60d: float | None = None
    stock_return_12m: float | None = None
    pct_from_52w_high: float | None = None
    pct_from_ath: float | None = None
    pullback_depth_pct: float | None = None
    setup_score: float
    setup_summary: str
    thesis: str
    future_growth_summary: str
    recent_quarter_summary: str
    valuation_summary: str | None = None
    recent_developments: list[str] = Field(default_factory=list)
    growth_drivers: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    key_metrics: dict[str, float | str] = Field(default_factory=dict)


class MoneyFlowStockIdeasResponse(BaseModel):
    recommendation_date: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    next_update_at: datetime
    consolidating_ideas: list[MoneyFlowStockIdea] = Field(default_factory=list)
    value_ideas: list[MoneyFlowStockIdea] = Field(default_factory=list)
    ai_model: str | None = None


class MoneyFlowStockIdeasHistoryResponse(BaseModel):
    reports: list[MoneyFlowStockIdeasResponse] = Field(default_factory=list)
    latest_recommendation_date: str | None = None


class CompanyQuestionRequest(BaseModel):
    symbol: str
    question: str


class CompanyQuestionResponse(BaseModel):
    symbol: str
    question: str
    answer: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    ai_model: str | None = None


class WatchlistItem(BaseModel):
    id: str
    name: str
    color: str
    symbols: list[str] = Field(default_factory=list)


class WatchlistsStateResponse(BaseModel):
    market: Literal["india", "us"]
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    active_watchlist_id: str | None = None
    watchlists: list[WatchlistItem] = Field(default_factory=list)


# ── Sector Rotation ──────────────────────────────────────────────────────────

class SectorRotationStock(BaseModel):
    symbol: str
    name: str
    rs_rating: int
    return_1d: float
    return_1w: float
    return_1m: float


class SectorRotationItem(BaseModel):

    sector: str
    total_stocks: int
    top_gainers_1d: int
    top_gainers_1w: int
    top_gainers_1m: int
    pct_top_gainers_1d: float
    pct_top_gainers_1w: float
    pct_top_gainers_1m: float
    avg_return_1d: float
    avg_return_1w: float
    avg_return_1m: float
    rank_1d: int
    rank_1w: int
    rank_1m: int
    stocks: list[SectorRotationStock] = Field(default_factory=list)


class SectorRotationResponse(BaseModel):
    sectors: list[SectorRotationItem]
    generated_at: datetime
