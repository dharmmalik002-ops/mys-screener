export type ScanDescriptor = {
  id: string;
  name: string;
  category: string;
  description: string;
  hit_count: number;
};

export type ScanMatch = {
  scan_id: string;
  symbol: string;
  name: string;
  exchange: string;
  listing_date?: string | null;
  sector: string;
  sub_sector?: string | null;
  market_cap_crore: number;
  last_price: number;
  change_pct: number;
  relative_volume: number;
  avg_rupee_volume_30d_crore?: number | null;
  score: number;
  pattern?: string | null;
  rs_rating?: number | null;
  rs_rating_1m_ago?: number | null;
  nifty_outperformance?: number | null;
  sector_outperformance?: number | null;
  three_month_rs?: number | null;
  stock_return_20d?: number | null;
  stock_return_60d?: number | null;
  stock_return_12m?: number | null;
  gap_pct?: number | null;
  reasons: string[];
};

export type AlertItem = {
  id: string;
  symbol: string;
  scan_name: string;
  message: string;
  created_at: string;
};

export type DashboardResponse = {
  app_name: string;
  generated_at: string;
  market_status: string;
  data_mode: "demo" | "upstox" | "free";
  market_cap_min_crore: number;
  universe_count: number;
  scanners: ScanDescriptor[];
  popular_scan_ids: string[];
  top_gainers: ScanMatch[];
  top_losers: ScanMatch[];
  top_volume_spikes: ScanMatch[];
  recent_alerts: AlertItem[];
};

export type IndexQuoteItem = {
  symbol: string;
  price: number;
  change_pct: number;
  updated_at: string;
};

export type IndexQuotesResponse = {
  generated_at: string;
  items: IndexQuoteItem[];
};

export type ScanResultsResponse = {
  scan: ScanDescriptor;
  generated_at: string;
  market_cap_min_crore: number;
  total_hits: number;
  items: ScanMatch[];
  sector_summaries: ScanSectorSummary[];
};

export type AiScanResponse = {
  results: ScanResultsResponse;
  parsed_request: CustomScanRequest;
};

export type ScanSectorSummary = {
  sector: string;
  current_hits: number;
  prior_week_hits: number;
  prior_month_hits: number;
  sector_return_1w: number;
  sector_return_1m: number;
};

export type UniverseBreadth = {
  universe: string;
  total: number;
  advances: number;
  declines: number;
  unchanged: number;
  above_ma20_pct: number;
  above_ma50_pct: number;
  above_sma200_pct: number;
  ma20_above_ma50_pct: number;
  ma50_above_ma200_pct: number;
  new_high_52w_pct: number;
  new_low_52w_pct: number;
  rsi_14_overbought_pct: number;
  rsi_14_oversold_pct: number;
};

export type MarketHealthResponse = {
  generated_at: string;
  universes: UniverseBreadth[];
};

export type HistoricalBreadthDataPoint = {
  date: string;
  above_ma20_pct: number;
  above_ma50_pct: number;
  above_sma200_pct: number;
  new_high_52w_pct: number;
  new_low_52w_pct: number;
};

export type HistoricalUniverseBreadth = {
  universe: string;
  history: HistoricalBreadthDataPoint[];
};

export type HistoricalBreadthResponse = {
  generated_at: string;
  universes: HistoricalUniverseBreadth[];
};

export type WatchlistItem = {
  id: string;
  name: string;
  color: string;
  symbols: string[];
};

export type WatchlistsStateResponse = {
  market: MarketKey;
  updated_at: string;
  active_watchlist_id: string | null;
  watchlists: WatchlistItem[];
};

type ScanRequestOptions = {
  includeSectorSummaries?: boolean;
  minLiquidityCrore?: number | null;
};

export type MarketKey = "india" | "us";

export type ChartBar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type ChartLinePoint = {
  time: number;
  value: number;
};

export type ChartLineMarker = {
  time: number;
  value: number;
  label: string;
  color: string;
};

export type ChartResponse = {
  symbol: string;
  timeframe: string;
  bars: ChartBar[];
  summary: StockOverview | null;
  rs_line: ChartLinePoint[];
  rs_line_markers: ChartLineMarker[];
};

export type ChartGridTimeframe = "3M" | "6M" | "1Y" | "2Y";

export type ChartGridCard = {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  sub_sector: string;
  market_cap_crore: number;
  last_price: number;
  change_pct: number;
  return_1d: number;
  return_1w: number;
  return_1m: number;
  return_3m: number;
  return_6m: number;
  return_1y: number;
  return_2y: number;
  rs_rating: number | null;
  weight_pct: number | null;
  sparkline: ChartLinePoint[];
};

export type ChartGridResponse = {
  generated_at: string;
  name: string;
  group_kind: "sector" | "index";
  timeframe: ChartGridTimeframe;
  total_items: number;
  cards: ChartGridCard[];
};

export type ChartGridSeriesItem = {
  symbol: string;
  bars: ChartBar[];
};

export type ChartGridSeriesResponse = {
  generated_at: string;
  timeframe: ChartGridTimeframe;
  total_items: number;
  items: ChartGridSeriesItem[];
};

export type QuarterlyResultItem = {
  period: string;
  sales_crore: number | null;
  expenses_crore: number | null;
  operating_profit_crore: number | null;
  operating_margin_pct: number | null;
  profit_before_tax_crore: number | null;
  net_profit_crore: number | null;
  eps: number | null;
  result_document_url: string | null;
};

export type ProfitLossItem = {
  period: string;
  sales_crore: number | null;
  operating_profit_crore: number | null;
  operating_margin_pct: number | null;
  net_profit_crore: number | null;
  eps: number | null;
  dividend_payout_pct: number | null;
};

export type GrowthSnapshot = {
  latest_period: string | null;
  sales_qoq_pct: number | null;
  sales_yoy_pct: number | null;
  profit_qoq_pct: number | null;
  profit_yoy_pct: number | null;
  operating_margin_latest_pct: number | null;
  operating_margin_previous_pct: number | null;
  net_margin_latest_pct: number | null;
  net_margin_previous_pct: number | null;
};

export type ValuationSnapshot = {
  market_cap_crore: number | null;
  pe_ratio: number | null;
  peg_ratio: number | null;
  operating_margin_pct: number | null;
  net_margin_pct: number | null;
  roce_pct: number | null;
  roe_pct: number | null;
  dividend_yield_pct: number | null;
};

export type ShareholdingPatternItem = {
  period: string;
  promoter_pct: number | null;
  fii_pct: number | null;
  dii_pct: number | null;
  public_pct: number | null;
  shareholder_count: number | null;
};

export type ShareholdingDelta = {
  latest_period: string | null;
  previous_period: string | null;
  promoter_change_pct: number | null;
  fii_change_pct: number | null;
  dii_change_pct: number | null;
  public_change_pct: number | null;
};

export type GrowthDriver = {
  title: string;
  detail: string;
  tone: "positive" | "neutral" | "watch";
};

export type CompanyUpdateItem = {
  title: string;
  source: string;
  published_at: string | null;
  summary: string | null;
  link: string | null;
  kind: "results" | "concall" | "news" | "holding" | "filing";
};

export type CompanyFundamentals = {
  symbol: string;
  name: string;
  exchange: string | null;
  sector: string | null;
  sub_sector: string | null;
  fetched_at: string;
  about: string | null;
  business_summary: string | null;
  company_website: string | null;
  headquarters: string | null;
  quarterly_results: QuarterlyResultItem[];
  profit_loss: ProfitLossItem[];
  balance_sheet: BalanceSheetItem[];
  cash_flow: CashFlowItem[];
  financial_ratios: FinancialRatios[];
  growth: GrowthSnapshot | null;
  valuation: ValuationSnapshot | null;
  growth_drivers: GrowthDriver[];
  management_team: Array<{ name: string; position: string; background?: string }>;
  management_guidance: ManagementGuidance[];
  strategy_and_outlook: string | null;
  competitive_position: CompetitivePosition | null;
  business_segments: BusinessSegment[];
  geographic_presence: string[];
  risks_and_opportunities: RiskAnalysis[];
  recent_updates: CompanyUpdateItem[];
  detailed_news: DetailedNews[];
  shareholding_pattern: ShareholdingPatternItem[];
  shareholding_delta: ShareholdingDelta | null;
  data_warnings: string[];
  ai_news_summary: AISummary | null;
  business_triggers: BusinessTrigger[];
  insider_transactions: InsiderTransaction[];
  last_news_update: string | null;
  latest_earnings_key_metrics: Record<string, number | string>;
  upcoming_events: Array<{ date: string; event: string; impact?: string }>;
};

export type BalanceSheetItem = {
  period: string;
  total_assets_crore: number | null;
  current_assets_crore: number | null;
  total_liabilities_crore: number | null;
  current_liabilities_crore: number | null;
  shareholders_equity_crore: number | null;
  debt_crore: number | null;
  cash_and_equivalents_crore: number | null;
  inventory_crore: number | null;
  receivables_crore: number | null;
};

export type CashFlowItem = {
  period: string;
  operating_cash_flow_crore: number | null;
  investing_cash_flow_crore: number | null;
  financing_cash_flow_crore: number | null;
  free_cash_flow_crore: number | null;
  capital_expenditure_crore: number | null;
  dividends_paid_crore: number | null;
};

export type FinancialRatios = {
  period: string;
  roe_pct: number | null;
  roa_pct: number | null;
  roce_pct: number | null;
  current_ratio: number | null;
  quick_ratio: number | null;
  debt_to_equity_ratio: number | null;
  debt_to_assets_ratio: number | null;
  interest_coverage: number | null;
  asset_turnover: number | null;
};

export type ManagementGuidance = {
  fiscal_year: string;
  revenue_growth_guidance_pct: number | null;
  ebitda_guidance_pct: number | null;
  eps_guidance: number | null;
  capex_guidance_crore: number | null;
  guidance_date: string | null;
  guidance_source: string | null;
  key_guidance_points: string[];
};

export type CompetitivePosition = {
  market_position: string | null;
  competitive_advantages: string[];
  market_share_estimate: number | null;
  key_competitors: string[];
};

export type BusinessSegment = {
  name: string;
  revenue_crore: number | null;
  revenue_pct: number | null;
  growth_pct: number | null;
  period: string;
};

export type DetailedNews = {
  title: string;
  summary: string;
  impact_category: string;
  sentiment: string;
  source: string;
  published_date: string;
  detailed_points: string[];
  relevance_score: number;
};

export type RiskAnalysis = {
  risk_category: string;
  description: string;
  severity: string;
  mitigation_strategy: string | null;
};

export type AISummary = {
  generated_at: string;
  summary: string;
  key_points: string[];
  sentiment: "positive" | "negative" | "neutral";
};

export type BusinessTrigger = {
  title: string;
  description: string;
  impact: "positive" | "negative" | "neutral";
  date: string;
  source: string;
  likelihood_to_impact: number;
};

export type InsiderTransaction = {
  person_name: string;
  position: string;
  transaction_type: "buy" | "sell";
  quantity: number;
  price_per_share: number;
  total_value_crore: number;
  date: string;
  pct_of_holding_change: number;
  remarks: string | null;
};

export type StockOverview = {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  sub_sector: string;
  circuit_band_label: string | null;
  upper_circuit_limit: number | null;
  lower_circuit_limit: number | null;
  market_cap_crore: number;
  last_price: number;
  change_pct: number;
  relative_volume: number;
  avg_rupee_volume_30d_crore: number;
  rs_rating: number | null;
  rs_rating_1d_ago: number;
  rs_rating_1w_ago: number;
  rs_rating_1m_ago: number;
  nifty_outperformance: number;
  sector_outperformance: number;
  three_month_rs: number;
  stock_return_5d: number;
  stock_return_20d: number;
  stock_return_60d: number;
  stock_return_126d: number;
  stock_return_12m: number;
  adr_pct_20: number;
  pct_from_52w_high: number;
  pct_from_ath: number;
  pct_from_52w_low: number;
  gap_pct: number;
};

export type CustomScanPattern =
  | "any"
  | "consolidating"
  | "breakout-ath"
  | "breakout-52w"
  | "breakout-range"
  | "volume-price"
  | "strong-nifty"
  | "strong-sector"
  | "clean-pullback"
  | "darvas-box"
  | "pivot-breakout"
  | "relative-strength";

export type CustomSortBy =
  | "pattern"
  | "price"
  | "change_pct"
  | "listing_date"
  | "relative_volume"
  | "relative_strength"
  | "rs_rating"
  | "three_month_rs"
  | "stock_return_20d"
  | "stock_return_60d"
  | "stock_return_12m"
  | "market_cap"
  | "avg_rupee_volume";

export type PriceVsMaMode = "any" | "above" | "below";
export type MaKey = "ema10" | "ema20" | "ema50" | "ema200";
export type ReturnPeriod = "1D" | "1W" | "1M" | "3M" | "6M" | "1Y";
export type NearHighPeriod = "1M" | "3M" | "6M" | "52W" | "ATH";
export type SectorSortBy = "1D" | "1W" | "1M" | "3M" | "6M" | "1Y" | "2Y";
export type ImprovingRsWindow = "1D" | "1W" | "1M";
export type PullBackMaMode = "either" | "ema10" | "ema20";

export type CustomScanRequest = {
  min_price: number | null;
  max_price: number | null;
  listing_date_from: string | null;
  listing_date_to: string | null;
  min_change_pct: number | null;
  max_change_pct: number | null;
  min_relative_volume: number | null;
  min_nifty_outperformance: number | null;
  min_sector_outperformance: number | null;
  min_rs_rating: number | null;
  max_rs_rating: number | null;
  min_stock_return_20d: number | null;
  min_stock_return_60d: number | null;
  min_market_cap_crore: number | null;
  max_market_cap_crore: number | null;
  min_trend_strength: number | null;
  max_pullback_depth_pct: number | null;
  min_avg_rupee_volume_30d_crore: number | null;
  min_avg_rupee_turnover_20d_crore: number | null;
  min_pct_from_52w_low: number | null;
  max_pct_from_52w_low: number | null;
  min_pct_from_52w_high: number | null;
  max_pct_from_52w_high: number | null;
  min_pct_from_ath: number | null;
  max_pct_from_ath: number | null;
  min_gap_pct: number | null;
  max_gap_pct: number | null;
  min_day_range_pct: number | null;
  max_day_range_pct: number | null;
  min_three_month_rs: number | null;
  near_high_period: NearHighPeriod | null;
  near_high_max_distance_pct: number | null;
  price_vs_ma_mode: PriceVsMaMode;
  price_vs_ma_key: MaKey;
  require_bullish_ma_order: boolean;
  require_bearish_ma_order: boolean;
  price_to_ma_key: MaKey;
  min_price_to_ma_ratio: number | null;
  max_price_to_ma_ratio: number | null;
  return_period: ReturnPeriod;
  min_return_pct: number | null;
  max_return_pct: number | null;
  above_ema20: boolean;
  above_ema50: boolean;
  above_ema200: boolean;
  pattern: CustomScanPattern;
  sort_by: CustomSortBy;
  sort_order: "asc" | "desc";
  limit: number;
};

export type SectorCompanyItem = {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  sub_sector: string;
  market_cap_crore: number;
  last_price: number;
  return_1d: number;
  return_1w: number;
  return_1m: number;
  return_3m: number;
  return_6m: number;
  return_1y: number;
  return_2y: number;
  rs_rating: number;
};

export type SectorGroup = {
  sub_sector: string;
  company_count: number;
  companies: SectorCompanyItem[];
};

export type SectorCard = {
  group_kind: "sector" | "index";
  sector: string;
  company_count: number;
  sub_sector_count: number;
  last_price?: number | null;
  return_1d: number;
  return_1w: number;
  return_1m: number;
  return_3m: number;
  return_6m: number;
  return_1y: number;
  return_2y: number;
  sparkline: ChartLinePoint[];
  sub_sectors: SectorGroup[];
};

export type SectorTabResponse = {
  generated_at: string;
  total_sectors: number;
  sort_by: SectorSortBy;
  sort_order: "asc" | "desc";
  sectors: SectorCard[];
};

export type IndustryGroupFilters = {
  min_market_cap_cr: number;
  min_avg_daily_value_cr: number;
};

export type IndustryGroupTopStock = {
  symbol: string;
  company_name: string;
  rs_rating: number | null;
  return_1m: number;
  return_3m: number;
  return_6m: number;
  relative_return_3m: number;
  relative_return_6m: number;
};

export type IndustryGroupMasterItem = {
  group_id: string;
  group_name: string;
  parent_sector: string;
  description: string;
  stock_count: number;
  symbols: string[];
};

export type IndustryGroupStockItem = {
  symbol: string;
  company_name: string;
  exchange: string;
  market_cap_cr: number;
  avg_traded_value_50d_cr: number;
  sector: string;
  raw_industry: string;
  final_group_id: string;
  final_group_name: string;
  last_price: number;
  change_pct: number;
  return_1m: number;
  return_3m: number;
  return_6m: number;
  return_1y: number;
  rs_rating: number | null;
};

export type IndustryGroupRankItem = {
  rank: number;
  rank_label: string;
  rank_change_1w: number | null;
  score_change_1w: number | null;
  strength_bucket: string;
  trend_label: string;
  group_id: string;
  group_name: string;
  parent_sector: string;
  description: string;
  stock_count: number;
  score: number;
  return_1m: number;
  return_3m: number;
  return_6m: number;
  relative_return_1m: number;
  relative_return_3m: number;
  relative_return_6m: number;
  median_return_1m: number;
  median_return_3m: number;
  median_return_6m: number;
  pct_above_50dma: number;
  pct_above_200dma: number;
  pct_outperform_benchmark_3m: number;
  pct_outperform_benchmark_6m: number;
  breadth_score: number;
  trend_health_score: number;
  leaders: string[];
  laggards: string[];
  top_constituents: IndustryGroupTopStock[];
  symbols: string[];
};

export type IndustryGroupsResponse = {
  generated_at: string;
  as_of_date: string;
  benchmark: string;
  filters: IndustryGroupFilters;
  total_groups: number;
  groups: IndustryGroupRankItem[];
  master: IndustryGroupMasterItem[];
  stocks: IndustryGroupStockItem[];
};

export type ImprovingRsItem = {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  sub_sector: string;
  market_cap_crore: number;
  last_price: number;
  change_pct: number;
  rs_rating: number;
  rs_rating_1d_ago: number;
  rs_rating_1w_ago: number;
  rs_rating_1m_ago: number;
  improvement_1d: number;
  improvement_1w: number;
  improvement_1m: number;
};

export type ImprovingRsResponse = {
  generated_at: string;
  window: ImprovingRsWindow;
  total_hits: number;
  items: ImprovingRsItem[];
};

export type NearPivotScanRequest = {
  min_rs_rating: number;
  max_pct_from_52w_high: number;
  max_consolidation_range_pct: number;
  min_consolidation_days: number;
  min_liquidity_crore: number | null;
  limit: number;
};

export type PullBackScanRequest = {
  enable_rs_rating: boolean;
  min_rs_rating: number;
  enable_first_leg_up: boolean;
  min_first_leg_up_pct: number;
  enable_consolidation_range: boolean;
  max_consolidation_range_pct: number;
  enable_consolidation_days: boolean;
  min_consolidation_days: number;
  enable_volume_contraction: boolean;
  max_recent_volume_vs_avg20: number;
  enable_ma_support: boolean;
  pullback_ma: PullBackMaMode;
  max_ma_distance_pct: number;
  min_liquidity_crore: number | null;
  limit: number;
};

export type ReturnsScanRequest = {
  timeframe: "1D" | "1W" | "1M" | "3M";
  min_return_pct: number | null;
  max_return_pct: number | null;
  above_21_ema: boolean;
  above_50_ema: boolean;
  above_200_sma: boolean;
  enable_first_leg_up: boolean;
  min_first_leg_up_pct: number;
  enable_consolidation_filter: boolean;
  max_drawdown_after_leg_up: number;
  max_consolidation_range_pct: number;
  min_consolidation_days: number;
  enable_volume_contraction: boolean;
  max_volume_vs_50d_avg: number;
  enable_price_move_filter: boolean;
  min_price_move_pct: number;
  max_price_move_pct: number;
  min_liquidity_crore: number | null;
  limit: number;
};

export type ConsolidatingScanRequest = {
  enable_run_up_consolidation: boolean;
  enable_near_multi_year_breakout: boolean;
  min_liquidity_crore: number | null;
  limit: number;
};

export type RefreshResponse = {
  ok: boolean;
  universe_count: number;
  market_cap_min_crore: number;
  refresh_mode:
    | "live-refresh"
    | "historical-refresh"
    | "historical-refresh-queued"
    | "cached-current"
    | "cache-fallback"
    | "timeout-fallback"
    | "error-fallback";
  message: string | null;
  snapshot_updated_at: string;
  snapshot_age_minutes: number;
  applied_quote_count: number;
  historical_rebuild: boolean;
  quote_source: string | null;
};

const API_BASE = import.meta.env.VITE_API_BASE || "https://dharmmalik-stock-scanner-backend.hf.space";

const FALLBACK_API_BASES = (import.meta.env.DEV
  ? ["", API_BASE, "http://127.0.0.1:8001", "http://localhost:8001", "http://127.0.0.1:8000", "http://localhost:8000"]
  : ["", API_BASE]
).filter((value, index, array) => array.indexOf(value) === index);

const RETRYABLE_STATUS_CODES = new Set([404, 502, 503, 504]);

function routeScopedMarket(): MarketKey | null {
  if (typeof window === "undefined") {
    return null;
  }

  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  if (pathname === "/us" || pathname.startsWith("/us/")) {
    return "us";
  }
  if (pathname === "/india" || pathname.startsWith("/india/")) {
    return "india";
  }
  return null;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let lastError: Error | null = null;

  for (const base of FALLBACK_API_BASES) {
    try {
      const response = await fetch(`${base}${path}`, {
        cache: "no-store",
        ...init,
      });

      if (!response.ok) {
        if (RETRYABLE_STATUS_CODES.has(response.status)) {
          lastError = new Error(`Request failed: ${response.status}`);
          continue;
        }
        throw new Error(`Request failed: ${response.status}`);
      }

      return response.json() as Promise<T>;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error("Failed to reach market data API");
    }
  }

  throw lastError ?? new Error("Failed to reach market data API");
}

function withMarket(path: string, market: MarketKey) {
  const scopedMarket = routeScopedMarket();
  if (scopedMarket === market && path.startsWith("/api/")) {
    return `/api/${market}${path.slice(4)}`;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}market=${market}`;
}

export function getDashboard(market: MarketKey) {
  return request<DashboardResponse>(withMarket("/api/dashboard", market));
}

export function getScanCounts(market: MarketKey) {
  return request<ScanDescriptor[]>(withMarket("/api/scan-counts", market));
}

function withScanOptions(path: string, market: MarketKey, options?: ScanRequestOptions) {
  const params = new URLSearchParams();
  params.set("market", market);
  if (options?.includeSectorSummaries) {
    params.set("include_sector_summaries", "true");
  }
  if (typeof options?.minLiquidityCrore === "number" && Number.isFinite(options.minLiquidityCrore)) {
    params.set("min_liquidity_crore", String(options.minLiquidityCrore));
  }
  const query = params.toString();
  if (!query) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}${query}`;
}

export function getScanResults(scanId: string, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions(`/api/scans/${scanId}`, market, options));
}

export function runCustomScan(body: CustomScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/custom-scan", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
}

export function runAiScan(query: string, market: MarketKey, options?: ScanRequestOptions) {
  return request<AiScanResponse>(withScanOptions("/api/ai-scan", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
  });
}

export function getChart(symbol: string, timeframe: string, market: MarketKey) {
  return request<ChartResponse>(`/api/chart/${encodeURIComponent(symbol)}?timeframe=${timeframe}&market=${market}`);
}

export function getChartHistory(symbol: string, timeframe: string, market: MarketKey) {
  return request<ChartResponse>(
    `/api/chart/${encodeURIComponent(symbol)}/history?timeframe=${timeframe}&market=${market}`,
  );
}

export function getChartGrid(name: string, groupKind: "sector" | "index", timeframe: ChartGridTimeframe, market: MarketKey) {
  return request<ChartGridResponse>(
    `/api/chart-grid?name=${encodeURIComponent(name)}&group_kind=${groupKind}&timeframe=${timeframe}&market=${market}`,
  );
}

export function getChartGridSeries(symbols: string[], timeframe: ChartGridTimeframe, market: MarketKey) {
  return request<ChartGridSeriesResponse>(
    `/api/chart-grid-series?symbols=${encodeURIComponent(symbols.join(","))}&timeframe=${timeframe}&market=${market}`,
  );
}

export function getFundamentals(symbol: string, market: MarketKey) {
  return request<CompanyFundamentals>(`/api/fundamentals/${symbol}?market=${market}`);
}

export function refreshMarketData(market: MarketKey) {
  return request<RefreshResponse>(withMarket("/api/refresh", market), {
    method: "POST",
  });
}

export function getIndexQuotes(symbols: string[], market: MarketKey) {
  return request<IndexQuotesResponse>(`/api/index-quotes?symbols=${encodeURIComponent(symbols.join(","))}&market=${market}`);
}

export type MarketMacroItem = {
  symbol: string;
  label: string;
  price: number | null;
  change_pct: number | null;
  trailing_pe: number | null;
  currency: string;
};

export type MarketOverviewResponse = {
  generated_at: string;
  items: MarketMacroItem[];
};

export function getMarketOverview(market: MarketKey) {
  return request<MarketOverviewResponse>(withMarket("/api/market-overview", market));
}

export type IndexPePoint = { date: string; pe: number };
export type IndexPeHistoryResponse = {
  symbol: string;
  label: string;
  points: IndexPePoint[];
  avg_5y: number | null;
  current_pe: number | null;
  forward_pe: number | null;
  source: "nse" | "proxy";
};

export function getIndexPeHistory(symbol: string, market: MarketKey) {
  return request<IndexPeHistoryResponse>(`/api/index-pe/${encodeURIComponent(symbol)}/history?market=${market}`);
}

export type MoneyFlowSector = {
  name: string;
  sentiment: "bullish" | "bearish" | "neutral";
  reason: string;
  magnitude: "strong" | "moderate" | "mild";
};

export type MoneyFlowReport = {
  week_key: string;
  week_start: string;
  generated_at: string;
  inflows: MoneyFlowSector[];
  outflows: MoneyFlowSector[];
  sector_performance: MoneyFlowSector[];
  short_term_headwinds: MoneyFlowSector[];
  short_term_tailwinds: MoneyFlowSector[];
  long_term_tailwinds: MoneyFlowSector[];
  macro_summary: string;
  ai_model: string;
};

export type MoneyFlowHistoryResponse = {
  reports: MoneyFlowReport[];
  latest_week_key: string | null;
};

export type MoneyFlowStockIdea = {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  sub_sector: string;
  recommendation_type: "consolidation" | "value";
  last_price: number;
  change_pct: number;
  market_cap_crore: number;
  rs_rating: number | null;
  relative_volume: number | null;
  stock_return_20d: number | null;
  stock_return_60d: number | null;
  stock_return_12m: number | null;
  pct_from_52w_high: number | null;
  pct_from_ath: number | null;
  pullback_depth_pct: number | null;
  setup_score: number;
  setup_summary: string;
  thesis: string;
  future_growth_summary: string;
  recent_quarter_summary: string;
  valuation_summary: string | null;
  recent_developments: string[];
  growth_drivers: string[];
  risk_flags: string[];
  key_metrics: Record<string, number | string>;
};

export type MoneyFlowStockIdeasResponse = {
  recommendation_date: string;
  generated_at: string;
  next_update_at: string;
  consolidating_ideas: MoneyFlowStockIdea[];
  value_ideas: MoneyFlowStockIdea[];
  ai_model: string | null;
};

export type MoneyFlowStockIdeasHistoryResponse = {
  reports: MoneyFlowStockIdeasResponse[];
  latest_recommendation_date: string | null;
};

export type CompanyQuestionResponse = {
  symbol: string;
  question: string;
  answer: string;
  generated_at: string;
  ai_model: string | null;
};

export function getMoneyFlowHistory(market: MarketKey) {
  return request<MoneyFlowHistoryResponse>(withMarket("/api/money-flow/history", market));
}

export function getMoneyFlowLatest(market: MarketKey) {
  return request<MoneyFlowReport>(withMarket("/api/money-flow/latest", market));
}

export function generateMoneyFlow(market: MarketKey) {
  return request<MoneyFlowReport>(withMarket("/api/money-flow/generate", market), { method: "POST" });
}

export function getMoneyFlowStocks(market: MarketKey) {
  return request<MoneyFlowStockIdeasResponse>(withMarket("/api/money-flow/stocks/latest", market));
}

export function getMoneyFlowStockHistory(market: MarketKey) {
  return request<MoneyFlowStockIdeasHistoryResponse>(withMarket("/api/money-flow/stocks/history", market));
}

export function generateMoneyFlowStocks(market: MarketKey) {
  return request<MoneyFlowStockIdeasResponse>(withMarket("/api/money-flow/stocks/generate", market), { method: "POST" });
}

export function askMoneyFlowCompanyQuestion(symbol: string, question: string, market: MarketKey) {
  return request<CompanyQuestionResponse>(withMarket("/api/money-flow/stocks/ask", market), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ symbol, question }),
  });
}

export type SectorRotationItem = {
  sector: string;
  total_stocks: number;
  top_gainers_1d: number;
  top_gainers_1w: number;
  top_gainers_1m: number;
  pct_top_gainers_1d: number;
  pct_top_gainers_1w: number;
  pct_top_gainers_1m: number;
  avg_return_1d: number;
  avg_return_1w: number;
  avg_return_1m: number;
  rank_1d: number;
  rank_1w: number;
  rank_1m: number;
  stocks: SectorRotationStock[];
};

export type SectorRotationStock = {
  symbol: string;
  name: string;
  rs_rating: number;
  return_1d: number;
  return_1w: number;
  return_1m: number;
};

export type SectorRotationResponse = {
  sectors: SectorRotationItem[];
  generated_at: string;
};

export function getSectorRotation(market: MarketKey) {
  return request<SectorRotationResponse>(withMarket("/api/sector-rotation", market));
}

export function getGapUpOpeners(
  minGapPct: number,
  market: MarketKey,
  minLiquidityCrore: number | null = null,
  options?: ScanRequestOptions,
) {
  const params = new URLSearchParams({
    min_gap_pct: String(minGapPct),
  });
  if (minLiquidityCrore !== null && Number.isFinite(minLiquidityCrore)) {
    params.set("min_liquidity_crore", String(minLiquidityCrore));
  }
  return request<ScanResultsResponse>(withScanOptions(`/api/gap-up-openers?${params.toString()}`, market, options));
}

export function getNearPivotScan(body: NearPivotScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/near-pivot", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
}

export function getPullBackScan(body: PullBackScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/pull-backs", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
}

export function getReturnsScan(body: ReturnsScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/returns", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
}

export function getConsolidatingScan(body: ConsolidatingScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/consolidating", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
}

export function getSectorTab(sortBy: SectorSortBy, sortOrder: "asc" | "desc", market: MarketKey) {
  return request<SectorTabResponse>(`/api/sectors?sort_by=${sortBy}&sort_order=${sortOrder}&market=${market}`);
}

export function getIndustryGroups(market: MarketKey) {
  return request<IndustryGroupsResponse>(withMarket("/api/groups", market));
}

export function getImprovingRs(window: ImprovingRsWindow, market: MarketKey) {
  return request<ImprovingRsResponse>(`/api/improving-rs?window=${window}&market=${market}`);
}

export function getMarketHealth(market: MarketKey) {
  return request<MarketHealthResponse>(withMarket("/api/market-health", market));
}

export function getHistoricalMarketHealth(market: MarketKey) {
  return request<HistoricalBreadthResponse>(withMarket("/api/market-health/history", market));
}

export function refreshHistoricalMarketHealth(market: MarketKey) {
  return request<HistoricalBreadthResponse>(withMarket("/api/market-health/history/refresh", market), {
    method: "POST",
  });
}

export function getWatchlistsState(market: MarketKey) {
  return request<WatchlistsStateResponse>(withMarket("/api/watchlists", market));
}

export function saveWatchlistsState(
  payload: Pick<WatchlistsStateResponse, "active_watchlist_id" | "watchlists">,
  market: MarketKey,
) {
  return request<WatchlistsStateResponse>(withMarket("/api/watchlists", market), {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      market,
      active_watchlist_id: payload.active_watchlist_id,
      watchlists: payload.watchlists,
    }),
  });
}
