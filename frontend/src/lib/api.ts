export type ScanDescriptor = {
  id: string;
  name: string;
  category: string;
  description: string;
  hit_count: number;
};

export type MomentumBurstPlan = {
  tag: string; // "Burst" | "10 EMA Setup" | "21 EMA Setup"
  rs_rating: number;
  burst_pct: number;
  burst_days: number;
  consolidation_days?: number | null;
  consolidation_range_pct?: number | null;
  dist_from_10ema_pct?: number | null;
  dist_from_21ema_pct?: number | null;
  volume_dryup_ratio?: number | null;
  giveback_pct?: number | null;
  entry?: number | null;
  stop?: number | null;
  risk_pct?: number | null;
  target_2r?: number | null;
  target_3r?: number | null;
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
  volume_push_date?: string | null;
  session_date?: string | null;
  new_since_prev?: boolean | null;
  also_in?: string[];
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
  momentum_burst?: MomentumBurstPlan | null;
};

export type AlertItem = {
  id: string;
  symbol: string;
  scan_name: string;
  message: string;
  created_at: string;
};

export type BreadthDayCounts = {
  date: string;
  advances: number;
  declines: number;
  unchanged: number;
  total: number;
};

export type XpBreadthPoint = {
  date: string;
  xp_score: number;
  regime: string;
  regime_color: string;
  warmup: boolean;
};

export type XpRegimeBand = {
  label: string;
  color: string;
  min: number | null;
  max: number | null;
};

export type XpBreadthScore = {
  date: string;
  xp_score: number;
  regime: string;
  regime_color: string;
  universe: string | null;
  history: XpBreadthPoint[];
  bands: XpRegimeBand[];
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
  breadth_today: BreadthDayCounts | null;
  breadth_history: BreadthDayCounts[];
  xp_breadth: XpBreadthScore | null;
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
  // Expansion-scanner-only overrides. The route ignores these for any other
  // scan_id, so the panel can leave them undefined when not relevant.
  expansionMinChangePct?: number | null;
  expansionMinRelativeVolume?: number | null;
  // Positive-earnings-scanner overrides.
  positiveEarningsMinCloseInRangePct?: number | null;
  positiveEarningsMinNextDayGapPct?: number | null;
  positiveEarningsMinDayRvol?: number | null;
  positiveEarningsMinReturn5dPct?: number | null;
  positiveEarningsLookbackDays?: number | null;
  // Volume-screener-only overrides (ignored by other scan_ids).
  volumeWindow?: "1m" | "3m" | "6m" | "1y" | null;
  volumeMinRvol?: number | null;
};

export type MarketKey = "india";

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
  earnings_markers: ChartLineMarker[];
  volume_markers: ChartLineMarker[];
  band_change_markers: ChartLineMarker[];
  band_history: BandHistorySegment[];
};

export type BandHistorySegment = {
  // ISO date the band became effective; null = before the first known revision.
  from_date: string | null;
  // Band percent (2/5/10/20); null = no fixed band (dynamic / F&O).
  band_pct: number | null;
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
  sales_qoq_pct?: number | null;
  sales_yoy_pct?: number | null;
  eps_qoq_pct?: number | null;
  eps_yoy_pct?: number | null;
  net_profit_qoq_pct?: number | null;
  net_profit_yoy_pct?: number | null;
};

export type CompanyEarningsSummary = {
  symbol: string;
  name: string;
  sector: string | null;
  sub_sector: string | null;
  fetched_at: string;
  source: string;
  valuation: Partial<ValuationSnapshot>;
  metrics: {
    pct_from_52w_high?: number | null;
    pct_from_52w_low?: number | null;
    adr_pct_20?: number | null;
    relative_volume?: number | null;
    turnover_1d_crore?: number | null;
    avg_turnover_50d_crore?: number | null;
  };
  quarterly_results: QuarterlyResultItem[];
  data_warnings: string[];
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

export type MomentumBurstScanRequest = {
  // Universe & liquidity (applied first)
  min_price: number;
  min_turnover_crore: number;
  exclude_surveillance: boolean;
  // Trend & RS context
  min_rs_rating: number;
  // Candidate types
  include_fresh_bursts: boolean;
  include_ema_setups: boolean;
  // Type A — fresh burst
  burst_min_gain_pct: number;
  burst_window_min: number;
  burst_window_max: number;
  burst_recency_sessions: number;
  burst_min_volume_ratio: number;
  // Type B — consolidation near the 10/21 EMA
  setup_min_move_pct: number;
  setup_move_window_min: number;
  setup_move_window_max: number;
  setup_move_lookback_sessions: number;
  consolidation_min_days: number;
  consolidation_max_days: number;
  consolidation_max_range_pct: number;
  ema_surf_distance_pct: number;
  max_giveback_10ema_pct: number;
  max_giveback_21ema_pct: number;
  volume_dryup_ratio: number;
  min_liquidity_crore: number | null;
  limit: number;
};

export const DEFAULT_MOMENTUM_BURST_REQUEST: MomentumBurstScanRequest = {
  min_price: 50,
  min_turnover_crore: 5,
  exclude_surveillance: true,
  min_rs_rating: 70,
  include_fresh_bursts: true,
  include_ema_setups: true,
  burst_min_gain_pct: 15,
  burst_window_min: 3,
  burst_window_max: 10,
  burst_recency_sessions: 5,
  burst_min_volume_ratio: 1.5,
  setup_min_move_pct: 20,
  setup_move_window_min: 5,
  setup_move_window_max: 15,
  setup_move_lookback_sessions: 30,
  consolidation_min_days: 3,
  consolidation_max_days: 15,
  consolidation_max_range_pct: 10,
  ema_surf_distance_pct: 4,
  max_giveback_10ema_pct: 33.33,
  max_giveback_21ema_pct: 50,
  volume_dryup_ratio: 0.7,
  min_liquidity_crore: null,
  limit: 1500,
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

const API_BASE = import.meta.env.VITE_API_BASE ?? "";
const DEFAULT_PRODUCTION_API_BASES = [
  "https://dharmmalik-stock-scanner-backend.hf.space",
];
const REQUEST_TIMEOUT_MS = (() => {
  const parsed = Number(import.meta.env.VITE_API_TIMEOUT_MS ?? 20000);
  if (!Number.isFinite(parsed) || parsed < 1000) {
    return 20000;
  }
  return parsed;
})();
const RETRY_BACKOFF_MS = 400;
const SAME_BASE_RETRY_ATTEMPTS = 2;
const SAME_BASE_RETRY_BACKOFF_MS = 1500;
function defaultApiBases() {
  const isBrowser = typeof window !== "undefined";
  const hostname = isBrowser ? window.location.hostname.toLowerCase() : "";
  const localhostBases = [
    "",
    API_BASE,
    "http://127.0.0.1:8001",
    "http://localhost:8001",
    "http://127.0.0.1:8000",
    "http://localhost:8000",
  ];

  if (hostname === "localhost" || hostname === "127.0.0.1") {
    return localhostBases;
  }

  // Production: only the Vercel rewrite ("") and the HF Space directly.
  // Localhost fallbacks would always TypeError from a browser on HTTPS
  // (mixed-content + connection refused), turning a transient HF blip into
  // a misleading "Network request failed, retrying..." message.
  return ["", API_BASE, ...DEFAULT_PRODUCTION_API_BASES];
}

const FALLBACK_API_BASES = defaultApiBases().filter(
  (value, index, array) => array.indexOf(value) === index,
);
// 500 is included because HF Spaces occasionally return a transient 500 when
// the Uvicorn worker is mid-recycle or a cache miss races a snapshot reload —
// almost always self-healing within a second or two. 404 is included because
// after a Space cold-start, the route table can briefly 404 before the
// FastAPI app finishes booting.
const RETRYABLE_STATUS_CODES = new Set([404, 500, 502, 503, 504]);
const SAME_BASE_RETRY_STATUS_CODES = new Set([500, 502, 503, 504]);
let preferredApiBase: string | null = null;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function readString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function readNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function readNumber(value: unknown, fallback = 0): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readNullableNumber(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function readStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function mapArray<T>(value: unknown, mapper: (item: unknown) => T): T[] {
  return Array.isArray(value) ? value.map(mapper) : [];
}

function normalizeScanDescriptor(value: unknown): ScanDescriptor {
  const raw = isRecord(value) ? value : {};
  return {
    id: readString(raw.id),
    name: readString(raw.name),
    category: readString(raw.category),
    description: readString(raw.description),
    hit_count: readNumber(raw.hit_count),
  };
}

function normalizeMomentumBurstPlan(value: unknown): MomentumBurstPlan | null {
  if (!isRecord(value)) return null;
  return {
    tag: readString(value.tag),
    rs_rating: readNumber(value.rs_rating),
    burst_pct: readNumber(value.burst_pct),
    burst_days: readNumber(value.burst_days),
    consolidation_days: readNullableNumber(value.consolidation_days),
    consolidation_range_pct: readNullableNumber(value.consolidation_range_pct),
    dist_from_10ema_pct: readNullableNumber(value.dist_from_10ema_pct),
    dist_from_21ema_pct: readNullableNumber(value.dist_from_21ema_pct),
    volume_dryup_ratio: readNullableNumber(value.volume_dryup_ratio),
    giveback_pct: readNullableNumber(value.giveback_pct),
    entry: readNullableNumber(value.entry),
    stop: readNullableNumber(value.stop),
    risk_pct: readNullableNumber(value.risk_pct),
    target_2r: readNullableNumber(value.target_2r),
    target_3r: readNullableNumber(value.target_3r),
  };
}

function normalizeScanMatch(value: unknown): ScanMatch {
  const raw = isRecord(value) ? value : {};
  return {
    scan_id: readString(raw.scan_id),
    symbol: readString(raw.symbol),
    name: readString(raw.name),
    exchange: readString(raw.exchange),
    listing_date: readNullableString(raw.listing_date),
    sector: readString(raw.sector, "Unclassified"),
    sub_sector: readNullableString(raw.sub_sector),
    market_cap_crore: readNumber(raw.market_cap_crore),
    last_price: readNumber(raw.last_price),
    change_pct: readNumber(raw.change_pct),
    relative_volume: readNumber(raw.relative_volume),
    avg_rupee_volume_30d_crore: readNullableNumber(raw.avg_rupee_volume_30d_crore),
    score: readNumber(raw.score),
    pattern: readNullableString(raw.pattern),
    volume_push_date: readNullableString(raw.volume_push_date),
    session_date: readNullableString(raw.session_date),
    new_since_prev: typeof raw.new_since_prev === "boolean" ? raw.new_since_prev : null,
    also_in: Array.isArray(raw.also_in) ? raw.also_in.filter((v): v is string => typeof v === "string") : [],
    rs_rating: readNullableNumber(raw.rs_rating),
    rs_rating_1m_ago: readNullableNumber(raw.rs_rating_1m_ago),
    nifty_outperformance: readNullableNumber(raw.nifty_outperformance),
    sector_outperformance: readNullableNumber(raw.sector_outperformance),
    three_month_rs: readNullableNumber(raw.three_month_rs),
    stock_return_20d: readNullableNumber(raw.stock_return_20d),
    stock_return_60d: readNullableNumber(raw.stock_return_60d),
    stock_return_12m: readNullableNumber(raw.stock_return_12m),
    gap_pct: readNullableNumber(raw.gap_pct),
    reasons: readStringArray(raw.reasons),
    momentum_burst: normalizeMomentumBurstPlan(raw.momentum_burst),
  };
}

function normalizeAlertItem(value: unknown): AlertItem {
  const raw = isRecord(value) ? value : {};
  return {
    id: readString(raw.id),
    symbol: readString(raw.symbol),
    scan_name: readString(raw.scan_name),
    message: readString(raw.message),
    created_at: readString(raw.created_at),
  };
}

export function normalizeDashboardResponse(value: unknown): DashboardResponse {
  const raw = isRecord(value) ? value : {};
  const dataMode = raw.data_mode === "demo" || raw.data_mode === "upstox" || raw.data_mode === "free"
    ? raw.data_mode
    : "free";
  return {
    app_name: readString(raw.app_name, "Stock Scanner"),
    generated_at: readString(raw.generated_at),
    market_status: readString(raw.market_status),
    data_mode: dataMode,
    market_cap_min_crore: readNumber(raw.market_cap_min_crore),
    universe_count: readNumber(raw.universe_count),
    scanners: mapArray(raw.scanners, normalizeScanDescriptor),
    popular_scan_ids: readStringArray(raw.popular_scan_ids),
    top_gainers: mapArray(raw.top_gainers, normalizeScanMatch),
    top_losers: mapArray(raw.top_losers, normalizeScanMatch),
    top_volume_spikes: mapArray(raw.top_volume_spikes, normalizeScanMatch),
    recent_alerts: mapArray(raw.recent_alerts, normalizeAlertItem),
    breadth_today: normalizeBreadthDayCounts(raw.breadth_today),
    breadth_history: mapArray(raw.breadth_history, (item) => normalizeBreadthDayCounts(item) ?? {
      date: "", advances: 0, declines: 0, unchanged: 0, total: 0,
    }).filter((item) => item.date !== ""),
    xp_breadth: normalizeXpBreadthScore(raw.xp_breadth),
  };
}

function normalizeXpBreadthScore(value: unknown): XpBreadthScore | null {
  if (!isRecord(value)) return null;
  const date = readString(value.date);
  if (!date) return null;
  return {
    date,
    xp_score: readNumber(value.xp_score),
    regime: readString(value.regime),
    regime_color: readString(value.regime_color, "#888888"),
    universe: value.universe == null ? null : readString(value.universe),
    history: mapArray(value.history, (item) => {
      const raw = isRecord(item) ? item : {};
      return {
        date: readString(raw.date),
        xp_score: readNumber(raw.xp_score),
        regime: readString(raw.regime),
        regime_color: readString(raw.regime_color, "#888888"),
        warmup: raw.warmup === true,
      } as XpBreadthPoint;
    }).filter((item) => item.date !== ""),
    bands: mapArray(value.bands, (item) => {
      const raw = isRecord(item) ? item : {};
      return {
        label: readString(raw.label),
        color: readString(raw.color, "#888888"),
        min: raw.min == null ? null : readNumber(raw.min),
        max: raw.max == null ? null : readNumber(raw.max),
      } as XpRegimeBand;
    }),
  };
}

function normalizeBreadthDayCounts(value: unknown): BreadthDayCounts | null {
  if (!isRecord(value)) return null;
  return {
    date: readString(value.date),
    advances: readNumber(value.advances),
    declines: readNumber(value.declines),
    unchanged: readNumber(value.unchanged),
    total: readNumber(value.total),
  };
}

function normalizeIndexQuoteItem(value: unknown): IndexQuoteItem {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    price: readNumber(raw.price),
    change_pct: readNumber(raw.change_pct),
    updated_at: readString(raw.updated_at),
  };
}

function normalizeIndexQuotesResponse(value: unknown): IndexQuotesResponse {
  const raw = isRecord(value) ? value : {};
  return {
    generated_at: readString(raw.generated_at),
    items: mapArray(raw.items, normalizeIndexQuoteItem),
  };
}

function normalizeScanSectorSummary(value: unknown): ScanSectorSummary {
  const raw = isRecord(value) ? value : {};
  return {
    sector: readString(raw.sector, "Unclassified"),
    current_hits: readNumber(raw.current_hits),
    prior_week_hits: readNumber(raw.prior_week_hits),
    prior_month_hits: readNumber(raw.prior_month_hits),
    sector_return_1w: readNumber(raw.sector_return_1w),
    sector_return_1m: readNumber(raw.sector_return_1m),
  };
}

function normalizeScanResultsResponse(value: unknown): ScanResultsResponse {
  const raw = isRecord(value) ? value : {};
  return {
    scan: normalizeScanDescriptor(raw.scan),
    generated_at: readString(raw.generated_at),
    market_cap_min_crore: readNumber(raw.market_cap_min_crore),
    total_hits: readNumber(raw.total_hits),
    items: mapArray(raw.items, normalizeScanMatch),
    sector_summaries: mapArray(raw.sector_summaries, normalizeScanSectorSummary),
  };
}

function normalizeChartBar(value: unknown): ChartBar {
  const raw = isRecord(value) ? value : {};
  return {
    time: readNumber(raw.time),
    open: readNumber(raw.open),
    high: readNumber(raw.high),
    low: readNumber(raw.low),
    close: readNumber(raw.close),
    volume: readNumber(raw.volume),
  };
}

function normalizeChartLinePoint(value: unknown): ChartLinePoint {
  const raw = isRecord(value) ? value : {};
  return {
    time: readNumber(raw.time),
    value: readNumber(raw.value),
  };
}

function normalizeChartLineMarker(value: unknown): ChartLineMarker {
  const raw = isRecord(value) ? value : {};
  return {
    time: readNumber(raw.time),
    value: readNumber(raw.value),
    label: readString(raw.label),
    color: readString(raw.color),
  };
}

function normalizeStockOverview(value: unknown): StockOverview | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    symbol: readString(value.symbol),
    name: readString(value.name),
    exchange: readString(value.exchange),
    sector: readString(value.sector, "Unclassified"),
    sub_sector: readString(value.sub_sector),
    circuit_band_label: readNullableString(value.circuit_band_label),
    upper_circuit_limit: readNullableNumber(value.upper_circuit_limit),
    lower_circuit_limit: readNullableNumber(value.lower_circuit_limit),
    market_cap_crore: readNumber(value.market_cap_crore),
    last_price: readNumber(value.last_price),
    change_pct: readNumber(value.change_pct),
    relative_volume: readNumber(value.relative_volume),
    avg_rupee_volume_30d_crore: readNumber(value.avg_rupee_volume_30d_crore),
    rs_rating: readNullableNumber(value.rs_rating),
    rs_rating_1d_ago: readNumber(value.rs_rating_1d_ago),
    rs_rating_1w_ago: readNumber(value.rs_rating_1w_ago),
    rs_rating_1m_ago: readNumber(value.rs_rating_1m_ago),
    nifty_outperformance: readNumber(value.nifty_outperformance),
    sector_outperformance: readNumber(value.sector_outperformance),
    three_month_rs: readNumber(value.three_month_rs),
    stock_return_5d: readNumber(value.stock_return_5d),
    stock_return_20d: readNumber(value.stock_return_20d),
    stock_return_60d: readNumber(value.stock_return_60d),
    stock_return_126d: readNumber(value.stock_return_126d),
    stock_return_12m: readNumber(value.stock_return_12m),
    adr_pct_20: readNumber(value.adr_pct_20),
    pct_from_52w_high: readNumber(value.pct_from_52w_high),
    pct_from_ath: readNumber(value.pct_from_ath),
    pct_from_52w_low: readNumber(value.pct_from_52w_low),
    gap_pct: readNumber(value.gap_pct),
  };
}

export function normalizeChartResponse(value: unknown): ChartResponse {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    timeframe: readString(raw.timeframe, "1D"),
    bars: mapArray(raw.bars, normalizeChartBar),
    summary: normalizeStockOverview(raw.summary),
    rs_line: mapArray(raw.rs_line, normalizeChartLinePoint),
    rs_line_markers: mapArray(raw.rs_line_markers, normalizeChartLineMarker),
    earnings_markers: mapArray(raw.earnings_markers, normalizeChartLineMarker),
    volume_markers: mapArray(raw.volume_markers, normalizeChartLineMarker),
    band_change_markers: mapArray(raw.band_change_markers, normalizeChartLineMarker),
    band_history: mapArray(raw.band_history, normalizeBandHistorySegment),
  };
}

function normalizeBandHistorySegment(value: unknown): BandHistorySegment {
  const raw = isRecord(value) ? value : {};
  const pct = Number(raw.band_pct);
  return {
    from_date: typeof raw.from_date === "string" && raw.from_date ? raw.from_date : null,
    band_pct: Number.isFinite(pct) && pct > 0 ? pct : null,
  };
}

function normalizeSectorCompanyItem(value: unknown): SectorCompanyItem {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    name: readString(raw.name),
    exchange: readString(raw.exchange),
    sector: readString(raw.sector, "Unclassified"),
    sub_sector: readString(raw.sub_sector),
    market_cap_crore: readNumber(raw.market_cap_crore),
    last_price: readNumber(raw.last_price),
    return_1d: readNumber(raw.return_1d),
    return_1w: readNumber(raw.return_1w),
    return_1m: readNumber(raw.return_1m),
    return_3m: readNumber(raw.return_3m),
    return_6m: readNumber(raw.return_6m),
    return_1y: readNumber(raw.return_1y),
    return_2y: readNumber(raw.return_2y),
    rs_rating: readNumber(raw.rs_rating),
  };
}

function normalizeSectorGroup(value: unknown): SectorGroup {
  const raw = isRecord(value) ? value : {};
  return {
    sub_sector: readString(raw.sub_sector, "Unclassified"),
    company_count: readNumber(raw.company_count),
    companies: mapArray(raw.companies, normalizeSectorCompanyItem),
  };
}

function normalizeSectorCard(value: unknown): SectorCard {
  const raw = isRecord(value) ? value : {};
  const groupKind = raw.group_kind === "index" ? "index" : "sector";
  return {
    group_kind: groupKind,
    sector: readString(raw.sector, "Unclassified"),
    company_count: readNumber(raw.company_count),
    sub_sector_count: readNumber(raw.sub_sector_count),
    last_price: readNullableNumber(raw.last_price),
    return_1d: readNumber(raw.return_1d),
    return_1w: readNumber(raw.return_1w),
    return_1m: readNumber(raw.return_1m),
    return_3m: readNumber(raw.return_3m),
    return_6m: readNumber(raw.return_6m),
    return_1y: readNumber(raw.return_1y),
    return_2y: readNumber(raw.return_2y),
    sparkline: mapArray(raw.sparkline, normalizeChartLinePoint),
    sub_sectors: mapArray(raw.sub_sectors, normalizeSectorGroup),
  };
}

export function normalizeSectorTabResponse(value: unknown): SectorTabResponse {
  const raw = isRecord(value) ? value : {};
  const sortByValues = new Set(["1D", "1W", "1M", "3M", "6M", "1Y", "2Y"]);
  const sortBy = typeof raw.sort_by === "string" && sortByValues.has(raw.sort_by) ? raw.sort_by as SectorSortBy : "1M";
  const sortOrder = raw.sort_order === "asc" ? "asc" : "desc";
  return {
    generated_at: readString(raw.generated_at),
    total_sectors: readNumber(raw.total_sectors),
    sort_by: sortBy,
    sort_order: sortOrder,
    sectors: mapArray(raw.sectors, normalizeSectorCard),
  };
}

function normalizeIndustryGroupTopStock(value: unknown): IndustryGroupTopStock {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    company_name: readString(raw.company_name),
    rs_rating: readNullableNumber(raw.rs_rating),
    return_1m: readNumber(raw.return_1m),
    return_3m: readNumber(raw.return_3m),
    return_6m: readNumber(raw.return_6m),
    relative_return_3m: readNumber(raw.relative_return_3m),
    relative_return_6m: readNumber(raw.relative_return_6m),
  };
}

function normalizeIndustryGroupMasterItem(value: unknown): IndustryGroupMasterItem {
  const raw = isRecord(value) ? value : {};
  return {
    group_id: readString(raw.group_id),
    group_name: readString(raw.group_name),
    parent_sector: readString(raw.parent_sector, "Unclassified"),
    description: readString(raw.description),
    stock_count: readNumber(raw.stock_count),
    symbols: readStringArray(raw.symbols),
  };
}

function normalizeIndustryGroupStockItem(value: unknown): IndustryGroupStockItem {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    company_name: readString(raw.company_name),
    exchange: readString(raw.exchange),
    market_cap_cr: readNumber(raw.market_cap_cr),
    avg_traded_value_50d_cr: readNumber(raw.avg_traded_value_50d_cr),
    sector: readString(raw.sector, "Unclassified"),
    raw_industry: readString(raw.raw_industry),
    final_group_id: readString(raw.final_group_id),
    final_group_name: readString(raw.final_group_name),
    last_price: readNumber(raw.last_price),
    change_pct: readNumber(raw.change_pct),
    return_1m: readNumber(raw.return_1m),
    return_3m: readNumber(raw.return_3m),
    return_6m: readNumber(raw.return_6m),
    return_1y: readNumber(raw.return_1y),
    rs_rating: readNullableNumber(raw.rs_rating),
  };
}

function normalizeIndustryGroupRankItem(value: unknown): IndustryGroupRankItem {
  const raw = isRecord(value) ? value : {};
  return {
    rank: readNumber(raw.rank),
    rank_label: readString(raw.rank_label),
    rank_change_1w: readNullableNumber(raw.rank_change_1w),
    score_change_1w: readNullableNumber(raw.score_change_1w),
    strength_bucket: readString(raw.strength_bucket),
    trend_label: readString(raw.trend_label),
    group_id: readString(raw.group_id),
    group_name: readString(raw.group_name),
    parent_sector: readString(raw.parent_sector, "Unclassified"),
    description: readString(raw.description),
    stock_count: readNumber(raw.stock_count),
    score: readNumber(raw.score),
    return_1m: readNumber(raw.return_1m),
    return_3m: readNumber(raw.return_3m),
    return_6m: readNumber(raw.return_6m),
    relative_return_1m: readNumber(raw.relative_return_1m),
    relative_return_3m: readNumber(raw.relative_return_3m),
    relative_return_6m: readNumber(raw.relative_return_6m),
    median_return_1m: readNumber(raw.median_return_1m),
    median_return_3m: readNumber(raw.median_return_3m),
    median_return_6m: readNumber(raw.median_return_6m),
    pct_above_50dma: readNumber(raw.pct_above_50dma),
    pct_above_200dma: readNumber(raw.pct_above_200dma),
    pct_outperform_benchmark_3m: readNumber(raw.pct_outperform_benchmark_3m),
    pct_outperform_benchmark_6m: readNumber(raw.pct_outperform_benchmark_6m),
    breadth_score: readNumber(raw.breadth_score),
    trend_health_score: readNumber(raw.trend_health_score),
    leaders: readStringArray(raw.leaders),
    laggards: readStringArray(raw.laggards),
    top_constituents: mapArray(raw.top_constituents, normalizeIndustryGroupTopStock),
    symbols: readStringArray(raw.symbols),
  };
}

export function normalizeIndustryGroupsResponse(value: unknown): IndustryGroupsResponse {
  const raw = isRecord(value) ? value : {};
  return {
    generated_at: readString(raw.generated_at),
    as_of_date: readString(raw.as_of_date),
    benchmark: readString(raw.benchmark),
    filters: isRecord(raw.filters)
      ? {
          min_market_cap_cr: readNumber(raw.filters.min_market_cap_cr),
          min_avg_daily_value_cr: readNumber(raw.filters.min_avg_daily_value_cr),
        }
      : {
          min_market_cap_cr: 0,
          min_avg_daily_value_cr: 0,
        },
    total_groups: readNumber(raw.total_groups),
    groups: mapArray(raw.groups, normalizeIndustryGroupRankItem),
    master: mapArray(raw.master, normalizeIndustryGroupMasterItem),
    stocks: mapArray(raw.stocks, normalizeIndustryGroupStockItem),
  };
}

function normalizeImprovingRsItem(value: unknown): ImprovingRsItem {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    name: readString(raw.name),
    exchange: readString(raw.exchange),
    sector: readString(raw.sector, "Unclassified"),
    sub_sector: readString(raw.sub_sector),
    market_cap_crore: readNumber(raw.market_cap_crore),
    last_price: readNumber(raw.last_price),
    change_pct: readNumber(raw.change_pct),
    rs_rating: readNumber(raw.rs_rating),
    rs_rating_1d_ago: readNumber(raw.rs_rating_1d_ago),
    rs_rating_1w_ago: readNumber(raw.rs_rating_1w_ago),
    rs_rating_1m_ago: readNumber(raw.rs_rating_1m_ago),
    improvement_1d: readNumber(raw.improvement_1d),
    improvement_1w: readNumber(raw.improvement_1w),
    improvement_1m: readNumber(raw.improvement_1m),
  };
}

function normalizeImprovingRsResponse(value: unknown): ImprovingRsResponse {
  const raw = isRecord(value) ? value : {};
  const window = raw.window === "1W" || raw.window === "1M" ? raw.window : "1D";
  return {
    generated_at: readString(raw.generated_at),
    window,
    total_hits: readNumber(raw.total_hits),
    items: mapArray(raw.items, normalizeImprovingRsItem),
  };
}

function normalizeChartGridCard(value: unknown): ChartGridCard {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    name: readString(raw.name),
    exchange: readString(raw.exchange),
    sector: readString(raw.sector, "Unclassified"),
    sub_sector: readString(raw.sub_sector),
    market_cap_crore: readNumber(raw.market_cap_crore),
    last_price: readNumber(raw.last_price),
    change_pct: readNumber(raw.change_pct),
    return_1d: readNumber(raw.return_1d),
    return_1w: readNumber(raw.return_1w),
    return_1m: readNumber(raw.return_1m),
    return_3m: readNumber(raw.return_3m),
    return_6m: readNumber(raw.return_6m),
    return_1y: readNumber(raw.return_1y),
    return_2y: readNumber(raw.return_2y),
    rs_rating: readNullableNumber(raw.rs_rating),
    weight_pct: readNullableNumber(raw.weight_pct),
    sparkline: mapArray(raw.sparkline, normalizeChartLinePoint),
  };
}

function normalizeChartGridResponse(value: unknown): ChartGridResponse {
  const raw = isRecord(value) ? value : {};
  const groupKind = raw.group_kind === "index" ? "index" : "sector";
  const timeframe = raw.timeframe === "3M" || raw.timeframe === "6M" || raw.timeframe === "2Y" ? raw.timeframe : "1Y";
  return {
    generated_at: readString(raw.generated_at),
    name: readString(raw.name),
    group_kind: groupKind,
    timeframe,
    total_items: readNumber(raw.total_items),
    cards: mapArray(raw.cards, normalizeChartGridCard),
  };
}

function normalizeChartGridSeriesItem(value: unknown): ChartGridSeriesItem {
  const raw = isRecord(value) ? value : {};
  return {
    symbol: readString(raw.symbol),
    bars: mapArray(raw.bars, normalizeChartBar),
  };
}

function normalizeChartGridSeriesResponse(value: unknown): ChartGridSeriesResponse {
  const raw = isRecord(value) ? value : {};
  const timeframe = raw.timeframe === "3M" || raw.timeframe === "6M" || raw.timeframe === "2Y" ? raw.timeframe : "1Y";
  return {
    generated_at: readString(raw.generated_at),
    timeframe,
    total_items: readNumber(raw.total_items),
    items: mapArray(raw.items, normalizeChartGridSeriesItem),
  };
}

function normalizeCompanyFundamentals(value: unknown): CompanyFundamentals {
  const raw = isRecord(value) ? value : {};
  const aiSummary: AISummary | null = isRecord(raw.ai_news_summary)
    ? {
        generated_at: readString(raw.ai_news_summary.generated_at),
        summary: readString(raw.ai_news_summary.summary),
        key_points: readStringArray(raw.ai_news_summary.key_points),
        sentiment:
          raw.ai_news_summary.sentiment === "positive"
          || raw.ai_news_summary.sentiment === "negative"
          || raw.ai_news_summary.sentiment === "neutral"
            ? raw.ai_news_summary.sentiment
            : "neutral",
      }
    : null;

  return {
    symbol: readString(raw.symbol),
    name: readString(raw.name),
    exchange: readNullableString(raw.exchange),
    sector: readNullableString(raw.sector),
    sub_sector: readNullableString(raw.sub_sector),
    fetched_at: readString(raw.fetched_at),
    about: readNullableString(raw.about),
    business_summary: readNullableString(raw.business_summary),
    company_website: readNullableString(raw.company_website),
    headquarters: readNullableString(raw.headquarters),
    quarterly_results: mapArray(raw.quarterly_results, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        period: readString(entry.period),
        sales_crore: readNullableNumber(entry.sales_crore),
        expenses_crore: readNullableNumber(entry.expenses_crore),
        operating_profit_crore: readNullableNumber(entry.operating_profit_crore),
        operating_margin_pct: readNullableNumber(entry.operating_margin_pct),
        profit_before_tax_crore: readNullableNumber(entry.profit_before_tax_crore),
        net_profit_crore: readNullableNumber(entry.net_profit_crore),
        eps: readNullableNumber(entry.eps),
        result_document_url: readNullableString(entry.result_document_url),
        sales_qoq_pct: readNullableNumber(entry.sales_qoq_pct),
        sales_yoy_pct: readNullableNumber(entry.sales_yoy_pct),
        eps_qoq_pct: readNullableNumber(entry.eps_qoq_pct),
        eps_yoy_pct: readNullableNumber(entry.eps_yoy_pct),
        net_profit_qoq_pct: readNullableNumber(entry.net_profit_qoq_pct),
        net_profit_yoy_pct: readNullableNumber(entry.net_profit_yoy_pct),
      };
    }),
    profit_loss: mapArray(raw.profit_loss, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        period: readString(entry.period),
        sales_crore: readNullableNumber(entry.sales_crore),
        operating_profit_crore: readNullableNumber(entry.operating_profit_crore),
        operating_margin_pct: readNullableNumber(entry.operating_margin_pct),
        net_profit_crore: readNullableNumber(entry.net_profit_crore),
        eps: readNullableNumber(entry.eps),
        dividend_payout_pct: readNullableNumber(entry.dividend_payout_pct),
      };
    }),
    balance_sheet: mapArray(raw.balance_sheet, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        period: readString(entry.period),
        total_assets_crore: readNullableNumber(entry.total_assets_crore),
        current_assets_crore: readNullableNumber(entry.current_assets_crore),
        total_liabilities_crore: readNullableNumber(entry.total_liabilities_crore),
        current_liabilities_crore: readNullableNumber(entry.current_liabilities_crore),
        shareholders_equity_crore: readNullableNumber(entry.shareholders_equity_crore),
        debt_crore: readNullableNumber(entry.debt_crore),
        cash_and_equivalents_crore: readNullableNumber(entry.cash_and_equivalents_crore),
        inventory_crore: readNullableNumber(entry.inventory_crore),
        receivables_crore: readNullableNumber(entry.receivables_crore),
      };
    }),
    cash_flow: mapArray(raw.cash_flow, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        period: readString(entry.period),
        operating_cash_flow_crore: readNullableNumber(entry.operating_cash_flow_crore),
        investing_cash_flow_crore: readNullableNumber(entry.investing_cash_flow_crore),
        financing_cash_flow_crore: readNullableNumber(entry.financing_cash_flow_crore),
        free_cash_flow_crore: readNullableNumber(entry.free_cash_flow_crore),
        capital_expenditure_crore: readNullableNumber(entry.capital_expenditure_crore),
        dividends_paid_crore: readNullableNumber(entry.dividends_paid_crore),
      };
    }),
    financial_ratios: mapArray(raw.financial_ratios, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        period: readString(entry.period),
        roe_pct: readNullableNumber(entry.roe_pct),
        roa_pct: readNullableNumber(entry.roa_pct),
        roce_pct: readNullableNumber(entry.roce_pct),
        current_ratio: readNullableNumber(entry.current_ratio),
        quick_ratio: readNullableNumber(entry.quick_ratio),
        debt_to_equity_ratio: readNullableNumber(entry.debt_to_equity_ratio),
        debt_to_assets_ratio: readNullableNumber(entry.debt_to_assets_ratio),
        interest_coverage: readNullableNumber(entry.interest_coverage),
        asset_turnover: readNullableNumber(entry.asset_turnover),
      };
    }),
    growth: isRecord(raw.growth)
      ? {
          latest_period: readNullableString(raw.growth.latest_period),
          sales_qoq_pct: readNullableNumber(raw.growth.sales_qoq_pct),
          sales_yoy_pct: readNullableNumber(raw.growth.sales_yoy_pct),
          profit_qoq_pct: readNullableNumber(raw.growth.profit_qoq_pct),
          profit_yoy_pct: readNullableNumber(raw.growth.profit_yoy_pct),
          operating_margin_latest_pct: readNullableNumber(raw.growth.operating_margin_latest_pct),
          operating_margin_previous_pct: readNullableNumber(raw.growth.operating_margin_previous_pct),
          net_margin_latest_pct: readNullableNumber(raw.growth.net_margin_latest_pct),
          net_margin_previous_pct: readNullableNumber(raw.growth.net_margin_previous_pct),
        }
      : null,
    valuation: isRecord(raw.valuation)
      ? {
          market_cap_crore: readNullableNumber(raw.valuation.market_cap_crore),
          pe_ratio: readNullableNumber(raw.valuation.pe_ratio),
          peg_ratio: readNullableNumber(raw.valuation.peg_ratio),
          operating_margin_pct: readNullableNumber(raw.valuation.operating_margin_pct),
          net_margin_pct: readNullableNumber(raw.valuation.net_margin_pct),
          roce_pct: readNullableNumber(raw.valuation.roce_pct),
          roe_pct: readNullableNumber(raw.valuation.roe_pct),
          dividend_yield_pct: readNullableNumber(raw.valuation.dividend_yield_pct),
        }
      : null,
    growth_drivers: mapArray(raw.growth_drivers, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        title: readString(entry.title),
        detail: readString(entry.detail),
        tone: entry.tone === "positive" || entry.tone === "watch" ? entry.tone : "neutral",
      };
    }),
    management_team: mapArray(raw.management_team, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        name: readString(entry.name),
        position: readString(entry.position),
        background: readNullableString(entry.background) ?? undefined,
      };
    }),
    management_guidance: mapArray(raw.management_guidance, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        fiscal_year: readString(entry.fiscal_year),
        revenue_growth_guidance_pct: readNullableNumber(entry.revenue_growth_guidance_pct),
        ebitda_guidance_pct: readNullableNumber(entry.ebitda_guidance_pct),
        eps_guidance: readNullableNumber(entry.eps_guidance),
        capex_guidance_crore: readNullableNumber(entry.capex_guidance_crore),
        guidance_date: readNullableString(entry.guidance_date),
        guidance_source: readNullableString(entry.guidance_source),
        key_guidance_points: readStringArray(entry.key_guidance_points),
      };
    }),
    strategy_and_outlook: readNullableString(raw.strategy_and_outlook),
    competitive_position: isRecord(raw.competitive_position)
      ? {
          market_position: readNullableString(raw.competitive_position.market_position),
          competitive_advantages: readStringArray(raw.competitive_position.competitive_advantages),
          market_share_estimate: readNullableNumber(raw.competitive_position.market_share_estimate),
          key_competitors: readStringArray(raw.competitive_position.key_competitors),
        }
      : null,
    business_segments: mapArray(raw.business_segments, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        name: readString(entry.name),
        revenue_crore: readNullableNumber(entry.revenue_crore),
        revenue_pct: readNullableNumber(entry.revenue_pct),
        growth_pct: readNullableNumber(entry.growth_pct),
        period: readString(entry.period),
      };
    }),
    geographic_presence: readStringArray(raw.geographic_presence),
    risks_and_opportunities: mapArray(raw.risks_and_opportunities, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        risk_category: readString(entry.risk_category),
        description: readString(entry.description),
        severity: readString(entry.severity),
        mitigation_strategy: readNullableString(entry.mitigation_strategy),
      };
    }),
    recent_updates: mapArray(raw.recent_updates, (item) => {
      const entry = isRecord(item) ? item : {};
      const kind = entry.kind === "results" || entry.kind === "concall" || entry.kind === "holding" || entry.kind === "filing"
        ? entry.kind
        : "news";
      return {
        title: readString(entry.title),
        source: readString(entry.source),
        published_at: readNullableString(entry.published_at),
        summary: readNullableString(entry.summary),
        link: readNullableString(entry.link),
        kind,
      };
    }),
    detailed_news: mapArray(raw.detailed_news, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        title: readString(entry.title),
        summary: readString(entry.summary),
        impact_category: readString(entry.impact_category),
        sentiment: readString(entry.sentiment),
        source: readString(entry.source),
        published_date: readString(entry.published_date),
        detailed_points: readStringArray(entry.detailed_points),
        relevance_score: readNumber(entry.relevance_score),
      };
    }),
    shareholding_pattern: mapArray(raw.shareholding_pattern, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        period: readString(entry.period),
        promoter_pct: readNullableNumber(entry.promoter_pct),
        fii_pct: readNullableNumber(entry.fii_pct),
        dii_pct: readNullableNumber(entry.dii_pct),
        public_pct: readNullableNumber(entry.public_pct),
        shareholder_count: readNullableNumber(entry.shareholder_count),
      };
    }),
    shareholding_delta: isRecord(raw.shareholding_delta)
      ? {
          latest_period: readNullableString(raw.shareholding_delta.latest_period),
          previous_period: readNullableString(raw.shareholding_delta.previous_period),
          promoter_change_pct: readNullableNumber(raw.shareholding_delta.promoter_change_pct),
          fii_change_pct: readNullableNumber(raw.shareholding_delta.fii_change_pct),
          dii_change_pct: readNullableNumber(raw.shareholding_delta.dii_change_pct),
          public_change_pct: readNullableNumber(raw.shareholding_delta.public_change_pct),
        }
      : null,
    data_warnings: readStringArray(raw.data_warnings),
    ai_news_summary: aiSummary,
    business_triggers: mapArray(raw.business_triggers, (item) => {
      const entry = isRecord(item) ? item : {};
      const impact = entry.impact === "positive" || entry.impact === "negative" ? entry.impact : "neutral";
      return {
        title: readString(entry.title),
        description: readString(entry.description),
        impact,
        date: readString(entry.date),
        source: readString(entry.source),
        likelihood_to_impact: readNumber(entry.likelihood_to_impact),
      };
    }),
    insider_transactions: mapArray(raw.insider_transactions, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        person_name: readString(entry.person_name),
        position: readString(entry.position),
        transaction_type: entry.transaction_type === "sell" ? "sell" : "buy",
        quantity: readNumber(entry.quantity),
        price_per_share: readNumber(entry.price_per_share),
        total_value_crore: readNumber(entry.total_value_crore),
        date: readString(entry.date),
        pct_of_holding_change: readNumber(entry.pct_of_holding_change),
        remarks: readNullableString(entry.remarks),
      };
    }),
    last_news_update: readNullableString(raw.last_news_update),
    latest_earnings_key_metrics: isRecord(raw.latest_earnings_key_metrics)
      ? Object.fromEntries(
          Object.entries(raw.latest_earnings_key_metrics).filter(([, metricValue]) =>
            typeof metricValue === "string" || Number.isFinite(Number(metricValue))),
        ) as Record<string, number | string>
      : {},
    upcoming_events: mapArray(raw.upcoming_events, (item) => {
      const entry = isRecord(item) ? item : {};
      return {
        date: readString(entry.date),
        event: readString(entry.event),
        impact: readNullableString(entry.impact) ?? undefined,
      };
    }),
  };
}

function normalizeQuarterlyResultItem(value: unknown): QuarterlyResultItem {
  const entry = isRecord(value) ? value : {};
  return {
    period: readString(entry.period),
    sales_crore: readNullableNumber(entry.sales_crore),
    expenses_crore: readNullableNumber(entry.expenses_crore),
    operating_profit_crore: readNullableNumber(entry.operating_profit_crore),
    operating_margin_pct: readNullableNumber(entry.operating_margin_pct),
    profit_before_tax_crore: readNullableNumber(entry.profit_before_tax_crore),
    net_profit_crore: readNullableNumber(entry.net_profit_crore),
    eps: readNullableNumber(entry.eps),
    result_document_url: readNullableString(entry.result_document_url),
    sales_qoq_pct: readNullableNumber(entry.sales_qoq_pct),
    sales_yoy_pct: readNullableNumber(entry.sales_yoy_pct),
    eps_qoq_pct: readNullableNumber(entry.eps_qoq_pct),
    eps_yoy_pct: readNullableNumber(entry.eps_yoy_pct),
    net_profit_qoq_pct: readNullableNumber(entry.net_profit_qoq_pct),
    net_profit_yoy_pct: readNullableNumber(entry.net_profit_yoy_pct),
  };
}

function normalizeCompanyEarningsSummary(value: unknown): CompanyEarningsSummary {
  const raw = isRecord(value) ? value : {};
  const valuation = isRecord(raw.valuation) ? raw.valuation : {};
  const metrics = isRecord(raw.metrics) ? raw.metrics : {};
  return {
    symbol: readString(raw.symbol),
    name: readString(raw.name),
    sector: readNullableString(raw.sector),
    sub_sector: readNullableString(raw.sub_sector),
    fetched_at: readString(raw.fetched_at),
    source: readString(raw.source),
    valuation: {
      market_cap_crore: readNullableNumber(valuation.market_cap_crore),
      pe_ratio: readNullableNumber(valuation.pe_ratio),
      roe_pct: readNullableNumber(valuation.roe_pct),
      operating_margin_pct: readNullableNumber(valuation.operating_margin_pct),
    },
    metrics: {
      pct_from_52w_high: readNullableNumber(metrics.pct_from_52w_high),
      pct_from_52w_low: readNullableNumber(metrics.pct_from_52w_low),
      adr_pct_20: readNullableNumber(metrics.adr_pct_20),
      relative_volume: readNullableNumber(metrics.relative_volume),
      turnover_1d_crore: readNullableNumber(metrics.turnover_1d_crore),
      avg_turnover_50d_crore: readNullableNumber(metrics.avg_turnover_50d_crore),
    },
    quarterly_results: mapArray(raw.quarterly_results, normalizeQuarterlyResultItem),
    data_warnings: readStringArray(raw.data_warnings),
  };
}

function normalizeWatchlistItem(value: unknown): WatchlistItem {
  const raw = isRecord(value) ? value : {};
  return {
    id: readString(raw.id),
    name: readString(raw.name),
    color: readString(raw.color),
    symbols: readStringArray(raw.symbols),
  };
}

function normalizeWatchlistsStateResponse(value: unknown): WatchlistsStateResponse {
  const raw = isRecord(value) ? value : {};
  const market = "india";
  return {
    market,
    updated_at: readString(raw.updated_at),
    active_watchlist_id: readNullableString(raw.active_watchlist_id),
    watchlists: mapArray(raw.watchlists, normalizeWatchlistItem),
  };
}

type ResponseNormalizer<T> = (payload: unknown) => T;

async function fetchWithTimeout(input: string, init?: RequestInit): Promise<Response> {
  return fetchWithTimeoutMs(input, REQUEST_TIMEOUT_MS, init);
}

async function fetchWithTimeoutMs(input: string, timeoutMs: number, init?: RequestInit): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetch(input, {
      cache: "no-store",
      ...init,
      signal: controller.signal,
    });
  } finally {
    window.clearTimeout(timeoutId);
  }
}

type RequestOptions = {
  timeoutMs?: number;
};

function routeScopedMarket(): MarketKey | null {
  if (typeof window === "undefined") {
    return null;
  }

  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  if (pathname === "/india" || pathname.startsWith("/india/")) {
    return "india";
  }
  return null;
}

function orderedApiBases() {
  if (!preferredApiBase) {
    return FALLBACK_API_BASES;
  }
  return [preferredApiBase, ...FALLBACK_API_BASES.filter((base) => base !== preferredApiBase)];
}

async function request<T>(
  path: string,
  init?: RequestInit,
  options?: RequestOptions,
  normalize?: ResponseNormalizer<T>,
): Promise<T> {
  let lastError: Error | null = null;
  const timeoutMs = options?.timeoutMs ?? REQUEST_TIMEOUT_MS;
  const bases = orderedApiBases();

  for (let i = 0; i < bases.length; i += 1) {
    const base = bases[i];
    // Retry on the same base for transient browser-level fetch failures
    // (TypeError "Failed to fetch") — usually a Vercel/HF cold-start blip
    // that clears within a couple of seconds. Hopping bases too eagerly
    // hides the recovery and surfaces a misleading error to the user.
    for (let attempt = 0; attempt <= SAME_BASE_RETRY_ATTEMPTS; attempt += 1) {
      try {
        const url = init?.method && init.method !== "GET"
          ? `${base}${path}`
          : `${base}${path}${path.includes("?") ? "&" : "?"}_v=${Date.now()}`;
        const response = await fetchWithTimeoutMs(url, timeoutMs, init);

        if (!response.ok) {
          if (RETRYABLE_STATUS_CODES.has(response.status)) {
            lastError = new Error(`Request failed: ${response.status}`);
            // Retry on the same base for transient 5xx — same rationale as
            // TypeError handling: HF Spaces self-heal in a second or two.
            if (
              SAME_BASE_RETRY_STATUS_CODES.has(response.status)
              && attempt < SAME_BASE_RETRY_ATTEMPTS
            ) {
              await new Promise((resolve) => setTimeout(resolve, SAME_BASE_RETRY_BACKOFF_MS));
              continue;
            }
            break;
          }
          throw new Error(`Request failed: ${response.status}`);
        }

        const contentType = response.headers.get("content-type") ?? "";
        if (!contentType.toLowerCase().includes("application/json")) {
          throw new Error("API returned a non-JSON response");
        }

        const payload = await response.json();
        preferredApiBase = base;
        return normalize ? normalize(payload) : payload as T;
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
          lastError = new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s`);
          break;
        } else if (error instanceof TypeError) {
          lastError = new Error("Backend is waking up, please wait...");
          if (attempt < SAME_BASE_RETRY_ATTEMPTS) {
            await new Promise((resolve) => setTimeout(resolve, SAME_BASE_RETRY_BACKOFF_MS));
            continue;
          }
        } else {
          lastError = error instanceof Error ? error : new Error("Failed to reach market data API");
          break;
        }
      }
    }
    if (i < bases.length - 1) {
      await new Promise((resolve) => setTimeout(resolve, RETRY_BACKOFF_MS));
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
  // 60s timeout: HF Space cold starts (after a deploy or 30+ min idle) can
  // take 30-50s before the first /api/dashboard responds. With the default
  // 20s timeout, fresh-browser users would just see "0 universe" for a few
  // minutes after every restart. 60s tolerates the cold start on the very
  // first hit; warm calls return in 1-3s.
  return request<DashboardResponse>(
    withMarket("/api/dashboard", market),
    undefined,
    { timeoutMs: 60_000 },
    normalizeDashboardResponse,
  );
}

// Lightweight ping used to keep the HF Space warm while the user has the
// tab open. HF free Spaces sleep after ~30 minutes idle; a periodic ping
// avoids the cold-start that surfaces as "Network request failed, retrying".
export async function pingBackendHealth(): Promise<boolean> {
  const bases = orderedApiBases();
  for (const base of bases) {
    try {
      const url = `${base}/api/health?_v=${Date.now()}`;
      const response = await fetchWithTimeoutMs(url, 8000);
      if (response.ok) {
        preferredApiBase = base;
        return true;
      }
    } catch {
      // try next base
    }
  }
  return false;
}

export function getScanCounts(market: MarketKey) {
  return request<ScanDescriptor[]>(
    withMarket("/api/scan-counts", market),
    undefined,
    undefined,
    (payload) => mapArray(payload, normalizeScanDescriptor),
  );
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
  if (typeof options?.expansionMinChangePct === "number" && Number.isFinite(options.expansionMinChangePct)) {
    params.set("expansion_min_change_pct", String(options.expansionMinChangePct));
  }
  if (typeof options?.expansionMinRelativeVolume === "number" && Number.isFinite(options.expansionMinRelativeVolume)) {
    params.set("expansion_min_relative_volume", String(options.expansionMinRelativeVolume));
  }
  if (typeof options?.positiveEarningsMinCloseInRangePct === "number" && Number.isFinite(options.positiveEarningsMinCloseInRangePct)) {
    params.set("positive_earnings_min_close_in_range_pct", String(options.positiveEarningsMinCloseInRangePct));
  }
  if (typeof options?.positiveEarningsMinNextDayGapPct === "number" && Number.isFinite(options.positiveEarningsMinNextDayGapPct)) {
    params.set("positive_earnings_min_next_day_gap_pct", String(options.positiveEarningsMinNextDayGapPct));
  }
  if (typeof options?.positiveEarningsMinDayRvol === "number" && Number.isFinite(options.positiveEarningsMinDayRvol)) {
    params.set("positive_earnings_min_day_rvol", String(options.positiveEarningsMinDayRvol));
  }
  if (typeof options?.positiveEarningsMinReturn5dPct === "number" && Number.isFinite(options.positiveEarningsMinReturn5dPct)) {
    params.set("positive_earnings_min_return_5d_pct", String(options.positiveEarningsMinReturn5dPct));
  }
  if (typeof options?.positiveEarningsLookbackDays === "number" && Number.isFinite(options.positiveEarningsLookbackDays)) {
    params.set("positive_earnings_lookback_days", String(options.positiveEarningsLookbackDays));
  }
  if (options?.volumeWindow) {
    params.set("volume_window", options.volumeWindow);
  }
  if (typeof options?.volumeMinRvol === "number" && Number.isFinite(options.volumeMinRvol)) {
    params.set("volume_min_rvol", String(options.volumeMinRvol));
  }
  const query = params.toString();
  if (!query) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}${query}`;
}

export type ScannerScorecardRow = {
  scan_id: string;
  scan_name: string;
  sessions: number;
  hits: number;
  avg_forward_return_pct: number | null;
  win_rate_pct: number | null;
  best_symbol: string | null;
  best_return_pct: number | null;
  worst_symbol: string | null;
  worst_return_pct: number | null;
};

export type ScannerScorecardResponse = {
  rows: ScannerScorecardRow[];
};

export type AiSwingTradePlan = {
  entry?: number | null;
  entry_logic?: string;
  stop_loss?: number | null;
  stop_logic?: string;
  target_1?: number | null;
  target_2?: number | null;
  risk_pct?: number | null;
};

export type AiSwingAnalysis = {
  error?: string;
  raw?: string;
  symbol?: string;
  session_date?: string;
  verdict?: "TAKE" | "WAIT" | "AVOID" | string;
  setup_type?: string;
  conviction?: number;
  headline?: string;
  tape_read?: string;
  trade_plan?: AiSwingTradePlan;
  pros?: string[];
  cons?: string[];
  market_context?: string;
  invalidation?: string;
};

export function getAiSwingAnalysis(symbol: string, market: MarketKey, asOf?: string | null): Promise<AiSwingAnalysis> {
  return request<AiSwingAnalysis>(
    withMarket("/api/ai/swing-analysis", market),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol, as_of: asOf || undefined }),
    },
    { timeoutMs: 90000 },
  );
}

export type AiJournalPosition = {
  symbol: string;
  status?: string;
  read?: string;
  action?: string;
};

export type AiJournalReview = {
  error?: string;
  raw?: string;
  overall?: string;
  doing_right?: string[];
  doing_wrong?: string[];
  fixes?: string[];
  open_positions?: AiJournalPosition[];
  one_lesson?: string;
};

export function runAiJournalReview(payload: unknown, market: MarketKey): Promise<AiJournalReview> {
  return request<AiJournalReview>(
    withMarket("/api/ai/journal-review", market),
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    { timeoutMs: 120000 },
  );
}

export function getScannerScorecard(market: MarketKey): Promise<ScannerScorecardResponse> {
  return request<ScannerScorecardResponse>(withMarket("/api/scanner-scorecard", market));
}

export function getScanResults(scanId: string, market: MarketKey, options?: ScanRequestOptions) {
  // 60s timeout: scan endpoints rebuild their cache on the first hit after a
  // snapshot reload (3-8s warm, 30-50s on a Space cold-start). The default 20s
  // would fire AbortError mid-warmup and leave the panel showing
  // "Request timed out".
  return request<ScanResultsResponse>(
    withScanOptions(`/api/scans/${scanId}`, market, options),
    undefined,
    { timeoutMs: 60_000 },
    normalizeScanResultsResponse,
  );
}

export function runCustomScan(body: CustomScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/custom-scan", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, { timeoutMs: 60_000 }, normalizeScanResultsResponse);
}

export function getChart(symbol: string, timeframe: string, market: MarketKey) {
  return request<ChartResponse>(
    `/api/chart/${encodeURIComponent(symbol)}?timeframe=${timeframe}&market=${market}`,
    undefined,
    { timeoutMs: timeframe === "1D" || timeframe === "1W" ? 30000 : 25000 },
    normalizeChartResponse,
  );
}

export function getChartHistory(symbol: string, timeframe: string, market: MarketKey) {
  return request<ChartResponse>(
    `/api/chart/${encodeURIComponent(symbol)}/history?timeframe=${timeframe}&market=${market}`,
    undefined,
    { timeoutMs: timeframe === "1D" || timeframe === "1W" ? 35000 : 30000 },
    normalizeChartResponse,
  );
}

export function getChartGrid(name: string, groupKind: "sector" | "index", timeframe: ChartGridTimeframe, market: MarketKey) {
  return request<ChartGridResponse>(
    `/api/chart-grid?name=${encodeURIComponent(name)}&group_kind=${groupKind}&timeframe=${timeframe}&market=${market}`,
    undefined,
    undefined,
    normalizeChartGridResponse,
  );
}

export function getChartGridSeries(symbols: string[], timeframe: ChartGridTimeframe, market: MarketKey) {
  return request<ChartGridSeriesResponse>(
    `/api/chart-grid-series?symbols=${encodeURIComponent(symbols.join(","))}&timeframe=${timeframe}&market=${market}`,
    undefined,
    undefined,
    normalizeChartGridSeriesResponse,
  );
}

export function getFundamentals(symbol: string, market: MarketKey) {
  return request<CompanyFundamentals>(
    `/api/fundamentals/${symbol}?market=${market}`,
    undefined,
    undefined,
    normalizeCompanyFundamentals,
  );
}

export function getEarningsSummary(symbol: string, market: MarketKey) {
  return request<CompanyEarningsSummary>(
    `/api/earnings/${encodeURIComponent(symbol)}?market=${market}`,
    undefined,
    { timeoutMs: 15000 },
    normalizeCompanyEarningsSummary,
  );
}

export function refreshMarketData(market: MarketKey) {
  return request<RefreshResponse>(withMarket("/api/refresh", market), {
    method: "POST",
  });
}

export function getIndexQuotes(symbols: string[], market: MarketKey) {
  return request<IndexQuotesResponse>(
    `/api/index-quotes?symbols=${encodeURIComponent(symbols.join(","))}&market=${market}`,
    undefined,
    undefined,
    normalizeIndexQuotesResponse,
  );
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

export type LiveNewsItem = {
  id: string;
  title: string;
  description: string;
  link: string;
  pub_date: string;
  image: string | null;
  category: string;
  companies: string[];
  source: { id: string; name: string; color: string };
};

export type LiveNewsResponse = {
  items: LiveNewsItem[];
  count: number;
  categories: string[];
};

export function getLiveNews(market: MarketKey, category?: string, limit = 150) {
  const params = new URLSearchParams({ market, limit: String(limit) });
  if (category && category !== "all") {
    params.set("category", category);
  }
  return request<LiveNewsResponse>(`/api/live-news?${params.toString()}`);
}

export function getArticleProxyUrl(articleUrl: string) {
  const params = new URLSearchParams({ url: articleUrl });
  return `/api/article-proxy?${params.toString()}`;
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
  return request<ScanResultsResponse>(
    withScanOptions(`/api/gap-up-openers?${params.toString()}`, market, options),
    undefined,
    undefined,
    normalizeScanResultsResponse,
  );
}

// Same 60s budget as the GET scan endpoints — these all share the same
// snapshot/scan-cache rebuild path and can run long after a Space restart.
const SCAN_POST_TIMEOUT_MS = 60_000;

export function getNearPivotScan(body: NearPivotScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/near-pivot", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, { timeoutMs: SCAN_POST_TIMEOUT_MS }, normalizeScanResultsResponse);
}

export function getPullBackScan(body: PullBackScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/pull-backs", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, { timeoutMs: SCAN_POST_TIMEOUT_MS }, normalizeScanResultsResponse);
}

export function getReturnsScan(body: ReturnsScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/returns", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, { timeoutMs: SCAN_POST_TIMEOUT_MS }, normalizeScanResultsResponse);
}

export function getConsolidatingScan(body: ConsolidatingScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/consolidating", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, { timeoutMs: SCAN_POST_TIMEOUT_MS }, normalizeScanResultsResponse);
}

export function getMomentumBurstScan(body: MomentumBurstScanRequest, market: MarketKey, options?: ScanRequestOptions) {
  return request<ScanResultsResponse>(withScanOptions("/api/momentum-burst", market, options), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, { timeoutMs: SCAN_POST_TIMEOUT_MS }, normalizeScanResultsResponse);
}

export function getIndustryGroups(market: MarketKey) {
  // Groups computation is heavy and can take 20-40s on a cold Space.
  return request<IndustryGroupsResponse>(
    withMarket("/api/groups", market),
    undefined,
    { timeoutMs: 60_000 },
    normalizeIndustryGroupsResponse,
  );
}

export function getImprovingRs(window: ImprovingRsWindow, market: MarketKey) {
  return request<ImprovingRsResponse>(
    `/api/improving-rs?window=${window}&market=${market}`,
    undefined,
    undefined,
    normalizeImprovingRsResponse,
  );
}

export function getWatchlistsState(market: MarketKey) {
  return request<WatchlistsStateResponse>(
    withMarket("/api/watchlists", market),
    undefined,
    undefined,
    normalizeWatchlistsStateResponse,
  );
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
  }, undefined, normalizeWatchlistsStateResponse);
}

export function getJournalData() {
  return request<Record<string, unknown>>("/api/journal");
}

export function saveJournalData(payload: Record<string, unknown>) {
  return request<Record<string, unknown>>("/api/journal", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
