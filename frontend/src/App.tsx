import { Suspense, lazy, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";

import type {
  ChartAnnotation,
  ChartColorSettings,
  ChartGroupSummary,
  ChartPaletteKey,
  ChartPanelTab,
  ChartStyle,
  ChartTimeframe,
  IndicatorKey,
} from "./components/ChartPanel";
import type { EandCSettings } from "./components/EandCScannerPanel";
import type { ScreenerMode } from "./components/ScreenerSidebar";
import type { LocalWatchlist } from "./components/WatchlistsPanel";
import type { ImportResult } from "./components/WatchlistImportModal";
import {
  type ChartBar,
  buildWatchdogEventsUrl,
  getChart,
  getConsolidatingScan,
  getDashboard,
  getEandCScan,
  getFundamentals,
  getGapUpOpeners,
  getIndustryGroups,
  getIndexQuotes,
  getNearPivotScan,
  getPullBackScan,
  getReturnsScan,
  getImprovingRs,
  getScanCounts,
  getScanResults,
  getSectorTab,
  getWatchlistsState,
  getWatchdogStatus,
  refreshMarketData,
  runWatchdogFix,
  runCustomScan,
  saveWatchlistsState,
  type ChartResponse,
  type CompanyFundamentals,
  type ConsolidatingScanRequest,
  type CustomScanRequest,
  type DashboardResponse,
  type EandCScanRequest,
  type EandCScanResponse,
  type ImprovingRsResponse,
  type ImprovingRsWindow,
  type IndustryGroupsResponse,
  type IndustryGroupStockItem,
  type MarketKey,
  type NearPivotScanRequest,
  type PullBackScanRequest,
  type ReturnsScanRequest,
  type RefreshResponse,
  type ScanMatch,
  type ScanSectorSummary,
  type SectorSortBy,
  type SectorTabResponse,
  type ScanResultsResponse,
  type WatchdogStatusResponse,
  type WatchlistsStateResponse,
  dispatchBackendEvent as _dispatchBackendEvent,
  startKeepAlive,
} from "./lib/api";
import { DEFAULT_CHART_COLORS } from "./lib/chartDefaults";
import { buildSymbolSuggestions } from "./lib/searchSuggestions";
import { applyScannerDisplayAlias, applyScannerDisplayAliases, DEFAULT_SCANNERS } from "./lib/scannerCatalog";

const ChartPanel = lazy(() => import("./components/ChartPanel").then((module) => ({ default: module.ChartPanel })));
const AiScreenerPanel = lazy(() => import("./components/AiScreenerPanel").then((module) => ({ default: module.AiScreenerPanel }))) as React.FC<{
  market: MarketKey;
  onPickSymbol: (symbol: string) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  onVisibleSymbolsChange?: (symbols: string[]) => void;
  selectedSymbol: string | null;
}>;
const ChartGroupModal = lazy(() => import("./components/ChartGroupModal"));
const ConsolidatingScannerPanel = lazy(() => import("./components/ConsolidatingScannerPanel").then((module) => ({ default: module.ConsolidatingScannerPanel })));
const CustomScannerPanel = lazy(() => import("./components/CustomScannerPanel").then((module) => ({ default: module.CustomScannerPanel })));
const EandCScannerPanel = lazy(() => import("./components/EandCScannerPanel").then((module) => ({ default: module.EandCScannerPanel })));
const GapUpScannerPanel = lazy(() => import("./components/GapUpScannerPanel").then((module) => ({ default: module.GapUpScannerPanel })));
const HomePanel = lazy(() => import("./components/HomePanel").then((module) => ({ default: module.HomePanel })));
const ImprovingRsPanel = lazy(() => import("./components/ImprovingRsPanel").then((module) => ({ default: module.ImprovingRsPanel })));
const MarketHealthPanel = lazy(() => import("./components/MarketHealthPanel").then((module) => ({ default: module.MarketHealthPanel })));
const MinerviniScannerPanel = lazy(() => import("./components/MinerviniScannerPanel").then((module) => ({ default: module.MinerviniScannerPanel })));
const MoneyFlowPanel = lazy(() => import("./components/MoneyFlowPanel").then((module) => ({ default: module.MoneyFlowPanel })));
const NewsTab = lazy(() => import("./components/NewsTab"));
const GroupsPanel = lazy(() => import("./components/GroupsPanel").then((module) => ({ default: module.GroupsPanel })));
const TradeJournalPanel = lazy(() => import("./components/TradeJournalPanel").then((module) => ({ default: module.TradeJournalPanel })));
const NearPivotScannerPanel = lazy(() => import("./components/NearPivotScannerPanel").then((module) => ({ default: module.NearPivotScannerPanel })));
const PullBackScannerPanel = lazy(() => import("./components/PullBackScannerPanel").then((module) => ({ default: module.PullBackScannerPanel })));
const ReturnsScannerPanel = lazy(() => import("./components/ReturnsScannerPanel").then((module) => ({ default: module.ReturnsScannerPanel })));
const ScanTable = lazy(() => import("./components/ScanTable").then((module) => ({ default: module.ScanTable })));
const ScreenerSidebar = lazy(() => import("./components/ScreenerSidebar").then((module) => ({ default: module.ScreenerSidebar })));
const SectorExplorerPanel = lazy(() => import("./components/SectorExplorerPanel").then((module) => ({ default: module.SectorExplorerPanel })));
const WatchlistPickerModal = lazy(() => import("./components/WatchlistPickerModal").then((module) => ({ default: module.WatchlistPickerModal })));
const WatchdogTasksModal = lazy(() => import("./components/WatchdogTasksModal").then((module) => ({ default: module.WatchdogTasksModal })));
const WatchlistsPanel = lazy(() => import("./components/WatchlistsPanel").then((module) => ({ default: module.WatchlistsPanel })));

const CHART_PREFERENCES_KEY = "mr-malik-chart-preferences:v2";
const CHART_DRAWINGS_KEY = "mr-malik-chart-drawings:v1";
const CHART_RESPONSE_CACHE_KEY = "mr-malik-chart-response-cache:v2";
const THEME_KEY = "mr-malik-theme:v1";
const CHART_PALETTE_KEY = "mr-malik-chart-palette:v1";
const WATCHLISTS_KEY = "mr-malik-watchlists:v1";
const WATCHLISTS_BACKUP_KEY = "mr-malik-watchlists:backup:v1";
const LEGACY_WATCHLISTS_KEYS = ["mr-malik-watchlists", "stock-scanner-watchlists:v1", "stock-scanner-watchlists"];
const ACTIVE_WATCHLIST_KEY = "mr-malik-active-watchlist:v1";
const SCANNER_SETTINGS_KEY = "mr-malik-scanner-settings:v1";
const SAVED_SCANNERS_KEY = "mr-malik-saved-scanners:v1";
const ACTIVE_MARKET_KEY = "mr-malik-active-market:v1";
const MARKET_VIEW_CACHE_KEY = "mr-malik-market-view-cache:v1";
const MARKET_VIEW_CACHE_MAX_AGE_MS = 4 * 60 * 60 * 1000;

type ThemeKey = "dark" | "light";
type AppPage = "home" | "screener" | "ai-screener" | "sectors" | "groups" | "watchlists" | "market-health" | "money-flow" | "newsdesk" | "journal";
type ResultSortMode = "change" | "rs";
type AutoRefreshMode = "market-open" | "after-hours";
type RefreshSource = "manual" | "auto";
type SavableScannerMode = Exclude<ScreenerMode, "improving-rs">;
type SectorGroupSortMode = "1W" | "1M" | "count-desc" | "count-asc";

function intersectEandCItems(contraction: ScanMatch[], expansion: ScanMatch[]) {
  const expansionBySymbol = new Map(expansion.map((item) => [item.symbol, item]));
  return contraction
    .filter((item) => expansionBySymbol.has(item.symbol))
    .map((item) => {
      const expansionItem = expansionBySymbol.get(item.symbol);
      if (!expansionItem) {
        return item;
      }
      return {
        ...item,
        reasons: [
          ...(item.reasons ?? []),
          "Also qualifies under expansion.",
          ...(expansionItem.reasons ?? []),
        ],
      };
    });
}

function unionEandCItems(contraction: ScanMatch[], expansion: ScanMatch[]) {
  const mergedBySymbol = new Map<string, ScanMatch>();

  for (const item of contraction) {
    mergedBySymbol.set(item.symbol, {
      ...item,
      reasons: [...(item.reasons ?? [])],
    });
  }

  for (const item of expansion) {
    const existing = mergedBySymbol.get(item.symbol);
    if (!existing) {
      mergedBySymbol.set(item.symbol, {
        ...item,
        reasons: [...(item.reasons ?? [])],
      });
      continue;
    }

    mergedBySymbol.set(item.symbol, {
      ...existing,
      score: Math.max(existing.score ?? 0, item.score ?? 0),
      reasons: [...(existing.reasons ?? []), ...(item.reasons ?? [])],
    });
  }

  return Array.from(mergedBySymbol.values()).sort((left, right) => (right.score ?? 0) - (left.score ?? 0));
}

// buildEandCScanResultsResponse was removed as Expansion and Contraction are now independent.

type RibbonItem = {
  key: string;
  label: string;
  price: number;
  change: number;
};

type MarketViewCacheEntry = {
  dashboard: DashboardResponse | null;
  sectorTabData: SectorTabResponse | null;
  groupsData: IndustryGroupsResponse | null;
  universeCatalog: ScanMatch[];
  selectedSymbol: string | null;
};

type PersistedMarketViewCacheEntry = {
  saved_at: string;
  payload: MarketViewCacheEntry;
};

type GroupFocusRequest = {
  groupId?: string | null;
  symbol?: string | null;
  nonce: number;
};

type ChartGroupMember = IndustryGroupStockItem & {
  group_member_rank: number;
};

type ChartGroupContext = {
  groupId: string;
  groupName: string;
  parentSector: string;
  description: string;
  groupRank: number;
  groupRankLabel: string;
  stockRank: number;
  stockCount: number;
  strengthBucket: string;
  trendLabel: string;
  symbols: string[];
  members: ChartGroupMember[];
};

type PersistedChartCacheEntry = {
  saved_at: string;
  payload: ChartResponse;
};

type SavedScannerPreset = {
  id: string;
  name: string;
  mode: SavableScannerMode;
  customFilters?: CustomScanRequest;
  gapUpThreshold?: number;
  gapUpMinLiquidityCrore?: number | null;
  minerviniMinLiquidityCrore?: number | null;
  nearPivotFilters?: NearPivotScanRequest;
  pullBackFilters?: PullBackScanRequest;
  returnsFilters?: ReturnsScanRequest;
  consolidatingFilters?: ConsolidatingScanRequest;
  lastMatchCount?: number;
  lastUpdatedAt?: string | null;
  symbols?: string[];
};

type AppProps = {
  initialMarket?: MarketKey;
  useMarketRoutes?: boolean;
};

type PersistedScannerSettings = {
  customFilters: CustomScanRequest;
  appliedCustomFilters: CustomScanRequest;
  hasAppliedFiltersOnce: boolean;
  gapUpThreshold: number;
  gapUpMinLiquidityCrore: number | null;
  minervini1mMinLiquidityCrore: number | null;
  appliedMinervini1mMinLiquidityCrore: number | null;
  minervini5mMinLiquidityCrore: number | null;
  appliedMinervini5mMinLiquidityCrore: number | null;
  nearPivotFilters: NearPivotScanRequest;
  appliedNearPivotFilters: NearPivotScanRequest;
  pullBackFilters: PullBackScanRequest;
  appliedPullBackFilters: PullBackScanRequest;
  returnsFilters: ReturnsScanRequest;
  appliedReturnsFilters: ReturnsScanRequest;
  consolidatingFilters: ConsolidatingScanRequest;
  appliedConsolidatingFilters: ConsolidatingScanRequest;
  eAndCSettings: EandCScanRequest;
  appliedEAndCSettings: EandCScanRequest;
};

const INDEX_RIBBON_CONFIG: Record<MarketKey, Array<{ key: string; label: string; symbol: string }>> = {
  india: [
    { key: "nifty-50", label: "Nifty 50", symbol: "^NSEI" },
    { key: "sensex", label: "Sensex", symbol: "^BSESN" },
    { key: "bank-nifty", label: "Bank Nifty", symbol: "^NSEBANK" },
    { key: "nifty-it", label: "Nifty IT", symbol: "^CNXIT" },
    { key: "nifty-auto", label: "Nifty Auto", symbol: "^CNXAUTO" },
    { key: "nifty-fmcg", label: "Nifty FMCG", symbol: "^CNXFMCG" },
    { key: "nifty-pharma", label: "Nifty Pharma", symbol: "^CNXPHARMA" },
    { key: "nifty-metal", label: "Nifty Metal", symbol: "^CNXMETAL" },
    { key: "nifty-realty", label: "Nifty Realty", symbol: "^CNXREALTY" },
  ],
  us: [
    { key: "sp500", label: "S&P 500", symbol: "^GSPC" },
    { key: "nasdaq", label: "Nasdaq", symbol: "^IXIC" },
    { key: "dow", label: "Dow 30", symbol: "^DJI" },
    { key: "russell", label: "Russell 2000", symbol: "^RUT" },
    { key: "tech", label: "XLK", symbol: "XLK" },
    { key: "financials", label: "XLF", symbol: "XLF" },
    { key: "energy", label: "XLE", symbol: "XLE" },
    { key: "healthcare", label: "XLV", symbol: "XLV" },
    { key: "industrials", label: "XLI", symbol: "XLI" },
  ],
};

const DEFAULT_WATCHLIST_COLORS = ["#4f8cff", "#00a389", "#ff9f1c", "#ef476f", "#7c5cff", "#06b6d4", "#84cc16", "#f97316"];
const MAX_PERSISTED_CHART_RESPONSES = 8;
const CHART_PREFETCH_FORWARD_COUNT = 20;
const CHART_PREFETCH_BACKWARD_COUNT = 5;
const CHART_PREFETCH_PARALLELISM = 4;

function DeferredPanelPlaceholder({ className = "workspace-pad", compact = false }: { className?: string; compact?: boolean }) {
  const blockClassName = compact ? "skeleton-block skeleton-block-sm" : "skeleton-block skeleton-block-lg";
  return (
    <section className={className}>
      <div className="loading-skeleton">
        <div className="skeleton-strip">
          <div className={blockClassName} />
          <div className={blockClassName} />
        </div>
      </div>
    </section>
  );
}

function marketScopedKey(baseKey: string, market: MarketKey) {
  return `${baseKey}:${market}`;
}

function buildChartCacheKey(market: MarketKey, symbol: string, timeframe: ChartTimeframe) {
  return `${market}:${symbol}:${timeframe}`;
}

function isChartResponseCacheCompatible(payload: ChartResponse | null | undefined, savedAt?: string) {
  if (!payload) {
    return false;
  }
  // Expire persisted chart cache entries older than 2 hours
  if (savedAt) {
    const savedDate = new Date(savedAt);
    const nowDate = new Date();
    const ageMs = Date.now() - savedDate.getTime();
    if (!Number.isFinite(ageMs) || ageMs > 2 * 60 * 60 * 1000) {
      return false;
    }
    if (
      savedDate.getFullYear() !== nowDate.getFullYear()
      || savedDate.getMonth() !== nowDate.getMonth()
      || savedDate.getDate() !== nowDate.getDate()
    ) {
      return false;
    }
  }
  if (!payload.summary) {
    return true;
  }
  return typeof payload.summary.adr_pct_20 === "number" && !Number.isNaN(payload.summary.adr_pct_20);
}

function readMarketScopedValue(baseKey: string, market: MarketKey, legacyKeys: string[] = []): string | null {
  if (typeof window === "undefined") {
    return null;
  }

  const scoped = window.localStorage.getItem(marketScopedKey(baseKey, market));
  if (scoped !== null) {
    return scoped;
  }

  if (market !== "india") {
    return null;
  }

  const legacyPrimary = window.localStorage.getItem(baseKey);
  if (legacyPrimary !== null) {
    return legacyPrimary;
  }

  for (const key of legacyKeys) {
    const legacyValue = window.localStorage.getItem(key);
    if (legacyValue !== null) {
      return legacyValue;
    }
  }

  return null;
}

function emptyMarketViewCacheEntry(): MarketViewCacheEntry {
  return {
    dashboard: null,
    sectorTabData: null,
    groupsData: null,
    universeCatalog: [],
    selectedSymbol: null,
  };
}

function normalizeMarketViewCacheEntry(raw: Partial<MarketViewCacheEntry> | null | undefined): MarketViewCacheEntry {
  const sectorTabData = raw?.sectorTabData ?? null;
  const derivedUniverseCatalog = sectorTabData ? buildUniverseCatalogFromSectorTab(sectorTabData) : [];
  return {
    dashboard: raw?.dashboard ?? null,
    sectorTabData,
    groupsData: raw?.groupsData ?? null,
    universeCatalog: raw?.universeCatalog?.length ? raw.universeCatalog : derivedUniverseCatalog,
    selectedSymbol: raw?.selectedSymbol ?? null,
  };
}

function readPersistedMarketViewCache(market: MarketKey): MarketViewCacheEntry {
  const raw = readMarketScopedValue(MARKET_VIEW_CACHE_KEY, market);
  if (!raw) {
    return emptyMarketViewCacheEntry();
  }

  try {
    const parsed = JSON.parse(raw) as PersistedMarketViewCacheEntry | Partial<MarketViewCacheEntry> | null;
    if (!parsed || typeof parsed !== "object") {
      return emptyMarketViewCacheEntry();
    }
    const savedAt = "saved_at" in parsed && typeof parsed.saved_at === "string" ? parsed.saved_at : null;
    const payload = "payload" in parsed ? parsed.payload : parsed;
    if (savedAt) {
      const ageMs = Date.now() - new Date(savedAt).getTime();
      if (!Number.isFinite(ageMs) || ageMs > MARKET_VIEW_CACHE_MAX_AGE_MS) {
        return emptyMarketViewCacheEntry();
      }
    }
    return normalizeMarketViewCacheEntry(payload);
  } catch {
    return emptyMarketViewCacheEntry();
  }
}

function persistMarketViewCache(market: MarketKey, entry: MarketViewCacheEntry) {
  if (typeof window === "undefined") {
    return;
  }

  const payload = normalizeMarketViewCacheEntry(entry);
  try {
    window.localStorage.setItem(
      marketScopedKey(MARKET_VIEW_CACHE_KEY, market),
      JSON.stringify({ saved_at: new Date().toISOString(), payload } satisfies PersistedMarketViewCacheEntry),
    );
  } catch {
    // Ignore persistence failures so app startup never depends on storage quota.
  }
}

const DEFAULT_CUSTOM_FILTERS: CustomScanRequest = {
  min_price: null,
  max_price: null,
  listing_date_from: null,
  listing_date_to: null,
  min_change_pct: null,
  max_change_pct: null,
  min_relative_volume: null,
  min_nifty_outperformance: null,
  min_sector_outperformance: null,
  min_rs_rating: null,
  max_rs_rating: null,
  min_stock_return_20d: null,
  min_stock_return_60d: null,
  min_market_cap_crore: 800,
  max_market_cap_crore: null,
  min_trend_strength: null,
  max_pullback_depth_pct: null,
  min_avg_rupee_volume_30d_crore: null,
  min_avg_rupee_turnover_20d_crore: null,
  min_pct_from_52w_low: null,
  max_pct_from_52w_low: null,
  min_pct_from_52w_high: null,
  max_pct_from_52w_high: null,
  min_pct_from_ath: null,
  max_pct_from_ath: null,
  min_gap_pct: null,
  max_gap_pct: null,
  min_day_range_pct: null,
  max_day_range_pct: null,
  min_three_month_rs: null,
  near_high_period: null,
  near_high_max_distance_pct: null,
  price_vs_ma_mode: "any",
  price_vs_ma_key: "ema20",
  require_bullish_ma_order: false,
  require_bearish_ma_order: false,
  price_to_ma_key: "ema10",
  min_price_to_ma_ratio: null,
  max_price_to_ma_ratio: null,
  return_period: "1Y",
  min_return_pct: null,
  max_return_pct: null,
  above_ema20: false,
  above_ema50: false,
  above_ema200: false,
  pattern: "any",
  sort_by: "rs_rating",
  sort_order: "desc",
  limit: 1500,
  scan_date: null,
  highest_vol_lookback_days: null,
};

const DEFAULT_NEAR_PIVOT_FILTERS: NearPivotScanRequest = {
  min_rs_rating: 70,
  max_pct_from_52w_high: 20,
  max_consolidation_range_pct: 8,
  min_consolidation_days: 4,
  min_liquidity_crore: null,
  limit: 1500,
};

const DEFAULT_PULL_BACK_FILTERS: PullBackScanRequest = {
  enable_rs_rating: true,
  min_rs_rating: 70,
  enable_first_leg_up: true,
  min_first_leg_up_pct: 20,
  enable_consolidation_range: true,
  max_consolidation_range_pct: 8,
  enable_consolidation_days: true,
  min_consolidation_days: 4,
  enable_volume_contraction: true,
  max_recent_volume_vs_avg20: 1,
  enable_ma_support: true,
  pullback_ma: "ema20",
  max_ma_distance_pct: 2,
  min_liquidity_crore: null,
  limit: 1500,
};

const DEFAULT_RETURNS_FILTERS: ReturnsScanRequest = {
  timeframe: "1M",
  min_return_pct: null,
  max_return_pct: null,
  above_21_ema: false,
  above_50_ema: false,
  above_200_sma: false,
  enable_first_leg_up: false,
  min_first_leg_up_pct: 15,
  enable_consolidation_filter: false,
  max_drawdown_after_leg_up: 8,
  max_consolidation_range_pct: 8,
  min_consolidation_days: 4,
  enable_volume_contraction: false,
  max_volume_vs_50d_avg: 0.85,
  enable_price_move_filter: false,
  min_price_move_pct: 1,
  max_price_move_pct: 10,
  min_liquidity_crore: null,
  limit: 1500,
};

const DEFAULT_CONSOLIDATING_FILTERS: ConsolidatingScanRequest = {
  enable_run_up_consolidation: true,
  enable_near_multi_year_breakout: true,
  min_liquidity_crore: null,
  limit: 1500,
};

const DEFAULT_E_AND_C_SETTINGS: EandCSettings = {
  min_price: 30,
  contraction_min_avg_volume_50d: 50000,
  contraction_min_day_volume: 25000,
  contraction_require_above_ema50: true,
  contraction_max_price_to_sma50_ratio: 1.25,
  contraction_max_today_change_abs_pct: 2.5,
  contraction_max_prev_day_change_abs_pct: 2.5,
  contraction_max_two_days_ago_change_abs_pct: 3.5,
  contraction_require_prior_run_up: true,
  contraction_min_return_5d: 10,
  contraction_min_return_20d: 20,
  contraction_min_return_60d: 30,
  expansion_min_change_pct: 5,
  expansion_min_avg_volume_50d: 15000,
  expansion_min_day_volume: 25000,
  expansion_min_volume_multiple: 2,
};

const SUPPORTED_INDICATORS: IndicatorKey[] = ["ema10", "ema20", "ema50", "ema200", "vwap"];

function normalizeIndicatorKeys(value: unknown): IndicatorKey[] {
  if (!Array.isArray(value)) {
    return ["ema20", "ema50"];
  }

  const indicators = value.filter((item): item is IndicatorKey => SUPPORTED_INDICATORS.includes(item as IndicatorKey));
  return indicators.length > 0 ? indicators : ["ema20", "ema50"];
}

function normalizeChartColors(value: unknown): ChartColorSettings {
  if (!value || typeof value !== "object") {
    return DEFAULT_CHART_COLORS;
  }

  const candidate = value as Partial<Record<keyof ChartColorSettings, unknown>>;
  return {
    ema10: typeof candidate.ema10 === "string" ? candidate.ema10 : DEFAULT_CHART_COLORS.ema10,
    ema20: typeof candidate.ema20 === "string" ? candidate.ema20 : DEFAULT_CHART_COLORS.ema20,
    ema50: typeof candidate.ema50 === "string" ? candidate.ema50 : DEFAULT_CHART_COLORS.ema50,
    ema200: typeof candidate.ema200 === "string" ? candidate.ema200 : DEFAULT_CHART_COLORS.ema200,
    vwap: typeof candidate.vwap === "string" ? candidate.vwap : DEFAULT_CHART_COLORS.vwap,
    candleUp: typeof candidate.candleUp === "string" ? candidate.candleUp : DEFAULT_CHART_COLORS.candleUp,
    candleDown: typeof candidate.candleDown === "string" ? candidate.candleDown : DEFAULT_CHART_COLORS.candleDown,
    volumeUp: typeof candidate.volumeUp === "string" ? candidate.volumeUp : DEFAULT_CHART_COLORS.volumeUp,
    volumeDown: typeof candidate.volumeDown === "string" ? candidate.volumeDown : DEFAULT_CHART_COLORS.volumeDown,
    volumeHQ: typeof candidate.volumeHQ === "string" ? candidate.volumeHQ : DEFAULT_CHART_COLORS.volumeHQ,
    volumeLQ: typeof candidate.volumeLQ === "string" ? candidate.volumeLQ : DEFAULT_CHART_COLORS.volumeLQ,
    rsLine: typeof candidate.rsLine === "string" ? candidate.rsLine : DEFAULT_CHART_COLORS.rsLine,
    rsMarker: typeof candidate.rsMarker === "string" ? candidate.rsMarker : DEFAULT_CHART_COLORS.rsMarker,
    rsMarkerSize:
      typeof candidate.rsMarkerSize === "number" && Number.isFinite(candidate.rsMarkerSize)
        ? Math.min(8, Math.max(0.5, Number(candidate.rsMarkerSize.toFixed(1))))
        : DEFAULT_CHART_COLORS.rsMarkerSize,
  };
}

function normalizeHexColor(value: unknown, fallback: string): string {
  if (typeof value !== "string") {
    return fallback;
  }
  const normalized = value.trim();
  return /^#[0-9a-fA-F]{6}$/.test(normalized) ? normalized : fallback;
}

function sortIndustryGroupMembers(members: IndustryGroupStockItem[]) {
  return [...members].sort((left, right) => {
    const rsDiff = (right.rs_rating ?? -1) - (left.rs_rating ?? -1);
    if (rsDiff !== 0) {
      return rsDiff;
    }
    return right.return_3m - left.return_3m;
  });
}

function resolveChartGroupContext(
  symbol: string | null,
  payload: IndustryGroupsResponse | null,
  preferredGroupId?: string | null,
): ChartGroupContext | null {
  const normalizedSymbol = symbol?.trim().toUpperCase() ?? "";
  if (!normalizedSymbol || !payload) {
    return null;
  }

  const stock = preferredGroupId
    ? payload.stocks.find((item) => item.final_group_id === preferredGroupId && item.symbol.toUpperCase() === normalizedSymbol)
      ?? payload.stocks.find((item) => item.final_group_id === preferredGroupId)
    : payload.stocks.find((item) => item.symbol.toUpperCase() === normalizedSymbol);
  const groupId = preferredGroupId ?? stock?.final_group_id ?? null;
  if (!groupId) {
    return null;
  }

  const group = payload.groups.find((item) => item.group_id === groupId);
  if (!group) {
    return null;
  }

  const rankedMembers = sortIndustryGroupMembers(
    payload.stocks.filter((item) => item.final_group_id === groupId),
  ).map((member, index) => ({
    ...member,
    group_member_rank: index + 1,
  }));
  const selectedMember = rankedMembers.find((item) => item.symbol.toUpperCase() === normalizedSymbol) ?? rankedMembers[0] ?? null;
  if (!selectedMember) {
    return null;
  }

  return {
    groupId: group.group_id,
    groupName: group.group_name,
    parentSector: group.parent_sector,
    description: group.description,
    groupRank: group.rank,
    groupRankLabel: group.rank_label,
    stockRank: selectedMember.group_member_rank,
    stockCount: rankedMembers.length,
    strengthBucket: group.strength_bucket,
    trendLabel: group.trend_label,
    symbols: Array.from(new Set((group.symbols.length > 0 ? group.symbols : rankedMembers.map((item) => item.symbol)).filter(Boolean))),
    members: rankedMembers,
  };
}

const INDIA_MARKET_TIME_FORMATTER = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Asia/Kolkata",
  weekday: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const US_MARKET_TIME_FORMATTER = new Intl.DateTimeFormat("en-GB", {
  timeZone: "America/New_York",
  weekday: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

function getMarketClock(market: MarketKey, now: Date = new Date()) {
  const formatter = market === "us" ? US_MARKET_TIME_FORMATTER : INDIA_MARKET_TIME_FORMATTER;
  const parts = formatter.formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const hour = Number(values.hour ?? "0");
  const minute = Number(values.minute ?? "0");
  return {
    weekday: values.weekday ?? "Mon",
    totalMinutes: hour * 60 + minute,
  };
}

function getIndiaClock(now: Date = new Date()) {
  return getMarketClock("india", now);
}

function getAutoRefreshSchedule(now: Date = new Date(), market: MarketKey = "india"): {
  mode: AutoRefreshMode;
  delayMs: number;
  label: string;
  detail: string;
  refreshFundamentals: boolean;
} {
  const { weekday, totalMinutes } = getMarketClock(market, now);
  const isTradingDay = weekday !== "Sat" && weekday !== "Sun";

  const openMinutes = market === "us" ? 9 * 60 + 30 : 9 * 60 + 15;
  const closeMinutes = market === "us" ? 16 * 60 : 15 * 60 + 30;
  // How long after close before we auto-refresh to pick up confirmed close data
  const postCloseGraceMinutes = market === "us" ? 20 : 35;
  const tzLabel = market === "us" ? "ET" : "IST";
  const openLabel = market === "us" ? "09:30" : "09:15";
  const closeLabel = market === "us" ? "16:00" : "15:30";

  const isMarketOpen = isTradingDay && totalMinutes >= openMinutes && totalMinutes <= closeMinutes;
  // Window just after close where we want to auto-refresh once to pick up close data
  const isJustAfterClose = isTradingDay && totalMinutes > closeMinutes && totalMinutes < closeMinutes + postCloseGraceMinutes + 5;
  const minutesSinceClose = isTradingDay && totalMinutes > closeMinutes ? totalMinutes - closeMinutes : null;

  if (isMarketOpen) {
    return {
      mode: "market-open" as const,
      delayMs: 0,
      label: market === "us" ? "US Live Sync" : "India Live Sync",
      detail: `Auto-syncing dashboard, screeners, and movers every 2 min during ${openLabel}-${closeLabel} ${tzLabel}`,
      refreshFundamentals: false,
    };
  }

  // Just closed and within the post-close refresh grace window: schedule auto-refresh
  // to fire once the confirmed close data is available
  if (isJustAfterClose && minutesSinceClose !== null) {
    const remainingMs = (postCloseGraceMinutes - minutesSinceClose) * 60_000;
    return {
      mode: "after-hours" as const,
      delayMs: Math.max(remainingMs, 10_000), // at least 10s
      label: market === "us" ? "US Close Snapshot" : "India Close Snapshot",
      detail: `Auto-refreshing close snapshot — ${closeLabel} ${tzLabel} session`,
      refreshFundamentals: false,
    };
  }

  return {
    mode: "after-hours" as const,
    delayMs: 0, // no auto-refresh outside the close window
    label: market === "us" ? "US Close Snapshot" : "India Close Snapshot",
    detail: `Daily cache updates after the ${closeLabel} ${tzLabel} close`,
    refreshFundamentals: false,
  };
}

function buildUniverseCatalogFromSectorTab(data: SectorTabResponse | null): ScanMatch[] {
  if (!data) {
    return [];
  }

  const bySymbol = new Map<string, ScanMatch>();
  for (const sector of data.sectors) {
    if (sector.group_kind === "index") {
      continue;
    }
    for (const group of sector.sub_sectors) {
      for (const company of group.companies) {
        const existing = bySymbol.get(company.symbol);
        if (existing && existing.market_cap_crore >= company.market_cap_crore) {
          continue;
        }
        bySymbol.set(company.symbol, {
          scan_id: "universe",
          symbol: company.symbol,
          name: company.name,
          exchange: company.exchange,
          sector: company.sector,
          sub_sector: company.sub_sector,
          market_cap_crore: company.market_cap_crore,
          last_price: company.last_price,
          change_pct: company.return_1d,
          relative_volume: 0,
          score: 0,
          rs_rating: company.rs_rating ?? null,
          reasons: [],
        });
      }
    }
  }

  return [...bySymbol.values()].sort((left, right) => right.market_cap_crore - left.market_cap_crore);
}

function firstSymbolFromSectorTab(data: SectorTabResponse | null): string | null {
  for (const sector of data?.sectors ?? []) {
    if (sector.group_kind === "index") {
      continue;
    }
    for (const group of sector.sub_sectors) {
      if (group.companies[0]?.symbol) {
        return group.companies[0].symbol;
      }
    }
  }
  return null;
}

function firstSymbolFromIndustryGroups(data: IndustryGroupsResponse | null): string | null {
  return data?.groups[0]?.symbols[0] ?? data?.stocks[0]?.symbol ?? null;
}

async function fetchIndexRibbonItems(market: MarketKey): Promise<RibbonItem[]> {
  const ribbonConfig = INDEX_RIBBON_CONFIG[market];
  try {
    const payload = await getIndexQuotes(ribbonConfig.map((item) => item.symbol), market);
    const liveItems = payload.items
      .map((item) => {
        const config = ribbonConfig.find((candidate) => candidate.symbol === item.symbol);
        if (!config) {
          return null;
        }
        return {
          key: config.key,
          label: config.label,
          price: item.price,
          change: item.change_pct,
        };
      })
      .filter((item): item is RibbonItem => item !== null);

    if (liveItems.length > 0) {
      return liveItems;
    }
  } catch {
    // Fallback below.
  }

  const fallbackItems = await Promise.all(
    ribbonConfig.map(async (indexItem) => {
      try {
        const payload = await getChart(indexItem.symbol, "1D", market);
        const lastBar = payload.bars[payload.bars.length - 1];
        const previousBar = payload.bars[payload.bars.length - 2];
        if (!lastBar || !previousBar || previousBar.close === 0) {
          return null;
        }

        return {
          key: indexItem.key,
          label: indexItem.label,
          price: lastBar.close,
          change: ((lastBar.close / previousBar.close) - 1) * 100,
        };
      } catch {
        return null;
      }
    }),
  );

  return fallbackItems.filter((item): item is RibbonItem => item !== null);
}

function settledError(result: PromiseSettledResult<unknown>): string | null {
  if (result.status !== "rejected") {
    return null;
  }
  return result.reason instanceof Error ? result.reason.message : "Request failed";
}

function normalizeTimeframe(value: string | undefined): ChartTimeframe {
  if (value === "15m" || value === "30m" || value === "1h" || value === "1W") {
    return value;
  }
  return "1D";
}

function normalizeChartPanelTab(value: string | undefined): ChartPanelTab {
  return value === "fundamentals" ? "fundamentals" : "technical";
}

function normalizeRvolScale(value: string | undefined): "sm" | "md" | "lg" {
  if (value === "sm" || value === "lg") {
    return value;
  }
  return "md";
}

function normalizeWidgetPosition(
  value: { x?: number | null; y?: number | null } | null | undefined,
): { x: number; y: number } | null {
  if (!value || typeof value.x !== "number" || typeof value.y !== "number") {
    return null;
  }
  if (!Number.isFinite(value.x) || !Number.isFinite(value.y)) {
    return null;
  }
  return {
    x: Math.max(0, value.x),
    y: Math.max(0, value.y),
  };
}

function readChartPreferences(market: MarketKey): {
  chartPanelTab: ChartPanelTab;
  timeframe: ChartTimeframe;
  chartStyle: ChartStyle;
  showBenchmarkOverlay: boolean;
  indicatorKeys: IndicatorKey[];
  chartColors: ChartColorSettings;
  drawingColor: string;
  showRvol: boolean;
  rvolPos: { x: number; y: number } | null;
  rvolAccentColor: string;
  rvolScale: "sm" | "md" | "lg";
  showEarningsMarkers: boolean;
  earningsMarkerColor: string;
} {
  const defaults = {
    chartPanelTab: "technical" as ChartPanelTab,
    timeframe: "1D" as ChartTimeframe,
    chartStyle: "candles" as ChartStyle,
    showBenchmarkOverlay: false,
    indicatorKeys: ["ema20", "ema50"] as IndicatorKey[],
    chartColors: DEFAULT_CHART_COLORS,
    drawingColor: "#00d2ff",
    showRvol: false,
    rvolPos: null as { x: number; y: number } | null,
    rvolAccentColor: "#00d2ff",
    rvolScale: "md" as const,
    showEarningsMarkers: false,
    earningsMarkerColor: "#ff78b0",
  };

  if (typeof window === "undefined") {
    return defaults;
  }

  try {
    const raw = readMarketScopedValue(CHART_PREFERENCES_KEY, market);
    if (!raw) {
      throw new Error("missing");
    }
    const parsed = JSON.parse(raw) as Partial<{
      chartPanelTab: ChartPanelTab;
      timeframe: string;
      chartStyle: ChartStyle;
      showBenchmarkOverlay: boolean;
      indicatorKeys: IndicatorKey[];
      chartColors: ChartColorSettings;
      drawingColor: string;
      showRvol: boolean;
      rvolPos: { x?: number | null; y?: number | null } | null;
      rvolAccentColor: string;
      rvolScale: "sm" | "md" | "lg";
      showEarningsMarkers: boolean;
      earningsMarkerColor: string;
    }>;
    return {
      chartPanelTab: normalizeChartPanelTab(parsed.chartPanelTab),
      timeframe: normalizeTimeframe(parsed.timeframe),
      chartStyle: parsed.chartStyle === "bars" ? "bars" : "candles",
      showBenchmarkOverlay: parsed.showBenchmarkOverlay === true,
      indicatorKeys: normalizeIndicatorKeys(parsed.indicatorKeys),
      chartColors: normalizeChartColors(parsed.chartColors),
      drawingColor: normalizeHexColor(parsed.drawingColor, defaults.drawingColor),
      showRvol: parsed.showRvol === true,
      rvolPos: normalizeWidgetPosition(parsed.rvolPos),
      rvolAccentColor: normalizeHexColor(parsed.rvolAccentColor, defaults.rvolAccentColor),
      rvolScale: normalizeRvolScale(parsed.rvolScale),
      showEarningsMarkers: parsed.showEarningsMarkers === true,
      earningsMarkerColor: normalizeHexColor(parsed.earningsMarkerColor, defaults.earningsMarkerColor),
    };
  } catch {
    return defaults;
  }
}

function macroIndexFallbackSymbol(cardName: string, market: MarketKey): string | null {
  const normalizedName = cardName.trim().toUpperCase();
  if (market === "us") {
    if (normalizedName === "S&P 500") {
      return "^GSPC";
    }
    if (normalizedName === "NASDAQ 100") {
      return "^IXIC";
    }
    if (normalizedName === "DOW 30") {
      return "^DJI";
    }
    return null;
  }
  if (normalizedName === "NIFTY 50") {
    return "^NSEI";
  }
  if (normalizedName === "BANK NIFTY" || normalizedName === "NIFTY BANK") {
    return "^NSEBANK";
  }
  if (normalizedName === "NIFTY MIDCAP 50") {
    return "^NSEMDCP50";
  }
  return null;
}

function buildIndexFallbackChart(
  sectorTabData: SectorTabResponse | null,
  symbol: string,
  timeframe: ChartTimeframe,
  market: MarketKey,
): ChartResponse | null {
  if (!sectorTabData || timeframe !== "1D") {
    return null;
  }

  const indexCard = sectorTabData.sectors.find(
    (card) => card.group_kind === "index" && macroIndexFallbackSymbol(card.sector, market) === symbol,
  );
  if (!indexCard || indexCard.sparkline.length < 8) {
    return null;
  }

  const bars: ChartBar[] = indexCard.sparkline.map((point, index) => {
    const previousValue = index > 0 ? indexCard.sparkline[index - 1].value : point.value;
    const open = Number(previousValue.toFixed(2));
    const close = Number(point.value.toFixed(2));
    return {
      time: point.time,
      open,
      high: Number(Math.max(open, close).toFixed(2)),
      low: Number(Math.min(open, close).toFixed(2)),
      close,
      volume: 0,
    };
  });

  return {
    symbol,
    timeframe,
    bars,
    summary: null,
    rs_line: [],
    rs_line_markers: [],
  };
}

function readSavedDrawings(market: MarketKey) {
  if (typeof window === "undefined") {
    return {} as Record<string, ChartAnnotation[]>;
  }

  try {
    const raw = readMarketScopedValue(CHART_DRAWINGS_KEY, market);
    return raw ? (JSON.parse(raw) as Record<string, ChartAnnotation[]>) : {};
  } catch {
    return {};
  }
}

function readPersistedChartCache(market: MarketKey) {
  if (typeof window === "undefined") {
    return {} as Record<string, PersistedChartCacheEntry>;
  }

  try {
    const raw = readMarketScopedValue(CHART_RESPONSE_CACHE_KEY, market);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as Record<string, PersistedChartCacheEntry>;
    return Object.fromEntries(
      Object.entries(parsed ?? {}).filter(
        ([, value]) => value && typeof value === "object" && isChartResponseCacheCompatible(value.payload),
      ),
    );
  } catch {
    return {};
  }
}

function prunePersistedChartCache(cache: Record<string, PersistedChartCacheEntry>) {
  return Object.fromEntries(
    Object.entries(cache)
      .sort(([, left], [, right]) => right.saved_at.localeCompare(left.saved_at))
      .slice(0, MAX_PERSISTED_CHART_RESPONSES),
  );
}

function readTheme(): ThemeKey {
  if (typeof window === "undefined") {
    return "dark";
  }
  const saved = window.localStorage.getItem(THEME_KEY);
  return saved === "light" || saved === "linen" ? "light" : "dark";
}

function readActiveMarket(): MarketKey {
  if (typeof window === "undefined") {
    return "india";
  }
  const saved = window.localStorage.getItem(ACTIVE_MARKET_KEY);
  return saved === "us" ? "us" : "india";
}

function readChartPalette(market: MarketKey): ChartPaletteKey {
  if (typeof window === "undefined") {
    return "current";
  }
  const saved = readMarketScopedValue(CHART_PALETTE_KEY, market);
  return saved === "editorial" ? "editorial" : "current";
}

function marketDisplayLabel(market: MarketKey) {
  return market === "india" ? "India" : "US";
}

function buildLocalId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

function normalizeStoredSymbol(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toUpperCase();
  return normalized || null;
}

function normalizeWatchlistColor(value: unknown, fallback: string): string {
  if (typeof value !== "string") {
    return fallback;
  }
  const normalized = value.trim();
  return /^#[0-9a-fA-F]{6}$/.test(normalized) ? normalized : fallback;
}

function normalizeWatchlistBifurcations(value: unknown, fallbackSymbols: string[]): LocalWatchlist["bifurcations"] {
  if (!Array.isArray(value)) {
    return [{ id: "main", name: "Main", symbols: [...fallbackSymbols] }];
  }

  const output: LocalWatchlist["bifurcations"] = [];
  const seenIds = new Set<string>();
  for (const item of value) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const candidate = item as { id?: unknown; name?: unknown; symbols?: unknown };
    const id = typeof candidate.id === "string" && candidate.id.trim() ? candidate.id.trim() : buildLocalId();
    const name = typeof candidate.name === "string" ? candidate.name.trim() : "";
    if (!name || seenIds.has(id)) {
      continue;
    }
    const symbols = Array.isArray(candidate.symbols)
      ? Array.from(
          new Set(
            candidate.symbols
              .map((symbol) => normalizeStoredSymbol(symbol))
              .filter((symbol): symbol is string => Boolean(symbol)),
          ),
        )
      : [];
    seenIds.add(id);
    output.push({ id, name, symbols });
  }

  if (output.length === 0) {
    return [{ id: "main", name: "Main", symbols: [...fallbackSymbols] }];
  }
  return output;
}

function watchlistSymbolsFromBifurcations(bifurcations: LocalWatchlist["bifurcations"]): string[] {
  return Array.from(new Set(bifurcations.flatMap((item) => item.symbols)));
}

function sanitizeWatchlists(value: unknown): LocalWatchlist[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const seenIds = new Set<string>();
  const output: LocalWatchlist[] = [];

  for (const item of value) {
    if (!item || typeof item !== "object") {
      continue;
    }

    const candidate = item as Partial<LocalWatchlist>;
    const rawName = typeof candidate.name === "string" ? candidate.name.trim() : "";
    if (!rawName) {
      continue;
    }

    const id = typeof candidate.id === "string" && candidate.id.trim() ? candidate.id.trim() : buildLocalId();
    if (seenIds.has(id)) {
      continue;
    }

    const legacySymbols = Array.isArray(candidate.symbols)
      ? Array.from(
          new Set(
            candidate.symbols
              .map((symbol) => normalizeStoredSymbol(symbol))
              .filter((symbol): symbol is string => Boolean(symbol)),
          ),
        )
      : [];
    const bifurcations = normalizeWatchlistBifurcations((candidate as { bifurcations?: unknown }).bifurcations, legacySymbols);
    const mergedSymbols = Array.from(new Set([...legacySymbols, ...watchlistSymbolsFromBifurcations(bifurcations)]));
    if (mergedSymbols.length > 0) {
      const firstBifurcation = bifurcations[0];
      if (firstBifurcation) {
        const firstSet = new Set(firstBifurcation.symbols);
        const missing = mergedSymbols.filter((symbol) => !firstSet.has(symbol));
        if (missing.length > 0) {
          firstBifurcation.symbols = [...firstBifurcation.symbols, ...missing];
        }
      }
    }
    const activeBifurcationIdRaw = (candidate as { active_bifurcation_id?: unknown }).active_bifurcation_id;
    const activeBifurcationId = typeof activeBifurcationIdRaw === "string" && bifurcations.some((item) => item.id === activeBifurcationIdRaw)
      ? activeBifurcationIdRaw
      : bifurcations[0]?.id ?? null;
    const color = normalizeWatchlistColor(candidate.color, DEFAULT_WATCHLIST_COLORS[output.length % DEFAULT_WATCHLIST_COLORS.length]);

    seenIds.add(id);
    output.push({
      id,
      name: rawName,
      color,
      symbols: mergedSymbols,
      bifurcations,
      active_bifurcation_id: activeBifurcationId,
    });
  }

  return output;
}

function parseStoredWatchlists(raw: string | null): LocalWatchlist[] | null {
  if (typeof raw !== "string") {
    return null;
  }

  try {
    return sanitizeWatchlists(JSON.parse(raw));
  } catch {
    return null;
  }
}

function readWatchlists(market: MarketKey): LocalWatchlist[] {
  if (typeof window === "undefined") {
    return [];
  }

  const primary = parseStoredWatchlists(readMarketScopedValue(WATCHLISTS_KEY, market));
  if (primary !== null) {
    return primary;
  }

  const backup = parseStoredWatchlists(readMarketScopedValue(WATCHLISTS_BACKUP_KEY, market));
  if (backup !== null) {
    return backup;
  }

  if (market === "india") {
    for (const key of LEGACY_WATCHLISTS_KEYS) {
      const legacy = parseStoredWatchlists(window.localStorage.getItem(key));
      if (legacy !== null) {
        return legacy;
      }
    }
  }

  return [];
}

function readActiveWatchlistId(watchlists: LocalWatchlist[], market: MarketKey): string | null {
  if (typeof window === "undefined") {
    return watchlists[0]?.id ?? null;
  }
  const saved = readMarketScopedValue(ACTIVE_WATCHLIST_KEY, market);
  return watchlists.some((watchlist) => watchlist.id === saved) ? saved : watchlists[0]?.id ?? null;
}

function normalizeWatchlistsStatePayload(
  payload: Pick<WatchlistsStateResponse, "watchlists" | "active_watchlist_id" | "updated_at">,
): { watchlists: LocalWatchlist[]; activeWatchlistId: string | null; updatedAt: number | null } {
  const watchlists = sanitizeWatchlists(payload.watchlists);
  const activeWatchlistId =
    watchlists.some((watchlist) => watchlist.id === payload.active_watchlist_id)
      ? payload.active_watchlist_id
      : watchlists[0]?.id ?? null;
  return { watchlists, activeWatchlistId, updatedAt: payload.updated_at ?? null };
}

function watchlistsStateSignature(watchlists: LocalWatchlist[], activeWatchlistId: string | null) {
  return JSON.stringify({ watchlists, active_watchlist_id: activeWatchlistId });
}

function mergeWithDefaults<T extends Record<string, unknown>>(defaults: T, value: unknown): T {
  if (!value || typeof value !== "object") {
    return { ...defaults };
  }
  return {
    ...defaults,
    ...(value as Partial<T>),
  };
}

function readScannerSettings(market: MarketKey): PersistedScannerSettings {
  const defaults: PersistedScannerSettings = {
    customFilters: DEFAULT_CUSTOM_FILTERS,
    appliedCustomFilters: DEFAULT_CUSTOM_FILTERS,
    hasAppliedFiltersOnce: false,
    gapUpThreshold: 1,
    gapUpMinLiquidityCrore: null,
    minervini1mMinLiquidityCrore: null,
    appliedMinervini1mMinLiquidityCrore: null,
    minervini5mMinLiquidityCrore: null,
    appliedMinervini5mMinLiquidityCrore: null,
    nearPivotFilters: DEFAULT_NEAR_PIVOT_FILTERS,
    appliedNearPivotFilters: DEFAULT_NEAR_PIVOT_FILTERS,
    pullBackFilters: DEFAULT_PULL_BACK_FILTERS,
    appliedPullBackFilters: DEFAULT_PULL_BACK_FILTERS,
    returnsFilters: DEFAULT_RETURNS_FILTERS,
    appliedReturnsFilters: DEFAULT_RETURNS_FILTERS,
    consolidatingFilters: DEFAULT_CONSOLIDATING_FILTERS,
    appliedConsolidatingFilters: DEFAULT_CONSOLIDATING_FILTERS,
    eAndCSettings: DEFAULT_E_AND_C_SETTINGS,
    appliedEAndCSettings: DEFAULT_E_AND_C_SETTINGS,
  };

  if (typeof window === "undefined") {
    return defaults;
  }

  try {
    const raw = readMarketScopedValue(SCANNER_SETTINGS_KEY, market);
    if (!raw) {
      return defaults;
    }
    const parsed = JSON.parse(raw) as Partial<PersistedScannerSettings>;
    return {
      customFilters: mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, parsed.customFilters),
      appliedCustomFilters: mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, parsed.appliedCustomFilters),
      hasAppliedFiltersOnce: Boolean(parsed.hasAppliedFiltersOnce),
      gapUpThreshold:
        typeof parsed.gapUpThreshold === "number" && Number.isFinite(parsed.gapUpThreshold) ? parsed.gapUpThreshold : 1,
      gapUpMinLiquidityCrore:
        typeof parsed.gapUpMinLiquidityCrore === "number" && Number.isFinite(parsed.gapUpMinLiquidityCrore)
          ? parsed.gapUpMinLiquidityCrore
          : null,
      minervini1mMinLiquidityCrore:
        typeof parsed.minervini1mMinLiquidityCrore === "number" && Number.isFinite(parsed.minervini1mMinLiquidityCrore)
          ? parsed.minervini1mMinLiquidityCrore
          : null,
      appliedMinervini1mMinLiquidityCrore:
        typeof parsed.appliedMinervini1mMinLiquidityCrore === "number"
        && Number.isFinite(parsed.appliedMinervini1mMinLiquidityCrore)
          ? parsed.appliedMinervini1mMinLiquidityCrore
          : null,
      minervini5mMinLiquidityCrore:
        typeof parsed.minervini5mMinLiquidityCrore === "number" && Number.isFinite(parsed.minervini5mMinLiquidityCrore)
          ? parsed.minervini5mMinLiquidityCrore
          : null,
      appliedMinervini5mMinLiquidityCrore:
        typeof parsed.appliedMinervini5mMinLiquidityCrore === "number"
        && Number.isFinite(parsed.appliedMinervini5mMinLiquidityCrore)
          ? parsed.appliedMinervini5mMinLiquidityCrore
          : null,
      nearPivotFilters: mergeWithDefaults(DEFAULT_NEAR_PIVOT_FILTERS, parsed.nearPivotFilters),
      appliedNearPivotFilters: mergeWithDefaults(DEFAULT_NEAR_PIVOT_FILTERS, parsed.appliedNearPivotFilters),
      pullBackFilters: mergeWithDefaults(DEFAULT_PULL_BACK_FILTERS, parsed.pullBackFilters),
      appliedPullBackFilters: mergeWithDefaults(DEFAULT_PULL_BACK_FILTERS, parsed.appliedPullBackFilters),
      returnsFilters: mergeWithDefaults(DEFAULT_RETURNS_FILTERS, parsed.returnsFilters),
      appliedReturnsFilters: mergeWithDefaults(DEFAULT_RETURNS_FILTERS, parsed.appliedReturnsFilters),
      consolidatingFilters: mergeWithDefaults(DEFAULT_CONSOLIDATING_FILTERS, parsed.consolidatingFilters),
      appliedConsolidatingFilters: mergeWithDefaults(DEFAULT_CONSOLIDATING_FILTERS, parsed.appliedConsolidatingFilters),
      eAndCSettings: mergeWithDefaults(DEFAULT_E_AND_C_SETTINGS, parsed.eAndCSettings),
      appliedEAndCSettings: mergeWithDefaults(DEFAULT_E_AND_C_SETTINGS, parsed.appliedEAndCSettings),
    };
  } catch {
    return defaults;
  }
}

function readSavedScanners(market: MarketKey): SavedScannerPreset[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = readMarketScopedValue(SAVED_SCANNERS_KEY, market);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw) as SavedScannerPreset[];
    const validModes = new Set<SavableScannerMode>([
      "custom-scan",
      "ipo",
      "gap-up-openers",
      "minervini-1m",
      "minervini-5m",
    ]);
    return Array.isArray(parsed)
      ? parsed.filter(
          (item): item is SavedScannerPreset =>
            Boolean(item)
            && typeof item.id === "string"
            && typeof item.name === "string"
            && typeof item.mode === "string"
            && validModes.has(item.mode as SavableScannerMode),
        )
      : [];
  } catch {
    return [];
  }
}

function scannerModeLabel(mode: SavableScannerMode): string {
  if (mode === "custom-scan") {
    return "Custom Scanner";
  }
  if (mode === "ipo") {
    return "IPO";
  }
  if (mode === "gap-up-openers") {
    return "Gap Up Openers";
  }
  if (mode === "near-pivot") {
    return "Near Pivot";
  }
  if (mode === "pull-backs") {
    return "Pull Backs";
  }
  if (mode === "returns") {
    return "Returns";
  }
  if (mode === "consolidating") {
    return "Consolidating";
  }
  if (mode === "minervini-1m") {
    return "Minervini 1 Month";
  }
  if (mode === "minervini-5m") {
    return "Minervini 5 Months";
  }
  return mode;
}

function isSavableScannerMode(mode: ScreenerMode): mode is SavableScannerMode {
  return mode !== "improving-rs" && mode !== "near-pivot" && mode !== "pull-backs" && mode !== "returns" && mode !== "consolidating";
}

function nextSavedScannerName(mode: SavableScannerMode, current: SavedScannerPreset[]) {
  const base = scannerModeLabel(mode);
  const existingCount = current.filter((item) => item.mode === mode).length;
  return `${base} ${existingCount + 1}`;
}

function indiaDateKey(date: Date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function marketDateKey(date: Date, market: MarketKey) {
  const timeZone = market === "us" ? "America/New_York" : "Asia/Kolkata";
  return new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function savedScannerFreshForMarket(savedAt: string | null | undefined, market: MarketKey) {
  if (!savedAt) {
    return false;
  }
  const parsed = new Date(savedAt);
  if (Number.isNaN(parsed.getTime())) {
    return false;
  }
  return marketDateKey(parsed, market) === marketDateKey(new Date(), market);
}

function isFundamentalsPayloadFresh(payload: CompanyFundamentals | null | undefined) {
  if (!payload?.fetched_at) {
    return false;
  }
  const fetchedAt = Date.parse(payload.fetched_at);
  if (!Number.isFinite(fetchedAt)) {
    return false;
  }
  return (Date.now() - fetchedAt) <= (2 * 60 * 60 * 1000);
}

function downloadTextFile(filename: string, contents: string) {
  if (typeof window === "undefined") {
    return;
  }
  const blob = new Blob([contents], { type: "text/plain;charset=utf-8" });
  const url = window.URL.createObjectURL(blob);
  const link = window.document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.URL.revokeObjectURL(url);
}

export default function App({ initialMarket, useMarketRoutes = false }: AppProps) {
  const bootstrapMarket = initialMarket ?? readActiveMarket();
  const initialPreferences = readChartPreferences(bootstrapMarket);
  const initialWatchlists = readWatchlists(bootstrapMarket);
  const initialScannerSettings = readScannerSettings(bootstrapMarket);
  const initialSavedScanners = readSavedScanners(bootstrapMarket);
  const initialSavedDrawings = readSavedDrawings(bootstrapMarket);
  const [backendStatus, setBackendStatus] = useState<"ok" | "warming" | "failed">("ok");
  const [watchdogStatus, setWatchdogStatus] = useState<"checking" | "healthy" | "stale" | "offline">("checking");
  const [watchdogStatusMeta, setWatchdogStatusMeta] = useState("Checking backend freshness...");
  const [activeMarket, setActiveMarket] = useState<MarketKey>(bootstrapMarket);
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [universeCatalog, setUniverseCatalog] = useState<ScanMatch[]>([]);
  const [scanResults, setScanResults] = useState<ScanResultsResponse | null>(null);
  const [scanSectorSummaries, setScanSectorSummaries] = useState<ScanSectorSummary[]>([]);
  const [scanSectorSummariesLoading, setScanSectorSummariesLoading] = useState(false);
  const [sectorTabData, setSectorTabData] = useState<SectorTabResponse | null>(null);
  const [groupsData, setGroupsData] = useState<IndustryGroupsResponse | null>(null);
  const [improvingRsData, setImprovingRsData] = useState<ImprovingRsResponse | null>(null);
  const [chart, setChart] = useState<ChartResponse | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [chartOpen, setChartOpen] = useState(false);
  const [chartPanelTab, setChartPanelTab] = useState<ChartPanelTab>(initialPreferences.chartPanelTab);
  const [timeframe, setTimeframe] = useState(initialPreferences.timeframe);
  const [chartStyle, setChartStyle] = useState<ChartStyle>(initialPreferences.chartStyle);
  const [chartPalette, setChartPalette] = useState<ChartPaletteKey>(readChartPalette(bootstrapMarket));
  const [showBenchmarkOverlay, setShowBenchmarkOverlay] = useState(initialPreferences.showBenchmarkOverlay);
  const [indicatorKeys, setIndicatorKeys] = useState<IndicatorKey[]>(initialPreferences.indicatorKeys);
  const [chartColors, setChartColors] = useState<ChartColorSettings>(initialPreferences.chartColors);
  const [chartDrawingColor, setChartDrawingColor] = useState(initialPreferences.drawingColor);
  const [showRvol, setShowRvol] = useState(initialPreferences.showRvol);
  const [rvolPos, setRvolPos] = useState<{ x: number; y: number } | null>(initialPreferences.rvolPos);
  const [rvolAccentColor, setRvolAccentColor] = useState(initialPreferences.rvolAccentColor);
  const [rvolScale, setRvolScale] = useState<"sm" | "md" | "lg">(initialPreferences.rvolScale);
  const [showEarningsMarkers, setShowEarningsMarkers] = useState(initialPreferences.showEarningsMarkers);
  const [earningsMarkerColor, setEarningsMarkerColor] = useState(initialPreferences.earningsMarkerColor);
  const [savedDrawings, setSavedDrawings] = useState<Record<string, ChartAnnotation[]>>(initialSavedDrawings);
  const [fundamentalsBySymbol, setFundamentalsBySymbol] = useState<Record<string, CompanyFundamentals>>({});
  const [chartError, setChartError] = useState<string | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartCacheState, setChartCacheState] = useState<"cached" | "live" | null>(null);
  const [fundamentalsLoading, setFundamentalsLoading] = useState(false);
  const [fundamentalsError, setFundamentalsError] = useState<string | null>(null);
  const [activePage, setActivePage] = useState<AppPage>("home");
  const [activeScanner, setActiveScanner] = useState<ScreenerMode>("custom-scan");
  const [eAndCSettings, setEAndCSettings] = useState<EandCScanRequest>(initialScannerSettings.eAndCSettings);
  const [appliedEAndCSettings, setAppliedEAndCSettings] = useState<EandCScanRequest>(initialScannerSettings.appliedEAndCSettings);
  const [resultSortMode, setResultSortMode] = useState<ResultSortMode>("rs");
  const [customFilters, setCustomFilters] = useState<CustomScanRequest>(initialScannerSettings.customFilters);
  const [appliedCustomFilters, setAppliedCustomFilters] = useState<CustomScanRequest>(initialScannerSettings.appliedCustomFilters);
  const [hasAppliedFiltersOnce, setHasAppliedFiltersOnce] = useState(initialScannerSettings.hasAppliedFiltersOnce);
  const [gapUpThreshold, setGapUpThreshold] = useState(initialScannerSettings.gapUpThreshold);
  const [gapUpMinLiquidityCrore, setGapUpMinLiquidityCrore] = useState<number | null>(initialScannerSettings.gapUpMinLiquidityCrore);
  const [minervini1mMinLiquidityCrore, setMinervini1mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.minervini1mMinLiquidityCrore,
  );
  const [appliedMinervini1mMinLiquidityCrore, setAppliedMinervini1mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.appliedMinervini1mMinLiquidityCrore,
  );
  const [minervini5mMinLiquidityCrore, setMinervini5mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.minervini5mMinLiquidityCrore,
  );
  const [appliedMinervini5mMinLiquidityCrore, setAppliedMinervini5mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.appliedMinervini5mMinLiquidityCrore,
  );
  const [nearPivotFilters, setNearPivotFilters] = useState<NearPivotScanRequest>(initialScannerSettings.nearPivotFilters);
  const [appliedNearPivotFilters, setAppliedNearPivotFilters] = useState<NearPivotScanRequest>(initialScannerSettings.appliedNearPivotFilters);
  const [pullBackFilters, setPullBackFilters] = useState<PullBackScanRequest>(initialScannerSettings.pullBackFilters);
  const [appliedPullBackFilters, setAppliedPullBackFilters] = useState<PullBackScanRequest>(initialScannerSettings.appliedPullBackFilters);
  const [returnsFilters, setReturnsFilters] = useState<ReturnsScanRequest>(initialScannerSettings.returnsFilters);
  const [appliedReturnsFilters, setAppliedReturnsFilters] = useState<ReturnsScanRequest>(initialScannerSettings.appliedReturnsFilters);
  const [consolidatingFilters, setConsolidatingFilters] = useState<ConsolidatingScanRequest>(initialScannerSettings.consolidatingFilters);
  const [appliedConsolidatingFilters, setAppliedConsolidatingFilters] = useState<ConsolidatingScanRequest>(initialScannerSettings.appliedConsolidatingFilters);
  const [sectorSortBy, setSectorSortBy] = useState<SectorSortBy>("1D");
  const [sectorSortOrder, setSectorSortOrder] = useState<"asc" | "desc">("desc");
  const [sectorVisibleSymbols, setSectorVisibleSymbols] = useState<string[]>([]);
  const [sectorLoading, setSectorLoading] = useState(false);
  const [groupsVisibleSymbols, setGroupsVisibleSymbols] = useState<string[]>([]);
  const [aiVisibleSymbols, setAiVisibleSymbols] = useState<string[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [improvingRsWindow, setImprovingRsWindow] = useState<ImprovingRsWindow>("1D");
  const [improvingRsLoading, setImprovingRsLoading] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const [showScannerSettings, setShowScannerSettings] = useState(true);
  const [theme, setTheme] = useState<ThemeKey>(readTheme);
  const [watchlists, setWatchlists] = useState<LocalWatchlist[]>(initialWatchlists);
  const [isWatchlistsHydrated, setIsWatchlistsHydrated] = useState(false);
  const [activeWatchlistId, setActiveWatchlistId] = useState<string | null>(readActiveWatchlistId(initialWatchlists, bootstrapMarket));
  const [watchlistPickerSymbol, setWatchlistPickerSymbol] = useState<string | null>(null);
  const [journalAddRequest, setJournalAddRequest] = useState<{ symbol: string; suggestedPrice?: number } | null>(null);
  const [chartGroupModalContext, setChartGroupModalContext] = useState<ChartGroupContext | null>(null);
  const [savedScanners, setSavedScanners] = useState<SavedScannerPreset[]>(initialSavedScanners);
  const [activeSavedScannerId, setActiveSavedScannerId] = useState<string | null>(null);
  const [scanArrangementMode, setScanArrangementMode] = useState<"flat" | "sector">("flat");
  const [sectorGroupSortMode, setSectorGroupSortMode] = useState<SectorGroupSortMode>("1W");
  const [groupsFocusRequest, setGroupsFocusRequest] = useState<GroupFocusRequest | null>(null);
  const [scannerRunNonce, setScannerRunNonce] = useState(0);
  const [savingScanner, setSavingScanner] = useState(false);
  const [navSearchQuery, setNavSearchQuery] = useState("");
  const deferredNavSearchQuery = useDeferredValue(navSearchQuery);
  const [tickerTapeItems, setTickerTapeItems] = useState<RibbonItem[]>([]);
  const [clockTick, setClockTick] = useState(() => Date.now());
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [watchdogFixing, setWatchdogFixing] = useState(false);
  const [watchdogTasksOpen, setWatchdogTasksOpen] = useState(false);
  const chartRequestIdRef = useRef(0);
  const fundamentalsRequestIdRef = useRef(0);
  const scanRequestIdRef = useRef(0);
  const scanSectorSummaryRequestIdRef = useRef(0);
  const refreshingRef = useRef(false);
  const lastAutoRefreshAtRef = useRef(0);
  const handleRefreshRef = useRef<(source?: RefreshSource) => Promise<void>>(async () => {});
  const visibleSymbolsRef = useRef<string[]>([]);
  const pageVisibleSymbolsRef = useRef<string[]>([]);
  const chartNavigationSymbolsRef = useRef<string[] | null>(null);
  const selectedSymbolRef = useRef<string | null>(null);
  const prewarmingChartKeysRef = useRef<Set<string>>(new Set());
  const chartCompatibilityRecoveryRef = useRef<Set<string>>(new Set());
  const previousActiveMarketRef = useRef<MarketKey>(activeMarket);
  const watchlistsSyncReadyRef = useRef<Record<MarketKey, boolean>>({ india: false, us: false });
  const watchlistsServerSignatureRef = useRef<Record<MarketKey, string | null>>({ india: null, us: null });
  const watchlistsHydrationRequestIdRef = useRef(0);
  const persistedChartCacheRef = useRef<Record<MarketKey, Record<string, PersistedChartCacheEntry>>>({
    india: readPersistedChartCache("india"),
    us: readPersistedChartCache("us"),
  });
  const marketViewCacheRef = useRef<Record<MarketKey, MarketViewCacheEntry>>({
    india: readPersistedMarketViewCache("india"),
    us: readPersistedMarketViewCache("us"),
  });
  const updateMarketViewCache = (
    market: MarketKey,
    updates: Partial<MarketViewCacheEntry>,
  ) => {
    marketViewCacheRef.current[market] = {
      ...marketViewCacheRef.current[market],
      ...updates,
    };
    persistMarketViewCache(market, marketViewCacheRef.current[market]);
  };

  const prefetchPageModules = (page: AppPage) => {
    if (page === "home") {
      void import("./components/HomePanel");
      return;
    }

    if (page === "screener") {
      void import("./components/ChartPanel");
      void import("./components/ScreenerSidebar");
      void import("./components/ScanTable");
      void import("./components/CustomScannerPanel");
      void import("./components/EandCScannerPanel");
      void import("./components/ImprovingRsPanel");
      void import("./components/GapUpScannerPanel");
      void import("./components/MinerviniScannerPanel");
      return;
    }

    if (page === "sectors") {
      void import("./components/SectorExplorerPanel");
      void import("./components/ChartPanel");
      return;
    }

    if (page === "groups") {
      void import("./components/GroupsPanel");
      void import("./components/ChartPanel");
      return;
    }

    if (page === "watchlists") {
      void import("./components/WatchlistsPanel");
      void import("./components/WatchlistPickerModal");
      void import("./components/ChartPanel");
      return;
    }

    if (page === "market-health") {
      void import("./components/MarketHealthPanel");
      return;
    }

    void import("./components/MoneyFlowPanel");
  };

  const readCachedChart = (market: MarketKey, symbol: string, chartTimeframe: ChartTimeframe) => {
    const cacheKey = buildChartCacheKey(market, symbol, chartTimeframe);
    const inMemory = chartResponseCacheRef.current[cacheKey];
    if (isChartResponseCacheCompatible(inMemory)) {
      return inMemory;
    }
    const persisted = persistedChartCacheRef.current[market][cacheKey];
    return isChartResponseCacheCompatible(persisted?.payload, persisted?.saved_at)
      ? persisted?.payload ?? null
      : null;
  };

  const storeCachedChart = (market: MarketKey, symbol: string, chartTimeframe: ChartTimeframe, payload: ChartResponse) => {
    const cacheKey = buildChartCacheKey(market, symbol, chartTimeframe);
    chartResponseCacheRef.current[cacheKey] = payload;
    const nextPersisted = prunePersistedChartCache({
      ...persistedChartCacheRef.current[market],
      [cacheKey]: {
        saved_at: new Date().toISOString(),
        payload,
      },
    });
    persistedChartCacheRef.current[market] = nextPersisted;
    if (typeof window !== "undefined") {
      try {
        window.localStorage.setItem(marketScopedKey(CHART_RESPONSE_CACHE_KEY, market), JSON.stringify(nextPersisted));
      } catch {
        // Ignore cache persistence failures so chart loading never breaks on quota limits.
      }
    }
  };

  const restoreCachedChart = (market: MarketKey, symbol: string | null, chartTimeframe: ChartTimeframe) => {
    if (!symbol) {
      setChart(null);
      setChartCacheState(null);
      setChartLoading(false);
      return null;
    }

    const cachedChart = readCachedChart(market, symbol, chartTimeframe);
    setChart(cachedChart);
    setChartCacheState(cachedChart ? "cached" : null);
    setChartLoading(false);
    return cachedChart;
  };

  const loadChartForSelection = async (
    symbol: string,
    chartTimeframe: ChartTimeframe,
    market: MarketKey,
    options: { forceNetwork?: boolean; preferCached?: boolean } = {},
  ) => {
    const cachedChart = readCachedChart(market, symbol, chartTimeframe);
    const fallbackChart = cachedChart ?? buildIndexFallbackChart(sectorTabData, symbol, chartTimeframe, market);
    const shouldUseCached = options.preferCached !== false;

    if (shouldUseCached && fallbackChart) {
      setChart(fallbackChart);
      setChartError(null);
      setChartCacheState(cachedChart ? "cached" : null);
      setChartLoading(Boolean(options.forceNetwork));
      if (!options.forceNetwork && cachedChart) {
        return cachedChart;
      }
    } else {
      setChartLoading(true);
    }

    const requestId = chartRequestIdRef.current + 1;
    chartRequestIdRef.current = requestId;

    try {
      const payload = await getChart(symbol, chartTimeframe, market);
      if (chartRequestIdRef.current !== requestId || payload.symbol !== symbol || payload.timeframe !== chartTimeframe) {
        return fallbackChart;
      }
      const shouldKeepFallbackChart = Boolean(
        fallbackChart && fallbackChart.bars.length >= 20 && (payload.bars?.length ?? 0) < 20,
      );
      if (shouldKeepFallbackChart) {
        setChart(fallbackChart);
        setChartError("Live chart data is temporarily sparse. Showing fallback chart.");
        setChartLoading(false);
        setChartCacheState(cachedChart ? "cached" : null);
        return fallbackChart;
      }
      storeCachedChart(market, symbol, chartTimeframe, payload);
      setChart(payload);
      setChartError(null);
      setChartLoading(false);
      setChartCacheState("live");
      return payload;
    } catch (loadError) {
      if (chartRequestIdRef.current !== requestId) {
        return fallbackChart;
      }
      setChartLoading(false);
      if (fallbackChart) {
        setChart(fallbackChart);
        setChartCacheState(cachedChart ? "cached" : null);
        setChartError(
          loadError instanceof Error ? `${loadError.message}. Showing cached chart.` : "Failed to refresh chart. Showing cached chart.",
        );
        return fallbackChart;
      }
      setChartCacheState(null);
      throw loadError;
    }
  };

  const applyChartPreferences = (preferences: ReturnType<typeof readChartPreferences>) => {
    setChartPanelTab(preferences.chartPanelTab);
    setTimeframe(preferences.timeframe);
    setChartStyle(preferences.chartStyle);
    setShowBenchmarkOverlay(preferences.showBenchmarkOverlay);
    setIndicatorKeys(preferences.indicatorKeys);
    setChartColors(preferences.chartColors);
    setChartDrawingColor(preferences.drawingColor);
    setShowRvol(preferences.showRvol);
    setRvolPos(preferences.rvolPos);
    setRvolAccentColor(preferences.rvolAccentColor);
    setRvolScale(preferences.rvolScale);
    setShowEarningsMarkers(preferences.showEarningsMarkers);
    setEarningsMarkerColor(preferences.earningsMarkerColor);
  };

  const applyScannerSettings = (settings: PersistedScannerSettings) => {
    setCustomFilters(settings.customFilters);
    setAppliedCustomFilters(settings.appliedCustomFilters);
    setHasAppliedFiltersOnce(settings.hasAppliedFiltersOnce);
    setGapUpThreshold(settings.gapUpThreshold);
    setGapUpMinLiquidityCrore(settings.gapUpMinLiquidityCrore);
    setMinervini1mMinLiquidityCrore(settings.minervini1mMinLiquidityCrore);
    setAppliedMinervini1mMinLiquidityCrore(settings.appliedMinervini1mMinLiquidityCrore);
    setMinervini5mMinLiquidityCrore(settings.minervini5mMinLiquidityCrore);
    setAppliedMinervini5mMinLiquidityCrore(settings.appliedMinervini5mMinLiquidityCrore);
    setNearPivotFilters(settings.nearPivotFilters);
    setAppliedNearPivotFilters(settings.appliedNearPivotFilters);
    setPullBackFilters(settings.pullBackFilters);
    setAppliedPullBackFilters(settings.appliedPullBackFilters);
    setReturnsFilters(settings.returnsFilters);
    setAppliedReturnsFilters(settings.appliedReturnsFilters);
    setConsolidatingFilters(settings.consolidatingFilters);
    setAppliedConsolidatingFilters(settings.appliedConsolidatingFilters);
    setEAndCSettings(settings.eAndCSettings);
    setAppliedEAndCSettings(settings.appliedEAndCSettings);
  };

  const handleMarketChange = (nextMarket: MarketKey) => {
    if (nextMarket === activeMarket) {
      return;
    }

    const nextWatchlists = readWatchlists(nextMarket);
    const nextPreferences = readChartPreferences(nextMarket);
    const nextScannerSettings = readScannerSettings(nextMarket);
    const nextSavedScanners = readSavedScanners(nextMarket);
    const nextSavedDrawings = readSavedDrawings(nextMarket);
    const nextChartPalette = readChartPalette(nextMarket);

    applyChartPreferences(nextPreferences);
    applyScannerSettings(nextScannerSettings);
    setWatchlists(nextWatchlists);
    setActiveWatchlistId(readActiveWatchlistId(nextWatchlists, nextMarket));
    setWatchlistPickerSymbol(null);
    setSavedScanners(nextSavedScanners);
    setActiveSavedScannerId(null);
    setSavedDrawings(nextSavedDrawings);
    setChartPalette(nextChartPalette);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setScanLoading(false);
    setImprovingRsLoading(false);
    setSectorLoading(false);
    setGroupsData(null);
    setGroupsVisibleSymbols([]);
    setGroupsLoading(false);
    setGroupsFocusRequest(null);
    setChartGroupModalContext(null);
    setJournalAddRequest(null);
    setSelectedSymbol(null);
    setChart(null);
    setChartOpen(false);
    setChartError(null);
    setChartLoading(false);
    setChartCacheState(null);
    setFundamentalsBySymbol({});
    setFundamentalsError(null);
    setActiveMarket(nextMarket);
  };

  useEffect(() => {
    if (!useMarketRoutes || typeof window === "undefined") {
      return;
    }

    const targetPath = activeMarket === "us" ? "/us" : "/india";
    if (window.location.pathname !== targetPath) {
      window.history.replaceState(window.history.state, "", targetPath);
    }
  }, [activeMarket, useMarketRoutes]);
  const chartResponseCacheRef = useRef<Record<string, ChartResponse>>({});
  const tickerRequestIdRef = useRef(0);
  const refreshTickerRibbonRef = useRef<() => void>(() => {});

  const refreshTickerRibbon = () => {
    const requestId = tickerRequestIdRef.current + 1;
    tickerRequestIdRef.current = requestId;

    void fetchIndexRibbonItems(activeMarket)
      .then((items) => {
        if (tickerRequestIdRef.current !== requestId || items.length === 0) {
          return;
        }
        setTickerTapeItems(items);
      })
      .catch(() => {});
  };

  refreshTickerRibbonRef.current = refreshTickerRibbon;

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(ACTIVE_MARKET_KEY, activeMarket);
    const cachedView = marketViewCacheRef.current[activeMarket];
    const cachedDashboard = cachedView.dashboard;
    const cachedSectorTab = cachedView.sectorTabData;
    const cachedGroups = cachedView.groupsData;
    const cachedUniverseCatalog = cachedView.universeCatalog;
    const fallbackSelectedSymbol = cachedView.selectedSymbol
      ?? cachedDashboard?.top_gainers[0]?.symbol
      ?? firstSymbolFromSectorTab(cachedSectorTab)
      ?? firstSymbolFromIndustryGroups(cachedGroups);

    setLoading(!cachedDashboard);
    setError(null);
    setDashboard(cachedDashboard);
    setSectorTabData(cachedSectorTab);
    setGroupsData(cachedGroups);
    setUniverseCatalog(cachedUniverseCatalog);
    setSelectedSymbol(fallbackSelectedSymbol);
    chartRequestIdRef.current += 1;
    restoreCachedChart(activeMarket, fallbackSelectedSymbol, timeframe);
    setChartError(null);
    setTickerTapeItems([]);
    setFundamentalsBySymbol({});
    setFundamentalsError(null);
    setImprovingRsData(null);

    setScanResults(null);
    setScanSectorSummaries([]);
  }, [activeMarket]);

  useEffect(() => {
    let active = true;
    const requestId = watchlistsHydrationRequestIdRef.current + 1;
    watchlistsHydrationRequestIdRef.current = requestId;
    watchlistsSyncReadyRef.current[activeMarket] = false;

    async function hydrateWatchlists() {
      const localWatchlists = readWatchlists(activeMarket);
      const localActiveWatchlistId = readActiveWatchlistId(localWatchlists, activeMarket);

      try {
        const remoteState = await getWatchlistsState(activeMarket);
        if (!active || watchlistsHydrationRequestIdRef.current !== requestId) {
          return;
        }

        const normalizedRemote = normalizeWatchlistsStatePayload(remoteState);
        const hasRemoteData = normalizedRemote.watchlists.length > 0;
        const remoteTimestamp = normalizedRemote.updatedAt ?? 0;
        
        // Find newest local timestamp across all watchlists as a proxy for most recent local edit.
        const localTimestamp = Math.max(0, ...localWatchlists.map(w => w.updated_at ?? 0));

        let nextWatchlists = localWatchlists;
        let nextActiveWatchlistId = localActiveWatchlistId;

        // "Smart Restore" Logic: 
        // 1. If server HAS data and it's NEWER than local, we download it (Multi-computer sync).
        // 2. If server has data but it's OLDER than local, we stick to local (it will be pushed below).
        // 3. If server is EMPTY but local is NOT, we stick to local (Update/Wipe recovery).
        const shouldPreferRemote = hasRemoteData && remoteTimestamp > localTimestamp;
        
        if (shouldPreferRemote) {
          nextWatchlists = normalizedRemote.watchlists;
          nextActiveWatchlistId = normalizedRemote.activeWatchlistId;
        }

        setWatchlists(nextWatchlists);
        setActiveWatchlistId(nextActiveWatchlistId);
        setIsWatchlistsHydrated(true);

        // Update technical signatures to avoid redundant syncs.
        watchlistsServerSignatureRef.current[activeMarket] = watchlistsStateSignature(
          hasRemoteData ? normalizedRemote.watchlists : [],
          hasRemoteData ? normalizedRemote.activeWatchlistId : null,
        );
        watchlistsSyncReadyRef.current[activeMarket] = true;

        // Trigger a force-push back to server if:
        // - The server was empty but we had local data.
        // - Our local data is newer than what we just saw on the server.
        const needsRestoration = (!hasRemoteData && localWatchlists.length > 0) || (hasRemoteData && localTimestamp > remoteTimestamp);
        
        if (needsRestoration) {
          const localPayload = {
            watchlists: nextWatchlists,
            active_watchlist_id: nextActiveWatchlistId,
            updated_at: localTimestamp || Date.now(),
          };
          const savedState = await saveWatchlistsState(localPayload, activeMarket);
          if (!active || watchlistsHydrationRequestIdRef.current !== requestId) {
            return;
          }
          const normalizedSaved = normalizeWatchlistsStatePayload(savedState);
          watchlistsServerSignatureRef.current[activeMarket] = watchlistsStateSignature(
            normalizedSaved.watchlists,
            normalizedSaved.activeWatchlistId,
          );
        }
      } catch (err) {
        if (!active || watchlistsHydrationRequestIdRef.current !== requestId) {
          return;
        }

        // Catch-all safety: ensure LocalStorage data is AT LEAST loaded locally.
        setWatchlists(localWatchlists);
        setActiveWatchlistId(localActiveWatchlistId);
        setIsWatchlistsHydrated(true);
        watchlistsServerSignatureRef.current[activeMarket] = null;
        watchlistsSyncReadyRef.current[activeMarket] = true;
      }
    }

    void hydrateWatchlists();
    return () => {
      active = false;
    };
  }, [activeMarket]);

  useEffect(() => {
    let active = true;

    async function loadInitialData() {
      const cachedView = marketViewCacheRef.current[activeMarket];
      const hadCachedDashboard = Boolean(cachedView.dashboard);
      let loadingGuardTimeoutId: number | null = null;
      if (!hadCachedDashboard) {
        setLoading(true);
        if (typeof window !== "undefined") {
          loadingGuardTimeoutId = window.setTimeout(() => {
            if (!active) {
              return;
            }
            setLoading(false);
            setError((current) => current ?? "Backend is waking up. Data can take up to a minute on first load.");
          }, 12000);
        }
      }

      try {
        const dashboardPromise = getDashboard(activeMarket);
        const sectorPromise = getSectorTab("1D", "desc", activeMarket);

        let dashboardResolved = false;

        void sectorPromise
          .then((sectorPayload) => {
            if (!active) {
              return;
            }
            const nextUniverseCatalog = buildUniverseCatalogFromSectorTab(sectorPayload);
            setSectorTabData(sectorPayload);
            setUniverseCatalog(nextUniverseCatalog);
            setSelectedSymbol((current) => {
              const universeSymbols = new Set(nextUniverseCatalog.map((item) => item.symbol));
              if (current && universeSymbols.has(current)) {
                updateMarketViewCache(activeMarket, {
                  sectorTabData: sectorPayload,
                  universeCatalog: nextUniverseCatalog,
                  selectedSymbol: current,
                });
                return current;
              }
              const nextSymbol = firstSymbolFromSectorTab(sectorPayload);
              updateMarketViewCache(activeMarket, {
                sectorTabData: sectorPayload,
                universeCatalog: nextUniverseCatalog,
                selectedSymbol: nextSymbol,
              });
              return nextSymbol;
            });
            updateMarketViewCache(activeMarket, {
              sectorTabData: sectorPayload,
              universeCatalog: nextUniverseCatalog,
            });

            if (!hadCachedDashboard && !dashboardResolved) {
              setLoading(false);
              setError((current) => (current === "Backend is waking up. Data can take up to a minute on first load." ? null : current));
            }
          })
          .catch((sectorError) => {
            if (!active && hadCachedDashboard) {
              return;
            }
            if (!hadCachedDashboard) {
              setError((current) => current ?? (sectorError instanceof Error ? sectorError.message : "Failed to load sectors"));
            }
          });

        const dashboardPayload = await dashboardPromise;
        dashboardResolved = true;
        if (loadingGuardTimeoutId !== null && typeof window !== "undefined") {
          window.clearTimeout(loadingGuardTimeoutId);
          loadingGuardTimeoutId = null;
        }
        if (!active) {
          return;
        }

        setDashboard(dashboardPayload);
        updateMarketViewCache(activeMarket, { dashboard: dashboardPayload });
        setSelectedSymbol((current) => current ?? dashboardPayload.top_gainers[0]?.symbol ?? null);
        setLoading(false);
        setError(null);
        refreshTickerRibbon();
      } catch (loadError) {
        if (loadingGuardTimeoutId !== null && typeof window !== "undefined") {
          window.clearTimeout(loadingGuardTimeoutId);
          loadingGuardTimeoutId = null;
        }
        if (active) {
          setLoading(false);
          if (!hadCachedDashboard) {
            setError(loadError instanceof Error ? loadError.message : "Failed to load home page");
          }
        }
      }
    }

    void loadInitialData();
    return () => {
      active = false;
    };
  }, [activeMarket]);

  useEffect(() => {
    void import("./components/HomePanel");
  }, []);

  useEffect(() => {
    if (loading || typeof window === "undefined") {
      return;
    }

    const connection = (navigator as Navigator & { connection?: { saveData?: boolean; effectiveType?: string } }).connection;
    if (connection?.saveData || ["slow-2g", "2g", "3g"].includes(connection?.effectiveType ?? "")) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      void import("./components/ScreenerSidebar");
      void import("./components/ScanTable");
      void import("./components/CustomScannerPanel");
      void import("./components/EandCScannerPanel");
      void import("./components/ImprovingRsPanel");
      void import("./components/GapUpScannerPanel");
      void import("./components/MinerviniScannerPanel");
      void import("./components/SectorExplorerPanel");
      void import("./components/GroupsPanel");
      void import("./components/WatchlistsPanel");
      void import("./components/WatchlistPickerModal");
      void import("./components/MarketHealthPanel");
      void import("./components/MoneyFlowPanel");
    }, 2000);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [loading]);

  useEffect(() => {
    updateMarketViewCache(activeMarket, {
      dashboard,
      sectorTabData,
      groupsData,
      universeCatalog,
      selectedSymbol,
    });
  }, [activeMarket, dashboard, sectorTabData, groupsData, universeCatalog, selectedSymbol]);

  useEffect(() => {
    if (activeMarket !== "us" || !dashboard?.generated_at) {
      return;
    }

    let active = true;
    const dashboardGeneratedAt = dashboard.generated_at;

    async function loadDashboardScanCounts() {
      try {
        const scanners = await getScanCounts(activeMarket);
        if (!active) {
          return;
        }

        setDashboard((current) => {
          if (!current || current.generated_at !== dashboardGeneratedAt) {
            return current;
          }

          const countsById = new Map(scanners.map((scanner) => [scanner.id, scanner.hit_count]));
          return {
            ...current,
            scanners: current.scanners.map((scanner) => ({
              ...scanner,
              hit_count: countsById.get(scanner.id) ?? scanner.hit_count,
            })),
          };
        });
      } catch {
        // Keep the lightweight dashboard response if the follow-up count request fails.
      }
    }

    void loadDashboardScanCounts();
    return () => {
      active = false;
    };
  }, [activeMarket, dashboard?.generated_at]);

  useEffect(() => {
    if (loading) {
      return;
    }

    let active = true;

    async function loadSelectedScanner() {
      const requestId = scanRequestIdRef.current + 1;
      scanRequestIdRef.current = requestId;
      try {
        if (activePage === "home" || activePage === "watchlists" || activePage === "market-health" || activePage === "money-flow") {
          setScanLoading(false);
          setScanSectorSummariesLoading(false);
          setError(null);
          return;
        }

        if (activePage === "sectors") {
          setScanLoading(false);
          setScanSectorSummariesLoading(false);
          if (sectorTabData && sectorTabData.sort_by === sectorSortBy && sectorTabData.sort_order === sectorSortOrder) {
            setSectorLoading(false);
            setError(null);
            return;
          }
          setSectorLoading(true);
          const payload = await getSectorTab(sectorSortBy, sectorSortOrder, activeMarket);
          if (!active) {
            return;
          }
          setSectorTabData(payload);
          setError(null);
          return;
        }

        if (activePage === "groups" || activePage === "watchlists") {
          setScanLoading(false);
          setScanSectorSummariesLoading(false);
          if (groupsData) {
            setGroupsLoading(false);
            setError(null);
            return;
          }
          setGroupsLoading(true);
          try {
            const payload = await getIndustryGroups(activeMarket);
            if (!active) {
              return;
            }
            setGroupsData(payload);
            updateMarketViewCache(activeMarket, { groupsData: payload });
            setSelectedSymbol((current) => (
              current && payload.stocks.some((item) => item.symbol === current)
                ? current
                : firstSymbolFromIndustryGroups(payload)
            ));
            setError(null);
          } catch {
            // ignore groups load failures — UI will render without group ranks.
          }
          return;
        }

        if (activePage === "screener" && activeScanner === "improving-rs") {
          setScanLoading(false);
          setScanSectorSummariesLoading(false);
          if (improvingRsData && improvingRsData.window === improvingRsWindow) {
            setImprovingRsLoading(false);
            setError(null);
            return;
          }
          setImprovingRsLoading(true);
          const payload = await getImprovingRs(improvingRsWindow, activeMarket);
          if (!active || scanRequestIdRef.current !== requestId) {
            return;
          }
          setImprovingRsData(payload);
          setError(null);
          setSelectedSymbol((current) =>
            current && payload.items.some((item) => item.symbol === current) ? current : payload.items[0]?.symbol ?? null,
          );
          return;
        }


        setScanLoading(true);
        setError(null);
        scanSectorSummaryRequestIdRef.current += 1;
        setScanSectorSummaries([]);
        setScanSectorSummariesLoading(false);
        const payload = await requestActiveScannerResults(false);

        if (!payload || !active || scanRequestIdRef.current !== requestId) {
          return;
        }
        setScanResults(payload);
        setScanSectorSummaries(payload.sector_summaries ?? []);
        setSelectedSymbol((current) => {
          if (current && payload.items.some((item) => item.symbol === current)) {
            return current;
          }
          return payload.items[0]?.symbol ?? null;
        });
        setError(null);
      } catch (loadError) {
        if (active && scanRequestIdRef.current === requestId) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load screener");
        }
      } finally {
        if (active && activePage === "sectors") {
          setSectorLoading(false);
        }
        if (active && activePage === "groups") {
          setGroupsLoading(false);
        }
        if (active && activePage === "screener" && activeScanner === "improving-rs") {
          setImprovingRsLoading(false);
        }
        if (active && scanRequestIdRef.current === requestId && activePage === "screener" && activeScanner !== "improving-rs") {
          setScanLoading(false);
        }
      }
    }

    void loadSelectedScanner();
    return () => {
      active = false;
    };
  }, [
    activePage,
    activeMarket,
    activeScanner,
    appliedConsolidatingFilters,
    appliedCustomFilters,
    appliedMinervini1mMinLiquidityCrore,
    appliedMinervini5mMinLiquidityCrore,
    appliedNearPivotFilters,
    appliedPullBackFilters,
    appliedReturnsFilters,
    gapUpMinLiquidityCrore,
    gapUpThreshold,
    groupsData,
    hasAppliedFiltersOnce,
    loading,
    scannerRunNonce,
    sectorSortBy,
    sectorSortOrder,
  ]);

  useEffect(() => {
    if (
      loading ||
      activePage !== "screener" ||
      activeScanner === "improving-rs" ||
      scanArrangementMode !== "sector" ||
      scanLoading ||
      !scanResults ||
      scanResults.items.length === 0 ||
      scanSectorSummaries.length > 0
    ) {
      return;
    }

    let active = true;
    const requestId = scanSectorSummaryRequestIdRef.current + 1;
    scanSectorSummaryRequestIdRef.current = requestId;
    setScanSectorSummariesLoading(true);

    async function loadSectorSummaries() {
      try {
        const payload = await requestActiveScannerResults(true);
        if (!payload || !active || scanSectorSummaryRequestIdRef.current !== requestId) {
          return;
        }
        setScanSectorSummaries(payload.sector_summaries ?? []);
      } catch {
        if (active && scanSectorSummaryRequestIdRef.current === requestId) {
          setScanSectorSummaries([]);
        }
      } finally {
        if (active && scanSectorSummaryRequestIdRef.current === requestId) {
          setScanSectorSummariesLoading(false);
        }
      }
    }

    void loadSectorSummaries();
    return () => {
      active = false;
    };
  }, [
    activePage,
    activeScanner,
    appliedConsolidatingFilters,
    appliedCustomFilters,
    appliedNearPivotFilters,
    appliedPullBackFilters,
    appliedReturnsFilters,
    gapUpMinLiquidityCrore,
    gapUpThreshold,
    hasAppliedFiltersOnce,
    loading,
    scanArrangementMode,
    scanLoading,
    scanResults,
    scanSectorSummaries.length,
  ]);

  useEffect(() => {
    if (loading || activePage === "sectors" || sectorTabData) {
      return;
    }

    let active = true;
    async function prefetchSectorPage() {
      try {
        const payload = await getSectorTab("1M", "desc", activeMarket);
        if (active) {
          setSectorTabData(payload);
        }
      } catch {
        // Prefetch is best-effort and shouldn't disturb the active scanner flow.
      }
    }

    void prefetchSectorPage();
    return () => {
      active = false;
    };
  }, [activeMarket, activePage, loading, sectorTabData]);

  useEffect(() => {
    if (loading || activePage === "groups" || groupsData) {
      return;
    }

    let active = true;
    const prefetchHandle = window.setTimeout(() => {
      void prefetchGroupsPage();
    }, 1200);

    async function prefetchGroupsPage() {
      // Retry with backoff so groups auto-populate even if the backend is
      // still warming up from a cold start on HF free tier.
      const maxAttempts = 3;
      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        if (!active) return;
        try {
          const payload = await getIndustryGroups(activeMarket);
          if (active) {
            setGroupsData(payload);
          }
          return;
        } catch {
          if (attempt < maxAttempts - 1) {
            await new Promise((r) => setTimeout(r, 3000 * (attempt + 1)));
          }
          // Final attempt failure is silently ignored — prefetch is best-effort.
        }
      }
    }

    return () => {
      active = false;
      window.clearTimeout(prefetchHandle);
    };
  }, [activeMarket, activePage, groupsData, loading]);

  useEffect(() => {
    if (!selectedSymbol) {
      chartRequestIdRef.current += 1;
      setChart(null);
      setChartError(null);
      setChartLoading(false);
      setChartCacheState(null);
      return;
    }

    if ((activePage === "home" && !chartOpen) || activePage === "market-health") {
      chartRequestIdRef.current += 1;
      setChartError(null);
      setChartLoading(false);
      return;
    }

    const symbol = selectedSymbol;

    async function loadChart() {
      try {
        await loadChartForSelection(symbol, timeframe, activeMarket, { preferCached: true });
      } catch (loadError) {
        setChartError(loadError instanceof Error ? loadError.message : "Failed to load chart");
      }
    }

    void loadChart();
    return () => {
      chartRequestIdRef.current += 1;
    };
  }, [activeMarket, activePage, chartOpen, selectedSymbol, timeframe]);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setClockTick(Date.now());
    }, 60_000);

    return () => window.clearInterval(intervalId);
  }, []);

  // Refresh the index ribbon (ticker tape) every 3 minutes so prices stay live.
  useEffect(() => {
    const ribbonIntervalId = window.setInterval(() => {
      refreshTickerRibbonRef.current();
    }, 3 * 60 * 1000);
    return () => window.clearInterval(ribbonIntervalId);
  }, []); // empty — always calls the latest version via ref

  // Silent market-data poll — runs every 2 minutes during market hours and
  // continues later into the evening so the home sector heatmap picks up the
  // official close snapshot when the Bhavcopy patch lands after 8:30–9:00 PM IST.
  const silentLivePollRef = useRef<() => void>(() => {});
  silentLivePollRef.current = () => {
    const { weekday, totalMinutes } = getMarketClock(activeMarket);
    const isTradingDay = weekday !== "Sat" && weekday !== "Sun";
    const openMin = activeMarket === "us" ? 9 * 60 + 30 : 9 * 60 + 15;
    const closeMin = activeMarket === "us" ? 16 * 60 : 15 * 60 + 30;
    const eveningSyncEndMin = activeMarket === "us" ? 22 * 60 : 23 * 60;
    const withinActiveWindow = totalMinutes >= openMin && totalMinutes <= closeMin;
    const withinCloseSyncWindow = totalMinutes > closeMin && totalMinutes <= eveningSyncEndMin;
    if (!isTradingDay || (!withinActiveWindow && !withinCloseSyncWindow) || refreshingRef.current) return;

    // First nudge the backend to refresh the market snapshot, then silently reload
    // whichever page the user is actively viewing so movers/screeners do not lag.
    void refreshMarketData(activeMarket).catch(() => null);

    void getSectorTab("1D", "desc", activeMarket)
      .then((payload) => {
        setSectorTabData(payload);
        setUniverseCatalog(buildUniverseCatalogFromSectorTab(payload));
        updateMarketViewCache(activeMarket, { sectorTabData: payload });
      })
      .catch(() => {});

    void getDashboard(activeMarket)
      .then((payload) => {
        setDashboard(payload);
        updateMarketViewCache(activeMarket, { dashboard: payload });
      })
      .catch(() => {});

    if (activePage === "screener" && activeScanner === "improving-rs") {
      void getImprovingRs(improvingRsWindow, activeMarket)
        .then((payload) => {
          setImprovingRsData(payload);
          const preferredSymbol = selectedSymbolRef.current;
          const nextSelected =
            preferredSymbol && payload.items.some((item) => item.symbol === preferredSymbol)
              ? preferredSymbol
              : payload.items[0]?.symbol ?? null;
          if (nextSelected) {
            setSelectedSymbol(nextSelected);
          }
        })
        .catch(() => {});
    } else if (activePage === "screener") {
      void requestActiveScannerResults(true)
        .then((payload) => {
          if (!payload) {
            return;
          }
          syncSelectedSymbolFromScan(payload, selectedSymbolRef.current);
          setScanSectorSummaries(payload.sector_summaries ?? []);
        })
        .catch(() => {});
    }

    if (activePage === "home" || activePage === "groups") {
      void getIndustryGroups(activeMarket)
        .then((payload) => {
          setGroupsData(payload);
          updateMarketViewCache(activeMarket, { groupsData: payload });
        })
        .catch(() => {});
    }
  };
  useEffect(() => {
    const initialDelayId = window.setTimeout(() => {
      silentLivePollRef.current();
    }, 3_000);
    const liveId = window.setInterval(() => {
      silentLivePollRef.current();
    }, 2 * 60 * 1000); // every 2 minutes
    return () => {
      window.clearTimeout(initialDelayId);
      window.clearInterval(liveId);
    };
  }, [activeMarket]);

  useEffect(() => {
    const eventSource = new EventSource(buildWatchdogEventsUrl(activeMarket));

    eventSource.onmessage = (event) => {
      let payload: { event?: string; market?: string; data?: { symbol?: string } } | null = null;
      try {
        payload = JSON.parse(event.data);
      } catch {
        return;
      }

      const eventType = String(payload?.event ?? "").toLowerCase();
      if (!eventType || eventType === "heartbeat") {
        return;
      }

      const eventMarket = String(payload?.market ?? "global").toLowerCase();
      if (eventMarket !== "global" && eventMarket !== activeMarket) {
        return;
      }

      if (eventType === "cache_invalidated") {
        try {
          window.localStorage.removeItem(marketScopedKey(MARKET_VIEW_CACHE_KEY, activeMarket));
        } catch {
          // Ignore storage access failures in private mode.
        }
      }

      if (eventType === "snapshot_refreshed") {
        silentLivePollRef.current();
      }

      if (eventType === "fundamentals_updated") {
        const updatedSymbol = String(payload?.data?.symbol ?? "").toUpperCase();
        const selected = String(selectedSymbolRef.current ?? "").toUpperCase();
        if (!updatedSymbol || !selected || updatedSymbol !== selected) {
          return;
        }

        void getFundamentals(updatedSymbol, activeMarket)
          .then((fundamentalsPayload) => {
            setFundamentalsBySymbol((current) => ({
              ...current,
              [updatedSymbol]: fundamentalsPayload,
            }));
          })
          .catch(() => {});
      }
    };

    return () => {
      eventSource.close();
    };
  }, [activeMarket]);

  // Auto-refresh once after market close to pick up confirmed close data
  useEffect(() => {
    const schedule = getAutoRefreshSchedule(new Date(), activeMarket);
    if (schedule.delayMs <= 0) return;
    const timerId = window.setTimeout(() => {
      void handleRefreshRef.current("auto");
    }, schedule.delayMs);
    return () => window.clearTimeout(timerId);
  }, [activeMarket, clockTick]); // clockTick drives re-evaluation every minute

  useEffect(() => {
    refreshingRef.current = refreshing;
  }, [refreshing]);

  useEffect(() => {
    // Prefetch fundamentals as soon as a stock is selected, regardless of tab.
    // This way the data is ready instantly when the user switches to the fundamentals tab.
    if (!selectedSymbol || activePage === "market-health") {
      setFundamentalsLoading(false);
      return;
    }
    // Don't show loading spinner until the user is actually on the fundamentals tab,
    // but always kick off the background fetch.
    const onFundamentalsTab = chartPanelTab === "fundamentals" && !(activePage === "home" && !chartOpen);
    const cachedFundamentals = fundamentalsBySymbol[selectedSymbol];
    const hasFreshFundamentals = isFundamentalsPayloadFresh(cachedFundamentals);

    if (cachedFundamentals && hasFreshFundamentals) {
      setFundamentalsError(null);
      if (onFundamentalsTab) setFundamentalsLoading(false);
      return;
    }

    if (!onFundamentalsTab) {
      // Background prefetch — no loading spinner, silent fetch
      let cancelled = false;
      getFundamentals(selectedSymbol, activeMarket)
        .then((payload) => {
          if (!cancelled && payload.symbol === selectedSymbol) {
            setFundamentalsBySymbol((prev) => ({ ...prev, [payload.symbol]: payload }));
          }
        })
        .catch(() => {});
      return () => { cancelled = true; };
    }

    let active = true;
    const symbol = selectedSymbol;
    const requestId = fundamentalsRequestIdRef.current + 1;
    fundamentalsRequestIdRef.current = requestId;
    setFundamentalsLoading(true);

    async function loadFundamentals() {
      try {
        const payload = await getFundamentals(symbol, activeMarket);
        if (!active || fundamentalsRequestIdRef.current !== requestId || payload.symbol !== symbol) {
          return;
        }
        setFundamentalsBySymbol((current) => ({
          ...current,
          [symbol]: payload,
        }));
        setFundamentalsError(null);
      } catch (loadError) {
        if (active && fundamentalsRequestIdRef.current === requestId) {
          setFundamentalsError(loadError instanceof Error ? loadError.message : "Failed to load fundamentals");
        }
      } finally {
        if (active && fundamentalsRequestIdRef.current === requestId) {
          setFundamentalsLoading(false);
        }
      }
    }

    void loadFundamentals();
    return () => {
      active = false;
    };
  }, [activeMarket, activePage, chartOpen, chartPanelTab, fundamentalsBySymbol, selectedSymbol]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(
      marketScopedKey(CHART_PREFERENCES_KEY, activeMarket),
      JSON.stringify({
        chartPanelTab,
        timeframe,
        chartStyle,
        showBenchmarkOverlay,
        indicatorKeys,
        chartColors,
        drawingColor: chartDrawingColor,
        showRvol,
        rvolPos,
        rvolAccentColor,
        rvolScale,
        showEarningsMarkers,
        earningsMarkerColor,
      }),
    );
  }, [
    activeMarket,
    chartColors,
    chartDrawingColor,
    chartPanelTab,
    chartStyle,
    earningsMarkerColor,
    indicatorKeys,
    rvolAccentColor,
    rvolPos,
    rvolScale,
    showBenchmarkOverlay,
    showEarningsMarkers,
    showRvol,
    timeframe,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(marketScopedKey(CHART_DRAWINGS_KEY, activeMarket), JSON.stringify(savedDrawings));
  }, [activeMarket, savedDrawings]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(marketScopedKey(CHART_PALETTE_KEY, activeMarket), chartPalette);
  }, [activeMarket, chartPalette]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(THEME_KEY, theme);
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  useEffect(() => {
    if (typeof window === "undefined" || !isWatchlistsHydrated) {
      return;
    }

    const serialized = JSON.stringify(watchlists);
    window.localStorage.setItem(marketScopedKey(WATCHLISTS_KEY, activeMarket), serialized);
    window.localStorage.setItem(marketScopedKey(WATCHLISTS_BACKUP_KEY, activeMarket), serialized);
  }, [activeMarket, watchlists]);

  useEffect(() => {
    if (!watchlistsSyncReadyRef.current[activeMarket]) {
      return;
    }

    const normalizedActiveWatchlistId =
      activeWatchlistId && watchlists.some((watchlist) => watchlist.id === activeWatchlistId)
        ? activeWatchlistId
        : watchlists[0]?.id ?? null;
    const signature = watchlistsStateSignature(watchlists, normalizedActiveWatchlistId);
    if (watchlistsServerSignatureRef.current[activeMarket] === signature) {
      return;
    }

    let cancelled = false;

    async function syncWatchlists() {
      try {
        const savedState = await saveWatchlistsState(
          {
            watchlists,
            active_watchlist_id: normalizedActiveWatchlistId,
          },
          activeMarket,
        );
        if (cancelled) {
          return;
        }

        const normalizedSaved = normalizeWatchlistsStatePayload(savedState);
        const savedSignature = watchlistsStateSignature(normalizedSaved.watchlists, normalizedSaved.activeWatchlistId);
        watchlistsServerSignatureRef.current[activeMarket] = savedSignature;

        if (savedSignature !== signature) {
          setWatchlists(normalizedSaved.watchlists);
          setActiveWatchlistId(normalizedSaved.activeWatchlistId);
        }
      } catch {
        // Keep localStorage as a fallback when the backend sync is temporarily unavailable.
      }
    }

    void syncWatchlists();
    return () => {
      cancelled = true;
    };
  }, [activeMarket, activeWatchlistId, watchlists]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleStorage = (event: StorageEvent) => {
      const watchlistsKey = marketScopedKey(WATCHLISTS_KEY, activeMarket);
      const watchlistsBackupKey = marketScopedKey(WATCHLISTS_BACKUP_KEY, activeMarket);
      if (event.key !== watchlistsKey && event.key !== watchlistsBackupKey) {
        return;
      }

      const nextWatchlists = readWatchlists(activeMarket);
      setWatchlists(nextWatchlists);
      setActiveWatchlistId((current) => {
        if (nextWatchlists.length === 0) {
          return null;
        }
        if (current && nextWatchlists.some((watchlist) => watchlist.id === current)) {
          return current;
        }
        return nextWatchlists[0].id;
      });
    };

    window.addEventListener("storage", handleStorage);
    return () => window.removeEventListener("storage", handleStorage);
  }, [activeMarket]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    if (activeWatchlistId) {
      window.localStorage.setItem(marketScopedKey(ACTIVE_WATCHLIST_KEY, activeMarket), activeWatchlistId);
    } else {
      window.localStorage.removeItem(marketScopedKey(ACTIVE_WATCHLIST_KEY, activeMarket));
    }
  }, [activeMarket, activeWatchlistId]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(
      marketScopedKey(SCANNER_SETTINGS_KEY, activeMarket),
      JSON.stringify({
        customFilters,
        appliedCustomFilters,
        hasAppliedFiltersOnce,
        gapUpThreshold,
        gapUpMinLiquidityCrore,
        minervini1mMinLiquidityCrore,
        appliedMinervini1mMinLiquidityCrore,
        minervini5mMinLiquidityCrore,
        appliedMinervini5mMinLiquidityCrore,
        nearPivotFilters,
        appliedNearPivotFilters,
        pullBackFilters,
        appliedPullBackFilters,
        returnsFilters,
        appliedReturnsFilters,
        consolidatingFilters,
        appliedConsolidatingFilters,
        eAndCSettings,
        appliedEAndCSettings,
      }),
    );
  }, [
    appliedConsolidatingFilters,
    appliedCustomFilters,
    appliedMinervini1mMinLiquidityCrore,
    appliedMinervini5mMinLiquidityCrore,
    appliedNearPivotFilters,
    appliedPullBackFilters,
    appliedReturnsFilters,
    consolidatingFilters,
    customFilters,
    gapUpMinLiquidityCrore,
    gapUpThreshold,
    hasAppliedFiltersOnce,
    minervini1mMinLiquidityCrore,
    minervini5mMinLiquidityCrore,
    nearPivotFilters,
    pullBackFilters,
    returnsFilters,
    eAndCSettings,
    appliedEAndCSettings,
    activeMarket,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(marketScopedKey(SAVED_SCANNERS_KEY, activeMarket), JSON.stringify(savedScanners));
  }, [activeMarket, savedScanners]);

  useEffect(() => {
    if (!watchlists.length) {
      setActiveWatchlistId(null);
      return;
    }

    if (!activeWatchlistId || !watchlists.some((watchlist) => watchlist.id === activeWatchlistId)) {
      setActiveWatchlistId(watchlists[0].id);
    }
  }, [activeWatchlistId, watchlists]);

  const patternOptions = applyScannerDisplayAliases(dashboard?.scanners ?? DEFAULT_SCANNERS).filter(
    (scanner) => scanner.category === "Setups" && scanner.id !== "custom-scan",
  );
  const displayScan = scanResults ? applyScannerDisplayAlias(scanResults.scan) : null;
  const activeWatchlist = watchlists.find((watchlist) => watchlist.id === activeWatchlistId) ?? watchlists[0] ?? null;
  const activeViewCount =
    activePage === "home"
      ? dashboard?.universe_count ?? 0
      : activePage === "sectors"
        ? sectorTabData?.total_sectors ?? 0
        : activePage === "groups"
          ? groupsData?.total_groups ?? 0
        : activePage === "watchlists"
          ? activeWatchlist?.symbols.length ?? 0
          : activeScanner === "improving-rs"
            ? improvingRsData?.total_hits ?? 0
            : scanResults?.total_hits ?? 0;
  const activeViewMetric = activePage === "screener" && activeScanner !== "improving-rs" && scanLoading ? "..." : activeViewCount;
  const activeViewLabel =
    activePage === "home"
      ? "Universe"
      : activePage === "sectors"
        ? "Sectors"
        : activePage === "groups"
          ? "Groups"
        : activePage === "watchlists"
            ? "Watchlist Stocks"
            : activeScanner === "improving-rs"
              ? "RS Leaders"
              : "Matches";
  const visibleScanItems =
    activePage !== "screener" || activeScanner === "improving-rs"
      ? []
      : scanResults?.scan.id === "custom-scan"
        ? [...(scanResults?.items ?? [])]
        : [...(scanResults?.items ?? [])].sort((left, right) => {
            if (resultSortMode === "change") {
              return right.change_pct - left.change_pct;
            }
            return (right.rs_rating ?? Number.NEGATIVE_INFINITY) - (left.rs_rating ?? Number.NEGATIVE_INFINITY);
          });
  const watchlistVisibleSymbols = activeWatchlist?.symbols ?? [];
  const pageVisibleSymbols =
    activePage === "screener" && activeScanner === "improving-rs"
      ? (improvingRsData?.items ?? []).map((item) => item.symbol)
      : activePage === "market-health"
        ? []
      : activePage === "ai-screener"
        ? aiVisibleSymbols
      : activePage === "sectors"
        ? sectorVisibleSymbols
        : activePage === "groups"
          ? groupsVisibleSymbols
        : activePage === "watchlists"
          ? watchlistVisibleSymbols
          : activePage === "home"
            ? (dashboard?.top_gainers ?? []).map((item) => item.symbol)
            : visibleScanItems.map((item) => item.symbol);
  const universeVisibleSymbols = universeCatalog.map((item) => item.symbol);
  const navigationSeed = pageVisibleSymbols.length > 0 ? pageVisibleSymbols : universeVisibleSymbols;
  const visibleSymbols = Array.from(
    new Set(
      chartOpen && selectedSymbol
        ? [selectedSymbol, ...navigationSeed]
        : navigationSeed,
    ),
  );
  const visibleSymbolsKey = visibleSymbols.join("|");
  const displayedChart = chart && chart.symbol === selectedSymbol && chart.timeframe === timeframe ? chart : null;
  const activeChartKey = selectedSymbol ? `${selectedSymbol}:${timeframe}` : null;
  const activeChartSavedAt = useMemo(() => {
    if (!selectedSymbol) {
      return null;
    }
    const chartCacheKey = buildChartCacheKey(activeMarket, selectedSymbol, timeframe);
    return persistedChartCacheRef.current[activeMarket][chartCacheKey]?.saved_at ?? null;
  }, [activeMarket, chart, selectedSymbol, timeframe]);
  const activeAnnotations = activeChartKey ? savedDrawings[activeChartKey] ?? [] : [];
  const activeFundamentals = selectedSymbol ? fundamentalsBySymbol[selectedSymbol] ?? null : null;
  const activeChartGroupContext = useMemo(
    () => resolveChartGroupContext(selectedSymbol, groupsData),
    [groupsData, selectedSymbol],
  );
  const activeChartGroupSummary = useMemo<ChartGroupSummary | null>(() => {
    if (!activeChartGroupContext) {
      return null;
    }
    return {
      groupId: activeChartGroupContext.groupId,
      groupName: activeChartGroupContext.groupName,
      groupRank: activeChartGroupContext.groupRank,
      groupRankLabel: activeChartGroupContext.groupRankLabel,
      stockRank: activeChartGroupContext.stockRank,
      stockCount: activeChartGroupContext.stockCount,
    };
  }, [activeChartGroupContext]);

  useEffect(() => {
    setChartGroupModalContext(null);
  }, [activeMarket, selectedSymbol]);

  useEffect(() => {
    if (!selectedSymbol || !displayedChart) {
      return;
    }

    if ((activePage === "home" && !chartOpen) || activePage === "market-health") {
      return;
    }

    if (isChartResponseCacheCompatible(displayedChart)) {
      return;
    }

    const recoveryKey = buildChartCacheKey(activeMarket, selectedSymbol, timeframe);
    if (chartCompatibilityRecoveryRef.current.has(recoveryKey)) {
      return;
    }

    chartCompatibilityRecoveryRef.current.add(recoveryKey);
    void loadChartForSelection(selectedSymbol, timeframe, activeMarket, {
      forceNetwork: true,
      preferCached: true,
    }).catch((loadError) => {
      setChartError(loadError instanceof Error ? loadError.message : "Failed to refresh chart");
      chartCompatibilityRecoveryRef.current.delete(recoveryKey);
    });
  }, [activeMarket, activePage, chartOpen, displayedChart, selectedSymbol, timeframe]);

  const clampResultLimit = (limit: number | undefined) => Math.max(1, Math.min(5000, limit || 1500));

  const normalizeCustomFilters = (filters: CustomScanRequest): CustomScanRequest => ({
    ...filters,
    limit: clampResultLimit(filters.limit),
  });

  const normalizeNearPivotFilters = (filters: NearPivotScanRequest): NearPivotScanRequest => ({
    ...filters,
    limit: clampResultLimit(filters.limit),
  });

  const normalizePullBackFilters = (filters: PullBackScanRequest): PullBackScanRequest => ({
    ...filters,
    limit: clampResultLimit(filters.limit),
  });

  const normalizeReturnsFilters = (filters: ReturnsScanRequest): ReturnsScanRequest => ({
    ...filters,
    limit: clampResultLimit(filters.limit),
  });

  const normalizeConsolidatingFilters = (filters: ConsolidatingScanRequest): ConsolidatingScanRequest => ({
    ...filters,
    limit: clampResultLimit(filters.limit),
  });

  const normalizeEandCSettings = (settings: EandCScanRequest): EandCScanRequest => ({
    ...DEFAULT_E_AND_C_SETTINGS,
    ...settings,
    min_price: Number.isFinite(settings.min_price) ? Math.max(settings.min_price, 0) : DEFAULT_E_AND_C_SETTINGS.min_price,
    contraction_min_avg_volume_50d: Number.isFinite(settings.contraction_min_avg_volume_50d)
      ? Math.max(Math.floor(settings.contraction_min_avg_volume_50d), 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_min_avg_volume_50d,
    contraction_min_day_volume: Number.isFinite(settings.contraction_min_day_volume)
      ? Math.max(Math.floor(settings.contraction_min_day_volume), 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_min_day_volume,
    contraction_max_price_to_sma50_ratio: Number.isFinite(settings.contraction_max_price_to_sma50_ratio)
      ? Math.max(settings.contraction_max_price_to_sma50_ratio, 0.1)
      : DEFAULT_E_AND_C_SETTINGS.contraction_max_price_to_sma50_ratio,
    contraction_max_today_change_abs_pct: Number.isFinite(settings.contraction_max_today_change_abs_pct)
      ? Math.max(settings.contraction_max_today_change_abs_pct, 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_max_today_change_abs_pct,
    contraction_max_prev_day_change_abs_pct: Number.isFinite(settings.contraction_max_prev_day_change_abs_pct)
      ? Math.max(settings.contraction_max_prev_day_change_abs_pct, 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_max_prev_day_change_abs_pct,
    contraction_max_two_days_ago_change_abs_pct: Number.isFinite(settings.contraction_max_two_days_ago_change_abs_pct)
      ? Math.max(settings.contraction_max_two_days_ago_change_abs_pct, 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_max_two_days_ago_change_abs_pct,
    contraction_require_prior_run_up: typeof settings.contraction_require_prior_run_up === "boolean"
      ? settings.contraction_require_prior_run_up
      : DEFAULT_E_AND_C_SETTINGS.contraction_require_prior_run_up,
    contraction_min_return_5d: Number.isFinite(settings.contraction_min_return_5d)
      ? Math.max(settings.contraction_min_return_5d, 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_min_return_5d,
    contraction_min_return_20d: Number.isFinite(settings.contraction_min_return_20d)
      ? Math.max(settings.contraction_min_return_20d, 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_min_return_20d,
    contraction_min_return_60d: Number.isFinite(settings.contraction_min_return_60d)
      ? Math.max(settings.contraction_min_return_60d, 0)
      : DEFAULT_E_AND_C_SETTINGS.contraction_min_return_60d,
    expansion_min_change_pct: Number.isFinite(settings.expansion_min_change_pct)
      ? settings.expansion_min_change_pct
      : DEFAULT_E_AND_C_SETTINGS.expansion_min_change_pct,
    expansion_min_avg_volume_50d: Number.isFinite(settings.expansion_min_avg_volume_50d)
      ? Math.max(Math.floor(settings.expansion_min_avg_volume_50d), 0)
      : DEFAULT_E_AND_C_SETTINGS.expansion_min_avg_volume_50d,
    expansion_min_day_volume: Number.isFinite(settings.expansion_min_day_volume)
      ? Math.max(Math.floor(settings.expansion_min_day_volume), 0)
      : DEFAULT_E_AND_C_SETTINGS.expansion_min_day_volume,
    expansion_min_volume_multiple: Number.isFinite(settings.expansion_min_volume_multiple)
      ? Math.max(settings.expansion_min_volume_multiple, 0)
      : DEFAULT_E_AND_C_SETTINGS.expansion_min_volume_multiple,
  });

  const requestActiveScannerResults = (includeSectorSummaries = false) => {
    const options = { includeSectorSummaries };
    if (activeScanner === "ipo") {
      return getScanResults("ipo", activeMarket, options);
    }
    if (activeScanner === "gap-up-openers") {
      return getGapUpOpeners(gapUpThreshold, activeMarket, gapUpMinLiquidityCrore, options);
    }
    if (activeScanner === "ema-expansion") {
      return getScanResults("ema-expansion", activeMarket, options);
    }
    if (activeScanner === "contraction") {
      return getScanResults("contraction", activeMarket, options);
    }
    if (activeScanner === "near-pivot") {
      return getNearPivotScan(appliedNearPivotFilters, activeMarket, options);
    }
    if (activeScanner === "pull-backs") {
      return getPullBackScan(appliedPullBackFilters, activeMarket, options);
    }
    if (activeScanner === "returns") {
      return getReturnsScan(appliedReturnsFilters, activeMarket, options);
    }
    if (activeScanner === "consolidating") {
      return getConsolidatingScan(appliedConsolidatingFilters, activeMarket, options);
    }
    if (activeScanner === "minervini-1m") {
      return getScanResults("minervini-1m", activeMarket, { ...options, minLiquidityCrore: appliedMinervini1mMinLiquidityCrore });
    }
    if (activeScanner === "minervini-5m") {
      return getScanResults("minervini-5m", activeMarket, { ...options, minLiquidityCrore: appliedMinervini5mMinLiquidityCrore });
    }
    if (activeScanner === "custom-scan" && !hasAppliedFiltersOnce) {
      return Promise.resolve(null);
    }
    return runCustomScan(appliedCustomFilters, activeMarket, options);
  };

  const syncSelectedSymbolFromScan = (payload: ScanResultsResponse, preferredSymbol?: string | null) => {
    const nextSelectedSymbol =
      preferredSymbol && payload.items.some((item) => item.symbol === preferredSymbol)
        ? preferredSymbol
        : payload.items[0]?.symbol ?? null;
    setScanResults(payload);
    setScanSectorSummaries(payload.sector_summaries ?? []);
    setSelectedSymbol(nextSelectedSymbol);
  };

  const buildCurrentScannerPreset = (
    mode: SavableScannerMode,
    source: "draft" | "applied",
    base?: SavedScannerPreset,
  ): SavedScannerPreset => {
    const preset: SavedScannerPreset = {
      id: base?.id ?? buildLocalId(),
      name: base?.name ?? nextSavedScannerName(mode, savedScanners),
      mode,
      lastMatchCount: base?.lastMatchCount ?? scanResults?.total_hits ?? 0,
      lastUpdatedAt: base?.lastUpdatedAt ?? null,
      symbols: base?.symbols ?? [],
    };

    if (mode === "custom-scan") {
      preset.customFilters = normalizeCustomFilters(source === "draft" ? customFilters : appliedCustomFilters);
    } else if (mode === "gap-up-openers") {
      preset.gapUpThreshold = gapUpThreshold;
      preset.gapUpMinLiquidityCrore = gapUpMinLiquidityCrore;
    } else if (mode === "near-pivot") {
      preset.nearPivotFilters = normalizeNearPivotFilters(source === "draft" ? nearPivotFilters : appliedNearPivotFilters);
    } else if (mode === "pull-backs") {
      preset.pullBackFilters = normalizePullBackFilters(source === "draft" ? pullBackFilters : appliedPullBackFilters);
    } else if (mode === "returns") {
      preset.returnsFilters = normalizeReturnsFilters(source === "draft" ? returnsFilters : appliedReturnsFilters);
    } else if (mode === "consolidating") {
      preset.consolidatingFilters = normalizeConsolidatingFilters(
        source === "draft" ? consolidatingFilters : appliedConsolidatingFilters,
      );
    } else if (mode === "minervini-1m") {
      preset.minerviniMinLiquidityCrore = source === "draft" ? minervini1mMinLiquidityCrore : appliedMinervini1mMinLiquidityCrore;
    } else if (mode === "minervini-5m") {
      preset.minerviniMinLiquidityCrore = source === "draft" ? minervini5mMinLiquidityCrore : appliedMinervini5mMinLiquidityCrore;
    }

    return preset;
  };

  const runSavedScannerRequest = (preset: SavedScannerPreset, includeSectorSummaries = false) => {
    const options = { includeSectorSummaries };
    if (preset.mode === "custom-scan") {
      return runCustomScan(mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, preset.customFilters), activeMarket, options);
    }
    if (preset.mode === "ipo") {
      return getScanResults("ipo", activeMarket, options);
    }
    if (preset.mode === "gap-up-openers") {
      return getGapUpOpeners(preset.gapUpThreshold ?? 1, activeMarket, preset.gapUpMinLiquidityCrore ?? null, options);
    }
    if (preset.mode === "near-pivot") {
      return getNearPivotScan(mergeWithDefaults(DEFAULT_NEAR_PIVOT_FILTERS, preset.nearPivotFilters), activeMarket, options);
    }
    if (preset.mode === "pull-backs") {
      return getPullBackScan(mergeWithDefaults(DEFAULT_PULL_BACK_FILTERS, preset.pullBackFilters), activeMarket, options);
    }
    if (preset.mode === "returns") {
      return getReturnsScan(mergeWithDefaults(DEFAULT_RETURNS_FILTERS, preset.returnsFilters), activeMarket, options);
    }
    if (preset.mode === "consolidating") {
      return getConsolidatingScan(mergeWithDefaults(DEFAULT_CONSOLIDATING_FILTERS, preset.consolidatingFilters), activeMarket, options);
    }
    if (preset.mode === "minervini-1m") {
      return getScanResults("minervini-1m", activeMarket, { ...options, minLiquidityCrore: preset.minerviniMinLiquidityCrore ?? null });
    }
    if (preset.mode === "minervini-5m") {
      return getScanResults("minervini-5m", activeMarket, { ...options, minLiquidityCrore: preset.minerviniMinLiquidityCrore ?? null });
    }
    return getConsolidatingScan(mergeWithDefaults(DEFAULT_CONSOLIDATING_FILTERS, preset.consolidatingFilters), activeMarket, options);
  };

  const syncSavedScanners = async (presets: SavedScannerPreset[]) => {
    if (!presets.length) {
      return;
    }

    const updates = await Promise.all(
      presets.map(async (preset) => {
        try {
          const payload = await runSavedScannerRequest(preset);
          return {
            id: preset.id,
            lastMatchCount: payload.total_hits,
            lastUpdatedAt: new Date().toISOString(),
            symbols: payload.items.map((item) => item.symbol),
          };
        } catch {
          return null;
        }
      }),
    );

    if (!updates.some(Boolean)) {
      return;
    }

    setSavedScanners((current) =>
      current.map((preset) => {
        const match = updates.find((item) => item?.id === preset.id);
        return match ? { ...preset, ...match } : preset;
      }),
    );
  };

  const handleExportScanResults = () => {
    if (!scanResults || visibleScanItems.length === 0) {
      return;
    }
    const lines = visibleScanItems
      .map((item) => {
        const exchange =
          activeMarket === "india"
            ? item.exchange === "BSE"
              ? "BSE"
              : "NSE"
            : (item.exchange?.trim() || "US");
        return `${exchange}:${item.symbol}`;
      })
      .join("\n");
    const filename = `${scanResults.scan.id.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-${indiaDateKey()}.txt`;
    downloadTextFile(filename, lines);
  };

  const handleSaveCurrentScanner = async () => {
    if (!isSavableScannerMode(activeScanner)) {
      return;
    }

    const activePreset =
      activeSavedScannerId !== null
        ? savedScanners.find((item) => item.id === activeSavedScannerId && item.mode === activeScanner) ?? null
        : null;
    const nextPreset = buildCurrentScannerPreset(activeScanner, "draft", activePreset ?? undefined);

    setSavingScanner(true);
    try {
      const payload = await runSavedScannerRequest(nextPreset);
      const savedAt = new Date().toISOString();
      const finalizedPreset: SavedScannerPreset = {
        ...nextPreset,
        lastMatchCount: payload.total_hits,
        lastUpdatedAt: savedAt,
        symbols: payload.items.map((item) => item.symbol),
      };

      if (activeScanner === "custom-scan" && finalizedPreset.customFilters) {
        setHasAppliedFiltersOnce(true);
        setCustomFilters(finalizedPreset.customFilters);
        setAppliedCustomFilters(finalizedPreset.customFilters);
      } else if (activeScanner === "near-pivot" && finalizedPreset.nearPivotFilters) {
        setNearPivotFilters(finalizedPreset.nearPivotFilters);
        setAppliedNearPivotFilters(finalizedPreset.nearPivotFilters);
      } else if (activeScanner === "pull-backs" && finalizedPreset.pullBackFilters) {
        setPullBackFilters(finalizedPreset.pullBackFilters);
        setAppliedPullBackFilters(finalizedPreset.pullBackFilters);
      } else if (activeScanner === "returns" && finalizedPreset.returnsFilters) {
        setReturnsFilters(finalizedPreset.returnsFilters);
        setAppliedReturnsFilters(finalizedPreset.returnsFilters);
      } else if (activeScanner === "consolidating" && finalizedPreset.consolidatingFilters) {
        setConsolidatingFilters(finalizedPreset.consolidatingFilters);
        setAppliedConsolidatingFilters(finalizedPreset.consolidatingFilters);
      } else if (activeScanner === "minervini-1m") {
        setMinervini1mMinLiquidityCrore(finalizedPreset.minerviniMinLiquidityCrore ?? null);
        setAppliedMinervini1mMinLiquidityCrore(finalizedPreset.minerviniMinLiquidityCrore ?? null);
      } else if (activeScanner === "minervini-5m") {
        setMinervini5mMinLiquidityCrore(finalizedPreset.minerviniMinLiquidityCrore ?? null);
        setAppliedMinervini5mMinLiquidityCrore(finalizedPreset.minerviniMinLiquidityCrore ?? null);
      }

      setSavedScanners((current) => {
        const hasExisting = current.some((item) => item.id === finalizedPreset.id);
        if (!hasExisting) {
          return [finalizedPreset, ...current];
        }
        return current.map((item) => (item.id === finalizedPreset.id ? finalizedPreset : item));
      });
      setActiveSavedScannerId(finalizedPreset.id);
      syncSelectedSymbolFromScan(payload, selectedSymbol);
      setError(null);
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : "Failed to save scanner");
    } finally {
      setSavingScanner(false);
    }
  };

  const handleLoadSavedScanner = (preset: SavedScannerPreset) => {
    setActivePage("screener");
    setShowScannerSettings(true);
    setActiveSavedScannerId(preset.id);
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setActiveScanner(preset.mode);
    setError(null);
    setScannerRunNonce((current) => current + 1);

    if (preset.mode === "custom-scan") {
      const nextFilters = mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, preset.customFilters);
      setCustomFilters(nextFilters);
      setAppliedCustomFilters(nextFilters);
      setHasAppliedFiltersOnce(true);
    } else if (preset.mode === "gap-up-openers") {
      setGapUpThreshold(preset.gapUpThreshold ?? 1);
      setGapUpMinLiquidityCrore(preset.gapUpMinLiquidityCrore ?? null);
    } else if (preset.mode === "near-pivot") {
      const nextFilters = mergeWithDefaults(DEFAULT_NEAR_PIVOT_FILTERS, preset.nearPivotFilters);
      setNearPivotFilters(nextFilters);
      setAppliedNearPivotFilters(nextFilters);
    } else if (preset.mode === "pull-backs") {
      const nextFilters = mergeWithDefaults(DEFAULT_PULL_BACK_FILTERS, preset.pullBackFilters);
      setPullBackFilters(nextFilters);
      setAppliedPullBackFilters(nextFilters);
    } else if (preset.mode === "returns") {
      const nextFilters = mergeWithDefaults(DEFAULT_RETURNS_FILTERS, preset.returnsFilters);
      setReturnsFilters(nextFilters);
      setAppliedReturnsFilters(nextFilters);
    } else if (preset.mode === "consolidating") {
      const nextFilters = mergeWithDefaults(DEFAULT_CONSOLIDATING_FILTERS, preset.consolidatingFilters);
      setConsolidatingFilters(nextFilters);
      setAppliedConsolidatingFilters(nextFilters);
    } else if (preset.mode === "minervini-1m") {
      setMinervini1mMinLiquidityCrore(preset.minerviniMinLiquidityCrore ?? null);
      setAppliedMinervini1mMinLiquidityCrore(preset.minerviniMinLiquidityCrore ?? null);
    } else if (preset.mode === "minervini-5m") {
      setMinervini5mMinLiquidityCrore(preset.minerviniMinLiquidityCrore ?? null);
      setAppliedMinervini5mMinLiquidityCrore(preset.minerviniMinLiquidityCrore ?? null);
    }

    if (preset.symbols?.length) {
      setSelectedSymbol((current) => (current && preset.symbols?.includes(current) ? current : preset.symbols?.[0] ?? current));
    }
  };

  const handleLoadSavedScannerById = (presetId: string) => {
    const preset = savedScanners.find((item) => item.id === presetId);
    if (!preset) {
      return;
    }
    handleLoadSavedScanner(preset);
  };

  const handleDeleteSavedScanner = (presetId: string) => {
    setSavedScanners((current) => current.filter((item) => item.id !== presetId));
    setActiveSavedScannerId((current) => (current === presetId ? null : current));
  };

  const handleScannerModeChange = (mode: ScreenerMode) => {
    setActiveSavedScannerId(null);
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setError(null);
    setActiveScanner(mode);
  };

  useEffect(() => {
    visibleSymbolsRef.current = visibleSymbols;
  }, [visibleSymbols]);

  useEffect(() => {
    pageVisibleSymbolsRef.current = pageVisibleSymbols;
  }, [pageVisibleSymbols]);

  useEffect(() => {
    selectedSymbolRef.current = selectedSymbol;
  }, [selectedSymbol]);

  useEffect(() => {
    if (!selectedSymbol || activePage === "market-health") {
      return;
    }

    const navigationSymbols = chartOpen
      ? chartNavigationSymbolsRef.current?.length
        ? chartNavigationSymbolsRef.current
        : pageVisibleSymbols
      : pageVisibleSymbols.length > 0
        ? pageVisibleSymbols
        : visibleSymbols;

    if (!navigationSymbols.length) {
      return;
    }

    const currentIndex = navigationSymbols.indexOf(selectedSymbol);
    const forwardSymbols = currentIndex >= 0
      ? [
          ...navigationSymbols.slice(currentIndex + 1),
          ...navigationSymbols.slice(0, currentIndex),
        ]
      : navigationSymbols;
    const backwardSymbols = currentIndex >= 0
      ? [...navigationSymbols.slice(0, currentIndex)].reverse()
      : [];
    const symbolsToWarm = Array.from(
      new Set([
        ...forwardSymbols.slice(0, CHART_PREFETCH_FORWARD_COUNT),
        ...backwardSymbols.slice(0, CHART_PREFETCH_BACKWARD_COUNT),
      ]),
    ).filter((symbol) => symbol && symbol !== selectedSymbol);

    if (symbolsToWarm.length === 0) {
      return;
    }

    let cancelled = false;

    const warmChartSymbol = async (symbol: string) => {
      const cacheKey = buildChartCacheKey(activeMarket, symbol, timeframe);
      if (readCachedChart(activeMarket, symbol, timeframe) || prewarmingChartKeysRef.current.has(cacheKey)) {
        return;
      }

      prewarmingChartKeysRef.current.add(cacheKey);
      try {
        const payload = await getChart(symbol, timeframe, activeMarket);
        if (!cancelled && payload.symbol === symbol && payload.timeframe === timeframe) {
          storeCachedChart(activeMarket, symbol, timeframe, payload);
        }
      } catch {
        // Ignore prewarm failures and let explicit chart loads retry on demand.
      } finally {
        prewarmingChartKeysRef.current.delete(cacheKey);
      }
    };

    const timeoutId = window.setTimeout(() => {
      void (async () => {
        for (let index = 0; index < symbolsToWarm.length; index += CHART_PREFETCH_PARALLELISM) {
          if (cancelled) {
            return;
          }
          const batch = symbolsToWarm.slice(index, index + CHART_PREFETCH_PARALLELISM);
          await Promise.allSettled(batch.map((symbol) => warmChartSymbol(symbol)));
        }
      })();
    }, chartOpen ? 80 : 150);

    return () => {
      cancelled = true;
      window.clearTimeout(timeoutId);
    };
  }, [activeMarket, activePage, chartOpen, pageVisibleSymbols, selectedSymbol, timeframe, visibleSymbols]);

  useEffect(() => {
    // When switching TO the watchlists page, only set a default selection if NONE exists.
    // We want to avoid resetting the user's selection just because they visited the watchlist page.
    if (activePage !== "watchlists" || !activeWatchlist) {
      return;
    }

    if (!activeWatchlist.symbols.length) {
      return;
    }

    // Only force a change if there's no selection at all. 
    // If the user already has a selection, we let them keep it, 
    // even if it's not in the currently active watchlist (they might have just added it to another one).
    if (!selectedSymbol) {
      setSelectedSymbol(activeWatchlist.symbols[0]);
    }
  }, [activePage, activeWatchlist]);

  useEffect(() => {
    if (activePage !== "groups" && !chartOpen) {
      chartNavigationSymbolsRef.current = null;
    }
  }, [activePage, chartOpen]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const shouldHandleListNavigation = activePage === "screener" || activePage === "watchlists" || activePage === "ai-screener";
      const shouldHandleGroupsNavigation = activePage === "groups" && !chartOpen && Boolean(chartNavigationSymbolsRef.current?.length);
      const shouldHandleChartContextNavigation =
        chartOpen &&
        (Boolean(chartNavigationSymbolsRef.current?.length) ||
          activePage === "screener" ||
          activePage === "watchlists" ||
          activePage === "sectors" ||
          activePage === "groups" ||
          activePage === "ai-screener");
      if (!shouldHandleChartContextNavigation && !shouldHandleListNavigation && !shouldHandleGroupsNavigation) {
        return;
      }

      const target = event.target as HTMLElement | null;
      const tagName = target?.tagName ?? "";
      const isFormField =
        tagName === "INPUT" ||
        tagName === "TEXTAREA" ||
        tagName === "SELECT" ||
        target?.isContentEditable;

      if (isFormField) {
        return;
      }

      const symbols = chartOpen
        ? chartNavigationSymbolsRef.current?.length
          ? chartNavigationSymbolsRef.current
          : pageVisibleSymbolsRef.current
        : shouldHandleGroupsNavigation && chartNavigationSymbolsRef.current?.length
          ? chartNavigationSymbolsRef.current
          : visibleSymbolsRef.current;
      const currentSymbol = selectedSymbolRef.current;
      if (!symbols.length || !currentSymbol) {
        return;
      }

      const currentIndex = symbols.indexOf(currentSymbol);
      if (currentIndex === -1) {
        return;
      }

      if (event.key === "ArrowUp") {
        event.preventDefault();
        const previousIndex = (currentIndex - 1 + symbols.length) % symbols.length;
        const previousSymbol = symbols[previousIndex];
        if (previousSymbol && previousSymbol !== currentSymbol) {
          setSelectedSymbol(previousSymbol);
          if (chartOpen) {
            setChartOpen(true);
          }
        }
      }

      if (event.key === "ArrowDown") {
        event.preventDefault();
        const nextIndex = (currentIndex + 1) % symbols.length;
        const nextSymbol = symbols[nextIndex];
        if (nextSymbol && nextSymbol !== currentSymbol) {
          setSelectedSymbol(nextSymbol);
          if (chartOpen) {
            setChartOpen(true);
          }
        }
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activePage, chartOpen]);

  useEffect(() => {
    if (!chartOpen) {
      chartNavigationSymbolsRef.current = null;
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setChartOpen(false);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [chartOpen]);

  const handleApplyCustomScan = () => {
    const nextFilters = normalizeCustomFilters(customFilters);
    setHasAppliedFiltersOnce(true);
    setActivePage("screener");
    setActiveScanner("custom-scan");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setCustomFilters(nextFilters);
    setAppliedCustomFilters(nextFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetCustomScan = () => {
    setHasAppliedFiltersOnce(true);
    setActivePage("screener");
    setActiveScanner("custom-scan");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setCustomFilters(DEFAULT_CUSTOM_FILTERS);
    setAppliedCustomFilters(DEFAULT_CUSTOM_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handleEandCSettingsChange = (updates: Partial<EandCScanRequest>) => {
    setEAndCSettings((current) => ({ ...current, ...updates }));
  };

  const handleApplyEandCSettings = () => {
    const nextSettings = normalizeEandCSettings(eAndCSettings);
    setEAndCSettings(nextSettings);
    setAppliedEAndCSettings(nextSettings);
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetEandCSettings = () => {
    setEAndCSettings(DEFAULT_E_AND_C_SETTINGS);
    setAppliedEAndCSettings(DEFAULT_E_AND_C_SETTINGS);
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setScannerRunNonce((current) => current + 1);
  };

  const handleRefreshEandCScan = () => {
    setActivePage("screener");
    setActiveScanner("ema-expansion");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyNearPivotScan = () => {
    const nextFilters = normalizeNearPivotFilters(nearPivotFilters);
    setActivePage("screener");
    setActiveScanner("near-pivot");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setNearPivotFilters(nextFilters);
    setAppliedNearPivotFilters(nextFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetNearPivotScan = () => {
    setActivePage("screener");
    setActiveScanner("near-pivot");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setNearPivotFilters(DEFAULT_NEAR_PIVOT_FILTERS);
    setAppliedNearPivotFilters(DEFAULT_NEAR_PIVOT_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyPullBackScan = () => {
    const nextFilters = normalizePullBackFilters(pullBackFilters);
    setActivePage("screener");
    setActiveScanner("pull-backs");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setPullBackFilters(nextFilters);
    setAppliedPullBackFilters(nextFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetPullBackScan = () => {
    setActivePage("screener");
    setActiveScanner("pull-backs");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setPullBackFilters(DEFAULT_PULL_BACK_FILTERS);
    setAppliedPullBackFilters(DEFAULT_PULL_BACK_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyReturnsScan = () => {
    const nextFilters = normalizeReturnsFilters(returnsFilters);
    setActivePage("screener");
    setActiveScanner("returns");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setReturnsFilters(nextFilters);
    setAppliedReturnsFilters(nextFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetReturnsScan = () => {
    setActivePage("screener");
    setActiveScanner("returns");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setReturnsFilters(DEFAULT_RETURNS_FILTERS);
    setAppliedReturnsFilters(DEFAULT_RETURNS_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyConsolidatingScan = () => {
    const nextFilters = normalizeConsolidatingFilters(consolidatingFilters);
    setActivePage("screener");
    setActiveScanner("consolidating");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setConsolidatingFilters(nextFilters);
    setAppliedConsolidatingFilters(nextFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetConsolidatingScan = () => {
    setActivePage("screener");
    setActiveScanner("consolidating");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setConsolidatingFilters(DEFAULT_CONSOLIDATING_FILTERS);
    setAppliedConsolidatingFilters(DEFAULT_CONSOLIDATING_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyMinervini1mScan = () => {
    setActivePage("screener");
    setActiveScanner("minervini-1m");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setAppliedMinervini1mMinLiquidityCrore(minervini1mMinLiquidityCrore);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetMinervini1mScan = () => {
    setActivePage("screener");
    setActiveScanner("minervini-1m");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setMinervini1mMinLiquidityCrore(null);
    setAppliedMinervini1mMinLiquidityCrore(null);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyMinervini5mScan = () => {
    setActivePage("screener");
    setActiveScanner("minervini-5m");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setAppliedMinervini5mMinLiquidityCrore(minervini5mMinLiquidityCrore);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetMinervini5mScan = () => {
    setActivePage("screener");
    setActiveScanner("minervini-5m");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setMinervini5mMinLiquidityCrore(null);
    setAppliedMinervini5mMinLiquidityCrore(null);
    setScannerRunNonce((current) => current + 1);
  };

  const handlePickSymbol = (symbol: string) => {
    chartNavigationSymbolsRef.current = null;
    setSelectedSymbol(symbol);
    setChartOpen(true);
  };

  const handlePickSymbolWithContext = (symbol: string, contextSymbols: string[]) => {
    const scoped = Array.from(new Set(contextSymbols.filter(Boolean)));
    chartNavigationSymbolsRef.current = scoped.length > 0 ? scoped : null;
    setSelectedSymbol(symbol);
    setChartOpen(true);
  };

  const handleToggleIndicator = (indicator: IndicatorKey) => {
    setIndicatorKeys((current) =>
      current.includes(indicator) ? current.filter((item) => item !== indicator) : [...current, indicator],
    );
  };

  const handleChartColorsChange = (nextChartColors: ChartColorSettings) => {
    setChartColors(nextChartColors);
  };

  const handleAnnotationsChange = (nextAnnotations: ChartAnnotation[]) => {
    if (!activeChartKey) {
      return;
    }

    setSavedDrawings((current) => {
      if (nextAnnotations.length === 0) {
        return Object.fromEntries(Object.entries(current).filter(([key]) => key !== activeChartKey));
      }

      return {
        ...current,
        [activeChartKey]: nextAnnotations,
      };
    });
  };

  const handleChartRefresh = () => {
    if (!selectedSymbol) {
      return;
    }

    void loadChartForSelection(selectedSymbol, timeframe, activeMarket, {
      forceNetwork: true,
      preferCached: true,
    }).catch((loadError) => {
      setChartError(loadError instanceof Error ? loadError.message : "Failed to refresh chart");
    });
  };

  const handleCreateWatchlist = async (name: string, initialSymbol?: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      return;
    }

    const symbol = initialSymbol?.trim().toUpperCase();
    const initialSymbols = symbol ? [symbol] : [];
    const nextWatchlist: LocalWatchlist = {
      id: buildLocalId(),
      name: trimmed,
      color: DEFAULT_WATCHLIST_COLORS[watchlists.length % DEFAULT_WATCHLIST_COLORS.length],
      symbols: initialSymbols,
      bifurcations: [{ id: "main", name: "Main", symbols: initialSymbols }],
      active_bifurcation_id: "main",
      updated_at: Date.now(),
    };
    const next = [...watchlists, nextWatchlist];
    setWatchlists(next);
    setActiveWatchlistId(nextWatchlist.id);
    setActivePage("watchlists");
    try {
      if (watchlistsSyncReadyRef.current[activeMarket]) {
        await saveWatchlistsState({ watchlists: next, active_watchlist_id: nextWatchlist.id }, activeMarket);
      }
    } catch {
      // ignore sync failures; localStorage still holds the change
    }
  };

  const handleImportWatchlist = (result: ImportResult) => {
    const mainId = buildLocalId();
    const nextWatchlist: LocalWatchlist = {
      id: buildLocalId(),
      name: result.name,
      color: result.color,
      symbols: result.symbols,
      bifurcations: [{ id: mainId, name: "Main", symbols: result.symbols }],
      active_bifurcation_id: mainId,
      updated_at: Date.now(),
    };
    setWatchlists((current) => [...current, nextWatchlist]);
    setActiveWatchlistId(nextWatchlist.id);
    setActivePage("watchlists");
  };

  const handleRenameWatchlist = (watchlistId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      return;
    }
    setWatchlists((current) =>
      current.map((watchlist) => (watchlist.id === watchlistId ? { ...watchlist, name: trimmed, updated_at: Date.now() } : watchlist)),
    );
  };

  const handleDeleteWatchlist = (watchlistId: string) => {
    setWatchlists((current) => {
      const remaining = current.filter((watchlist) => watchlist.id !== watchlistId);
      if (activeWatchlistId === watchlistId) {
        setActiveWatchlistId(remaining[0]?.id ?? null);
      }
      return remaining;
    });
  };

  const handleSetWatchlistColor = (watchlistId: string, color: string) => {
    const normalizedColor = normalizeWatchlistColor(color, DEFAULT_WATCHLIST_COLORS[0]);
    setWatchlists((current) =>
      current.map((watchlist) => (watchlist.id === watchlistId ? { ...watchlist, color: normalizedColor, updated_at: Date.now() } : watchlist)),
    );
  };

  const handleAddToWatchlist = async (watchlistId: string, symbol: string) => {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      return;
    }
    const next = watchlists.map((watchlist) =>
      watchlist.id !== watchlistId || watchlist.symbols.includes(normalizedSymbol)
        ? watchlist
        : {
            ...watchlist,
            symbols: [...watchlist.symbols, normalizedSymbol],
            bifurcations: watchlist.bifurcations.length > 0
              ? watchlist.bifurcations.map((bifurcation, index) => (
                index === 0 && !bifurcation.symbols.includes(normalizedSymbol)
                  ? { ...bifurcation, symbols: [...bifurcation.symbols, normalizedSymbol] }
                  : bifurcation
              ))
              : [{ id: "main", name: "Main", symbols: [normalizedSymbol] }],
            active_bifurcation_id: watchlist.active_bifurcation_id ?? watchlist.bifurcations[0]?.id ?? "main",
            updated_at: Date.now(),
          },
    );
    setWatchlists(next);
    setActiveWatchlistId(watchlistId);
    try {
      if (watchlistsSyncReadyRef.current[activeMarket]) {
        const saved = await saveWatchlistsState({ watchlists: next, active_watchlist_id: watchlistId }, activeMarket);
        const normalizedSaved = normalizeWatchlistsStatePayload(saved);
        watchlistsServerSignatureRef.current[activeMarket] = watchlistsStateSignature(normalizedSaved.watchlists, normalizedSaved.activeWatchlistId);
        // If server normalized changed the state, reflect it locally
        if (watchlistsServerSignatureRef.current[activeMarket] !== watchlistsStateSignature(next, watchlistId)) {
          setWatchlists(normalizedSaved.watchlists);
          setActiveWatchlistId(normalizedSaved.activeWatchlistId);
        }
      }
    } catch {
      // ignore sync errors
    }
  };

  const handleCopyToWatchlist = (watchlistId: string, symbol: string) => {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      return;
    }
    setWatchlists((current) =>
      current.map((watchlist) =>
        watchlist.id !== watchlistId || watchlist.symbols.includes(normalizedSymbol)
          ? watchlist
          : {
              ...watchlist,
              symbols: [...watchlist.symbols, normalizedSymbol],
              bifurcations: watchlist.bifurcations.length > 0
                ? watchlist.bifurcations.map((bifurcation, index) => (
                  index === 0 && !bifurcation.symbols.includes(normalizedSymbol)
                    ? { ...bifurcation, symbols: [...bifurcation.symbols, normalizedSymbol] }
                    : bifurcation
                ))
                : [{ id: "main", name: "Main", symbols: [normalizedSymbol] }],
              active_bifurcation_id: watchlist.active_bifurcation_id ?? watchlist.bifurcations[0]?.id ?? "main",
              updated_at: Date.now(),
            },
      ),
    );
    // intentionally does NOT switch active watchlist
  };

  const handleRemoveFromWatchlist = (watchlistId: string, symbol: string) => {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      return;
    }

    setWatchlists((current) =>
      current.map((watchlist) =>
        watchlist.id === watchlistId
          ? {
              ...watchlist,
              symbols: watchlist.symbols.filter((item) => item !== normalizedSymbol),
              bifurcations: watchlist.bifurcations.map((bifurcation) => ({
                ...bifurcation,
                symbols: bifurcation.symbols.filter((item) => item !== normalizedSymbol),
              })),
              updated_at: Date.now(),
            }
          : watchlist,
      ),
    );
  };

  const handleCreateBifurcation = (watchlistId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      return;
    }
    setWatchlists((current) => current.map((watchlist) => {
      if (watchlist.id !== watchlistId) {
        return watchlist;
      }
      const bifurcationId = buildLocalId();
      return {
        ...watchlist,
        bifurcations: [...watchlist.bifurcations, { id: bifurcationId, name: trimmed, symbols: [] }],
        active_bifurcation_id: bifurcationId,
        updated_at: Date.now(),
      };
    }));
  };

  const handleRenameBifurcation = (watchlistId: string, bifurcationId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      return;
    }
    setWatchlists((current) => current.map((watchlist) => {
      if (watchlist.id !== watchlistId) {
        return watchlist;
      }
      return {
        ...watchlist,
        bifurcations: watchlist.bifurcations.map((bifurcation) => (
          bifurcation.id === bifurcationId ? { ...bifurcation, name: trimmed } : bifurcation
        )),
        updated_at: Date.now(),
      };
    }));
  };

  const handleSetActiveBifurcation = (watchlistId: string, bifurcationId: string) => {
    setWatchlists((current) => current.map((watchlist) => {
      if (watchlist.id !== watchlistId) {
        return watchlist;
      }
      if (!watchlist.bifurcations.some((bifurcation) => bifurcation.id === bifurcationId)) {
        return watchlist;
      }
      return {
        ...watchlist,
        active_bifurcation_id: bifurcationId,
        updated_at: Date.now(),
      };
    }));
  };

  const handleMoveSymbolsBetweenBifurcations = (
    watchlistId: string,
    fromBifurcationId: string,
    toBifurcationId: string,
    symbols: string[],
  ) => {
    if (!watchlistId || !fromBifurcationId || !toBifurcationId || fromBifurcationId === toBifurcationId || symbols.length === 0) {
      return;
    }
    const normalizedSymbols = Array.from(new Set(symbols.map((item) => item.trim().toUpperCase()).filter(Boolean)));
    if (normalizedSymbols.length === 0) {
      return;
    }

    setWatchlists((current) => current.map((watchlist) => {
      if (watchlist.id !== watchlistId) {
        return watchlist;
      }
      if (!watchlist.bifurcations.some((item) => item.id === fromBifurcationId) || !watchlist.bifurcations.some((item) => item.id === toBifurcationId)) {
        return watchlist;
      }

      const nextBifurcations = watchlist.bifurcations.map((bifurcation) => {
        if (bifurcation.id === fromBifurcationId) {
          return {
            ...bifurcation,
            symbols: bifurcation.symbols.filter((item) => !normalizedSymbols.includes(item)),
          };
        }
        if (bifurcation.id === toBifurcationId) {
          return {
            ...bifurcation,
            symbols: Array.from(new Set([...bifurcation.symbols, ...normalizedSymbols])),
          };
        }
        return bifurcation;
      });

      return {
        ...watchlist,
        bifurcations: nextBifurcations,
        symbols: watchlistSymbolsFromBifurcations(nextBifurcations),
        active_bifurcation_id: toBifurcationId,
        updated_at: Date.now(),
      };
    }));
  };

  const handleReorderWatchlists = (orderedIds: string[]) => {
    setWatchlists((current) => {
      const map = new Map(current.map((wl) => [wl.id, wl]));
      const reordered = orderedIds.map((id) => map.get(id)).filter(Boolean) as typeof current;
      // Append any watchlists not in orderedIds (safety)
      const ordered = new Set(orderedIds);
      for (const wl of current) {
        if (!ordered.has(wl.id)) reordered.push(wl);
      }
      return reordered;
    });
  };

  const handleMoveWatchlistSymbols = (fromWatchlistId: string, toWatchlistId: string, symbols: string[]) => {
    if (!fromWatchlistId || !toWatchlistId || fromWatchlistId === toWatchlistId || symbols.length === 0) {
      return;
    }

    const normalizedSymbols = Array.from(
      new Set(
        symbols
          .map((symbol) => symbol.trim().toUpperCase())
          .filter((symbol) => Boolean(symbol)),
      ),
    );
    if (normalizedSymbols.length === 0) {
      return;
    }

    setWatchlists((current) => {
      if (!current.some((watchlist) => watchlist.id === fromWatchlistId) || !current.some((watchlist) => watchlist.id === toWatchlistId)) {
        return current;
      }

      return current.map((watchlist) => {
        if (watchlist.id === fromWatchlistId) {
          const nextSourceBifurcations = watchlist.bifurcations.map((bifurcation) => ({
            ...bifurcation,
            symbols: bifurcation.symbols.filter((item) => !normalizedSymbols.includes(item)),
          }));
          return {
            ...watchlist,
            symbols: watchlist.symbols.filter((item) => !normalizedSymbols.includes(item)),
            bifurcations: nextSourceBifurcations,
          };
        }

        if (watchlist.id === toWatchlistId) {
          const merged = Array.from(new Set([...watchlist.symbols, ...normalizedSymbols]));
          const nextTargetBifurcations = watchlist.bifurcations.length > 0
            ? watchlist.bifurcations.map((bifurcation, index) => (
              index === 0
                ? { ...bifurcation, symbols: Array.from(new Set([...bifurcation.symbols, ...normalizedSymbols])) }
                : bifurcation
            ))
            : [{ id: "main", name: "Main", symbols: [...normalizedSymbols] }];
          return {
            ...watchlist,
            symbols: merged,
            bifurcations: nextTargetBifurcations,
          };
        }

        return watchlist;
      }).map(w => (w.id === fromWatchlistId || w.id === toWatchlistId ? { ...w, updated_at: Date.now() } : w));
    });

    // Stay on the current (source) watchlist after moving — do NOT switch to target.
  };

  const handleExportWatchlist = (watchlistId: string) => {
    const watchlist = watchlists.find((item) => item.id === watchlistId);
    if (!watchlist || watchlist.symbols.length === 0) {
      return;
    }

    const universeLookup = new Map(universeCatalog.map((item) => [item.symbol, item] as const));
    const lines = watchlist.symbols
      .map((symbol) => {
        const match = universeLookup.get(symbol);
        const exchange =
          activeMarket === "india"
            ? match?.exchange === "BSE"
              ? "BSE"
              : "NSE"
            : (match?.exchange?.trim() || "US");
        return `${exchange}:${symbol}`;
      })
      .join("\n");
    const filename = `${watchlist.name.trim().replace(/[^a-z0-9]+/gi, "-").replace(/^-+|-+$/g, "").toLowerCase() || "watchlist"}-${activeMarket}-${indiaDateKey()}.txt`;
    downloadTextFile(filename, lines);
  };

  const findUniverseMatch = (query: string) => {
    const normalizedQuery = query.trim().toLowerCase();
    if (!normalizedQuery) {
      return null;
    }

    return (
      universeCatalog.find((item) => item.symbol.toLowerCase() === normalizedQuery) ??
      universeCatalog.find((item) => item.name.toLowerCase() === normalizedQuery) ??
      universeCatalog.find(
        (item) => item.symbol.toLowerCase().includes(normalizedQuery) || item.name.toLowerCase().includes(normalizedQuery),
      ) ??
      null
    );
  };

  const findGroupStockBySymbol = (symbol: string, payload: IndustryGroupsResponse | null) => {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol || !payload) {
      return null;
    }
    return payload.stocks.find((item) => item.symbol.toUpperCase() === normalizedSymbol) ?? null;
  };

  const ensureGroupsDataLoaded = async () => {
    if (groupsData) {
      return groupsData;
    }
    const payload = await getIndustryGroups(activeMarket);
    setGroupsData(payload);
    return payload;
  };

  const openGroupsView = async (options: { groupId?: string | null; symbol?: string | null } = {}) => {
    const payload = await ensureGroupsDataLoaded();
    const normalizedSymbol = options.symbol?.trim().toUpperCase() ?? null;
    const resolvedGroupStock = normalizedSymbol ? findGroupStockBySymbol(normalizedSymbol, payload) : null;
    const resolvedGroupId = options.groupId ?? resolvedGroupStock?.final_group_id ?? null;
    const resolvedSymbol = normalizedSymbol ?? (resolvedGroupId ? payload.groups.find((group) => group.group_id === resolvedGroupId)?.symbols[0] ?? null : null);
    const resolvedGroupSymbols = resolvedGroupId
      ? Array.from(new Set((payload.groups.find((group) => group.group_id === resolvedGroupId)?.symbols ?? []).filter(Boolean)))
      : [];

    setActivePage("groups");
    setChartOpen(false);
    chartNavigationSymbolsRef.current = resolvedGroupSymbols.length > 0 ? resolvedGroupSymbols : null;
    if (resolvedSymbol) {
      setSelectedSymbol(resolvedSymbol);
    }
    setGroupsFocusRequest({
      groupId: resolvedGroupId,
      symbol: resolvedSymbol,
      nonce: Date.now(),
    });
  };

  const handleOpenChartGroupModal = async (groupId: string) => {
    try {
      const payload = await ensureGroupsDataLoaded();
      const context = resolveChartGroupContext(selectedSymbol, payload, groupId);
      if (context) {
        setChartGroupModalContext(context);
      }
    } catch {
      // Keep the chart usable if the group lookup fails transiently.
    }
  };

  const handleSelectChartGroupSymbol = (symbol: string, context: ChartGroupContext) => {
    handlePickSymbolWithContext(symbol, context.symbols);
    setChartGroupModalContext(null);
  };

  const handleOpenChartGroupPage = async (context: ChartGroupContext) => {
    setChartGroupModalContext(null);
    await openGroupsView({ groupId: context.groupId, symbol: selectedSymbol ?? context.members[0]?.symbol ?? null });
  };

  const handleChartSearchSubmit = (query: string) => {
    const match = findUniverseMatch(query);
    if (!match) {
      return;
    }
    handlePickSymbol(match.symbol);
  };

  const handleGroupSearchSubmit = async (overrideQuery?: string) => {
    const query = (overrideQuery ?? navSearchQuery).trim();
    if (!query) {
      return;
    }

    const normalizedQuery = query.toLowerCase();
    const symbolQuery = normalizedQuery.startsWith("group:") ? query.slice(6).trim() : query;
    const match = findUniverseMatch(symbolQuery);
    if (!match) {
      return;
    }

    setNavSearchQuery("");
    await openGroupsView({ symbol: match.symbol });
  };

  const handleSearchSubmit = (overrideQuery?: string) => {
    const query = (overrideQuery ?? navSearchQuery).trim();
    if (!query) {
      return;
    }

    if (query.toLowerCase().startsWith("group:")) {
      void handleGroupSearchSubmit(query);
      return;
    }

    const match = findUniverseMatch(query);
    if (!match) {
      return;
    }

    setNavSearchQuery("");
    handlePickSymbol(match.symbol);
  };

  const handleRefresh = async (source: RefreshSource = "manual") => {
    if (refreshingRef.current) {
      return;
    }

    const refreshSchedule = getAutoRefreshSchedule(new Date(), activeMarket);
    const shouldRefreshFundamentals = source === "auto" && refreshSchedule.refreshFundamentals;
    const shouldRefreshSectorTab = activePage === "home" || activePage === "sectors" || sectorTabData === null;
    const shouldRefreshGroups = activePage === "groups";

    refreshingRef.current = true;
    setRefreshing(true);
    setChartCacheState(null);
    if (shouldRefreshFundamentals) {
      setFundamentalsError(null);
    }
    setChartError(null);
    refreshTickerRibbon();
    try {
      const refreshPayload = await refreshMarketData(activeMarket).catch(() => null);
      const refreshMode = refreshPayload ? (refreshPayload as RefreshResponse).refresh_mode : null;

      const [dashboardResult, sectorResult, groupsResult] = await Promise.allSettled([
        getDashboard(activeMarket),
        shouldRefreshSectorTab ? getSectorTab(sectorSortBy, sectorSortOrder, activeMarket) : Promise.resolve(sectorTabData),
        shouldRefreshGroups ? getIndustryGroups(activeMarket) : Promise.resolve(groupsData),
      ]);

      const dashboardPayload = dashboardResult.status === "fulfilled" ? dashboardResult.value : null;
      const sectorPayload = sectorResult.status === "fulfilled" ? sectorResult.value : null;
      const groupsPayload = groupsResult.status === "fulfilled" ? groupsResult.value : null;

      if (!dashboardPayload && !sectorPayload && !groupsPayload) {
        throw new Error(
          settledError(dashboardResult) ?? settledError(sectorResult) ?? settledError(groupsResult) ?? "Failed to refresh market data",
        );
      }

      if (dashboardPayload) {
        setDashboard(dashboardPayload);
      }
      if (sectorPayload) {
        setSectorTabData(sectorPayload);
        setUniverseCatalog(buildUniverseCatalogFromSectorTab(sectorPayload));
      }
      if (groupsPayload) {
        setGroupsData(groupsPayload);
      } else if (activePage !== "groups" && groupsData) {
        void getIndustryGroups(activeMarket)
          .then((payload) => {
            setGroupsData(payload);
          })
          .catch(() => {});
      }

      let nextSelectedSymbol = selectedSymbol;
      if (activePage === "screener" && activeScanner === "improving-rs") {
        setImprovingRsLoading(true);
        const improvingPayload = await getImprovingRs(improvingRsWindow, activeMarket);
        nextSelectedSymbol =
          selectedSymbol && improvingPayload.items.some((item) => item.symbol === selectedSymbol)
            ? selectedSymbol
            : improvingPayload.items[0]?.symbol ?? dashboardPayload?.top_gainers[0]?.symbol ?? null;
        setImprovingRsData(improvingPayload);
        setSelectedSymbol(nextSelectedSymbol);
      } else if (activePage === "screener") {
        scanSectorSummaryRequestIdRef.current += 1;
        const scanPayload = await requestActiveScannerResults(false);
        if (!scanPayload) {
          throw new Error("Scanner settings are not ready yet");
        }
        nextSelectedSymbol =
          selectedSymbol && scanPayload.items.some((item) => item.symbol === selectedSymbol)
            ? selectedSymbol
            : scanPayload.items[0]?.symbol ?? dashboardPayload?.top_gainers[0]?.symbol ?? null;
        setScanResults(scanPayload);
        setScanSectorSummaries(scanPayload.sector_summaries ?? []);
        setScanSectorSummariesLoading(false);
        setSelectedSymbol(nextSelectedSymbol);
      } else if (activePage === "groups" && groupsPayload) {
        nextSelectedSymbol =
          selectedSymbol && groupsPayload.stocks.some((item) => item.symbol === selectedSymbol)
            ? selectedSymbol
            : firstSymbolFromIndustryGroups(groupsPayload) ?? dashboardPayload?.top_gainers[0]?.symbol ?? null;
        setSelectedSymbol(nextSelectedSymbol);
      } else if (!nextSelectedSymbol) {
        nextSelectedSymbol = dashboardPayload?.top_gainers[0]?.symbol ?? firstSymbolFromSectorTab(sectorPayload) ?? null;
        setSelectedSymbol(nextSelectedSymbol);
      }

      if (nextSelectedSymbol) {
        const shouldForceChartReload =
          nextSelectedSymbol !== selectedSymbol ||
          refreshMode === "historical-refresh" ||
          refreshMode === "rebuilding-background";
        void loadChartForSelection(nextSelectedSymbol, timeframe, activeMarket, {
          forceNetwork: shouldForceChartReload,
          preferCached: true,
        }).catch((chartLoadError) => {
          setChartError(chartLoadError instanceof Error ? chartLoadError.message : "Failed to load chart");
        });

        if (chartPanelTab === "fundamentals" && shouldRefreshFundamentals) {
          void getFundamentals(nextSelectedSymbol, activeMarket)
            .then((fundamentalsPayload) => {
              setFundamentalsBySymbol((current) => ({
                ...current,
                [nextSelectedSymbol]: fundamentalsPayload,
              }));
              setFundamentalsError(null);
            })
            .catch((fundamentalsLoadError) => {
              setFundamentalsError(
                fundamentalsLoadError instanceof Error ? fundamentalsLoadError.message : "Failed to load fundamentals",
              );
            });
        }
      } else {
        chartRequestIdRef.current += 1;
        setChart(null);
        setChartError(null);
        setChartLoading(false);
        setChartCacheState(null);
      }

      if (savedScanners.length > 0) {
        void syncSavedScanners(savedScanners);
      }

      const loadWarnings = [settledError(dashboardResult), settledError(sectorResult), settledError(groupsResult)].filter(Boolean);

      if (refreshPayload) {
        const rp = refreshPayload as RefreshResponse;
        const isBackgroundRebuild = rp.refresh_mode === "rebuilding-background";
        const refreshSucceeded = rp.refresh_mode === "historical-refresh" || rp.refresh_mode === "cached-current" || isBackgroundRebuild;
        const clearableAutoRefresh = refreshSucceeded;

        if (isBackgroundRebuild) {
          // EOD rebuild running in background — show info message and auto-reload in ~2 min
          setError(rp.message ?? "Rebuilding EOD snapshot in background — data will update shortly.");
          setTimeout(() => void handleRefreshRef.current("auto"), 120_000);
        } else if (refreshSucceeded && loadWarnings.length === 0) {
          setError(null);
        } else if (source === "manual") {
          const snapshotLabel = rp.snapshot_updated_at
            ? new Date(rp.snapshot_updated_at).toLocaleString()
            : "the latest cached snapshot";
          setError(rp.message ?? loadWarnings[0] ?? `Showing cached market data from ${snapshotLabel}.`);
        } else if (source === "auto") {
          if (dashboardPayload || sectorPayload) {
            if (sectorPayload) {
              setUniverseCatalog(buildUniverseCatalogFromSectorTab(sectorPayload));
            }
            if (loadWarnings.length === 0 && clearableAutoRefresh) {
              setError(null);
            }
          }
        }
      } else if (source === "manual" && loadWarnings.length > 0) {
        setError(loadWarnings[0]);
      } else if (source === "manual" && !refreshPayload) {
        setError("Refresh service was unavailable, but the latest reachable market data has been loaded.");
      } else if (source === "auto") {
        // During auto-refresh, silently clear any previous errors if we got data
        if (dashboardPayload || sectorPayload) {
          if (sectorPayload) {
            setUniverseCatalog(buildUniverseCatalogFromSectorTab(sectorPayload));
          }
          setError(null);
        }
      }
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Failed to refresh market data");
    } finally {
      setSectorLoading(false);
      setImprovingRsLoading(false);
      refreshingRef.current = false;
      setRefreshing(false);
    }
  };

  const handleWatchdogFix = async () => {
    if (watchdogFixing || refreshingRef.current) {
      return;
    }

    setWatchdogFixing(true);
    setWatchdogStatus("checking");
    setWatchdogStatusMeta("Running self-heal now...");
    setError("Running watchdog self-heal — checking caches and forcing a fresh sync...");
    try {
      const result = await runWatchdogFix(activeMarket);
      await handleRefresh("manual");
      try {
        const status = await getWatchdogStatus(activeMarket);
        applyWatchdogBadge(status);
      } catch {
        setWatchdogStatus("offline");
        setWatchdogStatusMeta("Status unavailable right now.");
      }

      const notice = result.message ?? "Watchdog self-heal completed.";
      setError(notice);
      const clearAfterMs = result.refresh_mode === "rebuilding-background" ? 180_000 : 15_000;
      window.setTimeout(() => {
        setError((current) => (current === notice ? null : current));
      }, clearAfterMs);

      if (result.refresh_mode === "rebuilding-background") {
        window.setTimeout(() => {
          void handleRefreshRef.current("auto");
          void getWatchdogStatus(activeMarket)
            .then((status) => applyWatchdogBadge(status))
            .catch(() => {});
        }, 120_000);
      }
    } catch (watchdogError) {
      setWatchdogStatus("offline");
      setWatchdogStatusMeta("Watchdog self-heal failed.");
      setError(watchdogError instanceof Error ? watchdogError.message : "Watchdog self-heal failed");
    } finally {
      setWatchdogFixing(false);
    }
  };

  handleRefreshRef.current = handleRefresh;

  useEffect(() => {
    const previousMarket = previousActiveMarketRef.current;
    previousActiveMarketRef.current = activeMarket;

    if (previousMarket === activeMarket) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      if (!refreshingRef.current) {
        void handleRefreshRef.current("auto");
      }
    }, 2_500);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [activeMarket]);

  useEffect(() => {
    if (loading || savedScanners.length === 0) {
      return;
    }

    const stalePresets = savedScanners.filter((preset) => !savedScannerFreshForMarket(preset.lastUpdatedAt, activeMarket));
    if (stalePresets.length === 0) {
      return;
    }

    void syncSavedScanners(stalePresets);
  }, [activeMarket, loading, savedScanners]);

  const autoRefreshSchedule = getAutoRefreshSchedule(new Date(clockTick), activeMarket);
  const navSearchSuggestions = useMemo(
    () => buildSymbolSuggestions(universeCatalog, deferredNavSearchQuery, 80),
    [universeCatalog, deferredNavSearchQuery],
  );
  const brandEyebrow = activeMarket === "india" ? "NSE / BSE Stock Scanner" : "NYSE / Nasdaq Stock Scanner";
  const floorMetricLabel = activeMarket === "india" ? "Floor" : "US Filter";
  const freshnessWatermarks = useMemo(() => {
    const labels: Array<{ key: string; label: string; value: string }> = [];
    const toTimeLabel = (raw: string | null | undefined) => {
      if (!raw) {
        return null;
      }
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) {
        return null;
      }
      return parsed.toLocaleTimeString();
    };

    const dashboardTime = toTimeLabel(dashboard?.generated_at);
    if (dashboardTime) labels.push({ key: "dashboard", label: "Dashboard", value: dashboardTime });

    const sectorsTime = toTimeLabel(sectorTabData?.generated_at);
    if (sectorsTime) labels.push({ key: "sectors", label: "Sectors", value: sectorsTime });

    const groupsTime = toTimeLabel(groupsData?.generated_at);
    if (groupsTime) labels.push({ key: "groups", label: "Groups", value: groupsTime });

    const chartTime = toTimeLabel(activeChartSavedAt);
    if (chartTime) {
      labels.push({
        key: "chart",
        label: "Chart",
        value: chartCacheState === "cached" ? `${chartTime} (cached)` : chartTime,
      });
    }

    const fundamentalsTime = toTimeLabel(activeFundamentals?.fetched_at);
    if (fundamentalsTime) labels.push({ key: "fundamentals", label: "Fundamentals", value: fundamentalsTime });

    return labels;
  }, [activeChartSavedAt, activeFundamentals?.fetched_at, chartCacheState, dashboard?.generated_at, groupsData?.generated_at, sectorTabData?.generated_at]);

  // Backend warm-up detection: listen to status events dispatched by api.ts
  useEffect(() => {
    const handle = (e: Event) => {
      const type = (e as CustomEvent<{ type: string }>).detail.type;
      if (type === "warming") setBackendStatus("warming");
      else if (type === "ready") setBackendStatus("ok");
      else if (type === "failed") setBackendStatus("failed");
    };
    window.addEventListener("backend-status", handle);
    return () => window.removeEventListener("backend-status", handle);
  }, []);

  // Start keep-alive pings once the app mounts so the HF Space container
  // stays awake as long as the user has the tab open.
  useEffect(() => {
    startKeepAlive();
  }, []);

  const applyWatchdogBadge = (status: WatchdogStatusResponse) => {
    const ageSeconds = Math.max(0, Math.round(status.snapshot_age_seconds ?? 0));
    const ageLabel = ageSeconds < 90 ? `${ageSeconds}s old` : `${Math.round(ageSeconds / 60)}m old`;
    const freshnessLabel = status.snapshot_stale
      ? status.close_refresh_due
        ? "close sync due"
        : status.snapshot_session_date && status.expected_session_date && status.snapshot_session_date !== status.expected_session_date
          ? `${status.snapshot_session_date} snapshot loaded`
          : ageLabel
      : ageLabel;
    const siteCoverage =
      typeof status.site_systems_ready === "number" && typeof status.site_systems_total === "number"
        ? ` · ${status.site_systems_ready}/${status.site_systems_total} sections ready`
        : "";
    const needsAttention = status.snapshot_stale || (status.site_attention_count ?? 0) > 0;
    setWatchdogStatus(needsAttention ? "stale" : "healthy");
    setWatchdogStatusMeta(
      `${status.is_market_open ? "Live" : "Idle"} · ${freshnessLabel}${siteCoverage} · checks every ${status.watchdog_interval_seconds}s`,
    );
  };

  useEffect(() => {
    let active = true;

    async function pollWatchdogStatus() {
      try {
        const status = await getWatchdogStatus(activeMarket);
        if (!active) {
          return;
        }
        applyWatchdogBadge(status);
        if (status.snapshot_stale && !refreshingRef.current && !watchdogFixing) {
          const now = Date.now();
          if (now - lastAutoRefreshAtRef.current >= 3 * 60 * 1000) {
            lastAutoRefreshAtRef.current = now;
            void handleRefreshRef.current("auto");
          }
        }
      } catch {
        if (!active) {
          return;
        }
        setWatchdogStatus("offline");
        setWatchdogStatusMeta("Status unavailable right now.");
      }
    }

    void pollWatchdogStatus();
    const intervalId = window.setInterval(() => {
      void pollWatchdogStatus();
    }, 60_000);

    return () => {
      active = false;
      window.clearInterval(intervalId);
    };
  }, [activeMarket]);

  void _dispatchBackendEvent; // suppress unused import
  const floorMetricValue = activeMarket === "india" ? `${dashboard?.market_cap_min_crore ?? 800} Cr+` : ">$15 · 400K+ ADV";
  const watchdogBadgeLabel = watchdogFixing
    ? "Fixing"
    : watchdogStatus === "healthy"
      ? "Healthy"
      : watchdogStatus === "stale"
        ? "Needs sync"
        : watchdogStatus === "offline"
          ? "Offline"
          : "Checking";
  const watchdogBadgeTitle = watchdogFixing ? "Running watchdog self-heal now..." : watchdogStatusMeta;

  return (
    <div className="app-shell app-shell-simple">
      {backendStatus === "warming" && (
        <div className="backend-warmup-banner">
          <span className="warmup-spinner" />
          <span>Server is starting up after a recent update — connecting automatically, please wait…</span>
        </div>
      )}
      {backendStatus === "failed" && (
        <div className="backend-warmup-banner backend-warmup-banner--failed">
          Could not reach the backend after 6 minutes.{" "}
          <button className="warmup-retry-btn" onClick={() => { setBackendStatus("ok"); window.location.reload(); }}>Reload page</button>
        </div>
      )}
      <div className="ticker-ribbon">
        <div className="ticker-ribbon-track">
          {[...tickerTapeItems, ...tickerTapeItems].map((item, index) => (
            <div key={`${item.key}-${index}`} className="ticker-ribbon-item">
              <span>{item.label}</span>
              <strong>{item.price.toFixed(2)}</strong>
              <small className={item.change >= 0 ? "positive-text" : "negative-text"}>
                {item.change >= 0 ? "+" : ""}
                {item.change.toFixed(2)}%
              </small>
            </div>
          ))}
        </div>
      </div>
      <header className="top-nav">
        <div className="brand-stack">
          <div className="home-market-toggle nav-market-toggle" role="tablist" aria-label="Select market">
            <button
              type="button"
              className={activeMarket === "india" ? "home-market-toggle-btn active" : "home-market-toggle-btn"}
              onClick={() => handleMarketChange("india")}
              aria-pressed={activeMarket === "india"}
            >
              India
            </button>
            <button
              type="button"
              className={activeMarket === "us" ? "home-market-toggle-btn active" : "home-market-toggle-btn"}
              onClick={() => handleMarketChange("us")}
              aria-pressed={activeMarket === "us"}
            >
              US
            </button>
          </div>

          <div className="brand-cluster">
            <div className="brand-mark">MM</div>
            <div>
              <p className="eyebrow">{brandEyebrow}</p>
              <h1>Mr. Malik Scanner</h1>
            </div>
          </div>
        </div>

        <div className="nav-controls">
          <button
            type="button"
            className={activePage === "home" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("home")}
            onMouseEnter={() => prefetchPageModules("home")}
            onFocus={() => prefetchPageModules("home")}
          >
            Home
          </button>


          <button
            type="button"
            className={activePage === "screener" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("screener")}
            onMouseEnter={() => prefetchPageModules("screener")}
            onFocus={() => prefetchPageModules("screener")}
          >
            Screener
          </button>

          <button
            type="button"
            className={activePage === "ai-screener" ? "nav-button primary ai-sparkle" : "nav-button ghost"}
            onClick={() => setActivePage("ai-screener")}
            onMouseEnter={() => prefetchPageModules("ai-screener")}
            onFocus={() => prefetchPageModules("ai-screener")}
          >
            AI Screener
          </button>

          <button
            type="button"
            className={activePage === "sectors" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("sectors")}
            onMouseEnter={() => prefetchPageModules("sectors")}
            onFocus={() => prefetchPageModules("sectors")}
          >
            Sectors
          </button>

          <button
            type="button"
            className={activePage === "groups" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => {
              chartNavigationSymbolsRef.current = null;
              setActivePage("groups");
            }}
            onMouseEnter={() => prefetchPageModules("groups")}
            onFocus={() => prefetchPageModules("groups")}
          >
            Groups
          </button>

          <button
            type="button"
            className={activePage === "watchlists" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("watchlists")}
            onMouseEnter={() => prefetchPageModules("watchlists")}
            onFocus={() => prefetchPageModules("watchlists")}
          >
            Watchlists
          </button>

          <button
            type="button"
            className={activePage === "market-health" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("market-health")}
            onMouseEnter={() => prefetchPageModules("market-health")}
            onFocus={() => prefetchPageModules("market-health")}
          >
            Breadth
          </button>

          <button
            type="button"
            className={activePage === "money-flow" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("money-flow")}
            onMouseEnter={() => prefetchPageModules("money-flow")}
            onFocus={() => prefetchPageModules("money-flow")}
          >
            Money Flow
          </button>

          <button
            type="button"
            className={activePage === "newsdesk" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("newsdesk")}
          >
            Newsdesk
          </button>

          <button
            type="button"
            className={activePage === "journal" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("journal")}
          >
            📒 Journal
          </button>

          <form
            className="nav-search"
            onSubmit={(event) => {
              event.preventDefault();
              handleSearchSubmit();
            }}
          >
            <div className="nav-search-row">
              <div className="nav-search-field">
                <input
                  list="nav-stock-options"
                  value={navSearchQuery}
                  onChange={(event) => setNavSearchQuery(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && event.shiftKey) {
                      event.preventDefault();
                      void handleGroupSearchSubmit();
                    }
                  }}
                  onInput={(event) => {
                    const inputEvent = event.nativeEvent as InputEvent;
                    if (inputEvent.inputType === "insertReplacementText" || !inputEvent.inputType) {
                      const value = (event.target as HTMLInputElement).value;
                      setTimeout(() => handleSearchSubmit(value), 0);
                    }
                  }}
                  placeholder="Search symbol or company"
                />
                <span className="nav-search-hint">Enter opens chart. Shift+Enter jumps to the stock&apos;s group.</span>
              </div>
              <button type="submit" className="nav-button primary">
                Search
              </button>
            </div>
            <datalist id="nav-stock-options">
              {navSearchSuggestions.map((item) => (
                <option key={`nav-option-${item.symbol}`} value={item.symbol}>
                  {item.name}
                </option>
              ))}
            </datalist>
          </form>

          <div className="nav-mode-toggle">
            <span>Mode</span>
            <div className="mode-switch">
              <button
                type="button"
                className={theme === "dark" ? "tool-pill active" : "tool-pill"}
                onClick={() => setTheme("dark")}
              >
                Dark
              </button>
              <button
                type="button"
                className={theme === "light" ? "tool-pill active" : "tool-pill"}
                onClick={() => setTheme("light")}
              >
                Light
              </button>
            </div>
          </div>

          <div className="nav-mode-toggle">
            <span>Data Mode</span>
            <div className="mode-switch">
              <span className="tool-pill active" title={autoRefreshSchedule.detail}>
                {autoRefreshSchedule.label}
              </span>
            </div>
          </div>

          <div className="nav-mode-toggle">
            <span>Watchdog</span>
            <div className="mode-switch">
              <span
                className={`tool-pill active watchdog-status-pill watchdog-status-pill--${watchdogStatus}`}
                title={watchdogBadgeTitle}
              >
                <span className="watchdog-status-dot" />
                {watchdogBadgeLabel}
              </span>
            </div>
          </div>

          <button type="button" className="nav-button" onClick={() => void handleRefresh("manual")} disabled={refreshing || watchdogFixing}>
            {refreshing ? "Refreshing..." : "Refresh Close Snapshot"}
          </button>
          <button
            type="button"
            className="nav-button"
            onClick={() => void handleWatchdogFix()}
            disabled={refreshing || watchdogFixing}
            title="Runs the broader full-site self-heal watchdog: checks dashboard, sectors, screeners, groups, fundamentals, charts, and forces a fresh sync."
          >
            {watchdogFixing ? "Running Watchdog..." : "Watchdog Fix"}
          </button>
        </div>
      </header>

      <main className="workspace">
        {error ? <div className="error-banner">{error}</div> : null}

        {loading ? (
          <div className="loading-skeleton">
            <div className="skeleton-strip">
              <div className="skeleton-block skeleton-block-sm" />
              <div className="skeleton-block skeleton-block-sm" />
              <div className="skeleton-block skeleton-block-sm" />
              <div className="skeleton-block skeleton-block-sm" />
            </div>
            <div className="skeleton-strip">
              <div className="skeleton-block skeleton-block-lg" />
              <div className="skeleton-block skeleton-block-lg" />
            </div>
          </div>
        ) : null}

        {activePage === "home" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <HomePanel
              activeMarket={activeMarket}
              dashboard={dashboard}
              sectors={sectorTabData}
              groups={groupsData}
              onPickSymbol={handlePickSymbol}
              onOpenSectors={() => setActivePage("sectors")}
              onOpenGroups={(options) => {
                void openGroupsView(options);
              }}
              onOpenWatchdogTasks={() => setWatchdogTasksOpen(true)}
            />
          </Suspense>
        ) : null}
        {!loading && activePage === "market-health" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <MarketHealthPanel market={activeMarket} />
          </Suspense>
        ) : null}
        {!loading && activePage === "money-flow" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <MoneyFlowPanel
              key={activeMarket}
              market={activeMarket}
              onPickSymbol={handlePickSymbol}
              onPickSymbolWithContext={handlePickSymbolWithContext}
            />
          </Suspense>
        ) : null}
        {!loading && activePage === "ai-screener" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <AiScreenerPanel
              market={activeMarket}
              onPickSymbol={handlePickSymbol}
              onRequestAddToWatchlist={setWatchlistPickerSymbol}
              onVisibleSymbolsChange={setAiVisibleSymbols}
              selectedSymbol={selectedSymbol}
            />
          </Suspense>
        ) : null}
        {!loading && activePage === "newsdesk" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <NewsTab
              market={activeMarket}
              onPickSymbol={handlePickSymbol}
            />
          </Suspense>
        ) : null}
        {!loading && activePage === "journal" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <TradeJournalPanel
              market={activeMarket}
              addRequest={journalAddRequest}
              onAddRequestHandled={() => setJournalAddRequest(null)}
              onOpenSymbolChart={handlePickSymbol}
            />
          </Suspense>
        ) : null}
        {!loading && activePage !== "home" && activePage !== "market-health" && activePage !== "money-flow" && activePage !== "ai-screener" && activePage !== "newsdesk" && activePage !== "journal" ? (
          <Suspense fallback={<DeferredPanelPlaceholder compact />}>
            <>
            <section className="page-metrics-strip">
              {activePage === "screener" ? (
                <>
                  <div className="metric-card">
                    <span>{activeScanner === "improving-rs" ? "RS Leaders" : "Filtered Stocks"}</span>
                    <strong>{activeViewMetric}</strong>
                  </div>
                  <div className="metric-card">
                    <span>Universe</span>
                    <strong>{dashboard?.universe_count ?? 0}</strong>
                  </div>
                </>
              ) : (
                <>
                  <div className="metric-card">
                    <span>Universe</span>
                    <strong>{dashboard?.universe_count ?? 0}</strong>
                  </div>
                  <div className="metric-card">
                    <span>{activeViewLabel}</span>
                    <strong>{activeViewMetric}</strong>
                  </div>
                </>
              )}
              <div className="metric-card">
                <span>{floorMetricLabel}</span>
                <strong>{floorMetricValue}</strong>
              </div>
              <div className="metric-card">
                <span>Updated</span>
                <strong>{dashboard ? new Date(dashboard.generated_at).toLocaleTimeString() : "--"}</strong>
              </div>
            </section>

            {freshnessWatermarks.length > 0 ? (
              <section className="freshness-watermark-strip" aria-label="Section freshness timestamps">
                {freshnessWatermarks.map((item) => (
                  <span key={item.key} className="freshness-watermark-chip">
                    <strong>{item.label}</strong>
                    <small>{item.value}</small>
                  </span>
                ))}
              </section>
            ) : null}

            <section
              className={
                activePage === "screener"
                  ? "screener-page-grid"
                  : activePage === "sectors" || activePage === "groups" || activePage === "watchlists"
                    ? "workspace-grid workspace-grid-sector"
                    : "workspace-grid"
              }
            >
              {activePage === "screener" ? (
                <>
                  <ScreenerSidebar
                    market={activeMarket}
                    activeMode={activeScanner}
                    onModeChange={handleScannerModeChange}
                    counts={{
                      "custom-scan": activeScanner === "custom-scan" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "custom-scan" ? scanResults.total_hits : 0,
                      "ipo": activeScanner === "ipo" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "ipo" ? scanResults.total_hits : 0,
                      "gap-up-openers": activeScanner === "gap-up-openers" ? scanResults?.total_hits ?? 0 : 0,
                      "ema-expansion": activeScanner === "ema-expansion" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "ema-expansion" ? scanResults.total_hits : 0,
                      "contraction": activeScanner === "contraction" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "contraction" ? scanResults.total_hits : 0,
                      "near-pivot": activeScanner === "near-pivot" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "near-pivot" ? scanResults.total_hits : 0,
                      "pull-backs": activeScanner === "pull-backs" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "pull-backs" ? scanResults.total_hits : 0,
                      "returns": activeScanner === "returns" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "returns" ? scanResults.total_hits : 0,
                      "consolidating": activeScanner === "consolidating" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "consolidating" ? scanResults.total_hits : 0,
                      "minervini-1m": activeScanner === "minervini-1m" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "minervini-1m" ? scanResults.total_hits : 0,
                      "minervini-5m": activeScanner === "minervini-5m" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "minervini-5m" ? scanResults.total_hits : 0,
                      "improving-rs": improvingRsData?.total_hits ?? 0,
                    }}
                    savedScanners={savedScanners.map((preset) => ({
                      id: preset.id,
                      name: preset.name,
                      mode: preset.mode,
                      lastMatchCount: preset.lastMatchCount,
                      lastUpdatedAt: preset.lastUpdatedAt,
                      isStale: !savedScannerFreshForMarket(preset.lastUpdatedAt, activeMarket),
                    }))}
                    activeSavedScannerId={activeSavedScannerId}
                    onLoadSavedScanner={handleLoadSavedScannerById}
                    onDeleteSavedScanner={handleDeleteSavedScanner}
                  />

                  <div className="screener-main-stack">
                    {activeScanner !== "improving-rs" ? (
                      <section className="scanner-settings-shell">
                        <div className="scanner-settings-bar">
                          <div>
                            <strong>
                              {activeScanner === "custom-scan"
                                ? "Custom Scanner"
                                : activeScanner === "ipo"
                                  ? "IPO"
                                : activeScanner === "gap-up-openers"
                                  ? "Gap Up Openers"
                                  : activeScanner === "ema-expansion"
                                    ? "Expansion"
                                  : activeScanner === "contraction"
                                    ? "Contraction"
                                  : activeScanner === "near-pivot"
                                    ? "Near Pivot"
                                  : activeScanner === "returns"
                                    ? "Returns"
                                  : activeScanner === "consolidating"
                                    ? "Consolidating"
                                  : activeScanner === "minervini-1m"
                                    ? "Minervini 1 Month"
                                  : activeScanner === "minervini-5m"
                                    ? "Minervini 5 Months"
                                    : "Pull Backs"}
                            </strong>
                            <span>
                              {activeScanner === "custom-scan"
                                ? "Define your own universe filters and RS thresholds."
                                : activeScanner === "ipo"
                                  ? "Recently listed stocks from the last 12 months, ranked by recency and strength."
                                : activeScanner === "gap-up-openers"
                                  ? "Filter stocks by opening gap percentage."
                                  : activeScanner === "ema-expansion"
                                    ? "Strong price expansion with volume surge and minimum liquidity."
                                  : activeScanner === "contraction"
                                    ? "Tight 3-day ranges above the 50D EMA with liquidity floors."
                                  : activeScanner === "near-pivot"
                                    ? "Find high-RS stocks tightening close to their pivot zone."
                                  : activeScanner === "returns"
                                    ? "Scan for stocks by return range with optional confirmation filters."
                                  : activeScanner === "consolidating"
                                    ? "Toggle multi-year-high and long-base filters independently."
                                  : activeScanner === "minervini-1m"
                                    ? "Minervini 1 Month trend-template scan with an optional liquidity filter."
                                  : activeScanner === "minervini-5m"
                                    ? "Minervini 5 Months trend-template scan with an optional liquidity filter."
                              : "Find strong leaders pulling into the 10- or 20-day EMA on contraction."}
                            </span>
                          </div>

                          <div className="scanner-settings-actions">
                            <button type="button" className="tool-pill" onClick={() => setShowScannerSettings((current) => !current)}>
                              {showScannerSettings ? "Hide Settings" : "Show Settings"}
                            </button>
                            {isSavableScannerMode(activeScanner) ? (
                              <button type="button" className="tool-pill" onClick={() => void handleSaveCurrentScanner()} disabled={savingScanner}>
                                {savingScanner
                                  ? "Saving..."
                                  : activeSavedScannerId && savedScanners.some((item) => item.id === activeSavedScannerId && item.mode === activeScanner)
                                    ? "Update Scanner"
                                    : "Save Scanner"}
                              </button>
                            ) : null}
                          </div>
                        </div>

                        {showScannerSettings
                          ? activeScanner === "ipo"
                            ? (
                              <div className="scanner-settings-note">
                                <strong>Built-in scan</strong>
                                <span>The IPO screener uses the backend listing-date rule and does not have extra filters yet.</span>
                              </div>
                            )
                            : activeScanner === "gap-up-openers"
                            ? (
                              <GapUpScannerPanel
                                threshold={gapUpThreshold}
                                onThresholdChange={setGapUpThreshold}
                                minLiquidityCrore={gapUpMinLiquidityCrore}
                                onMinLiquidityCroreChange={setGapUpMinLiquidityCrore}
                              />
                            )
                            : activeScanner === "ema-expansion" || activeScanner === "contraction"
                              ? (
                                <div className="scanner-settings-note">
                                  <strong>Built-in scan</strong>
                                  <span>This scanner uses optimized backend logic with fixed filters.</span>
                                </div>
                              )
                            : activeScanner === "near-pivot"
                              ? (
                                <NearPivotScannerPanel
                                  filters={nearPivotFilters}
                                  onFiltersChange={setNearPivotFilters}
                                  onApply={handleApplyNearPivotScan}
                                  onReset={handleResetNearPivotScan}
                                />
                              )
                              : activeScanner === "pull-backs"
                                ? (
                                  <PullBackScannerPanel
                                    filters={pullBackFilters}
                                    onFiltersChange={setPullBackFilters}
                                    onApply={handleApplyPullBackScan}
                                    onReset={handleResetPullBackScan}
                                  />
                                )
                              : activeScanner === "returns"
                                ? (
                                  <ReturnsScannerPanel
                                    filters={returnsFilters}
                                    onFiltersChange={setReturnsFilters}
                                    onApply={handleApplyReturnsScan}
                                    onReset={handleResetReturnsScan}
                                  />
                                )
                              : activeScanner === "consolidating"
                                ? (
                                  <ConsolidatingScannerPanel
                                    filters={consolidatingFilters}
                                    onFiltersChange={setConsolidatingFilters}
                                    onApply={handleApplyConsolidatingScan}
                                    onReset={handleResetConsolidatingScan}
                                  />
                                )
                              : activeScanner === "minervini-1m"
                                ? (
                                  <MinerviniScannerPanel
                                    title="Minervini 1 Month"
                                    subtitle="Price above the 50/150/200 SMA, rising 200 SMA versus 1 month ago, within 25% of the 52-week high, and at least 25% above the 52-week low."
                                    minLiquidityCrore={minervini1mMinLiquidityCrore}
                                    onMinLiquidityCroreChange={setMinervini1mMinLiquidityCrore}
                                    onApply={handleApplyMinervini1mScan}
                                    onReset={handleResetMinervini1mScan}
                                  />
                                )
                              : activeScanner === "minervini-5m"
                                ? (
                                  <MinerviniScannerPanel
                                    title="Minervini 5 Months"
                                    subtitle="Price above the 50/150/200 SMA, rising 200 SMA over 1 and 5 months, within 25% of the 52-week high, and at least 30% above the 52-week low."
                                    minLiquidityCrore={minervini5mMinLiquidityCrore}
                                    onMinLiquidityCroreChange={setMinervini5mMinLiquidityCrore}
                                    onApply={handleApplyMinervini5mScan}
                                    onReset={handleResetMinervini5mScan}
                                  />
                                )
                              : (
                                <CustomScannerPanel
                                  filters={customFilters}
                                  onFiltersChange={setCustomFilters}
                                  onApply={handleApplyCustomScan}
                                  onReset={handleResetCustomScan}
                                  patternOptions={patternOptions}
                                />
                              )
                          : null}
                      </section>
                    ) : null}

                    {activeScanner === "improving-rs" ? (
                      <ImprovingRsPanel
                        market={activeMarket}
                        data={improvingRsData}
                        loading={improvingRsLoading}
                        window={improvingRsWindow}
                        onWindowChange={setImprovingRsWindow}
                        onPickSymbol={handlePickSymbol}
                        onRequestAddToWatchlist={setWatchlistPickerSymbol}
                        selectedSymbol={selectedSymbol}
                      />
                    ) : (
                      <ScanTable
                        market={activeMarket}
                        loading={scanLoading}
                        sectorSummaryLoading={scanSectorSummariesLoading}
                        scan={displayScan}
                        items={visibleScanItems}
                        sectorSummaries={scanSectorSummaries}
                        onPickSymbol={handlePickSymbol}
                        onRequestAddToWatchlist={setWatchlistPickerSymbol}
                        onRequestAddToJournal={(symbol, price) => { setJournalAddRequest({ symbol, suggestedPrice: price }); setActivePage("journal"); }}
                        selectedSymbol={selectedSymbol}
                        sortMode={resultSortMode}
                        onSortModeChange={setResultSortMode}
                        arrangementMode={scanArrangementMode}
                        onArrangementModeChange={setScanArrangementMode}
                        sectorSortMode={sectorGroupSortMode}
                        onSectorSortModeChange={setSectorGroupSortMode}
                        onExport={handleExportScanResults}
                      />
                    )}
                  </div>

                  <ChartPanel
                    key={activeChartKey ?? "empty-chart"}
                    market={activeMarket}
                    symbol={selectedSymbol}
                    bars={displayedChart?.bars ?? []}
                    rsLine={displayedChart?.rs_line ?? []}
                    rsLineMarkers={displayedChart?.rs_line_markers ?? []}
                    summary={displayedChart?.summary ?? null}
                    panelTab={chartPanelTab}
                    onPanelTabChange={setChartPanelTab}
                    chartError={chartError}
                    chartLoading={chartLoading}
                    chartCacheState={chartCacheState}
                    showRvol={showRvol}
                    onShowRvolChange={setShowRvol}
                    rvolPos={rvolPos}
                    onRvolPosChange={setRvolPos}
                    rvolAccentColor={rvolAccentColor}
                    onRvolAccentColorChange={setRvolAccentColor}
                    rvolScale={rvolScale}
                    onRvolScaleChange={setRvolScale}
                    showEarningsMarkers={showEarningsMarkers}
                    onShowEarningsMarkersChange={setShowEarningsMarkers}
                    earningsMarkerColor={earningsMarkerColor}
                    onEarningsMarkerColorChange={setEarningsMarkerColor}
                    fundamentals={activeFundamentals}
                    fundamentalsLoading={fundamentalsLoading}
                    fundamentalsError={fundamentalsError}
                    groupSummary={activeChartGroupSummary}
                    timeframe={timeframe}
                    onTimeframeChange={setTimeframe}
                    chartStyle={chartStyle}
                    onChartStyleChange={setChartStyle}
                    chartPalette={chartPalette}
                    onChartPaletteChange={setChartPalette}
                    showBenchmarkOverlay={showBenchmarkOverlay}
                    onShowBenchmarkOverlayChange={setShowBenchmarkOverlay}
                    indicatorKeys={indicatorKeys}
                    onToggleIndicator={handleToggleIndicator}
                    chartColors={chartColors}
                    onChartColorsChange={handleChartColorsChange}
                    drawingColor={chartDrawingColor}
                    onDrawingColorChange={setChartDrawingColor}
                    annotations={activeAnnotations}
                    onAnnotationsChange={handleAnnotationsChange}
                    onAddToWatchlist={setWatchlistPickerSymbol}
                    searchOptions={universeCatalog}
                    onSearchSymbol={handleChartSearchSubmit}
                    onOpenGroup={handleOpenChartGroupModal}
                    onRefreshChart={handleChartRefresh}
                    expanded
                  />
                </>
              ) : activePage === "sectors" ? (
                <SectorExplorerPanel
                  market={activeMarket}
                  data={sectorTabData}
                  loading={sectorLoading}
                  sortBy={sectorSortBy}
                  sortOrder={sectorSortOrder}
                  onSortByChange={setSectorSortBy}
                  onSortOrderChange={setSectorSortOrder}
                  onPickSymbol={handlePickSymbol}
                  onPickSymbolWithContext={handlePickSymbolWithContext}
                  onRequestAddToWatchlist={setWatchlistPickerSymbol}
                  onVisibleSymbolsChange={setSectorVisibleSymbols}
                  selectedSymbol={selectedSymbol}
                />
              ) : activePage === "groups" ? (
                <GroupsPanel
                  market={activeMarket}
                  data={groupsData}
                  loading={groupsLoading}
                  selectedSymbol={selectedSymbol}
                  focusRequest={groupsFocusRequest}
                  onPickSymbolWithContext={handlePickSymbolWithContext}
                  onRequestAddToWatchlist={setWatchlistPickerSymbol}
                  onVisibleSymbolsChange={setGroupsVisibleSymbols}
                />
              ) : activePage === "watchlists" ? (
                <WatchlistsPanel
                  market={activeMarket}
                  watchlists={watchlists}
                  activeWatchlistId={activeWatchlistId}
                  onSelectWatchlist={setActiveWatchlistId}
                  onCreateWatchlist={handleCreateWatchlist}
                  onRenameWatchlist={handleRenameWatchlist}
                  onDeleteWatchlist={handleDeleteWatchlist}
                  onExportWatchlist={handleExportWatchlist}
                  onSetWatchlistColor={handleSetWatchlistColor}
                  onRemoveFromWatchlist={handleRemoveFromWatchlist}
                  onMoveSymbols={handleMoveWatchlistSymbols}
                  onCreateBifurcation={handleCreateBifurcation}
                  onRenameBifurcation={handleRenameBifurcation}
                  onMoveSymbolsBetweenBifurcations={handleMoveSymbolsBetweenBifurcations}
                  onSetActiveBifurcation={handleSetActiveBifurcation}
                  onCopyToWatchlist={handleCopyToWatchlist}
                  onImportWatchlist={handleImportWatchlist}
                  onReorderWatchlists={handleReorderWatchlists}
                  onRequestAddToWatchlist={setWatchlistPickerSymbol}
                  onRequestAddToJournal={(symbol, price) => { setJournalAddRequest({ symbol, suggestedPrice: price }); setActivePage("journal"); }}
                  onPickSymbol={handlePickSymbol}
                  universeItems={universeCatalog}
                  selectedSymbol={selectedSymbol}
                  groupsData={groupsData}
                />
              ) : (
                null
              )}

              {activePage === "sectors" || activePage === "groups" || activePage === "watchlists" ? (
                <ChartPanel
                  key={activeChartKey ?? "empty-chart"}
                  market={activeMarket}
                  symbol={selectedSymbol}
                  bars={displayedChart?.bars ?? []}
                  rsLine={displayedChart?.rs_line ?? []}
                  rsLineMarkers={displayedChart?.rs_line_markers ?? []}
                  summary={displayedChart?.summary ?? null}
                  panelTab={chartPanelTab}
                  onPanelTabChange={setChartPanelTab}
                  chartError={chartError}
                  chartLoading={chartLoading}
                  chartCacheState={chartCacheState}
                  showRvol={showRvol}
                  onShowRvolChange={setShowRvol}
                  rvolPos={rvolPos}
                  onRvolPosChange={setRvolPos}
                  rvolAccentColor={rvolAccentColor}
                  onRvolAccentColorChange={setRvolAccentColor}
                  rvolScale={rvolScale}
                  onRvolScaleChange={setRvolScale}
                  showEarningsMarkers={showEarningsMarkers}
                  onShowEarningsMarkersChange={setShowEarningsMarkers}
                  earningsMarkerColor={earningsMarkerColor}
                  onEarningsMarkerColorChange={setEarningsMarkerColor}
                  fundamentals={activeFundamentals}
                  fundamentalsLoading={fundamentalsLoading}
                  fundamentalsError={fundamentalsError}
                  groupSummary={activeChartGroupSummary}
                  timeframe={timeframe}
                  onTimeframeChange={setTimeframe}
                  chartStyle={chartStyle}
                  onChartStyleChange={setChartStyle}
                  chartPalette={chartPalette}
                  onChartPaletteChange={setChartPalette}
                  showBenchmarkOverlay={showBenchmarkOverlay}
                  onShowBenchmarkOverlayChange={setShowBenchmarkOverlay}
                  indicatorKeys={indicatorKeys}
                  onToggleIndicator={handleToggleIndicator}
                  chartColors={chartColors}
                  onChartColorsChange={handleChartColorsChange}
                  drawingColor={chartDrawingColor}
                  onDrawingColorChange={setChartDrawingColor}
                  annotations={activeAnnotations}
                  onAnnotationsChange={handleAnnotationsChange}
                  onAddToWatchlist={setWatchlistPickerSymbol}
                  searchOptions={universeCatalog}
                  onSearchSymbol={handleChartSearchSubmit}
                  onOpenGroup={handleOpenChartGroupModal}
                  onRefreshChart={handleChartRefresh}
                  expanded={activePage === "sectors" || activePage === "groups"}
                />
              ) : null}
            </section>
            </>
          </Suspense>
        ) : null}

        {watchdogTasksOpen ? (
          <Suspense fallback={null}>
            <WatchdogTasksModal market={activeMarket} onClose={() => setWatchdogTasksOpen(false)} />
          </Suspense>
        ) : null}

        {chartGroupModalContext ? (
          <Suspense fallback={null}>
            <ChartGroupModal
              market={activeMarket}
              context={chartGroupModalContext}
              selectedSymbol={selectedSymbol}
              onClose={() => setChartGroupModalContext(null)}
              onSelectSymbol={(symbol: string) => handleSelectChartGroupSymbol(symbol, chartGroupModalContext)}
              onAddToWatchlist={setWatchlistPickerSymbol}
              onOpenGroupsPage={() => void handleOpenChartGroupPage(chartGroupModalContext)}
            />
          </Suspense>
        ) : null}
      </main>

      {chartOpen ? (
        <div className="chart-modal-backdrop" onClick={() => setChartOpen(false)}>
          <div className="chart-modal" onClick={(event) => event.stopPropagation()}>
            <button type="button" className="chart-modal-close" onClick={() => setChartOpen(false)}>
              Close
            </button>
            <Suspense fallback={<DeferredPanelPlaceholder compact />}>
              <ChartPanel
                key={`modal-${activeChartKey ?? "empty-chart"}`}
                market={activeMarket}
                symbol={selectedSymbol}
                bars={displayedChart?.bars ?? []}
                rsLine={displayedChart?.rs_line ?? []}
                rsLineMarkers={displayedChart?.rs_line_markers ?? []}
                summary={displayedChart?.summary ?? null}
                panelTab={chartPanelTab}
                onPanelTabChange={setChartPanelTab}
                chartError={chartError}
                chartLoading={chartLoading}
                chartCacheState={chartCacheState}
                showRvol={showRvol}
                onShowRvolChange={setShowRvol}
                rvolPos={rvolPos}
                onRvolPosChange={setRvolPos}
                rvolAccentColor={rvolAccentColor}
                onRvolAccentColorChange={setRvolAccentColor}
                rvolScale={rvolScale}
                onRvolScaleChange={setRvolScale}
                showEarningsMarkers={showEarningsMarkers}
                onShowEarningsMarkersChange={setShowEarningsMarkers}
                earningsMarkerColor={earningsMarkerColor}
                onEarningsMarkerColorChange={setEarningsMarkerColor}
                fundamentals={activeFundamentals}
                fundamentalsLoading={fundamentalsLoading}
                fundamentalsError={fundamentalsError}
                groupSummary={activeChartGroupSummary}
                timeframe={timeframe}
                onTimeframeChange={setTimeframe}
                chartStyle={chartStyle}
                onChartStyleChange={setChartStyle}
                chartPalette={chartPalette}
                onChartPaletteChange={setChartPalette}
                showBenchmarkOverlay={showBenchmarkOverlay}
                onShowBenchmarkOverlayChange={setShowBenchmarkOverlay}
                indicatorKeys={indicatorKeys}
                onToggleIndicator={handleToggleIndicator}
                chartColors={chartColors}
                onChartColorsChange={handleChartColorsChange}
                drawingColor={chartDrawingColor}
                onDrawingColorChange={setChartDrawingColor}
                annotations={activeAnnotations}
                onAnnotationsChange={handleAnnotationsChange}
                onAddToWatchlist={setWatchlistPickerSymbol}
                searchOptions={universeCatalog}
                onSearchSymbol={handleChartSearchSubmit}
                onOpenGroup={handleOpenChartGroupModal}
                onRefreshChart={handleChartRefresh}
                expanded
              />
            </Suspense>
          </div>
        </div>
      ) : null}

      {watchlistPickerSymbol ? (
        <Suspense fallback={null}>
          <WatchlistPickerModal
            market={activeMarket}
            symbol={watchlistPickerSymbol}
            watchlists={watchlists}
            onClose={() => setWatchlistPickerSymbol(null)}
            onAddToWatchlist={handleAddToWatchlist}
            onCreateWatchlist={handleCreateWatchlist}
          />
        </Suspense>
      ) : null}

      <footer className="app-footer">
        <span>Mr. Malik Scanner</span>
        <span className="app-footer-dot">·</span>
        <span>{activeMarket === "india" ? "NSE / BSE" : "NYSE / Nasdaq"}</span>
        <span className="app-footer-dot">·</span>
        <span>© {new Date().getFullYear()}</span>
      </footer>
    </div>
  );
}
