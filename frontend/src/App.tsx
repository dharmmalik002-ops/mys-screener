import { Suspense, lazy, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { Moon, RefreshCw, Search as SearchIcon, Sun } from "lucide-react";

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
import type { ScreenerMode } from "./components/ScreenerSidebar";
import { ToastProvider, useToast } from "./components/Toast";
import {
  collectLeafModes,
  defaultTotalScannerTree,
  describeTotalScanner,
  evaluateTotalScanner,
  normalizeTotalScannerTree,
  totalScannerLeafLabel,
  type TotalScannerLeafMode,
  type TotalScannerNode,
} from "./lib/totalScanner";
import { DEFAULT_POSITIVE_EARNINGS_FILTERS, type PositiveEarningsFilters } from "./components/PositiveEarningsScannerPanel";
import type { LocalWatchlist } from "./components/WatchlistsPanel";
import { Panel } from "./components/Panel";
import { CommandPalette } from "./components/CommandPalette";
import {
  type ChartBar,
  getChart,
  getConsolidatingScan,
  getDashboard,
  getDemandZoneScan,
  getFundamentals,
  getGapUpOpeners,
  getIndustryGroups,
  getIndexQuotes,
  getMomentumBurstScan,
  getNearPivotScan,
  getPullBackScan,
  getReturnsScan,
  getImprovingRs,
  DEFAULT_MOMENTUM_BURST_REQUEST,
  getScanCounts,
  getScanResults,
  getWatchlistsState,
  pingBackendHealth,
  refreshMarketData,
  runCustomScan,
  saveWatchlistsState,
  type ChartResponse,
  type CompanyFundamentals,
  type ConsolidatingScanRequest,
  type CustomScanRequest,
  type DashboardResponse,
  type DemandZoneScanRequest,
  type ImprovingRsResponse,
  type ImprovingRsWindow,
  type IndustryGroupsResponse,
  type IndustryGroupStockItem,
  type MarketKey,
  type MomentumBurstScanRequest,
  type NearPivotScanRequest,
  type PullBackScanRequest,
  type ReturnsScanRequest,
  type RefreshResponse,
  type ScanMatch,
  type ScanSectorSummary,
  type SectorSortBy,
  type SectorTabResponse,
  type ScanResultsResponse,
  type WatchlistsStateResponse,
  normalizeChartResponse,
  normalizeDashboardResponse,
  normalizeIndustryGroupsResponse,
  normalizeSectorTabResponse,
} from "./lib/api";
import { DEFAULT_CHART_COLORS } from "./lib/chartDefaults";
import { tradeMarkersForSymbol, useJournalTrades } from "./lib/journal";
import { buildSymbolSuggestions } from "./lib/searchSuggestions";
import { applyScannerDisplayAlias, applyScannerDisplayAliases, DEFAULT_SCANNERS } from "./lib/scannerCatalog";
import { AppStatusBanners } from "./components/AppStatusBanners";

const ChartPanel = lazy(() => import("./components/ChartPanel").then((module) => ({ default: module.ChartPanel })));
const ChartGroupModal = lazy(() => import("./components/ChartGroupModal"));
const TradeReviewModal = lazy(() => import("./components/TradeReviewModal").then((module) => ({ default: module.TradeReviewModal })));
const ChartCompareLayout = lazy(() => import("./components/ChartCompareLayout").then((module) => ({ default: module.ChartCompareLayout })));
const GroupStocksWidget = lazy(() => import("./components/GroupStocksWidget").then((module) => ({ default: module.GroupStocksWidget })));
const ConsolidatingScannerPanel = lazy(() => import("./components/ConsolidatingScannerPanel").then((module) => ({ default: module.ConsolidatingScannerPanel })));
const CustomScannerPanel = lazy(() => import("./components/CustomScannerPanel").then((module) => ({ default: module.CustomScannerPanel })));
const TotalScannerPanel = lazy(() => import("./components/TotalScannerPanel").then((module) => ({ default: module.TotalScannerPanel })));
const DemandZoneScannerPanel = lazy(() => import("./components/DemandZoneScannerPanel").then((module) => ({ default: module.DemandZoneScannerPanel })));
const GapUpScannerPanel = lazy(() => import("./components/GapUpScannerPanel").then((module) => ({ default: module.GapUpScannerPanel })));
const HomePanel = lazy(() => import("./components/HomePanel").then((module) => ({ default: module.HomePanel })));
const ImprovingRsPanel = lazy(() => import("./components/ImprovingRsPanel").then((module) => ({ default: module.ImprovingRsPanel })));
const MinerviniScannerPanel = lazy(() => import("./components/MinerviniScannerPanel").then((module) => ({ default: module.MinerviniScannerPanel })));
const PositiveEarningsScannerPanel = lazy(() => import("./components/PositiveEarningsScannerPanel").then((module) => ({ default: module.PositiveEarningsScannerPanel })));
const GroupsPanel = lazy(() => import("./components/GroupsPanel").then((module) => ({ default: module.GroupsPanel })));
const NearPivotScannerPanel = lazy(() => import("./components/NearPivotScannerPanel").then((module) => ({ default: module.NearPivotScannerPanel })));
const MomentumBurstScannerPanel = lazy(() => import("./components/MomentumBurstScannerPanel").then((module) => ({ default: module.MomentumBurstScannerPanel })));
const MomentumBurstResults = lazy(() => import("./components/MomentumBurstScannerPanel").then((module) => ({ default: module.MomentumBurstResults })));
const PullBackScannerPanel = lazy(() => import("./components/PullBackScannerPanel").then((module) => ({ default: module.PullBackScannerPanel })));
const ReturnsScannerPanel = lazy(() => import("./components/ReturnsScannerPanel").then((module) => ({ default: module.ReturnsScannerPanel })));
const ScanTable = lazy(() => import("./components/ScanTable").then((module) => ({ default: module.ScanTable })));
const ScanDashboard = lazy(() => import("./components/ScanDashboard").then((module) => ({ default: module.ScanDashboard })));
const ScanFooter = lazy(() => import("./components/ScanFooter").then((module) => ({ default: module.ScanFooter })));
const ScannerHeader = lazy(() => import("./components/ScannerHeader").then((module) => ({ default: module.ScannerHeader })));
const QueryBuilder = lazy(() => import("./components/QueryBuilder").then((module) => ({ default: module.QueryBuilder })));
const ScreenerSidebar = lazy(() => import("./components/ScreenerSidebar").then((module) => ({ default: module.ScreenerSidebar })));
const LivePanel = lazy(() => import("./components/LivePanel").then((module) => ({ default: module.LivePanel })));
const MarketsPanel = lazy(() => import("./components/MarketsPanel").then((module) => ({ default: module.MarketsPanel })));
const TradeJournalPanel = lazy(() => import("./components/TradeJournalPanel").then((module) => ({ default: module.TradeJournalPanel })));
const WatchlistPickerModal = lazy(() => import("./components/WatchlistPickerModal").then((module) => ({ default: module.WatchlistPickerModal })));
const WatchlistsPanel = lazy(() => import("./components/WatchlistsPanel").then((module) => ({ default: module.WatchlistsPanel })));

const CHART_PREFERENCES_KEY = "mr-malik-chart-preferences:v2";
const CHART_DRAWINGS_KEY = "mr-malik-chart-drawings:v1";
const CHART_RESPONSE_CACHE_KEY = "mr-malik-chart-response-cache:v3";
const GROUP_WIDGET_RECT_KEY = "mr-malik-group-widget-rect:v1";
const GROUP_WIDGET_OPEN_KEY = "mr-malik-group-widget-open:v1";
const THEME_KEY = "mr-malik-theme:v1";

type GroupWidgetRect = { x: number; y: number; width: number; height: number };

function readGroupWidgetRect(): GroupWidgetRect {
  const fallback: GroupWidgetRect = { x: 24, y: 80, width: 300, height: 520 };
  if (typeof window === "undefined") return fallback;
  try {
    const raw = window.localStorage.getItem(GROUP_WIDGET_RECT_KEY);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed.x === "number" &&
      typeof parsed.y === "number" &&
      typeof parsed.width === "number" &&
      typeof parsed.height === "number"
    ) {
      return parsed;
    }
  } catch {
    // ignore corrupted storage
  }
  return fallback;
}

function readGroupWidgetOpen(): boolean {
  if (typeof window === "undefined") return true;
  try {
    const raw = window.localStorage.getItem(GROUP_WIDGET_OPEN_KEY);
    if (raw === null) return true;
    return raw === "1";
  } catch {
    return true;
  }
}
const CHART_PALETTE_KEY = "mr-malik-chart-palette:v1";
const TOTAL_SCANNER_KEY = "mr-malik-total-scanner:v1";
const WATCHLISTS_KEY = "mr-malik-watchlists:v1";
const WATCHLISTS_BACKUP_KEY = "mr-malik-watchlists:backup:v1";
const LEGACY_WATCHLISTS_KEYS = ["mr-malik-watchlists", "stock-scanner-watchlists:v1", "stock-scanner-watchlists"];
const ACTIVE_WATCHLIST_KEY = "mr-malik-active-watchlist:v1";
const SCANNER_SETTINGS_KEY = "mr-malik-scanner-settings:v1";
const SAVED_SCANNERS_KEY = "mr-malik-saved-scanners:v1";
const ACTIVE_MARKET_KEY = "mr-malik-active-market:v1";
const MARKET_VIEW_CACHE_KEY = "mr-malik-market-view-cache:v2";
// Cap at 6 hours so a stale cache never outlives the next bhavcopy update.
// Previously 24 h kept yesterday's snapshot visible for an entire trading day.
const MARKET_VIEW_CACHE_MAX_AGE_MS = 6 * 60 * 60 * 1000;

type ThemeKey = "dark" | "light";
type AppPage = "home" | "screener" | "groups" | "watchlists" | "journal" | "live" | "markets";
type ResultSortMode = "change" | "rs";
type AutoRefreshMode = "market-open" | "after-hours";
type RefreshSource = "manual" | "auto";
type SavableScannerMode = Exclude<ScreenerMode, "improving-rs" | "momentum-burst">;
type SectorGroupSortMode = "1W" | "1M" | "count-desc" | "count-asc";

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
  demandZoneFilters?: DemandZoneScanRequest;
  /** Total Scanner only: the saved AND/OR combination. */
  totalScannerTree?: TotalScannerNode;
  lastMatchCount?: number;
  lastUpdatedAt?: string | null;
  symbols?: string[];
};

/**
 * A preset as it comes back off localStorage — every field still unverified,
 * and `mode` still a bare string so retired names can be migrated before the
 * record is accepted as a SavedScannerPreset.
 */
type StoredScannerPreset = Omit<Partial<SavedScannerPreset>, "mode"> & { mode?: unknown };

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
  demandZoneFilters: DemandZoneScanRequest;
  appliedDemandZoneFilters: DemandZoneScanRequest;
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
};

const DEFAULT_WATCHLIST_COLORS = ["#4f8cff", "#00a389", "#ff9f1c", "#ef476f", "#7c5cff", "#06b6d4", "#84cc16", "#f97316"];
const MAX_PERSISTED_CHART_RESPONSES = 8;

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

function shouldPersistChartResponse(timeframe: ChartTimeframe) {
  return timeframe === "1D" || timeframe === "1W";
}

/** ISO date of the newest bar in a cached chart payload, or null. */
function lastBarDate(payload: ChartResponse | null | undefined): string | null {
  const bars = payload?.bars;
  if (!bars?.length) return null;
  const time = bars[bars.length - 1]?.time;
  if (!Number.isFinite(time)) return null;
  return new Date(time * 1000).toISOString().slice(0, 10);
}

function isChartResponseCacheCompatible(
  payload: ChartResponse | null | undefined,
  savedAt?: string,
  latestSession?: string | null,
) {
  if (!payload) {
    return false;
  }
  // A cached chart whose newest bar predates the session the rest of the app is
  // showing is stale, however recently it was written. Age alone did not catch
  // this: the in-memory cache was never given a `savedAt` at all, so it never
  // expired within a session, and a chart fetched while the backend was still
  // warming kept its short history until the tab was closed — which is what
  // "no candles after 10 August" looked like.
  if (latestSession) {
    const newest = lastBarDate(payload);
    if (newest && newest < latestSession) {
      return false;
    }
  }
  // Expire persisted chart cache entries older than 2 hours
  if (savedAt) {
    const ageMs = Date.now() - new Date(savedAt).getTime();
    if (!Number.isFinite(ageMs) || ageMs > 2 * 60 * 60 * 1000) {
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
  const dashboard = raw?.dashboard ? normalizeDashboardResponse(raw.dashboard) : null;
  const sectorTabData = raw?.sectorTabData ? normalizeSectorTabResponse(raw.sectorTabData) : null;
  const groupsData = raw?.groupsData ? normalizeIndustryGroupsResponse(raw.groupsData) : null;
  const derivedUniverseCatalog = sectorTabData ? buildUniverseCatalogFromSectorTab(sectorTabData) : [];
  return {
    dashboard,
    sectorTabData,
    groupsData,
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
      // Also discard caches saved on a previous IST calendar day: an evening
      // snapshot is younger than the 6h cap next morning but still carries
      // yesterday's prices/RS, which made the catalog/search serve stale data.
      const istDay = (value: Date) =>
        new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Kolkata" }).format(value);
      if (istDay(new Date(savedAt)) !== istDay(new Date())) {
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

// Patterns the Custom Scanner's dropdown still surfaces. Anything outside
// this set is a scanner that has been promoted to its own sidebar mode
// (e.g. "positive-earnings") — if it lingers in saved customFilters, the
// scan still runs the restrictive evaluator and returns ~0 hits, so reset
// to "any" on load.
const CUSTOM_SCAN_PATTERN_ALLOWLIST = new Set<string>([
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
]);

function sanitizeCustomFiltersPattern<T extends { pattern?: string }>(filters: T): T {
  if (filters.pattern && !CUSTOM_SCAN_PATTERN_ALLOWLIST.has(filters.pattern)) {
    return { ...filters, pattern: "any" } as T;
  }
  return filters;
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
  min_adr_pct_20: null,
  max_adr_pct_20: null,
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
  // Default to hiding 2%/5% circuit-band names — they clutter results and
  // can't be traded as breakouts. Toggle off in the panel to see them.
  hide_low_band: true,
  pattern: "any",
  sort_by: "rs_rating",
  sort_order: "desc",
  limit: 1500,
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

const DEFAULT_DEMAND_ZONE_FILTERS: DemandZoneScanRequest = {
  timeframe: "weekly",
  max_distance_above_zone_pct: 3,
  min_rs_rating: 70,
  min_liquidity_crore: 5,
  min_departure_pct: 12,
  base_min_weeks: 2,
  base_max_weeks: 6,
  max_base_range_pct: 12,
  max_zone_age_weeks: 52,
  limit: 1500,
};

const DEFAULT_DAILY_DEMAND_ZONE_FILTERS: DemandZoneScanRequest = {
  timeframe: "daily",
  max_distance_above_zone_pct: 3,
  min_rs_rating: 70,
  min_liquidity_crore: 5,
  min_departure_pct: 8,
  base_min_weeks: 3,
  base_max_weeks: 12,
  max_base_range_pct: 8,
  max_zone_age_weeks: 45,
  limit: 1500,
};

function demandZoneDefaultsFor(value: unknown): DemandZoneScanRequest {
  const timeframe =
    value && typeof value === "object" && (value as Partial<DemandZoneScanRequest>).timeframe === "daily"
      ? "daily"
      : "weekly";
  return timeframe === "daily" ? DEFAULT_DAILY_DEMAND_ZONE_FILTERS : DEFAULT_DEMAND_ZONE_FILTERS;
}

function mergeDemandZoneFilters(value: unknown): DemandZoneScanRequest {
  const merged = mergeWithDefaults(demandZoneDefaultsFor(value), value);
  return {
    ...merged,
    max_distance_above_zone_pct: Math.max(0, Math.min(3, merged.max_distance_above_zone_pct)),
  };
}

const SUPPORTED_INDICATORS: IndicatorKey[] = ["ema10", "ema20", "ema50", "ema200", "vwap"];

function normalizeIndicatorKeys(value: unknown): IndicatorKey[] {
  if (!Array.isArray(value)) {
    return ["ema20", "ema50"];
  }

  const indicators = value.filter((item): item is IndicatorKey => SUPPORTED_INDICATORS.includes(item as IndicatorKey));
  return indicators.length > 0 ? indicators : ["ema20", "ema50"];
}

// Pre-2026-07 defaults. Saved preferences pin whatever the defaults were at
// save time, so without this mapping every existing install would keep the
// old neon candles forever. A stored value matching an old default is treated
// as "never customized" and upgraded to the current default.
const LEGACY_CHART_COLOR_DEFAULTS: Partial<Record<keyof ChartColorSettings, string>> = {
  candleUp: "#00d2ff",
  candleDown: "#ff3131",
  volumeUp: "#00d2ff",
  volumeDown: "#ff3131",
};

function migrateChartColor(key: keyof ChartColorSettings, value: unknown): string | null {
  if (typeof value !== "string") return null;
  const legacy = LEGACY_CHART_COLOR_DEFAULTS[key];
  if (legacy && value.trim().toLowerCase() === legacy) {
    return String(DEFAULT_CHART_COLORS[key]);
  }
  return value;
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
    candleUp: migrateChartColor("candleUp", candidate.candleUp) ?? DEFAULT_CHART_COLORS.candleUp,
    candleDown: migrateChartColor("candleDown", candidate.candleDown) ?? DEFAULT_CHART_COLORS.candleDown,
    candleExpansion: typeof candidate.candleExpansion === "string" ? candidate.candleExpansion : DEFAULT_CHART_COLORS.candleExpansion,
    volumeUp: migrateChartColor("volumeUp", candidate.volumeUp) ?? DEFAULT_CHART_COLORS.volumeUp,
    volumeDown: migrateChartColor("volumeDown", candidate.volumeDown) ?? DEFAULT_CHART_COLORS.volumeDown,
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

const INDIA_SNAPSHOT_DATE_FORMATTER = new Intl.DateTimeFormat("en-IN", {
  timeZone: "Asia/Kolkata",
  day: "2-digit",
  month: "short",
  year: "numeric",
});

const INDIA_SNAPSHOT_TIME_FORMATTER = new Intl.DateTimeFormat("en-IN", {
  timeZone: "Asia/Kolkata",
  hour: "2-digit",
  minute: "2-digit",
  hour12: true,
});

function getMarketClock(market: MarketKey, now: Date = new Date()) {
  const formatter = INDIA_MARKET_TIME_FORMATTER;
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

function formatSnapshotDate(market: MarketKey, value: string | null | undefined) {
  if (!value) {
    return "--";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "--";
  }
  return INDIA_SNAPSHOT_DATE_FORMATTER.format(parsed);
}

function formatSnapshotTime(market: MarketKey, value: string | null | undefined) {
  if (!value) {
    return "--";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "--";
  }
  return INDIA_SNAPSHOT_TIME_FORMATTER.format(parsed);
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

  const openMinutes = 9 * 60 + 15;
  const closeMinutes = 15 * 60 + 30;
  const tzLabel = "IST";
  const openLabel = "09:15";
  const closeLabel = "15:30";

  const isMarketOpen = isTradingDay && totalMinutes >= openMinutes && totalMinutes <= closeMinutes;

  if (isMarketOpen) {
    return {
      mode: "market-open" as const,
      delayMs: 24 * 60 * 60_000,
      label: "India Close Snapshot",
      detail: `Showing the last confirmed close during ${openLabel}-${closeLabel} ${tzLabel}`,
      refreshFundamentals: false,
    };
  }

  return {
    mode: "after-hours" as const,
    delayMs: 24 * 60 * 60_000,
    label: "India Close Snapshot",
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

function buildUniverseCatalogFromIndustryGroups(data: IndustryGroupsResponse | null): ScanMatch[] {
  if (!data) {
    return [];
  }
  return data.stocks.map((stock) => ({
    scan_id: "industry-groups",
    symbol: stock.symbol,
    name: stock.company_name,
    exchange: stock.exchange,
    sector: stock.sector,
    sub_sector: stock.final_group_name,
    market_cap_crore: stock.market_cap_cr,
    last_price: stock.last_price,
    change_pct: stock.change_pct,
    relative_volume: 0,
    score: stock.rs_rating ?? 0,
    rs_rating: stock.rs_rating,
    reasons: [],
  }));
}

// Parse a free-form pasted block of tickers into a deduped list of symbols.
// Handles broker-export prefixes ("NSE:RELIANCE", "BSE:RELIANCE",
// "NSE/BSE:RELIANCE"), trailing series ("RELIANCE-EQ"), and a mix of
// commas / semicolons / whitespace / newlines as separators.
function parseImportedSymbols(raw: string): string[] {
  if (!raw) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const token of raw.split(/[\s,;]+/)) {
    const trimmed = token.trim();
    if (!trimmed) continue;
    // Drop everything before the last colon ("NSE:" / "BSE:" / "NSE/BSE:")
    // and any trailing series suffix after the last hyphen ("-EQ", "-BE").
    let symbol = trimmed.includes(":") ? trimmed.slice(trimmed.lastIndexOf(":") + 1) : trimmed;
    symbol = symbol.replace(/-[A-Z]{1,3}$/i, "");
    symbol = symbol.replace(/[^A-Za-z0-9_-]/g, "").toUpperCase();
    if (!symbol || seen.has(symbol)) continue;
    seen.add(symbol);
    out.push(symbol);
  }
  return out;
}

function buildUniverseCatalogFromDashboard(data: DashboardResponse | null): ScanMatch[] {
  if (!data) return [];
  const seen = new Set<string>();
  const out: ScanMatch[] = [];
  for (const bucket of [data.top_gainers, data.top_losers, data.top_volume_spikes]) {
    for (const item of bucket) {
      if (!item?.symbol || seen.has(item.symbol)) continue;
      seen.add(item.symbol);
      out.push(item);
    }
  }
  return out;
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

function normalizeTimeframe(value: string | undefined, market: MarketKey = "india"): ChartTimeframe {
  if (market === "india" && (value === "15m" || value === "30m" || value === "1h")) {
    return "1D";
  }
  if (value === "15m" || value === "30m" || value === "1h" || value === "1W") {
    return value;
  }
  return "1D";
}

function normalizeChartPanelTab(value: string | undefined): ChartPanelTab {
  return value === "fundamentals" ? "fundamentals" : "technical";
}

function readChartPreferences(market: MarketKey): {
  chartPanelTab: ChartPanelTab;
  timeframe: ChartTimeframe;
  chartStyle: ChartStyle;
  showBenchmarkOverlay: boolean;
  indicatorKeys: IndicatorKey[];
  chartColors: ChartColorSettings;
  drawingColor: string;
} {
  if (typeof window === "undefined") {
    return {
      chartPanelTab: "technical",
      timeframe: "1D",
      chartStyle: "candles",
      showBenchmarkOverlay: false,
      indicatorKeys: ["ema20", "ema50"],
      chartColors: DEFAULT_CHART_COLORS,
      drawingColor: "#00d2ff",
    };
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
    }>;
    return {
      chartPanelTab: normalizeChartPanelTab(parsed.chartPanelTab),
      timeframe: normalizeTimeframe(parsed.timeframe, market),
      chartStyle: parsed.chartStyle === "bars" ? "bars" : parsed.chartStyle === "hlc" ? "hlc" : "candles",
      showBenchmarkOverlay: parsed.showBenchmarkOverlay === true,
      indicatorKeys: normalizeIndicatorKeys(parsed.indicatorKeys),
      chartColors: normalizeChartColors(parsed.chartColors),
      drawingColor: normalizeHexColor(parsed.drawingColor, "#00d2ff"),
    };
  } catch {
    return {
      chartPanelTab: "technical",
      timeframe: "1D",
      chartStyle: "candles",
      showBenchmarkOverlay: false,
      indicatorKeys: ["ema20", "ema50"],
      chartColors: DEFAULT_CHART_COLORS,
      drawingColor: "#00d2ff",
    };
  }
}

function macroIndexFallbackSymbol(cardName: string, market: MarketKey): string | null {
  const normalizedName = cardName.trim().toUpperCase();
  if (normalizedName === "NIFTY 50") {
    return "^NSEI";
  }
  if (normalizedName === "NIFTY SMALLCAP 250") {
    return "^CNXSC";
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
  const sparkline = Array.isArray(indexCard?.sparkline) ? indexCard.sparkline : [];
  if (!indexCard || sparkline.length < 8) {
    return null;
  }

  const bars: ChartBar[] = sparkline.map((point, index) => {
    const previousValue = index > 0 ? sparkline[index - 1].value : point.value;
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
    earnings_markers: [],
    upcoming_earnings_date: null,
    volume_markers: [],
    band_change_markers: [],
    band_history: [],
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
        ([, value]) =>
          value
          && typeof value === "object"
          && typeof value.payload?.timeframe === "string"
          && shouldPersistChartResponse(value.payload.timeframe as ChartTimeframe)
          && isChartResponseCacheCompatible(normalizeChartResponse(value.payload)),
      ).map(([key, value]) => [
        key,
        {
          ...value,
          payload: normalizeChartResponse(value.payload),
        } satisfies PersistedChartCacheEntry,
      ]),
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
  return "india";
}

function readChartPalette(market: MarketKey): ChartPaletteKey {
  if (typeof window === "undefined") {
    return "current";
  }
  const saved = readMarketScopedValue(CHART_PALETTE_KEY, market);
  return saved === "editorial" || saved === "mono" ? saved : "current";
}

function marketDisplayLabel(market: MarketKey) {
  return "India";
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

function sanitizeWatchlists(value: unknown): LocalWatchlist[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const seenIds = new Set<string>();
  // Exclusive-membership enforcement: a symbol belongs to exactly one
  // watchlist. The first watchlist in the array that claims a symbol
  // keeps it; every subsequent occurrence is stripped. This also cleans
  // up legacy state from before exclusivity was enforced.
  const claimedSymbols = new Set<string>();
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

    const symbols: string[] = [];
    if (Array.isArray(candidate.symbols)) {
      const seenInThisList = new Set<string>();
      for (const rawSymbol of candidate.symbols) {
        const symbol = normalizeStoredSymbol(rawSymbol);
        if (!symbol || seenInThisList.has(symbol) || claimedSymbols.has(symbol)) {
          continue;
        }
        seenInThisList.add(symbol);
        claimedSymbols.add(symbol);
        symbols.push(symbol);
      }
    }
    const color = normalizeWatchlistColor(candidate.color, DEFAULT_WATCHLIST_COLORS[output.length % DEFAULT_WATCHLIST_COLORS.length]);

    seenIds.add(id);
    output.push({
      id,
      name: rawName,
      color,
      symbols,
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
  payload: Pick<WatchlistsStateResponse, "watchlists" | "active_watchlist_id">,
): { watchlists: LocalWatchlist[]; activeWatchlistId: string | null } {
  const watchlists = sanitizeWatchlists(payload.watchlists);
  const activeWatchlistId =
    watchlists.some((watchlist) => watchlist.id === payload.active_watchlist_id)
      ? payload.active_watchlist_id
      : watchlists[0]?.id ?? null;
  return { watchlists, activeWatchlistId };
}

function normalizeJournalChartSymbol(symbol: string) {
  return symbol.trim().toUpperCase().replace(/\.(NS|BO|BSE|NSE)$/i, "");
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
    demandZoneFilters: DEFAULT_DEMAND_ZONE_FILTERS,
    appliedDemandZoneFilters: DEFAULT_DEMAND_ZONE_FILTERS,
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
      customFilters: sanitizeCustomFiltersPattern(mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, parsed.customFilters)),
      appliedCustomFilters: sanitizeCustomFiltersPattern(mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, parsed.appliedCustomFilters)),
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
      demandZoneFilters: mergeDemandZoneFilters(parsed.demandZoneFilters),
      appliedDemandZoneFilters: mergeDemandZoneFilters(parsed.appliedDemandZoneFilters),
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
    // localStorage is untrusted input: a tab that last wrote this key on an
    // older build can hold a partial record or a retired mode name such as
    // "e-and-c". Parse into the stored shape and let the checks below decide
    // what is usable, rather than asserting the validated type up front.
    const parsed = JSON.parse(raw) as StoredScannerPreset[];
    const validModes = new Set<SavableScannerMode>([
      "custom-scan",
      "ipo",
      "gap-up-openers",
      "ema-expansion",
      "contraction",
      "near-pivot",
      "pull-backs",
      "returns",
      "consolidating",
      "demand-zone",
      "minervini-1m",
      "minervini-5m",
      "positive-earnings",
      "total-scanner",
    ]);
    return Array.isArray(parsed)
      ? parsed
          .map((item) => {
            if (!item || typeof item !== "object") {
              return null;
            }
            const { id, name } = item;
            const normalizedMode = item.mode === "e-and-c" ? "ema-expansion" : item.mode;
            if (
              typeof id !== "string"
              || typeof name !== "string"
              || typeof normalizedMode !== "string"
              || !validModes.has(normalizedMode as SavableScannerMode)
            ) {
              return null;
            }
            if (normalizedMode === "total-scanner") {
              // Re-validate the saved combination: a leaf whose scanner no
              // longer exists is dropped, and a preset left with no runnable
              // combination is discarded rather than silently matching nothing.
              const tree = normalizeTotalScannerTree(item.totalScannerTree);
              if (!tree || collectLeafModes(tree).length === 0) {
                return null;
              }
              return { ...item, id, name, mode: normalizedMode, totalScannerTree: tree } satisfies SavedScannerPreset;
            }
            return { ...item, id, name, mode: normalizedMode as SavableScannerMode } satisfies SavedScannerPreset;
          })
          .filter((item): item is SavedScannerPreset => Boolean(item))
      : [];
  } catch {
    return [];
  }
}

function scannerModeLabel(mode: SavableScannerMode): string {
  if (mode === "total-scanner") {
    return "Total Scanner";
  }
  if (mode === "custom-scan") {
    return "Custom Scanner";
  }
  if (mode === "ipo") {
    return "IPO";
  }
  if (mode === "gap-up-openers") {
    return "Gap Up Openers";
  }
  if (mode === "ema-expansion") {
    return "Expansion";
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
  if (mode === "demand-zone") {
    return "Demand Zone Scanner";
  }
  if (mode === "minervini-1m") {
    return "Minervini 1 Month";
  }
  if (mode === "minervini-5m") {
    return "Minervini 5 Months";
  }
  if (mode === "positive-earnings") {
    return "Positive Earnings";
  }
  return mode;
}

function isSavableScannerMode(mode: ScreenerMode): mode is SavableScannerMode {
  return mode !== "improving-rs";
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

function savedScannerFreshToday(savedAt: string | null | undefined) {
  if (!savedAt) {
    return false;
  }
  const parsed = new Date(savedAt);
  if (Number.isNaN(parsed.getTime())) {
    return false;
  }
  return indiaDateKey(parsed) === indiaDateKey();
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

/**
 * Toasts must be provided ABOVE the component that consumes them — a component
 * cannot read a context it renders itself. AppShell holds all the existing app
 * logic; App is a thin wrapper that mounts the provider around it.
 */
export default function App(props: AppProps) {
  return (
    <ToastProvider>
      <AppShell {...props} />
    </ToastProvider>
  );
}

function AppShell({ initialMarket, useMarketRoutes = false }: AppProps) {
  const { showToast } = useToast();
  const bootstrapMarket = initialMarket ?? readActiveMarket();
  const initialPreferences = readChartPreferences(bootstrapMarket);
  const initialWatchlists = readWatchlists(bootstrapMarket);
  const initialScannerSettings = readScannerSettings(bootstrapMarket);
  const initialSavedScanners = readSavedScanners(bootstrapMarket);
  const initialSavedDrawings = readSavedDrawings(bootstrapMarket);
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
  const [savedDrawings, setSavedDrawings] = useState<Record<string, ChartAnnotation[]>>(initialSavedDrawings);
  const [fundamentalsBySymbol, setFundamentalsBySymbol] = useState<Record<string, CompanyFundamentals>>({});
  const [chartError, setChartError] = useState<string | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartCacheState, setChartCacheState] = useState<"cached" | "live" | null>(null);
  const [groupWidgetOpen, setGroupWidgetOpen] = useState<boolean>(readGroupWidgetOpen);
  const [groupWidgetRect, setGroupWidgetRect] = useState<GroupWidgetRect>(readGroupWidgetRect);
  const [compareMode, setCompareMode] = useState(false);
  const [compareLayout, setCompareLayout] = useState<"horizontal" | "vertical">("horizontal");
  const [compareDividerRatio, setCompareDividerRatio] = useState(0.5);
  const [paneBSymbol, setPaneBSymbol] = useState<string | null>(null);
  const [activePane, setActivePane] = useState<"A" | "B">("A");
  const [chartB, setChartB] = useState<ChartResponse | null>(null);
  const [chartBLoading, setChartBLoading] = useState(false);
  const [chartBError, setChartBError] = useState<string | null>(null);
  const [chartBCacheState, setChartBCacheState] = useState<"cached" | "live" | null>(null);
  const [fundamentalsLoading, setFundamentalsLoading] = useState(false);
  const [fundamentalsError, setFundamentalsError] = useState<string | null>(null);
  const [activePage, setActivePage] = useState<AppPage>("home");
  const [activeScanner, setActiveScanner] = useState<ScreenerMode>("custom-scan");
  const [resultSortMode, setResultSortMode] = useState<ResultSortMode>("rs");
  const [customFilters, setCustomFilters] = useState<CustomScanRequest>(initialScannerSettings.customFilters);
  const [appliedCustomFilters, setAppliedCustomFilters] = useState<CustomScanRequest>(initialScannerSettings.appliedCustomFilters);
  const [hasAppliedFiltersOnce, setHasAppliedFiltersOnce] = useState(initialScannerSettings.hasAppliedFiltersOnce);
  const [totalScannerTree, setTotalScannerTree] = useState<TotalScannerNode>(() => {
    if (typeof window === "undefined") return defaultTotalScannerTree();
    try {
      const raw = window.localStorage.getItem(TOTAL_SCANNER_KEY);
      return (raw ? normalizeTotalScannerTree(JSON.parse(raw)) : null) ?? defaultTotalScannerTree();
    } catch {
      return defaultTotalScannerTree();
    }
  });
  const [totalScannerError, setTotalScannerError] = useState<string | null>(null);
  const [gapUpThreshold, setGapUpThreshold] = useState(initialScannerSettings.gapUpThreshold);
  const [gapUpMinLiquidityCrore, setGapUpMinLiquidityCrore] = useState<number | null>(initialScannerSettings.gapUpMinLiquidityCrore);
  const [minervini1mMinLiquidityCrore, setMinervini1mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.minervini1mMinLiquidityCrore,
  );
  const [appliedMinervini1mMinLiquidityCrore, setAppliedMinervini1mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.appliedMinervini1mMinLiquidityCrore,
  );
  const [positiveEarningsFilters, setPositiveEarningsFilters] = useState<PositiveEarningsFilters>(
    DEFAULT_POSITIVE_EARNINGS_FILTERS,
  );
  const [appliedPositiveEarningsFilters, setAppliedPositiveEarningsFilters] = useState<PositiveEarningsFilters>(
    DEFAULT_POSITIVE_EARNINGS_FILTERS,
  );
  const [minervini5mMinLiquidityCrore, setMinervini5mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.minervini5mMinLiquidityCrore,
  );
  const [appliedMinervini5mMinLiquidityCrore, setAppliedMinervini5mMinLiquidityCrore] = useState<number | null>(
    initialScannerSettings.appliedMinervini5mMinLiquidityCrore,
  );
  const [nearPivotFilters, setNearPivotFilters] = useState<NearPivotScanRequest>(initialScannerSettings.nearPivotFilters);
  const [appliedNearPivotFilters, setAppliedNearPivotFilters] = useState<NearPivotScanRequest>(initialScannerSettings.appliedNearPivotFilters);
  // Momentum Burst keeps its own draft/applied state (not part of saved scanners).
  const [momentumBurstFilters, setMomentumBurstFilters] = useState<MomentumBurstScanRequest>(DEFAULT_MOMENTUM_BURST_REQUEST);
  const [appliedMomentumBurstFilters, setAppliedMomentumBurstFilters] = useState<MomentumBurstScanRequest>(DEFAULT_MOMENTUM_BURST_REQUEST);
  const [pullBackFilters, setPullBackFilters] = useState<PullBackScanRequest>(initialScannerSettings.pullBackFilters);
  const [appliedPullBackFilters, setAppliedPullBackFilters] = useState<PullBackScanRequest>(initialScannerSettings.appliedPullBackFilters);
  const [returnsFilters, setReturnsFilters] = useState<ReturnsScanRequest>(initialScannerSettings.returnsFilters);
  const [appliedReturnsFilters, setAppliedReturnsFilters] = useState<ReturnsScanRequest>(initialScannerSettings.appliedReturnsFilters);
  const [consolidatingFilters, setConsolidatingFilters] = useState<ConsolidatingScanRequest>(initialScannerSettings.consolidatingFilters);
  const [appliedConsolidatingFilters, setAppliedConsolidatingFilters] = useState<ConsolidatingScanRequest>(initialScannerSettings.appliedConsolidatingFilters);
  const [demandZoneFilters, setDemandZoneFilters] = useState<DemandZoneScanRequest>(initialScannerSettings.demandZoneFilters);
  const [appliedDemandZoneFilters, setAppliedDemandZoneFilters] = useState<DemandZoneScanRequest>(
    initialScannerSettings.appliedDemandZoneFilters,
  );
  // Expansion scanner overrides — let users widen the day-change% / RVOL gates
  // when the IBD-default 6.5%/3.0x feels too strict for the current data.
  const [expansionMinChangePct, setExpansionMinChangePct] = useState<number>(6.5);
  const [appliedExpansionMinChangePct, setAppliedExpansionMinChangePct] = useState<number>(6.5);
  const [expansionMinRelativeVolume, setExpansionMinRelativeVolume] = useState<number>(3.0);
  const [appliedExpansionMinRelativeVolume, setAppliedExpansionMinRelativeVolume] = useState<number>(3.0);
  const [sectorSortBy, setSectorSortBy] = useState<SectorSortBy>("1D");
  const [sectorSortOrder, setSectorSortOrder] = useState<"asc" | "desc">("desc");
  const [sectorVisibleSymbols, setSectorVisibleSymbols] = useState<string[]>([]);
  const [sectorLoading, setSectorLoading] = useState(false);
  const [groupsVisibleSymbols, setGroupsVisibleSymbols] = useState<string[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [improvingRsWindow, setImprovingRsWindow] = useState<ImprovingRsWindow>("1D");
  const [improvingRsLoading, setImprovingRsLoading] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const [showScannerSettings, setShowScannerSettings] = useState(true);
  const [theme, setTheme] = useState<ThemeKey>(readTheme);
  const [watchlists, setWatchlists] = useState<LocalWatchlist[]>(initialWatchlists);
  const [activeWatchlistId, setActiveWatchlistId] = useState<string | null>(readActiveWatchlistId(initialWatchlists, bootstrapMarket));
  const [watchlistPickerSymbol, setWatchlistPickerSymbol] = useState<string | null>(null);
  const [journalAddRequest, setJournalAddRequest] = useState<{ symbol: string; suggestedPrice?: number; suggestedStopLoss?: number; setup?: string } | null>(null);
  const [chartGroupModalContext, setChartGroupModalContext] = useState<ChartGroupContext | null>(null);
  const [tradeReviewContext, setTradeReviewContext] = useState<{ symbol: string; exitDate: string } | null>(null);
  const [savedScanners, setSavedScanners] = useState<SavedScannerPreset[]>(initialSavedScanners);
  const [activeSavedScannerId, setActiveSavedScannerId] = useState<string | null>(null);
  const [scanArrangementMode, setScanArrangementMode] = useState<"flat" | "sector" | "group">("flat");
  // The scan table's actual displayed row order (after its internal filter /
  // sort / sector-group arrangement). ScanTable reports it here so ArrowUp/Down
  // chart navigation steps through stocks in the order the user sees them,
  // rather than the flat pre-arrangement list.
  const [scanVisibleOrder, setScanVisibleOrder] = useState<string[]>([]);
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
  const [bootstrapNonce, setBootstrapNonce] = useState(0);
  const [refreshing, setRefreshing] = useState(false);
  const chartRequestIdRef = useRef(0);
  const chartBRequestIdRef = useRef(0);
  const paneBSymbolRef = useRef<string | null>(null);
  const activePaneRef = useRef<"A" | "B">("A");
  const compareModeRef = useRef(false);
  const groupNavOverrideRef = useRef<string[] | null>(null);
  const fundamentalsRequestIdRef = useRef(0);
  const scanRequestIdRef = useRef(0);
  const scanSectorSummaryRequestIdRef = useRef(0);
  const refreshingRef = useRef(false);
  const handleRefreshRef = useRef<(source?: RefreshSource) => Promise<void>>(async () => {});
  const visibleSymbolsRef = useRef<string[]>([]);
  const pageVisibleSymbolsRef = useRef<string[]>([]);
  const chartNavigationSymbolsRef = useRef<string[] | null>(null);
  const selectedSymbolRef = useRef<string | null>(null);
  const previousActiveWatchlistSymbolsRef = useRef<{ id: string | null; symbols: string[] }>({ id: null, symbols: [] });
  const activeMarketRef = useRef(activeMarket);
  const timeframeRef = useRef(timeframe);
  // In-flight prewarm requests keyed by chart cache key. A Map of promises
  // (not a Set) so an explicit chart open can await the request already in
  // flight instead of issuing a duplicate fetch.
  const prewarmingChartPromisesRef = useRef<Map<string, Promise<ChartResponse>>>(new Map());
  const hoverPrefetchTimerRef = useRef<number | null>(null);
  const hoverPrefetchInFlightRef = useRef(0);
  const chartCompatibilityRecoveryRef = useRef<Set<string>>(new Set());
  const watchlistsSyncReadyRef = useRef<Record<MarketKey, boolean>>({ india: false });
  const watchlistsServerSignatureRef = useRef<Record<MarketKey, string | null>>({ india: null });
  const watchlistsHydrationRequestIdRef = useRef(0);
  const autoRefreshAttemptKeyRef = useRef<Record<MarketKey, string | null>>({ india: null });
  const persistedChartCacheRef = useRef<Record<MarketKey, Record<string, PersistedChartCacheEntry>>>({
    india: readPersistedChartCache("india"),
  });
  const marketViewCacheRef = useRef<Record<MarketKey, MarketViewCacheEntry>>({
    india: readPersistedMarketViewCache("india"),
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
      void import("./components/ImprovingRsPanel");
      void import("./components/GapUpScannerPanel");
      void import("./components/NearPivotScannerPanel");
      void import("./components/PullBackScannerPanel");
      void import("./components/ReturnsScannerPanel");
      void import("./components/ConsolidatingScannerPanel");
      void import("./components/DemandZoneScannerPanel");
      void import("./components/MinerviniScannerPanel");
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

    if (page === "journal") {
      void import("./components/TradeJournalPanel");
      return;
    }
  };

  const readCachedChart = (market: MarketKey, symbol: string, chartTimeframe: ChartTimeframe) => {
    const cacheKey = buildChartCacheKey(market, symbol, chartTimeframe);
    // The session the rest of the app is displaying. Intraday timeframes are
    // exempt — their bars are not daily and the comparison is meaningless.
    const latestSession =
      chartTimeframe === "1D" || chartTimeframe === "1W"
        ? latestSessionDateRef.current
        : null;
    const inMemory = chartResponseCacheRef.current[cacheKey];
    if (isChartResponseCacheCompatible(inMemory, undefined, latestSession)) {
      return inMemory;
    }
    const persisted = persistedChartCacheRef.current[market][cacheKey];
    return isChartResponseCacheCompatible(persisted?.payload, persisted?.saved_at, latestSession)
      ? persisted?.payload ?? null
      : null;
  };

  const storeCachedChart = (market: MarketKey, symbol: string, chartTimeframe: ChartTimeframe, payload: ChartResponse) => {
    const cacheKey = buildChartCacheKey(market, symbol, chartTimeframe);
    chartResponseCacheRef.current[cacheKey] = payload;
    if (!shouldPersistChartResponse(chartTimeframe)) {
      return;
    }
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
      // Reuse an in-flight prewarm request for this exact chart instead of
      // issuing a duplicate fetch — the warm-up is usually most of the way
      // done by the time the user clicks.
      const inFlightPrewarm = prewarmingChartPromisesRef.current.get(buildChartCacheKey(market, symbol, chartTimeframe));
      const payload = await (inFlightPrewarm ?? getChart(symbol, chartTimeframe, market));
      const requestStillMatchesSelection =
        activeMarketRef.current === market &&
        selectedSymbolRef.current === symbol &&
        timeframeRef.current === chartTimeframe;
      if ((chartRequestIdRef.current !== requestId && !requestStillMatchesSelection) || payload.symbol !== symbol || payload.timeframe !== chartTimeframe) {
        if (requestStillMatchesSelection) {
          setChartLoading(false);
        }
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
      const requestStillMatchesSelection =
        activeMarketRef.current === market &&
        selectedSymbolRef.current === symbol &&
        timeframeRef.current === chartTimeframe;
      if (chartRequestIdRef.current !== requestId && !requestStillMatchesSelection) {
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

  const loadChartForPaneB = async (
    symbol: string,
    chartTimeframe: ChartTimeframe,
    market: MarketKey,
  ) => {
    const cachedChart = readCachedChart(market, symbol, chartTimeframe);
    const fallbackChart = cachedChart ?? buildIndexFallbackChart(sectorTabData, symbol, chartTimeframe, market);

    if (fallbackChart) {
      setChartB(fallbackChart);
      setChartBError(null);
      setChartBCacheState(cachedChart ? "cached" : null);
      setChartBLoading(!cachedChart);
      if (cachedChart) {
        return cachedChart;
      }
    } else {
      setChartBLoading(true);
    }

    const requestId = chartBRequestIdRef.current + 1;
    chartBRequestIdRef.current = requestId;

    try {
      const inFlightPrewarm = prewarmingChartPromisesRef.current.get(buildChartCacheKey(market, symbol, chartTimeframe));
      const payload = await (inFlightPrewarm ?? getChart(symbol, chartTimeframe, market));
      if (chartBRequestIdRef.current !== requestId) {
        return fallbackChart;
      }
      if (payload.symbol !== symbol || payload.timeframe !== chartTimeframe) {
        setChartBLoading(false);
        return fallbackChart;
      }
      storeCachedChart(market, symbol, chartTimeframe, payload);
      setChartB(payload);
      setChartBError(null);
      setChartBLoading(false);
      setChartBCacheState("live");
      return payload;
    } catch (loadError) {
      if (chartBRequestIdRef.current !== requestId) {
        return fallbackChart;
      }
      setChartBLoading(false);
      if (fallbackChart) {
        setChartB(fallbackChart);
        setChartBCacheState(cachedChart ? "cached" : null);
        setChartBError(
          loadError instanceof Error ? `${loadError.message}. Showing cached chart.` : "Failed to refresh chart. Showing cached chart.",
        );
        return fallbackChart;
      }
      setChartBCacheState(null);
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
  };

  const handleTimeframeChange = (nextTimeframe: ChartTimeframe) => {
    setTimeframe(normalizeTimeframe(nextTimeframe, activeMarket));
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
    setDemandZoneFilters(settings.demandZoneFilters);
    setAppliedDemandZoneFilters(settings.appliedDemandZoneFilters);
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
    setJournalAddRequest(null);
    setSavedScanners(nextSavedScanners);
    setActiveSavedScannerId(null);
    setSavedDrawings(nextSavedDrawings);
    setChartPalette(nextChartPalette);
    setGroupsData(null);
    setGroupsVisibleSymbols([]);
    setGroupsLoading(false);
    setGroupsFocusRequest(null);
    setSelectedSymbol(null);
    setChart(null);
    setChartOpen(false);
    setChartError(null);
    setChartLoading(false);
    setChartCacheState(null);
    setPaneBSymbol(null);
    setChartB(null);
    setChartBLoading(false);
    setChartBError(null);
    setChartBCacheState(null);
    setCompareMode(false);
    setActivePane("A");
    setFundamentalsBySymbol({});
    setFundamentalsError(null);
    setActiveMarket(nextMarket);
  };

  useEffect(() => {
    if (!useMarketRoutes || typeof window === "undefined") {
      return;
    }

    const targetPath = "/india";
    if (window.location.pathname !== targetPath) {
      window.history.replaceState(window.history.state, "", targetPath);
    }
  }, [activeMarket, useMarketRoutes]);
  const chartResponseCacheRef = useRef<Record<string, ChartResponse>>({});
  // Newest completed session the app knows about, used to expire chart caches
  // whose newest bar is older than it. A ref rather than state because
  // readCachedChart is called from callbacks that must not re-bind on it.
  const latestSessionDateRef = useRef<string | null>(null);
  const tickerRequestIdRef = useRef(0);

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
      ?? firstSymbolFromIndustryGroups(cachedGroups)
      ?? firstSymbolFromSectorTab(cachedSectorTab);

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
  }, [activeMarket, bootstrapNonce]);

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
        const nextWatchlists = normalizedRemote.watchlists.length > 0 ? normalizedRemote.watchlists : localWatchlists;
        const nextActiveWatchlistId = normalizedRemote.watchlists.length > 0
          ? normalizedRemote.activeWatchlistId
          : localActiveWatchlistId;

        setWatchlists(nextWatchlists);
        setActiveWatchlistId(nextActiveWatchlistId);
        watchlistsServerSignatureRef.current[activeMarket] = watchlistsStateSignature(
          normalizedRemote.watchlists,
          normalizedRemote.activeWatchlistId,
        );
        watchlistsSyncReadyRef.current[activeMarket] = true;

        if (normalizedRemote.watchlists.length === 0 && localWatchlists.length > 0) {
          const localPayload = {
            watchlists: localWatchlists,
            active_watchlist_id: localActiveWatchlistId,
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
      } catch {
        if (!active || watchlistsHydrationRequestIdRef.current !== requestId) {
          return;
        }

        setWatchlists(localWatchlists);
        setActiveWatchlistId(localActiveWatchlistId);
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
      if (!hadCachedDashboard) {
        setLoading(true);
      }

      try {
        const dashboardPromise = getDashboard(activeMarket);
        const groupsPromise = getIndustryGroups(activeMarket);

        const dashboardPayload = await dashboardPromise;
        if (!active) {
          return;
        }

        setDashboard(dashboardPayload);
        // Seed the universe catalog from dashboard immediately so the symbol
        // search, watchlist suggestions, and "X stocks in universe" count
        // never sit empty while the heavier /api/groups call is in flight.
        // Industry groups, when they arrive, replace this with the full
        // 1000+ symbol catalog.
        setUniverseCatalog((current) =>
          current.length > 0 ? current : buildUniverseCatalogFromDashboard(dashboardPayload),
        );
        updateMarketViewCache(activeMarket, { dashboard: dashboardPayload });
        setSelectedSymbol((current) => current ?? dashboardPayload.top_gainers[0]?.symbol ?? null);
        setLoading(false);
        setError(null);
        refreshTickerRibbon();

        void groupsPromise
          .then((groupsPayload) => {
            if (!active) {
              return;
            }
            const nextUniverseCatalog = buildUniverseCatalogFromIndustryGroups(groupsPayload);
            setGroupsData(groupsPayload);
            setUniverseCatalog(nextUniverseCatalog);
            setSelectedSymbol((current) => {
              const universeSymbols = new Set(nextUniverseCatalog.map((item) => item.symbol));
              if (current && universeSymbols.has(current)) {
                updateMarketViewCache(activeMarket, {
                  groupsData: groupsPayload,
                  universeCatalog: nextUniverseCatalog,
                  selectedSymbol: current,
                });
                return current;
              }
              const nextSymbol = dashboardPayload.top_gainers[0]?.symbol ?? firstSymbolFromIndustryGroups(groupsPayload);
              updateMarketViewCache(activeMarket, {
                groupsData: groupsPayload,
                universeCatalog: nextUniverseCatalog,
                selectedSymbol: nextSymbol,
              });
              return nextSymbol;
            });
            updateMarketViewCache(activeMarket, {
              groupsData: groupsPayload,
              universeCatalog: nextUniverseCatalog,
            });
          })
          .catch((groupsError) => {
            if (!active) {
              return;
            }
            setError((current) => current ?? (
              groupsError instanceof Error
                ? hadCachedDashboard
                  ? `${groupsError.message}. Group data may be stale.`
                  : groupsError.message
                : "Failed to load groups"
            ));
          });
      } catch (loadError) {
        if (active) {
          setLoading(false);
          setError(
            loadError instanceof Error
              ? hadCachedDashboard
                ? `${loadError.message}. Showing cached market snapshot.`
                : loadError.message
              : hadCachedDashboard
                ? "Failed to refresh live market data. Showing cached market snapshot."
                : "Failed to load home page",
          );
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
      void import("./components/GroupsPanel");
      void import("./components/WatchlistsPanel");
    }, 3500);

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
    if (loading) {
      return;
    }

    let active = true;

    async function loadSelectedScanner() {
      const requestId = scanRequestIdRef.current + 1;
      scanRequestIdRef.current = requestId;
      try {
        if (
          activePage === "home" ||
          activePage === "watchlists" ||
          activePage === "journal"
        ) {
          setScanLoading(false);
          setScanSectorSummariesLoading(false);
          setError(null);
          return;
        }

        if (activePage === "groups") {
          setScanLoading(false);
          setScanSectorSummariesLoading(false);
          if (groupsData) {
            setGroupsLoading(false);
            setError(null);
            return;
          }
          setGroupsLoading(true);
          const payload = await getIndustryGroups(activeMarket);
          if (!active) {
            return;
          }
          setGroupsData(payload);
          setSelectedSymbol((current) => (
            current && payload.stocks.some((item) => item.symbol === current)
              ? current
              : firstSymbolFromIndustryGroups(payload)
          ));
          setError(null);
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
        const payload = await requestActiveScannerResults(scanArrangementMode === "sector");

        if (!payload || !active || scanRequestIdRef.current !== requestId) {
          return;
        }
        setScanResults(payload);
        setScanSectorSummaries(payload.sector_summaries ?? []);
        setSelectedSymbol((current) => (current && payload.items.some((item) => item.symbol === current) ? current : payload.items[0]?.symbol ?? null));
        setError(null);
      } catch (loadError) {
        if (active && scanRequestIdRef.current === requestId) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load screener");
        }
      } finally {
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
    appliedDemandZoneFilters,
    appliedExpansionMinChangePct,
    appliedExpansionMinRelativeVolume,
    appliedMinervini1mMinLiquidityCrore,
    appliedMinervini5mMinLiquidityCrore,
    appliedMomentumBurstFilters,
    appliedNearPivotFilters,
    appliedPositiveEarningsFilters,
    appliedPullBackFilters,
    appliedReturnsFilters,
    gapUpMinLiquidityCrore,
    gapUpThreshold,
    groupsData,
    hasAppliedFiltersOnce,
    improvingRsWindow,
    loading,
    scannerRunNonce,
    scanArrangementMode,
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
    appliedDemandZoneFilters,
    appliedExpansionMinChangePct,
    appliedExpansionMinRelativeVolume,
    appliedNearPivotFilters,
    appliedPositiveEarningsFilters,
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
    if (loading || activePage === "groups" || groupsData) {
      return;
    }

    let active = true;
    const prefetchHandle = window.setTimeout(() => {
      void prefetchGroupsPage();
    }, 1200);

    async function prefetchGroupsPage() {
      try {
        const payload = await getIndustryGroups(activeMarket);
        if (active) {
          setGroupsData(payload);
        }
      } catch {
        // Prefetch is best-effort and should not disturb the active page.
      }
    }

    return () => {
      active = false;
      window.clearTimeout(prefetchHandle);
    };
  }, [activeMarket, activePage, groupsData, loading]);

  useEffect(() => {
    if (activeMarket === "india" && timeframe !== "1D" && timeframe !== "1W") {
      setTimeframe("1D");
    }
  }, [activeMarket, timeframe]);

  useEffect(() => {
    if (!selectedSymbol) {
      chartRequestIdRef.current += 1;
      setChart(null);
      setChartError(null);
      setChartLoading(false);
      setChartCacheState(null);
      return;
    }

    if (
      (activePage === "home" && !chartOpen) ||
      (activePage === "journal" && !chartOpen)
    ) {
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

  // Keep the HF Space warm while the user has the tab open. Free Spaces
  // sleep after ~30 minutes idle which surfaces in the UI as
  // "Network request failed, retrying... Showing cached market snapshot".
  // A 4-minute /api/health ping while the document is visible avoids that.
  useEffect(() => {
    if (typeof window === "undefined" || typeof document === "undefined") return;

    let cancelled = false;
    const KEEP_ALIVE_INTERVAL_MS = 4 * 60 * 1000;

    const fire = () => {
      if (cancelled) return;
      if (document.visibilityState !== "visible") return;
      void pingBackendHealth().catch(() => {});
    };

    fire();
    const intervalId = window.setInterval(fire, KEEP_ALIVE_INTERVAL_MS);
    const onVisibility = () => {
      if (document.visibilityState === "visible") fire();
    };
    document.addEventListener("visibilitychange", onVisibility);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  useEffect(() => {
    refreshingRef.current = refreshing;
  }, [refreshing]);

  useEffect(() => {
    activeMarketRef.current = activeMarket;
  }, [activeMarket]);

  useEffect(() => {
    timeframeRef.current = timeframe;
  }, [timeframe]);

  useEffect(() => {
    paneBSymbolRef.current = paneBSymbol;
  }, [paneBSymbol]);

  useEffect(() => {
    activePaneRef.current = activePane;
  }, [activePane]);

  useEffect(() => {
    compareModeRef.current = compareMode;
  }, [compareMode]);

  useEffect(() => {
    if (!compareMode || !chartOpen || !paneBSymbol) {
      chartBRequestIdRef.current += 1;
      if (!compareMode || !paneBSymbol) {
        setChartB(null);
        setChartBError(null);
        setChartBLoading(false);
        setChartBCacheState(null);
      }
      return;
    }
    const symbolForB = paneBSymbol;
    const timeframeForB = timeframe;
    const marketForB = activeMarket;
    void (async () => {
      try {
        await loadChartForPaneB(symbolForB, timeframeForB, marketForB);
      } catch (loadError) {
        setChartBError(loadError instanceof Error ? loadError.message : "Failed to load chart");
      }
    })();
    return () => {
      chartBRequestIdRef.current += 1;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeMarket, chartOpen, compareMode, paneBSymbol, timeframe]);

  useEffect(() => {
    if (
      !selectedSymbol ||
      chartPanelTab !== "fundamentals" ||
      (activePage === "home" && !chartOpen) ||
      (activePage === "journal" && !chartOpen)
    ) {
      setFundamentalsLoading(false);
      return;
    }

    if (fundamentalsBySymbol[selectedSymbol]) {
      setFundamentalsError(null);
      setFundamentalsLoading(false);
      return;
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
      }),
    );
  }, [activeMarket, chartColors, chartDrawingColor, chartPanelTab, chartStyle, indicatorKeys, showBenchmarkOverlay, timeframe]);

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
    if (typeof window === "undefined") {
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
        demandZoneFilters,
        appliedDemandZoneFilters,
      }),
    );
  }, [
    appliedConsolidatingFilters,
    appliedCustomFilters,
    appliedDemandZoneFilters,
    appliedMinervini1mMinLiquidityCrore,
    appliedMinervini5mMinLiquidityCrore,
    appliedNearPivotFilters,
    appliedPullBackFilters,
    appliedReturnsFilters,
    consolidatingFilters,
    customFilters,
    demandZoneFilters,
    gapUpMinLiquidityCrore,
    gapUpThreshold,
    hasAppliedFiltersOnce,
    minervini1mMinLiquidityCrore,
    minervini5mMinLiquidityCrore,
    nearPivotFilters,
    pullBackFilters,
    returnsFilters,
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
    // These scanners run through dedicated endpoints; including them here
    // would POST pattern values the CustomScanRequest schema/evaluator cannot run.
    (scanner) =>
      scanner.category === "Setups"
      && scanner.id !== "custom-scan"
      && scanner.id !== "positive-earnings"
      && scanner.id !== "demand-zone",
  );
  const displayScan = scanResults ? applyScannerDisplayAlias(scanResults.scan) : null;
  // Track the newest session the app has seen so stale chart caches expire.
  // Uses breadth_today.date (the EOD session the dashboard is built from)
  // rather than generated_at, which moves whenever the payload is rebuilt.
  useEffect(() => {
    const session = dashboard?.breadth_today?.date ?? null;
    if (session) latestSessionDateRef.current = session;
  }, [dashboard]);

  const snapshotDateLabel = formatSnapshotDate(activeMarket, dashboard?.generated_at);
  const snapshotTimeLabel = formatSnapshotTime(activeMarket, dashboard?.generated_at);
  const activeWatchlist = watchlists.find((watchlist) => watchlist.id === activeWatchlistId) ?? watchlists[0] ?? null;
  const activeViewCount =
    activePage === "home"
      ? dashboard?.universe_count ?? 0
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
      : activePage === "groups"
          ? "Groups"
        : activePage === "watchlists"
            ? "Watchlist Stocks"
            : activeScanner === "improving-rs"
              ? "52W High RS"
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
  // On the screener, the ScanTable can regroup/re-sort rows internally (sector
  // or group arrangement, in-table search), so the displayed order differs from
  // the flat visibleScanItems. Use the order ScanTable reported so arrow-key
  // navigation follows what the user sees. Momentum Burst / Improving RS render
  // their own flat lists, so they keep the flat order.
  const screenerNavSymbols =
    activeScanner === "momentum-burst"
      ? visibleScanItems.map((item) => item.symbol)
      : scanVisibleOrder.length > 0
        ? scanVisibleOrder
        : visibleScanItems.map((item) => item.symbol);
  const pageVisibleSymbols =
    activePage === "screener" && activeScanner === "improving-rs"
      ? (improvingRsData?.items ?? []).map((item) => item.symbol)
      : activePage === "groups"
          ? groupsVisibleSymbols
        : activePage === "watchlists"
          ? watchlistVisibleSymbols
          : activePage === "home"
            ? (dashboard?.top_gainers ?? []).map((item) => item.symbol)
            : activePage === "screener"
              ? screenerNavSymbols
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
  const navigationSeedKey = navigationSeed.join("|");
  const displayedChart = chart && chart.symbol === selectedSymbol && chart.timeframe === timeframe ? chart : null;
  const activeChartKey = selectedSymbol ? `${selectedSymbol}:${timeframe}` : null;
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

  const journalTrades = useJournalTrades();
  const activeTradeMarkers = useMemo(
    () => tradeMarkersForSymbol(journalTrades, selectedSymbol),
    [journalTrades, selectedSymbol],
  );
  const paneBTradeMarkers = useMemo(
    () => tradeMarkersForSymbol(journalTrades, paneBSymbol),
    [journalTrades, paneBSymbol],
  );

  const paneBDisplayedChart = chartB && paneBSymbol && chartB.symbol === paneBSymbol && chartB.timeframe === timeframe ? chartB : null;
  const paneBChartKey = paneBSymbol ? `${paneBSymbol}:${timeframe}` : null;
  const paneBAnnotations = paneBChartKey ? savedDrawings[paneBChartKey] ?? [] : [];
  const paneBFundamentals = paneBSymbol ? fundamentalsBySymbol[paneBSymbol] ?? null : null;
  const paneBChartGroupContext = useMemo(
    () => resolveChartGroupContext(paneBSymbol, groupsData),
    [groupsData, paneBSymbol],
  );
  const paneBChartGroupSummary = useMemo<ChartGroupSummary | null>(() => {
    if (!paneBChartGroupContext) return null;
    return {
      groupId: paneBChartGroupContext.groupId,
      groupName: paneBChartGroupContext.groupName,
      groupRank: paneBChartGroupContext.groupRank,
      groupRankLabel: paneBChartGroupContext.groupRankLabel,
      stockRank: paneBChartGroupContext.stockRank,
      stockCount: paneBChartGroupContext.stockCount,
    };
  }, [paneBChartGroupContext]);

  const handlePaneBAnnotationsChange = (annotations: ChartAnnotation[]) => {
    if (!paneBChartKey) return;
    setSavedDrawings((current) => ({ ...current, [paneBChartKey]: annotations }));
  };

  const groupWidgetContext = useMemo(() => {
    if (!activeChartGroupContext) return null;
    return {
      groupId: activeChartGroupContext.groupId,
      groupName: activeChartGroupContext.groupName,
      parentSector: activeChartGroupContext.parentSector,
      groupRankLabel: activeChartGroupContext.groupRankLabel,
      stockCount: activeChartGroupContext.stockCount,
      members: activeChartGroupContext.members,
    };
  }, [activeChartGroupContext]);

  useEffect(() => {
    if (chartOpen && groupWidgetOpen && activeChartGroupContext) {
      groupNavOverrideRef.current = activeChartGroupContext.members.map((member) => member.symbol);
    } else {
      groupNavOverrideRef.current = null;
    }
  }, [chartOpen, groupWidgetOpen, activeChartGroupContext]);

  useEffect(() => {
    setChartGroupModalContext(null);
  }, [activeMarket, selectedSymbol]);

  useEffect(() => {
    if (!chartOpen) {
      setCompareMode(false);
      setActivePane("A");
      setPaneBSymbol(null);
      setChartB(null);
      setChartBLoading(false);
      setChartBError(null);
      setChartBCacheState(null);
    }
  }, [chartOpen]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      window.localStorage.setItem(GROUP_WIDGET_OPEN_KEY, groupWidgetOpen ? "1" : "0");
    } catch {
      // ignore quota
    }
  }, [groupWidgetOpen]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      window.localStorage.setItem(GROUP_WIDGET_RECT_KEY, JSON.stringify(groupWidgetRect));
    } catch {
      // ignore quota
    }
  }, [groupWidgetRect]);

  useEffect(() => {
    if (!selectedSymbol || !displayedChart) {
      return;
    }

    if (activePage === "home" && !chartOpen) {
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

  const normalizeDemandZoneFilters = (filters: DemandZoneScanRequest): DemandZoneScanRequest => {
    const defaults = filters.timeframe === "daily" ? DEFAULT_DAILY_DEMAND_ZONE_FILTERS : DEFAULT_DEMAND_ZONE_FILTERS;
    const baseMinWeeks = Math.max(1, Math.min(12, Math.round(filters.base_min_weeks || defaults.base_min_weeks)));
    const baseMaxWeeks = Math.max(baseMinWeeks, Math.min(20, Math.round(filters.base_max_weeks || defaults.base_max_weeks)));
    return {
      ...filters,
      timeframe: filters.timeframe === "daily" ? "daily" : "weekly",
      max_distance_above_zone_pct: Math.max(0, Math.min(3, filters.max_distance_above_zone_pct ?? defaults.max_distance_above_zone_pct)),
      min_rs_rating: Math.max(1, Math.min(99, Math.round(filters.min_rs_rating ?? defaults.min_rs_rating))),
      min_liquidity_crore: Math.max(0, filters.min_liquidity_crore ?? defaults.min_liquidity_crore),
      min_departure_pct: Math.max(1, Math.min(100, filters.min_departure_pct ?? defaults.min_departure_pct)),
      base_min_weeks: baseMinWeeks,
      base_max_weeks: baseMaxWeeks,
      max_base_range_pct: Math.max(1, Math.min(50, filters.max_base_range_pct ?? defaults.max_base_range_pct)),
      max_zone_age_weeks: Math.max(1, Math.min(260, Math.round(filters.max_zone_age_weeks ?? defaults.max_zone_age_weeks))),
      limit: clampResultLimit(filters.limit),
    };
  };

  // Dispatch is parameterised by mode (not `activeScanner`) so Total Scanner
  // can run any leaf scanner through the exact same code path — including
  // Custom Scanner's applied filters and every per-scanner threshold.
  const requestScannerResults = (mode: ScreenerMode, includeSectorSummaries = false) => {
    const options = { includeSectorSummaries };
    // Must be first: without it "total-scanner" would fall through to the
    // Custom Scanner default at the bottom of this chain.
    if (mode === "total-scanner") {
      return runTotalScannerCombination();
    }
    if (mode === "bread-butter") {
      return getScanResults("bread-butter", activeMarket, options);
    }
    if (mode === "volume") {
      return getScanResults("volume", activeMarket, options);
    }
    if (mode === "ipo") {
      return getScanResults("ipo", activeMarket, options);
    }
    if (mode === "ema-expansion") {
      return getScanResults("ema-expansion", activeMarket, {
        ...options,
        expansionMinChangePct: appliedExpansionMinChangePct,
        expansionMinRelativeVolume: appliedExpansionMinRelativeVolume,
      });
    }
    if (mode === "contraction") {
      return getScanResults("contraction", activeMarket, options);
    }
    if (mode === "gap-up-openers") {
      return getGapUpOpeners(gapUpThreshold, activeMarket, gapUpMinLiquidityCrore, options);
    }
    if (mode === "near-pivot") {
      return getNearPivotScan(appliedNearPivotFilters, activeMarket, options);
    }
    if (mode === "pull-backs") {
      return getPullBackScan(appliedPullBackFilters, activeMarket, options);
    }
    if (mode === "returns") {
      return getReturnsScan(appliedReturnsFilters, activeMarket, options);
    }
    if (mode === "consolidating") {
      return getConsolidatingScan(appliedConsolidatingFilters, activeMarket, options);
    }
    if (mode === "demand-zone") {
      return getDemandZoneScan(appliedDemandZoneFilters, activeMarket, options);
    }
    if (mode === "momentum-burst") {
      return getMomentumBurstScan(appliedMomentumBurstFilters, activeMarket, options);
    }
    if (mode === "minervini-1m") {
      return getScanResults("minervini-1m", activeMarket, { ...options, minLiquidityCrore: appliedMinervini1mMinLiquidityCrore });
    }
    if (mode === "minervini-5m") {
      return getScanResults("minervini-5m", activeMarket, { ...options, minLiquidityCrore: appliedMinervini5mMinLiquidityCrore });
    }
    if (mode === "episodic-pivot") {
      return getScanResults("episodic-pivot", activeMarket, options);
    }
    if (mode === "rs-line-leads") {
      return getScanResults("rs-line-leads", activeMarket, options);
    }
    if (mode === "fresh-stage2") {
      return getScanResults("fresh-stage2", activeMarket, options);
    }
    if (mode === "high-tight-flag") {
      return getScanResults("high-tight-flag", activeMarket, options);
    }
    if (mode === "vcp") {
      return getScanResults("vcp", activeMarket, options);
    }
    if (mode === "tight-closes") {
      return getScanResults("tight-closes", activeMarket, options);
    }
    if (mode === "power-base") {
      return getScanResults("power-base", activeMarket, options);
    }
    if (mode === "positive-earnings") {
      return getScanResults("positive-earnings", activeMarket, {
        ...options,
        positiveEarningsMinCloseInRangePct: appliedPositiveEarningsFilters.minCloseInRangePct,
        positiveEarningsMinNextDayGapPct: appliedPositiveEarningsFilters.minNextDayGapPct,
        positiveEarningsMinDayRvol: appliedPositiveEarningsFilters.minDayRvol,
        positiveEarningsMinReturn5dPct: appliedPositiveEarningsFilters.minReturn5dPct,
        positiveEarningsLookbackDays: appliedPositiveEarningsFilters.lookbackDays,
      });
    }
    if (mode === "custom-scan" && !hasAppliedFiltersOnce) {
      return Promise.resolve(null);
    }
    return runCustomScan(appliedCustomFilters, activeMarket, options);
  };

  /**
   * Total Scanner: run every distinct leaf once, reduce each to a symbol set,
   * then let the AND/OR tree decide the survivors. Rows are reused from
   * whichever scanner surfaced the symbol first, with `also_in` carrying the
   * full list of scanners it matched (the table already renders that).
   */
  useEffect(() => {
    try {
      window.localStorage.setItem(TOTAL_SCANNER_KEY, JSON.stringify(totalScannerTree));
    } catch {
      // best-effort persistence only
    }
  }, [totalScannerTree]);

  const runTotalScannerCombination = async (
    tree: TotalScannerNode = totalScannerTree,
  ): Promise<ScanResultsResponse> => {
    // Always resolves a response: an empty combination legitimately means
    // "0 matched" rather than "don't update the results".
    const modes = collectLeafModes(tree);
    // One fetch per DISTINCT scanner, in parallel; a scanner that fails or is
    // unconfigured (e.g. Custom Scanner with no filters applied) contributes an
    // empty set rather than failing the whole run.
    const settled = await Promise.all(
      modes.map(async (mode) => {
        try {
          return [mode, await requestScannerResults(mode, false)] as const;
        } catch {
          return [mode, null] as const;
        }
      }),
    );

    const sets = new Map<TotalScannerLeafMode, Set<string>>();
    const rowBySymbol = new Map<string, ScanMatch>();
    const matchedBySymbol = new Map<string, string[]>();
    for (const [mode, payload] of settled) {
      const items = payload?.items ?? [];
      sets.set(mode, new Set(items.map((item) => item.symbol)));
      for (const item of items) {
        if (!rowBySymbol.has(item.symbol)) rowBySymbol.set(item.symbol, item);
        const label = totalScannerLeafLabel(mode);
        const list = matchedBySymbol.get(item.symbol);
        if (list) list.push(label);
        else matchedBySymbol.set(item.symbol, [label]);
      }
    }

    const symbols = evaluateTotalScanner(tree, sets);
    const items: ScanMatch[] = [];
    for (const symbol of symbols) {
      const row = rowBySymbol.get(symbol);
      if (row) items.push({ ...row, also_in: matchedBySymbol.get(symbol) ?? [] });
    }
    // Confluence first: names hit by more of the selected scanners lead, then
    // by RS. In an AND run every row ties on count, so RS does the sorting.
    items.sort(
      (a, b) => (b.also_in?.length ?? 0) - (a.also_in?.length ?? 0) || (b.rs_rating ?? 0) - (a.rs_rating ?? 0),
    );

    return {
      scan: {
        id: "total-scanner",
        name: "Total Scanner",
        category: "Custom",
        description: describeTotalScanner(tree),
        hit_count: items.length,
      },
      generated_at: new Date().toISOString(),
      market_cap_min_crore: settled.find(([, payload]) => payload)?.[1]?.market_cap_min_crore ?? 0,
      total_hits: items.length,
      items,
      sector_summaries: [],
    };
  };

  const requestActiveScannerResults = (includeSectorSummaries = false) =>
    requestScannerResults(activeScanner, includeSectorSummaries);

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

    if (mode === "total-scanner") {
      preset.totalScannerTree = totalScannerTree;
      if (!base?.name) {
        // Name it after the combination — "VCP AND Power Base" is far more
        // useful in the sidebar than "Total Scanner 2" once you have a few.
        const formula = describeTotalScanner(totalScannerTree);
        preset.name = formula.length <= 42 ? formula : `${formula.slice(0, 39)}…`;
      }
    } else if (mode === "custom-scan") {
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
    } else if (mode === "demand-zone") {
      preset.demandZoneFilters = normalizeDemandZoneFilters(source === "draft" ? demandZoneFilters : appliedDemandZoneFilters);
    } else if (mode === "minervini-1m") {
      preset.minerviniMinLiquidityCrore = source === "draft" ? minervini1mMinLiquidityCrore : appliedMinervini1mMinLiquidityCrore;
    } else if (mode === "minervini-5m") {
      preset.minerviniMinLiquidityCrore = source === "draft" ? minervini5mMinLiquidityCrore : appliedMinervini5mMinLiquidityCrore;
    }

    return preset;
  };

  const runSavedScannerRequest = (preset: SavedScannerPreset, includeSectorSummaries = false) => {
    const options = { includeSectorSummaries };
    if (preset.mode === "total-scanner") {
      // Run the preset's OWN saved combination, not whatever is currently in
      // the builder — otherwise refreshing a preset's count would silently
      // report the working tree's results against the preset's name.
      // Load-time validation discards presets without a runnable tree, so the
      // fallback here should be unreachable; it just keeps the type honest.
      const tree = normalizeTotalScannerTree(preset.totalScannerTree);
      return runTotalScannerCombination(tree ?? { kind: "group", id: preset.id, op: "AND", children: [] });
    }
    if (preset.mode === "custom-scan") {
      return runCustomScan(sanitizeCustomFiltersPattern(mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, preset.customFilters)), activeMarket, options);
    }
    if (preset.mode === "ipo") {
      return getScanResults("ipo", activeMarket, options);
    }
    if (preset.mode === "ema-expansion") {
      return getScanResults("ema-expansion", activeMarket, {
        ...options,
        expansionMinChangePct: appliedExpansionMinChangePct,
        expansionMinRelativeVolume: appliedExpansionMinRelativeVolume,
      });
    }
    if (preset.mode === "contraction") {
      return getScanResults("contraction", activeMarket, options);
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
    if (preset.mode === "demand-zone") {
      return getDemandZoneScan(mergeDemandZoneFilters(preset.demandZoneFilters), activeMarket, options);
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
      } else if (activeScanner === "demand-zone" && finalizedPreset.demandZoneFilters) {
        setDemandZoneFilters(finalizedPreset.demandZoneFilters);
        setAppliedDemandZoneFilters(finalizedPreset.demandZoneFilters);
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

    if (preset.mode === "total-scanner") {
      const tree = normalizeTotalScannerTree(preset.totalScannerTree);
      if (tree) {
        setTotalScannerTree(tree);
      }
      setTotalScannerError(null);
    } else if (preset.mode === "custom-scan") {
      const nextFilters = sanitizeCustomFiltersPattern(mergeWithDefaults(DEFAULT_CUSTOM_FILTERS, preset.customFilters));
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
    } else if (preset.mode === "demand-zone") {
      const nextFilters = mergeDemandZoneFilters(preset.demandZoneFilters);
      setDemandZoneFilters(nextFilters);
      setAppliedDemandZoneFilters(nextFilters);
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

  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setCommandPaletteOpen((current) => !current);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

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
    if (timeframe !== "1D" && timeframe !== "1W") {
      return;
    }

    if (activePage !== "screener" && activePage !== "watchlists") {
      return;
    }

    // Sliding prewarm window so chart navigation never waits:
    // - results just loaded (nothing selected): warm the first 20 charts;
    // - a chart is selected: warm 15 ahead + 5 behind it, nearest first.
    // Moving to the next chart slides the window — everything already warmed
    // is skipped via the cache check, so each step costs ~one fetch (the new
    // symbol entering the window). NOTE: window positions come from the
    // natural results order (navigationSeed), NOT visibleSymbols, which
    // hoists the open symbol to index 0 and would anchor the window to the
    // top of the list.
    const PREFETCH_INITIAL = 20;
    const PREFETCH_AHEAD = 15;
    const PREFETCH_BEHIND = 5;
    const orderedList = navigationSeed.filter(Boolean);
    const selectedIndex = selectedSymbol ? orderedList.indexOf(selectedSymbol) : -1;
    const symbolsToWarm =
      selectedIndex === -1
        ? orderedList.filter((symbol) => symbol !== selectedSymbol).slice(0, PREFETCH_INITIAL)
        : [
            ...orderedList.slice(selectedIndex + 1, selectedIndex + 1 + PREFETCH_AHEAD),
            ...orderedList.slice(Math.max(0, selectedIndex - PREFETCH_BEHIND), selectedIndex).reverse(),
          ];

    if (symbolsToWarm.length === 0) {
      return;
    }

    let stopQueue = false;
    const timeoutId = window.setTimeout(() => {
      void (async () => {
        // Concurrency matches the backend's 3-slot cold chart-fetch
        // semaphore, so the warm-up uses the full budget without ever
        // queueing scans or user-initiated chart loads behind it.
        const CONCURRENCY = 3;
        const queue = [...symbolsToWarm];
        const fetchOne = async (symbol: string) => {
          const cacheKey = buildChartCacheKey(activeMarket, symbol, timeframe);
          if (readCachedChart(activeMarket, symbol, timeframe) || prewarmingChartPromisesRef.current.has(cacheKey)) {
            return;
          }
          const request = getChart(symbol, timeframe, activeMarket);
          prewarmingChartPromisesRef.current.set(cacheKey, request);
          try {
            const payload = await request;
            // Cache regardless of stopQueue — the network call already
            // completed, throwing it away wastes the work and forces a
            // re-fetch when the user revisits this symbol.
            if (payload.symbol === symbol && payload.timeframe === timeframe) {
              storeCachedChart(activeMarket, symbol, timeframe, payload);
            }
          } catch {
            // Ignore prewarm failures and let explicit chart loads retry on demand.
          } finally {
            prewarmingChartPromisesRef.current.delete(cacheKey);
          }
        };
        const workers = Array.from({ length: Math.min(CONCURRENCY, queue.length) }, async () => {
          while (!stopQueue && queue.length > 0) {
            const symbol = queue.shift();
            if (!symbol) return;
            await fetchOne(symbol);
          }
        });
        await Promise.all(workers);
      })();
    }, 80);

    return () => {
      stopQueue = true;
      window.clearTimeout(timeoutId);
    };
  }, [activeMarket, activePage, selectedSymbol, timeframe, navigationSeedKey]);

  useEffect(() => {
    if (activePage !== "watchlists" || !activeWatchlist) {
      return;
    }

    if (!activeWatchlist.symbols.length) {
      previousActiveWatchlistSymbolsRef.current = { id: activeWatchlist.id, symbols: [] };
      return;
    }

    if (!selectedSymbol || !activeWatchlist.symbols.includes(selectedSymbol)) {
      const prev = previousActiveWatchlistSymbolsRef.current;
      const sameWatchlist = prev.id === activeWatchlist.id;
      let nextSymbol: string | null = null;
      if (sameWatchlist && selectedSymbol) {
        const oldIndex = prev.symbols.indexOf(selectedSymbol);
        if (oldIndex !== -1) {
          for (let i = oldIndex + 1; i < prev.symbols.length; i += 1) {
            if (activeWatchlist.symbols.includes(prev.symbols[i])) {
              nextSymbol = prev.symbols[i];
              break;
            }
          }
          if (!nextSymbol) {
            for (let i = oldIndex - 1; i >= 0; i -= 1) {
              if (activeWatchlist.symbols.includes(prev.symbols[i])) {
                nextSymbol = prev.symbols[i];
                break;
              }
            }
          }
        }
      }
      setSelectedSymbol(nextSymbol ?? activeWatchlist.symbols[0]);
    }

    previousActiveWatchlistSymbolsRef.current = { id: activeWatchlist.id, symbols: activeWatchlist.symbols };
  }, [activePage, activeWatchlist, selectedSymbol]);

  useEffect(() => {
    if (activePage !== "groups" && !chartOpen) {
      chartNavigationSymbolsRef.current = null;
    }
  }, [activePage, chartOpen]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const shouldHandleListNavigation = activePage === "screener" || activePage === "watchlists";
      const shouldHandleGroupsNavigation = activePage === "groups" && !chartOpen && Boolean(chartNavigationSymbolsRef.current?.length);
      const shouldHandleChartContextNavigation =
        chartOpen &&
        (Boolean(chartNavigationSymbolsRef.current?.length) ||
          activePage === "screener" ||
          activePage === "watchlists" ||
          activePage === "groups");
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

      // Priority: the list the chart was opened WITH (Markets focus, group page,
      // etc.), then the current page's visible list (scanner rows, watchlist),
      // and only THEN the group-sidebar members. The group override must not win
      // over the scanner/page list — otherwise merely having the group sidebar
      // open hijacks Up/Down onto group peers, which reads as "random" stocks.
      const symbols = chartOpen
        ? chartNavigationSymbolsRef.current?.length
          ? chartNavigationSymbolsRef.current
          : pageVisibleSymbolsRef.current?.length
            ? pageVisibleSymbolsRef.current
            : groupNavOverrideRef.current?.length
              ? groupNavOverrideRef.current
              : []
        : shouldHandleGroupsNavigation && chartNavigationSymbolsRef.current?.length
          ? chartNavigationSymbolsRef.current
          : visibleSymbolsRef.current;
      const targetActivePane = activePaneRef.current;
      const isPaneBNav = chartOpen && compareModeRef.current && targetActivePane === "B";
      const currentSymbol = isPaneBNav ? paneBSymbolRef.current : selectedSymbolRef.current;
      if (!symbols.length || !currentSymbol) {
        return;
      }

      const currentIndex = symbols.indexOf(currentSymbol);
      if (currentIndex === -1) {
        return;
      }

      const applySymbol = (nextSymbol: string) => {
        if (!nextSymbol || nextSymbol === currentSymbol) return;
        if (isPaneBNav) {
          setPaneBSymbol(nextSymbol);
        } else {
          setSelectedSymbol(nextSymbol);
          if (chartOpen) {
            setChartOpen(true);
          }
        }
      };

      if (event.key === "ArrowUp") {
        event.preventDefault();
        const previousIndex = (currentIndex - 1 + symbols.length) % symbols.length;
        applySymbol(symbols[previousIndex]);
      }

      if (event.key === "ArrowDown") {
        event.preventDefault();
        const nextIndex = (currentIndex + 1) % symbols.length;
        applySymbol(symbols[nextIndex]);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activePage, chartOpen]);

  // "L" swaps the main chart series between candles and IBD-style HLC bars.
  // Registered here rather than in ChartPanel because compare mode mounts two
  // panels — two listeners would toggle twice and cancel each other out.
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey) {
        return; // leave Cmd+L (address bar) and friends alone
      }
      if (event.key !== "l" && event.key !== "L") {
        return;
      }
      const target = event.target as HTMLElement | null;
      const tagName = target?.tagName ?? "";
      if (tagName === "INPUT" || tagName === "TEXTAREA" || tagName === "SELECT" || target?.isContentEditable) {
        return;
      }
      event.preventDefault();
      setChartStyle((prev) => (prev === "hlc" ? "candles" : "hlc"));
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  useEffect(() => {
    if (!chartOpen) {
      chartNavigationSymbolsRef.current = null;
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (compareModeRef.current) {
          setCompareMode(false);
          setActivePane("A");
          return;
        }
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

  /** Run the Total Scanner combination through the normal scan pipeline. */
  const handleRunTotalScan = () => {
    const modes = collectLeafModes(totalScannerTree);
    if (modes.length === 0) {
      setTotalScannerError("Add at least one scanner to the combination.");
      return;
    }
    if (modes.includes("custom-scan") && !hasAppliedFiltersOnce) {
      setTotalScannerError(
        "Custom Scanner is in this combination but has no filters applied yet — open Custom Scanner, apply your filters, then run this again.",
      );
      return;
    }
    setTotalScannerError(null);
    setActivePage("screener");
    setActiveScanner("total-scanner");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setScannerRunNonce((current) => current + 1);
  };

  /** Stage 3: unified "Run Scanner" handler for the new ScannerHeader CTA. */
  const handleRunActiveScanner = () => {
    switch (activeScanner) {
      case "custom-scan":
        handleApplyCustomScan();
        return;
      case "near-pivot":
        handleApplyNearPivotScan();
        return;
      case "pull-backs":
        handleApplyPullBackScan();
        return;
      case "returns":
        handleApplyReturnsScan();
        return;
      case "consolidating":
        handleApplyConsolidatingScan();
        return;
      case "demand-zone":
        handleApplyDemandZoneScan();
        return;
      case "minervini-1m":
        handleApplyMinervini1mScan();
        return;
      case "minervini-5m":
        handleApplyMinervini5mScan();
        return;
      case "positive-earnings":
        handleApplyPositiveEarningsScan();
        return;
      default:
        // ipo / ema-expansion / contraction / gap-up-openers — bump nonce to refetch
        setActivePage("screener");
        setScanLoading(true);
        setScanResults(null);
        setScanSectorSummaries([]);
        setScanSectorSummariesLoading(false);
        setScannerRunNonce((current) => current + 1);
    }
  };

  /** Stage 3: rename the currently-active saved scanner preset. */
  const handleRenameActiveSavedScanner = (newName: string) => {
    if (!activeSavedScannerId) return;
    setSavedScanners((current) =>
      current.map((preset) =>
        preset.id === activeSavedScannerId ? { ...preset, name: newName } : preset,
      ),
    );
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

  const handleApplyDemandZoneScan = () => {
    const nextFilters = normalizeDemandZoneFilters(demandZoneFilters);
    setActivePage("screener");
    setActiveScanner("demand-zone");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setDemandZoneFilters(nextFilters);
    setAppliedDemandZoneFilters(nextFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetDemandZoneScan = () => {
    setActivePage("screener");
    setActiveScanner("demand-zone");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setDemandZoneFilters(DEFAULT_DEMAND_ZONE_FILTERS);
    setAppliedDemandZoneFilters(DEFAULT_DEMAND_ZONE_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handleApplyMomentumBurstScan = () => {
    setActivePage("screener");
    setActiveScanner("momentum-burst");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setAppliedMomentumBurstFilters(momentumBurstFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetMomentumBurstScan = () => {
    setActivePage("screener");
    setActiveScanner("momentum-burst");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setMomentumBurstFilters(DEFAULT_MOMENTUM_BURST_REQUEST);
    setAppliedMomentumBurstFilters(DEFAULT_MOMENTUM_BURST_REQUEST);
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

  const handleApplyPositiveEarningsScan = () => {
    setActivePage("screener");
    setActiveScanner("positive-earnings");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setAppliedPositiveEarningsFilters(positiveEarningsFilters);
    setScannerRunNonce((current) => current + 1);
  };

  const handleResetPositiveEarningsScan = () => {
    setActivePage("screener");
    setActiveScanner("positive-earnings");
    setScanLoading(true);
    setScanResults(null);
    setScanSectorSummaries([]);
    setScanSectorSummariesLoading(false);
    setPositiveEarningsFilters(DEFAULT_POSITIVE_EARNINGS_FILTERS);
    setAppliedPositiveEarningsFilters(DEFAULT_POSITIVE_EARNINGS_FILTERS);
    setScannerRunNonce((current) => current + 1);
  };

  const handlePickSymbol = (symbol: string) => {
    chartNavigationSymbolsRef.current = null;
    setSelectedSymbol(symbol);
    setChartOpen(true);
  };

  const handleScanVisibleOrderChange = useCallback((symbols: string[]) => {
    setScanVisibleOrder((current) =>
      current.length === symbols.length && current.every((sym, idx) => sym === symbols[idx])
        ? current
        : symbols,
    );
  }, []);

  const handlePrefetchSymbol = (symbol: string) => {
    if (!symbol || symbol === selectedSymbolRef.current) {
      return;
    }
    if (timeframe !== "1D" && timeframe !== "1W") {
      return;
    }
    // Hover-intent gate: sweeping the cursor across a results table must not
    // fire a request per row — only the row the cursor rests on (160ms) is
    // prefetched, and at most two hover prefetches run at a time so the
    // backend's scan/chart endpoints never get starved by prefetch traffic.
    if (hoverPrefetchTimerRef.current !== null) {
      window.clearTimeout(hoverPrefetchTimerRef.current);
      hoverPrefetchTimerRef.current = null;
    }
    hoverPrefetchTimerRef.current = window.setTimeout(() => {
      hoverPrefetchTimerRef.current = null;
      if (hoverPrefetchInFlightRef.current >= 2) {
        return;
      }
      const cacheKey = buildChartCacheKey(activeMarket, symbol, timeframe);
      if (prewarmingChartPromisesRef.current.has(cacheKey)) {
        return;
      }
      if (readCachedChart(activeMarket, symbol, timeframe)) {
        return;
      }
      const request = getChart(symbol, timeframe, activeMarket);
      prewarmingChartPromisesRef.current.set(cacheKey, request);
      hoverPrefetchInFlightRef.current += 1;
      void (async () => {
        try {
          const payload = await request;
          if (payload.symbol === symbol && payload.timeframe === timeframe) {
            storeCachedChart(activeMarket, symbol, timeframe, payload);
          }
        } catch {
          // Hover prefetch is best-effort; explicit clicks will retry on demand.
        } finally {
          prewarmingChartPromisesRef.current.delete(cacheKey);
          hoverPrefetchInFlightRef.current -= 1;
        }
      })();
    }, 160);
  };

  const handleLogPlanToJournal = (item: ScanMatch) => {
    const plan = item.momentum_burst;
    setJournalAddRequest({
      symbol: item.symbol,
      suggestedPrice: plan?.entry ?? item.last_price,
      suggestedStopLoss: plan?.stop ?? undefined,
      setup: plan?.tag ? `Momentum Burst — ${plan.tag}` : "Momentum Burst",
    });
    setActivePage("journal");
  };

  const handleJournalOpenSymbolChart = (symbol: string) => {
    const stripped = normalizeJournalChartSymbol(symbol);
    if (!stripped) return;
    const match = findUniverseMatch(stripped);
    handlePickSymbol(match?.symbol ?? stripped);
  };

  // Open the full chart modal for a symbol AND arm up/down navigation through
  // the supplied list (Markets page: focus / leaders / sector-breakout lists).
  const handleOpenChartWithList = (symbol: string, symbols: string[]) => {
    const resolve = (s: string) => findUniverseMatch(normalizeJournalChartSymbol(s) || s)?.symbol ?? s;
    const resolved = symbols.map(resolve).filter(Boolean);
    setSelectedSymbol(resolve(symbol));
    setChartOpen(true);
    // Set AFTER the state updates above (handlePickSymbol clears this ref, so
    // we intentionally do not call it) — arms arrow-key stepping over the list.
    chartNavigationSymbolsRef.current = resolved.length ? resolved : null;
  };

  const handleChartAddToJournal = (symbol: string, suggestedPrice?: number) => {
    const normalizedSymbol = normalizeJournalChartSymbol(symbol);
    if (!normalizedSymbol) {
      return;
    }
    setJournalAddRequest({ symbol: normalizedSymbol, suggestedPrice });
    setActivePage("journal");
    setChartOpen(false);
  };

  const handleSellMarkerClick = (symbol: string, exitDate: string) => {
    const normalizedSymbol = normalizeJournalChartSymbol(symbol);
    if (!normalizedSymbol || !exitDate) return;
    setTradeReviewContext({ symbol: normalizedSymbol, exitDate });
  };

  const handlePickSymbolWithContext = (symbol: string, contextSymbols: string[]) => {
    const scoped = Array.from(new Set(contextSymbols.filter(Boolean)));
    chartNavigationSymbolsRef.current = scoped.length > 0 ? scoped : null;
    setSelectedSymbol(symbol);
    setChartOpen(true);
  };

  // Mouse-clickable equivalent of the ArrowUp/ArrowDown chart navigation:
  // step to the previous (-1) / next (+1) symbol in the current chart's context
  // list, wrapping around. Mirrors the keydown handler's list resolution.
  const stepChartSymbol = (direction: 1 | -1, pane: "A" | "B" = "A") => {
    // Same priority as the keydown handler: armed list → page-visible list →
    // group-sidebar members (last resort). The group override must not beat the
    // scanner/page list.
    const symbols = chartNavigationSymbolsRef.current?.length
      ? chartNavigationSymbolsRef.current
      : pageVisibleSymbolsRef.current?.length
        ? pageVisibleSymbolsRef.current
        : groupNavOverrideRef.current?.length
          ? groupNavOverrideRef.current
          : [];
    const usePaneB = pane === "B";
    const currentSymbol = usePaneB ? paneBSymbolRef.current : selectedSymbolRef.current;
    if (!symbols?.length || !currentSymbol) {
      return;
    }
    const currentIndex = symbols.indexOf(currentSymbol);
    if (currentIndex === -1) {
      return;
    }
    const nextSymbol = symbols[(currentIndex + direction + symbols.length) % symbols.length];
    if (!nextSymbol || nextSymbol === currentSymbol) {
      return;
    }
    if (usePaneB) {
      setPaneBSymbol(nextSymbol);
    } else {
      setSelectedSymbol(nextSymbol);
      setChartOpen(true);
    }
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

  const handleCreateWatchlist = (name: string, initialSymbol?: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      return;
    }

    const symbol = initialSymbol?.trim().toUpperCase();
    const nextWatchlist: LocalWatchlist = {
      id: buildLocalId(),
      name: trimmed,
      color: DEFAULT_WATCHLIST_COLORS[watchlists.length % DEFAULT_WATCHLIST_COLORS.length],
      symbols: symbol ? [symbol] : [],
    };
    setWatchlists((current) => {
      if (!symbol) {
        return [...current, nextWatchlist];
      }
      return [
        ...current.map((watchlist) => ({
          ...watchlist,
          symbols: watchlist.symbols.filter((item) => item !== symbol),
        })),
        nextWatchlist,
      ];
    });
    setActiveWatchlistId(nextWatchlist.id);
    setActivePage("watchlists");
  };

  const handleRenameWatchlist = (watchlistId: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      return;
    }
    setWatchlists((current) =>
      current.map((watchlist) => (watchlist.id === watchlistId ? { ...watchlist, name: trimmed } : watchlist)),
    );
  };

  const handleDeleteWatchlist = (watchlistId: string) => {
    // Deleting a watchlist was silent and irreversible — the one place the app
    // could actually lose your data. Capture the list AND its position so Undo
    // restores it where it was, not appended to the end.
    //
    // Read from state BEFORE the setter: a setWatchlists updater runs later, so
    // capturing inside it leaves this scope with null and no toast is shown.
    const removedIndex = watchlists.findIndex((watchlist) => watchlist.id === watchlistId);
    const restored = watchlists[removedIndex] ?? null;
    const previousActiveId = activeWatchlistId;

    setWatchlists((current) => {
      const remaining = current.filter((watchlist) => watchlist.id !== watchlistId);
      if (activeWatchlistId === watchlistId) {
        setActiveWatchlistId(remaining[0]?.id ?? null);
      }
      return remaining;
    });

    if (!restored) return;
    const count = restored.symbols.length;
    showToast(
      `Deleted "${restored.name}"${count ? ` and its ${count} symbol${count === 1 ? "" : "s"}` : ""}.`,
      {
        tone: "danger",
        onUndo: () => {
          setWatchlists((current) => {
            if (current.some((watchlist) => watchlist.id === restored.id)) return current;
            const next = [...current];
            next.splice(removedIndex < 0 ? next.length : removedIndex, 0, restored);
            return next;
          });
          setActiveWatchlistId(previousActiveId);
        },
      },
    );
  };

  const handleSetWatchlistColor = (watchlistId: string, color: string) => {
    const normalizedColor = normalizeWatchlistColor(color, DEFAULT_WATCHLIST_COLORS[0]);
    setWatchlists((current) =>
      current.map((watchlist) => (watchlist.id === watchlistId ? { ...watchlist, color: normalizedColor } : watchlist)),
    );
  };

  const handleAddToWatchlist = (watchlistId: string, symbol: string) => {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      return;
    }
    setWatchlists((current) => {
      if (!current.some((watchlist) => watchlist.id === watchlistId)) {
        return current;
      }
      // Exclusive membership: a symbol lives in exactly one watchlist.
      // Adding to the target removes it from every other watchlist so
      // "Move here" actually moves instead of duplicating.
      return current.map((watchlist) => {
        if (watchlist.id === watchlistId) {
          if (watchlist.symbols.includes(normalizedSymbol)) {
            return watchlist;
          }
          return { ...watchlist, symbols: [...watchlist.symbols, normalizedSymbol] };
        }
        if (!watchlist.symbols.includes(normalizedSymbol)) {
          return watchlist;
        }
        return { ...watchlist, symbols: watchlist.symbols.filter((item) => item !== normalizedSymbol) };
      });
    });
  };

  // Bulk-import handler. Accepts free-form text where each line / token is
  // one of: "NSE:RELIANCE", "BSE:RELIANCE", "NSE/BSE:RELIANCE", or just
  // "RELIANCE". Splits on commas, semicolons, whitespace, and newlines so
  // the user can paste from a broker watchlist export, a Tijori share, etc.
  // Imported symbols are moved into the target watchlist — they're removed
  // from any other watchlist they're currently in to keep memberships
  // exclusive.
  const handleImportToWatchlist = (watchlistId: string, raw: string): { added: number; duplicates: number } => {
    const normalizedSymbols = parseImportedSymbols(raw);
    if (normalizedSymbols.length === 0) {
      return { added: 0, duplicates: 0 };
    }
    let added = 0;
    let duplicates = 0;
    setWatchlists((current) => {
      if (!current.some((watchlist) => watchlist.id === watchlistId)) {
        return current;
      }
      const importedSet = new Set(normalizedSymbols);
      return current.map((watchlist) => {
        if (watchlist.id !== watchlistId) {
          if (!watchlist.symbols.some((item) => importedSet.has(item))) {
            return watchlist;
          }
          return { ...watchlist, symbols: watchlist.symbols.filter((item) => !importedSet.has(item)) };
        }
        const existing = new Set(watchlist.symbols);
        const next = [...watchlist.symbols];
        for (const symbol of normalizedSymbols) {
          if (existing.has(symbol)) {
            duplicates += 1;
            continue;
          }
          existing.add(symbol);
          next.push(symbol);
          added += 1;
        }
        return added > 0 ? { ...watchlist, symbols: next } : watchlist;
      });
    });
    return { added, duplicates };
  };

  const handleRemoveFromWatchlist = (watchlistId: string, symbol: string) => {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      return;
    }

    // Remember where it sat so Undo puts it back in place, not at the end.
    const source = watchlists.find((watchlist) => watchlist.id === watchlistId);
    const previousIndex = source?.symbols.indexOf(normalizedSymbol) ?? -1;
    if (previousIndex >= 0) {
      showToast(`Removed ${normalizedSymbol} from "${source?.name ?? "watchlist"}".`, {
        tone: "danger",
        onUndo: () => {
          setWatchlists((current) =>
            current.map((watchlist) => {
              if (watchlist.id !== watchlistId) return watchlist;
              if (watchlist.symbols.includes(normalizedSymbol)) return watchlist;
              const symbols = [...watchlist.symbols];
              symbols.splice(Math.min(previousIndex, symbols.length), 0, normalizedSymbol);
              return { ...watchlist, symbols };
            }),
          );
        },
      });
    }

    setWatchlists((current) =>
      current.map((watchlist) =>
        watchlist.id === watchlistId
          ? { ...watchlist, symbols: watchlist.symbols.filter((item) => item !== normalizedSymbol) }
          : watchlist,
      ),
    );
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
        if (watchlist.id === toWatchlistId) {
          const merged = Array.from(new Set([...watchlist.symbols, ...normalizedSymbols]));
          return {
            ...watchlist,
            symbols: merged,
          };
        }

        // Exclusive membership: strip the moved symbols from every other
        // watchlist (source plus any stragglers from pre-exclusivity data).
        if (!watchlist.symbols.some((item) => normalizedSymbols.includes(item))) {
          return watchlist;
        }
        return {
          ...watchlist,
          symbols: watchlist.symbols.filter((item) => !normalizedSymbols.includes(item)),
        };
      });
    });
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

  // Searching a symbol outside the filtered universe (e.g. a microcap below
  // the market-cap floor) falls back to the raw NSE symbol — the chart
  // endpoint accepts any symbol, and a "no data" chart beats a silent no-op.
  const rawSearchSymbol = (query: string): string | null => {
    const cleaned = query.trim().toUpperCase().replace(/[^A-Z0-9.&-]/g, "");
    return cleaned.length >= 2 && cleaned.length <= 20 ? cleaned : null;
  };

  const handleChartSearchSubmit = (query: string) => {
    const match = findUniverseMatch(query);
    const symbol = match?.symbol ?? rawSearchSymbol(query);
    if (!symbol) {
      return;
    }
    handlePickSymbol(symbol);
  };

  const handlePaneSearchSubmit = (pane: "A" | "B", query: string) => {
    const match = findUniverseMatch(query);
    const symbol = match?.symbol ?? rawSearchSymbol(query);
    if (!symbol) return;
    if (pane === "B" && compareMode) {
      setPaneBSymbol(symbol);
      setActivePane("B");
    } else {
      setSelectedSymbol(symbol);
      setChartOpen(true);
      setActivePane("A");
    }
  };

  const handleToggleCompareMode = () => {
    if (compareMode) {
      setCompareMode(false);
      setActivePane("A");
      return;
    }
    const context = activeChartGroupContext;
    let leaderSymbol: string | null = null;
    if (context && context.members.length > 0) {
      const sel = (selectedSymbol ?? "").toUpperCase();
      leaderSymbol =
        context.members.find((member) => member.symbol.toUpperCase() !== sel)?.symbol ??
        context.members[0]?.symbol ??
        null;
    }
    if (!leaderSymbol) return;
    setPaneBSymbol(leaderSymbol);
    setCompareMode(true);
    setActivePane("B");
  };

  const handleGroupWidgetSelect = (symbol: string) => {
    if (compareMode && activePane === "B") {
      setPaneBSymbol(symbol);
    } else {
      setSelectedSymbol(symbol);
      setActivePane("A");
    }
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
    const shouldRefreshGroups = activePage === "home" || activePage === "groups" || groupsData === null;

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

      const [dashboardResult, groupsResult] = await Promise.allSettled([
        getDashboard(activeMarket),
        shouldRefreshGroups ? getIndustryGroups(activeMarket) : Promise.resolve(groupsData),
      ]);

      const dashboardPayload = dashboardResult.status === "fulfilled" ? dashboardResult.value : null;
      const groupsPayload = groupsResult.status === "fulfilled" ? groupsResult.value : null;

      if (!dashboardPayload && !groupsPayload) {
        throw new Error(
          settledError(dashboardResult) ?? settledError(groupsResult) ?? "Failed to refresh market data",
        );
      }

      if (dashboardPayload) {
        setDashboard(dashboardPayload);
      }
      if (groupsPayload) {
        setGroupsData(groupsPayload);
        setUniverseCatalog(buildUniverseCatalogFromIndustryGroups(groupsPayload));
      } else if (activePage !== "groups" && groupsData) {
        void getIndustryGroups(activeMarket)
          .then((payload) => {
            setGroupsData(payload);
            setUniverseCatalog(buildUniverseCatalogFromIndustryGroups(payload));
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
        const scanPayload = await requestActiveScannerResults(scanArrangementMode === "sector");
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
        nextSelectedSymbol = dashboardPayload?.top_gainers[0]?.symbol ?? firstSymbolFromIndustryGroups(groupsPayload) ?? null;
        setSelectedSymbol(nextSelectedSymbol);
      }

      if (nextSelectedSymbol) {
        const shouldForceChartReload =
          nextSelectedSymbol !== selectedSymbol ||
          refreshMode === "historical-refresh";
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

      const loadWarnings = [settledError(dashboardResult), settledError(groupsResult)].filter(Boolean);

      if (refreshPayload) {
        const rp = refreshPayload as RefreshResponse;
        const refreshSucceeded = rp.refresh_mode === "historical-refresh" || rp.refresh_mode === "live-refresh" || rp.refresh_mode === "cached-current";
        const clearableAutoRefresh = refreshSucceeded;

        if (refreshSucceeded && loadWarnings.length === 0) {
          setError(null);
        } else if (source === "manual") {
          const snapshotLabel = rp.snapshot_updated_at
            ? new Date(rp.snapshot_updated_at).toLocaleString()
            : "the latest cached snapshot";
          setError(rp.message ?? loadWarnings[0] ?? `Showing cached market data from ${snapshotLabel}.`);
        } else if (source === "auto") {
          if (dashboardPayload || groupsPayload) {
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
        if (dashboardPayload || groupsPayload) {
          setError(null);
        }
      }
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Failed to refresh market data");
    } finally {
      setImprovingRsLoading(false);
      refreshingRef.current = false;
      setRefreshing(false);
    }
  };

  handleRefreshRef.current = handleRefresh;
  const hasBootstrappedData = Boolean(dashboard || groupsData);

  const handleRetryConnection = () => {
    setError(null);
    setChartError(null);
    if (!hasBootstrappedData || loading) {
      setBootstrapNonce((current) => current + 1);
      return;
    }
    void handleRefreshRef.current("manual");
    if (selectedSymbol) {
      void loadChartForSelection(selectedSymbol, timeframe, activeMarket, {
        forceNetwork: true,
        preferCached: true,
      }).catch((loadError) => {
        setChartError(loadError instanceof Error ? loadError.message : "Failed to load chart");
      });
    }
  };

  useEffect(() => {
    if (loading || refreshing || !dashboard) {
      return;
    }

    const refreshSchedule = getAutoRefreshSchedule(new Date(), activeMarket);

    const generatedAtMs = new Date(dashboard.generated_at).getTime();
    const ageMs = Date.now() - generatedAtMs;
    if (Number.isFinite(generatedAtMs) && ageMs <= 10 * 60 * 1000) {
      return;
    }

    const attemptKey = `${activeMarket}:${dashboard.generated_at}`;
    if (autoRefreshAttemptKeyRef.current[activeMarket] === attemptKey) {
      return;
    }
    autoRefreshAttemptKeyRef.current[activeMarket] = attemptKey;

    const timeoutId = window.setTimeout(() => {
      void handleRefreshRef.current("auto");
    }, refreshSchedule.delayMs);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [activeMarket, dashboard, loading, refreshing]);

  useEffect(() => {
    if (loading || savedScanners.length === 0) {
      return;
    }

    const stalePresets = savedScanners.filter((preset) => !savedScannerFreshToday(preset.lastUpdatedAt));
    if (stalePresets.length === 0) {
      return;
    }

    void syncSavedScanners(stalePresets);
  }, [loading]);

  const autoRefreshSchedule = getAutoRefreshSchedule(new Date(clockTick), activeMarket);
  const navSearchSuggestions = buildSymbolSuggestions(universeCatalog, deferredNavSearchQuery, 80);
  const brandEyebrow = "NSE / BSE Stock Scanner";
  const floorMetricLabel = "Floor";
  const floorMetricValue = `${dashboard?.market_cap_min_crore ?? 800} Cr+`;

  return (
    <div className="app-shell app-shell-simple">
      {/* Liquid-glass refraction filter — referenced by backdrop-filter on the
          nav/glass chrome (Chromium/Safari). Subtle organic displacement of the
          background image behind the panel. */}
      <svg className="liquid-glass-defs" width="0" height="0" aria-hidden="true" focusable="false">
        <filter id="liquidGlass" x="-20%" y="-20%" width="140%" height="140%" colorInterpolationFilters="sRGB">
          <feTurbulence type="fractalNoise" baseFrequency="0.012 0.012" numOctaves="2" seed="7" result="noise" />
          <feGaussianBlur in="noise" stdDeviation="2" result="blurNoise" />
          <feDisplacementMap in="SourceGraphic" in2="blurNoise" scale="16" xChannelSelector="R" yChannelSelector="G" />
        </filter>
      </svg>
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
            className={activePage === "markets" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("markets")}
          >
            Markets
          </button>

          <button
            type="button"
            className={activePage === "live" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("live")}
          >
            Live
          </button>

          <button
            type="button"
            className={activePage === "journal" ? "nav-button primary" : "nav-button ghost"}
            onClick={() => setActivePage("journal")}
            onMouseEnter={() => prefetchPageModules("journal")}
            onFocus={() => prefetchPageModules("journal")}
          >
            Journal
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
                <span className="search-icon" aria-hidden="true">
                  <SearchIcon size={14} strokeWidth={2.2} />
                </span>
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
                  aria-label="Search symbol or company. Enter opens chart, Shift+Enter jumps to group."
                />
                <span className="nav-search-hint">Enter opens chart. Shift+Enter jumps to the stock&apos;s group.</span>
              </div>
            </div>
            <datalist id="nav-stock-options">
              {navSearchSuggestions.map((item) => (
                <option key={`nav-option-${item.symbol}`} value={item.symbol}>
                  {item.name}
                </option>
              ))}
            </datalist>
          </form>

          <span className="status-pill" title={autoRefreshSchedule.detail}>
            <span className="status-dot" aria-hidden="true" />
            <span>{autoRefreshSchedule.label}</span>
            <span className="status-pill-sep" aria-hidden="true" />
            <strong>{snapshotDateLabel}</strong>
          </span>

          <button
            type="button"
            className={refreshing ? "icon-btn is-spinning" : "icon-btn"}
            onClick={() => void handleRefresh("manual")}
            disabled={refreshing}
            title={refreshing ? "Refreshing snapshot…" : "Refresh close snapshot"}
            aria-label="Refresh close snapshot"
          >
            <RefreshCw size={15} strokeWidth={2.2} />
          </button>

          <button
            type="button"
            className="icon-btn"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
            aria-label="Toggle theme"
          >
            {theme === "dark" ? <Sun size={15} strokeWidth={2.2} /> : <Moon size={15} strokeWidth={2.2} />}
          </button>
        </div>
      </header>

      <CommandPalette
        open={commandPaletteOpen}
        onClose={() => setCommandPaletteOpen(false)}
        symbols={universeCatalog.map((item) => ({ symbol: item.symbol, name: item.name }))}
        groups={(groupsData?.groups ?? []).map((group) => ({ id: group.group_id, name: group.group_name, rank: group.rank }))}
        scanners={[
          { mode: "vcp", label: "VCP" },
          { mode: "power-base", label: "Power Base" },
          { mode: "tight-closes", label: "3 Tight Closes" },
          { mode: "bread-butter", label: "Bread & Butter" },
          { mode: "custom-scan", label: "Custom Scanner" },
          { mode: "volume", label: "Volume" },
          { mode: "ipo", label: "IPO" },
          { mode: "gap-up-openers", label: "Gap Up Openers" },
          { mode: "ema-expansion", label: "Expansion" },
          { mode: "contraction", label: "Contraction" },
          { mode: "demand-zone", label: "Demand Zone Scanner" },
          { mode: "momentum-burst", label: "Momentum Burst" },
          { mode: "minervini-1m", label: "Minervini 1 Month" },
          { mode: "minervini-5m", label: "Minervini 5 Months" },
          { mode: "positive-earnings", label: "Positive Earnings" },
          { mode: "improving-rs", label: "52 Week High RS" },
        ]}
        pages={[
          { page: "home", label: "Home" },
          { page: "screener", label: "Screener" },
          { page: "groups", label: "Groups" },
          { page: "watchlists", label: "Watchlists" },
          { page: "journal", label: "Journal" },
        ]}
        onPickSymbol={handlePickSymbol}
        onPickScanner={(mode) => {
          setActivePage("screener");
          handleScannerModeChange(mode as ScreenerMode);
        }}
        onPickGroup={(groupId) => {
          void openGroupsView({ groupId });
        }}
        onPickPage={(page) => setActivePage(page as AppPage)}
      />

      <AppStatusBanners
        error={error}
        hasBootstrappedData={hasBootstrappedData}
        loading={loading}
        market={activeMarket}
        onRetry={handleRetryConnection}
      />

      {/* India EOD status now lives inside the top-nav status-pill */}

      <main className="workspace">

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
              groups={groupsData}
              snapshotDateLabel={snapshotDateLabel}
              snapshotTimeLabel={snapshotTimeLabel}
              onPickSymbol={handlePickSymbol}
              onOpenGroups={(options) => {
                void openGroupsView(options);
              }}
            />
          </Suspense>
        ) : null}
        {!loading && activePage === "live" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <LivePanel watchlists={watchlists} onOpenSymbolChart={handleJournalOpenSymbolChart} />
          </Suspense>
        ) : null}
        {!loading && activePage === "markets" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <MarketsPanel onOpenSymbolChart={handleJournalOpenSymbolChart} onOpenChartWithList={handleOpenChartWithList} xpBreadth={dashboard?.xp_breadth ?? null} />
          </Suspense>
        ) : null}
        {!loading && activePage === "journal" ? (
          <Suspense fallback={<DeferredPanelPlaceholder />}>
            <TradeJournalPanel
              market={activeMarket}
              addRequest={journalAddRequest}
              onAddRequestHandled={() => setJournalAddRequest(null)}
              onOpenSymbolChart={handleJournalOpenSymbolChart}
              groupsData={groupsData}
              xpBreadth={dashboard?.xp_breadth ?? null}
            />
          </Suspense>
        ) : null}
        {!loading && activePage !== "home" && activePage !== "journal" && activePage !== "live" && activePage !== "markets" ? (
          <Suspense fallback={<DeferredPanelPlaceholder compact />}>
            <>
            <section className="page-metrics-strip">
              {activePage === "screener" ? (
                <>
                  <div className="metric-card">
                    <span>{activeScanner === "improving-rs" ? "52W High RS" : "Filtered Stocks"}</span>
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
                <span>{activeMarket === "india" ? "EOD As Of" : "Snapshot Date"}</span>
                <strong>{snapshotDateLabel}</strong>
              </div>
              <div className="metric-card">
                <span>Published</span>
                <strong>{snapshotTimeLabel}</strong>
              </div>
            </section>

            <section
              className={
                activePage === "screener"
                  ? "screener-page-grid"
                  : activePage === "watchlists"
                    ? "workspace-grid workspace-grid-sector workspace-grid-watchlists"
                    : activePage === "groups"
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
                      "volume": activeScanner === "volume" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "volume" ? scanResults.total_hits : 0,
                      "ipo": activeScanner === "ipo" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "ipo" ? scanResults.total_hits : 0,
                      "gap-up-openers": activeScanner === "gap-up-openers" ? scanResults?.total_hits ?? 0 : 0,
                      "ema-expansion": activeScanner === "ema-expansion" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "ema-expansion" ? scanResults.total_hits : 0,
                      "contraction": activeScanner === "contraction" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "contraction" ? scanResults.total_hits : 0,
                      "near-pivot": activeScanner === "near-pivot" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "near-pivot" ? scanResults.total_hits : 0,
                      "pull-backs": activeScanner === "pull-backs" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "pull-backs" ? scanResults.total_hits : 0,
                      "returns": activeScanner === "returns" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "returns" ? scanResults.total_hits : 0,
                      "consolidating": activeScanner === "consolidating" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "consolidating" ? scanResults.total_hits : 0,
                      "demand-zone": activeScanner === "demand-zone" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "demand-zone" ? scanResults.total_hits : 0,
                      "minervini-1m": activeScanner === "minervini-1m" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "minervini-1m" ? scanResults.total_hits : 0,
                      "minervini-5m": activeScanner === "minervini-5m" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "minervini-5m" ? scanResults.total_hits : 0,
                      "positive-earnings": activeScanner === "positive-earnings" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "positive-earnings" ? scanResults.total_hits : 0,
                      "episodic-pivot": activeScanner === "episodic-pivot" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "episodic-pivot" ? scanResults.total_hits : 0,
                      "rs-line-leads": activeScanner === "rs-line-leads" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "rs-line-leads" ? scanResults.total_hits : 0,
                      "fresh-stage2": activeScanner === "fresh-stage2" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "fresh-stage2" ? scanResults.total_hits : 0,
                      "high-tight-flag": activeScanner === "high-tight-flag" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "high-tight-flag" ? scanResults.total_hits : 0,
                      "vcp": activeScanner === "vcp" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "vcp" ? scanResults.total_hits : 0,
                      "tight-closes": activeScanner === "tight-closes" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "tight-closes" ? scanResults.total_hits : 0,
                      "power-base": activeScanner === "power-base" ? scanResults?.total_hits ?? 0 : scanResults?.scan.id === "power-base" ? scanResults.total_hits : 0,
                      "improving-rs": improvingRsData?.total_hits ?? 0,
                    }}
                    savedScanners={savedScanners.map((preset) => ({
                      id: preset.id,
                      name: preset.name,
                      mode: preset.mode,
                      lastMatchCount: preset.lastMatchCount,
                    }))}
                    activeSavedScannerId={activeSavedScannerId}
                    onLoadSavedScanner={handleLoadSavedScannerById}
                    onDeleteSavedScanner={handleDeleteSavedScanner}
                  />

                  <div className="screener-main-stack">
                    {activeScanner !== "improving-rs" ? (
                      <section className="scanner-settings-shell">
                        {(() => {
                          const scannerTitle =
                            activeScanner === "total-scanner"
                              ? "Total Scanner"
                              : activeScanner === "custom-scan"
                              ? "Custom Screener"
                              : activeScanner === "volume"
                                ? "Volume"
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
                                            : activeScanner === "demand-zone"
                                              ? "Demand Zone Scanner"
                                              : activeScanner === "minervini-1m"
                                                ? "Minervini 1 Month"
                                                : activeScanner === "minervini-5m"
                                                  ? "Minervini 5 Months"
                                                  : activeScanner === "positive-earnings"
                                                    ? "Positive Earnings"
                                                    : activeScanner === "episodic-pivot"
                                                      ? "Episodic Pivot"
                                                      : activeScanner === "rs-line-leads"
                                                        ? "RS Line Leads"
                                                        : activeScanner === "fresh-stage2"
                                                          ? "Fresh Stage 2"
                                                          : activeScanner === "high-tight-flag"
                                                            ? "High Tight Flag"
                                                            : activeScanner === "vcp"
                                                              ? "VCP"
                                                              : activeScanner === "tight-closes"
                                                                ? "3 Tight Closes"
                                                                : activeScanner === "power-base"
                                                                  ? "Power Base"
                                                                  : "Pull Backs";
                          const scannerDesc =
                            activeScanner === "total-scanner"
                              ? `Scanners combined with AND / OR — ${describeTotalScanner(totalScannerTree)}.`
                              : activeScanner === "custom-scan"
                              ? "Define your own universe filters and RS thresholds."
                              : activeScanner === "volume"
                                ? "Stocks that pushed a new volume high in the last ~1 month, newest on top — each badged Monthly / Quarterly / Half-yearly / Yearly by the longest window it cleared."
                              : activeScanner === "ipo"
                                ? "Recently listed stocks from the last 12 months, ranked by recency and strength."
                                : activeScanner === "gap-up-openers"
                                  ? "Filter stocks by opening gap percentage."
                                  : activeScanner === "ema-expansion"
                                    ? "Built-in expansion scan using price, RVOL, and same-day liquidity rules."
                                    : activeScanner === "contraction"
                                      ? "Built-in contraction scan for tight 3-day pull-ins above the 50D EMA."
                                      : activeScanner === "near-pivot"
                                        ? "Find high-RS stocks tightening close to their pivot zone."
                                        : activeScanner === "returns"
                                          ? "Scan for stocks by return range with optional confirmation filters."
                                          : activeScanner === "consolidating"
                                            ? "Toggle multi-year-high and long-base filters independently."
                                            : activeScanner === "demand-zone"
                                              ? "Stage 2 stocks within 3% of strong daily or weekly demand-zone lows."
                                              : activeScanner === "minervini-1m"
                                                ? "Minervini 1 Month trend-template scan with an optional liquidity filter."
                                                : activeScanner === "minervini-5m"
                                                  ? "Minervini 5 Months trend-template scan with an optional liquidity filter."
                                                  : activeScanner === "positive-earnings"
                                                    ? "Stocks with a strong confirmed reaction to the latest quarterly result in the last 60 days: top-quartile close, +1% gap up, 2x volume, +10% over 5 sessions."
                                                    : activeScanner === "episodic-pivot"
                                                      ? "Day-one episodic pivots: gap-up ≥4% on 3x+ relative volume out of a flat 20-day base — caught before the follow-through, not after."
                                                      : activeScanner === "rs-line-leads"
                                                        ? "RS rating at a fresh high across 1D/1W/1M while price is still 3–20% below the 52-week high — strength building before the breakout."
                                                        : activeScanner === "fresh-stage2"
                                                          ? "New entrants to the Minervini 5M trend template vs recent sessions — the delta list, not the standing leaders."
                                                          : activeScanner === "high-tight-flag"
                                                            ? "60%+ pole in ~8 weeks, then a 2–15 session flag no deeper than 25%, listed while still at or under the pivot."
                                                            : activeScanner === "vcp"
                                                              ? "Minervini VCP: 30%+ run-up, 2–18 week base under 30% deep, progressively shallower pullbacks (T1 > T2 > T3), volume dry-up, within 6% of the pivot — with entry, stop, and risk %."
                                                              : activeScanner === "tight-closes"
                                                                ? "3 closes within 1.5% (or 5 within 2.5%) on quiet, drying volume near the highs — the pre-breakout coil before the expansion day."
                                                                : activeScanner === "power-base"
                                                                  ? "A 30%+ first leg (up to ~3 months in the making), now consolidating for 1–8 weeks while keeping most of the move — leaders digesting a completed leg, with entry, stop, and risk %."
                                                                  : "Find strong leaders pulling into the 10- or 20-day EMA on contraction.";
                          const activeSavedPreset =
                            activeSavedScannerId
                              ? savedScanners.find(
                                  (item) =>
                                    item.id === activeSavedScannerId && item.mode === activeScanner,
                                ) ?? null
                              : null;
                          const savableMode = isSavableScannerMode(activeScanner);
                          return (
                            <ScannerHeader
                              title={scannerTitle}
                              description={scannerDesc}
                              resultCount={
                                scanResults && scanResults.scan.id === activeScanner
                                  ? scanResults.total_hits
                                  : null
                              }
                              activeSavedName={activeSavedPreset?.name ?? null}
                              onRenameActiveSaved={handleRenameActiveSavedScanner}
                              settingsOpen={showScannerSettings}
                              onToggleSettings={() => setShowScannerSettings((c) => !c)}
                              onRunScanner={handleRunActiveScanner}
                              loading={scanLoading}
                              isSavable={savableMode}
                              savedExists={Boolean(activeSavedPreset)}
                              onSaveScanner={() => void handleSaveCurrentScanner()}
                              saving={savingScanner}
                            />
                          );
                        })()}

                        {showScannerSettings
                          ? activeScanner === "volume"
                            ? (
                              <div className="scanner-settings-note">
                                <strong>Volume push list</strong>
                                <span>One rolling list of stocks that pushed a new volume high in the last ~1 month. Each is badged with the longest window it cleared — Monthly, Quarterly, Half-yearly, or Yearly — and the newest pushes sit on top, so stocks stay listed for about a month to track their post-push behavior.</span>
                              </div>
                            )
                          : activeScanner === "ipo"
                            ? (
                              <div className="scanner-settings-note">
                                <strong>Built-in scan</strong>
                                <span>The IPO screener uses the backend listing-date rule and does not have extra filters yet.</span>
                              </div>
                            )
                            : activeScanner === "ema-expansion"
                              ? (
                                <Panel
                                  title="Expansion thresholds"
                                  subtitle="Stocks with day change ≥ X% AND 50-day RVOL > Yx, kept for the last 30 market sessions."
                                  actions={(
                                    <div className="custom-panel-actions">
                                      <button
                                        type="button"
                                        className="nav-button ghost"
                                        onClick={() => {
                                          setAppliedExpansionMinChangePct(expansionMinChangePct);
                                          setAppliedExpansionMinRelativeVolume(expansionMinRelativeVolume);
                                          // Force a refetch even when applied values
                                          // are identical to the previous run.
                                          setScannerRunNonce((current) => current + 1);
                                        }}
                                      >
                                        Apply
                                      </button>
                                      <button
                                        type="button"
                                        className="nav-button ghost"
                                        onClick={() => {
                                          setExpansionMinChangePct(6.5);
                                          setExpansionMinRelativeVolume(3.0);
                                          setAppliedExpansionMinChangePct(6.5);
                                          setAppliedExpansionMinRelativeVolume(3.0);
                                          setScannerRunNonce((current) => current + 1);
                                        }}
                                      >
                                        Reset
                                      </button>
                                    </div>
                                  )}
                                >
                                  <div className="scan-settings-grid" style={{ marginTop: "0.65rem" }}>
                                    <label>
                                      <span>Min Day Change %</span>
                                      <input
                                        type="number"
                                        min={0}
                                        max={100}
                                        step={0.1}
                                        value={expansionMinChangePct}
                                        onChange={(event) => setExpansionMinChangePct(Number(event.target.value) || 0)}
                                      />
                                    </label>
                                    <label>
                                      <span>Min 50-Day RVOL (x)</span>
                                      <input
                                        type="number"
                                        min={0}
                                        max={50}
                                        step={0.1}
                                        value={expansionMinRelativeVolume}
                                        onChange={(event) => setExpansionMinRelativeVolume(Number(event.target.value) || 0)}
                                      />
                                    </label>
                                  </div>
                                </Panel>
                              )
                              : activeScanner === "contraction"
                                ? (
                                  <div className="scanner-settings-note">
                                    <strong>Built-in scan</strong>
                                    <span>The Contraction screener uses the backend contraction rules and does not use custom scanner filters.</span>
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
                              : activeScanner === "demand-zone"
                                ? (
                                  <DemandZoneScannerPanel
                                    filters={demandZoneFilters}
                                    onFiltersChange={setDemandZoneFilters}
                                    onApply={handleApplyDemandZoneScan}
                                    onReset={handleResetDemandZoneScan}
                                  />
                                )
                              : activeScanner === "momentum-burst"
                                ? (
                                  <MomentumBurstScannerPanel
                                    filters={momentumBurstFilters}
                                    onFiltersChange={setMomentumBurstFilters}
                                    onApply={handleApplyMomentumBurstScan}
                                    onReset={handleResetMomentumBurstScan}
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
                              : activeScanner === "total-scanner"
                                ? (
                                  <TotalScannerPanel
                                    tree={totalScannerTree}
                                    onTreeChange={setTotalScannerTree}
                                    onRun={handleRunTotalScan}
                                    loading={scanLoading}
                                    matchCount={scanResults?.scan?.id === "total-scanner" ? scanResults.total_hits : null}
                                    error={totalScannerError}
                                  />
                                )
                              : activeScanner === "positive-earnings"
                                ? (
                                  <PositiveEarningsScannerPanel
                                    filters={positiveEarningsFilters}
                                    onFiltersChange={setPositiveEarningsFilters}
                                    onApply={handleApplyPositiveEarningsScan}
                                    onReset={handleResetPositiveEarningsScan}
                                  />
                                )
                              : (
                                <>
                                  <QueryBuilder
                                    filters={customFilters}
                                    defaults={DEFAULT_CUSTOM_FILTERS}
                                    onFiltersChange={setCustomFilters}
                                    onAddFilter={() => {
                                      const el = document.getElementById("scanner-form-anchor");
                                      if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
                                    }}
                                  />
                                  <div id="scanner-form-anchor" />
                                  <CustomScannerPanel
                                    filters={customFilters}
                                    onFiltersChange={setCustomFilters}
                                    onApply={handleApplyCustomScan}
                                    onReset={handleResetCustomScan}
                                    patternOptions={patternOptions}
                                  />
                                </>
                              )
                          : null}
                      </section>
                    ) : null}

                    {activeScanner === "improving-rs" ? (
                      <ImprovingRsPanel
                        market={activeMarket}
                        data={improvingRsData}
                        loading={improvingRsLoading}
                        onPickSymbol={handlePickSymbol}
                        onRequestAddToWatchlist={setWatchlistPickerSymbol}
                        selectedSymbol={selectedSymbol}
                      />
                    ) : activeScanner === "momentum-burst" ? (
                      <MomentumBurstResults
                        items={visibleScanItems}
                        loading={scanLoading}
                        onPickSymbol={handlePickSymbol}
                        onPrefetchSymbol={handlePrefetchSymbol}
                        onLogPlan={handleLogPlanToJournal}
                        selectedSymbol={selectedSymbol}
                      />
                    ) : (
                      <>
                        {visibleScanItems.length > 0 ? (
                          <ScanDashboard
                            items={visibleScanItems}
                            groupsData={groupsData}
                          />
                        ) : null}
                      <ScanTable
                        market={activeMarket}
                        loading={scanLoading}
                        sectorSummaryLoading={scanSectorSummariesLoading}
                        scan={displayScan}
                        items={visibleScanItems}
                        sectorSummaries={scanSectorSummaries}
                        onPickSymbol={handlePickSymbol}
                        onPrefetchSymbol={handlePrefetchSymbol}
                        onRequestAddToWatchlist={setWatchlistPickerSymbol}
                        selectedSymbol={selectedSymbol}
                        sortMode={resultSortMode}
                        onSortModeChange={setResultSortMode}
                        arrangementMode={scanArrangementMode}
                        onArrangementModeChange={setScanArrangementMode}
                        sectorSortMode={sectorGroupSortMode}
                        onSectorSortModeChange={setSectorGroupSortMode}
                        groupsData={groupsData}
                        onExport={handleExportScanResults}
                        onVisibleOrderChange={handleScanVisibleOrderChange}
                      />
                      {visibleScanItems.length > 0 ? (
                        <ScanFooter
                          loading={scanLoading}
                          items={visibleScanItems}
                          generatedAt={scanResults?.generated_at ?? null}
                          onPickSymbol={handlePickSymbol}
                        />
                      ) : null}
                      </>
                    )}
                  </div>

                  <ChartPanel
                    key={activeChartKey ?? "empty-chart"}
                    market={activeMarket}
                    symbol={selectedSymbol}
                    bars={displayedChart?.bars ?? []}
                    rsLine={displayedChart?.rs_line ?? []}
                    rsLineMarkers={displayedChart?.rs_line_markers ?? []}
                    earningsMarkers={displayedChart?.earnings_markers ?? []}
                    upcomingEarningsDate={displayedChart?.upcoming_earnings_date ?? null}
                    volumeMarkers={displayedChart?.volume_markers ?? []}
                    bandChangeMarkers={displayedChart?.band_change_markers ?? []}
                    bandHistory={displayedChart?.band_history ?? []}
                    tradeMarkers={activeTradeMarkers}
                    onSellMarkerClick={handleSellMarkerClick}
                    summary={displayedChart?.summary ?? null}
                    panelTab={chartPanelTab}
                    onPanelTabChange={setChartPanelTab}
                    chartError={chartError}
                    chartLoading={chartLoading}
                    chartCacheState={chartCacheState}
                    fundamentals={activeFundamentals}
                    fundamentalsLoading={fundamentalsLoading}
                    fundamentalsError={fundamentalsError}
                    groupSummary={activeChartGroupSummary}
                    timeframe={timeframe}
                    onTimeframeChange={handleTimeframeChange}
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
                    onAddToJournal={handleChartAddToJournal}
                    searchOptions={universeCatalog}
                    onSearchSymbol={handleChartSearchSubmit}
                    onOpenGroup={handleOpenChartGroupModal}
                    onRefreshChart={handleChartRefresh}
                    onStepChart={stepChartSymbol}
                    expanded
                  />
                </>
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
                  onRequestAddToWatchlist={setWatchlistPickerSymbol}
                  onPickSymbol={handlePickSymbol}
                  onOpenGroup={(groupId) => void openGroupsView({ groupId })}
                  onOpenScreener={() => setActivePage("screener")}
                  onPrefetchSymbol={handlePrefetchSymbol}
                  onImportSymbols={handleImportToWatchlist}
                  universeItems={universeCatalog}
                  groupsData={groupsData}
                  selectedSymbol={selectedSymbol}
                />
              ) : (
                null
              )}

              {activePage === "groups" || activePage === "watchlists" ? (
                <ChartPanel
                  key={activeChartKey ?? "empty-chart"}
                  market={activeMarket}
                  symbol={selectedSymbol}
                  bars={displayedChart?.bars ?? []}
                  rsLine={displayedChart?.rs_line ?? []}
                  rsLineMarkers={displayedChart?.rs_line_markers ?? []}
                  earningsMarkers={displayedChart?.earnings_markers ?? []}
                    upcomingEarningsDate={displayedChart?.upcoming_earnings_date ?? null}
                  volumeMarkers={displayedChart?.volume_markers ?? []}
                    bandChangeMarkers={displayedChart?.band_change_markers ?? []}
                    bandHistory={displayedChart?.band_history ?? []}
                  tradeMarkers={activeTradeMarkers}
                  onSellMarkerClick={handleSellMarkerClick}
                  summary={displayedChart?.summary ?? null}
                  panelTab={chartPanelTab}
                  onPanelTabChange={setChartPanelTab}
                  chartError={chartError}
                  chartLoading={chartLoading}
                  chartCacheState={chartCacheState}
                  fundamentals={activeFundamentals}
                  fundamentalsLoading={fundamentalsLoading}
                  fundamentalsError={fundamentalsError}
                  groupSummary={activeChartGroupSummary}
                  timeframe={timeframe}
                  onTimeframeChange={handleTimeframeChange}
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
                  onRemoveFromWatchlist={
                    activePage === "watchlists" && activeWatchlist
                      ? (symbol) => handleRemoveFromWatchlist(activeWatchlist.id, symbol)
                      : undefined
                  }
                  onAddToJournal={handleChartAddToJournal}
                  searchOptions={universeCatalog}
                  onSearchSymbol={handleChartSearchSubmit}
                  onOpenGroup={handleOpenChartGroupModal}
                  onRefreshChart={handleChartRefresh}
                  onStepChart={stepChartSymbol}
                  expanded={activePage === "groups"}
                />
              ) : null}
            </section>
            </>
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

      {tradeReviewContext ? (
        <Suspense fallback={null}>
          <TradeReviewModal
            symbol={tradeReviewContext.symbol}
            exitDate={tradeReviewContext.exitDate}
            onClose={() => setTradeReviewContext(null)}
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
            {groupWidgetContext ? (
              <button
                type="button"
                className={
                  groupWidgetOpen
                    ? "chart-modal-widget-toggle is-active"
                    : "chart-modal-widget-toggle"
                }
                onClick={() => setGroupWidgetOpen((prev) => !prev)}
                title={groupWidgetOpen ? "Hide group stocks" : "Show group stocks"}
              >
                {groupWidgetOpen ? "Hide Group" : "Show Group"}
              </button>
            ) : null}
            <Suspense fallback={<DeferredPanelPlaceholder compact />}>
              <ChartCompareLayout
                    compareMode={compareMode}
                    layout={compareLayout}
                    activePane={activePane}
                    dividerRatio={compareDividerRatio}
                    onDividerRatioChange={setCompareDividerRatio}
                    onActivePaneChange={setActivePane}
                    paneA={
                      <ChartPanel
                        key={`modal-A-${activeChartKey ?? "empty-chart"}`}
                        market={activeMarket}
                        symbol={selectedSymbol}
                        bars={displayedChart?.bars ?? []}
                        rsLine={displayedChart?.rs_line ?? []}
                        rsLineMarkers={displayedChart?.rs_line_markers ?? []}
                        earningsMarkers={displayedChart?.earnings_markers ?? []}
                    upcomingEarningsDate={displayedChart?.upcoming_earnings_date ?? null}
                        volumeMarkers={displayedChart?.volume_markers ?? []}
                    bandChangeMarkers={displayedChart?.band_change_markers ?? []}
                    bandHistory={displayedChart?.band_history ?? []}
                        tradeMarkers={activeTradeMarkers}
                        onSellMarkerClick={handleSellMarkerClick}
                        summary={displayedChart?.summary ?? null}
                        panelTab={chartPanelTab}
                        onPanelTabChange={setChartPanelTab}
                        chartError={chartError}
                        chartLoading={chartLoading}
                        chartCacheState={chartCacheState}
                        fundamentals={activeFundamentals}
                        fundamentalsLoading={fundamentalsLoading}
                        fundamentalsError={fundamentalsError}
                        groupSummary={activeChartGroupSummary}
                        timeframe={timeframe}
                        onTimeframeChange={handleTimeframeChange}
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
                        onRemoveFromWatchlist={
                          activePage === "watchlists" && activeWatchlist
                            ? (symbol) => handleRemoveFromWatchlist(activeWatchlist.id, symbol)
                            : undefined
                        }
                        onAddToJournal={handleChartAddToJournal}
                        searchOptions={universeCatalog}
                        onSearchSymbol={(query) => handlePaneSearchSubmit("A", query)}
                        onOpenGroup={handleOpenChartGroupModal}
                        onRefreshChart={handleChartRefresh}
                        onStepChart={(direction) => stepChartSymbol(direction, "A")}
                        expanded
                      />
                    }
                    paneB={
                      <ChartPanel
                        key={`modal-B-${paneBChartKey ?? "empty-chart"}`}
                        market={activeMarket}
                        symbol={paneBSymbol}
                        bars={paneBDisplayedChart?.bars ?? []}
                        rsLine={paneBDisplayedChart?.rs_line ?? []}
                        rsLineMarkers={paneBDisplayedChart?.rs_line_markers ?? []}
                        earningsMarkers={paneBDisplayedChart?.earnings_markers ?? []}
                    upcomingEarningsDate={paneBDisplayedChart?.upcoming_earnings_date ?? null}
                        volumeMarkers={paneBDisplayedChart?.volume_markers ?? []}
                        bandChangeMarkers={paneBDisplayedChart?.band_change_markers ?? []}
                        bandHistory={paneBDisplayedChart?.band_history ?? []}
                        tradeMarkers={paneBTradeMarkers}
                        onSellMarkerClick={handleSellMarkerClick}
                        summary={paneBDisplayedChart?.summary ?? null}
                        panelTab={chartPanelTab}
                        onPanelTabChange={setChartPanelTab}
                        chartError={chartBError}
                        chartLoading={chartBLoading}
                        chartCacheState={chartBCacheState}
                        fundamentals={paneBFundamentals}
                        fundamentalsLoading={false}
                        fundamentalsError={null}
                        groupSummary={paneBChartGroupSummary}
                        timeframe={timeframe}
                        onTimeframeChange={handleTimeframeChange}
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
                        annotations={paneBAnnotations}
                        onAnnotationsChange={handlePaneBAnnotationsChange}
                        onAddToWatchlist={setWatchlistPickerSymbol}
                        onAddToJournal={handleChartAddToJournal}
                        searchOptions={universeCatalog}
                        onSearchSymbol={(query) => handlePaneSearchSubmit("B", query)}
                        onOpenGroup={handleOpenChartGroupModal}
                        onRefreshChart={() => {
                          if (paneBSymbol) {
                            void loadChartForPaneB(paneBSymbol, timeframe, activeMarket).catch(() => {});
                          }
                        }}
                        onStepChart={(direction) => stepChartSymbol(direction, "B")}
                        expanded
                      />
                    }
                  />
                </Suspense>
            {groupWidgetOpen && groupWidgetContext ? (
              <Suspense fallback={null}>
                <GroupStocksWidget
                  market={activeMarket}
                  context={groupWidgetContext}
                  selectedSymbolA={selectedSymbol}
                  selectedSymbolB={paneBSymbol}
                  activePane={activePane}
                  compareMode={compareMode}
                  compareLayout={compareLayout}
                  rect={groupWidgetRect}
                  onRectChange={setGroupWidgetRect}
                  onClose={() => setGroupWidgetOpen(false)}
                  onSelectMember={handleGroupWidgetSelect}
                  onToggleCompare={handleToggleCompareMode}
                  onLayoutChange={setCompareLayout}
                />
              </Suspense>
            ) : null}
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
        <span>NSE / BSE</span>
        <span className="app-footer-dot">·</span>
        <span>© {new Date().getFullYear()}</span>
      </footer>
    </div>
  );
}
