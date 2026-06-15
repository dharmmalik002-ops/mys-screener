import { useDeferredValue, useEffect, useId, useMemo, useRef, useState, type ChangeEvent, type PointerEvent as ReactPointerEvent } from "react";
import { ColorType, createChart, type UTCTimestamp } from "lightweight-charts";

import { getAiSwingAnalysis, getChartHistory, getEarningsSummary, type AiSwingAnalysis, type BandHistorySegment, type ChartBar, type ChartLineMarker, type ChartLinePoint, type ChartResponse, type CompanyEarningsSummary, type CompanyFundamentals, type MarketKey, type QuarterlyResultItem, type StockOverview } from "../lib/api";
import { sanitizeChartBars, sanitizeLineMarkers, sanitizeLinePoints } from "../lib/chartData";
import { computeAutoLevels, type AutoLevels } from "../lib/levels";
import type { ChartTradeMarker } from "../lib/journal";
import { DEFAULT_CHART_COLORS } from "../lib/chartDefaults";
import { buildSymbolSuggestions } from "../lib/searchSuggestions";
import { Panel } from "./Panel";

export type IndicatorKey = "ema10" | "ema20" | "ema50" | "ema200" | "vwap";
export type ChartStyle = "candles" | "bars";
export type ChartTimeframe = "15m" | "30m" | "1h" | "1D" | "1W";
export type ChartPanelTab = "technical" | "fundamentals";
export type ChartPaletteKey = "current" | "editorial";
export type ChartColorSettings = {
  ema10: string;
  ema20: string;
  ema50: string;
  ema200: string;
  vwap: string;
  candleUp: string;
  candleDown: string;
  volumeUp: string;
  volumeDown: string;
  rsLine: string;
  rsMarker: string;
  rsMarkerSize: number;
};

export type ChartGroupSummary = {
  groupId: string;
  groupName: string;
  groupRank: number;
  groupRankLabel: string;
  stockRank: number;
  stockCount: number;
};

type DrawingTool = "none" | "hline" | "vline" | "trendline" | "ray" | "rectangle" | "measure" | "text";
type FavoriteItemId = `tool:${DrawingTool}` | `indicator:${IndicatorKey}` | "action:load-full-history" | "overlay:rvol" | "overlay:pocket-pivot" | "overlay:earnings";

type FavoritesSettings = {
  enabled: boolean;
  itemIds: FavoriteItemId[];
};

type FavoritesWidgetState = {
  enabled: boolean;
  x: number;
  y: number;
  width: number;
  height: number;
  accentColor: string;
};

type PocketPivotWidgetState = {
  enabled: boolean;
  x: number;
  y: number;
  width: number;
  height: number;
  dotColor: string;
  dotSize: number;
};

type NotesWidgetState = {
  enabled: boolean;
  x: number;
  y: number;
  width: number;
  height: number;
  noteColor: string;
  noteFont: string;
  noteFontSize: number;
};

type EarningsGrowthMode = "yoy" | "qoq";

type EarningsWidgetState = {
  enabled: boolean;
  x: number;
  y: number;
  width: number;
  height: number;
  accentColor: string;
  quarters: number;
  growthMode: EarningsGrowthMode;
};

type RvolPoint = {
  time: number;
  value: number | null;
  volume: number;
  averageVolume: number | null;
};

type RvolWidgetSettings = {
  enabled: boolean;
  pos: { x: number; y: number } | null;
  accentColor: string;
  scale: "sm" | "md" | "lg";
};

type PocketPivotNoteStyle = Pick<NotesWidgetState, "noteColor" | "noteFont" | "noteFontSize">;
type PocketPivotNotesMap = Record<string, string>;

type ChartAnchor = {
  time: number;
  price: number;
};

type AnnotationHandleKey = "point" | "start" | "end";

type ActiveAnnotationDrag = {
  annotationId: string;
  handleKey: AnnotationHandleKey;
};

type HoveredPriceBar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  changeValue: number | null;
  changePct: number | null;
};

export type ChartAnnotation =
  | {
      id: string;
      type: "hline";
      point: ChartAnchor;
      color?: string;
      lineWidth?: number;
    }
  | {
      id: string;
      type: "vline";
      point: ChartAnchor;
      color?: string;
      lineWidth?: number;
    }
  | {
      id: string;
      type: "trendline";
      start: ChartAnchor;
      end: ChartAnchor;
      color?: string;
      lineWidth?: number;
    }
  | {
      id: string;
      type: "ray";
      start: ChartAnchor;
      end: ChartAnchor;
      color?: string;
      lineWidth?: number;
    }
  | {
      id: string;
      type: "rectangle";
      start: ChartAnchor;
      end: ChartAnchor;
      color?: string;
      lineWidth?: number;
    }
  | {
      id: string;
      type: "measure";
      start: ChartAnchor;
      end: ChartAnchor;
      color?: string;
      lineWidth?: number;
    }
  | {
      id: string;
      type: "text";
      point: ChartAnchor;
      text: string;
      color?: string;
    };

export { DEFAULT_CHART_COLORS };

type ChartPanelProps = {
  market: MarketKey;
  symbol: string | null;
  bars: ChartBar[];
  rsLine: ChartLinePoint[];
  rsLineMarkers: ChartLineMarker[];
  earningsMarkers?: ChartLineMarker[];
  volumeMarkers?: ChartLineMarker[];
  bandChangeMarkers?: ChartLineMarker[];
  bandHistory?: BandHistorySegment[];
  tradeMarkers?: ChartTradeMarker[];
  onSellMarkerClick?: (symbol: string, exitDate: string) => void;
  summary: StockOverview | null;
  panelTab: ChartPanelTab;
  onPanelTabChange: (tab: ChartPanelTab) => void;
  chartError: string | null;
  chartLoading: boolean;
  chartCacheState: "cached" | "live" | null;
  fundamentals: CompanyFundamentals | null;
  fundamentalsLoading: boolean;
  fundamentalsError: string | null;
  groupSummary?: ChartGroupSummary | null;
  timeframe: ChartTimeframe;
  onTimeframeChange: (timeframe: ChartTimeframe) => void;
  chartStyle: ChartStyle;
  onChartStyleChange: (style: ChartStyle) => void;
  chartPalette: ChartPaletteKey;
  onChartPaletteChange: (palette: ChartPaletteKey) => void;
  showBenchmarkOverlay: boolean;
  onShowBenchmarkOverlayChange: (show: boolean) => void;
  indicatorKeys: IndicatorKey[];
  onToggleIndicator: (indicator: IndicatorKey) => void;
  chartColors: ChartColorSettings;
  onChartColorsChange: (colors: ChartColorSettings) => void;
  drawingColor: string;
  onDrawingColorChange: (color: string) => void;
  annotations: ChartAnnotation[];
  onAnnotationsChange: (annotations: ChartAnnotation[]) => void;
  onAddToWatchlist?: (symbol: string) => void;
  onRemoveFromWatchlist?: (symbol: string) => void;
  onAddToJournal?: (symbol: string, suggestedPrice?: number) => void;
  searchOptions?: Array<{ symbol: string; name: string }>;
  onSearchSymbol?: (query: string) => void;
  onOpenGroup?: (groupId: string) => void;
  onRefreshChart?: () => void;
  expanded?: boolean;
};

const TIMEFRAMES: ChartTimeframe[] = ["15m", "30m", "1h", "1D", "1W"];
const CHART_STYLES: Array<{ key: ChartStyle; label: string }> = [
  { key: "candles", label: "Candles" },
  { key: "bars", label: "Bars" },
];
type IndicatorColorKey = "ema10" | "ema20" | "ema50" | "ema200" | "vwap";

const INDICATORS: Array<{ key: IndicatorKey; label: string; colorKey: IndicatorColorKey }> = [
  { key: "ema10", label: "EMA10", colorKey: "ema10" },
  { key: "ema20", label: "EMA20", colorKey: "ema20" },
  { key: "ema50", label: "EMA50", colorKey: "ema50" },
  { key: "ema200", label: "SMA200", colorKey: "ema200" },
  { key: "vwap", label: "VWAP", colorKey: "vwap" },
];
const DRAWING_TOOLS: Array<{ key: DrawingTool; label: string }> = [
  { key: "none", label: "Cursor" },
  { key: "hline", label: "Horizontal Line" },
  { key: "vline", label: "Vertical Line" },
  { key: "trendline", label: "Trendline" },
  { key: "ray", label: "Ray" },
  { key: "rectangle", label: "Rectangle" },
  { key: "measure", label: "Measure" },
  { key: "text", label: "Text" },
];
type ChartColorFieldKey =
  | "ema10"
  | "ema20"
  | "ema50"
  | "ema200"
  | "vwap"
  | "candleUp"
  | "candleDown"
  | "volumeUp"
  | "volumeDown"
  | "rsLine"
  | "rsMarker";

const CHART_COLOR_FIELDS: Array<{ key: ChartColorFieldKey; label: string }> = [
  { key: "ema10", label: "EMA10" },
  { key: "ema20", label: "EMA20" },
  { key: "ema50", label: "EMA50" },
  { key: "ema200", label: "SMA200" },
  { key: "vwap", label: "VWAP" },
  { key: "candleUp", label: "Up Candle" },
  { key: "candleDown", label: "Down Candle" },
  { key: "volumeUp", label: "Up Volume" },
  { key: "volumeDown", label: "Down Volume" },
  { key: "rsLine", label: "RS Line" },
  { key: "rsMarker", label: "RS Circle" },
];
const PANEL_TABS: Array<{ key: ChartPanelTab; label: string }> = [
  { key: "technical", label: "Technical" },
  { key: "fundamentals", label: "Fundamentals" },
];
const CHART_PALETTES: Record<
  ChartPaletteKey,
  {
    label: string;
    background: string;
    textColor: string;
    gridColor: string;
    crosshairColor: string;
    borderColor: string;
    upColor: string;
    downColor: string;
    volumeUpColor: string;
    volumeDownColor: string;
    rsLineColor: string;
    rsMarkerColor: string;
  }
> = {
  current: {
    label: "Current",
    background: "#0d1117",
    textColor: "#8b949e",
    gridColor: "rgba(0, 210, 255, 0.07)",
    crosshairColor: "rgba(0, 210, 255, 0.22)",
    borderColor: "rgba(48, 54, 61, 0.95)",
    upColor: "#00d2ff",
    downColor: "#ff3131",
    volumeUpColor: "rgba(0, 210, 255, 0.38)",
    volumeDownColor: "rgba(255, 49, 49, 0.35)",
    rsLineColor: "#39ff14",
    rsMarkerColor: "#39ff14",
  },
  editorial: {
    label: "Editorial",
    background: "#fcfbff",
    textColor: "#48536a",
    gridColor: "rgba(117, 83, 201, 0.08)",
    crosshairColor: "rgba(117, 83, 201, 0.25)",
    borderColor: "rgba(154, 132, 202, 0.48)",
    upColor: "#7b61ff",
    downColor: "#ff6b6b",
    volumeUpColor: "rgba(123, 97, 255, 0.28)",
    volumeDownColor: "rgba(255, 107, 107, 0.25)",
    rsLineColor: "#00a6a6",
    rsMarkerColor: "#8f2dff",
  },
};
const RIGHT_EDGE_PADDING_BARS = 12;
const FUTURE_DRAW_EXTENSION_BARS = 96;
const CHART_FAVORITES_STORAGE_KEY = "stockScanner.chartFavorites.v1";
const FAVORITES_WIDGET_STORAGE_KEY = "stockScanner.favoritesWidget.v1";
const POCKET_PIVOT_STORAGE_KEY = "stockScanner.pocketPivotWidget.v1";
const POCKET_PIVOT_NOTES_STORAGE_KEY = "stockScanner.pocketPivotNotes.v1";
const NOTES_WIDGET_STORAGE_KEY = "stockScanner.notesWidget.v1";
const EARNINGS_WIDGET_STORAGE_KEY = "stockScanner.earningsWidget.v1";
const RVOL_WIDGET_STORAGE_KEY = "stockScanner.rvolWidget.v1";
const CHART_RANGE_STORAGE_KEY = "stockScanner.chartRange.v1";
const CIRCUIT_WIDGET_STORAGE_KEY = "stockScanner.circuitWidget.v1";
const CIRCUIT_LOCKS_STORAGE_KEY = "stockScanner.circuitLocks.v1";
const AUTO_LEVELS_STORAGE_KEY = "stockScanner.chartLevels.v1";
const AI_TOGGLE_STORAGE_KEY = "stockScanner.chartAi.v1";

function readAutoLevelsEnabled(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return JSON.parse(window.localStorage.getItem(AUTO_LEVELS_STORAGE_KEY) ?? "false") === true;
  } catch {
    return false;
  }
}

function readAiEnabled(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return JSON.parse(window.localStorage.getItem(AI_TOGGLE_STORAGE_KEY) ?? "false") === true;
  } catch {
    return false;
  }
}

function readCircuitLocksEnabled(): boolean {
  if (typeof window === "undefined") return true;
  try {
    const raw = window.localStorage.getItem(CIRCUIT_LOCKS_STORAGE_KEY);
    return raw === null ? true : JSON.parse(raw) === true;
  } catch {
    return true;
  }
}

// Assumed daily price band when the exchange feed doesn't supply exact limits.
// NSE bands are 2/5/10/20% of the previous close; 0 disables the widget lines,
// -1 auto-detects the band from the stock's own history.
const CIRCUIT_BAND_OPTIONS = [0, 2, 5, 10, 20] as const;
const CIRCUIT_BAND_AUTO = -1;
const CIRCUIT_BAND_SELECT: Array<{ value: number; label: string }> = [
  { value: CIRCUIT_BAND_AUTO, label: "Auto" },
  { value: 0, label: "Off" },
  { value: 2, label: "±2%" },
  { value: 5, label: "±5%" },
  { value: 10, label: "±10%" },
  { value: 20, label: "±20%" },
];
const DEFAULT_CIRCUIT_BAND_PCT = CIRCUIT_BAND_AUTO;

function readCircuitBandPct(): number {
  if (typeof window === "undefined") return DEFAULT_CIRCUIT_BAND_PCT;
  try {
    const raw = window.localStorage.getItem(CIRCUIT_WIDGET_STORAGE_KEY);
    if (raw === null) return DEFAULT_CIRCUIT_BAND_PCT;
    const parsed = Number(JSON.parse(raw));
    return CIRCUIT_BAND_SELECT.some((option) => option.value === parsed) ? parsed : DEFAULT_CIRCUIT_BAND_PCT;
  } catch {
    return DEFAULT_CIRCUIT_BAND_PCT;
  }
}

type CircuitLimits = {
  upper: number | null;
  lower: number | null;
  exact: boolean;
  prevClose: number | null;
};

function computeCircuitLimits(summary: StockOverview | null, bandPct: number): CircuitLimits {
  const exactUpper = summary?.upper_circuit_limit ?? null;
  const exactLower = summary?.lower_circuit_limit ?? null;
  if (typeof exactUpper === "number" && exactUpper > 0 && typeof exactLower === "number" && exactLower > 0) {
    return { upper: exactUpper, lower: exactLower, exact: true, prevClose: null };
  }
  const denominator = summary ? 1 + summary.change_pct / 100 : 0;
  const prevClose = summary && denominator !== 0 && summary.last_price > 0 ? summary.last_price / denominator : null;
  if (!prevClose || prevClose <= 0 || bandPct <= 0) {
    return { upper: exactUpper, lower: exactLower, exact: false, prevClose };
  }
  // If today's move already exceeds the assumed band, the stock can't be on
  // that band — escalate to the smallest standard band that contains the move
  // so the estimated lines stay meaningful (a +18% day implies a 20% band).
  let effectiveBandPct = bandPct;
  const movePct = Math.abs(summary?.change_pct ?? 0);
  for (const option of CIRCUIT_BAND_OPTIONS) {
    if (option >= bandPct && option >= movePct) {
      effectiveBandPct = option;
      break;
    }
  }
  if (movePct > effectiveBandPct) {
    // Moved beyond the widest standard band (F&O dynamic-band stock) — an
    // estimated line would be misleading, so draw nothing.
    return { upper: null, lower: null, exact: false, prevClose };
  }
  return {
    upper: prevClose * (1 + effectiveBandPct / 100),
    lower: prevClose * (1 - effectiveBandPct / 100),
    exact: false,
    prevClose,
  };
}

// Detect the stock's fixed daily price band from its own history, the way the
// popular TradingView circuit indicators do: the smallest standard band that
// contains every close-to-close move in the loaded window. Returns null for
// dynamic-band stocks (moves beyond 20%) where a fixed level would mislead.
function detectCircuitBandPct(bars: ChartBar[]): number | null {
  if (bars.length < 10) return 5;
  let maxMovePct = 0;
  const start = Math.max(1, bars.length - 250);
  for (let i = start; i < bars.length; i += 1) {
    const prev = bars[i - 1]?.close;
    const close = bars[i]?.close;
    if (!prev || prev <= 0 || !close) continue;
    const movePct = Math.abs(close / prev - 1) * 100;
    if (movePct > maxMovePct) maxMovePct = movePct;
  }
  for (const option of CIRCUIT_BAND_OPTIONS) {
    if (option > 0 && maxMovePct <= option + 0.05) {
      return option;
    }
  }
  return null;
}

type CircuitLevelSeries = {
  // value is omitted (whitespace point) for dynamic-band periods, which breaks
  // the line instead of bridging across the gap.
  upper: { time: UTCTimestamp; value?: number }[];
  lower: { time: UTCTimestamp; value?: number }[];
  ucLockTimes: UTCTimestamp[];
  lcLockTimes: UTCTimestamp[];
};

// Resolved per-date band timeline: effective band % from `fromTime` onward
// (UTC midnight, matching daily bar timestamps). null band = dynamic, no level.
type BandTimelineEntry = { fromTime: number; bandPct: number | null };

function buildBandTimeline(bandHistory: BandHistorySegment[]): BandTimelineEntry[] {
  const timeline: BandTimelineEntry[] = [];
  for (const segment of bandHistory) {
    let fromTime = 0;
    if (segment.from_date) {
      const parsed = Date.parse(`${segment.from_date}T00:00:00Z`);
      if (!Number.isFinite(parsed)) continue;
      fromTime = parsed / 1000;
    }
    timeline.push({ fromTime, bandPct: segment.band_pct });
  }
  timeline.sort((left, right) => left.fromTime - right.fromTime);
  return timeline;
}

function bandPctAt(timeline: BandTimelineEntry[], barTime: number, fallbackBandPct: number): number | null {
  let effective: number | null | undefined;
  for (const entry of timeline) {
    if (entry.fromTime <= barTime) {
      effective = entry.bandPct;
    } else {
      break;
    }
  }
  return effective === undefined ? fallbackBandPct : effective;
}

// Per-bar circuit levels (each day's band applied to the previous close) plus
// the days the stock actually locked at a circuit — close within 0.25% of the
// band level counts as a lock. When the symbol's real NSE band timeline is
// known, each bar uses the band in force on that date; otherwise the single
// fallback band applies to the whole window.
function computeCircuitLevelSeries(
  bars: ChartBar[],
  bandPct: number,
  bandTimeline: BandTimelineEntry[] = [],
): CircuitLevelSeries {
  const upper: CircuitLevelSeries["upper"] = [];
  const lower: CircuitLevelSeries["lower"] = [];
  const ucLockTimes: UTCTimestamp[] = [];
  const lcLockTimes: UTCTimestamp[] = [];
  const LOCK_TOLERANCE = 0.0025;
  for (let i = 1; i < bars.length; i += 1) {
    const prev = bars[i - 1]?.close;
    const bar = bars[i];
    if (!prev || prev <= 0 || !bar) continue;
    const barBandPct = bandTimeline.length ? bandPctAt(bandTimeline, Number(bar.time), bandPct) : bandPct;
    if (barBandPct == null || barBandPct <= 0) {
      upper.push({ time: bar.time as UTCTimestamp });
      lower.push({ time: bar.time as UTCTimestamp });
      continue;
    }
    const uc = prev * (1 + barBandPct / 100);
    const lc = prev * (1 - barBandPct / 100);
    upper.push({ time: bar.time as UTCTimestamp, value: uc });
    lower.push({ time: bar.time as UTCTimestamp, value: lc });
    if (bar.close >= uc * (1 - LOCK_TOLERANCE)) {
      ucLockTimes.push(bar.time as UTCTimestamp);
    } else if (bar.close <= lc * (1 + LOCK_TOLERANCE)) {
      lcLockTimes.push(bar.time as UTCTimestamp);
    }
  }
  return { upper, lower, ucLockTimes, lcLockTimes };
}

type ChartRangeKey = "3M" | "6M" | "1Y" | "FULL";
const CHART_RANGE_OPTIONS: { value: ChartRangeKey; label: string }[] = [
  { value: "3M", label: "3M  (a)" },
  { value: "6M", label: "6M  (s)" },
  { value: "1Y", label: "1Y  (d)" },
  { value: "FULL", label: "Full  (f)" },
];
const DEFAULT_CHART_RANGE: ChartRangeKey = "1Y";

function readChartRange(): ChartRangeKey {
  if (typeof window === "undefined") return DEFAULT_CHART_RANGE;
  try {
    const raw = window.localStorage.getItem(CHART_RANGE_STORAGE_KEY);
    if (raw && CHART_RANGE_OPTIONS.some((opt) => opt.value === raw)) {
      return raw as ChartRangeKey;
    }
  } catch {
    /* ignore */
  }
  return DEFAULT_CHART_RANGE;
}

function barsForChartRange(timeframe: ChartTimeframe, range: ChartRangeKey): number | null {
  if (range === "FULL") return null;
  const monthsByRange: Record<Exclude<ChartRangeKey, "FULL">, number> = { "3M": 3, "6M": 6, "1Y": 12 };
  const months = monthsByRange[range];
  // Approximate trading-bar counts per month for each timeframe.
  const barsPerMonth: Record<ChartTimeframe, number> = {
    "1D": 21,
    "1W": 4.33,
    "1h": 132, // ~6.25 trading hours/day × 21 trading days
    "30m": 264,
    "15m": 528,
  } as Record<ChartTimeframe, number>;
  const perMonth = barsPerMonth[timeframe] ?? 21;
  return Math.max(20, Math.round(perMonth * months));
}
const DEFAULT_FAVORITES_SETTINGS: FavoritesSettings = {
  enabled: true,
  itemIds: ["tool:trendline", "tool:measure", "overlay:rvol"],
};
const DEFAULT_FAVORITES_WIDGET: FavoritesWidgetState = {
  enabled: false,
  x: 18,
  y: 274,
  width: 276,
  height: 260,
  accentColor: "#6ea8ff",
};
const DEFAULT_POCKET_PIVOT_WIDGET: PocketPivotWidgetState = {
  enabled: false,
  x: 18,
  y: 86,
  width: 240,
  height: 172,
  dotColor: "#ffd36f",
  dotSize: 1.8,
};
const DEFAULT_NOTES_WIDGET: NotesWidgetState = {
  enabled: false,
  x: 280,
  y: 86,
  width: 280,
  height: 220,
  noteColor: "#f8fafc",
  noteFont: "Inter, system-ui, sans-serif",
  noteFontSize: 13,
};
const DEFAULT_EARNINGS_WIDGET: EarningsWidgetState = {
  enabled: false,
  x: 574,
  y: 86,
  width: 380,
  height: 260,
  accentColor: "#4bf0b3",
  quarters: 4,
  growthMode: "yoy",
};
const DEFAULT_RVOL_WIDGET: RvolWidgetSettings = {
  enabled: false,
  pos: null,
  accentColor: "#00d2ff",
  scale: "md",
};
const FAVORITE_ITEMS: Array<{ id: FavoriteItemId; label: string; kind: "Tool" | "Indicator" | "Overlay" | "Action" }> = [
  ...DRAWING_TOOLS.filter((tool) => tool.key !== "none").map((tool) => ({
    id: `tool:${tool.key}` as FavoriteItemId,
    label: tool.label,
    kind: "Tool" as const,
  })),
  ...INDICATORS.map((indicator) => ({
    id: `indicator:${indicator.key}` as FavoriteItemId,
    label: indicator.label,
    kind: "Indicator" as const,
  })),
  { id: "overlay:rvol", label: "RVOL", kind: "Overlay" },
  { id: "overlay:pocket-pivot", label: "Pocket Pivot", kind: "Overlay" },
  { id: "overlay:earnings", label: "Earnings", kind: "Overlay" },
  { id: "action:load-full-history", label: "Full History", kind: "Action" },
];

function supportedTimeframes(market: MarketKey): ChartTimeframe[] {
  return ["1D", "1W"];
}

const ANNOTATION_DEFAULT_COLORS: Record<string, string> = {
  hline: "#00d2ff",
  vline: "#6ea8ff",
  trendline: "#ffd36f",
  ray: "#8ee6ff",
  rectangle: "#59c4ff",
  measure: "#4bf0b3",
  text: "#ffd36f",
};

function defaultVisibleBars(timeframe: ChartTimeframe) {
  if (timeframe === "1D") {
    return 252;
  }
  if (timeframe === "1W") {
    return 104;
  }
  if (timeframe === "1h") {
    return 260;
  }
  if (timeframe === "30m") {
    return 260;
  }
  return 220;
}

function computeEma(bars: ChartBar[], length: number) {
  if (bars.length < length) {
    return [];
  }

  const multiplier = 2 / (length + 1);
  let previous = bars.slice(0, length).reduce((sum, bar) => sum + bar.close, 0) / length;
  const points = [
    {
      time: bars[length - 1].time as UTCTimestamp,
      value: Number(previous.toFixed(2)),
    },
  ];

  for (let index = length; index < bars.length; index += 1) {
    const bar = bars[index];
    previous = (bar.close - previous) * multiplier + previous;
    points.push({
      time: bar.time as UTCTimestamp,
      value: Number(previous.toFixed(2)),
    });
  }

  return points;
}

function computeSma(bars: ChartBar[], length: number) {
  if (bars.length < length) {
    return [];
  }

  const points: Array<{ time: UTCTimestamp; value: number }> = [];
  let sum = 0;

  for (let index = 0; index < bars.length; index += 1) {
    sum += bars[index]?.close ?? 0;
    if (index >= length) {
      sum -= bars[index - length]?.close ?? 0;
    }
    if (index >= length - 1) {
      points.push({
        time: bars[index].time as UTCTimestamp,
        value: Number((sum / length).toFixed(2)),
      });
    }
  }

  return points;
}

function computeVolumeSma(bars: ChartBar[], length: number) {
  if (bars.length < length) {
    return [];
  }

  const points: Array<{ time: UTCTimestamp; value: number }> = [];
  let sum = 0;

  for (let index = 0; index < bars.length; index += 1) {
    sum += bars[index]?.volume ?? 0;
    if (index >= length) {
      sum -= bars[index - length]?.volume ?? 0;
    }
    if (index >= length - 1) {
      points.push({
        time: bars[index].time as UTCTimestamp,
        value: Number((sum / length).toFixed(2)),
      });
    }
  }

  return points;
}

function computeRvolPoints(bars: ChartBar[], length = 50): RvolPoint[] {
  const points: RvolPoint[] = [];
  let priorVolumeSum = 0;

  for (let index = 0; index < bars.length; index += 1) {
    const bar = bars[index];
    const windowLength = Math.min(index, length);
    const averageVolume = windowLength > 0 ? priorVolumeSum / windowLength : null;
    points.push({
      time: bar.time,
      value: averageVolume && averageVolume > 0 ? Number((bar.volume / averageVolume).toFixed(2)) : null,
      volume: bar.volume,
      averageVolume,
    });

    priorVolumeSum += bar.volume;
    if (index >= length) {
      priorVolumeSum -= bars[index - length]?.volume ?? 0;
    }
  }

  return points;
}

function computePocketPivotBars(bars: ChartBar[]) {
  const matches: Array<ChartBar & { rvol: number | null }> = [];
  const rvolPoints = computeRvolPoints(bars);

  for (let index = 1; index < bars.length; index += 1) {
    const current = bars[index];
    const previous = bars[index - 1];
    if (!current || !previous || current.close <= previous.close) {
      continue;
    }

    let highestDownVolume = 0;
    const firstLookbackIndex = Math.max(1, index - 10);
    for (let lookbackIndex = firstLookbackIndex; lookbackIndex < index; lookbackIndex += 1) {
      const lookback = bars[lookbackIndex];
      const lookbackPrevious = bars[lookbackIndex - 1];
      if (lookback && lookbackPrevious && lookback.close < lookbackPrevious.close) {
        highestDownVolume = Math.max(highestDownVolume, lookback.volume);
      }
    }

    if (highestDownVolume > 0 && current.volume > highestDownVolume) {
      matches.push({
        ...current,
        rvol: rvolPoints[index]?.value ?? null,
      });
    }
  }

  return matches;
}

function computeVwap(bars: ChartBar[]) {
  let cumulativePriceVolume = 0;
  let cumulativeVolume = 0;

  return bars.map((bar) => {
    const typicalPrice = (bar.high + bar.low + bar.close) / 3;
    cumulativePriceVolume += typicalPrice * bar.volume;
    cumulativeVolume += bar.volume;
    const value = cumulativeVolume === 0 ? typicalPrice : cumulativePriceVolume / cumulativeVolume;
    return {
      time: bar.time as UTCTimestamp,
      value: Number(value.toFixed(2)),
    };
  });
}

function withOpacity(color: string, opacity: number) {
  const hex = color.trim();
  const normalized = hex.startsWith("#") ? hex.slice(1) : hex;
  if (![3, 6].includes(normalized.length)) {
    return color;
  }

  const expanded = normalized.length === 3 ? normalized.split("").map((value) => `${value}${value}`).join("") : normalized;
  const red = Number.parseInt(expanded.slice(0, 2), 16);
  const green = Number.parseInt(expanded.slice(2, 4), 16);
  const blue = Number.parseInt(expanded.slice(4, 6), 16);
  if ([red, green, blue].some((value) => Number.isNaN(value))) {
    return color;
  }
  return `rgba(${red}, ${green}, ${blue}, ${opacity})`;
}

function normalizeChartTime(value: unknown): number | null {
  if (typeof value === "number") {
    return value;
  }
  if (value && typeof value === "object" && "year" in value && "month" in value && "day" in value) {
    const businessDay = value as { year: number; month: number; day: number };
    return Math.floor(Date.UTC(businessDay.year, businessDay.month - 1, businessDay.day) / 1000);
  }
  return null;
}

function timeframeStepSeconds(timeframe: ChartTimeframe) {
  if (timeframe === "15m") {
    return 15 * 60;
  }
  if (timeframe === "30m") {
    return 30 * 60;
  }
  if (timeframe === "1h") {
    return 60 * 60;
  }
  if (timeframe === "1W") {
    return 7 * 24 * 60 * 60;
  }
  return 24 * 60 * 60;
}

function addBusinessDays(timestamp: number, businessDays: number) {
  const date = new Date(timestamp * 1000);
  let remaining = businessDays;
  while (remaining > 0) {
    date.setUTCDate(date.getUTCDate() + 1);
    const day = date.getUTCDay();
    if (day !== 0 && day !== 6) {
      remaining -= 1;
    }
  }
  return Math.floor(date.getTime() / 1000);
}

function buildFutureWhitespaceTimes(bars: ChartBar[], timeframe: ChartTimeframe, count: number) {
  const lastTime = bars[bars.length - 1]?.time;
  if (!lastTime || count <= 0) {
    return [] as number[];
  }

  return Array.from({ length: count }, (_, index) => {
    const step = index + 1;
    if (timeframe === "1D") {
      return addBusinessDays(lastTime, step);
    }
    return lastTime + (timeframeStepSeconds(timeframe) * step);
  });
}

function buildId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function projectAnchor(chart: ReturnType<typeof createChart> | null, mainSeries: any, anchor: ChartAnchor) {
  if (!chart || !mainSeries) {
    return null;
  }

  const x = chart.timeScale().timeToCoordinate(anchor.time as UTCTimestamp);
  const y = mainSeries.priceToCoordinate(anchor.price);
  if (x === null || x === undefined || y === null || y === undefined) {
    return null;
  }

  return { x, y };
}

function getAnnotationHandleAnchors(annotation: ChartAnnotation): Array<{ key: AnnotationHandleKey; anchor: ChartAnchor }> {
  if ("point" in annotation) {
    return [{ key: "point", anchor: annotation.point }];
  }

  return [
    { key: "start", anchor: annotation.start },
    { key: "end", anchor: annotation.end },
  ];
}

function isTwoPointTool(tool: DrawingTool) {
  return tool === "trendline" || tool === "ray" || tool === "rectangle" || tool === "measure";
}

function chartSubtitle(tool: DrawingTool, draftStart: ChartAnchor | null, chartStyle: ChartStyle) {
  if (tool === "hline") {
    return "Horizontal line mode: click once to place a saved price level";
  }
  if (tool === "vline") {
    return "Vertical line mode: click once to mark a date";
  }
  if (tool === "trendline") {
    return draftStart ? "Trendline mode: pick the second point" : "Trendline mode: click the first point";
  }
  if (tool === "ray") {
    return draftStart ? "Ray mode: pick the second point to set direction" : "Ray mode: click the first point";
  }
  if (tool === "rectangle") {
    return draftStart ? "Rectangle mode: pick the opposite corner" : "Rectangle mode: click the first corner";
  }
  if (tool === "measure") {
    return draftStart ? "Measure mode: pick the second point to measure move and bars" : "Measure mode: click the first point";
  }
  if (tool === "text") {
    return "Text mode: click a candle or bar to place a saved note";
  }
  return chartStyle === "bars" ? "Bar chart, volume, indicators, and saved drawings" : "Candles, volume, indicators, and saved drawings";
}

function numberLocaleForMarket(market: MarketKey) {
  return "en-IN";
}

function formatNumber(value: number | null | undefined, digits = 2, market: MarketKey = "india") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return value.toLocaleString(numberLocaleForMarket(market), {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatPercent(value: number | null | undefined, market: MarketKey = "india") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${value >= 0 ? "+" : ""}${formatNumber(value, 2, market)}%`;
}

function formatPlainPercent(value: number | null | undefined, market: MarketKey = "india") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${formatNumber(value, 2, market)}%`;
}

function formatCrore(value: number | null | undefined, market: MarketKey = "india", digits?: number) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${formatNumber(value, digits ?? 2, market)} Cr`;
}

function formatPrice(value: number | null | undefined, market: MarketKey, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `₹${formatNumber(value, digits, market)}`;
}

function formatCount(value: number | null | undefined, market: MarketKey = "india") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return Math.round(value).toLocaleString(numberLocaleForMarket(market));
}

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  return date.toLocaleString();
}

function formatChartDateFromTimestamp(value: number | null | undefined, market: MarketKey = "india") {
  if (!value) {
    return "—";
  }
  const date = new Date(value * 1000);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  return date.toLocaleDateString(numberLocaleForMarket(market), {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatCircuitBand(summary: StockOverview, market: MarketKey) {
  if (market !== "india") {
    return summary.circuit_band_label ?? "N/A";
  }

  const denominator = 1 + (summary.change_pct / 100);
  const previousClose = denominator === 0 ? null : summary.last_price / denominator;
  if (previousClose && previousClose > 0 && summary.lower_circuit_limit != null && summary.upper_circuit_limit != null) {
    const lowerPct = ((summary.lower_circuit_limit / previousClose) - 1) * 100;
    const upperPct = ((summary.upper_circuit_limit / previousClose) - 1) * 100;
    const symmetric = Math.abs(Math.abs(lowerPct) - Math.abs(upperPct)) < 0.05;
    if (symmetric) {
      return `±${formatNumber(Math.abs(upperPct), 2, market)}%`;
    }
    return `${lowerPct >= 0 ? "+" : ""}${formatNumber(lowerPct, 2, market)}% / ${upperPct >= 0 ? "+" : ""}${formatNumber(upperPct, 2, market)}%`;
  }

  return summary.circuit_band_label ?? "N/A";
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function readFavoritesSettings(): FavoritesSettings {
  if (typeof window === "undefined") {
    return DEFAULT_FAVORITES_SETTINGS;
  }

  try {
    const raw = window.localStorage.getItem(CHART_FAVORITES_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_FAVORITES_SETTINGS;
    }
    const parsed = JSON.parse(raw) as Partial<FavoritesSettings>;
    const validIds = new Set(FAVORITE_ITEMS.map((item) => item.id));
    const itemIds = Array.isArray(parsed.itemIds)
      ? parsed.itemIds.filter((id): id is FavoriteItemId => typeof id === "string" && validIds.has(id as FavoriteItemId))
      : DEFAULT_FAVORITES_SETTINGS.itemIds;
    return {
      enabled: parsed.enabled ?? DEFAULT_FAVORITES_SETTINGS.enabled,
      itemIds,
    };
  } catch {
    return DEFAULT_FAVORITES_SETTINGS;
  }
}

function readFavoritesWidgetState(): FavoritesWidgetState {
  if (typeof window === "undefined") {
    return DEFAULT_FAVORITES_WIDGET;
  }

  try {
    const raw = window.localStorage.getItem(FAVORITES_WIDGET_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_FAVORITES_WIDGET;
    }
    const parsed = JSON.parse(raw) as Partial<FavoritesWidgetState>;
    return {
      enabled: parsed.enabled ?? DEFAULT_FAVORITES_WIDGET.enabled,
      x: clamp(Number(parsed.x ?? DEFAULT_FAVORITES_WIDGET.x), 0, 1200),
      y: clamp(Number(parsed.y ?? DEFAULT_FAVORITES_WIDGET.y), 0, 900),
      width: clamp(Number(parsed.width ?? DEFAULT_FAVORITES_WIDGET.width), 210, 520),
      height: clamp(Number(parsed.height ?? DEFAULT_FAVORITES_WIDGET.height), 170, 460),
      accentColor: typeof parsed.accentColor === "string" ? parsed.accentColor : DEFAULT_FAVORITES_WIDGET.accentColor,
    };
  } catch {
    return DEFAULT_FAVORITES_WIDGET;
  }
}

function readPocketPivotWidgetState(): PocketPivotWidgetState {
  if (typeof window === "undefined") {
    return DEFAULT_POCKET_PIVOT_WIDGET;
  }

  try {
    const raw = window.localStorage.getItem(POCKET_PIVOT_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_POCKET_PIVOT_WIDGET;
    }
    const parsed = JSON.parse(raw) as Partial<PocketPivotWidgetState>;
    return {
      enabled: parsed.enabled ?? DEFAULT_POCKET_PIVOT_WIDGET.enabled,
      x: clamp(Number(parsed.x ?? DEFAULT_POCKET_PIVOT_WIDGET.x), 0, 1200),
      y: clamp(Number(parsed.y ?? DEFAULT_POCKET_PIVOT_WIDGET.y), 0, 900),
      width: clamp(Number(parsed.width ?? DEFAULT_POCKET_PIVOT_WIDGET.width), 190, 460),
      height: clamp(Number(parsed.height ?? DEFAULT_POCKET_PIVOT_WIDGET.height), 130, 360),
      dotColor: typeof parsed.dotColor === "string" ? parsed.dotColor : DEFAULT_POCKET_PIVOT_WIDGET.dotColor,
      dotSize: clamp(Number(parsed.dotSize ?? DEFAULT_POCKET_PIVOT_WIDGET.dotSize), 0.5, 8),
    };
  } catch {
    return DEFAULT_POCKET_PIVOT_WIDGET;
  }
}

function readNotesWidgetState(): NotesWidgetState {
  if (typeof window === "undefined") {
    return DEFAULT_NOTES_WIDGET;
  }

  try {
    const raw = window.localStorage.getItem(NOTES_WIDGET_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_NOTES_WIDGET;
    }
    const parsed = JSON.parse(raw) as Partial<NotesWidgetState>;
    return {
      enabled: parsed.enabled ?? DEFAULT_NOTES_WIDGET.enabled,
      x: clamp(Number(parsed.x ?? DEFAULT_NOTES_WIDGET.x), 0, 1200),
      y: clamp(Number(parsed.y ?? DEFAULT_NOTES_WIDGET.y), 0, 900),
      width: clamp(Number(parsed.width ?? DEFAULT_NOTES_WIDGET.width), 220, 520),
      height: clamp(Number(parsed.height ?? DEFAULT_NOTES_WIDGET.height), 160, 480),
      noteColor: typeof parsed.noteColor === "string" ? parsed.noteColor : DEFAULT_NOTES_WIDGET.noteColor,
      noteFont: typeof parsed.noteFont === "string" ? parsed.noteFont : DEFAULT_NOTES_WIDGET.noteFont,
      noteFontSize: clamp(Number(parsed.noteFontSize ?? DEFAULT_NOTES_WIDGET.noteFontSize), 10, 28),
    };
  } catch {
    return DEFAULT_NOTES_WIDGET;
  }
}

function readEarningsWidgetState(): EarningsWidgetState {
  if (typeof window === "undefined") {
    return DEFAULT_EARNINGS_WIDGET;
  }

  try {
    const raw = window.localStorage.getItem(EARNINGS_WIDGET_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_EARNINGS_WIDGET;
    }
    const parsed = JSON.parse(raw) as Partial<EarningsWidgetState>;
    return {
      enabled: parsed.enabled ?? DEFAULT_EARNINGS_WIDGET.enabled,
      x: clamp(Number(parsed.x ?? DEFAULT_EARNINGS_WIDGET.x), 0, 1200),
      y: clamp(Number(parsed.y ?? DEFAULT_EARNINGS_WIDGET.y), 0, 900),
      width: clamp(Number(parsed.width ?? DEFAULT_EARNINGS_WIDGET.width), 300, 640),
      height: clamp(Number(parsed.height ?? DEFAULT_EARNINGS_WIDGET.height), 190, 560),
      accentColor: typeof parsed.accentColor === "string" ? parsed.accentColor : DEFAULT_EARNINGS_WIDGET.accentColor,
      quarters: clamp(Math.round(Number(parsed.quarters ?? DEFAULT_EARNINGS_WIDGET.quarters)), 1, 8),
      growthMode: parsed.growthMode === "qoq" ? "qoq" : "yoy",
    };
  } catch {
    return DEFAULT_EARNINGS_WIDGET;
  }
}

function quarterSortValue(period: string | null | undefined) {
  const [monthPart = "", yearPart = "0"] = String(period ?? "").replace(/[-']/g, " ").trim().split(/\s+/);
  const month = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"].indexOf(monthPart.slice(0, 3).toLowerCase()) + 1;
  const parsedYear = Number(yearPart);
  const year = Number.isFinite(parsedYear) ? (parsedYear < 100 ? parsedYear + 2000 : parsedYear) : 0;
  return year * 100 + Math.max(month, 0);
}

function sortQuarterlyResultsLatestFirst(items: QuarterlyResultItem[]) {
  return [...items].sort((left, right) => quarterSortValue(right.period) - quarterSortValue(left.period));
}

function readPocketPivotNotes(): PocketPivotNotesMap {
  if (typeof window === "undefined") {
    return {};
  }

  try {
    const raw = window.localStorage.getItem(POCKET_PIVOT_NOTES_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as PocketPivotNotesMap : {};
  } catch {
    return {};
  }
}

function readRvolWidgetSettings(): RvolWidgetSettings {
  if (typeof window === "undefined") {
    return DEFAULT_RVOL_WIDGET;
  }

  try {
    const raw = window.localStorage.getItem(RVOL_WIDGET_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_RVOL_WIDGET;
    }
    const parsed = JSON.parse(raw) as Partial<RvolWidgetSettings>;
    const parsedPos = parsed.pos && typeof parsed.pos === "object" ? parsed.pos : null;
    const scale = parsed.scale === "sm" || parsed.scale === "lg" || parsed.scale === "md" ? parsed.scale : DEFAULT_RVOL_WIDGET.scale;
    return {
      enabled: parsed.enabled ?? DEFAULT_RVOL_WIDGET.enabled,
      pos: parsedPos && typeof parsedPos.x === "number" && typeof parsedPos.y === "number"
        ? { x: clamp(parsedPos.x, 0, 1200), y: clamp(parsedPos.y, 0, 900) }
        : null,
      accentColor: typeof parsed.accentColor === "string" ? parsed.accentColor : DEFAULT_RVOL_WIDGET.accentColor,
      scale,
    };
  } catch {
    return DEFAULT_RVOL_WIDGET;
  }
}

function pointToSegmentDist(px: number, py: number, x1: number, y1: number, x2: number, y2: number) {
  const dx = x2 - x1;
  const dy = y2 - y1;
  if (dx === 0 && dy === 0) return Math.hypot(px - x1, py - y1);
  const t = Math.max(0, Math.min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)));
  return Math.hypot(px - (x1 + t * dx), py - (y1 + t * dy));
}

function nearestBarTime(bars: ChartBar[], targetTime: number) {
  if (!bars.length) {
    return targetTime;
  }

  let low = 0;
  let high = bars.length - 1;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const candidate = bars[mid]?.time ?? targetTime;
    if (candidate === targetTime) {
      return candidate;
    }
    if (candidate < targetTime) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  const left = bars[Math.max(0, high)]?.time ?? targetTime;
  const right = bars[Math.min(bars.length - 1, low)]?.time ?? targetTime;
  return Math.abs(left - targetTime) <= Math.abs(right - targetTime) ? left : right;
}

function parseTradeDateToSeconds(value: string): number | null {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const dateOnly = trimmed.split("T")[0];
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateOnly);
  if (!match) {
    const fallback = Date.parse(trimmed);
    return Number.isFinite(fallback) ? Math.floor(fallback / 1000) : null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  // Anchor at ~mid-day IST (06:30 UTC) so we snap to a sensible intraday bar
  // when timeframe < daily, and a clean daily bar when timeframe >= daily.
  const utcMs = Date.UTC(year, month - 1, day, 6, 30, 0);
  return Number.isFinite(utcMs) ? Math.floor(utcMs / 1000) : null;
}

function findClosestBarTime(bars: ChartBar[], targetTime: number): number | null {
  if (!bars.length) return null;
  let lo = 0;
  let hi = bars.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (bars[mid].time < targetTime) lo = mid + 1;
    else hi = mid;
  }
  const after = bars[lo];
  const before = lo > 0 ? bars[lo - 1] : null;
  if (!before) return after.time;
  return Math.abs(after.time - targetTime) <= Math.abs(before.time - targetTime)
    ? after.time
    : before.time;
}

type SnappedTradeMarker = {
  time: number;
  type: "buy" | "sell";
  entries: Array<{ price: number; qty: number; date: string }>;
};

function findBarIndexAtOrBefore(bars: ChartBar[], targetTime: number) {
  if (!bars.length) {
    return -1;
  }

  let low = 0;
  let high = bars.length - 1;
  let best = -1;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const candidate = bars[mid]?.time ?? targetTime;
    if (candidate <= targetTime) {
      best = mid;
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return best;
}

function barsBetweenTimes(bars: ChartBar[], startTime: number, endTime: number) {
  const from = Math.min(startTime, endTime);
  const to = Math.max(startTime, endTime);
  return bars.filter((bar) => bar.time >= from && bar.time <= to).length;
}

function projectRayEnd(start: { x: number; y: number }, end: { x: number; y: number }, stageWidth: number) {
  const deltaX = end.x - start.x;
  const deltaY = end.y - start.y;
  if (Math.abs(deltaX) < 0.001) {
    return {
      x: end.x,
      y: end.y + (deltaY >= 0 ? 1200 : -1200),
    };
  }

  const targetX = deltaX >= 0 ? stageWidth : 0;
  const slope = deltaY / deltaX;
  return {
    x: targetX,
    y: end.y + slope * (targetX - end.x),
  };
}

function updateKindLabel(kind: CompanyFundamentals["recent_updates"][number]["kind"]) {
  if (kind === "results") {
    return "Results";
  }
  if (kind === "concall") {
    return "Call";
  }
  if (kind === "holding") {
    return "Holding";
  }
  if (kind === "filing") {
    return "Filing";
  }
  return "News";
}

// ─── RVOL helpers ─────────────────────────────────────────────────────────────
type RvolEntry = {
  time: number;
  rvol50: number;
  turnoverRvol50: number;
  volume: number;
  avgVolume: number;
  turnover: number;
  avgTurnover: number;
};

function computeRvolBars(bars: ChartBar[], period = 50): RvolEntry[] {
  const result: RvolEntry[] = [];
  let volumeSum = 0;
  let turnoverSum = 0;

  for (let i = 0; i < period && i < bars.length; i += 1) {
    volumeSum += bars[i].volume;
    turnoverSum += bars[i].volume * bars[i].close;
  }

  for (let i = period; i < bars.length; i++) {
    const avgVolume = volumeSum / period;
    const avgTurnover = turnoverSum / period;
    const bar = bars[i];
    const turnover = bar.volume * bar.close;
    result.push({
      time: bar.time,
      rvol50: avgVolume > 0 ? bar.volume / avgVolume : 0,
      turnoverRvol50: avgTurnover > 0 ? turnover / avgTurnover : 0,
      volume: bar.volume,
      avgVolume,
      turnover,
      avgTurnover,
    });

    const outgoing = bars[i - period];
    volumeSum += bar.volume - outgoing.volume;
    turnoverSum += turnover - (outgoing.volume * outgoing.close);
  }
  return result;
}

function rvolToneColor(rvol: number) {
  if (rvol >= 3) return "#00d2ff";
  if (rvol >= 2) return "#39ff14";
  if (rvol >= 1) return "#ffd36f";
  return "#8b949e";
}

function formatVolumeShort(v: number, market: MarketKey) {
  if (market === "india") {
    if (v >= 1e7) return `${(v / 1e7).toFixed(2)} Cr`;
    if (v >= 1e5) return `${(v / 1e5).toFixed(1)} L`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(1)} K`;
    return `${Math.round(v)}`;
  }
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return `${Math.round(v)}`;
}

function formatTurnoverShort(t: number, market: MarketKey) {
  if (market === "india") {
    const cr = t / 1e7;
    if (cr >= 1000) return `₹${(cr / 1000).toFixed(1)}K Cr`;
    return `₹${cr.toFixed(1)} Cr`;
  }
  if (t >= 1e9) return `$${(t / 1e9).toFixed(2)}B`;
  if (t >= 1e6) return `$${(t / 1e6).toFixed(2)}M`;
  return `$${(t / 1e3).toFixed(1)}K`;
}

function buildBenchmarkOverlaySeries(primaryBars: ChartBar[], benchmarkBars: ChartBar[] | null) {
  if (!benchmarkBars || primaryBars.length < 2 || benchmarkBars.length < 2) {
    return [];
  }

  const startTime = primaryBars[0]?.time;
  const endTime = primaryBars[primaryBars.length - 1]?.time;
  const primaryBase = primaryBars[0]?.close;
  if (!startTime || !endTime || !primaryBase || primaryBase <= 0) {
    return [];
  }

  const scopedBars = benchmarkBars.filter((bar) => bar.time >= startTime && bar.time <= endTime && bar.close > 0);
  if (scopedBars.length < 2) {
    return [];
  }

  const benchmarkBase = scopedBars[0]?.close;
  if (!benchmarkBase || benchmarkBase <= 0) {
    return [];
  }

  return scopedBars.map((bar) => ({
    time: bar.time as UTCTimestamp,
    value: Number((primaryBase * (bar.close / benchmarkBase)).toFixed(4)),
  }));
}

export function ChartPanel({
  market,
  symbol,
  bars,
  rsLine,
  rsLineMarkers,
  earningsMarkers,
  volumeMarkers,
  bandChangeMarkers,
  bandHistory,
  tradeMarkers,
  onSellMarkerClick,
  summary,
  panelTab,
  onPanelTabChange,
  chartError,
  chartLoading,
  chartCacheState,
  fundamentals,
  fundamentalsLoading,
  fundamentalsError,
  groupSummary = null,
  timeframe,
  onTimeframeChange,
  chartStyle,
  onChartStyleChange,
  chartPalette,
  onChartPaletteChange,
  showBenchmarkOverlay,
  onShowBenchmarkOverlayChange,
  indicatorKeys,
  onToggleIndicator,
  chartColors,
  onChartColorsChange,
  drawingColor,
  onDrawingColorChange,
  annotations,
  onAnnotationsChange,
  onAddToWatchlist,
  onRemoveFromWatchlist,
  onAddToJournal,
  searchOptions = [],
  onSearchSymbol,
  onOpenGroup,
  onRefreshChart,
  expanded = false,
}: ChartPanelProps) {
  const searchListId = `chart-search-${useId()}`;
  const stageRef = useRef<HTMLDivElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const interactionLayerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const mainSeriesRef = useRef<any>(null);
  const benchmarkHistoryCacheRef = useRef<Record<string, ChartBar[]>>({});
  const indicatorKeysRef = useRef(indicatorKeys);
  const drawingToolRef = useRef<DrawingTool>("none");
  const draftTrendStartRef = useRef<ChartAnchor | null>(null);
  const pointerDownAnchorRef = useRef<ChartAnchor | null>(null);
  const pointerDownPositionRef = useRef<{ x: number; y: number } | null>(null);
  const pointerMovedRef = useRef(false);
  const annotationsRef = useRef(annotations);
  const onAnnotationsChangeRef = useRef(onAnnotationsChange);
  const onSellMarkerClickRef = useRef(onSellMarkerClick);
  const symbolRef = useRef(symbol);
  const annotationDragRef = useRef<ActiveAnnotationDrag | null>(null);
  const rvolWidgetRef = useRef<HTMLDivElement | null>(null);
  const rvolDragRef = useRef<{ startPX: number; startPY: number; startWX: number; startWY: number } | null>(null);
  const overlayFrameRef = useRef<number | null>(null);
  const crosshairFrameRef = useRef<number | null>(null);
  const pendingCrosshairParamRef = useRef<any>(null);
  const favoritesWidgetDragRef = useRef<{
    mode: "move" | "resize";
    startClientX: number;
    startClientY: number;
    startState: FavoritesWidgetState;
  } | null>(null);
  const floatingWidgetDragRef = useRef<{
    mode: "move" | "resize";
    startClientX: number;
    startClientY: number;
    startState: PocketPivotWidgetState;
  } | null>(null);
  const notesWidgetDragRef = useRef<{
    mode: "move" | "resize";
    startClientX: number;
    startClientY: number;
    startState: NotesWidgetState;
  } | null>(null);
  const earningsWidgetDragRef = useRef<{
    mode: "move" | "resize";
    startClientX: number;
    startClientY: number;
    startState: EarningsWidgetState;
  } | null>(null);
  const [drawingTool, setDrawingTool] = useState<DrawingTool>("none");
  const [draftTrendStart, setDraftTrendStart] = useState<ChartAnchor | null>(null);
  const [hoverAnchor, setHoverAnchor] = useState<ChartAnchor | null>(null);
  const [hoveredRsPoint, setHoveredRsPoint] = useState<ChartLinePoint | null>(null);
  const [hoveredBar, setHoveredBar] = useState<HoveredPriceBar | null>(null);
  const [hoveredTradeMarkers, setHoveredTradeMarkers] = useState<SnappedTradeMarker[]>([]);
  const [chartSearchQuery, setChartSearchQuery] = useState(symbol ?? "");
  const deferredChartSearchQuery = useDeferredValue(chartSearchQuery);
  const [, setOverlayVersion] = useState(0);
  const [selectedAnnotationId, setSelectedAnnotationId] = useState<string | null>(null);
  const [draggingAnnotationHandle, setDraggingAnnotationHandle] = useState<string | null>(null);
  const [extendedHistory, setExtendedHistory] = useState<ChartResponse | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [benchmarkBars, setBenchmarkBars] = useState<ChartBar[] | null>(null);
  const [benchmarkLoading, setBenchmarkLoading] = useState(false);
  const [benchmarkError, setBenchmarkError] = useState<string | null>(null);
  const initialRvolWidget = useMemo(() => readRvolWidgetSettings(), []);
  const [showRvol, setShowRvol] = useState(initialRvolWidget.enabled);
  const [rvolPos, setRvolPos] = useState<{ x: number; y: number } | null>(initialRvolWidget.pos);
  const [rvolAccentColor, setRvolAccentColor] = useState(initialRvolWidget.accentColor);
  const [rvolScale, setRvolScale] = useState<"sm" | "md" | "lg">(initialRvolWidget.scale);
  const [favoritesSettings, setFavoritesSettings] = useState<FavoritesSettings>(() => readFavoritesSettings());
  const [favoritesWidget, setFavoritesWidget] = useState<FavoritesWidgetState>(() => readFavoritesWidgetState());
  const [pocketPivotWidget, setPocketPivotWidget] = useState<PocketPivotWidgetState>(() => readPocketPivotWidgetState());
  const [circuitBandPct, setCircuitBandPct] = useState<number>(() => readCircuitBandPct());
  const [circuitLocksEnabled, setCircuitLocksEnabled] = useState<boolean>(() => readCircuitLocksEnabled());
  const [autoLevelsEnabled, setAutoLevelsEnabled] = useState<boolean>(() => readAutoLevelsEnabled());
  const [aiEnabled, setAiEnabled] = useState<boolean>(() => readAiEnabled());
  // Zen mode: nothing but the chart and the search bar. Persisted so arrow-key
  // navigation to the next stock stays in zen until the user exits.
  const [zenMode, setZenMode] = useState<boolean>(() => {
    try {
      return JSON.parse(window.localStorage.getItem("stockScanner.chartZen.v1") ?? "false") === true;
    } catch {
      return false;
    }
  });

  useEffect(() => {
    try {
      window.localStorage.setItem("stockScanner.chartZen.v1", JSON.stringify(zenMode));
    } catch {
      // Ignore storage failures.
    }
  }, [zenMode]);

  useEffect(() => {
    if (!zenMode) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setZenMode(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [zenMode]);
  const [aiAnalysis, setAiAnalysis] = useState<AiSwingAnalysis | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const aiCacheRef = useRef<Map<string, AiSwingAnalysis>>(new Map());
  const [aiAsOf, setAiAsOf] = useState<string>("");

  useEffect(() => {
    try {
      window.localStorage.setItem(AI_TOGGLE_STORAGE_KEY, JSON.stringify(aiEnabled));
    } catch {
      // Ignore storage failures.
    }
  }, [aiEnabled]);

  useEffect(() => {
    if (!aiEnabled || !symbol) {
      setAiAnalysis(null);
      return;
    }
    const cacheKey = `${symbol}|${aiAsOf || "live"}`;
    const cached = aiCacheRef.current.get(cacheKey);
    if (cached) {
      setAiAnalysis(cached);
      return;
    }
    let active = true;
    setAiLoading(true);
    setAiAnalysis(null);
    getAiSwingAnalysis(symbol, market, aiAsOf || null)
      .then((result) => {
        if (!active) return;
        aiCacheRef.current.set(cacheKey, result);
        setAiAnalysis(result);
      })
      .catch((error: unknown) => {
        if (active) setAiAnalysis({ error: error instanceof Error ? error.message : "AI analysis failed." });
      })
      .finally(() => {
        if (active) setAiLoading(false);
      });
    return () => {
      active = false;
    };
  }, [aiEnabled, symbol, market, aiAsOf]);
  const [notesWidget, setNotesWidget] = useState<NotesWidgetState>(() => readNotesWidgetState());
  const [pocketPivotNotes, setPocketPivotNotes] = useState<PocketPivotNotesMap>(() => readPocketPivotNotes());
  const [earningsWidget, setEarningsWidget] = useState<EarningsWidgetState>(() => readEarningsWidgetState());
  const [earningsSummary, setEarningsSummary] = useState<CompanyEarningsSummary | null>(null);
  const [earningsLoading, setEarningsLoading] = useState(false);
  const [earningsError, setEarningsError] = useState<string | null>(null);
  const [chartRange, setChartRange] = useState<ChartRangeKey>(() => readChartRange());
  const palette = CHART_PALETTES[chartPalette];
  const availableTimeframes = useMemo(() => supportedTimeframes(market), [market]);
  const activeBars = useMemo(() => sanitizeChartBars(extendedHistory?.bars ?? bars), [bars, extendedHistory]);
  // Auto S/R + weekly/monthly demand-supply zones + trendlines, computed from
  // the loaded daily bars (only when the toggle is on, to stay light).
  const autoLevels = useMemo<AutoLevels>(
    () => (autoLevelsEnabled ? computeAutoLevels(activeBars) : { srLevels: [], zones: [], trendlines: [] }),
    [autoLevelsEnabled, activeBars],
  );
  // Real per-date NSE band timeline when known (from the price-band-changes
  // report); empty for symbols with no recorded revisions.
  const circuitBandTimeline = useMemo(
    () => buildBandTimeline(extendedHistory?.band_history ?? bandHistory ?? []),
    [extendedHistory, bandHistory],
  );
  // Whether Auto mode is backed by real NSE band data (vs. heuristic detection).
  const circuitBandFromNse = circuitBandPct === CIRCUIT_BAND_AUTO && circuitBandTimeline.length > 0;
  // null = dynamic-band stock (no fixed limit); 0 = off.
  const resolvedCircuitBandPct = useMemo(() => {
    if (circuitBandPct === CIRCUIT_BAND_AUTO) {
      if (circuitBandTimeline.length > 0) {
        // Current band = the last segment of the real timeline.
        return circuitBandTimeline[circuitBandTimeline.length - 1].bandPct;
      }
      return detectCircuitBandPct(activeBars);
    }
    return circuitBandPct;
  }, [circuitBandPct, circuitBandTimeline, activeBars]);
  const summaryCircuitLimits = useMemo(
    () => computeCircuitLimits(summary ?? null, resolvedCircuitBandPct ?? 0),
    [summary, resolvedCircuitBandPct],
  );
  const safeRsLine = useMemo(() => sanitizeLinePoints(extendedHistory?.rs_line ?? rsLine), [extendedHistory, rsLine]);
  const safeRsLineMarkers = useMemo(
    () => sanitizeLineMarkers(extendedHistory?.rs_line_markers ?? rsLineMarkers),
    [extendedHistory, rsLineMarkers],
  );
  const safeEarningsMarkers = useMemo(
    () => sanitizeLineMarkers(extendedHistory?.earnings_markers ?? earningsMarkers ?? []),
    [extendedHistory, earningsMarkers],
  );
  const safeVolumeMarkers = useMemo(
    () => sanitizeLineMarkers(extendedHistory?.volume_markers ?? volumeMarkers ?? []),
    [extendedHistory, volumeMarkers],
  );
  const safeBandChangeMarkers = useMemo(
    () => sanitizeLineMarkers(extendedHistory?.band_change_markers ?? bandChangeMarkers ?? []),
    [extendedHistory, bandChangeMarkers],
  );
  const snappedTradeMarkers = useMemo<SnappedTradeMarker[]>(() => {
    const list = tradeMarkers ?? [];
    if (!list.length) return [];
    const baseBars = sanitizeChartBars(extendedHistory?.bars ?? bars);
    if (!baseBars.length) return [];
    const groups = new Map<string, SnappedTradeMarker>();
    for (const trade of list) {
      const t = parseTradeDateToSeconds(trade.date);
      if (t === null) continue;
      const snapped = findClosestBarTime(baseBars, t);
      if (snapped === null) continue;
      const key = `${snapped}|${trade.type}`;
      let group = groups.get(key);
      if (!group) {
        group = { time: snapped, type: trade.type, entries: [] };
        groups.set(key, group);
      }
      group.entries.push({ price: trade.price, qty: trade.qty, date: trade.date });
    }
    return Array.from(groups.values()).sort((a, b) => a.time - b.time);
  }, [tradeMarkers, extendedHistory, bars]);
  const safeBenchmarkBars = useMemo(() => sanitizeChartBars(benchmarkBars ?? []), [benchmarkBars]);
  const pocketPivotBars = useMemo(() => computePocketPivotBars(activeBars), [activeBars]);
  const latestPocketPivot = pocketPivotBars[pocketPivotBars.length - 1] ?? null;
  const normalizedSymbol = symbol?.trim().toUpperCase() ?? "";
  const pocketPivotNote = normalizedSymbol ? pocketPivotNotes[normalizedSymbol] ?? "" : "";
  const earningsSource = fundamentals?.symbol === symbol ? fundamentals : earningsSummary?.symbol === symbol ? earningsSummary : null;
  const visibleQuarterlyResults = sortQuarterlyResultsLatestFirst(earningsSource?.quarterly_results ?? []).slice(0, earningsWidget.quarters);
  const earningsValuation = earningsSource?.valuation ?? null;
  const earningsMetrics = earningsSummary?.symbol === symbol
    ? earningsSummary.metrics
    : {
        pct_from_52w_high: summary?.pct_from_52w_high,
        pct_from_52w_low: summary?.pct_from_52w_low,
        adr_pct_20: summary?.adr_pct_20,
        relative_volume: summary?.relative_volume,
        turnover_1d_crore: null,
        avg_turnover_50d_crore: summary?.avg_rupee_volume_30d_crore,
      };
  const earningsTitle = earningsSource
    ? `${earningsSource.sector ?? "Unclassified"}↔${earningsSource.sub_sector ?? "Unclassified"}`
    : "Earnings";
  const futureWhitespaceTimes = useMemo(
    () => buildFutureWhitespaceTimes(activeBars, timeframe, FUTURE_DRAW_EXTENSION_BARS),
    [activeBars, timeframe],
  );
  const benchmarkSymbol = null;
  const canShowBenchmarkOverlay = false;
  const benchmarkOverlayData = useMemo(
    () => (showBenchmarkOverlay ? buildBenchmarkOverlaySeries(activeBars, safeBenchmarkBars) : []),
    [activeBars, safeBenchmarkBars, showBenchmarkOverlay],
  );
  const rvolData = useMemo(() => (showRvol ? computeRvolBars(activeBars, 50) : []), [activeBars, showRvol]);
  const currentRvol = useMemo<RvolEntry | null>(() => {
    if (!rvolData.length) return null;
    if (hoveredBar) {
      for (let index = rvolData.length - 1; index >= 0; index -= 1) {
        const entry = rvolData[index];
        if (entry.time <= hoveredBar.time) {
          return entry;
        }
      }
      return rvolData[rvolData.length - 1];
    }
    return rvolData[rvolData.length - 1];
  }, [rvolData, hoveredBar]);
  const formatValue = (value: number | null | undefined, digits = 2) => formatNumber(value, digits, market);
  const formatSignedPercentValue = (value: number | null | undefined) => formatPercent(value, market);
  const formatPercentValue = (value: number | null | undefined) => formatPlainPercent(value, market);
  const scheduleOverlayUpdate = () => {
    if (overlayFrameRef.current !== null) {
      return;
    }
    overlayFrameRef.current = window.requestAnimationFrame(() => {
      overlayFrameRef.current = null;
      setOverlayVersion((version) => version + 1);
    });
  };

  useEffect(() => {
    if (!availableTimeframes.includes(timeframe)) {
      onTimeframeChange(availableTimeframes[0] ?? "1D");
    }
  }, [availableTimeframes, onTimeframeChange, timeframe]);
  const formatAmountValue = (value: number | null | undefined, digits?: number) => formatCrore(value, market, digits);
  const formatPriceValue = (value: number | null | undefined, digits = 2) => formatPrice(value, market, digits);
  const formatCountValue = (value: number | null | undefined) => formatCount(value, market);
  const ownershipLabels = market === "india"
    ? {
        title: "Promoter / FII / DII Activity",
        description: "Latest shareholding pattern and quarter-on-quarter change.",
        promoter: "Promoter",
        fii: "FII",
        dii: "DII",
        promoterChange: "Promoter Change",
        fiiChange: "FII Change",
        diiChange: "DII Change",
      }
    : {
        title: "Ownership Activity",
        description: "Latest ownership mix and quarter-on-quarter change.",
        promoter: "Insiders",
        fii: "Foreign",
        dii: "Domestic",
        promoterChange: "Insider Change",
        fiiChange: "Foreign Change",
        diiChange: "Domestic Change",
      };

  const switchDrawingTool = (nextTool: DrawingTool) => {
    setDraftTrendStart(null);
    setHoverAnchor(null);
    setDrawingTool(nextTool);
  };

  indicatorKeysRef.current = indicatorKeys;

  useEffect(() => {
    drawingToolRef.current = drawingTool;
  }, [drawingTool]);

  useEffect(() => {
    draftTrendStartRef.current = draftTrendStart;
  }, [draftTrendStart]);

  useEffect(() => {
    annotationsRef.current = annotations;
    scheduleOverlayUpdate();
  }, [annotations]);

  useEffect(() => {
    onAnnotationsChangeRef.current = onAnnotationsChange;
  }, [onAnnotationsChange]);

  useEffect(() => {
    onSellMarkerClickRef.current = onSellMarkerClick;
  }, [onSellMarkerClick]);

  useEffect(() => {
    symbolRef.current = symbol;
  }, [symbol]);

  useEffect(() => {
    try {
      window.localStorage.setItem(CHART_FAVORITES_STORAGE_KEY, JSON.stringify(favoritesSettings));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [favoritesSettings]);

  useEffect(() => {
    try {
      window.localStorage.setItem(FAVORITES_WIDGET_STORAGE_KEY, JSON.stringify(favoritesWidget));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [favoritesWidget]);

  useEffect(() => {
    try {
      window.localStorage.setItem(POCKET_PIVOT_STORAGE_KEY, JSON.stringify(pocketPivotWidget));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [pocketPivotWidget]);

  useEffect(() => {
    try {
      window.localStorage.setItem(CIRCUIT_WIDGET_STORAGE_KEY, JSON.stringify(circuitBandPct));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [circuitBandPct]);

  useEffect(() => {
    try {
      window.localStorage.setItem(CIRCUIT_LOCKS_STORAGE_KEY, JSON.stringify(circuitLocksEnabled));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [circuitLocksEnabled]);

  useEffect(() => {
    try {
      window.localStorage.setItem(AUTO_LEVELS_STORAGE_KEY, JSON.stringify(autoLevelsEnabled));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [autoLevelsEnabled]);

  useEffect(() => {
    try {
      window.localStorage.setItem(NOTES_WIDGET_STORAGE_KEY, JSON.stringify(notesWidget));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [notesWidget]);

  useEffect(() => {
    try {
      window.localStorage.setItem(POCKET_PIVOT_NOTES_STORAGE_KEY, JSON.stringify(pocketPivotNotes));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [pocketPivotNotes]);

  useEffect(() => {
    try {
      window.localStorage.setItem(EARNINGS_WIDGET_STORAGE_KEY, JSON.stringify(earningsWidget));
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [earningsWidget]);

  // Keyboard shortcuts for swapping chart range without taking hands off
  // the keyboard. Skip when focus is in a text input so typing isn't
  // hijacked, and require no Cmd/Ctrl/Meta/Alt so browser combos still
  // work (Cmd+A select-all, Ctrl+F find, etc.).
  useEffect(() => {
    const SHORTCUT_TO_RANGE: Record<string, ChartRangeKey> = {
      a: "3M",
      s: "6M",
      d: "1Y",
      f: "FULL",
    };
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey) {
        return;
      }
      const target = event.target as HTMLElement | null;
      if (target) {
        const tagName = target.tagName;
        if (
          tagName === "INPUT"
          || tagName === "TEXTAREA"
          || tagName === "SELECT"
          || target.isContentEditable
        ) {
          return;
        }
      }
      const range = SHORTCUT_TO_RANGE[event.key.toLowerCase()];
      if (!range) {
        return;
      }
      event.preventDefault();
      setChartRange(range);
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(CHART_RANGE_STORAGE_KEY, chartRange);
    } catch {
      // Ignore private browsing/storage quota failures.
    }
    const chart = chartRef.current;
    if (!chart || activeBars.length === 0) {
      return;
    }
    const rangeBars = barsForChartRange(timeframe, chartRange);
    if (chartRange === "FULL" || rangeBars === null) {
      chart.timeScale().fitContent();
      chart.timeScale().scrollToPosition(RIGHT_EDGE_PADDING_BARS, false);
      return;
    }
    const endIndex = Math.max(0, activeBars.length - 1);
    const startIndex = Math.max(0, activeBars.length - rangeBars);
    if (activeBars.length > rangeBars) {
      chart.timeScale().setVisibleLogicalRange({
        from: startIndex,
        to: endIndex + RIGHT_EDGE_PADDING_BARS,
      });
    } else {
      chart.timeScale().fitContent();
      chart.timeScale().scrollToPosition(RIGHT_EDGE_PADDING_BARS, false);
    }
  }, [chartRange, activeBars, timeframe]);

  useEffect(() => {
    if (!earningsWidget.enabled || !symbol || earningsSource) {
      setEarningsLoading(false);
      return;
    }

    let active = true;
    setEarningsLoading(true);
    setEarningsError(null);

    void getEarningsSummary(symbol, market)
      .then((payload) => {
        if (!active || payload.symbol !== symbol) {
          return;
        }
        setEarningsSummary(payload);
        setEarningsError(null);
      })
      .catch((error: unknown) => {
        if (active) {
          setEarningsError(error instanceof Error ? error.message : "Failed to load earnings");
        }
      })
      .finally(() => {
        if (active) {
          setEarningsLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [earningsSource, earningsWidget.enabled, market, symbol]);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        RVOL_WIDGET_STORAGE_KEY,
        JSON.stringify({
          enabled: showRvol,
          pos: rvolPos,
          accentColor: rvolAccentColor,
          scale: rvolScale,
        } satisfies RvolWidgetSettings),
      );
    } catch {
      // Ignore private browsing/storage quota failures.
    }
  }, [rvolAccentColor, rvolPos, rvolScale, showRvol]);

  useEffect(() => {
    return () => {
      if (overlayFrameRef.current !== null) {
        window.cancelAnimationFrame(overlayFrameRef.current);
      }
      if (crosshairFrameRef.current !== null) {
        window.cancelAnimationFrame(crosshairFrameRef.current);
      }
    };
  }, []);

  useEffect(() => {
    setDrawingTool("none");
    setDraftTrendStart(null);
    setHoverAnchor(null);
    setHoveredRsPoint(null);
    setHoveredBar(null);
    setHoveredTradeMarkers([]);
    setSelectedAnnotationId(null);
    annotationDragRef.current = null;
    setDraggingAnnotationHandle(null);
    setExtendedHistory(null);
    setEarningsSummary(null);
    setEarningsError(null);
  }, [symbol, timeframe]);

  useEffect(() => {
    if (canShowBenchmarkOverlay) {
      return;
    }
    if (showBenchmarkOverlay) {
      onShowBenchmarkOverlayChange(false);
    }
    setBenchmarkLoading(false);
    setBenchmarkError(null);
  }, [canShowBenchmarkOverlay, onShowBenchmarkOverlayChange, showBenchmarkOverlay]);

  useEffect(() => {
    if (!showBenchmarkOverlay || !benchmarkSymbol || !symbol || symbol === benchmarkSymbol || panelTab !== "technical") {
      setBenchmarkLoading(false);
      setBenchmarkError(null);
      return;
    }

    const cacheKey = `${market}:${benchmarkSymbol}:${timeframe}`;
    const cached = benchmarkHistoryCacheRef.current[cacheKey];
    if (cached) {
      setBenchmarkBars(cached);
      setBenchmarkLoading(false);
      setBenchmarkError(null);
      return;
    }

    let active = true;
    setBenchmarkBars(null);
    setBenchmarkLoading(true);
    setBenchmarkError(null);

    void getChartHistory(benchmarkSymbol, timeframe, market)
      .then((payload) => {
        if (!active) {
          return;
        }
        benchmarkHistoryCacheRef.current[cacheKey] = payload.bars;
        setBenchmarkBars(payload.bars);
        setBenchmarkError(null);
      })
      .catch((error: unknown) => {
        if (!active) {
          return;
        }
        setBenchmarkError(error instanceof Error ? error.message : `Failed to load ${benchmarkSymbol} overlay.`);
      })
      .finally(() => {
        if (active) {
          setBenchmarkLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [benchmarkSymbol, market, panelTab, showBenchmarkOverlay, symbol, timeframe]);

  useEffect(() => {
    setChartSearchQuery(symbol ?? "");
  }, [symbol]);

  const anchorFromPointer = (clientX: number, clientY: number, rectOverride?: DOMRect): ChartAnchor | null => {
    const stage = stageRef.current;
    const container = containerRef.current;
    const chart = chartRef.current;
    const mainSeries = mainSeriesRef.current;
    if (!stage || !container || !chart || !mainSeries) {
      return null;
    }

    const rect = rectOverride ?? interactionLayerRef.current?.getBoundingClientRect() ?? container.getBoundingClientRect();
    const x = clientX - rect.left;
    const y = clientY - rect.top;
    const rawTime = (chart.timeScale() as any).coordinateToTime(x);
    const rawPrice = mainSeries.coordinateToPrice(y);
    const time = normalizeChartTime(rawTime);

    if (time === null || rawPrice === null || rawPrice === undefined) {
      return null;
    }

    const lastBarTime = activeBars[activeBars.length - 1]?.time ?? null;
    const snappedTime = lastBarTime !== null && time > lastBarTime ? time : nearestBarTime(activeBars, time);

    return {
      time: snappedTime,
      price: Number(rawPrice.toFixed(2)),
    };
  };

  const commitDrawingAnchor = (anchor: ChartAnchor) => {
    const tool = drawingToolRef.current;
    if (tool === "none") {
      return;
    }

    if (tool === "hline") {
      onAnnotationsChangeRef.current([
        ...annotationsRef.current,
        {
          id: buildId(),
          type: "hline",
          point: anchor,
          color: drawingColor,
        },
      ]);
      setHoverAnchor(null);
      setDrawingTool("none");
      return;
    }

    if (tool === "vline") {
      onAnnotationsChangeRef.current([
        ...annotationsRef.current,
        {
          id: buildId(),
          type: "vline",
          point: anchor,
          color: drawingColor,
        },
      ]);
      setHoverAnchor(null);
      setDrawingTool("none");
      return;
    }

    if (isTwoPointTool(tool)) {
      const draft = draftTrendStartRef.current;
      if (!draft) {
        setDraftTrendStart(anchor);
        return;
      }

      onAnnotationsChangeRef.current([
        ...annotationsRef.current,
        {
          id: buildId(),
          type: tool,
          start: draft,
          end: anchor,
          color: drawingColor,
        } as Extract<ChartAnnotation, { type: "trendline" | "ray" | "rectangle" | "measure" }>,
      ]);
      setDraftTrendStart(null);
      setHoverAnchor(null);
      setDrawingTool("none");
      return;
    }

    const note = window.prompt("Add note to chart");
    if (!note || !note.trim()) {
      return;
    }

    onAnnotationsChangeRef.current([
      ...annotationsRef.current,
      {
        id: buildId(),
        type: "text",
        point: anchor,
        text: note.trim(),
        color: drawingColor,
      },
    ]);
    setHoverAnchor(null);
    setDrawingTool("none");
  };

  const handleStagePointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.setPointerCapture?.(event.pointerId);
    const anchor = anchorFromPointer(event.clientX, event.clientY, event.currentTarget.getBoundingClientRect());
    if (!anchor) {
      return;
    }
    pointerDownAnchorRef.current = anchor;
    pointerDownPositionRef.current = { x: event.clientX, y: event.clientY };
    pointerMovedRef.current = false;
  };

  const handleStagePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (pointerDownPositionRef.current) {
      const deltaX = event.clientX - pointerDownPositionRef.current.x;
      const deltaY = event.clientY - pointerDownPositionRef.current.y;
      if (Math.hypot(deltaX, deltaY) > 4) {
        pointerMovedRef.current = true;
      }
    }

    if (!isTwoPointTool(drawingToolRef.current) || !draftTrendStartRef.current) {
      setHoverAnchor(null);
      return;
    }

    const anchor = anchorFromPointer(event.clientX, event.clientY, event.currentTarget.getBoundingClientRect());
    if (!anchor) {
      setHoverAnchor(null);
      return;
    }

    setHoverAnchor(anchor);
  };

  const handleStagePointerLeave = () => {
    if (isTwoPointTool(drawingToolRef.current) && !pointerDownPositionRef.current) {
      setHoverAnchor(null);
    }
  };

  const handleStagePointerUp = (event: ReactPointerEvent<HTMLDivElement>) => {
    event.currentTarget.releasePointerCapture?.(event.pointerId);
    const downAnchor = pointerDownAnchorRef.current;
    pointerDownAnchorRef.current = null;
    pointerDownPositionRef.current = null;

    if (!downAnchor || pointerMovedRef.current) {
      pointerMovedRef.current = false;
      return;
    }

    pointerMovedRef.current = false;
    const anchor = anchorFromPointer(event.clientX, event.clientY, event.currentTarget.getBoundingClientRect()) ?? downAnchor;
    commitDrawingAnchor(anchor);
  };

  useEffect(() => {
    if (panelTab !== "technical" || !containerRef.current) {
      return;
    }

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: palette.background },
        textColor: palette.textColor,
      },
      grid: {
        // Editorial style: plain background, no grid lines in either theme.
        vertLines: { visible: false },
        horzLines: { visible: false },
      },
      crosshair: {
        vertLine: { color: palette.crosshairColor },
        horzLine: { color: palette.crosshairColor },
      },
      leftPriceScale: {
        visible: false,
        borderColor: palette.borderColor,
      },
      rightPriceScale: {
        borderColor: palette.borderColor,
      },
      timeScale: {
        borderColor: palette.borderColor,
        timeVisible: timeframe === "15m" || timeframe === "30m" || timeframe === "1h",
        rightOffset: RIGHT_EDGE_PADDING_BARS,
      },
    });

    const mainSeries =
      chartStyle === "bars"
        ? chart.addBarSeries({
            // TradingView-classic OHLC bars: thin sticks, blue up / red down.
            upColor: "#2962ff",
            downColor: "#f23645",
            thinBars: true,
          })
        : chart.addCandlestickSeries({
            upColor: chartColors.candleUp,
            downColor: chartColors.candleDown,
            wickUpColor: chartColors.candleUp,
            wickDownColor: chartColors.candleDown,
            borderVisible: false,
          });
    mainSeries.priceScale().applyOptions({
      scaleMargins: safeRsLine.length ? { top: 0.04, bottom: 0.32 } : { top: 0.04, bottom: 0.18 },
    });

    const volumeSeries = chart.addHistogramSeries({
      color: "#00d2ff",
      priceScaleId: "",
      priceFormat: { type: "volume" },
    });
    const volumeSmaSeries = chart.addLineSeries({
      color: withOpacity(chartColors.volumeUp, 0.92),
      lineWidth: 2,
      priceScaleId: "",
      priceFormat: { type: "volume" },
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: safeRsLine.length ? 0.88 : 0.82, bottom: 0 },
    });

    const ohlcvData = [
      ...activeBars.map((bar) => ({
        time: bar.time as UTCTimestamp,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      })),
      ...futureWhitespaceTimes.map((time) => ({ time: time as UTCTimestamp })),
    ];

    mainSeries.setData(ohlcvData);

    // Circuit limits are shown in the Circuit Limits chip (values) and as
    // optional UC/LC lock-day markers — no lines are drawn on the chart, by
    // request, to keep the editorial look.
    const combinedMarkers: any[] = [];
    if (pocketPivotWidget.enabled) {
      for (const bar of pocketPivotBars) {
        combinedMarkers.push({
          time: bar.time as UTCTimestamp,
          position: "belowBar",
          shape: "circle",
          color: pocketPivotWidget.dotColor,
          text: "",
          size: pocketPivotWidget.dotSize,
        });
      }
    }
    // Earnings "E" pip — one per result announcement day within the
    // currently-loaded window.
    for (const marker of safeEarningsMarkers) {
      combinedMarkers.push({
        time: marker.time as UTCTimestamp,
        position: "aboveBar",
        shape: "circle",
        color: marker.color || "#f59e0b",
        text: marker.label || "E",
        size: 1.2,
      });
    }
    // NSE price-band revisions — the NEW band % (e.g. "5%") sits on top of
    // the candle of the day the limit changed, per the price-band-changes report.
    for (const marker of safeBandChangeMarkers) {
      combinedMarkers.push({
        time: marker.time as UTCTimestamp,
        position: "aboveBar",
        shape: "square",
        color: marker.color || "#f7b955",
        text: marker.label || "",
        size: 0.6,
      });
    }
    // (Volume-push HQV/HHV/HYV pips are attached to the volume histogram
    // series below, not the price candles.)
    // Trade journal "B"/"S" markers — one per buy/sell on the chart's symbol.
    for (const marker of snappedTradeMarkers) {
      combinedMarkers.push({
        time: marker.time as UTCTimestamp,
        position: marker.type === "buy" ? "belowBar" : "aboveBar",
        shape: marker.type === "buy" ? "arrowUp" : "arrowDown",
        color: marker.type === "buy" ? "#10b981" : "#ef4444",
        text: marker.type === "buy" ? "B" : "S",
        size: 1.5,
      });
    }
    // Circuit-lock markers: a "UC"/"LC" pip on the days the stock closed at
    // its band (per the real per-date band timeline when known). Toggleable
    // from the Circuit Limits chip. Daily bands only make sense on 1D.
    if (
      circuitLocksEnabled &&
      timeframe === "1D" &&
      activeBars.length > 2 &&
      (circuitBandFromNse || (resolvedCircuitBandPct != null && resolvedCircuitBandPct > 0))
    ) {
      const circuitLevels = computeCircuitLevelSeries(
        activeBars,
        resolvedCircuitBandPct ?? 0,
        circuitBandFromNse ? circuitBandTimeline : [],
      );
      for (const lockTime of circuitLevels.ucLockTimes) {
        combinedMarkers.push({
          time: lockTime,
          position: "aboveBar",
          shape: "arrowUp",
          color: "#22c55e",
          text: "UC",
          size: 1,
        });
      }
      for (const lockTime of circuitLevels.lcLockTimes) {
        combinedMarkers.push({
          time: lockTime,
          position: "belowBar",
          shape: "arrowDown",
          color: "#ef4444",
          text: "LC",
          size: 1,
        });
      }
    }
    // lightweight-charts requires markers sorted by time.
    combinedMarkers.sort((left, right) => Number(left.time) - Number(right.time));
    mainSeries.setMarkers(combinedMarkers);
    if (benchmarkOverlayData.length) {
      const benchmarkSeries = chart.addLineSeries({
        color: "#ffb347",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      benchmarkSeries.setData(benchmarkOverlayData);
    }
    volumeSeries.setData(
      activeBars.map((bar) => ({
        time: bar.time as UTCTimestamp,
        value: bar.volume,
        color: bar.close >= bar.open ? withOpacity(chartColors.volumeUp, 0.38) : withOpacity(chartColors.volumeDown, 0.35),
      })),
    );
    // Volume-push "HQV"/"HHV"/"HYV" pips ride the VOLUME histogram (not the
    // price candles) — placed above the volume bar of the day the stock pushed
    // a new Quarterly/Half-yearly/Yearly volume high.
    if (safeVolumeMarkers.length) {
      volumeSeries.setMarkers(
        safeVolumeMarkers
          .map((marker) => ({
            time: marker.time as UTCTimestamp,
            position: "aboveBar" as const,
            shape: "circle" as const,
            color: marker.color || "#16a34a",
            text: marker.label || "HQV",
            size: 1.4,
          }))
          .sort((left, right) => Number(left.time) - Number(right.time)),
      );
    } else {
      volumeSeries.setMarkers([]);
    }
    volumeSmaSeries.setData(computeVolumeSma(activeBars, 50));

    let rsSeries: any = null;
    if (safeRsLine.length) {
      rsSeries = chart.addLineSeries({
        color: chartColors.rsLine,
        lineWidth: 2,
        priceScaleId: "rs-rating",
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: true,
      });
      rsSeries.priceScale().applyOptions({
        visible: false,
        scaleMargins: { top: 0.72, bottom: 0.14 },
      });
      rsSeries.setData(
        safeRsLine.map((point) => ({
          time: point.time as UTCTimestamp,
          value: point.value,
        })),
      );
      rsSeries.setMarkers(
        safeRsLineMarkers.map((marker) => ({
          time: marker.time as UTCTimestamp,
          position: "inBar",
          shape: "circle",
          color: chartColors.rsMarker,
          text: "",
          size: chartColors.rsMarkerSize,
        })),
      );
    }

    for (const indicator of INDICATORS) {
      if (!indicatorKeysRef.current.includes(indicator.key)) {
        continue;
      }

      const lineSeries = chart.addLineSeries({
        color: chartColors[indicator.colorKey],
        lineWidth: indicator.key === "ema200" ? 2 : 1,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });

      lineSeries.setData(
        indicator.key === "ema10"
          ? computeEma(activeBars, 10)
          : indicator.key === "ema20"
            ? computeEma(activeBars, 20)
            : indicator.key === "ema50"
              ? computeEma(activeBars, 50)
            : indicator.key === "ema200"
              ? computeSma(activeBars, 200)
              : computeVwap(activeBars),
      );
    }

    // Auto strong support/resistance — native price lines (axis-tagged,
    // full-width, pan-safe). Zones + trendlines are drawn in the SVG overlay.
    if (autoLevelsEnabled) {
      for (const level of autoLevels.srLevels) {
        const isSupport = level.kind === "support";
        mainSeries.createPriceLine({
          price: level.price,
          color: isSupport ? "#22c55e" : "#ef4444",
          lineWidth: 1,
          lineStyle: 2, // dashed
          axisLabelVisible: true,
          title: isSupport ? "S" : "R",
        });
      }
    }

    const updateOverlay = () => {
      scheduleOverlayUpdate();
    };
    const processCrosshairMove = (param: any) => {
      if (!param?.time) {
        setHoveredRsPoint(null);
        setHoveredBar(null);
        setHoveredTradeMarkers([]);
        return;
      }

      const hoveredTime = normalizeChartTime(param.time);
      if (hoveredTime === null) {
        setHoveredRsPoint(null);
        setHoveredBar(null);
        setHoveredTradeMarkers([]);
        return;
      }

      const matchedTradeMarkers = snappedTradeMarkers.filter((m) => m.time === hoveredTime);
      setHoveredTradeMarkers(matchedTradeMarkers);

      const priceData = param.seriesData?.get?.(mainSeries) as
        | {
            open?: number;
            high?: number;
            low?: number;
            close?: number;
          }
        | undefined;
      if (
        priceData &&
        typeof priceData.open === "number" &&
        typeof priceData.high === "number" &&
        typeof priceData.low === "number" &&
        typeof priceData.close === "number"
      ) {
        const barIndex = findBarIndexAtOrBefore(activeBars, hoveredTime);
        const previousClose = barIndex > 0 ? activeBars[barIndex - 1]?.close ?? null : null;
        const changeValue = previousClose === null ? null : Number(priceData.close) - previousClose;
        const changePct = previousClose && previousClose !== 0 && changeValue !== null ? (changeValue / previousClose) * 100 : null;
        setHoveredBar({
          time: hoveredTime,
          open: Number(priceData.open),
          high: Number(priceData.high),
          low: Number(priceData.low),
          close: Number(priceData.close),
          changeValue,
          changePct,
        });
      } else {
        const fallbackIndex = findBarIndexAtOrBefore(activeBars, hoveredTime);
        const fallbackBar = fallbackIndex >= 0 ? activeBars[fallbackIndex] : null;
        const previousClose = fallbackIndex > 0 ? activeBars[fallbackIndex - 1]?.close ?? null : null;
        const changeValue = fallbackBar && previousClose !== null ? fallbackBar.close - previousClose : null;
        const changePct = changeValue !== null && previousClose && previousClose !== 0 ? (changeValue / previousClose) * 100 : null;
        setHoveredBar(
          fallbackBar
            ? {
                time: fallbackBar.time,
                open: fallbackBar.open,
                high: fallbackBar.high,
                low: fallbackBar.low,
                close: fallbackBar.close,
                changeValue,
                changePct,
              }
            : null,
        );
      }

      if (!rsSeries) {
        setHoveredRsPoint(null);
        return;
      }

      const seriesData = param.seriesData?.get?.(rsSeries) as { value?: number } | undefined;
      if (seriesData?.value !== undefined) {
        setHoveredRsPoint({
          time: hoveredTime,
          value: Number(seriesData.value),
        });
        return;
      }

      const fallbackPoint = [...safeRsLine].reverse().find((point) => point.time <= hoveredTime) ?? null;
      setHoveredRsPoint(fallbackPoint);
    };
    const handleCrosshairMove = (param: any) => {
      pendingCrosshairParamRef.current = param;
      if (crosshairFrameRef.current !== null) {
        return;
      }
      crosshairFrameRef.current = window.requestAnimationFrame(() => {
        crosshairFrameRef.current = null;
        processCrosshairMove(pendingCrosshairParamRef.current);
      });
    };

    const handleChartClick = (param: any) => {
      if (drawingToolRef.current !== "none") return;
      const onClickSell = onSellMarkerClickRef.current;
      if (!onClickSell) return;
      if (!param?.time || !param?.point) return;
      const clickedTime = normalizeChartTime(param.time);
      if (clickedTime === null) return;
      const sellMarker = snappedTradeMarkers.find(
        (m) => m.time === clickedTime && m.type === "sell",
      );
      if (!sellMarker) return;
      const bar = activeBars[findBarIndexAtOrBefore(activeBars, clickedTime)];
      if (!bar) return;
      const barHighY = mainSeries.priceToCoordinate(bar.high);
      if (typeof barHighY === "number" && Number.isFinite(barHighY)) {
        // S markers sit above the bar — click must be near/above the bar's high.
        // 24px tolerance keeps clicks forgiving without intercepting clicks
        // on B markers further down the candle.
        if (param.point.y > barHighY + 24) return;
      }
      const exitDate = sellMarker.entries[0]?.date;
      const sym = symbolRef.current;
      if (!exitDate || !sym) return;
      onClickSell(sym, exitDate);
    };

    chart.timeScale().subscribeVisibleLogicalRangeChange(updateOverlay);
    chart.subscribeCrosshairMove(handleCrosshairMove);
    chart.subscribeClick(handleChartClick);

    const rangeBars = barsForChartRange(timeframe, chartRange);
    const visibleBars = rangeBars ?? defaultVisibleBars(timeframe);
    const endIndex = Math.max(0, activeBars.length - 1);
    const startIndex = Math.max(0, activeBars.length - visibleBars);
    if (chartRange === "FULL") {
      chart.timeScale().fitContent();
      chart.timeScale().scrollToPosition(RIGHT_EDGE_PADDING_BARS, false);
    } else if (activeBars.length > visibleBars) {
      chart.timeScale().setVisibleLogicalRange({
        from: startIndex,
        to: endIndex + RIGHT_EDGE_PADDING_BARS,
      });
    } else {
      chart.timeScale().fitContent();
      chart.timeScale().scrollToPosition(RIGHT_EDGE_PADDING_BARS, false);
    }

    chartRef.current = chart;
    mainSeriesRef.current = mainSeries;
    updateOverlay();

    return () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(updateOverlay);
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
      chart.unsubscribeClick(handleChartClick);
      chart.remove();
      chartRef.current = null;
      mainSeriesRef.current = null;
    };
  }, [activeBars, benchmarkOverlayData, chartColors, chartPalette, chartStyle, futureWhitespaceTimes, indicatorKeys, panelTab, palette.background, palette.borderColor, palette.crosshairColor, palette.gridColor, palette.textColor, pocketPivotBars, pocketPivotWidget.dotColor, pocketPivotWidget.dotSize, pocketPivotWidget.enabled, safeEarningsMarkers, safeVolumeMarkers, safeBandChangeMarkers, safeRsLine, safeRsLineMarkers, snappedTradeMarkers, resolvedCircuitBandPct, circuitBandFromNse, circuitBandTimeline, circuitLocksEnabled, autoLevelsEnabled, autoLevels.srLevels, timeframe]);

  useEffect(() => {
    const handleResize = () => {
      scheduleOverlayUpdate();
    };

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (!chartRef.current) return;
    const isDrawing = drawingTool !== "none";
    chartRef.current.applyOptions({
      handleScroll: isDrawing
        ? false
        : {
            mouseWheel: true,
            pressedMouseMove: true,
            horzTouchDrag: true,
            vertTouchDrag: true,
          },
      handleScale: isDrawing
        ? false
        : {
            mouseWheel: true,
            pinch: true,
            axisPressedMouseMove: {
              time: true,
              price: true,
            },
          },
    });
  }, [drawingTool]);

  const handleRvolDragStart = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    event.preventDefault();
    event.stopPropagation();
    const widget = rvolWidgetRef.current;
    const stage = stageRef.current;
    if (!widget || !stage) return;
    const widgetRect = widget.getBoundingClientRect();
    const stageRect = stage.getBoundingClientRect();
    rvolDragRef.current = {
      startPX: event.clientX,
      startPY: event.clientY,
      startWX: widgetRect.left - stageRect.left,
      startWY: widgetRect.top - stageRect.top,
    };
    const onMove = (ev: PointerEvent) => {
      if (!rvolDragRef.current) return;
      const stageNow = stageRef.current;
      if (!stageNow) return;
      const stageRectNow = stageNow.getBoundingClientRect();
      const dx = ev.clientX - rvolDragRef.current.startPX;
      const dy = ev.clientY - rvolDragRef.current.startPY;
      const newX = Math.max(0, Math.min(rvolDragRef.current.startWX + dx, stageRectNow.width - 40));
      const newY = Math.max(0, Math.min(rvolDragRef.current.startWY + dy, stageRectNow.height - 40));
      setRvolPos({ x: newX, y: newY });
    };
    const onUp = () => {
      rvolDragRef.current = null;
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  };

  const handleLoadFullHistory = async () => {
    if (!symbol || historyLoading) return;
    if (extendedHistory) {
      setExtendedHistory(null);
      return;
    }
    setHistoryLoading(true);
    try {
      const result = await getChartHistory(symbol, timeframe, market);
      if (result.bars.length > 0) {
        setExtendedHistory(result);
      }
    } catch {
      // silently ignore
    } finally {
      setHistoryLoading(false);
    }
  };

  const toggleFavoriteItem = (itemId: FavoriteItemId) => {
    setFavoritesSettings((current) => ({
      ...current,
      itemIds: current.itemIds.includes(itemId)
        ? current.itemIds.filter((id) => id !== itemId)
        : [...current.itemIds, itemId],
    }));
  };

  const runFavoriteItem = (itemId: FavoriteItemId) => {
    if (itemId.startsWith("tool:")) {
      switchDrawingTool(itemId.slice("tool:".length) as DrawingTool);
      return;
    }
    if (itemId.startsWith("indicator:")) {
      onToggleIndicator(itemId.slice("indicator:".length) as IndicatorKey);
      return;
    }
    if (itemId === "overlay:rvol") {
      setShowRvol((current) => !current);
      return;
    }
    if (itemId === "overlay:pocket-pivot") {
      setPocketPivotWidget((current) => ({ ...current, enabled: !current.enabled }));
      return;
    }
    if (itemId === "overlay:earnings") {
      setEarningsWidget((current) => ({ ...current, enabled: !current.enabled }));
      return;
    }
    void handleLoadFullHistory();
  };

  const beginPocketPivotWidgetDrag = (event: ReactPointerEvent<HTMLDivElement>, mode: "move" | "resize") => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    floatingWidgetDragRef.current = {
      mode,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startState: pocketPivotWidget,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const beginNotesWidgetDrag = (event: ReactPointerEvent<HTMLDivElement>, mode: "move" | "resize") => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    notesWidgetDragRef.current = {
      mode,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startState: notesWidget,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const beginEarningsWidgetDrag = (event: ReactPointerEvent<HTMLDivElement>, mode: "move" | "resize") => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    earningsWidgetDragRef.current = {
      mode,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startState: earningsWidget,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const beginFavoritesWidgetDrag = (event: ReactPointerEvent<HTMLDivElement>, mode: "move" | "resize") => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    favoritesWidgetDragRef.current = {
      mode,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startState: favoritesWidget,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const isFavoriteItemActive = (itemId: FavoriteItemId) => {
    if (itemId.startsWith("tool:")) {
      return drawingTool === itemId.slice("tool:".length);
    }
    if (itemId.startsWith("indicator:")) {
      return indicatorKeys.includes(itemId.slice("indicator:".length) as IndicatorKey);
    }
    if (itemId === "overlay:rvol") {
      return showRvol;
    }
    if (itemId === "overlay:pocket-pivot") {
      return pocketPivotWidget.enabled;
    }
    if (itemId === "overlay:earnings") {
      return earningsWidget.enabled;
    }
    return Boolean(extendedHistory);
  };

  const updatePocketPivotNoteStyle = (patch: Partial<PocketPivotNoteStyle>) => {
    setNotesWidget((current) => ({ ...current, ...patch }));
  };

  const updatePocketPivotNote = (value: string) => {
    if (!normalizedSymbol) {
      return;
    }
    setPocketPivotNotes((current) => {
      if (!value.trim()) {
        const { [normalizedSymbol]: _removed, ...rest } = current;
        return rest;
      }
      return { ...current, [normalizedSymbol]: value };
    });
  };

  const stageWidth = containerRef.current?.clientWidth ?? 0;
  const stageHeight = containerRef.current?.clientHeight ?? 0;

  const selectAnnotation = (id: string) =>
    setSelectedAnnotationId((prev) => (prev === id ? null : id));

  const updateAnnotation = (id: string, patch: Partial<ChartAnnotation>) =>
    onAnnotationsChange(annotations.map((a) => (a.id === id ? { ...a, ...patch } as ChartAnnotation : a)));

  const updateAnnotationAnchor = (annotationId: string, handleKey: AnnotationHandleKey, anchor: ChartAnchor) => {
    onAnnotationsChangeRef.current(
      annotationsRef.current.map((annotation) => {
        if (annotation.id !== annotationId) {
          return annotation;
        }
        if (handleKey === "point" && "point" in annotation) {
          return { ...annotation, point: anchor };
        }
        if (handleKey === "start" && "start" in annotation) {
          return { ...annotation, start: anchor };
        }
        if (handleKey === "end" && "end" in annotation) {
          return { ...annotation, end: anchor };
        }
        return annotation;
      }),
    );
  };

  const startAnnotationHandleDrag = (
    event: ReactPointerEvent<Element>,
    annotationId: string,
    handleKey: AnnotationHandleKey,
  ) => {
    if (event.button !== 0) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    const anchor = anchorFromPointer(event.clientX, event.clientY);
    setSelectedAnnotationId(annotationId);
    annotationDragRef.current = { annotationId, handleKey };
    setDraggingAnnotationHandle(`${annotationId}:${handleKey}`);
    if (anchor) {
      updateAnnotationAnchor(annotationId, handleKey, anchor);
    }
  };

  const editTextAnnotation = (annotationId: string) => {
    const target = annotationsRef.current.find((a) => a.id === annotationId);
    if (!target || target.type !== "text") {
      return;
    }
    const next = window.prompt("Edit chart text", target.text);
    if (next === null) {
      return;
    }
    const trimmed = next.trim();
    if (!trimmed) {
      deleteAnnotation(annotationId);
      return;
    }
    updateAnnotation(annotationId, { text: trimmed } as any);
  };

  const deleteAnnotation = (id: string) => {
    if (annotationDragRef.current?.annotationId === id) {
      annotationDragRef.current = null;
      setDraggingAnnotationHandle(null);
    }
    setSelectedAnnotationId(null);
    onAnnotationsChange(annotations.filter((a) => a.id !== id));
  };

  useEffect(() => {
    if (!draggingAnnotationHandle) {
      return;
    }

    const handlePointerMove = (event: PointerEvent) => {
      const activeDrag = annotationDragRef.current;
      if (!activeDrag) {
        return;
      }
      const anchor = anchorFromPointer(event.clientX, event.clientY);
      if (anchor) {
        updateAnnotationAnchor(activeDrag.annotationId, activeDrag.handleKey, anchor);
      }
    };

    const stopDragging = () => {
      annotationDragRef.current = null;
      setDraggingAnnotationHandle(null);
    };

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", stopDragging);
    window.addEventListener("pointercancel", stopDragging);
    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", stopDragging);
      window.removeEventListener("pointercancel", stopDragging);
    };
  }, [draggingAnnotationHandle, activeBars]);

  useEffect(() => {
    const handlePointerMove = (event: PointerEvent) => {
      const favoritesDrag = favoritesWidgetDragRef.current;
      if (favoritesDrag) {
        const deltaX = event.clientX - favoritesDrag.startClientX;
        const deltaY = event.clientY - favoritesDrag.startClientY;
        setFavoritesWidget((current) => {
          const maxX = Math.max(stageWidth - favoritesDrag.startState.width - 8, 0);
          const maxY = Math.max(stageHeight - favoritesDrag.startState.height - 8, 0);
          if (favoritesDrag.mode === "resize") {
            return {
              ...current,
              width: clamp(favoritesDrag.startState.width + deltaX, 210, Math.max(stageWidth - favoritesDrag.startState.x - 8, 240)),
              height: clamp(favoritesDrag.startState.height + deltaY, 170, Math.max(stageHeight - favoritesDrag.startState.y - 8, 190)),
            };
          }

          return {
            ...current,
            x: clamp(favoritesDrag.startState.x + deltaX, 0, maxX),
            y: clamp(favoritesDrag.startState.y + deltaY, 0, maxY),
          };
        });
        return;
      }

      const notesDrag = notesWidgetDragRef.current;
      if (notesDrag) {
        const dx = event.clientX - notesDrag.startClientX;
        const dy = event.clientY - notesDrag.startClientY;
        setNotesWidget((current) => {
          const maxX = Math.max(stageWidth - notesDrag.startState.width - 8, 0);
          const maxY = Math.max(stageHeight - notesDrag.startState.height - 8, 0);
          if (notesDrag.mode === "resize") {
            return {
              ...current,
              width: clamp(notesDrag.startState.width + dx, 220, Math.max(stageWidth - notesDrag.startState.x - 8, 240)),
              height: clamp(notesDrag.startState.height + dy, 160, Math.max(stageHeight - notesDrag.startState.y - 8, 180)),
            };
          }
          return {
            ...current,
            x: clamp(notesDrag.startState.x + dx, 0, maxX),
            y: clamp(notesDrag.startState.y + dy, 0, maxY),
          };
        });
        return;
      }

      const earningsDrag = earningsWidgetDragRef.current;
      if (earningsDrag) {
        const dx = event.clientX - earningsDrag.startClientX;
        const dy = event.clientY - earningsDrag.startClientY;
        setEarningsWidget((current) => {
          const maxX = Math.max(stageWidth - earningsDrag.startState.width - 8, 0);
          const maxY = Math.max(stageHeight - earningsDrag.startState.height - 8, 0);
          if (earningsDrag.mode === "resize") {
            return {
              ...current,
              width: clamp(earningsDrag.startState.width + dx, 300, Math.max(stageWidth - earningsDrag.startState.x - 8, 320)),
              height: clamp(earningsDrag.startState.height + dy, 190, Math.max(stageHeight - earningsDrag.startState.y - 8, 220)),
            };
          }
          return {
            ...current,
            x: clamp(earningsDrag.startState.x + dx, 0, maxX),
            y: clamp(earningsDrag.startState.y + dy, 0, maxY),
          };
        });
        return;
      }

      const activeDrag = floatingWidgetDragRef.current;
      if (!activeDrag) {
        return;
      }

      const deltaX = event.clientX - activeDrag.startClientX;
      const deltaY = event.clientY - activeDrag.startClientY;
      setPocketPivotWidget((current) => {
        const maxX = Math.max(stageWidth - activeDrag.startState.width - 8, 0);
        const maxY = Math.max(stageHeight - activeDrag.startState.height - 8, 0);
        if (activeDrag.mode === "resize") {
          return {
            ...current,
            width: clamp(activeDrag.startState.width + deltaX, 190, Math.max(stageWidth - activeDrag.startState.x - 8, 220)),
            height: clamp(activeDrag.startState.height + deltaY, 130, Math.max(stageHeight - activeDrag.startState.y - 8, 150)),
          };
        }

        return {
          ...current,
          x: clamp(activeDrag.startState.x + deltaX, 0, maxX),
          y: clamp(activeDrag.startState.y + deltaY, 0, maxY),
        };
      });
    };

    const stopDragging = () => {
      favoritesWidgetDragRef.current = null;
      floatingWidgetDragRef.current = null;
      notesWidgetDragRef.current = null;
      earningsWidgetDragRef.current = null;
    };

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", stopDragging);
    window.addEventListener("pointercancel", stopDragging);
    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", stopDragging);
      window.removeEventListener("pointercancel", stopDragging);
    };
  }, [stageHeight, stageWidth]);

  const horizontalLineOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "hline" }> => annotation.type === "hline")
    .map((annotation) => {
      const point = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.point);
      if (!point) return null;
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.hline;
      const lw = annotation.lineWidth ?? 1.6;
      return (
        <g key={annotation.id} style={{ pointerEvents: "auto", cursor: "pointer" }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}>
          <line x1={0} y1={point.y} x2={stageWidth} y2={point.y} stroke="transparent" strokeWidth={14} />
          {isSel && <line x1={0} y1={point.y} x2={stageWidth} y2={point.y} stroke={color} strokeWidth={lw + 6} opacity={0.22} />}
          <line x1={0} y1={point.y} x2={stageWidth} y2={point.y} stroke={color} strokeWidth={lw} strokeDasharray="8 5" />
          <rect x={Math.max(stageWidth - 84, 4)} y={Math.max(point.y - 11, 4)} width="80" height="22" rx="8" fill="rgba(13, 17, 23, 0.92)" />
          <text x={Math.max(stageWidth - 44, 10)} y={point.y + 4} fill={color} fontSize="11" textAnchor="middle">
            {annotation.point.price.toFixed(2)}
          </text>
        </g>
      );
    });
  const verticalLineOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "vline" }> => annotation.type === "vline")
    .map((annotation) => {
      const point = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.point);
      if (!point) return null;
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.vline;
      const lw = annotation.lineWidth ?? 1.5;
      const labelX = clamp(point.x + 8, 6, Math.max(stageWidth - 96, 6));
      return (
        <g key={annotation.id} style={{ pointerEvents: "auto", cursor: "pointer" }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}>
          <line x1={point.x} y1={0} x2={point.x} y2={stageHeight} stroke="transparent" strokeWidth={14} />
          {isSel && <line x1={point.x} y1={0} x2={point.x} y2={stageHeight} stroke={color} strokeWidth={lw + 6} opacity={0.22} />}
          <line x1={point.x} y1={0} x2={point.x} y2={stageHeight} stroke={color} strokeWidth={lw} strokeDasharray="7 5" />
          <rect x={labelX} y={8} width="88" height="22" rx="8" fill="rgba(13, 17, 23, 0.9)" />
          <text x={labelX + 44} y={22} fill={color} fontSize="11" textAnchor="middle">
            {formatChartDateFromTimestamp(annotation.point.time)}
          </text>
        </g>
      );
    });
  const trendlineOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "trendline" }> => annotation.type === "trendline")
    .map((annotation) => {
      const start = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.start);
      const end = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.end);
      if (!start || !end) return null;
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.trendline;
      const lw = annotation.lineWidth ?? 2;
      return (
        <g key={annotation.id} style={{ pointerEvents: "auto", cursor: "pointer" }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}>
          <line x1={start.x} y1={start.y} x2={end.x} y2={end.y} stroke="transparent" strokeWidth={14} strokeLinecap="round" />
          {isSel && <line x1={start.x} y1={start.y} x2={end.x} y2={end.y} stroke={color} strokeWidth={lw + 6} opacity={0.22} strokeLinecap="round" />}
          <line x1={start.x} y1={start.y} x2={end.x} y2={end.y} stroke={color} strokeWidth={lw} strokeLinecap="round" strokeDasharray="6 4" />
        </g>
      );
    });
  const rayOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "ray" }> => annotation.type === "ray")
    .map((annotation) => {
      const start = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.start);
      const end = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.end);
      if (!start || !end) return null;
      const rayEnd = projectRayEnd(start, end, stageWidth);
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.ray;
      const lw = annotation.lineWidth ?? 2;
      return (
        <g key={annotation.id} style={{ pointerEvents: "auto", cursor: "pointer" }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}>
          <line x1={start.x} y1={start.y} x2={rayEnd.x} y2={rayEnd.y} stroke="transparent" strokeWidth={14} strokeLinecap="round" />
          {isSel && <line x1={start.x} y1={start.y} x2={rayEnd.x} y2={rayEnd.y} stroke={color} strokeWidth={lw + 6} opacity={0.22} strokeLinecap="round" />}
          <line x1={start.x} y1={start.y} x2={rayEnd.x} y2={rayEnd.y} stroke={color} strokeWidth={lw} strokeLinecap="round" />
        </g>
      );
    });
  const rectangleOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "rectangle" }> => annotation.type === "rectangle")
    .map((annotation) => {
      const start = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.start);
      const end = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.end);
      if (!start || !end) return null;
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.rectangle;
      const lw = annotation.lineWidth ?? 1.6;
      const x = Math.min(start.x, end.x);
      const y = Math.min(start.y, end.y);
      const w = Math.max(Math.abs(end.x - start.x), 2);
      const h = Math.max(Math.abs(end.y - start.y), 2);
      return (
        <g key={annotation.id} style={{ pointerEvents: "auto", cursor: "pointer" }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}>
          {isSel && <rect x={x - 4} y={y - 4} width={w + 8} height={h + 8} rx="8" fill={color} opacity={0.1} />}
          <rect x={x} y={y} width={w} height={h} rx="6"
            fill={isSel ? `${color}22` : "rgba(89, 196, 255, 0.12)"}
            stroke={color} strokeWidth={lw} strokeDasharray="6 4"
            style={{ pointerEvents: "auto" }} />
        </g>
      );
    });
  // Auto demand/supply zones (shaded full-width bands) + trendlines (diagonal),
  // computed in levels.ts and drawn in the same SVG overlay so they follow
  // pan/zoom. Non-interactive; sit beneath the user's own drawings.
  const autoZoneOverlays = autoLevelsEnabled
    ? autoLevels.zones.map((zone, index) => {
        const ms = mainSeriesRef.current;
        if (!ms) return null;
        const yHigh = ms.priceToCoordinate(zone.high);
        const yLow = ms.priceToCoordinate(zone.low);
        if (yHigh === null || yHigh === undefined || yLow === null || yLow === undefined) return null;
        const top = Math.min(yHigh, yLow);
        const height = Math.max(Math.abs(yLow - yHigh), 2);
        const demand = zone.kind === "demand";
        // Demand colored by timeframe: daily = green, weekly = blue. Supply = red.
        const color = demand ? (zone.timeframe === "W" ? "#3b82f6" : "#22c55e") : "#ef4444";
        const tfLabel = zone.timeframe === "M" ? "Mthly" : zone.timeframe === "W" ? "Wkly" : "Daily";
        const label = `${tfLabel} ${demand ? "Demand" : "Supply"}`;
        return (
          <g key={`auto-zone-${index}`} style={{ pointerEvents: "none" }}>
            <rect x={0} y={top} width={stageWidth} height={height} fill={`${color}1f`} stroke={`${color}66`} strokeWidth={1} strokeDasharray="2 3" />
            <text x={6} y={top + 12} fontSize="10" fontWeight={600} fill={color}>{label}</text>
          </g>
        );
      })
    : null;
  const autoTrendlineOverlays = autoLevelsEnabled
    ? autoLevels.trendlines.map((line, index) => {
        const p1 = projectAnchor(chartRef.current, mainSeriesRef.current, { time: line.t1, price: line.p1 });
        const p2 = projectAnchor(chartRef.current, mainSeriesRef.current, { time: line.t2, price: line.p2 });
        if (!p1 || !p2) return null;
        const color = line.kind === "up" ? "#22c55e" : "#ef4444";
        return (
          <g key={`auto-trend-${index}`} style={{ pointerEvents: "none" }}>
            <line x1={p1.x} y1={p1.y} x2={p2.x} y2={p2.y} stroke={color} strokeWidth={1.6} />
          </g>
        );
      })
    : null;
  const measureOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "measure" }> => annotation.type === "measure")
    .map((annotation) => {
      const start = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.start);
      const end = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.end);
      if (!start || !end) return null;
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.measure;
      const lw = annotation.lineWidth ?? 2;
      const change = annotation.end.price - annotation.start.price;
      const changePct = annotation.start.price === 0 ? 0 : (change / annotation.start.price) * 100;
      const spanBars = barsBetweenTimes(activeBars, annotation.start.time, annotation.end.time);
      const midX = (start.x + end.x) / 2;
      const midY = (start.y + end.y) / 2;
      const label = `${change >= 0 ? "+" : ""}${formatValue(change, 2)} | ${changePct >= 0 ? "+" : ""}${formatValue(changePct, 2)}% | ${spanBars} bars`;
      const labelWidth = Math.max(170, Math.min(240, label.length * 6.4));
      const labelX = clamp(midX - labelWidth / 2, 6, Math.max(stageWidth - labelWidth - 6, 6));
      const labelY = clamp(midY - 26, 8, Math.max(stageHeight - 28, 8));
      return (
        <g key={annotation.id} style={{ pointerEvents: "auto", cursor: "pointer" }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}>
          <line x1={start.x} y1={start.y} x2={end.x} y2={end.y} stroke="transparent" strokeWidth={14} strokeLinecap="round" />
          {isSel && <line x1={start.x} y1={start.y} x2={end.x} y2={end.y} stroke={color} strokeWidth={lw + 6} opacity={0.22} strokeLinecap="round" />}
          <line x1={start.x} y1={start.y} x2={end.x} y2={end.y} stroke={color} strokeWidth={lw} strokeLinecap="round" strokeDasharray="5 4" />
          <circle cx={start.x} cy={start.y} r="4" fill={color} />
          <circle cx={end.x} cy={end.y} r="4" fill={color} />
          <rect x={labelX} y={labelY} width={labelWidth} height="24" rx="8" fill="rgba(4, 8, 17, 0.92)" />
          <text x={labelX + labelWidth / 2} y={labelY + 15} fill={color} fontSize="11" textAnchor="middle">
            {label}
          </text>
        </g>
      );
    });

  const textOverlays = annotations
    .filter((annotation): annotation is Extract<ChartAnnotation, { type: "text" }> => annotation.type === "text")
    .map((annotation) => {
      const point = projectAnchor(chartRef.current, mainSeriesRef.current, annotation.point);
      if (!point) return null;
      const isSel = selectedAnnotationId === annotation.id;
      const color = annotation.color ?? ANNOTATION_DEFAULT_COLORS.text;
      return (
        <div
          key={annotation.id}
          className={isSel ? "chart-note selected" : "chart-note"}
          style={{
            left: `${Math.min(point.x + 10, Math.max(stageWidth - 180, 12))}px`,
            top: `${Math.max(point.y - 12, 10)}px`,
            borderColor: isSel ? color : undefined,
            color: isSel ? color : undefined,
          }}
          onPointerDown={(event) => startAnnotationHandleDrag(event, annotation.id, "point")}
          onClick={(event) => { event.stopPropagation(); selectAnnotation(annotation.id); }}
          onDoubleClick={(event) => { event.stopPropagation(); editTextAnnotation(annotation.id); }}
          title="Drag to move • Double-click to edit"
        >
          {annotation.text}
        </div>
      );
    });

  const draftPoint = draftTrendStart ? projectAnchor(chartRef.current, mainSeriesRef.current, draftTrendStart) : null;
  const hoverPoint = hoverAnchor ? projectAnchor(chartRef.current, mainSeriesRef.current, hoverAnchor) : null;
  const draftMeasureLabel =
    draftTrendStart && hoverAnchor
      ? `${hoverAnchor.price - draftTrendStart.price >= 0 ? "+" : ""}${formatValue(hoverAnchor.price - draftTrendStart.price, 2)} | ${
          draftTrendStart.price === 0
            ? "0.00"
            : `${hoverAnchor.price - draftTrendStart.price >= 0 ? "+" : ""}${formatValue(((hoverAnchor.price - draftTrendStart.price) / draftTrendStart.price) * 100, 2)}`
        }% | ${barsBetweenTimes(activeBars, draftTrendStart.time, hoverAnchor.time)} bars`
      : null;

  const selectedAnnotation = annotations.find((a) => a.id === selectedAnnotationId) ?? null;
  const selectedAnnotationHandles = selectedAnnotation
    ? getAnnotationHandleAnchors(selectedAnnotation).map(({ key, anchor }) => {
        const point = projectAnchor(chartRef.current, mainSeriesRef.current, anchor);
        if (!point) {
          return null;
        }

        const color = selectedAnnotation.color ?? ANNOTATION_DEFAULT_COLORS[selectedAnnotation.type] ?? "#ffd36f";
        const handleId = `${selectedAnnotation.id}:${key}`;
        const isDragging = draggingAnnotationHandle === handleId;
        return (
          <g
            key={handleId}
            style={{ pointerEvents: "auto", cursor: isDragging ? "grabbing" : "grab" }}
            onPointerDown={(event) => startAnnotationHandleDrag(event, selectedAnnotation.id, key)}
            onClick={(event) => event.stopPropagation()}
          >
            <circle cx={point.x} cy={point.y} r="11" fill="transparent" />
            <circle cx={point.x} cy={point.y} r={isDragging ? 6 : 5} fill={color} stroke="rgba(4, 8, 17, 0.92)" strokeWidth="2" />
          </g>
        );
      })
    : [];
  const annotationEditPos = (() => {
    if (!selectedAnnotation) return null;
    let ex = 20, ey = 20;
    if ("point" in selectedAnnotation) {
      const pt = projectAnchor(chartRef.current, mainSeriesRef.current, selectedAnnotation.point);
      if (pt) { ex = clamp(pt.x - 90, 4, Math.max(stageWidth - 210, 4)); ey = clamp(pt.y - 54, 4, Math.max(stageHeight - 66, 4)); }
    } else if ("start" in selectedAnnotation) {
      const st = projectAnchor(chartRef.current, mainSeriesRef.current, selectedAnnotation.start);
      const en = projectAnchor(chartRef.current, mainSeriesRef.current, selectedAnnotation.end);
      if (st && en) { ex = clamp((st.x + en.x) / 2 - 90, 4, Math.max(stageWidth - 210, 4)); ey = clamp(Math.min(st.y, en.y) - 54, 4, Math.max(stageHeight - 66, 4)); }
    }
    return { x: ex, y: ey };
  })();
  const valuation = fundamentals?.valuation ?? null;
  const growth = fundamentals?.growth ?? null;
  const ratioCards = [
    { label: "P/E", value: formatValue(valuation?.pe_ratio, 2) },
    { label: "PEG", value: formatValue(valuation?.peg_ratio, 2) },
    { label: "OPM", value: formatPercentValue(valuation?.operating_margin_pct) },
    { label: "Net Margin", value: formatPercentValue(valuation?.net_margin_pct) },
    { label: "ROCE", value: formatPercentValue(valuation?.roce_pct) },
    { label: "ROE", value: formatPercentValue(valuation?.roe_pct) },
    { label: "Dividend Yield", value: formatPercentValue(valuation?.dividend_yield_pct) },
    { label: "Market Cap", value: formatAmountValue(valuation?.market_cap_crore) },
  ];
  const growthCards = [
    { label: "Sales QoQ", value: formatSignedPercentValue(growth?.sales_qoq_pct) },
    { label: "Sales YoY", value: formatSignedPercentValue(growth?.sales_yoy_pct) },
    { label: "Profit QoQ", value: formatSignedPercentValue(growth?.profit_qoq_pct) },
    { label: "Profit YoY", value: formatSignedPercentValue(growth?.profit_yoy_pct) },
    { label: "OPM", value: formatPercentValue(growth?.operating_margin_latest_pct) },
    { label: "Net Margin", value: formatPercentValue(growth?.net_margin_latest_pct) },
  ];
  const chartTitle = summary?.name ?? (symbol ? `${symbol}` : "Chart");
  const chartSubtitleText = summary
    ? `${summary.symbol} • ${summary.exchange} • ${summary.sector}${summary.sub_sector ? ` • ${summary.sub_sector}` : ""}`
    : symbol
      ? `${symbol} • Live chart`
      : undefined;
  const priceTrendClass = summary ? (summary.change_pct >= 0 ? "positive" : "negative") : "neutral";
  const rsTrendClass = summary
    ? (summary.rs_rating ?? summary.rs_rating_1w_ago) >= summary.rs_rating_1w_ago
      ? "positive"
      : "negative"
    : "neutral";
  const hoveredPriceTrendClass =
    hoveredBar?.changePct !== null && hoveredBar?.changePct !== undefined
      ? hoveredBar.changePct >= 0
        ? "positive"
        : "negative"
      : priceTrendClass;
  const priceLine1 = hoveredBar
    ? `${formatChartDateFromTimestamp(hoveredBar.time, market)} · O ${formatPriceValue(hoveredBar.open, 2)} · H ${formatPriceValue(hoveredBar.high, 2)} · L ${formatPriceValue(hoveredBar.low, 2)} · C ${formatPriceValue(hoveredBar.close, 2)}`
    : summary
      ? `Current ${formatPriceValue(summary.last_price, 2)} · ${formatSignedPercentValue(summary.change_pct)}`
      : "Hover the chart to inspect OHLC detail.";
  const priceLine2 = hoveredBar
    ? (hoveredBar.changePct !== null && hoveredBar.changeValue !== null
        ? `Chg ${hoveredBar.changeValue >= 0 ? "+" : ""}${formatValue(hoveredBar.changeValue, 2)} (${formatSignedPercentValue(hoveredBar.changePct)})`
        : null)
    : summary
      ? "Hover the chart to inspect OHLC detail."
      : null;
  const tradeHoverLines = hoveredTradeMarkers.length
    ? hoveredTradeMarkers.flatMap((marker) =>
        marker.entries.map((entry) => {
          const label = marker.type === "buy" ? "B" : "S";
          const qtyText = entry.qty ? ` × ${formatValue(entry.qty, 0)}` : "";
          return `${label} ${formatPriceValue(entry.price, 2)}${qtyText}`;
        }),
      )
    : [];
  const chartSearchSuggestions = useMemo(
    () => buildSymbolSuggestions(searchOptions, deferredChartSearchQuery, 100),
    [deferredChartSearchQuery, searchOptions],
  );
  const favoriteItems = FAVORITE_ITEMS.filter((item) => favoritesSettings.itemIds.includes(item.id));

  const handleChartSearchSubmit = (query: string) => {
    const trimmed = query.trim();
    if (!trimmed) {
      return;
    }
    onSearchSymbol?.(trimmed);
  };

  return (
    <Panel
      title={chartTitle}
      subtitle={chartSubtitleText}
      actions={
        <div className="chart-actions">
          {onSearchSymbol ? (
            <form
              className="chart-search-form"
              onSubmit={(event) => {
                event.preventDefault();
                handleChartSearchSubmit(chartSearchQuery);
              }}
            >
              <input
                list={searchListId}
                value={chartSearchQuery}
                onChange={(event) => setChartSearchQuery(event.target.value)}
                onInput={(event) => {
                  const inputEvent = event.nativeEvent as InputEvent;
                  if (inputEvent.inputType === "insertReplacementText" || !inputEvent.inputType) {
                    const value = (event.target as HTMLInputElement).value;
                    setTimeout(() => handleChartSearchSubmit(value), 0);
                  }
                }}
                placeholder="Search another stock"
                aria-label="Search another stock"
              />
              <datalist id={searchListId}>
                {chartSearchSuggestions.map((item) => (
                  <option key={`chart-search-${item.symbol}`} value={item.symbol}>
                    {item.name}
                  </option>
                ))}
              </datalist>
              <button type="submit" className="tool-pill">
                Search
              </button>
            </form>
          ) : null}
          <div className="chart-tab-switcher">
            {PANEL_TABS.map((tab) => (
              <button
                key={tab.key}
                type="button"
                className={panelTab === tab.key ? "scanner-tab active" : "scanner-tab"}
                onClick={() => onPanelTabChange(tab.key)}
              >
                {tab.label}
              </button>
            ))}
          </div>
          {panelTab === "technical" ? (
            <>
              <div className="timeframe-switcher">
                {availableTimeframes.map((item) => (
                  <button
                    key={item}
                    type="button"
                    className={item === timeframe ? "timeframe-pill active" : "timeframe-pill"}
                    onClick={() => onTimeframeChange(item)}
                  >
                    {item}
                  </button>
                ))}
              </div>
              <div className="chart-style-switcher">
                {CHART_STYLES.map((style) => (
                  <button
                    key={style.key}
                    type="button"
                    className={style.key === chartStyle ? "timeframe-pill active" : "timeframe-pill"}
                    onClick={() => onChartStyleChange(style.key)}
                  >
                    {style.label}
                  </button>
                ))}
              </div>
              <div className="chart-style-switcher">
                {Object.entries(CHART_PALETTES).map(([key, value]) => (
                  <button
                    key={key}
                    type="button"
                    className={chartPalette === key ? "timeframe-pill active" : "timeframe-pill"}
                    onClick={() => onChartPaletteChange(key as ChartPaletteKey)}
                  >
                    {value.label}
                  </button>
                ))}
              </div>
              <div className="indicator-switcher">
                {INDICATORS.map((indicator) => (
                  <button
                    key={indicator.key}
                    type="button"
                    className={indicatorKeys.includes(indicator.key) ? "indicator-pill active" : "indicator-pill"}
                    onClick={() => onToggleIndicator(indicator.key)}
                  >
                    {indicator.label}
                  </button>
                ))}
                <button
                  type="button"
                  className={showRvol ? "indicator-pill active" : "indicator-pill"}
                  onClick={() => setShowRvol((v) => !v)}
                  title="Relative Volume vs 50-day average. Helps spot unusual activity."
                >
                  RVOL
                </button>
                <button
                  type="button"
                  className={autoLevelsEnabled ? "indicator-pill active" : "indicator-pill"}
                  onClick={() => setAutoLevelsEnabled((v) => !v)}
                  title="Auto support/resistance, weekly/monthly demand-supply zones, and trendlines — strong levels only"
                >
                  Auto Levels
                </button>
              </div>
              {symbol ? (
                <button type="button" className="tool-pill" onClick={() => onAddToWatchlist?.(symbol)}>
                  Add to Watchlist
                </button>
              ) : null}
              <button
                type="button"
                className={chartLoading ? "tool-pill loading" : "tool-pill"}
                onClick={() => onRefreshChart?.()}
                disabled={!symbol || chartLoading || !onRefreshChart}
                title="Refresh the chart from the backend while keeping cached data visible"
              >
                {chartLoading ? "Refreshing..." : "Refresh Chart"}
              </button>
              {chartCacheState === "cached" ? <span className="chart-save-pill">Cached view</span> : null}
              {showBenchmarkOverlay && benchmarkError ? <span className="chart-save-pill">{benchmarkError}</span> : null}
            </>
          ) : (
            <div className="fundamentals-toolbar">
              {symbol ? (
                <button type="button" className="tool-pill" onClick={() => onAddToWatchlist?.(symbol)}>
                  Add to Watchlist
                </button>
              ) : null}
              <span className="chart-save-pill">Updated {formatDateTime(fundamentals?.fetched_at)}</span>
            </div>
          )}
        </div>
      }
      className={`${expanded ? "chart-panel expanded" : "chart-panel"}${zenMode ? " chart-zen" : ""}`}
    >
      {panelTab === "technical" ? (
        <div className="chart-drawing-toolbar">
          <label className="drawing-tool-select">
            <select
              value={drawingTool}
              onChange={(event: ChangeEvent<HTMLSelectElement>) => switchDrawingTool(event.target.value as DrawingTool)}
            >
              {DRAWING_TOOLS.map((tool) => (
                <option key={tool.key} value={tool.key}>
                  {tool.label}
                </option>
              ))}
            </select>
          </label>
          {drawingTool !== "none" ? (
            <label className="draft-color-row" title="Drawing color">
              <span>Color</span>
              <input type="color" value={drawingColor} onChange={(e) => onDrawingColorChange(e.target.value)} />
            </label>
          ) : null}
          <button
            type="button"
            className="tool-pill"
            onClick={() => onAnnotationsChange(annotations.slice(0, -1))}
            disabled={!annotations.length}
          >
            Undo
          </button>
          <button
            type="button"
            className="tool-pill"
            onClick={() => {
              setDraftTrendStart(null);
              setHoverAnchor(null);
              setDrawingTool("none");
              setSelectedAnnotationId(null);
              onAnnotationsChange([]);
            }}
            disabled={!annotations.length && !draftTrendStart}
          >
            Clear All
          </button>
          {annotations.length > 0 && <span className="chart-save-pill">{annotations.length} saved</span>}
          <details className="chart-color-settings">
            <summary>Indicator Colors</summary>
            <div className="chart-color-grid">
              {CHART_COLOR_FIELDS.map((field) => (
                <label key={field.key} className="chart-color-field">
                  <span>{field.label}</span>
                  <input
                    type="color"
                    value={chartColors[field.key]}
                    onChange={(event) =>
                      onChartColorsChange({
                        ...chartColors,
                        [field.key]: event.target.value,
                      })
                    }
                  />
                </label>
              ))}
            </div>
            <label className="chart-slider-field">
              <span>RS Circle Size</span>
              <div>
                <input
                  type="range"
                  min="0.5"
                  max="8"
                  step="0.5"
                  value={chartColors.rsMarkerSize}
                  onChange={(event) =>
                    onChartColorsChange({
                      ...chartColors,
                      rsMarkerSize: Number(event.target.value),
                    })
                  }
                />
                <strong>{chartColors.rsMarkerSize}px</strong>
              </div>
            </label>
          </details>
          <button
            type="button"
            className={historyLoading ? "tool-pill loading" : "tool-pill"}
            onClick={handleLoadFullHistory}
            disabled={historyLoading || !symbol}
            title={extendedHistory ? "Return to the standard chart range" : "Load full price history for this stock"}
          >
            {historyLoading ? "Loading..." : extendedHistory ? "Show Recent History" : "Load Full History"}
          </button>
          <div className="chart-widget-menu">
            <button
              type="button"
              className={favoritesWidget.enabled ? "tool-pill active" : "tool-pill"}
              onClick={() => setFavoritesWidget((current) => ({ ...current, enabled: !current.enabled }))}
            >
              Favourites
            </button>
          </div>
          <div className="chart-widget-menu">
            <button
              type="button"
              className={pocketPivotWidget.enabled ? "tool-pill active" : "tool-pill"}
              onClick={() => setPocketPivotWidget((current) => ({ ...current, enabled: !current.enabled }))}
            >
              Pocket Pivot
            </button>
          </div>
          <div className="chart-widget-menu">
            <button
              type="button"
              className={zenMode ? "tool-pill active" : "tool-pill"}
              onClick={() => setZenMode((current) => !current)}
              title="Zen mode — just the chart and the search bar (Esc to exit)"
            >
              ⛶ Zen
            </button>
          </div>
          <div className="chart-widget-menu">
            <button
              type="button"
              className={aiEnabled ? "tool-pill active chart-ai-pill" : "tool-pill chart-ai-pill"}
              onClick={() => setAiEnabled((current) => !current)}
              title="AI swing-trade read: pullback/breakout setup, entry, stop, pros & cons, tape read"
            >
              ✦ AI
            </button>
          </div>
          <div className="chart-widget-menu">
            <button
              type="button"
              className={notesWidget.enabled ? "tool-pill active" : "tool-pill"}
              onClick={() => setNotesWidget((current) => ({ ...current, enabled: !current.enabled }))}
            >
              Notes
            </button>
          </div>
          <div className="chart-widget-menu">
            <button
              type="button"
              className={earningsWidget.enabled ? "tool-pill active" : "tool-pill"}
              onClick={() => setEarningsWidget((current) => ({ ...current, enabled: !current.enabled }))}
            >
              Earnings
            </button>
          </div>
          <div className="chart-widget-menu chart-range-menu">
            <select
              className="tool-pill chart-range-select"
              value={chartRange}
              onChange={(event) => setChartRange(event.target.value as ChartRangeKey)}
              title="Chart visible range"
              aria-label="Chart visible range"
            >
              {CHART_RANGE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
          {selectedAnnotation ? <span className="chart-save-pill">Drag endpoints</span> : null}
        </div>
      ) : null}
      {summary ? (
        <div className="chart-summary-strip compact">
          <div className={`chart-summary-chip ${priceTrendClass}`}>
            <span>Price</span>
            <strong>{formatPriceValue(summary.last_price, 2)}</strong>
          </div>
          <div className={`chart-summary-chip ${priceTrendClass}`}>
            <span>1D Change</span>
            <strong>{formatSignedPercentValue(summary.change_pct)}</strong>
          </div>
          <div className={`chart-summary-chip strong ${rsTrendClass}`}>
            <span>RS Rating</span>
            <strong>{summary.rs_rating}</strong>
          </div>
          {groupSummary ? (
            onOpenGroup ? (
              <button
                type="button"
                className="chart-summary-chip chart-summary-chip-action"
                onClick={() => onOpenGroup(groupSummary.groupId)}
                title={`Open ${groupSummary.groupName}`}
              >
                <span>Group</span>
                <strong>{groupSummary.groupName}</strong>
              </button>
            ) : (
              <div className="chart-summary-chip">
                <span>Group</span>
                <strong>{groupSummary.groupName}</strong>
              </div>
            )
          ) : null}
          {groupSummary ? (
            <div className="chart-summary-chip">
              <span>Group Rank</span>
              <strong>{groupSummary.groupRankLabel}</strong>
            </div>
          ) : null}
          {groupSummary ? (
            <div className="chart-summary-chip">
              <span>Stock Rank</span>
              <strong>{`${groupSummary.stockRank}/${groupSummary.stockCount}`}</strong>
            </div>
          ) : null}
          <div className="chart-summary-chip">
            <span>RS 1W Ago</span>
            <strong>{summary.rs_rating_1w_ago}</strong>
          </div>
          <div className="chart-summary-chip">
            <span>RS Rating 1M Ago</span>
            <strong>{summary.rs_rating_1m_ago}</strong>
          </div>
          <div className="chart-summary-chip">
            <span>12M Return</span>
            <strong>{formatPercentValue(summary.stock_return_12m)}</strong>
          </div>
          <div className="chart-summary-chip">
            <span>20D ADR</span>
            <strong>{formatPercentValue(summary.adr_pct_20)}</strong>
          </div>
          <div className="chart-summary-chip">
            <span>30D Traded Value</span>
            <strong>{formatAmountValue(summary.avg_rupee_volume_30d_crore)}</strong>
          </div>
          <div className="chart-summary-chip chart-circuit-widget">
            <span>
              Circuit Limits
              {!summaryCircuitLimits.exact ? (
                <select
                  className="chart-circuit-band-select"
                  value={circuitBandPct}
                  onChange={(event) => setCircuitBandPct(Number(event.target.value))}
                  aria-label="Assumed circuit band percent"
                  title="Daily price band. Auto detects it from the stock's own history (exchange feed has no exact limits for this stock)"
                >
                  {CIRCUIT_BAND_SELECT.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              ) : null}
              <button
                type="button"
                className={`chart-circuit-locks-toggle${circuitLocksEnabled ? " is-on" : ""}`}
                onClick={() => setCircuitLocksEnabled((current) => !current)}
                title="Show a UC/LC pip on the candles of circuit-locked days"
              >
                Marks {circuitLocksEnabled ? "On" : "Off"}
              </button>
            </span>
            <strong>
              {summaryCircuitLimits.upper != null && summaryCircuitLimits.lower != null ? (
                <>
                  {resolvedCircuitBandPct ? (
                    <em className="chart-circuit-band">±{resolvedCircuitBandPct}%</em>
                  ) : null}
                  {resolvedCircuitBandPct ? " · " : null}
                  <em className="chart-circuit-uc">UC {formatPriceValue(summaryCircuitLimits.upper)}</em>
                  {" · "}
                  <em className="chart-circuit-lc">LC {formatPriceValue(summaryCircuitLimits.lower)}</em>
                  {!summaryCircuitLimits.exact ? (
                    <small className="chart-circuit-est">
                      {circuitBandFromNse ? " NSE" : " est."}
                    </small>
                  ) : null}
                </>
              ) : circuitBandPct === CIRCUIT_BAND_AUTO && resolvedCircuitBandPct === null ? (
                <small className="chart-circuit-est">No fixed band (dynamic)</small>
              ) : (
                formatCircuitBand(summary, market)
              )}
            </strong>
          </div>
        </div>
      ) : null}
      {aiEnabled && symbol ? (
        <div className="chart-ai-card chart-ai-widget">
          <div className="chart-ai-widget-bar">
            <strong>✦ AI · {symbol}</strong>
            <label className="chart-ai-asof" title="Analyse the chart only up to this date — later candles are ignored">
              as of
              <input
                type="date"
                value={aiAsOf}
                onChange={(event) => setAiAsOf(event.target.value)}
                max={new Date().toISOString().slice(0, 10)}
              />
            </label>
            {aiAsOf ? (
              <button type="button" className="chart-ai-asof-clear" onClick={() => setAiAsOf("")}>Live</button>
            ) : null}
            <button type="button" className="chart-ai-close" onClick={() => setAiEnabled(false)} aria-label="Close AI widget">×</button>
          </div>
          {aiLoading ? (
            <div className="chart-ai-loading">✦ Reading the tape on {symbol}… (market regime, group, price action, news)</div>
          ) : aiAnalysis?.error ? (
            <div className="chart-ai-error">{aiAnalysis.error}</div>
          ) : aiAnalysis?.raw ? (
            <div className="chart-ai-rawtext">{aiAnalysis.raw}</div>
          ) : aiAnalysis ? (
            <>
              <div className="chart-ai-head">
                <span className={`chart-ai-verdict ${String(aiAnalysis.verdict || "wait").toLowerCase()}`}>
                  {aiAnalysis.verdict ?? "—"}
                </span>
                {aiAnalysis.setup_type && aiAnalysis.setup_type !== "None" ? (
                  <span className="chart-ai-setup">{aiAnalysis.setup_type}</span>
                ) : null}
                {typeof aiAnalysis.conviction === "number" ? (
                  <span className="chart-ai-conviction">Conviction {aiAnalysis.conviction}/10</span>
                ) : null}
                <strong className="chart-ai-headline">{aiAnalysis.headline}</strong>
              </div>
              {aiAnalysis.trade_plan && (aiAnalysis.trade_plan.entry || aiAnalysis.trade_plan.stop_loss) ? (
                <div className="chart-ai-plan">
                  {aiAnalysis.trade_plan.entry ? (
                    <span title={aiAnalysis.trade_plan.entry_logic}>
                      Entry <strong>{formatPriceValue(aiAnalysis.trade_plan.entry)}</strong>
                    </span>
                  ) : null}
                  {aiAnalysis.trade_plan.stop_loss ? (
                    <span title={aiAnalysis.trade_plan.stop_logic}>
                      Stop <strong className="neg">{formatPriceValue(aiAnalysis.trade_plan.stop_loss)}</strong>
                      {aiAnalysis.trade_plan.risk_pct ? ` (${aiAnalysis.trade_plan.risk_pct.toFixed(1)}%)` : ""}
                    </span>
                  ) : null}
                  {aiAnalysis.trade_plan.target_1 ? (
                    <span>
                      Targets <strong className="pos">{formatPriceValue(aiAnalysis.trade_plan.target_1)}</strong>
                      {aiAnalysis.trade_plan.target_2 ? <> / <strong className="pos">{formatPriceValue(aiAnalysis.trade_plan.target_2)}</strong></> : null}
                    </span>
                  ) : null}
                </div>
              ) : null}
              {aiAnalysis.tape_read ? <p className="chart-ai-tape"><strong>Tape:</strong> {aiAnalysis.tape_read}</p> : null}
              <div className="chart-ai-proscons">
                {aiAnalysis.pros?.length ? (
                  <div>
                    <span className="chart-ai-list-title pos">For the trade</span>
                    <ul>{aiAnalysis.pros.map((item, index) => <li key={index}>{item}</li>)}</ul>
                  </div>
                ) : null}
                {aiAnalysis.cons?.length ? (
                  <div>
                    <span className="chart-ai-list-title neg">Against the trade</span>
                    <ul>{aiAnalysis.cons.map((item, index) => <li key={index}>{item}</li>)}</ul>
                  </div>
                ) : null}
              </div>
              {aiAnalysis.market_context ? <p className="chart-ai-context">{aiAnalysis.market_context}</p> : null}
              {aiAnalysis.invalidation ? <p className="chart-ai-context"><strong>Invalidation:</strong> {aiAnalysis.invalidation}</p> : null}
              <small className="chart-ai-foot">AI read · {aiAnalysis.session_date ?? ""} · not financial advice — your plan, your risk.</small>
            </>
          ) : null}
        </div>
      ) : null}
      {panelTab === "technical" ? (
        !symbol ? (
          <div className="empty-state">Pick a stock to view the chart.</div>
        ) : chartLoading && activeBars.length === 0 ? (
          <div className="empty-state">
            Loading {timeframe} chart…
            {timeframe !== "1D" && timeframe !== "1W" ? (
              <div className="empty-state-subtitle">Intraday data can take longer on the first request.</div>
            ) : null}
          </div>
        ) : chartError && activeBars.length === 0 ? (
          <div className="empty-state">
            <div>{chartError}</div>
            {timeframe !== "1D" && timeframe !== "1W" ? (
              <div className="empty-state-subtitle">If the intraday feed is slow, try Refresh Chart once more.</div>
            ) : null}
          </div>
        ) : !chartLoading && !chartError && activeBars.length === 0 ? (
          <div className="empty-state">
            <div>No chart data available for {symbol} on {timeframe}.</div>
            <div className="empty-state-subtitle">Try a different timeframe or refresh — the data feed may be temporarily unavailable.</div>
          </div>
        ) : (
        <div className="chart-stage">
          {symbol ? (
            <div className="chart-stage-quick-actions">
              <button
                type="button"
                className="chart-stage-action-button"
                onClick={() => onAddToWatchlist?.(symbol)}
                aria-label={`Add ${symbol} to a watchlist`}
                title={`Add ${symbol} to watchlist`}
              >
                +
              </button>
              {onRemoveFromWatchlist ? (
                <button
                  type="button"
                  className="chart-stage-action-button danger"
                  onClick={() => onRemoveFromWatchlist(symbol)}
                  aria-label={`Remove ${symbol} from this watchlist`}
                  title={`Remove ${symbol} from this watchlist`}
                >
                  −
                </button>
              ) : null}
              {onAddToJournal ? (
                <button
                  type="button"
                  className="chart-stage-action-button journal"
                  onClick={() => onAddToJournal(symbol, summary?.last_price ?? activeBars[activeBars.length - 1]?.close)}
                  aria-label={`Add ${symbol} to open positions`}
                  title={`Add ${symbol} to Journal open positions`}
                >
                  J
                </button>
              ) : null}
            </div>
          ) : null}
          {favoritesWidget.enabled ? (
            <div
              className="favorites-floating-widget"
              style={{
                left: `${clamp(favoritesWidget.x, 0, Math.max(stageWidth - favoritesWidget.width - 8, 0))}px`,
                top: `${clamp(favoritesWidget.y, 0, Math.max(stageHeight - favoritesWidget.height - 8, 0))}px`,
                width: `${favoritesWidget.width}px`,
                minHeight: `${favoritesWidget.height}px`,
                borderColor: `color-mix(in srgb, ${favoritesWidget.accentColor} 52%, transparent)`,
              }}
              onPointerDown={(event) => event.stopPropagation()}
            >
              <div className="favorites-widget-head" onPointerDown={(event) => beginFavoritesWidgetDrag(event, "move")}>
                <strong style={{ color: favoritesWidget.accentColor }}>Favourites</strong>
                <button
                  type="button"
                  onPointerDown={(event) => event.stopPropagation()}
                  onClick={() => setFavoritesWidget((current) => ({ ...current, enabled: false }))}
                  aria-label="Close Favourites widget"
                >
                  ×
                </button>
              </div>
              <div className="favorites-widget-actions">
                {favoritesSettings.enabled && favoriteItems.length > 0 ? (
                  favoriteItems.map((item) => (
                    <button
                      key={`fav-widget-action-${item.id}`}
                      type="button"
                      className={isFavoriteItemActive(item.id) ? "favorites-widget-action active" : "favorites-widget-action"}
                      style={isFavoriteItemActive(item.id) ? { borderColor: favoritesWidget.accentColor, color: favoritesWidget.accentColor } : undefined}
                      onClick={() => runFavoriteItem(item.id)}
                    >
                      <span>{item.label}</span>
                      <small>{item.kind}</small>
                    </button>
                  ))
                ) : (
                  <span className="favorites-widget-empty">No favourites selected</span>
                )}
              </div>
              <div className="favorites-widget-controls">
                <label className="chart-toggle-row">
                  <input
                    type="checkbox"
                    checked={favoritesSettings.enabled}
                    onChange={(event) => setFavoritesSettings((current) => ({ ...current, enabled: event.target.checked }))}
                  />
                  <span>Enabled</span>
                </label>
                <label className="favorites-widget-color">
                  <span>Colour</span>
                  <input
                    type="color"
                    value={favoritesWidget.accentColor}
                    onChange={(event) => setFavoritesWidget((current) => ({ ...current, accentColor: event.target.value }))}
                  />
                </label>
              </div>
              <div className="favorites-widget-manage">
                {FAVORITE_ITEMS.map((item) => (
                  <button
                    key={`fav-widget-manage-${item.id}`}
                    type="button"
                    className={favoritesSettings.itemIds.includes(item.id) ? "chart-favourite-option active" : "chart-favourite-option"}
                    style={favoritesSettings.itemIds.includes(item.id) ? { borderColor: favoritesWidget.accentColor } : undefined}
                    onClick={() => toggleFavoriteItem(item.id)}
                  >
                    <span>{item.label}</span>
                    <small>{item.kind}</small>
                  </button>
                ))}
              </div>
              <div className="favorites-widget-resize" onPointerDown={(event) => beginFavoritesWidgetDrag(event, "resize")} />
            </div>
          ) : null}
          {showRvol && currentRvol ? (
            <div
              ref={rvolWidgetRef}
              className={`rvol-widget rvol-widget--${rvolScale}`}
              style={{
                ...(rvolPos ? { left: rvolPos.x, top: rvolPos.y, bottom: "auto", right: "auto" } : {}),
                borderColor: `color-mix(in srgb, ${rvolAccentColor} 45%, transparent)`,
              }}
            >
              <div className="rvol-widget-header" onPointerDown={handleRvolDragStart}>
                <span className="rvol-widget-title" style={{ color: rvolAccentColor }}>
                  RVOL <span className="rvol-widget-subtitle">vs 50d avg</span>
                </span>
                <div className="rvol-widget-controls" onPointerDown={(e) => e.stopPropagation()}>
                  {(["sm", "md", "lg"] as const).map((s) => (
                    <button
                      key={s}
                      type="button"
                      className={`rvol-size-btn${rvolScale === s ? " active" : ""}`}
                      onClick={() => setRvolScale(s)}
                      style={rvolScale === s ? { borderColor: rvolAccentColor, color: rvolAccentColor } : {}}
                      title={s === "sm" ? "Small" : s === "md" ? "Medium" : "Large"}
                    >
                      {s === "sm" ? "S" : s === "md" ? "M" : "L"}
                    </button>
                  ))}
                  <input
                    type="color"
                    className="rvol-color-input"
                    value={rvolAccentColor}
                    onChange={(e) => setRvolAccentColor(e.target.value)}
                    title="Widget colour"
                  />
                  <button
                    type="button"
                    className="rvol-close-btn"
                    onClick={() => setShowRvol(false)}
                    title="Hide widget"
                    aria-label="Close RVOL widget"
                  >
                    ×
                  </button>
                </div>
              </div>
              <div className="rvol-widget-row">
                <span className="rvol-label">Vol</span>
                <span className="rvol-value" style={{ color: rvolToneColor(currentRvol.rvol50) }}>
                  {currentRvol.rvol50.toFixed(2)}×
                </span>
                <span className="rvol-bar-track">
                  <span
                    className="rvol-bar-fill"
                    style={{
                      width: `${Math.min(currentRvol.rvol50 / 4, 1) * 100}%`,
                      background: rvolToneColor(currentRvol.rvol50),
                    }}
                  />
                </span>
              </div>
              <div className="rvol-widget-row">
                <span className="rvol-label">Turn</span>
                <span className="rvol-value" style={{ color: rvolToneColor(currentRvol.turnoverRvol50) }}>
                  {currentRvol.turnoverRvol50.toFixed(2)}×
                </span>
                <span className="rvol-bar-track">
                  <span
                    className="rvol-bar-fill"
                    style={{
                      width: `${Math.min(currentRvol.turnoverRvol50 / 4, 1) * 100}%`,
                      background: rvolToneColor(currentRvol.turnoverRvol50),
                    }}
                  />
                </span>
              </div>
              <div className="rvol-widget-detail">
                {formatVolumeShort(currentRvol.volume, market)} / avg {formatVolumeShort(currentRvol.avgVolume, market)}
              </div>
              <div className="rvol-widget-detail">
                {formatTurnoverShort(currentRvol.turnover, market)} / avg {formatTurnoverShort(currentRvol.avgTurnover, market)}
              </div>
            </div>
          ) : null}
        <div className="chart-stage-meta">
            <span className={`chart-stage-label chart-stage-label--ohlc ${hoveredPriceTrendClass}`} style={{ color: palette.textColor, background: palette.background, borderColor: palette.borderColor }}>
              <span>{priceLine1}</span>
              {priceLine2 ? <span style={{ opacity: 0.75 }}>{priceLine2}</span> : null}
            </span>
            {tradeHoverLines.length ? (
              <span
                className="chart-stage-label chart-stage-label--trade"
                style={{ color: palette.textColor, background: palette.background, borderColor: palette.borderColor }}
              >
                {tradeHoverLines.map((line, idx) => (
                  <span
                    key={`trade-hover-${idx}`}
                    style={{ color: line.startsWith("B") ? "#10b981" : "#ef4444" }}
                  >
                    {line}
                  </span>
                ))}
              </span>
            ) : null}
            <span className={`chart-stage-label ${rsTrendClass}`} style={{ color: palette.textColor, background: palette.background, borderColor: palette.borderColor }}>
              {hoveredRsPoint ? `RS Rating ${Math.round(hoveredRsPoint.value)} on ${formatChartDateFromTimestamp(hoveredRsPoint.time)}` : "RS Rating line is plotted below price."}
            </span>
            {draftTrendStart ? <span className="chart-stage-label emphasis" style={{ background: palette.background, borderColor: palette.borderColor }}>{chartSubtitle(drawingTool, draftTrendStart, chartStyle)}</span> : null}
          </div>
          {pocketPivotWidget.enabled ? (
            <div
              className="pocket-pivot-widget"
              style={{
                left: `${clamp(pocketPivotWidget.x, 0, Math.max(stageWidth - pocketPivotWidget.width - 8, 0))}px`,
                top: `${clamp(pocketPivotWidget.y, 0, Math.max(stageHeight - pocketPivotWidget.height - 8, 0))}px`,
                width: `${pocketPivotWidget.width}px`,
                minHeight: `${pocketPivotWidget.height}px`,
              }}
              onPointerDown={(event) => event.stopPropagation()}
            >
              <div className="pocket-pivot-widget-head" onPointerDown={(event) => beginPocketPivotWidgetDrag(event, "move")}>
                <strong>Pocket Pivot</strong>
                <button
                  type="button"
                  onPointerDown={(event) => event.stopPropagation()}
                  onClick={() => setPocketPivotWidget((current) => ({ ...current, enabled: false }))}
                  aria-label="Close Pocket Pivot widget"
                >
                  ×
                </button>
              </div>
              <div className="pocket-pivot-widget-body">
                <div>
                  <span>Found</span>
                  <strong>{pocketPivotBars.length}</strong>
                </div>
                <div>
                  <span>Latest</span>
                  <strong>{latestPocketPivot ? formatChartDateFromTimestamp(latestPocketPivot.time, market) : "—"}</strong>
                </div>
                <div>
                  <span>Latest RVOL</span>
                  <strong>{latestPocketPivot?.rvol == null ? "—" : `${formatValue(latestPocketPivot.rvol, 2)}x`}</strong>
                </div>
              </div>
              <label className="pocket-pivot-control">
                <span>Dot Color</span>
                <input
                  type="color"
                  value={pocketPivotWidget.dotColor}
                  onChange={(event) => setPocketPivotWidget((current) => ({ ...current, dotColor: event.target.value }))}
                />
              </label>
              <label className="pocket-pivot-control">
                <span>Dot Size</span>
                <input
                  type="range"
                  min="0.5"
                  max="8"
                  step="0.5"
                  value={pocketPivotWidget.dotSize}
                  onChange={(event) => setPocketPivotWidget((current) => ({ ...current, dotSize: Number(event.target.value) }))}
                />
                <strong>{pocketPivotWidget.dotSize}px</strong>
              </label>
              <div className="pocket-pivot-resize" onPointerDown={(event) => beginPocketPivotWidgetDrag(event, "resize")} />
            </div>
          ) : null}
          {earningsWidget.enabled ? (
            <div
              className="earnings-widget"
              style={{
                left: `${clamp(earningsWidget.x, 0, Math.max(stageWidth - earningsWidget.width - 8, 0))}px`,
                top: `${clamp(earningsWidget.y, 0, Math.max(stageHeight - earningsWidget.height - 8, 0))}px`,
                width: `${earningsWidget.width}px`,
                minHeight: `${earningsWidget.height}px`,
                borderColor: withOpacity(earningsWidget.accentColor, 0.54),
              }}
              onPointerDown={(event) => event.stopPropagation()}
            >
              <div className="earnings-widget-head" onPointerDown={(event) => beginEarningsWidgetDrag(event, "move")}>
                <strong>{earningsTitle}</strong>
                <button
                  type="button"
                  onPointerDown={(event) => event.stopPropagation()}
                  onClick={() => setEarningsWidget((current) => ({ ...current, enabled: false }))}
                  aria-label="Close Earnings widget"
                >
                  x
                </button>
              </div>
              <div className="earnings-widget-controls">
                <label className="pocket-pivot-control">
                  <span>Color</span>
                  <input
                    type="color"
                    value={earningsWidget.accentColor}
                    onChange={(event) => setEarningsWidget((current) => ({ ...current, accentColor: event.target.value }))}
                  />
                </label>
                <label className="pocket-pivot-control">
                  <span>Quarters</span>
                  <input
                    type="range"
                    min="1"
                    max="8"
                    step="1"
                    value={earningsWidget.quarters}
                    onChange={(event) => setEarningsWidget((current) => ({ ...current, quarters: Number(event.target.value) }))}
                  />
                  <strong>{earningsWidget.quarters}</strong>
                </label>
                <label className="pocket-pivot-control">
                  <span>Growth</span>
                  <button
                    type="button"
                    className={earningsWidget.growthMode === "yoy" ? "earnings-growth-toggle active" : "earnings-growth-toggle"}
                    onClick={() => setEarningsWidget((current) => ({ ...current, growthMode: "yoy" }))}
                  >
                    YoY
                  </button>
                  <button
                    type="button"
                    className={earningsWidget.growthMode === "qoq" ? "earnings-growth-toggle active" : "earnings-growth-toggle"}
                    onClick={() => setEarningsWidget((current) => ({ ...current, growthMode: "qoq" }))}
                  >
                    QoQ
                  </button>
                </label>
              </div>
              <div className="earnings-widget-body">
                {!symbol ? (
                  <div className="earnings-widget-empty">Pick a stock.</div>
                ) : earningsError && !earningsSource ? (
                  <div className="earnings-widget-empty">{earningsError}</div>
                ) : !earningsSource ? (
                  <div className="earnings-widget-empty">Loading earnings...</div>
                ) : visibleQuarterlyResults.length ? (
                  <>
                    <div className="earnings-widget-metrics">
                      <span>MCap</span>
                      <strong>{formatAmountValue(earningsValuation?.market_cap_crore, 1)}</strong>
                      <span>P/E</span>
                      <strong>{formatValue(earningsValuation?.pe_ratio, 1)}</strong>
                      <span>ROE</span>
                      <strong>{formatPercentValue(earningsValuation?.roe_pct)}</strong>
                    </div>
                    <table>
                      <thead>
                        <tr>
                          <th>Qtr</th>
                          <th>EPS</th>
                          <th>{earningsWidget.growthMode === "qoq" ? "QoQ%" : "YoY%"}</th>
                          <th>Sales</th>
                          <th>{earningsWidget.growthMode === "qoq" ? "QoQ%" : "YoY%"}</th>
                          <th>OPM</th>
                        </tr>
                      </thead>
                      <tbody>
                        {visibleQuarterlyResults.map((item) => {
                          const epsGrowth = earningsWidget.growthMode === "qoq" ? item.eps_qoq_pct : item.eps_yoy_pct;
                          const salesGrowth = earningsWidget.growthMode === "qoq" ? item.sales_qoq_pct : item.sales_yoy_pct;
                          return (
                            <tr key={`earnings-${item.period}`}>
                              <td>{item.period}</td>
                              <td className={(item.eps ?? 0) >= 0 ? "positive" : "negative"}>{formatValue(item.eps, 1)}</td>
                              <td className={(epsGrowth ?? 0) >= 0 ? "positive" : "negative"}>{formatSignedPercentValue(epsGrowth)}</td>
                              <td>{formatValue(item.sales_crore, 1)}</td>
                              <td className={(salesGrowth ?? 0) >= 0 ? "positive" : "negative"}>{formatSignedPercentValue(salesGrowth)}</td>
                              <td className={(item.operating_margin_pct ?? 0) >= 0 ? "positive" : "negative"}>{formatPercentValue(item.operating_margin_pct)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                    <div className="earnings-widget-range">
                      <span>▼ 52W HIGH</span>
                      <strong>{formatPercentValue(earningsMetrics.pct_from_52w_high)}</strong>
                      <span>TOver ₹ | 1D</span>
                      <strong>{formatAmountValue(earningsMetrics.turnover_1d_crore, 2)}</strong>
                      <span>▲ 52W LOW</span>
                      <strong>{formatPercentValue(earningsMetrics.pct_from_52w_low)}</strong>
                      <span>Avg TOver | 50</span>
                      <strong>{formatAmountValue(earningsMetrics.avg_turnover_50d_crore, 2)}</strong>
                      <span>ADR | 20</span>
                      <strong>{formatPercentValue(earningsMetrics.adr_pct_20)}</strong>
                      <span>RVOL(x) | 50</span>
                      <strong>{formatValue(earningsMetrics.relative_volume, 2)}</strong>
                    </div>
                  </>
                ) : (
                  <div className="earnings-widget-empty">No quarterly earnings data available.</div>
                )}
              </div>
              <div className="earnings-widget-resize" onPointerDown={(event) => beginEarningsWidgetDrag(event, "resize")} />
            </div>
          ) : null}
          {notesWidget.enabled ? (
            <div
              className="notes-widget"
              style={{
                left: `${clamp(notesWidget.x, 0, Math.max(stageWidth - notesWidget.width - 8, 0))}px`,
                top: `${clamp(notesWidget.y, 0, Math.max(stageHeight - notesWidget.height - 8, 0))}px`,
                width: `${notesWidget.width}px`,
                minHeight: `${notesWidget.height}px`,
              }}
              onPointerDown={(event) => event.stopPropagation()}
            >
              <div className="notes-widget-head" onPointerDown={(event) => beginNotesWidgetDrag(event, "move")}>
                <strong>Notes</strong>
                <small>{normalizedSymbol || "No symbol"}</small>
                <button
                  type="button"
                  onPointerDown={(event) => event.stopPropagation()}
                  onClick={() => setNotesWidget((current) => ({ ...current, enabled: false }))}
                  aria-label="Close Notes widget"
                >
                  ×
                </button>
              </div>
              <textarea
                className="notes-widget-textarea"
                value={pocketPivotNote}
                onChange={(event) => updatePocketPivotNote(event.target.value)}
                placeholder="Add notes for this stock..."
                style={{
                  color: notesWidget.noteColor,
                  fontFamily: notesWidget.noteFont,
                  fontSize: `${notesWidget.noteFontSize}px`,
                }}
                disabled={!normalizedSymbol}
              />
              <div className="pocket-pivot-note-controls">
                <label className="pocket-pivot-control">
                  <span>Text Color</span>
                  <input
                    type="color"
                    value={notesWidget.noteColor}
                    onChange={(event) => updatePocketPivotNoteStyle({ noteColor: event.target.value })}
                  />
                </label>
                <label className="pocket-pivot-control">
                  <span>Font</span>
                  <select
                    value={notesWidget.noteFont}
                    onChange={(event) => updatePocketPivotNoteStyle({ noteFont: event.target.value })}
                  >
                    <option value="Inter, system-ui, sans-serif">Sans</option>
                    <option value="Georgia, serif">Serif</option>
                    <option value="'SFMono-Regular', Consolas, monospace">Mono</option>
                  </select>
                </label>
                <label className="pocket-pivot-control">
                  <span>Font Size</span>
                  <input
                    type="range"
                    min="10"
                    max="28"
                    step="1"
                    value={notesWidget.noteFontSize}
                    onChange={(event) => updatePocketPivotNoteStyle({ noteFontSize: Number(event.target.value) })}
                  />
                  <strong>{notesWidget.noteFontSize}px</strong>
                </label>
              </div>
              <div className="notes-widget-resize" onPointerDown={(event) => beginNotesWidgetDrag(event, "resize")} />
            </div>
          ) : null}
          <div
            ref={stageRef}
            className={drawingTool === "none" ? "chart-stage-hitbox" : "chart-stage-hitbox drawing-active"}
          >
            <div ref={containerRef} className="chart-canvas" />
            {drawingTool !== "none" ? (
              <div
                ref={interactionLayerRef}
                className="chart-interaction-layer"
                onPointerDown={handleStagePointerDown}
                onPointerMove={handleStagePointerMove}
                onPointerLeave={handleStagePointerLeave}
                onPointerUp={handleStagePointerUp}
              />
            ) : null}
            <svg
              className="chart-overlay"
              width={Math.max(stageWidth, 1)}
              height={Math.max(stageHeight, 1)}
              viewBox={`0 0 ${Math.max(stageWidth, 1)} ${Math.max(stageHeight, 1)}`}
              preserveAspectRatio="none"
            >
              {autoZoneOverlays}
              {autoTrendlineOverlays}
              {verticalLineOverlays}
              {horizontalLineOverlays}
              {trendlineOverlays}
              {rayOverlays}
              {rectangleOverlays}
              {measureOverlays}
              {draftPoint ? <circle cx={draftPoint.x} cy={draftPoint.y} r="5" fill="#ffd36f" /> : null}
              {draftPoint && hoverPoint && drawingTool === "trendline" ? (
                <line
                  x1={draftPoint.x}
                  y1={draftPoint.y}
                  x2={hoverPoint.x}
                  y2={hoverPoint.y}
                  stroke="#ffd36f"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeDasharray="4 4"
                />
              ) : null}
              {draftPoint && hoverPoint && drawingTool === "ray" ? (
                <line
                  x1={draftPoint.x}
                  y1={draftPoint.y}
                  x2={projectRayEnd(draftPoint, hoverPoint, stageWidth).x}
                  y2={projectRayEnd(draftPoint, hoverPoint, stageWidth).y}
                  stroke="#8ee6ff"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeDasharray="4 4"
                />
              ) : null}
              {draftPoint && hoverPoint && drawingTool === "rectangle" ? (
                <rect
                  x={Math.min(draftPoint.x, hoverPoint.x)}
                  y={Math.min(draftPoint.y, hoverPoint.y)}
                  width={Math.max(Math.abs(hoverPoint.x - draftPoint.x), 2)}
                  height={Math.max(Math.abs(hoverPoint.y - draftPoint.y), 2)}
                  rx="6"
                  fill="rgba(89, 196, 255, 0.1)"
                  stroke="#59c4ff"
                  strokeWidth="1.6"
                  strokeDasharray="5 4"
                />
              ) : null}
              {draftPoint && hoverPoint && drawingTool === "measure" ? (
                <g>
                  <line
                    x1={draftPoint.x}
                    y1={draftPoint.y}
                    x2={hoverPoint.x}
                    y2={hoverPoint.y}
                    stroke="#4bf0b3"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeDasharray="5 4"
                  />
                  {draftMeasureLabel ? (
                    <>
                      <rect
                        x={clamp(((draftPoint.x + hoverPoint.x) / 2) - 92, 6, Math.max(stageWidth - 188, 6))}
                        y={clamp(((draftPoint.y + hoverPoint.y) / 2) - 26, 8, Math.max(stageHeight - 30, 8))}
                        width="184"
                        height="24"
                        rx="8"
                        fill="rgba(4, 8, 17, 0.92)"
                      />
                      <text
                        x={clamp(((draftPoint.x + hoverPoint.x) / 2), 98, Math.max(stageWidth - 98, 98))}
                        y={clamp(((draftPoint.y + hoverPoint.y) / 2) - 10, 23, Math.max(stageHeight - 15, 23))}
                        fill="#4bf0b3"
                        fontSize="11"
                        textAnchor="middle"
                      >
                        {draftMeasureLabel}
                      </text>
                    </>
                  ) : null}
                </g>
              ) : null}
              {selectedAnnotationHandles}
              {hoverPoint ? <circle cx={hoverPoint.x} cy={hoverPoint.y} r="4" fill="#ffd36f" /> : null}
            </svg>
            <div className="chart-note-layer">{textOverlays}</div>
            {selectedAnnotation && annotationEditPos ? (
              <div
                className="annotation-edit-panel"
                style={{ left: annotationEditPos.x, top: annotationEditPos.y }}
                onClick={(e) => e.stopPropagation()}
              >
                <label className="annotation-edit-color" title="Line color">
                  <input
                    type="color"
                    value={selectedAnnotation.color ?? ANNOTATION_DEFAULT_COLORS[selectedAnnotation.type] ?? "#ffd36f"}
                    onChange={(e) => updateAnnotation(selectedAnnotation.id, { color: e.target.value } as any)}
                  />
                </label>
                {selectedAnnotation.type !== "text" ? (
                  <div className="annotation-edit-widths">
                    {([1, 2, 3] as const).map((w) => (
                      <button
                        key={w}
                        type="button"
                        className={(selectedAnnotation.lineWidth ?? 2) === w ? "width-btn active" : "width-btn"}
                        onClick={() => updateAnnotation(selectedAnnotation.id, { lineWidth: w } as any)}
                        title={`Line width ${w}`}
                      >
                        <span style={{ display: "block", height: w + 1, width: 16, background: "currentColor", borderRadius: 2 }} />
                      </button>
                    ))}
                  </div>
                ) : (
                  <button
                    type="button"
                    className="annotation-edit-edit"
                    onClick={() => editTextAnnotation(selectedAnnotation.id)}
                    title="Edit text"
                  >
                    Edit
                  </button>
                )}
                <button
                  type="button"
                  className="annotation-edit-delete"
                  onClick={() => deleteAnnotation(selectedAnnotation.id)}
                  title="Delete drawing"
                >
                  Delete
                </button>
                <button
                  type="button"
                  className="annotation-edit-close"
                  onClick={() => setSelectedAnnotationId(null)}
                  title="Deselect"
                >
                  ✕
                </button>
              </div>
            ) : null}
          </div>
        </div>
        )
      ) : !symbol ? (
        <div className="empty-state">Pick a stock to view fundamentals.</div>
      ) : fundamentalsLoading ? (
        <div className="empty-state">Loading fundamentals for {symbol}...</div>
      ) : fundamentalsError ? (
        <div className="empty-state">{fundamentalsError}</div>
      ) : !fundamentals ? (
        <div className="empty-state">Fundamentals are not available for this stock yet.</div>
      ) : (
        <div className="fundamentals-layout">
          <section className="fundamentals-card fundamentals-overview-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>{fundamentals.name}</h3>
                <p>
                  {fundamentals.exchange} • {fundamentals.sector ?? "Unclassified"} • {fundamentals.sub_sector ?? "Unclassified"}
                </p>
              </div>
              <span className="fundamentals-stamp">Updated {formatDateTime(fundamentals.fetched_at)}</span>
            </div>
            <p>{fundamentals.about ?? "Recent business summary is not available right now."}</p>
          </section>



          {/* Management Team */}
          {fundamentals.management_team && fundamentals.management_team.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Management Team</h3>
                  <p>Key leadership driving the company's strategic direction.</p>
                </div>
              </div>
              <div className="management-team-grid">
                {fundamentals.management_team.map((member, index) => (
                  <div key={index} className="management-member-card">
                    <h4>{member.name}</h4>
                    <p className="member-position">{member.position}</p>
                    <p className="member-background">{member.background}</p>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Management Guidance */}
          {fundamentals.management_guidance && fundamentals.management_guidance.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Management Guidance & Outlook</h3>
                  <p>Forward-looking guidance from management and strategic plans.</p>
                </div>
              </div>
              <div className="management-guidance-list">
                {fundamentals.management_guidance.map((guidance, index) => (
                  <div key={index} className="guidance-item">
                    <h4>{guidance.fiscal_year} Guidance</h4>
                    <div className="guidance-metrics">
                      {guidance.revenue_growth_guidance_pct !== null && (
                        <div className="guidance-metric">
                          <span className="metric-label">Revenue Growth:</span>
                          <span className="metric-value">{guidance.revenue_growth_guidance_pct}%</span>
                        </div>
                      )}
                      {guidance.ebitda_guidance_pct !== null && (
                        <div className="guidance-metric">
                          <span className="metric-label">EBITDA Target:</span>
                          <span className="metric-value">{guidance.ebitda_guidance_pct}%</span>
                        </div>
                      )}
                      {guidance.capex_guidance_crore !== null && (
                        <div className="guidance-metric">
                          <span className="metric-label">CapEx Plan:</span>
                          <span className="metric-value">{formatAmountValue(guidance.capex_guidance_crore, 0)}</span>
                        </div>
                      )}
                    </div>
                    {guidance.key_guidance_points && guidance.key_guidance_points.length > 0 && (
                      <div className="guidance-points">
                        <strong>Key Initiatives:</strong>
                        <ul>
                          {guidance.key_guidance_points.map((point, i) => (
                            <li key={i}>{point}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Strategy and Outlook */}
          {fundamentals.strategy_and_outlook && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Strategy & Long-term Outlook</h3>
                  <p>Management's strategic vision and competitive positioning for the future.</p>
                </div>
              </div>
              <p className="strategy-text">{fundamentals.strategy_and_outlook}</p>
            </section>
          )}

          {/* Competitive Position */}
          {fundamentals.competitive_position && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Competitive Position & Market Standing</h3>
                  <p>How the company stacks up against competitors in the market.</p>
                </div>
              </div>
              <div className="competitive-position-section">
                <div className="comp-position-item">
                  <span className="comp-label">Market Position:</span>
                  <strong>{fundamentals.competitive_position.market_position}</strong>
                </div>
                <div className="comp-position-item">
                  <span className="comp-label">Market Share:</span>
                  <strong>{fundamentals.competitive_position.market_share_estimate}%</strong>
                </div>
                {fundamentals.competitive_position.competitive_advantages && fundamentals.competitive_position.competitive_advantages.length > 0 && (
                  <div className="comp-advantages">
                    <strong>Competitive Advantages:</strong>
                    <ul>
                      {fundamentals.competitive_position.competitive_advantages.map((adv, i) => (
                        <li key={i}>{adv}</li>
                      ))}
                    </ul>
                  </div>
                )}
                {fundamentals.competitive_position.key_competitors && fundamentals.competitive_position.key_competitors.length > 0 && (
                  <div className="competitors-list">
                    <strong>Key Competitors:</strong>
                    <div className="competitors-tags">
                      {fundamentals.competitive_position.key_competitors.map((comp, i) => (
                        <span key={i} className="competitor-tag">{comp}</span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </section>
          )}

          {/* Business Segments */}
          {fundamentals.business_segments && fundamentals.business_segments.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Business Segments & Revenue Mix</h3>
                  <p>Revenue breakdown by business unit and growth trajectories.</p>
                </div>
              </div>
              <div className="fundamentals-table-wrap">
                <table className="fundamentals-table">
                  <thead>
                    <tr>
                      <th>Segment</th>
                      <th>Revenue</th>
                      <th>Revenue %</th>
                      <th>Growth %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fundamentals.business_segments.map((segment, index) => (
                      <tr key={index}>
                        <td>{segment.name}</td>
                        <td>{formatAmountValue(segment.revenue_crore, 0)}</td>
                        <td>{formatPercentValue(segment.revenue_pct)}</td>
                        <td className={segment.growth_pct !== null && segment.growth_pct > 0 ? "positive" : "negative"}>
                          {segment.growth_pct !== null ? (segment.growth_pct > 0 ? "+" : "") + formatPercentValue(segment.growth_pct) : "N/A"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* Geographic Presence */}
          {fundamentals.geographic_presence && fundamentals.geographic_presence.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Geographic Presence</h3>
                  <p>Revenue distribution and market presence across regions.</p>
                </div>
              </div>
              <div className="geographic-presence-list">
                {fundamentals.geographic_presence.map((region, index) => (
                  <div key={index} className="geographic-item">{region}</div>
                ))}
              </div>
            </section>
          )}

          {/* Balance Sheet */}
          {fundamentals.balance_sheet && fundamentals.balance_sheet.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Balance Sheet Analysis</h3>
                  <p>Financial position and asset allocation snapshot.</p>
                </div>
              </div>
              <div className="fundamentals-table-wrap">
                <table className="fundamentals-table">
                  <thead>
                    <tr>
                      <th>Period</th>
                      <th>Total Assets</th>
                      <th>Total Liabilities</th>
                      <th>Equity</th>
                      <th>Debt</th>
                      <th>Cash</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fundamentals.balance_sheet.map((item, index) => (
                      <tr key={index}>
                        <td>{item.period}</td>
                        <td>{formatAmountValue(item.total_assets_crore, 0)}</td>
                        <td>{formatAmountValue(item.total_liabilities_crore, 0)}</td>
                        <td>{formatAmountValue(item.shareholders_equity_crore, 0)}</td>
                        <td>{formatAmountValue(item.debt_crore, 0)}</td>
                        <td>{formatAmountValue(item.cash_and_equivalents_crore, 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* Cash Flow */}
          {fundamentals.cash_flow && fundamentals.cash_flow.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Cash Flow Analysis</h3>
                  <p>How the company generates and uses cash from operations.</p>
                </div>
              </div>
              <div className="fundamentals-table-wrap">
                <table className="fundamentals-table">
                  <thead>
                    <tr>
                      <th>Period</th>
                      <th>Operating CF</th>
                      <th>Free Cash Flow</th>
                      <th>CapEx</th>
                      <th>Dividends Paid</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fundamentals.cash_flow.map((item, index) => (
                      <tr key={index}>
                        <td>{item.period}</td>
                        <td>{formatAmountValue(item.operating_cash_flow_crore, 0)}</td>
                        <td>{formatAmountValue(item.free_cash_flow_crore, 0)}</td>
                        <td>{formatAmountValue(item.capital_expenditure_crore, 0)}</td>
                        <td>{formatAmountValue(item.dividends_paid_crore, 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* Financial Ratios */}
          {fundamentals.financial_ratios && fundamentals.financial_ratios.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Financial Ratios & Metrics</h3>
                  <p>Key financial metrics for profitability, efficiency, and solvency analysis.</p>
                </div>
              </div>
              <div className="fundamentals-table-wrap">
                <table className="fundamentals-table">
                  <thead>
                    <tr>
                      <th>Period</th>
                      <th>ROE</th>
                      <th>ROA</th>
                      <th>ROCE</th>
                      <th>Current Ratio</th>
                      <th>D/E Ratio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fundamentals.financial_ratios.map((item, index) => (
                      <tr key={index}>
                        <td>{item.period}</td>
                        <td>{formatPercentValue(item.roe_pct)}</td>
                        <td>{formatPercentValue(item.roa_pct)}</td>
                        <td>{formatPercentValue(item.roce_pct)}</td>
                        <td>{formatValue(item.current_ratio, 2)}</td>
                        <td>{formatValue(item.debt_to_equity_ratio, 2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* Risk Analysis */}
          {fundamentals.risks_and_opportunities && fundamentals.risks_and_opportunities.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Risk Analysis & Opportunities</h3>
                  <p>Key risks and growth opportunities for the company ahead.</p>
                </div>
              </div>
              <div className="risks-opportunities-list">
                {fundamentals.risks_and_opportunities.map((item, index) => (
                  <div key={index} className={`risk-opportunity-item risk-${item.risk_category.toLowerCase()}`}>
                    <div className="risk-header">
                      <h4>{item.risk_category}</h4>
                      <span className={`severity-badge severity-${item.severity.toLowerCase()}`}>{item.severity}</span>
                    </div>
                    <p className="risk-description">{item.description}</p>
                    <div className="mitigation-strategy">
                      <strong>Mitigation/Strategy:</strong>
                      <p>{item.mitigation_strategy}</p>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Detailed News Articles */}
          {fundamentals.detailed_news && fundamentals.detailed_news.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Detailed News & Developments</h3>
                  <p>In-depth analysis of recent company news and market developments.</p>
                </div>
              </div>
              <div className="detailed-news-list">
                {fundamentals.detailed_news.map((newsItem, index) => (
                  <article key={index} className="detailed-news-article">
                    <div className="news-header">
                      <h4>{newsItem.title}</h4>
                      <div className="news-meta">
                        <span className={`news-impact impact-${newsItem.impact_category.toLowerCase()}`}>
                          {newsItem.impact_category}
                        </span>
                        <span className={`sentiment-badge sentiment-${newsItem.sentiment}`}>
                          {newsItem.sentiment}
                        </span>
                        <span className="news-source">{newsItem.source}</span>
                        <span className="news-date">{formatDateTime(newsItem.published_date)}</span>
                      </div>
                    </div>
                    <p className="news-summary">{newsItem.summary}</p>
                    {newsItem.detailed_points && newsItem.detailed_points.length > 0 && (
                      <ul className="news-detailed-points">
                        {newsItem.detailed_points.map((point, i) => (
                          <li key={i}>{point}</li>
                        ))}
                      </ul>
                    )}
                    <div className="news-relevance">Relevance Score: {Math.round(newsItem.relevance_score * 100)}%</div>
                  </article>
                ))}
              </div>
            </section>
          )}

          {/* Latest Earnings Key Metrics */}
          {fundamentals.latest_earnings_key_metrics && Object.keys(fundamentals.latest_earnings_key_metrics).length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Latest Earnings Key Metrics</h3>
                  <p>Summary of the most recent quarterly or annual results.</p>
                </div>
              </div>
              <div className="earnings-metrics-grid">
                {Object.entries(fundamentals.latest_earnings_key_metrics).map(([key, value]) => (
                  <div key={key} className="earnings-metric">
                    <span className="metric-label">{key}</span>
                    <strong className="metric-value">{value}</strong>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Upcoming Events */}
          {fundamentals.upcoming_events && fundamentals.upcoming_events.length > 0 && (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Upcoming Events & Catalysts</h3>
                  <p>Important dates and potential market-moving events ahead.</p>
                </div>
              </div>
              <div className="upcoming-events-list">
                {fundamentals.upcoming_events.map((event, index) => (
                  <div key={index} className="upcoming-event">
                    <div className="event-date">{event.date}</div>
                    <div className="event-content">
                      <h4>{event.event}</h4>
                      <p>{event.impact}</p>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>Valuation & Margins</h3>
                <p>Built from the latest reported numbers and current market profile.</p>
              </div>
            </div>
            <div className="fundamentals-stat-grid">
              {ratioCards.map((card) => (
                <div key={card.label} className="fundamentals-stat">
                  <span>{card.label}</span>
                  <strong>{card.value}</strong>
                </div>
              ))}
            </div>
          </section>

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>Latest Growth</h3>
                <p>{growth?.latest_period ? `Recent quarter: ${growth.latest_period}` : "Recent quarter growth snapshot"}</p>
              </div>
            </div>
            <div className="fundamentals-stat-grid">
              {growthCards.map((card) => (
                <div key={card.label} className="fundamentals-stat">
                  <span>{card.label}</span>
                  <strong>{card.value}</strong>
                </div>
              ))}
            </div>
          </section>

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>Growth Drivers</h3>
                <p>What is supporting or pressuring the current business momentum.</p>
              </div>
            </div>
            <div className="fundamentals-driver-list">
              {fundamentals.growth_drivers.length ? (
                fundamentals.growth_drivers.map((driver, index) => (
                  <article key={`${driver.title}-${index}`} className={`fundamentals-driver ${driver.tone}`}>
                    <strong>{driver.title}</strong>
                    <p>{driver.detail}</p>
                  </article>
                ))
              ) : (
                <div className="empty-state">No recent growth drivers are available for this company right now.</div>
              )}
            </div>
          </section>

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>Quarterly Results</h3>
                <p>Sales, profit, and margin progression from recent reported quarters.</p>
              </div>
            </div>
            <div className="fundamentals-table-wrap">
              <table className="fundamentals-table">
                <thead>
                  <tr>
                    <th>Quarter</th>
                    <th>Sales</th>
                    <th>OP</th>
                    <th>OPM</th>
                    <th>PBT</th>
                    <th>Net Profit</th>
                    <th>EPS</th>
                  </tr>
                </thead>
                <tbody>
                  {fundamentals.quarterly_results.map((item) => (
                    <tr key={item.period}>
                      <td>{item.period}</td>
                      <td>{formatAmountValue(item.sales_crore)}</td>
                      <td>{formatAmountValue(item.operating_profit_crore)}</td>
                      <td>{formatPercentValue(item.operating_margin_pct)}</td>
                      <td>{formatAmountValue(item.profit_before_tax_crore)}</td>
                      <td>{formatAmountValue(item.net_profit_crore)}</td>
                      <td>{formatValue(item.eps, 2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>Profit & Loss</h3>
                <p>Annual view to understand how the business has compounded over time.</p>
              </div>
            </div>
            <div className="fundamentals-table-wrap">
              <table className="fundamentals-table">
                <thead>
                  <tr>
                    <th>Period</th>
                    <th>Sales</th>
                    <th>OP</th>
                    <th>OPM</th>
                    <th>Net Profit</th>
                    <th>EPS</th>
                    <th>Dividend Payout</th>
                  </tr>
                </thead>
                <tbody>
                  {fundamentals.profit_loss.map((item) => (
                    <tr key={item.period}>
                      <td>{item.period}</td>
                      <td>{formatAmountValue(item.sales_crore)}</td>
                      <td>{formatAmountValue(item.operating_profit_crore)}</td>
                      <td>{formatPercentValue(item.operating_margin_pct)}</td>
                      <td>{formatAmountValue(item.net_profit_crore)}</td>
                      <td>{formatValue(item.eps, 2)}</td>
                      <td>{formatPercentValue(item.dividend_payout_pct)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>{ownershipLabels.title}</h3>
                <p>{ownershipLabels.description}</p>
              </div>
            </div>
            {fundamentals.shareholding_delta ? (
              <div className="fundamentals-stat-grid fundamentals-stat-grid-compact">
                <div className="fundamentals-stat">
                  <span>{ownershipLabels.promoterChange}</span>
                  <strong>{formatSignedPercentValue(fundamentals.shareholding_delta.promoter_change_pct)}</strong>
                </div>
                <div className="fundamentals-stat">
                  <span>{ownershipLabels.fiiChange}</span>
                  <strong>{formatSignedPercentValue(fundamentals.shareholding_delta.fii_change_pct)}</strong>
                </div>
                <div className="fundamentals-stat">
                  <span>{ownershipLabels.diiChange}</span>
                  <strong>{formatSignedPercentValue(fundamentals.shareholding_delta.dii_change_pct)}</strong>
                </div>
                <div className="fundamentals-stat">
                  <span>Public Change</span>
                  <strong>{formatSignedPercentValue(fundamentals.shareholding_delta.public_change_pct)}</strong>
                </div>
              </div>
            ) : null}
            <div className="fundamentals-table-wrap">
              <table className="fundamentals-table">
                <thead>
                  <tr>
                    <th>Period</th>
                    <th>{ownershipLabels.promoter}</th>
                    <th>{ownershipLabels.fii}</th>
                    <th>{ownershipLabels.dii}</th>
                    <th>Public</th>
                    <th>Shareholders</th>
                  </tr>
                </thead>
                <tbody>
                  {fundamentals.shareholding_pattern.map((item) => (
                    <tr key={item.period}>
                      <td>{item.period}</td>
                      <td>{formatPercentValue(item.promoter_pct)}</td>
                      <td>{formatPercentValue(item.fii_pct)}</td>
                      <td>{formatPercentValue(item.dii_pct)}</td>
                      <td>{formatPercentValue(item.public_pct)}</td>
                      <td>{formatCountValue(item.shareholder_count)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="fundamentals-card">
            <div className="fundamentals-card-head">
              <div>
                <h3>Recent News, Results & Calls</h3>
                <p>Recent public updates merged from filings and headline/news feeds.</p>
              </div>
            </div>
            <div className="fundamentals-update-list">
              {fundamentals.recent_updates.length ? (
                fundamentals.recent_updates.map((item, index) => (
                  <article key={`${item.title}-${index}`} className="fundamentals-update">
                    <div className="fundamentals-update-meta">
                      <span className={`fundamentals-badge ${item.kind}`}>{updateKindLabel(item.kind)}</span>
                      <span>{item.source}</span>
                      <span>{formatDateTime(item.published_at)}</span>
                    </div>
                    {item.link ? (
                      <a href={item.link} target="_blank" rel="noreferrer">
                        {item.title}
                      </a>
                    ) : (
                      <strong>{item.title}</strong>
                    )}
                    {item.summary ? <p>{item.summary}</p> : null}
                  </article>
                ))
              ) : (
                <div className="empty-state">No recent news or company updates are available right now.</div>
              )}
            </div>
          </section>

          {fundamentals.ai_news_summary ? (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>AI News Summary</h3>
                  <p>AI-generated summary of latest news about this company.</p>
                </div>
                {fundamentals.last_news_update ? (
                  <span className="fundamentals-stamp">Updated {fundamentals.last_news_update}</span>
                ) : null}
              </div>
              <div className="ai-news-content">
                <p className="ai-summary-text">{fundamentals.ai_news_summary.summary}</p>
                {fundamentals.ai_news_summary.key_points.length ? (
                  <div className="ai-key-points">
                    <strong>Key Points:</strong>
                    <ul>
                      {fundamentals.ai_news_summary.key_points.map((point, index) => (
                        <li key={index}>{point}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                <span className={`sentiment-badge sentiment-${fundamentals.ai_news_summary.sentiment}`}>
                  Sentiment: {fundamentals.ai_news_summary.sentiment}
                </span>
              </div>
            </section>
          ) : null}

          {fundamentals.business_triggers.length ? (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Business Triggers</h3>
                  <p>Recent developments likely to impact stock price.</p>
                </div>
              </div>
              <div className="business-triggers-list">
                {fundamentals.business_triggers.map((trigger, index) => (
                  <article key={index} className="business-trigger-item">
                    <div className="trigger-header">
                      <strong>{trigger.title}</strong>
                      <span className={`trigger-impact-badge impact-${trigger.impact}`}>{trigger.impact}</span>
                    </div>
                    <p className="trigger-description">{trigger.description}</p>
                    <div className="trigger-meta">
                      <span className="trigger-source">{trigger.source}</span>
                      <span className="trigger-date">{trigger.date}</span>
                      <span className="trigger-likelihood">
                        Price Impact Likelihood: {Math.round(trigger.likelihood_to_impact * 100)}%
                      </span>
                    </div>
                  </article>
                ))}
              </div>
            </section>
          ) : null}

          {fundamentals.insider_transactions.length ? (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Insider Transactions</h3>
                  <p>Recent insider buying and selling activity.</p>
                </div>
              </div>
              <div className="fundamentals-table-wrap">
                <table className="fundamentals-table insider-transactions-table">
                  <thead>
                    <tr>
                      <th>Person</th>
                      <th>Position</th>
                      <th>Type</th>
                      <th>Quantity</th>
                      <th>Price</th>
                      <th>Total Value</th>
                      <th>Date</th>
                      <th>% Change</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fundamentals.insider_transactions.map((txn, index) => (
                      <tr key={index} className={`insider-txn-${txn.transaction_type}`}>
                        <td>{txn.person_name}</td>
                        <td>{txn.position}</td>
                        <td>
                          <span className={`transaction-badge txn-${txn.transaction_type}`}>
                            {txn.transaction_type.toUpperCase()}
                          </span>
                        </td>
                        <td>{formatCountValue(txn.quantity)}</td>
                        <td>{formatPriceValue(txn.price_per_share, 2)}</td>
                        <td>{formatAmountValue(txn.total_value_crore)}</td>
                        <td>{txn.date}</td>
                        <td className={txn.pct_of_holding_change > 0 ? "positive" : "negative"}>
                          {txn.pct_of_holding_change > 0 ? "+" : ""}{formatPercentValue(txn.pct_of_holding_change)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {fundamentals.data_warnings.length ? (
            <section className="fundamentals-card">
              <div className="fundamentals-card-head">
                <div>
                  <h3>Data Notes</h3>
                  <p>Useful context when a public data source is missing or delayed.</p>
                </div>
              </div>
              <div className="fundamentals-warning-list">
                {fundamentals.data_warnings.map((warning, index) => (
                  <span key={`${warning}-${index}`} className="fundamentals-warning-pill">
                    {warning}
                  </span>
                ))}
              </div>
            </section>
          ) : null}
        </div>
      )}
    </Panel>
  );
}
