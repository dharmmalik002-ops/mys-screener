import { useDeferredValue, useEffect, useId, useMemo, useRef, useState, type ChangeEvent, type PointerEvent as ReactPointerEvent } from "react";
import { ColorType, createChart, type UTCTimestamp } from "lightweight-charts";

import { getChartHistory, type ChartBar, type ChartLineMarker, type ChartLinePoint, type ChartResponse, type CompanyFundamentals, type MarketKey, type StockOverview } from "../lib/api";
import { sanitizeChartBars, sanitizeLineMarkers, sanitizeLinePoints } from "../lib/chartData";
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
const USD_TO_INR = 83;

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

  return bars.slice(length - 1).map((bar, offset) => {
    const startIndex = offset;
    const window = bars.slice(startIndex, startIndex + length);
    const average = window.reduce((sum, item) => sum + item.close, 0) / window.length;
    return {
      time: bar.time as UTCTimestamp,
      value: Number(average.toFixed(2)),
    };
  });
}

function computeVolumeSma(bars: ChartBar[], length: number) {
  if (bars.length < length) {
    return [];
  }

  return bars.slice(length - 1).map((bar, offset) => {
    const startIndex = offset;
    const window = bars.slice(startIndex, startIndex + length);
    const average = window.reduce((sum, item) => sum + item.volume, 0) / window.length;
    return {
      time: bar.time as UTCTimestamp,
      value: Number(average.toFixed(2)),
    };
  });
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
  return market === "us" ? "en-US" : "en-IN";
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
  if (market === "us") {
    const usdValue = (value * 10_000_000) / USD_TO_INR;
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      notation: "compact",
      maximumFractionDigits: digits ?? 1,
    }).format(usdValue);
  }
  return `${formatNumber(value, digits ?? 2, market)} Cr`;
}

function formatPrice(value: number | null | undefined, market: MarketKey, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${market === "us" ? "$" : "₹"}${formatNumber(value, digits, market)}`;
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
  const annotationDragRef = useRef<ActiveAnnotationDrag | null>(null);
  const [drawingTool, setDrawingTool] = useState<DrawingTool>("none");
  const [draftTrendStart, setDraftTrendStart] = useState<ChartAnchor | null>(null);
  const [hoverAnchor, setHoverAnchor] = useState<ChartAnchor | null>(null);
  const [hoveredRsPoint, setHoveredRsPoint] = useState<ChartLinePoint | null>(null);
  const [hoveredBar, setHoveredBar] = useState<HoveredPriceBar | null>(null);
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
  const palette = CHART_PALETTES[chartPalette];
  const activeBars = useMemo(() => sanitizeChartBars(extendedHistory?.bars ?? bars), [bars, extendedHistory]);
  const safeRsLine = useMemo(() => sanitizeLinePoints(extendedHistory?.rs_line ?? rsLine), [extendedHistory, rsLine]);
  const safeRsLineMarkers = useMemo(
    () => sanitizeLineMarkers(extendedHistory?.rs_line_markers ?? rsLineMarkers),
    [extendedHistory, rsLineMarkers],
  );
  const safeBenchmarkBars = useMemo(() => sanitizeChartBars(benchmarkBars ?? []), [benchmarkBars]);
  const futureWhitespaceTimes = useMemo(
    () => buildFutureWhitespaceTimes(activeBars, timeframe, FUTURE_DRAW_EXTENSION_BARS),
    [activeBars, timeframe],
  );
  const benchmarkSymbol = market === "us" ? "SPY" : null;
  const canShowBenchmarkOverlay = market === "us" && Boolean(symbol) && symbol !== benchmarkSymbol;
  const benchmarkOverlayData = useMemo(
    () => (showBenchmarkOverlay ? buildBenchmarkOverlaySeries(activeBars, safeBenchmarkBars) : []),
    [activeBars, safeBenchmarkBars, showBenchmarkOverlay],
  );
  const formatValue = (value: number | null | undefined, digits = 2) => formatNumber(value, digits, market);
  const formatSignedPercentValue = (value: number | null | undefined) => formatPercent(value, market);
  const formatPercentValue = (value: number | null | undefined) => formatPlainPercent(value, market);
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
    setOverlayVersion((version) => version + 1);
  }, [annotations]);

  useEffect(() => {
    onAnnotationsChangeRef.current = onAnnotationsChange;
  }, [onAnnotationsChange]);

  useEffect(() => {
    setDrawingTool("none");
    setDraftTrendStart(null);
    setHoverAnchor(null);
    setHoveredRsPoint(null);
    setHoveredBar(null);
    setSelectedAnnotationId(null);
    annotationDragRef.current = null;
    setDraggingAnnotationHandle(null);
    setExtendedHistory(null);
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
        vertLines: { color: palette.gridColor },
        horzLines: { color: palette.gridColor },
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
            upColor: chartColors.candleUp,
            downColor: chartColors.candleDown,
            thinBars: false,
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

    const updateOverlay = () => {
      setOverlayVersion((version) => version + 1);
    };
    const handleCrosshairMove = (param: any) => {
      if (!param?.time) {
        setHoveredRsPoint(null);
        setHoveredBar(null);
        return;
      }

      const hoveredTime = normalizeChartTime(param.time);
      if (hoveredTime === null) {
        setHoveredRsPoint(null);
        setHoveredBar(null);
        return;
      }

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

    chart.timeScale().subscribeVisibleLogicalRangeChange(updateOverlay);
    chart.subscribeCrosshairMove(handleCrosshairMove);

    const visibleBars = defaultVisibleBars(timeframe);
    const endIndex = Math.max(0, activeBars.length - 1);
    const startIndex = Math.max(0, activeBars.length - visibleBars);
    if (activeBars.length > visibleBars) {
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
      chart.remove();
      chartRef.current = null;
      mainSeriesRef.current = null;
    };
  }, [activeBars, benchmarkOverlayData, chartColors, chartPalette, chartStyle, futureWhitespaceTimes, indicatorKeys, panelTab, palette.background, palette.borderColor, palette.crosshairColor, palette.gridColor, palette.textColor, safeRsLine, safeRsLineMarkers, timeframe]);

  useEffect(() => {
    const handleResize = () => {
      setOverlayVersion((version) => version + 1);
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
    event: ReactPointerEvent<SVGGElement>,
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
            cursor: "pointer",
          }}
          onClick={(e) => { e.stopPropagation(); selectAnnotation(annotation.id); }}
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
  const chartSearchSuggestions = useMemo(
    () => buildSymbolSuggestions(searchOptions, deferredChartSearchQuery, 100),
    [deferredChartSearchQuery, searchOptions],
  );

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
                {TIMEFRAMES.map((item) => (
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
              </div>
              {market === "us" ? (
                <div className="chart-style-switcher">
                  <button
                    type="button"
                    className={showBenchmarkOverlay ? "timeframe-pill active" : "timeframe-pill"}
                    onClick={() => onShowBenchmarkOverlayChange(!showBenchmarkOverlay)}
                    disabled={!canShowBenchmarkOverlay || benchmarkLoading}
                    title={canShowBenchmarkOverlay ? "Overlay SPY on the active price chart." : "SPY overlay is unavailable for SPY itself."}
                  >
                    {benchmarkLoading ? "Loading SPY..." : "SPY Overlay"}
                  </button>
                </div>
              ) : null}
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
              {showBenchmarkOverlay && !benchmarkLoading ? <span className="chart-save-pill">SPY compare</span> : null}
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
      className={expanded ? "chart-panel expanded" : "chart-panel"}
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
          <div className="chart-summary-chip">
            <span>{market === "india" ? "Circuit Band" : "Circuit Limit"}</span>
            <strong>{formatCircuitBand(summary, market)}</strong>
          </div>
        </div>
      ) : null}
      {panelTab === "technical" ? (
        !symbol ? (
          <div className="empty-state">Pick a stock to view the chart.</div>
        ) : chartError && activeBars.length === 0 ? (
          <div className="empty-state">{chartError}</div>
        ) : (
        <div className="chart-stage">
          {symbol ? (
            <button
              type="button"
              className="chart-stage-watchlist-add"
              onClick={() => onAddToWatchlist?.(symbol)}
              aria-label={`Add ${symbol} to a watchlist`}
              title={`Add ${symbol} to watchlist`}
            >
              +
            </button>
          ) : null}
        <div className="chart-stage-meta">
            <span className={`chart-stage-label chart-stage-label--ohlc ${hoveredPriceTrendClass}`} style={{ color: palette.textColor, background: palette.background, borderColor: palette.borderColor }}>
              <span>{priceLine1}</span>
              {priceLine2 ? <span style={{ opacity: 0.75 }}>{priceLine2}</span> : null}
            </span>
            <span className={`chart-stage-label ${rsTrendClass}`} style={{ color: palette.textColor, background: palette.background, borderColor: palette.borderColor }}>
              {hoveredRsPoint ? `RS Rating ${Math.round(hoveredRsPoint.value)} on ${formatChartDateFromTimestamp(hoveredRsPoint.time)}` : "RS Rating line is plotted below price."}
            </span>
            {draftTrendStart ? <span className="chart-stage-label emphasis" style={{ background: palette.background, borderColor: palette.borderColor }}>{chartSubtitle(drawingTool, draftTrendStart, chartStyle)}</span> : null}
          </div>
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
                ) : null}
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
                          <span className="metric-value">{formatAmountValue(guidance.capex_guidance_crore, market === "us" ? 1 : 0)}</span>
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
                        <td>{formatAmountValue(segment.revenue_crore, market === "us" ? 1 : 0)}</td>
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
                        <td>{formatAmountValue(item.total_assets_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.total_liabilities_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.shareholders_equity_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.debt_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.cash_and_equivalents_crore, market === "us" ? 1 : 0)}</td>
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
                        <td>{formatAmountValue(item.operating_cash_flow_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.free_cash_flow_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.capital_expenditure_crore, market === "us" ? 1 : 0)}</td>
                        <td>{formatAmountValue(item.dividends_paid_crore, market === "us" ? 1 : 0)}</td>
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
