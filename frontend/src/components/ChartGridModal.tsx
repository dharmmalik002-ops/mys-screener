import { createPortal } from "react-dom";
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type PointerEvent as ReactPointerEvent, type ReactNode, type UIEvent } from "react";

import type { ChartBar, ChartGridTimeframe, ChartLinePoint } from "../lib/api";
import { computeAutoLevels, type AutoLevels } from "../lib/levels";

const AUTO_LEVELS_STORAGE_KEY = "stockScanner.chartLevels.v1";
function readAutoLevelsEnabled(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return JSON.parse(window.localStorage.getItem(AUTO_LEVELS_STORAGE_KEY) ?? "false") === true;
  } catch {
    return false;
  }
}
const EMPTY_LEVELS: AutoLevels = { srLevels: [], zones: [], trendlines: [] };

type GridTone = "positive" | "negative" | "neutral";

export type ChartGridChartStyle = "line" | "candles" | "bars";
export type ChartGridDisplayMode = "compact" | "normal";
export type ChartGridSortBy = "selected_return" | "day_return" | "rs_rating" | "market_cap" | "constituents";

type ChartGridBadge = {
  label: string;
  tone?: GridTone;
};

export type ChartGridDisplayCard = {
  id: string;
  symbol?: string;
  entityLabel: string;
  title: string;
  subtitle: string;
  footerValue: string;
  footerLabel?: string;
  primaryBadge: ChartGridBadge;
  secondaryBadge?: ChartGridBadge;
  points: ChartLinePoint[];
  selectedReturn: number;
  dayReturn: number | null;
  rsRating: number | null;
  marketCapCrore: number | null;
  constituents: number | null;
  onClick?: () => void;
};

export type ChartGridStat = {
  label: string;
  value: string;
  tone?: GridTone;
};

type ChartGridModalProps = {
  contextLabel: string;
  title: string;
  subtitle: string;
  cards: ChartGridDisplayCard[];
  stats?: ChartGridStat[];
  columns: number;
  rows: number;
  timeframe: ChartGridTimeframe;
  sortBy: ChartGridSortBy;
  chartStyle: ChartGridChartStyle;
  displayMode: ChartGridDisplayMode;
  loading?: boolean;
  error?: string | null;
  onColumnsChange: (value: number) => void;
  onRowsChange: (value: number) => void;
  onTimeframeChange: (value: ChartGridTimeframe) => void;
  onSortByChange: (value: ChartGridSortBy) => void;
  onChartStyleChange: (value: ChartGridChartStyle) => void;
  onDisplayModeChange: (value: ChartGridDisplayMode) => void;
  onLoadSeries?: (symbols: string[], timeframe: ChartGridTimeframe) => Promise<Record<string, ChartBar[]>>;
  onAddToWatchlist?: (symbol: string) => void;
  onClose: () => void;
};

const GRID_TIMEFRAMES: ChartGridTimeframe[] = ["3M", "6M", "1Y", "2Y"];
const GRID_COLUMNS = [1, 2, 3, 4, 5, 6];
const GRID_ROWS = [1, 2, 3, 4, 5];
const GRID_STYLES: Array<{ value: ChartGridChartStyle; label: string }> = [
  { value: "line", label: "Line" },
  { value: "candles", label: "Candles" },
  { value: "bars", label: "Bars" },
];
const GRID_DISPLAY_MODES: Array<{ value: ChartGridDisplayMode; label: string }> = [
  { value: "compact", label: "Compact" },
  { value: "normal", label: "Normal" },
];
const GRID_SORT_OPTIONS: Array<{ value: ChartGridSortBy; label: string }> = [
  { value: "selected_return", label: "Selected Return" },
  { value: "day_return", label: "1D Return" },
  { value: "rs_rating", label: "RS Rating" },
  { value: "market_cap", label: "Market Cap" },
  { value: "constituents", label: "Constituents" },
];
const GRID_ZOOM_LEVELS = [0.25, 0.4, 0.6, 0.8, 1] as const;

function badgeClassName(tone: GridTone | undefined) {
  return tone === "positive"
    ? "chart-grid-badge positive"
    : tone === "negative"
      ? "chart-grid-badge negative"
      : "chart-grid-badge";
}

function toneFromPoints(points: ChartLinePoint[]): GridTone {
  if (points.length < 2) {
    return "neutral";
  }
  const delta = points[points.length - 1].value - points[0].value;
  if (delta > 0) {
    return "positive";
  }
  if (delta < 0) {
    return "negative";
  }
  return "neutral";
}

function safePoints(points: ChartLinePoint[]) {
  if (points.length > 1) {
    return points;
  }
  return [
    { time: 0, value: points[0]?.value ?? 0 },
    { time: 1, value: points[0]?.value ?? 0 },
  ];
}

function chartWindowBars(timeframe: ChartGridTimeframe) {
  return {
    "3M": 78,
    "6M": 132,
    "1Y": 260,
    "2Y": 520,
  }[timeframe];
}

function chartWindowPoints(timeframe: ChartGridTimeframe) {
  return {
    "3M": 60,
    "6M": 72,
    "1Y": 96,
    "2Y": 120,
  }[timeframe];
}

function visibleWindow<T>(items: T[], size: number, position: number) {
  if (items.length <= size) {
    return items;
  }
  const maxOffset = Math.max(items.length - size, 0);
  const offset = Math.round((Math.max(0, Math.min(position, 100)) / 100) * maxOffset);
  return items.slice(offset, offset + size);
}

function formatAxisTime(timestamp: number, spanDays: number) {
  const date = new Date(timestamp * 1000);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  if (spanDays >= 365) {
    return date.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
  }
  return date.toLocaleDateString(undefined, { day: "numeric", month: "short" });
}

function axisLabels(points: ChartLinePoint[]) {
  if (!points.length) {
    return ["--", "--", "--"];
  }
  const first = points[0];
  const middle = points[Math.floor((points.length - 1) / 2)];
  const last = points[points.length - 1];
  const spanDays = Math.max((last.time - first.time) / 86_400, 1);
  return [
    formatAxisTime(first.time, spanDays),
    formatAxisTime(middle.time, spanDays),
    formatAxisTime(last.time, spanDays),
  ];
}

function formatMarketCap(value: number | null) {
  if (!value || value <= 0) {
    return null;
  }
  if (value >= 100_000) {
    return `${(value / 100_000).toFixed(2)}L Cr`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K Cr`;
  }
  return `${value.toFixed(0)} Cr`;
}

function sortValue(card: ChartGridDisplayCard, sortBy: ChartGridSortBy) {
  if (sortBy === "day_return") {
    return card.dayReturn ?? Number.NEGATIVE_INFINITY;
  }
  if (sortBy === "rs_rating") {
    return card.rsRating ?? Number.NEGATIVE_INFINITY;
  }
  if (sortBy === "market_cap") {
    return card.marketCapCrore ?? Number.NEGATIVE_INFINITY;
  }
  if (sortBy === "constituents") {
    return card.constituents ?? Number.NEGATIVE_INFINITY;
  }
  return card.selectedReturn;
}

function useMeasuredSize() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useEffect(() => {
    const element = ref.current;
    if (!element) {
      return;
    }
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (rect) {
        setSize((current) => {
          const w = Math.round(rect.width);
          const h = Math.round(rect.height);
          return current.w === w && current.h === h ? current : { w, h };
        });
      }
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return { ref, size };
}

// All renderers draw in MEASURED PIXEL coordinates (1:1 viewBox) the way
// TradingView does — geometry is never stretched. Right side reserves a price
// scale; the bottom rows carry month/date labels INSIDE the chart. No grid
// lines, per the owner's "keep it clean" instruction.
const CHART_PAD_TOP = 8;
const CHART_PAD_BOTTOM = 18;
const CHART_PAD_LEFT = 4;
const CHART_PAD_RIGHT = 56;

type OverlayLine = { key: string; color: string; values: Array<number | null> };

// Moving-average overlays (same set the main chart uses).
const MA_OVERLAYS: Array<{ key: string; label: string; color: string; kind: "ema" | "sma"; length: number }> = [
  { key: "e10", label: "10 EMA", color: "#ef4444", kind: "ema", length: 10 },
  { key: "e21", label: "21 EMA", color: "#22c55e", kind: "ema", length: 21 },
  { key: "s50", label: "50 SMA", color: "#3b82f6", kind: "sma", length: 50 },
  { key: "s200", label: "200 SMA", color: "#f4f6fb", kind: "sma", length: 200 },
];

function smaOverlay(values: number[], window: number): Array<number | null> {
  const out: Array<number | null> = new Array(values.length).fill(null);
  let sum = 0;
  for (let i = 0; i < values.length; i += 1) {
    sum += values[i];
    if (i >= window) {
      sum -= values[i - window];
    }
    if (i >= window - 1) {
      out[i] = sum / window;
    }
  }
  return out;
}

function emaOverlay(values: number[], span: number): Array<number | null> {
  const out: Array<number | null> = new Array(values.length).fill(null);
  if (values.length < span) {
    return out;
  }
  let seed = 0;
  for (let i = 0; i < span; i += 1) {
    seed += values[i];
  }
  let ema = seed / span;
  out[span - 1] = ema;
  const k = 2 / (span + 1);
  for (let i = span; i < values.length; i += 1) {
    ema = values[i] * k + ema * (1 - k);
    out[i] = ema;
  }
  return out;
}

function computeMaOverlays(bars: ChartBar[]): OverlayLine[] {
  const closes = bars.map((bar) => bar.close);
  return MA_OVERLAYS.map((config) => ({
    key: config.key,
    color: config.color,
    values: config.kind === "ema" ? emaOverlay(closes, config.length) : smaOverlay(closes, config.length),
  }));
}

function chartScales(w: number, h: number, n: number, min: number, max: number, volumeBand = 0) {
  const innerW = Math.max(w - CHART_PAD_LEFT - CHART_PAD_RIGHT, 1);
  // The price pane reserves a strip at the bottom for the volume histogram
  // (when volumeBand > 0); month labels sit below that, inside CHART_PAD_BOTTOM.
  const priceH = Math.max(h - CHART_PAD_TOP - CHART_PAD_BOTTOM - volumeBand, 1);
  const spread = Math.max(max - min, 1e-6);
  const slot = innerW / Math.max(n, 1);
  const x = (index: number) => CHART_PAD_LEFT + index * slot + slot / 2;
  const y = (value: number) => CHART_PAD_TOP + (1 - (value - min) / spread) * priceH;
  const volTop = CHART_PAD_TOP + priceH;
  return { slot, x, y, innerW, innerH: priceH, volTop, volH: volumeBand };
}

function formatScalePrice(value: number): string {
  if (value >= 10_000) {
    return value.toLocaleString("en-IN", { maximumFractionDigits: 0 });
  }
  if (value >= 1_000) {
    return value.toFixed(0);
  }
  if (value >= 100) {
    return value.toFixed(1);
  }
  return value.toFixed(2);
}

function priceTicks(min: number, max: number, count = 4): number[] {
  const spread = Math.max(max - min, 1e-6);
  const rawStep = spread / (count + 1);
  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const residual = rawStep / magnitude;
  const niceStep = (residual >= 5 ? 5 : residual >= 2 ? 2 : 1) * magnitude;
  const ticks: number[] = [];
  for (let tick = Math.ceil(min / niceStep) * niceStep; tick <= max; tick += niceStep) {
    ticks.push(tick);
  }
  return ticks.slice(0, count + 2);
}

function PriceScale({ w, h, min, max, volumeBand = 0 }: { w: number; h: number; min: number; max: number; volumeBand?: number }) {
  const { y } = chartScales(w, h, 1, min, max, volumeBand);
  return (
    <g className="chart-grid-price-scale" aria-hidden>
      {priceTicks(min, max).map((tick) => (
        <text key={tick} x={w - CHART_PAD_RIGHT + 8} y={y(tick) + 3}>
          {formatScalePrice(tick)}
        </text>
      ))}
    </g>
  );
}

function MonthAxis({ bars, w, h, min, max }: { bars: ChartBar[]; w: number; h: number; min: number; max: number }) {
  const { x } = chartScales(w, h, bars.length, min, max);
  const labels: Array<{ x: number; text: string }> = [];
  let previousMonth = -1;
  let previousYear = -1;
  const spanDays = bars.length > 1 ? (bars[bars.length - 1].time - bars[0].time) / 86_400 : 0;
  bars.forEach((bar, index) => {
    const dateValue = new Date(bar.time * 1000);
    if (Number.isNaN(dateValue.getTime())) {
      return;
    }
    const month = dateValue.getMonth();
    const year = dateValue.getFullYear();
    if (index === 0 || month !== previousMonth) {
      const showYear = spanDays > 360 && (month === 0 || index === 0);
      labels.push({
        x: x(index),
        text: showYear
          ? dateValue.toLocaleDateString(undefined, { month: "short", year: "2-digit" })
          : dateValue.toLocaleDateString(undefined, { month: "short" }),
      });
      previousMonth = month;
      previousYear = year;
    }
  });
  void previousYear;
  // Thin the labels if they would collide (~34px per label).
  const minGap = 34;
  const spaced: Array<{ x: number; text: string }> = [];
  labels.forEach((label) => {
    if (!spaced.length || label.x - spaced[spaced.length - 1].x >= minGap) {
      spaced.push(label);
    }
  });
  return (
    <g className="chart-grid-month-axis" aria-hidden>
      {spaced.map((label) => (
        <text key={`${label.x}:${label.text}`} x={label.x} y={h - 4}>
          {label.text}
        </text>
      ))}
    </g>
  );
}

function OverlayPaths({
  overlays,
  w,
  h,
  min,
  max,
  count,
  volumeBand = 0,
  light = false,
}: {
  overlays: OverlayLine[];
  w: number;
  h: number;
  min: number;
  max: number;
  count: number;
  volumeBand?: number;
  light?: boolean;
}) {
  const { x, y } = chartScales(w, h, count, min, max, volumeBand);
  return (
    <g className="chart-grid-ma" aria-hidden>
      {overlays.map((overlay) => {
        let path = "";
        let pen = false;
        overlay.values.forEach((value, index) => {
          if (value === null || !Number.isFinite(value)) {
            pen = false;
            return;
          }
          path += `${pen ? "L" : "M"}${x(index).toFixed(1)},${y(value).toFixed(1)}`;
          pen = true;
        });
        if (!path) {
          return null;
        }
        // The 200 SMA is white for the dark theme; on a white background flip it
        // to near-black so it stays visible.
        const stroke = light && overlay.key === "s200" ? "#111827" : overlay.color;
        return <path key={overlay.key} d={path} fill="none" stroke={stroke} strokeWidth={1.3} />;
      })}
    </g>
  );
}

function overlayBounds(overlays: OverlayLine[]): [number, number] | null {
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  overlays.forEach((overlay) => {
    overlay.values.forEach((value) => {
      if (value !== null && Number.isFinite(value)) {
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
    });
  });
  return Number.isFinite(min) && Number.isFinite(max) ? [min, max] : null;
}

function Sparkline({ points }: { points: ChartLinePoint[] }) {
  const { ref, size } = useMeasuredSize();
  const normalized = safePoints(points);
  const tone = toneFromPoints(normalized);
  const { w, h } = size;

  let line = "";
  let area = "";
  let bounds: [number, number] | null = null;
  if (w > 10 && h > 10 && normalized.length > 1) {
    const values = normalized.map((point) => point.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    bounds = [min, max];
    const { y, innerW } = chartScales(w, h, normalized.length, min, max);
    const coords = normalized.map((point, index) => {
      const px = CHART_PAD_LEFT + (index / (normalized.length - 1)) * innerW;
      return `${px.toFixed(1)},${y(point.value).toFixed(1)}`;
    });
    line = coords.join(" ");
    area = `${CHART_PAD_LEFT},${h - CHART_PAD_BOTTOM} ${line} ${w - CHART_PAD_RIGHT},${h - CHART_PAD_BOTTOM}`;
  }

  return (
    <div ref={ref} className="chart-grid-canvas">
      {line && bounds ? (
        <svg className={`chart-grid-sparkline ${tone}`} width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden="true">
          <polygon className="chart-grid-sparkline-area" points={area} />
          <polyline className="chart-grid-sparkline-line" points={line} />
          <PriceScale w={w} h={h} min={bounds[0]} max={bounds[1]} />
        </svg>
      ) : null}
    </div>
  );
}

function OhlcChart({
  bars,
  chartStyle,
  overlays,
  showVolume = true,
  light = false,
  levels,
}: {
  bars: ChartBar[];
  chartStyle: ChartGridChartStyle;
  overlays: OverlayLine[];
  showVolume?: boolean;
  light?: boolean;
  levels?: AutoLevels;
}) {
  const { ref, size } = useMeasuredSize();
  const { w, h } = size;
  const ready = w > 10 && h > 10 && bars.length > 1;
  let body: ReactNode[] = [];
  let volumeBars: ReactNode[] = [];
  let zoneRects: ReactNode[] = [];
  let srLines: ReactNode[] = [];
  let min = 0;
  let max = 1;
  // Volume occupies a bottom strip of the chart (TradingView-style), leaving
  // the price pane the rest. Skipped when no volume data is present.
  const hasVolume = showVolume && ready && bars.some((bar) => (bar.volume ?? 0) > 0);
  const volumeBand = hasVolume ? Math.max(Math.min((h - CHART_PAD_TOP - CHART_PAD_BOTTOM) * 0.24, 70), 18) : 0;
  if (ready) {
    const values = bars.flatMap((bar) => [bar.low, bar.high]);
    min = Math.min(...values);
    max = Math.max(...values);
    const maBounds = overlayBounds(overlays);
    if (maBounds) {
      min = Math.min(min, maBounds[0]);
      max = Math.max(max, maBounds[1]);
    }
    const { slot, x, y, volTop, volH } = chartScales(w, h, bars.length, min, max, volumeBand);
    if (hasVolume) {
      const maxVol = Math.max(...bars.map((bar) => bar.volume ?? 0), 1);
      const volWidth = Math.max(slot * 0.6, 0.8);
      volumeBars = bars.map((bar, index) => {
        const vol = bar.volume ?? 0;
        const barH = Math.max((vol / maxVol) * volH, vol > 0 ? 0.6 : 0);
        const tone = bar.close >= bar.open ? "positive" : "negative";
        return (
          <rect
            key={`v${bar.time}:${index}`}
            className={`chart-grid-vol ${tone}`}
            x={x(index) - volWidth / 2}
            y={volTop + volH - barH}
            width={volWidth}
            height={barH}
          />
        );
      });
    }
    if (chartStyle === "candles") {
      const bodyWidth = Math.min(Math.max(slot * 0.65, 1.5), 13);
      body = bars.map((bar, index) => {
        const cx = x(index);
        const openY = y(bar.open);
        const closeY = y(bar.close);
        const top = Math.min(openY, closeY);
        const height = Math.max(Math.abs(closeY - openY), 1);
        const tone = bar.close >= bar.open ? "positive" : "negative";
        return (
          <g key={`${bar.time}:${index}`} className={`chart-grid-candle ${tone}`}>
            <line x1={cx} y1={y(bar.high)} x2={cx} y2={y(bar.low)} />
            <rect x={cx - bodyWidth / 2} y={top} width={bodyWidth} height={height} rx={bodyWidth > 4 ? 0.8 : 0} />
          </g>
        );
      });
    } else {
      const tick = Math.min(Math.max(slot * 0.36, 2), 9);
      body = bars.map((bar, index) => {
        const cx = x(index);
        const tone = bar.close >= bar.open ? "positive" : "negative";
        return (
          <g key={`${bar.time}:${index}`} className={`chart-grid-bar ${tone}`}>
            <line x1={cx} y1={y(bar.high)} x2={cx} y2={y(bar.low)} />
            <line x1={cx - tick} y1={y(bar.open)} x2={cx} y2={y(bar.open)} />
            <line x1={cx} y1={y(bar.close)} x2={cx + tick} y2={y(bar.close)} />
          </g>
        );
      });
    }
    // Auto levels (subtle on small cards): shaded demand/supply zones + dashed
    // S/R lines. Horizontal/price-only, so they render correctly on any window.
    if (levels) {
      const xL = CHART_PAD_LEFT;
      const xR = w - CHART_PAD_RIGHT;
      // Last displayed bar index at or before a zone time (clamps off-screen left to 0).
      const idxAtOrBefore = (t: number) => {
        let idx = 0;
        for (let i = 0; i < bars.length; i += 1) { if (bars[i].time <= t) idx = i; else break; }
        return idx;
      };
      const firstTime = bars[0]?.time ?? 0;
      const lastIdx = bars.length - 1;
      zoneRects = levels.zones.map((zone, index) => {
        const top = y(zone.high);
        const bottom = y(zone.low);
        // Demand colored by timeframe: daily = green, weekly = blue. Supply = red.
        const color = zone.kind === "demand" ? (zone.timeframe === "W" ? "#3b82f6" : "#22c55e") : "#ef4444";
        // Anchor at the origin candle; stop where the band ends (first test / latest bar).
        const sIdx = idxAtOrBefore(zone.startTime);
        const eIdx = idxAtOrBefore(zone.endTime);
        const left = zone.startTime <= firstTime ? xL : x(sIdx);
        const right = eIdx >= lastIdx ? xR : x(eIdx);
        const zx = Math.max(xL, Math.min(left, right));
        const zw = Math.max(Math.min(xR, Math.max(left, right)) - zx, 1);
        return (
          <rect
            key={`gz-${index}`}
            x={zx}
            y={Math.min(top, bottom)}
            width={zw}
            height={Math.max(Math.abs(bottom - top), 1)}
            fill={`${color}1f`}
            stroke={`${color}55`}
            strokeWidth={0.8}
          />
        );
      });
      srLines = levels.srLevels.map((level, index) => {
        const ly = y(level.price);
        const color = level.kind === "support" ? "#22c55e" : "#ef4444";
        return <line key={`gsr-${index}`} x1={xL} y1={ly} x2={xR} y2={ly} stroke={color} strokeWidth={0.9} strokeDasharray="3 3" opacity={0.75} />;
      });
    }
  }
  return (
    <div ref={ref} className="chart-grid-canvas">
      {ready ? (
        <svg className="chart-grid-ohlc" width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden="true">
          {zoneRects}
          {volumeBars}
          {body}
          {srLines}
          <OverlayPaths overlays={overlays} w={w} h={h} min={min} max={max} count={bars.length} volumeBand={volumeBand} light={light} />
          <PriceScale w={w} h={h} min={min} max={max} volumeBand={volumeBand} />
          <MonthAxis bars={bars} w={w} h={h} min={min} max={max} />
        </svg>
      ) : null}
    </div>
  );
}

function GridCard({
  card,
  fullBars,
  chartStyle,
  displayMode,
  timeframe,
  zoomFactor,
  globalPosition,
  hiddenMas,
  light,
  showLevels,
  onAddToWatchlist,
}: {
  card: ChartGridDisplayCard;
  fullBars: ChartBar[];
  chartStyle: ChartGridChartStyle;
  displayMode: ChartGridDisplayMode;
  timeframe: ChartGridTimeframe;
  zoomFactor: number;
  globalPosition: number;
  hiddenMas: ReadonlySet<string>;
  light: boolean;
  showLevels: boolean;
  onAddToWatchlist?: (symbol: string) => void;
}) {
  // Per-card lookback: the slider EXPANDS the time horizon. The right edge is
  // always pinned to today — at the default (100) the chart shows the selected
  // timeframe's window ending today; dragging LEFT stretches the window back
  // through the loaded ~2y of history while today's bar stays in view. Until
  // touched the card follows the toolbar's master Lookback slider.
  const [localPosition, setLocalPosition] = useState<number | null>(null);
  const position = localPosition ?? globalPosition;
  // A timeframe change (pill or a/s/d/f shortcut) is authoritative: drop any
  // per-card slider override so every chart snaps to the chosen window at once.
  useEffect(() => {
    setLocalPosition(null);
  }, [timeframe]);

  const overlaysFull = useMemo(() => (fullBars.length > 1 ? computeMaOverlays(fullBars) : []), [fullBars]);
  // Auto S/R + weekly/monthly zones from the full 2Y series (price-only, so they
  // render on any visible window). Trendlines are omitted on small grid cards.
  const autoLevels = useMemo<AutoLevels>(
    () => (showLevels && fullBars.length > 1 ? computeAutoLevels(fullBars) : EMPTY_LEVELS),
    [showLevels, fullBars],
  );

  const hasBars = fullBars.length > 1;
  const baseWindow = Math.max(12, Math.round(chartWindowBars(timeframe) * zoomFactor));
  const stretch = (100 - Math.max(0, Math.min(position, 100))) / 100; // 0 = base window, 1 = everything
  const windowSize = Math.round(baseWindow + stretch * Math.max(fullBars.length - baseWindow, 0));
  const bars = hasBars ? fullBars.slice(-windowSize) : [];
  const overlays = hasBars
    ? overlaysFull
        .filter((overlay) => !hiddenMas.has(overlay.key))
        .map((overlay) => ({ ...overlay, values: overlay.values.slice(-windowSize) }))
    : [];
  const basePointsWindow = Math.max(12, Math.round(chartWindowPoints(timeframe) * zoomFactor));
  const allPoints = safePoints(card.points);
  const pointsWindow = Math.round(basePointsWindow + stretch * Math.max(allPoints.length - basePointsWindow, 0));
  const scopedPoints = hasBars ? [] : allPoints.slice(-pointsWindow);
  const labels = hasBars ? [] : axisLabels(scopedPoints);
  const metaLabel = card.rsRating !== null ? `RS ${card.rsRating}` : formatMarketCap(card.marketCapCrore);

  return (
    <div className={`chart-grid-card ${displayMode}`}>
      <button
        type="button"
        className={card.onClick ? "chart-grid-card-hit clickable" : "chart-grid-card-hit"}
        onClick={card.onClick}
        disabled={!card.onClick}
      >
        <div className="chart-grid-card-head">
          <div>
            <small className="chart-grid-card-context">{card.entityLabel}</small>
            <strong>{card.title}</strong>
            <small>{card.subtitle}</small>
          </div>
          <div className="chart-grid-card-badges">
            {card.secondaryBadge ? (
              <span className={badgeClassName(card.secondaryBadge.tone)}>{card.secondaryBadge.label}</span>
            ) : null}
            <span className={badgeClassName(card.primaryBadge.tone)}>{card.primaryBadge.label}</span>
          </div>
        </div>

        <div className={`chart-grid-card-chart ${displayMode}`}>
          {hasBars ? (
            <OhlcChart bars={bars} chartStyle={chartStyle} overlays={overlays} light={light} levels={autoLevels} />
          ) : (
            <Sparkline points={scopedPoints} />
          )}
        </div>
      </button>

      {hasBars ? (
        <input
          type="range"
          className="chart-grid-card-slider"
          min={0}
          max={100}
          step={1}
          value={position}
          onChange={(event) => setLocalPosition(Number(event.target.value))}
          title="Drag left to extend the lookback — today\u2019s bar always stays in view"
          aria-label={`Extend ${card.title} lookback`}
        />
      ) : (
        <div className="chart-grid-card-axis">
          {labels.map((label, index) => (
            <span key={`${card.id}:axis:${index}`}>{label}</span>
          ))}
        </div>
      )}

      <div className="chart-grid-card-foot">
        <div>
          <small>{card.footerLabel ?? "Latest"}</small>
          <strong>{card.footerValue}</strong>
        </div>
        <div className="chart-grid-card-foot-right">
          {metaLabel ? <span className="chart-grid-meta-chip">{metaLabel}</span> : null}
          {card.symbol && onAddToWatchlist ? (
            <button
              type="button"
              className="chart-grid-wl-btn"
              title={`Add ${card.symbol} to a watchlist`}
              aria-label={`Add ${card.symbol} to a watchlist`}
              onClick={(event) => {
                event.stopPropagation();
                onAddToWatchlist(card.symbol!);
              }}
            >
              <span aria-hidden>＋</span> Watchlist
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export function ChartGridModal({
  contextLabel,
  title,
  subtitle,
  cards,
  stats = [],
  columns,
  rows,
  timeframe,
  sortBy,
  chartStyle,
  displayMode,
  loading = false,
  error = null,
  onColumnsChange,
  onRowsChange,
  onTimeframeChange,
  onSortByChange,
  onChartStyleChange,
  onDisplayModeChange,
  onLoadSeries,
  onAddToWatchlist,
  onClose,
}: ChartGridModalProps) {
  const modalRef = useRef<HTMLDivElement | null>(null);
  const wallRef = useRef<HTMLDivElement | null>(null);
  // Custom draggable scrollbar pinned to the right of the grid.
  const dragRef = useRef<{ startY: number; startScroll: number; trackH: number; thumbH: number } | null>(null);
  const [scrollbar, setScrollbar] = useState<{ top: number; left: number; height: number; thumbTop: number; thumbH: number; visible: boolean } | null>(null);
  const [rangePosition, setRangePosition] = useState(100);
  const [zoomLevelIndex, setZoomLevelIndex] = useState(GRID_ZOOM_LEVELS.length - 1);
  const [renderCount, setRenderCount] = useState(Math.max(columns * rows * 2, 12));
  const [cleanMode, setCleanMode] = useState(false);
  const [lightMode, setLightMode] = useState(false);
  const [levelsOn, setLevelsOn] = useState<boolean>(() => readAutoLevelsEnabled());
  const [hiddenMas, setHiddenMas] = useState<ReadonlySet<string>>(new Set());
  const [seriesStore, setSeriesStore] = useState<Record<string, ChartBar[]>>({});
  const hasRsData = useMemo(() => cards.some((card) => card.rsRating !== null), [cards]);
  const hasMarketCapData = useMemo(() => cards.some((card) => card.marketCapCrore !== null), [cards]);
  const hasConstituentData = useMemo(() => cards.some((card) => card.constituents !== null), [cards]);

  useEffect(() => {
    setRangePosition(100);
  }, [timeframe]);

  // ESC closes the grid; a/s/d/f switch the timeframe of EVERY chart at once.
  useEffect(() => {
    const TIMEFRAME_KEYS: Record<string, ChartGridTimeframe> = {
      a: "3M",
      s: "6M",
      d: "1Y",
      f: "2Y", // full (longest loaded window)
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
        return;
      }
      // Don't hijack typing in inputs or while modifier keys are held.
      if (event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target?.isContentEditable) return;
      const tf = TIMEFRAME_KEYS[event.key.toLowerCase()];
      if (tf) {
        event.preventDefault();
        onTimeframeChange(tf);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, onTimeframeChange]);

  useEffect(() => {
    modalRef.current?.scrollTo({ top: 0, left: 0, behavior: "auto" });
    if (typeof window !== "undefined") {
      window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    }
  }, [contextLabel, title]);

  const zoomFactor = GRID_ZOOM_LEVELS[zoomLevelIndex];
  const zoomLabel = `${Math.round(zoomFactor * 100)}%`;

  useEffect(() => {
    setRenderCount(Math.min(cards.length, Math.max(columns * rows * 2, 12)));
  }, [cards.length, columns, rows]);

  const availableSortOptions = useMemo(
    () =>
      GRID_SORT_OPTIONS.filter((option) => {
        if (option.value === "rs_rating") {
          return hasRsData;
        }
        if (option.value === "market_cap") {
          return hasMarketCapData;
        }
        if (option.value === "constituents") {
          return hasConstituentData;
        }
        return true;
      }),
    [hasConstituentData, hasMarketCapData, hasRsData],
  );

  const effectiveSortBy = useMemo(() => {
    if (sortBy === "rs_rating" && !hasRsData) {
      return "selected_return";
    }
    if (sortBy === "market_cap" && !hasMarketCapData) {
      return "selected_return";
    }
    if (sortBy === "constituents" && !hasConstituentData) {
      return "selected_return";
    }
    return sortBy;
  }, [hasConstituentData, hasMarketCapData, hasRsData, sortBy]);

  const sortedCards = useMemo(
    () =>
      [...cards].sort((left, right) => {
        const valueDiff = sortValue(right, effectiveSortBy) - sortValue(left, effectiveSortBy);
        if (valueDiff !== 0) {
          return valueDiff;
        }
        const dayDiff = (right.dayReturn ?? 0) - (left.dayReturn ?? 0);
        if (dayDiff !== 0) {
          return dayDiff;
        }
        return (right.marketCapCrore ?? 0) - (left.marketCapCrore ?? 0);
      }),
    [cards, effectiveSortBy],
  );

  const visibleCards = useMemo(() => sortedCards.slice(0, renderCount), [renderCount, sortedCards]);

  useEffect(() => {
    if (!onLoadSeries) {
      return;
    }
    const visibleSymbols = visibleCards
      .map((card) => card.symbol)
      .filter((symbol): symbol is string => Boolean(symbol));
    const missingSymbols = visibleSymbols.filter((symbol) => !seriesStore[`2Y:${symbol}`]);
    if (!missingSymbols.length) {
      return;
    }

    let active = true;
    // Always fetch the full ~2y daily series; the timeframe pills and the
    // per-card sliders slice it client-side (also powers the 200 SMA).
    void onLoadSeries(missingSymbols, "2Y")
      .then((loaded) => {
        if (!active) {
          return;
        }
        setSeriesStore((current) => {
          const next = { ...current };
          Object.entries(loaded).forEach(([symbol, bars]) => {
            next[`2Y:${symbol}`] = bars;
          });
          return next;
        });
      })
      .catch(() => {
        // Keep the sparkline fallback visible when daily bars are unavailable.
      });

    return () => {
      active = false;
    };
  }, [onLoadSeries, seriesStore, timeframe, visibleCards]);

  // Recompute the custom scrollbar's rail + thumb geometry from the live scroll
  // metrics. The modal is centered and stationary, so the rail tracks its right edge.
  const RAIL_TOP_PAD = 56; // clear the top-right Close button
  const RAIL_BOTTOM_PAD = 14;
  const updateScrollbar = useCallback(() => {
    const el = modalRef.current;
    if (!el) {
      return;
    }
    const rect = el.getBoundingClientRect();
    const trackH = rect.height - RAIL_TOP_PAD - RAIL_BOTTOM_PAD;
    const scrollable = el.scrollHeight - el.clientHeight;
    const visible = scrollable > 6 && trackH > 60;
    const thumbH = visible ? Math.max(44, (el.clientHeight / el.scrollHeight) * trackH) : 0;
    const thumbTop = visible && scrollable > 0 ? (el.scrollTop / scrollable) * (trackH - thumbH) : 0;
    setScrollbar({ top: rect.top + RAIL_TOP_PAD, left: rect.right - 16, height: trackH, thumbTop, thumbH, visible });
  }, []);

  const handleScroll = (event: UIEvent<HTMLDivElement>) => {
    const element = event.currentTarget;
    updateScrollbar();
    const nearBottom = element.scrollHeight - element.scrollTop - element.clientHeight < 260;
    if (!nearBottom || renderCount >= sortedCards.length) {
      return;
    }
    setRenderCount((current) => Math.min(sortedCards.length, current + Math.max(columns * rows, 8)));
  };

  // Keep the scrollbar in sync with content growth (lazy-load), zoom, view and resize.
  useEffect(() => {
    const id = requestAnimationFrame(updateScrollbar);
    return () => cancelAnimationFrame(id);
  }, [updateScrollbar, visibleCards.length, displayMode, zoomFactor, stats.length, error]);

  useEffect(() => {
    const el = modalRef.current;
    if (!el || typeof ResizeObserver === "undefined") {
      return;
    }
    // Observe both the modal (viewport) and the inner wall (content grows as
    // charts load asynchronously, which changes scrollHeight without a re-render).
    const ro = new ResizeObserver(() => updateScrollbar());
    ro.observe(el);
    if (wallRef.current) {
      ro.observe(wallRef.current);
    }
    window.addEventListener("resize", updateScrollbar);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", updateScrollbar);
    };
  }, [updateScrollbar, loading]);

  // Drag the thumb (or click the track) to scroll.
  const onThumbPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    const el = modalRef.current;
    if (!el || !scrollbar) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    dragRef.current = { startY: event.clientY, startScroll: el.scrollTop, trackH: scrollbar.height, thumbH: scrollbar.thumbH };
    const onMove = (moveEvent: PointerEvent) => {
      const drag = dragRef.current;
      const node = modalRef.current;
      if (!drag || !node) {
        return;
      }
      const scrollable = node.scrollHeight - node.clientHeight;
      const denom = drag.trackH - drag.thumbH;
      if (denom <= 0) {
        return;
      }
      node.scrollTop = drag.startScroll + ((moveEvent.clientY - drag.startY) * scrollable) / denom;
      updateScrollbar();
    };
    const onUp = () => {
      dragRef.current = null;
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      updateScrollbar();
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  };

  const onTrackPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    const el = modalRef.current;
    if (!el || !scrollbar) {
      return;
    }
    event.stopPropagation();
    const offset = event.clientY - scrollbar.top - scrollbar.thumbH / 2;
    const denom = scrollbar.height - scrollbar.thumbH;
    if (denom <= 0) {
      return;
    }
    const scrollable = el.scrollHeight - el.clientHeight;
    el.scrollTop = Math.max(0, Math.min(scrollable, (offset / denom) * scrollable));
    updateScrollbar();
  };

  return createPortal(
    <div className="chart-modal-backdrop" onClick={onClose}>
      <div
        ref={modalRef}
        className={`chart-grid-modal${cleanMode ? " chart-grid-clean" : ""}${lightMode ? " chart-grid-light" : ""}`}
        onClick={(event) => event.stopPropagation()}
        onScroll={handleScroll}
      >
        <button type="button" className="chart-modal-close" onClick={onClose}>
          Close <kbd>Esc</kbd>
        </button>

        <div className="chart-grid-modal-head">
          <div className="chart-grid-modal-head-main">
            <div className="chart-grid-modal-heading">
              <p className="eyebrow">{contextLabel} Grid</p>
              <h2>{title}</h2>
              <small>{subtitle}</small>
            </div>

            <div className="chart-grid-toolbar">
              <label className="nav-select chart-grid-select">
                <span>View</span>
                <select value={displayMode} onChange={(event) => onDisplayModeChange(event.target.value as ChartGridDisplayMode)}>
                  {GRID_DISPLAY_MODES.map((option) => (
                    <option key={`grid-display-${option.value}`} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="nav-select chart-grid-select">
                <span>Style</span>
                <select value={chartStyle} onChange={(event) => onChartStyleChange(event.target.value as ChartGridChartStyle)}>
                  {GRID_STYLES.map((option) => (
                    <option key={`grid-style-${option.value}`} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="nav-select chart-grid-select">
                <span>Sort</span>
                <select value={effectiveSortBy} onChange={(event) => onSortByChange(event.target.value as ChartGridSortBy)}>
                  {availableSortOptions.map((option) => (
                    <option key={`grid-sort-${option.value}`} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="nav-select chart-grid-select">
                <span>Columns</span>
                <button
                  type="button"
                  className={cleanMode ? "tool-pill active" : "tool-pill"}
                  onClick={() => setCleanMode((current) => !current)}
                  title="Just charts — hide stats and footers"
                  style={{ marginRight: 8 }}
                >
                  ⛶ Clean
                </button>
                <button
                  type="button"
                  className={lightMode ? "tool-pill active" : "tool-pill"}
                  onClick={() => setLightMode((current) => !current)}
                  title="White chart background"
                  style={{ marginRight: 8 }}
                >
                  ☀ White
                </button>
                <button
                  type="button"
                  className={levelsOn ? "tool-pill active" : "tool-pill"}
                  onClick={() =>
                    setLevelsOn((current) => {
                      const next = !current;
                      try {
                        window.localStorage.setItem(AUTO_LEVELS_STORAGE_KEY, JSON.stringify(next));
                      } catch {
                        // ignore storage failures
                      }
                      return next;
                    })
                  }
                  title="Auto support/resistance + weekly/monthly demand-supply zones"
                  style={{ marginRight: 8 }}
                >
                  Auto Levels
                </button>
                <select value={columns} onChange={(event) => onColumnsChange(Number(event.target.value))}>
                  {GRID_COLUMNS.map((value) => (
                    <option key={`grid-col-${value}`} value={value}>
                      {value} per row
                    </option>
                  ))}
                </select>
              </label>

              <label className="nav-select chart-grid-select">
                <span>Rows</span>
                <select value={rows} onChange={(event) => onRowsChange(Number(event.target.value))}>
                  {GRID_ROWS.map((value) => (
                    <option key={`grid-row-${value}`} value={value}>
                      {value} on screen
                    </option>
                  ))}
                </select>
              </label>

              <label className="chart-grid-range">
                <span>Lookback</span>
                <input
                  type="range"
                  min={0}
                  max={100}
                  step={1}
                  value={rangePosition}
                  onChange={(event) => setRangePosition(Number(event.target.value))}
                />
                <small>Drag left for more history</small>
              </label>

              <div className="chart-grid-zoom">
                <span>Zoom</span>
                <div className="chart-grid-zoom-controls">
                  <button
                    type="button"
                    className="tool-pill"
                    onClick={() => setZoomLevelIndex((current) => Math.max(0, current - 1))}
                    disabled={zoomLevelIndex === 0}
                  >
                    +
                  </button>
                  <strong>{zoomLabel}</strong>
                  <button
                    type="button"
                    className="tool-pill"
                    onClick={() => setZoomLevelIndex((current) => Math.min(GRID_ZOOM_LEVELS.length - 1, current + 1))}
                    disabled={zoomLevelIndex === GRID_ZOOM_LEVELS.length - 1}
                  >
                    -
                  </button>
                </div>
              </div>

              <div className="chart-grid-ma-legend">
                {MA_OVERLAYS.map((config) => {
                  const hidden = hiddenMas.has(config.key);
                  return (
                    <button
                      key={`legend-${config.key}`}
                      type="button"
                      className={hidden ? "chart-grid-ma-toggle is-off" : "chart-grid-ma-toggle"}
                      onClick={() =>
                        setHiddenMas((current) => {
                          const next = new Set(current);
                          if (next.has(config.key)) {
                            next.delete(config.key);
                          } else {
                            next.add(config.key);
                          }
                          return next;
                        })
                      }
                      title={hidden ? `Show ${config.label}` : `Hide ${config.label}`}
                    >
                      <i style={{ background: config.color }} />
                      {config.label}
                    </button>
                  );
                })}
              </div>

              <div className="sector-sort-pills chart-grid-timeframes">
                {GRID_TIMEFRAMES.map((option) => {
                  const shortcut = { "3M": "a", "6M": "s", "1Y": "d", "2Y": "f" }[option];
                  const label = option === "2Y" ? "Full" : option;
                  return (
                    <button
                      key={`chart-grid-timeframe-${option}`}
                      type="button"
                      className={timeframe === option ? "tool-pill active" : "tool-pill"}
                      onClick={() => onTimeframeChange(option)}
                      title={`${label} — shortcut: ${shortcut} (applies to all charts)`}
                    >
                      {label}
                      {shortcut ? <kbd className="chart-grid-tf-key">{shortcut}</kbd> : null}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        {stats.length > 0 ? (
          <div className="chart-grid-stats">
            {stats.map((stat) => (
              <article key={`${stat.label}:${stat.value}`} className="chart-grid-stat">
                <span>{stat.label}</span>
                <strong className={stat.tone === "positive" ? "positive-text" : stat.tone === "negative" ? "negative-text" : ""}>
                  {stat.value}
                </strong>
              </article>
            ))}
          </div>
        ) : null}

        {error ? <div className="error-banner">{error}</div> : null}

        <div
          ref={wallRef}
          className={`chart-grid-wall ${displayMode === "normal" ? "normal" : "compact"}`}
          style={
            {
              "--chart-grid-columns": columns,
              "--chart-grid-rows": rows,
            } as CSSProperties
          }
        >
          {loading ? <div className="empty-state">Loading charts...</div> : null}
          {!loading && visibleCards.length === 0 ? <div className="empty-state">No charts are available for this selection yet.</div> : null}
          {!loading
            ? visibleCards.map((card) => (
                <GridCard
                  key={card.id}
                  card={card}
                  fullBars={card.symbol ? (seriesStore[`2Y:${card.symbol}`] ?? []) : []}
                  chartStyle={chartStyle}
                  displayMode={displayMode}
                  timeframe={timeframe}
                  zoomFactor={zoomFactor}
                  globalPosition={rangePosition}
                  hiddenMas={hiddenMas}
                  light={lightMode}
                  showLevels={levelsOn}
                  onAddToWatchlist={onAddToWatchlist}
                />
              ))
            : null}
        </div>

        {scrollbar?.visible ? (
          <div
            className="chart-grid-scrollbar"
            style={{ top: scrollbar.top, left: scrollbar.left, height: scrollbar.height }}
            onPointerDown={onTrackPointerDown}
            role="scrollbar"
            aria-label="Scroll charts"
            aria-orientation="vertical"
          >
            <div
              className="chart-grid-scrollbar-thumb"
              style={{ height: scrollbar.thumbH, transform: `translateY(${scrollbar.thumbTop}px)` }}
              onPointerDown={onThumbPointerDown}
            />
          </div>
        ) : null}
      </div>
    </div>
  , document.body);
}
