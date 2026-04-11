import { useEffect, useMemo, useRef, useState, type CSSProperties, type UIEvent } from "react";

import type { ChartBar, ChartGridTimeframe, ChartLinePoint } from "../lib/api";

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
  onClose: () => void;
};

const GRID_TIMEFRAMES: ChartGridTimeframe[] = ["3M", "6M", "1Y", "2Y"];
const GRID_COLUMNS = [2, 3, 4, 5, 6];
const GRID_ROWS = [2, 3, 4, 5];
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

function normalizeY(value: number, min: number, max: number) {
  const spread = Math.max(max - min, 1e-6);
  return 40 - (((value - min) / spread) * 32) - 4;
}

function barsToPoints(bars: ChartBar[]) {
  return bars.map((bar) => ({ time: bar.time, value: bar.close }));
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

function Sparkline({ points }: { points: ChartLinePoint[] }) {
  const normalized = safePoints(points);
  const values = normalized.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const tone = toneFromPoints(normalized);

  const polyline = normalized
    .map((point, index) => {
      const x = normalized.length === 1 ? 100 : (index / (normalized.length - 1)) * 100;
      const y = normalizeY(point.value, min, max);
      return `${x},${y}`;
    })
    .join(" ");

  const area = `0,36 ${polyline} 100,36`;

  return (
    <svg className={`chart-grid-sparkline ${tone}`} viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden="true">
      <polyline className="chart-grid-sparkline-baseline" points="0,36 100,36" />
      <polygon className="chart-grid-sparkline-area" points={area} />
      <polyline className="chart-grid-sparkline-line" points={polyline} />
    </svg>
  );
}

function CandlePreview({ bars }: { bars: ChartBar[] }) {
  const values = bars.flatMap((bar) => [bar.low, bar.high]);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = bars.length <= 1 ? 90 : 100 / bars.length;

  return (
    <svg className="chart-grid-ohlc" viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden="true">
      <line className="chart-grid-sparkline-baseline" x1="0" y1="36" x2="100" y2="36" />
      {bars.map((bar, index) => {
        const xCenter = (index * width) + (width / 2);
        const bodyWidth = Math.max(width * 0.56, 1.2);
        const openY = normalizeY(bar.open, min, max);
        const closeY = normalizeY(bar.close, min, max);
        const highY = normalizeY(bar.high, min, max);
        const lowY = normalizeY(bar.low, min, max);
        const y = Math.min(openY, closeY);
        const height = Math.max(Math.abs(closeY - openY), 1.2);
        const tone = bar.close >= bar.open ? "positive" : "negative";
        return (
          <g key={`${bar.time}:${index}`} className={`chart-grid-candle ${tone}`}>
            <line x1={xCenter} y1={highY} x2={xCenter} y2={lowY} />
            <rect x={xCenter - (bodyWidth / 2)} y={y} width={bodyWidth} height={height} rx="0.4" />
          </g>
        );
      })}
    </svg>
  );
}

function BarPreview({ bars }: { bars: ChartBar[] }) {
  const values = bars.flatMap((bar) => [bar.low, bar.high]);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = bars.length <= 1 ? 90 : 100 / bars.length;

  return (
    <svg className="chart-grid-ohlc" viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden="true">
      <line className="chart-grid-sparkline-baseline" x1="0" y1="36" x2="100" y2="36" />
      {bars.map((bar, index) => {
        const xCenter = (index * width) + (width / 2);
        const openY = normalizeY(bar.open, min, max);
        const closeY = normalizeY(bar.close, min, max);
        const highY = normalizeY(bar.high, min, max);
        const lowY = normalizeY(bar.low, min, max);
        const tickWidth = Math.max(width * 0.28, 1.1);
        const tone = bar.close >= bar.open ? "positive" : "negative";
        return (
          <g key={`${bar.time}:${index}`} className={`chart-grid-bar ${tone}`}>
            <line x1={xCenter} y1={highY} x2={xCenter} y2={lowY} />
            <line x1={xCenter - tickWidth} y1={openY} x2={xCenter} y2={openY} />
            <line x1={xCenter} y1={closeY} x2={xCenter + tickWidth} y2={closeY} />
          </g>
        );
      })}
    </svg>
  );
}

function MiniChart({
  points,
  bars,
  chartStyle,
}: {
  points: ChartLinePoint[];
  bars: ChartBar[];
  chartStyle: ChartGridChartStyle;
}) {
  if (chartStyle === "candles" && bars.length > 1) {
    return <CandlePreview bars={bars} />;
  }
  if (chartStyle === "bars" && bars.length > 1) {
    return <BarPreview bars={bars} />;
  }
  return <Sparkline points={points} />;
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
  onClose,
}: ChartGridModalProps) {
  const modalRef = useRef<HTMLDivElement | null>(null);
  const [rangePosition, setRangePosition] = useState(100);
  const [zoomLevelIndex, setZoomLevelIndex] = useState(GRID_ZOOM_LEVELS.length - 1);
  const [renderCount, setRenderCount] = useState(Math.max(columns * rows * 2, 12));
  const [seriesStore, setSeriesStore] = useState<Record<string, ChartBar[]>>({});
  const hasRsData = useMemo(() => cards.some((card) => card.rsRating !== null), [cards]);
  const hasMarketCapData = useMemo(() => cards.some((card) => card.marketCapCrore !== null), [cards]);
  const hasConstituentData = useMemo(() => cards.some((card) => card.constituents !== null), [cards]);

  useEffect(() => {
    setRangePosition(100);
  }, [timeframe]);

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
    const missingSymbols = visibleSymbols.filter((symbol) => !seriesStore[`${timeframe}:${symbol}`]);
    if (!missingSymbols.length) {
      return;
    }

    let active = true;
    void onLoadSeries(missingSymbols, timeframe)
      .then((loaded) => {
        if (!active) {
          return;
        }
        setSeriesStore((current) => {
          const next = { ...current };
          Object.entries(loaded).forEach(([symbol, bars]) => {
            next[`${timeframe}:${symbol}`] = bars;
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

  const handleScroll = (event: UIEvent<HTMLDivElement>) => {
    const element = event.currentTarget;
    const nearBottom = element.scrollHeight - element.scrollTop - element.clientHeight < 260;
    if (!nearBottom || renderCount >= sortedCards.length) {
      return;
    }
    setRenderCount((current) => Math.min(sortedCards.length, current + Math.max(columns * rows, 8)));
  };

  return (
    <div className="chart-modal-backdrop" onClick={onClose}>
      <div ref={modalRef} className="chart-grid-modal" onClick={(event) => event.stopPropagation()} onScroll={handleScroll}>
        <button type="button" className="chart-modal-close" onClick={onClose}>
          Close
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
                <span>Date</span>
                <input
                  type="range"
                  min={0}
                  max={100}
                  step={1}
                  value={rangePosition}
                  onChange={(event) => setRangePosition(Number(event.target.value))}
                />
                <small>Earlier to latest</small>
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

              <div className="sector-sort-pills chart-grid-timeframes">
                {GRID_TIMEFRAMES.map((option) => (
                  <button
                    key={`chart-grid-timeframe-${option}`}
                    type="button"
                    className={timeframe === option ? "tool-pill active" : "tool-pill"}
                    onClick={() => onTimeframeChange(option)}
                  >
                    {option}
                  </button>
                ))}
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
            ? visibleCards.map((card) => {
                const bars = card.symbol ? (seriesStore[`${timeframe}:${card.symbol}`] ?? []) : [];
                const scopedBars = visibleWindow(
                  bars,
                  Math.max(12, Math.round(chartWindowBars(timeframe) * zoomFactor)),
                  rangePosition,
                );
                const scopedPoints = bars.length > 1
                  ? barsToPoints(scopedBars)
                  : visibleWindow(
                      safePoints(card.points),
                      Math.max(12, Math.round(chartWindowPoints(timeframe) * zoomFactor)),
                      rangePosition,
                    );
                const labels = axisLabels(scopedPoints);
                const metaLabel = card.rsRating !== null ? `RS ${card.rsRating}` : formatMarketCap(card.marketCapCrore);

                return (
                  <button
                    key={card.id}
                    type="button"
                    className={card.onClick ? `chart-grid-card ${displayMode} clickable` : `chart-grid-card ${displayMode}`}
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
                      <MiniChart points={scopedPoints} bars={scopedBars} chartStyle={chartStyle} />
                    </div>

                    <div className="chart-grid-card-axis">
                      {labels.map((label, index) => (
                        <span key={`${card.id}:axis:${index}`}>{label}</span>
                      ))}
                    </div>

                    <div className="chart-grid-card-foot">
                      <div>
                        <small>{card.footerLabel ?? "Latest"}</small>
                        <strong>{card.footerValue}</strong>
                      </div>
                      {metaLabel ? <span className="chart-grid-meta-chip">{metaLabel}</span> : null}
                    </div>
                  </button>
                );
              })
            : null}
        </div>
      </div>
    </div>
  );
}
