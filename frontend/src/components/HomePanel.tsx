import { Suspense, lazy, useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { createPortal } from "react-dom";
import type { IChartApi, ISeriesApi, UTCTimestamp } from "lightweight-charts";

import {
  getChart,
  getChartGrid,
  getChartGridSeries,
  getIndexPeHistory,
  type ChartBar,
  type ChartGridCard,
  type ChartGridResponse,
  type ChartGridTimeframe,
  type ChartLinePoint,
  type DashboardResponse,
  type IndustryGroupsResponse,
  type IndexPeHistoryResponse,
  type MarketKey,
  type ScanMatch,
  type SectorCard,
  type SectorSortBy,
  type SectorTabResponse,
} from "../lib/api";
import type { ChartGridChartStyle, ChartGridDisplayCard, ChartGridDisplayMode, ChartGridSortBy, ChartGridStat } from "./ChartGridModal";
import { sanitizeChartBars, sanitizeIndexPePoints } from "../lib/chartData";
import { Panel } from "./Panel";

const ChartGridModal = lazy(() => import("./ChartGridModal").then((module) => ({ default: module.ChartGridModal })));

type HomePanelProps = {
  activeMarket: MarketKey;
  dashboard: DashboardResponse | null;
  groups: IndustryGroupsResponse | null;
  onPickSymbol: (symbol: string) => void;
  onOpenGroups: (options?: { groupId?: string; symbol?: string }) => void;
};

type GridTarget =
  | { type: "section"; section: "indices" | "sectors" }
  | { type: "members"; name: string; groupKind: "sector" | "index" };

type HomeGroupFilter = 10 | 20 | 40;
type MoverListKind = "gainers" | "losers" | "active";

const HOME_LIST_SKELETON_COUNT = 6;
const HOME_HEATMAP_SKELETON_COUNTS: Record<MarketKey, { indices: number; sectors: number }> = {
  india: { indices: 4, sectors: 22 },
};

function formatReturn(value: number) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function metricClass(value: number) {
  return value >= 0 ? "positive-text" : "negative-text";
}

function formatPrice(value: number, market: MarketKey) {
  const locale = "en-IN";
  const symbol = "₹";
  return `${symbol}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function shortName(item: ScanMatch) {
  return item.name.length > 32 ? `${item.name.slice(0, 32)}…` : item.name;
}

function getSectorReturn(card: SectorCard, window: SectorSortBy) {
  if (window === "1D") {
    return card.return_1d;
  }
  if (window === "1W") {
    return card.return_1w;
  }
  if (window === "1M") {
    return card.return_1m;
  }
  if (window === "3M") {
    return card.return_3m;
  }
  if (window === "6M") {
    return card.return_6m;
  }
  if (window === "1Y") {
    return card.return_1y;
  }
  return card.return_2y;
}

function getGridReturn(card: SectorCard, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return card.return_3m;
  }
  if (timeframe === "6M") {
    return card.return_6m;
  }
  if (timeframe === "1Y") {
    return card.return_1y;
  }
  return card.return_2y;
}

function topCompanyCount(card: SectorCard) {
  return card.company_count;
}

function heatStyle(value: number, surface: "sector" | "company"): CSSProperties {
  const intensity = Math.min(1, Math.abs(value) / (surface === "sector" ? 5 : 4));

  if (value >= 0) {
    const backgroundAlpha = surface === "sector" ? 0.66 + intensity * 0.18 : 0.32 + intensity * 0.3;
    const borderAlpha = 0.3 + intensity * 0.24;
    return {
      background: `linear-gradient(160deg, rgba(20, 86, 41, ${backgroundAlpha}), rgba(66, 168, 92, ${Math.min(0.95, backgroundAlpha + 0.1)}))`,
      borderColor: `rgba(186, 255, 208, ${borderAlpha})`,
      boxShadow: `inset 0 1px 0 rgba(255,255,255,0.08), 0 0 0 1px rgba(46, 143, 74, ${0.12 + intensity * 0.18})`,
    };
  }

  const backgroundAlpha = surface === "sector" ? 0.66 + intensity * 0.16 : 0.32 + intensity * 0.28;
  const borderAlpha = 0.3 + intensity * 0.22;
  return {
    background: `linear-gradient(160deg, rgba(121, 31, 31, ${backgroundAlpha}), rgba(198, 66, 66, ${Math.min(0.95, backgroundAlpha + 0.1)}))`,
    borderColor: `rgba(255, 216, 216, ${borderAlpha})`,
    boxShadow: `inset 0 1px 0 rgba(255,255,255,0.06), 0 0 0 1px rgba(176, 52, 52, ${0.12 + intensity * 0.16})`,
  };
}

function getMemberGridReturn(card: ChartGridCard, timeframe: ChartGridTimeframe) {
  if (timeframe === "3M") {
    return card.return_3m;
  }
  if (timeframe === "6M") {
    return card.return_6m;
  }
  if (timeframe === "1Y") {
    return card.return_1y;
  }
  return card.return_2y;
}

function downsamplePoints(points: ChartLinePoint[], limit: number) {
  if (points.length <= limit) {
    return points;
  }
  const step = Math.max(points.length / limit, 1);
  const sampled = Array.from({ length: limit }, (_, index) => points[Math.min(points.length - 1, Math.floor(index * step))]);
  if (sampled[sampled.length - 1]?.time !== points[points.length - 1]?.time) {
    sampled[sampled.length - 1] = points[points.length - 1];
  }
  return sampled;
}

function fallbackSparkline(returnPct: number): ChartLinePoint[] {
  const now = Math.floor(Date.now() / 1000);
  const baseline = 100;
  const current = baseline * (1 + (returnPct / 100));
  return [
    { time: now - (63 * 24 * 60 * 60), value: Number(baseline.toFixed(4)) },
    { time: now, value: Number(current.toFixed(4)) },
  ];
}

function sparklineToBars(points: ChartLinePoint[]): ChartBar[] {
  if (!points || points.length < 2) {
    return [];
  }
  return points.map((point, index) => {
    const prev = index > 0 ? points[index - 1].value : point.value;
    const open = Number(prev.toFixed(2));
    const close = Number(point.value.toFixed(2));
    const high = Number(Math.max(open, close).toFixed(2));
    const low = Number(Math.min(open, close).toFixed(2));
    return {
      time: point.time,
      open,
      high,
      low,
      close,
      volume: 0,
    };
  });
}

function scopeSparkline(points: ChartLinePoint[], timeframe: ChartGridTimeframe) {
  if (!points.length) {
    return [];
  }
  const latestTime = points[points.length - 1]?.time ?? Math.floor(Date.now() / 1000);
  const lookbackDays = {
    "3M": 95,
    "6M": 190,
    "1Y": 380,
    "2Y": 760,
  }[timeframe];
  const pointLimit = {
    "3M": 60,
    "6M": 72,
    "1Y": 96,
    "2Y": 120,
  }[timeframe];
  const threshold = latestTime - (lookbackDays * 24 * 60 * 60);
  const scoped = points.filter((point) => point.time >= threshold);
  return downsamplePoints(scoped.length > 1 ? scoped : points, pointLimit);
}

function buildSectionCards(
  cards: SectorCard[],
  market: MarketKey,
  timeframe: ChartGridTimeframe,
  onOpenMembers: (card: SectorCard) => void,
): ChartGridDisplayCard[] {
  return cards.map((card) => {
    const selectedReturn = getGridReturn(card, timeframe);
    const footerLabel = card.group_kind === "index" && typeof card.last_price === "number" ? "Price" : "Constituents";
    const footerValue = card.group_kind === "index" && typeof card.last_price === "number"
      ? formatPrice(card.last_price, market)
      : `${card.company_count}`;
    return {
      id: `${card.group_kind}:${card.sector}`,
      entityLabel: card.group_kind === "index" ? "Index" : "Sector",
      title: card.sector,
      subtitle: `${card.sub_sector_count} groups`,
      footerLabel,
      footerValue,
      primaryBadge: {
        label: `${timeframe} ${formatReturn(selectedReturn)}`,
        tone: selectedReturn >= 0 ? "positive" : "negative",
      },
      secondaryBadge: {
        label: `1D ${formatReturn(card.return_1d)}`,
        tone: card.return_1d >= 0 ? "positive" : "negative",
      },
      points: scopeSparkline(card.sparkline, timeframe).length > 0 ? scopeSparkline(card.sparkline, timeframe) : fallbackSparkline(selectedReturn),
      selectedReturn,
      dayReturn: card.return_1d,
      rsRating: null,
      marketCapCrore: null,
      constituents: card.company_count,
      onClick: () => onOpenMembers(card),
    };
  });
}

function buildMemberCards(
  payload: ChartGridResponse | null,
  timeframe: ChartGridTimeframe,
  onPickSymbol: (symbol: string) => void,
): ChartGridDisplayCard[] {
  return (payload?.cards ?? []).map((card) => {
    const selectedReturn = getMemberGridReturn(card, timeframe);
    return {
      id: `${payload?.group_kind ?? "sector"}:${card.symbol}`,
      symbol: card.symbol,
      entityLabel: "Stock",
      title: card.symbol,
      subtitle: card.name,
      footerLabel: "Price",
      footerValue: card.last_price.toFixed(2),
      primaryBadge: {
        label: `${timeframe} ${formatReturn(selectedReturn)}`,
        tone: selectedReturn >= 0 ? "positive" : "negative",
      },
      secondaryBadge: {
        label: `1D ${formatReturn(card.change_pct)}`,
        tone: card.change_pct >= 0 ? "positive" : "negative",
      },
      points: card.sparkline,
      selectedReturn,
      dayReturn: card.change_pct,
      rsRating: card.rs_rating,
      marketCapCrore: card.market_cap_crore,
      constituents: null,
      onClick: () => onPickSymbol(card.symbol),
    };
  });
}

function gridStatsForMembers(payload: ChartGridResponse | null, timeframe: ChartGridTimeframe): ChartGridStat[] {
  const cards = payload?.cards ?? [];
  const advances = cards.filter((card) => getMemberGridReturn(card, timeframe) > 0).length;
  const declines = cards.filter((card) => getMemberGridReturn(card, timeframe) < 0).length;
  const topWeight = cards.slice(0, 3).map((card) => card.symbol).join(", ");

  return [
    { label: "Stocks", value: `${cards.length}` },
    { label: "Advancing", value: `${advances}`, tone: advances >= declines ? "positive" : "neutral" },
    { label: "Declining", value: `${declines}`, tone: declines > advances ? "negative" : "neutral" },
    { label: "Top Weight", value: topWeight || "--" },
  ];
}

function gridStatsForSection(cards: SectorCard[], timeframe: ChartGridTimeframe): ChartGridStat[] {
  const advances = cards.filter((card) => getGridReturn(card, timeframe) > 0).length;
  const declines = cards.filter((card) => getGridReturn(card, timeframe) < 0).length;
  const leaders = cards.slice(0, 3).map((card) => card.sector).join(", ");

  return [
    { label: "Cards", value: `${cards.length}` },
    { label: "Advancing", value: `${advances}`, tone: advances >= declines ? "positive" : "neutral" },
    { label: "Declining", value: `${declines}`, tone: declines > advances ? "negative" : "neutral" },
    { label: "Leaders", value: leaders || "--" },
  ];
}

function PeChartModal({
  market,
  symbol,
  onClose,
  fallbackBars = [],
}: {
  market: MarketKey;
  symbol: string;
  onClose: () => void;
  fallbackBars?: ChartBar[];
}) {
  const [data, setData] = useState<IndexPeHistoryResponse | null>(null);
  const [priceBars, setPriceBars] = useState<ChartBar[]>([]);
  const [activeTab, setActiveTab] = useState<"price" | "pe">("price");
  const [loading, setLoading] = useState(true);

  const priceContainerRef = useRef<HTMLDivElement | null>(null);
  const peContainerRef = useRef<HTMLDivElement | null>(null);

  function chartHeight(): number {
    if (typeof window === "undefined") return 520;
    const viewH = window.innerHeight;
    if (window.innerWidth <= 480) return Math.max(260, Math.floor(viewH * 0.42));
    if (window.innerWidth <= 768) return Math.max(320, Math.floor(viewH * 0.48));
    return Math.max(460, Math.floor(viewH * 0.58));
  }

  useEffect(() => {
    let active = true;
    setLoading(true);
    Promise.all([
      getIndexPeHistory(symbol, market).catch(() => null),
      getChart(symbol, "1D", market).catch(() => ({ bars: [] as ChartBar[] })),
    ])
      .then(([peRes, priceRes]) => {
        if (!active) return;
        const fetchedPriceBars = priceRes.bars ?? [];
        const rawPriceBars = fetchedPriceBars.length >= 20 || fallbackBars.length < 20 ? fetchedPriceBars : fallbackBars;
        const safePriceBars = sanitizeChartBars(rawPriceBars);
        setPriceBars(safePriceBars);

        const pePoints = sanitizeIndexPePoints(peRes?.points ?? []);
        if (peRes && pePoints.length >= 2) {
          setData({ ...peRes, points: pePoints });
        } else if (safePriceBars.length >= 2) {
          const anchorPe = peRes?.current_pe ?? pePoints[pePoints.length - 1]?.pe ?? 22;
          const tailBars = safePriceBars.slice(-520);
          const lastClose = tailBars[tailBars.length - 1]?.close || 1;
          const proxyPoints = sanitizeIndexPePoints(tailBars.map((bar) => {
            const dt = new Date(bar.time * 1000);
            const date = `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
            const pe = Number((anchorPe * (bar.close / lastClose)).toFixed(2));
            return { date, pe };
          }));
          const avg = proxyPoints.reduce((sum, point) => sum + point.pe, 0) / proxyPoints.length;
          setData({
            symbol,
            label: peRes?.label ?? symbol,
            points: proxyPoints,
            avg_5y: Number(avg.toFixed(2)),
            current_pe: proxyPoints[proxyPoints.length - 1]?.pe ?? anchorPe,
            forward_pe: peRes?.forward_pe ?? null,
            source: "proxy",
          });
        } else {
          setData(peRes ? { ...peRes, points: pePoints } : peRes);
        }
        setLoading(false);
      })
      .catch(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [market, symbol, fallbackBars]);

  const priceSummary = useMemo(() => {
    if (!priceBars || priceBars.length < 2) return null;
    const bars = priceBars.slice(-900);
    const first = bars[0]?.close ?? 0;
    const last = bars[bars.length - 1]?.close ?? 0;
    const high = Math.max(...bars.map((bar) => bar.high));
    const low = Math.min(...bars.map((bar) => bar.low));
    const changePct = first > 0 ? ((last / first) - 1) * 100 : 0;
    return {
      first,
      last,
      high,
      low,
      changePct,
      trendUp: last >= first,
    };
  }, [priceBars]);

  const peSummary = useMemo(() => {
    if (!data || data.points.length < 2) return null;
    const latest = data.current_pe ?? data.points[data.points.length - 1]?.pe ?? null;
    const avg = data.avg_5y;
    const premiumPct = latest && avg ? ((latest / avg) - 1) * 100 : null;
    const min = Math.min(...data.points.map((point) => point.pe));
    const max = Math.max(...data.points.map((point) => point.pe));
    return {
      latest,
      avg,
      min,
      max,
      premiumPct,
      isAboveAvg: premiumPct !== null ? premiumPct > 0 : null,
    };
  }, [data]);

  useEffect(() => {
    if (activeTab !== "price" || !priceContainerRef.current || priceBars.length < 2) {
      return;
    }

    const bars = priceBars.slice(-900);
    let cancelled = false;
    let chart: IChartApi | null = null;
    let handleResize: (() => void) | null = null;

    async function renderChart() {
      const { ColorType, createChart } = await import("lightweight-charts");
      if (cancelled || !priceContainerRef.current) {
        return;
      }

      const baseHeight = chartHeight();
      chart = createChart(priceContainerRef.current, {
        width: priceContainerRef.current.clientWidth,
        height: baseHeight,
        layout: {
          background: { type: ColorType.Solid, color: "transparent" },
          textColor: "#94a3b8",
        },
        grid: {
          vertLines: { color: "rgba(148, 163, 184, 0.14)" },
          horzLines: { color: "rgba(148, 163, 184, 0.14)" },
        },
        crosshair: {
          vertLine: { color: "rgba(125, 211, 252, 0.35)" },
          horzLine: { color: "rgba(125, 211, 252, 0.35)" },
        },
        rightPriceScale: {
          borderColor: "rgba(148, 163, 184, 0.35)",
        },
        timeScale: {
          borderColor: "rgba(148, 163, 184, 0.35)",
        },
      });

      const candleSeries = chart.addCandlestickSeries({
        upColor: "#22c55e",
        downColor: "#ef4444",
        wickUpColor: "#22c55e",
        wickDownColor: "#ef4444",
        borderVisible: false,
      });
      candleSeries.setData(
        bars.map((bar) => ({
          time: bar.time as UTCTimestamp,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
        })),
      );

      const volumeSeries = chart.addHistogramSeries({
        priceScaleId: "",
        priceFormat: { type: "volume" },
      });
      volumeSeries.priceScale().applyOptions({
        scaleMargins: { top: 0.78, bottom: 0 },
      });
      volumeSeries.setData(
        bars.map((bar) => ({
          time: bar.time as UTCTimestamp,
          value: Math.max(bar.volume, 0),
          color: bar.close >= bar.open ? "rgba(34, 197, 94, 0.45)" : "rgba(239, 68, 68, 0.45)",
        })),
      );

      chart.timeScale().fitContent();

      handleResize = () => {
        if (!priceContainerRef.current || !chart) return;
        chart.applyOptions({
          width: priceContainerRef.current.clientWidth,
          height: chartHeight(),
        });
      };
      window.addEventListener("resize", handleResize);
    }

    void renderChart();

    return () => {
      cancelled = true;
      if (handleResize) {
        window.removeEventListener("resize", handleResize);
      }
      chart?.remove();
    };
  }, [activeTab, priceBars]);

  useEffect(() => {
    if (activeTab !== "pe" || !peContainerRef.current || !data || data.points.length < 2) {
      return;
    }

    const points = data.points
      .map((point) => {
        const timestamp = Math.floor(new Date(`${point.date}T00:00:00Z`).getTime() / 1000);
        if (!Number.isFinite(timestamp)) return null;
        return {
          time: timestamp as UTCTimestamp,
          value: point.pe,
        };
      })
      .filter((point): point is { time: UTCTimestamp; value: number } => point !== null);

    if (points.length < 2) {
      return;
    }

    const peData = data;

    let cancelled = false;
    let chart: IChartApi | null = null;
    let handleResize: (() => void) | null = null;

    async function renderChart() {
      const { ColorType, createChart } = await import("lightweight-charts");
      if (cancelled || !peContainerRef.current) {
        return;
      }

      const baseHeight = chartHeight();
      chart = createChart(peContainerRef.current, {
        width: peContainerRef.current.clientWidth,
        height: baseHeight,
        layout: {
          background: { type: ColorType.Solid, color: "transparent" },
          textColor: "#94a3b8",
        },
        grid: {
          vertLines: { color: "rgba(148, 163, 184, 0.14)" },
          horzLines: { color: "rgba(148, 163, 184, 0.14)" },
        },
        crosshair: {
          vertLine: { color: "rgba(125, 211, 252, 0.35)" },
          horzLine: { color: "rgba(125, 211, 252, 0.35)" },
        },
        rightPriceScale: {
          borderColor: "rgba(148, 163, 184, 0.35)",
        },
        timeScale: {
          borderColor: "rgba(148, 163, 184, 0.35)",
        },
      });

      const latestPe = peData.current_pe ?? points[points.length - 1]?.value ?? 0;
      const avgPe = peData.avg_5y ?? 0;
      const lineColor = avgPe > 0 && latestPe > avgPe ? "#ef4444" : "#22c55e";

      const peSeries = chart.addAreaSeries({
        lineColor,
        topColor: lineColor.replace(")", ", 0.25)").replace("rgb", "rgba"),
        bottomColor: lineColor.replace(")", ", 0.03)").replace("rgb", "rgba"),
        lineWidth: 2,
      });
      peSeries.setData(points);

      if (avgPe > 0) {
        const avgSeries = chart.addLineSeries({
          color: "#94a3b8",
          lineWidth: 2,
          lineStyle: 2,
        });
        avgSeries.setData(points.map((point) => ({ time: point.time, value: avgPe })));
      }

      const premiumSeries = chart.addHistogramSeries({
        priceScaleId: "",
        priceFormat: { type: "volume" },
      });
      premiumSeries.priceScale().applyOptions({
        scaleMargins: { top: 0.78, bottom: 0 },
      });
      premiumSeries.setData(
        points.map((point) => {
          const spread = avgPe > 0 ? point.value - avgPe : 0;
          return {
            time: point.time,
            value: Math.abs(spread),
            color: spread >= 0 ? "rgba(239, 68, 68, 0.4)" : "rgba(34, 197, 94, 0.4)",
          };
        }),
      );

      chart.timeScale().fitContent();

      handleResize = () => {
        if (!peContainerRef.current || !chart) return;
        chart.applyOptions({
          width: peContainerRef.current.clientWidth,
          height: chartHeight(),
        });
      };
      window.addEventListener("resize", handleResize);
    }

    void renderChart();

    return () => {
      cancelled = true;
      if (handleResize) {
        window.removeEventListener("resize", handleResize);
      }
      chart?.remove();
    };
  }, [activeTab, data]);

  return (
    <div className="pe-modal-overlay" onClick={onClose}>
      <div className="pe-modal" onClick={(e) => e.stopPropagation()}>
        <div className="pe-modal-header">
          <h3>{data?.label ?? symbol} — Historical Charts</h3>
          <button type="button" className="pe-modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="pe-modal-tabs">
          <button
            type="button"
            className={activeTab === "price" ? "pe-modal-tab active" : "pe-modal-tab"}
            onClick={() => setActiveTab("price")}
          >
            Index Price
          </button>
          <button
            type="button"
            className={activeTab === "pe" ? "pe-modal-tab active" : "pe-modal-tab"}
            onClick={() => setActiveTab("pe")}
          >
            Valuation (P/E)
          </button>
        </div>

        {loading && <div className="pe-modal-loading"><div className="spinner" /></div>}

        {!loading && activeTab === "price" && priceSummary && (
          <>
            <div className="pe-modal-stats">
              <div className="pe-stat">
                <span>Current</span>
                <strong style={{ color: priceSummary.trendUp ? "#22c55e" : "#ef4444" }}>
                  {priceSummary.last.toLocaleString("en-IN", { maximumFractionDigits: 2 })}
                </strong>
              </div>
              <div className="pe-stat">
                <span>5Y Start</span>
                <strong>{priceSummary.first.toLocaleString("en-IN", { maximumFractionDigits: 2 })}</strong>
              </div>
              <div className="pe-stat">
                <span>Change</span>
                <strong style={{ color: priceSummary.trendUp ? "#22c55e" : "#ef4444" }}>
                  {priceSummary.changePct >= 0 ? "+" : ""}{priceSummary.changePct.toFixed(1)}%
                </strong>
              </div>
              <div className="pe-stat">
                <span>Range</span>
                <strong>{priceSummary.low.toFixed(1)} - {priceSummary.high.toFixed(1)}</strong>
              </div>
            </div>

            <div className="pe-rich-chart" ref={priceContainerRef} />

            <p className="pe-modal-note">
              Candlestick + volume chart. Use cursor and zoom for detailed price structure.
            </p>
          </>
        )}

        {!loading && activeTab === "pe" && data && peSummary && (
          <>
            <div className="pe-modal-stats">
              <div className="pe-stat">
                <span>Current P/E</span>
                <strong style={{ color: peSummary.isAboveAvg ? "#ef4444" : "#22c55e" }}>
                  {peSummary.latest?.toFixed(1) ?? "—"}
                </strong>
              </div>
              <div className="pe-stat">
                <span>5-Year Avg</span>
                <strong>{peSummary.avg?.toFixed(1) ?? "—"}</strong>
              </div>
              <div className="pe-stat">
                <span>Valuation</span>
                <strong style={{ color: peSummary.isAboveAvg ? "#ef4444" : "#22c55e" }}>
                  {peSummary.avg && peSummary.latest
                    ? (peSummary.latest > peSummary.avg ? "Above avg ↑" : "Below avg ↓")
                    : "—"}
                </strong>
              </div>
              <div className="pe-stat">
                <span>Min / Max</span>
                <strong>{peSummary.min.toFixed(1)} / {peSummary.max.toFixed(1)}</strong>
              </div>
              {data.forward_pe && (
                <div className="pe-stat">
                  <span>Fwd P/E</span>
                  <strong>{data.forward_pe.toFixed(1)}</strong>
                </div>
              )}
            </div>

            <div className="pe-rich-chart" ref={peContainerRef} />

            <p className="pe-modal-note">
              {data.source === "proxy"
                ? "Historical valuation uses a price-derived proxy because direct valuation history is currently unavailable."
                : ""}
              {peSummary.avg && peSummary.latest && peSummary.latest > peSummary.avg
                ? ` ${`P/E is ${((peSummary.latest / peSummary.avg - 1) * 100).toFixed(0)}% above the 5-year average — market is pricing in premium growth.`}`
                : peSummary.avg && peSummary.latest
                  ? ` ${`P/E is ${((1 - peSummary.latest / peSummary.avg) * 100).toFixed(0)}% below the 5-year average — market is relatively undervalued historically.`}`
                  : ""}
            </p>
          </>
        )}

        {!loading && activeTab === "price" && !priceSummary && (
          <p className="pe-modal-note">Price history not available for this symbol.</p>
        )}

        {!loading && activeTab === "pe" && (!data || data.points.length < 2) && (
          <p className="pe-modal-note">P/E history not available for this symbol.</p>
        )}
      </div>
    </div>
  );
}

export function HomePanel({
  activeMarket,
  dashboard,
  groups,
  onPickSymbol,
  onOpenGroups,
}: HomePanelProps) {
  const [heatmapWindow, setHeatmapWindow] = useState<SectorSortBy>("1D");
  const [groupFilter, setGroupFilter] = useState<HomeGroupFilter>(10);
  const [gridTimeframe, setGridTimeframe] = useState<ChartGridTimeframe>("1Y");
  const [gridSortBy, setGridSortBy] = useState<ChartGridSortBy>("selected_return");
  const [gridChartStyle, setGridChartStyle] = useState<ChartGridChartStyle>("bars");
  const [gridDisplayMode, setGridDisplayMode] = useState<ChartGridDisplayMode>("normal");
  const [gridColumns, setGridColumns] = useState(4);
  const [gridRows, setGridRows] = useState(3);
  const [gridTarget, setGridTarget] = useState<GridTarget | null>(null);
  const [memberGridData, setMemberGridData] = useState<ChartGridResponse | null>(null);
  const [memberGridLoading, setMemberGridLoading] = useState(false);
  const [memberGridError, setMemberGridError] = useState<string | null>(null);
  const memberGridCacheRef = useRef<Record<string, ChartGridResponse>>({});
  const memberGridSeriesCacheRef = useRef<Record<string, ChartBar[]>>({});
  const [peModalSymbol, setPeModalSymbol] = useState<string | null>(null);
  const [moverModal, setMoverModal] = useState<MoverListKind | null>(null);

  const sortedSectorCards = useMemo(
    (): SectorCard[] => [],
    [],
  );

  const indexCards = useMemo(
    () => sortedSectorCards.filter((card) => card.group_kind === "index"),
    [sortedSectorCards],
  );
  const sectorCards = useMemo(
    () => sortedSectorCards.filter((card) => card.group_kind !== "index"),
    [sortedSectorCards],
  );

  const indexFallbackBarsBySymbol = useMemo(() => {
    const map: Record<string, ChartBar[]> = {};
    for (const card of indexCards) {
      const bars = sparklineToBars(card.sparkline ?? []);
      const nameUpper = card.sector.toUpperCase();
      if (nameUpper.includes("NIFTY 50")) map["^NSEI"] = bars;
      if (nameUpper.includes("SMALLCAP 250")) map["^CNXSC"] = bars;
      if (nameUpper.includes("MIDCAP 50")) map["^NSEMDCP50"] = bars;
    }
    return map;
  }, [indexCards]);

  useEffect(() => {
    memberGridCacheRef.current = {};
    memberGridSeriesCacheRef.current = {};
  }, [activeMarket]);

  async function loadMemberGridSeries(symbols: string[], timeframe: ChartGridTimeframe) {
    const keys = symbols.map((symbol) => `${timeframe}:${symbol}`);
    const missingSymbols = symbols.filter((symbol) => !memberGridSeriesCacheRef.current[`${timeframe}:${symbol}`]);

    if (missingSymbols.length > 0) {
      const payload = await getChartGridSeries(missingSymbols, timeframe, activeMarket);
      payload.items.forEach((item) => {
        memberGridSeriesCacheRef.current[`${timeframe}:${item.symbol}`] = item.bars;
      });
    }

    return keys.reduce<Record<string, ChartBar[]>>((accumulator, key, index) => {
      const bars = memberGridSeriesCacheRef.current[key];
      if (bars) {
        accumulator[symbols[index]] = bars;
      }
      return accumulator;
    }, {});
  }

  useEffect(() => {
    if (!gridTarget || gridTarget.type !== "members") {
      setMemberGridData(null);
      setMemberGridLoading(false);
      setMemberGridError(null);
      return;
    }

    const cacheKey = `${activeMarket}:${gridTarget.groupKind}:${gridTarget.name}:${gridTimeframe}`;
    const cached = memberGridCacheRef.current[cacheKey];
    if (cached) {
      setMemberGridData(cached);
      setMemberGridLoading(false);
      setMemberGridError(null);
      return;
    }

    let active = true;
    setMemberGridLoading(true);
    setMemberGridError(null);

    void getChartGrid(gridTarget.name, gridTarget.groupKind, gridTimeframe, activeMarket)
      .then((payload) => {
        if (!active) {
          return;
        }
        memberGridCacheRef.current[cacheKey] = payload;
        setMemberGridData(payload);
      })
      .catch((error) => {
        if (active) {
          setMemberGridError(error instanceof Error ? error.message : "Failed to load the chart grid.");
        }
      })
      .finally(() => {
        if (active) {
          setMemberGridLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [activeMarket, gridTarget, gridTimeframe]);

  const modalCards = useMemo(() => {
    if (!gridTarget) {
      return [];
    }
    if (gridTarget.type === "section") {
      const cards = gridTarget.section === "indices" ? indexCards : sectorCards;
      return buildSectionCards(cards, activeMarket, gridTimeframe, (card) =>
        setGridTarget({
          type: "members",
          name: card.sector,
          groupKind: card.group_kind,
        }),
      );
    }
    return buildMemberCards(memberGridData, gridTimeframe, (symbol) => {
      onPickSymbol(symbol);
    });
  }, [activeMarket, gridTarget, gridTimeframe, indexCards, memberGridData, onPickSymbol, sectorCards]);

  const modalTitle = useMemo(() => {
    if (!gridTarget) {
      return "";
    }
    if (gridTarget.type === "section") {
      return gridTarget.section === "indices" ? "Indices" : "Sectors";
    }
    return gridTarget.name;
  }, [gridTarget]);

  const modalSubtitle = useMemo(() => {
    if (!gridTarget) {
      return "";
    }
    if (gridTarget.type === "section") {
      return gridTarget.section === "indices"
        ? `${gridTimeframe} performance of the tracked indices. Open any card to drill into its members.`
        : `${gridTimeframe} performance of the tracked sectors. Open any card to drill into its members.`;
    }
    return `${memberGridData?.total_items ?? 0} companies · ${gridTimeframe} chart wall`;
  }, [gridTarget, gridTimeframe, memberGridData?.total_items]);

  const modalStats = useMemo(() => {
    if (!gridTarget) {
      return [];
    }
    if (gridTarget.type === "section") {
      return gridStatsForSection(gridTarget.section === "indices" ? indexCards : sectorCards, gridTimeframe);
    }
    return gridStatsForMembers(memberGridData, gridTimeframe);
  }, [gridTarget, gridTimeframe, indexCards, memberGridData, sectorCards]);

  const modalContextLabel = useMemo(() => {
    if (!gridTarget) {
      return "";
    }
    if (gridTarget.type === "section") {
      return gridTarget.section === "indices" ? "Indices" : "Sectors";
    }
    return gridTarget.groupKind === "index" ? "Index" : "Sector";
  }, [gridTarget]);

  const dashboardLoading = !dashboard;
  const groupsLoading = !groups;
  const topGainers = (dashboard?.top_gainers ?? []).slice(0, HOME_LIST_SKELETON_COUNT);
  const topLosers = (dashboard?.top_losers ?? []).slice(0, HOME_LIST_SKELETON_COUNT);
  const topVolumeSpikes = (dashboard?.top_volume_spikes ?? []).slice(0, HOME_LIST_SKELETON_COUNT);
  const allMoverLists: Record<MoverListKind, ScanMatch[]> = {
    gainers: dashboard?.top_gainers ?? [],
    losers: dashboard?.top_losers ?? [],
    active: dashboard?.top_volume_spikes ?? [],
  };
  const moverModalTitle = moverModal === "gainers" ? "Top Gainers" : moverModal === "losers" ? "Top Losers" : "Most Active";
  const moverModalItems = moverModal ? allMoverLists[moverModal] : [];
  const topGroups = useMemo(
    () => (groups?.groups ?? []).slice(0, 10),
    [groups],
  );

  function renderStockRowSkeleton(prefix: string) {
    return Array.from({ length: HOME_LIST_SKELETON_COUNT }, (_, index) => (
      <div key={`${prefix}-skeleton-${index}`} className="home-stock-row home-stock-row-skeleton" aria-hidden="true">
        <span>
          <span className="skeleton-block home-skeleton-line home-skeleton-line-title" />
          <span className="skeleton-block home-skeleton-line home-skeleton-line-subtitle" />
        </span>
        <span className="skeleton-block home-skeleton-line home-skeleton-line-value" />
        <span className="skeleton-block home-skeleton-line home-skeleton-line-chip" />
      </div>
    ));
  }

  function renderMoverRow(item: ScanMatch, kind: MoverListKind, keyPrefix: string) {
    const value = kind === "active" ? `${item.relative_volume.toFixed(2)}x` : formatReturn(item.change_pct);
    const meta = kind === "active" ? `${formatReturn(item.change_pct)} · ${shortName(item)}` : shortName(item);
    return (
      <button key={`${keyPrefix}-${item.symbol}`} type="button" className="home-stock-row" onClick={() => onPickSymbol(item.symbol)}>
        <span>
          <strong>{item.symbol}</strong>
          <small>{meta}</small>
        </span>
        <span>{item.last_price.toFixed(2)}</span>
        <span className={kind === "active" ? "neutral-text" : metricClass(item.change_pct)}>{value}</span>
      </button>
    );
  }

  function renderMoverPanel(title: string, subtitle: string, kind: MoverListKind, items: ScanMatch[]) {
    return (
      <Panel
        title={title}
        subtitle={subtitle}
        className={dashboardLoading ? "home-panel home-panel-list home-panel-list-loading" : "home-panel home-panel-list"}
        actions={
          <button type="button" className="tool-pill" onClick={() => setMoverModal(kind)} disabled={allMoverLists[kind].length === 0}>
            View All
          </button>
        }
      >
        <div className="home-stock-list">
          {dashboardLoading
            ? renderStockRowSkeleton(kind)
            : items.length > 0
              ? items.map((item) => renderMoverRow(item, kind, `home-${kind}`))
              : <div className="empty-state">No stocks available in the current universe.</div>}
        </div>
      </Panel>
    );
  }

  function renderHeatmapSkeleton(prefix: string, count: number) {
    return Array.from({ length: count }, (_, index) => (
      <div key={`${prefix}-skeleton-${index}`} className="sector-heatmap-card sector-heatmap-card-skeleton" aria-hidden="true">
        <div className="sector-heatmap-card-header">
          <span className="skeleton-block home-skeleton-line home-skeleton-sector-name" />
          <span className="skeleton-block home-skeleton-line home-skeleton-sector-return" />
        </div>
        <div className="sector-heatmap-card-footer">
          <span className="skeleton-block home-skeleton-line home-skeleton-sector-meta" />
          <span className="skeleton-block home-skeleton-line home-skeleton-sector-meta" />
        </div>
      </div>
    ));
  }

  return (
    <div className="home-layout">
      {peModalSymbol && (
        <PeChartModal
          market={activeMarket}
          symbol={peModalSymbol}
          fallbackBars={indexFallbackBarsBySymbol[peModalSymbol] ?? []}
          onClose={() => setPeModalSymbol(null)}
        />
      )}
      <section className="home-hero-card home-hero-card-minimal">
        <div className="home-hero-topline">
          <div className="home-market-copy">
            <span className="eyebrow">Market</span>
            <h2>Indian Markets</h2>
            <p>NSE and BSE screens, indices, and industry group leaders in one workspace.</p>
          </div>
        </div>
        <div className="home-hero-metrics">
          <div className="metric-card">
            <span>Universe</span>
            <strong>{dashboard?.universe_count ?? 0}</strong>
          </div>
          <div className="metric-card">
            <span>Groups</span>
            <strong>{groups?.total_groups ?? 0}</strong>
          </div>
          <div className="metric-card">
            <span>Movers</span>
            <strong>{dashboard ? dashboard.top_gainers.length + dashboard.top_losers.length + dashboard.top_volume_spikes.length : 0}</strong>
          </div>
        </div>
      </section>

      <Panel
        title="Top 10 Industry Groups"
        subtitle="Ranked by recent group performance"
        actions={
          <div className="home-groups-actions">
            <button type="button" className="tool-pill" onClick={() => onOpenGroups()}>
              Open Groups
            </button>
          </div>
        }
        className={groupsLoading ? "home-panel home-groups-panel deferred-render-panel" : "home-panel home-groups-panel deferred-render-panel"}
      >
        <div className="home-stock-list home-industry-list">
          {groupsLoading
            ? Array.from({ length: 10 }, (_, index) => (
                <div key={`home-group-skeleton-${index}`} className="home-stock-row home-stock-row-skeleton" aria-hidden="true" />
              ))
            : topGroups.length > 0
              ? topGroups.map((group) => (
                  <button
                    key={`home-group-${group.group_id}`}
                    type="button"
                    className="home-stock-row"
                    onClick={() => onOpenGroups({ groupId: group.group_id, symbol: group.leaders[0] ?? group.symbols[0] })}
                  >
                      <span>
                        <strong>{group.group_name}</strong>
                        <small>{group.parent_sector}</small>
                      </span>
                    <span>{group.stock_count} stocks</span>
                    <span className={metricClass(group.return_3m)}>{formatReturn(group.return_3m)}</span>
                  </button>
                ))
              : <div className="empty-state">No ranked groups available yet.</div>}
        </div>
      </Panel>

      <section className="home-grid-secondary deferred-render-section">
        {renderMoverPanel("Top Gainers", "Largest positive moves in the current universe", "gainers", topGainers)}
        {renderMoverPanel("Top Losers", "Largest negative moves in the current universe", "losers", topLosers)}
        {renderMoverPanel("Most Active", "Highest relative volume in the current universe", "active", topVolumeSpikes)}
      </section>

      {moverModal && typeof document !== "undefined" ? createPortal(
        <div className="mover-modal-backdrop" onClick={() => setMoverModal(null)}>
          <div className="mover-modal" onClick={(event) => event.stopPropagation()}>
            <button type="button" className="chart-modal-close" onClick={() => setMoverModal(null)}>
              Close
            </button>
            <div className="mover-modal-head">
              <p className="eyebrow">Market Movers</p>
              <h2>{moverModalTitle}</h2>
              <small>{moverModalItems.length} stocks from the latest dashboard list</small>
            </div>
            <div className="mover-modal-list">
              {moverModalItems.map((item) => renderMoverRow(item, moverModal, `modal-${moverModal}`))}
            </div>
          </div>
        </div>,
        document.body,
      ) : null}

      {gridTarget ? (
        <Suspense fallback={null}>
          <ChartGridModal
            contextLabel={modalContextLabel}
            title={modalTitle}
            subtitle={modalSubtitle}
            cards={modalCards}
            stats={modalStats}
            columns={gridColumns}
            rows={gridRows}
            timeframe={gridTimeframe}
            sortBy={gridSortBy}
            chartStyle={gridChartStyle}
            displayMode={gridDisplayMode}
            loading={gridTarget.type === "members" ? memberGridLoading : false}
            error={gridTarget.type === "members" ? memberGridError : null}
            onColumnsChange={setGridColumns}
            onRowsChange={setGridRows}
            onTimeframeChange={setGridTimeframe}
            onSortByChange={setGridSortBy}
            onChartStyleChange={setGridChartStyle}
            onDisplayModeChange={setGridDisplayMode}
            onLoadSeries={gridTarget.type === "members" ? loadMemberGridSeries : undefined}
            onClose={() => setGridTarget(null)}
          />
        </Suspense>
      ) : null}
    </div>
  );
}
