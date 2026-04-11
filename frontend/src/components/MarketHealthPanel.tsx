import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { 
  getMarketHealth, 
  getHistoricalMarketHealth,
  refreshHistoricalMarketHealth,
  type MarketHealthResponse, 
  type MarketKey,
  type UniverseBreadth,
  type HistoricalBreadthResponse,
  type HistoricalBreadthDataPoint
} from "../lib/api";
import { sanitizeHistoricalBreadth } from "../lib/chartData";
import type { ISeriesApi, LineData, Time } from "lightweight-charts";

const MARKET_HEALTH_CACHE_KEY = "mr-malik-market-health-cache:v1";
const MARKET_HEALTH_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;

type PersistedMarketHealthCache = {
  saved_at: string;
  live: MarketHealthResponse | null;
  history: HistoricalBreadthResponse | null;
};

function marketHealthCacheKey(market: MarketKey) {
  return `${MARKET_HEALTH_CACHE_KEY}:${market}`;
}

function readMarketHealthCache(market: MarketKey): PersistedMarketHealthCache | null {
  if (typeof window === "undefined") {
    return null;
  }

  const raw = window.localStorage.getItem(marketHealthCacheKey(market));
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as PersistedMarketHealthCache;
    const ageMs = Date.now() - new Date(parsed.saved_at).getTime();
    if (!Number.isFinite(ageMs) || ageMs > MARKET_HEALTH_CACHE_MAX_AGE_MS) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function persistMarketHealthCache(market: MarketKey, live: MarketHealthResponse | null, history: HistoricalBreadthResponse | null) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(
      marketHealthCacheKey(market),
      JSON.stringify({ saved_at: new Date().toISOString(), live, history } satisfies PersistedMarketHealthCache),
    );
  } catch {
    // Ignore cache persistence failures so the live page never depends on storage.
  }
}

type MarketHealthPanelProps = {
  market: MarketKey;
};

export function MarketHealthPanel({ market }: MarketHealthPanelProps) {
  const [data, setData] = useState<MarketHealthResponse | null>(() => readMarketHealthCache(market)?.live ?? null);
  const [historyData, setHistoryData] = useState<HistoricalBreadthResponse | null>(() => readMarketHealthCache(market)?.history ?? null);
  const [loading, setLoading] = useState<boolean>(() => !readMarketHealthCache(market)?.live);
  const [refreshingLatestBreadth, setRefreshingLatestBreadth] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [collapsedUniverses, setCollapsedUniverses] = useState<Record<string, boolean>>({});

  const universeNames = useMemo(() => data?.universes.map((universe) => universe.universe) ?? [], [data]);

  useEffect(() => {
    setCollapsedUniverses({});
  }, [market, data?.generated_at]);

  useEffect(() => {
    const cached = readMarketHealthCache(market);
    setData(cached?.live ?? null);
    setHistoryData(cached?.history ?? null);
    setLoading(!(cached?.live));
    setError(null);
  }, [market]);

  useEffect(() => {
    if (!data && !historyData) {
      return;
    }
    persistMarketHealthCache(market, data, historyData);
  }, [data, historyData, market]);

  function setAllCollapsed(collapsed: boolean) {
    setCollapsedUniverses(Object.fromEntries(universeNames.map((universe) => [universe, collapsed])));
  }

  function toggleUniverse(universe: string) {
    setCollapsedUniverses((current) => ({
      ...current,
      [universe]: !current[universe],
    }));
  }

  useEffect(() => {
    let active = true;
    async function fetchData() {
      const cached = readMarketHealthCache(market);
      if (!cached?.live) {
        setLoading(true);
      }
      setError(null);
      try {
        const healthResult = await getMarketHealth(market);
        if (active) {
          setData(healthResult);
          setLoading(false);
        }
      } catch (err) {
        if (active && !cached?.live) {
          setError(err instanceof Error ? err.message : "Failed to load market health.");
          setLoading(false);
        }
      }

      try {
        const historyResult = await getHistoricalMarketHealth(market);
        if (active) {
          setHistoryData(historyResult);
        }
      } catch {
        // Historical breadth should never block the live market-health view.
      }
    }
    fetchData();
    return () => {
      active = false;
    };
  }, [market]);

  const latestBreadthUpdate = historyData?.generated_at ?? data?.generated_at ?? null;
  const latestBreadthLabel = latestBreadthUpdate
    ? new Date(latestBreadthUpdate).toLocaleDateString(market === "us" ? "en-US" : "en-IN", {
        timeZone: market === "us" ? "America/New_York" : "Asia/Kolkata",
        day: "numeric",
        month: "short",
        year: "numeric",
      })
    : null;

  async function handleRefreshLatestBreadth() {
    setRefreshingLatestBreadth(true);
    try {
      const [healthResult, historyResult] = await Promise.all([
        getMarketHealth(market),
        refreshHistoricalMarketHealth(market),
      ]);
      setData(healthResult);
      setHistoryData(historyResult);
      setError(null);
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Failed to update latest market breadth.");
    } finally {
      setRefreshingLatestBreadth(false);
    }
  }

  if (loading) {
    return (
      <div className="workspace-pad">
        <div className="loading-skeleton">
          <div className="skeleton-strip">
            <div className="skeleton-block skeleton-block-lg" />
            <div className="skeleton-block skeleton-block-lg" />
            <div className="skeleton-block skeleton-block-lg" />
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="workspace-pad">
        <div className="error-banner">{error}</div>
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="workspace-pad">
      <header className="page-header">
        <h2>Market Health</h2>
        <p className="page-desc">
          Macro breadth indicators spanning tracked market universes
          {latestBreadthLabel ? ` • Latest breadth update: ${latestBreadthLabel}` : ""}
        </p>
        {universeNames.length > 0 ? (
          <div className="health-page-actions">
            <button
              type="button"
              className="secondary-action-btn"
              onClick={() => void handleRefreshLatestBreadth()}
              disabled={refreshingLatestBreadth}
            >
              {refreshingLatestBreadth ? "Updating Breadth..." : "Update Latest Breadth"}
            </button>
            <button type="button" className="secondary-action-btn" onClick={() => setAllCollapsed(false)}>
              Expand All
            </button>
            <button type="button" className="secondary-action-btn" onClick={() => setAllCollapsed(true)}>
              Collapse All
            </button>
          </div>
        ) : null}
      </header>
      
      <div className="health-universes-grid">
        {data.universes.map((univ) => {
          const univHistory = historyData?.universes.find(u => u.universe === univ.universe)?.history;
          return (
            <UniverseHealthCard
              key={univ.universe}
              breadth={univ}
              history={univHistory}
              collapsed={collapsedUniverses[univ.universe] === true}
              onToggleCollapsed={() => toggleUniverse(univ.universe)}
            />
          );
        })}
      </div>
    </div>
  );
}

function UniverseHealthCard({
  breadth,
  history,
  collapsed,
  onToggleCollapsed,
}: {
  breadth: UniverseBreadth;
  history?: HistoricalBreadthDataPoint[];
  collapsed: boolean;
  onToggleCollapsed: () => void;
}) {
  // A simple AD ratio calculator for color coding
  const adRatio = breadth.declines > 0 ? breadth.advances / breadth.declines : breadth.advances;
  const isBullish = adRatio > 1.2;
  const isBearish = adRatio < 0.8;
  const adColorClass = isBullish ? "positive-text" : isBearish ? "negative-text" : "neutral-text";
  const safeHistory = sanitizeHistoricalBreadth(history);
  
  return (
    <div className="health-card">
      <div className="health-card-header">
        <div className="health-card-heading">
          <h3>{breadth.universe}</h3>
          <span>{breadth.total} stocks</span>
        </div>
        <button type="button" className="health-collapse-toggle" onClick={onToggleCollapsed}>
          {collapsed ? "Expand" : "Collapse"}
        </button>
      </div>
      
      <div className="health-card-body">
        <div className="health-summary-grid">
          <div className="health-summary-pill">
            <span>A/D</span>
            <strong className={adColorClass}>{adRatio.toFixed(2)}</strong>
          </div>
          <div className="health-summary-pill">
            <span>&gt; 50 EMA</span>
            <strong>{breadth.above_ma50_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-summary-pill">
            <span>52W Highs</span>
            <strong className="positive-text">{breadth.new_high_52w_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-summary-pill">
            <span>52W Lows</span>
            <strong className="negative-text">{breadth.new_low_52w_pct.toFixed(1)}%</strong>
          </div>
        </div>

        {collapsed ? null : (
          <>
        <div className="health-metric-row">
          <span>Advances</span>
          <strong className="positive-text">{breadth.advances}</strong>
        </div>
        <div className="health-metric-row">
          <span>Declines</span>
          <strong className="negative-text">{breadth.declines}</strong>
        </div>
        <div className="health-metric-row">
          <span>Unchanged</span>
          <strong className="neutral-text">{breadth.unchanged}</strong>
        </div>
        
        <div className="health-ad-bar">
          <div className="health-ad-advances" style={{ width: `${(breadth.advances / Math.max(1, breadth.total)) * 100}%` }} />
          <div className="health-ad-declines" style={{ width: `${(breadth.declines / Math.max(1, breadth.total)) * 100}%` }} />
        </div>
        
        <hr className="health-divider" />
        
        <div className="health-metric-group">
          <h4>Moving Averages</h4>
          <div className="health-metric-row">
            <span>Above 20-Day EMA</span>
            <strong>{breadth.above_ma20_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-metric-row">
            <span>Above 50-Day EMA</span>
            <strong>{breadth.above_ma50_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-metric-row">
            <span>Above 200-Day SMA</span>
            <strong>{breadth.above_sma200_pct.toFixed(1)}%</strong>
          </div>
        </div>
        
        <hr className="health-divider" />
        
        <div className="health-metric-group">
          <h4>Momentum & Extremes</h4>
          <div className="health-metric-row">
            <span>New 52w Highs</span>
            <strong className="positive-text">{breadth.new_high_52w_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-metric-row">
            <span>New 52w Lows</span>
            <strong className="negative-text">{breadth.new_low_52w_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-metric-row">
            <span>RSI &gt; 70 (Overbought)</span>
            <strong>{breadth.rsi_14_overbought_pct.toFixed(1)}%</strong>
          </div>
          <div className="health-metric-row">
            <span>RSI &lt; 30 (Oversold)</span>
            <strong>{breadth.rsi_14_oversold_pct.toFixed(1)}%</strong>
          </div>
        </div>

        {safeHistory.length > 0 && (
          <div className="health-charts-container" style={{ marginTop: "2rem" }}>
            <hr className="health-divider" style={{ marginBottom: "1.5rem" }} />
            
            <LightweightHealthChart 
              history={safeHistory} 
              title="Historical MA Breadth (2Y)"
              lines={[
                { key: "above_ma20_pct", name: "> 20 EMA", color: "#0ea5e9" },
                { key: "above_ma50_pct", name: "> 50 EMA", color: "#8b5cf6" },
                { key: "above_sma200_pct", name: "> 200 SMA", color: "#f59e0b" },
              ]}
            />

            <LightweightHealthChart 
              history={safeHistory} 
              title="New 52W Highs/Lows (2Y)"
              lines={[
                { key: "new_high_52w_pct", name: "New Highs %", color: "#10b981" },
                { key: "new_low_52w_pct", name: "New Lows %", color: "#ef4444" },
              ]}
            />
          </div>
        )}
          </>
        )}
      </div>
    </div>
  );
}

function LightweightHealthChart({ 
  history, 
  title, 
  lines 
}: { 
  history: HistoricalBreadthDataPoint[], 
  title: string, 
  lines: { key: keyof HistoricalBreadthDataPoint, name: string, color: string }[] 
}) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);
  const seriesRef = useRef<Record<string, ISeriesApi<"Line">>>({});
  const [chartError, setChartError] = useState<string | null>(null);
  const [visibleLines, setVisibleLines] = useState<Record<string, boolean>>(() => 
    lines.reduce((acc, l) => ({ ...acc, [l.key]: true }), {})
  );

  useEffect(() => {
    if (!chartContainerRef.current) return;
    setChartError(null);
    seriesRef.current = {};
    let cancelled = false;
    let cleanup: (() => void) | null = null;

    async function renderChart() {
      try {
        const { ColorType, createChart } = await import("lightweight-charts");
        if (cancelled || !chartContainerRef.current) {
          return;
        }

        const chart = createChart(chartContainerRef.current, {
          layout: {
            background: { type: ColorType.Solid, color: 'transparent' },
            textColor: '#333333',
          },
          grid: {
            vertLines: { visible: false },
            horzLines: { visible: false },
          },
          rightPriceScale: {
            borderColor: 'rgba(0, 0, 0, 0.1)',
            visible: true,
          },
          timeScale: {
            borderColor: 'rgba(0, 0, 0, 0.1)',
            timeVisible: true,
            fixLeftEdge: true,
            fixRightEdge: true,
            tickMarkFormatter: (time: any) => {
              const parts = time.toString().split('-');
              if (parts.length === 3) {
                const date = new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]));
                return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
              }
              return time.toString();
            },
          },
          crosshair: {
            mode: 1,
            vertLine: {
              color: '#333333',
              width: 1,
              style: 1,
              labelBackgroundColor: '#111111',
            },
            horzLine: {
              color: '#333333',
              width: 1,
              style: 1,
              labelBackgroundColor: '#111111',
            },
          },
          autoSize: true,
        });

        chartRef.current = chart;

        lines.forEach(({ key, color }) => {
          const series = chart.addLineSeries({
            color,
            lineWidth: 2,
            crosshairMarkerVisible: true,
            priceFormat: {
              type: 'custom',
              formatter: (price: number) => `${price.toFixed(1)}%`,
            },
          });

          const data: LineData[] = history
            .map((point) => ({
              time: point.date as Time,
              value: Number(point[key]),
            }))
            .filter((point) => Number.isFinite(point.value));

          if (data.length > 0) {
            series.setData(data);
          }
          seriesRef.current[key] = series;
        });

        chart.timeScale().fitContent();
        cleanup = () => {
          chart.remove();
        };
      } catch (error) {
        chartRef.current = null;
        setChartError(error instanceof Error ? error.message : 'Failed to render chart');
      }
    }

    void renderChart();

    return () => {
      cancelled = true;
      cleanup?.();
    };
  }, [history]);

  // Synchronize visibility whenever state changes
  useLayoutEffect(() => {
    Object.keys(visibleLines).forEach(key => {
      const series = seriesRef.current[key];
      if (series) {
        series.applyOptions({ visible: visibleLines[key] });
      }
    });
  }, [visibleLines]);

  return (
    <div style={{ marginBottom: "2.5rem" }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: "1rem" }}>
        <h4 style={{ color: "var(--text-color)", margin: 0 }}>{title}</h4>
        <div style={{ display: 'flex', gap: '15px' }}>
          {lines.map((l) => (
            <div 
              key={l.key} 
              onClick={() => setVisibleLines(prev => ({ ...prev, [l.key]: !prev[l.key] }))}
              style={{
                display: 'flex', 
                alignItems: 'center', 
                gap: '8px', 
                cursor: 'pointer',
                opacity: visibleLines[l.key] ? 1 : 0.3,
                fontSize: '12px',
                transition: 'opacity 0.2s ease',
                userSelect: 'none'
              }}
            >
              <div style={{ width: '12px', height: '12px', borderRadius: '4px', backgroundColor: l.color }} />
              <span style={{ color: 'rgba(0,0,0,0.8)' }}>{l.name}</span>
            </div>
          ))}
        </div>
      </div>
      {chartError ? (
        <div className="error-banner" style={{ marginBottom: "1rem" }}>{chartError}</div>
      ) : null}
      <div 
        ref={chartContainerRef} 
        style={{ width: "100%", height: 260, position: "relative" }} 
      />
    </div>
  );
}
