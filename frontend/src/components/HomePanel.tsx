import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";

import {
  getChart,
  getMarketOverview,
  type BreadthDayCounts,
  type ChartBar,
  type DashboardResponse,
  type IndustryGroupsResponse,
  type IndustryGroupRankItem,
  type MarketKey,
  type MarketMacroItem,
  type ScanMatch,
  type XpBreadthScore,
} from "../lib/api";

import "./HomePanel.css";

type HomePanelProps = {
  activeMarket: MarketKey;
  dashboard: DashboardResponse | null;
  groups: IndustryGroupsResponse | null;
  snapshotDateLabel: string;
  snapshotTimeLabel: string;
  onPickSymbol: (symbol: string) => void;
  onOpenGroups: (options?: { groupId?: string; symbol?: string }) => void;
};

type NiftyTimeframe = "6M" | "1Y" | "3Y";

const NIFTY_TIMEFRAMES: NiftyTimeframe[] = ["6M", "1Y", "3Y"];

type ViewAllMode = "gainers" | "losers" | "active";

function formatReturn(value: number) {
  if (!Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatPrice(value: number | null | undefined, opts: { locale?: string; currency?: string } = {}) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  const { locale = "en-IN", currency = "₹" } = opts;
  return `${currency}${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function shortName(item: ScanMatch) {
  return item.name.length > 28 ? `${item.name.slice(0, 28)}…` : item.name;
}

function formatCompact(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1e7) return `₹${(value / 1e7).toFixed(2)} Cr`;
  if (Math.abs(value) >= 1e5) return `₹${(value / 1e5).toFixed(2)} L`;
  return `₹${value.toLocaleString("en-IN", { maximumFractionDigits: 2 })}`;
}

function initials(symbol: string) {
  return symbol.slice(0, 2).toUpperCase();
}

function getLogoUrl(symbol: string) {
  const clean = symbol.replace("^", "").toUpperCase();
  // Map common NSE symbols to TradingView logo IDs
  const mapping: Record<string, string> = {
    "RELIANCE": "reliance-industries",
    "TCS": "tata-consultancy-services",
    "HDFCBANK": "hdfc-bank",
    "INFY": "infosys",
    "ICICIBANK": "icici-bank",
    "SBIN": "state-bank-of-india",
    "BHARTIARTL": "bharti-airtel",
    "LICI": "lic-of-india",
    "ITC": "itc",
    "HINDUNILVR": "hindustan-unilever",
    "LT": "larsen-and-toubro",
    "BAJFINANCE": "bajaj-finance",
    "MARUTI": "maruti-suzuki",
    "ASIANPAINT": "asian-paints",
    "AXISBANK": "axis-bank",
    "ADANIENT": "adani-enterprises",
    "SUNPHARMA": "sun-pharma",
    "TITAN": "titan",
    "ULTRACEMCO": "ultratech-cement",
    "WIPRO": "wipro",
    "NTPC": "ntpc",
    "ONGC": "ongc",
    "JSWSTEEL": "jsw-steel",
    "M&M": "mahindra-and-mahindra",
    "POWERGRID": "power-grid",
    "HCLTECH": "hcl-technologies",
    "KOTAKBANK": "kotak-mahindra-bank",
    "COALINDIA": "coal-india",
    "ADANIPORTS": "adani-ports",
    "TATASTEEL": "tata-steel",
    "GRASIM": "grasim",
    "HINDALCO": "hindalco",
    "TECHM": "tech-mahindra",
    "NESTLEIND": "nestle-india",
    "BAJAJFINSV": "bajaj-finserv",
    "SBILIFE": "sbi-life-insurance",
    "DRREDDY": "dr-reddys-labs",
    "CIPLA": "cipla",
    "INDUSINDBK": "indusind-bank",
    "TATAMOTORS": "tata-motors",
    "BPCL": "bpcl",
    "BRITANNIA": "britannia",
    "EICHERMOT": "eicher-motors",
    "DIVISLAB": "divis-labs",
    "APOLLOHOSP": "apollo-hospitals",
    "UPL": "upl",
    "HEROMOTOCO": "hero-motocorp",
    "BAJAJ-AUTO": "bajaj-auto",
    "LTIM": "lti-mindtree",
  };
  const id = mapping[clean];
  if (id) return `https://s3-symbol-logo.tradingview.com/${id}.svg`;
  return null;
}

/* ---------- SVG helpers ---------- */

function Sparkline({ values, color, fill, height = 36 }: { values: number[]; color: string; fill?: string; height?: number }) {
  const width = 100;
  if (!values || values.length < 2) {
    return (
      <svg className="homepro-kpi-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
        <line x1="0" y1={height / 2} x2={width} y2={height / 2} stroke={color} strokeOpacity="0.3" strokeWidth="1.5" />
      </svg>
    );
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const points = values.map((v, i) => `${(i * step).toFixed(2)},${(height - ((v - min) / range) * (height - 4) - 2).toFixed(2)}`);
  const pathD = `M ${points.join(" L ")}`;
  const areaD = `${pathD} L ${width},${height} L 0,${height} Z`;
  return (
    <svg className="homepro-kpi-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true" style={{ height }}>
      {fill ? <path d={areaD} fill={fill} /> : null}
      <path d={pathD} stroke={color} strokeWidth="1.8" fill="none" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

function MiniSparkline({ values, color, fill }: { values: number[]; color: string; fill: string }) {
  return (
    <svg
      className="homepro-mini-spark"
      viewBox="0 0 100 46"
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      {(() => {
        if (!values || values.length < 2) return null;
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const step = 100 / (values.length - 1);
        const pts = values.map((v, i) => `${(i * step).toFixed(2)},${(46 - ((v - min) / range) * 40 - 4).toFixed(2)}`);
        const d = `M ${pts.join(" L ")}`;
        return (
          <>
            <path d={`${d} L 100,46 L 0,46 Z`} fill={fill} />
            <path d={d} stroke={color} strokeWidth="1.6" fill="none" strokeLinejoin="round" strokeLinecap="round" />
          </>
        );
      })()}
    </svg>
  );
}

function Donut({ segments, size = 180 }: { segments: { value: number; color: string }[]; size?: number }) {
  const total = segments.reduce((sum, s) => sum + Math.max(0, s.value), 0);
  const radius = size / 2 - 12;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;
  const cx = size / 2;
  const cy = size / 2;
  return (
    <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} aria-hidden="true" style={{ transform: "rotate(-90deg)" }}>
      <circle cx={cx} cy={cy} r={radius} stroke="rgba(15,23,42,0.06)" strokeWidth="18" fill="none" />
      {total > 0 && segments.map((seg, i) => {
        const frac = Math.max(0, seg.value) / total;
        const dash = frac * circumference;
        const gap = circumference - dash;
        const element = (
          <circle
            key={i}
            cx={cx}
            cy={cy}
            r={radius}
            stroke={seg.color}
            strokeWidth="18"
            fill="none"
            strokeDasharray={`${dash} ${gap}`}
            strokeDashoffset={-offset}
            strokeLinecap="butt"
          />
        );
        offset += dash;
        return element;
      })}
    </svg>
  );
}

function BreadthHistoryChart({ history }: { history: BreadthDayCounts[] }) {
  const [activeIdx, setActiveIdx] = useState<number | null>(null);

  if (!history || history.length === 0) {
    return (
      <div className="homepro-breadth-history-empty">
        Building 10-day history…
      </div>
    );
  }
  const days = history.slice(-10);
  const focused = activeIdx !== null ? days[activeIdx] : days[days.length - 1];
  const focusedTotal = Math.max(1, focused.total);
  const focusedAdvPct = (focused.advances / focusedTotal) * 100;
  const focusedDecPct = (focused.declines / focusedTotal) * 100;

  const labelFor = (d: BreadthDayCounts, opts: { long?: boolean } = {}) => {
    const dt = new Date(d.date + "T00:00:00");
    if (Number.isNaN(dt.getTime())) return d.date.slice(5);
    return dt.toLocaleDateString("en-IN", opts.long
      ? { weekday: "short", day: "numeric", month: "short" }
      : { day: "numeric", month: "short" }
    );
  };

  return (
    <div className="homepro-breadth-history-wrap">
      <div className="homepro-breadth-history-summary">
        <div className="homepro-breadth-history-summary-date">
          {labelFor(focused, { long: true })}
        </div>
        <div className="homepro-breadth-history-summary-pcts">
          <span className="pos">↑ {focusedAdvPct.toFixed(1)}%</span>
          <span className="muted">·</span>
          <span className="neg">↓ {focusedDecPct.toFixed(1)}%</span>
        </div>
        <div className="homepro-breadth-history-summary-counts">
          <span className="pos">{focused.advances.toLocaleString("en-IN")} adv</span>
          <span className="neg">{focused.declines.toLocaleString("en-IN")} dec</span>
          <span className="muted">{focused.unchanged.toLocaleString("en-IN")} flat</span>
        </div>
      </div>
      <div className="homepro-breadth-history-bars" onMouseLeave={() => setActiveIdx(null)}>
        {days.map((d, idx) => {
          const total = Math.max(1, d.total);
          const advPct = (d.advances / total) * 100;
          const decPct = (d.declines / total) * 100;
          const uncPct = Math.max(0, 100 - advPct - decPct);
          const advLeads = d.advances >= d.declines;
          const isActive = idx === (activeIdx ?? days.length - 1);
          return (
            <button
              type="button"
              className={`homepro-breadth-history-day${isActive ? " active" : ""}`}
              key={d.date}
              onMouseEnter={() => setActiveIdx(idx)}
              onFocus={() => setActiveIdx(idx)}
              aria-label={`${labelFor(d, { long: true })}: ${d.advances} advancing, ${d.declines} declining, ${d.unchanged} flat`}
            >
              <div className="homepro-breadth-history-stack" aria-hidden="true">
                <div className="homepro-breadth-history-seg adv" style={{ height: `${advPct}%` }} />
                <div className="homepro-breadth-history-seg unc" style={{ height: `${uncPct}%` }} />
                <div className="homepro-breadth-history-seg dec" style={{ height: `${decPct}%` }} />
              </div>
              <div className={`homepro-breadth-history-pct ${advLeads ? "pos" : "neg"}`}>
                {Math.round(advPct)}%
              </div>
              <div className="homepro-breadth-history-date">{labelFor(d)}</div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function XpBreadthChart({ xp, height = 200 }: { xp: XpBreadthScore; height?: number }) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const width = 880;
  const padL = 30;
  const padR = 10;
  const padY = 10;

  // Prefer the live (post warm-up) series; fall back to whatever exists.
  const live = xp.history.filter((p) => !p.warmup);
  const points = (live.length >= 5 ? live : xp.history).slice(-160);
  if (points.length < 2) {
    return <div className="homepro-xp-empty">Not enough history yet — run the breadth backfill.</div>;
  }

  const scores = points.map((p) => p.xp_score);
  const bandStops = xp.bands.flatMap((b) => [b.min, b.max].filter((v): v is number => v != null));
  const dataMin = Math.min(...scores, ...bandStops.filter((v) => v <= Math.min(...scores) + 5));
  const dataMax = Math.max(...scores, ...bandStops.filter((v) => v >= Math.max(...scores) - 5));
  const yMin = Math.max(0, Math.floor(Math.min(dataMin, Math.min(...scores)) - 1));
  const yMax = Math.ceil(Math.max(dataMax, Math.max(...scores)) + 1);
  const range = yMax - yMin || 1;
  const innerH = height - padY * 2;
  const innerW = width - padL - padR;

  const y = (v: number) => padY + innerH - ((v - yMin) / range) * innerH;
  const x = (i: number) => padL + (points.length === 1 ? innerW / 2 : (i / (points.length - 1)) * innerW);

  const linePath = points
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p.xp_score).toFixed(1)}`)
    .join(" ");
  const areaPath = `${linePath} L${x(points.length - 1).toFixed(1)},${y(yMin).toFixed(1)} L${x(0).toFixed(1)},${y(yMin).toFixed(1)} Z`;

  const hovered = hoverIdx != null ? points[hoverIdx] : points[points.length - 1];
  const gridTicks = [yMin, ...xp.bands.map((b) => b.min).filter((v): v is number => v != null && v > yMin && v < yMax), yMax];

  return (
    <div className="homepro-xp-chart-wrap">
      <svg
        className="homepro-xp-svg"
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        onMouseLeave={() => setHoverIdx(null)}
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const px = ((e.clientX - rect.left) / rect.width) * width;
          const i = Math.round(((px - padL) / innerW) * (points.length - 1));
          setHoverIdx(Math.max(0, Math.min(points.length - 1, i)));
        }}
      >
        {/* regime band backgrounds */}
        {xp.bands.map((b) => {
          const top = Math.min(yMax, b.max ?? yMax);
          const bot = Math.max(yMin, b.min ?? yMin);
          if (top <= bot) return null;
          return (
            <rect
              key={b.label}
              x={padL}
              y={y(top)}
              width={innerW}
              height={Math.max(0, y(bot) - y(top))}
              fill={b.color}
              opacity={0.12}
            />
          );
        })}
        {/* gridlines + y labels at band boundaries */}
        {gridTicks.map((t) => (
          <g key={`tick-${t}`}>
            <line x1={padL} x2={width - padR} y1={y(t)} y2={y(t)} stroke="var(--hp-border, #2a2e39)" strokeWidth={0.5} opacity={0.5} />
            <text x={padL - 4} y={y(t) + 3} textAnchor="end" fontSize={9} fill="var(--hp-muted, #9aa0aa)">{t}</text>
          </g>
        ))}
        <path d={areaPath} fill={xp.regime_color} opacity={0.1} />
        <path d={linePath} fill="none" stroke={xp.regime_color} strokeWidth={1.6} />
        {/* last / hovered marker */}
        <circle cx={x(hoverIdx ?? points.length - 1)} cy={y(hovered.xp_score)} r={3} fill={hovered.regime_color} stroke="#fff" strokeWidth={1} />
      </svg>
      <div className="homepro-xp-hover">
        <span>{hovered.date}</span>
        <strong style={{ color: hovered.regime_color }}>{hovered.xp_score.toFixed(2)}</strong>
        <span>{hovered.regime}</span>
      </div>
    </div>
  );
}

function CandlestickChart({ bars, height = 220 }: { bars: ChartBar[]; height?: number }) {
  const width = 640;
  if (!bars || bars.length < 2) {
    return (
      <div className="homepro-nifty-chart" style={{ display: "grid", placeItems: "center", color: "var(--hp-muted)", fontSize: 12 }}>
        Loading chart…
      </div>
    );
  }
  const highs = bars.map((b) => b.high);
  const lows = bars.map((b) => b.low);
  const min = Math.min(...lows);
  const max = Math.max(...highs);
  const range = max - min || 1;
  const paddingY = 12;
  const innerH = height - paddingY * 2;
  const slot = width / bars.length;
  const candleW = Math.max(2, Math.min(10, slot * 0.65));

  function y(value: number) {
    return paddingY + innerH - ((value - min) / range) * innerH;
  }

  return (
    <svg className="homepro-nifty-chart" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
      {bars.map((bar, i) => {
        const cx = slot * (i + 0.5);
        const up = bar.close >= bar.open;
        const color = up ? "#10b981" : "#ef4444";
        const yHigh = y(bar.high);
        const yLow = y(bar.low);
        const yOpen = y(bar.open);
        const yClose = y(bar.close);
        const bodyTop = Math.min(yOpen, yClose);
        const bodyH = Math.max(1, Math.abs(yOpen - yClose));
        return (
          <g key={i}>
            <line x1={cx} x2={cx} y1={yHigh} y2={yLow} stroke={color} strokeWidth="1" />
            <rect
              x={cx - candleW / 2}
              y={bodyTop}
              width={candleW}
              height={bodyH}
              fill={color}
              stroke={color}
            />
          </g>
        );
      })}
    </svg>
  );
}

/* ---------- Component ---------- */

export function HomePanel({
  activeMarket,
  dashboard,
  groups,
  snapshotDateLabel,
  snapshotTimeLabel,
  onPickSymbol,
  onOpenGroups,
}: HomePanelProps) {
  const [macroItems, setMacroItems] = useState<MarketMacroItem[]>([]);
  const [niftyBars, setNiftyBars] = useState<ChartBar[]>([]);
  const [niftyTF, setNiftyTF] = useState<NiftyTimeframe>("1Y");

  // Fetch macro strip
  useEffect(() => {
    let active = true;
    getMarketOverview(activeMarket)
      .then((res) => { if (active) setMacroItems(res.items); })
      .catch(() => {});
    return () => { active = false; };
  }, [activeMarket, dashboard?.generated_at]);

  // Fetch Nifty chart
  useEffect(() => {
    let active = true;
    getChart("^NSEI", "3Y", activeMarket)
      .then((res) => { if (active) setNiftyBars(res.bars ?? []); })
      .catch(() => { if (active) setNiftyBars([]); });
    return () => { active = false; };
  }, [activeMarket]);

  const universeCount = dashboard?.universe_count ?? 0;
  const marketStatusRaw = (dashboard?.market_status ?? "").toLowerCase();
  const marketOpen = marketStatusRaw.includes("open") || marketStatusRaw === "live";

  const topGainers = (dashboard?.top_gainers ?? []).slice(0, 5);
  const topLosers = (dashboard?.top_losers ?? []).slice(0, 5);
  const mostActive = (dashboard?.top_volume_spikes ?? []).slice(0, 5);

  // Real market-wide breadth from the dashboard endpoint. Falls back to
  // zeros when the snapshot hasn't computed it yet (older deploys).
  const breadthToday = dashboard?.breadth_today ?? null;
  const breadthHistory = dashboard?.breadth_history ?? [];
  const breadthTotal = breadthToday?.total ?? 0;
  const advances = breadthToday?.advances ?? 0;
  const declines = breadthToday?.declines ?? 0;
  const unchanged = breadthToday?.unchanged ?? 0;
  const advPct = breadthTotal > 0 ? (advances / breadthTotal) * 100 : 0;

  // XP market breadth score (computed EOD over all bhavcopy equities).
  const xpBreadth = dashboard?.xp_breadth ?? null;

  const topGroups = useMemo<IndustryGroupRankItem[]>(
    () => (groups?.groups ?? []).slice(0, 10),
    [groups],
  );

  const [viewAllMode, setViewAllMode] = useState<ViewAllMode | null>(null);

  const allGainers = useMemo(() => {
    const list = [...(dashboard?.top_gainers ?? [])];
    list.sort((a, b) => b.change_pct - a.change_pct);
    return list.slice(0, 20);
  }, [dashboard?.top_gainers]);

  const allLosers = useMemo(() => {
    const list = [...(dashboard?.top_losers ?? [])];
    list.sort((a, b) => a.change_pct - b.change_pct);
    return list.slice(0, 20);
  }, [dashboard?.top_losers]);

  const allActive = useMemo(() => {
    const list = [...(dashboard?.top_volume_spikes ?? [])];
    list.sort((a, b) => b.relative_volume - a.relative_volume);
    return list.slice(0, 20);
  }, [dashboard?.top_volume_spikes]);

  function genMockSparkline(seed: number, changePct: number): number[] {
    // Deterministic wavy curve biased by sign of change_pct
    const out: number[] = [];
    const len = 24;
    const drift = changePct / len;
    let v = 100;
    for (let i = 0; i < len; i++) {
      const noise = Math.sin((seed + i) * 0.7) * 1.2 + Math.cos((seed + i) * 0.3) * 0.8;
      v += drift + noise * 0.3;
      out.push(v);
    }
    return out;
  }

  const niftyPoint = macroItems.find((c) => c.symbol === "^NSEI");
  const niftyPrice = niftyPoint?.price ?? null;
  const niftyChange = niftyPoint?.change_pct ?? null;

  return (
    <div className="homepro">
      {/* ============ ROW 1 — KPIs + SNAPSHOT ============ */}
      <div className="homepro-row-top">
        {/* KPI cards */}
        <div className="homepro-kpis">
          {/* Universe */}
          <div className="homepro-kpi homepro-kpi-universe">
            <div className="homepro-kpi-label">Universe</div>
            <div className="homepro-kpi-value">{universeCount.toLocaleString("en-IN")}</div>
            <div className="homepro-kpi-sub">Total Stocks</div>
            <Sparkline
              values={genMockSparkline(1, 1.5)}
              color="#8b5cf6"
              fill="rgba(139, 92, 246, 0.18)"
            />
            <div className="homepro-kpi-sub" style={{ color: "var(--hp-green)" }}>+12 vs yesterday</div>
          </div>

          {/* Market Status */}
          <div className="homepro-kpi homepro-kpi-status">
            <div className="homepro-kpi-label">Market Status</div>
            <div className="homepro-kpi-value">
              <span>{marketOpen ? "Open" : "Closed"}</span>
              <span className={marketOpen ? "homepro-status-dot" : "homepro-status-dot closed"} />
            </div>
            <div className="homepro-kpi-sub">Market is {marketOpen ? "live" : "closed"}</div>
            <Sparkline
              values={genMockSparkline(7, niftyChange ?? 0.5)}
              color="#3b82f6"
              fill="rgba(59, 130, 246, 0.16)"
            />
            <div className="homepro-kpi-sub">{marketOpen ? "Closes in 01:24:15" : `Next session ${snapshotDateLabel}`}</div>
          </div>

          {/* EOD Date */}
          <div className="homepro-kpi homepro-kpi-date">
            <div className="homepro-kpi-label">EOD Date</div>
            <div className="homepro-kpi-value" style={{ fontSize: 22 }}>{snapshotDateLabel || "—"}</div>
            <div className="homepro-kpi-sub">Last Updated</div>
            <div className="homepro-kpi-bottom">
              <div className="homepro-kpi-icon" aria-hidden="true">📅</div>
              <div style={{ fontSize: 13, fontWeight: 600 }}>{snapshotTimeLabel || "—"}</div>
            </div>
          </div>

          {/* Advances / Declines */}
          <div className="homepro-kpi homepro-kpi-breadth">
            <div className="homepro-kpi-label">Advances / Declines</div>
            <div className="homepro-kpi-value" style={{ fontSize: 22 }}>{advances} / {declines}</div>
            <div className="homepro-kpi-sub">Stocks</div>
            <div className="homepro-kpi-bottom">
              <div style={{ position: "relative", width: 56, height: 56 }}>
                <Donut
                  size={56}
                  segments={[
                    { value: advances, color: "#10b981" },
                    { value: declines, color: "#ef4444" },
                  ]}
                />
              </div>
              <div style={{ display: "flex", gap: 12, fontSize: 12, fontWeight: 700 }}>
                <span style={{ color: "#059669" }}>{Math.round(advPct)}%</span>
                <span style={{ color: "#b45309" }}>{100 - Math.round(advPct)}%</span>
              </div>
            </div>
          </div>
        </div>

      </div>

      {/* ============ ROW 2 — Groups + Breadth + Nifty ============ */}
      <div className="homepro-row-mid">
        {/* Top 10 Industry Groups */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top 10 Industry Groups</h3>
            <button className="homepro-link" onClick={() => onOpenGroups()}>View All Groups</button>
          </div>
          <table className="homepro-groups-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Industry Group</th>
                <th className="homepro-num">Stocks</th>
                <th className="homepro-num">Change %</th>
                <th className="homepro-num">Day Performance</th>
              </tr>
            </thead>
            <tbody>
              {topGroups.length === 0 ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <tr key={`grp-skel-${i}`}>
                    <td colSpan={5}>
                      <div className="homepro-skel" style={{ height: 14, margin: "4px 0" }} />
                    </td>
                  </tr>
                ))
              ) : topGroups.map((group, i) => {
                const up = group.return_1m >= 0;
                return (
                  <tr
                    key={`home-group-${group.group_id}`}
                    onClick={() => onOpenGroups({ groupId: group.group_id })}
                  >
                    <td className="homepro-rank">{i + 1}.</td>
                    <td className="homepro-group-name">{group.group_name}</td>
                    <td className="homepro-num">{group.stock_count}</td>
                    <td className={`homepro-num homepro-chg ${up ? "pos" : "neg"}`}>
                      {formatReturn(group.return_1m)}
                    </td>
                    <td className="homepro-num homepro-spark-cell">
                      <Sparkline
                        values={genMockSparkline(i + 5, group.return_1m)}
                        color={up ? "#10b981" : "#ef4444"}
                        height={24}
                      />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Market Breadth */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Market Breadth</h3>
          </div>
          <div className="homepro-breadth-body">
            <div className="homepro-donut-wrap">
              <Donut
                segments={[
                  { value: advances, color: "#10b981" },
                  { value: declines, color: "#ef4444" },
                  { value: unchanged, color: "#cbd5e1" },
                ]}
              />
              <div className="homepro-donut-center">
                <div>
                  <strong>{breadthTotal.toLocaleString("en-IN")}</strong>
                  <small>Stocks</small>
                </div>
              </div>
            </div>
            <div className="homepro-legend">
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "#10b981" }} />Advancing</span>
                <span><strong>{advances}</strong> ({((advances / Math.max(1, breadthTotal)) * 100).toFixed(1)}%)</span>
              </div>
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "#ef4444" }} />Declining</span>
                <span><strong>{declines}</strong> ({((declines / Math.max(1, breadthTotal)) * 100).toFixed(1)}%)</span>
              </div>
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "#cbd5e1" }} />Unchanged</span>
                <span><strong>{unchanged}</strong> ({((unchanged / Math.max(1, breadthTotal)) * 100).toFixed(1)}%)</span>
              </div>
            </div>

            <div className="homepro-breadth-history">
              <div className="homepro-breadth-history-head">
                <span>Last 10 Days A/D</span>
              </div>
              <BreadthHistoryChart history={breadthHistory} />
            </div>
          </div>
        </div>

        {/* Nifty 50 Performance */}
        <div className="homepro-card homepro-nifty">
          <div className="homepro-nifty-head">
            <div>
              <h3>Nifty 50 Performance</h3>
              <div className="homepro-nifty-price">
                <strong>{niftyPrice !== null ? niftyPrice.toLocaleString("en-IN", { maximumFractionDigits: 2 }) : "—"}</strong>
                {niftyChange !== null && (
                  <span style={{ color: niftyChange >= 0 ? "#10b981" : "#ef4444", fontWeight: 700, fontSize: 13 }}>
                    {formatReturn(niftyChange)}
                  </span>
                )}
              </div>
            </div>
            <div style={{ textAlign: "right" }}>
              <div className="homepro-timeframes">
                {NIFTY_TIMEFRAMES.map((tf) => (
                  <button
                    key={tf}
                    type="button"
                    className={`homepro-tf${tf === niftyTF ? " active" : ""}`}
                    onClick={() => setNiftyTF(tf)}
                  >
                    {tf}
                  </button>
                ))}
              </div>
              <div style={{ fontSize: 11, color: "var(--hp-muted)", marginTop: 6 }}>At Close</div>
            </div>
          </div>
          <CandlestickChart bars={sliceBars(niftyBars, niftyTF)} />
          <div className="homepro-nifty-foot">
            <span>EOD Applied for: {snapshotDateLabel || "—"}</span>
            <span>Last updated: {snapshotTimeLabel || "—"}</span>
          </div>
        </div>
      </div>

      {/* ============ XP Market Breadth Score ============ */}
      {xpBreadth && (
        <div className="homepro-card homepro-xp-card">
          <div className="homepro-card-head">
            <h3>
              XP Market Breadth Score
              <span className="homepro-xp-sub">all listed equities · EOD</span>
            </h3>
            <div className="homepro-xp-badge" style={{ background: xpBreadth.regime_color }}>
              <strong>{xpBreadth.xp_score.toFixed(2)}</strong>
              <span>{xpBreadth.regime}</span>
            </div>
          </div>
          <XpBreadthChart xp={xpBreadth} />
          <div className="homepro-xp-legend">
            {xpBreadth.bands.map((b) => (
              <span key={b.label} className="homepro-xp-legend-item">
                <span className="homepro-legend-swatch" style={{ background: b.color }} />
                {b.label}
                <em>
                  {b.min == null ? `< ${b.max}` : b.max == null ? `> ${b.min}` : `${b.min}–${b.max}`}
                </em>
              </span>
            ))}
            <span className="homepro-xp-asof">As of {xpBreadth.date}</span>
          </div>
        </div>
      )}

      {/* ============ ROW 3 — Gainers / Losers / Most Active ============ */}
      <div className="homepro-row-bot">
        {/* Top Gainers */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top Gainers</h3>
            <button className="homepro-link" onClick={() => setViewAllMode("gainers")}>View All</button>
          </div>
          <div className="homepro-list">
            {topGainers.length === 0 ? renderListSkeleton("g") : topGainers.map((item) => {
              const logo = getLogoUrl(item.symbol);
              return (
                <button key={`g-${item.symbol}`} type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
                  {logo ? (
                    <img src={logo} className="homepro-logo-img" alt="" onError={(e) => (e.currentTarget.style.display = "none")} />
                  ) : (
                    <span className="homepro-avatar homepro-avatar-g">{initials(item.symbol)}</span>
                  )}
                  <span className="homepro-row-meta">
                    <span className="homepro-row-sym">{item.symbol}</span>
                    <span className="homepro-row-sub">NSE</span>
                  </span>
                  <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
                  <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
                </button>
              );
            })}
          </div>
        </div>

        {/* Top Losers */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top Losers</h3>
            <button className="homepro-link" onClick={() => setViewAllMode("losers")}>View All</button>
          </div>
          <div className="homepro-list">
            {topLosers.length === 0 ? renderListSkeleton("l") : topLosers.map((item) => {
              const logo = getLogoUrl(item.symbol);
              return (
                <button key={`l-${item.symbol}`} type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
                  {logo ? (
                    <img src={logo} className="homepro-logo-img" alt="" onError={(e) => (e.currentTarget.style.display = "none")} />
                  ) : (
                    <span className="homepro-avatar homepro-avatar-r">{initials(item.symbol)}</span>
                  )}
                  <span className="homepro-row-meta">
                    <span className="homepro-row-sym">{item.symbol}</span>
                    <span className="homepro-row-sub">NSE</span>
                  </span>
                  <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
                  <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
                </button>
              );
            })}
          </div>
        </div>

        {/* Most Active */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Most Active</h3>
            <button className="homepro-link" onClick={() => setViewAllMode("active")}>View All</button>
          </div>
          <div className="homepro-list">
            {mostActive.length === 0 ? renderListSkeleton("a") : mostActive.map((item, i) => {
              const logo = getLogoUrl(item.symbol);
              return (
                <button key={`a-${item.symbol}`} type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
                  {logo ? (
                    <img src={logo} className="homepro-logo-img" alt="" onError={(e) => (e.currentTarget.style.display = "none")} />
                  ) : (
                    <span className={`homepro-avatar ${i % 2 === 0 ? "homepro-avatar-b" : "homepro-avatar-v"}`}>{initials(item.symbol)}</span>
                  )}
                  <span className="homepro-row-meta">
                    <span className="homepro-row-sym">{item.symbol}</span>
                    <span className="homepro-row-sub">NSE</span>
                  </span>
                  <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
                  <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {viewAllMode && (
        <ViewAllModal
          mode={viewAllMode}
          items={viewAllMode === "gainers" ? allGainers : viewAllMode === "losers" ? allLosers : allActive}
          onClose={() => setViewAllMode(null)}
          onPickSymbol={(symbol) => {
            setViewAllMode(null);
            onPickSymbol(symbol);
          }}
        />
      )}
    </div>
  );
}

function ViewAllModal({
  mode,
  items,
  onClose,
  onPickSymbol,
}: {
  mode: ViewAllMode;
  items: ScanMatch[];
  onClose: () => void;
  onPickSymbol: (symbol: string) => void;
}) {
  const title = mode === "gainers" ? "Top 20 Gainers" : mode === "losers" ? "Top 20 Losers" : "Top 20 Most Active";
  const subtitle =
    mode === "gainers"
      ? "Stocks with the largest positive change today"
      : mode === "losers"
        ? "Stocks with the largest negative change today"
        : "Stocks with the highest relative volume today";
  const accent = mode === "gainers" ? "homepro-avatar-g" : mode === "losers" ? "homepro-avatar-r" : "homepro-avatar-b";

  useEffect(() => {
    function handleKey(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }
    window.addEventListener("keydown", handleKey);
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", handleKey);
      document.body.style.overflow = "";
    };
  }, [onClose]);

  return createPortal(
    <div className="homepro-modal-overlay" onClick={onClose}>
      <div className="homepro-modal" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
        <div className="homepro-modal-head">
          <div>
            <h2>{title}</h2>
            <p>{subtitle}</p>
          </div>
          <button type="button" className="homepro-modal-close" onClick={onClose} aria-label="Close">✕</button>
        </div>
        <div className="homepro-modal-body">
          {items.length === 0 ? (
            <div className="homepro-empty">No stocks available yet.</div>
          ) : (
            <table className="homepro-modal-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Stock</th>
                  <th>Sector</th>
                  <th className="homepro-num">Price</th>
                  <th className="homepro-num">{mode === "active" ? "RVOL" : "Change"}</th>
                  <th className="homepro-num">{mode === "active" ? "Change" : "Score"}</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item, i) => (
                  <tr key={`vall-${item.symbol}`} onClick={() => onPickSymbol(item.symbol)}>
                    <td className="homepro-rank">{i + 1}</td>
                    <td>
                      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                        <span className={`homepro-avatar ${accent}`}>{item.symbol.slice(0, 2).toUpperCase()}</span>
                        <span>
                          <strong style={{ display: "block" }}>{item.symbol}</strong>
                          <small style={{ color: "var(--hp-muted)" }}>{item.name.length > 32 ? `${item.name.slice(0, 32)}…` : item.name}</small>
                        </span>
                      </div>
                    </td>
                    <td style={{ color: "var(--hp-muted)", fontSize: 12 }}>{item.sector || "—"}</td>
                    <td className="homepro-num">{formatPrice(item.last_price)}</td>
                    <td className="homepro-num">
                      {mode === "active" ? (
                        <span className="homepro-chip pos" style={{ background: "rgba(59,130,246,0.12)", color: "#1d4ed8" }}>
                          {item.relative_volume.toFixed(2)}x
                        </span>
                      ) : (
                        <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
                      )}
                    </td>
                    <td className="homepro-num">
                      {mode === "active"
                        ? <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
                        : <span style={{ fontSize: 12, color: "var(--hp-muted)" }}>{item.score?.toFixed(1) ?? "—"}</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>,
    document.body,
  );
}

function renderListSkeleton(prefix: string) {
  return Array.from({ length: 5 }).map((_, i) => (
    <div key={`${prefix}-skel-${i}`} className="homepro-row" aria-hidden>
      <div className="homepro-skel" style={{ width: 28, height: 28, borderRadius: 8 }} />
      <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 0 }}>
        <div className="homepro-skel" style={{ width: "70%", height: 12 }} />
        <div className="homepro-skel" style={{ width: "45%", height: 10 }} />
      </div>
      <div className="homepro-skel" style={{ width: 60, height: 12 }} />
      <div className="homepro-skel" style={{ width: 50, height: 18, borderRadius: 999 }} />
    </div>
  ));
}

function sliceBars(bars: ChartBar[], tf: NiftyTimeframe): ChartBar[] {
  if (!bars || bars.length === 0) return [];
  const windows: Record<NiftyTimeframe, number> = {
    "6M": 126,
    "1Y": 252,
    "3Y": 756,
  };
  const count = windows[tf];
  return bars.slice(-count);
}
