import { useEffect, useId, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { CalendarDays } from "lucide-react";

import {
  getChart,
  getGroupRankHistory,
  getMarketOverview,
  type BreadthDayCounts,
  type ChartBar,
  type DashboardResponse,
  type GroupRankHistoryPoint,
  type IndustryGroupsResponse,
  type IndustryGroupRankItem,
  type MarketKey,
  type MarketMacroItem,
  type ScanMatch,
  type XpBreadthScore,
} from "../lib/api";

import { Sparkline } from "./Sparkline";

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

/* (removed) Local `Sparkline` and `MiniSparkline` lived here. Both are now the
   shared ./Sparkline component, which refuses to draw a curve from fewer than
   two real points instead of inventing one. */

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

// Per-regime band shading opacity. Kept muted so the score line stays the
// focal point; "Avoid Longs" gets slightly more presence as the risk zone.
const XP_BAND_OPACITY: Record<string, number> = {
  "Avoid Longs": 0.12,
  "Choppy / Spurt Only": 0.07,
  "Progressive Exposure": 0.05,
  "Swing-Friendly": 0.08,
  "Extremely Strong": 0.1,
};

/* Signature XP gauge: a semicircular arc built from the regime bands with a
   needle at the current score — the score reads as "where on the dial am I"
   instead of a bare number in a colored box. Domain [5, 30] covers the bands
   (Avoid <9.5 … Extremely Strong >25) with visible headroom either side. */
function XpGauge({ xp }: { xp: XpBreadthScore }) {
  const LO = 5;
  const HI = 30;
  const CX = 80;
  const CY = 84;
  const R = 60;
  const polar = (r: number, deg: number): readonly [number, number] => {
    const rad = (deg * Math.PI) / 180;
    return [CX + r * Math.cos(rad), CY - r * Math.sin(rad)] as const;
  };
  const angleFor = (value: number) => {
    const t = (Math.min(HI, Math.max(LO, value)) - LO) / (HI - LO);
    return 180 - 180 * t;
  };
  const arcPath = (a0: number, a1: number) => {
    const [x0, y0] = polar(R, a0);
    const [x1, y1] = polar(R, a1);
    return `M ${x0.toFixed(2)} ${y0.toFixed(2)} A ${R} ${R} 0 0 1 ${x1.toFixed(2)} ${y1.toFixed(2)}`;
  };
  const segments = (xp.bands ?? [])
    .map((band) => {
      const v0 = Math.max(LO, band.min ?? LO);
      const v1 = Math.min(HI, band.max ?? HI);
      return v1 > v0 ? { color: band.color, path: arcPath(angleFor(v0), angleFor(v1)) } : null;
    })
    .filter((seg): seg is { color: string; path: string } => seg !== null);
  const needleAngle = angleFor(xp.xp_score);
  const [nx, ny] = polar(R - 14, needleAngle);
  const [tx, ty] = polar(R + 4, needleAngle);
  return (
    <div className="homepro-xp-gauge" title={`XP ${xp.xp_score.toFixed(2)} — ${xp.regime}`}>
      <svg viewBox="0 0 160 100" role="img" aria-label={`XP breadth ${xp.xp_score.toFixed(2)}, ${xp.regime}`}>
        {segments.map((seg, i) => (
          <path key={i} d={seg.path} fill="none" stroke={seg.color} strokeWidth={9} opacity={0.92} />
        ))}
        <line x1={nx} y1={ny} x2={tx} y2={ty} stroke="var(--text)" strokeWidth={2.6} strokeLinecap="round" />
        <text x={CX} y={62} textAnchor="middle" className="homepro-xp-gauge-score" fill={xp.regime_color}>
          {xp.xp_score.toFixed(2)}
        </text>
        <text x={CX} y={80} textAnchor="middle" className="homepro-xp-gauge-regime">
          {xp.regime}
        </text>
      </svg>
    </div>
  );
}

function XpBreadthChart({ xp, height = 240 }: { xp: XpBreadthScore; height?: number }) {
  const uid = useId().replace(/[:]/g, "");
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(960);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [zoomN, setZoomN] = useState<number | null>(null); // sessions to show; null = all

  useEffect(() => {
    const el = wrapRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width;
      if (w) setWidth(Math.max(320, Math.round(w)));
    });
    ro.observe(el);
    setWidth(Math.max(320, Math.round(el.clientWidth || 960)));
    return () => ro.disconnect();
  }, []);

  // Prefer the live (post warm-up) series; fall back to whatever exists.
  const live = xp.history.filter((p) => !p.warmup);
  const allPoints = live.length >= 5 ? live : xp.history;
  const maxN = allPoints.length;
  const minN = Math.min(20, maxN);
  const winN = Math.max(minN, Math.min(zoomN ?? maxN, maxN));
  const points = allPoints.slice(-winN);
  if (points.length < 2) {
    return (
      <div className="homepro-xp-chart-wrap" ref={wrapRef}>
        <div className="homepro-xp-empty">Not enough history yet — run the breadth backfill.</div>
      </div>
    );
  }

  const padL = 12;
  const padR = 78; // gutter for regime labels
  const padT = 18;
  const padB = 26;
  const innerW = Math.max(10, width - padL - padR);
  const innerH = Math.max(10, height - padT - padB);

  const scores = points.map((p) => p.xp_score);
  // Always show the full regime ladder so every band keeps real vertical
  // thickness (clamp the floor <= 8 and the ceiling >= 27).
  const yMin = Math.max(0, Math.min(8, Math.floor(Math.min(...scores) - 1.5)));
  const yMax = Math.max(27, Math.ceil(Math.max(...scores) + 1.5));
  const range = yMax - yMin || 1;

  const x = (i: number) => padL + (points.length === 1 ? innerW / 2 : (i / (points.length - 1)) * innerW);
  const y = (v: number) =>
    padT + innerH - ((Math.max(yMin, Math.min(yMax, v)) - yMin) / range) * innerH;

  const pts = points.map((p, i) => [x(i), y(p.xp_score)] as const);

  // Catmull-Rom -> cubic bézier for a smooth, non-overshooting line.
  const linePath = (() => {
    if (pts.length < 3) return pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ");
    let d = `M${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)}`;
    const t = 0.16;
    for (let i = 0; i < pts.length - 1; i++) {
      const p0 = pts[i - 1] || pts[i];
      const p1 = pts[i];
      const p2 = pts[i + 1];
      const p3 = pts[i + 2] || p2;
      const c1x = p1[0] + (p2[0] - p0[0]) * t;
      const c1y = p1[1] + (p2[1] - p0[1]) * t;
      const c2x = p2[0] - (p3[0] - p1[0]) * t;
      const c2y = p2[1] - (p3[1] - p1[1]) * t;
      d += ` C${c1x.toFixed(1)},${c1y.toFixed(1)} ${c2x.toFixed(1)},${c2y.toFixed(1)} ${p2[0].toFixed(1)},${p2[1].toFixed(1)}`;
    }
    return d;
  })();
  const baseY = padT + innerH;
  const areaPath = `${linePath} L${pts[pts.length - 1][0].toFixed(1)},${baseY.toFixed(1)} L${pts[0][0].toFixed(1)},${baseY.toFixed(1)} Z`;

  const hi = hoverIdx == null ? points.length - 1 : Math.max(0, Math.min(points.length - 1, hoverIdx));
  const hovered = points[hi];
  const hx = x(hi);
  const hyv = y(hovered.xp_score);
  const lineColor = xp.regime_color;

  const fmtDate = (d: string) => {
    const dt = new Date(d);
    return isNaN(dt.getTime()) ? d : dt.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
  };

  // Visible regime bands (clipped to the current y-range) for shading + labels.
  const visBands = xp.bands
    .map((b) => {
      const top = Math.min(yMax, b.max ?? yMax);
      const bot = Math.max(yMin, b.min ?? yMin);
      return { ...b, top, bot };
    })
    .filter((b) => b.top > b.bot);

  // Tooltip placement (clamped within the plot).
  const tipLeft = Math.max(64, Math.min(width - 64, hx));

  return (
    <div className="homepro-xp-chart-wrap" ref={wrapRef}>
      <svg
        className="homepro-xp-svg"
        width={width}
        height={height}
        role="img"
        onMouseLeave={() => setHoverIdx(null)}
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const px = e.clientX - rect.left;
          const i = Math.round(((px - padL) / innerW) * (points.length - 1));
          setHoverIdx(Math.max(0, Math.min(points.length - 1, i)));
        }}
      >
        <defs>
          <linearGradient id={`area-${uid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={lineColor} stopOpacity={0.34} />
            <stop offset="60%" stopColor={lineColor} stopOpacity={0.08} />
            <stop offset="100%" stopColor={lineColor} stopOpacity={0} />
          </linearGradient>
          <filter id={`glow-${uid}`} x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="3.2" result="b" />
            <feMerge>
              <feMergeNode in="b" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* regime band shading + right-edge labels. Label Y positions get a
            collision pass — thin bands (e.g. Progressive Exposure over Choppy)
            otherwise print their labels on top of each other. */}
        {(() => {
          const LABEL_GAP = 12;
          const geom = visBands
            .map((b) => {
              const yTop = y(b.top);
              const h = Math.max(0, y(b.bot) - y(b.top));
              return { band: b, yTop, h, labelY: yTop + h / 2 + 3 };
            })
            .sort((a, b2) => a.labelY - b2.labelY);
          for (let i = 1; i < geom.length; i += 1) {
            if (geom[i].labelY - geom[i - 1].labelY < LABEL_GAP) {
              geom[i].labelY = geom[i - 1].labelY + LABEL_GAP;
            }
          }
          return geom.map(({ band: b, yTop, h, labelY }) => {
            const fillOpacity = XP_BAND_OPACITY[b.label] ?? 0.1;
            return (
              <g key={b.label}>
                <rect x={padL} y={yTop} width={innerW} height={h} fill={b.color} opacity={fillOpacity} />
                <line x1={padL} x2={padL + innerW} y1={yTop} y2={yTop} stroke={b.color} strokeWidth={1} strokeDasharray="2 4" opacity={0.4} />
                <text x={padL + innerW + 8} y={labelY} fontSize={10} fontWeight={700} fill={b.color} opacity={0.95}>
                  {b.label}
                </text>
              </g>
            );
          });
        })()}

        {/* area + line */}
        <path d={areaPath} fill={`url(#area-${uid})`} />
        <path
          d={linePath}
          fill="none"
          stroke={lineColor}
          strokeWidth={2.4}
          strokeLinejoin="round"
          strokeLinecap="round"
        />

        {/* crosshair on hover */}
        {hoverIdx != null && (
          <line x1={hx} x2={hx} y1={padT} y2={baseY} stroke="var(--hp-text, #0f172a)" strokeWidth={1} strokeDasharray="3 3" opacity={0.25} />
        )}

        {/* current/hovered marker with glow */}
        <circle cx={hx} cy={hyv} r={9} fill={hovered.regime_color} opacity={0.18} filter={`url(#glow-${uid})`} />
        <circle cx={hx} cy={hyv} r={4.5} fill={hovered.regime_color} stroke="#fff" strokeWidth={2} />

        {/* x-axis end dates */}
        <text x={padL} y={height - 7} fontSize={10} fill="var(--hp-muted, #94a0b8)">{fmtDate(points[0].date)}</text>
        <text x={padL + innerW} y={height - 7} fontSize={10} textAnchor="end" fill="var(--hp-muted, #94a0b8)">{fmtDate(points[points.length - 1].date)}</text>
      </svg>

      {/* floating tooltip */}
      <div
        className={`homepro-xp-tip${hoverIdx != null ? " show" : ""}`}
        style={{ left: tipLeft }}
      >
        <span className="homepro-xp-tip-date">{fmtDate(hovered.date)}</span>
        <span className="homepro-xp-tip-val" style={{ color: hovered.regime_color }}>{hovered.xp_score.toFixed(2)}</span>
        <span className="homepro-xp-tip-regime" style={{ color: hovered.regime_color }}>{hovered.regime}</span>
      </div>

      {/* zoom slider */}
      {maxN > minN && (
        <div className="homepro-xp-zoom">
          <button
            type="button"
            className="homepro-xp-zoom-btn"
            title="Zoom in (fewer, more recent sessions)"
            onClick={() => setZoomN(Math.max(minN, Math.round(winN / 1.5)))}
          >
            +
          </button>
          <input
            className="homepro-xp-range"
            type="range"
            min={minN}
            max={maxN}
            value={winN}
            onChange={(e) => setZoomN(Number(e.target.value))}
            aria-label="Zoom: number of sessions shown"
            title="Drag to zoom"
          />
          <button
            type="button"
            className="homepro-xp-zoom-btn"
            title="Zoom out (more history)"
            onClick={() => setZoomN(Math.min(maxN, Math.round(winN * 1.5)))}
          >
            −
          </button>
          <span className="homepro-xp-zoom-info">
            {winN} sessions · from {fmtDate(points[0].date)}
          </span>
        </div>
      )}
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
  // Real per-group rank history for the trend column. Previously a sine wave.
  const [rankHistory, setRankHistory] = useState<Record<string, GroupRankHistoryPoint[]>>({});
  // Ticks every second so the close countdown actually counts down.
  const [nowTick, setNowTick] = useState(() => Date.now());

  useEffect(() => {
    const id = window.setInterval(() => setNowTick(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  /** Real time-to-close for the NSE session (15:30 IST), as HH:MM:SS. */
  const sessionCountdown = useMemo(() => {
    // Convert "now" into IST wall-clock regardless of the viewer's timezone.
    const ist = new Date(nowTick + (new Date(nowTick).getTimezoneOffset() + 330) * 60_000);
    const secondsToClose =
      (15 * 3600 + 30 * 60) - (ist.getHours() * 3600 + ist.getMinutes() * 60 + ist.getSeconds());
    if (secondsToClose <= 0) return "00:00:00";
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${pad(Math.floor(secondsToClose / 3600))}:${pad(Math.floor((secondsToClose % 3600) / 60))}:${pad(secondsToClose % 60)}`;
  }, [nowTick]);

  useEffect(() => {
    let active = true;
    getGroupRankHistory(activeMarket, 30)
      .then((payload) => {
        if (active) setRankHistory(payload.groups);
      })
      .catch(() => {
        // Non-fatal: the column falls back to a "no trend yet" hairline.
        if (active) setRankHistory({});
      });
    return () => {
      active = false;
    };
  }, [activeMarket]);

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

  /* (removed) `genMockSparkline` lived here — a deterministic sine wave that
     fed the two KPI cards and every row of the groups table under a column
     headed "Day Performance". Seeded by row index, so the same row always drew
     the same curve no matter which group occupied it. Real series only now. */

  const niftyPoint = macroItems.find((c) => c.symbol === "^NSEI");
  const niftyPrice = niftyPoint?.price ?? null;
  const niftyChange = niftyPoint?.change_pct ?? null;

  const briefing = (() => {
    const xp = dashboard?.xp_breadth ?? null;
    const breadth = dashboard?.breadth_today ?? null;
    const improving = (groups?.groups ?? [])
      .filter((g) => (g.rank_change_1w ?? 0) > 0)
      .sort((a, b) => (b.rank_change_1w ?? 0) - (a.rank_change_1w ?? 0))
      .slice(0, 3);
    const topGroups = (groups?.groups ?? []).slice(0, 3);
    if (!xp && !breadth && topGroups.length === 0) return null;
    return { xp, breadth, improving, topGroups };
  })();

  return (
    <div className="homepro">
      {briefing ? (
        <div className="homepro-briefing">
          <div className="homepro-briefing-title">Morning Briefing · {snapshotDateLabel}</div>
          <div className="homepro-briefing-body">
            {briefing.xp ? (
              <span>
                Market regime is{" "}
                <strong style={{ color: briefing.xp.regime_color || undefined }}>{briefing.xp.regime}</strong>
                {" "}(XP {briefing.xp.xp_score.toFixed(1)}).
              </span>
            ) : null}
            {briefing.breadth && briefing.breadth.total > 0 ? (
              <span>
                {" "}Breadth: <strong className={briefing.breadth.advances >= briefing.breadth.declines ? "pos" : "neg"}>
                  {briefing.breadth.advances} adv / {briefing.breadth.declines} dec
                </strong>.
              </span>
            ) : null}
            {briefing.improving.length > 0 ? (
              <span>
                {" "}Improving groups:{" "}
                {briefing.improving.map((g, i) => (
                  <button
                    key={g.group_id}
                    type="button"
                    className="homepro-briefing-link"
                    onClick={() => onOpenGroups({ groupId: g.group_id })}
                  >
                    {g.group_name} (▲{g.rank_change_1w}){i < briefing.improving.length - 1 ? "," : ""}
                  </button>
                ))}
                .
              </span>
            ) : briefing.topGroups.length > 0 ? (
              <span>
                {" "}Leading groups:{" "}
                {briefing.topGroups.map((g, i) => (
                  <button
                    key={g.group_id}
                    type="button"
                    className="homepro-briefing-link"
                    onClick={() => onOpenGroups({ groupId: g.group_id })}
                  >
                    {g.group_name}{i < briefing.topGroups.length - 1 ? "," : ""}
                  </button>
                ))}
                .
              </span>
            ) : null}
          </div>
        </div>
      ) : null}
      {/* ============ ROW 1 — KPIs + SNAPSHOT ============ */}
      <div className="homepro-row-top">
        {/* KPI cards */}
        <div className="homepro-kpis">
          {/* Universe */}
          <div className="homepro-kpi homepro-kpi-universe">
            <div className="homepro-kpi-label">Universe</div>
            <div className="homepro-kpi-value">{universeCount.toLocaleString("en-IN")}</div>
            <div className="homepro-kpi-sub">Total Stocks</div>
            {/* No sparkline and no "+12 vs yesterday": there is no universe-count
                time series to draw, and the old ones were fabricated. */}
            <div className="homepro-kpi-sub">Passing the liquidity &amp; market-cap floor</div>
          </div>

          {/* Market Status */}
          <div className="homepro-kpi homepro-kpi-status">
            <div className="homepro-kpi-label">Market Status</div>
            <div className="homepro-kpi-value">
              <span>{marketOpen ? "Open" : "Closed"}</span>
              <span className={marketOpen ? "homepro-status-dot" : "homepro-status-dot closed"} />
            </div>
            <div className="homepro-kpi-sub">Market is {marketOpen ? "live" : "closed"}</div>
            {/* The old sparkline here was a sine wave; the countdown was the
                hardcoded string "Closes in 01:24:15" and never counted down. */}
            <div className="homepro-kpi-sub">
              {marketOpen ? `Closes in ${sessionCountdown}` : `Next session ${snapshotDateLabel}`}
            </div>
          </div>

          {/* EOD Date */}
          <div className="homepro-kpi homepro-kpi-date">
            <div className="homepro-kpi-label">EOD Date</div>
            <div className="homepro-kpi-value" style={{ fontSize: 22 }}>{snapshotDateLabel || "—"}</div>
            <div className="homepro-kpi-sub">Last Updated</div>
            <div className="homepro-kpi-bottom">
              <div className="homepro-kpi-icon" aria-hidden="true"><CalendarDays size={16} strokeWidth={2.2} /></div>
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

      {/* ============ XP Market Breadth Score ============ */}
      {xpBreadth && (
        <div className="homepro-card homepro-xp-card">
          <div className="homepro-card-head homepro-xp-head">
            <div className="homepro-xp-title">
              <h3>XP Market Breadth Score</h3>
              <span className="homepro-xp-sub">NSE listed · EOD · calibrated to EM</span>
            </div>
            <div className="homepro-xp-badge-wrap">
              {(() => {
                const h = xpBreadth.history;
                const prev = h.length >= 2 ? h[h.length - 2].xp_score : null;
                const delta = prev == null ? null : xpBreadth.xp_score - prev;
                if (delta == null) return null;
                const cls = Math.abs(delta) < 0.05 ? "flat" : delta > 0 ? "up" : "down";
                const arrow = cls === "flat" ? "▬" : cls === "up" ? "▲" : "▼";
                return (
                  <span className={`homepro-xp-delta ${cls}`} title="Change vs previous session">
                    {arrow} {Math.abs(delta).toFixed(2)}
                  </span>
                );
              })()}
              <XpGauge xp={xpBreadth} />
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
                <th className="homepro-num" title="Daily rank across the last 30 stored sessions — rising means the group is climbing the rankings.">
                  Rank Trend
                </th>
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
                      {(() => {
                        const series = rankHistory[group.group_id] ?? [];
                        const ranks = series.map((point) => point.rank);
                        // Lower rank is better, so invert: a climbing group rises.
                        const improving = ranks.length >= 2 && ranks[ranks.length - 1] <= ranks[0];
                        return (
                          <Sparkline
                            values={ranks}
                            invert
                            color={improving ? "#10b981" : "#ef4444"}
                            height={24}
                            label={
                              ranks.length >= 2
                                ? `${group.group_name} rank ${ranks[0]} to ${ranks[ranks.length - 1]} over ${ranks.length} sessions`
                                : `${group.group_name}: not enough rank history yet`
                            }
                          />
                        );
                      })()}
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
