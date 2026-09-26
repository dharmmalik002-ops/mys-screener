import type { CSSProperties } from "react";
import { Fragment, useEffect, useId, useMemo, useRef, useState } from "react";
import { activatable } from "../lib/activate";
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
  const stroke = 11;
  const radius = size / 2 - stroke;
  const circumference = 2 * Math.PI * radius;
  const cx = size / 2;
  const cy = size / 2;
  // A small gap between arcs keeps two adjacent colours from reading as one
  // muddy band; round caps make each arc a distinct object.
  const live = segments.filter((s) => s.value > 0).length;
  const gap = live > 1 ? 7 : 0;
  let offset = 0;
  return (
    <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size} aria-hidden="true" className="homepro-donut" style={{ transform: "rotate(-90deg)" }}>
      <circle cx={cx} cy={cy} r={radius} stroke="var(--line)" strokeWidth={stroke} fill="none" />
      <circle cx={cx} cy={cy} r={radius - stroke - 3} stroke="var(--line)" strokeWidth={1} fill="none" strokeDasharray="1 4" />
      {total > 0 && segments.map((seg, i) => {
        const frac = Math.max(0, seg.value) / total;
        const dash = Math.max(0, frac * circumference - gap);
        const element = dash > 0 ? (
          <circle
            key={i}
            className="homepro-donut-arc"
            cx={cx}
            cy={cy}
            r={radius}
            stroke={seg.color}
            strokeWidth={stroke}
            fill="none"
            strokeDasharray={`${dash} ${circumference - dash}`}
            strokeDashoffset={-(offset + gap / 2)}
            strokeLinecap="round"
            style={{ "--arc-len": `${dash}` } as CSSProperties}
          />
        ) : null;
        offset += frac * circumference;
        return element;
      })}
    </svg>
  );
}

function BreadthHistoryChart({ history }: { history: BreadthDayCounts[] }) {
  const [activeIdx, setActiveIdx] = useState<number | null>(null);
  // These bars were <button>s with no onClick — they invited a click and did
  // nothing. Clicking now PINS a day so the readout survives mouse-leave;
  // clicking the pinned day again releases it.
  const [pinnedIdx, setPinnedIdx] = useState<number | null>(null);

  if (!history || history.length === 0) {
    return (
      <div className="homepro-breadth-history-empty">
        Building 10-day history…
      </div>
    );
  }
  const days = history.slice(-10);
  const focusedIdx = activeIdx ?? pinnedIdx ?? days.length - 1;
  const focused = days[focusedIdx] ?? days[days.length - 1];
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
        {/* hover is transient; a pinned day persists after the pointer leaves */}
        {days.map((d, idx) => {
          const total = Math.max(1, d.total);
          const advPct = (d.advances / total) * 100;
          const decPct = (d.declines / total) * 100;
          const uncPct = Math.max(0, 100 - advPct - decPct);
          const advLeads = d.advances >= d.declines;
          const isActive = idx === focusedIdx;
          const isPinned = idx === pinnedIdx;
          return (
            <button
              type="button"
              className={`homepro-breadth-history-day${isActive ? " active" : ""}${isPinned ? " pinned" : ""}`}
              key={d.date}
              onMouseEnter={() => setActiveIdx(idx)}
              onFocus={() => setActiveIdx(idx)}
              onClick={() => setPinnedIdx((current) => (current === idx ? null : idx))}
              aria-pressed={isPinned}
              title={isPinned ? "Click to unpin this day" : "Click to pin this day"}
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
  const uid = useId().replace(/[:]/g, "");
  const LO = 5;
  const HI = 30;
  const CX = 80;
  const CY = 84;
  const R = 62;
  const polar = (r: number, deg: number): readonly [number, number] => {
    const rad = (deg * Math.PI) / 180;
    return [CX + r * Math.cos(rad), CY - r * Math.sin(rad)] as const;
  };
  const angleFor = (value: number) => {
    const t = (Math.min(HI, Math.max(LO, value)) - LO) / (HI - LO);
    return 180 - 180 * t;
  };
  const arcPath = (a0: number, a1: number, r = R) => {
    const [x0, y0] = polar(r, a0);
    const [x1, y1] = polar(r, a1);
    const large = Math.abs(a0 - a1) > 180 ? 1 : 0;
    return `M ${x0.toFixed(2)} ${y0.toFixed(2)} A ${r} ${r} 0 ${large} 1 ${x1.toFixed(2)} ${y1.toFixed(2)}`;
  };
  // A 1.6° gap between bands turns one striped arc into five distinct segments.
  const GAP = 1.6;
  const segments = (xp.bands ?? [])
    .map((band) => {
      const v0 = Math.max(LO, band.min ?? LO);
      const v1 = Math.min(HI, band.max ?? HI);
      if (v1 <= v0) return null;
      const a0 = angleFor(v0) - GAP / 2;
      const a1 = angleFor(v1) + GAP / 2;
      return a0 > a1 ? { color: band.color, path: arcPath(a0, a1), active: xp.xp_score >= v0 && xp.xp_score < v1 } : null;
    })
    .filter((seg): seg is { color: string; path: string; active: boolean } => seg !== null);
  const needleAngle = angleFor(xp.xp_score);
  const [kx, ky] = polar(R, needleAngle);
  const [ix, iy] = polar(R - 16, needleAngle);
  return (
    <div className="homepro-xp-gauge" title={`XP ${xp.xp_score.toFixed(2)} — ${xp.regime}`}>
      <svg viewBox="0 0 160 100" role="img" aria-label={`XP breadth ${xp.xp_score.toFixed(2)}, ${xp.regime}`}>
        <defs>
          <filter id={`gauge-glow-${uid}`} x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="2.4" />
          </filter>
        </defs>
        <path d={arcPath(180, 0, R)} fill="none" stroke="var(--line)" strokeWidth={11} strokeLinecap="round" />
        {segments.map((seg, i) => (
          <path
            key={i}
            d={seg.path}
            fill="none"
            stroke={seg.color}
            strokeWidth={seg.active ? 7 : 5}
            strokeLinecap="butt"
            opacity={seg.active ? 1 : 0.38}
          />
        ))}
        <line x1={ix} y1={iy} x2={kx} y2={ky} stroke="var(--text)" strokeWidth={1.4} strokeLinecap="round" opacity={0.7} />
        <circle cx={kx} cy={ky} r={7} fill={xp.regime_color} opacity={0.45} filter={`url(#gauge-glow-${uid})`} />
        <circle cx={kx} cy={ky} r={4.2} fill="var(--card-flat)" stroke={xp.regime_color} strokeWidth={2.2} />
        <text
          x={CX}
          y={66}
          textAnchor="middle"
          className="homepro-xp-gauge-score"
          style={{ "--regime-color": xp.regime_color } as CSSProperties}
        >
          {xp.xp_score.toFixed(2)}
        </text>
        <text x={CX} y={82} textAnchor="middle" className="homepro-xp-gauge-regime">
          {xp.regime}
        </text>
      </svg>
    </div>
  );
}

function XpBreadthChart({ xp, height = 260 }: { xp: XpBreadthScore; height?: number }) {
  const uid = useId().replace(/[:]/g, "");
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [width, setWidth] = useState(960);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [zoomN, setZoomN] = useState<number | null>(null); // sessions to show; null = all

  useEffect(() => {
    const el = wrapRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width;
      if (w) setWidth(Math.max(260, Math.round(w)));
    });
    ro.observe(el);
    setWidth(Math.max(260, Math.round(el.clientWidth || 960)));
    return () => ro.disconnect();
  }, []);

  // Prefer the live (post warm-up) series; fall back to whatever exists.
  const live = xp.history.filter((p) => !p.warmup);
  const allPoints = live.length >= 5 ? live : xp.history;
  const maxN = allPoints.length;
  const minN = Math.min(20, maxN);
  const winN = Math.max(minN, Math.min(zoomN ?? maxN, maxN));

  // Ctrl/⌘ + wheel (and trackpad pinch, which arrives as ctrlKey) zooms the
  // plot. A plain wheel is left alone so the page still scrolls past the
  // chart. Bound natively because React's onWheel is passive and cannot
  // preventDefault the browser's own page zoom.
  useEffect(() => {
    const el = svgRef.current;
    if (!el || maxN <= minN) return;
    const onWheel = (event: WheelEvent) => {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      setZoomN((current) => {
        const base = Math.max(minN, Math.min(current ?? maxN, maxN));
        const next = event.deltaY > 0 ? Math.round(base * 1.12) : Math.round(base / 1.12);
        return Math.max(minN, Math.min(maxN, next));
      });
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [maxN, minN]);

  const points = allPoints.slice(-winN);
  if (points.length < 2) {
    return (
      <div className="homepro-xp-chart-wrap" ref={wrapRef}>
        <div className="homepro-xp-empty">Not enough history yet — run the breadth backfill.</div>
      </div>
    );
  }

  const padL = 34;
  // Phone widths: the in-chart regime labels ("Progressive Exposure") are
  // wider than the gutter reserved for them, so they used to spill past the
  // card edge and get clipped. Drop both the gutter and the labels below
  // 520px — the legend directly under the chart names every band anyway.
  const compact = width < 520;
  const padR = compact ? 12 : 120; // gutter for regime labels
  const padT = 14;
  const padB = 28;
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

  const hovering = hoverIdx != null;
  const hi = hoverIdx == null ? points.length - 1 : Math.max(0, Math.min(points.length - 1, hoverIdx));
  const hovered = points[hi];
  const prevPoint = hi > 0 ? points[hi - 1] : null;
  const hoverDelta = prevPoint ? hovered.xp_score - prevPoint.xp_score : null;
  const hx = x(hi);
  const hyv = y(hovered.xp_score);

  const fmtDate = (d: string) => {
    const dt = new Date(d);
    return isNaN(dt.getTime()) ? d : dt.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
  };
  const fmtTick = (d: string) => {
    const dt = new Date(d);
    return isNaN(dt.getTime()) ? d : dt.toLocaleDateString("en-IN", { month: "short", year: "2-digit" });
  };

  // Visible regime bands (clipped to the current y-range) for shading + labels.
  const visBands = xp.bands
    .map((b) => {
      const top = Math.min(yMax, b.max ?? yMax);
      const bot = Math.max(yMin, b.min ?? yMin);
      return { ...b, top, bot };
    })
    .filter((b) => b.top > b.bot);

  // The line is painted with a VERTICAL gradient whose stops sit exactly on
  // the regime thresholds, so the stroke itself changes colour as the score
  // crosses a band — the chart reads its own regime without a legend.
  const bandStops = [...visBands]
    .sort((a, b) => b.top - a.top)
    .flatMap((b) => {
      const o0 = ((y(b.top) - padT) / innerH) * 100;
      const o1 = ((y(b.bot) - padT) / innerH) * 100;
      return [
        { offset: o0, color: b.color },
        { offset: o1, color: b.color },
      ];
    });

  // X ticks: ~one per 140px, on evenly spaced sessions.
  const tickCount = Math.max(2, Math.min(8, Math.floor(innerW / 140)));
  const ticks = Array.from({ length: tickCount }, (_, k) => Math.round((k / (tickCount - 1)) * (points.length - 1)));

  // Tooltip placement (clamped within the plot).
  const tipW = 188;
  const tipLeft = Math.max(padL + tipW / 2, Math.min(padL + innerW - tipW / 2, hx));

  return (
    <div className="homepro-xp-chart-wrap" ref={wrapRef}>
      <svg
        ref={svgRef}
        className="homepro-xp-svg"
        width={width}
        height={height}
        role="img"
        aria-label={`XP breadth history, ${points.length} sessions`}
        onPointerLeave={() => setHoverIdx(null)}
        onPointerMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const px = e.clientX - rect.left;
          const i = Math.round(((px - padL) / innerW) * (points.length - 1));
          setHoverIdx(Math.max(0, Math.min(points.length - 1, i)));
        }}
      >
        <defs>
          <linearGradient id={`stroke-${uid}`} gradientUnits="userSpaceOnUse" x1="0" y1={padT} x2="0" y2={baseY}>
            {bandStops.map((s, i) => (
              <stop key={i} offset={`${Math.max(0, Math.min(100, s.offset)).toFixed(2)}%`} stopColor={s.color} />
            ))}
          </linearGradient>
          <linearGradient id={`area-${uid}`} gradientUnits="userSpaceOnUse" x1="0" y1={padT} x2="0" y2={baseY}>
            {bandStops.map((s, i) => (
              <stop
                key={i}
                offset={`${Math.max(0, Math.min(100, s.offset)).toFixed(2)}%`}
                stopColor={s.color}
                stopOpacity={0.2 * (1 - Math.max(0, Math.min(100, s.offset)) / 100) + 0.02}
              />
            ))}
          </linearGradient>
          <filter id={`glow-${uid}`} x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="3.2" />
          </filter>
          <clipPath id={`reveal-${uid}`}>
            <rect x={padL} y={0} width={hovering ? hx - padL : innerW} height={height} />
          </clipPath>
        </defs>

        {/* y grid + ticks at the regime thresholds */}
        {visBands.map((b) => (
          <g key={`g-${b.label}`}>
            <line x1={padL} x2={padL + innerW} y1={y(b.top)} y2={y(b.top)} className="homepro-xp-grid" />
            <text x={padL - 8} y={y(b.top) + 3} textAnchor="end" className="homepro-xp-axis">
              {b.top.toFixed(b.top % 1 ? 1 : 0)}
            </text>
          </g>
        ))}

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
            const fillOpacity = (XP_BAND_OPACITY[b.label] ?? 0.1) * 0.55;
            const active = hovered.regime === b.label;
            return (
              <g key={b.label}>
                <rect x={padL} y={yTop} width={innerW} height={h} fill={b.color} opacity={active ? fillOpacity * 1.9 : fillOpacity} />
                {compact ? null : (
                  <g className={active ? "homepro-xp-band-label is-active" : "homepro-xp-band-label"}>
                    <circle cx={padL + innerW + 14} cy={labelY - 3.5} r={3} fill={b.color} />
                    <text x={padL + innerW + 23} y={labelY} fill={active ? "var(--text)" : "var(--text-muted)"}>
                      {b.label}
                    </text>
                  </g>
                )}
              </g>
            );
          });
        })()}

        {/* x ticks */}
        {ticks.map((ti, k) => (
          <text
            key={`t-${ti}-${k}`}
            x={x(ti)}
            y={height - 8}
            textAnchor={k === 0 ? "start" : k === ticks.length - 1 ? "end" : "middle"}
            className="homepro-xp-axis"
          >
            {fmtTick(points[ti].date)}
          </text>
        ))}

        {/* area + line. While hovering, the part of the line to the right of
            the cursor dims so the eye stays on "what it was on that day". */}
        <path d={areaPath} fill={`url(#area-${uid})`} />
        <path d={linePath} fill="none" stroke={`url(#stroke-${uid})`} strokeWidth={1.8} strokeLinejoin="round" strokeLinecap="round" opacity={hovering ? 0.28 : 1} />
        {hovering ? (
          <path
            d={linePath}
            fill="none"
            stroke={`url(#stroke-${uid})`}
            strokeWidth={2}
            strokeLinejoin="round"
            strokeLinecap="round"
            clipPath={`url(#reveal-${uid})`}
          />
        ) : null}

        {/* crosshair */}
        {hovering ? (
          <g className="homepro-xp-crosshair">
            <line x1={hx} x2={hx} y1={padT} y2={baseY} />
            <line x1={padL} x2={padL + innerW} y1={hyv} y2={hyv} strokeDasharray="2 4" />
            <rect x={2} y={hyv - 9} width={padL - 6} height={18} rx={5} className="homepro-xp-axis-pill" />
            <text x={padL / 2 - 2} y={hyv + 3.5} textAnchor="middle" className="homepro-xp-axis-pill-text">
              {hovered.xp_score.toFixed(1)}
            </text>
          </g>
        ) : null}

        {/* current / hovered marker */}
        {!hovering ? <circle cx={hx} cy={hyv} r={4} fill={hovered.regime_color} className="homepro-xp-pulse" /> : null}
        <circle cx={hx} cy={hyv} r={9} fill={hovered.regime_color} opacity={0.35} filter={`url(#glow-${uid})`} />
        <circle cx={hx} cy={hyv} r={4.2} fill="var(--card-flat)" stroke={hovered.regime_color} strokeWidth={2.2} />
      </svg>

      {/* floating tooltip */}
      <div
        className={`homepro-xp-tip${hovering ? " show" : ""}`}
        style={{ left: tipLeft, top: Math.max(0, Math.min(height - 96, hyv - 104)) }}
      >
        <span className="homepro-xp-tip-date">{fmtDate(hovered.date)}</span>
        <span className="homepro-xp-tip-row">
          <span className="homepro-xp-tip-val" style={{ "--regime-color": hovered.regime_color } as CSSProperties}>
            {hovered.xp_score.toFixed(2)}
          </span>
          {hoverDelta != null ? (
            <span className={`homepro-xp-tip-delta ${hoverDelta > 0 ? "up" : hoverDelta < 0 ? "down" : ""}`}>
              {hoverDelta > 0 ? "+" : ""}
              {hoverDelta.toFixed(2)}
            </span>
          ) : null}
        </span>
        <span className="homepro-xp-tip-regime" style={{ "--regime-color": hovered.regime_color } as CSSProperties}>
          <i style={{ background: hovered.regime_color }} />
          {hovered.regime}
        </span>
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
            style={{ "--fill": `${((winN - minN) / Math.max(1, maxN - minN)) * 100}%` } as CSSProperties}
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
            <span className="homepro-xp-zoom-hint"> · ⌘/Ctrl + scroll to zoom</span>
          </span>
        </div>
      )}
    </div>
  );
}

function CandlestickChart({ bars, height = 240 }: { bars: ChartBar[]; height?: number }) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(640);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width;
      if (w) setWidth(Math.max(240, Math.round(w)));
    });
    ro.observe(el);
    setWidth(Math.max(240, Math.round(el.clientWidth || 640)));
    return () => ro.disconnect();
  }, []);

  if (!bars || bars.length < 2) {
    return (
      <div className="homepro-nifty-chart homepro-nifty-empty" ref={wrapRef}>
        Loading chart…
      </div>
    );
  }

  const padL = 4;
  const padR = 58; // right price axis, the terminal convention
  const padT = 10;
  const volH = 34;
  const padB = 22;
  const innerW = Math.max(10, width - padL - padR);
  const priceH = Math.max(10, height - padT - padB - volH - 6);
  const highs = bars.map((b) => b.high);
  const lows = bars.map((b) => b.low);
  const rawMin = Math.min(...lows);
  const rawMax = Math.max(...highs);
  const pad = (rawMax - rawMin || 1) * 0.06;
  const min = rawMin - pad;
  const max = rawMax + pad;
  const range = max - min || 1;
  const maxVol = Math.max(1, ...bars.map((b) => b.volume || 0));
  const slot = innerW / bars.length;
  const candleW = Math.max(1.2, Math.min(9, slot * 0.62));

  const y = (value: number) => padT + priceH - ((value - min) / range) * priceH;
  const cxAt = (i: number) => padL + slot * (i + 0.5);
  const volTop = padT + priceH + 6;

  // Four evenly spaced, rounded price levels for the grid.
  const step = (() => {
    const raw = range / 4;
    const mag = 10 ** Math.floor(Math.log10(raw));
    const norm = raw / mag;
    return (norm >= 5 ? 5 : norm >= 2 ? 2 : 1) * mag;
  })();
  const levels: number[] = [];
  for (let v = Math.ceil(min / step) * step; v <= max; v += step) levels.push(v);

  const toDate = (t: number) => new Date(t < 1e12 ? t * 1000 : t);
  const fmtDay = (t: number) => toDate(t).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
  const fmtTick = (t: number) => toDate(t).toLocaleDateString("en-IN", { month: "short", year: "2-digit" });
  const fmtPx = (v: number) => v.toLocaleString("en-IN", { maximumFractionDigits: 1, minimumFractionDigits: 1 });

  const last = bars[bars.length - 1];
  const lastUp = last.close >= last.open;
  const hi = hoverIdx == null ? null : Math.max(0, Math.min(bars.length - 1, hoverIdx));
  const hb = hi == null ? null : bars[hi];
  const prevClose = hi != null && hi > 0 ? bars[hi - 1].close : null;
  const hbChange = hb && prevClose ? ((hb.close - prevClose) / prevClose) * 100 : null;

  const tickCount = Math.max(2, Math.min(6, Math.floor(innerW / 110)));
  const ticks = Array.from({ length: tickCount }, (_, k) => Math.round((k / (tickCount - 1)) * (bars.length - 1)));

  return (
    <div className="homepro-nifty-chart" ref={wrapRef}>
      <svg
        width={width}
        height={height}
        role="img"
        aria-label="Nifty 50 daily candles"
        onPointerLeave={() => setHoverIdx(null)}
        onPointerMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const i = Math.floor((e.clientX - rect.left - padL) / slot);
          setHoverIdx(Math.max(0, Math.min(bars.length - 1, i)));
        }}
      >
        {levels.map((v) => (
          <g key={v}>
            <line x1={padL} x2={padL + innerW} y1={y(v)} y2={y(v)} className="homepro-nifty-grid" />
            <text x={padL + innerW + 8} y={y(v) + 3.5} className="homepro-nifty-axis">
              {v.toLocaleString("en-IN", { maximumFractionDigits: 0 })}
            </text>
          </g>
        ))}

        {bars.map((bar, i) => {
          const cx = cxAt(i);
          const up = bar.close >= bar.open;
          const color = up ? "var(--candle-up)" : "var(--candle-down)";
          const bodyTop = Math.min(y(bar.open), y(bar.close));
          const bodyH = Math.max(1, Math.abs(y(bar.open) - y(bar.close)));
          const vh = ((bar.volume || 0) / maxVol) * volH;
          const dim = hi != null && hi !== i;
          return (
            <g key={i} opacity={dim ? 0.55 : 1}>
              <rect x={cx - candleW / 2} y={volTop + volH - vh} width={candleW} height={vh} fill={color} opacity={0.22} />
              <line x1={cx} x2={cx} y1={y(bar.high)} y2={y(bar.low)} stroke={color} strokeWidth={1} />
              <rect x={cx - candleW / 2} y={bodyTop} width={candleW} height={bodyH} fill={color} rx={candleW > 4 ? 0.8 : 0} />
            </g>
          );
        })}

        {/* last price line + axis pill */}
        <line x1={padL} x2={padL + innerW} y1={y(last.close)} y2={y(last.close)} className={`homepro-nifty-last ${lastUp ? "up" : "down"}`} />
        <rect x={padL + innerW + 2} y={y(last.close) - 9} width={padR - 4} height={18} rx={4} className={`homepro-nifty-last-pill ${lastUp ? "up" : "down"}`} />
        <text x={padL + innerW + padR / 2} y={y(last.close) + 3.5} textAnchor="middle" className="homepro-nifty-last-text">
          {last.close.toLocaleString("en-IN", { maximumFractionDigits: 0 })}
        </text>

        {ticks.map((ti, k) => (
          <text
            key={`t-${ti}-${k}`}
            x={cxAt(ti)}
            y={height - 6}
            textAnchor={k === 0 ? "start" : k === ticks.length - 1 ? "end" : "middle"}
            className="homepro-nifty-axis"
          >
            {fmtTick(bars[ti].time)}
          </text>
        ))}

        {hb && hi != null ? (
          <g className="homepro-nifty-crosshair">
            <line x1={cxAt(hi)} x2={cxAt(hi)} y1={padT} y2={volTop + volH} />
            <line x1={padL} x2={padL + innerW} y1={y(hb.close)} y2={y(hb.close)} strokeDasharray="2 4" />
            <rect x={padL + innerW + 2} y={y(hb.close) - 9} width={padR - 4} height={18} rx={4} className="homepro-nifty-hover-pill" />
            <text x={padL + innerW + padR / 2} y={y(hb.close) + 3.5} textAnchor="middle" className="homepro-nifty-hover-text">
              {hb.close.toLocaleString("en-IN", { maximumFractionDigits: 0 })}
            </text>
          </g>
        ) : null}
      </svg>

      {/* OHLC legend — the TradingView convention: always visible, top-left,
          it follows the cursor and falls back to the latest session. */}
      {(() => {
        const b = hb ?? last;
        const i = hi ?? bars.length - 1;
        const pc = i > 0 ? bars[i - 1].close : null;
        const chg = hb ? hbChange : pc ? ((last.close - pc) / pc) * 100 : null;
        const up = chg == null ? b.close >= b.open : chg >= 0;
        return (
          <div className="homepro-nifty-ohlc">
            <span className="homepro-nifty-ohlc-date">{fmtDay(b.time)}</span>
            <span>O <b>{fmtPx(b.open)}</b></span>
            <span>H <b>{fmtPx(b.high)}</b></span>
            <span>L <b>{fmtPx(b.low)}</b></span>
            <span>C <b className={up ? "up" : "down"}>{fmtPx(b.close)}</b></span>
            {chg != null ? <span className={up ? "up" : "down"}>{chg >= 0 ? "+" : ""}{chg.toFixed(2)}%</span> : null}
          </div>
        );
      })()}
    </div>
  );
}

/* ---------- Component ---------- */

/**
 * One row of a mover list (Gainers / Losers / Most Active).
 *
 * The three lists were three copies of the same 18 lines that differed only in
 * the avatar tint, so the sparkline lands here once instead of three times.
 * Values are the real closes off `spark_closes`; when the backend has no recent
 * history for a symbol the shared Sparkline draws its dashed empty state rather
 * than inventing a shape.
 */
function MoverRow({
  item,
  avatarClass,
  onPickSymbol,
}: {
  item: ScanMatch;
  avatarClass: string;
  onPickSymbol: (symbol: string) => void;
}) {
  const logo = getLogoUrl(item.symbol);
  const series = item.spark_closes ?? [];
  const trendUp = series.length >= 2 ? series[series.length - 1] >= series[0] : item.change_pct >= 0;
  return (
    <button type="button" className="homepro-row" onClick={() => onPickSymbol(item.symbol)}>
      {logo ? (
        <img src={logo} className="homepro-logo-img" alt="" onError={(e) => (e.currentTarget.style.display = "none")} />
      ) : (
        <span className={`homepro-avatar ${avatarClass}`}>{initials(item.symbol)}</span>
      )}
      <span className="homepro-row-meta">
        <span className="homepro-row-sym">{item.symbol}</span>
        <span className="homepro-row-sub">NSE</span>
      </span>
      <span className="homepro-row-spark">
        <Sparkline
          values={series}
              minRangePct={10}
          color={trendUp ? "var(--positive)" : "var(--negative)"}
          width={46}
          height={18}
          label={
            series.length >= 2
              ? `${item.symbol}: ${series.length}-session close trend`
              : `${item.symbol}: no recent close history`
          }
        />
      </span>
      <span className="homepro-row-price">{formatPrice(item.last_price)}</span>
      <span className={`homepro-chip ${item.change_pct >= 0 ? "pos" : "neg"}`}>{formatReturn(item.change_pct)}</span>
    </button>
  );
}

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

  /** The next NSE weekday session from now in IST. Exchange holidays are not
      known here, so this is the next weekday — never the EOD date just printed,
      which is what this label used to show after the close. */
  const nextSessionLabel = useMemo(() => {
    const ist = new Date(nowTick + (new Date(nowTick).getTimezoneOffset() + 330) * 60_000);
    const beforeOpen = ist.getHours() * 60 + ist.getMinutes() < 9 * 60 + 15;
    const day = new Date(ist.getFullYear(), ist.getMonth(), ist.getDate());
    const isWeekday = (d: Date) => d.getDay() !== 0 && d.getDay() !== 6;
    if (!(beforeOpen && isWeekday(day))) {
      do { day.setDate(day.getDate() + 1); } while (!isWeekday(day));
    }
    return day.toLocaleDateString("en-IN", { weekday: "short", day: "numeric", month: "short" });
  }, [nowTick]);

  useEffect(() => {
    let active = true;
    getGroupRankHistory(activeMarket)
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
          <div className="homepro-briefing-title">Market Briefing · {snapshotDateLabel}</div>
          <div className="homepro-briefing-body">
            {briefing.xp ? (
              <span>
                Market regime is{" "}
                <strong
                  className="homepro-briefing-regime"
                  style={{ "--regime-color": briefing.xp.regime_color || "var(--text)" } as CSSProperties}
                >
                  {briefing.xp.regime}
                </strong>
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
                  <Fragment key={g.group_id}>
                  {i > 0 ? ", " : null}
                  <button
                    type="button"
                    className="homepro-briefing-link"
                    onClick={() => onOpenGroups({ groupId: g.group_id })}
                  >
                    {g.group_name} (▲{g.rank_change_1w})
                  </button>
                  </Fragment>
                ))}
                .
              </span>
            ) : briefing.topGroups.length > 0 ? (
              <span>
                {" "}Leading groups:{" "}
                {briefing.topGroups.map((g, i) => (
                  <Fragment key={g.group_id}>
                  {i > 0 ? ", " : null}
                  <button
                    type="button"
                    className="homepro-briefing-link"
                    onClick={() => onOpenGroups({ groupId: g.group_id })}
                  >
                    {g.group_name}
                  </button>
                  </Fragment>
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
              {marketOpen ? `Closes in ${sessionCountdown}` : `Next session ${nextSessionLabel}`}
            </div>
          </div>

          {/* EOD Date */}
          <div className="homepro-kpi homepro-kpi-date">
            <div className="homepro-kpi-label">EOD Date</div>
            <div className="homepro-kpi-value" style={{ fontSize: "var(--fs-heading)" }}>{snapshotDateLabel || "—"}</div>
            <div className="homepro-kpi-sub">Last Updated</div>
            <div className="homepro-kpi-bottom">
              <div className="homepro-kpi-icon" aria-hidden="true"><CalendarDays size={16} strokeWidth={2.2} /></div>
              <div style={{ fontSize: "var(--fs-base)", fontWeight: 600 }}>{snapshotTimeLabel || "—"}</div>
            </div>
          </div>

          {/* Advances / Declines */}
          <div className="homepro-kpi homepro-kpi-breadth">
            <div className="homepro-kpi-label">Advances / Declines</div>
            <div className="homepro-kpi-value" style={{ fontSize: "var(--fs-heading)" }}>{advances} / {declines}</div>
            <div className="homepro-kpi-sub">Stocks</div>
            <div className="homepro-kpi-bottom">
              <div style={{ position: "relative", width: 56, height: 56 }}>
                <Donut
                  size={56}
                  segments={[
                    { value: advances, color: "var(--positive)" },
                    { value: declines, color: "var(--negative)" },
                  ]}
                />
              </div>
              <div style={{ display: "flex", gap: 12, fontSize: "var(--fs-small)", fontWeight: 700 }}>
                <span style={{ color: "var(--positive)" }}>{Math.round(advPct)}%</span>
                <span style={{ color: "var(--negative)" }}>{100 - Math.round(advPct)}%</span>
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
                <th className="homepro-num" title="Group return over the last month">1M %</th>
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
                    aria-label={`Open group ${group.group_name ?? group.group_id}`}
                    {...activatable(() => onOpenGroups({ groupId: group.group_id }))}
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
                            color={improving ? "var(--positive)" : "var(--negative)"}
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
                  { value: advances, color: "var(--positive)" },
                  { value: declines, color: "var(--negative)" },
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
                <span><span className="homepro-legend-swatch" style={{ background: "var(--positive)" }} />Advancing</span>
                <span><strong>{advances}</strong> ({((advances / Math.max(1, breadthTotal)) * 100).toFixed(1)}%)</span>
              </div>
              <div className="homepro-legend-row">
                <span><span className="homepro-legend-swatch" style={{ background: "var(--negative)" }} />Declining</span>
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
                  <span style={{ color: niftyChange >= 0 ? "var(--positive)" : "var(--negative)", fontWeight: 700, fontSize: "var(--fs-base)" }}>
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
              <div style={{ fontSize: "var(--fs-tiny)", color: "var(--hp-muted)", marginTop: 6 }}>At Close</div>
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
            {topGainers.length === 0 ? renderListSkeleton("g") : topGainers.map((item) => (
              <MoverRow
                key={`g-${item.symbol}`}
                item={item}
                avatarClass="homepro-avatar-g"
                onPickSymbol={onPickSymbol}
              />
            ))}
          </div>
        </div>

        {/* Top Losers */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Top Losers</h3>
            <button className="homepro-link" onClick={() => setViewAllMode("losers")}>View All</button>
          </div>
          <div className="homepro-list">
            {topLosers.length === 0 ? renderListSkeleton("l") : topLosers.map((item) => (
              <MoverRow
                key={`l-${item.symbol}`}
                item={item}
                avatarClass="homepro-avatar-r"
                onPickSymbol={onPickSymbol}
              />
            ))}
          </div>
        </div>

        {/* Most Active */}
        <div className="homepro-card">
          <div className="homepro-card-head">
            <h3>Most Active</h3>
            <button className="homepro-link" onClick={() => setViewAllMode("active")}>View All</button>
          </div>
          <div className="homepro-list">
            {mostActive.length === 0 ? renderListSkeleton("a") : mostActive.map((item, i) => (
              <MoverRow
                key={`a-${item.symbol}`}
                item={item}
                avatarClass={i % 2 === 0 ? "homepro-avatar-b" : "homepro-avatar-v"}
                onPickSymbol={onPickSymbol}
              />
            ))}
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
                  <th className="homepro-num">Trend</th>
                  <th className="homepro-num">Price</th>
                  <th className="homepro-num">{mode === "active" ? "RVOL" : "Change"}</th>
                  <th className="homepro-num">{mode === "active" ? "Change" : "Score"}</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item, i) => (
                  <tr key={`vall-${item.symbol}`} aria-label={`Open ${item.symbol}`} {...activatable(() => onPickSymbol(item.symbol))}>
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
                    <td style={{ color: "var(--hp-muted)", fontSize: "var(--fs-small)" }}>{item.sector || "—"}</td>
                    <td className="homepro-modal-spark">
                      {(() => {
                        const series = item.spark_closes ?? [];
                        const up = series.length >= 2 ? series[series.length - 1] >= series[0] : item.change_pct >= 0;
                        return (
                          <Sparkline
                            values={series}
                            color={up ? "var(--positive)" : "var(--negative)"}
                            width={64}
                            height={22}
                            minRangePct={10}
                            label={
                              series.length >= 2
                                ? `${item.symbol}: ${series.length}-session close trend`
                                : `${item.symbol}: no recent close history`
                            }
                          />
                        );
                      })()}
                    </td>
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
                        : <span style={{ fontSize: "var(--fs-small)", color: "var(--hp-muted)" }}>{item.score?.toFixed(1) ?? "—"}</span>}
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
