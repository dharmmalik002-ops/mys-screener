import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getChart, getJournalData, saveJournalData, type MarketKey } from "../lib/api";
import { notifyJournalUpdated } from "../lib/journal";
import { NewsModal } from "./NewsModal";
import "./TradeJournalPanel.css";

// ─── Types ─────────────────────────────────────────────────────────────────────

type OpenPosCat = "full" | "half" | "quarter";
interface VCP { t?: string; depth?: string; vol?: string; }
interface Trade {
  symbol: string; type: string; qty: number; price: number; date: string;
  setupType: string; stoploss: number; target: number; tags: string[];
  remarks: string; img?: string; vcp?: VCP;
}
interface PosMeta { cmp?: number; sl?: number; fetchTicker?: string; prev_close?: number; }
interface ClosedTrade {
  symbol: string; qty: number; entryPx: number; exitPx: number;
  entryDate: string; exitDate: string; pnl: number; perc: number;
  setupType: string; tags: string[]; remarks: string; img?: string; vcp?: VCP;
  equitySnapshot: number; posSizePct: number; sellIndex: number; buyIndices: number[];
}
interface OpenPosition {
  symbol: string; qty: number; avgPx: number; totalInvested: number;
  buyIndices: number[]; tags: string[]; remarks: string; img?: string; setupType?: string;
}
interface FIFOResult {
  closedTrades: ClosedTrade[]; openPositions: OpenPosition[];
  currentEquity: number; openLotsDict: Record<string, Trade[]>;
}

export interface JournalAddRequest {
  symbol: string;
  suggestedPrice?: number;
}

// ─── Props ──────────────────────────────────────────────────────────────────────
interface TradeJournalPanelProps {
  market?: MarketKey;
  addRequest?: JournalAddRequest | null;
  onAddRequestHandled?: () => void;
  onOpenSymbolChart?: (symbol: string) => void;
}

// ─── Constants ─────────────────────────────────────────────────────────────────
const LS_DATA = "tradingJournalData";
const LS_EQUITY = "tradingJournalEquity";
const LS_SETUPS = "tradingJournalSetups";
const LS_POSITIONS = "tradingJournalPositions";
const LS_META = "tradingJournalPosMeta";

const PREDEFINED_TAGS = [
  "FOMO", "Early Entry", "Late Entry", "Perfect Entry", "Chased",
  "Held Well", "Sold Early", "Held Too Long", "Averaged Down",
  "Followed Plan", "Broke Plan", "Emotional",
];
const DEFAULT_SETUPS = ["VCP", "Flat Base", "Cup & Handle", "Breakout", "Pullback", "Stage 2", "Other"];

// ─── localStorage helpers ──────────────────────────────────────────────────────
function lsGet<T>(key: string, fallback: T): T {
  try { const s = localStorage.getItem(key); return s ? JSON.parse(s) as T : fallback; } catch { return fallback; }
}
function lsSet(key: string, val: unknown) {
  try { localStorage.setItem(key, JSON.stringify(val)); } catch { /* ignore */ }
}

// ─── FIFO ──────────────────────────────────────────────────────────────────────
function getSafeTime(d: string): number {
  if (!d) return 0;
  const t = new Date(d.includes("T") ? d : d + "T12:00:00");
  return isNaN(t.getTime()) ? 0 : t.getTime();
}

function calculateFIFO(trades: Trade[], startEquity: number): FIFOResult {
  const sorted = [...trades].sort((a, b) => {
    const ta = getSafeTime(a.date), tb = getSafeTime(b.date);
    if (ta !== tb) return ta - tb;
    if (a.type.toLowerCase() === "buy" && b.type.toLowerCase() !== "buy") return -1;
    if (a.type.toLowerCase() !== "buy" && b.type.toLowerCase() === "buy") return 1;
    return 0;
  });
  const originalIndex = (t: Trade) => trades.indexOf(t);
  const buyQueues: Record<string, Array<{ trade: Trade; remaining: number; origIdx: number }>> = {};
  const closedTrades: ClosedTrade[] = [];
  let currentEquity = startEquity;
  const openLotsDict: Record<string, Trade[]> = {};

  sorted.forEach(trade => {
    const sym = trade.symbol.toUpperCase();
    const qty = Math.abs(Number(trade.qty) || 0);
    if (qty <= 0) return;
    if (trade.type.toLowerCase() === "buy") {
      if (!buyQueues[sym]) buyQueues[sym] = [];
      buyQueues[sym].push({ trade, remaining: qty, origIdx: originalIndex(trade) });
    } else {
      if (!buyQueues[sym]?.length) return;
      let toSell = qty;
      const sellOrigIdx = originalIndex(trade);
      const buyIndices: number[] = [];
      while (toSell > 0 && buyQueues[sym].length > 0) {
        const lot = buyQueues[sym][0];
        const matched = Math.min(lot.remaining, toSell);
        const entryPx = Number(lot.trade.price) || 0;
        const exitPx = Number(trade.price) || 0;
        const pnl = (exitPx - entryPx) * matched;
        const perc = entryPx > 0 ? ((exitPx - entryPx) / entryPx) * 100 : 0;
        const posSizePct = currentEquity > 0 ? (entryPx * matched / currentEquity) * 100 : 0;
        currentEquity += pnl;
        buyIndices.push(lot.origIdx);
        closedTrades.push({
          symbol: sym, qty: matched, entryPx, exitPx,
          entryDate: lot.trade.date, exitDate: trade.date,
          pnl, perc, setupType: lot.trade.setupType || trade.setupType || "",
          tags: [...(lot.trade.tags || [])], remarks: lot.trade.remarks || "",
          img: lot.trade.img, vcp: lot.trade.vcp,
          equitySnapshot: currentEquity, posSizePct,
          sellIndex: sellOrigIdx, buyIndices,
        });
        lot.remaining -= matched; toSell -= matched;
        if (lot.remaining <= 0) buyQueues[sym].shift();
      }
    }
  });

  const openPositions: OpenPosition[] = [];
  Object.entries(buyQueues).forEach(([sym, lots]) => {
    const active = lots.filter(l => l.remaining > 0);
    if (!active.length) return;
    let totalQty = 0, totalInvested = 0;
    const buyIndices: number[] = [], tags: string[] = [];
    let remarks = "", img: string | undefined, setupType: string | undefined;
    active.forEach(l => {
      totalQty += l.remaining; totalInvested += l.remaining * (Number(l.trade.price) || 0);
      buyIndices.push(l.origIdx);
      (l.trade.tags || []).forEach(t => { if (!tags.includes(t)) tags.push(t); });
      if (l.trade.remarks) remarks = l.trade.remarks;
      if (l.trade.img) img = l.trade.img;
      if (l.trade.setupType) setupType = l.trade.setupType;
    });
    openPositions.push({ symbol: sym, qty: totalQty, avgPx: totalQty > 0 ? totalInvested / totalQty : 0, totalInvested, buyIndices, tags, remarks, img, setupType });
    openLotsDict[sym] = active.map(l => l.trade);
  });

  return { closedTrades, openPositions, currentEquity, openLotsDict };
}

// ─── Formatters ────────────────────────────────────────────────────────────────
function fmt(n: number, dec = 2) { return n.toLocaleString("en-IN", { minimumFractionDigits: dec, maximumFractionDigits: dec }); }
function fmtPnl(n: number) { return `${n >= 0 ? "+" : "−"}₹${fmt(Math.abs(n))}`; }
function fmtPerc(n: number) { return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`; }

// ─── Equity Curve ──────────────────────────────────────────────────────────────
type EquityCurvePoint = { x: number; y: number; val: number; date: string; label: string };

function EquityCurve({
  closed,
  startEquity,
  unrealizedTail,
  metric,
  focus,
}: {
  closed: ClosedTrade[];
  startEquity: number;
  unrealizedTail: number;
  metric: "combined" | "realized";
  focus: "all" | "winners" | "losers";
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const [hover, setHover] = useState<{ idx: number; pts: EquityCurvePoint[]; rect: DOMRect } | null>(null);

  const sortedClosed = useMemo(
    () => [...closed].sort((a, b) => getSafeTime(a.exitDate) - getSafeTime(b.exitDate)),
    [closed],
  );

  const points = useMemo<EquityCurvePoint[]>(() => {
    if (!sortedClosed.length) return [];
    let eq = startEquity;
    const pts: EquityCurvePoint[] = [
      { x: 0, y: 0, val: eq, date: sortedClosed[0].entryDate, label: "Start" },
    ];
    sortedClosed.forEach((t) => {
      eq += t.pnl;
      pts.push({ x: 0, y: 0, val: eq, date: t.exitDate, label: t.symbol });
    });
    if (metric === "combined" && Math.abs(unrealizedTail) > 1) {
      const last = pts[pts.length - 1];
      pts.push({ x: 0, y: 0, val: last.val + unrealizedTail, date: last.date, label: "Now (incl. unrealized)" });
    }
    return pts;
  }, [sortedClosed, startEquity, metric, unrealizedTail]);

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current) return;
    const W = containerRef.current.clientWidth || 600;
    const H = 320;
    const pad = { t: 18, r: 20, b: 38, l: 72 };
    const innerW = W - pad.l - pad.r;
    const innerH = H - pad.t - pad.b;

    svgRef.current.setAttribute("viewBox", `0 0 ${W} ${H}`);
    svgRef.current.style.height = `${H}px`;

    if (!points.length) {
      const focusLabel = focus === "all" ? "" : ` (${focus} only)`;
      svgRef.current.innerHTML = `<text x="${W / 2}" y="${H / 2}" fill="var(--text-muted)" font-size="13" text-anchor="middle" dominant-baseline="middle">No closed trades yet${focusLabel}</text>`;
      return;
    }

    const minV = Math.min(...points.map((p) => p.val), startEquity);
    const maxV = Math.max(...points.map((p) => p.val), startEquity);
    const vRange = Math.max(maxV - minV, 1);

    points.forEach((p, i) => {
      p.x = pad.l + (i / Math.max(points.length - 1, 1)) * innerW;
      p.y = pad.t + (1 - (p.val - minV) / vRange) * innerH;
    });

    const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
    const areaPath = `${linePath} L${points[points.length - 1].x.toFixed(1)},${(pad.t + innerH).toFixed(1)} L${pad.l.toFixed(1)},${(pad.t + innerH).toFixed(1)} Z`;

    const yTicks = 5;
    const yTickLines = Array.from({ length: yTicks + 1 }, (_, i) => {
      const frac = i / yTicks;
      const val = minV + frac * vRange;
      const y = pad.t + (1 - frac) * innerH;
      const isStart = Math.abs(val - startEquity) / Math.max(Math.abs(startEquity), 1) < 0.02;
      const label = Math.abs(val) >= 1e7
        ? `₹${(val / 1e7).toFixed(2)}Cr`
        : Math.abs(val) >= 1e5
        ? `₹${(val / 1e5).toFixed(2)}L`
        : `₹${(val / 1000).toFixed(0)}k`;
      return `
        <line x1="${pad.l}" y1="${y.toFixed(1)}" x2="${W - pad.r}" y2="${y.toFixed(1)}" stroke="var(--line)" stroke-width="1" stroke-dasharray="${isStart ? "0" : "3 3"}" opacity="${isStart ? "0.6" : "0.45"}"/>
        <text x="${pad.l - 8}" y="${y.toFixed(1)}" fill="var(--text-muted)" font-size="10.5" text-anchor="end" dominant-baseline="middle">${label}</text>
      `;
    }).join("");

    const xLabelCount = Math.min(points.length, 6);
    const xStep = Math.max(1, Math.floor((points.length - 1) / Math.max(xLabelCount - 1, 1)));
    const xLabels = Array.from({ length: xLabelCount }, (_, i) => {
      const pt = points[Math.min(i * xStep, points.length - 1)];
      const label = pt.date ? pt.date.slice(0, 7) : "";
      return `<text x="${pt.x.toFixed(1)}" y="${(pad.t + innerH + 18).toFixed(1)}" fill="var(--text-muted)" font-size="10" text-anchor="middle">${label}</text>`;
    }).join("");

    const finalVal = points[points.length - 1].val;
    const isPositive = finalVal >= startEquity;
    const stroke = isPositive ? "var(--positive)" : "var(--negative)";
    const peakIdx = points.reduce((bi, p, i) => (p.val > points[bi].val ? i : bi), 0);
    const troughIdx = points.reduce((bi, p, i) => (p.val < points[bi].val ? i : bi), 0);
    const peak = points[peakIdx];
    const trough = points[troughIdx];

    const startY = pad.t + (1 - (startEquity - minV) / vRange) * innerH;

    svgRef.current.innerHTML = `
      <defs>
        <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${stroke}" stop-opacity="0.36"/>
          <stop offset="60%" stop-color="${stroke}" stop-opacity="0.10"/>
          <stop offset="100%" stop-color="${stroke}" stop-opacity="0.0"/>
        </linearGradient>
        <linearGradient id="eqLine" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stop-color="${stroke}" stop-opacity="0.85"/>
          <stop offset="100%" stop-color="${stroke}" stop-opacity="1"/>
        </linearGradient>
        <clipPath id="eqClip">
          <rect x="${pad.l}" y="${pad.t}" width="${innerW}" height="${innerH}"/>
        </clipPath>
        <filter id="eqGlow" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="3" result="blur"/>
          <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
      </defs>
      <rect x="${pad.l}" y="${pad.t}" width="${innerW}" height="${innerH}" fill="none"/>
      ${yTickLines}
      <line x1="${pad.l}" y1="${startY.toFixed(1)}" x2="${(W - pad.r).toFixed(1)}" y2="${startY.toFixed(1)}" stroke="var(--text-muted)" stroke-width="1" stroke-dasharray="2 4" opacity="0.55"/>
      <text x="${(W - pad.r - 4).toFixed(1)}" y="${(startY - 4).toFixed(1)}" fill="var(--text-muted)" font-size="9.5" text-anchor="end">Start ₹${(startEquity / 1000).toFixed(0)}k</text>
      <g clip-path="url(#eqClip)">
        <path d="${areaPath}" fill="url(#eqGrad)"/>
        <path d="${linePath}" fill="none" stroke="url(#eqLine)" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round" filter="url(#eqGlow)"/>
        <circle cx="${peak.x.toFixed(1)}" cy="${peak.y.toFixed(1)}" r="3.5" fill="var(--positive)" stroke="var(--surface)" stroke-width="1.5"/>
        <circle cx="${trough.x.toFixed(1)}" cy="${trough.y.toFixed(1)}" r="3.5" fill="var(--negative)" stroke="var(--surface)" stroke-width="1.5"/>
      </g>
      ${xLabels}
      <line x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${pad.t + innerH}" stroke="var(--line-strong)" stroke-width="1"/>
      <line x1="${pad.l}" y1="${pad.t + innerH}" x2="${W - pad.r}" y2="${pad.t + innerH}" stroke="var(--line-strong)" stroke-width="1"/>
    `;
  }, [points, startEquity, focus]);

  useEffect(() => {
    draw();
    if (!containerRef.current) return;
    const ro = new ResizeObserver(draw);
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, [draw]);

  const handleMove = (event: React.MouseEvent<SVGSVGElement>) => {
    if (!points.length || !svgRef.current) return;
    const rect = svgRef.current.getBoundingClientRect();
    const px = event.clientX - rect.left;
    let bestIdx = 0;
    let bestDist = Infinity;
    points.forEach((p, i) => {
      const screenX = (p.x / (containerRef.current?.clientWidth || rect.width)) * rect.width;
      const dist = Math.abs(screenX - px);
      if (dist < bestDist) {
        bestDist = dist;
        bestIdx = i;
      }
    });
    setHover({ idx: bestIdx, pts: points, rect });
  };
  const handleLeave = () => setHover(null);

  const tooltip = hover && hover.pts[hover.idx]
    ? (() => {
        const p = hover.pts[hover.idx];
        const W = containerRef.current?.clientWidth || hover.rect.width;
        const screenX = (p.x / W) * hover.rect.width;
        const left = Math.max(8, Math.min(screenX - 90, hover.rect.width - 188));
        const delta = p.val - startEquity;
        const deltaPct = startEquity > 0 ? (delta / startEquity) * 100 : 0;
        return (
          <div className="tj-eq-tooltip" style={{ left, top: 8 }}>
            <div className="tj-eq-tooltip-row strong">{p.label}</div>
            <div className="tj-eq-tooltip-row muted">{p.date?.slice(0, 10) || "—"}</div>
            <div className="tj-eq-tooltip-row">Equity {`₹${fmt(p.val, 0)}`}</div>
            <div className={`tj-eq-tooltip-row ${delta >= 0 ? "pos" : "neg"}`}>
              {fmtPnl(delta)} <span className="tj-eq-tooltip-pct">({fmtPerc(deltaPct)})</span>
            </div>
          </div>
        );
      })()
    : null;

  return (
    <div ref={containerRef} className="tj-eq-wrap" style={{ width: "100%", position: "relative" }}>
      <svg
        ref={svgRef}
        className="tj-eq-svg"
        style={{ width: "100%", height: 320, display: "block" }}
        onMouseMove={handleMove}
        onMouseLeave={handleLeave}
      />
      {tooltip}
    </div>
  );
}

// ─── P&L Distribution ─────────────────────────────────────────────────────────
const PNL_BIN_EDGES: number[] = [-100, -25, -15, -10, -5, -2, 0, 2, 5, 10, 15, 25, 100];

function makePnlBins(percs: number[]) {
  const bins: { from: number; to: number; label: string; count: number; mid: number; isWin: boolean }[] = [];
  for (let i = 0; i < PNL_BIN_EDGES.length - 1; i += 1) {
    const from = PNL_BIN_EDGES[i];
    const to = PNL_BIN_EDGES[i + 1];
    const isWin = from >= 0;
    let label: string;
    if (i === 0) label = `<${to}%`;
    else if (i === PNL_BIN_EDGES.length - 2) label = `>${from}%`;
    else label = `${from}→${to}%`;
    bins.push({ from, to, label, count: 0, mid: (from + to) / 2, isWin });
  }
  percs.forEach((p) => {
    let idx = bins.findIndex((b) => p >= b.from && p < b.to);
    if (idx === -1) idx = p >= bins[bins.length - 1].to ? bins.length - 1 : 0;
    bins[idx].count += 1;
  });
  return bins;
}

function PnlDistribution({
  closed,
  focus,
}: {
  closed: ClosedTrade[];
  focus: "all" | "winners" | "losers";
}) {
  const [hovered, setHovered] = useState<number | null>(null);

  if (!closed.length) {
    const focusLabel = focus === "all" ? "" : ` (${focus} only)`;
    return <div className="tj-placeholder">No closed trades yet{focusLabel}</div>;
  }

  const percs = closed.map((c) => c.perc);
  const bins = makePnlBins(percs);
  const maxCount = Math.max(...bins.map((b) => b.count), 1);
  const total = closed.length;
  const winnerCount = closed.filter((c) => c.pnl > 0).length;
  const loserCount = total - winnerCount;
  const avgPerc = percs.reduce((a, b) => a + b, 0) / total;
  const bestPerc = Math.max(...percs);
  const worstPerc = Math.min(...percs);
  const winRate = total > 0 ? (winnerCount / total) * 100 : 0;

  return (
    <div className="tj-dist-wrap">
      <div className="tj-dist-summary">
        <div className="tj-dist-summary-cell">
          <span className="tj-dist-summary-label">Trades</span>
          <strong className="tj-dist-summary-value">{total}</strong>
        </div>
        <div className="tj-dist-summary-cell pos">
          <span className="tj-dist-summary-label">Winners</span>
          <strong className="tj-dist-summary-value">
            {winnerCount} <span className="tj-dist-summary-pct">({total ? Math.round((winnerCount / total) * 100) : 0}%)</span>
          </strong>
        </div>
        <div className="tj-dist-summary-cell neg">
          <span className="tj-dist-summary-label">Losers</span>
          <strong className="tj-dist-summary-value">
            {loserCount} <span className="tj-dist-summary-pct">({total ? Math.round((loserCount / total) * 100) : 0}%)</span>
          </strong>
        </div>
        <div className="tj-dist-summary-cell">
          <span className="tj-dist-summary-label">Win rate</span>
          <strong className="tj-dist-summary-value">{winRate.toFixed(1)}%</strong>
        </div>
        <div className="tj-dist-summary-cell">
          <span className="tj-dist-summary-label">Avg %</span>
          <strong className={`tj-dist-summary-value ${avgPerc >= 0 ? "pos" : "neg"}`}>{fmtPerc(avgPerc)}</strong>
        </div>
        <div className="tj-dist-summary-cell">
          <span className="tj-dist-summary-label">Best / Worst</span>
          <strong className="tj-dist-summary-value">
            <span className="pos">{fmtPerc(bestPerc)}</span> / <span className="neg">{fmtPerc(worstPerc)}</span>
          </strong>
        </div>
      </div>

      <div className="tj-dist-chart-wrap">
        <div className="tj-dist-chart">
          {bins.map((b, i) => {
            const heightPct = b.count > 0 ? (b.count / maxCount) * 100 : 0;
            const sharePct = total > 0 ? (b.count / total) * 100 : 0;
            const active = hovered === i;
            return (
              <div
                key={i}
                className={`tj-dist-col ${active ? "active" : ""}`}
                onMouseEnter={() => setHovered(i)}
                onMouseLeave={() => setHovered((current) => (current === i ? null : current))}
                title={`${b.label} · ${b.count} trade${b.count !== 1 ? "s" : ""} (${sharePct.toFixed(1)}%)`}
              >
                <div className="tj-dist-bar-area">
                  {b.count > 0 && (
                    <div className="tj-dist-bar-top">
                      <span className="tj-dist-count">{b.count}</span>
                      <span className="tj-dist-share">{sharePct.toFixed(0)}%</span>
                    </div>
                  )}
                  <div
                    className={`tj-dist-bar ${b.isWin ? "pos" : "neg"} ${active ? "active" : ""}`}
                    style={{ height: `${heightPct}%` }}
                  />
                </div>
                <div className={`tj-dist-label ${b.isWin ? "pos" : "neg"}`}>{b.label}</div>
              </div>
            );
          })}
        </div>
        <div className="tj-dist-baseline" />
      </div>
    </div>
  );
}

// ─── Last Trades Table ─────────────────────────────────────────────────────────
function LastTradesTable({
  closed,
  onOpenSymbolChart,
}: {
  closed: ClosedTrade[];
  onOpenSymbolChart?: (symbol: string) => void;
}) {
  if (!closed.length) {
    return <div className="tj-empty">No closed trades yet</div>;
  }
  const recent = [...closed]
    .sort((a, b) => getSafeTime(b.exitDate) - getSafeTime(a.exitDate))
    .slice(0, 10);
  // Cumulative computed oldest → newest within the 10-trade window so the
  // top (newest) row shows the running total of the period.
  const cumulativeFor = new Map<ClosedTrade, number>();
  let running = 0;
  [...recent].reverse().forEach((trade) => {
    running += trade.pnl;
    cumulativeFor.set(trade, running);
  });
  const totalPnl = recent.reduce((sum, t) => sum + t.pnl, 0);
  const totalPerc = recent.reduce((sum, t) => sum + t.perc, 0);
  const avgInvested = recent.reduce((sum, t) => sum + t.posSizePct, 0) / recent.length;

  return (
    <div className="tj-recent-wrap">
      <table className="tj-recent-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Symbol</th>
            <th>Exit</th>
            <th className="num">% Invested</th>
            <th className="num">% P&amp;L</th>
            <th className="num">Total P&amp;L</th>
            <th className="num">Cumulative</th>
          </tr>
        </thead>
        <tbody>
          {recent.map((trade, idx) => {
            const cum = cumulativeFor.get(trade) ?? 0;
            return (
              <tr key={`${trade.symbol}-${trade.exitDate}-${idx}`}>
                <td className="muted">{idx + 1}</td>
                <td>
                  <button
                    type="button"
                    className="tj-symbol-link tj-symbol-link-inline"
                    onClick={() => onOpenSymbolChart?.(trade.symbol)}
                    title="Open big chart"
                  >
                    {trade.symbol}
                  </button>
                </td>
                <td className="muted">{trade.exitDate?.slice(0, 10) || "—"}</td>
                <td className="num">{trade.posSizePct.toFixed(1)}%</td>
                <td className={`num ${trade.perc >= 0 ? "pos" : "neg"}`}>{fmtPerc(trade.perc)}</td>
                <td className={`num ${trade.pnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(trade.pnl)}</td>
                <td className={`num ${cum >= 0 ? "pos" : "neg"}`}>{fmtPnl(cum)}</td>
              </tr>
            );
          })}
        </tbody>
        <tfoot>
          <tr className="tj-recent-total-row">
            <td colSpan={3}>Total · {recent.length} trade{recent.length !== 1 ? "s" : ""}</td>
            <td className="num muted">{avgInvested.toFixed(1)}% avg</td>
            <td className={`num ${totalPerc >= 0 ? "pos" : "neg"}`}>{fmtPerc(totalPerc)}</td>
            <td className={`num ${totalPnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(totalPnl)}</td>
            <td className={`num ${totalPnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(totalPnl)}</td>
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

// ─── Monthly P&L Calendar ──────────────────────────────────────────────────────
function MonthlyCalendar({
  closed,
  onOpenSymbolChart,
}: {
  closed: ClosedTrade[];
  onOpenSymbolChart?: (symbol: string) => void;
}) {
  const { byDay, tradesByDay } = useMemo(() => {
    const totals: Record<string, number> = {};
    const trades: Record<string, ClosedTrade[]> = {};
    closed.forEach(c => {
      const d = (c.exitDate || "").split("T")[0];
      if (!d) return;
      totals[d] = (totals[d] || 0) + c.pnl;
      (trades[d] ||= []).push(c);
    });
    return { byDay: totals, tradesByDay: trades };
  }, [closed]);

  const today = new Date();
  const [view, setView] = useState(() => new Date(today.getFullYear(), today.getMonth(), 1));
  const [selectedDay, setSelectedDay] = useState<string | null>(null);

  const year = view.getFullYear();
  const month = view.getMonth();
  const monthLabel = view.toLocaleString("default", { month: "short", year: "numeric" });
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const firstDow = new Date(year, month, 1).getDay();
  const daysInPrev = new Date(year, month, 0).getDate();

  // Build 6-week grid (42 cells)
  const cells: { date: Date; inMonth: boolean }[] = [];
  for (let i = 0; i < firstDow; i++) {
    const d = new Date(year, month - 1, daysInPrev - firstDow + 1 + i);
    cells.push({ date: d, inMonth: false });
  }
  for (let d = 1; d <= daysInMonth; d++) {
    cells.push({ date: new Date(year, month, d), inMonth: true });
  }
  while (cells.length < 42) {
    const last = cells[cells.length - 1].date;
    cells.push({ date: new Date(last.getFullYear(), last.getMonth(), last.getDate() + 1), inMonth: false });
  }

  function dayKey(d: Date) {
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }

  // Monthly P&L total
  const monthlyPnl = useMemo(() => {
    let sum = 0;
    for (let d = 1; d <= daysInMonth; d++) {
      const k = `${year}-${String(month + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
      if (byDay[k] !== undefined) sum += byDay[k];
    }
    return sum;
  }, [byDay, year, month, daysInMonth]);

  const todayKey = dayKey(today);
  const DOW = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

  function shift(delta: number) {
    setView(new Date(year, month + delta, 1));
  }
  function goToday() {
    setView(new Date(today.getFullYear(), today.getMonth(), 1));
  }

  return (
    <div className="tj-cal-root">
      <div className="tj-cal-toolbar">
        <button type="button" className="tj-cal-nav" onClick={() => shift(-1)} aria-label="Previous month">‹</button>
        <div className="tj-cal-title-wrap">
          <div className="tj-cal-title">{monthLabel}</div>
          <div className={`tj-cal-subtotal ${monthlyPnl >= 0 ? "pos" : "neg"}`}>
            Monthly P/L: {monthlyPnl >= 0 ? "₹" : "−₹"}{fmt(Math.abs(monthlyPnl), 0)}
          </div>
        </div>
        <button type="button" className="tj-cal-nav" onClick={() => shift(1)} aria-label="Next month">›</button>
        <button type="button" className="tj-cal-today" onClick={goToday}>Today</button>
      </div>
      <div className="tj-cal-dow">
        {DOW.map(d => <div key={d} className="tj-cal-dow-cell">{d}</div>)}
      </div>
      <div className="tj-cal-grid">
        {cells.map(({ date, inMonth }, idx) => {
          const k = dayKey(date);
          const pnl = byDay[k];
          const isWeekend = date.getDay() === 0 || date.getDay() === 6;
          const isToday = k === todayKey;
          const hasTrades = inMonth && pnl !== undefined;
          const cls = [
            "tj-cal-cell",
            !inMonth ? "out" : "",
            isToday ? "today" : "",
            hasTrades ? (pnl >= 0 ? "pos" : "neg") : "",
            hasTrades ? "clickable" : "",
          ].filter(Boolean).join(" ");
          const baseProps = {
            className: cls,
            title: hasTrades ? `${k}: ${fmtPnl(pnl)} · click for details` : k,
          };
          const inner = (
            <>
              <span className="tj-cal-day">{date.getDate()}</span>
              {hasTrades && (
                <span className="tj-cal-amt">
                  {pnl >= 0 ? "₹" : "−₹"}{fmt(Math.abs(pnl), 0)}
                </span>
              )}
              {inMonth && pnl === undefined && !isWeekend && (
                <span className="tj-cal-empty">No Trades</span>
              )}
            </>
          );
          return hasTrades ? (
            <button key={idx} type="button" {...baseProps} onClick={() => setSelectedDay(k)}>
              {inner}
            </button>
          ) : (
            <div key={idx} {...baseProps}>{inner}</div>
          );
        })}
      </div>
      {selectedDay && (
        <CalendarDayModal
          dayKey={selectedDay}
          trades={tradesByDay[selectedDay] || []}
          onClose={() => setSelectedDay(null)}
          onOpenSymbolChart={onOpenSymbolChart}
        />
      )}
    </div>
  );
}

function CalendarDayModal({
  dayKey,
  trades,
  onClose,
  onOpenSymbolChart,
}: {
  dayKey: string;
  trades: ClosedTrade[];
  onClose: () => void;
  onOpenSymbolChart?: (symbol: string) => void;
}) {
  useEffect(() => {
    function onKey(e: KeyboardEvent) { if (e.key === "Escape") onClose(); }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);
  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.length - wins;
  const dateLabel = new Date(dayKey + "T00:00:00").toLocaleDateString("en-IN", {
    weekday: "long", day: "numeric", month: "short", year: "numeric",
  });

  return (
    <div className="tj-overlay" onClick={onClose}>
      <div className="tj-modal tj-cal-modal" onClick={e => e.stopPropagation()}>
        <button type="button" className="tj-modal-x" onClick={onClose} aria-label="Close">×</button>
        <div className="tj-modal-title">{dateLabel}</div>
        <div className="tj-cal-modal-summary">
          <div>
            <span className="tj-cal-modal-stat-label">Day P&L</span>
            <span className={`tj-cal-modal-stat-val ${totalPnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(totalPnl)}</span>
          </div>
          <div>
            <span className="tj-cal-modal-stat-label">Trades</span>
            <span className="tj-cal-modal-stat-val">{trades.length}</span>
          </div>
          <div>
            <span className="tj-cal-modal-stat-label">W / L</span>
            <span className="tj-cal-modal-stat-val">
              <span className="pos">{wins}</span> / <span className="neg">{losses}</span>
            </span>
          </div>
        </div>
        {trades.length === 0 ? (
          <div className="tj-empty">No trades</div>
        ) : (
          <div className="tj-cal-modal-table-wrap">
            <table className="tj-recent-table">
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th className="num">Qty</th>
                  <th className="num">Entry</th>
                  <th className="num">Exit</th>
                  <th className="num">% P&amp;L</th>
                  <th className="num">P&amp;L</th>
                </tr>
              </thead>
              <tbody>
                {trades.map((t, i) => (
                  <tr key={`${t.symbol}-${t.exitDate}-${i}`}>
                    <td>
                      <button
                        type="button"
                        className="tj-symbol-link tj-symbol-link-inline"
                        onClick={() => { onOpenSymbolChart?.(t.symbol); onClose(); }}
                        title="Open big chart"
                      >
                        {t.symbol}
                      </button>
                      {t.setupType && <div className="tj-cal-modal-sub">{t.setupType}</div>}
                    </td>
                    <td className="num">{t.qty}</td>
                    <td className="num">{fmt(t.entryPx)}</td>
                    <td className="num">{fmt(t.exitPx)}</td>
                    <td className={`num ${t.perc >= 0 ? "pos" : "neg"}`}>{fmtPerc(t.perc)}</td>
                    <td className={`num ${t.pnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(t.pnl)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Win / Loss Donut ──────────────────────────────────────────────────────────
function WinLossDonut({ winners, losers }: { winners: number; losers: number }) {
  const total = winners + losers;
  const wins = total > 0 ? winners / total : 0;
  const winRate = total > 0 ? Math.round(wins * 100) : 0;
  // SVG donut math
  const r = 56;
  const c = 2 * Math.PI * r;
  const winLen = c * wins;
  const lossLen = c * (1 - wins);
  return (
    <div className="tj-donut-wrap">
      <svg viewBox="0 0 160 160" className="tj-donut-svg" aria-hidden>
        <defs>
          <linearGradient id="tjWinG" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="var(--positive)" stopOpacity="0.95" />
            <stop offset="100%" stopColor="var(--positive)" stopOpacity="0.65" />
          </linearGradient>
          <linearGradient id="tjLossG" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="var(--negative)" stopOpacity="0.95" />
            <stop offset="100%" stopColor="var(--negative)" stopOpacity="0.6" />
          </linearGradient>
        </defs>
        <circle cx="80" cy="80" r={r} fill="none" stroke="var(--line)" strokeWidth="14" />
        {total > 0 ? (
          <>
            <circle
              cx="80" cy="80" r={r} fill="none"
              stroke="url(#tjLossG)" strokeWidth="14"
              strokeDasharray={`${lossLen} ${c}`}
              strokeDashoffset={-winLen}
              transform="rotate(-90 80 80)"
              strokeLinecap="butt"
            />
            <circle
              cx="80" cy="80" r={r} fill="none"
              stroke="url(#tjWinG)" strokeWidth="14"
              strokeDasharray={`${winLen} ${c}`}
              transform="rotate(-90 80 80)"
              strokeLinecap="butt"
            />
          </>
        ) : null}
      </svg>
      <div className="tj-donut-center">
        <span className="tj-donut-value">{winRate}%</span>
        <span className="tj-donut-label">Win rate</span>
      </div>
      <div className="tj-donut-legend">
        <span className="tj-donut-leg pos"><span className="tj-donut-swatch pos" />Winners <strong>{winners}</strong></span>
        <span className="tj-donut-leg neg"><span className="tj-donut-swatch neg" />Losers <strong>{losers}</strong></span>
      </div>
    </div>
  );
}

// ─── Setup Breakdown ───────────────────────────────────────────────────────────
function SetupBreakdown({ closed }: { closed: ClosedTrade[] }) {
  if (!closed.length) return <div className="tj-placeholder">No closed trades yet</div>;
  const map: Record<string, { count: number; pnl: number; wins: number }> = {};
  closed.forEach(t => {
    const key = (t.setupType || "Unspecified").trim() || "Unspecified";
    const row = map[key] || { count: 0, pnl: 0, wins: 0 };
    row.count += 1; row.pnl += t.pnl; if (t.pnl > 0) row.wins += 1;
    map[key] = row;
  });
  const rows = Object.entries(map)
    .map(([setup, v]) => ({ setup, ...v, winRate: v.count > 0 ? (v.wins / v.count) * 100 : 0 }))
    .sort((a, b) => b.pnl - a.pnl);
  const maxAbs = Math.max(1, ...rows.map(r => Math.abs(r.pnl)));
  return (
    <div className="tj-setup-list">
      {rows.map(r => {
        const pct = (Math.abs(r.pnl) / maxAbs) * 100;
        const positive = r.pnl >= 0;
        return (
          <div key={r.setup} className="tj-setup-row">
            <div className="tj-setup-meta">
              <span className="tj-setup-name">{r.setup}</span>
              <span className="tj-setup-stats">{r.count} trades · WR {r.winRate.toFixed(0)}%</span>
            </div>
            <div className="tj-setup-bar-track">
              <div
                className={`tj-setup-bar ${positive ? "pos" : "neg"}`}
                style={{ width: `${pct}%` }}
              />
            </div>
            <span className={`tj-setup-pnl ${positive ? "pos" : "neg"}`}>{fmtPnl(r.pnl)}</span>
          </div>
        );
      })}
    </div>
  );
}

// ─── Hold-time Histogram ──────────────────────────────────────────────────────
function HoldTimeHistogram({ closed }: { closed: ClosedTrade[] }) {
  if (!closed.length) return <div className="tj-placeholder">No closed trades yet</div>;
  const buckets: Array<{ label: string; max: number; wins: number; losses: number; pnl: number }> = [
    { label: "1d", max: 1, wins: 0, losses: 0, pnl: 0 },
    { label: "2-5d", max: 5, wins: 0, losses: 0, pnl: 0 },
    { label: "6-15d", max: 15, wins: 0, losses: 0, pnl: 0 },
    { label: "16-30d", max: 30, wins: 0, losses: 0, pnl: 0 },
    { label: "1-3m", max: 90, wins: 0, losses: 0, pnl: 0 },
    { label: ">3m", max: Infinity, wins: 0, losses: 0, pnl: 0 },
  ];
  closed.forEach(t => {
    const a = getSafeTime(t.entryDate);
    const b = getSafeTime(t.exitDate);
    if (!a || !b) return;
    const days = Math.max(0, Math.round((b - a) / (1000 * 60 * 60 * 24)));
    const idx = buckets.findIndex(bk => days <= bk.max);
    const bucket = idx >= 0 ? buckets[idx] : buckets[buckets.length - 1];
    bucket.pnl += t.pnl;
    if (t.pnl >= 0) bucket.wins += 1; else bucket.losses += 1;
  });
  const maxCount = Math.max(1, ...buckets.map(b => b.wins + b.losses));
  return (
    <div className="tj-hold-grid">
      {buckets.map(b => {
        const total = b.wins + b.losses;
        const heightPct = (total / maxCount) * 100;
        const winShare = total > 0 ? (b.wins / total) * 100 : 0;
        return (
          <div key={b.label} className="tj-hold-col" title={`${b.label}: ${total} trades · ${fmtPnl(b.pnl)}`}>
            <div className="tj-hold-bar-area">
              <div className="tj-hold-bar" style={{ height: `${heightPct}%` }}>
                <span className="tj-hold-bar-pos" style={{ height: `${winShare}%` }} />
              </div>
              {total > 0 ? <span className="tj-hold-count">{total}</span> : null}
            </div>
            <span className="tj-hold-label">{b.label}</span>
            <span className={`tj-hold-pnl ${b.pnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(b.pnl)}</span>
          </div>
        );
      })}
    </div>
  );
}

// ─── Day-of-week winrate ──────────────────────────────────────────────────────
function DayOfWeekStats({ closed }: { closed: ClosedTrade[] }) {
  if (!closed.length) return <div className="tj-placeholder">No closed trades yet</div>;
  const labels = ["Mon", "Tue", "Wed", "Thu", "Fri"];
  const stats = labels.map(label => ({ label, wins: 0, losses: 0, pnl: 0 }));
  closed.forEach(t => {
    const ts = getSafeTime(t.exitDate);
    if (!ts) return;
    const dow = new Date(ts).getDay();
    if (dow < 1 || dow > 5) return;
    const slot = stats[dow - 1];
    slot.pnl += t.pnl;
    if (t.pnl >= 0) slot.wins += 1; else slot.losses += 1;
  });
  return (
    <div className="tj-dow-grid">
      {stats.map(s => {
        const total = s.wins + s.losses;
        const wr = total > 0 ? (s.wins / total) * 100 : 0;
        return (
          <div key={s.label} className="tj-dow-cell">
            <span className="tj-dow-label">{s.label}</span>
            <div className="tj-dow-ring" style={{ background: `conic-gradient(var(--positive) ${wr}%, var(--negative) ${wr}% ${total > 0 ? 100 : 0}%, var(--line) ${total > 0 ? 100 : 0}% 100%)` }}>
              <span className="tj-dow-ring-inner">{total > 0 ? `${wr.toFixed(0)}%` : "—"}</span>
            </div>
            <span className={`tj-dow-pnl ${s.pnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(s.pnl)}</span>
            <span className="tj-dow-meta">{total} trades</span>
          </div>
        );
      })}
    </div>
  );
}

// ─── Main Component ────────────────────────────────────────────────────────────
export function TradeJournalPanel({ market, addRequest, onAddRequestHandled, onOpenSymbolChart }: TradeJournalPanelProps) {
  const [activeTab, setActiveTab] = useState(0);
  const [trades, setTrades] = useState<Trade[]>(() => lsGet<Trade[]>(LS_DATA, []));
  const [startEquity, setStartEquity] = useState<number>(() => lsGet<number>(LS_EQUITY, 100000));
  const [setups, setSetups] = useState<string[]>(() => lsGet<string[]>(LS_SETUPS, DEFAULT_SETUPS));
  const [openPosCats, setOpenPosCats] = useState<Record<string, OpenPosCat>>(() => lsGet(LS_POSITIONS, {}));
  const [posMeta, setPosMeta] = useState<Record<string, PosMeta>>(() => lsGet(LS_META, {}));
  const [syncing, setSyncing] = useState(false);
  const [syncStatus, setSyncStatus] = useState<string | null>(null);
  const [newsModalOpen, setNewsModalOpen] = useState(false);
  const [backendSyncing, setBackendSyncing] = useState(false);
  const [equityInput, setEquityInput] = useState(String(startEquity));

  // Smart Entry
  const [entrySymbol, setEntrySymbol] = useState("");
  const [entryType, setEntryType] = useState("Buy");
  const [entryQty, setEntryQty] = useState("");
  const [entryPrice, setEntryPrice] = useState("");
  const [fetchingEntryPrice, setFetchingEntryPrice] = useState(false);
  const [entryDate, setEntryDate] = useState(() => new Date().toISOString().split("T")[0]);
  const [entrySetup, setEntrySetup] = useState(DEFAULT_SETUPS[0]);
  const [entrySL, setEntrySL] = useState("");
  const [entryTarget, setEntryTarget] = useState("");
  const [entryImg, setEntryImg] = useState("");
  const [entryRemarks, setEntryRemarks] = useState("");
  const [entryTags, setEntryTags] = useState<Set<string>>(new Set());
  const [customTagInput, setCustomTagInput] = useState("");
  const [vcpT, setVcpT] = useState("");
  const [vcpDepth, setVcpDepth] = useState("");
  const [vcpVol, setVcpVol] = useState("");
  const [checkboxes, setCheckboxes] = useState<boolean[]>(Array(6).fill(false));

  // Calculator
  const [calcCap, setCalcCap] = useState(String(startEquity));
  const [calcRisk, setCalcRisk] = useState("1");
  const [calcEntry, setCalcEntry] = useState("");
  const [calcStop, setCalcStop] = useState("");
  const [calcQtyRes, setCalcQtyRes] = useState("");

  // Position sizer
  const [sizerEquity, setSizerEquity] = useState(String(startEquity));
  const [sizerRiskPct, setSizerRiskPct] = useState("1");
  const [sizerEntry, setSizerEntry] = useState("");
  const [sizerSLPct, setSizerSLPct] = useState("2");
  const [sizerResultQty, setSizerResultQty] = useState(0);
  const [sizerResultSL, setSizerResultSL] = useState(0);
  const [sizerResultRisk, setSizerResultRisk] = useState(0);
  const [sizerResultPos, setSizerResultPos] = useState(0);

  // Filters
  const [filterMonth, setFilterMonth] = useState("all");
  const [filterOutcome, setFilterOutcome] = useState("all");
  const [filterSymbol, setFilterSymbol] = useState("");
  const [dashboardMetric, setDashboardMetric] = useState<"combined" | "realized">("combined");
  const [dashboardFocus, setDashboardFocus] = useState<"all" | "winners" | "losers">("all");

  // Modals
  type ModalState =
    | null
    | { type: "close-pos"; symbol: string; maxQty: number; cmp: number }
    | { type: "edit-closed"; sellIndex: number; buyIndices: number[] }
    | { type: "edit-open"; symbol: string }
    | { type: "edit-sl"; symbol: string }
    | { type: "add-setup"; }
    | { type: "add-from-screener"; symbol: string; suggestedPrice?: number };

  const [modal, setModal] = useState<ModalState>(null);
  const [modalClosePrice, setModalClosePrice] = useState("");
  const [modalCloseQty, setModalCloseQty] = useState("");
  const [modalCloseDate, setModalCloseDate] = useState(() => new Date().toISOString().split("T")[0]);
  const [modalEditEntryPx, setModalEditEntryPx] = useState("");
  const [modalEditExitPx, setModalEditExitPx] = useState("");
  const [modalEditTags, setModalEditTags] = useState<Set<string>>(new Set());
  const [modalEditRemarks, setModalEditRemarks] = useState("");
  const [modalEditImg, setModalEditImg] = useState("");
  const [modalEditCustomTags, setModalEditCustomTags] = useState("");
  const [modalOpenSL, setModalOpenSL] = useState("");
  const [modalOpenFetchTicker, setModalOpenFetchTicker] = useState("");
  const [modalOpenSetupType, setModalOpenSetupType] = useState("");
  const [newSetupName, setNewSetupName] = useState("");

  // Add-from-screener state
  const [screenerQty, setScreenerQty] = useState("");
  const [screenerPrice, setScreenerPrice] = useState("");
  const [screenerSL, setScreenerSL] = useState("");
  const [screenerDate, setScreenerDate] = useState(() => new Date().toISOString().split("T")[0]);
  const [screenerSetup, setScreenerSetup] = useState(DEFAULT_SETUPS[0]);

  const dragSymbol = useRef<string | null>(null);

  // ── Persist & sync ──────────────────────────────────────────────────────────
  const buildPayload = useCallback((
    t: Trade[], se: number, su: string[], op: Record<string, OpenPosCat>, pm: Record<string, PosMeta>
  ) => ({ trades: t, startEquity: se, setups: su, openPosCats: op, posMeta: pm }), []);

  const syncToBackend = useCallback(async (
    t: Trade[], se: number, su: string[], op: Record<string, OpenPosCat>, pm: Record<string, PosMeta>
  ) => {
    try {
      setBackendSyncing(true);
      await saveJournalData(buildPayload(t, se, su, op, pm) as Record<string, unknown>);
    } catch { /* ignore backend errors, localStorage is source of truth */ }
    finally { setBackendSyncing(false); }
  }, [buildPayload]);

  const saveTrades = useCallback((next: Trade[]) => {
    setTrades(next); lsSet(LS_DATA, next); notifyJournalUpdated();
    syncToBackend(next, startEquity, setups, openPosCats, posMeta);
  }, [startEquity, setups, openPosCats, posMeta, syncToBackend]);

  // Load from backend on mount, merge with localStorage
  useEffect(() => {
    getJournalData().then(remote => {
      if (!remote || typeof remote !== "object" || Object.keys(remote).length === 0) return;
      const r = remote as Record<string, unknown>;
      // Only restore if localStorage is empty (first load on new device)
      const localTrades = lsGet<Trade[]>(LS_DATA, []);
      if (localTrades.length === 0 && Array.isArray(r.trades) && (r.trades as Trade[]).length > 0) {
        const rt = r.trades as Trade[];
        setTrades(rt); lsSet(LS_DATA, rt); notifyJournalUpdated();
      }
      if (!localTrades.length && typeof r.startEquity === "number" && r.startEquity > 0) {
        setStartEquity(r.startEquity); setEquityInput(String(r.startEquity));
        setSizerEquity(String(r.startEquity)); setCalcCap(String(r.startEquity));
        lsSet(LS_EQUITY, r.startEquity);
      }
      if (!localTrades.length && Array.isArray(r.setups) && (r.setups as string[]).length > 0) {
        setSetups(r.setups as string[]); lsSet(LS_SETUPS, r.setups);
      }
      if (!localTrades.length && r.openPosCats && typeof r.openPosCats === "object") {
        setOpenPosCats(r.openPosCats as Record<string, OpenPosCat>); lsSet(LS_POSITIONS, r.openPosCats);
      }
      if (!localTrades.length && r.posMeta && typeof r.posMeta === "object") {
        setPosMeta(r.posMeta as Record<string, PosMeta>); lsSet(LS_META, r.posMeta);
      }
    }).catch(() => {});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Handle add from screener ──────────────────────────────────────────────
  useEffect(() => {
    if (!addRequest) return;
    setScreenerQty("");
    setScreenerPrice(String(addRequest.suggestedPrice || ""));
    setScreenerSL("");
    setScreenerDate(new Date().toISOString().split("T")[0]);
    setScreenerSetup(setups[0] || DEFAULT_SETUPS[0]);
    setModal({ type: "add-from-screener", symbol: addRequest.symbol, suggestedPrice: addRequest.suggestedPrice });
  }, [addRequest, setups]);

  // ── Position sizer reactive calc ─────────────────────────────────────────
  useEffect(() => {
    const eq = parseFloat(sizerEquity) || 0, rp = parseFloat(sizerRiskPct) || 0;
    const en = parseFloat(sizerEntry) || 0, sp = parseFloat(sizerSLPct) || 0;
    if (eq > 0 && rp > 0 && en > 0 && sp > 0) {
      const riskAmt = eq * (rp / 100), slPx = en - en * (sp / 100), rps = en - slPx;
      if (rps > 0) {
        const qty = Math.floor(riskAmt / rps);
        setSizerResultQty(qty); setSizerResultSL(slPx); setSizerResultRisk(riskAmt); setSizerResultPos(qty * en); return;
      }
    }
    setSizerResultQty(0); setSizerResultSL(0); setSizerResultRisk(0); setSizerResultPos(0);
  }, [sizerEquity, sizerRiskPct, sizerEntry, sizerSLPct]);

  // ── Auto price sync: on mount + when open-positions tab is active ─────────
  const autoSyncRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const retryRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Stable ref so setInterval always calls the latest version of syncPricesSilent
  const syncPricesSilentRef = useRef<() => Promise<void>>(async () => {});
  // Sync once on mount so CMP is fresh even before the user visits the tab
  useEffect(() => {
    syncPricesSilentRef.current();
  }, []);
  useEffect(() => {
    if (activeTab === 2) {
      // Sync immediately on tab entry, then every 5 min
      syncPricesSilentRef.current();
      autoSyncRef.current = setInterval(() => syncPricesSilentRef.current(), 5 * 60 * 1000);
    } else {
      if (autoSyncRef.current) clearInterval(autoSyncRef.current);
    }
    return () => { if (autoSyncRef.current) clearInterval(autoSyncRef.current); };
  }, [activeTab]);

  // ── FIFO ─────────────────────────────────────────────────────────────────
  const fifo = calculateFIFO(trades, startEquity);
  const { closedTrades, openPositions } = fifo;

  // ── Dashboard stats ───────────────────────────────────────────────────────
  const totalPnl = closedTrades.reduce((s, t) => s + t.pnl, 0);
  const winners = closedTrades.filter(t => t.pnl > 0);
  const losers = closedTrades.filter(t => t.pnl < 0);
  const winRate = closedTrades.length > 0 ? (winners.length / closedTrades.length) * 100 : 0;
  const totalInvested = openPositions.reduce((s, p) => s + p.totalInvested, 0);
  const avgPosSize = closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + t.posSizePct, 0) / closedTrades.length : 0;
  const top10Win = [...winners].sort((a, b) => b.perc - a.perc).slice(0, 10);
  const top10Loss = [...losers].sort((a, b) => a.perc - b.perc).slice(0, 10);

  // ── Filtered closed trades ────────────────────────────────────────────────
  const monthOptions = Array.from(new Set(closedTrades.map(t => t.exitDate?.slice(0, 7)).filter(Boolean))).sort().reverse();
  const filteredClosed = closedTrades.filter(t => {
    if (filterOutcome === "win" && t.pnl <= 0) return false;
    if (filterOutcome === "loss" && t.pnl >= 0) return false;
    if (filterSymbol && !t.symbol.toLowerCase().includes(filterSymbol.toLowerCase())) return false;
    if (filterMonth !== "all") {
      const [fy, fm] = filterMonth.split("-").map(Number);
      const ex = new Date(t.exitDate);
      if (ex.getFullYear() !== fy || ex.getMonth() + 1 !== fm) return false;
    }
    return true;
  });

  // ── Insights ─────────────────────────────────────────────────────────────
  const setupMap: Record<string, { wins: number; losses: number; pnl: number }> = {};
  closedTrades.forEach(t => {
    const s = t.setupType || "Unknown";
    if (!setupMap[s]) setupMap[s] = { wins: 0, losses: 0, pnl: 0 };
    if (t.pnl > 0) setupMap[s].wins++; else setupMap[s].losses++;
    setupMap[s].pnl += t.pnl;
  });
  const tagMap: Record<string, { closedCount: number; openCount: number; realizedPnl: number; unrealizedPnl: number }> = {};
  closedTrades.forEach(t => (t.tags || []).forEach(tag => {
    if (!tagMap[tag]) tagMap[tag] = { closedCount: 0, openCount: 0, realizedPnl: 0, unrealizedPnl: 0 };
    tagMap[tag].closedCount++; tagMap[tag].realizedPnl += t.pnl;
  }));
  openPositions.forEach(p => {
    const cmp = posMeta[p.symbol]?.cmp || p.avgPx;
    const uPnl = (cmp - p.avgPx) * p.qty;
    (p.tags || []).forEach(tag => {
      if (!tagMap[tag]) tagMap[tag] = { closedCount: 0, openCount: 0, realizedPnl: 0, unrealizedPnl: 0 };
      tagMap[tag].openCount++; tagMap[tag].unrealizedPnl += uPnl;
    });
  });
  const allHolds = closedTrades.map(t => Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000));
  const avgHoldAll = allHolds.length ? allHolds.reduce((a, b) => a + b, 0) / allHolds.length : 0;
  const winHolds = closedTrades.filter(t => t.pnl > 0).map(t => Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000));
  const avgHoldWin = winHolds.length ? winHolds.reduce((a, b) => a + b, 0) / winHolds.length : 0;
  const lossHolds = closedTrades.filter(t => t.pnl <= 0).map(t => Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000));
  const avgHoldLoss = lossHolds.length ? lossHolds.reduce((a, b) => a + b, 0) / lossHolds.length : 0;

  // ── Price sync (silent = no alert) ────────────────────────────────────────
  async function syncPricesSilent() {
    if (!openPositions.length) return;
    const updated = { ...posMeta };
    const mkt: MarketKey = market ?? "india";
    let anyUpdated = false;
    for (const pos of openPositions) {
      try {
        // Use fetchTicker override if set (e.g. LAURUS → LAURUSLABS); fall back to pos.symbol
        const ticker = posMeta[pos.symbol]?.fetchTicker || pos.symbol;
        const result = await getChart(ticker, "1D", mkt);
        const price = result.summary?.last_price ?? result.bars[result.bars.length - 1]?.close ?? null;
        const prevClose = result.bars[result.bars.length - 2]?.close ?? null;
        if (price && isFinite(price)) {
          if (!updated[pos.symbol]) updated[pos.symbol] = {};
          updated[pos.symbol].cmp = price;
          if (prevClose && isFinite(prevClose)) updated[pos.symbol].prev_close = prevClose;
          anyUpdated = true;
        }
      } catch { /* ignore */ }
      await new Promise(r => setTimeout(r, 100));
    }
    setPosMeta(updated); lsSet(LS_META, updated);
    // If backend was sleeping (all fetches failed), retry once after 45s for cold-start
    if (!anyUpdated && retryRef.current === null) {
      retryRef.current = setTimeout(() => {
        retryRef.current = null;
        syncPricesSilentRef.current();
      }, 45_000);
    }
  }
  // Keep ref in sync with the latest closure so setInterval/useEffect use fresh state
  syncPricesSilentRef.current = syncPricesSilent;

  async function syncPrices() {
    if (!openPositions.length) { alert("No open positions."); return; }
    setSyncing(true); setSyncStatus("Syncing prices…");
    const updated = { ...posMeta };
    const failed: string[] = [];
    let updatedCount = 0;
    const mkt: MarketKey = market ?? "india";
    for (const pos of openPositions) {
      try {
        const result = await getChart(pos.symbol, "1D", mkt);
        const price = result.summary?.last_price ?? result.bars[result.bars.length - 1]?.close ?? null;
        const prevClose = result.bars[result.bars.length - 2]?.close ?? null;
        if (price && isFinite(price)) {
          if (!updated[pos.symbol]) updated[pos.symbol] = {};
          updated[pos.symbol].cmp = price;
          if (prevClose && isFinite(prevClose)) updated[pos.symbol].prev_close = prevClose;
          updatedCount++;
        } else {
          failed.push(pos.symbol);
        }
      } catch { failed.push(pos.symbol); }
      await new Promise(r => setTimeout(r, 100));
    }
    setPosMeta(updated); lsSet(LS_META, updated);
    setSyncing(false);
    setSyncStatus(failed.length > 0 ? `Synced ${updatedCount}/${openPositions.length} · Failed: ${failed.join(", ")}` : `All ${updatedCount} prices synced ✓`);
    setTimeout(() => setSyncStatus(null), 5000);
  }

  // ── Export / Import ───────────────────────────────────────────────────────
  function exportJSON() {
    const payload = buildPayload(trades, startEquity, setups, openPosCats, posMeta);
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
    a.download = `TradeJournal_${new Date().toISOString().slice(0, 10)}.json`; a.click();
  }

  function importJSON(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]; if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      try {
        const data = JSON.parse(ev.target?.result as string);
        function unwrap<T>(val: unknown, fallback: T): T {
          if (val === undefined || val === null) return fallback;
          if (typeof val === "string") { try { return JSON.parse(val) as T; } catch { return fallback; } }
          return val as T;
        }
        const isOriginalFormat = "tradingJournalData" in data;
        let importedTrades: Trade[], importedEquity: number, importedSetups: string[];
        let importedPositions: Record<string, OpenPosCat>, importedMeta: Record<string, PosMeta>;
        if (isOriginalFormat) {
          importedTrades = unwrap<Trade[]>(data.tradingJournalData, []);
          importedEquity = unwrap<number>(data.tradingJournalEquity, 100000);
          importedSetups = unwrap<string[]>(data.tradingJournalSetups, DEFAULT_SETUPS);
          importedPositions = unwrap<Record<string, OpenPosCat>>(data.tradingJournalPositions, {});
          importedMeta = unwrap<Record<string, PosMeta>>(data.tradingJournalPosMeta, {});
        } else {
          importedTrades = unwrap<Trade[]>(data.trades, []);
          importedEquity = unwrap<number>(data.startEquity, 100000);
          importedSetups = unwrap<string[]>(data.setups, DEFAULT_SETUPS);
          importedPositions = unwrap<Record<string, OpenPosCat>>(data.openPosCats, {});
          importedMeta = unwrap<Record<string, PosMeta>>(data.posMeta, {});
        }
        if (Array.isArray(importedTrades) && importedTrades.length > 0) { saveTrades(importedTrades); }
        if (importedEquity > 0) { setStartEquity(importedEquity); lsSet(LS_EQUITY, importedEquity); setEquityInput(String(importedEquity)); setSizerEquity(String(importedEquity)); setCalcCap(String(importedEquity)); }
        if (Array.isArray(importedSetups) && importedSetups.length > 0) { setSetups(importedSetups); lsSet(LS_SETUPS, importedSetups); }
        if (Object.keys(importedPositions).length > 0) { setOpenPosCats(importedPositions); lsSet(LS_POSITIONS, importedPositions); }
        if (Object.keys(importedMeta).length > 0) { setPosMeta(importedMeta); lsSet(LS_META, importedMeta); }
        // Push to backend after import
        syncToBackend(
          importedTrades.length ? importedTrades : trades,
          importedEquity > 0 ? importedEquity : startEquity,
          importedSetups.length ? importedSetups : setups,
          Object.keys(importedPositions).length ? importedPositions : openPosCats,
          Object.keys(importedMeta).length ? importedMeta : posMeta,
        );
        alert("Journal imported and saved to cloud!");
      } catch { alert("Invalid JSON file."); }
    };
    reader.readAsText(file); e.target.value = "";
  }

  // ── Auto-fetch current price when symbol is entered ─────────────────────
  async function fetchEntryPrice(sym: string) {
    const s = sym.trim().toUpperCase();
    if (!s || entryPrice) return; // don't overwrite a manually entered price
    setFetchingEntryPrice(true);
    try {
      const result = await getChart(s, "1D", market ?? "india");
      const price = result.summary?.last_price ?? result.bars[result.bars.length - 1]?.close ?? null;
      if (price && isFinite(price)) setEntryPrice(String(price));
    } catch { /* ignore */ }
    finally { setFetchingEntryPrice(false); }
  }

  // ── Add Trade ─────────────────────────────────────────────────────────────
  function handleAddTrade(e: React.FormEvent) {
    e.preventDefault();
    const customTags = customTagInput.split(",").map(s => s.trim()).filter(Boolean);
    const t: Trade = {
      symbol: entrySymbol.trim().toUpperCase(), type: entryType,
      qty: parseFloat(entryQty) || 0, price: parseFloat(entryPrice) || 0,
      date: entryDate, setupType: entrySetup,
      stoploss: parseFloat(entrySL) || 0, target: parseFloat(entryTarget) || 0,
      tags: [...entryTags, ...customTags], remarks: entryRemarks, img: entryImg,
      vcp: { t: vcpT, depth: vcpDepth, vol: vcpVol },
    };
    saveTrades([...trades, t]);
    setEntrySymbol(""); setEntryQty(""); setEntryPrice(""); setEntrySL(""); setEntryTarget("");
    setFetchingEntryPrice(false);
    setEntryImg(""); setEntryRemarks(""); setEntryTags(new Set()); setCustomTagInput("");
    setVcpT(""); setVcpDepth(""); setVcpVol(""); setCheckboxes(Array(6).fill(false));
    alert("Trade added!");
    setActiveTab(entryType.toLowerCase() === "buy" ? 2 : 1);
  }

  // ── Add from screener ─────────────────────────────────────────────────────
  function submitFromScreener() {
    if (modal?.type !== "add-from-screener") return;
    const qty = parseFloat(screenerQty), price = parseFloat(screenerPrice);
    if (!qty || !price || !screenerDate) { alert("Please fill Qty, Price and Date."); return; }
    const sl = parseFloat(screenerSL) || 0;
    const t: Trade = {
      symbol: modal.symbol.trim().toUpperCase(), type: "Buy",
      qty, price, date: screenerDate, setupType: screenerSetup || DEFAULT_SETUPS[0],
      stoploss: sl, target: 0, tags: [], remarks: "", vcp: {},
    };
    // Update posMeta with SL if provided
    if (sl > 0) {
      const nextMeta = { ...posMeta, [modal.symbol.toUpperCase()]: { ...posMeta[modal.symbol.toUpperCase()], sl, fetchTicker: modal.symbol.toUpperCase() + ".NS" } };
      setPosMeta(nextMeta); lsSet(LS_META, nextMeta);
    }
    saveTrades([...trades, t]);
    setModal(null);
    onAddRequestHandled?.();
    setTimeout(() => setActiveTab(2), 100);
    alert(`${modal.symbol} added to Open Positions!`);
  }

  // ── Delete trade ─────────────────────────────────────────────────────────
  function deleteTrade(idx: number) {
    if (!confirm("Delete this trade?")) return;
    saveTrades(trades.filter((_, i) => i !== idx));
  }

  // ── Close position modal ──────────────────────────────────────────────────
  function openCloseModal(symbol: string, maxQty: number, cmp: number) {
    setModalClosePrice(String(cmp || ""));
    setModalCloseQty(String(Math.round(maxQty)));
    setModalCloseDate(new Date().toISOString().split("T")[0]);
    setModal({ type: "close-pos", symbol, maxQty, cmp });
  }

  function submitClose() {
    if (modal?.type !== "close-pos") return;
    const { symbol } = modal;
    const price = parseFloat(modalClosePrice), qty = parseFloat(modalCloseQty);
    if (isNaN(price) || isNaN(qty) || !modalCloseDate) { alert("Fill all fields."); return; }
    const openLots = fifo.openLotsDict[symbol];
    if (openLots?.[0]) {
      if (getSafeTime(modalCloseDate) < getSafeTime(openLots[0].date)) {
        alert(`Close date (${modalCloseDate}) is before buy date (${openLots[0].date}).`); return;
      }
    }
    const existingTags = (fifo.openLotsDict[symbol] || []).flatMap(l => l.tags || []);
    saveTrades([...trades, { symbol, type: "Sell", qty, price, date: modalCloseDate, setupType: "Close", tags: [...new Set(existingTags)], remarks: "", stoploss: 0, target: 0 }]);
    setModal(null); alert("Position closed!"); setActiveTab(1);
  }

  // ── Edit open position ────────────────────────────────────────────────────
  function openReviewModal(symbol: string) {
    const meta = posMeta[symbol] || {};
    const pos = fifo.openPositions.find(p => p.symbol === symbol);
    setModalOpenSL(String(meta.sl || ""));
    setModalOpenFetchTicker(meta.fetchTicker || (symbol.includes(".") ? symbol : symbol + ".NS"));
    setModalOpenSetupType(pos?.setupType || "");
    setModalEditTags(new Set(pos?.tags || []));
    setModalEditRemarks(pos?.remarks || "");
    setModal({ type: "edit-open", symbol });
  }

  function saveReviewEdits() {
    if (modal?.type !== "edit-open") return;
    const { symbol } = modal;
    const newSL = parseFloat(modalOpenSL) || 0;
    const nextMeta = { ...posMeta, [symbol]: { ...posMeta[symbol], sl: newSL, fetchTicker: modalOpenFetchTicker } };
    setPosMeta(nextMeta); lsSet(LS_META, nextMeta);
    const openIdxs = fifo.openPositions.find(p => p.symbol === symbol)?.buyIndices || [];
    const newSetupType = modalOpenSetupType.trim();
    const nextTrades = trades.map((t, i) =>
      openIdxs.includes(i)
        ? {
            ...t,
            tags: [...modalEditTags],
            remarks: modalEditRemarks,
            ...(newSetupType ? { setupType: newSetupType } : {}),
            // Propagate the new SL to the buy lots so historical exports stay in sync.
            stoploss: newSL,
          }
        : t,
    );
    saveTrades(nextTrades); setModal(null);
  }

  // ── Quick stop-loss edit (applies to the position's total open quantity) ─
  function openEditSLModal(symbol: string) {
    const meta = posMeta[symbol] || {};
    setModalOpenSL(String(meta.sl || ""));
    setModal({ type: "edit-sl", symbol });
  }

  function saveSLEdit() {
    if (modal?.type !== "edit-sl") return;
    const { symbol } = modal;
    const newSL = parseFloat(modalOpenSL) || 0;
    const nextMeta = { ...posMeta, [symbol]: { ...posMeta[symbol], sl: newSL } };
    setPosMeta(nextMeta); lsSet(LS_META, nextMeta);
    const openIdxs = fifo.openPositions.find(p => p.symbol === symbol)?.buyIndices || [];
    const nextTrades = trades.map((t, i) =>
      openIdxs.includes(i) ? { ...t, stoploss: newSL } : t,
    );
    saveTrades(nextTrades);
    setModal(null);
  }

  // ── Lock-breakeven helper ──────────────────────────────────────────────────
  // Sells N shares at CMP such that, if the remaining shares get stopped out
  // at SL, the realized profit from the sale exactly cancels the loss from
  // the remainder. Derivation (using weighted avg as the cost basis):
  //   (cmp − avg) × N  =  (avg − SL) × (qty − N)
  //   ⇒ N = qty × (avg − SL) / (cmp − SL)
  // The result is rounded UP to the next whole share so the realized profit
  // is guaranteed to be ≥ the residual risk — i.e. the trade can no longer
  // lose money once this slice is sold.
  function computeBreakevenSellQty(qty: number, avgEntry: number, sl: number, cmp: number): number {
    if (qty <= 0 || avgEntry <= 0 || sl <= 0 || cmp <= 0) return 0;
    if (avgEntry <= sl) return 0;          // SL already at/above entry → no risk to neutralize
    if (cmp <= avgEntry) return 0;          // not yet profitable → can't hedge
    if (cmp <= sl) return 0;                // already trading at/below SL — should have stopped out
    const raw = (qty * (avgEntry - sl)) / (cmp - sl);
    const rounded = Math.ceil(raw);
    if (rounded <= 0 || rounded >= qty) return 0;
    return rounded;
  }

  function openLockBreakevenModal(symbol: string, sellQty: number, cmp: number, maxQty: number) {
    setModalClosePrice(String(cmp));
    setModalCloseQty(String(sellQty));
    setModalCloseDate(new Date().toISOString().split("T")[0]);
    setModal({ type: "close-pos", symbol, maxQty, cmp });
  }

  // ── Edit closed trade ─────────────────────────────────────────────────────
  function openEditClosedModal(sellIndex: number, buyIndices: number[]) {
    const sellTrade = trades[sellIndex], origTrade = trades[buyIndices[0]] || {} as Trade;
    let totalQty = 0, totalInvested = 0;
    buyIndices.forEach(i => { if (trades[i]) { totalQty += trades[i].qty; totalInvested += trades[i].qty * trades[i].price; } });
    setModalEditEntryPx((totalQty > 0 ? totalInvested / totalQty : 0).toFixed(2));
    setModalEditExitPx(String(sellTrade?.price || ""));
    setModalEditTags(new Set(origTrade.tags || []));
    setModalEditRemarks(origTrade.remarks || "");
    setModalEditImg(origTrade.img || "");
    setModalEditCustomTags("");
    setModal({ type: "edit-closed", sellIndex, buyIndices });
  }

  function saveClosedEdits() {
    if (modal?.type !== "edit-closed") return;
    const { sellIndex, buyIndices } = modal;
    const customTags = modalEditCustomTags.split(",").map(s => s.trim()).filter(Boolean);
    const finalTags = [...modalEditTags, ...customTags];
    const newEntryPx = parseFloat(modalEditEntryPx), newExitPx = parseFloat(modalEditExitPx);
    const nextTrades = trades.map((t, i) => {
      if (buyIndices.includes(i)) return { ...t, tags: finalTags, remarks: modalEditRemarks, img: modalEditImg, ...(!isNaN(newEntryPx) && newEntryPx > 0 ? { price: newEntryPx } : {}) };
      if (i === sellIndex && !isNaN(newExitPx) && newExitPx > 0) return { ...t, price: newExitPx };
      return t;
    });
    saveTrades(nextTrades); setModal(null); alert("Trade updated!");
  }

  // ── Drag & drop ───────────────────────────────────────────────────────────
  function onDragStart(symbol: string) { dragSymbol.current = symbol; }
  function onDrop(cat: OpenPosCat) {
    if (!dragSymbol.current) return;
    const next = { ...openPosCats, [dragSymbol.current]: cat };
    setOpenPosCats(next); lsSet(LS_POSITIONS, next); dragSymbol.current = null;
  }

  // ── Quick calc ────────────────────────────────────────────────────────────
  function runCalc() {
    const cap = parseFloat(calcCap) || 0, rp = parseFloat(calcRisk) || 0;
    const en = parseFloat(calcEntry) || 0, st = parseFloat(calcStop) || 0;
    if (en > st && en - st > 0) {
      const qty = Math.floor((cap * (rp / 100)) / (en - st));
      setCalcQtyRes(`${qty} Qty`); setEntryQty(String(qty)); setEntryPrice(calcEntry); setEntrySL(calcStop);
    }
  }

  // ── Kanban data ───────────────────────────────────────────────────────────
  function posForCat(cat: OpenPosCat) { return openPositions.filter(p => (openPosCats[p.symbol] || "full") === cat); }
  const totalUnrealized = openPositions.reduce((s, p) => { const cmp = posMeta[p.symbol]?.cmp || p.avgPx; return s + (cmp - p.avgPx) * p.qty; }, 0);
  const totalRisk = openPositions.reduce((s, p) => { const sl = posMeta[p.symbol]?.sl || p.avgPx * 0.92; return s + (p.avgPx - sl) * p.qty; }, 0);
  const totalTodayPnl = openPositions.reduce((s, p) => {
    const meta = posMeta[p.symbol];
    if (!meta?.cmp || !meta?.prev_close) return s;
    return s + (meta.cmp - meta.prev_close) * p.qty;
  }, 0);
  const todayBaseValue = openPositions.reduce((s, p) => {
    const meta = posMeta[p.symbol];
    if (!meta?.prev_close) return s;
    return s + meta.prev_close * p.qty;
  }, 0);
  const hasTodayData = todayBaseValue > 0;
  const deployedPctEquity = startEquity > 0 ? (totalInvested / startEquity) * 100 : 0;
  const unrealPctDeployed = totalInvested > 0 ? (totalUnrealized / totalInvested) * 100 : 0;
  const riskPctEquity = startEquity > 0 ? (totalRisk / startEquity) * 100 : 0;
  const todayPctBase = hasTodayData ? (totalTodayPnl / todayBaseValue) * 100 : 0;
  // ── Per-symbol score map + focus filter (drives Equity Curve + Distribution charts) ──
  const symbolScoreMap = useMemo(() => {
    const map: Record<string, { realized: number; unrealized: number; combined: number }> = {};
    closedTrades.forEach((t) => {
      const sym = t.symbol.toUpperCase();
      if (!map[sym]) map[sym] = { realized: 0, unrealized: 0, combined: 0 };
      map[sym].realized += t.pnl;
    });
    openPositions.forEach((pos) => {
      const sym = pos.symbol.toUpperCase();
      if (!map[sym]) map[sym] = { realized: 0, unrealized: 0, combined: 0 };
      const cmp = posMeta[pos.symbol]?.cmp || pos.avgPx;
      map[sym].unrealized += (cmp - pos.avgPx) * pos.qty;
    });
    Object.values(map).forEach((entry) => {
      entry.combined = entry.realized + entry.unrealized;
    });
    return map;
  }, [closedTrades, openPositions, posMeta]);

  const focusedSymbolSet = useMemo(() => {
    if (dashboardFocus === "all") return null;
    const allowed = new Set<string>();
    Object.entries(symbolScoreMap).forEach(([sym, score]) => {
      const value = dashboardMetric === "combined" ? score.combined : score.realized;
      if (dashboardFocus === "winners" && value >= 0) allowed.add(sym);
      else if (dashboardFocus === "losers" && value < 0) allowed.add(sym);
    });
    return allowed;
  }, [symbolScoreMap, dashboardFocus, dashboardMetric]);

  const focusedClosedTrades = useMemo(() => {
    if (!focusedSymbolSet) return closedTrades;
    return closedTrades.filter((t) => focusedSymbolSet.has(t.symbol.toUpperCase()));
  }, [closedTrades, focusedSymbolSet]);

  const focusedUnrealized = useMemo(() => {
    if (dashboardMetric !== "combined") return 0;
    return openPositions.reduce((sum, pos) => {
      const sym = pos.symbol.toUpperCase();
      if (focusedSymbolSet && !focusedSymbolSet.has(sym)) return sum;
      const cmp = posMeta[pos.symbol]?.cmp || pos.avgPx;
      return sum + (cmp - pos.avgPx) * pos.qty;
    }, 0);
  }, [openPositions, posMeta, focusedSymbolSet, dashboardMetric]);

  // ── Save new setup ────────────────────────────────────────────────────────
  function saveNewSetup() {
    if (!newSetupName.trim()) return;
    const next = [...setups, newSetupName.trim()];
    setSetups(next); lsSet(LS_SETUPS, next); setNewSetupName(""); setModal(null);
  }

  // ── Checklist ────────────────────────────────────────────────────────────
  const CHECKLIST_ITEMS = [
    "Market/Index trend is favorable (Stage 2 or recovery)",
    "Sector is a leading group or showing strength",
    "Stock is in confirmed Stage 2 uptrend",
    "VCP or technical pattern is properly formed",
    "Volume dry-up confirmed near pivot",
    "Entry is at or near pivot with tight risk (<8%)",
  ];

  // ── Kanban Card ───────────────────────────────────────────────────────────
  function KanbanCard({ p }: { p: OpenPosition }) {
    const meta = posMeta[p.symbol] || {};
    const cmp = meta.cmp || 0;
    const hasSL = typeof meta.sl === "number" && (meta.sl as number) > 0;
    const sl = hasSL ? (meta.sl as number) : p.avgPx * 0.92;

    // All numbers below are computed off the TOTAL open quantity (p.qty) and
    // the FIFO-weighted average entry (p.avgPx). When the user adds shares
    // and sets a new SL, p.avgPx is recomputed and posMeta[symbol].sl carries
    // the new SL, so the totals reflect the rebuilt position automatically.
    const totalQty = p.qty;
    const avgEntry = p.avgPx;

    // slDistance is SIGNED. When the trader adds to a winner and trails the SL
    // up past the weighted-average entry (e.g. avg ₹106.67 with new SL ₹110),
    // slDistance is negative — that's not "zero risk", it's GUARANTEED profit
    // if the SL hits. We display that case as "Locked" instead of "Risk".
    const slDistance = avgEntry - sl;                            // ₹ per share, signed
    const riskAmtINR = slDistance * totalQty;                    // ₹ total, signed
    const isLockedProfit = slDistance < 0;
    const hasOpenRisk = slDistance > 0;
    const riskPctPos = avgEntry > 0 ? (slDistance / avgEntry) * 100 : 0;
    const riskPctEquity = startEquity > 0 ? (Math.abs(riskAmtINR) / startEquity) * 100 : 0;

    const hasLive = cmp > 0;
    const uPnl = hasLive ? (cmp - avgEntry) * totalQty : 0;     // reward at CMP in ₹
    const uPerc = hasLive && avgEntry > 0 ? ((cmp - avgEntry) / avgEntry) * 100 : 0;
    // R-multiple is only defined while there is genuine downside (slDistance > 0).
    const rMultiple = hasLive && hasOpenRisk ? (cmp - avgEntry) / slDistance : 0;

    // Portfolio impact (per user definition): how much this position's CURRENT
    // unrealized P&L moves total equity, expressed as a % of starting equity.
    // Positive = lifting equity, negative = dragging it down. Book weight kept
    // as a secondary detail so the trader still sees concentration.
    const pnlImpactPct = hasLive && startEquity > 0 ? (uPnl / startEquity) * 100 : 0;
    const bookWeight = totalInvested > 0 ? (p.totalInvested / totalInvested) * 100 : 0;

    // Lock-breakeven: shares to sell at CMP so that if SL hits the rest, the
    // trade nets ~₹0. Only meaningful while position is profitable AND there
    // is still real risk in the remaining position.
    const breakevenSellQty = hasLive ? computeBreakevenSellQty(totalQty, avgEntry, sl, cmp) : 0;
    const canLockBreakeven = breakevenSellQty > 0 && breakevenSellQty < totalQty;

    return (
      <div className="tj-kcard" draggable onDragStart={() => onDragStart(p.symbol)}>
        <div className="tj-kcard-header">
          <div className="tj-kcard-sym">
            <button
              type="button"
              className="tj-symbol-link tj-symbol-link-inline tj-kcard-sym-text"
              onClick={(event) => {
                event.stopPropagation();
                onOpenSymbolChart?.(p.symbol);
              }}
              title="Open big chart"
            >
              {p.symbol}
            </button>
            {p.setupType && <span className="tj-kcard-setup">{p.setupType}</span>}
          </div>
          <span className="tj-kcard-qty">×{Math.round(totalQty)}</span>
        </div>
        <div className="tj-kcard-metrics">
          <div className="tj-kcard-metric"><span className="tj-kcard-ml">Avg / Invested</span><span>₹{fmt(avgEntry)} · ₹{fmt(p.totalInvested, 0)}</span></div>
          {hasLive && <div className="tj-kcard-metric"><span className="tj-kcard-ml">CMP</span><span className="tj-kcard-cmp">₹{fmt(cmp)}</span></div>}
          {hasLive && <div className={`tj-kcard-metric tj-kcard-pnl ${uPnl >= 0 ? "pos" : "neg"}`}><span className="tj-kcard-ml">P&L</span><span>{fmtPnl(uPnl)} <small>({fmtPerc(uPerc)})</small></span></div>}
          <div className="tj-kcard-metric">
            <span className="tj-kcard-ml">SL</span>
            <span>
              {hasSL ? `₹${fmt(sl)}` : <span className="muted">not set</span>}
              <button
                type="button"
                onClick={() => openEditSLModal(p.symbol)}
                title="Edit stop loss (applies to total quantity)"
                aria-label="Edit stop loss"
                style={{ marginLeft: 6, padding: "0 6px", fontSize: 11, lineHeight: "16px", border: "1px solid var(--border, #4a5568)", borderRadius: 4, background: "transparent", color: "inherit", cursor: "pointer" }}
              >
                ✎
              </button>
            </span>
          </div>
          {isLockedProfit ? (
            <div className="tj-kcard-metric pos">
              <span className="tj-kcard-ml">Locked</span>
              <span title={`SL ₹${fmt(sl)} > Avg ₹${fmt(avgEntry)} → if SL hits, you still book (₹${fmt(sl)} − ₹${fmt(avgEntry)}) × ${Math.round(totalQty)} shares`}>
                +₹{fmt(Math.abs(riskAmtINR), 0)} <small>({Math.abs(riskPctPos).toFixed(1)}% pos · {riskPctEquity.toFixed(2)}% port)</small>
              </span>
            </div>
          ) : (
            <div className="tj-kcard-metric">
              <span className="tj-kcard-ml">Risk</span>
              <span className="neg" title={`(₹${fmt(avgEntry)} − ₹${fmt(sl)}) × ${Math.round(totalQty)} shares`}>
                ₹{fmt(riskAmtINR, 0)} <small>({riskPctPos.toFixed(1)}% pos · {riskPctEquity.toFixed(2)}% port)</small>
              </span>
            </div>
          )}
          {hasLive ? (
            <div className={`tj-kcard-metric ${uPnl >= 0 ? "pos" : "neg"}`}>
              <span className="tj-kcard-ml">Reward</span>
              <span title={`(₹${fmt(cmp)} − ₹${fmt(avgEntry)}) × ${Math.round(totalQty)} shares`}>
                {fmtPnl(uPnl)}{hasOpenRisk ? <small> ({rMultiple >= 0 ? "+" : ""}{rMultiple.toFixed(1)}R)</small> : null}
              </span>
            </div>
          ) : (
            <div className="tj-kcard-metric"><span className="tj-kcard-ml">Reward</span><span className="muted">—</span></div>
          )}
          <div className="tj-kcard-metric">
            <span className="tj-kcard-ml">Impact</span>
            {hasLive ? (
              <span className={pnlImpactPct >= 0 ? "pos" : "neg"} title="Current unrealized P&L as % of starting equity · book weight (vs total deployed)">
                {pnlImpactPct >= 0 ? "+" : ""}{pnlImpactPct.toFixed(2)}% equity <small>· {bookWeight.toFixed(1)}% book</small>
              </span>
            ) : (
              <span className="muted">—</span>
            )}
          </div>
        </div>
        {canLockBreakeven && (
          <div
            style={{
              fontSize: 11,
              marginTop: 6,
              padding: "6px 8px",
              borderRadius: 4,
              background: "rgba(34,197,94,0.08)",
              border: "1px solid rgba(34,197,94,0.25)",
              lineHeight: 1.4,
            }}
            title={`Sell ${breakevenSellQty} @ ₹${fmt(cmp)} → realized profit ≈ ₹${fmt((cmp - avgEntry) * breakevenSellQty, 0)}. If remaining ${Math.round(totalQty) - breakevenSellQty} shares hit SL ₹${fmt(sl)} → loss ≈ ₹${fmt((avgEntry - sl) * (totalQty - breakevenSellQty), 0)}. Net ≈ ₹0.`}
          >
            <div style={{ opacity: 0.85, marginBottom: 4 }}>
              Sell <strong>{breakevenSellQty}</strong> @ ₹{fmt(cmp)} →
              if SL hits remaining {Math.round(totalQty) - breakevenSellQty}, trade nets ~₹0.
            </div>
            <button
              type="button"
              className="tj-action-btn ghost"
              style={{ width: "100%" }}
              onClick={() => openLockBreakevenModal(p.symbol, breakevenSellQty, cmp, totalQty)}
            >
              Lock breakeven ({breakevenSellQty} @ ₹{fmt(cmp)})
            </button>
          </div>
        )}
        {p.tags.length > 0 && <div className="tj-chip-row">{p.tags.slice(0, 4).map(t => <span key={t} className="tj-chip sm">{t}</span>)}</div>}
        <div className="tj-kcard-actions">
          <button className="tj-action-btn danger-outline" onClick={() => openCloseModal(p.symbol, p.qty, cmp || p.avgPx)}>Close</button>
          <button className="tj-action-btn ghost" onClick={() => openEditSLModal(p.symbol)} title="Quick edit stop loss">Edit SL</button>
          <button className="tj-action-btn ghost" onClick={() => openReviewModal(p.symbol)}>Review</button>
        </div>
      </div>
    );
  }

  function KanbanCol({ cat, label, accent }: { cat: OpenPosCat; label: string; accent: string }) {
    const items = posForCat(cat);
    return (
      <div className={`tj-kcol tj-kcol-${accent}`} onDragOver={e => e.preventDefault()} onDrop={() => onDrop(cat)}>
        <div className="tj-kcol-title">
          <span>{label}</span>
          <span className="tj-kcol-badge">{items.length}</span>
        </div>
        <div className="tj-kcol-body">
          {items.map(p => <KanbanCard key={p.symbol} p={p} />)}
          {items.length === 0 && <div className="tj-kcol-empty">Drag here</div>}
        </div>
      </div>
    );
  }

  // ─── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="tj-root">
      {/* ── Header ── */}
      <div className="tj-header">
        <div className="tj-header-brand">
          <span className="tj-brand-icon">📒</span>
          <div>
            <div className="tj-brand-name">Trade Journal</div>
            <div className="tj-brand-sub">TradeOS VCP Journal · {trades.length} trades · {openPositions.length} open</div>
          </div>
        </div>
        <div className="tj-header-controls">
          <div className="tj-equity-wrap">
            <label className="tj-equity-label">Starting Equity ₹</label>
            <input
              className="tj-equity-input"
              type="number"
              value={equityInput}
              onChange={e => setEquityInput(e.target.value)}
              onBlur={() => {
                const val = parseFloat(equityInput);
                if (val > 0) {
                  setStartEquity(val); lsSet(LS_EQUITY, val);
                  setSizerEquity(String(val)); setCalcCap(String(val));
                  syncToBackend(trades, val, setups, openPosCats, posMeta);
                }
              }}
            />
          </div>
          {backendSyncing && <span className="tj-cloud-badge">☁ Saving…</span>}
          <button className="tj-btn secondary" onClick={exportJSON}>↓ Export</button>
          <label className="tj-btn secondary" style={{ cursor: "pointer" }}>
            ↑ Import
            <input type="file" accept=".json" style={{ display: "none" }} onChange={importJSON} />
          </label>
        </div>
      </div>

      {/* ── Tabs ── */}
      <div className="tj-tabbar">
        {["Dashboard", "Trade Log", "Open Positions", "Smart Entry", "Insights", "Position Sizer"].map((t, i) => (
          <button
            key={t}
            className={`tj-tabbtn ${activeTab === i ? "active" : ""}`}
            onClick={() => { setActiveTab(i); }}
          >
            {t}
            {i === 2 && openPositions.length > 0 && <span className="tj-tabbadge">{openPositions.length}</span>}
          </button>
        ))}
        <button
          type="button"
          className="tj-tabbtn news-trigger-btn news-tab-btn"
          onClick={() => setNewsModalOpen(true)}
          disabled={openPositions.length === 0}
          title={openPositions.length === 0 ? "Add a position first" : "Open news for all open positions (full-screen)"}
        >
          <span className="news-trigger-emoji">📰</span> News Radar
        </button>
      </div>

      {/* ── Tab 0: Dashboard ── */}
      {activeTab === 0 && (
        <div className="tj-page tj-page-dashboard tj-lite">
          <div className="tj-kpis">
            <div className={`tj-kpi ${totalPnl >= 0 ? "pos" : "neg"}`}>
              <div className="tj-kpi-label">Total Realized P&L</div>
              <div className="tj-kpi-value">{fmtPnl(totalPnl)}</div>
              <div className="tj-kpi-sub">{closedTrades.length} closed trades</div>
            </div>
            <div className="tj-kpi">
              <div className="tj-kpi-label">Win Rate</div>
              <div className={`tj-kpi-value ${winRate >= 50 ? "pos" : "neg"}`}>{winRate.toFixed(1)}%</div>
              <div className="tj-kpi-sub">{winners.length} W / {losers.length} L</div>
            </div>
            <div className="tj-kpi">
              <div className="tj-kpi-label">Avg Position Size</div>
              <div className="tj-kpi-value">{avgPosSize.toFixed(1)}%</div>
              <div className="tj-kpi-sub">of equity per trade</div>
            </div>
            <div className={`tj-kpi ${totalUnrealized >= 0 ? "pos" : "neg"}`}>
              <div className="tj-kpi-label">Unrealized P&L</div>
              <div className="tj-kpi-value">{fmtPnl(totalUnrealized)}</div>
              <div className="tj-kpi-sub">₹{fmt(totalInvested, 0)} deployed</div>
            </div>
          </div>

          <div className="tj-dashboard-controlbar">
            <div className="tj-toggle-group" role="tablist" aria-label="Dashboard metric mode">
              <button type="button" className={`tj-toggle-btn ${dashboardMetric === "combined" ? "active" : ""}`} onClick={() => setDashboardMetric("combined")}>Combined P&L</button>
              <button type="button" className={`tj-toggle-btn ${dashboardMetric === "realized" ? "active" : ""}`} onClick={() => setDashboardMetric("realized")}>Realized P&L</button>
            </div>
            <div className="tj-toggle-group" role="tablist" aria-label="Dashboard symbol focus">
              <button type="button" className={`tj-toggle-btn ${dashboardFocus === "all" ? "active" : ""}`} onClick={() => setDashboardFocus("all")}>All</button>
              <button type="button" className={`tj-toggle-btn ${dashboardFocus === "winners" ? "active" : ""}`} onClick={() => setDashboardFocus("winners")}>Winners</button>
              <button type="button" className={`tj-toggle-btn ${dashboardFocus === "losers" ? "active" : ""}`} onClick={() => setDashboardFocus("losers")}>Losers</button>
            </div>
          </div>

          <div className="tj-chart-row">
            <div className="tj-card full-width">
              <div className="tj-card-hdr">
                Equity Curve
                <span className="tj-card-hdr-sub">
                  {dashboardMetric === "combined" ? "Combined" : "Realized"}
                  {dashboardFocus !== "all" ? ` · ${dashboardFocus === "winners" ? "Winners" : "Losers"}` : ""}
                  {focusedSymbolSet ? ` · ${focusedSymbolSet.size} symbol${focusedSymbolSet.size !== 1 ? "s" : ""}` : ""}
                </span>
              </div>
              <EquityCurve
                closed={focusedClosedTrades}
                startEquity={startEquity}
                unrealizedTail={focusedUnrealized}
                metric={dashboardMetric}
                focus={dashboardFocus}
              />
            </div>
          </div>

          <div className="tj-chart-row two-col">
            <div className="tj-card">
              <div className="tj-card-hdr">
                P&L Distribution
                <span className="tj-card-hdr-sub">
                  {dashboardFocus !== "all" ? `${dashboardFocus === "winners" ? "Winners" : "Losers"} only` : "All trades"}
                </span>
              </div>
              <PnlDistribution closed={focusedClosedTrades} focus={dashboardFocus} />
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Top Winners vs Losers</div>
              <div className="tj-wl-grid">
                <div>
                  <div className="tj-wl-title pos">▲ Top Winners</div>
                  {top10Win.length === 0 ? <div className="tj-empty">No winners yet</div> : top10Win.map((t, i) => (
                    <div key={i} className="tj-wl-row">
                      <span className="tj-wl-rank">#{i + 1}</span>
                      <button
                        type="button"
                        className="tj-symbol-link tj-symbol-link-inline tj-wl-sym"
                        onClick={() => onOpenSymbolChart?.(t.symbol)}
                        title="Open big chart"
                      >
                        {t.symbol}
                      </button>
                      <span className="pos">{fmtPerc(t.perc)}</span>
                      <span className="pos tj-wl-pnl">{fmtPnl(t.pnl)}</span>
                    </div>
                  ))}
                </div>
                <div>
                  <div className="tj-wl-title neg">▼ Top Losers</div>
                  {top10Loss.length === 0 ? <div className="tj-empty">No losers yet</div> : top10Loss.map((t, i) => (
                    <div key={i} className="tj-wl-row">
                      <span className="tj-wl-rank">#{i + 1}</span>
                      <button
                        type="button"
                        className="tj-symbol-link tj-symbol-link-inline tj-wl-sym"
                        onClick={() => onOpenSymbolChart?.(t.symbol)}
                        title="Open big chart"
                      >
                        {t.symbol}
                      </button>
                      <span className="neg">{fmtPerc(t.perc)}</span>
                      <span className="neg tj-wl-pnl">{fmtPnl(t.pnl)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <div className="tj-chart-row tj-chart-trio">
            <div className="tj-card">
              <div className="tj-card-hdr">Win / Loss Mix</div>
              <WinLossDonut winners={winners.length} losers={losers.length} />
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Hold Time vs Outcome</div>
              <HoldTimeHistogram closed={closedTrades} />
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Day of Week</div>
              <DayOfWeekStats closed={closedTrades} />
            </div>
          </div>

          <div className="tj-card" style={{ marginBottom: 16 }}>
            <div className="tj-card-hdr">Setup Performance</div>
            <SetupBreakdown closed={closedTrades} />
          </div>

          <div className="tj-card tj-recent-card">
            <div className="tj-card-hdr">
              Last 10 Trades
              <span className="tj-card-hdr-sub">Most recent at top · Cumulative over the window</span>
            </div>
            <LastTradesTable closed={closedTrades} onOpenSymbolChart={onOpenSymbolChart} />
          </div>

          <div className="tj-card">
            <div className="tj-card-hdr">Monthly Consistency</div>
            <MonthlyCalendar closed={closedTrades} onOpenSymbolChart={onOpenSymbolChart} />
          </div>
        </div>
      )}

      {/* ── Tab 1: Trade Log ── */}
      {activeTab === 1 && (
        <div className="tj-page">
          <div className="tj-log-toolbar">
            <select className="tj-select" value={filterMonth} onChange={e => setFilterMonth(e.target.value)}>
              <option value="all">All Months</option>
              {monthOptions.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
            <select className="tj-select" value={filterOutcome} onChange={e => setFilterOutcome(e.target.value)}>
              <option value="all">All Outcomes</option>
              <option value="win">Winners</option>
              <option value="loss">Losers</option>
            </select>
            <input className="tj-input" placeholder="Filter by symbol…" value={filterSymbol} onChange={e => setFilterSymbol(e.target.value)} style={{ maxWidth: 180 }} />
            <div className="tj-log-summary">
              <span className="pos">{filteredClosed.filter(t => t.pnl > 0).length}W</span>
              <span className="neg">{filteredClosed.filter(t => t.pnl <= 0).length}L</span>
              <span>₹{fmt(filteredClosed.reduce((s, t) => s + t.pnl, 0))}</span>
            </div>
          </div>
          {filteredClosed.length === 0 ? (
            <div className="tj-empty-page">No closed trades yet — add trades in the Smart Entry tab.</div>
          ) : (
            <div className="tj-table-wrap">
              <table className="tj-table">
                <thead>
                  <tr><th>Symbol</th><th>Setup</th><th>Entry ₹</th><th>Exit ₹</th><th>Entry</th><th>Exit</th><th>P&L ₹</th><th>%</th><th>Size %</th><th>Tags</th><th></th></tr>
                </thead>
                <tbody>
                  {filteredClosed.map((t, i) => (
                    <tr key={i} className={t.pnl >= 0 ? "win-row" : "loss-row"}>
                      <td className="tj-sym-cell">
                        <button type="button" className="tj-symbol-link" onClick={() => onOpenSymbolChart?.(t.symbol)} title="Open big chart">
                          {t.symbol}
                        </button>
                      </td>
                      <td>{t.setupType || "—"}</td>
                      <td>{fmt(t.entryPx)}</td>
                      <td>{fmt(t.exitPx)}</td>
                      <td className="tj-date-cell">{t.entryDate}</td>
                      <td className="tj-date-cell">{t.exitDate}</td>
                      <td className={t.pnl >= 0 ? "pos fw" : "neg fw"}>{fmtPnl(t.pnl)}</td>
                      <td className={t.perc >= 0 ? "pos" : "neg"}>{fmtPerc(t.perc)}</td>
                      <td>{t.posSizePct.toFixed(1)}%</td>
                      <td>{(t.tags || []).slice(0, 3).map(tag => <span key={tag} className="tj-chip xs">{tag}</span>)}</td>
                      <td className="tj-action-cell">
                        <button className="tj-action-btn ghost" onClick={() => openEditClosedModal(t.sellIndex, t.buyIndices)}>Edit</button>
                        <button className="tj-action-btn danger" onClick={() => deleteTrade(t.sellIndex)}>×</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ── Tab 2: Open Positions ── */}
      {activeTab === 2 && (
        <div className="tj-page">
          <div className="tj-kanban-topbar">
            <div className="tj-kanban-metrics">
              <div className="tj-kbox tj-kbox-deployed">
                <span className="tj-kbox-label">Total Deployed</span>
                <strong className="tj-kbox-value">₹{fmt(totalInvested, 0)}</strong>
                <span className="tj-kbox-sub">{deployedPctEquity.toFixed(1)}% of equity · {openPositions.length} positions</span>
              </div>
              <div className={`tj-kbox tj-kbox-unreal ${totalUnrealized >= 0 ? "pos" : "neg"}`}>
                <span className="tj-kbox-label">Unrealized P&L</span>
                <strong className="tj-kbox-value">{fmtPnl(totalUnrealized)}</strong>
                <span className="tj-kbox-sub">{fmtPerc(unrealPctDeployed)} on deployed</span>
              </div>
              <div className="tj-kbox tj-kbox-risk neg">
                <span className="tj-kbox-label">Total Risk</span>
                <strong className="tj-kbox-value">₹{fmt(totalRisk, 0)}</strong>
                <span className="tj-kbox-sub">{riskPctEquity.toFixed(2)}% of equity</span>
              </div>
              <div className={`tj-kbox tj-kbox-today ${hasTodayData ? (totalTodayPnl >= 0 ? "pos" : "neg") : ""}`}>
                <span className="tj-kbox-label">Today's P&L</span>
                <strong className="tj-kbox-value">
                  {hasTodayData ? fmtPnl(totalTodayPnl) : "—"}
                </strong>
                <span className="tj-kbox-sub">
                  {hasTodayData ? `${fmtPerc(todayPctBase)} on prev close` : "Sync prices to compute"}
                </span>
              </div>
            </div>
            <div className="tj-kanban-actions">
              {syncStatus && <span className="tj-sync-status">{syncStatus}</span>}
              <button
                type="button"
                className="news-trigger-btn"
                onClick={() => setNewsModalOpen(true)}
                disabled={openPositions.length === 0}
                title={openPositions.length === 0 ? "No open positions" : "Open full-screen news widget"}
              >
                <span className="news-trigger-emoji">📰</span> News
              </button>
              <button className={`tj-btn primary ${syncing ? "loading" : ""}`} onClick={syncPrices} disabled={syncing}>
                {syncing ? "Syncing…" : "⟳ Sync Prices"}
              </button>
            </div>
          </div>
          {openPositions.length === 0 ? (
            <div className="tj-empty-page">No open positions. Add a Buy trade in Smart Entry, or use the screener to send stocks here.</div>
          ) : (
            <div className="tj-kanban">
              <KanbanCol cat="full" label="Full Size" accent="full" />
              <KanbanCol cat="half" label="Half Size" accent="half" />
              <KanbanCol cat="quarter" label="Pilot + Testing" accent="quarter" />
            </div>
          )}
        </div>
      )}

      {/* ── Tab 3: Smart Entry ── */}
      {activeTab === 3 && (
        <div className="tj-page">
          <div className="tj-entry-layout">
            <div className="tj-card">
              <div className="tj-card-hdr">Quick Calculator</div>
              <div className="tj-form-stack">
                <div className="tj-form-field"><label>Account Size (₹)</label><input className="tj-input" type="number" value={calcCap} onChange={e => setCalcCap(e.target.value)} /></div>
                <div className="tj-form-field"><label>Risk % per Trade</label><input className="tj-input" type="number" step="0.1" value={calcRisk} onChange={e => setCalcRisk(e.target.value)} /></div>
                <div className="tj-form-field"><label>Entry Price</label><input className="tj-input" type="number" step="any" value={calcEntry} onChange={e => setCalcEntry(e.target.value)} /></div>
                <div className="tj-form-field"><label>Stop Loss Price</label><input className="tj-input" type="number" step="any" value={calcStop} onChange={e => setCalcStop(e.target.value)} /></div>
                <button className="tj-btn primary" style={{ marginTop: 4 }} onClick={runCalc}>Calculate → Apply to Form</button>
                {calcQtyRes && <div className="tj-calc-result">{calcQtyRes}</div>}
              </div>

              <div className="tj-card-hdr" style={{ marginTop: 24 }}>Tags & Habits</div>
              <div className="tj-chip-row">
                {PREDEFINED_TAGS.map(tag => (
                  <div key={tag} className={`tj-chip clickable ${entryTags.has(tag) ? "selected" : ""}`}
                    onClick={() => setEntryTags(prev => { const n = new Set(prev); n.has(tag) ? n.delete(tag) : n.add(tag); return n; })}>
                    {tag}
                  </div>
                ))}
              </div>
              <input className="tj-input" style={{ marginTop: 8 }} placeholder="Custom tags (comma separated)…" value={customTagInput} onChange={e => setCustomTagInput(e.target.value)} />
              <div className="tj-form-field" style={{ marginTop: 12 }}>
                <label>Trade Remarks</label>
                <textarea className="tj-textarea" rows={3} value={entryRemarks} onChange={e => setEntryRemarks(e.target.value)} placeholder="Notes about this trade…" />
              </div>
            </div>

            <div className="tj-card">
              <div className="tj-card-hdr">Add Trade</div>
              <form onSubmit={handleAddTrade}>
                <div className="tj-form-grid-2">
                  <div className="tj-form-field"><label>Symbol *</label><input className="tj-input" required value={entrySymbol} onChange={e => { setEntrySymbol(e.target.value); setEntryPrice(""); }} onBlur={e => fetchEntryPrice(e.target.value)} placeholder="RELIANCE" /></div>
                  <div className="tj-form-field"><label>Type</label>
                    <select className="tj-select" value={entryType} onChange={e => setEntryType(e.target.value)}>
                      <option value="Buy">Buy</option><option value="Sell">Sell</option>
                    </select>
                  </div>
                  <div className="tj-form-field"><label>Quantity *</label><input className="tj-input" type="number" required value={entryQty} onChange={e => setEntryQty(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Price ₹ {fetchingEntryPrice ? <span style={{fontWeight:400,fontSize:11,color:"var(--clr-accent)"}}>fetching…</span> : "*"}</label><input className="tj-input" type="number" step="any" required value={entryPrice} onChange={e => setEntryPrice(e.target.value)} placeholder={fetchingEntryPrice ? "loading…" : ""} /></div>
                  <div className="tj-form-field"><label>Date *</label><input className="tj-input" type="date" required value={entryDate} onChange={e => setEntryDate(e.target.value)} /></div>
                  <div className="tj-form-field">
                    <label>Setup</label>
                    <div style={{ display: "flex", gap: 6 }}>
                      <select className="tj-select" value={entrySetup} onChange={e => setEntrySetup(e.target.value)}>
                        {setups.map(s => <option key={s} value={s}>{s}</option>)}
                      </select>
                      <button type="button" className="tj-btn secondary" onClick={() => setModal({ type: "add-setup" })}>+</button>
                    </div>
                  </div>
                  <div className="tj-form-field"><label>Stop Loss ₹</label><input className="tj-input" type="number" step="any" value={entrySL} onChange={e => setEntrySL(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Target ₹</label><input className="tj-input" type="number" step="any" value={entryTarget} onChange={e => setEntryTarget(e.target.value)} /></div>
                </div>

                <div className="tj-card-hdr" style={{ marginTop: 18 }}>VCP Specifics</div>
                <div className="tj-form-grid-3">
                  <div className="tj-form-field"><label>Contractions</label><input className="tj-input" placeholder="e.g. 3T" value={vcpT} onChange={e => setVcpT(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Depth %</label><input className="tj-input" placeholder="e.g. 3:1:0.5" value={vcpDepth} onChange={e => setVcpDepth(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Vol Dry-up</label><input className="tj-input" placeholder="Yes/No" value={vcpVol} onChange={e => setVcpVol(e.target.value)} /></div>
                </div>

                <div className="tj-form-field" style={{ marginTop: 12 }}>
                  <label>Chart URL</label>
                  <input className="tj-input" value={entryImg} onChange={e => setEntryImg(e.target.value)} placeholder="https://…" />
                </div>

                <div className="tj-card-hdr" style={{ marginTop: 18 }}>Pre-Trade Checklist</div>
                <div className="tj-checklist">
                  {CHECKLIST_ITEMS.map((item, i) => (
                    <label key={i} className="tj-check-row">
                      <input type="checkbox" checked={checkboxes[i]} onChange={() => setCheckboxes(p => p.map((v, j) => j === i ? !v : v))} />
                      <span className={checkboxes[i] ? "checked" : ""}>{item}</span>
                    </label>
                  ))}
                </div>
                {checkboxes.some(c => !c) && <div className="tj-check-warn">⚠ Complete all checks before entering a trade</div>}

                <button type="submit" className="tj-btn primary" style={{ width: "100%", marginTop: 16 }}>Add Trade</button>
              </form>
            </div>
          </div>
        </div>
      )}

      {/* ── Tab 4: Insights ── */}
      {activeTab === 4 && (
        <div className="tj-page">
          <div className="tj-insights-top">
            <div className="tj-card">
              <div className="tj-card-hdr">Hold Time</div>
              <div className="tj-metric-row"><span>All Trades</span><strong>{avgHoldAll.toFixed(1)} days</strong></div>
              <div className="tj-metric-row pos"><span>Winners</span><strong>{avgHoldWin.toFixed(1)} days</strong></div>
              <div className="tj-metric-row neg"><span>Losers</span><strong>{avgHoldLoss.toFixed(1)} days</strong></div>
            </div>
            <div className="tj-card" style={{ flex: 2 }}>
              <div className="tj-card-hdr">Setup Performance</div>
              {Object.keys(setupMap).length === 0 ? <div className="tj-empty">No data yet</div> : (
                <table className="tj-table">
                  <thead><tr><th>Setup</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Net P&L</th></tr></thead>
                  <tbody>
                    {Object.entries(setupMap).sort((a, b) => b[1].pnl - a[1].pnl).map(([s, d]) => (
                      <tr key={s}>
                        <td><span className="tj-chip sm">{s}</span></td>
                        <td className="pos">{d.wins}</td><td className="neg">{d.losses}</td>
                        <td>{((d.wins / (d.wins + d.losses)) * 100).toFixed(0)}%</td>
                        <td className={d.pnl >= 0 ? "pos fw" : "neg fw"}>{fmtPnl(d.pnl)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
          <div className="tj-card" style={{ marginTop: 16 }}>
            <div className="tj-card-hdr">Tag & Habit Analysis</div>
            {Object.keys(tagMap).length === 0 ? <div className="tj-empty">No tagged trades yet</div> : (
              <table className="tj-table">
                <thead><tr><th>Tag</th><th>Closed Trades</th><th>Open Trades</th><th>Realized P&L</th><th>Unrealized P&L</th></tr></thead>
                <tbody>
                  {Object.entries(tagMap).sort((a, b) => b[1].realizedPnl - a[1].realizedPnl).map(([tag, d]) => (
                    <tr key={tag}>
                      <td><span className="tj-chip sm">{tag}</span></td>
                      <td>{d.closedCount}</td><td>{d.openCount}</td>
                      <td className={d.realizedPnl >= 0 ? "pos fw" : "neg fw"}>{fmtPnl(d.realizedPnl)}</td>
                      <td className={d.unrealizedPnl >= 0 ? "pos" : "neg"}>{fmtPnl(d.unrealizedPnl)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}

      {/* ── Tab 5: Position Sizer ── */}
      {activeTab === 5 && (
        <div className="tj-page tj-sizer-page">
          <div className="tj-card tj-sizer-card">
            <div className="tj-card-hdr">Position Sizer</div>
            <div className="tj-form-grid-2">
              <div className="tj-form-field"><label>Account Equity (₹)</label><input className="tj-input" type="number" value={sizerEquity} onChange={e => setSizerEquity(e.target.value)} /></div>
              <div className="tj-form-field"><label>Risk per Trade (%)</label><input className="tj-input" type="number" step="0.1" value={sizerRiskPct} onChange={e => setSizerRiskPct(e.target.value)} /></div>
              <div className="tj-form-field"><label>Entry Price (₹)</label><input className="tj-input" type="number" step="any" value={sizerEntry} onChange={e => setSizerEntry(e.target.value)} /></div>
              <div className="tj-form-field"><label>Stop Loss (%)</label><input className="tj-input" type="number" step="0.1" value={sizerSLPct} onChange={e => setSizerSLPct(e.target.value)} /></div>
            </div>
            <div className="tj-sizer-results">
              <div className="tj-sizer-box"><div className="tj-sizer-label">Qty to Buy</div><div className="tj-sizer-val accent">{sizerResultQty}</div></div>
              <div className="tj-sizer-box"><div className="tj-sizer-label">SL Price</div><div className="tj-sizer-val neg">₹{fmt(sizerResultSL)}</div></div>
              <div className="tj-sizer-box"><div className="tj-sizer-label">Capital at Risk</div><div className="tj-sizer-val neg">₹{fmt(sizerResultRisk)}</div></div>
              <div className="tj-sizer-box"><div className="tj-sizer-label">Position Size</div><div className="tj-sizer-val">₹{fmt(sizerResultPos)}</div></div>
            </div>
          </div>
        </div>
      )}

      {/* News Radar is now a full-screen modal triggered from the tab/button (see NewsModal mount below) */}

      {/* ── Modals ── */}
      {modal && (
        <div className="tj-overlay" onClick={e => { if ((e.target as HTMLElement).classList.contains("tj-overlay")) { setModal(null); onAddRequestHandled?.(); } }}>
          <div className="tj-modal">
            <button className="tj-modal-x" onClick={() => { setModal(null); onAddRequestHandled?.(); }}>✕</button>

            {modal.type === "add-from-screener" && (
              <>
                <div className="tj-modal-title">Add to Journal: <span style={{ color: "var(--accent)" }}>{modal.symbol}</span></div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-field"><label>Quantity *</label><input className="tj-input" type="number" autoFocus value={screenerQty} onChange={e => setScreenerQty(e.target.value)} placeholder="e.g. 50" /></div>
                  <div className="tj-form-field"><label>Buy Price ₹ *</label><input className="tj-input" type="number" step="any" value={screenerPrice} onChange={e => setScreenerPrice(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Stop Loss ₹</label><input className="tj-input" type="number" step="any" value={screenerSL} onChange={e => setScreenerSL(e.target.value)} placeholder="Optional" /></div>
                  <div className="tj-form-field"><label>Date</label><input className="tj-input" type="date" value={screenerDate} onChange={e => setScreenerDate(e.target.value)} /></div>
                </div>
                <div className="tj-form-field" style={{ marginTop: 8 }}>
                  <label>Setup Type</label>
                  <select className="tj-select" value={screenerSetup} onChange={e => setScreenerSetup(e.target.value)}>
                    {setups.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
                <button className="tj-btn primary" style={{ width: "100%", marginTop: 16 }} onClick={submitFromScreener}>
                  Add to Open Positions
                </button>
              </>
            )}

            {modal.type === "close-pos" && (
              <>
                <div className="tj-modal-title">Close Position: {modal.symbol}</div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-field"><label>Exit Price</label><input className="tj-input" type="number" step="0.05" value={modalClosePrice} onChange={e => setModalClosePrice(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Qty (max {Math.round(modal.maxQty)})</label><input className="tj-input" type="number" value={modalCloseQty} max={modal.maxQty} onChange={e => setModalCloseQty(e.target.value)} /></div>
                </div>
                <div className="tj-form-field"><label>Date</label><input className="tj-input" type="date" value={modalCloseDate} onChange={e => setModalCloseDate(e.target.value)} /></div>
                <button className="tj-btn primary" style={{ width: "100%", marginTop: 16 }} onClick={submitClose}>Confirm Close</button>
              </>
            )}

            {modal.type === "edit-closed" && (
              <>
                <div className="tj-modal-title">Edit Closed Trade</div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-field"><label>Avg Entry Price</label><input className="tj-input" type="number" step="any" value={modalEditEntryPx} onChange={e => setModalEditEntryPx(e.target.value)} /></div>
                  <div className="tj-form-field"><label>Exit Price</label><input className="tj-input" type="number" step="any" value={modalEditExitPx} onChange={e => setModalEditExitPx(e.target.value)} /></div>
                </div>
                <div className="tj-card-hdr" style={{ marginTop: 12 }}>Tags</div>
                <div className="tj-chip-row">
                  {PREDEFINED_TAGS.map(tag => (
                    <div key={tag} className={`tj-chip clickable ${modalEditTags.has(tag) ? "selected" : ""}`}
                      onClick={() => setModalEditTags(p => { const n = new Set(p); n.has(tag) ? n.delete(tag) : n.add(tag); return n; })}>
                      {tag}
                    </div>
                  ))}
                </div>
                <input className="tj-input" style={{ marginTop: 8 }} placeholder="Custom tags (comma separated)…" value={modalEditCustomTags} onChange={e => setModalEditCustomTags(e.target.value)} />
                <div className="tj-form-field" style={{ marginTop: 8 }}><label>Remarks</label><textarea className="tj-textarea" rows={2} value={modalEditRemarks} onChange={e => setModalEditRemarks(e.target.value)} /></div>
                <div className="tj-form-field"><label>Chart URL</label><input className="tj-input" value={modalEditImg} onChange={e => setModalEditImg(e.target.value)} /></div>
                <button className="tj-btn primary" style={{ width: "100%", marginTop: 16 }} onClick={saveClosedEdits}>Save Changes</button>
              </>
            )}

            {modal.type === "edit-open" && (
              <>
                <div className="tj-modal-title">Review Position: {modal.symbol}</div>
                <div style={{ fontSize: 12, opacity: 0.75, marginBottom: 10 }}>
                  Stop loss and setup type apply to the total open quantity
                  ({Math.round(fifo.openPositions.find(p => p.symbol === modal.symbol)?.qty || 0)} shares).
                </div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-field"><label>Stop Loss ₹</label><input className="tj-input" type="number" step="any" value={modalOpenSL} onChange={e => setModalOpenSL(e.target.value)} /></div>
                  <div className="tj-form-field">
                    <label>Setup Type</label>
                    <select className="tj-input" value={modalOpenSetupType} onChange={e => setModalOpenSetupType(e.target.value)}>
                      <option value="">— None —</option>
                      {setups.map(s => <option key={s} value={s}>{s}</option>)}
                    </select>
                  </div>
                </div>
                <div className="tj-form-field" style={{ marginTop: 8 }}><label>Yahoo Finance Ticker</label><input className="tj-input" placeholder="e.g. RELIANCE.NS" value={modalOpenFetchTicker} onChange={e => setModalOpenFetchTicker(e.target.value)} /></div>
                <div className="tj-card-hdr" style={{ marginTop: 12 }}>Tags</div>
                <div className="tj-chip-row">
                  {PREDEFINED_TAGS.map(tag => (
                    <div key={tag} className={`tj-chip clickable ${modalEditTags.has(tag) ? "selected" : ""}`}
                      onClick={() => setModalEditTags(p => { const n = new Set(p); n.has(tag) ? n.delete(tag) : n.add(tag); return n; })}>
                      {tag}
                    </div>
                  ))}
                </div>
                <div className="tj-form-field" style={{ marginTop: 8 }}><label>Remarks</label><textarea className="tj-textarea" rows={2} value={modalEditRemarks} onChange={e => setModalEditRemarks(e.target.value)} /></div>
                <button className="tj-btn primary" style={{ width: "100%", marginTop: 16 }} onClick={saveReviewEdits}>Save</button>
              </>
            )}

            {modal.type === "edit-sl" && (
              <>
                <div className="tj-modal-title">Update Stop Loss · {modal.symbol}</div>
                <div style={{ fontSize: 12, opacity: 0.75, marginBottom: 10 }}>
                  Applies to total open quantity
                  ({Math.round(fifo.openPositions.find(p => p.symbol === modal.symbol)?.qty || 0)} shares).
                  Risk recalculates as (Avg entry − SL) × total quantity.
                </div>
                <div className="tj-form-field">
                  <label>Stop Loss ₹</label>
                  <input
                    className="tj-input"
                    type="number"
                    step="any"
                    autoFocus
                    value={modalOpenSL}
                    onChange={e => setModalOpenSL(e.target.value)}
                    onKeyDown={e => { if (e.key === "Enter") saveSLEdit(); }}
                  />
                </div>
                <div style={{ display: "flex", gap: 8, marginTop: 16 }}>
                  <button className="tj-btn ghost" style={{ flex: 1 }} onClick={() => setModal(null)}>Cancel</button>
                  <button className="tj-btn primary" style={{ flex: 2 }} onClick={saveSLEdit}>Save Stop Loss</button>
                </div>
              </>
            )}

            {modal.type === "add-setup" && (
              <>
                <div className="tj-modal-title">Add Setup Type</div>
                <div className="tj-form-field"><label>Setup Name</label><input className="tj-input" autoFocus value={newSetupName} onChange={e => setNewSetupName(e.target.value)} placeholder="e.g. Ascending Triangle" /></div>
                <button className="tj-btn primary" style={{ width: "100%", marginTop: 16 }} onClick={saveNewSetup}>Add</button>
              </>
            )}
          </div>
        </div>
      )}
      <NewsModal
        isOpen={newsModalOpen}
        onClose={() => setNewsModalOpen(false)}
        title="News · Open Positions"
        symbols={openPositions.map(p => p.symbol)}
        market={market ?? "india"}
        accentColor="#06d6a0"
      />
    </div>
  );
}
