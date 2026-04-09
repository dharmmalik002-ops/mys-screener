import { useCallback, useEffect, useRef, useState } from "react";
import { getChart, getJournalData, saveJournalData, type MarketKey } from "../lib/api";
import "./TradeJournalPanel.css";

// ─── Types ─────────────────────────────────────────────────────────────────────

type OpenPosCat = "full" | "half" | "quarter";
interface VCP { t?: string; depth?: string; vol?: string; }
interface Trade {
  symbol: string; type: string; qty: number; price: number; date: string;
  setupType: string; stoploss: number; target: number; tags: string[];
  remarks: string; img?: string; vcp?: VCP;
}
interface PosMeta { cmp?: number; sl?: number; fetchTicker?: string; }
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
function EquityCurve({ closed, startEquity }: { closed: ClosedTrade[]; startEquity: number }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current) return;
    const W = containerRef.current.clientWidth || 600, H = 240;
    const pad = { t: 16, r: 20, b: 36, l: 70 };
    const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;

    if (!closed.length) {
      svgRef.current.innerHTML = `<text x="${W / 2}" y="${H / 2}" fill="var(--text-muted)" font-size="13" text-anchor="middle" dominant-baseline="middle">No closed trades yet</text>`;
      svgRef.current.setAttribute("viewBox", `0 0 ${W} ${H}`);
      return;
    }

    const sorted = [...closed].sort((a, b) => getSafeTime(a.exitDate) - getSafeTime(b.exitDate));
    let eq = startEquity;
    const pts: Array<{ x: number; y: number; val: number; date: string }> = [{ x: 0, y: 0, val: eq, date: sorted[0].entryDate }];
    sorted.forEach(t => { eq += t.pnl; pts.push({ x: 0, y: 0, val: eq, date: t.exitDate }); });

    const minV = Math.min(...pts.map(p => p.val));
    const maxV = Math.max(...pts.map(p => p.val));
    const vRange = Math.max(maxV - minV, 1);

    pts.forEach((p, i) => {
      p.x = pad.l + (i / Math.max(pts.length - 1, 1)) * innerW;
      p.y = pad.t + (1 - (p.val - minV) / vRange) * innerH;
    });

    const linePath = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
    const areaPath = linePath + ` L${pts[pts.length - 1].x.toFixed(1)},${(pad.t + innerH).toFixed(1)} L${pad.l},${(pad.t + innerH).toFixed(1)} Z`;

    // Y-axis ticks
    const yTicks = 5;
    const yTickLines = Array.from({ length: yTicks + 1 }, (_, i) => {
      const frac = i / yTicks;
      const val = minV + frac * vRange;
      const y = pad.t + (1 - frac) * innerH;
      return `
        <line x1="${pad.l}" y1="${y.toFixed(1)}" x2="${W - pad.r}" y2="${y.toFixed(1)}" stroke="var(--line)" stroke-width="1"/>
        <text x="${pad.l - 8}" y="${y.toFixed(1)}" fill="var(--text-muted)" font-size="10" text-anchor="end" dominant-baseline="middle">₹${(val / 1000).toFixed(0)}k</text>
      `;
    }).join("");

    // X-axis date labels (up to 6)
    const xLabelCount = Math.min(pts.length, 6);
    const xStep = Math.floor((pts.length - 1) / Math.max(xLabelCount - 1, 1));
    const xLabels = Array.from({ length: xLabelCount }, (_, i) => {
      const pt = pts[Math.min(i * xStep, pts.length - 1)];
      const label = pt.date ? pt.date.slice(0, 7) : "";
      return `<text x="${pt.x.toFixed(1)}" y="${(pad.t + innerH + 16).toFixed(1)}" fill="var(--text-muted)" font-size="9.5" text-anchor="middle">${label}</text>`;
    }).join("");

    const finalVal = pts[pts.length - 1].val;
    const isPositive = finalVal >= startEquity;

    svgRef.current.setAttribute("viewBox", `0 0 ${W} ${H}`);
    svgRef.current.innerHTML = `
      <defs>
        <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${isPositive ? "var(--positive)" : "var(--negative)"}" stop-opacity="0.28"/>
          <stop offset="100%" stop-color="${isPositive ? "var(--positive)" : "var(--negative)"}" stop-opacity="0.02"/>
        </linearGradient>
        <clipPath id="eqClip">
          <rect x="${pad.l}" y="${pad.t}" width="${innerW}" height="${innerH}"/>
        </clipPath>
      </defs>
      <rect x="${pad.l}" y="${pad.t}" width="${innerW}" height="${innerH}" fill="none"/>
      ${yTickLines}
      <g clip-path="url(#eqClip)">
        <path d="${areaPath}" fill="url(#eqGrad)"/>
        <path d="${linePath}" fill="none" stroke="${isPositive ? "var(--positive)" : "var(--negative)"}" stroke-width="2"/>
      </g>
      ${xLabels}
      <line x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${pad.t + innerH}" stroke="var(--line-strong)" stroke-width="1"/>
      <line x1="${pad.l}" y1="${pad.t + innerH}" x2="${W - pad.r}" y2="${pad.t + innerH}" stroke="var(--line-strong)" stroke-width="1"/>
    `;
  }, [closed, startEquity]);

  useEffect(() => {
    draw();
    if (!containerRef.current) return;
    const ro = new ResizeObserver(draw);
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, [draw]);

  return (
    <div ref={containerRef} style={{ width: "100%" }}>
      <svg ref={svgRef} style={{ width: "100%", height: 240, display: "block" }} />
    </div>
  );
}

// ─── P&L Distribution ─────────────────────────────────────────────────────────
function PnlDistribution({ closed }: { closed: ClosedTrade[] }) {
  if (!closed.length) return <div className="tj-placeholder">No closed trades yet</div>;
  const percs = closed.map(c => c.perc);
  const binMap: Record<number, number> = {};
  percs.forEach(p => { const b = Math.round(p); binMap[b] = (binMap[b] || 0) + 1; });
  const bins = Object.entries(binMap).map(([b, c]) => ({ bin: Number(b), count: c })).sort((a, b) => a.bin - b.bin);
  const maxCount = Math.max(...bins.map(b => b.count), 1);

  return (
    <div className="tj-dist-wrap">
      <div className="tj-dist-chart">
        {bins.map(({ bin, count }) => (
          <div key={bin} className="tj-dist-col" title={`${bin > 0 ? "+" : ""}${bin}% : ${count} trade${count !== 1 ? "s" : ""}`}>
            <div className="tj-dist-bar-area">
              <div
                className={`tj-dist-bar ${bin >= 0 ? "pos" : "neg"}`}
                style={{ height: `${(count / maxCount) * 100}%` }}
              />
              {count > 0 && <span className="tj-dist-count">{count}</span>}
            </div>
            <div className="tj-dist-label">{bin > 0 ? "+" : ""}{bin}%</div>
          </div>
        ))}
      </div>
      <div className="tj-dist-stats">
        <span className="tj-dist-stat pos">Winners: {closed.filter(c => c.pnl > 0).length}</span>
        <span className="tj-dist-stat neg">Losers: {closed.filter(c => c.pnl <= 0).length}</span>
        <span className="tj-dist-stat">Avg: {fmtPerc(percs.reduce((a, b) => a + b, 0) / percs.length)}</span>
      </div>
    </div>
  );
}

// ─── Monthly Heatmap ───────────────────────────────────────────────────────────
function Heatmap({ closed }: { closed: ClosedTrade[] }) {
  const byDay: Record<string, number> = {};
  closed.forEach(c => {
    const d = (c.exitDate || "").split("T")[0];
    if (d) byDay[d] = (byDay[d] || 0) + c.pnl;
  });

  const now = new Date();
  const months = Array.from({ length: 12 }, (_, i) => {
    const date = new Date(now.getFullYear(), now.getMonth() - 11 + i, 1);
    const year = date.getFullYear(), month = date.getMonth();
    const label = date.toLocaleString("default", { month: "short", year: "2-digit" });
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const firstDow = new Date(year, month, 1).getDay(); // 0=Sun
    const days = Array.from({ length: daysInMonth }, (_, di) => {
      const dayStr = `${year}-${String(month + 1).padStart(2, "0")}-${String(di + 1).padStart(2, "0")}`;
      return { dayStr, pnl: byDay[dayStr] };
    });
    return { label, days, firstDow };
  });

  function tileClass(pnl: number | undefined) {
    if (pnl === undefined) return "empty";
    if (pnl > 10000) return "win-3";
    if (pnl > 0) return "win-1";
    if (pnl < -10000) return "loss-3";
    return "loss-1";
  }

  const DOW_LABELS = ["S", "M", "T", "W", "T", "F", "S"];

  return (
    <div className="tj-hm-root">
      {months.map(m => (
        <div key={m.label} className="tj-hm-month">
          <div className="tj-hm-month-title">{m.label}</div>
          <div className="tj-hm-dow-row">{DOW_LABELS.map((d, i) => <span key={i} className="tj-hm-dow">{d}</span>)}</div>
          <div className="tj-hm-grid">
            {/* Empty cells before first day */}
            {Array.from({ length: m.firstDow }, (_, i) => <div key={`e${i}`} className="tj-hm-tile empty" />)}
            {m.days.map(({ dayStr, pnl }) => (
              <div
                key={dayStr}
                className={`tj-hm-tile ${tileClass(pnl)}`}
                title={pnl !== undefined ? `${dayStr}: ${fmtPnl(pnl)}` : dayStr}
              />
            ))}
          </div>
        </div>
      ))}
      <div className="tj-hm-legend">
        <span className="tj-hm-tile win-3" /><span>High gain</span>
        <span className="tj-hm-tile win-1" /><span>Gain</span>
        <span className="tj-hm-tile empty" style={{ border: "1px solid var(--line)" }} /><span>No trade</span>
        <span className="tj-hm-tile loss-1" /><span>Loss</span>
        <span className="tj-hm-tile loss-3" /><span>High loss</span>
      </div>
    </div>
  );
}

// ─── Main Component ────────────────────────────────────────────────────────────
export function TradeJournalPanel({ market, addRequest, onAddRequestHandled }: TradeJournalPanelProps) {
  const [activeTab, setActiveTab] = useState(0);
  const [trades, setTrades] = useState<Trade[]>(() => lsGet<Trade[]>(LS_DATA, []));
  const [startEquity, setStartEquity] = useState<number>(() => lsGet<number>(LS_EQUITY, 100000));
  const [setups, setSetups] = useState<string[]>(() => lsGet<string[]>(LS_SETUPS, DEFAULT_SETUPS));
  const [openPosCats, setOpenPosCats] = useState<Record<string, OpenPosCat>>(() => lsGet(LS_POSITIONS, {}));
  const [posMeta, setPosMeta] = useState<Record<string, PosMeta>>(() => lsGet(LS_META, {}));
  const [syncing, setSyncing] = useState(false);
  const [syncStatus, setSyncStatus] = useState<string | null>(null);
  const [fetchingNews, setFetchingNews] = useState(false);
  const [newsItems, setNewsItems] = useState<Array<{ symbol: string; items: Array<{ title: string; link: string; summary: string; date: string }> }>>([]);
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

  // Modals
  type ModalState =
    | null
    | { type: "close-pos"; symbol: string; maxQty: number; cmp: number }
    | { type: "edit-closed"; sellIndex: number; buyIndices: number[] }
    | { type: "edit-open"; symbol: string }
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
    setTrades(next); lsSet(LS_DATA, next);
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
        setTrades(rt); lsSet(LS_DATA, rt);
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
  // Sync once on mount so CMP is fresh even before the user visits the tab
  useEffect(() => {
    syncPricesSilent();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  useEffect(() => {
    if (activeTab === 2) {
      // Sync immediately on tab entry, then every 5 min
      syncPricesSilent();
      autoSyncRef.current = setInterval(syncPricesSilent, 5 * 60 * 1000);
    } else {
      if (autoSyncRef.current) clearInterval(autoSyncRef.current);
    }
    return () => { if (autoSyncRef.current) clearInterval(autoSyncRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
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
    for (const pos of openPositions) {
      try {
        const result = await getChart(pos.symbol, "1D", mkt);
        const price = result.summary?.last_price ?? result.bars[result.bars.length - 1]?.close ?? null;
        if (price && isFinite(price)) {
          if (!updated[pos.symbol]) updated[pos.symbol] = {};
          updated[pos.symbol].cmp = price;
        }
      } catch { /* ignore */ }
      await new Promise(r => setTimeout(r, 100));
    }
    setPosMeta(updated); lsSet(LS_META, updated);
  }

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
        if (price && isFinite(price)) {
          if (!updated[pos.symbol]) updated[pos.symbol] = {};
          updated[pos.symbol].cmp = price;
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
    setModalOpenSL(String(meta.sl || ""));
    setModalOpenFetchTicker(meta.fetchTicker || (symbol.includes(".") ? symbol : symbol + ".NS"));
    const tags = fifo.openPositions.find(p => p.symbol === symbol)?.tags || [];
    setModalEditTags(new Set(tags));
    setModalEditRemarks(fifo.openPositions.find(p => p.symbol === symbol)?.remarks || "");
    setModal({ type: "edit-open", symbol });
  }

  function saveReviewEdits() {
    if (modal?.type !== "edit-open") return;
    const { symbol } = modal;
    const nextMeta = { ...posMeta, [symbol]: { ...posMeta[symbol], sl: parseFloat(modalOpenSL) || 0, fetchTicker: modalOpenFetchTicker } };
    setPosMeta(nextMeta); lsSet(LS_META, nextMeta);
    const openIdxs = fifo.openPositions.find(p => p.symbol === symbol)?.buyIndices || [];
    const nextTrades = trades.map((t, i) => openIdxs.includes(i) ? { ...t, tags: [...modalEditTags], remarks: modalEditRemarks } : t);
    saveTrades(nextTrades); setModal(null);
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

  // ── News fetch ────────────────────────────────────────────────────────────
  async function fetchNews() {
    if (!openPositions.length) { alert("No open positions."); return; }
    setFetchingNews(true); setNewsItems([]);
    const results: typeof newsItems = [];
    for (const pos of openPositions) {
      const ticker = posMeta[pos.symbol]?.fetchTicker || (pos.symbol.includes(".") ? pos.symbol : pos.symbol + ".NS");
      let items: typeof results[0]["items"] = [];
      try {
        const cb = Date.now();
        let feedUrl = `https://feeds.finance.yahoo.com/rss/2.0/headline?s=${ticker}&region=IN&lang=en-IN&cb=${cb}`;
        let res = await fetch(`https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`);
        let data = await res.json();
        if (data.status !== "ok" || !data.items?.length) {
          const q = encodeURIComponent(`${pos.symbol} stock india when:3d`);
          feedUrl = `https://news.google.com/rss/search?q=${q}&hl=en-IN&gl=IN&ceid=IN:en&cb=${cb}`;
          res = await fetch(`https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`);
          data = await res.json();
        }
        if (data.status === "ok" && data.items?.length) {
          items = data.items.slice(0, 5).map((item: { title: string; link: string; description?: string; pubDate?: string }) => {
            let summary = (item.description || "").replace(/<[^>]*>/g, "").trim().replace(/&nbsp;/g, " ").replace(/&amp;/g, "&");
            if (summary.length < 20) summary = "Click to view full article.";
            else if (summary.length > 200) summary = summary.slice(0, 200) + "…";
            let dateStr = item.pubDate || "";
            try { dateStr = new Date(item.pubDate?.replace(/-/g, "/") || "").toLocaleString([], { dateStyle: "medium", timeStyle: "short" }); } catch { /* ignore */ }
            return { title: item.title, link: item.link, summary, date: dateStr };
          });
        }
      } catch { /* ignore */ }
      results.push({ symbol: pos.symbol, items });
      await new Promise(r => setTimeout(r, 600));
    }
    setNewsItems(results); setFetchingNews(false);
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
    const sl = meta.sl || p.avgPx * 0.92;
    const uPnl = cmp > 0 ? (cmp - p.avgPx) * p.qty : 0;
    const uPerc = p.avgPx > 0 && cmp > 0 ? ((cmp - p.avgPx) / p.avgPx) * 100 : 0;
    const riskAmt = (p.avgPx - sl) * p.qty;
    const riskPct = p.avgPx > 0 ? ((p.avgPx - sl) / p.avgPx) * 100 : 0;
    const posSize = startEquity > 0 ? (p.totalInvested / startEquity) * 100 : 0;
    const hasLive = cmp > 0;
    return (
      <div className="tj-kcard" draggable onDragStart={() => onDragStart(p.symbol)}>
        <div className="tj-kcard-header">
          <div className="tj-kcard-sym">
            <span className="tj-kcard-sym-text">{p.symbol}</span>
            {p.setupType && <span className="tj-kcard-setup">{p.setupType}</span>}
          </div>
          <span className="tj-kcard-qty">×{Math.round(p.qty)}</span>
        </div>
        <div className="tj-kcard-metrics">
          <div className="tj-kcard-metric"><span className="tj-kcard-ml">Avg</span><span>₹{fmt(p.avgPx)}</span></div>
          {hasLive && <div className="tj-kcard-metric"><span className="tj-kcard-ml">CMP</span><span className="tj-kcard-cmp">₹{fmt(cmp)}</span></div>}
          {hasLive && <div className={`tj-kcard-metric tj-kcard-pnl ${uPnl >= 0 ? "pos" : "neg"}`}><span className="tj-kcard-ml">P&L</span><span>{fmtPnl(uPnl)} <small>({fmtPerc(uPerc)})</small></span></div>}
          <div className="tj-kcard-metric"><span className="tj-kcard-ml">Risk</span><span className="neg">₹{fmt(riskAmt, 0)} <small>({riskPct.toFixed(1)}%)</small></span></div>
          <div className="tj-kcard-metric"><span className="tj-kcard-ml">Size</span><span>{posSize.toFixed(1)}%</span></div>
        </div>
        {p.tags.length > 0 && <div className="tj-chip-row">{p.tags.slice(0, 4).map(t => <span key={t} className="tj-chip sm">{t}</span>)}</div>}
        <div className="tj-kcard-actions">
          <button className="tj-action-btn danger-outline" onClick={() => openCloseModal(p.symbol, p.qty, cmp || p.avgPx)}>Close</button>
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
        {["Dashboard", "Trade Log", "Open Positions", "Smart Entry", "Insights", "Position Sizer", "News Radar"].map((t, i) => (
          <button
            key={t}
            className={`tj-tabbtn ${activeTab === i ? "active" : ""}`}
            onClick={() => { setActiveTab(i); if (i === 6 && !fetchingNews && !newsItems.length) fetchNews(); }}
          >
            {t}
            {i === 2 && openPositions.length > 0 && <span className="tj-tabbadge">{openPositions.length}</span>}
          </button>
        ))}
      </div>

      {/* ── Tab 0: Dashboard ── */}
      {activeTab === 0 && (
        <div className="tj-page">
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

          <div className="tj-chart-row">
            <div className="tj-card full-width">
              <div className="tj-card-hdr">Equity Curve</div>
              <EquityCurve closed={closedTrades} startEquity={startEquity} />
            </div>
          </div>

          <div className="tj-chart-row two-col">
            <div className="tj-card">
              <div className="tj-card-hdr">P&L Distribution</div>
              <PnlDistribution closed={closedTrades} />
            </div>
            <div className="tj-card">
              <div className="tj-card-hdr">Top Winners vs Losers</div>
              <div className="tj-wl-grid">
                <div>
                  <div className="tj-wl-title pos">▲ Top Winners</div>
                  {top10Win.length === 0 ? <div className="tj-empty">No winners yet</div> : top10Win.map((t, i) => (
                    <div key={i} className="tj-wl-row">
                      <span className="tj-wl-rank">#{i + 1}</span>
                      <span className="tj-wl-sym">{t.symbol}</span>
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
                      <span className="tj-wl-sym">{t.symbol}</span>
                      <span className="neg">{fmtPerc(t.perc)}</span>
                      <span className="neg tj-wl-pnl">{fmtPnl(t.pnl)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <div className="tj-card">
            <div className="tj-card-hdr">Monthly Consistency</div>
            <Heatmap closed={closedTrades} />
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
                      <td className="tj-sym-cell">{t.symbol}</td>
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
              <div className="tj-km"><span>Deployed</span><strong>₹{fmt(totalInvested, 0)}</strong></div>
              <div className="tj-km"><span>Positions</span><strong>{openPositions.length}</strong></div>
              <div className={`tj-km ${totalUnrealized >= 0 ? "pos" : "neg"}`}><span>Unrealized P&L</span><strong>{fmtPnl(totalUnrealized)}</strong></div>
              <div className="tj-km neg"><span>Total Risk</span><strong>₹{fmt(totalRisk, 0)}</strong></div>
            </div>
            <div className="tj-kanban-actions">
              {syncStatus && <span className="tj-sync-status">{syncStatus}</span>}
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

      {/* ── Tab 6: News Radar ── */}
      {activeTab === 6 && (
        <div className="tj-page">
          <div className="tj-news-topbar">
            <span className="tj-card-hdr" style={{ margin: 0 }}>News for Open Positions</span>
            <button className={`tj-btn primary ${fetchingNews ? "loading" : ""}`} onClick={fetchNews} disabled={fetchingNews}>
              {fetchingNews ? "Fetching…" : "⟳ Refresh"}
            </button>
          </div>
          {fetchingNews && <div className="tj-loading-state">Fetching latest headlines…</div>}
          {!fetchingNews && newsItems.length === 0 && openPositions.length === 0 && <div className="tj-empty-page">No open positions.</div>}
          {!fetchingNews && newsItems.length === 0 && openPositions.length > 0 && <div className="tj-empty-page">Click Refresh to load headlines.</div>}
          <div className="tj-news-grid">
            {newsItems.map(n => (
              <div key={n.symbol} className="tj-news-card">
                <div className="tj-news-sym">📰 {n.symbol}</div>
                {n.items.length === 0 ? <div className="tj-news-none">No recent news found.</div> : n.items.map((item, i) => (
                  <div key={i} className="tj-news-item">
                    <a href={item.link} target="_blank" rel="noopener noreferrer" className="tj-news-title">{item.title}</a>
                    <div className="tj-news-summary">{item.summary}</div>
                    <div className="tj-news-date">🕐 {item.date}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      )}

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
                <div className="tj-form-field"><label>Stop Loss ₹</label><input className="tj-input" type="number" step="any" value={modalOpenSL} onChange={e => setModalOpenSL(e.target.value)} /></div>
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
    </div>
  );
}
