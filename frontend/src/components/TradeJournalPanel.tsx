import { useCallback, useEffect, useRef, useState } from "react";
import "./TradeJournalPanel.css";

// ─── Types ─────────────────────────────────────────────────────────────────────

type OpenPosCat = "full" | "half" | "quarter";

interface VCP {
  t?: string;
  depth?: string;
  vol?: string;
}

interface Trade {
  symbol: string;
  type: "Buy" | "Sell" | string;
  qty: number;
  price: number;
  date: string;
  setupType: string;
  stoploss: number;
  target: number;
  tags: string[];
  remarks: string;
  img?: string;
  vcp?: VCP;
}

interface PosMeta {
  cmp?: number;
  sl?: number;
  fetchTicker?: string;
}

interface ClosedTrade {
  symbol: string;
  qty: number;
  entryPx: number;
  exitPx: number;
  entryDate: string;
  exitDate: string;
  pnl: number;
  perc: number;
  setupType: string;
  tags: string[];
  remarks: string;
  img?: string;
  vcp?: VCP;
  equitySnapshot: number;
  posSizePct: number;
  sellIndex: number;
  buyIndices: number[];
}

interface OpenPosition {
  symbol: string;
  qty: number;
  avgPx: number;
  totalInvested: number;
  buyIndices: number[];
  tags: string[];
  remarks: string;
  img?: string;
  setupType?: string;
}

interface FIFOResult {
  closedTrades: ClosedTrade[];
  openPositions: OpenPosition[];
  currentEquity: number;
  openLotsDict: Record<string, Trade[]>;
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
  "Followed Plan", "Broke Plan", "Emotional"
];

const DEFAULT_SETUPS = ["VCP", "Flat Base", "Cup & Handle", "Breakout", "Pullback", "Stage 2", "Other"];

// ─── localStorage helpers ──────────────────────────────────────────────────────

function lsGet<T>(key: string, fallback: T): T {
  try {
    const s = localStorage.getItem(key);
    return s ? JSON.parse(s) as T : fallback;
  } catch { return fallback; }
}
function lsSet(key: string, val: unknown) {
  try { localStorage.setItem(key, JSON.stringify(val)); } catch { /* ignore */ }
}

// ─── FIFO Calculation ──────────────────────────────────────────────────────────

function getSafeTime(dateStr: string): number {
  if (!dateStr) return 0;
  const d = new Date(dateStr.includes("T") ? dateStr : dateStr + "T12:00:00");
  return isNaN(d.getTime()) ? 0 : d.getTime();
}

function calculateFIFO(trades: Trade[], startEquity: number): FIFOResult {
  const sorted = [...trades].sort((a, b) => {
    const ta = getSafeTime(a.date), tb = getSafeTime(b.date);
    if (ta !== tb) return ta - tb;
    if (a.type.toLowerCase() === "buy" && b.type.toLowerCase() !== "buy") return -1;
    if (a.type.toLowerCase() !== "buy" && b.type.toLowerCase() === "buy") return 1;
    return 0;
  });

  const originalIndex = (t: Trade) => trades.findIndex(x => x === t);

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
      if (!buyQueues[sym] || buyQueues[sym].length === 0) return;
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
        const posSizePct = currentEquity > 0 ? ((entryPx * matched) / currentEquity) * 100 : 0;
        currentEquity += pnl;
        buyIndices.push(lot.origIdx);
        closedTrades.push({
          symbol: sym,
          qty: matched,
          entryPx,
          exitPx,
          entryDate: lot.trade.date,
          exitDate: trade.date,
          pnl,
          perc,
          setupType: lot.trade.setupType || trade.setupType || "",
          tags: [...(lot.trade.tags || [])],
          remarks: lot.trade.remarks || "",
          img: lot.trade.img,
          vcp: lot.trade.vcp,
          equitySnapshot: currentEquity,
          posSizePct,
          sellIndex: sellOrigIdx,
          buyIndices,
        });
        lot.remaining -= matched;
        toSell -= matched;
        if (lot.remaining <= 0) buyQueues[sym].shift();
      }
    }
  });

  // Build open positions
  const openPositions: OpenPosition[] = [];
  Object.entries(buyQueues).forEach(([sym, lots]) => {
    const active = lots.filter(l => l.remaining > 0);
    if (!active.length) return;
    let totalQty = 0, totalInvested = 0;
    const buyIndices: number[] = [];
    const tags: string[] = [];
    let remarks = "";
    let img: string | undefined;
    let setupType: string | undefined;
    active.forEach(l => {
      totalQty += l.remaining;
      totalInvested += l.remaining * (Number(l.trade.price) || 0);
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

// ─── Format helpers ────────────────────────────────────────────────────────────

function fmt(n: number, dec = 2): string {
  return n.toLocaleString("en-IN", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}
function fmtPnl(n: number): string {
  return `${n >= 0 ? "+" : ""}₹${fmt(Math.abs(n))}`;
}
function fmtPerc(n: number): string {
  return `${n >= 0 ? "+" : ""}${fmt(Math.abs(n))}%`;
}

// ─── Equity Chart (SVG, no D3 needed) ─────────────────────────────────────────

function EquityCurve({ closed, startEquity }: { closed: ClosedTrade[]; startEquity: number }) {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current || !closed.length) return;
    const sorted = [...closed].sort((a, b) => getSafeTime(a.exitDate) - getSafeTime(b.exitDate));
    let eq = startEquity;
    const pts: Array<{ x: number; y: number; val: number }> = [{ x: 0, y: 0, val: eq }];
    sorted.forEach(t => { eq += t.pnl; pts.push({ x: 0, y: 0, val: eq }); });

    const W = svgRef.current.clientWidth || 560, H = 220;
    const pad = { t: 12, r: 16, b: 30, l: 52 };
    const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
    const minV = Math.min(...pts.map(p => p.val));
    const maxV = Math.max(...pts.map(p => p.val));
    const vRange = maxV - minV || 1;

    pts.forEach((p, i) => {
      p.x = pad.l + (i / (pts.length - 1 || 1)) * innerW;
      p.y = pad.t + (1 - (p.val - minV) / vRange) * innerH;
    });

    const pathD = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
    const areaD = pathD + ` L${pts[pts.length - 1].x.toFixed(1)},${(pad.t + innerH).toFixed(1)} L${pad.l},${(pad.t + innerH).toFixed(1)} Z`;

    const svg = svgRef.current;
    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    svg.innerHTML = `
      <defs>
        <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--accent)" stop-opacity="0.35"/>
          <stop offset="100%" stop-color="var(--accent)" stop-opacity="0.03"/>
        </linearGradient>
      </defs>
      <path d="${areaD}" fill="url(#eqGrad)"/>
      <path d="${pathD}" fill="none" stroke="var(--accent)" stroke-width="2"/>
      ${pts.filter((_, i) => i === 0 || i === pts.length - 1).map(p =>
        `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="4" fill="var(--accent)"/>`
      ).join("")}
      <line x1="${pad.l}" y1="${pad.t}" x2="${pad.l}" y2="${pad.t + innerH}" stroke="var(--line-strong)" stroke-width="1"/>
      <line x1="${pad.l}" y1="${pad.t + innerH}" x2="${W - pad.r}" y2="${pad.t + innerH}" stroke="var(--line-strong)" stroke-width="1"/>
      <text x="${pad.l - 4}" y="${pad.t + 4}" fill="var(--text-muted)" font-size="10" text-anchor="end">₹${fmt(maxV, 0)}</text>
      <text x="${pad.l - 4}" y="${pad.t + innerH}" fill="var(--text-muted)" font-size="10" text-anchor="end">₹${fmt(minV, 0)}</text>
    `;
  }, [closed, startEquity]);

  if (!closed.length) return <div className="tj-empty-chart">No closed trades yet</div>;
  return <svg ref={svgRef} className="tj-equity-svg" style={{ width: "100%", height: 220 }} />;
}

// ─── Bell Curve ────────────────────────────────────────────────────────────────

function BellCurve({ closed }: { closed: ClosedTrade[] }) {
  if (!closed.length) return <div className="tj-empty-chart">No data</div>;
  const percs = closed.map(c => c.perc);
  const bins: Record<number, { count: number; positive: boolean }> = {};
  percs.forEach(p => {
    const bin = Math.round(p);
    if (!bins[bin]) bins[bin] = { count: 0, positive: p >= 0 };
    bins[bin].count++;
  });
  const binArr = Object.entries(bins).sort((a, b) => Number(a[0]) - Number(b[0]));
  const maxCount = Math.max(...binArr.map(([, v]) => v.count), 1);
  return (
    <div className="tj-bell">
      {binArr.map(([bin, { count, positive }]) => (
        <div key={bin} className="tj-bell-col">
          <div
            className={`tj-bell-bar ${positive ? "win" : "loss"}`}
            style={{ height: `${(count / maxCount) * 100}%` }}
            title={`${bin}%: ${count} trade${count !== 1 ? "s" : ""}`}
          />
          <div className="tj-bell-label">{bin}%</div>
        </div>
      ))}
    </div>
  );
}

// ─── Monthly Heatmap ───────────────────────────────────────────────────────────

function Heatmap({ closed }: { closed: ClosedTrade[] }) {
  const byDay: Record<string, number> = {};
  closed.forEach(c => {
    const d = c.exitDate?.split("T")[0] || c.exitDate;
    if (!d) return;
    byDay[d] = (byDay[d] || 0) + c.pnl;
  });

  const now = new Date();
  const months = Array.from({ length: 12 }, (_, i) => {
    const d = new Date(now.getFullYear(), now.getMonth() - 11 + i, 1);
    const year = d.getFullYear(), month = d.getMonth();
    const label = d.toLocaleString("default", { month: "short" }) + " " + year;
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const days = Array.from({ length: daysInMonth }, (_, di) => {
      const day = String(di + 1).padStart(2, "0");
      const key = `${year}-${String(month + 1).padStart(2, "0")}-${day}`;
      const pnl = byDay[key];
      return { key, pnl };
    });
    return { label, days };
  });

  function dayClass(pnl: number | undefined): string {
    if (pnl === undefined) return "tj-hm-day empty";
    if (pnl > 0) return pnl > 5000 ? "tj-hm-day win-high" : "tj-hm-day win-low";
    return pnl < -5000 ? "tj-hm-day loss-high" : "tj-hm-day loss-low";
  }

  return (
    <div className="tj-heatmap-grid">
      {months.map(m => (
        <div key={m.label} className="tj-hm-month">
          <div className="tj-hm-label">{m.label}</div>
          <div className="tj-hm-days">
            {m.days.map(d => (
              <div key={d.key} className={dayClass(d.pnl)} title={d.pnl !== undefined ? `${d.key}: ₹${fmt(d.pnl)}` : d.key} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Main Component ────────────────────────────────────────────────────────────

export function TradeJournalPanel() {
  const [activeTab, setActiveTab] = useState(0);
  const [trades, setTrades] = useState<Trade[]>(() => lsGet<Trade[]>(LS_DATA, []));
  const [startEquity, setStartEquity] = useState<number>(() => lsGet<number>(LS_EQUITY, 100000));
  const [setups, setSetups] = useState<string[]>(() => lsGet<string[]>(LS_SETUPS, DEFAULT_SETUPS));
  const [openPosCats, setOpenPosCats] = useState<Record<string, OpenPosCat>>(() => lsGet(LS_POSITIONS, {}));
  const [posMeta, setPosMeta] = useState<Record<string, PosMeta>>(() => lsGet(LS_META, {}));
  const [syncing, setSyncing] = useState(false);
  const [fetchingNews, setFetchingNews] = useState(false);
  const [newsItems, setNewsItems] = useState<Array<{ symbol: string; items: Array<{ title: string; link: string; summary: string; date: string }> }>>([]);

  // Smart Entry form state
  const [entrySymbol, setEntrySymbol] = useState("");
  const [entryType, setEntryType] = useState("Buy");
  const [entryQty, setEntryQty] = useState("");
  const [entryPrice, setEntryPrice] = useState("");
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

  // Calc state
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

  // Closed trade filters
  const [filterMonth, setFilterMonth] = useState("all");
  const [filterOutcome, setFilterOutcome] = useState("all");
  const [filterSymbol, setFilterSymbol] = useState("");

  // Modal
  const [modal, setModal] = useState<null | { type: "close-pos"; symbol: string; maxQty: number; cmp: number } | { type: "edit-closed"; sellIndex: number; buyIndices: number[] } | { type: "edit-open"; symbol: string } | { type: "add-setup" }>(null);
  const [modalClosePrice, setModalClosePrice] = useState("");
  const [modalCloseQty, setModalCloseQty] = useState("");
  const [modalCloseDate, setModalCloseDate] = useState(() => new Date().toISOString().split("T")[0]);
  const [modalEditEntryPx, setModalEditEntryPx] = useState("");
  const [modalEditExitPx, setModalEditExitPx] = useState("");
  const [modalEditTags, setModalEditTags] = useState<Set<string>>(new Set());
  const [modalEditRemarks, setModalEditRemarks] = useState("");
  const [modalEditImg, setModalEditImg] = useState("");
  const [modalEditCustomTags, setModalEditCustomTags] = useState("");
  const [modalOpenAvgPx, setModalOpenAvgPx] = useState("");
  const [modalOpenSL, setModalOpenSL] = useState("");
  const [modalOpenFetchTicker, setModalOpenFetchTicker] = useState("");
  const [newSetupName, setNewSetupName] = useState("");

  const [equityInput, setEquityInput] = useState(String(startEquity));

  // Drag source
  const dragSymbol = useRef<string | null>(null);

  // ── Persist whenever trades change ──
  const saveTrades = useCallback((next: Trade[]) => {
    setTrades(next);
    lsSet(LS_DATA, next);
  }, []);

  // ── FIFO ──
  const fifo = calculateFIFO(trades, startEquity);

  // ── Dashboard stats ──
  const { closedTrades, openPositions } = fifo;
  const totalPnl = closedTrades.reduce((s, t) => s + t.pnl, 0);
  const winners = closedTrades.filter(t => t.pnl > 0);
  const losers = closedTrades.filter(t => t.pnl < 0);
  const winRate = closedTrades.length > 0 ? (winners.length / closedTrades.length) * 100 : 0;
  const totalInvested = openPositions.reduce((s, p) => s + p.totalInvested, 0);
  const avgPosSize = closedTrades.length > 0 ? closedTrades.reduce((s, t) => s + t.posSizePct, 0) / closedTrades.length : 0;

  // ── Top 10 ──
  const top10Win = [...winners].sort((a, b) => b.perc - a.perc).slice(0, 10);
  const top10Loss = [...losers].sort((a, b) => a.perc - b.perc).slice(0, 10);

  // ── Filtered closed trades ──
  const filteredClosed = closedTrades.filter(t => {
    if (filterOutcome === "win" && t.pnl <= 0) return false;
    if (filterOutcome === "loss" && t.pnl >= 0) return false;
    if (filterSymbol && !t.symbol.toLowerCase().includes(filterSymbol.toLowerCase())) return false;
    if (filterMonth !== "all") {
      const [fy, fm] = filterMonth.split("-").map(Number);
      const exitD = new Date(t.exitDate);
      if (exitD.getFullYear() !== fy || exitD.getMonth() + 1 !== fm) return false;
    }
    return true;
  });

  // unique months for dropdown
  const monthOptions = Array.from(
    new Set(closedTrades.map(t => t.exitDate?.slice(0, 7)).filter(Boolean))
  ).sort().reverse();

  // ── Insights ──
  const setupMap: Record<string, { wins: number; losses: number; pnl: number }> = {};
  closedTrades.forEach(t => {
    const s = t.setupType || "Unknown";
    if (!setupMap[s]) setupMap[s] = { wins: 0, losses: 0, pnl: 0 };
    if (t.pnl > 0) setupMap[s].wins++;
    else setupMap[s].losses++;
    setupMap[s].pnl += t.pnl;
  });

  const tagMap: Record<string, { closedCount: number; openCount: number; realizedPnl: number; unrealizedPnl: number }> = {};
  closedTrades.forEach(t => {
    (t.tags || []).forEach(tag => {
      if (!tagMap[tag]) tagMap[tag] = { closedCount: 0, openCount: 0, realizedPnl: 0, unrealizedPnl: 0 };
      tagMap[tag].closedCount++;
      tagMap[tag].realizedPnl += t.pnl;
    });
  });
  openPositions.forEach(p => {
    const cmp = (posMeta[p.symbol]?.cmp || p.avgPx);
    const uPnl = (cmp - p.avgPx) * p.qty;
    (p.tags || []).forEach(tag => {
      if (!tagMap[tag]) tagMap[tag] = { closedCount: 0, openCount: 0, realizedPnl: 0, unrealizedPnl: 0 };
      tagMap[tag].openCount++;
      tagMap[tag].unrealizedPnl += uPnl;
    });
  });

  const allHoldTimes = closedTrades.map(t => Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000));
  const avgHoldAll = allHoldTimes.length ? allHoldTimes.reduce((a, b) => a + b, 0) / allHoldTimes.length : 0;
  const winHolds = closedTrades.filter(t => t.pnl > 0).map(t => Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000));
  const avgHoldWin = winHolds.length ? winHolds.reduce((a, b) => a + b, 0) / winHolds.length : 0;
  const lossHolds = closedTrades.filter(t => t.pnl <= 0).map(t => Math.max(0, (getSafeTime(t.exitDate) - getSafeTime(t.entryDate)) / 86400000));
  const avgHoldLoss = lossHolds.length ? lossHolds.reduce((a, b) => a + b, 0) / lossHolds.length : 0;

  // ── Position sizer calc ──
  useEffect(() => {
    const eq = parseFloat(sizerEquity) || 0;
    const rp = parseFloat(sizerRiskPct) || 0;
    const en = parseFloat(sizerEntry) || 0;
    const sp = parseFloat(sizerSLPct) || 0;
    if (eq > 0 && rp > 0 && en > 0 && sp > 0) {
      const riskAmt = eq * (rp / 100);
      const slPx = en - en * (sp / 100);
      const rps = en - slPx;
      if (rps > 0) {
        const qty = Math.floor(riskAmt / rps);
        setSizerResultQty(qty);
        setSizerResultSL(slPx);
        setSizerResultRisk(riskAmt);
        setSizerResultPos(qty * en);
        return;
      }
    }
    setSizerResultQty(0); setSizerResultSL(0); setSizerResultRisk(0); setSizerResultPos(0);
  }, [sizerEquity, sizerRiskPct, sizerEntry, sizerSLPct]);

  // ── Quick calc ──
  function runCalc() {
    const cap = parseFloat(calcCap) || 0;
    const rp = parseFloat(calcRisk) || 0;
    const en = parseFloat(calcEntry) || 0;
    const st = parseFloat(calcStop) || 0;
    if (en > st && (en - st) > 0) {
      const qty = Math.floor((cap * (rp / 100)) / (en - st));
      setCalcQtyRes(`${qty} Qty`);
      setEntryQty(String(qty));
      setEntryPrice(calcEntry);
      setEntrySL(calcStop);
    }
  }

  // ── Export / Import ──
  function exportJSON() {
    const blob = new Blob([JSON.stringify({ trades, startEquity, setups, openPosCats, posMeta }, null, 2)], { type: "application/json" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
    a.download = `TradeJournal_${new Date().toISOString().slice(0, 10)}.json`; a.click();
  }

  function importJSON(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]; if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      try {
        const data = JSON.parse(ev.target?.result as string);
        if (Array.isArray(data.trades)) { saveTrades(data.trades); }
        if (data.startEquity) { setStartEquity(data.startEquity); lsSet(LS_EQUITY, data.startEquity); setEquityInput(String(data.startEquity)); }
        if (data.setups) { setSetups(data.setups); lsSet(LS_SETUPS, data.setups); }
        if (data.openPosCats) { setOpenPosCats(data.openPosCats); lsSet(LS_POSITIONS, data.openPosCats); }
        if (data.posMeta) { setPosMeta(data.posMeta); lsSet(LS_META, data.posMeta); }
        alert("Journal imported successfully!");
      } catch { alert("Invalid JSON file."); }
    };
    reader.readAsText(file);
    e.target.value = "";
  }

  // ── Add Trade ──
  function handleAddTrade(e: React.FormEvent) {
    e.preventDefault();
    const customTags = customTagInput.split(",").map(s => s.trim()).filter(Boolean);
    const finalTags = [...entryTags, ...customTags];
    const t: Trade = {
      symbol: entrySymbol.trim().toUpperCase(),
      type: entryType,
      qty: parseFloat(entryQty) || 0,
      price: parseFloat(entryPrice) || 0,
      date: entryDate,
      setupType: entrySetup,
      stoploss: parseFloat(entrySL) || 0,
      target: parseFloat(entryTarget) || 0,
      tags: finalTags,
      remarks: entryRemarks,
      img: entryImg,
      vcp: { t: vcpT, depth: vcpDepth, vol: vcpVol },
    };
    const next = [...trades, t];
    saveTrades(next);
    // Reset form
    setEntrySymbol(""); setEntryQty(""); setEntryPrice(""); setEntrySL("");
    setEntryTarget(""); setEntryImg(""); setEntryRemarks(""); setEntryTags(new Set());
    setCustomTagInput(""); setVcpT(""); setVcpDepth(""); setVcpVol("");
    setCheckboxes(Array(6).fill(false));
    alert("Trade added!");
    setActiveTab(entryType.toLowerCase() === "buy" ? 2 : 1);
  }

  // ── Delete trade ──
  function deleteTrade(idx: number) {
    if (!confirm("Delete this trade?")) return;
    const next = trades.filter((_, i) => i !== idx);
    saveTrades(next);
  }

  // ── Close position modal ──
  function openCloseModal(symbol: string, maxQty: number, cmp: number) {
    setModalClosePrice(String(cmp || ""));
    setModalCloseQty(String(maxQty));
    setModalCloseDate(new Date().toISOString().split("T")[0]);
    setModal({ type: "close-pos", symbol, maxQty, cmp });
  }

  function submitClose() {
    if (modal?.type !== "close-pos") return;
    const { symbol } = modal;
    const price = parseFloat(modalClosePrice);
    const qty = parseFloat(modalCloseQty);
    if (isNaN(price) || isNaN(qty) || !modalCloseDate) { alert("Please fill all fields."); return; }

    const openLots = fifo.openLotsDict[symbol];
    if (openLots && openLots[0]) {
      const buyTime = getSafeTime(openLots[0].date);
      const sellTime = getSafeTime(modalCloseDate);
      if (sellTime < buyTime) {
        alert(`Close date (${modalCloseDate}) is before buy date (${openLots[0].date}).\nPlease use a later date.`);
        return;
      }
    }

    const existingTags = (fifo.openLotsDict[symbol] || []).flatMap(l => l.tags || []);
    saveTrades([...trades, {
      symbol, type: "Sell", qty, price, date: modalCloseDate,
      setupType: "Close", tags: [...new Set(existingTags)], remarks: ""
    }]);
    setModal(null);
    alert("Position closed!");
    setActiveTab(1);
  }

  // ── Edit open position (review modal) ──
  function openReviewModal(symbol: string) {
    const meta = posMeta[symbol] || {};
    setModalOpenAvgPx(String(fifo.openPositions.find(p => p.symbol === symbol)?.avgPx.toFixed(2) || ""));
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

    // Also update tags/remarks on open buy lots
    const openIdxs = fifo.openPositions.find(p => p.symbol === symbol)?.buyIndices || [];
    const nextTrades = trades.map((t, i) => {
      if (openIdxs.includes(i)) return { ...t, tags: [...modalEditTags], remarks: modalEditRemarks };
      return t;
    });
    saveTrades(nextTrades);
    setModal(null);
  }

  // ── Edit closed trade modal ──
  function openEditClosedModal(sellIndex: number, buyIndices: number[]) {
    const sellTrade = trades[sellIndex];
    const origTrade = trades[buyIndices[0]] || {};
    let totalQty = 0, totalInvested = 0;
    buyIndices.forEach(i => { if (trades[i]) { totalQty += trades[i].qty; totalInvested += trades[i].qty * trades[i].price; } });
    const avgEntry = totalQty > 0 ? totalInvested / totalQty : 0;
    setModalEditEntryPx(avgEntry.toFixed(2));
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
    const newEntryPx = parseFloat(modalEditEntryPx);
    const newExitPx = parseFloat(modalEditExitPx);

    const nextTrades = trades.map((t, i) => {
      if (buyIndices.includes(i)) return { ...t, tags: finalTags, remarks: modalEditRemarks, img: modalEditImg, ...(!isNaN(newEntryPx) && newEntryPx > 0 ? { price: newEntryPx } : {}) };
      if (i === sellIndex && !isNaN(newExitPx) && newExitPx > 0) return { ...t, price: newExitPx };
      return t;
    });
    saveTrades(nextTrades);
    setModal(null);
    alert("Trade updated!");
  }

  // ── Drag and drop (kanban) ──
  function onDragStart(symbol: string) { dragSymbol.current = symbol; }
  function onDrop(cat: OpenPosCat) {
    if (!dragSymbol.current) return;
    const next = { ...openPosCats, [dragSymbol.current]: cat };
    setOpenPosCats(next); lsSet(LS_POSITIONS, next);
    dragSymbol.current = null;
  }

  // ── Live price sync ──
  async function syncPrices() {
    if (!openPositions.length) { alert("No open positions."); return; }
    setSyncing(true);
    const updated: Record<string, PosMeta> = { ...posMeta };
    const failed: string[] = [];

    for (const pos of openPositions) {
      let ticker = updated[pos.symbol]?.fetchTicker || (pos.symbol.includes(".") ? pos.symbol : pos.symbol + ".NS");
      let price: number | null = null;

      try {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d`;
        const proxy = `https://corsproxy.io/?${encodeURIComponent(url)}`;
        const res = await fetch(proxy);
        const json = await res.json();
        if (json.chart?.result?.[0]?.meta?.regularMarketPrice) price = json.chart.result[0].meta.regularMarketPrice;
      } catch { /* try fallback */ }

      if (!price) {
        try {
          const scrapeUrl = `https://finance.yahoo.com/quote/${ticker}`;
          const proxy2 = `https://api.allorigins.win/get?url=${encodeURIComponent(scrapeUrl)}`;
          const res = await fetch(proxy2);
          const data = await res.json();
          const match = data.contents?.match(/"regularMarketPrice":\{"raw":([\d.]+)/);
          if (match) price = parseFloat(match[1]);
        } catch { /* ignore */ }
      }

      if (price && isFinite(price)) {
        if (!updated[pos.symbol]) updated[pos.symbol] = {};
        updated[pos.symbol].cmp = price;
      } else { failed.push(pos.symbol); }

      await new Promise(r => setTimeout(r, 200));
    }

    setPosMeta(updated); lsSet(LS_META, updated);
    setSyncing(false);
    if (failed.length) alert(`Synced ${openPositions.length - failed.length}/${openPositions.length}.\nFailed: ${failed.join(", ")}\n\nTip: Use the Review button to set Yahoo Ticker (e.g., RELIANCE.NS)`);
    else alert(`All ${openPositions.length} prices synced!`);
  }

  // ── News fetch ──
  async function fetchNews() {
    if (!openPositions.length) { alert("No open positions."); return; }
    setFetchingNews(true);
    setNewsItems([]);
    const results: typeof newsItems = [];

    for (const pos of openPositions) {
      const ticker = posMeta[pos.symbol]?.fetchTicker || (pos.symbol.includes(".") ? pos.symbol : pos.symbol + ".NS");
      let items: Array<{ title: string; link: string; summary: string; date: string }> = [];

      try {
        const cb = Date.now();
        let feedUrl = `https://feeds.finance.yahoo.com/rss/2.0/headline?s=${ticker}&region=IN&lang=en-IN&cb=${cb}`;
        let apiUrl = `https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`;
        let res = await fetch(apiUrl);
        let data = await res.json();
        if (data.status !== "ok" || !data.items?.length) {
          const q = encodeURIComponent(`${pos.symbol} stock india when:3d`);
          feedUrl = `https://news.google.com/rss/search?q=${q}&hl=en-IN&gl=IN&ceid=IN:en&cb=${cb}`;
          apiUrl = `https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`;
          res = await fetch(apiUrl); data = await res.json();
        }
        if (data.status === "ok" && data.items?.length) {
          items = data.items.slice(0, 5).map((item: { title: string; link: string; description?: string; pubDate?: string }) => {
            let summary = (item.description || "").replace(/<[^>]*>/g, "").trim()
              .replace(/&nbsp;/g, " ").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/&#39;/g, "'");
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

    setNewsItems(results);
    setFetchingNews(false);
  }

  // ── Kanban column data ──
  function posForCat(cat: OpenPosCat): OpenPosition[] {
    return openPositions.filter(p => (openPosCats[p.symbol] || "full") === cat);
  }

  function KanbanCard({ p }: { p: OpenPosition }) {
    const meta = posMeta[p.symbol] || {};
    const cmp = meta.cmp || 0;
    const sl = meta.sl || p.avgPx * 0.95;
    const uPnl = cmp > 0 ? (cmp - p.avgPx) * p.qty : 0;
    const uPerc = p.avgPx > 0 ? ((cmp - p.avgPx) / p.avgPx) * 100 : 0;
    const riskAmt = (p.avgPx - sl) * p.qty;
    const riskPct = p.avgPx > 0 ? ((p.avgPx - sl) / p.avgPx) * 100 : 0;
    const posSize = startEquity > 0 ? (p.totalInvested / startEquity) * 100 : 0;

    return (
      <div
        className="tj-kanban-card"
        draggable
        onDragStart={() => onDragStart(p.symbol)}
      >
        <div className="tj-kc-header">
          <span className="tj-kc-sym">{p.symbol}</span>
          <span className="tj-kc-meta">{"×" + p.qty}</span>
        </div>
        <div className="tj-kc-row"><span className="tj-kc-label">Avg</span><span>₹{fmt(p.avgPx)}</span></div>
        {cmp > 0 && <div className="tj-kc-row"><span className="tj-kc-label">CMP</span><span>₹{fmt(cmp)}</span></div>}
        {cmp > 0 && <div className={`tj-kc-row tj-kc-pnl ${uPnl >= 0 ? "pos" : "neg"}`}><span className="tj-kc-label">P&L</span><span>{fmtPnl(uPnl)} ({fmtPerc(uPerc)})</span></div>}
        <div className="tj-kc-row"><span className="tj-kc-label">Risk</span><span className="neg">₹{fmt(riskAmt)} ({fmt(riskPct)}%)</span></div>
        <div className="tj-kc-row"><span className="tj-kc-label">Size</span><span>{fmt(posSize)}%</span></div>
        {p.setupType && <div className="tj-kc-row"><span className="tj-kc-label">Setup</span><span>{p.setupType}</span></div>}
        {p.tags.length > 0 && <div className="tj-kc-tags">{p.tags.map(t => <span key={t} className="tj-chip">{t}</span>)}</div>}
        <div className="tj-kc-actions">
          <button className="tj-kc-btn" onClick={() => openCloseModal(p.symbol, Math.round(p.qty), cmp)}>Close</button>
          <button className="tj-kc-btn secondary" onClick={() => openReviewModal(p.symbol)}>Review</button>
        </div>
      </div>
    );
  }

  function KanbanCol({ cat, label }: { cat: OpenPosCat; label: string }) {
    const items = posForCat(cat);
    return (
      <div
        className="tj-kanban-col"
        onDragOver={e => e.preventDefault()}
        onDrop={() => onDrop(cat)}
      >
        <div className="tj-kcol-header">{label} <span className="tj-kcol-count">{items.length}</span></div>
        {items.map(p => <KanbanCard key={p.symbol} p={p} />)}
        {items.length === 0 && <div className="tj-kcol-empty">Drop here</div>}
      </div>
    );
  }

  // ── Add Setup ──
  function saveNewSetup() {
    if (!newSetupName.trim()) return;
    const next = [...setups, newSetupName.trim()];
    setSetups(next); lsSet(LS_SETUPS, next);
    setNewSetupName(""); setModal(null);
  }

  const CHECKLIST_ITEMS = [
    "Market/Index trend is favorable (Stage 2 or recovey)",
    "Sector is in leading group or showing strength",
    "Stock is in confirmed Stage 2 uptrend",
    "VCP or technical pattern is properly formed",
    "Volume dry-up confirmed near pivot",
    "Entry is at or near pivot with tight risk (<8%)",
  ];

  // ─── Render ───────────────────────────────────────────────────────────────────
  return (
    <div className="tj-root">
      {/* Header */}
      <div className="tj-header">
        <div className="tj-header-left">
          <span className="tj-logo">📒 TradeOS Journal</span>
          <span className="tj-version">v11.0</span>
        </div>
        <div className="tj-header-right">
          <input
            type="number"
            className="tj-equity-input"
            value={equityInput}
            onChange={e => setEquityInput(e.target.value)}
            onBlur={() => {
              const val = parseFloat(equityInput);
              if (val > 0) { setStartEquity(val); lsSet(LS_EQUITY, val); setSizerEquity(String(val)); setCalcCap(String(val)); }
            }}
            placeholder="Starting Equity ₹"
          />
          <button className="tj-btn-secondary" onClick={exportJSON}>Export</button>
          <label className="tj-btn-secondary" style={{ cursor: "pointer" }}>
            Import
            <input type="file" accept=".json" style={{ display: "none" }} onChange={importJSON} />
          </label>
        </div>
      </div>

      {/* Tabs */}
      <div className="tj-tabs">
        {["Dashboard", "Trade Log", "Open Positions", "Smart Entry", "Insights", "Position Sizer", "News Radar"].map((t, i) => (
          <button
            key={t}
            className={`tj-tab ${activeTab === i ? "active" : ""}`}
            onClick={() => { setActiveTab(i); if (i === 6 && !fetchingNews && !newsItems.length) fetchNews(); }}
          >
            {t}
          </button>
        ))}
      </div>

      {/* ─── Tab 0: Dashboard ─── */}
      {activeTab === 0 && (
        <div className="tj-panel">
          <div className="tj-stat-grid">
            <div className="tj-stat-card">
              <div className="tj-stat-label">Total P&L</div>
              <div className={`tj-stat-value ${totalPnl >= 0 ? "pos" : "neg"}`}>{fmtPnl(totalPnl)}</div>
            </div>
            <div className="tj-stat-card">
              <div className="tj-stat-label">Win Rate</div>
              <div className="tj-stat-value">{fmt(winRate)}%</div>
              <div className="tj-stat-sub">{winners.length}W / {losers.length}L / {closedTrades.length} total</div>
            </div>
            <div className="tj-stat-card">
              <div className="tj-stat-label">Avg Position Size</div>
              <div className="tj-stat-value">{fmt(avgPosSize)}%</div>
              <div className="tj-stat-sub">of equity (closed)</div>
            </div>
            <div className="tj-stat-card">
              <div className="tj-stat-label">Open Invested</div>
              <div className="tj-stat-value">₹{fmt(totalInvested, 0)}</div>
              <div className="tj-stat-sub">{openPositions.length} position{openPositions.length !== 1 ? "s" : ""}</div>
            </div>
          </div>

          <div className="tj-dash-row">
            <div className="tj-card tj-chart-card">
              <div className="tj-card-title">Equity Curve</div>
              <EquityCurve closed={closedTrades} startEquity={startEquity} />
            </div>
            <div className="tj-card tj-chart-card">
              <div className="tj-card-title">P&L Distribution (%)</div>
              <BellCurve closed={closedTrades} />
            </div>
          </div>

          <div className="tj-dash-row">
            <div className="tj-card">
              <div className="tj-card-title">Top 10 Winners</div>
              {top10Win.length === 0 ? <div className="tj-empty">No winners yet</div> : (
                <div className="tj-list">
                  {top10Win.map((t, i) => (
                    <div key={i} className="tj-list-row">
                      <span className="tj-list-rank">#{i + 1}</span>
                      <span className="tj-list-sym">{t.symbol}</span>
                      <span className="tj-list-val pos">+{fmt(t.perc)}%</span>
                      <span className="tj-list-sub pos">{fmtPnl(t.pnl)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div className="tj-card">
              <div className="tj-card-title">Top 10 Losers</div>
              {top10Loss.length === 0 ? <div className="tj-empty">No losers yet</div> : (
                <div className="tj-list">
                  {top10Loss.map((t, i) => (
                    <div key={i} className="tj-list-row">
                      <span className="tj-list-rank">#{i + 1}</span>
                      <span className="tj-list-sym">{t.symbol}</span>
                      <span className="tj-list-val neg">{fmt(t.perc)}%</span>
                      <span className="tj-list-sub neg">{fmtPnl(t.pnl)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          <div className="tj-card">
            <div className="tj-card-title">Monthly Consistency Heatmap</div>
            <Heatmap closed={closedTrades} />
          </div>
        </div>
      )}

      {/* ─── Tab 1: Trade Log ─── */}
      {activeTab === 1 && (
        <div className="tj-panel">
          <div className="tj-log-filters">
            <select value={filterMonth} onChange={e => setFilterMonth(e.target.value)} className="tj-select">
              <option value="all">All Months</option>
              {monthOptions.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
            <select value={filterOutcome} onChange={e => setFilterOutcome(e.target.value)} className="tj-select">
              <option value="all">All Outcomes</option>
              <option value="win">Winners</option>
              <option value="loss">Losers</option>
            </select>
            <input className="tj-input" placeholder="Filter by symbol…" value={filterSymbol} onChange={e => setFilterSymbol(e.target.value)} />
            <span className="tj-log-count">{filteredClosed.length} trades</span>
          </div>
          {filteredClosed.length === 0 ? (
            <div className="tj-empty-state">No closed trades found. Add trades in Smart Entry tab.</div>
          ) : (
            <div className="tj-table-wrap">
              <table className="tj-table">
                <thead>
                  <tr>
                    <th>Symbol</th><th>Setup</th><th>Entry ₹</th><th>Exit ₹</th>
                    <th>Entry Date</th><th>Exit Date</th><th>P&L ₹</th><th>%</th>
                    <th>Size %</th><th>Tags</th><th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredClosed.map((t, i) => (
                    <tr key={i} className={t.pnl >= 0 ? "win-row" : "loss-row"}>
                      <td className="tj-sym-cell">{t.symbol}</td>
                      <td>{t.setupType || "—"}</td>
                      <td>{fmt(t.entryPx)}</td>
                      <td>{fmt(t.exitPx)}</td>
                      <td>{t.entryDate}</td>
                      <td>{t.exitDate}</td>
                      <td className={t.pnl >= 0 ? "pos" : "neg"}>{fmtPnl(t.pnl)}</td>
                      <td className={t.perc >= 0 ? "pos" : "neg"}>{fmtPerc(t.perc)}</td>
                      <td>{fmt(t.posSizePct)}%</td>
                      <td>{(t.tags || []).map(tag => <span key={tag} className="tj-chip sm">{tag}</span>)}</td>
                      <td>
                        <button className="tj-kc-btn" onClick={() => openEditClosedModal(t.sellIndex, t.buyIndices)}>Edit</button>
                        <button className="tj-kc-btn danger" style={{ marginLeft: 4 }} onClick={() => deleteTrade(t.sellIndex)}>Del</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ─── Tab 2: Open Positions ─── */}
      {activeTab === 2 && (
        <div className="tj-panel">
          <div className="tj-kanban-header">
            <div className="tj-kanban-stats">
              <span>Invested: <strong>₹{fmt(totalInvested, 0)}</strong></span>
              <span>Positions: <strong>{openPositions.length}</strong></span>
              <span>Total Risk: <strong className="neg">₹{fmt(openPositions.reduce((s, p) => {
                const sl = posMeta[p.symbol]?.sl || p.avgPx * 0.95;
                return s + (p.avgPx - sl) * p.qty;
              }, 0), 0)}</strong></span>
              <span>Unrealized P&L: <strong className={openPositions.reduce((s, p) => {
                const cmp = posMeta[p.symbol]?.cmp || p.avgPx;
                return s + (cmp - p.avgPx) * p.qty;
              }, 0) >= 0 ? "pos" : "neg"}>
                {fmtPnl(openPositions.reduce((s, p) => {
                  const cmp = posMeta[p.symbol]?.cmp || p.avgPx;
                  return s + (cmp - p.avgPx) * p.qty;
                }, 0))}
              </strong></span>
            </div>
            <button className={`tj-btn-primary ${syncing ? "loading" : ""}`} onClick={syncPrices} disabled={syncing}>
              {syncing ? "Syncing…" : "⟳ Sync Prices"}
            </button>
          </div>
          {openPositions.length === 0 ? (
            <div className="tj-empty-state">No open positions. Add a Buy trade in Smart Entry.</div>
          ) : (
            <div className="tj-kanban">
              <KanbanCol cat="full" label="Full Size" />
              <KanbanCol cat="half" label="Half Size" />
              <KanbanCol cat="quarter" label="Pilot + Testing" />
            </div>
          )}
        </div>
      )}

      {/* ─── Tab 3: Smart Entry ─── */}
      {activeTab === 3 && (
        <div className="tj-panel">
          <div className="tj-entry-grid">
            {/* Left — Calculator + Tags */}
            <div className="tj-card">
              <div className="tj-card-title">Quick Calculator</div>
              <div className="tj-form-row">
                <label>Account Size (₹)</label>
                <input className="tj-input" type="number" value={calcCap} onChange={e => setCalcCap(e.target.value)} />
              </div>
              <div className="tj-form-row">
                <label>Risk % per Trade</label>
                <input className="tj-input" type="number" value={calcRisk} onChange={e => setCalcRisk(e.target.value)} />
              </div>
              <div className="tj-form-row">
                <label>Entry Price</label>
                <input className="tj-input" type="number" value={calcEntry} onChange={e => setCalcEntry(e.target.value)} />
              </div>
              <div className="tj-form-row">
                <label>Stop Loss Price</label>
                <input className="tj-input" type="number" value={calcStop} onChange={e => setCalcStop(e.target.value)} />
              </div>
              <button className="tj-btn-primary" style={{ width: "100%", marginTop: 8 }} onClick={runCalc}>Calculate → Apply to Form</button>
              {calcQtyRes && <div className="tj-calc-result">{calcQtyRes}</div>}

              <div className="tj-card-title" style={{ marginTop: 20 }}>Tags & Habits</div>
              <div className="tj-chip-container">
                {PREDEFINED_TAGS.map(tag => (
                  <div
                    key={tag}
                    className={`tj-chip clickable ${entryTags.has(tag) ? "selected" : ""}`}
                    onClick={() => setEntryTags(prev => { const next = new Set(prev); next.has(tag) ? next.delete(tag) : next.add(tag); return next; })}
                  >
                    {tag}
                  </div>
                ))}
              </div>
              <input className="tj-input" style={{ marginTop: 8 }} placeholder="Custom tags (comma separated)…" value={customTagInput} onChange={e => setCustomTagInput(e.target.value)} />
              <div className="tj-form-row" style={{ marginTop: 12 }}>
                <label>Trade Remarks</label>
                <textarea className="tj-textarea" rows={3} placeholder="Notes on this trade…" value={entryRemarks} onChange={e => setEntryRemarks(e.target.value)} />
              </div>
            </div>

            {/* Right — Trade Form */}
            <div className="tj-card">
              <div className="tj-card-title">Add Trade</div>
              <form onSubmit={handleAddTrade}>
                <div className="tj-form-grid-2">
                  <div className="tj-form-row">
                    <label>Symbol</label>
                    <input className="tj-input" required value={entrySymbol} onChange={e => setEntrySymbol(e.target.value)} placeholder="RELIANCE" />
                  </div>
                  <div className="tj-form-row">
                    <label>Type</label>
                    <select className="tj-select" value={entryType} onChange={e => setEntryType(e.target.value)}>
                      <option value="Buy">Buy</option>
                      <option value="Sell">Sell</option>
                    </select>
                  </div>
                  <div className="tj-form-row">
                    <label>Quantity</label>
                    <input className="tj-input" type="number" required value={entryQty} onChange={e => setEntryQty(e.target.value)} />
                  </div>
                  <div className="tj-form-row">
                    <label>Price ₹</label>
                    <input className="tj-input" type="number" step="any" required value={entryPrice} onChange={e => setEntryPrice(e.target.value)} />
                  </div>
                  <div className="tj-form-row">
                    <label>Date</label>
                    <input className="tj-input" type="date" required value={entryDate} onChange={e => setEntryDate(e.target.value)} />
                  </div>
                  <div className="tj-form-row">
                    <label>Setup</label>
                    <div style={{ display: "flex", gap: 6 }}>
                      <select className="tj-select" value={entrySetup} onChange={e => setEntrySetup(e.target.value)}>
                        {setups.map(s => <option key={s} value={s}>{s}</option>)}
                      </select>
                      <button type="button" className="tj-btn-secondary" onClick={() => setModal({ type: "add-setup" })}>+</button>
                    </div>
                  </div>
                  <div className="tj-form-row">
                    <label>Stop Loss ₹</label>
                    <input className="tj-input" type="number" step="any" value={entrySL} onChange={e => setEntrySL(e.target.value)} />
                  </div>
                  <div className="tj-form-row">
                    <label>Target ₹</label>
                    <input className="tj-input" type="number" step="any" value={entryTarget} onChange={e => setEntryTarget(e.target.value)} />
                  </div>
                </div>

                <div className="tj-card-title" style={{ marginTop: 16 }}>VCP Specifics</div>
                <div className="tj-form-grid-3">
                  <div className="tj-form-row">
                    <label>Contractions (T)</label>
                    <input className="tj-input" placeholder="e.g. 3T" value={vcpT} onChange={e => setVcpT(e.target.value)} />
                  </div>
                  <div className="tj-form-row">
                    <label>Depth %</label>
                    <input className="tj-input" placeholder="e.g. 3:1:0.5" value={vcpDepth} onChange={e => setVcpDepth(e.target.value)} />
                  </div>
                  <div className="tj-form-row">
                    <label>Vol Dry-up</label>
                    <input className="tj-input" placeholder="Yes/No" value={vcpVol} onChange={e => setVcpVol(e.target.value)} />
                  </div>
                </div>

                <div className="tj-form-row" style={{ marginTop: 12 }}>
                  <label>Chart URL</label>
                  <input className="tj-input" placeholder="https://…" value={entryImg} onChange={e => setEntryImg(e.target.value)} />
                </div>

                <div className="tj-card-title" style={{ marginTop: 16 }}>Pre-Trade Checklist</div>
                <div className="tj-checklist">
                  {CHECKLIST_ITEMS.map((item, i) => (
                    <label key={i} className="tj-check-row">
                      <input type="checkbox" checked={checkboxes[i]} onChange={() => setCheckboxes(prev => prev.map((v, j) => j === i ? !v : v))} />
                      <span>{item}</span>
                    </label>
                  ))}
                </div>
                {checkboxes.some(c => !c) && (
                  <div className="tj-checklist-warn">⚠ Complete all checklist items before entering</div>
                )}

                <button type="submit" className="tj-btn-primary" style={{ width: "100%", marginTop: 16 }}>
                  Add Trade
                </button>
              </form>
            </div>
          </div>
        </div>
      )}

      {/* ─── Tab 4: Trade Insights ─── */}
      {activeTab === 4 && (
        <div className="tj-panel">
          <div className="tj-insights-row">
            <div className="tj-card">
              <div className="tj-card-title">Hold Time Metrics</div>
              <div className="tj-stat-row"><span>Avg Hold (All)</span><strong>{fmt(avgHoldAll, 1)} days</strong></div>
              <div className="tj-stat-row pos"><span>Avg Hold (Winners)</span><strong>{fmt(avgHoldWin, 1)} days</strong></div>
              <div className="tj-stat-row neg"><span>Avg Hold (Losers)</span><strong>{fmt(avgHoldLoss, 1)} days</strong></div>
            </div>

            <div className="tj-card">
              <div className="tj-card-title">Setup Performance</div>
              {Object.keys(setupMap).length === 0 ? <div className="tj-empty">No data</div> : (
                <table className="tj-table">
                  <thead><tr><th>Setup</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Net P&L</th></tr></thead>
                  <tbody>
                    {Object.entries(setupMap).map(([s, d]) => (
                      <tr key={s}>
                        <td>{s}</td><td className="pos">{d.wins}</td><td className="neg">{d.losses}</td>
                        <td>{fmt((d.wins / (d.wins + d.losses)) * 100)}%</td>
                        <td className={d.pnl >= 0 ? "pos" : "neg"}>{fmtPnl(d.pnl)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="tj-card" style={{ marginTop: 16 }}>
            <div className="tj-card-title">Tag & Habit Analysis</div>
            {Object.keys(tagMap).length === 0 ? <div className="tj-empty">No tagged trades</div> : (
              <table className="tj-table">
                <thead><tr><th>Tag</th><th>Closed</th><th>Open</th><th>Realized P&L</th><th>Unrealized P&L</th></tr></thead>
                <tbody>
                  {Object.entries(tagMap).map(([tag, d]) => (
                    <tr key={tag}>
                      <td><span className="tj-chip sm">{tag}</span></td>
                      <td>{d.closedCount}</td><td>{d.openCount}</td>
                      <td className={d.realizedPnl >= 0 ? "pos" : "neg"}>{fmtPnl(d.realizedPnl)}</td>
                      <td className={d.unrealizedPnl >= 0 ? "pos" : "neg"}>{fmtPnl(d.unrealizedPnl)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}

      {/* ─── Tab 5: Position Sizer ─── */}
      {activeTab === 5 && (
        <div className="tj-panel tj-sizer-panel">
          <div className="tj-card tj-sizer-card">
            <div className="tj-card-title">Position Sizer</div>
            <div className="tj-form-grid-2">
              <div className="tj-form-row">
                <label>Account Equity (₹)</label>
                <input className="tj-input" type="number" value={sizerEquity} onChange={e => setSizerEquity(e.target.value)} />
              </div>
              <div className="tj-form-row">
                <label>Risk per Trade (%)</label>
                <input className="tj-input" type="number" step="0.1" value={sizerRiskPct} onChange={e => setSizerRiskPct(e.target.value)} />
              </div>
              <div className="tj-form-row">
                <label>Entry Price (₹)</label>
                <input className="tj-input" type="number" step="any" value={sizerEntry} onChange={e => setSizerEntry(e.target.value)} />
              </div>
              <div className="tj-form-row">
                <label>Stop Loss (%)</label>
                <input className="tj-input" type="number" step="0.1" value={sizerSLPct} onChange={e => setSizerSLPct(e.target.value)} />
              </div>
            </div>

            <div className="tj-sizer-results">
              <div className="tj-sizer-result-card">
                <div className="tj-sizer-result-label">Qty to Buy</div>
                <div className="tj-sizer-result-value accent">{sizerResultQty}</div>
              </div>
              <div className="tj-sizer-result-card">
                <div className="tj-sizer-result-label">SL Price</div>
                <div className="tj-sizer-result-value neg">₹{fmt(sizerResultSL)}</div>
              </div>
              <div className="tj-sizer-result-card">
                <div className="tj-sizer-result-label">Capital at Risk</div>
                <div className="tj-sizer-result-value neg">₹{fmt(sizerResultRisk)}</div>
              </div>
              <div className="tj-sizer-result-card">
                <div className="tj-sizer-result-label">Total Position Size</div>
                <div className="tj-sizer-result-value">₹{fmt(sizerResultPos)}</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ─── Tab 6: News Radar ─── */}
      {activeTab === 6 && (
        <div className="tj-panel">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <div className="tj-card-title" style={{ margin: 0 }}>News Radar — Open Positions</div>
            <button className={`tj-btn-primary ${fetchingNews ? "loading" : ""}`} onClick={fetchNews} disabled={fetchingNews}>
              {fetchingNews ? "Fetching…" : "⟳ Refresh News"}
            </button>
          </div>
          {fetchingNews && <div className="tj-loading">Fetching latest headlines…</div>}
          {!fetchingNews && newsItems.length === 0 && openPositions.length === 0 && (
            <div className="tj-empty-state">No open positions. Add a Buy trade in Smart Entry.</div>
          )}
          {!fetchingNews && newsItems.length === 0 && openPositions.length > 0 && (
            <div className="tj-empty-state">Click "Refresh News" to load headlines for your open positions.</div>
          )}
          {newsItems.map(n => (
            <div key={n.symbol} className="tj-news-card">
              <div className="tj-news-header">📰 {n.symbol}</div>
              {n.items.length === 0 ? (
                <div className="tj-news-empty">No recent news found.</div>
              ) : (
                n.items.map((item, i) => (
                  <div key={i} className="tj-news-item">
                    <a href={item.link} target="_blank" rel="noopener noreferrer" className="tj-news-title">{item.title}</a>
                    <div className="tj-news-summary">{item.summary}</div>
                    <div className="tj-news-date">🕐 {item.date}</div>
                  </div>
                ))
              )}
            </div>
          ))}
        </div>
      )}

      {/* ─── Modals ─── */}
      {modal && (
        <div className="tj-modal-overlay" onClick={e => { if ((e.target as HTMLElement).classList.contains("tj-modal-overlay")) setModal(null); }}>
          <div className="tj-modal">
            <button className="tj-modal-close" onClick={() => setModal(null)}>✕</button>

            {/* Close Position */}
            {modal.type === "close-pos" && (
              <>
                <div className="tj-modal-title">Close Position: {modal.symbol}</div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-row"><label>Exit Price</label><input className="tj-input" type="number" step="0.05" value={modalClosePrice} onChange={e => setModalClosePrice(e.target.value)} /></div>
                  <div className="tj-form-row"><label>Exit Qty (max {modal.maxQty})</label><input className="tj-input" type="number" value={modalCloseQty} max={modal.maxQty} onChange={e => setModalCloseQty(e.target.value)} /></div>
                </div>
                <div className="tj-form-row"><label>Date</label><input className="tj-input" type="date" value={modalCloseDate} onChange={e => setModalCloseDate(e.target.value)} /></div>
                <button className="tj-btn-primary" style={{ width: "100%", marginTop: 16 }} onClick={submitClose}>Confirm Close</button>
              </>
            )}

            {/* Edit Closed Trade */}
            {modal.type === "edit-closed" && (
              <>
                <div className="tj-modal-title">Edit Closed Trade</div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-row"><label>Avg Entry Price</label><input className="tj-input" type="number" step="any" value={modalEditEntryPx} onChange={e => setModalEditEntryPx(e.target.value)} /></div>
                  <div className="tj-form-row"><label>Exit Price</label><input className="tj-input" type="number" step="any" value={modalEditExitPx} onChange={e => setModalEditExitPx(e.target.value)} /></div>
                </div>
                <div className="tj-card-title" style={{ marginTop: 12 }}>Tags</div>
                <div className="tj-chip-container">
                  {PREDEFINED_TAGS.map(tag => (
                    <div key={tag} className={`tj-chip clickable ${modalEditTags.has(tag) ? "selected" : ""}`}
                      onClick={() => setModalEditTags(prev => { const n = new Set(prev); n.has(tag) ? n.delete(tag) : n.add(tag); return n; })}>
                      {tag}
                    </div>
                  ))}
                </div>
                <input className="tj-input" style={{ marginTop: 8 }} placeholder="Custom tags (comma separated)…" value={modalEditCustomTags} onChange={e => setModalEditCustomTags(e.target.value)} />
                <div className="tj-form-row" style={{ marginTop: 8 }}><label>Remarks</label><textarea className="tj-textarea" rows={2} value={modalEditRemarks} onChange={e => setModalEditRemarks(e.target.value)} /></div>
                <div className="tj-form-row"><label>Chart URL</label><input className="tj-input" value={modalEditImg} onChange={e => setModalEditImg(e.target.value)} /></div>
                <button className="tj-btn-primary" style={{ width: "100%", marginTop: 16 }} onClick={saveClosedEdits}>Save Changes</button>
              </>
            )}

            {/* Review Open Position */}
            {modal.type === "edit-open" && (
              <>
                <div className="tj-modal-title">Review Position: {modal.symbol}</div>
                <div className="tj-form-grid-2">
                  <div className="tj-form-row"><label>Avg Entry Price</label><input className="tj-input" type="number" step="any" value={modalOpenAvgPx} onChange={e => setModalOpenAvgPx(e.target.value)} /></div>
                  <div className="tj-form-row"><label>Stop Loss ₹</label><input className="tj-input" type="number" step="any" value={modalOpenSL} onChange={e => setModalOpenSL(e.target.value)} /></div>
                </div>
                <div className="tj-form-row"><label>Yahoo Finance Ticker</label><input className="tj-input" placeholder="e.g. RELIANCE.NS" value={modalOpenFetchTicker} onChange={e => setModalOpenFetchTicker(e.target.value)} /></div>
                <div className="tj-card-title" style={{ marginTop: 12 }}>Tags</div>
                <div className="tj-chip-container">
                  {PREDEFINED_TAGS.map(tag => (
                    <div key={tag} className={`tj-chip clickable ${modalEditTags.has(tag) ? "selected" : ""}`}
                      onClick={() => setModalEditTags(prev => { const n = new Set(prev); n.has(tag) ? n.delete(tag) : n.add(tag); return n; })}>
                      {tag}
                    </div>
                  ))}
                </div>
                <div className="tj-form-row" style={{ marginTop: 8 }}><label>Remarks</label><textarea className="tj-textarea" rows={2} value={modalEditRemarks} onChange={e => setModalEditRemarks(e.target.value)} /></div>
                <button className="tj-btn-primary" style={{ width: "100%", marginTop: 16 }} onClick={saveReviewEdits}>Save</button>
              </>
            )}

            {/* Add Setup */}
            {modal.type === "add-setup" && (
              <>
                <div className="tj-modal-title">Add New Setup Type</div>
                <div className="tj-form-row"><label>Setup Name</label><input className="tj-input" value={newSetupName} onChange={e => setNewSetupName(e.target.value)} placeholder="e.g. Ascending Triangle" /></div>
                <button className="tj-btn-primary" style={{ width: "100%", marginTop: 16 }} onClick={saveNewSetup}>Add Setup</button>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
